use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
pub const SOURCE_OBSERVATION_LIFECYCLE_VERSION: u16 = 1;
pub const PUBLISHER_IMPORT_BOUND_VERSION: u16 = 3;
mod registration_audit;
mod scope_files;
pub use scope_files::publisher_outbox_path;
pub use registration_audit::{
    REGISTRATION_AUDIT_DIRECTORY, RegistrationAudit, RegistrationAuditDecision,
};

/// One process-output file owned by an AQS-managed strategy. Remote runners redirect
/// stdout/stderr to files because the desktop must not keep an SSH session open for the
/// lifetime of the strategy; the runtime host tails these files onto the private log subject.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeLogSource {
    pub stream: String,
    pub path: PathBuf,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeHostMetadata {
    pub protocol_version: u16,
    #[serde(default)]
    pub host_version: String,
    pub runtime_id: String,
    pub pid: u32,
    pub nats_pid: u32,
    pub nats_url: String,
    pub nats_token: String,
    pub runtime_directory: PathBuf,
    pub started_at_us: i64,
}

impl RuntimeHostMetadata {
    pub fn read(path: impl AsRef<Path>) -> Result<Self, String> {
        use std::io::Read;
        const MAX_BYTES: u64 = 64 * 1024;
        let path = path.as_ref();
        let before = std::fs::symlink_metadata(path)
            .map_err(|_| "Cannot inspect private runtime metadata")?;
        #[cfg(windows)]
        {
            use std::os::windows::fs::MetadataExt;
            if before.file_attributes() & 0x400 != 0 {
                return Err("Runtime metadata must not be a reparse point".into());
            }
        }
        if !before.is_file() || before.file_type().is_symlink() || before.len() > MAX_BYTES {
            return Err("Runtime metadata must be a bounded regular file, not a link".into());
        }
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let file = options
            .open(path)
            .map_err(|_| "Cannot open private runtime metadata")?;
        let opened = file
            .metadata()
            .map_err(|_| "Cannot inspect opened runtime metadata")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if before.dev() != opened.dev() || before.ino() != opened.ino() {
                return Err("Runtime metadata changed while opening".into());
            }
        }
        if !opened.is_file() || opened.len() > MAX_BYTES {
            return Err("Runtime metadata must be a bounded regular file".into());
        }
        let mut bytes = Vec::with_capacity(opened.len() as usize);
        file.take(MAX_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| "Cannot read private runtime metadata")?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err("Runtime metadata is too large".into());
        }
        serde_json::from_slice(&bytes)
            .map_err(|_| "Invalid private runtime metadata JSON (contents redacted)".into())
    }
}

/// Private, per-strategy registration written by AQS and consumed by the runtime host.
/// It is never published to NATS or SurrealDB because it contains the live-session token.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeStrategyRegistration {
    pub protocol_version: u16,
    #[serde(default)]
    pub registration_generation: u64,
    pub runtime_id: String,
    #[serde(default)]
    pub run_id: String,
    pub strategy_id: String,
    pub live_session_id: String,
    pub process_id: u32,
    pub user_id: String,
    pub server_node_id: String,
    pub batch_url: String,
    /// None inherits the host's legacy environment setting. Some("") explicitly
    /// retains direct database routing for this process registration.
    #[serde(default)]
    pub event_gateway_url: Option<String>,
    pub actions_url: String,
    pub signin_url: String,
    pub namespace: String,
    pub database: String,
    pub access_method: String,
    pub bearer_token: String,
    #[serde(default)]
    pub session_secret: Option<String>,
    #[serde(default)]
    pub log_sources: Vec<RuntimeLogSource>,
    #[serde(default)]
    pub created_at_us: i64,
}

impl RuntimeStrategyRegistration {
    pub fn registration_key(&self) -> String {
        scope_files::scope_stem(b"aqs-runtime-registration-file-v1\0", "registration", &self.strategy_id, &self.live_session_id)
    }

    /// A single portable filename component, never a raw database key or shell
    /// fragment. Preserve existing ordinary names for rolling-host compatibility;
    /// opaque/long IDs use a length-delimited, domain-separated digest.
    pub fn registration_file_stem(&self) -> String {
        self.registration_key()
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeHostHealth {
    #[serde(default, skip_serializing_if = "is_zero")]
    pub orphaned_registrations: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub orphaned_strategy_ids: Vec<String>,
    pub runtime_id: String,
    pub nats_running: bool,
    pub registered_strategies: usize,
    pub active_strategies: usize,
    #[serde(default)]
    pub managed_sync_agents: usize,
    #[serde(default)]
    pub runtime_host_memory_bytes: u64,
    #[serde(default)]
    pub nats_memory_bytes: u64,
    #[serde(default)]
    pub outbox_database_bytes: u64,
    /// Optional local payload budget, not a subscription/billing allowance or a
    /// proven cap on SQLite/WAL/metadata/physical disk bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outbox_payload_limit_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outbox_retained_payload_bytes: Option<u64>,
    /// A per-store admission estimate, not a combined filesystem quota.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outbox_physical_capture: Option<super::PhysicalCaptureUsage>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub deferred_publisher_imports: usize,
    /// Zero/absent means an older host without an exclusive source-import
    /// fence. Roll out all compatible producers/importers before capture caps.
    #[serde(default, skip_serializing_if = "is_zero_version")]
    pub capture_owner_lease_version: u16,
    /// Zero/absent retains unresolved source gaps but cannot hand them to the
    /// bounded diagnostic mailbox and authenticated remote commit endpoint.
    #[serde(default, skip_serializing_if = "is_zero_version")]
    pub capture_gap_notice_version: u16,
    /// Independent local metadata request/reply reader; older hosts can import
    /// exited sources but cannot accept notices from a still-running producer.
    #[serde(default, skip_serializing_if = "is_zero_version")]
    pub capture_gap_local_handoff_version: u16,
    /// Older importers cannot settle an unclosed source-observation witness.
    #[serde(default, skip_serializing_if = "is_zero_version")]
    pub source_observation_lifecycle_version: u16,
    /// Importer can discover pre-first-witness pauses retained only in a
    /// command journal. Absent/zero hosts are not capture-cap compatible.
    #[serde(default, skip_serializing_if = "is_zero_version")]
    pub journal_recovery_link_version: u16,
    /// Operational outbox retries/receipts use bounded post-image commits.
    #[serde(default, skip_serializing_if = "is_zero_version")]
    pub outbox_metadata_bound_version: u16,
    /// V1 bounds exited publisher imports. V2 also verifies whole-file scope
    /// and discovers collision-resistant names. V3 requires the current saved
    /// format and no longer adopts lossy legacy filenames.
    #[serde(default, skip_serializing_if = "is_zero_version")]
    pub publisher_import_bound_version: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publisher_import_physical_limit_bytes: Option<u64>,
    pub last_updated_at_us: i64,
}

fn is_zero(value: &usize) -> bool {
    *value == 0
}
fn is_zero_version(value: &u16) -> bool {
    *value == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_reads_are_bounded_and_parse_errors_do_not_echo_credentials() {
        let root =
            std::env::temp_dir().join(format!("aqs-metadata-reader-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("runtime.json");
        std::fs::write(&path, br#"{"protocolVersion":"a-sensitive-invalid-value"}"#).unwrap();
        let error = RuntimeHostMetadata::read(&path).unwrap_err();
        assert!(error.contains("redacted"));
        assert!(!error.contains("sensitive-invalid"));
        std::fs::write(&path, vec![b' '; 64 * 1024 + 1]).unwrap();
        assert!(
            RuntimeHostMetadata::read(&path)
                .unwrap_err()
                .contains("bounded")
        );
        #[cfg(unix)]
        {
            let alias = root.join("linked.json");
            std::os::unix::fs::symlink(&path, &alias).unwrap();
            assert!(
                RuntimeHostMetadata::read(&alias)
                    .unwrap_err()
                    .contains("not a link")
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn registration_filenames_keep_ordinary_ids_but_never_interpret_opaque_keys() {
        let registration = |strategy: &str, session: &str| RuntimeStrategyRegistration {
            protocol_version: 1,
            registration_generation: 1,
            runtime_id: "runtime".into(),
            run_id: "run".into(),
            strategy_id: strategy.into(),
            live_session_id: session.into(),
            process_id: 1,
            user_id: "users:test".into(),
            server_node_id: "local".into(),
            batch_url: String::new(),
            event_gateway_url: None,
            actions_url: String::new(),
            signin_url: String::new(),
            namespace: "aqs".into(),
            database: "aqs".into(),
            access_method: "aqe_live".into(),
            bearer_token: "private".into(),
            session_secret: None,
            log_sources: vec![],
            created_at_us: 1,
        };
        assert_eq!(
            registration("strategy-a", "session-b").registration_file_stem(),
            "strategy-a--session-b"
        );
        let mut names = std::collections::HashSet::new();
        for strategy in [
            "../outside",
            r"C:\\outside:stream",
            "$(arbitrary shell text)",
            "a/../b",
            "a:b",
            "東京",
            "x\0y",
            &"x".repeat(300),
        ] {
            let item = registration(strategy, "session");
            let name = item.registration_file_stem();
            assert_eq!(Path::new(&name).components().count(), 1);
            assert!(name.len() < 100 && !name.contains("--"));
            assert!(
                name.bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            );
            assert!(names.insert(name.clone()));
            assert_eq!(name, item.registration_file_stem());
            assert_eq!(item.strategy_id, strategy);
        }
        assert_ne!(
            registration("a\0b", "c").registration_file_stem(),
            registration("a", "b\0c").registration_file_stem()
        );
        for ((a, b), (c, d)) in [
            (("a--b", "c"), ("a", "b--c")),
            (("a-", "b"), ("a", "-b")),
            (("Case", "session"), ("case", "session")),
        ] {
            assert_ne!(registration(a, b).registration_key().to_lowercase(),
                registration(c, d).registration_key().to_lowercase());
        }
    }

    #[test]
    fn old_host_health_does_not_enable_payload_limits_or_emit_new_empty_fields() {
        let health: RuntimeHostHealth = serde_json::from_value(serde_json::json!({
            "runtimeId":"fixture", "natsRunning":true, "registeredStrategies":1,
            "activeStrategies":1, "lastUpdatedAtUs":1,
        }))
        .unwrap();
        assert_eq!(health.outbox_payload_limit_bytes, None);
        assert_eq!(health.outbox_retained_payload_bytes, None);
        assert_eq!(health.outbox_physical_capture, None);
        assert_eq!(health.deferred_publisher_imports, 0);
        assert_eq!(health.capture_owner_lease_version, 0);
        assert_eq!(health.capture_gap_notice_version, 0);
        assert_eq!(health.capture_gap_local_handoff_version, 0);
        assert_eq!(health.source_observation_lifecycle_version, 0);
        assert_eq!(health.journal_recovery_link_version, 0);
        assert_eq!(health.outbox_metadata_bound_version, 0);
        assert_eq!(health.publisher_import_bound_version, 0);
        assert_eq!(health.publisher_import_physical_limit_bytes, None);
        assert!(
            serde_json::to_value(&health)
                .unwrap()
                .get("outboxMetadataBoundVersion")
                .is_none()
        );
        let value = serde_json::to_value(health).unwrap();
        assert!(value.get("sourceObservationLifecycleVersion").is_none());
        assert!(value.get("journalRecoveryLinkVersion").is_none());
        for field in [
            "outboxPayloadLimitBytes",
            "outboxRetainedPayloadBytes",
            "outboxPhysicalCapture",
            "deferredPublisherImports",
            "captureOwnerLeaseVersion",
            "captureGapNoticeVersion",
            "captureGapLocalHandoffVersion",
        ] {
            assert!(
                value.get(field).is_none(),
                "{field} must be omitted for legacy/default operation"
            );
        }
    }
}
