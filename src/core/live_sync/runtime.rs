use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
mod registration_audit;
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

#[derive(Clone, Debug, Serialize, Deserialize)]
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
        let bytes = std::fs::read(path).map_err(|error| error.to_string())?;
        serde_json::from_slice(&bytes).map_err(|error| error.to_string())
    }
}

/// Private, per-strategy registration written by AQS and consumed by the runtime host.
/// It is never published to NATS or SurrealDB because it contains the live-session token.
#[derive(Clone, Debug, Serialize, Deserialize)]
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
        format!("{}--{}", self.strategy_id, self.live_session_id)
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
    pub last_updated_at_us: i64,
}

fn is_zero(value: &usize) -> bool {
    *value == 0
}
