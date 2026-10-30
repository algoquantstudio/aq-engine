//! Validate an entire leased publisher file before initialization or handoff.
//! No payloads are decoded, no identities are rewritten and no application rows
//! are mutated. The min/max probes use scope indexes where those are available.
use super::*;
use crate::core::live_sync::CaptureOwnerLease;

fn invalid() -> OutboxStoreError {
    OutboxStoreError::InvalidRecord(
        "publisher recovery file is outside its registered strategy/session; source retained"
            .into(),
    )
}
fn db(error: impl std::fmt::Display) -> OutboxStoreError {
    OutboxStoreError::Database(error.to_string())
}

impl OutboxStore {
    /// Call while holding the producer/import lease, BEFORE opening an existing
    /// source with `OutboxStore::open*` (which can upgrade or prune old rows).
    /// Runtime and run IDs may differ legitimately after a host restart; the
    /// strategy/session tuple must agree across all retained metadata and rows.
    pub async fn validate_publisher_file_scope(
        path: &Path,
        lease: &CaptureOwnerLease,
        strategy: &str,
        session: &str,
    ) -> Result<(), OutboxStoreError> {
        if !lease.matches_path(path)
            || [strategy, session]
                .iter()
                .any(|v| v.is_empty() || v.len() > 512)
            || (!lease.strategy.is_empty()
                && (lease.strategy != strategy || lease.session != session))
        {
            return Err(invalid());
        }
        let mut exists = false;
        let mut empty = false;
        let mut sidecars = false;
        for suffix in ["", "-wal", "-shm", "-journal"] {
            let mut file = path.as_os_str().to_os_string();
            file.push(suffix);
            match std::fs::symlink_metadata(PathBuf::from(file)) {
                Ok(metadata) => {
                    #[cfg(windows)]
                    {
                        use std::os::windows::fs::MetadataExt;
                        if metadata.file_attributes() & 0x400 != 0 {
                            return Err(invalid());
                        }
                    }
                    if !metadata.is_file() || metadata.file_type().is_symlink() {
                        return Err(invalid());
                    }
                    if suffix.is_empty() {
                        exists = true;
                        empty = metadata.len() == 0;
                    } else if !exists {
                        return Err(invalid());
                    } else {
                        sidecars = true;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(db(error)),
            }
        }
        if !exists {
            return Ok(());
        }
        // A crash before first schema initialization may leave an empty file.
        // With no sidecars it contains no evidence to adopt; permit normal retry.
        if empty {
            return if sidecars { Err(invalid()) } else { Ok(()) };
        }
        let database = Builder::new_local(path.to_str().ok_or_else(invalid)?)
            .build()
            .await
            .map_err(db)?;
        let connection = database.connect().map_err(db)?;
        crate::core::utils::storage_format::require_current(
            &connection, "live outbox", 1095847746, 1,
        )
        .await
        .map_err(OutboxStoreError::InvalidRecord)?;
        connection
            .execute_batch(
                "PRAGMA cache_size=-2048; PRAGMA busy_timeout=1000; PRAGMA query_only=1;",
            )
            .await
            .map_err(db)?;
        let mut columns = connection
            .query("PRAGMA table_info(outbox_events)", ())
            .await
            .map_err(db)?;
        let mut core = std::collections::HashSet::new();
        while let Some(row) = columns.next().await.map_err(db)? {
            core.insert(row.get::<String>(1).map_err(db)?);
        }
        drop(columns);
        if ![
            "event_id",
            "wire_payload",
            "strategy_id",
            "live_session_id",
            "state",
        ]
        .iter()
        .all(|name| core.contains(*name))
        {
            return Err(invalid());
        }
        // Capacity tables may be absent before capture is enabled. A missing core
        // table is an unrelated SQLite file, never an invitation to initialize it.
        for (table, has_session) in [
            ("outbox_events", true),
            ("safety_observations", true),
            ("sync_cursors", true),
            ("publisher_sequence_cursors", true),
            ("publisher_event_reservations", true),
            ("reconciliation_jobs", true),
            ("processed_actions", true),
            ("latency_traces", true),
            ("capture_pauses", true),
            ("capture_source_observation", true),
            ("capture_gap_notices", true),
            ("outbox_batches", false),
            ("dead_letters", false),
            ("capture_journal_gap_handoffs", false),
        ] {
            let mut rows = connection
                .query(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
                    params![table],
                )
                .await
                .map_err(db)?;
            let present = rows.next().await.map_err(db)?.is_some();
            drop(rows);
            if !present {
                if table == "outbox_events" {
                    return Err(invalid());
                }
                continue;
            }
            for order in ["ASC", "DESC"] {
                // Table/column/order fragments are fixed above, never user SQL.
                let fields = if has_session {
                    "CASE WHEN length(CAST(strategy_id AS BLOB))<=512 THEN strategy_id ELSE NULL END,
                     CASE WHEN length(CAST(live_session_id AS BLOB))<=512 THEN live_session_id ELSE NULL END"
                } else {
                    "CASE WHEN length(CAST(strategy_id AS BLOB))<=512 THEN strategy_id ELSE NULL END"
                };
                let ordering = if has_session {
                    format!("strategy_id {order},live_session_id {order}")
                } else {
                    format!("strategy_id {order}")
                };
                let query = format!("SELECT {fields} FROM {table} ORDER BY {ordering} LIMIT 1");
                let mut rows = connection.query(&query, ()).await.map_err(db)?;
                if let Some(row) = rows.next().await.map_err(db)? {
                    if row.get::<Option<String>>(0).map_err(db)?.as_deref() != Some(strategy)
                        || (has_session
                            && row.get::<Option<String>>(1).map_err(db)?.as_deref()
                                != Some(session))
                    {
                        return Err(invalid());
                    }
                }
            }
        }
        Ok(())
    }

    /// Whole-file retirement is stronger than a single registration's drain
    /// count. Unknown/foreign failed rows or diagnostic work must retain the file.
    pub async fn publisher_retirement_uncommitted(&self) -> Result<u64, OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let mut rows = self
            .connection
            .query(
                "SELECT (SELECT COUNT(*) FROM outbox_events WHERE state != 'committed') +
                    (SELECT COUNT(*) FROM safety_observations WHERE delivered=0) +
                    (SELECT COUNT(*) FROM capture_gap_notices)",
                (),
            )
            .await
            .map_err(db)?;
        let row = rows.next().await.map_err(db)?.ok_or_else(invalid)?;
        u64::try_from(row.get::<i64>(0).map_err(db)?).map_err(|_| invalid())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (PathBuf, PathBuf) {
        let root =
            std::env::temp_dir().join(format!("aqe-publisher-scope-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("publisher.db");
        (root, path)
    }

    #[tokio::test]
    #[ignore = "owned subprocess for environment-isolated publisher startup"]
    async fn publisher_startup_scope_child() {
        let root = PathBuf::from(
            std::env::var("AQE_DISPOSABLE_PUBLISHER_SCOPE_ROOT").expect("owned fixture"),
        );
        assert!(root.starts_with(std::env::temp_dir()));
        assert!(
            root.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("aqe-publisher-scope-")
        );
        let result = crate::core::live_sync::LocalEventPublisher::from_environment(
            Some(root.join("publisher.db")),
            Some(("strategy", "session")),
            Default::default(),
        )
        .await;
        match result {
            Err(error) => assert!(
                error.to_string().contains("outside its registered"),
                "{error}"
            ),
            Ok(_) => panic!("publisher startup adopted a foreign source"),
        }
    }

    #[tokio::test]
    async fn actual_publisher_startup_refuses_a_foreign_existing_file() {
        let (root, path) = fixture();
        let store = OutboxStore::open(&path).await.unwrap();
        let event = EventEnvelope::new(
            "old-runtime",
            "foreign",
            "session",
            "data",
            1,
            EventPriority::Critical,
            EventKind::InsightProjection,
            serde_json::json!({"value":42}),
        );
        store.insert_events(&[event]).await.unwrap();
        drop(store);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "core::live_sync::outbox::publisher_scope::tests::publisher_startup_scope_child",
                "--ignored",
                "--nocapture",
            ])
            .env("AQE_DISPOSABLE_PUBLISHER_SCOPE_ROOT", &root)
            .env("AQE_EVENT_BUS", "in_process")
            .env("AQE_RUNTIME_ID", "runtime")
            .env("AQE_RUN_ID", "run")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let store = OutboxStore::open(&path).await.unwrap();
        assert_eq!(store.pending_count().await.unwrap(), 1);
        assert_eq!(
            store
                .pending_batch(EventPriority::Critical, 1, 10000)
                .await
                .unwrap()[0]
                .event
                .strategy_id,
            "foreign"
        );
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn whole_file_validation_keeps_runtime_history_but_rejects_a_foreign_session() {
        let (root, path) = fixture();
        let lease = CaptureOwnerLease::acquire_for_import(&path).unwrap();
        OutboxStore::validate_publisher_file_scope(&path, &lease, "strategy", "session")
            .await
            .unwrap();
        assert!(
            !path.exists(),
            "an absent source is not initialized by validation"
        );
        std::fs::write(&path, b"").unwrap();
        OutboxStore::validate_publisher_file_scope(&path, &lease, "strategy", "session")
            .await
            .unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
        let store = OutboxStore::open(&path).await.unwrap();
        for runtime in ["old-runtime", "current-runtime"] {
            let event = EventEnvelope::new(
                runtime,
                "strategy",
                "session",
                runtime,
                1,
                EventPriority::Critical,
                EventKind::InsightProjection,
                serde_json::json!({"value":42}),
            );
            store.insert_events(&[event]).await.unwrap();
        }
        drop(store);
        OutboxStore::validate_publisher_file_scope(&path, &lease, "strategy", "session")
            .await
            .unwrap();
        assert!(
            OutboxStore::validate_publisher_file_scope(&path, &lease, "strategy", "other-session")
                .await
                .is_err()
        );
        let store = OutboxStore::open(&path).await.unwrap();
        assert_eq!(store.pending_count().await.unwrap(), 2);
        assert_eq!(store.publisher_retirement_uncommitted().await.unwrap(), 2);
        drop(store);
        drop(lease);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn scope_validation_rejects_retired_format_without_reinitializing_it() {
        let (root, path) = fixture();
        let lease = CaptureOwnerLease::acquire_for_import(&path).unwrap();
        let store = OutboxStore::open(&path).await.unwrap();
        store.connection.execute_batch("PRAGMA user_version=0;").await.unwrap();
        drop(store);
        let error = OutboxStore::validate_publisher_file_scope(
            &path, &lease, "strategy", "session",
        ).await.unwrap_err();
        assert!(error.to_string().contains("unsupported live outbox saved-data format"));
        let database = Builder::new_local(path.to_str().unwrap()).build().await.unwrap();
        let connection = database.connect().unwrap();
        let mut rows = connection.query("PRAGMA user_version", ()).await.unwrap();
        assert_eq!(rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(), 0);
        drop(rows);
        drop(connection);
        drop(database);
        drop(lease);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn scope_validation_refuses_unrelated_sqlite_without_initialization() {
        let (root, path) = fixture();
        let lease = CaptureOwnerLease::acquire_for_import(&path).unwrap();
        let database = Builder::new_local(path.to_str().unwrap())
            .build()
            .await
            .unwrap();
        let connection = database.connect().unwrap();
        connection.execute_batch("CREATE TABLE valuable_journal (value TEXT); INSERT INTO valuable_journal VALUES('preserve');").await.unwrap();
        drop(connection);
        drop(database);
        assert!(
            OutboxStore::validate_publisher_file_scope(&path, &lease, "strategy", "session")
                .await
                .is_err()
        );
        let database = Builder::new_local(path.to_str().unwrap())
            .build()
            .await
            .unwrap();
        let connection = database.connect().unwrap();
        let mut rows = connection
            .query("SELECT value FROM valuable_journal", ())
            .await
            .unwrap();
        assert_eq!(
            rows.next()
                .await
                .unwrap()
                .unwrap()
                .get::<String>(0)
                .unwrap(),
            "preserve"
        );
        drop(rows);
        let mut rows = connection
            .query("SELECT 1 FROM sqlite_master WHERE name='outbox_events'", ())
            .await
            .unwrap();
        assert!(rows.next().await.unwrap().is_none());
        drop(rows);
        drop(connection);
        drop(database);
        drop(lease);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn scope_validation_checks_foreign_metadata_even_without_economic_rows() {
        let (root, path) = fixture();
        let lease = CaptureOwnerLease::acquire_for_import(&path).unwrap();
        let store = OutboxStore::open(&path).await.unwrap();
        store.ensure_capture_capacity_schema().await.unwrap();
        store.connection.execute("INSERT INTO publisher_sequence_cursors(runtime_id,strategy_id,live_session_id,critical_through,updated_at_us) VALUES('old-runtime','strategy','foreign',42,1)", ()).await.unwrap();
        drop(store);
        let error =
            OutboxStore::validate_publisher_file_scope(&path, &lease, "strategy", "session")
                .await
                .unwrap_err();
        assert!(error.to_string().contains("outside its registered"));
        let store = OutboxStore::open(&path).await.unwrap();
        let mut rows = store
            .connection
            .query(
                "SELECT critical_through FROM publisher_sequence_cursors",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            42
        );
        drop(rows);
        drop(store);
        drop(lease);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn publisher_retirement_keeps_failed_rows_outside_the_requested_scope() {
        let (root, path) = fixture();
        let store = OutboxStore::open(&path).await.unwrap();
        let event = EventEnvelope::new(
            "runtime",
            "foreign",
            "foreign-session",
            "data",
            1,
            EventPriority::Critical,
            EventKind::InsightProjection,
            serde_json::json!({"value":42}),
        );
        store.insert_events(&[event]).await.unwrap();
        store
            .connection
            .execute("UPDATE outbox_events SET state='failed'", ())
            .await
            .unwrap();
        assert_eq!(store.pending_count().await.unwrap(), 0);
        assert_eq!(
            store
                .retirement_uncommitted_for_scope("strategy", "session")
                .await
                .unwrap(),
            0
        );
        assert_eq!(store.publisher_retirement_uncommitted().await.unwrap(), 1);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn high_cardinality_scope_probes_use_indexes_in_both_directions() {
        let (root, path) = fixture();
        let store = OutboxStore::open(&path).await.unwrap();
        for (table, index) in [
            ("outbox_events", "idx_outbox_scope_state"),
            (
                "publisher_event_reservations",
                "idx_publisher_reservations_scope",
            ),
            ("latency_traces", "idx_latency_traces_scope"),
            ("safety_observations", "idx_safety_scope"),
        ] {
            for order in ["ASC", "DESC"] {
                let mut rows = store.connection.query(&format!(
                    "EXPLAIN QUERY PLAN SELECT CASE WHEN length(CAST(strategy_id AS BLOB))<=512 THEN strategy_id ELSE NULL END,
                     CASE WHEN length(CAST(live_session_id AS BLOB))<=512 THEN live_session_id ELSE NULL END
                     FROM {table} ORDER BY strategy_id {order},live_session_id {order} LIMIT 1"
                ), ()).await.unwrap();
                let mut plan = String::new();
                while let Some(row) = rows.next().await.unwrap() {
                    plan.push_str(&row.get::<String>(3).unwrap());
                }
                assert!(plan.contains(index), "{table} {order}: {plan}");
                assert!(!plan.to_uppercase().contains("TEMP B-TREE"), "{plan}");
            }
        }
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn scope_validation_refuses_linked_sqlite_sidecars() {
        let (root, path) = fixture();
        let lease = CaptureOwnerLease::acquire_for_import(&path).unwrap();
        let store = OutboxStore::open(&path).await.unwrap();
        drop(store);
        let foreign = root.join("foreign");
        std::fs::write(&foreign, b"preserve").unwrap();
        let sidecar = PathBuf::from(format!("{}-journal", path.display()));
        std::os::unix::fs::symlink(&foreign, &sidecar).unwrap();
        assert!(
            OutboxStore::validate_publisher_file_scope(&path, &lease, "strategy", "session")
                .await
                .is_err()
        );
        assert_eq!(std::fs::read(&foreign).unwrap(), b"preserve");
        drop(lease);
        std::fs::remove_dir_all(root).unwrap();
    }
}
