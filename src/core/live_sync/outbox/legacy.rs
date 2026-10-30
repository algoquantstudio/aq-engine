//! Bounded, resumable legacy upgrades. Never materialize an entire retained
//! history, or fetch an oversized BLOB only to reject it afterwards.
use super::*;

pub(super) const MAX_LEGACY_ENVELOPE_BYTES: usize = 8 * 1024 * 1024;
const MAX_OFFLINE_ENVELOPE_BYTES: usize = 64 * 1024 * 1024;

fn envelope_query(predicate: &str, after: bool) -> String {
    let operator = if after { ">" } else { ">=" };
    // The runtime-scope OR predicate can otherwise select a scope index and
    // repeatedly sort the remaining history. Walk the existing primary key.
    format!(
        "SELECT event_id,length(wire_payload),
        CASE WHEN length(wire_payload)<=?2 THEN wire_payload ELSE NULL END
        FROM outbox_events INDEXED BY sqlite_autoindex_outbox_events_1
        WHERE ({predicate}) AND event_id {operator} ?1 ORDER BY event_id LIMIT 1"
    )
}

impl OutboxStore {
    /// Explicit operator-only maintenance on a stopped publisher's outbox.
    /// Runtime startup keeps its 8 MiB limit. This one-shot operation raises it
    /// only within a finite allocation, holds the producer ownership lease,
    /// retains original envelopes and uses the normal deterministic upgrade.
    /// Take an offline backup first; this does not convert broker journals.
    pub async fn upgrade_legacy_offline(
        path: impl AsRef<Path>,
        budget: PhysicalCaptureBudget,
        envelope_limit_bytes: usize,
    ) -> Result<OutboxHealth, OutboxStoreError> {
        if !(MAX_LEGACY_ENVELOPE_BYTES..=MAX_OFFLINE_ENVELOPE_BYTES)
            .contains(&envelope_limit_bytes)
        {
            return Err(OutboxStoreError::InvalidRecord(
                "offline envelope allocation must be between 8 and 64 MiB".into(),
            ));
        }
        let path = path.as_ref();
        let metadata = std::fs::symlink_metadata(path)
            .map_err(|_| OutboxStoreError::InvalidRecord("existing offline outbox required".into()))?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(OutboxStoreError::InvalidRecord("regular offline outbox required".into()));
        }
        let budget = budget.validate()?;
        let _owner = super::super::CaptureOwnerLease::acquire_for_import(path)
            .map_err(|error| OutboxStoreError::InvalidRecord(error.to_string()))?;
        // Do not initialize an execution journal or an unrelated SQLite file.
        let database = Builder::new_local(path.to_string_lossy().as_ref()).build().await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let connection = database.connect()
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let mut columns = connection.query("PRAGMA table_info(outbox_events)", ()).await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let mut names = std::collections::HashSet::new();
        while let Some(row) = columns.next().await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
        {
            names.insert(row.get::<String>(1)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?);
        }
        if !["event_id", "wire_payload", "strategy_id", "live_session_id", "state"]
            .iter().all(|name| names.contains(*name))
        {
            return Err(OutboxStoreError::InvalidRecord("not a publisher outbox; no upgrade performed".into()));
        }
        drop(columns);
        drop(connection);
        drop(database);
        let store = Self::open_configured_for_legacy_upgrade(
            path, Some(budget), envelope_limit_bytes,
        ).await?;
        store.ensure_capture_capacity_schema().await?;
        store.try_reclaim_physical_wal().await?;
        store.health().await
    }

    /// Decode one envelope at a time, but commit small scope-only pages. This
    /// avoids one write transaction per event without retaining payload pages.
    /// Startup owns this connection; every refused page rolls back together.
    pub(super) async fn backfill_legacy_runtime_scope(&self) -> Result<(), OutboxStoreError> {
        let mut after = None;
        loop {
            let mut updates = Vec::with_capacity(64);
            let mut scope_bytes = 0usize;
            while updates.len() < 64 && scope_bytes < 64 * 1024 {
                let Some((id, payload)) = self
                    .next_legacy_envelope("runtime_id IS NULL OR runtime_id=''", after.as_deref())
                    .await?
                else {
                    break;
                };
                after = Some(id.clone());
                let event = FlatbufferEventCodec::decode(&payload)?;
                scope_bytes = scope_bytes
                    .saturating_add(id.len())
                    .saturating_add(event.runtime_id.len());
                updates.push((id, event.runtime_id));
            }
            if updates.is_empty() {
                return Ok(());
            }
            let tx = turso::transaction::Transaction::new_unchecked(
                &self.connection,
                turso::transaction::TransactionBehavior::Immediate,
            )
            .await
            .map_err(|e| OutboxStoreError::Database(e.to_string()))?;
            let result=async {
                for (id,runtime) in updates {
                    self.connection.execute(
                        "UPDATE outbox_events SET runtime_id=?2 WHERE event_id=?1 AND (runtime_id IS NULL OR runtime_id='')",
                        params![id,runtime],
                    ).await.map_err(|e|OutboxStoreError::Database(e.to_string()))?;
                }
                Ok::<_,OutboxStoreError>(())
            }.await;
            match result {
                Ok(()) => self.commit_bounded_metadata(tx).await?,
                Err(error) => {
                    let _ = tx.rollback().await;
                    return Err(error);
                }
            }
        }
    }

    /// Predicates are internal constants, never supplied by a client. The PK
    /// cursor stays indexable and the row reader is dropped before any write.
    pub(super) async fn next_legacy_envelope(
        &self,
        predicate: &str,
        after: Option<&str>,
    ) -> Result<Option<(String, Vec<u8>)>, OutboxStoreError> {
        let mut rows = self
            .connection
            .query(
                envelope_query(predicate, after.is_some()),
                params![after.unwrap_or(""), self.legacy_envelope_limit_bytes as i64],
            )
            .await
            .map_err(|e| OutboxStoreError::Database(e.to_string()))?;
        let Some(row) = rows
            .next()
            .await
            .map_err(|e| OutboxStoreError::Database(e.to_string()))?
        else {
            return Ok(None);
        };
        let bytes = row
            .get::<i64>(1)
            .map_err(|e| OutboxStoreError::Database(e.to_string()))?;
        if bytes < 0 || bytes as u64 > self.legacy_envelope_limit_bytes as u64 {
            return Err(OutboxStoreError::InvalidRecord(format!(
                "retained legacy envelope ({bytes} bytes) exceeds this upgrade's {} byte allocation; source evidence was retained; use the explicit offline upgrade for larger legacy envelopes",
                self.legacy_envelope_limit_bytes,
            )));
        }
        Ok(Some((
            row.get(0)
                .map_err(|e| OutboxStoreError::Database(e.to_string()))?,
            row.get(2)
                .map_err(|e| OutboxStoreError::Database(e.to_string()))?,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn offline_budget() -> PhysicalCaptureBudget {
        PhysicalCaptureBudget {
            payload_limit_bytes: 128 * 1024 * 1024,
            physical_limit_bytes: 512 * 1024 * 1024,
            operational_headroom_bytes: MIN_OPERATIONAL_HEADROOM_BYTES,
        }
    }

    #[tokio::test]
    async fn offline_upgrade_preserves_large_snapshot_and_is_idempotent() {
        let directory = std::env::temp_dir().join(format!("aqe-offline-upgrade-{}", uuid::Uuid::new_v4()));
        let path = directory.join("outbox.db");
        let store = OutboxStore::open(&path).await.unwrap();
        let records = (0..4352).map(|n| ReconciliationRecord {
            record_type: "remote_insight".into(), entity_id: format!("insight-{n}"),
            payload: serde_json::json!({"fixture":"x".repeat(4096)}),
        }).collect();
        let snapshot = ReconciliationSnapshot {
            generation: "offline-generation".into(), runtime_id: "runtime".into(),
            strategy_id: "strategy".into(), live_session_id: "session".into(),
            snapshot_complete: true, critical_cutoff: Some(41), records,
        };
        let event = EventEnvelope::new("runtime","strategy","session","legacy",1,
            EventPriority::Reconciliation, EventKind::ReconciliationSnapshot,
            serde_json::to_value(snapshot).unwrap());
        assert!(FlatbufferEventCodec::encode(&event).unwrap().len() > MAX_LEGACY_ENVELOPE_BYTES);
        store.insert_events(&[event.clone()]).await.unwrap();
        let mut rows = store.connection.query("SELECT wire_payload FROM outbox_events WHERE event_id=?1",
            params![event.event_id.to_string()]).await.unwrap();
        let original = rows.next().await.unwrap().unwrap().get::<Vec<u8>>(0).unwrap();
        drop(rows);
        drop(store);
        assert!(OutboxStore::open_with_physical_capture_budget(&path, offline_budget()).await.is_err(),
            "normal startup must not silently increase its envelope allocation");
        assert!(OutboxStore::upgrade_legacy_offline(&path, offline_budget(), MAX_LEGACY_ENVELOPE_BYTES).await.is_err());
        // A finite allocation can refuse midway through committed pages. The
        // original remains pending and a larger explicit allocation resumes.
        let small = PhysicalCaptureBudget { payload_limit_bytes: 24 * 1024 * 1024, ..offline_budget() };
        assert!(matches!(OutboxStore::upgrade_legacy_offline(&path, small, 32 * 1024 * 1024).await,
            Err(OutboxStoreError::CapturePaused(_))));
        let first = OutboxStore::upgrade_legacy_offline(&path, offline_budget(), 32 * 1024 * 1024).await.unwrap();
        assert_eq!(first.dead_letters, 0);
        assert!(first.pending_reconciliation > 256);
        let second = OutboxStore::upgrade_legacy_offline(&path, offline_budget(), 32 * 1024 * 1024).await.unwrap();
        assert_eq!(second.pending_reconciliation, first.pending_reconciliation);
        let store = OutboxStore::open_with_physical_capture_budget(&path, offline_budget()).await.unwrap();
        let mut rows = store.connection.query("SELECT wire_payload,state FROM outbox_events WHERE event_id=?1",
            params![event.event_id.to_string()]).await.unwrap();
        let row = rows.next().await.unwrap().unwrap();
        assert!(row.get::<Vec<u8>>(0).unwrap() == original, "accepted source bytes must remain exact");
        assert_eq!(row.get::<String>(1).unwrap(), "committed");
        drop(rows);
        let mut rows = store.connection.query("SELECT wire_payload FROM outbox_events WHERE state='pending' ORDER BY entity_id", ()).await.unwrap();
        let mut recovered = std::collections::HashSet::new();
        while let Some(row) = rows.next().await.unwrap() {
            let wire = row.get::<Vec<u8>>(0).unwrap();
            assert!(wire.len() <= LOCAL_EVENT_HARD_LIMIT_BYTES);
            let converted = FlatbufferEventCodec::decode(&wire).unwrap();
            if converted.kind == EventKind::ReconciliationChunk {
                let chunk: ReconciliationChunk = serde_json::from_value(converted.payload).unwrap();
                for record in chunk.records { assert!(recovered.insert(record.entity_id)); }
            }
        }
        assert_eq!(recovered.len(), 4352);
        drop(rows); drop(store);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn offline_upgrade_refuses_owned_sources_unrelated_files_and_unbounded_allocations() {
        let directory = std::env::temp_dir().join(format!("aqe-offline-refusal-{}", uuid::Uuid::new_v4()));
        let path = directory.join("outbox.db");
        let store = OutboxStore::open(&path).await.unwrap(); drop(store);
        let lease = super::super::super::CaptureOwnerLease::acquire_for_import(&path).unwrap();
        assert!(OutboxStore::upgrade_legacy_offline(&path, offline_budget(), 16*1024*1024).await.is_err());
        drop(lease);
        assert!(OutboxStore::upgrade_legacy_offline(&path, offline_budget(), 65*1024*1024).await.is_err());
        assert!(OutboxStore::upgrade_legacy_offline(directory.join("missing.db"), offline_budget(), 16*1024*1024).await.is_err());
        assert!(!directory.join("missing.db").exists());
        let unrelated = directory.join("unrelated.db");
        let database = Builder::new_local(unrelated.to_string_lossy().as_ref()).build().await.unwrap();
        let connection = database.connect().unwrap();
        connection.execute("CREATE TABLE essential_execution(id TEXT PRIMARY KEY)", ()).await.unwrap();
        drop(connection); drop(database);
        assert!(OutboxStore::upgrade_legacy_offline(&unrelated, offline_budget(), 16*1024*1024).await.is_err());
        let database = Builder::new_local(unrelated.to_string_lossy().as_ref()).build().await.unwrap();
        let connection = database.connect().unwrap();
        let mut rows = connection.query("SELECT name FROM sqlite_master WHERE name='outbox_events'", ()).await.unwrap();
        assert!(rows.next().await.unwrap().is_none());
        drop(rows); drop(connection); drop(database);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn runtime_backfill_commits_bounded_pages_and_resumes_after_a_bad_envelope() {
        let directory =
            std::env::temp_dir().join(format!("aqe-legacy-pages-{}", uuid::Uuid::new_v4()));
        let store = OutboxStore::open(directory.join("outbox.db"))
            .await
            .unwrap();
        let events = (0..128)
            .map(|index| {
                EventEnvelope::new(
                    "runtime",
                    "strategy",
                    "session",
                    format!("row-{index}"),
                    1,
                    EventPriority::Critical,
                    EventKind::StrategyLifecycle,
                    serde_json::json!({"state":"running"}),
                )
            })
            .collect::<Vec<_>>();
        store.insert_events(&events).await.unwrap();
        let mut rows=store.connection.query(
            "SELECT event_id,wire_payload FROM outbox_events ORDER BY event_id LIMIT 1 OFFSET 64",(),
        ).await.unwrap();
        let row = rows.next().await.unwrap().unwrap();
        let id = row.get::<String>(0).unwrap();
        let saved = row.get::<Vec<u8>>(1).unwrap();
        drop(rows);
        store
            .connection
            .execute("UPDATE outbox_events SET runtime_id=NULL", ())
            .await
            .unwrap();
        store
            .connection
            .execute(
                "UPDATE outbox_events SET wire_payload=?2 WHERE event_id=?1",
                params![id.clone(), vec![0u8]],
            )
            .await
            .unwrap();
        assert!(store.backfill_legacy_runtime_scope().await.is_err());
        let mut rows = store
            .connection
            .query(
                "SELECT count(*) FROM outbox_events WHERE runtime_id='runtime'",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            64
        );
        drop(rows);
        store
            .connection
            .execute(
                "UPDATE outbox_events SET wire_payload=?2 WHERE event_id=?1",
                params![id, saved],
            )
            .await
            .unwrap();
        store.backfill_legacy_runtime_scope().await.unwrap();
        let mut rows = store
            .connection
            .query(
                "SELECT count(*) FROM outbox_events WHERE runtime_id='runtime'",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            128
        );
        drop(rows);
        store.backfill_legacy_runtime_scope().await.unwrap();
        assert_eq!(store.pending_count().await.unwrap(), 128);
        drop(store);
        std::fs::remove_dir_all(directory).unwrap();
    }
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[tokio::test]
    #[ignore = "owned fresh subprocess invoked by the legacy memory benchmark"]
    async fn legacy_open_memory_child() {
        let path = PathBuf::from(std::env::var_os("AQE_LEGACY_MEMORY_PATH").unwrap());
        assert!(path.starts_with(std::env::temp_dir()));
        assert!(
            path.parent()
                .unwrap()
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("aqe-legacy-memory-")
        );
        let started = Instant::now();
        let store = OutboxStore::open_with_physical_capture_budget(
            &path,
            PhysicalCaptureBudget {
                payload_limit_bytes: 128 * 1024 * 1024,
                physical_limit_bytes: 512 * 1024 * 1024,
                operational_headroom_bytes: MIN_OPERATIONAL_HEADROOM_BYTES,
            },
        )
        .await
        .unwrap();
        let mut rows = store
            .connection
            .query(
                "SELECT count(*) FROM outbox_events WHERE runtime_id='runtime'",
                (),
            )
            .await
            .unwrap();
        let count = rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap();
        drop(rows);
        // getrusage writes exactly one initialized rusage to a valid pointer.
        let peak = unsafe {
            let mut usage: libc::rusage = std::mem::zeroed();
            assert_eq!(libc::getrusage(libc::RUSAGE_SELF, &mut usage), 0);
            usage.ru_maxrss as u64
        };
        let peak = if cfg!(target_os = "macos") {
            peak
        } else {
            peak * 1024
        };
        println!(
            "LEGACY_OPEN_MEMORY {}",
            serde_json::json!({"rows":count,"peak_resident_bytes":peak,"elapsed_us":started.elapsed().as_micros()})
        );
    }

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[tokio::test]
    #[ignore = "owned 64 MiB history/fresh-process memory qualification"]
    async fn legacy_open_memory_does_not_scale_with_history() {
        let mut reports = Vec::new();
        for count in [256, 8192] {
            let directory =
                std::env::temp_dir().join(format!("aqe-legacy-memory-{}", uuid::Uuid::new_v4()));
            let path = directory.join("outbox.db");
            let store = OutboxStore::open(&path).await.unwrap();
            for start in (0..count).step_by(64) {
                let events = (start..(start + 64).min(count))
                    .map(|n| {
                        EventEnvelope::new(
                            "runtime",
                            "strategy",
                            "session",
                            format!("row-{n}"),
                            1,
                            EventPriority::Critical,
                            EventKind::StrategyLifecycle,
                            serde_json::json!({"state":"running","fixture":"x".repeat(8192)}),
                        )
                    })
                    .collect::<Vec<_>>();
                store.insert_events(&events).await.unwrap();
            }
            store
                .connection
                .execute("UPDATE outbox_events SET runtime_id=NULL", ())
                .await
                .unwrap();
            store.checkpoint_wal(true).await.unwrap();
            drop(store);
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .env_clear()
                .envs(std::env::vars_os().filter(|(k, _)| {
                    let k = k.to_string_lossy();
                    !k.starts_with("AQE_") && !k.starts_with("AQS_")
                }))
                .env("AQE_LEGACY_MEMORY_PATH", &path)
                .env("RUST_MIN_STACK", "33554432")
                .args([
                    "--exact",
                    "core::live_sync::outbox::legacy::tests::legacy_open_memory_child",
                    "--ignored",
                    "--nocapture",
                ])
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let stdout = String::from_utf8_lossy(&output.stdout);
            let report: serde_json::Value = serde_json::from_str(
                stdout
                    .lines()
                    .find_map(|line| line.strip_prefix("LEGACY_OPEN_MEMORY "))
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(report["rows"], count);
            reports.push(report);
            std::fs::remove_dir_all(directory).unwrap();
        }
        let small = reports[0]["peak_resident_bytes"].as_u64().unwrap();
        let large = reports[1]["peak_resident_bytes"].as_u64().unwrap();
        assert!(
            large <= small + 64 * 1024 * 1024,
            "bounded startup must not materialize the 64 MiB history: {reports:?}"
        );
        println!(
            "LEGACY_MEMORY_REPORT {}",
            serde_json::json!({"source":"owned synthetic lifecycle envelopes","threshold_growth_bytes":64*1024*1024,"reports":reports,"whole_service_memory_gate":false})
        );
    }
    #[tokio::test]
    async fn legacy_reader_is_keyset_bounded_and_oversized_sources_are_preserved() {
        let directory =
            std::env::temp_dir().join(format!("aqe-legacy-reader-{}", uuid::Uuid::new_v4()));
        let path = directory.join("outbox.db");
        let store = OutboxStore::open(&path).await.unwrap();
        for after in [false, true] {
            let mut rows = store
                .connection
                .query(
                    format!(
                        "EXPLAIN QUERY PLAN {}",
                        envelope_query("runtime_id IS NULL OR runtime_id=''", after)
                    ),
                    params!["", MAX_LEGACY_ENVELOPE_BYTES as i64],
                )
                .await
                .unwrap();
            let mut plan = String::new();
            while let Some(row) = rows.next().await.unwrap() {
                plan.push_str(&row.get::<String>(3).unwrap());
            }
            assert!(plan.contains("sqlite_autoindex_outbox_events_1"), "{plan}");
            assert!(
                !plan.contains("TEMP B-TREE"),
                "legacy page must not sort the remaining history: {plan}"
            );
        }
        for index in 0..64 {
            let event = EventEnvelope::new(
                "runtime",
                "strategy",
                "session",
                format!("row-{index}"),
                1,
                EventPriority::Critical,
                EventKind::StrategyLifecycle,
                serde_json::json!({"state":"running"}),
            );
            store.insert_events(&[event]).await.unwrap();
        }
        let mut after = None;
        let mut count = 0;
        while let Some((id, body)) = store
            .next_legacy_envelope("1=1", after.as_deref())
            .await
            .unwrap()
        {
            assert!(body.len() <= MAX_LEGACY_ENVELOPE_BYTES);
            assert!(after.as_ref().is_none_or(|old| old < &id));
            count += 1;
            after = Some(id);
        }
        assert_eq!(count, 64);
        store.connection.execute("UPDATE outbox_events SET runtime_id=NULL,wire_payload=?1 WHERE event_id=(SELECT min(event_id) FROM outbox_events)",
            params![vec![0_u8;MAX_LEGACY_ENVELOPE_BYTES+1]]).await.unwrap();
        assert!(matches!(
            store.next_legacy_envelope("runtime_id IS NULL", None).await,
            Err(OutboxStoreError::InvalidRecord(_))
        ));
        drop(store);
        assert!(matches!(
            OutboxStore::open(&path).await,
            Err(OutboxStoreError::InvalidRecord(_))
        ));
        let db = Builder::new_local(path.to_string_lossy().as_ref())
            .build()
            .await
            .unwrap();
        let conn = db.connect().unwrap();
        let mut rows = conn
            .query(
                "SELECT count(*),max(length(wire_payload)) FROM outbox_events",
                (),
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        assert_eq!(row.get::<i64>(0).unwrap(), 64);
        assert_eq!(
            row.get::<i64>(1).unwrap(),
            MAX_LEGACY_ENVELOPE_BYTES as i64 + 1
        );
        drop(rows);
        drop(conn);
        drop(db);
        let _ = std::fs::remove_dir_all(directory);
    }
}
