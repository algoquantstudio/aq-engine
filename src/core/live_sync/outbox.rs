use super::{
    CloudEvent, EventEnvelope, EventId, EventKind, EventPriority, FlatbufferEventCodec,
    LOCAL_EVENT_HARD_LIMIT_BYTES, RECONCILIATION_TARGET_BYTES, ReconciliationChunk,
    ReconciliationComplete, ReconciliationRecord, ReconciliationSnapshot, ReconciliationStart,
    deterministic_event_id, now_epoch_micros,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use turso::{Builder, Connection, params};

mod publisher_reservations;
mod types;
pub use types::{
    DeadLetterRecord, OutboxEventRecord, OutboxHealth, OutboxStoreError, ReconciliationJobProgress,
};

pub const DEFAULT_OUTBOX_SOFT_LIMIT_BYTES: u64 = 1024 * 1024 * 1024;
const OUTBOX_PRESSURE_NUMERATOR: u64 = 80;
const OUTBOX_PRESSURE_DENOMINATOR: u64 = 100;
const TRACE_RETENTION_US: i64 = 7 * 24 * 60 * 60 * 1_000_000;
const RAW_LATENCY_SAMPLE_RETENTION_US: i64 = 2 * 60 * 1_000_000;
const LATENCY_ROLLUP_RETENTION_US: i64 = 30 * 24 * 60 * 60 * 1_000_000;
const AUXILIARY_RETENTION_US: i64 = 7 * 24 * 60 * 60 * 1_000_000;
const PENDING_SCOPE_COUNT_QUERY: &str =
    "SELECT COUNT(*) FROM outbox_events INDEXED BY idx_outbox_scope_state
     WHERE state = 'pending' AND strategy_id = ?1 AND live_session_id = ?2";
const PENDING_RUNTIME_IDS_QUERY: &str =
    "SELECT DISTINCT runtime_id FROM outbox_events INDEXED BY idx_outbox_scope_state
     WHERE state = 'pending' AND strategy_id = ?1 AND live_session_id = ?2
       AND runtime_id IS NOT NULL AND runtime_id != '' ORDER BY runtime_id";
const CRITICAL_BACKLOG_EXCEEDS_QUERY: &str =
    "SELECT EXISTS(SELECT 1 FROM outbox_events INDEXED BY idx_outbox_runtime_pending
     WHERE runtime_id = ?1 AND strategy_id = ?2 AND live_session_id = ?3
       AND priority = 'critical' AND state IN ('pending', 'in_flight') LIMIT 1 OFFSET ?4)";

fn pending_batch_query(runtime: bool, strategy: bool, session: bool) -> String {
    // Only SQL structure is selected here; every identity remains a bound parameter.
    // Optional-parameter OR predicates prevent an exact-scope index seek.
    let index = if runtime && strategy && session {
        "idx_outbox_runtime_ordered"
    } else {
        "idx_outbox_ordered"
    };
    let runtime = if runtime {
        "runtime_id = ?4"
    } else {
        "?4 IS NULL"
    };
    let strategy = if strategy {
        "strategy_id = ?5"
    } else {
        "?5 IS NULL"
    };
    let session = if session {
        "live_session_id = ?6"
    } else {
        "?6 IS NULL"
    };
    format!(
        "SELECT wire_payload, attempt_count, next_attempt_at_us, local_committed_at_us, last_error
         FROM outbox_events INDEXED BY {index}
         WHERE state = 'pending' AND priority = ?1 AND next_attempt_at_us <= ?2
           AND {runtime} AND {strategy} AND {session}
           AND (?1 != 'critical' OR NOT EXISTS (
             SELECT 1 FROM outbox_events AS blocked
             WHERE blocked.state = 'pending' AND blocked.priority = 'critical'
               AND blocked.next_attempt_at_us > ?2
               AND blocked.runtime_id = outbox_events.runtime_id
               AND blocked.strategy_id = outbox_events.strategy_id
               AND blocked.live_session_id = outbox_events.live_session_id
               AND blocked.critical_sequence < outbox_events.critical_sequence
           ))
         ORDER BY CASE WHEN critical_sequence IS NULL THEN 1 ELSE 0 END,
                  critical_sequence ASC, created_at_us ASC, event_id ASC LIMIT ?3"
    )
}

fn pending_deadline_query(priority: EventPriority) -> &'static str {
    if priority == EventPriority::Critical {
        "SELECT next_attempt_at_us FROM outbox_events INDEXED BY idx_outbox_runtime_ordered
         WHERE state = 'pending' AND priority = ?1
           AND runtime_id = ?2 AND strategy_id = ?3 AND live_session_id = ?4
         ORDER BY CASE WHEN critical_sequence IS NULL THEN 1 ELSE 0 END,
                  critical_sequence ASC, created_at_us ASC, event_id ASC LIMIT 1"
    } else {
        "SELECT next_attempt_at_us FROM outbox_events INDEXED BY idx_outbox_runtime_pending
         WHERE state = 'pending' AND priority = ?1
           AND runtime_id = ?2 AND strategy_id = ?3 AND live_session_id = ?4
         ORDER BY next_attempt_at_us LIMIT 1"
    }
}

#[derive(Clone)]
pub struct OutboxStore {
    path: PathBuf,
    connection: Arc<Connection>,
    operation_gate: Arc<tokio::sync::Mutex<()>>,
    priority_notify: Arc<[Arc<tokio::sync::Notify>; 4]>,
    #[cfg(test)]
    query_counts: Arc<[std::sync::atomic::AtomicUsize; 3]>,
    last_maintenance: Arc<parking_lot::Mutex<Option<Instant>>>,
    health_cache: Arc<tokio::sync::Mutex<Option<(Instant, OutboxHealth)>>>,
}

impl OutboxStore {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, OutboxStoreError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        }
        let database = Builder::new_local(path.to_string_lossy().as_ref())
            .build()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let connection = database
            .connect()
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let store = Self {
            path,
            connection: Arc::new(connection),
            operation_gate: Arc::new(tokio::sync::Mutex::new(())),
            priority_notify: Arc::new(std::array::from_fn(
                |_| Arc::new(tokio::sync::Notify::new()),
            )),
            #[cfg(test)]
            query_counts: Arc::new(std::array::from_fn(|_| {
                std::sync::atomic::AtomicUsize::new(0)
            })),
            last_maintenance: Arc::new(parking_lot::Mutex::new(None)),
            health_cache: Arc::new(tokio::sync::Mutex::new(None)),
        };
        store.initialize().await?;
        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn change_notifier(&self) -> Arc<tokio::sync::Notify> {
        self.priority_change_notifier(EventPriority::Critical)
    }

    pub fn priority_change_notifier(&self, priority: EventPriority) -> Arc<tokio::sync::Notify> {
        let index = match priority {
            EventPriority::Critical => 0,
            EventPriority::Telemetry => 1,
            EventPriority::Audit => 2,
            EventPriority::Reconciliation => 3,
        };
        Arc::clone(&self.priority_notify[index])
    }

    #[cfg(test)]
    pub(super) fn diagnostic_query_counts(&self) -> [usize; 3] {
        self.query_counts
            .each_ref()
            .map(|count| count.load(std::sync::atomic::Ordering::Relaxed))
    }

    /// Clones share one maintenance schedule, not one full-database pass per agent.
    pub fn claim_maintenance(&self, now: Instant, interval: Duration) -> bool {
        let mut last = self.last_maintenance.lock();
        if last.is_some_and(|last| now.saturating_duration_since(last) < interval) {
            return false;
        }
        *last = Some(now);
        true
    }

    /// Only diagnostics use the short cache; durability/retirement checks remain exact.
    pub async fn cached_health(&self, max_age: Duration) -> Result<OutboxHealth, OutboxStoreError> {
        let mut cached = self.health_cache.lock().await;
        if let Some((at, health)) = cached.as_ref().filter(|(at, _)| at.elapsed() < max_age) {
            let mut health = health.clone();
            health.oldest_pending_age_ms = health
                .oldest_pending_age_ms
                .map(|age| age.saturating_add(at.elapsed().as_millis() as u64));
            return Ok(health);
        }
        let health = self.health().await?;
        *cached = Some((Instant::now(), health.clone()));
        Ok(health)
    }

    /// Returns the next retry deadline for one exact runtime/strategy/session scope.
    /// Critical work selects the lowest sequence: a later due event cannot pass an earlier
    /// event in backoff. Independent background work selects the earliest deadline instead.
    pub async fn next_pending_deadline_for_runtime_scope(
        &self,
        priority: EventPriority,
        runtime_id: &str,
        strategy_id: &str,
        live_session_id: &str,
    ) -> Result<Option<i64>, OutboxStoreError> {
        #[cfg(test)]
        self.query_counts[1].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _operation = self.operation_gate.lock().await;
        let mut rows = self
            .connection
            .query(
                pending_deadline_query(priority),
                params![
                    priority_label(priority),
                    runtime_id.to_string(),
                    strategy_id.to_string(),
                    live_session_id.to_string(),
                ],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        rows.next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
            .map(|row| {
                row.get(0)
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))
            })
            .transpose()
    }

    /// Returns locally durable, remotely unconfirmed events for one exact entity. The runtime
    /// host owns the SQLite connection and exposes this through its private loopback event bus;
    /// UI processes must not open the sync-agent database themselves.
    pub async fn pending_events_for_entity(
        &self,
        strategy_id: &str,
        live_session_id: &str,
        entity_id: &str,
        event_kind: EventKind,
        limit: usize,
    ) -> Result<Vec<CloudEvent>, OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let mut rows = self
            .connection
            .query(
                "SELECT wire_payload FROM outbox_events
                 WHERE strategy_id = ?1 AND live_session_id = ?2 AND entity_id = ?3
                   AND event_kind = ?4 AND state IN ('pending', 'in_flight')
                 ORDER BY entity_revision DESC, created_at_us DESC
                 LIMIT ?5",
                params![
                    strategy_id.to_string(),
                    live_session_id.to_string(),
                    entity_id.to_string(),
                    event_kind.to_string(),
                    limit.clamp(1, 100) as i64,
                ],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let mut events = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
        {
            let payload = row
                .get::<Vec<u8>>(0)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            events.push(FlatbufferEventCodec::decode(&payload)?);
        }
        Ok(events)
    }

    async fn initialize(&self) -> Result<(), OutboxStoreError> {
        // `PRAGMA journal_mode` returns a row. Turso deliberately rejects using an
        // execution-only API for row-producing statements, so consume it explicitly.
        let mut journal_mode = self
            .connection
            .query("PRAGMA journal_mode = WAL", ())
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        while journal_mode
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
            .is_some()
        {}
        self.connection
            .execute_batch(
                r#"
                PRAGMA synchronous = NORMAL;
                PRAGMA busy_timeout = 5000;
                PRAGMA cache_size = -8192;
                PRAGMA wal_autocheckpoint = 256;
                PRAGMA journal_size_limit = 8388608;

                CREATE TABLE IF NOT EXISTS outbox_events (
                    event_id TEXT PRIMARY KEY,
                    runtime_id TEXT,
                    priority TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    live_session_id TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    entity_revision INTEGER NOT NULL,
                    critical_sequence INTEGER,
                    event_kind TEXT NOT NULL,
                    wire_payload BLOB NOT NULL,
                    state TEXT NOT NULL DEFAULT 'pending',
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at_us INTEGER NOT NULL,
                    created_at_us INTEGER NOT NULL,
                    local_committed_at_us INTEGER NOT NULL,
                    remote_committed_at_us INTEGER,
                    last_error TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_outbox_pending
                    ON outbox_events(state, priority, next_attempt_at_us, critical_sequence, created_at_us);
                CREATE INDEX IF NOT EXISTS idx_outbox_entity_revision
                    ON outbox_events(strategy_id, entity_id, entity_revision);

                CREATE TABLE IF NOT EXISTS outbox_batches (
                    batch_id TEXT PRIMARY KEY,
                    strategy_id TEXT NOT NULL,
                    first_critical_sequence INTEGER,
                    last_critical_sequence INTEGER,
                    event_count INTEGER NOT NULL,
                    byte_count INTEGER NOT NULL,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    created_at_us INTEGER NOT NULL,
                    committed_at_us INTEGER,
                    last_error TEXT
                );

                CREATE TABLE IF NOT EXISTS sync_cursors (
                    runtime_id TEXT NOT NULL DEFAULT '',
                    strategy_id TEXT NOT NULL,
                    live_session_id TEXT NOT NULL,
                    critical_through INTEGER NOT NULL DEFAULT 0,
                    updated_at_us INTEGER NOT NULL,
                    PRIMARY KEY(runtime_id, strategy_id, live_session_id)
                );

                CREATE TABLE IF NOT EXISTS publisher_sequence_cursors (
                    runtime_id TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    live_session_id TEXT NOT NULL,
                    critical_through INTEGER NOT NULL DEFAULT 0,
                    updated_at_us INTEGER NOT NULL,
                    PRIMARY KEY(runtime_id, strategy_id, live_session_id)
                );

                CREATE TABLE IF NOT EXISTS publisher_event_reservations (
                    event_id TEXT PRIMARY KEY,
                    runtime_id TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    live_session_id TEXT NOT NULL,
                    critical_sequence INTEGER NOT NULL,
                    fingerprint BLOB NOT NULL,
                    UNIQUE(runtime_id, strategy_id, live_session_id, critical_sequence)
                );

                CREATE TABLE IF NOT EXISTS dead_letters (
                    event_id TEXT PRIMARY KEY,
                    strategy_id TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    wire_payload BLOB NOT NULL,
                    failed_at_us INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS reconciliation_jobs (
                    generation TEXT PRIMARY KEY,
                    strategy_id TEXT NOT NULL,
                    live_session_id TEXT NOT NULL DEFAULT '',
                    run_id TEXT,
                    status TEXT NOT NULL,
                    uploaded_records INTEGER NOT NULL DEFAULT 0,
                    processed_records INTEGER NOT NULL DEFAULT 0,
                    candidate_count INTEGER NOT NULL DEFAULT 0,
                    classified_count INTEGER NOT NULL DEFAULT 0,
                    unresolved_count INTEGER NOT NULL DEFAULT 0,
                    current_cursor TEXT,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    snapshot_complete INTEGER NOT NULL DEFAULT 0,
                    critical_cutoff INTEGER,
                    updated_at_us INTEGER NOT NULL,
                    last_error TEXT
                );

                CREATE TABLE IF NOT EXISTS sync_health (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at_us INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS processed_actions (
                    action_id TEXT PRIMARY KEY,
                    strategy_id TEXT NOT NULL,
                    live_session_id TEXT NOT NULL,
                    accepted_at_us INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_processed_actions_scope
                    ON processed_actions(strategy_id, live_session_id, accepted_at_us);

                CREATE TABLE IF NOT EXISTS latency_rollups (
                    bucket_start_us INTEGER NOT NULL,
                    stage TEXT NOT NULL,
                    sample_count INTEGER NOT NULL,
                    p50_us INTEGER,
                    p95_us INTEGER,
                    p99_us INTEGER,
                    max_us INTEGER,
                    PRIMARY KEY(bucket_start_us, stage)
                );

                CREATE TABLE IF NOT EXISTS latency_traces (
                    event_id TEXT PRIMARY KEY,
                    strategy_id TEXT NOT NULL,
                    live_session_id TEXT NOT NULL,
                    event_kind TEXT NOT NULL,
                    priority TEXT NOT NULL,
                    trace_json TEXT NOT NULL,
                    final_latency_us INTEGER,
                    retained_at_us INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_latency_traces_retention
                    ON latency_traces(retained_at_us);

                CREATE TABLE IF NOT EXISTS latency_samples (
                    event_id TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    duration_us INTEGER NOT NULL,
                    observed_at_us INTEGER NOT NULL,
                    PRIMARY KEY(event_id, stage)
                );
                CREATE INDEX IF NOT EXISTS idx_latency_samples_bucket
                    ON latency_samples(observed_at_us, stage);
                "#,
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;

        self.ensure_runtime_scope_column().await?;
        self.ensure_sync_cursor_runtime_scope().await?;
        self.ensure_reconciliation_job_columns().await?;
        self.connection
            .execute_batch(
                r#"
                CREATE INDEX IF NOT EXISTS idx_outbox_runtime_pending
                    ON outbox_events(runtime_id, strategy_id, live_session_id, state, priority,
                                     next_attempt_at_us, critical_sequence, created_at_us);
                CREATE INDEX IF NOT EXISTS idx_outbox_runtime_entity_revision
                    ON outbox_events(runtime_id, strategy_id, live_session_id, entity_id,
                                     entity_revision);
                CREATE INDEX IF NOT EXISTS idx_outbox_runtime_critical_sequence
                    ON outbox_events(runtime_id, strategy_id, live_session_id, critical_sequence);
                CREATE INDEX IF NOT EXISTS idx_outbox_pending_entity_lookup
                    ON outbox_events(strategy_id, live_session_id, entity_id, event_kind, state,
                                     entity_revision);
                CREATE INDEX IF NOT EXISTS idx_outbox_runtime_ordered
                    ON outbox_events(runtime_id, strategy_id, live_session_id, state, priority,
                        CASE WHEN critical_sequence IS NULL THEN 1 ELSE 0 END,
                        critical_sequence, created_at_us, event_id, next_attempt_at_us);
                CREATE INDEX IF NOT EXISTS idx_outbox_ordered
                    ON outbox_events(state, priority,
                        CASE WHEN critical_sequence IS NULL THEN 1 ELSE 0 END,
                        critical_sequence, created_at_us, event_id, next_attempt_at_us);
                CREATE INDEX IF NOT EXISTS idx_outbox_health_pending
                    ON outbox_events(state, priority, local_committed_at_us, next_attempt_at_us);
                CREATE INDEX IF NOT EXISTS idx_outbox_scope_state
                    ON outbox_events(strategy_id, live_session_id, state, runtime_id);
                CREATE INDEX IF NOT EXISTS idx_outbox_error_deadline
                    ON outbox_events(state, next_attempt_at_us) WHERE last_error IS NOT NULL;
                "#,
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        self.discard_terminal_log_events().await?;
        self.requeue_retryable_dead_letters().await?;
        self.reset_pending_contention_backoff().await?;
        self.upgrade_legacy_reconciliation_snapshots().await
    }

    /// Terminal output is a bounded local runtime artifact, not synchronization data. Remove
    /// rows created by older runtime-host versions before they can consume remote batch capacity.
    async fn discard_terminal_log_events(&self) -> Result<(), OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        self.connection
            .execute_batch(
                r#"
                BEGIN IMMEDIATE;
                DELETE FROM latency_samples
                    WHERE event_id IN (SELECT event_id FROM outbox_events WHERE event_kind = 'log');
                DELETE FROM latency_traces
                    WHERE event_id IN (SELECT event_id FROM outbox_events WHERE event_kind = 'log');
                DELETE FROM outbox_events WHERE event_kind = 'log';
                COMMIT;
                "#,
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))
    }

    /// Older sync agents treated SurrealDB transaction contention as an invalid payload. Recover
    /// those records on startup; contention is transient and must never strand lifecycle work in
    /// the DLQ.
    async fn requeue_retryable_dead_letters(&self) -> Result<(), OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let now = now_epoch_micros();
        self.connection
            .execute(
                "UPDATE outbox_events
                 SET state = 'pending', attempt_count = 0, next_attempt_at_us = ?1,
                     last_error = NULL
                 WHERE state = 'dead_letter' AND priority != 'reconciliation' AND event_id IN (
                     SELECT event_id FROM dead_letters
                     WHERE lower(reason) LIKE '%transaction conflict%'
                        OR lower(reason) LIKE '%resource busy%'
                        OR lower(reason) LIKE '%database is locked%'
                        OR lower(reason) LIKE '%this transaction can be retried%'
                 )",
                params![now],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        self.connection
            .execute(
                "DELETE FROM dead_letters WHERE event_id IN (
                    SELECT event_id FROM outbox_events
                    WHERE state = 'pending' AND priority != 'reconciliation'
                 ) AND (
                    lower(reason) LIKE '%transaction conflict%'
                    OR lower(reason) LIKE '%resource busy%'
                    OR lower(reason) LIKE '%database is locked%'
                    OR lower(reason) LIKE '%this transaction can be retried%'
                 )",
                (),
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        Ok(())
    }

    /// A process restart is an immediate opportunity to retry work that was delayed only by
    /// transient database writer contention. Keeping the old exponential-backoff deadline can
    /// otherwise leave the first critical sequence blocked for up to five minutes even though
    /// the competing writer no longer exists. Reconciliation has its own resumable retry state
    /// and is deliberately excluded here.
    async fn reset_pending_contention_backoff(&self) -> Result<(), OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let now = now_epoch_micros();
        self.connection
            .execute(
                "UPDATE outbox_events
                 SET attempt_count = 0, next_attempt_at_us = ?1, last_error = NULL
                 WHERE state = 'pending' AND priority != 'reconciliation'
                   AND last_error IS NOT NULL
                   AND (
                       lower(last_error) LIKE '%transaction conflict%'
                       OR lower(last_error) LIKE '%resource busy%'
                       OR lower(last_error) LIKE '%database is locked%'
                       OR lower(last_error) LIKE '%this transaction can be retried%'
                   )",
                params![now],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        Ok(())
    }

    /// Converts pending protocol-v1 monolithic reconciliation snapshots into bounded,
    /// deterministic protocol-v2 messages. The original row is marked committed only after all
    /// replacement rows are durable, so a process crash cannot lose reconciliation work.
    async fn upgrade_legacy_reconciliation_snapshots(&self) -> Result<(), OutboxStoreError> {
        let mut rows = self
            .connection
            .query(
                "SELECT event_id, wire_payload FROM outbox_events
                 WHERE state = 'pending' AND event_kind = 'reconciliation_snapshot'",
                (),
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let mut legacy = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
        {
            let event_id: String = row
                .get(0)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            let payload: Vec<u8> = row
                .get(1)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            legacy.push((event_id, payload));
        }
        drop(rows);

        for (stored_event_id, payload) in legacy {
            let event = match FlatbufferEventCodec::decode(&payload) {
                Ok(event) => event,
                Err(error) => {
                    if let Ok(event_id) = EventId::parse_str(&stored_event_id) {
                        self.move_to_dead_letter(
                            event_id,
                            &format!("invalid legacy reconciliation envelope: {error}"),
                        )
                        .await?;
                    }
                    continue;
                }
            };
            let snapshot =
                match serde_json::from_value::<ReconciliationSnapshot>(event.payload.clone()) {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        self.move_to_dead_letter(
                            event.event_id,
                            &format!("invalid legacy reconciliation snapshot: {error}"),
                        )
                        .await?;
                        continue;
                    }
                };
            let chunks = bounded_reconciliation_chunks(snapshot.records.clone());
            let total_chunks = chunks.len() as u32;
            let total_records = snapshot.records.len() as u64;
            let mut replacements = Vec::with_capacity(chunks.len() + 2);
            let start = ReconciliationStart {
                generation: snapshot.generation.clone(),
                runtime_id: snapshot.runtime_id.clone(),
                strategy_id: snapshot.strategy_id.clone(),
                live_session_id: snapshot.live_session_id.clone(),
                run_id: event.run_id.clone(),
                snapshot_complete: snapshot.snapshot_complete,
                broker_features: Vec::new(),
                critical_cutoff: snapshot.critical_cutoff,
                total_chunks,
                total_records,
            };
            replacements.push(upgraded_reconciliation_event(
                &event,
                "start",
                EventKind::ReconciliationStart,
                serde_json::to_value(start).unwrap_or_default(),
            ));
            for (index, records) in chunks.into_iter().enumerate() {
                replacements.push(upgraded_reconciliation_event(
                    &event,
                    &format!("chunk:{index}"),
                    EventKind::ReconciliationChunk,
                    serde_json::to_value(ReconciliationChunk {
                        generation: snapshot.generation.clone(),
                        chunk_index: index as u32,
                        records,
                    })
                    .unwrap_or_default(),
                ));
            }
            replacements.push(upgraded_reconciliation_event(
                &event,
                "complete",
                EventKind::ReconciliationComplete,
                serde_json::to_value(ReconciliationComplete {
                    generation: snapshot.generation,
                    total_chunks,
                    total_records,
                    snapshot_complete: snapshot.snapshot_complete,
                })
                .unwrap_or_default(),
            ));

            let oversized = replacements.iter().find_map(|replacement| {
                FlatbufferEventCodec::encode(replacement)
                    .ok()
                    .filter(|bytes| bytes.len() > LOCAL_EVENT_HARD_LIMIT_BYTES)
                    .map(|bytes| (replacement.kind.clone(), bytes.len()))
            });
            if let Some((kind, bytes)) = oversized {
                self.move_to_dead_letter(
                    event.event_id,
                    &format!(
                        "legacy reconciliation contains oversized singleton: type={kind} bytes={bytes} max={LOCAL_EVENT_HARD_LIMIT_BYTES}"
                    ),
                )
                .await?;
                continue;
            }
            self.insert_events(&replacements).await?;
            self.connection
                .execute(
                    "UPDATE outbox_events
                     SET state = 'committed', remote_committed_at_us = ?2,
                         last_error = 'upgraded to bounded protocol-v2 reconciliation'
                     WHERE event_id = ?1 AND state = 'pending'",
                    params![event.event_id.to_string(), now_epoch_micros()],
                )
                .await
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        }
        Ok(())
    }

    /// Adds and backfills the runtime scope for outboxes created before runtime generations
    /// became part of the SQLite projection. The immutable wire envelope is authoritative, so
    /// this migration preserves every pending event instead of guessing from host metadata.
    async fn ensure_runtime_scope_column(&self) -> Result<(), OutboxStoreError> {
        let mut columns = self
            .connection
            .query("PRAGMA table_info(outbox_events)", ())
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let mut has_runtime_id = false;
        while let Some(row) = columns
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
        {
            let name: String = row
                .get(1)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            has_runtime_id |= name == "runtime_id";
        }
        drop(columns);
        if !has_runtime_id {
            self.connection
                .execute("ALTER TABLE outbox_events ADD COLUMN runtime_id TEXT", ())
                .await
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        }

        let mut rows = self
            .connection
            .query(
                "SELECT event_id, wire_payload FROM outbox_events
                 WHERE runtime_id IS NULL OR runtime_id = ''",
                (),
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let mut backfill = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
        {
            let event_id: String = row
                .get(0)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            let payload: Vec<u8> = row
                .get(1)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            let event = FlatbufferEventCodec::decode(&payload)?;
            backfill.push((event_id, event.runtime_id));
        }
        drop(rows);
        for (event_id, runtime_id) in backfill {
            self.connection
                .execute(
                    "UPDATE outbox_events SET runtime_id = ?2 WHERE event_id = ?1",
                    params![event_id, runtime_id],
                )
                .await
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        }
        Ok(())
    }

    /// Migrates the remote persistence cursor from strategy/session scope to the exact runtime
    /// generation used by the SurrealDB endpoint. A new process must never inherit or overwrite
    /// another runtime's cursor merely because it reuses the same live session.
    async fn ensure_sync_cursor_runtime_scope(&self) -> Result<(), OutboxStoreError> {
        let mut columns = self
            .connection
            .query("PRAGMA table_info(sync_cursors)", ())
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let mut has_runtime_id = false;
        while let Some(row) = columns
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
        {
            let name: String = row
                .get(1)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            has_runtime_id |= name == "runtime_id";
        }
        drop(columns);
        if has_runtime_id {
            return Ok(());
        }

        self.connection
            .execute_batch(
                r#"
                BEGIN IMMEDIATE;
                ALTER TABLE sync_cursors RENAME TO sync_cursors_legacy;
                CREATE TABLE sync_cursors (
                    runtime_id TEXT NOT NULL DEFAULT '',
                    strategy_id TEXT NOT NULL,
                    live_session_id TEXT NOT NULL,
                    critical_through INTEGER NOT NULL DEFAULT 0,
                    updated_at_us INTEGER NOT NULL,
                    PRIMARY KEY(runtime_id, strategy_id, live_session_id)
                );
                INSERT INTO sync_cursors(
                    runtime_id, strategy_id, live_session_id, critical_through, updated_at_us
                )
                SELECT COALESCE((
                           SELECT runtime_id FROM outbox_events
                           WHERE outbox_events.strategy_id = legacy.strategy_id
                             AND outbox_events.live_session_id = legacy.live_session_id
                             AND runtime_id IS NOT NULL AND runtime_id != ''
                           ORDER BY created_at_us DESC LIMIT 1
                       ), ''),
                       strategy_id, live_session_id, critical_through, updated_at_us
                FROM sync_cursors_legacy AS legacy;
                DROP TABLE sync_cursors_legacy;
                COMMIT;
                "#,
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))
    }

    async fn ensure_reconciliation_job_columns(&self) -> Result<(), OutboxStoreError> {
        let mut rows = self
            .connection
            .query("PRAGMA table_info(reconciliation_jobs)", ())
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let mut columns = std::collections::HashSet::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
        {
            let name: String = row
                .get(1)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            columns.insert(name);
        }
        drop(rows);
        let additions = [
            ("live_session_id", "TEXT NOT NULL DEFAULT ''"),
            ("run_id", "TEXT"),
            ("candidate_count", "INTEGER NOT NULL DEFAULT 0"),
            ("classified_count", "INTEGER NOT NULL DEFAULT 0"),
            ("unresolved_count", "INTEGER NOT NULL DEFAULT 0"),
            ("current_cursor", "TEXT"),
            ("retry_count", "INTEGER NOT NULL DEFAULT 0"),
        ];
        for (name, definition) in additions {
            if !columns.contains(name) {
                self.connection
                    .execute(
                        format!("ALTER TABLE reconciliation_jobs ADD COLUMN {name} {definition}"),
                        (),
                    )
                    .await
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            }
        }
        Ok(())
    }

    /// Starts or resumes one deterministic run-scoped reconciliation generation. Pending work
    /// from older runs is acknowledged as superseded locally so it can never race the current
    /// broker snapshot. Retryable reconciliation DLQ rows are restored only for this generation.
    pub async fn begin_reconciliation_job(
        &self,
        start: &ReconciliationStart,
    ) -> Result<(), OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let mut rows = self
            .connection
            .query(
                "SELECT event_id, wire_payload FROM outbox_events
                 WHERE state = 'pending' AND priority = 'reconciliation'
                   AND strategy_id = ?1 AND live_session_id = ?2",
                params![start.strategy_id.clone(), start.live_session_id.clone()],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let mut superseded_event_ids = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
        {
            let event_id: String = row
                .get(0)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            let payload: Vec<u8> = row
                .get(1)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            if FlatbufferEventCodec::decode(&payload)
                .ok()
                .and_then(|event| {
                    event
                        .payload
                        .get("generation")
                        .and_then(serde_json::Value::as_str)
                        .map(ToString::to_string)
                })
                .is_some_and(|generation| generation != start.generation)
            {
                superseded_event_ids.push(event_id);
            }
        }
        drop(rows);

        let mut dead_letters = self
            .connection
            .query(
                "SELECT event_id, reason, wire_payload FROM dead_letters
                 WHERE strategy_id = ?1",
                params![start.strategy_id.clone()],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let mut retry_event_ids = Vec::new();
        while let Some(row) = dead_letters
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
        {
            let event_id: String = row
                .get(0)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            let reason: String = row
                .get(1)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            let payload: Vec<u8> = row
                .get(2)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            let retryable = {
                let reason = reason.to_ascii_lowercase();
                reason.contains("transaction conflict")
                    || reason.contains("resource busy")
                    || reason.contains("database is locked")
                    || reason.contains("this transaction can be retried")
            };
            let current_generation = FlatbufferEventCodec::decode(&payload)
                .ok()
                .filter(|event| event.live_session_id == start.live_session_id)
                .and_then(|event| {
                    event
                        .payload
                        .get("generation")
                        .and_then(serde_json::Value::as_str)
                        .map(ToString::to_string)
                })
                .is_some_and(|generation| generation == start.generation);
            if retryable && current_generation {
                retry_event_ids.push(event_id);
            }
        }
        drop(dead_letters);

        self.connection
            .execute_batch("BEGIN IMMEDIATE")
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let result = async {
            for event_id in superseded_event_ids {
                self.connection
                    .execute(
                        "UPDATE outbox_events SET state = 'committed', remote_committed_at_us = ?2,
                         last_error = 'superseded by a newer reconciliation run'
                         WHERE event_id = ?1 AND state = 'pending'",
                        params![event_id, now_epoch_micros()],
                    )
                    .await
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            }
            self.connection
                .execute(
                    "UPDATE reconciliation_jobs SET status = 'superseded', updated_at_us = ?3
                     WHERE strategy_id = ?1 AND live_session_id = ?2
                       AND generation != ?4 AND status NOT IN ('completed', 'superseded')",
                    params![
                        start.strategy_id.clone(),
                        start.live_session_id.clone(),
                        now_epoch_micros(),
                        start.generation.clone(),
                    ],
                )
                .await
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            self.connection
                .execute(
                    "INSERT INTO reconciliation_jobs(
                        generation, strategy_id, live_session_id, run_id, status,
                        uploaded_records, processed_records, candidate_count, classified_count,
                        unresolved_count, retry_count, snapshot_complete, critical_cutoff,
                        updated_at_us, last_error
                     ) VALUES (?1, ?2, ?3, ?4, 'queued', 0, 0, 0, 0, 0, 0, ?5, ?6, ?7, NULL)
                     ON CONFLICT(generation) DO UPDATE SET
                        run_id = excluded.run_id,
                        snapshot_complete = excluded.snapshot_complete,
                        critical_cutoff = excluded.critical_cutoff,
                        updated_at_us = excluded.updated_at_us",
                    params![
                        start.generation.clone(),
                        start.strategy_id.clone(),
                        start.live_session_id.clone(),
                        start.run_id.clone(),
                        i64::from(start.snapshot_complete),
                        start.critical_cutoff.map(|value| value as i64),
                        now_epoch_micros(),
                    ],
                )
                .await
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            for event_id in retry_event_ids {
                self.connection
                    .execute(
                        "UPDATE outbox_events SET state = 'pending', attempt_count = 0,
                         next_attempt_at_us = ?2, last_error = NULL WHERE event_id = ?1",
                        params![event_id.clone(), now_epoch_micros()],
                    )
                    .await
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
                self.connection
                    .execute(
                        "DELETE FROM dead_letters WHERE event_id = ?1",
                        params![event_id],
                    )
                    .await
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            }
            Ok(())
        }
        .await;
        match result {
            Ok(()) => self
                .connection
                .execute_batch("COMMIT")
                .await
                .map_err(|error| OutboxStoreError::Database(error.to_string())),
            Err(error) => {
                let _ = self.connection.execute_batch("ROLLBACK").await;
                Err(error)
            }
        }
    }

    pub async fn update_reconciliation_progress(
        &self,
        progress: &ReconciliationJobProgress,
    ) -> Result<(), OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        self.connection
            .execute(
                "UPDATE reconciliation_jobs SET status = ?2, candidate_count = ?3,
                 classified_count = ?4, processed_records = ?4, unresolved_count = ?5,
                 current_cursor = ?6, retry_count = MAX(retry_count, ?7), last_error = ?8,
                 updated_at_us = ?9 WHERE generation = ?1",
                params![
                    progress.generation.clone(),
                    progress.status.clone(),
                    progress.candidate_count as i64,
                    progress.classified_count as i64,
                    progress.unresolved_count as i64,
                    progress.current_cursor.clone(),
                    progress.retry_count as i64,
                    progress.last_error.clone(),
                    now_epoch_micros(),
                ],
            )
            .await
            .map(|_| ())
            .map_err(|error| OutboxStoreError::Database(error.to_string()))
    }

    pub async fn record_reconciliation_retry(
        &self,
        generation: &str,
        error: &str,
    ) -> Result<(), OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        self.connection
            .execute(
                "UPDATE reconciliation_jobs SET retry_count = retry_count + 1,
                 last_error = ?2, status = 'retrying', updated_at_us = ?3
                 WHERE generation = ?1",
                params![
                    generation.to_string(),
                    error.to_string(),
                    now_epoch_micros()
                ],
            )
            .await
            .map(|_| ())
            .map_err(|error| OutboxStoreError::Database(error.to_string()))
    }

    pub async fn insert_events(&self, events: &[CloudEvent]) -> Result<usize, OutboxStoreError> {
        let events = if events.iter().any(|event| event.kind == EventKind::Log) {
            std::borrow::Cow::Owned(
                events
                    .iter()
                    .filter(|event| event.kind != EventKind::Log)
                    .cloned()
                    .collect::<Vec<_>>(),
            )
        } else {
            std::borrow::Cow::Borrowed(events)
        };
        if events.is_empty() {
            return Ok(0);
        }
        let _operation = self.operation_gate.lock().await;
        self.connection
            .execute_batch("BEGIN IMMEDIATE")
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let result = self.insert_events_in_transaction(&events).await;
        match result {
            Ok(inserted) => {
                self.connection
                    .execute_batch("COMMIT")
                    .await
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
                if inserted != 0 {
                    for priority in [
                        EventPriority::Critical,
                        EventPriority::Telemetry,
                        EventPriority::Audit,
                        EventPriority::Reconciliation,
                    ] {
                        if events.iter().any(|event| event.priority == priority) {
                            // Register-before-read + broadcast preserves every scope's
                            // wakeup; unrelated priorities stay asleep.
                            self.priority_change_notifier(priority).notify_waiters();
                        }
                    }
                }
                Ok(inserted)
            }
            Err(error) => {
                let _ = self.connection.execute_batch("ROLLBACK").await;
                Err(error)
            }
        }
    }

    /// Returns the last sequence reserved or committed for one strategy session.
    ///
    /// The publisher uses this before accepting new critical events so a strategy process
    /// restart continues the same durable sequence instead of starting again at one. Both
    /// pending outbox rows and the compact cursor are considered because committed rows may
    /// already have been pruned by retention.
    pub async fn critical_sequence_cursor(
        &self,
        strategy_id: &str,
        live_session_id: &str,
    ) -> Result<u64, OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let mut rows = self
            .connection
            .query(
                "SELECT MAX(value) FROM (
                    SELECT COALESCE(MAX(critical_sequence), 0) AS value
                    FROM outbox_events
                    WHERE strategy_id = ?1 AND live_session_id = ?2
                    UNION ALL
                    SELECT COALESCE(MAX(critical_through), 0) AS value
                    FROM sync_cursors
                    WHERE strategy_id = ?1 AND live_session_id = ?2
                 )",
                params![strategy_id.to_string(), live_session_id.to_string()],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let value = rows
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
            .and_then(|row| row.get::<Option<i64>>(0).ok().flatten())
            .unwrap_or(0);
        Ok(value.max(0) as u64)
    }

    /// Returns the publisher cursor for one exact runtime generation. Remote sequence cursors
    /// are runtime-scoped, so a replacement runtime must not inherit an obsolete generation's
    /// sequence merely because it reuses the same strategy session and recovery database.
    pub async fn critical_sequence_cursor_for_runtime_scope(
        &self,
        runtime_id: &str,
        strategy_id: &str,
        live_session_id: &str,
    ) -> Result<u64, OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let mut rows = self
            .connection
            .query(
                "SELECT MAX(value) FROM (
                    SELECT COALESCE(MAX(critical_sequence), 0) AS value
                    FROM outbox_events
                    WHERE runtime_id = ?1 AND strategy_id = ?2 AND live_session_id = ?3
                    UNION ALL
                    SELECT COALESCE(MAX(critical_through), 0) AS value
                    FROM publisher_sequence_cursors
                    WHERE runtime_id = ?1 AND strategy_id = ?2 AND live_session_id = ?3
                 )",
                params![
                    runtime_id.to_string(),
                    strategy_id.to_string(),
                    live_session_id.to_string()
                ],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let value = rows
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
            .and_then(|row| row.get::<Option<i64>>(0).ok().flatten())
            .unwrap_or(0);
        Ok(value.max(0) as u64)
    }

    pub async fn remote_critical_cursor_for_runtime_scope(
        &self,
        runtime_id: &str,
        strategy_id: &str,
        live_session_id: &str,
    ) -> Result<u64, OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let mut rows = self
            .connection
            .query(
                "SELECT COALESCE(critical_through, 0) FROM sync_cursors
                 WHERE runtime_id = ?1 AND strategy_id = ?2 AND live_session_id = ?3",
                params![
                    runtime_id.to_string(),
                    strategy_id.to_string(),
                    live_session_id.to_string(),
                ],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let value = rows
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
            .and_then(|row| row.get::<i64>(0).ok())
            .unwrap_or(0);
        Ok(value.max(0) as u64)
    }

    /// Durably reserves the highest critical sequence assigned by a strategy publisher.
    ///
    /// The publisher owns a separate emergency database from the runtime-host sync agent. Even
    /// when NATS accepts every event, this cursor must advance so restarting the strategy cannot
    /// reuse sequence numbers that the host has already accepted.
    pub async fn record_critical_sequence_cursor(
        &self,
        strategy_id: &str,
        live_session_id: &str,
        critical_through: u64,
    ) -> Result<(), OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let now = now_epoch_micros();
        self.connection
            .execute(
                "INSERT INTO sync_cursors(runtime_id, strategy_id, live_session_id, critical_through, updated_at_us)
                 VALUES ('', ?1, ?2, ?3, ?4)
                 ON CONFLICT(runtime_id, strategy_id, live_session_id) DO UPDATE SET
                   critical_through = MAX(sync_cursors.critical_through, excluded.critical_through),
                   updated_at_us = excluded.updated_at_us",
                params![
                    strategy_id.to_string(),
                    live_session_id.to_string(),
                    critical_through as i64,
                    now,
                ],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        Ok(())
    }

    pub async fn record_critical_sequence_cursor_for_runtime_scope(
        &self,
        runtime_id: &str,
        strategy_id: &str,
        live_session_id: &str,
        critical_through: u64,
    ) -> Result<(), OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let now = now_epoch_micros();
        self.connection
            .execute(
                "INSERT INTO publisher_sequence_cursors(
                    runtime_id, strategy_id, live_session_id, critical_through, updated_at_us
                 ) VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(runtime_id, strategy_id, live_session_id) DO UPDATE SET
                   critical_through = MAX(
                       publisher_sequence_cursors.critical_through,
                       excluded.critical_through
                   ),
                   updated_at_us = excluded.updated_at_us",
                params![
                    runtime_id.to_string(),
                    strategy_id.to_string(),
                    live_session_id.to_string(),
                    critical_through as i64,
                    now,
                ],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        Ok(())
    }

    /// Returns whether a runtime-host control action was already accepted by the strategy.
    /// Keeping this cursor in the durable outbox prevents a host restart from replaying a
    /// non-idempotent action after its local acknowledgement was received.
    pub async fn action_was_accepted(
        &self,
        action_id: &str,
        strategy_id: &str,
        live_session_id: &str,
    ) -> Result<bool, OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let mut rows = self
            .connection
            .query(
                "SELECT 1 FROM processed_actions
                 WHERE action_id = ?1 AND strategy_id = ?2 AND live_session_id = ?3
                 LIMIT 1",
                params![
                    action_id.to_string(),
                    strategy_id.to_string(),
                    live_session_id.to_string(),
                ],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        Ok(rows
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
            .is_some())
    }

    pub async fn mark_action_accepted(
        &self,
        action_id: &str,
        strategy_id: &str,
        live_session_id: &str,
    ) -> Result<(), OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        self.connection
            .execute(
                "INSERT OR IGNORE INTO processed_actions(
                    action_id, strategy_id, live_session_id, accepted_at_us
                 ) VALUES (?1, ?2, ?3, ?4)",
                params![
                    action_id.to_string(),
                    strategy_id.to_string(),
                    live_session_id.to_string(),
                    now_epoch_micros(),
                ],
            )
            .await
            .map(|_| ())
            .map_err(|error| OutboxStoreError::Database(error.to_string()))
    }

    async fn insert_events_in_transaction(
        &self,
        events: &[CloudEvent],
    ) -> Result<usize, OutboxStoreError> {
        let mut inserted = 0usize;
        for event in events {
            let committed_at = now_epoch_micros();
            let payload = FlatbufferEventCodec::encode_with_local_commit(event, committed_at)?;
            let affected = self
                .connection
                .execute(
                    "INSERT OR IGNORE INTO outbox_events (
                        event_id, runtime_id, priority, strategy_id, live_session_id, entity_id,
                        entity_revision, critical_sequence, event_kind, wire_payload, state,
                        attempt_count, next_attempt_at_us, created_at_us, local_committed_at_us
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'pending', 0, ?11, ?12, ?11)",
                    params![
                        event.event_id.to_string(),
                        event.runtime_id.clone(),
                        priority_label(event.priority),
                        event.strategy_id.clone(),
                        event.live_session_id.clone(),
                        event.entity_id.clone(),
                        event.entity_revision as i64,
                        event.critical_sequence.map(|value| value as i64),
                        event.kind.to_string(),
                        payload,
                        committed_at,
                        event.created_at.timestamp_micros(),
                    ],
                )
                .await
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            inserted += affected as usize;
            if affected == 0 {
                let mut rows = self
                    .connection
                    .query(
                        "SELECT runtime_id, priority, strategy_id, live_session_id, entity_id,
                                entity_revision, critical_sequence, event_kind
                         FROM outbox_events WHERE event_id = ?1",
                        params![event.event_id.to_string()],
                    )
                    .await
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
                let row = rows
                    .next()
                    .await
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?
                    .ok_or_else(|| {
                        OutboxStoreError::InvalidRecord(format!(
                            "event {} was ignored without an existing outbox row",
                            event.event_id
                        ))
                    })?;
                let existing_runtime: Option<String> = row
                    .get(0)
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
                let existing_priority: String = row
                    .get(1)
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
                let existing_strategy: String = row
                    .get(2)
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
                let existing_session: String = row
                    .get(3)
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
                let existing_entity: String = row
                    .get(4)
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
                let existing_revision: i64 = row
                    .get(5)
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
                let existing_sequence: Option<i64> = row
                    .get(6)
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
                let existing_kind: String = row
                    .get(7)
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
                let matches = existing_runtime.as_deref().unwrap_or_default() == event.runtime_id
                    && existing_priority == priority_label(event.priority)
                    && existing_strategy == event.strategy_id
                    && existing_session == event.live_session_id
                    && existing_entity == event.entity_id
                    && existing_revision == event.entity_revision as i64
                    && existing_sequence == event.critical_sequence.map(|value| value as i64)
                    && existing_kind == event.kind.to_string();
                if !matches {
                    return Err(OutboxStoreError::InvalidRecord(format!(
                        "event {} was reused with a different runtime, sequence, or entity revision",
                        event.event_id
                    )));
                }
            }
        }
        self.set_health_value("last_local_ack_at_us", &now_epoch_micros().to_string())
            .await?;
        Ok(inserted)
    }

    pub async fn insert_telemetry_latest(
        &self,
        event: &CloudEvent,
    ) -> Result<(), OutboxStoreError> {
        if event.kind == EventKind::Log {
            return Ok(());
        }
        let _operation = self.operation_gate.lock().await;
        self.connection
            .execute_batch("BEGIN IMMEDIATE")
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let result = async {
            let mut rows = self
                .connection
                .query(
                    "SELECT MAX(entity_revision) FROM outbox_events
                     WHERE state = 'pending'
                       AND priority = 'telemetry'
                       AND runtime_id = ?1
                       AND strategy_id = ?2
                       AND live_session_id = ?3
                       AND entity_id = ?4",
                    params![
                        event.runtime_id.clone(),
                        event.strategy_id.clone(),
                        event.live_session_id.clone(),
                        event.entity_id.clone(),
                    ],
                )
                .await
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            let newest_pending = rows
                .next()
                .await
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?
                .and_then(|row| row.get::<Option<i64>>(0).ok().flatten())
                .unwrap_or(-1);
            if newest_pending >= event.entity_revision as i64 {
                return Ok(0);
            }
            self.connection
                .execute(
                    "DELETE FROM outbox_events
                     WHERE state = 'pending'
                       AND priority = 'telemetry'
                       AND runtime_id = ?1
                       AND strategy_id = ?2
                       AND live_session_id = ?3
                       AND entity_id = ?4
                       AND entity_revision < ?5",
                    params![
                        event.runtime_id.clone(),
                        event.strategy_id.clone(),
                        event.live_session_id.clone(),
                        event.entity_id.clone(),
                        event.entity_revision as i64,
                    ],
                )
                .await
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            self.insert_events_in_transaction(std::slice::from_ref(event))
                .await
        }
        .await;
        match result {
            Ok(inserted) => {
                self.connection
                    .execute_batch("COMMIT")
                    .await
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
                if inserted != 0 {
                    self.priority_change_notifier(EventPriority::Telemetry)
                        .notify_waiters();
                }
                Ok(())
            }
            Err(error) => {
                let _ = self.connection.execute_batch("ROLLBACK").await;
                Err(error)
            }
        }
    }

    pub async fn pending_batch(
        &self,
        priority: EventPriority,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<Vec<OutboxEventRecord>, OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        self.pending_batch_scoped(priority, None, None, None, max_events, max_bytes)
            .await
    }

    /// Loads a due batch for one remote authorization scope. A SurrealDB live-sync
    /// transaction must never contain records from different strategies or sessions.
    pub async fn pending_batch_for_scope(
        &self,
        priority: EventPriority,
        strategy_id: &str,
        live_session_id: &str,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<Vec<OutboxEventRecord>, OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        self.pending_batch_scoped(
            priority,
            None,
            Some(strategy_id),
            Some(live_session_id),
            max_events,
            max_bytes,
        )
        .await
    }

    /// Loads one due batch for an exact runtime generation. Critical sequence cursors restart
    /// for each runtime, so records from different generations must never share a remote batch.
    pub async fn pending_batch_for_runtime_scope(
        &self,
        priority: EventPriority,
        runtime_id: &str,
        strategy_id: &str,
        live_session_id: &str,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<Vec<OutboxEventRecord>, OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        self.pending_batch_scoped(
            priority,
            Some(runtime_id),
            Some(strategy_id),
            Some(live_session_id),
            max_events,
            max_bytes,
        )
        .await
    }

    async fn pending_batch_scoped(
        &self,
        priority: EventPriority,
        runtime_id: Option<&str>,
        strategy_id: Option<&str>,
        live_session_id: Option<&str>,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<Vec<OutboxEventRecord>, OutboxStoreError> {
        #[cfg(test)]
        self.query_counts[0].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let now = now_epoch_micros();
        let query = pending_batch_query(
            runtime_id.is_some(),
            strategy_id.is_some(),
            live_session_id.is_some(),
        );
        let mut rows = self
            .connection
            .query(
                &query,
                params![
                    priority_label(priority),
                    now,
                    max_events.max(1) as i64,
                    runtime_id.map(str::to_owned),
                    strategy_id.map(str::to_owned),
                    live_session_id.map(str::to_owned),
                ],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let mut batch = Vec::new();
        let mut bytes = 0usize;
        while let Some(row) = rows
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
        {
            let payload = row
                .get::<Vec<u8>>(0)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            if !batch.is_empty() && bytes.saturating_add(payload.len()) > max_bytes {
                break;
            }
            bytes = bytes.saturating_add(payload.len());
            batch.push(OutboxEventRecord {
                event: FlatbufferEventCodec::decode(&payload)?,
                attempt_count: row
                    .get::<i64>(1)
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?
                    .max(0) as u32,
                next_attempt_at_us: row
                    .get(2)
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?,
                local_committed_at_us: row
                    .get(3)
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?,
                last_error: row
                    .get(4)
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?,
            });
        }
        Ok(batch)
    }

    pub async fn mark_remote_committed(
        &self,
        event_ids: &[EventId],
        critical_through: Option<u64>,
    ) -> Result<(), OutboxStoreError> {
        if event_ids.is_empty() {
            return Ok(());
        }
        let _operation = self.operation_gate.lock().await;
        let now = now_epoch_micros();
        self.connection
            .execute_batch("BEGIN IMMEDIATE")
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let result = async {
            for event_id in event_ids {
                self.connection
                    .execute(
                        "UPDATE outbox_events
                         SET state = 'committed', remote_committed_at_us = ?2, last_error = NULL
                         WHERE event_id = ?1",
                        params![event_id.to_string(), now],
                    )
                    .await
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            }
            if let Some(critical_through) = critical_through {
                let first = event_ids.first().expect("checked non-empty");
                let mut rows = self
                    .connection
                    .query(
                        "SELECT runtime_id, strategy_id, live_session_id FROM outbox_events WHERE event_id = ?1",
                        params![first.to_string()],
                    )
                    .await
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
                if let Some(row) = rows
                    .next()
                    .await
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?
                {
                    let runtime_id: Option<String> = row
                        .get(0)
                        .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
                    let strategy_id: String = row
                        .get(1)
                        .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
                    let live_session_id: String = row
                        .get(2)
                        .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
                    self.connection
                        .execute(
                            "INSERT INTO sync_cursors(runtime_id, strategy_id, live_session_id, critical_through, updated_at_us)
                             VALUES (?1, ?2, ?3, ?4, ?5)
                             ON CONFLICT(runtime_id, strategy_id, live_session_id) DO UPDATE SET
                               critical_through = MAX(sync_cursors.critical_through, excluded.critical_through),
                               updated_at_us = excluded.updated_at_us",
                            params![
                                runtime_id.unwrap_or_default(),
                                strategy_id,
                                live_session_id,
                                critical_through as i64,
                                now
                            ],
                        )
                        .await
                        .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
                }
            }
            self.set_health_value("last_remote_commit_at_us", &now.to_string())
                .await
        }
        .await;
        match result {
            Ok(()) => {
                self.connection
                    .execute_batch("COMMIT")
                    .await
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
                Ok(())
            }
            Err(error) => {
                let _ = self.connection.execute_batch("ROLLBACK").await;
                Err(error)
            }
        }
    }

    /// Retains critical lifecycle latency traces, a deterministic one-percent telemetry sample,
    /// and every trace above the slow-event threshold. Instrumentation is deliberately separate
    /// from durability: callers treat an error here as best-effort after the remote commit.
    pub async fn record_persistence_acknowledgements(
        &self,
        events: &[CloudEvent],
        acknowledged_at_us: i64,
    ) -> Result<(), OutboxStoreError> {
        if events.is_empty() {
            return Ok(());
        }
        let _operation = self.operation_gate.lock().await;
        self.connection
            .execute_batch("BEGIN IMMEDIATE")
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let result = async {
            for event in events {
                let mut trace = event.latency.clone();
                trace
                    .persistence_committed_at_us
                    .get_or_insert(acknowledged_at_us);
                trace.persistence_ack_received_at_us = Some(acknowledged_at_us);
                let is_critical = event.priority != EventPriority::Telemetry;
                let deterministic_sample = event.event_id.as_bytes()[0] < 3;
                if !is_critical && !deterministic_sample && !trace.should_retain_trace(false) {
                    continue;
                }

                let trace_json = serde_json::to_string(&trace)
                    .map_err(|error| OutboxStoreError::InvalidRecord(error.to_string()))?;
                self.connection
                    .execute(
                        "INSERT OR REPLACE INTO latency_traces (
                            event_id, strategy_id, live_session_id, event_kind, priority,
                            trace_json, final_latency_us, retained_at_us
                         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                        params![
                            event.event_id.to_string(),
                            event.strategy_id.clone(),
                            event.live_session_id.clone(),
                            event.kind.to_string(),
                            priority_label(event.priority),
                            trace_json,
                            trace.final_latency_us(),
                            acknowledged_at_us,
                        ],
                    )
                    .await
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
                for (stage, duration_us) in latency_stage_durations(&trace) {
                    self.connection
                        .execute(
                            "INSERT OR REPLACE INTO latency_samples (
                                event_id, stage, duration_us, observed_at_us
                             ) VALUES (?1, ?2, ?3, ?4)",
                            params![
                                event.event_id.to_string(),
                                stage,
                                duration_us,
                                acknowledged_at_us,
                            ],
                        )
                        .await
                        .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
                }
            }
            Ok(())
        }
        .await;
        match result {
            Ok(()) => self
                .connection
                .execute_batch("COMMIT")
                .await
                .map_err(|error| OutboxStoreError::Database(error.to_string())),
            Err(error) => {
                let _ = self.connection.execute_batch("ROLLBACK").await;
                Err(error)
            }
        }
    }

    /// Rebuilds the current and previous minute's stage histograms. Rebuilding the two buckets
    /// makes the boundary resilient to timer jitter while deterministic event/stage keys prevent
    /// duplicate samples after replay.
    pub async fn refresh_latency_rollups(&self) -> Result<(), OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        const MINUTE_US: i64 = 60_000_000;
        let now = now_epoch_micros();
        let current_bucket = now - now.rem_euclid(MINUTE_US);
        let from = current_bucket.saturating_sub(MINUTE_US);
        let mut rows = self
            .connection
            .query(
                "SELECT stage, duration_us, observed_at_us FROM latency_samples
                 WHERE observed_at_us >= ?1 ORDER BY observed_at_us ASC",
                params![from],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let mut samples: BTreeMap<(i64, String), Vec<i64>> = BTreeMap::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
        {
            let stage: String = row
                .get(0)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            let duration_us: i64 = row
                .get(1)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            let observed_at_us: i64 = row
                .get(2)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            let bucket = observed_at_us - observed_at_us.rem_euclid(MINUTE_US);
            samples
                .entry((bucket, stage))
                .or_default()
                .push(duration_us);
        }
        // End the read transaction before beginning maintenance writes and before a WAL
        // checkpoint is attempted by the periodic maintenance pass.
        drop(rows);

        self.connection
            .execute_batch("BEGIN IMMEDIATE")
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let result = async {
            for ((bucket, stage), mut values) in samples {
                values.sort_unstable();
                let count = values.len();
                self.connection
                    .execute(
                        "INSERT INTO latency_rollups (
                            bucket_start_us, stage, sample_count, p50_us, p95_us, p99_us, max_us
                         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                         ON CONFLICT(bucket_start_us, stage) DO UPDATE SET
                            sample_count = excluded.sample_count,
                            p50_us = excluded.p50_us,
                            p95_us = excluded.p95_us,
                            p99_us = excluded.p99_us,
                            max_us = excluded.max_us",
                        params![
                            bucket,
                            stage,
                            count as i64,
                            percentile(&values, 50),
                            percentile(&values, 95),
                            percentile(&values, 99),
                            values.last().copied().unwrap_or(0),
                        ],
                    )
                    .await
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            }
            let cutoff = now.saturating_sub(TRACE_RETENTION_US);
            self.connection
                .execute(
                    "DELETE FROM latency_traces WHERE retained_at_us < ?1",
                    params![cutoff],
                )
                .await
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            self.connection
                .execute(
                    "DELETE FROM latency_samples WHERE observed_at_us < ?1",
                    params![now.saturating_sub(RAW_LATENCY_SAMPLE_RETENTION_US)],
                )
                .await
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            self.connection
                .execute(
                    "DELETE FROM latency_rollups WHERE bucket_start_us < ?1",
                    params![now.saturating_sub(LATENCY_ROLLUP_RETENTION_US)],
                )
                .await
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            self.connection
                .execute(
                    "DELETE FROM processed_actions WHERE accepted_at_us < ?1",
                    params![now.saturating_sub(AUXILIARY_RETENTION_US)],
                )
                .await
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            self.connection
                .execute(
                    "DELETE FROM reconciliation_jobs
                     WHERE updated_at_us < ?1 AND status IN ('completed', 'superseded')",
                    params![now.saturating_sub(AUXILIARY_RETENTION_US)],
                )
                .await
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            Ok(())
        }
        .await;
        match result {
            Ok(()) => self
                .connection
                .execute_batch("COMMIT")
                .await
                .map_err(|error| OutboxStoreError::Database(error.to_string())),
            Err(error) => {
                let _ = self.connection.execute_batch("ROLLBACK").await;
                Err(error)
            }
        }
    }

    pub async fn schedule_retry(
        &self,
        event_ids: &[EventId],
        delay_seconds: u64,
        error: &str,
    ) -> Result<(), OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let next_attempt =
            now_epoch_micros().saturating_add((delay_seconds as i64).saturating_mul(1_000_000));
        for event_id in event_ids {
            self.connection
                .execute(
                    "UPDATE outbox_events
                     SET attempt_count = attempt_count + 1,
                         next_attempt_at_us = ?2,
                         last_error = ?3
                     WHERE event_id = ?1 AND state = 'pending'",
                    params![event_id.to_string(), next_attempt, error.to_string()],
                )
                .await
                .map_err(|database_error| OutboxStoreError::Database(database_error.to_string()))?;
        }
        Ok(())
    }

    pub async fn move_to_dead_letter(
        &self,
        event_id: EventId,
        reason: &str,
    ) -> Result<(), OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let now = now_epoch_micros();
        self.connection
            .execute_batch("BEGIN IMMEDIATE")
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let result = async {
            self.connection
                .execute(
                    "INSERT OR REPLACE INTO dead_letters(event_id, strategy_id, reason, wire_payload, failed_at_us)
                     SELECT event_id, strategy_id, ?2, wire_payload, ?3
                     FROM outbox_events WHERE event_id = ?1",
                    params![event_id.to_string(), reason.to_string(), now],
                )
                .await
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            self.connection
                .execute(
                    "UPDATE outbox_events SET state = 'dead_letter', last_error = ?2 WHERE event_id = ?1",
                    params![event_id.to_string(), reason.to_string()],
                )
                .await
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
            Ok(())
        }
        .await;
        match result {
            Ok(()) => self
                .connection
                .execute_batch("COMMIT")
                .await
                .map_err(|error| OutboxStoreError::Database(error.to_string())),
            Err(error) => {
                let _ = self.connection.execute_batch("ROLLBACK").await;
                Err(error)
            }
        }
    }

    pub async fn prune_committed(&self, retention_micros: i64) -> Result<u64, OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let cutoff = now_epoch_micros().saturating_sub(retention_micros.max(0));
        let deleted = self
            .connection
            .execute(
                "DELETE FROM outbox_events
                 WHERE state = 'committed' AND remote_committed_at_us < ?1",
                params![cutoff],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        self.connection
            .execute("PRAGMA shrink_memory", ())
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        Ok(deleted)
    }

    /// Checkpoints WAL pages after retention work. A passive checkpoint never waits for readers;
    /// the configured journal-size limit bounds the reusable WAL after a successful reset.
    pub async fn checkpoint_wal(&self, truncate: bool) -> Result<(), OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let pragma = if truncate {
            "PRAGMA wal_checkpoint(TRUNCATE)"
        } else {
            "PRAGMA wal_checkpoint(PASSIVE)"
        };
        let mut rows = self
            .connection
            .query(pragma, ())
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        while rows
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
            .is_some()
        {}
        Ok(())
    }

    /// Enforces the soft storage policy without ever deleting locally acknowledged,
    /// remotely-uncommitted critical lifecycle events. Under pressure, committed telemetry
    /// and reconciliation rows are discarded first, followed by other committed rows.
    pub async fn enforce_storage_policy(
        &self,
        soft_limit_bytes: u64,
    ) -> Result<u64, OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let database_bytes = self.database_bytes();
        let threshold = soft_limit_bytes.saturating_mul(OUTBOX_PRESSURE_NUMERATOR)
            / OUTBOX_PRESSURE_DENOMINATOR;
        if database_bytes < threshold {
            return Ok(0);
        }

        self.connection
            .execute_batch("BEGIN IMMEDIATE")
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let result = async {
            let mut deleted = 0u64;
            deleted = deleted.saturating_add(
                self.connection
                    .execute(
                        "DELETE FROM outbox_events
                         WHERE state = 'committed'
                           AND priority IN ('telemetry', 'reconciliation')",
                        (),
                    )
                    .await
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?,
            );
            if self.database_bytes() >= soft_limit_bytes {
                deleted = deleted.saturating_add(
                    self.connection
                        .execute("DELETE FROM outbox_events WHERE state = 'committed'", ())
                        .await
                        .map_err(|error| OutboxStoreError::Database(error.to_string()))?,
                );
            }
            Ok(deleted)
        }
        .await;
        match result {
            Ok(deleted) => {
                self.connection
                    .execute_batch("COMMIT")
                    .await
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
                Ok(deleted)
            }
            Err(error) => {
                let _ = self.connection.execute_batch("ROLLBACK").await;
                Err(error)
            }
        }
    }

    pub async fn health(&self) -> Result<OutboxHealth, OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let mut rows = self
            .connection
            .query(
                "SELECT
                    SUM(CASE WHEN state = 'pending' AND priority = 'critical' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN state = 'pending' AND priority = 'telemetry' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN state = 'pending' AND priority = 'audit' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN state = 'pending' AND priority = 'reconciliation' THEN 1 ELSE 0 END),
                    MIN(CASE WHEN state = 'pending' THEN local_committed_at_us ELSE NULL END),
                    (SELECT COUNT(*) FROM dead_letters),
                    (SELECT CAST(value AS INTEGER) FROM sync_health WHERE key = 'last_local_ack_at_us'),
                    (SELECT CAST(value AS INTEGER) FROM sync_health WHERE key = 'last_remote_commit_at_us'),
                    MAX(CASE WHEN state = 'pending' THEN MAX(next_attempt_at_us - ?1, 0) ELSE 0 END),
                    (SELECT last_error FROM outbox_events
                     WHERE state = 'pending' AND last_error IS NOT NULL
                     ORDER BY next_attempt_at_us DESC LIMIT 1)
                 FROM outbox_events INDEXED BY idx_outbox_health_pending WHERE state = 'pending'",
                params![now_epoch_micros()],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let row = rows
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
            .ok_or_else(|| OutboxStoreError::InvalidRecord("missing health row".to_string()))?;
        let oldest: Option<i64> = row
            .get(4)
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let mut job_rows = self
            .connection
            .query(
                "SELECT generation, run_id, status, candidate_count, classified_count,
                        unresolved_count, current_cursor, retry_count, last_error
                 FROM reconciliation_jobs
                 WHERE status != 'superseded'
                 ORDER BY updated_at_us DESC LIMIT 1",
                (),
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let reconciliation = if let Some(job) = job_rows
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
        {
            Some(ReconciliationJobProgress {
                generation: job
                    .get(0)
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?,
                run_id: job
                    .get(1)
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?,
                status: job
                    .get(2)
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?,
                candidate_count: job
                    .get::<i64>(3)
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?
                    .max(0) as u64,
                classified_count: job
                    .get::<i64>(4)
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?
                    .max(0) as u64,
                unresolved_count: job
                    .get::<i64>(5)
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?
                    .max(0) as u64,
                current_cursor: job
                    .get(6)
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?,
                retry_count: job
                    .get::<i64>(7)
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?
                    .max(0) as u64,
                last_error: job
                    .get(8)
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?,
            })
        } else {
            None
        };
        Ok(OutboxHealth {
            pending_critical: row
                .get::<Option<i64>>(0)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?
                .unwrap_or(0)
                .max(0) as u64,
            pending_telemetry: row
                .get::<Option<i64>>(1)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?
                .unwrap_or(0)
                .max(0) as u64,
            pending_audit: row
                .get::<Option<i64>>(2)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?
                .unwrap_or(0)
                .max(0) as u64,
            pending_reconciliation: row
                .get::<Option<i64>>(3)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?
                .unwrap_or(0)
                .max(0) as u64,
            oldest_pending_age_ms: oldest.map(|timestamp| {
                now_epoch_micros().saturating_sub(timestamp).max(0) as u64 / 1_000
            }),
            dead_letters: row
                .get::<i64>(5)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?
                .max(0) as u64,
            last_local_ack_at_us: row
                .get(6)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?,
            last_remote_commit_at_us: row
                .get(7)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?,
            current_retry_delay_ms: row
                .get::<Option<i64>>(8)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?
                .unwrap_or(0)
                .max(0) as u64
                / 1_000,
            last_error: row
                .get(9)
                .map_err(|error| OutboxStoreError::Database(error.to_string()))?,
            database_bytes: self.database_bytes(),
            reconciliation,
        })
    }

    /// Pending durable work for exactly one strategy session. Runtime-host process cleanup uses
    /// this count to keep the sync agent and its credentials alive after the strategy process
    /// exits, until the final lifecycle events have reached the remote cursor or the DLQ.
    pub async fn pending_count_for_scope(
        &self,
        strategy_id: &str,
        live_session_id: &str,
    ) -> Result<u64, OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let mut rows = self
            .connection
            .query(
                PENDING_SCOPE_COUNT_QUERY,
                params![strategy_id.to_string(), live_session_id.to_string()],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let row = rows
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
            .ok_or_else(|| {
                OutboxStoreError::InvalidRecord("missing scoped count row".to_string())
            })?;
        Ok(row
            .get::<i64>(0)
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
            .max(0) as u64)
    }

    /// Total remotely-uncommitted work across every priority and runtime scope.
    /// Stronger than pending_count: failed/dead-letter rows also block retirement.
    pub async fn retirement_uncommitted_for_scope(
        &self,
        strategy_id: &str,
        live_session_id: &str,
    ) -> Result<u64, OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let mut rows = self
            .connection
            .query(
                "SELECT COUNT(*) FROM outbox_events INDEXED BY idx_outbox_scope_state
             WHERE state != 'committed' AND strategy_id = ?1 AND live_session_id = ?2",
                params![strategy_id.to_string(), live_session_id.to_string()],
            )
            .await
            .map_err(|e| OutboxStoreError::Database(e.to_string()))?;
        let row = rows
            .next()
            .await
            .map_err(|e| OutboxStoreError::Database(e.to_string()))?
            .ok_or_else(|| OutboxStoreError::InvalidRecord("missing retirement count".into()))?;
        Ok(row
            .get::<i64>(0)
            .map_err(|e| OutboxStoreError::Database(e.to_string()))?
            .max(0) as u64)
    }

    /// Total remotely-uncommitted work across every priority and runtime scope.
    pub async fn pending_count(&self) -> Result<u64, OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let mut rows = self
            .connection
            .query(
                "SELECT COUNT(*) FROM outbox_events WHERE state IN ('pending', 'in_flight')",
                (),
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let row = rows
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
            .ok_or_else(|| OutboxStoreError::InvalidRecord("missing pending count".to_string()))?;
        Ok(row
            .get::<i64>(0)
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
            .max(0) as u64)
    }

    /// Backpressure is local to the same durable stream as `pending_batch_for_runtime_scope`.
    /// An expired credential for another strategy must not starve healthy audit/reconciliation.
    pub async fn pending_critical_for_runtime_scope(
        &self,
        runtime_id: &str,
        strategy_id: &str,
        live_session_id: &str,
    ) -> Result<u64, OutboxStoreError> {
        #[cfg(test)]
        self.query_counts[2].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _operation = self.operation_gate.lock().await;
        let mut rows = self
            .connection
            .query(
                "SELECT COUNT(*) FROM outbox_events INDEXED BY idx_outbox_runtime_pending
             WHERE runtime_id = ?1 AND strategy_id = ?2 AND live_session_id = ?3
               AND priority = 'critical' AND state IN ('pending', 'in_flight')",
                params![
                    runtime_id.to_string(),
                    strategy_id.to_string(),
                    live_session_id.to_string()
                ],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let row = rows
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
            .ok_or_else(|| {
                OutboxStoreError::InvalidRecord("missing scoped critical count".into())
            })?;
        Ok(row
            .get::<i64>(0)
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
            .max(0) as u64)
    }

    /// Backpressure needs a threshold decision, not a count of the entire durable backlog.
    /// Stop after at most limit + 1 matching rows; this remains exact and uncached.
    pub async fn critical_backlog_exceeds_for_runtime_scope(
        &self,
        runtime_id: &str,
        strategy_id: &str,
        live_session_id: &str,
        limit: u64,
    ) -> Result<bool, OutboxStoreError> {
        #[cfg(test)]
        self.query_counts[2].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _operation = self.operation_gate.lock().await;
        let mut rows = self
            .connection
            .query(
                CRITICAL_BACKLOG_EXCEEDS_QUERY,
                params![
                    runtime_id.to_string(),
                    strategy_id.to_string(),
                    live_session_id.to_string(),
                    limit.min(i64::MAX as u64) as i64
                ],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let row = rows
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
            .ok_or_else(|| {
                OutboxStoreError::InvalidRecord("missing critical backlog decision".into())
            })?;
        row.get::<i64>(0)
            .map(|value| value != 0)
            .map_err(|error| OutboxStoreError::Database(error.to_string()))
    }

    pub async fn pending_count_for_runtime_scope(
        &self,
        runtime_id: &str,
        strategy_id: &str,
        live_session_id: &str,
    ) -> Result<u64, OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let mut rows = self
            .connection
            .query(
                "SELECT COUNT(*) FROM outbox_events INDEXED BY idx_outbox_runtime_pending
                 WHERE state = 'pending' AND runtime_id = ?1
                   AND strategy_id = ?2 AND live_session_id = ?3",
                params![
                    runtime_id.to_string(),
                    strategy_id.to_string(),
                    live_session_id.to_string(),
                ],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let row = rows
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
            .ok_or_else(|| {
                OutboxStoreError::InvalidRecord("missing runtime-scoped count row".to_string())
            })?;
        Ok(row
            .get::<i64>(0)
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
            .max(0) as u64)
    }

    /// Runtime generations with durable work for one strategy session. The runtime host uses
    /// this to start independent recovery agents because every generation owns its own remote
    /// critical cursor.
    pub async fn pending_runtime_ids_for_scope(
        &self,
        strategy_id: &str,
        live_session_id: &str,
    ) -> Result<Vec<String>, OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let mut rows = self
            .connection
            .query(
                PENDING_RUNTIME_IDS_QUERY,
                params![strategy_id.to_string(), live_session_id.to_string()],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let mut runtime_ids = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
        {
            runtime_ids.push(
                row.get(0)
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?,
            );
        }
        Ok(runtime_ids)
    }

    /// Repairs outboxes written before the runtime host persisted one complete critical stream
    /// per runtime generation. Retained committed rows are made replayable (remote application is
    /// idempotent), while genuinely absent sequence slots become explicit diagnostic checkpoints.
    /// This is only intended for historical runtime IDs, never the currently executing runtime.
    pub async fn prepare_legacy_runtime_replay(
        &self,
        runtime_id: &str,
        strategy_id: &str,
        live_session_id: &str,
    ) -> Result<usize, OutboxStoreError> {
        let operation = self.operation_gate.lock().await;
        // Validate before rewriting committed rows or allocating the retained
        // sequence set. Oversized legacy journals are operator-repair work,
        // not a reason to rescan/requeue thousands of rows on every retry.
        let mut max_rows = self
            .connection
            .query(
                "SELECT COALESCE(MAX(critical_sequence), 0) FROM outbox_events
             WHERE priority = 'critical' AND runtime_id = ?1
               AND strategy_id = ?2 AND live_session_id = ?3",
                params![
                    runtime_id.to_string(),
                    strategy_id.to_string(),
                    live_session_id.to_string()
                ],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let max_sequence = max_rows
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
            .map(|row| row.get::<i64>(0))
            .transpose()
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
            .unwrap_or(0)
            .max(0) as u64;
        drop(max_rows);
        if max_sequence > 10_000 {
            return Err(OutboxStoreError::InvalidRecord(format!(
                "legacy runtime {runtime_id} requires an unsafe number of sequence checkpoints ({max_sequence})"
            )));
        }
        self.connection
            .execute(
                "UPDATE outbox_events
                 SET state = 'pending', next_attempt_at_us = 0
                 WHERE state = 'committed' AND priority = 'critical'
                   AND runtime_id = ?1 AND strategy_id = ?2 AND live_session_id = ?3",
                params![
                    runtime_id.to_string(),
                    strategy_id.to_string(),
                    live_session_id.to_string(),
                ],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let mut rows = self
            .connection
            .query(
                "SELECT critical_sequence FROM outbox_events
                 WHERE priority = 'critical' AND runtime_id = ?1
                   AND strategy_id = ?2 AND live_session_id = ?3
                   AND critical_sequence IS NOT NULL
                 ORDER BY critical_sequence",
                params![
                    runtime_id.to_string(),
                    strategy_id.to_string(),
                    live_session_id.to_string(),
                ],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let mut sequences = std::collections::BTreeSet::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
        {
            sequences.insert(
                row.get::<i64>(0)
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))?
                    .max(0) as u64,
            );
        }
        drop(rows);
        drop(operation);

        let Some(max_sequence) = sequences.last().copied() else {
            return Ok(0);
        };
        let mut checkpoints = Vec::new();
        for sequence in 1..=max_sequence {
            if sequences.contains(&sequence) {
                continue;
            }
            let mut event = EventEnvelope::new(
                runtime_id,
                strategy_id,
                live_session_id,
                format!("legacy-critical-gap:{sequence}"),
                sequence,
                EventPriority::Critical,
                EventKind::ReconciliationProgress,
                serde_json::json!({
                    "status": "legacy_sequence_checkpoint",
                    "missing_critical_sequence": sequence,
                    "message": "Historical runtime outbox did not retain this pre-migration sequence; checkpoint inserted so later durable events can replay in order"
                }),
            );
            event.critical_sequence = Some(sequence);
            checkpoints.push(event);
        }
        self.insert_events(&checkpoints).await?;
        Ok(checkpoints.len())
    }

    /// Ensures an unexpectedly exited managed process leaves a durable terminal lifecycle event.
    /// AQE normally emits this during graceful teardown; the runtime host supplies it only when
    /// the process is gone and the scoped outbox has otherwise drained.
    pub async fn ensure_runtime_stopped_lifecycle(
        &self,
        runtime_id: &str,
        strategy_id: &str,
        live_session_id: &str,
    ) -> Result<bool, OutboxStoreError> {
        self.ensure_runtime_stopped_lifecycle_for_run(
            runtime_id,
            strategy_id,
            live_session_id,
            None,
        )
        .await
    }

    pub async fn ensure_runtime_stopped_lifecycle_for_run(
        &self,
        runtime_id: &str,
        strategy_id: &str,
        live_session_id: &str,
        run_id: Option<&str>,
    ) -> Result<bool, OutboxStoreError> {
        let operation = self.operation_gate.lock().await;
        let mut rows = self
            .connection
            .query(
                "SELECT wire_payload FROM outbox_events
                 WHERE runtime_id = ?1 AND strategy_id = ?2 AND live_session_id = ?3
                   AND event_kind = 'strategy_lifecycle'
                 ORDER BY critical_sequence DESC, created_at_us DESC LIMIT 1",
                params![
                    runtime_id.to_string(),
                    strategy_id.to_string(),
                    live_session_id.to_string(),
                ],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let latest = rows
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
            .map(|row| {
                row.get::<Vec<u8>>(0)
                    .map_err(|error| OutboxStoreError::Database(error.to_string()))
                    .and_then(|payload| FlatbufferEventCodec::decode(&payload).map_err(Into::into))
            })
            .transpose()?;
        drop(rows);
        if latest.as_ref().is_some_and(|event| {
            event
                .payload
                .get("state")
                .and_then(serde_json::Value::as_str)
                != Some("running")
        }) {
            return Ok(false);
        }

        let mut sequence_rows = self
            .connection
            .query(
                "SELECT COALESCE(MAX(critical_sequence), 0) FROM outbox_events
                 WHERE runtime_id = ?1 AND strategy_id = ?2 AND live_session_id = ?3",
                params![
                    runtime_id.to_string(),
                    strategy_id.to_string(),
                    live_session_id.to_string(),
                ],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        let next_sequence = sequence_rows
            .next()
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?
            .and_then(|row| row.get::<i64>(0).ok())
            .unwrap_or(0)
            .max(0) as u64
            + 1;
        drop(sequence_rows);
        drop(operation);

        let revision = now_epoch_micros().max(1) as u64;
        let mut event = EventEnvelope::new(
            runtime_id,
            strategy_id,
            live_session_id,
            "strategy-lifecycle",
            revision,
            EventPriority::Critical,
            EventKind::StrategyLifecycle,
            serde_json::json!({
                "strategy_id": strategy_id,
                "state": "stopped",
                "status": "Stopped",
                "reason": "managed_runtime_process_exited"
            }),
        );
        event.critical_sequence = Some(next_sequence);
        event.run_id = run_id
            .map(str::to_owned)
            .or_else(|| latest.and_then(|event| event.run_id));
        Ok(self.insert_events(&[event]).await? > 0)
    }

    fn database_bytes(&self) -> u64 {
        let main = std::fs::metadata(&self.path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        let wal = std::fs::metadata(format!("{}-wal", self.path.to_string_lossy()))
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        let shared_memory = std::fs::metadata(format!("{}-shm", self.path.to_string_lossy()))
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        main.saturating_add(wal).saturating_add(shared_memory)
    }

    async fn set_health_value(&self, key: &str, value: &str) -> Result<(), OutboxStoreError> {
        self.connection
            .execute(
                "INSERT INTO sync_health(key, value, updated_at_us) VALUES (?1, ?2, ?3)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at_us = excluded.updated_at_us",
                params![key.to_string(), value.to_string(), now_epoch_micros()],
            )
            .await
            .map_err(|error| OutboxStoreError::Database(error.to_string()))?;
        Ok(())
    }
}

fn priority_label(priority: EventPriority) -> &'static str {
    match priority {
        EventPriority::Critical => "critical",
        EventPriority::Telemetry => "telemetry",
        EventPriority::Audit => "audit",
        EventPriority::Reconciliation => "reconciliation",
    }
}

fn bounded_reconciliation_chunks(
    records: Vec<ReconciliationRecord>,
) -> Vec<Vec<ReconciliationRecord>> {
    let target = RECONCILIATION_TARGET_BYTES.saturating_sub(2 * 1024);
    let mut chunks = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes = 0usize;
    for record in records {
        let bytes = serde_json::to_vec(&record)
            .map(|value| value.len())
            .unwrap_or(target);
        if !current.is_empty() && current_bytes.saturating_add(bytes).saturating_add(1) > target {
            chunks.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes = current_bytes
            .saturating_add(usize::from(!current.is_empty()))
            .saturating_add(bytes);
        current.push(record);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

fn upgraded_reconciliation_event(
    source: &CloudEvent,
    suffix: &str,
    kind: EventKind,
    payload: serde_json::Value,
) -> CloudEvent {
    let mut event = EventEnvelope::new(
        source.runtime_id.clone(),
        source.strategy_id.clone(),
        source.live_session_id.clone(),
        format!("{}:{suffix}", source.entity_id),
        source.entity_revision,
        EventPriority::Reconciliation,
        kind,
        payload,
    );
    event.run_id = source.run_id.clone();
    event.event_id = deterministic_event_id(&format!(
        "aqs-live-reconciliation-v2:{}:{suffix}",
        source.event_id
    ));
    event.created_at = source.created_at;
    event
}

fn latency_stage_durations(trace: &super::LatencyTrace) -> Vec<(&'static str, i64)> {
    let mut durations = Vec::with_capacity(7);
    let mut add = |stage, start: Option<i64>, end: Option<i64>| {
        if let (Some(start), Some(end)) = (start, end) {
            durations.push((stage, end.saturating_sub(start).max(0)));
        }
    };
    add(
        "source_to_adapter_queue",
        trace.source_occurred_at_us,
        trace.adapter_queued_at_us,
    );
    add(
        "adapter_queue_to_runtime",
        trace.adapter_queued_at_us,
        trace.runtime_received_at_us,
    );
    add(
        "runtime_to_local_bus",
        trace.runtime_received_at_us,
        trace.local_bus_published_at_us,
    );
    add(
        "local_bus_to_outbox",
        trace.local_bus_published_at_us,
        trace.local_outbox_committed_at_us,
    );
    add(
        "outbox_to_persistence_request",
        trace.local_outbox_committed_at_us,
        trace.persistence_request_sent_at_us,
    );
    add(
        "persistence_request_to_ack",
        trace.persistence_request_sent_at_us,
        trace.persistence_ack_received_at_us,
    );
    add(
        "source_to_persistence_ack",
        trace.source_occurred_at_us.or(trace.runtime_received_at_us),
        trace.persistence_ack_received_at_us,
    );
    durations
}

fn percentile(values: &[i64], percentile: usize) -> i64 {
    if values.is_empty() {
        return 0;
    }
    let index = ((values.len() - 1) * percentile).div_ceil(100);
    values[index.min(values.len() - 1)]
}

#[cfg(test)]
#[path = "outbox_tests.rs"]
mod tests;
