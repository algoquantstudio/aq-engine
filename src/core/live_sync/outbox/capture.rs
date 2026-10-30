//! Admission for NEW cloud capture only. Never delete or expire accepted payloads
//! to meet a cap. The ordinary handoff/recovery APIs deliberately remain separate.
//!
//! These primitives are not enabled by opening an outbox. A producer/host must
//! explicitly use the capped boundary and handle CapturePaused nonfatally before
//! rollout activation. No billing, tier, broker or calendar concepts live here.
use super::*;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
mod ownership;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CaptureCapacity {
    /// Full retained wire payloads, including committed rows awaiting pruning
    /// and dead-letter copies. Not an estimate of SQLite pages/WAL/index sizes.
    pub retained_bytes: u64,
    pub requested_bytes: u64,
    pub limit_bytes: u64,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub physical: Option<super::PhysicalCaptureUsage>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CapturePauseRecord {
    pub runtime_id: String,
    pub strategy_id: String,
    pub live_session_id: String,
    /// Changes for every new refused batch or intentional observation pause;
    /// an earlier reconciliation cannot clear a later pause. One row per scope.
    pub generation: u64,
    pub started_at_us: i64,
    pub last_blocked_at_us: i64,
    pub blocked_attempts: u64,
    pub resumed_at_us: Option<i64>,
    pub capacity: CaptureCapacity,
    /// Original missing-history provenance survives local owner handoff. A
    /// legacy row without a recorded run stays explicitly unknown.
    #[serde(default)]
    pub origin_runtime_id: Option<String>,
    #[serde(default)]
    pub origin_run_id: Option<String>,
    #[serde(default)]
    pub last_blocked_run_id: Option<String>,
    #[serde(default)]
    pub owner_run_id: Option<String>,
    #[serde(default)]
    pub handoff_count: u64,
}

fn db(error: impl std::fmt::Display) -> OutboxStoreError {
    OutboxStoreError::Database(error.to_string())
}

pub(super) fn same_accepted_payload(left: &CloudEvent, right: &CloudEvent) -> bool {
    let mut left = left.clone();
    let mut right = right.clone();
    left.latency = Default::default();
    right.latency = Default::default();
    if left.created_at.timestamp_micros() == right.created_at.timestamp_micros() {
        right.created_at = left.created_at;
    }
    left == right
}

fn pause_row(row: turso::Row) -> Result<CapturePauseRecord, OutboxStoreError> {
    Ok(CapturePauseRecord {
        runtime_id: row.get(0).map_err(db)?,
        strategy_id: row.get(1).map_err(db)?,
        live_session_id: row.get(2).map_err(db)?,
        generation: u64::try_from(row.get::<i64>(3).map_err(db)?).map_err(db)?,
        started_at_us: row.get(4).map_err(db)?,
        last_blocked_at_us: row.get(5).map_err(db)?,
        blocked_attempts: u64::try_from(row.get::<i64>(6).map_err(db)?).map_err(db)?,
        resumed_at_us: row.get(7).map_err(db)?,
        capacity: CaptureCapacity {
            retained_bytes: u64::try_from(row.get::<i64>(8).map_err(db)?).map_err(db)?,
            requested_bytes: u64::try_from(row.get::<i64>(9).map_err(db)?).map_err(db)?,
            limit_bytes: u64::try_from(row.get::<i64>(10).map_err(db)?).map_err(db)?,
            physical:None,
        },
        origin_runtime_id: row.get(11).map_err(db)?,
        origin_run_id: row.get(12).map_err(db)?,
        last_blocked_run_id: row.get(13).map_err(db)?,
        owner_run_id: row.get(14).map_err(db)?,
        handoff_count: u64::try_from(row.get::<i64>(15).map_err(db)?).map_err(db)?,
    })
}

impl OutboxStore {
    /// Lazy, transactional local-schema upgrade. The only full inventory is on
    /// first installation. Every later admission reads one aggregate row plus
    /// primary-key lookups for the bounded candidate batch. Triggers keep the
    /// aggregate correct across existing insert/prune/DLQ/coalescing paths too.
    pub(super) async fn ensure_capture_capacity_schema(&self) -> Result<(), OutboxStoreError> {
        if self.capture_initialized.load(Ordering::Acquire) {
            return Ok(());
        }
        let _operation = self.operation_gate.lock().await;
        if self.capture_initialized.load(Ordering::Acquire) {
            return Ok(());
        }
        let transaction = turso::transaction::Transaction::new_unchecked(
            &self.connection,
            turso::transaction::TransactionBehavior::Immediate,
        )
        .await
        .map_err(db)?;
        let result = async {
            self.connection.execute_batch(
                "CREATE TABLE IF NOT EXISTS capture_capacity (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    retained_bytes INTEGER NOT NULL CHECK(retained_bytes>=0));
                 CREATE TABLE IF NOT EXISTS capture_pauses (
                    runtime_id TEXT NOT NULL, strategy_id TEXT NOT NULL,
                    live_session_id TEXT NOT NULL, generation INTEGER NOT NULL,
                    started_at_us INTEGER NOT NULL, last_blocked_at_us INTEGER NOT NULL,
                    blocked_attempts INTEGER NOT NULL, resumed_at_us INTEGER,
                    retained_bytes INTEGER NOT NULL, requested_bytes INTEGER NOT NULL,
                    limit_bytes INTEGER NOT NULL,
                    origin_runtime_id TEXT, origin_run_id TEXT, last_blocked_run_id TEXT, owner_run_id TEXT,
                    handoff_count INTEGER NOT NULL DEFAULT 0, notice_handoff_generation INTEGER,
                    origin_runtime_unknown INTEGER NOT NULL DEFAULT 0, source_observation_pause_id TEXT,
                    PRIMARY KEY(runtime_id,strategy_id,live_session_id));
                 CREATE INDEX IF NOT EXISTS capture_pauses_open
                    ON capture_pauses(resumed_at_us,last_blocked_at_us DESC);
                 CREATE INDEX IF NOT EXISTS capture_pauses_scope
                    ON capture_pauses(strategy_id,live_session_id,resumed_at_us);
                 CREATE INDEX IF NOT EXISTS capture_pauses_scope_latest
                    ON capture_pauses(strategy_id,live_session_id,resumed_at_us,last_blocked_at_us DESC);"
            ).await.map_err(db)?;
            self.connection.execute_batch(
                "CREATE TABLE IF NOT EXISTS capture_source_observation (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL CHECK(version=1),
                    token TEXT NOT NULL CHECK(length(token)=36),runtime_id TEXT NOT NULL CHECK(length(runtime_id) BETWEEN 1 AND 128),
                    strategy_id TEXT NOT NULL CHECK(length(strategy_id) BETWEEN 1 AND 128),
                    live_session_id TEXT NOT NULL CHECK(length(live_session_id) BETWEEN 1 AND 128),
                    run_id TEXT NOT NULL CHECK(length(run_id) BETWEEN 1 AND 128),
                    journal_source_id TEXT NOT NULL CHECK(length(journal_source_id) BETWEEN 1 AND 128),
                    account_id TEXT NOT NULL CHECK(length(account_id) BETWEEN 1 AND 128),
                    started_at_us INTEGER NOT NULL CHECK(started_at_us>0),closed_at_us INTEGER,
                    limit_bytes INTEGER NOT NULL CHECK(limit_bytes>0));
                 CREATE TABLE IF NOT EXISTS capture_journal_gap_handoffs (
                    journal_source_id TEXT PRIMARY KEY,source_generation INTEGER NOT NULL,
                    source_epoch INTEGER NOT NULL,strategy_id TEXT NOT NULL,account_id TEXT NOT NULL,
                    pause_id TEXT NOT NULL,receipt TEXT NOT NULL);"
            ).await.map_err(db)?;
            let mut rows = self.connection.query(
                "SELECT retained_bytes FROM capture_capacity WHERE singleton=1", ()
            ).await.map_err(db)?;
            let exists = rows.next().await.map_err(db)?.is_some();
            drop(rows);
            if !exists {
                self.connection.execute_batch(
                    "INSERT INTO capture_capacity(singleton,retained_bytes)
                     SELECT 1,
                       (SELECT COALESCE(SUM(length(wire_payload)),0) FROM outbox_events) +
                       (SELECT COALESCE(SUM(length(wire_payload)),0) FROM dead_letters);"
                ).await.map_err(db)?;
            }
            self.connection.execute_batch(
                "CREATE TRIGGER IF NOT EXISTS capture_outbox_insert AFTER INSERT ON outbox_events BEGIN
                   UPDATE capture_capacity SET retained_bytes=retained_bytes+length(NEW.wire_payload) WHERE singleton=1; END;
                 CREATE TRIGGER IF NOT EXISTS capture_outbox_delete AFTER DELETE ON outbox_events BEGIN
                   UPDATE capture_capacity SET retained_bytes=retained_bytes-length(OLD.wire_payload) WHERE singleton=1; END;
                 CREATE TRIGGER IF NOT EXISTS capture_outbox_resize AFTER UPDATE OF wire_payload ON outbox_events BEGIN
                   UPDATE capture_capacity SET retained_bytes=retained_bytes-length(OLD.wire_payload)+length(NEW.wire_payload) WHERE singleton=1; END;
                 CREATE TRIGGER IF NOT EXISTS capture_dlq_insert AFTER INSERT ON dead_letters BEGIN
                   UPDATE capture_capacity SET retained_bytes=retained_bytes+length(NEW.wire_payload) WHERE singleton=1; END;
                 CREATE TRIGGER IF NOT EXISTS capture_dlq_delete AFTER DELETE ON dead_letters BEGIN
                   UPDATE capture_capacity SET retained_bytes=retained_bytes-length(OLD.wire_payload) WHERE singleton=1; END;
                 CREATE TRIGGER IF NOT EXISTS capture_dlq_resize AFTER UPDATE OF wire_payload ON dead_letters BEGIN
                   UPDATE capture_capacity SET retained_bytes=retained_bytes-length(OLD.wire_payload)+length(NEW.wire_payload) WHERE singleton=1; END;"
            ).await.map_err(db)?;
            Ok::<_, OutboxStoreError>(())
        }.await;
        match result {
            Ok(()) => {
                self.commit_bounded_metadata(transaction).await?;
                self.capture_initialized.store(true, Ordering::Release);
                Ok(())
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    pub(super) async fn capture_retained_bytes_in_transaction(&self) -> Result<u64, OutboxStoreError> {
        let mut rows = self
            .connection
            .query(
                "SELECT retained_bytes FROM capture_capacity WHERE singleton=1",
                (),
            )
            .await
            .map_err(db)?;
        let value: i64 = rows
            .next()
            .await
            .map_err(db)?
            .ok_or_else(|| db("capture capacity aggregate missing"))?
            .get(0)
            .map_err(db)?;
        u64::try_from(value).map_err(db)
    }

    /// Indexed aggregate, never a history scan after initial installation.
    pub async fn capture_retained_bytes(&self) -> Result<u64, OutboxStoreError> {
        self.ensure_capture_capacity_schema().await?;
        let _operation = self.operation_gate.lock().await;
        self.capture_retained_bytes_in_transaction().await
    }

    /// Cleanup/retirement check only. Do not install capture accounting or scan
    /// old payloads merely to inspect an outbox whose cap was never enabled.
    pub async fn has_unresolved_capture_pause_for_scope(
        &self,
        strategy: &str,
        session: &str,
    ) -> Result<bool, OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let mut schema = self
            .connection
            .query(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='capture_pauses'",
                (),
            )
            .await
            .map_err(db)?;
        if schema.next().await.map_err(db)?.is_none() {
            return Ok(false);
        }
        drop(schema);
        let mut rows = self
            .connection
            .query(
                "SELECT EXISTS(SELECT 1 FROM capture_pauses
             WHERE strategy_id=?1 AND live_session_id=?2 AND resumed_at_us IS NULL LIMIT 1)",
                params![strategy.to_owned(), session.to_owned()],
            )
            .await
            .map_err(db)?;
        Ok(rows
            .next()
            .await
            .map_err(db)?
            .ok_or_else(|| db("missing capture pause check"))?
            .get::<i64>(0)
            .map_err(db)?
            != 0)
    }

    /// Called inside the SAME write transaction as payload/sequence admission.
    /// Exact duplicates reserve no additional payload bytes, even at the cap.
    pub(super) async fn check_capture_capacity_in_transaction(
        &self,
        events: &[CloudEvent],
        limit_bytes: u64,
    ) -> Result<CaptureCapacity, OutboxStoreError> {
        if limit_bytes == 0 || limit_bytes > i64::MAX as u64 {
            return Err(OutboxStoreError::InvalidRecord(
                "invalid capture byte limit".into(),
            ));
        }
        if events.len() > 256 {
            return Err(OutboxStoreError::InvalidRecord(
                "capture batch exceeds 256 events".into(),
            ));
        }
        let mut requested_bytes = 0_u64;
        let mut seen = HashSet::with_capacity(events.len());
        for event in events {
            if event.kind == EventKind::Log || !seen.insert(event.event_id) {
                continue;
            }
            let mut rows = self
                .connection
                .query(
                    "SELECT event_id FROM outbox_events WHERE event_id=?1",
                    params![event.event_id.to_string()],
                )
                .await
                .map_err(db)?;
            if rows.next().await.map_err(db)?.is_some() {
                continue;
            }
            drop(rows);
            // The local commit timestamp is fixed-width on the wire. Encode the
            // same representation as insert_events_in_transaction, not JSON.
            let bytes =
                FlatbufferEventCodec::encode_with_local_commit(event, now_epoch_micros())?.len();
            if bytes > LOCAL_EVENT_HARD_LIMIT_BYTES {
                return Err(OutboxStoreError::InvalidRecord(
                    "capture event exceeds wire limit".into(),
                ));
            }
            requested_bytes = requested_bytes
                .checked_add(bytes as u64)
                .ok_or_else(|| db("capture byte reservation overflow"))?;
        }
        let capacity = CaptureCapacity {
            retained_bytes: self.capture_retained_bytes_in_transaction().await?,
            requested_bytes,
            limit_bytes,
            physical:None,
        };
        if requested_bytes > 0
            && requested_bytes > limit_bytes.saturating_sub(capacity.retained_bytes)
        {
            return Err(OutboxStoreError::CapturePaused(capacity));
        }
        Ok(capacity)
    }

    /// New-capture boundary for a host-owned outbox. A paused result does NOT
    /// acknowledge any event and does not mutate/evict already accepted work.
    /// Recovery of previously accepted events must retain its original owner
    /// until an actual durable handoff acknowledgement is received.
    pub async fn capture_events_with_capacity(
        &self,
        events: &[CloudEvent],
        limit_bytes: u64,
    ) -> Result<usize, OutboxStoreError> {
        self.insert_bounded_events(events, Some(limit_bytes), true)
            .await
    }

    /// Ownership transfer of evidence already accepted by another local
    /// outbox. A refusal retains the source owner and must not manufacture a
    /// history gap or allocate a critical sequence at the destination.
    pub async fn handoff_accepted_events_with_capacity(
        &self,
        events: &[CloudEvent],
        limit_bytes: u64,
    ) -> Result<usize, OutboxStoreError> {
        self.insert_bounded_events(events, Some(limit_bytes), false)
            .await
    }

    /// Exact accepted-work handoff for the legacy/unlimited local configuration.
    /// Does not install or scan the optional capacity aggregate.
    pub async fn handoff_accepted_events(
        &self,
        events: &[CloudEvent],
    ) -> Result<usize, OutboxStoreError> {
        self.insert_bounded_events(events, None, false).await
    }

    /// Accept the largest ordered prefix that fits in one transaction. The
    /// source may release only these first N envelopes. A full destination
    /// examines only the head event, rather than retrying halved transactions.
    pub async fn handoff_accepted_prefix_with_capacity(
        &self,
        events: &[CloudEvent],
        limit_bytes: u64,
    ) -> Result<(usize, Option<CaptureCapacity>), OutboxStoreError> {
        let limit_bytes=self.physical_budget.map_or(limit_bytes,|policy|limit_bytes.min(policy.payload_limit_bytes));
        if events.is_empty() {
            return Ok((0, None));
        }
        if events.len() > 256
            || events.iter().any(|event| event.kind == EventKind::Log)
            || limit_bytes == 0
            || limit_bytes > i64::MAX as u64
        {
            return Err(OutboxStoreError::InvalidRecord(
                "invalid accepted-work prefix bounds".into(),
            ));
        }
        self.ensure_capture_capacity_schema().await?;
        let _operation = self.operation_gate.lock().await;
        let transaction = turso::transaction::Transaction::new_unchecked(
            &self.connection,
            turso::transaction::TransactionBehavior::Immediate,
        )
        .await
        .map_err(db)?;
        let result = async {
            let retained = self.capture_retained_bytes_in_transaction().await?;
            let mut requested = 0u64;
            let mut accepted = 0usize;
            let mut deferred = None;
            let mut seen = HashSet::with_capacity(events.len());
            for event in events {
                if seen.insert(event.event_id) {
                    let mut rows = self
                        .connection
                        .query(
                            "SELECT event_id FROM outbox_events WHERE event_id=?1",
                            params![event.event_id.to_string()],
                        )
                        .await
                        .map_err(db)?;
                    let exists = rows.next().await.map_err(db)?.is_some();
                    drop(rows);
                    if !exists {
                        let size = FlatbufferEventCodec::encode_with_local_commit(
                            event,
                            now_epoch_micros(),
                        )?
                        .len() as u64;
                        if size > LOCAL_EVENT_HARD_LIMIT_BYTES as u64 {
                            return Err(OutboxStoreError::InvalidRecord(
                                "accepted handoff event exceeds wire limit".into(),
                            ));
                        }
                        let next = requested
                            .checked_add(size)
                            .ok_or_else(|| db("handoff size overflow"))?;
                        if next > limit_bytes.saturating_sub(retained) {
                            deferred = Some(CaptureCapacity {
                                retained_bytes: retained,
                                requested_bytes: next,
                                limit_bytes,
                                physical:None,
                            });
                            break;
                        }
                        requested = next;
                    }
                }
                accepted += 1;
            }
            if accepted > 0 {
                self.insert_events_with_payload_check_in_transaction(&events[..accepted], true)
                    .await?;
            }
            // Guard even the clock-width boundary between estimating and
            // encoding local commit metadata; physical inserts are authoritative.
            let actual = self.capture_retained_bytes_in_transaction().await?;
            if requested > 0 && actual > limit_bytes {
                return Err(OutboxStoreError::CapturePaused(CaptureCapacity {
                    retained_bytes: retained,
                    requested_bytes: actual.saturating_sub(retained),
                    limit_bytes,
                    physical:None,
                }));
            }
            Ok((accepted, deferred))
        }
        .await;
        match result {
            Ok((accepted, deferred)) => {
                transaction.commit().await.map_err(db)?;
                for event in &events[..accepted] {
                    self.priority_change_notifier(event.priority)
                        .notify_waiters();
                }
                if events[..accepted]
                    .iter()
                    .any(|event| aqe_live_protocol::SafetyObservation::from_event(event).is_some())
                {
                    self.safety_notify.notify_waiters();
                }
                Ok((accepted, deferred))
            }
            Err(OutboxStoreError::CapturePaused(capacity)) => {
                transaction.rollback().await.map_err(db)?;
                Ok((0, Some(self.normalize_physical_refusal(capacity).await?)))
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    async fn insert_bounded_events(
        &self,
        events: &[CloudEvent],
        limit_bytes: Option<u64>,
        new_capture: bool,
    ) -> Result<usize, OutboxStoreError> {
        let limit_bytes=match self.physical_budget {
            Some(policy)=>Some(limit_bytes.unwrap_or(policy.payload_limit_bytes).min(policy.payload_limit_bytes)),
            None=>limit_bytes,
        };
        if events
            .iter()
            .filter(|event| event.kind != EventKind::Log)
            .take(257)
            .count()
            > 256
        {
            return Err(OutboxStoreError::InvalidRecord(
                "capture batch exceeds 256 events".into(),
            ));
        }
        let events = events
            .iter()
            .filter(|event| event.kind != EventKind::Log)
            .cloned()
            .collect::<Vec<_>>();
        if events.is_empty() {
            return Ok(0);
        }
        if limit_bytes.is_some() {
            self.ensure_capture_capacity_schema().await?;
        }
        let _operation = self.operation_gate.lock().await;
        let transaction = turso::transaction::Transaction::new_unchecked(
            &self.connection,
            turso::transaction::TransactionBehavior::Immediate,
        )
        .await
        .map_err(db)?;
        let result = async {
            if let Some(limit) = limit_bytes {
                self.check_capture_capacity_in_transaction(&events, limit)
                    .await?;
            }
            // Verify immutable payloads only on INSERT-OR-IGNORE conflicts.
            // Ordinary successful inserts need no extra payload lookup/decode.
            self.insert_events_with_payload_check_in_transaction(&events, true)
                .await
        }
        .await;
        match result {
            Ok(inserted) => {
                transaction.commit().await.map_err(db)?;
                for event in &events {
                    self.priority_change_notifier(event.priority)
                        .notify_waiters();
                }
                if events
                    .iter()
                    .any(|event| aqe_live_protocol::SafetyObservation::from_event(event).is_some())
                {
                    self.safety_notify.notify_waiters();
                }
                Ok(inserted)
            }
            Err(OutboxStoreError::CapturePaused(capacity)) => {
                transaction.rollback().await.map_err(db)?;
                let capacity=self.normalize_physical_refusal(capacity).await?;
                if new_capture {
                    self.record_capture_pause_locked(&events, capacity).await?;
                    self.safety_notify.notify_waiters();
                }
                Err(OutboxStoreError::CapturePaused(capacity))
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    pub(super) async fn record_capture_pause_locked(
        &self,
        events: &[CloudEvent],
        capacity: CaptureCapacity,
    ) -> Result<(), OutboxStoreError> {
        let now = now_epoch_micros();
        let mut scopes = std::collections::BTreeMap::<(&str, &str, &str), (i64, Option<&str>, Option<&str>)>::new();
        for event in events {
            if event.kind == EventKind::Log {
                continue;
            }
            let at = event.created_at.timestamp_micros().min(now);
            scopes
                .entry((
                    &event.runtime_id,
                    &event.strategy_id,
                    &event.live_session_id,
                ))
                .and_modify(|span| {
                    if at < span.0 { span.0 = at; span.1 = event.run_id.as_deref(); }
                    span.2 = event.run_id.as_deref();
                })
                .or_insert((at, event.run_id.as_deref(), event.run_id.as_deref()));
        }
        for ((runtime, strategy, session), (started, origin_run, last_run)) in scopes {
            self.execute_bounded_metadata(
                "INSERT INTO capture_pauses(runtime_id,strategy_id,live_session_id,generation,
                   started_at_us,last_blocked_at_us,blocked_attempts,resumed_at_us,
                   retained_bytes,requested_bytes,limit_bytes,origin_runtime_id,origin_run_id,last_blocked_run_id,owner_run_id,handoff_count)
                 VALUES(?1,?2,?3,1,?4,?5,1,NULL,?6,?7,?8,?1,?9,?10,?10,0)
                 ON CONFLICT(runtime_id,strategy_id,live_session_id) DO UPDATE SET
                   generation=capture_pauses.generation+1,
                   started_at_us=CASE WHEN capture_pauses.resumed_at_us IS NULL THEN MIN(capture_pauses.started_at_us,excluded.started_at_us) ELSE excluded.started_at_us END,
                   origin_runtime_id=CASE WHEN capture_pauses.resumed_at_us IS NULL THEN CASE WHEN capture_pauses.origin_runtime_unknown=1 THEN NULL ELSE COALESCE(capture_pauses.origin_runtime_id,capture_pauses.runtime_id) END ELSE excluded.origin_runtime_id END,
                   origin_runtime_unknown=CASE WHEN capture_pauses.resumed_at_us IS NULL THEN capture_pauses.origin_runtime_unknown ELSE 0 END,
                   origin_run_id=CASE WHEN capture_pauses.resumed_at_us IS NULL THEN capture_pauses.origin_run_id ELSE excluded.origin_run_id END,
                   owner_run_id=CASE WHEN capture_pauses.resumed_at_us IS NULL THEN capture_pauses.owner_run_id ELSE excluded.owner_run_id END,
                   handoff_count=CASE WHEN capture_pauses.resumed_at_us IS NULL THEN capture_pauses.handoff_count ELSE 0 END,
                   last_blocked_run_id=excluded.last_blocked_run_id,
                   last_blocked_at_us=excluded.last_blocked_at_us,
                   blocked_attempts=capture_pauses.blocked_attempts+1,resumed_at_us=NULL,
                   retained_bytes=excluded.retained_bytes,requested_bytes=excluded.requested_bytes,
                   limit_bytes=excluded.limit_bytes",
                params![runtime.to_owned(),strategy.to_owned(),session.to_owned(),
                    started,now,capacity.retained_bytes as i64, capacity.requested_bytes as i64, capacity.limit_bytes as i64,
                    origin_run.map(str::to_owned),last_run.map(str::to_owned)],
            ).await?;
        }
        Ok(())
    }

    pub async fn capture_pause_for_scope(
        &self,
        runtime_id: &str,
        strategy_id: &str,
        live_session_id: &str,
    ) -> Result<Option<CapturePauseRecord>, OutboxStoreError> {
        self.ensure_capture_capacity_schema().await?;
        let _operation = self.operation_gate.lock().await;
        let mut rows = self
            .connection
            .query(
                "SELECT runtime_id,strategy_id,live_session_id,generation,started_at_us,last_blocked_at_us,blocked_attempts,resumed_at_us,
                    retained_bytes,requested_bytes,limit_bytes,origin_runtime_id,origin_run_id,last_blocked_run_id,owner_run_id,handoff_count FROM capture_pauses
             WHERE runtime_id=?1 AND strategy_id=?2 AND live_session_id=?3",
                params![runtime_id, strategy_id, live_session_id],
            )
            .await
            .map_err(db)?;
        let Some(row) = rows.next().await.map_err(db)? else {
            return Ok(None);
        };
        Ok(Some(pause_row(row)?))
    }

    /// A publisher outbox is normally owned by one strategy/session. Return one
    /// unresolved scope at a time so restart/recovery cannot silently clear an
    /// older pause when the current process has a new run identity.
    pub(crate) async fn latest_capture_pause(
        &self,
    ) -> Result<Option<CapturePauseRecord>, OutboxStoreError> {
        self.ensure_capture_capacity_schema().await?;
        let _operation = self.operation_gate.lock().await;
        let mut rows=self.connection.query(
            "SELECT runtime_id,strategy_id,live_session_id,generation,started_at_us,last_blocked_at_us,blocked_attempts,resumed_at_us,
                    retained_bytes,requested_bytes,limit_bytes,origin_runtime_id,origin_run_id,last_blocked_run_id,owner_run_id,handoff_count FROM capture_pauses INDEXED BY capture_pauses_open
             WHERE resumed_at_us IS NULL ORDER BY last_blocked_at_us DESC LIMIT 1", ()
        ).await.map_err(db)?;
        rows.next().await.map_err(db)?.map(pause_row).transpose()
    }

    /// Call only AFTER current-state reconciliation has durably completed.
    /// Capacity recovery alone must not claim the missing history was recovered.
    /// The 80% low watermark prevents rapid pause/resume thrashing.
    pub async fn complete_capture_reconciliation(
        &self,
        runtime_id: &str,
        strategy_id: &str,
        live_session_id: &str,
        generation: u64,
    ) -> Result<bool, OutboxStoreError> {
        self.ensure_capture_capacity_schema().await?;
        let _operation = self.operation_gate.lock().await;
        let changed = self.execute_bounded_metadata(
            "UPDATE capture_pauses SET resumed_at_us=?1
             WHERE runtime_id=?2 AND strategy_id=?3 AND live_session_id=?4
               AND generation=?5 AND resumed_at_us IS NULL
               AND (SELECT retained_bytes FROM capture_capacity WHERE singleton=1) <= (limit_bytes/5)*4",
            params![now_epoch_micros(), runtime_id, strategy_id, live_session_id,
                i64::try_from(generation).map_err(db)?],
        ).await?;
        Ok(changed == 1)
    }
}

#[cfg(test)]
mod tests;
