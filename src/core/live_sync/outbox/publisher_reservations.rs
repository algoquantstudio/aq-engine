//! Publisher-owned identity reservations. Never expire these independently of
//! their runtime cursor: a late duplicate must not allocate a new sequence after
//! the acknowledged payload has been pruned. Full payloads remain in the outbox
//! only until the downstream durable receiver acknowledges them.

use super::*;
use sha2::{Digest, Sha256};
use std::fmt;

fn db(error: impl fmt::Display) -> OutboxStoreError {
    OutboxStoreError::Database(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(revision: u64) -> CloudEvent {
        EventEnvelope::new(
            "runtime",
            "strategy",
            "session",
            "insight",
            revision,
            EventPriority::Critical,
            EventKind::InsightProjection,
            serde_json::json!({"revision": revision}),
        )
    }

    async fn cursor(store: &OutboxStore) -> u64 {
        store
            .critical_sequence_cursor_for_runtime_scope("runtime", "strategy", "session")
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn publisher_reservations_survive_payload_pruning_and_reopen() {
        let path = std::env::temp_dir().join(format!(
            "publisher-reservations-{}.db",
            uuid::Uuid::new_v4()
        ));
        let store = OutboxStore::open(&path).await.unwrap();
        let first = event(1);
        let second = event(2);
        let result = store
            .reserve_publisher_events(&[first.clone(), first.clone(), second.clone()], 0)
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].critical_sequence, Some(1));
        assert_eq!(result[1].critical_sequence, Some(2));
        store
            .mark_remote_committed(&[first.event_id, second.event_id], Some(2))
            .await
            .unwrap();
        assert_eq!(store.prune_committed(0).await.unwrap(), 2);
        drop(store);
        let store = OutboxStore::open(&path).await.unwrap();
        let mut retry = first.clone();
        retry.latency.runtime_received_at_us = Some(now_epoch_micros());
        assert!(
            store
                .reserve_publisher_events(&[retry, second], 0)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(cursor(&store).await, 2);
        assert_eq!(
            store
                .reserve_publisher_events(&[event(3)], 0)
                .await
                .unwrap()[0]
                .critical_sequence,
            Some(3)
        );
    }

    #[tokio::test]
    async fn publisher_rejects_unreserved_payloads_without_adopting_or_deleting_them() {
        let path = std::env::temp_dir().join(format!(
            "publisher-current-format-{}.db",
            uuid::Uuid::new_v4()
        ));
        let store = OutboxStore::open(&path).await.unwrap();
        let mut retained = event(1);
        retained.critical_sequence = Some(12);
        store.insert_events(&[retained.clone()]).await.unwrap();
        let saved_cursor = cursor(&store).await;
        assert_eq!(saved_cursor, 12);
        let mut retry = retained.clone();
        retry.critical_sequence = None;
        assert!(matches!(
            store.reserve_publisher_events(&[retry], 0).await,
            Err(OutboxStoreError::InvalidRecord(message))
                if message.contains("unreserved publisher payload")
        ));
        assert_eq!(store.pending_count().await.unwrap(), 1);
        assert_eq!(cursor(&store).await, saved_cursor);
        let mut gap = event(2);
        gap.critical_sequence = Some(14);
        assert!(matches!(
            store.reserve_publisher_events(&[gap], 0).await,
            Err(OutboxStoreError::InvalidRecord(_))
        ));
        assert_eq!(store.pending_count().await.unwrap(), 1);
        assert_eq!(cursor(&store).await, saved_cursor);
        drop(store);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn publisher_conflict_rolls_back_payload_identity_and_cursor() {
        let path =
            std::env::temp_dir().join(format!("publisher-conflict-{}.db", uuid::Uuid::new_v4()));
        let store = OutboxStore::open(&path).await.unwrap();
        let first = event(1);
        store
            .reserve_publisher_events(&[first.clone()], 0)
            .await
            .unwrap();
        let second = event(2);
        let mut conflict = first.clone();
        conflict.payload = serde_json::json!({"state": "not the accepted payload"});
        assert!(matches!(
            store
                .reserve_publisher_events(&[second.clone(), conflict.clone()], 0)
                .await,
            Err(OutboxStoreError::InvalidRecord(_))
        ));
        assert_eq!(cursor(&store).await, 1);
        let fresh = store.reserve_publisher_events(&[second], 0).await.unwrap();
        assert_eq!(fresh[0].critical_sequence, Some(2));
        // A conflicting duplicate gets a separate deterministic DLQ entry;
        // it cannot replace the accepted event or reserve another sequence.
        store
            .reject_publisher_event(&conflict, "identity conflict")
            .await
            .unwrap();
        conflict.latency.runtime_received_at_us = Some(now_epoch_micros());
        store
            .reject_publisher_event(&conflict, "identity conflict")
            .await
            .unwrap();
        assert_eq!(store.health().await.unwrap().dead_letters, 1);
        assert!(
            store
                .reserve_publisher_events(&[first], 0)
                .await
                .unwrap()
                .is_empty()
        );
    }

}

fn fingerprint(event: &CloudEvent) -> Result<Vec<u8>, OutboxStoreError> {
    let mut value =
        serde_json::to_value(event).map_err(|e| OutboxStoreError::InvalidRecord(e.to_string()))?;
    let object = value.as_object_mut().expect("event envelope is an object");
    object.remove("latency");
    // Assigned by this transaction, not part of the caller's immutable identity.
    object.remove("criticalSequence");
    // FlatBuffers preserves epoch microseconds, whereas the in-memory chrono
    // value can include nanoseconds. Reopened payloads must hash identically.
    object.insert(
        "createdAt".into(),
        serde_json::json!(event.created_at.timestamp_micros()),
    );
    value.sort_all_objects();
    Ok(Sha256::digest(serde_json::to_vec(&value).map_err(db)?).to_vec())
}

impl OutboxStore {
    /// A rejected retry must not overwrite the already accepted event or its
    /// outbox row. Give the diagnostic its own deterministic DLQ identity.
    pub(crate) async fn reject_publisher_event(
        &self,
        event: &CloudEvent,
        reason: &str,
    ) -> Result<(), OutboxStoreError> {
        let payload = FlatbufferEventCodec::encode(event)?;
        let mut content = fingerprint(event)?;
        content.extend_from_slice(&event.critical_sequence.unwrap_or(0).to_be_bytes());
        let digest = Sha256::digest(&content);
        let id = uuid::Uuid::new_v5(&event.event_id, &digest);
        let _operation = self.operation_gate.lock().await;
        let transaction=turso::transaction::Transaction::new_unchecked(
            &self.connection,turso::transaction::TransactionBehavior::Immediate).await.map_err(db)?;
        let result=async {
        let inserted=self.connection.execute(
            "INSERT OR IGNORE INTO dead_letters
             (event_id, strategy_id, reason, wire_payload, failed_at_us) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![id.to_string(), event.strategy_id.clone(),
                format!("publisher event {} rejected: {reason}", event.event_id), payload, now_epoch_micros()],
        ).await.map_err(db)?;
        if inserted>0 {self.check_physical_capture_in_transaction().await?;}
        Ok(())
        }.await;
        match result {
            Ok(())=>transaction.commit().await.map_err(db),
            Err(OutboxStoreError::CapturePaused(capacity))=>{
                transaction.rollback().await.map_err(db)?;
                let capacity=self.normalize_physical_refusal(capacity).await?;
                self.record_capture_pause_locked(std::slice::from_ref(event),capacity).await?;
                self.safety_notify.notify_waiters();
                Err(OutboxStoreError::CapturePaused(capacity))
            }
            Err(error)=>{let _=transaction.rollback().await;Err(error)}
        }
    }

    /// Atomically deduplicate and assign contiguous critical sequences, retaining
    /// every newly reserved payload before any publication. Runs in the publisher
    /// worker, never in the strategy's synchronous enqueue method.
    ///
    /// Returns fresh critical events only. Duplicates retain their original
    /// durable reservation even after payload cleanup. Any invalid member rolls
    /// back the entire batch, including its cursor advances.
    pub(crate) async fn reserve_publisher_events(
        &self,
        events: &[CloudEvent],
        initial_sequence: u64,
    ) -> Result<Vec<CloudEvent>, OutboxStoreError> {
        self.reserve_publisher_events_with_optional_capacity(events, initial_sequence, None)
            .await
    }

    /// New producer capture only. A capacity refusal rolls back payloads,
    /// identities AND sequence allocations; it is not an accepted event/DLQ.
    pub(crate) async fn reserve_publisher_events_with_capacity(
        &self,
        events: &[CloudEvent],
        initial_sequence: u64,
        limit_bytes: u64,
    ) -> Result<Vec<CloudEvent>, OutboxStoreError> {
        self.ensure_capture_capacity_schema().await?;
        self.reserve_publisher_events_with_optional_capacity(
            events,
            initial_sequence,
            Some(limit_bytes),
        )
        .await
    }

    async fn reserve_publisher_events_with_optional_capacity(
        &self,
        events: &[CloudEvent],
        initial_sequence: u64,
        capacity: Option<u64>,
    ) -> Result<Vec<CloudEvent>, OutboxStoreError> {
        let capacity=match self.physical_budget {
            Some(policy)=>Some(capacity.unwrap_or(policy.payload_limit_bytes).min(policy.payload_limit_bytes)),
            None=>capacity,
        };
        if events.is_empty() {
            return Ok(Vec::new());
        }
        if capacity.is_some() && events.len() > 256 {
            return Err(OutboxStoreError::InvalidRecord(
                "capture batch exceeds 256 events".into(),
            ));
        }
        let _operation = self.operation_gate.lock().await;
        let transaction=turso::transaction::Transaction::new_unchecked(
            &self.connection,turso::transaction::TransactionBehavior::Immediate).await.map_err(db)?;
        let result = self
            .reserve_publisher_events_in_transaction(events, initial_sequence, capacity)
            .await;
        match result {
            Ok(fresh) => {
                transaction.commit().await.map_err(db)?;
                if !fresh.is_empty() {
                    if fresh.iter().any(|event| {
                        aqe_live_protocol::SafetyObservation::from_event(event).is_some()
                    }) {
                        self.safety_notify.notify_waiters();
                    }
                    self.priority_change_notifier(EventPriority::Critical)
                        .notify_waiters();
                }
                Ok(fresh)
            }
            Err(OutboxStoreError::CapturePaused(capacity)) => {
                transaction.rollback().await.map_err(db)?;
                let capacity=self.normalize_physical_refusal(capacity).await?;
                // A metadata-only refusal must not fabricate missing history.
                if capacity.requested_bytes>0 || capacity.physical.is_none() {
                    self.record_capture_pause_locked(events, capacity).await?;
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

    async fn reserve_publisher_events_in_transaction(
        &self,
        events: &[CloudEvent],
        initial_sequence: u64,
        capacity: Option<u64>,
    ) -> Result<Vec<CloudEvent>, OutboxStoreError> {
        let mut fresh = Vec::with_capacity(events.len());
        for event in events {
            if event.priority != EventPriority::Critical || event.kind == EventKind::Log {
                return Err(OutboxStoreError::InvalidRecord(
                    "publisher sequence reservations require critical lifecycle events".into(),
                ));
            }
            let digest = fingerprint(event)?;
            let mut rows = self
                .connection
                .query(
                    "SELECT critical_sequence, fingerprint FROM publisher_event_reservations
                 WHERE event_id = ?1",
                    params![event.event_id.to_string()],
                )
                .await
                .map_err(db)?;
            if let Some(row) = rows.next().await.map_err(db)? {
                let sequence: i64 = row.get(0).map_err(db)?;
                let existing: Vec<u8> = row.get(1).map_err(db)?;
                if existing != digest
                    || event
                        .critical_sequence
                        .is_some_and(|s| s != sequence as u64)
                {
                    return Err(OutboxStoreError::InvalidRecord(format!(
                        "publisher event {} conflicts with its durable reservation",
                        event.event_id
                    )));
                }
                continue;
            }
            drop(rows);

            // Current publishers reserve identity and sequence before capture.
            // An unreserved retained payload is not silently adopted or relabeled.
            let mut saved=self.connection.query("SELECT 1 FROM outbox_events WHERE event_id=?1",params![event.event_id.to_string()]).await.map_err(db)?;
            if saved.next().await.map_err(db)?.is_some() {
                return Err(OutboxStoreError::InvalidRecord("unsupported unreserved publisher payload; source retained".into()));
            }
            drop(saved);
            let mut rows = self.connection.query(
                "SELECT MAX(value) FROM (
                    SELECT COALESCE(MAX(critical_through), 0) AS value FROM publisher_sequence_cursors
                      WHERE runtime_id = ?1 AND strategy_id = ?2 AND live_session_id = ?3
                    UNION ALL
                    SELECT COALESCE(MAX(critical_sequence), 0) AS value FROM outbox_events
                      WHERE runtime_id = ?1 AND strategy_id = ?2 AND live_session_id = ?3
                 )",
                params![event.runtime_id.clone(), event.strategy_id.clone(), event.live_session_id.clone()],
            ).await.map_err(db)?;
            let cursor: i64 = rows
                .next()
                .await
                .map_err(db)?
                .ok_or_else(|| db("publisher cursor query returned no row"))?
                .get(0)
                .map_err(db)?;
            drop(rows);
            let initial_sequence = i64::try_from(initial_sequence).map_err(|_| {
                OutboxStoreError::InvalidRecord("publisher sequence exhausted".into())
            })?;
            let next = cursor
                .max(initial_sequence)
                .checked_add(1)
                .filter(|s| *s > 0)
                .ok_or_else(|| {
                    OutboxStoreError::InvalidRecord("publisher sequence exhausted".into())
                })?;
            let sequence = next;
            if event
                .critical_sequence
                .is_some_and(|s| s != sequence as u64)
            {
                return Err(OutboxStoreError::InvalidRecord(format!(
                    "publisher event {} would introduce a sequence gap or reuse",
                    event.event_id
                )));
            }
            self.connection.execute(
                "INSERT INTO publisher_event_reservations
                    (event_id, runtime_id, strategy_id, live_session_id, critical_sequence, fingerprint)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![event.event_id.to_string(), event.runtime_id.clone(), event.strategy_id.clone(),
                    event.live_session_id.clone(), sequence, digest],
            ).await.map_err(db)?;
            self.connection.execute(
                "INSERT INTO publisher_sequence_cursors
                    (runtime_id, strategy_id, live_session_id, critical_through, updated_at_us)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(runtime_id, strategy_id, live_session_id) DO UPDATE SET
                    critical_through = MAX(publisher_sequence_cursors.critical_through, excluded.critical_through),
                    updated_at_us = excluded.updated_at_us",
                params![event.runtime_id.clone(), event.strategy_id.clone(), event.live_session_id.clone(),
                    sequence, now_epoch_micros()],
            ).await.map_err(db)?;
            let mut event = event.clone();
            event.critical_sequence = Some(sequence as u64);
            fresh.push(event);
        }
        if let Some(limit) = capacity {
            self.check_capture_capacity_in_transaction(&fresh, limit)
                .await?;
        }
        self.insert_events_in_transaction(&fresh).await?;
        Ok(fresh)
    }
}
