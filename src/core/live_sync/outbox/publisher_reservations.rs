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

    #[tokio::test]
    async fn publisher_reservations_upgrade_legacy_payloads_and_reject_sequence_gaps() {
        let path =
            std::env::temp_dir().join(format!("publisher-legacy-{}.db", uuid::Uuid::new_v4()));
        let store = OutboxStore::open(&path).await.unwrap();
        let mut legacy = event(1);
        legacy.critical_sequence = Some(12);
        store.insert_events(&[legacy.clone()]).await.unwrap();
        let mut retry = legacy.clone();
        retry.critical_sequence = None;
        assert!(
            store
                .reserve_publisher_events(&[retry.clone()], 0)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(cursor(&store).await, 12);
        store
            .mark_remote_committed(&[legacy.event_id], Some(12))
            .await
            .unwrap();
        store.prune_committed(0).await.unwrap();
        assert!(
            store
                .reserve_publisher_events(&[retry], 0)
                .await
                .unwrap()
                .is_empty()
        );
        let mut gap = event(2);
        gap.critical_sequence = Some(14);
        assert!(matches!(
            store.reserve_publisher_events(&[gap.clone()], 0).await,
            Err(OutboxStoreError::InvalidRecord(_))
        ));
        gap.critical_sequence = None;
        assert_eq!(
            store.reserve_publisher_events(&[gap], 0).await.unwrap()[0].critical_sequence,
            Some(13)
        );
        let mut other = event(3);
        other.runtime_id = "different-runtime".into();
        assert_eq!(
            store.reserve_publisher_events(&[other], 0).await.unwrap()[0].critical_sequence,
            Some(1)
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
        self.connection.execute(
            "INSERT OR IGNORE INTO dead_letters
             (event_id, strategy_id, reason, wire_payload, failed_at_us) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![id.to_string(), event.strategy_id.clone(),
                format!("publisher event {} rejected: {reason}", event.event_id), payload, now_epoch_micros()],
        ).await.map_err(db)?;
        Ok(())
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
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let _operation = self.operation_gate.lock().await;
        self.connection
            .execute_batch("BEGIN IMMEDIATE")
            .await
            .map_err(db)?;
        let result = self
            .reserve_publisher_events_in_transaction(events, initial_sequence)
            .await;
        match result {
            Ok(fresh) => {
                if let Err(error) = self.connection.execute_batch("COMMIT").await {
                    let _ = self.connection.execute_batch("ROLLBACK").await;
                    return Err(db(error));
                }
                if !fresh.is_empty() {
                    self.priority_change_notifier(EventPriority::Critical)
                        .notify_waiters();
                }
                Ok(fresh)
            }
            Err(error) => {
                let _ = self.connection.execute_batch("ROLLBACK").await;
                Err(error)
            }
        }
    }

    async fn reserve_publisher_events_in_transaction(
        &self,
        events: &[CloudEvent],
        initial_sequence: u64,
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

            // Upgrade lazily from a legacy pending/committed payload. Existing
            // payloads already have a sequence and must not receive a new one.
            let mut rows = self
                .connection
                .query(
                    "SELECT wire_payload FROM outbox_events WHERE event_id = ?1",
                    params![event.event_id.to_string()],
                )
                .await
                .map_err(db)?;
            let legacy = if let Some(row) = rows.next().await.map_err(db)? {
                let payload: Vec<u8> = row.get(0).map_err(db)?;
                let existing = FlatbufferEventCodec::decode(&payload)?;
                if fingerprint(&existing)? != digest {
                    return Err(OutboxStoreError::InvalidRecord(format!(
                        "publisher event {} conflicts with its existing outbox payload",
                        event.event_id
                    )));
                }
                if existing.critical_sequence.is_none_or(|s| s == 0) {
                    return Err(OutboxStoreError::InvalidRecord(
                        "legacy publisher payload has no valid critical sequence".into(),
                    ));
                }
                Some(existing)
            } else {
                None
            };
            drop(rows);

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
            let sequence = match legacy.as_ref().and_then(|e| e.critical_sequence) {
                Some(sequence) => i64::try_from(sequence).map_err(|_| {
                    OutboxStoreError::InvalidRecord("invalid legacy sequence".into())
                })?,
                None => next,
            };
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
            if legacy.is_none() {
                let mut event = event.clone();
                event.critical_sequence = Some(sequence as u64);
                fresh.push(event);
            }
        }
        self.insert_events_in_transaction(&fresh).await?;
        Ok(fresh)
    }
}
