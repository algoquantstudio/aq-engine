use super::*;
use aqe_live_protocol::SafetyObservation;

impl OutboxStore {
    pub fn safety_change_notifier(&self) -> Arc<tokio::sync::Notify> {
        self.safety_notify.clone()
    }

    pub(super) async fn record_safety_in_transaction(
        &self,
        event: &CloudEvent,
    ) -> Result<(), OutboxStoreError> {
        let Some(o) = SafetyObservation::from_event(event) else {
            return Ok(());
        };
        self.record_safety_observation_locked(&o, true).await?;
        Ok(())
    }

    /// Exact indexed scope and a small page, not a history/payload scan.
    pub async fn pending_safety_observations(
        &self,
        runtime: &str,
        strategy: &str,
        session: &str,
        run: &str,
    ) -> Result<Vec<SafetyObservation>, OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let mut rows = self
            .connection
            .query(
                "SELECT observation FROM safety_observations INDEXED BY idx_safety_pending
             WHERE runtime_id=?1 AND strategy_id=?2 AND live_session_id=?3 AND run_id=?4
                AND delivered=0 AND next_attempt_at_us<=?5
             ORDER BY next_attempt_at_us LIMIT 32",
                params![
                    runtime.to_owned(),
                    strategy.to_owned(),
                    session.to_owned(),
                    run.to_owned(),
                    now_epoch_micros()
                ],
            )
            .await
            .map_err(|e| OutboxStoreError::Database(e.to_string()))?;
        let mut observations = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| OutboxStoreError::Database(e.to_string()))?
        {
            let wire: String = row
                .get(0)
                .map_err(|e| OutboxStoreError::Database(e.to_string()))?;
            observations.push(
                serde_json::from_str(&wire)
                    .map_err(|e| OutboxStoreError::InvalidRecord(e.to_string()))?,
            );
        }
        Ok(observations)
    }

    /// Compare the exact event, so a delayed response cannot hide a newer fact.
    /// This cursor is separate from outbox_events and critical_sequence.
    pub async fn settle_safety_observation(
        &self,
        o: &SafetyObservation,
        delivered: bool,
    ) -> Result<(), OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        self.execute_bounded_metadata(
            "UPDATE safety_observations SET delivered=MAX(delivered,?1),next_attempt_at_us=?2
             WHERE runtime_id=?3 AND strategy_id=?4 AND live_session_id=?5 AND run_id=?6 AND entity_key=?7 AND event_id=?8",
            params![i64::from(delivered),now_epoch_micros()+5_000_000,o.runtime_id.clone(),o.strategy_id.clone(),
                o.live_session_id.clone(),o.run_id.clone(),o.entity_key().to_owned(),o.event_id.to_string()])
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn event(revision: u64, state: &str) -> CloudEvent {
        let mut e = EventEnvelope::new(
            "runtime",
            "strategy",
            "session",
            "lifecycle",
            revision,
            EventPriority::Critical,
            EventKind::StrategyLifecycle,
            serde_json::json!({"state":state,"account":{"private":true}}),
        );
        e.run_id = Some("run".into());
        e.critical_sequence = Some(revision);
        e
    }
    #[tokio::test]
    async fn safety_compacts_atomically_survives_reopen_and_never_advances_evidence_cursor() {
        let path = std::env::temp_dir().join(format!("aqe-safety-{}.db", uuid::Uuid::new_v4()));
        let store = OutboxStore::open(&path).await.unwrap();
        let stopping = event(1, "stopping");
        store.insert_events(&[stopping.clone()]).await.unwrap();
        let first = store
            .pending_safety_observations("runtime", "strategy", "session", "run")
            .await
            .unwrap()
            .remove(0);
        let stopped = event(2, "stopped");
        store.insert_events(&[stopped.clone()]).await.unwrap();
        store.settle_safety_observation(&first, true).await.unwrap();
        let pending = store
            .pending_safety_observations("runtime", "strategy", "session", "run")
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].event_id, stopped.event_id);
        assert!(
            store
                .pending_safety_observations("other", "strategy", "session", "run")
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .pending_safety_observations("runtime", "strategy", "session", "new-run")
                .await
                .unwrap()
                .is_empty()
        );
        store.insert_events(&[event(3, "stopping")]).await.unwrap();
        assert_eq!(
            store
                .pending_safety_observations("runtime", "strategy", "session", "run")
                .await
                .unwrap()[0]
                .state,
            "stopped"
        );
        drop(store);
        let store = OutboxStore::open(&path).await.unwrap();
        let terminal = store
            .pending_safety_observations("runtime", "strategy", "session", "run")
            .await
            .unwrap()
            .remove(0);
        store
            .settle_safety_observation(&terminal, true)
            .await
            .unwrap();
        assert!(
            store
                .pending_safety_observations("runtime", "strategy", "session", "run")
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .pending_count_for_scope("strategy", "session")
                .await
                .unwrap(),
            3,
            "safety receipt never marks full evidence committed"
        );
        let mut rows = store
            .connection
            .query("SELECT count(*) FROM safety_observations", ())
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            1
        );
        drop(rows);
        // Operational receipt alone did not prune anything. Only after the
        // original's durable handoff can its compact recovery row be removed.
        store
            .mark_remote_committed(&[stopping.event_id, stopped.event_id], Some(2))
            .await
            .unwrap();
        store.prune_committed(0).await.unwrap();
        let mut rows = store
            .connection
            .query("SELECT count(*) FROM safety_observations", ())
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            0
        );
        drop(rows);
        drop(store);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn rejected_event_transaction_does_not_publish_safety_and_publisher_reservations_do() {
        let path = std::env::temp_dir().join(format!("aqe-safety-{}.db", uuid::Uuid::new_v4()));
        let store = OutboxStore::open(&path).await.unwrap();
        let first = event(1, "running");
        store.insert_events(&[first.clone()]).await.unwrap();
        let mut conflicting = first;
        conflicting.runtime_id = "other".into();
        assert!(
            store
                .insert_events(&[event(2, "stopping"), conflicting])
                .await
                .is_err()
        );
        assert!(
            store
                .pending_safety_observations("runtime", "strategy", "session", "run")
                .await
                .unwrap()
                .is_empty()
        );
        store
            .reserve_publisher_events(&[event(2, "stopped")], 1)
            .await
            .unwrap();
        assert_eq!(
            store
                .pending_safety_observations("runtime", "strategy", "session", "run")
                .await
                .unwrap()
                .len(),
            1
        );
        drop(store);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn exited_runtime_reports_terminal_even_while_full_evidence_is_pending() {
        let path =
            std::env::temp_dir().join(format!("aqe-safety-exit-{}.db", uuid::Uuid::new_v4()));
        let store = OutboxStore::open(&path).await.unwrap();
        store.insert_events(&[event(1, "stopping")]).await.unwrap();
        assert!(
            store
                .ensure_runtime_stopped_lifecycle_for_run(
                    "runtime",
                    "strategy",
                    "session",
                    Some("run")
                )
                .await
                .unwrap()
        );
        assert!(
            !store
                .ensure_runtime_stopped_lifecycle_for_run(
                    "runtime",
                    "strategy",
                    "session",
                    Some("run")
                )
                .await
                .unwrap()
        );
        let pending = store
            .pending_safety_observations("runtime", "strategy", "session", "run")
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].state, "stopped");
        assert_eq!(
            store
                .pending_count_for_scope("strategy", "session")
                .await
                .unwrap(),
            2
        );
        drop(store);
        let _ = std::fs::remove_file(path);
    }
}
