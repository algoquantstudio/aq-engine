//! Worker diagnostics must not inherit another runtime's retained failures.
use super::*;

impl OutboxStore {
    pub async fn health_for_runtime_scope(
        &self,
        runtime: &str,
        strategy: &str,
        session: &str,
        run: Option<&str>,
    ) -> Result<OutboxHealth, OutboxStoreError> {
        // Physical usage belongs to the shared host. Its short cache is still
        // shared, while connection/backlog fields below use exact indexed scope.
        let mut health = self.cached_health(Duration::from_secs(1)).await?;
        let _operation = self.operation_gate.lock().await;
        let db = |e| OutboxStoreError::Database(format!("{e}"));
        let mut rows = self.connection.query(
            "SELECT
             SUM(CASE WHEN state IN ('pending','in_flight') AND priority='critical' THEN 1 ELSE 0 END),
             SUM(CASE WHEN state IN ('pending','in_flight') AND priority='telemetry' THEN 1 ELSE 0 END),
             SUM(CASE WHEN state IN ('pending','in_flight') AND priority='audit' THEN 1 ELSE 0 END),
             SUM(CASE WHEN state IN ('pending','in_flight') AND priority='reconciliation' THEN 1 ELSE 0 END),
             MIN(CASE WHEN state IN ('pending','in_flight') THEN local_committed_at_us END),
             SUM(CASE WHEN state='dead_letter' THEN 1 ELSE 0 END),
             MAX(CASE WHEN state IN ('pending','in_flight') THEN local_committed_at_us END),
             MAX(CASE WHEN state='pending' THEN MAX(next_attempt_at_us-?4,0) ELSE 0 END),
             MAX(CASE WHEN state='pending' THEN last_error END)
             FROM outbox_events INDEXED BY idx_outbox_runtime_pending
             WHERE runtime_id=?1 AND strategy_id=?2 AND live_session_id=?3
               AND state IN ('pending','in_flight','dead_letter')",
            params![runtime.to_owned(),strategy.to_owned(),session.to_owned(),now_epoch_micros()],
        ).await.map_err(db)?;
        let row =
            rows.next().await.map_err(db)?.ok_or_else(|| {
                OutboxStoreError::InvalidRecord("missing scoped health row".into())
            })?;
        let count = |i| {
            row.get::<Option<i64>>(i)
                .map(|n| n.unwrap_or(0).max(0) as u64)
                .map_err(db)
        };
        health.pending_critical = count(0)?;
        health.pending_telemetry = count(1)?;
        health.pending_audit = count(2)?;
        health.pending_reconciliation = count(3)?;
        health.oldest_pending_age_ms = row
            .get::<Option<i64>>(4)
            .map_err(db)?
            .map(|at| now_epoch_micros().saturating_sub(at).max(0) as u64 / 1_000);
        health.dead_letters = count(5)?;
        // No pending row means no retained local-ACK timestamp for this scope;
        // do not substitute another worker's last acknowledgement.
        health.last_local_ack_at_us = row.get(6).map_err(db)?;
        health.current_retry_delay_ms = count(7)? / 1_000;
        health.last_error = row.get(8).map_err(db)?;
        drop(rows);
        let mut cursors=self.connection.query(
            "SELECT updated_at_us FROM sync_cursors WHERE runtime_id=?1 AND strategy_id=?2 AND live_session_id=?3",
            params![runtime.to_owned(),strategy.to_owned(),session.to_owned()],
        ).await.map_err(db)?;
        health.last_remote_commit_at_us = match cursors.next().await.map_err(db)? {
            Some(row) => row.get(0).map_err(db)?,
            None => None,
        };
        drop(cursors);
        let mut jobs=self.connection.query(
            "SELECT generation,run_id,status,candidate_count,classified_count,unresolved_count,current_cursor,retry_count,last_error
             FROM reconciliation_jobs WHERE strategy_id=?1 AND live_session_id=?2
               AND ((?3 IS NULL AND run_id IS NULL) OR run_id=?3) AND status!='superseded'
             ORDER BY updated_at_us DESC LIMIT 1",
            params![strategy.to_owned(),session.to_owned(),run.map(str::to_owned)],
        ).await.map_err(db)?;
        health.reconciliation = match jobs.next().await.map_err(db)? {
            Some(row) => Some(ReconciliationJobProgress {
                generation: row.get(0).map_err(db)?,
                run_id: row.get(1).map_err(db)?,
                status: row.get(2).map_err(db)?,
                candidate_count: row.get::<i64>(3).map_err(db)?.max(0) as u64,
                classified_count: row.get::<i64>(4).map_err(db)?.max(0) as u64,
                unresolved_count: row.get::<i64>(5).map_err(db)?.max(0) as u64,
                current_cursor: row.get(6).map_err(db)?,
                retry_count: row.get::<i64>(7).map_err(db)?.max(0) as u64,
                last_error: row.get(8).map_err(db)?,
            }),
            None => None,
        };
        Ok(health)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn worker_health_excludes_other_runtimes_strategies_and_sessions() {
        let dir = std::env::temp_dir().join(format!("aqe-scoped-health-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let store = OutboxStore::open(dir.join("outbox.db")).await.unwrap();
        let mut events = Vec::new();
        for (runtime, strategy, session) in [
            ("current", "strategy", "session"),
            ("old", "strategy", "session"),
            ("current", "other", "session"),
            ("current", "strategy", "other"),
        ] {
            let mut event = EventEnvelope::new(
                runtime,
                strategy,
                session,
                "heartbeat",
                1,
                EventPriority::Critical,
                EventKind::Heartbeat,
                serde_json::json!({}),
            );
            event.critical_sequence = Some(1);
            events.push(event);
        }
        store.insert_events(&events).await.unwrap();
        for event in &events[1..] {
            store
                .move_to_dead_letter(event.event_id, "historical failure")
                .await
                .unwrap();
        }
        let scoped = store
            .health_for_runtime_scope("current", "strategy", "session", None)
            .await
            .unwrap();
        assert_eq!(scoped.pending_critical, 1);
        assert_eq!(scoped.dead_letters, 0);
        assert_eq!(
            store.health().await.unwrap().dead_letters,
            3,
            "host diagnostics retain all evidence"
        );
        store
            .move_to_dead_letter(events[0].event_id, "current failure")
            .await
            .unwrap();
        let scoped = store
            .health_for_runtime_scope("current", "strategy", "session", None)
            .await
            .unwrap();
        assert_eq!(scoped.pending_critical, 0);
        assert_eq!(scoped.dead_letters, 1);
        assert!(scoped.last_local_ack_at_us.is_none());
        assert!(scoped.last_remote_commit_at_us.is_none());
        drop(store);
        let _ = std::fs::remove_dir_all(dir);
    }
}
