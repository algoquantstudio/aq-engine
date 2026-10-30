//! Bounded restart adoption. An exclusive source lease, not a caller-supplied
//! runtime string alone, authorizes moving gap metadata to a new local owner.
use super::*;
use crate::core::live_sync::CaptureOwnerLease;

impl OutboxStore {
    /// Adopt at most one older open gap in a transaction. Repeated refreshes
    /// process legacy multi-runtime sources without a history scan or an
    /// unbounded restart transaction. Accepted payloads/cursors are untouched.
    pub(crate) async fn adopt_latest_capture_pause(
        &self,
        owner: &CaptureOwnerLease,
    ) -> Result<Option<CapturePauseRecord>, OutboxStoreError> {
        if !owner.matches_path(&self.path)
            || owner.run.is_empty()
            || owner.strategy.is_empty()
            || owner.session.is_empty()
            || owner.runtime.is_empty()
        {
            return Err(OutboxStoreError::InvalidRecord(
                "capture lease belongs to another source".into(),
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
            let mut rows = self.connection.query(
                "SELECT runtime_id,strategy_id,live_session_id,generation,started_at_us,last_blocked_at_us,blocked_attempts,resumed_at_us,
                 retained_bytes,requested_bytes,limit_bytes,origin_runtime_id,origin_run_id,last_blocked_run_id,owner_run_id,handoff_count,origin_runtime_unknown
                 FROM capture_pauses INDEXED BY capture_pauses_scope_latest WHERE strategy_id=?1 AND live_session_id=?2 AND resumed_at_us IS NULL
                 ORDER BY last_blocked_at_us DESC LIMIT 1", params![owner.strategy.clone(),owner.session.clone()],
            ).await.map_err(db)?;
            let Some(row) = rows.next().await.map_err(db)? else { return Ok(None); };
            let mut unknown_origin=row.get::<i64>(16).map_err(db)?!=0;
            let mut pause = pause_row(row)?;
            drop(rows);
            if pause.runtime_id == owner.runtime && pause.owner_run_id.as_deref() == Some(&owner.run) {
                return Ok(Some(pause));
            }
            let old_runtime = pause.runtime_id.clone();
            let old_generation = pause.generation;
            if old_runtime != owner.runtime {
                let mut current = self.connection.query(
                    "SELECT runtime_id,strategy_id,live_session_id,generation,started_at_us,last_blocked_at_us,blocked_attempts,resumed_at_us,
                     retained_bytes,requested_bytes,limit_bytes,origin_runtime_id,origin_run_id,last_blocked_run_id,owner_run_id,handoff_count,origin_runtime_unknown
                     FROM capture_pauses WHERE runtime_id=?1 AND strategy_id=?2 AND live_session_id=?3",
                    params![owner.runtime.clone(),owner.strategy.clone(),owner.session.clone()],
                ).await.map_err(db)?;
                if let Some(row) = current.next().await.map_err(db)? {
                    let current_unknown=row.get::<i64>(16).map_err(db)?!=0;
                    let current = pause_row(row)?;
                    pause.generation = pause.generation.max(current.generation);
                    if current.resumed_at_us.is_none() {
                        if current.started_at_us < pause.started_at_us {
                            pause.started_at_us = current.started_at_us;
                            unknown_origin=current_unknown;
                            pause.origin_runtime_id = if current_unknown {None} else {current.origin_runtime_id.or(Some(current.runtime_id))};
                            pause.origin_run_id = current.origin_run_id;
                        }
                        pause.blocked_attempts = pause.blocked_attempts.checked_add(current.blocked_attempts).ok_or_else(|| db("capture attempts overflow"))?;
                        pause.handoff_count = pause.handoff_count.checked_add(current.handoff_count).ok_or_else(|| db("capture handoff count overflow"))?;
                    }
                }
            }
            pause.generation = pause.generation.checked_add(1).filter(|v| *v <= i64::MAX as u64).ok_or_else(|| db("capture generation overflow"))?;
            pause.handoff_count = pause.handoff_count.checked_add(1).filter(|v| *v <= i64::MAX as u64).ok_or_else(|| db("capture handoff count overflow"))?;
            if pause.origin_runtime_id.is_none() && !unknown_origin { pause.origin_runtime_id = Some(old_runtime.clone()); }
            pause.runtime_id = owner.runtime.clone();
            pause.owner_run_id = Some(owner.run.clone());
            self.connection.execute(
                "INSERT INTO capture_pauses(runtime_id,strategy_id,live_session_id,generation,started_at_us,last_blocked_at_us,blocked_attempts,resumed_at_us,
                 retained_bytes,requested_bytes,limit_bytes,origin_runtime_id,origin_run_id,last_blocked_run_id,owner_run_id,handoff_count,origin_runtime_unknown)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,NULL,?8,?9,?10,?11,?12,?13,?14,?15,?16)
                 ON CONFLICT(runtime_id,strategy_id,live_session_id) DO UPDATE SET generation=excluded.generation,started_at_us=excluded.started_at_us,
                 last_blocked_at_us=excluded.last_blocked_at_us,blocked_attempts=excluded.blocked_attempts,resumed_at_us=NULL,
                 retained_bytes=excluded.retained_bytes,requested_bytes=excluded.requested_bytes,limit_bytes=excluded.limit_bytes,
                 origin_runtime_id=excluded.origin_runtime_id,origin_run_id=excluded.origin_run_id,last_blocked_run_id=excluded.last_blocked_run_id,
                 owner_run_id=excluded.owner_run_id,handoff_count=excluded.handoff_count,origin_runtime_unknown=excluded.origin_runtime_unknown",
                params![pause.runtime_id.clone(),pause.strategy_id.clone(),pause.live_session_id.clone(),pause.generation as i64,pause.started_at_us,pause.last_blocked_at_us,
                    i64::try_from(pause.blocked_attempts).map_err(db)?,pause.capacity.retained_bytes as i64,pause.capacity.requested_bytes as i64,pause.capacity.limit_bytes as i64,
                    pause.origin_runtime_id.clone(),pause.origin_run_id.clone(),pause.last_blocked_run_id.clone(),pause.owner_run_id.clone(),pause.handoff_count as i64,i64::from(unknown_origin)],
            ).await.map_err(db)?;
            if old_runtime != owner.runtime {
                let removed = self.connection.execute("DELETE FROM capture_pauses WHERE runtime_id=?1 AND strategy_id=?2 AND live_session_id=?3 AND generation=?4 AND resumed_at_us IS NULL",
                    params![old_runtime,owner.strategy.clone(),owner.session.clone(),old_generation as i64]).await.map_err(db)?;
                if removed != 1 { return Err(db("capture adoption source fence changed")); }
            }
            Ok(Some(pause))
        }.await;
        match result {
            Ok(pause) => {
                self.commit_bounded_metadata(transaction).await?;
                Ok(pause)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn event(revision: u64) -> CloudEvent {
        EventEnvelope::new(
            "runtime",
            "strategy",
            "session",
            format!("insight:{revision}"),
            revision,
            EventPriority::Critical,
            EventKind::InsightProjection,
            serde_json::json!({"revision":revision,"data":"x".repeat(1024)}),
        )
    }

    #[tokio::test]
    async fn adoption_merges_only_gap_metadata_and_import_leases_cannot_resume_it() {
        let directory =
            std::env::temp_dir().join(format!("merged-capture-adoption-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("publisher.db");
        let store = OutboxStore::open(&path).await.unwrap();
        let mut old = event(1);
        old.run_id = Some("original".into());
        assert!(
            store
                .capture_events_with_capacity(&[old.clone()], 1)
                .await
                .is_err()
        );
        let original = store.latest_capture_pause().await.unwrap().unwrap();
        let mut current = event(2);
        current.runtime_id = "next".into();
        current.run_id = Some("current".into());
        assert!(
            store
                .capture_events_with_capacity(&[current], 1)
                .await
                .is_err()
        );
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        assert!(store.capture_events_with_capacity(&[old], 1).await.is_err());
        let lease =
            CaptureOwnerLease::acquire(&path, "next", "strategy", "session", "current").unwrap();
        let merged = store
            .adopt_latest_capture_pause(&lease)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(merged.started_at_us, original.started_at_us);
        assert_eq!(merged.origin_runtime_id.as_deref(), Some("runtime"));
        assert_eq!(merged.origin_run_id.as_deref(), Some("original"));
        assert_eq!(merged.owner_run_id.as_deref(), Some("current"));
        assert_eq!(merged.blocked_attempts, 3);
        assert_eq!(merged.generation, 3);
        assert_eq!(store.pending_count().await.unwrap(), 0);
        let mut rows = store
            .connection
            .query(
                "SELECT count(*) FROM capture_pauses WHERE resumed_at_us IS NULL",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            1
        );
        drop(rows);
        drop(lease);
        let import = CaptureOwnerLease::acquire_for_import(&path).unwrap();
        assert!(store.adopt_latest_capture_pause(&import).await.is_err());
        assert!(
            store
                .has_unresolved_capture_pause_for_scope("strategy", "session")
                .await
                .unwrap()
        );
        drop(import);
        let mut plan=store.connection.query("EXPLAIN QUERY PLAN SELECT runtime_id FROM capture_pauses INDEXED BY capture_pauses_scope_latest WHERE strategy_id='strategy' AND live_session_id='session' AND resumed_at_us IS NULL ORDER BY last_blocked_at_us DESC LIMIT 1",()).await.unwrap();
        let mut details = String::new();
        while let Some(row) = plan.next().await.unwrap() {
            details.push_str(&row.get::<String>(3).unwrap());
        }
        assert!(details.contains("capture_pauses_scope_latest"));
        assert!(!details.contains("TEMP B-TREE"));
        drop(plan);
        drop(store);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn leased_restart_preserves_origin_and_accepted_payloads_and_fences_stale_completion() {
        let directory =
            std::env::temp_dir().join(format!("capture-adoption-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("publisher.db");
        let store = OutboxStore::open(&path).await.unwrap();
        let mut accepted = event(1);
        accepted.run_id = Some("old-run".into());
        store
            .handoff_accepted_events(&[accepted.clone()])
            .await
            .unwrap();
        let mut refused = event(2);
        refused.run_id = Some("old-run".into());
        assert!(matches!(
            store.capture_events_with_capacity(&[refused], 1).await,
            Err(OutboxStoreError::CapturePaused(_))
        ));
        let old = store.latest_capture_pause().await.unwrap().unwrap();
        let lease = CaptureOwnerLease::acquire(
            &path,
            "new-runtime",
            &old.strategy_id,
            &old.live_session_id,
            "new-run",
        )
        .unwrap();
        let next = store
            .adopt_latest_capture_pause(&lease)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            next.origin_runtime_id.as_deref(),
            Some(old.runtime_id.as_str())
        );
        assert_eq!(next.origin_run_id.as_deref(), Some("old-run"));
        assert_eq!(next.last_blocked_run_id.as_deref(), Some("old-run"));
        assert_eq!(next.owner_run_id.as_deref(), Some("new-run"));
        assert_eq!(next.started_at_us, old.started_at_us);
        assert_eq!(next.generation, old.generation + 1);
        assert_eq!(next.handoff_count, 1);
        assert_eq!(
            store.adopt_latest_capture_pause(&lease).await.unwrap(),
            Some(next.clone()),
            "lost adoption acknowledgement is idempotent"
        );
        assert!(
            !store
                .complete_capture_reconciliation(
                    &old.runtime_id,
                    &old.strategy_id,
                    &old.live_session_id,
                    old.generation
                )
                .await
                .unwrap()
        );
        let retained = store
            .pending_batch(EventPriority::Critical, 10, 1_000_000)
            .await
            .unwrap();
        assert_eq!(retained.len(), 1);
        assert!(same_accepted_payload(&retained[0].event, &accepted));
        let other = OutboxStore::open(directory.join("other.db")).await.unwrap();
        assert!(store.capture_retained_bytes().await.unwrap() > 0);
        assert!(other.adopt_latest_capture_pause(&lease).await.is_err());
        drop(other);
        drop(lease);
        drop(store);
        std::fs::remove_dir_all(directory).unwrap();
    }

}
