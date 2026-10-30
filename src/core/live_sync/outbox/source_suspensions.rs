//! Recorded observation suspension or an unclosed quality witness. Neither is
//! a claim that a specific fill was lost or that its loss time is known.
//! One idempotency marker per scope; no economic event or cursor is allocated.
use super::*;
use crate::core::live_sync::CaptureOwnerLease;
fn db(error: impl std::fmt::Display) -> OutboxStoreError {
    OutboxStoreError::Database(error.to_string())
}

impl OutboxStore {
    pub(crate) async fn record_source_observation_suspension(
        &self,
        owner: &CaptureOwnerLease,
        started_at_us: i64,
        limit: u64,
    ) -> Result<(), OutboxStoreError> {
        if !owner.matches_path(&self.path)
            || started_at_us <= 0
            || started_at_us > now_epoch_micros().saturating_add(1_000_000)
            || limit == 0
            || limit > i64::MAX as u64
            || [&owner.runtime, &owner.strategy, &owner.session, &owner.run]
                .iter()
                .any(|id| id.trim().is_empty() || id.len() > 128)
        {
            return Err(OutboxStoreError::InvalidRecord(
                "invalid source observation suspension scope".into(),
            ));
        }
        self.ensure_capture_capacity_schema().await?;
        self.adopt_latest_capture_pause(owner).await?;
        let _operation = self.operation_gate.lock().await;
        let tx = turso::transaction::Transaction::new_unchecked(
            &self.connection,
            turso::transaction::TransactionBehavior::Immediate,
        )
        .await
        .map_err(db)?;
        let result = async {
            self.record_source_suspension_locked(
                ObservationScope::from(owner),
                started_at_us,
                limit,
            )
            .await
        }
        .await;
        match result {
            Ok(()) => {
                self.commit_bounded_metadata(tx).await?;
                self.safety_notify.notify_waiters();
                Ok(())
            }
            Err(error) => {
                tx.rollback().await.map_err(db)?;
                Err(error)
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct ObservationScope<'a> {
    pub runtime: &'a str,
    pub strategy: &'a str,
    pub session: &'a str,
    pub run: &'a str,
}
impl<'a> From<&'a CaptureOwnerLease> for ObservationScope<'a> {
    fn from(owner: &'a CaptureOwnerLease) -> Self {
        Self {
            runtime: &owner.runtime,
            strategy: &owner.strategy,
            session: &owner.session,
            run: &owner.run,
        }
    }
}
impl OutboxStore {
    /// Caller owns the immediate transaction and validated exclusive source lease.
    pub(super) async fn record_source_suspension_locked(
        &self,
        scope: ObservationScope<'_>,
        started_at_us: i64,
        limit: u64,
    ) -> Result<(), OutboxStoreError> {
        let id = uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            &serde_json::to_vec(&(
                1u16,
                &scope.runtime,
                &scope.strategy,
                &scope.session,
                &scope.run,
                started_at_us,
            ))
            .map_err(db)?,
        )
        .to_string();
        let mut rows=self.connection.query("SELECT source_observation_pause_id,resumed_at_us FROM capture_pauses WHERE runtime_id=?1 AND strategy_id=?2 AND live_session_id=?3",
                params![scope.runtime.to_string(),scope.strategy.to_string(),scope.session.to_string()]).await.map_err(db)?;
        if let Some(row) = rows.next().await.map_err(db)? {
            if row.get::<Option<String>>(0).map_err(db)?.as_deref() == Some(&id)
                && row.get::<Option<i64>>(1).map_err(db)?.is_none()
            {
                return Ok(());
            }
        }
        drop(rows);
        let retained = self.capture_retained_bytes_in_transaction().await?;
        let changed=self.connection.execute("INSERT INTO capture_pauses(runtime_id,strategy_id,live_session_id,generation,started_at_us,last_blocked_at_us,blocked_attempts,resumed_at_us,
                retained_bytes,requested_bytes,limit_bytes,origin_runtime_id,origin_run_id,last_blocked_run_id,owner_run_id,handoff_count,origin_runtime_unknown,source_observation_pause_id)
                VALUES(?1,?2,?3,1,?4,?4,1,NULL,?5,0,?6,?1,?7,?7,?7,0,0,?8)
                ON CONFLICT(runtime_id,strategy_id,live_session_id) DO UPDATE SET generation=capture_pauses.generation+1,
                origin_runtime_id=CASE WHEN capture_pauses.resumed_at_us IS NOT NULL OR excluded.started_at_us<capture_pauses.started_at_us THEN excluded.origin_runtime_id ELSE capture_pauses.origin_runtime_id END,
                origin_runtime_unknown=CASE WHEN capture_pauses.resumed_at_us IS NOT NULL OR excluded.started_at_us<capture_pauses.started_at_us THEN 0 ELSE capture_pauses.origin_runtime_unknown END,
                origin_run_id=CASE WHEN capture_pauses.resumed_at_us IS NOT NULL OR excluded.started_at_us<capture_pauses.started_at_us THEN excluded.origin_run_id ELSE capture_pauses.origin_run_id END,
                last_blocked_run_id=CASE WHEN capture_pauses.resumed_at_us IS NOT NULL OR excluded.last_blocked_at_us>=capture_pauses.last_blocked_at_us THEN excluded.last_blocked_run_id ELSE capture_pauses.last_blocked_run_id END,
                started_at_us=CASE WHEN capture_pauses.resumed_at_us IS NULL THEN MIN(capture_pauses.started_at_us,excluded.started_at_us) ELSE excluded.started_at_us END,
                last_blocked_at_us=CASE WHEN capture_pauses.resumed_at_us IS NULL THEN MAX(capture_pauses.last_blocked_at_us,excluded.last_blocked_at_us) ELSE excluded.last_blocked_at_us END,
                blocked_attempts=capture_pauses.blocked_attempts+1,resumed_at_us=NULL,retained_bytes=excluded.retained_bytes,requested_bytes=0,limit_bytes=excluded.limit_bytes,
                owner_run_id=excluded.owner_run_id,handoff_count=CASE WHEN capture_pauses.resumed_at_us IS NULL THEN capture_pauses.handoff_count ELSE 0 END,
                source_observation_pause_id=excluded.source_observation_pause_id
                WHERE capture_pauses.generation<9223372036854775807 AND capture_pauses.blocked_attempts<9223372036854775807",
                params![scope.runtime.to_string(),scope.strategy.to_string(),scope.session.to_string(),started_at_us,retained as i64,limit as i64,scope.run.to_string(),id]).await.map_err(db)?;
        if changed != 1 {
            return Err(db("source observation suspension fence exhausted"));
        }
        Ok(())
    }
}
