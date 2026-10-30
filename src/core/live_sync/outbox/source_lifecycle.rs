//! One durable active-observation witness per leased source file. It records
//! uncertainty after an unclean owner exit, never an invented fill or ACK.
use super::source_suspensions::ObservationScope;
use super::*;
use crate::core::broker::command_journal::AnalyticsCaptureStatus;
use crate::core::live_sync::CaptureOwnerLease;

fn db(error: impl std::fmt::Display) -> OutboxStoreError {
    OutboxStoreError::Database(error.to_string())
}
fn invalid() -> OutboxStoreError {
    OutboxStoreError::InvalidRecord("invalid source observation lifecycle owner".into())
}
fn valid_scope(owner: &CaptureOwnerLease, path: &Path) -> bool {
    owner.matches_path(path)
        && [&owner.runtime, &owner.strategy, &owner.session, &owner.run]
            .iter()
            .all(|v| !v.trim().is_empty() && v.len() <= 128)
}
fn strategy(value: &str) -> &str {
    value.strip_prefix("strategy:").unwrap_or(value)
}

impl OutboxStore {
    /// Commit BEFORE a first source sample may open new capture. A lost commit
    /// acknowledgement is safe to retry using the exact activation token.
    /// Recovering the previous witness and replacing it is one transaction.
    pub(crate) async fn begin_source_observation(
        &self,
        owner: &CaptureOwnerLease,
        token: uuid::Uuid,
        report: &AnalyticsCaptureStatus,
        limit: u64,
    ) -> Result<(), OutboxStoreError> {
        report.validate().map_err(|_| invalid())?;
        if !valid_scope(owner, &self.path)
            || strategy(&report.strategy_id) != strategy(&owner.strategy)
            || limit == 0
            || limit > i64::MAX as u64
        {
            return Err(invalid());
        }
        owner
            .claim_source_observation(token)
            .map_err(|_| invalid())?;
        self.ensure_capture_capacity_schema().await?;
        let _operation = self.operation_gate.lock().await;
        let tx = turso::transaction::Transaction::new_unchecked(
            &self.connection,
            turso::transaction::TransactionBehavior::Immediate,
        )
        .await
        .map_err(db)?;
        let result=async {
   let mut rows=self.connection.query("SELECT version,token,runtime_id,strategy_id,live_session_id,run_id,journal_source_id,account_id,started_at_us,closed_at_us,limit_bytes FROM capture_source_observation WHERE singleton=1",()).await.map_err(db)?;
   if let Some(row)=rows.next().await.map_err(db)? {
    let version=row.get::<i64>(0).map_err(db)?;let previous=row.get::<String>(1).map_err(db)?;
    let runtime=row.get::<String>(2).map_err(db)?;let scope=row.get::<String>(3).map_err(db)?;let session=row.get::<String>(4).map_err(db)?;let run=row.get::<String>(5).map_err(db)?;
    let source=row.get::<String>(6).map_err(db)?;let account=row.get::<String>(7).map_err(db)?;
    let started=row.get::<i64>(8).map_err(db)?;let closed=row.get::<Option<i64>>(9).map_err(db)?;
    let previous_limit=u64::try_from(row.get::<i64>(10).map_err(db)?).map_err(|_|invalid())?;
    drop(rows);
    if version!=1||uuid::Uuid::parse_str(&previous).is_err()||started<=0||started>now_epoch_micros().saturating_add(1_000_000)
      ||[&runtime,&scope,&session,&run,&source,&account].iter().any(|v|v.trim().is_empty()||v.len()>128)
      ||scope!=owner.strategy||previous_limit==0 {return Err(invalid());}
    if previous==token.to_string() {
     if runtime!=owner.runtime||session!=owner.session||run!=owner.run||source!=report.journal_source_id||account!=report.account_id||previous_limit!=limit||closed.is_some(){return Err(invalid());}
     return Ok(());
    }
    if closed.is_none() {
     // Only the conservative lower bound is known: the old observer was armed
     // at this time but never proved clean final ownership. Preserve its actual
     // runtime/run provenance; do not attribute old uncertainty to a new run.
     self.record_source_suspension_locked(ObservationScope{runtime:&runtime,strategy:&scope,session:&session,run:&run},started,previous_limit).await?;
    }
   } else {drop(rows);}
   self.connection.execute("INSERT INTO capture_source_observation(singleton,version,token,runtime_id,strategy_id,live_session_id,run_id,journal_source_id,account_id,started_at_us,closed_at_us,limit_bytes)
    VALUES(1,1,?1,?2,?3,?4,?5,?6,?7,?8,NULL,?9) ON CONFLICT(singleton) DO UPDATE SET version=1,token=excluded.token,runtime_id=excluded.runtime_id,strategy_id=excluded.strategy_id,live_session_id=excluded.live_session_id,
     run_id=excluded.run_id,journal_source_id=excluded.journal_source_id,account_id=excluded.account_id,started_at_us=excluded.started_at_us,closed_at_us=NULL,limit_bytes=excluded.limit_bytes",
    params![token.to_string(),owner.runtime.clone(),owner.strategy.clone(),owner.session.clone(),owner.run.clone(),report.journal_source_id.clone(),report.account_id.clone(),report.observed_at_us,limit as i64]).await.map_err(db)?;
   Ok(())
  }.await;
        match result {
            Ok(()) => {
                self.commit_bounded_metadata(tx).await?;
                self.safety_notify.notify_waiters();
                Ok(())
            }
            Err(e) => {
                let _ = tx.rollback().await;
                Err(e)
            }
        }
    }

    /// Import only: the exclusive lease authorizes transferring persisted
    /// uncertainty, not adopting a new run or resuming capture. Both the gap
    /// and witness settlement commit before the source can be retired.
    pub async fn recover_exited_source_observation(
        &self,
        lease: &CaptureOwnerLease,
        strategy: &str,
        session: &str,
    ) -> Result<bool, OutboxStoreError> {
        if !lease.matches_path(&self.path)
            || [&lease.runtime, &lease.strategy, &lease.session, &lease.run]
                .iter()
                .any(|v| !v.is_empty())
        {
            return Err(invalid());
        }
        self.ensure_capture_capacity_schema().await?;
        let _operation = self.operation_gate.lock().await;
        let tx = turso::transaction::Transaction::new_unchecked(
            &self.connection,
            turso::transaction::TransactionBehavior::Immediate,
        )
        .await
        .map_err(db)?;
        let result=async {
            let mut rows=self.connection.query("SELECT version,token,runtime_id,strategy_id,live_session_id,run_id,journal_source_id,account_id,started_at_us,closed_at_us,limit_bytes FROM capture_source_observation WHERE singleton=1",()).await.map_err(db)?;
            let Some(row)=rows.next().await.map_err(db)? else{return Ok(false);};
            let version=row.get::<i64>(0).map_err(db)?;let token=row.get::<String>(1).map_err(db)?;
            let runtime=row.get::<String>(2).map_err(db)?;let scope=row.get::<String>(3).map_err(db)?;
            let source_session=row.get::<String>(4).map_err(db)?;let run=row.get::<String>(5).map_err(db)?;
            let source=row.get::<String>(6).map_err(db)?;let account=row.get::<String>(7).map_err(db)?;
            let started=row.get::<i64>(8).map_err(db)?;let closed=row.get::<Option<i64>>(9).map_err(db)?;
            let limit=u64::try_from(row.get::<i64>(10).map_err(db)?).map_err(|_|invalid())?;drop(rows);
            if version!=1||uuid::Uuid::parse_str(&token).is_err()||started<=0||started>now_epoch_micros().saturating_add(1_000_000)||limit==0
                ||[&runtime,&scope,&source_session,&run,&source,&account].iter().any(|v|v.trim().is_empty()||v.len()>128)
                ||scope!=strategy||source_session!=session{return Err(invalid());}
            if closed.is_some(){return Ok(false);}
            self.record_source_suspension_locked(ObservationScope{runtime:&runtime,strategy:&scope,session:&source_session,run:&run},started,limit).await?;
            let changed=self.connection.execute("UPDATE capture_source_observation SET closed_at_us=?1 WHERE singleton=1 AND token=?2 AND closed_at_us IS NULL",params![now_epoch_micros(),token]).await.map_err(db)?;
            if changed!=1{return Err(invalid());}Ok(true)
        }.await;
        match result {
            Ok(changed) => {
                self.commit_bounded_metadata(tx).await?;
                if changed {
                    self.safety_notify.notify_waiters();
                }
                Ok(changed)
            }
            Err(error) => {
                let _ = tx.rollback().await;
                Err(error)
            }
        }
    }

    /// Only the owning publisher with the exclusive, immutable final journal may
    /// call this. Closing a witness never clears an existing real capture pause.
    pub(crate) async fn close_source_observation(
        &self,
        owner: &CaptureOwnerLease,
        token: uuid::Uuid,
    ) -> Result<(), OutboxStoreError> {
        if !valid_scope(owner, &self.path) {
            return Err(invalid());
        }
        owner
            .claim_source_observation(token)
            .map_err(|_| invalid())?;
        self.ensure_capture_capacity_schema().await?;
        let _operation = self.operation_gate.lock().await;
        let changed=self.execute_bounded_metadata("UPDATE capture_source_observation SET closed_at_us=COALESCE(closed_at_us,?1) WHERE singleton=1 AND token=?2 AND runtime_id=?3 AND strategy_id=?4 AND live_session_id=?5 AND run_id=?6",
    params![now_epoch_micros(),token.to_string(),owner.runtime.clone(),owner.strategy.clone(),owner.session.clone(),owner.run.clone()]).await?;
        if changed != 1 {
            return Err(invalid());
        }
        Ok(())
    }
}
