//! Durable local journal-quality handoff into the existing source pause lane.
//! This records no economic event, sequence cursor or cloud commit receipt.
use super::*;
use crate::core::{
    broker::command_journal::{AnalyticsCaptureHandoffReceipt, AnalyticsCapturePause},
    live_sync::CaptureOwnerLease,
};

const MAX_JOURNAL_GAP_SOURCES: i64 = 256;
fn db(error: impl std::fmt::Display) -> OutboxStoreError {
    OutboxStoreError::Database(error.to_string())
}
fn strategy(id: &str) -> &str {
    id.trim().strip_prefix("strategy:").unwrap_or(id.trim())
}
struct JournalGapOwner {runtime:String,strategy:String,session:String,run:String}

impl OutboxStore {
    /// None is capacity backpressure: retain the source marker and retry later.
    /// The exact source-file lease is required even for metadata-only work.
    /// A receipt transfers diagnostic ownership only, never capture resume or
    /// an assertion that missing fills/history have been reconstructed.
    pub async fn handoff_analytics_capture_pause(
        &self,
        owner: &CaptureOwnerLease,
        source: &AnalyticsCapturePause,
        payload_limit: u64,
    ) -> Result<Option<AnalyticsCaptureHandoffReceipt>, OutboxStoreError> {
        source
            .validate()
            .map_err(|e| OutboxStoreError::InvalidRecord(e.into()))?;
        if !owner.matches_path(&self.path)
            || strategy(&source.strategy_id).is_empty()
            || strategy(&source.strategy_id) != strategy(&owner.strategy)
            || [&owner.runtime, &owner.strategy, &owner.session, &owner.run]
                .iter()
                .any(|id| id.trim().is_empty() || id.len() > 128)
            || payload_limit == 0
            || payload_limit > i64::MAX as u64
        {
            return Err(OutboxStoreError::InvalidRecord(
                "journal gap handoff is outside its leased source scope".into(),
            ));
        }
        self.ensure_capture_capacity_schema().await?;
        // Reuse the existing exact-file owner adoption before merging another
        // source fact. This may move an old pause, never rewrite its evidence.
        self.adopt_latest_capture_pause(owner).await?;
        self.handoff_journal_pause(JournalGapOwner{runtime:owner.runtime.clone(),strategy:owner.strategy.clone(),session:owner.session.clone(),run:owner.run.clone()},source,payload_limit).await
    }

    /// An exited importer may transfer the exact declared journal pause using
    /// its original delivery scope. It cannot adopt a new run or resume capture.
    pub async fn handoff_exited_journal_pause(
        &self,owner:&CaptureOwnerLease,link:&crate::core::live_sync::JournalRecoveryLink,source:&AnalyticsCapturePause,
    )->Result<Option<AnalyticsCaptureHandoffReceipt>,OutboxStoreError>{
        link.validate().map_err(|_|db("invalid journal recovery link"))?;
        source.validate().map_err(|_|db("invalid exited journal pause"))?;
        if !owner.matches_path(&self.path)||[&owner.runtime,&owner.strategy,&owner.session,&owner.run].iter().any(|v|!v.is_empty())
            ||source.journal_source_id!=link.journal_source_id||source.strategy_id!=link.journal_strategy_id||source.account_id!=link.account_id {
            return Err(OutboxStoreError::InvalidRecord("exited journal handoff is outside its declared scope".into()));
        }
        self.ensure_capture_capacity_schema().await?;
        self.handoff_journal_pause(JournalGapOwner{runtime:link.runtime_id.clone(),strategy:link.strategy_id.clone(),session:link.live_session_id.clone(),run:link.run_id.clone()},source,link.payload_limit_bytes).await
    }

    async fn handoff_journal_pause(&self,owner:JournalGapOwner,source:&AnalyticsCapturePause,payload_limit:u64)
        ->Result<Option<AnalyticsCaptureHandoffReceipt>,OutboxStoreError>{
        let payload_limit = self.physical_budget.map_or(payload_limit, |policy| {
            payload_limit.min(policy.payload_limit_bytes)
        });
        let _operation = self.operation_gate.lock().await;
        let tx = turso::transaction::Transaction::new_unchecked(
            &self.connection,
            turso::transaction::TransactionBehavior::Immediate,
        )
        .await
        .map_err(db)?;
        let result=async {
            let mut rows=self.connection.query("SELECT source_generation,source_epoch,strategy_id,account_id,pause_id,receipt FROM capture_journal_gap_handoffs WHERE journal_source_id=?1",
                params![source.journal_source_id.clone()]).await.map_err(db)?;
            let previous=rows.next().await.map_err(db)?;drop(rows);
            if let Some(row)=previous.as_ref() {
                let generation=row.get::<i64>(0).map_err(db)?;
                let epoch=row.get::<i64>(1).map_err(db)?;
                if row.get::<String>(2).map_err(db)?!=source.strategy_id || row.get::<String>(3).map_err(db)?!=source.account_id
                    || generation<=0 || epoch<=0 || source.generation<generation as u64 || source.execution_epoch<epoch as u64 {
                    return Err(OutboxStoreError::InvalidRecord("stale or conflicting journal gap lineage".into()));
                }
                if source.generation==generation as u64 {
                    if row.get::<String>(4).map_err(db)?!=source.stable_id().to_string() {
                        return Err(OutboxStoreError::InvalidRecord("journal gap generation was reused for different facts".into()));
                    }
                    let wire=row.get::<String>(5).map_err(db)?;
                    if wire.len()>2048 {return Err(db("oversized saved journal gap receipt"));}
                    let receipt:AnalyticsCaptureHandoffReceipt=serde_json::from_str(&wire).map_err(db)?;
                    if !receipt.covers(source) {return Err(db("saved journal gap receipt is inconsistent"));}
                    return Ok(Some(receipt));
                }
            } else {
                let mut rows=self.connection.query("SELECT count(*) FROM capture_journal_gap_handoffs",()).await.map_err(db)?;
                let count=rows.next().await.map_err(db)?.ok_or_else(||db("missing journal gap count"))?.get::<i64>(0).map_err(db)?;
                drop(rows);
                if count>=MAX_JOURNAL_GAP_SOURCES {return Ok(None);}
            }
            let retained=self.capture_retained_bytes_in_transaction().await?;
            let changed=self.connection.execute(
                "INSERT INTO capture_pauses(runtime_id,strategy_id,live_session_id,generation,started_at_us,last_blocked_at_us,blocked_attempts,resumed_at_us,
                    retained_bytes,requested_bytes,limit_bytes,origin_runtime_id,origin_run_id,last_blocked_run_id,owner_run_id,handoff_count,origin_runtime_unknown)
                 VALUES(?1,?2,?3,1,?4,?5,1,NULL,?6,0,?7,NULL,NULL,NULL,?8,0,1)
                 ON CONFLICT(runtime_id,strategy_id,live_session_id) DO UPDATE SET
                    generation=capture_pauses.generation+1,
                    origin_runtime_id=CASE WHEN capture_pauses.resumed_at_us IS NOT NULL OR excluded.started_at_us<=capture_pauses.started_at_us THEN NULL ELSE capture_pauses.origin_runtime_id END,
                    origin_runtime_unknown=CASE WHEN capture_pauses.resumed_at_us IS NOT NULL OR excluded.started_at_us<=capture_pauses.started_at_us THEN 1 ELSE capture_pauses.origin_runtime_unknown END,
                    origin_run_id=CASE WHEN capture_pauses.resumed_at_us IS NOT NULL OR excluded.started_at_us<=capture_pauses.started_at_us THEN NULL ELSE capture_pauses.origin_run_id END,
                    last_blocked_run_id=CASE WHEN capture_pauses.resumed_at_us IS NOT NULL OR excluded.last_blocked_at_us>=capture_pauses.last_blocked_at_us THEN NULL ELSE capture_pauses.last_blocked_run_id END,
                    started_at_us=CASE WHEN capture_pauses.resumed_at_us IS NULL THEN MIN(capture_pauses.started_at_us,excluded.started_at_us) ELSE excluded.started_at_us END,
                    last_blocked_at_us=CASE WHEN capture_pauses.resumed_at_us IS NULL THEN MAX(capture_pauses.last_blocked_at_us,excluded.last_blocked_at_us) ELSE excluded.last_blocked_at_us END,
                    blocked_attempts=capture_pauses.blocked_attempts+1,resumed_at_us=NULL,
                    retained_bytes=excluded.retained_bytes,requested_bytes=0,limit_bytes=excluded.limit_bytes,
                    owner_run_id=excluded.owner_run_id,handoff_count=CASE WHEN capture_pauses.resumed_at_us IS NULL THEN capture_pauses.handoff_count ELSE 0 END
                 WHERE capture_pauses.generation<9223372036854775807 AND capture_pauses.blocked_attempts<9223372036854775807",
                params![owner.runtime.clone(),owner.strategy.clone(),owner.session.clone(),source.started_at_us,source.last_blocked_at_us,
                    retained as i64,payload_limit as i64,owner.run.clone()],
            ).await.map_err(db)?;
            if changed!=1 {return Err(db("journal gap target generation or attempt count exhausted"));}
            let mut saved=self.connection.query("SELECT generation FROM capture_pauses WHERE runtime_id=?1 AND strategy_id=?2 AND live_session_id=?3",
                params![owner.runtime.clone(),owner.strategy.clone(),owner.session.clone()]).await.map_err(db)?;
            let generation=saved.next().await.map_err(db)?.ok_or_else(||db("missing target journal gap"))?.get::<i64>(0).map_err(db)?;
            drop(saved);
            let receipt=AnalyticsCaptureHandoffReceipt {
                version:1,pause_id:source.stable_id(),journal_source_id:source.journal_source_id.clone(),source_generation:source.generation,
                strategy_id:source.strategy_id.clone(),account_id:source.account_id.clone(),target_runtime_id:owner.runtime.clone(),
                target_live_session_id:owner.session.clone(),target_run_id:owner.run.clone(),target_pause_generation:u64::try_from(generation).map_err(db)?,
            };
            receipt.validate().map_err(|e|OutboxStoreError::InvalidRecord(e.into()))?;
            self.connection.execute("INSERT INTO capture_journal_gap_handoffs(journal_source_id,source_generation,source_epoch,strategy_id,account_id,pause_id,receipt)
                VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(journal_source_id) DO UPDATE SET
                source_generation=excluded.source_generation,source_epoch=excluded.source_epoch,pause_id=excluded.pause_id,receipt=excluded.receipt",
                params![source.journal_source_id.clone(),source.generation as i64,source.execution_epoch as i64,source.strategy_id.clone(),
                    source.account_id.clone(),source.stable_id().to_string(),serde_json::to_string(&receipt).map_err(db)?]).await.map_err(db)?;
            Ok(Some(receipt))
        }.await;
        match result {
            Ok(receipt) => {
                self.commit_bounded_metadata(tx).await?;
                if receipt.is_some() {
                    self.safety_notify.notify_waiters();
                }
                Ok(receipt)
            }
            Err(error) => {
                tx.rollback().await.map_err(db)?;
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests;
