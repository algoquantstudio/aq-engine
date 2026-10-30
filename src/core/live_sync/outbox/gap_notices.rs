//! A fixed-format, bounded operational mailbox. Receipts release only gap
//! metadata ownership, never trading events or a critical sequence cursor.
use super::*;
use aqe_live_protocol::{CaptureGapNotice, CaptureGapReceipt};

pub const MAX_PENDING_CAPTURE_GAP_NOTICES: usize = 256;
#[cfg(test)]
mod tests;
fn db(e: impl std::fmt::Display) -> OutboxStoreError {
    OutboxStoreError::Database(e.to_string())
}

impl OutboxStore {
    pub(super) async fn ensure_gap_notice_schema(&self) -> Result<(), OutboxStoreError> {
        self.execute_bounded_metadata_batch("CREATE TABLE IF NOT EXISTS capture_gap_notices (
            notice_id TEXT PRIMARY KEY,strategy_id TEXT NOT NULL,live_session_id TEXT NOT NULL,
            notice TEXT NOT NULL,next_attempt_at_us INTEGER NOT NULL DEFAULT 0);
            CREATE INDEX IF NOT EXISTS capture_gap_notices_pending ON capture_gap_notices(strategy_id,next_attempt_at_us);
            CREATE INDEX IF NOT EXISTS capture_gap_notices_scope ON capture_gap_notices(strategy_id,live_session_id);").await?;
        Ok(())
    }

    /// False means no ownership transfer: keep the source metadata and retry
    /// after existing notices drain. Exact retries fit even at the slot cap.
    pub async fn accept_capture_gap_notice(
        &self,
        n: &CaptureGapNotice,
    ) -> Result<bool, OutboxStoreError> {
        n.validate()
            .map_err(|e| OutboxStoreError::InvalidRecord(e.into()))?;
        let _operation = self.operation_gate.lock().await;
        let tx = turso::transaction::Transaction::new_unchecked(
            &self.connection,
            turso::transaction::TransactionBehavior::Immediate,
        )
        .await
        .map_err(db)?;
        let result=async {
            let mut rows=self.connection.query("SELECT notice FROM capture_gap_notices WHERE notice_id=?1",params![n.notice_id.to_string()]).await.map_err(db)?;
            if let Some(row)=rows.next().await.map_err(db)? {
                let saved:CaptureGapNotice=serde_json::from_str(&row.get::<String>(0).map_err(db)?).map_err(db)?;
                if &saved!=n {return Err(OutboxStoreError::InvalidRecord("gap notice identity conflict".into()));}
                return Ok(true);
            }
            drop(rows);
            let mut count_rows=self.connection.query("SELECT count(*) FROM capture_gap_notices",()).await.map_err(db)?;
            let count=count_rows.next().await.map_err(db)?.ok_or_else(||db("missing gap mailbox count"))?.get::<i64>(0).map_err(db)?;
            drop(count_rows);
            if count>=MAX_PENDING_CAPTURE_GAP_NOTICES as i64 {return Ok(false);}
            self.connection.execute("INSERT INTO capture_gap_notices(notice_id,strategy_id,live_session_id,notice,next_attempt_at_us) VALUES(?1,?2,?3,?4,0)",
                params![n.notice_id.to_string(),n.strategy_id.clone(),n.live_session_id.clone(),serde_json::to_string(n).map_err(db)?]).await.map_err(db)?;
            Ok(true)
        }.await;
        match result {
            Ok(accepted) => {
                self.commit_bounded_metadata(tx).await?;
                if accepted {
                    self.safety_notify.notify_waiters();
                }
                Ok(accepted)
            }
            Err(e) => {
                let _ = tx.rollback().await;
                Err(e)
            }
        }
    }

    pub async fn pending_capture_gap_notices(
        &self,
        strategy: &str,
    ) -> Result<Vec<CaptureGapNotice>, OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let mut rows=self.connection.query("SELECT notice FROM capture_gap_notices INDEXED BY capture_gap_notices_pending WHERE strategy_id=?1 AND next_attempt_at_us<=?2 ORDER BY next_attempt_at_us LIMIT 16",
            params![strategy,now_epoch_micros()]).await.map_err(db)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(db)? {
            out.push(serde_json::from_str(&row.get::<String>(0).map_err(db)?).map_err(db)?);
        }
        Ok(out)
    }

    pub async fn settle_capture_gap_notice(
        &self,
        n: &CaptureGapNotice,
        receipt: Option<&CaptureGapReceipt>,
    ) -> Result<(), OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        if receipt.is_some_and(|r| r.covers(n)) {
            self
                .execute_bounded_metadata(
                    "DELETE FROM capture_gap_notices WHERE notice_id=?1 AND notice=?2",
                    params![
                        n.notice_id.to_string(),
                        serde_json::to_string(n).map_err(db)?
                    ],
                )
                .await
                ?;
        } else {
            self
                .execute_bounded_metadata(
                    "UPDATE capture_gap_notices SET next_attempt_at_us=?1 WHERE notice_id=?2",
                    params![now_epoch_micros() + 5_000_000, n.notice_id.to_string()],
                )
                .await
                ?;
        }
        Ok(())
    }

    /// Caller holds the exact private deletion audit. This affects only the
    /// bounded operational mailbox, never accepted trading data or source files.
    pub async fn discard_gap_notices_for_deleted_scope(
        &self,
        strategy: &str,
        session: &str,
    ) -> Result<(), OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        self
            .execute_bounded_metadata(
                "DELETE FROM capture_gap_notices WHERE strategy_id=?1 AND live_session_id=?2",
                params![strategy, session],
            )
            .await
            ?;
        Ok(())
    }

    pub async fn local_capture_gap_notices(
        &self,
        strategy: &str,
        session: &str,
    ) -> Result<Vec<CaptureGapNotice>, OutboxStoreError> {
        if !self
            .has_unresolved_capture_pause_for_scope(strategy, session)
            .await?
        {
            return Ok(Vec::new());
        }
        self.ensure_capture_capacity_schema().await?;
        let _operation = self.operation_gate.lock().await;
        let mut rows=self.connection.query("SELECT runtime_id,strategy_id,live_session_id,generation,started_at_us,last_blocked_at_us,
            origin_runtime_id,origin_run_id,last_blocked_run_id,owner_run_id,handoff_count FROM capture_pauses
            INDEXED BY capture_pauses_scope_latest WHERE strategy_id=?1 AND live_session_id=?2 AND resumed_at_us IS NULL
            AND (notice_handoff_generation IS NULL OR notice_handoff_generation!=generation)
            ORDER BY last_blocked_at_us DESC LIMIT 16",params![strategy,session]).await.map_err(db)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(db)? {
            let mut n = CaptureGapNotice {
                version: 1,
                notice_id: uuid::Uuid::nil(),
                source_runtime_id: row.get(0).map_err(db)?,
                strategy_id: row.get(1).map_err(db)?,
                live_session_id: row.get(2).map_err(db)?,
                source_generation: u64::try_from(row.get::<i64>(3).map_err(db)?).map_err(db)?,
                started_at_us: row.get(4).map_err(db)?,
                last_blocked_at_us: row.get(5).map_err(db)?,
                origin_runtime_id: row.get(6).map_err(db)?,
                origin_run_id: row.get(7).map_err(db)?,
                last_blocked_run_id: row.get(8).map_err(db)?,
                source_run_id: row.get(9).map_err(db)?,
                ownership_handoffs: u64::try_from(row.get::<i64>(10).map_err(db)?).map_err(db)?,
            };
            n.notice_id = n.stable_id();
            n.validate()
                .map_err(|e| OutboxStoreError::InvalidRecord(e.into()))?;
            out.push(n);
        }
        Ok(out)
    }

    /// Exact-generation receipt; this is NOT capture resume or broker proof.
    pub async fn settle_local_capture_gap_notice(
        &self,
        n: &CaptureGapNotice,
        receipt: &CaptureGapReceipt,
    ) -> Result<bool, OutboxStoreError> {
        n.validate()
            .map_err(|e| OutboxStoreError::InvalidRecord(e.into()))?;
        if !receipt.covers(n) {
            return Ok(false);
        }
        self.ensure_capture_capacity_schema().await?;
        let _operation = self.operation_gate.lock().await;
        let changed=self.execute_bounded_metadata("UPDATE capture_pauses SET notice_handoff_generation=?1 WHERE runtime_id=?2 AND strategy_id=?3 AND live_session_id=?4 AND generation=?1 AND resumed_at_us IS NULL
            AND started_at_us=?5 AND last_blocked_at_us=?6 AND owner_run_id IS ?7 AND origin_runtime_id IS ?8 AND origin_run_id IS ?9
            AND last_blocked_run_id IS ?10 AND handoff_count=?11",
            params![n.source_generation as i64,n.source_runtime_id.clone(),n.strategy_id.clone(),n.live_session_id.clone(),n.started_at_us,n.last_blocked_at_us,
                n.source_run_id.clone(),n.origin_runtime_id.clone(),n.origin_run_id.clone(),n.last_blocked_run_id.clone(),n.ownership_handoffs as i64]).await?;
        Ok(changed == 1)
    }

    pub async fn has_untransferred_capture_pause_for_scope(
        &self,
        strategy: &str,
        session: &str,
    ) -> Result<bool, OutboxStoreError> {
        if !self
            .local_capture_gap_notices(strategy, session)
            .await?
            .is_empty() {return Ok(true);}
        let _operation=self.operation_gate.lock().await;
        let mut rows=self.connection.query("SELECT singleton FROM capture_source_observation WHERE singleton=1 AND strategy_id=?1 AND live_session_id=?2 AND closed_at_us IS NULL",
            params![strategy.to_string(),session.to_string()]).await.map_err(db)?;
        Ok(rows.next().await.map_err(db)?.is_some())
    }
}
