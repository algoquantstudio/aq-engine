//! Exact local diagnostic ownership transfer, not an event/export receipt or
//! cloud commit. The journal pause remains active after acknowledgement.
use super::*;

pub const ANALYTICS_CAPTURE_HANDOFF_VERSION: u16 = 1;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnalyticsCaptureHandoffReceipt {
    pub version: u16,
    pub pause_id: uuid::Uuid,
    pub journal_source_id: String,
    pub source_generation: u64,
    pub strategy_id: String,
    pub account_id: String,
    pub target_runtime_id: String,
    pub target_live_session_id: String,
    pub target_run_id: String,
    pub target_pause_generation: u64,
}
impl AnalyticsCaptureHandoffReceipt {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.version != ANALYTICS_CAPTURE_HANDOFF_VERSION
            || self.source_generation == 0
            || self.source_generation > i64::MAX as u64
            || self.target_pause_generation == 0
            || self.target_pause_generation > i64::MAX as u64
            || [
                &self.journal_source_id,
                &self.strategy_id,
                &self.account_id,
                &self.target_runtime_id,
                &self.target_live_session_id,
                &self.target_run_id,
            ]
            .iter()
            .any(|id| id.trim().is_empty() || id.len() > 128)
            || serde_json::to_vec(self)
                .map_err(|_| "invalid journal handoff receipt encoding")?
                .len()
                > 2048
        {
            return Err("invalid analytics capture handoff receipt");
        }
        Ok(())
    }
    pub fn covers(&self, pause: &AnalyticsCapturePause) -> bool {
        self.validate().is_ok()
            && pause.validate().is_ok()
            && self.pause_id == pause.stable_id()
            && self.journal_source_id == pause.journal_source_id
            && self.source_generation == pause.generation
            && self.strategy_id == pause.strategy_id
            && self.account_id == pause.account_id
    }
}

pub(in crate::core::broker::command_journal) async fn install(connection: &Connection) -> Result<(), BrokerCommandError> {
    let mut rows = connection
        .query("PRAGMA table_info(broker_analytics_capture)", ())
        .await
        .map_err(journal_error)?;
    let mut found = false;
    while let Some(row) = rows.next().await.map_err(journal_error)? {
        found |= row.get::<String>(1).map_err(journal_error)? == "handoff_receipt";
    }
    drop(rows);
    if !found {
        connection
            .execute_batch("ALTER TABLE broker_analytics_capture ADD COLUMN handoff_receipt TEXT;")
            .await
            .map_err(journal_error)?;
    }
    Ok(())
}

impl BrokerCommandJournal {
    /// Store only the exact receipt for the current immutable local gap. This
    /// does not clear its pause, export rows, sequence clocks, or any commands.
    pub async fn acknowledge_analytics_capture_handoff(
        &self,
        receipt: &AnalyticsCaptureHandoffReceipt,
    ) -> Result<bool, BrokerCommandError> {
        receipt.validate().map_err(journal_error)?;
        let tx = turso::transaction::Transaction::new_unchecked(
            self.analytics_connection(),
            turso::transaction::TransactionBehavior::Immediate,
        )
        .await
        .map_err(journal_error)?;
        let Some(pause) = self.analytics_capture_pause().await? else {
            return Ok(false);
        };
        if !receipt.covers(&pause) {
            return Ok(false);
        }
        if let Some(saved) = self.analytics_capture_handoff_receipt().await? {
            let same = saved == *receipt;
            tx.commit().await.map_err(journal_error)?;
            return Ok(same);
        }
        let wire = serde_json::to_string(receipt).map_err(journal_error)?;
        let changed=tx.execute("UPDATE broker_analytics_capture SET handoff_receipt=?1 WHERE singleton=1 AND version=1 AND paused=1 AND generation=?2 AND (handoff_receipt IS NULL OR handoff_receipt=?1)",
            params![wire,pause.generation as i64]).await.map_err(journal_error)?;
        self.commit_analytics_maintenance(tx).await?;
        Ok(changed == 1)
    }

    pub async fn analytics_capture_handoff_receipt(
        &self,
    ) -> Result<Option<AnalyticsCaptureHandoffReceipt>, BrokerCommandError> {
        let Some(pause) = self.analytics_capture_pause().await? else {
            return Ok(None);
        };
        let mut rows=self.analytics_connection().query("SELECT handoff_receipt FROM broker_analytics_capture WHERE singleton=1 AND version=1 AND paused=1 AND generation=?1",params![pause.generation as i64]).await.map_err(journal_error)?;
        let wire = rows
            .next()
            .await
            .map_err(journal_error)?
            .ok_or_else(|| journal_error("missing analytics capture gap"))?
            .get::<Option<String>>(0)
            .map_err(journal_error)?;
        let Some(wire) = wire else {
            return Ok(None);
        };
        if wire.len() > 2048 {
            return Err(journal_error("oversized analytics capture receipt"));
        }
        let receipt: AnalyticsCaptureHandoffReceipt =
            serde_json::from_str(&wire).map_err(journal_error)?;
        if !receipt.covers(&pause) {
            return Err(journal_error(
                "analytics capture receipt does not cover the current pause",
            ));
        }
        Ok(Some(receipt))
    }
}
