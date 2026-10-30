//! Optional observation admission only. Broker command ownership, canonical
//! checkpoints, execution epochs and stop settlement never consult this cap.
use super::*;
use crate::core::live_sync::{
    PhysicalCaptureLimit, PhysicalCaptureUsage, install_physical_capture_spill_control,
    measure_physical_capture,
};
pub(super) mod handoff;
pub use handoff::AnalyticsCaptureHandoffReceipt;
mod status;
pub use status::AnalyticsCaptureStatus;

pub(super) const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS broker_analytics_capture (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL,
    generation INTEGER NOT NULL CHECK(generation>=0),paused INTEGER NOT NULL CHECK(paused IN(0,1)),
    started_at_us INTEGER,last_blocked_at_us INTEGER,execution_epoch INTEGER,resumed_at_us INTEGER
);
INSERT INTO broker_analytics_capture(singleton,version,generation,paused) VALUES(1,1,0,0)
    ON CONFLICT(singleton) DO NOTHING;";

/// A local quality fact, not an AQS run identity or an execution receipt. The
/// original journal epoch/time survive a process restart. No runtime/run ID is
/// inferred from them; later transport must keep unknown provenance explicit.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnalyticsCapturePause {
    pub version: u16,
    pub journal_source_id: String,
    pub strategy_id: String,
    pub account_id: String,
    pub generation: u64,
    pub execution_epoch: u64,
    pub started_at_us: i64,
    pub last_blocked_at_us: i64,
}

impl AnalyticsCapturePause {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.version != 1
            || self.generation == 0
            || self.generation > i64::MAX as u64
            || self.execution_epoch == 0
            || self.execution_epoch > i64::MAX as u64
            || self.started_at_us <= 0
            || self.last_blocked_at_us < self.started_at_us
            || chrono::DateTime::from_timestamp_micros(self.last_blocked_at_us).is_none()
            || [&self.journal_source_id, &self.strategy_id, &self.account_id]
                .iter()
                .any(|id| id.trim().is_empty() || id.len() > 128)
            || serde_json::to_vec(self)
                .map_err(|_| "invalid journal gap encoding")?
                .len()
                > 2048
        {
            return Err("invalid analytics capture pause");
        }
        Ok(())
    }
    pub fn stable_id(&self) -> uuid::Uuid {
        let fields = (
            self.version,
            &self.journal_source_id,
            &self.strategy_id,
            &self.account_id,
            self.generation,
            self.execution_epoch,
            self.started_at_us,
            self.last_blocked_at_us,
        );
        uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            &serde_json::to_vec(&fields).expect("fixed journal gap tuple"),
        )
    }
}

impl BrokerCommandJournal {
    /// Explicit per-journal analytics partition. Normal standalone journals are
    /// unchanged. Reinstall on every opted-in launch; changing a limit alone
    /// never clears a recorded gap or acknowledges an export.
    pub async fn with_analytics_capture_budget(
        mut self,
        limit: PhysicalCaptureLimit,
    ) -> Result<Self, BrokerCommandError> {
        if [
            &self.evidence_source_id,
            &self.strategy_id,
            &self.account_id,
        ]
        .iter()
        .any(|id| id.trim().is_empty() || id.len() > 128)
        {
            return Err(journal_error(
                "bounded analytics capture scope exceeds its metadata limit",
            ));
        }
        let limit = limit.validate().map_err(journal_error)?;
        install_physical_capture_spill_control(self.analytics_connection())
            .await
            .map_err(journal_error)?;
        let metadata = turso::transaction::Transaction::new_unchecked(
            self.analytics_connection(), turso::transaction::TransactionBehavior::Immediate,
        ).await.map_err(journal_error)?;
        self.analytics_connection()
            .execute_batch(SCHEMA)
            .await
            .map_err(journal_error)?;
        let mut rows = self.analytics_connection()
            .query(
                "SELECT version FROM broker_analytics_capture WHERE singleton=1",
                (),
            )
            .await
            .map_err(journal_error)?;
        if rows
            .next()
            .await
            .map_err(journal_error)?
            .ok_or_else(|| journal_error("missing analytics capture policy"))?
            .get::<i64>(0)
            .map_err(journal_error)?
            != 1
        {
            return Err(journal_error("unsupported analytics capture state version"));
        }
        drop(rows);
        handoff::install(self.analytics_connection()).await?;
        super::cloud_history::commit_operational(metadata,self.analytics_path(),
            self.cloud_history.as_ref().map(|_|limit)).await?;
        self.analytics_capture_budget = Some(limit);
        Ok(self)
    }

    pub fn analytics_capture_budget(&self) -> Option<PhysicalCaptureLimit> {
        self.analytics_capture_budget
    }

    pub(crate) fn analytics_capture_identity(&self) -> (&str, &str, &str) {
        (&self.evidence_source_id, &self.strategy_id, &self.account_id)
    }

    /// Startup-only fallback, before any command worker or broker dispatch.
    /// One FULL-synchronous singleton fact survives an unavailable publisher
    /// store. A retry preserves the original pause and any handoff receipt.
    pub(crate) async fn suspend_analytics_for_source_initialization(
        &self,
    ) -> Result<AnalyticsCapturePause, BrokerCommandError> {
        if self.analytics_capture_budget.is_none() || self.lifecycle.epoch == 0 {
            return Err(journal_error("source initialization requires a scoped capture epoch"));
        }
        if let Some(pause) = self.analytics_capture_pause().await? {
            return Ok(pause);
        }
        let at = chrono::Utc::now().timestamp_micros();
        let tx=turso::transaction::Transaction::new_unchecked(self.analytics_connection(),turso::transaction::TransactionBehavior::Immediate).await.map_err(journal_error)?;
        let changed = tx.execute(
            "UPDATE broker_analytics_capture SET generation=generation+1,paused=1,
             started_at_us=?1,last_blocked_at_us=?1,execution_epoch=?2,
             resumed_at_us=NULL,handoff_receipt=NULL
             WHERE singleton=1 AND version=1 AND paused=0 AND generation<9223372036854775807",
            params![at, self.lifecycle.epoch as i64],
        ).await.map_err(journal_error)?;
        if changed != 1 {
            return Err(journal_error("source initialization pause was not durably recorded"));
        }
        self.commit_analytics_maintenance(tx).await?;
        self.analytics_capture_pause().await?.ok_or_else(||
            journal_error("source initialization pause is unavailable after commit"))
    }

    pub async fn analytics_capture_usage(
        &self,
    ) -> Result<Option<PhysicalCaptureUsage>, BrokerCommandError> {
        match self.analytics_capture_budget {
            Some(limit) => measure_physical_capture(self.analytics_connection(), self.analytics_path(), limit)
                .await
                .map(Some)
                .map_err(journal_error),
            None => Ok(None),
        }
    }

    pub async fn analytics_capture_pause(
        &self,
    ) -> Result<Option<AnalyticsCapturePause>, BrokerCommandError> {
        if self.analytics_capture_budget.is_none() {
            return Ok(None);
        }
        let mut rows = self.analytics_connection()
            .query(
                "SELECT version,generation,execution_epoch,started_at_us,last_blocked_at_us
            FROM broker_analytics_capture WHERE singleton=1 AND paused=1",
                (),
            )
            .await
            .map_err(journal_error)?;
        let Some(row) = rows.next().await.map_err(journal_error)? else {
            return Ok(None);
        };
        let version = row.get::<i64>(0).map_err(journal_error)?;
        if version != 1 {
            return Err(journal_error("unsupported analytics capture pause version"));
        }
        let pause = AnalyticsCapturePause {
            version: 1,
            journal_source_id: self.evidence_source_id.clone(),
            strategy_id: self.strategy_id.clone(),
            account_id: self.account_id.clone(),
            generation: u64::try_from(row.get::<i64>(1).map_err(journal_error)?)
                .map_err(journal_error)?,
            execution_epoch: u64::try_from(row.get::<i64>(2).map_err(journal_error)?)
                .map_err(journal_error)?,
            started_at_us: row.get(3).map_err(journal_error)?,
            last_blocked_at_us: row.get(4).map_err(journal_error)?,
        };
        pause.validate().map_err(journal_error)?;
        Ok(Some(pause))
    }

    /// Free space is not proof of recovered observations. The caller must have
    /// committed current state and preserved the quality notice before this
    /// exact-generation completion; no automatic resume occurs on restart.
    pub async fn complete_analytics_capture_reconciliation(
        &self,
        generation: u64,
    ) -> Result<bool, BrokerCommandError> {
        self.complete_analytics_capture_inner(generation, None)
            .await
    }

    /// Managed owner completion after sealed current-state/quality replay.
    /// The exact durable diagnostic receipt is required in addition to the
    /// local generation and low-water check; it is not itself a replay proof.
    pub async fn complete_analytics_capture_with_handoff(
        &self,
        receipt: &AnalyticsCaptureHandoffReceipt,
    ) -> Result<bool, BrokerCommandError> {
        receipt.validate().map_err(journal_error)?;
        if receipt.journal_source_id != self.evidence_source_id
            || receipt.strategy_id != self.strategy_id
            || receipt.account_id != self.account_id
        {
            return Ok(false);
        }
        self.complete_analytics_capture_inner(receipt.source_generation, Some(receipt))
            .await
    }

    async fn complete_analytics_capture_inner(
        &self,
        generation: u64,
        receipt: Option<&AnalyticsCaptureHandoffReceipt>,
    ) -> Result<bool, BrokerCommandError> {
        let Some(limit) = self.analytics_capture_budget else {
            return Ok(false);
        };
        let tx = turso::transaction::Transaction::new_unchecked(
            self.analytics_connection(),
            turso::transaction::TransactionBehavior::Immediate,
        )
        .await
        .map_err(journal_error)?;
        if let Some(receipt)=receipt {
            let Some(pause)=self.analytics_capture_pause().await? else{return Ok(false);};
            if !receipt.covers(&pause){return Ok(false);}
        }
        let usage = measure_physical_capture(&tx, self.analytics_path(), limit)
            .await
            .map_err(journal_error)?;
        if !usage.below_resume_threshold() {
            return Ok(false);
        }
        let generation = i64::try_from(generation).map_err(journal_error)?;
        let changed=match receipt {
            Some(receipt)=>tx.execute("UPDATE broker_analytics_capture SET paused=0,resumed_at_us=?1 WHERE singleton=1 AND version=1 AND paused=1 AND generation=?2 AND handoff_receipt=?3",
                params![chrono::Utc::now().timestamp_micros(),generation,serde_json::to_string(receipt).map_err(journal_error)?]).await.map_err(journal_error)?,
            None=>tx.execute("UPDATE broker_analytics_capture SET paused=0,resumed_at_us=?1 WHERE singleton=1 AND version=1 AND paused=1 AND generation=?2",
                params![chrono::Utc::now().timestamp_micros(),generation]).await.map_err(journal_error)?,
        };
        self.commit_analytics_maintenance(tx).await?;
        Ok(changed == 1)
    }

    pub(super) async fn check_analytics_admission(&self) -> Result<(), BrokerCommandError> {
        if self.analytics_capture_pause().await?.is_some() {
            return Err(BrokerCommandError::CapturePaused);
        }
        Ok(())
    }
}

/// The transaction owns all fills/heads/comparison clocks or marks/heads. Never
/// report a physical refusal until every attempted observation has rolled back.
pub(super) async fn commit_observations(
    tx: turso::transaction::Transaction<'_>,
    connection: &Connection,
    path: &Path,
    limit: Option<PhysicalCaptureLimit>,
    epoch: u64,
) -> Result<(), BrokerCommandError> {
    if let Some(limit) = limit {
        let usage = measure_physical_capture(&tx, path, limit)
            .await
            .map_err(journal_error)?;
        if usage.exceeds_data_limit() {
            tx.rollback().await.map_err(journal_error)?;
            let at = chrono::Utc::now().timestamp_micros();
            // One bounded, FULL-synchronous operational fact in the reserved
            // headroom. Rejected data gets neither an export cursor nor a head.
            let metadata=turso::transaction::Transaction::new_unchecked(connection,turso::transaction::TransactionBehavior::Immediate).await.map_err(journal_error)?;
            let changed=metadata.execute("UPDATE broker_analytics_capture SET
                generation=generation+1,paused=1,started_at_us=?1,last_blocked_at_us=?1,execution_epoch=?2,resumed_at_us=NULL,handoff_receipt=NULL
                WHERE singleton=1 AND version=1 AND paused=0 AND generation<9223372036854775807",params![at,epoch as i64]).await.map_err(journal_error)?;
            if changed != 1 {
                return Err(journal_error(
                    "analytics capture pause was not durably recorded",
                ));
            }
            super::cloud_history::commit_operational(metadata,path,Some(limit)).await?;
            return Err(BrokerCommandError::CapturePaused);
        }
    }
    tx.commit().await.map_err(journal_error)
}

#[cfg(test)]
mod tests;
