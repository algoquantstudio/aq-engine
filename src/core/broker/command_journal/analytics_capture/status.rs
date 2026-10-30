//! Constant-size owner observation, not a customer usage meter or remote ACK.
use super::*;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnalyticsCaptureStatus {
    pub version: u16,
    pub journal_source_id: String,
    pub strategy_id: String,
    pub account_id: String,
    pub generation: u64,
    pub pause: Option<AnalyticsCapturePause>,
    pub handoff_receipt: Option<AnalyticsCaptureHandoffReceipt>,
    pub physical: PhysicalCaptureUsage,
    pub observed_at_us: i64,
}

impl AnalyticsCaptureStatus {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.version != 1
            || self.generation > i64::MAX as u64
            || self.observed_at_us <= 0
            || chrono::DateTime::from_timestamp_micros(self.observed_at_us).is_none()
            || [&self.journal_source_id, &self.strategy_id, &self.account_id]
                .iter()
                .any(|id| id.trim().is_empty() || id.len() > 128)
            || self.physical_limit().validate().is_err()
            || self.physical.projected_commit_bytes < self.physical.actual_bytes()
        {
            return Err("invalid analytics capture status");
        }
        match (&self.pause, &self.handoff_receipt) {
            (Some(pause), receipt) => {
                pause.validate()?;
                if pause.generation != self.generation
                    || pause.journal_source_id != self.journal_source_id
                    || pause.strategy_id != self.strategy_id
                    || pause.account_id != self.account_id
                    || receipt
                        .as_ref()
                        .is_some_and(|receipt| !receipt.covers(pause))
                {
                    return Err("inconsistent analytics capture status");
                }
            }
            (None, Some(_)) => return Err("unpaused analytics status has a pause receipt"),
            (None, None) => {}
        }
        if serde_json::to_vec(self)
            .map_err(|_| "invalid analytics status encoding")?
            .len()
            > 4096
        {
            return Err("analytics capture status exceeds its bound");
        }
        Ok(())
    }
    pub fn physical_limit(&self) -> PhysicalCaptureLimit {
        PhysicalCaptureLimit {
            physical_limit_bytes: self.physical.limit_bytes,
            operational_headroom_bytes: self.physical.operational_headroom_bytes,
        }
    }
}

impl BrokerCommandJournal {
    /// One primary-key state read and constant-size footprint inspection. The
    /// single journal owner serializes this with writes; it must not await any
    /// other file/service while its bounded analytics permit is held.
    pub async fn analytics_capture_status(
        &self,
    ) -> Result<Option<AnalyticsCaptureStatus>, BrokerCommandError> {
        let Some(limit) = self.analytics_capture_budget else {
            return Ok(None);
        };
        let mut rows=self.analytics_connection().query("SELECT version,generation,paused,execution_epoch,started_at_us,last_blocked_at_us,handoff_receipt FROM broker_analytics_capture WHERE singleton=1",()).await.map_err(journal_error)?;
        let row = rows
            .next()
            .await
            .map_err(journal_error)?
            .ok_or_else(|| journal_error("missing analytics capture state"))?;
        drop(rows);
        if row.get::<i64>(0).map_err(journal_error)? != 1 {
            return Err(journal_error("unsupported analytics capture state version"));
        }
        let generation =
            u64::try_from(row.get::<i64>(1).map_err(journal_error)?).map_err(journal_error)?;
        let pause = match row.get::<i64>(2).map_err(journal_error)? {
            0 => None,
            1 => Some(AnalyticsCapturePause {
                version: 1,
                journal_source_id: self.evidence_source_id.clone(),
                strategy_id: self.strategy_id.clone(),
                account_id: self.account_id.clone(),
                generation,
                execution_epoch: u64::try_from(row.get::<i64>(3).map_err(journal_error)?)
                    .map_err(journal_error)?,
                started_at_us: row.get(4).map_err(journal_error)?,
                last_blocked_at_us: row.get(5).map_err(journal_error)?,
            }),
            _ => return Err(journal_error("invalid analytics capture paused state")),
        };
        let wire = row.get::<Option<String>>(6).map_err(journal_error)?;
        let handoff_receipt = if pause.is_some() {
            match wire {
                Some(wire) if wire.len() <= 2048 => {
                    Some(serde_json::from_str(&wire).map_err(journal_error)?)
                }
                Some(_) => return Err(journal_error("oversized analytics capture receipt")),
                None => None,
            }
        } else {
            None
        };
        let status = AnalyticsCaptureStatus {
            version: 1,
            journal_source_id: self.evidence_source_id.clone(),
            strategy_id: self.strategy_id.clone(),
            account_id: self.account_id.clone(),
            generation,
            pause,
            handoff_receipt,
            physical: measure_physical_capture(self.analytics_connection(), self.analytics_path(), limit)
                .await
                .map_err(journal_error)?,
            observed_at_us: chrono::Utc::now().timestamp_micros(),
        };
        status.validate().map_err(journal_error)?;
        Ok(Some(status))
    }
}
