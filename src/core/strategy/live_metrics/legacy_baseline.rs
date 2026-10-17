use super::{
    LiveMetricsCheckpoint, LiveMetricsSnapshot, LivePerformanceTracker, TradeHistoryRebuild,
};
use chrono::{DateTime, Utc};

/// Legacy equity/risk approximation only. Unidentified legacy trade totals must
/// never be added to live trade totals; the separate history rebuild owns those.
#[derive(Clone, Debug)]
pub(crate) struct LegacyRiskBaseline {
    checkpoint: LiveMetricsCheckpoint,
}

pub(crate) enum LegacyRiskMerge {
    CompleteCheckpointPreferred,
    NeedsTradeHistory,
}

impl LegacyRiskBaseline {
    pub(crate) fn prepare(
        snapshot: LiveMetricsSnapshot,
        account_id: String,
        origin: Option<(f64, DateTime<Utc>)>,
    ) -> Result<Self, String> {
        let updated_at = snapshot.updated_at;
        let equity = snapshot.final_equity;
        if origin.is_some_and(|(cash, at)| !cash.is_finite() || at > updated_at) {
            return Err(
                "Legacy account origin is invalid or newer than its metrics snapshot".into(),
            );
        }
        let mut historical = LivePerformanceTracker::default();
        historical.restore_legacy_snapshot(snapshot, account_id, equity, updated_at, [])?;
        historical.apply_trade_history_rebuild(TradeHistoryRebuild::default());
        historical.mark_history_recovery_incomplete();
        if let Some((cash, at)) = origin {
            historical.set_continuity_origin(cash, at);
        }
        Ok(Self {
            checkpoint: historical
                .checkpoint()
                .ok_or_else(|| "Legacy risk baseline has no account identity".to_string())?,
        })
    }
}

impl LivePerformanceTracker {
    pub(crate) fn merge_legacy_risk_baseline(
        &mut self,
        baseline: LegacyRiskBaseline,
    ) -> Result<LegacyRiskMerge, String> {
        let historical = baseline.checkpoint;
        if self.account_id.as_deref() != Some(historical.account_id.as_str()) {
            return Err("Legacy risk baseline belongs to a different broker account".into());
        }
        // An exact local checkpoint takes precedence over an approximate legacy
        // Cloud record, matching startup selection without reinitializing state.
        if self.history_recovery_complete {
            return Ok(LegacyRiskMerge::CompleteCheckpointPreferred);
        }
        // A local incomplete checkpoint can already contain the legacy risk
        // baseline. Retry the trade rebuild without adding its samples again.
        if self.executed_at == Some(historical.executed_at)
            && self.starting_cash == historical.starting_cash
            && self
                .updated_at
                .is_some_and(|at| at >= historical.updated_at)
            && self.return_count >= historical.return_count
            && self.max_drawdown >= historical.max_drawdown
        {
            return Ok(LegacyRiskMerge::NeedsTradeHistory);
        }
        // All overlap validation happens before mutation. The historical
        // checkpoint has no trade totals, so live closes and exposure survive.
        self.merge_historical_checkpoint(historical)?;
        Ok(LegacyRiskMerge::NeedsTradeHistory)
    }
}

#[cfg(test)]
mod tests;
