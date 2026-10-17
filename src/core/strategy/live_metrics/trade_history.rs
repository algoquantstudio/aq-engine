use super::{HistoricalClosedInsightMetric, LivePerformanceTracker};

const MAX_RECOVERY_LIVE_TRADES: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TradeRecoveryToken {
    id: uuid::Uuid,
    account_id: String,
}

impl TradeRecoveryToken {
    pub(crate) fn matches_account(&self, account_id: &str) -> bool {
        self.account_id == account_id.trim()
    }
}

#[derive(Clone, Debug)]
struct CapturedTrade {
    insight_id: String,
    pnl: f64,
    return_pct: f64,
    held_secs: i64,
    at: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, Debug)]
pub(super) struct TradeRecoveryCapture {
    token: TradeRecoveryToken,
    trades: Vec<CapturedTrade>,
    overflowed: bool,
}

/// Stages only trade accumulators and deduplication identities. Database pages
/// can be dropped immediately; an incomplete attempt never mutates live metrics.
#[derive(Default)]
pub(crate) struct TradeHistoryRebuild {
    staged: LivePerformanceTracker,
}

impl TradeHistoryRebuild {
    pub(crate) fn push_page(
        &mut self,
        records: impl IntoIterator<Item = HistoricalClosedInsightMetric>,
    ) {
        for record in records {
            let pnl = super::historical_trade_pnl(&record);
            let return_pct = super::historical_trade_return_pct(&record);
            let held = super::historical_trade_hold_secs(&record);
            let at = record.closed_at.unwrap_or(record.updated_at);
            if !self
                .staged
                .recorded_closed_insight_ids
                .insert(record.insight_id)
            {
                continue;
            }
            self.staged.record_closed_values(pnl, return_pct, held, at);
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.staged.total_trades
    }
}

impl LivePerformanceTracker {
    pub(crate) fn is_trade_history_recovery_current(&self, token: &TradeRecoveryToken) -> bool {
        self.account_id.as_deref() == Some(token.account_id.as_str())
            && self
                .trade_recovery
                .as_ref()
                .is_some_and(|capture| &capture.token == token)
    }

    pub(crate) fn begin_trade_history_recovery(&mut self) -> Result<TradeRecoveryToken, String> {
        if self.trade_recovery.is_some() {
            return Err("Trade history recovery is already active".into());
        }
        let account_id = self
            .account_id
            .as_deref()
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| {
                "Trade history recovery requires a broker account identity".to_string()
            })?;
        let token = TradeRecoveryToken {
            id: uuid::Uuid::new_v4(),
            account_id: account_id.to_string(),
        };
        self.trade_recovery = Some(TradeRecoveryCapture {
            token: token.clone(),
            trades: Vec::new(),
            overflowed: false,
        });
        Ok(token)
    }

    pub(crate) fn cancel_trade_history_recovery(&mut self, token: &TradeRecoveryToken) {
        if self
            .trade_recovery
            .as_ref()
            .is_some_and(|capture| &capture.token == token)
        {
            self.trade_recovery = None;
        }
    }

    pub(super) fn capture_recovery_trade(
        &mut self,
        insight_id: &str,
        pnl: f64,
        return_pct: f64,
        held_secs: i64,
        at: chrono::DateTime<chrono::Utc>,
    ) {
        let Some(capture) = self.trade_recovery.as_mut() else {
            return;
        };
        if capture.trades.len() == MAX_RECOVERY_LIVE_TRADES {
            capture.overflowed = true;
            return;
        }
        capture.trades.push(CapturedTrade {
            insight_id: insight_id.to_string(),
            pnl,
            return_pct,
            held_secs,
            at,
        });
    }

    /// Consume a complete rebuild only if it still belongs to this attempt and
    /// covers every known close. Closes accepted during the read are merged by
    /// ID; a concurrent Cloud projection of the same close is not counted twice.
    pub(crate) fn try_apply_trade_history_recovery(
        &mut self,
        token: &TradeRecoveryToken,
        mut rebuild: TradeHistoryRebuild,
    ) -> Result<(), String> {
        if !self.is_trade_history_recovery_current(token) {
            return Err("Trade history recovery belongs to an obsolete attempt or account".into());
        }
        let capture = self
            .trade_recovery
            .take()
            .expect("validated recovery capture");
        if capture.overflowed {
            return Err("Trade history recovery exceeded the bounded concurrent-close capture; retry required".into());
        }
        for trade in capture.trades {
            if rebuild
                .staged
                .recorded_closed_insight_ids
                .insert(trade.insight_id)
            {
                rebuild.staged.record_closed_values(
                    trade.pnl,
                    trade.return_pct,
                    trade.held_secs,
                    trade.at,
                );
            }
        }
        if !self
            .recorded_closed_insight_ids
            .is_subset(&rebuild.staged.recorded_closed_insight_ids)
        {
            return Err("Trade history recovery is missing previously recorded closes; preserving current metrics".into());
        }
        self.apply_trade_history_rebuild(rebuild);
        Ok(())
    }

    /// Commit the finished trade-only rebuild. Account, open exposure, equity
    /// samples and drawdown remain owned by the live tracker.
    pub(crate) fn apply_trade_history_rebuild(&mut self, rebuild: TradeHistoryRebuild) {
        self.trade_recovery = None;
        let staged = rebuild.staged;
        self.total_trades = staged.total_trades;
        self.recorded_closed_insight_ids = staged.recorded_closed_insight_ids;
        self.winning_trades = staged.winning_trades;
        self.losing_trades = staged.losing_trades;
        self.gross_profit = staged.gross_profit;
        self.gross_loss_abs = staged.gross_loss_abs;
        self.sum_winner = staged.sum_winner;
        self.sum_loser_abs = staged.sum_loser_abs;
        self.sum_winner_pct = staged.sum_winner_pct;
        self.sum_loser_pct_abs = staged.sum_loser_pct_abs;
        self.best_trade = staged.best_trade;
        self.worst_trade = staged.worst_trade;
        self.sum_hold_secs = staged.sum_hold_secs;
        self.longest_winning_trade_held_secs = staged.longest_winning_trade_held_secs;
        self.longest_losing_trade_held_secs = staged.longest_losing_trade_held_secs;
        self.updated_at = self.updated_at.max(staged.updated_at);
        self.history_recovery_complete = true;
        self.last_persisted = None;
        self.last_persisted_checkpoint = None;
    }
}
