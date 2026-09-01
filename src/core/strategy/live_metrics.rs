use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use surrealdb::types::SurrealValue;

use crate::core::insight::InsightSnapshot;

pub const LIVE_METRICS_CHECKPOINT_VERSION: u16 = 1;

/// Versioned persistence state for the online live-performance accumulator.
/// The public metrics snapshot alone is not sufficient to resume Welford
/// variance, downside deviation, or an in-progress drawdown exactly.
#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue, PartialEq)]
pub struct LiveMetricsCheckpoint {
    pub version: u16,
    pub account_id: String,
    #[serde(default)]
    pub history_recovery_complete: bool,
    pub starting_cash: f64,
    pub current_equity: f64,
    pub peak_equity: f64,
    pub max_drawdown: f64,
    pub total_trades: usize,
    #[serde(default)]
    pub recorded_closed_insight_ids: Vec<String>,
    pub winning_trades: usize,
    pub losing_trades: usize,
    pub gross_profit: f64,
    pub gross_loss_abs: f64,
    pub sum_winner: f64,
    pub sum_loser_abs: f64,
    pub sum_winner_pct: f64,
    pub sum_loser_pct_abs: f64,
    pub best_trade: f64,
    pub worst_trade: f64,
    pub sum_hold_secs: i64,
    pub longest_winning_trade_held_secs: i64,
    pub longest_losing_trade_held_secs: i64,
    pub open_positions_count: usize,
    pub open_insights_count: usize,
    pub open_positions_unrealized_pnl: f64,
    pub open_positions_profitable_count: usize,
    pub open_positions_losing_count: usize,
    pub symbols: Vec<String>,
    pub return_count: u64,
    pub return_mean: f64,
    pub return_m2: f64,
    pub downside_return_count: u64,
    pub downside_return_sum_sq: f64,
    pub previous_equity: Option<f64>,
    pub underwater_start: Option<DateTime<Utc>>,
    pub max_drawdown_duration_secs: i64,
    pub executed_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, SurrealValue)]
pub(crate) struct HistoricalClosedInsightMetric {
    pub insight_id: String,
    pub side: String,
    pub quantity: Option<f64>,
    pub limit_price: Option<f64>,
    pub stop_price: Option<f64>,
    pub filled_price: Option<f64>,
    pub close_price: Option<f64>,
    pub broker_realized_pnl: Option<f64>,
    pub commission: Option<f64>,
    #[serde(default)]
    pub swap: Option<f64>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub filled_at: Option<DateTime<Utc>>,
    pub closed_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LiveMetricsSnapshot {
    pub starting_cash: f64,
    pub final_equity: f64,
    pub total_return: f64,
    pub total_return_pct: f64,
    pub total_trades: usize,
    pub winning_trades: usize,
    pub losing_trades: usize,
    pub win_rate: f64,
    pub max_drawdown: f64,
    pub cagr: f64,
    pub annualized_volatility: f64,
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub calmar_ratio: f64,
    pub max_drawdown_duration_days: f64,
    pub expectancy: f64,
    pub profit_factor: f64,
    pub payoff_ratio: f64,
    pub avg_winner: f64,
    pub avg_loser: f64,
    pub avg_winner_pct: f64,
    pub avg_loser_pct: f64,
    pub best_trade: f64,
    pub worst_trade: f64,
    pub consistency_score: f64,
    pub longest_winning_trade_held_secs: i64,
    pub longest_losing_trade_held_secs: i64,
    pub average_trade_held_secs: i64,
    pub open_positions_count: usize,
    pub open_insights_count: usize,
    pub open_positions_unrealized_pnl: f64,
    pub open_positions_profitable_count: usize,
    pub open_positions_losing_count: usize,
    pub symbols: Vec<String>,
    pub executed_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

impl LiveMetricsSnapshot {
    /// SurrealDB float fields and JSON cannot represent NaN or infinity. Keep
    /// the persistence snapshot total even while very young live sessions can
    /// temporarily produce unbounded annualized ratios.
    fn sanitize_non_finite(mut self) -> Self {
        for value in [
            &mut self.starting_cash,
            &mut self.final_equity,
            &mut self.total_return,
            &mut self.total_return_pct,
            &mut self.win_rate,
            &mut self.max_drawdown,
            &mut self.cagr,
            &mut self.annualized_volatility,
            &mut self.sharpe_ratio,
            &mut self.sortino_ratio,
            &mut self.calmar_ratio,
            &mut self.max_drawdown_duration_days,
            &mut self.expectancy,
            &mut self.profit_factor,
            &mut self.payoff_ratio,
            &mut self.avg_winner,
            &mut self.avg_loser,
            &mut self.avg_winner_pct,
            &mut self.avg_loser_pct,
            &mut self.best_trade,
            &mut self.worst_trade,
            &mut self.consistency_score,
            &mut self.open_positions_unrealized_pnl,
        ] {
            if !value.is_finite() {
                *value = 0.0;
            }
        }
        self
    }
}

#[derive(Clone, Debug, Default)]
pub struct LivePerformanceTracker {
    account_id: Option<String>,
    history_recovery_complete: bool,
    starting_cash: f64,
    current_equity: f64,
    peak_equity: f64,
    max_drawdown: f64,
    total_trades: usize,
    recorded_closed_insight_ids: HashSet<String>,
    winning_trades: usize,
    losing_trades: usize,
    gross_profit: f64,
    gross_loss_abs: f64,
    sum_winner: f64,
    sum_loser_abs: f64,
    sum_winner_pct: f64,
    sum_loser_pct_abs: f64,
    best_trade: f64,
    worst_trade: f64,
    sum_hold_secs: i64,
    longest_winning_trade_held_secs: i64,
    longest_losing_trade_held_secs: i64,
    open_positions_count: usize,
    open_insights_count: usize,
    open_positions_unrealized_pnl: f64,
    open_positions_profitable_count: usize,
    open_positions_losing_count: usize,
    symbols: Vec<String>,
    return_count: u64,
    return_mean: f64,
    return_m2: f64,
    downside_return_count: u64,
    downside_return_sum_sq: f64,
    previous_equity: Option<f64>,
    underwater_start: Option<DateTime<Utc>>,
    max_drawdown_duration_secs: i64,
    executed_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    updated_at: Option<DateTime<Utc>>,
    last_persisted: Option<LiveMetricsSnapshot>,
    last_persisted_checkpoint: Option<LiveMetricsCheckpoint>,
}

impl LivePerformanceTracker {
    pub fn newest_compatible_checkpoint(
        account_id: &str,
        checkpoints: impl IntoIterator<Item = LiveMetricsCheckpoint>,
    ) -> Option<LiveMetricsCheckpoint> {
        let account_id = account_id.trim();
        checkpoints
            .into_iter()
            .filter(|checkpoint| {
                checkpoint.version == LIVE_METRICS_CHECKPOINT_VERSION
                    && checkpoint.account_id.trim() == account_id
            })
            .max_by_key(|checkpoint| checkpoint.updated_at)
    }

    pub fn initialize(
        &mut self,
        starting_cash: f64,
        current_equity: f64,
        executed_at: DateTime<Utc>,
        symbols: impl IntoIterator<Item = String>,
    ) {
        self.initialize_for_account(None, starting_cash, current_equity, executed_at, symbols);
    }

    pub fn initialize_for_account(
        &mut self,
        account_id: Option<String>,
        starting_cash: f64,
        current_equity: f64,
        executed_at: DateTime<Utc>,
        symbols: impl IntoIterator<Item = String>,
    ) {
        self.account_id = account_id
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        self.history_recovery_complete = true;
        self.starting_cash = starting_cash;
        self.current_equity = current_equity;
        self.peak_equity = current_equity.max(starting_cash);
        self.max_drawdown = 0.0;
        self.total_trades = 0;
        self.recorded_closed_insight_ids.clear();
        self.winning_trades = 0;
        self.losing_trades = 0;
        self.gross_profit = 0.0;
        self.gross_loss_abs = 0.0;
        self.sum_winner = 0.0;
        self.sum_loser_abs = 0.0;
        self.sum_winner_pct = 0.0;
        self.sum_loser_pct_abs = 0.0;
        self.best_trade = 0.0;
        self.worst_trade = 0.0;
        self.sum_hold_secs = 0;
        self.longest_winning_trade_held_secs = 0;
        self.longest_losing_trade_held_secs = 0;
        self.open_positions_count = 0;
        self.open_insights_count = 0;
        self.open_positions_unrealized_pnl = 0.0;
        self.open_positions_profitable_count = 0;
        self.open_positions_losing_count = 0;
        self.symbols = symbols.into_iter().collect();
        self.return_count = 0;
        self.return_mean = 0.0;
        self.return_m2 = 0.0;
        self.downside_return_count = 0;
        self.downside_return_sum_sq = 0.0;
        self.previous_equity = Some(current_equity);
        self.underwater_start = None;
        self.max_drawdown_duration_secs = 0;
        self.executed_at = Some(executed_at);
        self.finished_at = None;
        self.updated_at = Some(executed_at);
        self.last_persisted = None;
        self.last_persisted_checkpoint = None;
    }

    pub fn restore(
        &mut self,
        checkpoint: LiveMetricsCheckpoint,
        current_equity: f64,
        at: DateTime<Utc>,
        current_symbols: impl IntoIterator<Item = String>,
    ) -> Result<(), String> {
        if checkpoint.version != LIVE_METRICS_CHECKPOINT_VERSION {
            return Err(format!(
                "unsupported live metrics checkpoint version {}",
                checkpoint.version
            ));
        }
        let account_id = checkpoint.account_id.trim();
        if account_id.is_empty() {
            return Err("live metrics checkpoint is missing its broker account id".to_string());
        }

        self.account_id = Some(account_id.to_string());
        self.history_recovery_complete = checkpoint.history_recovery_complete;
        self.starting_cash = checkpoint.starting_cash;
        self.current_equity = current_equity;
        self.peak_equity = checkpoint.peak_equity.max(current_equity);
        self.max_drawdown = checkpoint.max_drawdown;
        self.total_trades = checkpoint.total_trades;
        self.recorded_closed_insight_ids =
            checkpoint.recorded_closed_insight_ids.into_iter().collect();
        self.winning_trades = checkpoint.winning_trades;
        self.losing_trades = checkpoint.losing_trades;
        self.gross_profit = checkpoint.gross_profit;
        self.gross_loss_abs = checkpoint.gross_loss_abs;
        self.sum_winner = checkpoint.sum_winner;
        self.sum_loser_abs = checkpoint.sum_loser_abs;
        self.sum_winner_pct = checkpoint.sum_winner_pct;
        self.sum_loser_pct_abs = checkpoint.sum_loser_pct_abs;
        self.best_trade = checkpoint.best_trade;
        self.worst_trade = checkpoint.worst_trade;
        self.sum_hold_secs = checkpoint.sum_hold_secs;
        self.longest_winning_trade_held_secs = checkpoint.longest_winning_trade_held_secs;
        self.longest_losing_trade_held_secs = checkpoint.longest_losing_trade_held_secs;
        self.open_positions_count = checkpoint.open_positions_count;
        self.open_insights_count = checkpoint.open_insights_count;
        self.open_positions_unrealized_pnl = checkpoint.open_positions_unrealized_pnl;
        self.open_positions_profitable_count = checkpoint.open_positions_profitable_count;
        self.open_positions_losing_count = checkpoint.open_positions_losing_count;
        self.symbols = checkpoint.symbols;
        for symbol in current_symbols {
            if !self.symbols.contains(&symbol) {
                self.symbols.push(symbol);
            }
        }
        self.symbols.sort_unstable();
        self.symbols.dedup();
        self.return_count = checkpoint.return_count;
        self.return_mean = checkpoint.return_mean;
        self.return_m2 = checkpoint.return_m2;
        self.downside_return_count = checkpoint.downside_return_count;
        self.downside_return_sum_sq = checkpoint.downside_return_sum_sq;
        // Refreshing the broker value at process startup must not manufacture a
        // statistical return sample for the time while AQE was stopped.
        self.previous_equity = Some(current_equity);
        self.underwater_start = checkpoint.underwater_start;
        self.max_drawdown_duration_secs = checkpoint.max_drawdown_duration_secs;
        self.executed_at = Some(checkpoint.executed_at);
        self.finished_at = None;
        self.updated_at = Some(at);
        if current_equity < self.peak_equity {
            let drawdown = (self.peak_equity - current_equity) / self.peak_equity.max(f64::EPSILON);
            self.max_drawdown = self.max_drawdown.max(drawdown * 100.0);
            if self.underwater_start.is_none() {
                self.underwater_start = Some(at);
            }
        } else {
            self.underwater_start = None;
        }
        self.last_persisted = None;
        self.last_persisted_checkpoint = None;
        Ok(())
    }

    pub fn restore_legacy_snapshot(
        &mut self,
        snapshot: LiveMetricsSnapshot,
        account_id: String,
        current_equity: f64,
        at: DateTime<Utc>,
        current_symbols: impl IntoIterator<Item = String>,
    ) -> Result<(), String> {
        let annualizer = (252.0f64).sqrt();
        let sample_std_dev = (snapshot.annualized_volatility / annualizer).abs();
        let return_count = u64::from(sample_std_dev > f64::EPSILON) * 2;
        let return_mean = if sample_std_dev > f64::EPSILON {
            snapshot.sharpe_ratio * sample_std_dev / annualizer
        } else {
            0.0
        };
        let downside_deviation = if snapshot.sortino_ratio.abs() > f64::EPSILON {
            (return_mean * annualizer / snapshot.sortino_ratio).abs()
        } else {
            0.0
        };
        let checkpoint = LiveMetricsCheckpoint {
            version: LIVE_METRICS_CHECKPOINT_VERSION,
            account_id,
            history_recovery_complete: false,
            starting_cash: snapshot.starting_cash,
            current_equity: snapshot.final_equity,
            peak_equity: snapshot.final_equity.max(snapshot.starting_cash),
            max_drawdown: snapshot.max_drawdown,
            total_trades: snapshot.total_trades,
            recorded_closed_insight_ids: Vec::new(),
            winning_trades: snapshot.winning_trades,
            losing_trades: snapshot.losing_trades,
            gross_profit: snapshot.avg_winner.max(0.0) * snapshot.winning_trades as f64,
            gross_loss_abs: snapshot.avg_loser.abs() * snapshot.losing_trades as f64,
            sum_winner: snapshot.avg_winner.max(0.0) * snapshot.winning_trades as f64,
            sum_loser_abs: snapshot.avg_loser.abs() * snapshot.losing_trades as f64,
            sum_winner_pct: snapshot.avg_winner_pct.max(0.0) * snapshot.winning_trades as f64,
            sum_loser_pct_abs: snapshot.avg_loser_pct.abs() * snapshot.losing_trades as f64,
            best_trade: snapshot.best_trade,
            worst_trade: snapshot.worst_trade,
            sum_hold_secs: snapshot.average_trade_held_secs * snapshot.total_trades as i64,
            longest_winning_trade_held_secs: snapshot.longest_winning_trade_held_secs,
            longest_losing_trade_held_secs: snapshot.longest_losing_trade_held_secs,
            open_positions_count: snapshot.open_positions_count,
            open_insights_count: snapshot.open_insights_count,
            open_positions_unrealized_pnl: snapshot.open_positions_unrealized_pnl,
            open_positions_profitable_count: snapshot.open_positions_profitable_count,
            open_positions_losing_count: snapshot.open_positions_losing_count,
            symbols: snapshot.symbols,
            return_count,
            return_mean,
            return_m2: sample_std_dev.powi(2),
            downside_return_count: u64::from(downside_deviation > f64::EPSILON),
            downside_return_sum_sq: downside_deviation.powi(2),
            previous_equity: Some(snapshot.final_equity),
            underwater_start: None,
            max_drawdown_duration_secs: (snapshot.max_drawdown_duration_days * 86_400.0).round()
                as i64,
            executed_at: snapshot.executed_at,
            updated_at: snapshot.updated_at,
        };
        self.restore(checkpoint, current_equity, at, current_symbols)
    }

    pub fn checkpoint(&self) -> Option<LiveMetricsCheckpoint> {
        let mut recorded_closed_insight_ids = self
            .recorded_closed_insight_ids
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        recorded_closed_insight_ids.sort_unstable();
        Some(LiveMetricsCheckpoint {
            version: LIVE_METRICS_CHECKPOINT_VERSION,
            account_id: self.account_id.clone()?,
            history_recovery_complete: self.history_recovery_complete,
            starting_cash: self.starting_cash,
            current_equity: self.current_equity,
            peak_equity: self.peak_equity,
            max_drawdown: self.max_drawdown,
            total_trades: self.total_trades,
            recorded_closed_insight_ids,
            winning_trades: self.winning_trades,
            losing_trades: self.losing_trades,
            gross_profit: self.gross_profit,
            gross_loss_abs: self.gross_loss_abs,
            sum_winner: self.sum_winner,
            sum_loser_abs: self.sum_loser_abs,
            sum_winner_pct: self.sum_winner_pct,
            sum_loser_pct_abs: self.sum_loser_pct_abs,
            best_trade: self.best_trade,
            worst_trade: self.worst_trade,
            sum_hold_secs: self.sum_hold_secs,
            longest_winning_trade_held_secs: self.longest_winning_trade_held_secs,
            longest_losing_trade_held_secs: self.longest_losing_trade_held_secs,
            open_positions_count: self.open_positions_count,
            open_insights_count: self.open_insights_count,
            open_positions_unrealized_pnl: self.open_positions_unrealized_pnl,
            open_positions_profitable_count: self.open_positions_profitable_count,
            open_positions_losing_count: self.open_positions_losing_count,
            symbols: self.symbols.clone(),
            return_count: self.return_count,
            return_mean: self.return_mean,
            return_m2: self.return_m2,
            downside_return_count: self.downside_return_count,
            downside_return_sum_sq: self.downside_return_sum_sq,
            previous_equity: self.previous_equity,
            underwater_start: self.underwater_start,
            max_drawdown_duration_secs: self.max_drawdown_duration_secs,
            executed_at: self.executed_at?,
            updated_at: self.updated_at.unwrap_or_else(Utc::now),
        })
    }

    pub fn mark_history_recovery_incomplete(&mut self) {
        self.history_recovery_complete = false;
        self.last_persisted_checkpoint = None;
    }

    pub fn mark_history_recovery_complete(&mut self) {
        self.history_recovery_complete = true;
        self.last_persisted_checkpoint = None;
    }

    /// Merge a checkpoint that ends before this tracker began. This is used
    /// when AQE continued locally while Cloud continuity was temporarily
    /// unavailable. The non-overlap guard prevents replaying the same trades or
    /// return samples twice.
    pub fn merge_historical_checkpoint(
        &mut self,
        historical: LiveMetricsCheckpoint,
    ) -> Result<(), String> {
        let current_account = self
            .account_id
            .as_deref()
            .ok_or_else(|| "current live metrics have no broker account id".to_string())?;
        if historical.version != LIVE_METRICS_CHECKPOINT_VERSION {
            return Err(format!(
                "unsupported historical checkpoint version {}",
                historical.version
            ));
        }
        if historical.account_id.trim() != current_account {
            return Err("historical checkpoint belongs to a different broker account".to_string());
        }
        let current_started_at = self
            .executed_at
            .ok_or_else(|| "current live metrics are not initialized".to_string())?;
        if historical.updated_at > current_started_at {
            return Err(format!(
                "historical checkpoint overlaps this run ({} > {})",
                historical.updated_at, current_started_at
            ));
        }

        let current_total_trades = self.total_trades;
        if historical
            .recorded_closed_insight_ids
            .iter()
            .any(|insight_id| self.recorded_closed_insight_ids.contains(insight_id))
        {
            return Err(
                "historical checkpoint overlaps this run's closed insight identities".to_string(),
            );
        }
        self.starting_cash = historical.starting_cash;
        self.peak_equity = self.peak_equity.max(historical.peak_equity);
        self.max_drawdown = self.max_drawdown.max(historical.max_drawdown);
        self.total_trades += historical.total_trades;
        self.recorded_closed_insight_ids
            .extend(historical.recorded_closed_insight_ids);
        self.winning_trades += historical.winning_trades;
        self.losing_trades += historical.losing_trades;
        self.gross_profit += historical.gross_profit;
        self.gross_loss_abs += historical.gross_loss_abs;
        self.sum_winner += historical.sum_winner;
        self.sum_loser_abs += historical.sum_loser_abs;
        self.sum_winner_pct += historical.sum_winner_pct;
        self.sum_loser_pct_abs += historical.sum_loser_pct_abs;
        if historical.total_trades > 0 {
            self.best_trade = if current_total_trades > 0 {
                self.best_trade.max(historical.best_trade)
            } else {
                historical.best_trade
            };
            self.worst_trade = if current_total_trades > 0 {
                self.worst_trade.min(historical.worst_trade)
            } else {
                historical.worst_trade
            };
        }
        self.sum_hold_secs += historical.sum_hold_secs;
        self.longest_winning_trade_held_secs = self
            .longest_winning_trade_held_secs
            .max(historical.longest_winning_trade_held_secs);
        self.longest_losing_trade_held_secs = self
            .longest_losing_trade_held_secs
            .max(historical.longest_losing_trade_held_secs);
        for symbol in historical.symbols {
            if !self.symbols.contains(&symbol) {
                self.symbols.push(symbol);
            }
        }
        self.symbols.sort_unstable();
        self.symbols.dedup();

        let current_count = self.return_count;
        if historical.return_count > 0 {
            if current_count == 0 {
                self.return_count = historical.return_count;
                self.return_mean = historical.return_mean;
                self.return_m2 = historical.return_m2;
            } else {
                let left_count = historical.return_count as f64;
                let right_count = current_count as f64;
                let combined_count = left_count + right_count;
                let delta = self.return_mean - historical.return_mean;
                self.return_mean = (historical.return_mean * left_count
                    + self.return_mean * right_count)
                    / combined_count;
                self.return_m2 += historical.return_m2
                    + delta.powi(2) * left_count * right_count / combined_count;
                self.return_count += historical.return_count;
            }
        }
        self.downside_return_count += historical.downside_return_count;
        self.downside_return_sum_sq += historical.downside_return_sum_sq;
        self.max_drawdown_duration_secs = self
            .max_drawdown_duration_secs
            .max(historical.max_drawdown_duration_secs);
        self.executed_at = Some(historical.executed_at);
        self.history_recovery_complete &= historical.history_recovery_complete;
        self.last_persisted = None;
        self.last_persisted_checkpoint = None;
        Ok(())
    }

    pub fn set_continuity_origin(&mut self, starting_cash: f64, executed_at: DateTime<Utc>) {
        self.starting_cash = starting_cash;
        self.executed_at = Some(executed_at);
        self.last_persisted = None;
        self.last_persisted_checkpoint = None;
    }

    pub(crate) fn replace_trade_history(
        &mut self,
        records: impl IntoIterator<Item = HistoricalClosedInsightMetric>,
    ) {
        self.total_trades = 0;
        self.recorded_closed_insight_ids.clear();
        self.winning_trades = 0;
        self.losing_trades = 0;
        self.gross_profit = 0.0;
        self.gross_loss_abs = 0.0;
        self.sum_winner = 0.0;
        self.sum_loser_abs = 0.0;
        self.sum_winner_pct = 0.0;
        self.sum_loser_pct_abs = 0.0;
        self.best_trade = 0.0;
        self.worst_trade = 0.0;
        self.sum_hold_secs = 0;
        self.longest_winning_trade_held_secs = 0;
        self.longest_losing_trade_held_secs = 0;
        for record in records {
            if !self
                .recorded_closed_insight_ids
                .insert(record.insight_id.clone())
            {
                continue;
            }
            self.record_closed_values(
                historical_trade_pnl(&record),
                historical_trade_return_pct(&record),
                historical_trade_hold_secs(&record),
                record.closed_at.unwrap_or(record.updated_at),
            );
        }
        self.history_recovery_complete = true;
        self.last_persisted = None;
        self.last_persisted_checkpoint = None;
    }

    pub fn update_equity(&mut self, equity: f64, at: DateTime<Utc>) {
        if self.executed_at.is_none() {
            self.initialize(equity, equity, at, std::iter::empty());
            return;
        }

        if let Some(previous_equity) = self.previous_equity {
            if previous_equity.abs() > f64::EPSILON {
                let ret = (equity - previous_equity) / previous_equity;
                self.update_online_returns(ret);
                if ret < 0.0 {
                    self.downside_return_count += 1;
                    self.downside_return_sum_sq += ret.powi(2);
                }
            }
        }

        self.previous_equity = Some(equity);
        self.apply_current_equity(equity, at);
    }

    /// Refresh the latest equity without adding a statistical return sample.
    /// Account state is synchronized more frequently than risk metrics are sampled,
    /// so a faster UI refresh must not change Sharpe or volatility calculations.
    pub fn refresh_equity(&mut self, equity: f64, at: DateTime<Utc>) {
        if self.executed_at.is_none() {
            self.initialize(equity, equity, at, std::iter::empty());
            return;
        }
        if self.current_equity == equity {
            return;
        }
        self.apply_current_equity(equity, at);
    }

    fn apply_current_equity(&mut self, equity: f64, at: DateTime<Utc>) {
        self.current_equity = equity;
        if equity >= self.peak_equity {
            if let Some(start) = self.underwater_start.take() {
                self.max_drawdown_duration_secs = self
                    .max_drawdown_duration_secs
                    .max((at - start).num_seconds().max(0));
            }
            self.peak_equity = equity;
        } else if self.underwater_start.is_none() {
            self.underwater_start = Some(at);
        }
        if self.peak_equity.abs() > f64::EPSILON {
            let drawdown = (self.peak_equity - equity) / self.peak_equity;
            self.max_drawdown = self.max_drawdown.max(drawdown * 100.0);
        }
        self.updated_at = Some(at);
    }

    pub fn record_closed_insight(&mut self, snapshot: &InsightSnapshot) {
        if !self
            .recorded_closed_insight_ids
            .insert(snapshot.insight_id.clone())
        {
            return;
        }
        let pnl = trade_pnl(snapshot);
        let return_pct = trade_return_pct(snapshot);
        let hold_secs = trade_hold_secs(snapshot);

        self.record_closed_values(pnl, return_pct, hold_secs, Utc::now());
    }

    fn record_closed_values(
        &mut self,
        pnl: f64,
        return_pct: f64,
        hold_secs: i64,
        at: DateTime<Utc>,
    ) {
        self.total_trades += 1;
        self.sum_hold_secs += hold_secs;

        if pnl >= 0.0 {
            self.winning_trades += 1;
            self.gross_profit += pnl;
            self.sum_winner += pnl;
            self.sum_winner_pct += return_pct.max(0.0);
            self.best_trade = if self.total_trades == 1 {
                pnl
            } else {
                self.best_trade.max(pnl)
            };
            self.longest_winning_trade_held_secs =
                self.longest_winning_trade_held_secs.max(hold_secs);
        } else {
            let pnl_abs = pnl.abs();
            let return_abs = return_pct.abs();
            self.losing_trades += 1;
            self.gross_loss_abs += pnl_abs;
            self.sum_loser_abs += pnl_abs;
            self.sum_loser_pct_abs += return_abs;
            self.worst_trade = if self.total_trades == 1 {
                pnl
            } else {
                self.worst_trade.min(pnl)
            };
            self.longest_losing_trade_held_secs =
                self.longest_losing_trade_held_secs.max(hold_secs);
        }

        self.updated_at = Some(at);
    }

    pub fn update_open_position_metrics(
        &mut self,
        open_positions_count: usize,
        open_insights_count: usize,
        open_positions_unrealized_pnl: f64,
        open_positions_profitable_count: usize,
        open_positions_losing_count: usize,
        symbols: Vec<String>,
        at: DateTime<Utc>,
    ) {
        let changed = self.open_positions_count != open_positions_count
            || self.open_insights_count != open_insights_count
            || self.open_positions_unrealized_pnl != open_positions_unrealized_pnl
            || self.open_positions_profitable_count != open_positions_profitable_count
            || self.open_positions_losing_count != open_positions_losing_count
            || self.symbols != symbols;
        if !changed {
            return;
        }

        self.open_positions_count = open_positions_count;
        self.open_insights_count = open_insights_count;
        self.open_positions_unrealized_pnl = open_positions_unrealized_pnl;
        self.open_positions_profitable_count = open_positions_profitable_count;
        self.open_positions_losing_count = open_positions_losing_count;
        self.symbols = symbols;
        self.updated_at = Some(at);
    }

    pub fn finish(&mut self, at: DateTime<Utc>) {
        self.finished_at = Some(at);
        self.updated_at = Some(at);
    }

    pub fn snapshot(&self) -> Option<LiveMetricsSnapshot> {
        let executed_at = self.executed_at?;
        let updated_at = self.updated_at.unwrap_or(executed_at);
        let total_return = self.current_equity - self.starting_cash;
        let total_return_pct = if self.starting_cash.abs() > f64::EPSILON {
            (total_return / self.starting_cash) * 100.0
        } else {
            0.0
        };
        let win_rate = if self.total_trades > 0 {
            (self.winning_trades as f64 / self.total_trades as f64) * 100.0
        } else {
            0.0
        };
        let expectancy = if self.total_trades > 0 {
            (self.gross_profit - self.gross_loss_abs) / self.total_trades as f64
        } else {
            0.0
        };
        let profit_factor = if self.gross_loss_abs > f64::EPSILON {
            self.gross_profit / self.gross_loss_abs
        } else if self.gross_profit > 0.0 {
            self.gross_profit
        } else {
            0.0
        };
        let avg_winner = if self.winning_trades > 0 {
            self.sum_winner / self.winning_trades as f64
        } else {
            0.0
        };
        let avg_loser = if self.losing_trades > 0 {
            -(self.sum_loser_abs / self.losing_trades as f64)
        } else {
            0.0
        };
        let avg_winner_pct = if self.winning_trades > 0 {
            self.sum_winner_pct / self.winning_trades as f64
        } else {
            0.0
        };
        let avg_loser_pct = if self.losing_trades > 0 {
            -(self.sum_loser_pct_abs / self.losing_trades as f64)
        } else {
            0.0
        };
        let payoff_ratio = if avg_loser.abs() > f64::EPSILON {
            avg_winner / avg_loser.abs()
        } else if avg_winner > 0.0 {
            avg_winner
        } else {
            0.0
        };
        let consistency_score = consistency_score(win_rate / 100.0, profit_factor, payoff_ratio);
        let average_trade_held_secs = if self.total_trades > 0 {
            self.sum_hold_secs / self.total_trades as i64
        } else {
            0
        };

        Some(
            LiveMetricsSnapshot {
                starting_cash: self.starting_cash,
                final_equity: self.current_equity,
                total_return,
                total_return_pct,
                total_trades: self.total_trades,
                winning_trades: self.winning_trades,
                losing_trades: self.losing_trades,
                win_rate,
                max_drawdown: self.max_drawdown,
                cagr: self.cagr(updated_at),
                annualized_volatility: self.annualized_volatility(),
                sharpe_ratio: self.sharpe_ratio(),
                sortino_ratio: self.sortino_ratio(),
                calmar_ratio: self.calmar_ratio(updated_at),
                max_drawdown_duration_days: self.max_drawdown_duration_days(updated_at),
                expectancy,
                profit_factor,
                payoff_ratio,
                avg_winner,
                avg_loser,
                avg_winner_pct,
                avg_loser_pct,
                best_trade: if self.total_trades > 0 {
                    self.best_trade
                } else {
                    0.0
                },
                worst_trade: if self.total_trades > 0 {
                    self.worst_trade
                } else {
                    0.0
                },
                consistency_score,
                longest_winning_trade_held_secs: self.longest_winning_trade_held_secs,
                longest_losing_trade_held_secs: self.longest_losing_trade_held_secs,
                average_trade_held_secs,
                open_positions_count: self.open_positions_count,
                open_insights_count: self.open_insights_count,
                open_positions_unrealized_pnl: self.open_positions_unrealized_pnl,
                open_positions_profitable_count: self.open_positions_profitable_count,
                open_positions_losing_count: self.open_positions_losing_count,
                symbols: self.symbols.clone(),
                executed_at,
                finished_at: self.finished_at,
                updated_at,
            }
            .sanitize_non_finite(),
        )
    }

    pub fn should_persist(&self) -> bool {
        let Some(current) = self.snapshot() else {
            return false;
        };
        self.last_persisted.as_ref() != Some(&current)
            || self.last_persisted_checkpoint.as_ref() != self.checkpoint().as_ref()
    }

    pub fn mark_persisted(&mut self) {
        self.last_persisted = self.snapshot();
        self.last_persisted_checkpoint = self.checkpoint();
    }

    fn update_online_returns(&mut self, value: f64) {
        self.return_count += 1;
        let count = self.return_count as f64;
        let delta = value - self.return_mean;
        self.return_mean += delta / count;
        let delta2 = value - self.return_mean;
        self.return_m2 += delta * delta2;
    }

    fn sharpe_ratio(&self) -> f64 {
        if self.return_count < 2 {
            return 0.0;
        }
        let variance = self.return_m2 / (self.return_count as f64 - 1.0);
        if variance <= f64::EPSILON {
            return 0.0;
        }
        let std_dev = variance.sqrt();
        if std_dev <= f64::EPSILON {
            return 0.0;
        }
        self.return_mean / std_dev * (252.0f64).sqrt()
    }

    fn annualized_volatility(&self) -> f64 {
        if self.return_count < 2 {
            return 0.0;
        }
        let variance = self.return_m2 / (self.return_count as f64 - 1.0);
        if variance <= f64::EPSILON {
            return 0.0;
        }
        variance.sqrt() * (252.0f64).sqrt()
    }

    fn sortino_ratio(&self) -> f64 {
        if self.downside_return_count == 0 {
            return 0.0;
        }
        let downside_deviation =
            (self.downside_return_sum_sq / self.downside_return_count as f64).sqrt();
        if downside_deviation <= f64::EPSILON {
            return 0.0;
        }
        self.return_mean / downside_deviation * (252.0f64).sqrt()
    }

    fn cagr(&self, updated_at: DateTime<Utc>) -> f64 {
        let Some(executed_at) = self.executed_at else {
            return 0.0;
        };
        if self.starting_cash.abs() <= f64::EPSILON || self.current_equity <= 0.0 {
            return 0.0;
        }
        let days = (updated_at - executed_at).num_seconds().max(0) as f64 / 86_400.0;
        if days <= f64::EPSILON {
            return (self.current_equity - self.starting_cash) / self.starting_cash;
        }
        let years = days / 365.25;
        if years <= f64::EPSILON {
            return (self.current_equity - self.starting_cash) / self.starting_cash;
        }
        let cagr = (self.current_equity / self.starting_cash).powf(1.0 / years) - 1.0;
        if cagr.is_finite() { cagr } else { 0.0 }
    }

    fn calmar_ratio(&self, updated_at: DateTime<Utc>) -> f64 {
        let max_drawdown_fraction = self.max_drawdown / 100.0;
        if max_drawdown_fraction <= f64::EPSILON {
            return 0.0;
        }
        let calmar = self.cagr(updated_at) / max_drawdown_fraction.abs();
        if calmar.is_finite() { calmar } else { 0.0 }
    }

    fn max_drawdown_duration_days(&self, updated_at: DateTime<Utc>) -> f64 {
        let mut secs = self.max_drawdown_duration_secs;
        if let Some(start) = self.underwater_start {
            secs = secs.max((updated_at - start).num_seconds().max(0));
        }
        secs as f64 / 86_400.0
    }
}

fn trade_return_pct(snapshot: &InsightSnapshot) -> f64 {
    let Some(entry_price) = snapshot
        .filled_price
        .or(snapshot.limit_price)
        .or(snapshot.stop_price)
    else {
        return 0.0;
    };
    let Some(close_price) = snapshot.close_price else {
        return 0.0;
    };
    if entry_price.abs() <= f64::EPSILON {
        return 0.0;
    }
    let qty = snapshot.quantity.unwrap_or(0.0);
    if qty <= f64::EPSILON {
        return 0.0;
    }
    let gross_pnl = match snapshot.side.as_str() {
        "Buy" => (close_price - entry_price) * qty,
        "Sell" => (entry_price - close_price) * qty,
        _ => 0.0,
    };
    ((gross_pnl + snapshot.swap.unwrap_or(0.0) - snapshot.commission.unwrap_or(0.0))
        / (entry_price * qty))
        * 100.0
}

fn trade_pnl(snapshot: &InsightSnapshot) -> f64 {
    if let Some(realized_pnl) = snapshot.broker_realized_pnl {
        return realized_pnl;
    }

    let Some(entry_price) = snapshot
        .filled_price
        .or(snapshot.limit_price)
        .or(snapshot.stop_price)
    else {
        return 0.0;
    };
    let Some(close_price) = snapshot.close_price else {
        return 0.0;
    };
    let qty = snapshot.quantity.unwrap_or(0.0);
    let gross_pnl = match snapshot.side.as_str() {
        "Buy" => (close_price - entry_price) * qty,
        "Sell" => (entry_price - close_price) * qty,
        _ => 0.0,
    };
    gross_pnl + snapshot.swap.unwrap_or(0.0) - snapshot.commission.unwrap_or(0.0)
}

fn trade_hold_secs(snapshot: &InsightSnapshot) -> i64 {
    let start = snapshot.filled_at.or(Some(snapshot.created_at));
    let end = snapshot.closed_at.or(Some(snapshot.updated_at));
    match (start, end) {
        (Some(start), Some(end)) => (end - start).num_seconds().max(0),
        _ => 0,
    }
}

fn historical_trade_entry(record: &HistoricalClosedInsightMetric) -> Option<f64> {
    record
        .filled_price
        .or(record.limit_price)
        .or(record.stop_price)
}

fn historical_trade_return_pct(record: &HistoricalClosedInsightMetric) -> f64 {
    let Some(entry_price) = historical_trade_entry(record) else {
        return 0.0;
    };
    let Some(close_price) = record.close_price else {
        return 0.0;
    };
    let quantity = record.quantity.unwrap_or_default();
    if entry_price.abs() <= f64::EPSILON || quantity <= f64::EPSILON {
        return 0.0;
    }
    let gross_pnl = match record.side.as_str() {
        "Buy" => (close_price - entry_price) * quantity,
        "Sell" => (entry_price - close_price) * quantity,
        _ => 0.0,
    };
    ((gross_pnl + record.swap.unwrap_or_default() - record.commission.unwrap_or_default())
        / (entry_price * quantity))
        * 100.0
}

fn historical_trade_pnl(record: &HistoricalClosedInsightMetric) -> f64 {
    if let Some(realized_pnl) = record.broker_realized_pnl {
        return realized_pnl;
    }
    let Some(entry_price) = historical_trade_entry(record) else {
        return 0.0;
    };
    let Some(close_price) = record.close_price else {
        return 0.0;
    };
    let quantity = record.quantity.unwrap_or_default();
    let gross_pnl = match record.side.as_str() {
        "Buy" => (close_price - entry_price) * quantity,
        "Sell" => (entry_price - close_price) * quantity,
        _ => 0.0,
    };
    gross_pnl + record.swap.unwrap_or_default() - record.commission.unwrap_or_default()
}

fn historical_trade_hold_secs(record: &HistoricalClosedInsightMetric) -> i64 {
    let start = record.filled_at.unwrap_or(record.created_at);
    let end = record.closed_at.unwrap_or(record.updated_at);
    (end - start).num_seconds().max(0)
}

fn consistency_score(win_rate: f64, profit_factor: f64, payoff_ratio: f64) -> f64 {
    let pf_component = (profit_factor / 3.0).min(1.0);
    let payoff_component = (payoff_ratio / 3.0).min(1.0);
    ((win_rate + pf_component + payoff_component) / 3.0 * 100.0).clamp(0.0, 100.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeDelta;

    #[test]
    fn very_young_live_session_never_serializes_unbounded_metrics_as_null() {
        let started_at = Utc::now();
        let mut tracker = LivePerformanceTracker::default();
        tracker.initialize(100.0, 100.0, started_at, ["SOLUSD".to_string()]);
        tracker.update_equity(200.0, started_at + TimeDelta::seconds(1));
        tracker.update_open_position_metrics(
            1,
            1,
            f64::INFINITY,
            1,
            0,
            vec!["SOLUSD".to_string()],
            started_at + TimeDelta::seconds(1),
        );

        let snapshot = tracker.snapshot().expect("initialized live metrics");
        assert!(snapshot.cagr.is_finite());
        assert!(snapshot.calmar_ratio.is_finite());
        assert!(snapshot.open_positions_unrealized_pnl.is_finite());

        let json = serde_json::to_value(snapshot).expect("serializable live metrics");
        assert!(json["cagr"].is_number());
        assert!(json["calmar_ratio"].is_number());
        assert!(json["open_positions_unrealized_pnl"].is_number());
    }

    #[test]
    fn fast_equity_refresh_does_not_change_return_sampling_frequency() {
        let started_at = Utc::now();
        let mut tracker = LivePerformanceTracker::default();
        tracker.initialize(100.0, 100.0, started_at, ["SOLUSD".to_string()]);

        tracker.refresh_equity(101.0, started_at + TimeDelta::seconds(1));
        tracker.refresh_equity(102.0, started_at + TimeDelta::seconds(2));
        assert_eq!(tracker.return_count, 0);
        assert_eq!(tracker.previous_equity, Some(100.0));

        tracker.update_equity(103.0, started_at + TimeDelta::seconds(5));
        assert_eq!(tracker.return_count, 1);
        assert_eq!(tracker.previous_equity, Some(103.0));
        assert_eq!(tracker.snapshot().unwrap().final_equity, 103.0);
    }

    #[test]
    fn unchanged_open_position_metrics_do_not_mark_tracker_dirty() {
        let started_at = Utc::now();
        let mut tracker = LivePerformanceTracker::default();
        tracker.initialize(100.0, 100.0, started_at, ["SOLUSD".to_string()]);
        tracker.mark_persisted();

        tracker.update_open_position_metrics(
            0,
            0,
            0.0,
            0,
            0,
            vec!["SOLUSD".to_string()],
            started_at + TimeDelta::seconds(1),
        );

        assert!(!tracker.should_persist());
    }

    #[test]
    fn checkpoint_round_trip_preserves_accumulators_without_restart_sample() {
        let started_at = Utc::now() - TimeDelta::hours(3);
        let mut tracker = LivePerformanceTracker::default();
        tracker.initialize_for_account(
            Some(" broker-42 ".to_string()),
            100_000.0,
            100_000.0,
            started_at,
            ["ETHUSD".to_string()],
        );
        tracker.update_equity(101_000.0, started_at + TimeDelta::minutes(5));
        let checkpoint = tracker.checkpoint().expect("checkpoint");
        let checkpoint: LiveMetricsCheckpoint =
            serde_json::from_slice(&serde_json::to_vec(&checkpoint).expect("serialize checkpoint"))
                .expect("deserialize checkpoint");

        let mut restored = LivePerformanceTracker::default();
        restored
            .restore(
                checkpoint.clone(),
                101_500.0,
                started_at + TimeDelta::hours(3),
                ["SOLUSD".to_string()],
            )
            .expect("restore checkpoint");

        assert_eq!(restored.account_id.as_deref(), Some("broker-42"));
        assert_eq!(restored.return_count, checkpoint.return_count);
        assert_eq!(restored.previous_equity, Some(101_500.0));
        assert_eq!(restored.snapshot().unwrap().executed_at, started_at);
        assert_eq!(restored.snapshot().unwrap().finished_at, None);
        assert_eq!(restored.symbols, vec!["ETHUSD", "SOLUSD"]);
    }

    #[test]
    fn newest_checkpoint_is_selected_without_adding_accumulators() {
        let now = Utc::now();
        let checkpoint = |account: &str, updated_at| LiveMetricsCheckpoint {
            version: LIVE_METRICS_CHECKPOINT_VERSION,
            account_id: account.to_string(),
            history_recovery_complete: true,
            starting_cash: 100.0,
            current_equity: 100.0,
            peak_equity: 100.0,
            max_drawdown: 0.0,
            total_trades: if updated_at == now { 7 } else { 3 },
            recorded_closed_insight_ids: Vec::new(),
            winning_trades: 0,
            losing_trades: 0,
            gross_profit: 0.0,
            gross_loss_abs: 0.0,
            sum_winner: 0.0,
            sum_loser_abs: 0.0,
            sum_winner_pct: 0.0,
            sum_loser_pct_abs: 0.0,
            best_trade: 0.0,
            worst_trade: 0.0,
            sum_hold_secs: 0,
            longest_winning_trade_held_secs: 0,
            longest_losing_trade_held_secs: 0,
            open_positions_count: 0,
            open_insights_count: 0,
            open_positions_unrealized_pnl: 0.0,
            open_positions_profitable_count: 0,
            open_positions_losing_count: 0,
            symbols: vec![],
            return_count: 0,
            return_mean: 0.0,
            return_m2: 0.0,
            downside_return_count: 0,
            downside_return_sum_sq: 0.0,
            previous_equity: Some(100.0),
            underwater_start: None,
            max_drawdown_duration_secs: 0,
            executed_at: now - TimeDelta::hours(1),
            updated_at,
        };
        let selected = LivePerformanceTracker::newest_compatible_checkpoint(
            "account-a",
            [
                checkpoint("account-a", now - TimeDelta::minutes(1)),
                checkpoint("account-a", now),
                checkpoint("account-b", now + TimeDelta::minutes(1)),
            ],
        )
        .expect("matching checkpoint");
        assert_eq!(selected.total_trades, 7);
    }

    #[test]
    fn legacy_trade_rebuild_is_deterministic_and_idempotent() {
        let now = Utc::now();
        let mut records = vec![
            HistoricalClosedInsightMetric {
                insight_id: "winner".to_string(),
                side: "Buy".to_string(),
                quantity: Some(2.0),
                limit_price: None,
                stop_price: None,
                filled_price: Some(10.0),
                close_price: Some(12.0),
                broker_realized_pnl: Some(3.5),
                commission: Some(0.25),
                swap: Some(0.1),
                created_at: now - TimeDelta::minutes(20),
                updated_at: now - TimeDelta::minutes(10),
                filled_at: Some(now - TimeDelta::minutes(18)),
                closed_at: Some(now - TimeDelta::minutes(10)),
            },
            HistoricalClosedInsightMetric {
                insight_id: "loser".to_string(),
                side: "Sell".to_string(),
                quantity: Some(1.0),
                limit_price: None,
                stop_price: None,
                filled_price: Some(10.0),
                close_price: Some(11.0),
                broker_realized_pnl: None,
                commission: Some(0.2),
                swap: Some(-0.1),
                created_at: now - TimeDelta::minutes(9),
                updated_at: now,
                filled_at: Some(now - TimeDelta::minutes(8)),
                closed_at: Some(now),
            },
        ];
        records.insert(1, records[0].clone());
        let mut tracker = LivePerformanceTracker::default();
        tracker.initialize_for_account(
            Some("account".to_string()),
            100.0,
            102.2,
            now - TimeDelta::hours(1),
            std::iter::empty(),
        );

        tracker.replace_trade_history(records.clone());
        let first = tracker.checkpoint().unwrap();
        tracker.replace_trade_history(records);
        let second = tracker.checkpoint().unwrap();

        assert_eq!(first.total_trades, 2);
        assert_eq!(first.winning_trades, 1);
        assert_eq!(first.losing_trades, 1);
        assert_eq!(first.gross_profit, 3.5);
        assert!((first.gross_loss_abs - 1.3).abs() < 1e-9);
        assert_eq!(first.total_trades, second.total_trades);
        assert_eq!(first.gross_profit, second.gross_profit);
        assert_eq!(first.gross_loss_abs, second.gross_loss_abs);
        assert!(second.history_recovery_complete);
    }

    #[test]
    fn checkpoint_for_a_different_broker_account_is_not_compatible() {
        let now = Utc::now();
        let mut tracker = LivePerformanceTracker::default();
        tracker.initialize_for_account(
            Some("account-a".to_string()),
            100.0,
            100.0,
            now,
            std::iter::empty(),
        );
        assert!(
            LivePerformanceTracker::newest_compatible_checkpoint(
                "account-b",
                [tracker.checkpoint().unwrap()]
            )
            .is_none()
        );
    }

    #[test]
    fn non_overlapping_checkpoint_merge_combines_online_statistics_once() {
        let now = Utc::now();
        let mut historical_tracker = LivePerformanceTracker::default();
        historical_tracker.initialize_for_account(
            Some("account".to_string()),
            100.0,
            100.0,
            now - TimeDelta::hours(2),
            ["ETHUSD".to_string()],
        );
        historical_tracker.update_equity(101.0, now - TimeDelta::hours(1));
        let historical = historical_tracker.checkpoint().unwrap();

        let mut current = LivePerformanceTracker::default();
        current.initialize_for_account(
            Some("account".to_string()),
            101.0,
            101.0,
            now,
            ["SOLUSD".to_string()],
        );
        current.update_equity(102.0, now + TimeDelta::minutes(5));
        current
            .merge_historical_checkpoint(historical.clone())
            .expect("non-overlapping merge");

        assert_eq!(current.return_count, 2);
        assert_eq!(current.snapshot().unwrap().starting_cash, 100.0);
        assert_eq!(
            current.snapshot().unwrap().executed_at,
            historical.executed_at
        );
        assert_eq!(current.symbols, vec!["ETHUSD", "SOLUSD"]);
        assert!(
            current
                .merge_historical_checkpoint(historical)
                .unwrap_err()
                .contains("overlaps")
        );
    }
}
