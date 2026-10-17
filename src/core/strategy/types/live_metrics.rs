use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::types::SurrealValue;

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
    pub(crate) fn sanitize_non_finite(mut self) -> Self {
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
