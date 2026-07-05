use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet, VecDeque};

use std::path::{Path, PathBuf};

use super::types::{Account, Bar, OrderSide, TradeRecord, TradeRecordType};
use crate::core::events::{
    BacktestMarketStep, MarketDataEvent, MarketStreamKey, ResolvedEventStream,
};
use crate::core::insight::InsightSnapshot;
use crate::core::tui::BacktestProgressSnapshot;

fn is_terminal_insight_state(state: &str) -> bool {
    matches!(
        state.trim().to_ascii_lowercase().as_str(),
        "closed" | "cancelled" | "rejected"
    )
}

/// Shared state for backtesting. Held behind `Arc<parking_lot::RwLock<BacktestState>>`
/// by both the `PaperBroker` (execution) and `UnifiedBroker` (orchestrator).
///
/// Historical bars are stored per-symbol as `Vec<Bar>` for O(1) indexed access.
/// A Polars `DataFrame` copy is kept for the strategy layer via `historical_dataframes`.
#[derive(Debug)]
pub struct BacktestState {
    /// Raw bar data per symbol — used by PaperBroker for fast order fill lookups
    pub historical_bars: HashMap<String, Vec<Bar>>,
    /// Current bar index per symbol (incremented each step)
    pub bar_indices: HashMap<String, usize>,
    /// Raw bar data per registered market stream.
    pub event_stream_bars: HashMap<MarketStreamKey, Vec<Bar>>,
    /// Current bar index per registered market stream.
    pub event_stream_indices: HashMap<MarketStreamKey, usize>,
    /// Stream metadata keyed by registered market stream.
    pub event_streams: HashMap<MarketStreamKey, ResolvedEventStream>,
    /// Total number of bars to process (max across all symbols)
    pub total_bars: usize,
    /// Current simulation time
    pub current_time: DateTime<Utc>,
    /// Previous simulation time (for range queries)
    pub previous_time: Option<DateTime<Utc>>,
    /// Account equity snapshots over time
    pub account_history: Vec<(DateTime<Utc>, Account)>,
    /// Filled order history for results & trade log
    pub trade_log: Vec<TradeRecord>,
    /// Historical insight snapshots captured throughout the backtest.
    pub insight_snapshots: HashMap<String, InsightSnapshot>,
    /// Running minimum unrealized return per order, tracked while positions are open.
    pub trade_mae_by_order_id: HashMap<String, f64>,
    /// Configured backtest start timestamp.
    pub backtest_start: Option<DateTime<Utc>>,
    /// Configured backtest end timestamp.
    pub backtest_end: Option<DateTime<Utc>>,

    executed_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
}

impl BacktestState {
    pub fn new() -> Self {
        Self {
            historical_bars: HashMap::new(),
            bar_indices: HashMap::new(),
            event_stream_bars: HashMap::new(),
            event_stream_indices: HashMap::new(),
            event_streams: HashMap::new(),
            total_bars: 0,
            current_time: Utc::now(),
            previous_time: None,
            account_history: Vec::new(),
            trade_log: Vec::new(),
            insight_snapshots: HashMap::new(),
            trade_mae_by_order_id: HashMap::new(),
            backtest_start: None,
            backtest_end: None,
            executed_at: Utc::now(),
            finished_at: None,
        }
    }

    /// Load historical bars for a symbol.
    /// Automatically sets `total_bars` to the max bar count across all symbols.
    pub fn load_bars(&mut self, symbol: String, bars: Vec<Bar>) {
        let len = bars.len();
        self.bar_indices.insert(symbol.clone(), 0);
        self.historical_bars.insert(symbol, bars);
        self.total_bars = self.total_bars.max(len);
    }

    pub fn load_event_stream_bars(&mut self, stream: ResolvedEventStream, bars: Vec<Bar>) {
        let len = bars.len();
        let key = stream.key.clone();
        self.event_stream_indices.insert(key.clone(), 0);
        self.event_stream_bars.insert(key.clone(), bars);
        self.event_streams.insert(key, stream);
        self.total_bars = self.total_bars.max(len);
    }

    pub fn set_backtest_window(&mut self, start: DateTime<Utc>, end: DateTime<Utc>) {
        self.backtest_start = Some(start);
        self.backtest_end = Some(end);
    }

    /// Get the current bar for a symbol (at the current bar index).
    /// Returns `None` if the symbol doesn't exist or we've exhausted all bars.
    #[inline]
    pub fn get_current_bar(&self, symbol: &str) -> Option<&Bar> {
        let idx = *self.bar_indices.get(symbol)?;
        self.historical_bars.get(symbol)?.get(idx)
    }

    /// Get all current bars (one per symbol) for the current step.
    pub fn get_current_bars(&self) -> HashMap<String, Bar> {
        let mut bars = HashMap::with_capacity(self.bar_indices.len());
        for (symbol, &idx) in &self.bar_indices {
            if let Some(bar_vec) = self.historical_bars.get(symbol) {
                if let Some(bar) = bar_vec.get(idx) {
                    bars.insert(symbol.clone(), bar.clone());
                }
            }
        }
        bars
    }

    pub fn get_last_bars(&self) -> HashMap<String, Bar> {
        self.historical_bars
            .iter()
            .filter_map(|(symbol, bars)| bars.last().cloned().map(|bar| (symbol.clone(), bar)))
            .collect()
    }

    pub fn next_market_step(&mut self) -> Option<BacktestMarketStep> {
        let timestamp = self
            .event_stream_indices
            .iter()
            .filter_map(|(key, &idx)| {
                self.event_stream_bars
                    .get(key)
                    .and_then(|bars| bars.get(idx))
                    .map(|bar| bar.timestamp)
            })
            .min()?;

        self.previous_time = Some(self.current_time);
        self.current_time = timestamp;

        let mut events = Vec::new();
        let mut execution_bars = HashMap::new();
        let mut has_tradable_events = false;

        let mut keys: Vec<MarketStreamKey> = self.event_stream_indices.keys().cloned().collect();
        keys.sort_by(|left, right| {
            left.symbol.cmp(&right.symbol).then(
                left.timeframe
                    .compact_label()
                    .cmp(&right.timeframe.compact_label()),
            )
        });

        for key in keys {
            let Some(&idx) = self.event_stream_indices.get(&key) else {
                continue;
            };
            let Some(bar) = self
                .event_stream_bars
                .get(&key)
                .and_then(|bars| bars.get(idx))
                .filter(|bar| bar.timestamp == timestamp)
                .cloned()
            else {
                continue;
            };
            let Some(stream) = self.event_streams.get(&key).cloned() else {
                continue;
            };

            if stream.allow_trading {
                has_tradable_events = true;
                if !stream.is_feature {
                    execution_bars.insert(key.symbol.clone(), bar.clone());
                }
            }

            events.push(MarketDataEvent {
                context: stream.context_at(timestamp),
                bar,
            });
        }

        if events.is_empty() {
            return None;
        }

        Some(BacktestMarketStep {
            timestamp,
            events,
            execution_bars,
            has_tradable_events,
        })
    }

    pub fn advance_market_step(&mut self, step: &BacktestMarketStep) {
        for event in &step.events {
            let key = MarketStreamKey::new(
                event.context.event_type,
                event.context.symbol.clone(),
                event.context.timeframe,
            );
            if let Some(idx) = self.event_stream_indices.get_mut(&key) {
                *idx += 1;
            }
            if !event.context.is_feature {
                if let Some(idx) = self.bar_indices.get_mut(&event.context.symbol) {
                    *idx += 1;
                }
            }
        }
    }

    pub fn is_market_stream_complete(&self) -> bool {
        self.event_stream_indices.iter().all(|(key, &idx)| {
            self.event_stream_bars
                .get(key)
                .map(|bars| idx >= bars.len())
                .unwrap_or(true)
        })
    }

    pub fn progress_snapshot(&self) -> BacktestProgressSnapshot {
        let total_steps = self
            .event_stream_bars
            .values()
            .map(|bars| bars.len())
            .sum::<usize>();
        let processed_steps = self.event_stream_indices.values().copied().sum::<usize>();
        let progress_pct = if total_steps > 0 {
            (processed_steps.min(total_steps) as f64 / total_steps as f64) * 100.0
        } else {
            0.0
        };
        let tradable_stream_count = self
            .event_streams
            .values()
            .filter(|stream| stream.allow_trading)
            .count();
        BacktestProgressSnapshot {
            processed_steps: processed_steps.min(total_steps),
            total_steps,
            progress_pct,
            current_time: Some(self.current_time),
            stream_count: self.event_stream_bars.len(),
            tradable_stream_count,
        }
    }

    /// Advance all symbols to the next bar.
    /// Returns `false` if all symbols have been exhausted.
    pub fn advance(&mut self) -> bool {
        self.previous_time = Some(self.current_time);
        let mut any_remaining = false;

        for (symbol, idx) in self.bar_indices.iter_mut() {
            let max_idx = self
                .historical_bars
                .get(symbol)
                .map(|v| v.len())
                .unwrap_or(0);
            if *idx + 1 < max_idx {
                *idx += 1;
                any_remaining = true;
                // Update current_time to the latest bar's timestamp
                if let Some(bar) = self.historical_bars.get(symbol).and_then(|v| v.get(*idx)) {
                    if bar.timestamp > self.current_time {
                        self.current_time = bar.timestamp;
                    }
                }
            }
        }

        any_remaining
    }

    /// Check whether the backtest has completed (all bars consumed).
    #[inline]
    pub fn is_complete(&self) -> bool {
        self.bar_indices.iter().all(|(symbol, &idx)| {
            self.historical_bars
                .get(symbol)
                .map(|v| idx + 1 >= v.len())
                .unwrap_or(true)
        })
    }

    /// Snapshot the current account state for the equity curve.
    pub fn snapshot_account(&mut self, account: &Account) {
        self.account_history
            .push((self.current_time, account.clone()));
    }

    /// Record a filled trade.
    pub fn record_trade(&mut self, record: TradeRecord) {
        self.trade_log.push(record);
    }

    pub fn record_insight_snapshot(&mut self, snapshot: InsightSnapshot) {
        self.insight_snapshots
            .insert(snapshot.insight_id.clone(), snapshot);
    }

    pub fn record_insight_snapshots(&mut self, snapshots: Vec<InsightSnapshot>) {
        for snapshot in snapshots {
            self.record_insight_snapshot(snapshot);
        }
    }

    pub fn record_trade_mae(&mut self, order_id: &str, mae_pct: f64) {
        self.trade_mae_by_order_id
            .entry(order_id.to_string())
            .and_modify(|value| *value = value.min(mae_pct))
            .or_insert(mae_pct.min(0.0));
    }

    pub fn set_executed_at(&mut self, executed_at: DateTime<Utc>) {
        self.executed_at = executed_at;
    }

    pub fn set_finished_at(&mut self, finished_at: DateTime<Utc>) {
        self.finished_at = Some(finished_at);
    }

    pub fn get_executed_at(&self) -> DateTime<Utc> {
        self.executed_at
    }

    pub fn get_finished_at(&self) -> Option<DateTime<Utc>> {
        self.finished_at
    }
}
// ─────────────────────── Backtest Results ───────────────────────

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct BacktestResults {
    pub starting_cash: f64,
    pub final_equity: f64,
    pub total_return_pct: f64,
    pub total_trades: usize,
    pub winning_trades: usize,
    pub losing_trades: usize,
    pub win_rate: f64,
    pub max_drawdown: f64,
    #[serde(skip)]
    pub trade_log: Vec<TradeRecord>,
    #[serde(skip)]
    pub account_history: Vec<(DateTime<Utc>, Account)>,
    pub executed_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    #[serde(default)]
    pub backtest_start: Option<DateTime<Utc>>,
    #[serde(default)]
    pub backtest_end: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct BacktestMetrics {
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
    pub executed_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub backtest_start: Option<DateTime<Utc>>,
    pub backtest_end: Option<DateTime<Utc>>,
    pub symbols: Vec<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AccountHistoryItem {
    pub timestamp: DateTime<Utc>,
    pub equity: f64,
    #[serde(default)]
    pub accrued_commission: f64,
}

impl BacktestResults {
    // ─────────────── Internal: pair entries with exits ───────────────

    /// Build round-trip trades from the trade log by pairing Entry ↔ Exit
    /// records that share the same `order_id`.  Single-pass, O(n).
    pub(crate) fn round_trip_trades(&self) -> Vec<RoundTripTrade> {
        let mut entries: HashMap<String, (&TradeRecord, f64)> = HashMap::new();
        let mut trips = Vec::new();

        for rec in &self.trade_log {
            match rec.trade_type {
                TradeRecordType::Entry => {
                    entries.insert(rec.order_id.clone(), (rec, rec.qty));
                }
                TradeRecordType::Exit => {
                    if let Some((entry, remaining_qty)) = entries.get_mut(&rec.order_id) {
                        let matched_qty = rec.qty.min(*remaining_qty);
                        if matched_qty <= 0.0 {
                            continue;
                        }

                        let gross_pnl = match entry.side {
                            OrderSide::Buy => (rec.price - entry.price) * matched_qty,
                            OrderSide::Sell => (entry.price - rec.price) * matched_qty,
                        };
                        let entry_commission = if entry.qty.abs() > f64::EPSILON {
                            entry.commission * (matched_qty / entry.qty)
                        } else {
                            0.0
                        };
                        let commission = entry_commission + rec.commission;
                        let swap = rec.swap;
                        let pnl = gross_pnl + swap - commission;
                        let return_pct = if entry.price != 0.0 {
                            (pnl / (entry.price * matched_qty)) * 100.0
                        } else {
                            0.0
                        };
                        let hold_secs = (rec.date - entry.date).num_seconds();

                        trips.push(RoundTripTrade {
                            order_id: entry.order_id.clone(),
                            symbol: entry.symbol.clone(),
                            side: entry.side.clone(),
                            insight_id: entry.insight_id.clone(),
                            strategy_type: entry.strategy_type.clone(),
                            entry_time: entry.date,
                            exit_time: rec.date,
                            entry_price: entry.price,
                            exit_price: rec.price,
                            qty: matched_qty,
                            pnl,
                            commission,
                            swap,
                            return_pct,
                            hold_secs,
                        });

                        *remaining_qty -= matched_qty;
                        if *remaining_qty <= f64::EPSILON {
                            entries.remove(&rec.order_id);
                        }
                    }
                }
            }
        }
        trips
    }

    // ───────────────────── Risk-Adjusted Metrics ─────────────────────

    /// Annualised Sharpe ratio computed from daily equity returns.
    /// Uses 252 trading days. Returns 0.0 when there are fewer than 2 data points.
    pub fn sharpe_ratio(&self) -> f64 {
        let returns = self.equity_returns();
        annualized_sharpe(&returns)
    }

    pub fn annualized_volatility(&self) -> f64 {
        let returns = self.equity_returns();
        annualized_volatility(&returns)
    }

    pub fn sortino_ratio(&self) -> f64 {
        let returns = self.equity_returns();
        annualized_sortino(&returns)
    }

    pub fn cagr(&self) -> f64 {
        if self.starting_cash.abs() <= f64::EPSILON || self.final_equity <= 0.0 {
            return 0.0;
        }

        let Some((start, end)) = self.performance_window() else {
            return self.total_return_pct / 100.0;
        };

        let days = (end - start).num_seconds().max(0) as f64 / 86_400.0;
        let years = days / 365.25;
        if years <= f64::EPSILON {
            return self.total_return_pct / 100.0;
        }

        let cagr = (self.final_equity / self.starting_cash).powf(1.0 / years) - 1.0;
        if cagr.is_finite() {
            cagr
        } else {
            self.total_return_pct / 100.0
        }
    }

    pub fn calmar_ratio(&self) -> f64 {
        if self.max_drawdown <= f64::EPSILON {
            return 0.0;
        }
        let calmar = self.cagr() / self.max_drawdown.abs();
        if calmar.is_finite() { calmar } else { 0.0 }
    }

    fn performance_window(&self) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
        Self::valid_window(self.backtest_start, self.backtest_end)
            .or_else(|| {
                let start = self
                    .account_history
                    .first()
                    .map(|(timestamp, _)| *timestamp);
                let end = self.account_history.last().map(|(timestamp, _)| *timestamp);
                Self::valid_window(start, end)
            })
            .or_else(|| Self::valid_window(Some(self.executed_at), Some(self.finished_at)))
    }

    fn valid_window(
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
        let start = start?;
        let end = end?;
        (end > start).then_some((start, end))
    }

    pub fn max_drawdown_duration_days(&self) -> f64 {
        if self.account_history.len() < 2 {
            return 0.0;
        }

        let mut peak = self.account_history[0].1.equity;
        let mut underwater_start: Option<DateTime<Utc>> = None;
        let mut longest_secs = 0_i64;

        for (timestamp, account) in &self.account_history {
            if account.equity >= peak {
                if let Some(start) = underwater_start.take() {
                    longest_secs = longest_secs.max((*timestamp - start).num_seconds().max(0));
                }
                peak = account.equity;
            } else if underwater_start.is_none() {
                underwater_start = Some(*timestamp);
            }
        }

        if let (Some(start), Some((end, _))) = (underwater_start, self.account_history.last()) {
            longest_secs = longest_secs.max((*end - start).num_seconds().max(0));
        }

        longest_secs as f64 / 86_400.0
    }

    fn equity_returns(&self) -> Vec<f64> {
        let equities: Vec<f64> = self.account_history.iter().map(|(_, a)| a.equity).collect();
        equities
            .windows(2)
            .filter_map(|w| {
                if w[0] != 0.0 {
                    Some((w[1] - w[0]) / w[0])
                } else {
                    None
                }
            })
            .collect()
    }
}

fn annualized_sharpe(returns: &[f64]) -> f64 {
    let Some((mean, std_dev)) = return_mean_std(returns) else {
        return 0.0;
    };
    if std_dev <= f64::EPSILON {
        return 0.0;
    }
    (mean / std_dev) * (252.0_f64).sqrt()
}

fn annualized_volatility(returns: &[f64]) -> f64 {
    let Some((_, std_dev)) = return_mean_std(returns) else {
        return 0.0;
    };
    std_dev * (252.0_f64).sqrt()
}

fn annualized_sortino(returns: &[f64]) -> f64 {
    if returns.is_empty() {
        return 0.0;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let downside: Vec<f64> = returns
        .iter()
        .copied()
        .filter(|value| *value < 0.0)
        .collect();
    if downside.is_empty() {
        return 0.0;
    }
    let downside_deviation =
        (downside.iter().map(|value| value.powi(2)).sum::<f64>() / downside.len() as f64).sqrt();
    if downside_deviation <= f64::EPSILON {
        return 0.0;
    }
    (mean / downside_deviation) * (252.0_f64).sqrt()
}

fn return_mean_std(returns: &[f64]) -> Option<(f64, f64)> {
    if returns.len() < 2 {
        return None;
    }
    let n = returns.len() as f64;
    let mean = returns.iter().sum::<f64>() / n;
    let variance = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n;
    Some((mean, variance.sqrt()))
}

impl BacktestResults {
    // ───────────────────── Trade-Level Metrics ─────────────────────
    /// Expectancy = average PnL per trade (winners and losers combined).
    pub fn expectancy(&self) -> f64 {
        let trips = self.round_trip_trades();
        if trips.is_empty() {
            return 0.0;
        }
        trips.iter().map(|t| t.pnl).sum::<f64>() / trips.len() as f64
    }

    /// Profit factor = gross profit / gross loss (absolute).
    /// Returns f64::INFINITY when there are no losing trades.
    pub fn profit_factor(&self) -> f64 {
        let trips = self.round_trip_trades();
        let gross_profit: f64 = trips.iter().filter(|t| t.pnl > 0.0).map(|t| t.pnl).sum();
        let gross_loss: f64 = trips
            .iter()
            .filter(|t| t.pnl < 0.0)
            .map(|t| t.pnl.abs())
            .sum();
        if gross_loss == 0.0 {
            if gross_profit > 0.0 {
                f64::INFINITY
            } else {
                0.0
            }
        } else {
            gross_profit / gross_loss
        }
    }

    /// Payoff ratio = average winner / average loser (absolute).
    pub fn payoff_ratio(&self) -> f64 {
        let trips = self.round_trip_trades();
        let winners: Vec<f64> = trips
            .iter()
            .filter(|t| t.pnl > 0.0)
            .map(|t| t.pnl)
            .collect();
        let losers: Vec<f64> = trips
            .iter()
            .filter(|t| t.pnl < 0.0)
            .map(|t| t.pnl.abs())
            .collect();
        let avg_win = if winners.is_empty() {
            0.0
        } else {
            winners.iter().sum::<f64>() / winners.len() as f64
        };
        let avg_loss = if losers.is_empty() {
            0.0
        } else {
            losers.iter().sum::<f64>() / losers.len() as f64
        };
        if avg_loss == 0.0 {
            0.0
        } else {
            avg_win / avg_loss
        }
    }

    // ───────────────── Hold Duration Metrics ─────────────────────

    /// Average trade held in seconds.
    pub fn average_trade_held_secs(&self) -> i64 {
        let trips = self.round_trip_trades();
        if trips.is_empty() {
            return 0;
        }
        trips.iter().map(|t| t.hold_secs).sum::<i64>() / trips.len() as i64
    }

    /// Longest‐held **winning** trade in seconds.
    pub fn longest_winning_trade_held_secs(&self) -> i64 {
        self.round_trip_trades()
            .iter()
            .filter(|t| t.pnl > 0.0)
            .map(|t| t.hold_secs)
            .max()
            .unwrap_or(0)
    }

    /// Longest‐held **losing** trade in seconds.
    pub fn longest_losing_trade_held_secs(&self) -> i64 {
        self.round_trip_trades()
            .iter()
            .filter(|t| t.pnl < 0.0)
            .map(|t| t.hold_secs)
            .max()
            .unwrap_or(0)
    }

    // ──────────────── Long / Short Breakdowns ────────────────────

    /// Average return **%** for **long** (Buy‐side) trades.
    pub fn avg_return_pct_long(&self) -> f64 {
        let v: Vec<f64> = self
            .round_trip_trades()
            .iter()
            .filter(|t| t.side == OrderSide::Buy)
            .map(|t| t.return_pct)
            .collect();
        if v.is_empty() {
            0.0
        } else {
            v.iter().sum::<f64>() / v.len() as f64
        }
    }

    /// Average return **%** for **short** (Sell‐side) trades.
    pub fn avg_return_pct_short(&self) -> f64 {
        let v: Vec<f64> = self
            .round_trip_trades()
            .iter()
            .filter(|t| t.side == OrderSide::Sell)
            .map(|t| t.return_pct)
            .collect();
        if v.is_empty() {
            0.0
        } else {
            v.iter().sum::<f64>() / v.len() as f64
        }
    }

    /// Average nominal (dollar) PnL for **long** trades.
    pub fn avg_nominal_long(&self) -> f64 {
        let v: Vec<f64> = self
            .round_trip_trades()
            .iter()
            .filter(|t| t.side == OrderSide::Buy)
            .map(|t| t.pnl)
            .collect();
        if v.is_empty() {
            0.0
        } else {
            v.iter().sum::<f64>() / v.len() as f64
        }
    }

    /// Average nominal (dollar) PnL for **short** trades.
    pub fn avg_nominal_short(&self) -> f64 {
        let v: Vec<f64> = self
            .round_trip_trades()
            .iter()
            .filter(|t| t.side == OrderSide::Sell)
            .map(|t| t.pnl)
            .collect();
        if v.is_empty() {
            0.0
        } else {
            v.iter().sum::<f64>() / v.len() as f64
        }
    }

    // ──────────────── Winner / Loser Averages ────────────────────

    /// Average $ PnL of winning trades.
    pub fn avg_winner(&self) -> f64 {
        let v: Vec<f64> = self
            .round_trip_trades()
            .iter()
            .filter(|t| t.pnl > 0.0)
            .map(|t| t.pnl)
            .collect();
        if v.is_empty() {
            0.0
        } else {
            v.iter().sum::<f64>() / v.len() as f64
        }
    }

    /// Average $ PnL of losing trades (returned as positive value).
    pub fn avg_loser(&self) -> f64 {
        let v: Vec<f64> = self
            .round_trip_trades()
            .iter()
            .filter(|t| t.pnl < 0.0)
            .map(|t| t.pnl.abs())
            .collect();
        if v.is_empty() {
            0.0
        } else {
            v.iter().sum::<f64>() / v.len() as f64
        }
    }

    /// Average % return of winning trades.
    pub fn avg_winner_pct(&self) -> f64 {
        let v: Vec<f64> = self
            .round_trip_trades()
            .iter()
            .filter(|t| t.pnl > 0.0)
            .map(|t| t.return_pct)
            .collect();
        if v.is_empty() {
            0.0
        } else {
            v.iter().sum::<f64>() / v.len() as f64
        }
    }

    /// Average % return of losing trades (returned as positive value).
    pub fn avg_loser_pct(&self) -> f64 {
        let v: Vec<f64> = self
            .round_trip_trades()
            .iter()
            .filter(|t| t.pnl < 0.0)
            .map(|t| t.return_pct.abs())
            .collect();
        if v.is_empty() {
            0.0
        } else {
            v.iter().sum::<f64>() / v.len() as f64
        }
    }

    // ──────────────── Convenience: count long/short ──────────────

    fn count_long_short(&self) -> (usize, usize) {
        let trips = self.round_trip_trades();
        let longs = trips.iter().filter(|t| t.side == OrderSide::Buy).count();
        let shorts = trips.iter().filter(|t| t.side == OrderSide::Sell).count();
        (longs, shorts)
    }

    // ──────────────── Pretty-Print All Metrics ──────────────────

    fn format_duration_seconds(secs: i64) -> String {
        if secs == 0 {
            return "N/A".into();
        }
        let d = secs / 86_400;
        let h = (secs % 86_400) / 3_600;
        let m = (secs % 3_600) / 60;
        let s = secs % 60;
        if d > 0 {
            format!("{}d {}h {}m {}s", d, h, m, s)
        } else if h > 0 {
            format!("{}h {}m {}s", h, m, s)
        } else {
            format!("{}m {}s", m, s)
        }
    }

    pub fn metric_summary_lines(&self) -> Vec<String> {
        let (n_long, n_short) = self.count_long_short();
        vec![
            "═══════════════════════════════════════════════".to_string(),
            "            BACKTEST RESULTS".to_string(),
            "═══════════════════════════════════════════════".to_string(),
            format!("  Starting Cash:     ${:.2}", self.starting_cash),
            format!("  Final Equity:      ${:.2}", self.final_equity),
            format!("  Total Return:      {:.2}%", self.total_return_pct),
            format!("  Max Drawdown:      {:.2}%", self.max_drawdown * 100.0),
            "  ─────────────────────────────────────────────".to_string(),
            format!("  Sharpe Ratio:      {:.4}", self.sharpe_ratio()),
            format!("  Expectancy:        ${:.2}", self.expectancy()),
            format!("  Profit Factor:     {:.2}", self.profit_factor()),
            format!("  Payoff Ratio:      {:.2}", self.payoff_ratio()),
            "  ─────────────────────────────────────────────".to_string(),
            format!("  Total Trades:      {}", self.total_trades),
            format!("  Winning Trades:    {}", self.winning_trades),
            format!("  Losing Trades:     {}", self.losing_trades),
            format!("  Win Rate:          {:.2}%", self.win_rate * 100.0),
            "  ─────────────────────────────────────────────".to_string(),
            format!(
                "  Avg Winner:        ${:.2}  ({:.2}%)",
                self.avg_winner(),
                self.avg_winner_pct()
            ),
            format!(
                "  Avg Loser:        -${:.2}  ({:.2}%)",
                self.avg_loser(),
                self.avg_loser_pct()
            ),
            format!(
                "  Longest Win Held:  {}",
                Self::format_duration_seconds(self.longest_winning_trade_held_secs())
            ),
            format!(
                "  Longest Loss Held: {}",
                Self::format_duration_seconds(self.longest_losing_trade_held_secs())
            ),
            format!(
                "  Average Trade Held: {}",
                Self::format_duration_seconds(self.average_trade_held_secs())
            ),
            "  ─────────────────────────────────────────────".to_string(),
            format!("  Long Trades:       {}", n_long),
            format!(
                "  Avg Return (L):    {:.2}%  (${:.2})",
                self.avg_return_pct_long(),
                self.avg_nominal_long()
            ),
            format!("  Short Trades:      {}", n_short),
            format!(
                "  Avg Return (S):    {:.2}%  (${:.2})",
                self.avg_return_pct_short(),
                self.avg_nominal_short()
            ),
            "  ─────────────────────────────────────────────".to_string(),
            format!("  Executed At:       {:?}", self.executed_at),
            format!("  Finished At:       {:?}", self.finished_at),
            format!(
                "  Duration:          {}",
                Self::format_duration_seconds(
                    self.finished_at.timestamp() - self.executed_at.timestamp()
                )
            ),
            format!("  History Points:    {}", self.account_history.len()),
            format!("  Trade Log Size:    {}", self.trade_log.len()),
            "═══════════════════════════════════════════════".to_string(),
        ]
    }

    /// Print a comprehensive, formatted summary of all backtest metrics.
    pub fn print_metrics(&self) {
        if crate::core::tui::terminal_output_suspended() {
            return;
        }
        for line in self.metric_summary_lines() {
            println!("{line}");
        }
    }
}

/// A completed entry→exit round-trip trade used for metric calculations.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RoundTripTrade {
    pub order_id: String,
    pub symbol: String,
    pub side: OrderSide,
    #[serde(default)]
    pub insight_id: Option<String>,
    #[serde(default)]
    pub strategy_type: Option<String>,
    pub entry_time: chrono::DateTime<chrono::Utc>,
    pub exit_time: chrono::DateTime<chrono::Utc>,
    pub entry_price: f64,
    pub exit_price: f64,
    pub qty: f64,
    /// Dollar PnL (positive = profit, negative = loss).
    pub pnl: f64,
    #[serde(default)]
    pub commission: f64,
    #[serde(default)]
    pub swap: f64,
    /// % return relative to entry price.
    pub return_pct: f64,
    /// How long the position was held (seconds).
    pub hold_secs: i64,
}

#[cfg(feature = "runtime")]
fn is_aqmeta_path(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("aqmeta"))
}

#[cfg(feature = "runtime")]
fn find_current_project_aqmeta() -> Option<PathBuf> {
    let project_dir = std::env::current_dir().ok()?;
    std::fs::read_dir(project_dir)
        .ok()?
        .flatten()
        .map(|entry| entry.path())
        .find(|path| is_aqmeta_path(path))
}

#[cfg(feature = "runtime")]
fn json_string(value: Option<&serde_json::Value>, fallback: &str) -> serde_json::Value {
    serde_json::Value::String(
        value
            .and_then(|value| value.as_str())
            .unwrap_or(fallback)
            .to_string(),
    )
}

#[cfg(feature = "runtime")]
fn display_data_feed(value: Option<&serde_json::Value>) -> serde_json::Value {
    let label = match value.and_then(|value| value.as_str()).unwrap_or_default() {
        "YahooFinance" | "yahooFinance" | "YAHOO_FINANCE" => "Yahoo Finance",
        "Mt5" | "mt5" | "MT5" => "MT5",
        other if !other.is_empty() => other,
        _ => "Yahoo Finance",
    };
    serde_json::Value::String(label.to_string())
}

#[cfg(feature = "runtime")]
fn display_execution_broker(value: Option<&serde_json::Value>) -> serde_json::Value {
    let label = match value.and_then(|value| value.as_str()).unwrap_or_default() {
        "Paper" | "paper" | "PAPER" => "Paper Broker",
        "Mt5" | "mt5" | "MT5" => "MT5 Broker",
        other if !other.is_empty() => other,
        _ => "Paper Broker",
    };
    serde_json::Value::String(label.to_string())
}

#[cfg(feature = "runtime")]
fn object_field<'a>(
    object: &'a serde_json::Value,
    camel_case: &str,
    snake_case: &str,
) -> Option<&'a serde_json::Value> {
    object.get(camel_case).or_else(|| object.get(snake_case))
}

#[cfg(feature = "runtime")]
fn node_id(node: &serde_json::Value) -> Option<String> {
    node.get("id")
        .and_then(|value| value.as_str())
        .map(str::to_string)
}

#[cfg(feature = "runtime")]
fn node_type_matches(node: &serde_json::Value, expected: &str) -> bool {
    node.get("type")
        .or_else(|| node.get("nodeType"))
        .and_then(|value| value.as_str())
        .is_some_and(|value| value.eq_ignore_ascii_case(expected))
}

#[cfg(feature = "runtime")]
fn endpoint_node_id(endpoint: &serde_json::Value) -> Option<String> {
    object_field(endpoint, "nodeId", "node_id")
        .and_then(|value| value.as_str())
        .map(str::to_string)
}

#[cfg(feature = "runtime")]
fn endpoint_port(endpoint: &serde_json::Value) -> Option<&str> {
    endpoint
        .get("port")
        .or_else(|| endpoint.get("input"))
        .or_else(|| endpoint.get("output"))
        .and_then(|value| value.as_str())
}

#[cfg(feature = "runtime")]
fn reachable_aqmeta_node_ids(aqmeta: &serde_json::Value) -> HashSet<String> {
    let nodes = aqmeta
        .get("nodes")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();

    let all_ids: HashSet<String> = nodes.iter().filter_map(node_id).collect();
    let roots: Vec<String> = nodes
        .iter()
        .filter(|node| node_type_matches(node, "strategy"))
        .filter_map(node_id)
        .collect();

    if roots.is_empty() {
        return all_ids;
    }

    let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();
    if let Some(connections) = aqmeta.get("connections").and_then(|value| value.as_array()) {
        for connection in connections {
            let to_endpoint = connection.get("to");
            let Some(from_id) = connection.get("from").and_then(endpoint_node_id) else {
                continue;
            };
            let Some(to_id) = to_endpoint.and_then(endpoint_node_id) else {
                continue;
            };
            adjacency
                .entry(from_id.clone())
                .or_default()
                .push(to_id.clone());
            if to_endpoint
                .and_then(endpoint_port)
                .is_some_and(|port| port == "insights_pipes")
            {
                adjacency.entry(to_id).or_default().push(from_id);
            }
        }
    }

    let mut reachable = HashSet::new();
    let mut queue: VecDeque<String> = roots.into_iter().collect();
    while let Some(node_id) = queue.pop_front() {
        if !reachable.insert(node_id.clone()) {
            continue;
        }
        if let Some(children) = adjacency.get(&node_id) {
            for child in children {
                queue.push_back(child.clone());
            }
        }
    }

    reachable
}

#[cfg(feature = "runtime")]
fn aqmeta_node_snapshot(aqmeta: &serde_json::Value, expected_type: &str) -> serde_json::Value {
    let reachable = reachable_aqmeta_node_ids(aqmeta);
    let nodes = aqmeta
        .get("nodes")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();

    let snapshot = nodes
        .iter()
        .filter(|node| node_type_matches(node, expected_type))
        .filter(|node| {
            node_id(node)
                .as_ref()
                .is_some_and(|id| reachable.contains(id))
        })
        .map(|node| {
            let inputs = node
                .get("inputs")
                .and_then(|value| value.as_array())
                .cloned()
                .unwrap_or_default()
                .iter()
                .map(|input| {
                    serde_json::json!({
                        "name": input.get("name").cloned().unwrap_or(serde_json::Value::Null),
                        "type": object_field(input, "inputType", "input_type")
                            .or_else(|| input.get("type"))
                            .cloned()
                            .unwrap_or(serde_json::Value::Null),
                        "value": input.get("value").cloned().unwrap_or(serde_json::Value::Null),
                        "isPublic": object_field(input, "isPublic", "is_public")
                            .cloned()
                            .unwrap_or(serde_json::Value::Bool(true)),
                    })
                })
                .collect::<Vec<_>>();

            serde_json::json!({
                "id": node.get("id").cloned().unwrap_or(serde_json::Value::Null),
                "label": node.get("label").cloned().unwrap_or(serde_json::Value::Null),
                "sourceFile": object_field(node, "sourceFile", "source_file")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
                "inputs": inputs,
            })
        })
        .collect::<Vec<_>>();

    serde_json::Value::Array(snapshot)
}

#[cfg(feature = "runtime")]
fn backtest_time_value(value: Option<DateTime<Utc>>) -> serde_json::Value {
    value
        .map(|value| serde_json::Value::String(value.to_rfc3339()))
        .unwrap_or(serde_json::Value::Null)
}

#[cfg(feature = "runtime")]
fn build_aqmeta_run_config(
    aqmeta: &serde_json::Value,
    state: &BacktestState,
    aqmeta_copied: bool,
) -> serde_json::Value {
    let config = aqmeta
        .get("config")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));

    serde_json::json!({
        "strategyId": aqmeta.get("id").cloned().unwrap_or(serde_json::Value::Null),
        "strategyName": json_string(aqmeta.get("name"), "Unknown Strategy"),
        "strategyVersion": json_string(aqmeta.get("version"), "1.0.0"),
        "dataFeed": display_data_feed(aqmeta.get("dataFeed")),
        "dataFeedId": object_field(aqmeta, "dataFeedId", "data_feed_id")
            .cloned()
            .unwrap_or(serde_json::Value::Null),
        "executionBroker": display_execution_broker(aqmeta.get("broker")),
        "brokerId": object_field(aqmeta, "brokerId", "broker_id")
            .cloned()
            .unwrap_or(serde_json::Value::Null),
        "backtestExecutionBroker": "Paper Broker",
        "brokerLeverage": config
            .get("brokerLeverage")
            .cloned()
            .unwrap_or_else(|| serde_json::json!(1)),
        "backtestStart": backtest_time_value(state.backtest_start),
        "backtestEnd": backtest_time_value(state.backtest_end),
        "timeframeAmount": config
            .get("timeframeAmount")
            .cloned()
            .unwrap_or_else(|| serde_json::json!(1)),
        "timeframeUnit": config
            .get("timeframeUnit")
            .cloned()
            .unwrap_or_else(|| serde_json::json!("Minute")),
        "strategyConfig": config,
        "alphaModels": aqmeta_node_snapshot(aqmeta, "alpha"),
        "insightPipes": aqmeta_node_snapshot(aqmeta, "pipe"),
        "aqmetaSnapshot": if aqmeta_copied {
            serde_json::Value::String("strategy.aqmeta".to_string())
        } else {
            serde_json::Value::Null
        },
    })
}

#[cfg(feature = "runtime")]
fn attach_run_config_to_metrics(metrics: &mut serde_json::Value, run_config: serde_json::Value) {
    let Some(obj) = metrics.as_object_mut() else {
        return;
    };

    obj.insert("run_config".to_string(), run_config.clone());
    obj.insert("data_feed".to_string(), run_config["dataFeed"].clone());
    obj.insert("data_feed_id".to_string(), run_config["dataFeedId"].clone());
    obj.insert(
        "execution_broker".to_string(),
        run_config["executionBroker"].clone(),
    );
    obj.insert("broker_id".to_string(), run_config["brokerId"].clone());
    obj.insert(
        "backtest_execution_broker".to_string(),
        run_config["backtestExecutionBroker"].clone(),
    );
    obj.insert(
        "broker_leverage".to_string(),
        run_config["brokerLeverage"].clone(),
    );
    obj.insert(
        "backtest_start".to_string(),
        run_config["backtestStart"].clone(),
    );
    obj.insert(
        "backtest_end".to_string(),
        run_config["backtestEnd"].clone(),
    );
    obj.insert(
        "timeframe_amount".to_string(),
        run_config["timeframeAmount"].clone(),
    );
    obj.insert(
        "timeframe_unit".to_string(),
        run_config["timeframeUnit"].clone(),
    );
    obj.insert(
        "alpha_models".to_string(),
        run_config["alphaModels"].clone(),
    );
    obj.insert(
        "insight_pipes".to_string(),
        run_config["insightPipes"].clone(),
    );
    obj.insert(
        "aqmeta_snapshot".to_string(),
        run_config["aqmetaSnapshot"].clone(),
    );
}

#[cfg(feature = "runtime")]
fn enrich_backtest_metrics_with_aqmeta_path(
    metrics: &mut serde_json::Value,
    dir_path: &Path,
    state: &BacktestState,
    aqmeta_path: &Path,
) -> Result<(), String> {
    let aqmeta_copied = std::fs::copy(aqmeta_path, dir_path.join("strategy.aqmeta")).is_ok();
    let content = std::fs::read_to_string(aqmeta_path).map_err(|error| error.to_string())?;
    let aqmeta =
        serde_json::from_str::<serde_json::Value>(&content).map_err(|error| error.to_string())?;
    let run_config = build_aqmeta_run_config(&aqmeta, state, aqmeta_copied);
    attach_run_config_to_metrics(metrics, run_config);
    Ok(())
}

#[cfg(feature = "runtime")]
fn enrich_backtest_metrics_with_project_aqmeta(
    metrics: &mut serde_json::Value,
    dir_path: &Path,
    state: &BacktestState,
) {
    let Some(aqmeta_path) = find_current_project_aqmeta() else {
        return;
    };

    let _ = enrich_backtest_metrics_with_aqmeta_path(metrics, dir_path, state, &aqmeta_path);
}

impl BacktestResults {
    /// Save the results to the specified directory.
    /// Metrics are saved as JSON and the larger backtest artifacts are stored in `backtest.db`.
    #[cfg(feature = "runtime")]
    pub fn save_to_disk(&self, dir_path: &Path, state: &BacktestState) -> Result<(), String> {
        std::thread::scope(|scope| {
            let handle = scope.spawn(|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| e.to_string())?;
                runtime.block_on(self.save_to_disk_async(dir_path, state))
            });

            handle
                .join()
                .map_err(|_| "Backtest persistence thread panicked".to_string())?
        })
    }

    /// Async backtest artifact writer used by newer generated strategy code.
    #[cfg(feature = "runtime")]
    pub async fn save_to_disk_async(
        &self,
        dir_path: &Path,
        state: &BacktestState,
    ) -> Result<(), String> {
        std::fs::create_dir_all(dir_path).map_err(|e| e.to_string())?;

        let mut symbols: Vec<String> = state.historical_bars.keys().cloned().collect();
        if symbols.is_empty() {
            symbols.push("Unknown".to_string());
        }

        let trips = self.round_trip_trades();
        let best_trade = trips
            .iter()
            .map(|t| t.pnl)
            .fold(f64::NEG_INFINITY, f64::max);
        let worst_trade = trips.iter().map(|t| t.pnl).fold(f64::INFINITY, f64::min);
        let best_trade = if best_trade == f64::NEG_INFINITY {
            0.0
        } else {
            best_trade
        };
        let worst_trade = if worst_trade == f64::INFINITY {
            0.0
        } else {
            worst_trade
        };

        // Consistency score: combines win_rate, profit_factor, and payoff_ratio
        // into a single 0–100 score. Formula:
        //   score = (win_rate_component + pf_component + payoff_component) / 3 * 100
        // Each component is clamped to [0, 1].
        let win_rate_val = self.win_rate; // already 0..1
        let pf = self.profit_factor();
        let pf_component = (pf / 3.0).min(1.0); // PF of 3+ = perfect score
        let pr = self.payoff_ratio();
        let payoff_component = (pr / 3.0).min(1.0); // Payoff ratio of 3+ = perfect
        let consistency_score =
            ((win_rate_val + pf_component + payoff_component) / 3.0 * 100.0).clamp(0.0, 100.0);

        let mut open_insights_count = 0usize;
        let mut open_positions_count = 0usize;
        let mut open_positions_unrealized_pnl = 0.0f64;
        let mut open_positions_profitable_count = 0usize;
        let mut open_positions_losing_count = 0usize;

        for snapshot in state.insight_snapshots.values() {
            if !is_terminal_insight_state(snapshot.state.as_str()) {
                open_insights_count += 1;
            }

            let Some(filled_price) = snapshot.filled_price else {
                continue;
            };
            if snapshot.closed_at.is_some() {
                continue;
            }

            let original_qty = snapshot.quantity.unwrap_or(0.0);
            let closed_qty = snapshot.partial_filled_quantity.unwrap_or(0.0);
            let remaining_qty = (original_qty - closed_qty).max(0.0);
            if remaining_qty <= f64::EPSILON {
                continue;
            }

            let Some(last_price) = state
                .historical_bars
                .get(&snapshot.symbol)
                .and_then(|bars| bars.last())
                .map(|bar| bar.close)
            else {
                continue;
            };

            let unrealized_pnl = if snapshot.side.eq_ignore_ascii_case("buy") {
                (last_price - filled_price) * remaining_qty
            } else {
                (filled_price - last_price) * remaining_qty
            };

            open_positions_count += 1;
            open_positions_unrealized_pnl += unrealized_pnl;
            if unrealized_pnl > 0.0 {
                open_positions_profitable_count += 1;
            } else if unrealized_pnl < 0.0 {
                open_positions_losing_count += 1;
            }
        }

        let metrics = BacktestMetrics {
            starting_cash: self.starting_cash,
            final_equity: self.final_equity,
            total_return: self.final_equity - self.starting_cash,
            total_return_pct: self.total_return_pct,
            total_trades: self.total_trades,
            winning_trades: self.winning_trades,
            losing_trades: self.losing_trades,
            win_rate: self.win_rate,
            max_drawdown: self.max_drawdown,
            cagr: self.cagr(),
            annualized_volatility: self.annualized_volatility(),
            sharpe_ratio: self.sharpe_ratio(),
            sortino_ratio: self.sortino_ratio(),
            calmar_ratio: self.calmar_ratio(),
            max_drawdown_duration_days: self.max_drawdown_duration_days(),
            expectancy: self.expectancy(),
            profit_factor: self.profit_factor(),
            payoff_ratio: self.payoff_ratio(),
            avg_winner: self.avg_winner(),
            avg_loser: self.avg_loser(),
            avg_winner_pct: self.avg_winner_pct(),
            avg_loser_pct: self.avg_loser_pct(),
            best_trade,
            worst_trade,
            consistency_score,
            longest_winning_trade_held_secs: self.longest_winning_trade_held_secs(),
            longest_losing_trade_held_secs: self.longest_losing_trade_held_secs(),
            average_trade_held_secs: self.average_trade_held_secs(),
            open_positions_count,
            open_insights_count,
            open_positions_unrealized_pnl,
            open_positions_profitable_count,
            open_positions_losing_count,
            executed_at: self.executed_at,
            finished_at: self.finished_at,
            backtest_start: self.backtest_start,
            backtest_end: self.backtest_end,
            symbols,
        };

        // Save metrics to JSON. If the project has an `.aqmeta` file, attach the AQS
        // run metadata and copy the snapshot without requiring generated main.rs code.
        let mut metrics_json_value = serde_json::to_value(&metrics).map_err(|e| e.to_string())?;
        enrich_backtest_metrics_with_project_aqmeta(&mut metrics_json_value, dir_path, state);
        let metrics_json =
            serde_json::to_string_pretty(&metrics_json_value).map_err(|e| e.to_string())?;
        std::fs::write(dir_path.join("metrics.json"), metrics_json).map_err(|e| e.to_string())?;
        crate::core::backtest_storage::write_backtest_db(dir_path, self, state).await?;
        Ok(())
    }
}

#[cfg(all(test, feature = "runtime"))]
mod tests {
    use super::*;
    use crate::core::broker::types::AccountType;
    use chrono::TimeZone;

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "aqe_backtest_metrics_{}_{}",
                label,
                uuid::Uuid::new_v4()
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self { path }
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn test_account(equity: f64) -> Account {
        Account {
            account_id: "test".to_string(),
            account_type: AccountType::Paper,
            equity,
            cash: equity,
            currency: "USD".to_string(),
            buying_power: equity,
            accrued_commission: 0.0,
            shorting_enabled: true,
            leverage: 1,
        }
    }

    #[test]
    fn terminal_insight_state_uses_canonical_insight_states() {
        assert!(is_terminal_insight_state("Closed"));
        assert!(is_terminal_insight_state("Cancelled"));
        assert!(is_terminal_insight_state(" rejected "));
        assert!(!is_terminal_insight_state("Filled"));
        assert!(!is_terminal_insight_state("Executed"));
        assert!(!is_terminal_insight_state(""));
    }

    #[test]
    fn enriches_metrics_from_aqmeta_without_codegen_metadata_block() {
        let source_dir = TestDir::new("source");
        let output_dir = TestDir::new("output");
        let aqmeta_path = source_dir.path.join("strategy.aqmeta");
        std::fs::write(
            &aqmeta_path,
            serde_json::json!({
                "id": "strategy-1",
                "name": "Prop Test",
                "version": "2.0.0",
                "dataFeed": "yahooFinance",
                "dataFeedId": "feed-1",
                "broker": "paper",
                "brokerId": "broker-1",
                "config": {
                    "timeframeAmount": 5,
                    "timeframeUnit": "Minute",
                    "brokerLeverage": 4,
                    "startingCash": 25000.0
                },
                "nodes": [
                    { "id": "strategy_root", "type": "strategy", "label": "Prop Test", "inputs": [] },
                    { "id": "alpha-1", "type": "alpha", "label": "Entry Alpha", "sourceFile": "entry_alpha.rs", "inputs": [
                        { "name": "lookback", "inputType": "INT", "value": 20, "isPublic": true }
                    ] },
                    { "id": "pipe-1", "type": "pipe", "label": "Submit Pipe", "sourceFile": "built_in/insight_submit.rs", "inputs": [] },
                    { "id": "unused-alpha", "type": "alpha", "label": "Unused Alpha", "inputs": [] }
                ],
                "connections": [
                    { "from": { "nodeId": "strategy_root", "port": "on_bar" }, "to": { "nodeId": "alpha-1", "port": "on_bar" } },
                    { "from": { "nodeId": "alpha-1", "port": "insights_out" }, "to": { "nodeId": "pipe-1", "port": "insights" } }
                ]
            })
            .to_string(),
        )
        .unwrap();

        let start = "2026-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let end = "2026-01-31T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let mut state = BacktestState::new();
        state.set_backtest_window(start, end);

        let mut metrics = serde_json::json!({ "starting_cash": 25000.0 });
        enrich_backtest_metrics_with_aqmeta_path(
            &mut metrics,
            &output_dir.path,
            &state,
            &aqmeta_path,
        )
        .unwrap();

        assert!(output_dir.path.join("strategy.aqmeta").is_file());
        assert_eq!(metrics["broker_leverage"], serde_json::json!(4));
        assert_eq!(
            metrics["backtest_start"],
            serde_json::json!(start.to_rfc3339())
        );
        assert_eq!(metrics["backtest_end"], serde_json::json!(end.to_rfc3339()));
        assert_eq!(metrics["data_feed"], serde_json::json!("Yahoo Finance"));
        assert_eq!(
            metrics["execution_broker"],
            serde_json::json!("Paper Broker")
        );
        assert_eq!(
            metrics["run_config"]["strategyName"],
            serde_json::json!("Prop Test")
        );
        assert_eq!(
            metrics["run_config"]["aqmetaSnapshot"],
            serde_json::json!("strategy.aqmeta")
        );

        let alpha_models = metrics["alpha_models"].as_array().unwrap();
        assert_eq!(alpha_models.len(), 1);
        assert_eq!(alpha_models[0]["label"], serde_json::json!("Entry Alpha"));
        assert_eq!(metrics["insight_pipes"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn cagr_and_calmar_use_simulated_backtest_window() {
        let run_start = Utc.with_ymd_and_hms(2026, 6, 14, 10, 0, 0).unwrap();
        let backtest_start = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let backtest_end = Utc.with_ymd_and_hms(2025, 7, 1, 0, 0, 0).unwrap();
        let results = BacktestResults {
            starting_cash: 10_000.0,
            final_equity: 11_000.0,
            total_return_pct: 10.0,
            total_trades: 1,
            winning_trades: 1,
            losing_trades: 0,
            win_rate: 1.0,
            max_drawdown: 0.05,
            trade_log: Vec::new(),
            account_history: vec![
                (backtest_start, test_account(10_000.0)),
                (backtest_end, test_account(11_000.0)),
            ],
            executed_at: run_start,
            finished_at: run_start + chrono::Duration::seconds(2),
            backtest_start: Some(backtest_start),
            backtest_end: Some(backtest_end),
        };

        let years = (backtest_end - backtest_start).num_seconds() as f64 / 86_400.0 / 365.25;
        let expected_cagr = (1.1_f64).powf(1.0 / years) - 1.0;

        assert!(results.cagr().is_finite());
        assert!((results.cagr() - expected_cagr).abs() < 1e-10);
        assert!((results.calmar_ratio() - expected_cagr / 0.05).abs() < 1e-10);
    }
}
