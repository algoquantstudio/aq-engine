use chrono::{DateTime, Utc};
use std::collections::HashMap;

use std::path::Path;

use super::types::{Account, Bar, OrderSide, TradeRecord, TradeRecordType};
use crate::core::insight::InsightSnapshot;

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

    executed_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
}

impl BacktestState {
    pub fn new() -> Self {
        Self {
            historical_bars: HashMap::new(),
            bar_indices: HashMap::new(),
            total_bars: 0,
            current_time: Utc::now(),
            previous_time: None,
            account_history: Vec::new(),
            trade_log: Vec::new(),
            insight_snapshots: HashMap::new(),
            trade_mae_by_order_id: HashMap::new(),
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
    pub symbols: Vec<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AccountHistoryItem {
    pub timestamp: DateTime<Utc>,
    pub equity: f64,
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

                        let pnl = match entry.side {
                            OrderSide::Buy => (rec.price - entry.price) * matched_qty,
                            OrderSide::Sell => (entry.price - rec.price) * matched_qty,
                        };
                        let return_pct = if entry.price != 0.0 {
                            match entry.side {
                                OrderSide::Buy => (rec.price - entry.price) / entry.price * 100.0,
                                OrderSide::Sell => (entry.price - rec.price) / entry.price * 100.0,
                            }
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

        let days = (self.finished_at - self.executed_at).num_seconds().max(0) as f64 / 86_400.0;
        if days <= f64::EPSILON {
            return self.total_return_pct / 100.0;
        }

        let years = days / 365.25;
        if years <= f64::EPSILON {
            return self.total_return_pct / 100.0;
        }

        (self.final_equity / self.starting_cash).powf(1.0 / years) - 1.0
    }

    pub fn calmar_ratio(&self) -> f64 {
        if self.max_drawdown <= f64::EPSILON {
            return 0.0;
        }
        self.cagr() / self.max_drawdown.abs()
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

    /// Print a comprehensive, formatted summary of all backtest metrics.
    pub fn print_metrics(&self) {
        let (n_long, n_short) = self.count_long_short();

        println!("═══════════════════════════════════════════════");
        println!("            BACKTEST RESULTS");
        println!("═══════════════════════════════════════════════");

        // ── Portfolio Overview
        println!("  Starting Cash:     ${:.2}", self.starting_cash);
        println!("  Final Equity:      ${:.2}", self.final_equity);
        println!("  Total Return:      {:.2}%", self.total_return_pct);
        println!("  Max Drawdown:      {:.2}%", self.max_drawdown * 100.0);

        // ── Risk‑Adjusted
        println!("  ─────────────────────────────────────────────");
        println!("  Sharpe Ratio:      {:.4}", self.sharpe_ratio());
        println!("  Expectancy:        ${:.2}", self.expectancy());
        println!("  Profit Factor:     {:.2}", self.profit_factor());
        println!("  Payoff Ratio:      {:.2}", self.payoff_ratio());

        // ── Trade Summary
        println!("  ─────────────────────────────────────────────");
        println!("  Total Trades:      {}", self.total_trades);
        println!("  Winning Trades:    {}", self.winning_trades);
        println!("  Losing Trades:     {}", self.losing_trades);
        println!("  Win Rate:          {:.2}%", self.win_rate * 100.0);

        // ── Winners / Losers
        println!("  ─────────────────────────────────────────────");
        println!(
            "  Avg Winner:        ${:.2}  ({:.2}%)",
            self.avg_winner(),
            self.avg_winner_pct()
        );
        println!(
            "  Avg Loser:        -${:.2}  ({:.2}%)",
            self.avg_loser(),
            self.avg_loser_pct()
        );

        // ── Hold Duration
        let fmt_dur = |secs: i64| -> String {
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
        };
        println!(
            "  Longest Win Held:  {}",
            fmt_dur(self.longest_winning_trade_held_secs())
        );
        println!(
            "  Longest Loss Held: {}",
            fmt_dur(self.longest_losing_trade_held_secs())
        );
        println!(
            "  Average Trade Held: {}",
            fmt_dur(self.average_trade_held_secs())
        );

        // ── Long / Short Breakdown
        println!("  ─────────────────────────────────────────────");
        println!("  Long Trades:       {}", n_long);
        println!(
            "  Avg Return (L):    {:.2}%  (${:.2})",
            self.avg_return_pct_long(),
            self.avg_nominal_long()
        );
        println!("  Short Trades:      {}", n_short);
        println!(
            "  Avg Return (S):    {:.2}%  (${:.2})",
            self.avg_return_pct_short(),
            self.avg_nominal_short()
        );

        // ── Data
        println!("  ─────────────────────────────────────────────");
        println!("  Executed At:       {:?}", self.executed_at);
        println!("  Finished At:       {:?}", self.finished_at);
        println!(
            "  Duration:          {}",
            fmt_dur(self.finished_at.timestamp() - self.executed_at.timestamp())
        );
        println!("  History Points:    {}", self.account_history.len());
        println!("  Trade Log Size:    {}", self.trade_log.len());
        println!("═══════════════════════════════════════════════");
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
    /// % return relative to entry price.
    pub return_pct: f64,
    /// How long the position was held (seconds).
    pub hold_secs: i64,
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
            let state_name = snapshot.state.as_str();
            let is_terminal = matches!(state_name, "Closed" | "Canceled" | "Rejected");
            if !is_terminal {
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
            symbols,
        };

        // Save metrics to JSON
        let metrics_json = serde_json::to_string_pretty(&metrics).map_err(|e| e.to_string())?;
        std::fs::write(dir_path.join("metrics.json"), metrics_json).map_err(|e| e.to_string())?;
        crate::core::backtest_storage::write_backtest_db(dir_path, self, state).await?;
        Ok(())
    }
}
