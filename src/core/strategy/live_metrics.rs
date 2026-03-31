use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::core::insight::InsightSnapshot;

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
    pub sharpe_ratio: f64,
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

#[derive(Clone, Debug, Default)]
pub struct LivePerformanceTracker {
    starting_cash: f64,
    current_equity: f64,
    peak_equity: f64,
    max_drawdown: f64,
    total_trades: usize,
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
    previous_equity: Option<f64>,
    executed_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    updated_at: Option<DateTime<Utc>>,
    last_persisted: Option<LiveMetricsSnapshot>,
}

impl LivePerformanceTracker {
    pub fn initialize(
        &mut self,
        starting_cash: f64,
        current_equity: f64,
        executed_at: DateTime<Utc>,
        symbols: impl IntoIterator<Item = String>,
    ) {
        self.starting_cash = starting_cash;
        self.current_equity = current_equity;
        self.peak_equity = current_equity.max(starting_cash);
        self.max_drawdown = 0.0;
        self.total_trades = 0;
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
        self.previous_equity = Some(current_equity);
        self.executed_at = Some(executed_at);
        self.finished_at = None;
        self.updated_at = Some(executed_at);
        self.last_persisted = None;
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
            }
        }

        self.current_equity = equity;
        self.previous_equity = Some(equity);
        self.peak_equity = self.peak_equity.max(equity);
        if self.peak_equity.abs() > f64::EPSILON {
            let drawdown = (self.peak_equity - equity) / self.peak_equity;
            self.max_drawdown = self.max_drawdown.max(drawdown * 100.0);
        }
        self.updated_at = Some(at);
    }

    pub fn record_closed_insight(&mut self, snapshot: &InsightSnapshot) {
        let pnl = trade_pnl(snapshot);
        let return_pct = trade_return_pct(snapshot);
        let hold_secs = trade_hold_secs(snapshot);

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

        self.updated_at = Some(Utc::now());
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
            total_return / self.total_trades as f64
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

        Some(LiveMetricsSnapshot {
            starting_cash: self.starting_cash,
            final_equity: self.current_equity,
            total_return,
            total_return_pct,
            total_trades: self.total_trades,
            winning_trades: self.winning_trades,
            losing_trades: self.losing_trades,
            win_rate,
            max_drawdown: self.max_drawdown,
            sharpe_ratio: self.sharpe_ratio(),
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
        })
    }

    pub fn should_persist(&self) -> bool {
        let Some(current) = self.snapshot() else {
            return false;
        };
        self.last_persisted.as_ref() != Some(&current)
    }

    pub fn mark_persisted(&mut self) {
        self.last_persisted = self.snapshot();
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
    match snapshot.side.as_str() {
        "Buy" => ((close_price - entry_price) / entry_price) * 100.0,
        "Sell" => ((entry_price - close_price) / entry_price) * 100.0,
        _ => 0.0,
    }
}

fn trade_pnl(snapshot: &InsightSnapshot) -> f64 {
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
    match snapshot.side.as_str() {
        "Buy" => (close_price - entry_price) * qty,
        "Sell" => (entry_price - close_price) * qty,
        _ => 0.0,
    }
}

fn trade_hold_secs(snapshot: &InsightSnapshot) -> i64 {
    let start = snapshot.filled_at.or(Some(snapshot.created_at));
    let end = snapshot.closed_at.or(Some(snapshot.updated_at));
    match (start, end) {
        (Some(start), Some(end)) => (end - start).num_seconds().max(0),
        _ => 0,
    }
}

fn consistency_score(win_rate: f64, profit_factor: f64, payoff_ratio: f64) -> f64 {
    let pf_component = (profit_factor / 3.0).min(1.0);
    let payoff_component = (payoff_ratio / 3.0).min(1.0);
    ((win_rate + pf_component + payoff_component) / 3.0 * 100.0).clamp(0.0, 100.0)
}
