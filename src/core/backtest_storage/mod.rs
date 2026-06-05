use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use chrono::{DateTime, Datelike, Timelike, Utc};
use turso::{Builder, Connection, params, transaction::Transaction};

mod types;

use crate::core::broker::backtest_state::BacktestState;
use crate::core::broker::backtest_state::{AccountHistoryItem, BacktestResults, RoundTripTrade};
use crate::core::broker::types::{Account, Bar, TradeRecord};
use crate::core::insight::InsightSnapshot;

pub use types::BacktestTradeLogRow;

pub const BACKTEST_DB_FILE: &str = "backtest.db";

fn to_storage_err<E: std::fmt::Display>(value: E) -> String {
    value.to_string()
}

async fn connect_database(dir_path: &Path) -> Result<Connection, String> {
    let db_path = dir_path.join(BACKTEST_DB_FILE);
    let db = Builder::new_local(db_path.to_string_lossy().as_ref())
        .build()
        .await
        .map_err(to_storage_err)?;
    db.connect().map_err(to_storage_err)
}

async fn init_schema(conn: &Connection) -> Result<(), String> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS trade_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_at TEXT NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            qty REAL NOT NULL,
            price REAL NOT NULL,
            order_id TEXT NOT NULL,
            insight_id TEXT,
            strategy_type TEXT,
            trade_type TEXT NOT NULL,
            payload_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_trade_log_symbol_at ON trade_log(symbol, event_at);
        CREATE INDEX IF NOT EXISTS idx_trade_log_insight_id ON trade_log(insight_id);

        CREATE TABLE IF NOT EXISTS trade_log_rows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            entry_time TEXT NOT NULL,
            insight_id TEXT,
            status TEXT NOT NULL,
            payload_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_trade_log_rows_symbol_entry_time ON trade_log_rows(symbol, entry_time);
        CREATE INDEX IF NOT EXISTS idx_trade_log_rows_insight_id ON trade_log_rows(insight_id);

        CREATE TABLE IF NOT EXISTS round_trips (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            insight_id TEXT,
            strategy_type TEXT,
            entry_time TEXT NOT NULL,
            exit_time TEXT NOT NULL,
            entry_price REAL NOT NULL,
            exit_price REAL NOT NULL,
            qty REAL NOT NULL,
            pnl REAL NOT NULL,
            return_pct REAL NOT NULL,
            hold_secs INTEGER NOT NULL,
            payload_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_round_trips_symbol_entry_time ON round_trips(symbol, entry_time);
        CREATE INDEX IF NOT EXISTS idx_round_trips_insight_id ON round_trips(insight_id);

        CREATE TABLE IF NOT EXISTS account_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_at TEXT NOT NULL,
            equity REAL NOT NULL,
            cash REAL NOT NULL,
            buying_power REAL NOT NULL,
            payload_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_account_history_event_at ON account_history(event_at);

        CREATE TABLE IF NOT EXISTS insights (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            insight_id TEXT NOT NULL UNIQUE,
            symbol TEXT NOT NULL,
            strategy_type TEXT NOT NULL,
            state TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            filled_at TEXT,
            closed_at TEXT,
            payload_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_insights_symbol_created_at ON insights(symbol, created_at);

        CREATE TABLE IF NOT EXISTS bars (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            event_at TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            payload_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_bars_symbol_event_at ON bars(symbol, event_at);

        CREATE TABLE IF NOT EXISTS monthly_returns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INT NOT NULL,
            month INT NOT NULL,
            return_pct REAL NOT NULL,
            payload_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_monthly_returns_period ON monthly_returns(year, month);

        CREATE TABLE IF NOT EXISTS param_sweep (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            param1_name TEXT NOT NULL,
            param1_value REAL NOT NULL,
            param2_name TEXT NOT NULL,
            param2_value REAL NOT NULL,
            sharpe REAL NOT NULL,
            total_return REAL NOT NULL,
            max_drawdown REAL NOT NULL,
            payload_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS time_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            day_of_week INT NOT NULL,
            hour INT NOT NULL,
            avg_return_bps REAL NOT NULL,
            trade_count INT NOT NULL,
            payload_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_time_performance_slot ON time_performance(day_of_week, hour);

        CREATE TABLE IF NOT EXISTS drawdown_series (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_name TEXT NOT NULL,
            period TEXT NOT NULL,
            drawdown_pct REAL NOT NULL,
            payload_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_drawdown_series_period ON drawdown_series(strategy_name, period);

        CREATE TABLE IF NOT EXISTS regime_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vol_regime TEXT NOT NULL,
            trend_regime TEXT NOT NULL,
            avg_return_pct REAL NOT NULL,
            bar_count INT NOT NULL,
            payload_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS factor_exposure (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            factor_name TEXT NOT NULL,
            beta REAL NOT NULL,
            payload_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS strategy_correlations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_a TEXT NOT NULL,
            strategy_b TEXT NOT NULL,
            correlation REAL NOT NULL,
            payload_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS position_concentration (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            sector TEXT NOT NULL,
            weight_pct REAL NOT NULL,
            payload_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_position_concentration_date ON position_concentration(date);

        CREATE TABLE IF NOT EXISTS slippage_analysis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id TEXT NOT NULL,
            expected_cost_bps REAL NOT NULL,
            actual_cost_bps REAL NOT NULL,
            order_size REAL NOT NULL,
            payload_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS trade_mae (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id TEXT NOT NULL,
            mae_pct REAL NOT NULL,
            final_pnl_pct REAL NOT NULL,
            is_winner BOOL NOT NULL,
            payload_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS setup_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            setup_name TEXT NOT NULL,
            win_rate REAL NOT NULL,
            payoff_ratio REAL NOT NULL,
            trade_count INT NOT NULL,
            total_pnl REAL NOT NULL,
            payload_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS rolling_sharpe (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            window_days INT NOT NULL,
            period TEXT NOT NULL,
            sharpe REAL NOT NULL,
            payload_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS walk_forward (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            window_id INT NOT NULL,
            is_sharpe REAL NOT NULL,
            oos_sharpe REAL NOT NULL,
            ratio REAL NOT NULL,
            train_start TEXT NOT NULL,
            train_end TEXT NOT NULL,
            test_start TEXT NOT NULL,
            test_end TEXT NOT NULL,
            payload_json TEXT NOT NULL
        );
        "#,
    )
    .await
    .map_err(to_storage_err)?;
    Ok(())
}

async fn insert_trade_log(tx: &Transaction<'_>, trade_log: &[TradeRecord]) -> Result<(), String> {
    let mut stmt = tx
        .prepare(
            "INSERT INTO trade_log (event_at, symbol, side, qty, price, order_id, insight_id, strategy_type, trade_type, payload_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        )
        .await
        .map_err(to_storage_err)?;
    for record in trade_log {
        stmt.execute(params![
            record.date.to_rfc3339(),
            record.symbol.clone(),
            format!("{:?}", record.side),
            record.qty,
            record.price,
            record.order_id.clone(),
            record.insight_id.clone(),
            record.strategy_type.clone(),
            format!("{:?}", record.trade_type),
            serde_json::to_string(record).map_err(to_storage_err)?
        ])
        .await
        .map_err(to_storage_err)?;
    }
    Ok(())
}

fn format_backtest_timestamp(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%d %H:%M:%S").to_string()
}

fn build_trade_log_rows(
    _round_trips: &[RoundTripTrade],
    trade_events: &[TradeRecord],
) -> Vec<BacktestTradeLogRow> {
    let mut rows = Vec::new();
    let mut entry_remaining_by_order: std::collections::HashMap<String, (TradeRecord, f64)> =
        std::collections::HashMap::new();

    for trade in trade_events {
        match trade.trade_type {
            crate::core::broker::types::TradeRecordType::Entry => {
                entry_remaining_by_order.insert(trade.order_id.clone(), (trade.clone(), trade.qty));
            }
            crate::core::broker::types::TradeRecordType::Exit => {
                if let Some((entry, remaining_qty)) =
                    entry_remaining_by_order.get_mut(&trade.order_id)
                {
                    let exit_qty = trade.qty.min(*remaining_qty);
                    *remaining_qty -= exit_qty;
                    let fully_closed = *remaining_qty <= f64::EPSILON;
                    let pnl = match entry.side {
                        crate::core::broker::types::OrderSide::Buy => {
                            (trade.price - entry.price) * exit_qty
                        }
                        crate::core::broker::types::OrderSide::Sell => {
                            (entry.price - trade.price) * exit_qty
                        }
                    };
                    let return_pct = if entry.price.abs() > f64::EPSILON {
                        (pnl / (entry.price * exit_qty)) * 100.0
                    } else {
                        0.0
                    };

                    rows.push(BacktestTradeLogRow {
                        id: 0,
                        symbol: trade.symbol.clone(),
                        side: format!("{:?}", trade.side).to_uppercase(),
                        strategy_type: trade.strategy_type.clone(),
                        insight_id: trade.insight_id.clone(),
                        entry_time: format_backtest_timestamp(entry.date),
                        exit_time: Some(format_backtest_timestamp(trade.date)),
                        qty: exit_qty,
                        entry_price: entry.price,
                        exit_price: Some(trade.price),
                        return_pct: Some(return_pct),
                        pnl: Some(pnl),
                        status: if fully_closed {
                            "CLOSED".to_string()
                        } else {
                            "PARTIAL".to_string()
                        },
                    });

                    if fully_closed {
                        entry_remaining_by_order.remove(&trade.order_id);
                    }
                } else {
                    rows.push(BacktestTradeLogRow {
                        id: 0,
                        symbol: trade.symbol.clone(),
                        side: format!("{:?}", trade.side).to_uppercase(),
                        strategy_type: trade.strategy_type.clone(),
                        insight_id: trade.insight_id.clone(),
                        entry_time: format_backtest_timestamp(trade.date),
                        exit_time: None,
                        qty: trade.qty,
                        entry_price: trade.price,
                        exit_price: None,
                        return_pct: None,
                        pnl: None,
                        status: format!("{:?}", trade.trade_type).to_uppercase(),
                    });
                }
            }
        }
    }

    let mut next_id = rows.len() as i32 + 1;

    for (_, (trade, remaining_qty)) in entry_remaining_by_order {
        if remaining_qty <= f64::EPSILON {
            continue;
        }
        rows.push(BacktestTradeLogRow {
            id: next_id,
            symbol: trade.symbol.clone(),
            side: format!("{:?}", trade.side).to_uppercase(),
            strategy_type: trade.strategy_type.clone(),
            insight_id: trade.insight_id.clone(),
            entry_time: format_backtest_timestamp(trade.date),
            exit_time: None,
            qty: remaining_qty,
            entry_price: trade.price,
            exit_price: None,
            return_pct: None,
            pnl: None,
            status: "OPEN".to_string(),
        });
        next_id += 1;
    }

    rows.sort_by(|a, b| {
        let a_time = a.exit_time.as_ref().unwrap_or(&a.entry_time);
        let b_time = b.exit_time.as_ref().unwrap_or(&b.entry_time);
        b_time.cmp(a_time)
    });
    for (index, row) in rows.iter_mut().enumerate() {
        row.id = (index + 1) as i32;
    }
    rows
}

async fn insert_trade_log_rows(
    tx: &Transaction<'_>,
    round_trips: &[RoundTripTrade],
    trade_events: &[TradeRecord],
) -> Result<(), String> {
    let rows = build_trade_log_rows(round_trips, trade_events);
    let mut stmt = tx
        .prepare(
            "INSERT INTO trade_log_rows (symbol, entry_time, insight_id, status, payload_json)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .await
        .map_err(to_storage_err)?;
    for row in rows {
        stmt.execute(params![
            row.symbol.clone(),
            row.entry_time.clone(),
            row.insight_id.clone(),
            row.status.clone(),
            serde_json::to_string(&row).map_err(to_storage_err)?
        ])
        .await
        .map_err(to_storage_err)?;
    }
    Ok(())
}

async fn insert_round_trips(tx: &Transaction<'_>, trips: &[RoundTripTrade]) -> Result<(), String> {
    let mut stmt = tx
        .prepare(
            "INSERT INTO round_trips (symbol, side, insight_id, strategy_type, entry_time, exit_time, entry_price, exit_price, qty, pnl, return_pct, hold_secs, payload_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        )
        .await
        .map_err(to_storage_err)?;
    for trip in trips {
        stmt.execute(params![
            trip.symbol.clone(),
            format!("{:?}", trip.side),
            trip.insight_id.clone(),
            trip.strategy_type.clone(),
            trip.entry_time.to_rfc3339(),
            trip.exit_time.to_rfc3339(),
            trip.entry_price,
            trip.exit_price,
            trip.qty,
            trip.pnl,
            trip.return_pct,
            trip.hold_secs,
            serde_json::to_string(trip).map_err(to_storage_err)?
        ])
        .await
        .map_err(to_storage_err)?;
    }
    Ok(())
}

async fn insert_account_history(
    tx: &Transaction<'_>,
    account_history: &[(DateTime<Utc>, Account)],
) -> Result<(), String> {
    let mut stmt = tx
        .prepare(
            "INSERT INTO account_history (event_at, equity, cash, buying_power, payload_json)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .await
        .map_err(to_storage_err)?;
    for (timestamp, account) in account_history {
        let payload = serde_json::to_string(&AccountHistoryItem {
            timestamp: *timestamp,
            equity: account.equity,
        })
        .map_err(to_storage_err)?;
        stmt.execute(params![
            timestamp.to_rfc3339(),
            account.equity,
            account.cash,
            account.buying_power,
            payload
        ])
        .await
        .map_err(to_storage_err)?;
    }
    Ok(())
}

async fn insert_insights(tx: &Transaction<'_>, insights: &[InsightSnapshot]) -> Result<(), String> {
    let mut stmt = tx
        .prepare(
            "INSERT OR REPLACE INTO insights (insight_id, symbol, strategy_type, state, created_at, updated_at, filled_at, closed_at, payload_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        )
        .await
        .map_err(to_storage_err)?;
    for insight in insights {
        stmt.execute(params![
            insight.insight_id.clone(),
            insight.symbol.clone(),
            insight.strategy_type.clone(),
            insight.state.clone(),
            insight.created_at.to_rfc3339(),
            insight.updated_at.to_rfc3339(),
            insight.filled_at.map(|value| value.to_rfc3339()),
            insight.closed_at.map(|value| value.to_rfc3339()),
            serde_json::to_string(insight).map_err(to_storage_err)?
        ])
        .await
        .map_err(to_storage_err)?;
    }
    Ok(())
}

async fn insert_bars(
    tx: &Transaction<'_>,
    bars_by_symbol: &std::collections::HashMap<String, Vec<Bar>>,
) -> Result<(), String> {
    let mut stmt = tx
        .prepare(
            "INSERT INTO bars (symbol, event_at, open, high, low, close, volume, payload_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )
        .await
        .map_err(to_storage_err)?;
    for bars in bars_by_symbol.values() {
        for bar in bars {
            stmt.execute(params![
                bar.symbol.clone(),
                bar.timestamp.to_rfc3339(),
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
                serde_json::to_string(bar).map_err(to_storage_err)?
            ])
            .await
            .map_err(to_storage_err)?;
        }
    }
    Ok(())
}

#[derive(Debug, serde::Serialize)]
struct MonthlyReturnRow {
    year: i32,
    month: u32,
    return_pct: f64,
}

#[derive(Debug, serde::Serialize)]
struct TimePerformanceRow {
    day_of_week: u32,
    hour: u32,
    avg_return_bps: f64,
    trade_count: usize,
}

#[derive(Debug, serde::Serialize)]
struct DrawdownSeriesRow {
    strategy_name: String,
    period: String,
    drawdown_pct: f64,
}

#[derive(Debug, serde::Serialize)]
struct RegimePerformanceRow {
    vol_regime: String,
    trend_regime: String,
    avg_return_pct: f64,
    bar_count: usize,
}

#[derive(Debug, serde::Serialize)]
struct RollingSharpeRow {
    window_days: i32,
    period: String,
    sharpe: f64,
}

#[derive(Debug, serde::Serialize)]
struct TradeMaeRow {
    trade_id: String,
    mae_pct: f64,
    final_pnl_pct: f64,
    is_winner: bool,
}

#[derive(Debug, serde::Serialize)]
struct SetupPerformanceRow {
    setup_name: String,
    win_rate: f64,
    payoff_ratio: f64,
    trade_count: usize,
    total_pnl: f64,
}

#[derive(Debug, serde::Serialize)]
struct PositionConcentrationRow {
    date: String,
    sector: String,
    weight_pct: f64,
}

#[derive(Debug, serde::Serialize)]
struct StrategyCorrelationRow {
    strategy_a: String,
    strategy_b: String,
    correlation: f64,
}

fn month_period(timestamp: DateTime<Utc>) -> String {
    format!("{:04}-{:02}", timestamp.year(), timestamp.month())
}

fn quarter_period(timestamp: DateTime<Utc>) -> String {
    format!(
        "{:04}-Q{}",
        timestamp.year(),
        ((timestamp.month() - 1) / 3) + 1
    )
}

fn primary_strategy_name(trips: &[RoundTripTrade]) -> String {
    trips
        .iter()
        .find_map(|trip| trip.strategy_type.clone())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "Strategy".to_string())
}

fn build_monthly_returns(account_history: &[(DateTime<Utc>, Account)]) -> Vec<MonthlyReturnRow> {
    let mut buckets: BTreeMap<(i32, u32), (f64, f64)> = BTreeMap::new();
    for (timestamp, account) in account_history {
        buckets
            .entry((timestamp.year(), timestamp.month()))
            .and_modify(|(_, last)| *last = account.equity)
            .or_insert((account.equity, account.equity));
    }

    buckets
        .into_iter()
        .map(|((year, month), (first, last))| MonthlyReturnRow {
            year,
            month,
            return_pct: if first.abs() > f64::EPSILON {
                ((last - first) / first) * 100.0
            } else {
                0.0
            },
        })
        .collect()
}

fn build_time_performance(trips: &[RoundTripTrade]) -> Vec<TimePerformanceRow> {
    let mut buckets: BTreeMap<(u32, u32), (f64, usize)> = BTreeMap::new();
    for trip in trips {
        let key = (
            trip.entry_time.weekday().number_from_monday(),
            trip.entry_time.hour(),
        );
        let return_bps = trip.return_pct * 100.0;
        buckets
            .entry(key)
            .and_modify(|(sum, count)| {
                *sum += return_bps;
                *count += 1;
            })
            .or_insert((return_bps, 1));
    }

    buckets
        .into_iter()
        .map(|((day_of_week, hour), (sum, count))| TimePerformanceRow {
            day_of_week,
            hour,
            avg_return_bps: if count > 0 { sum / count as f64 } else { 0.0 },
            trade_count: count,
        })
        .collect()
}

fn build_drawdown_series(
    account_history: &[(DateTime<Utc>, Account)],
    strategy_name: String,
) -> Vec<DrawdownSeriesRow> {
    let mut peak = account_history
        .first()
        .map(|(_, account)| account.equity)
        .unwrap_or(0.0);
    let mut buckets: BTreeMap<String, f64> = BTreeMap::new();

    for (timestamp, account) in account_history {
        peak = peak.max(account.equity);
        let drawdown_pct = if peak.abs() > f64::EPSILON {
            ((account.equity - peak) / peak) * 100.0
        } else {
            0.0
        };
        let period = month_period(*timestamp);
        buckets
            .entry(period)
            .and_modify(|value| *value = value.min(drawdown_pct))
            .or_insert(drawdown_pct);
    }

    buckets
        .into_iter()
        .map(|(period, drawdown_pct)| DrawdownSeriesRow {
            strategy_name: strategy_name.clone(),
            period,
            drawdown_pct,
        })
        .collect()
}

fn equity_returns(account_history: &[(DateTime<Utc>, Account)]) -> Vec<(DateTime<Utc>, f64)> {
    account_history
        .windows(2)
        .filter_map(|window| {
            let previous = window[0].1.equity;
            if previous.abs() <= f64::EPSILON {
                return None;
            }
            Some((window[1].0, (window[1].1.equity - previous) / previous))
        })
        .collect()
}

fn sharpe_from_returns(returns: &[f64]) -> f64 {
    if returns.len() < 2 {
        return 0.0;
    }
    let n = returns.len() as f64;
    let mean = returns.iter().sum::<f64>() / n;
    let variance = returns
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / n;
    let std_dev = variance.sqrt();
    if std_dev <= f64::EPSILON {
        return 0.0;
    }
    (mean / std_dev) * (252.0_f64).sqrt()
}

fn build_rolling_sharpe(account_history: &[(DateTime<Utc>, Account)]) -> Vec<RollingSharpeRow> {
    let returns = equity_returns(account_history);
    if returns.len() < 2 {
        return Vec::new();
    }

    let Some((first_timestamp, _)) = account_history.first() else {
        return Vec::new();
    };
    let Some((last_timestamp, _)) = account_history.last() else {
        return Vec::new();
    };
    let duration_secs = (*last_timestamp - *first_timestamp).num_seconds().max(1) as f64;
    let duration_days = duration_secs / 86_400.0;
    let samples_per_day = (returns.len() as f64 / duration_days.max(1.0 / 24.0)).max(1.0);
    let max_window_days = duration_days.ceil().max(1.0) as i32;
    let mut windows: Vec<i32> = [1_i32, 2, 3, 7, 14, 30, 60, 90, 120, 180]
        .into_iter()
        .filter(|window| *window <= max_window_days)
        .collect();
    if windows.is_empty() {
        windows.push(1);
    }
    let mut rows = Vec::new();

    for window_days in windows {
        let window_len = ((window_days as f64) * samples_per_day).round() as usize;
        let window_len = window_len.clamp(2, returns.len());
        let mut latest_by_period: BTreeMap<String, f64> = BTreeMap::new();
        for index in 0..returns.len() {
            if index + 1 < window_len {
                continue;
            }
            let start = index + 1 - window_len;
            let slice: Vec<f64> = returns[start..=index]
                .iter()
                .map(|(_, value)| *value)
                .collect();
            let period = if duration_days <= 2.0 {
                returns[index].0.format("%Y-%m-%d %H:00").to_string()
            } else if duration_days <= 14.0 {
                returns[index].0.format("%Y-%m-%d").to_string()
            } else {
                quarter_period(returns[index].0)
            };
            latest_by_period.insert(period, sharpe_from_returns(&slice));
        }
        rows.extend(
            latest_by_period
                .into_iter()
                .map(|(period, sharpe)| RollingSharpeRow {
                    window_days,
                    period,
                    sharpe,
                }),
        );
    }

    rows
}

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = ((sorted.len() - 1) as f64 * pct).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

fn std_dev(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    (values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / values.len() as f64)
        .sqrt()
}

fn build_regime_performance(
    account_history: &[(DateTime<Utc>, Account)],
    bars_by_symbol: &HashMap<String, Vec<Bar>>,
) -> Vec<RegimePerformanceRow> {
    let Some((_, bars)) = bars_by_symbol.iter().next() else {
        return Vec::new();
    };
    if bars.len() < 51 {
        return Vec::new();
    }

    let mut bar_returns = Vec::with_capacity(bars.len().saturating_sub(1));
    for window in bars.windows(2) {
        if window[0].close.abs() > f64::EPSILON {
            bar_returns.push((
                window[1].timestamp,
                (window[1].close - window[0].close) / window[0].close,
            ));
        }
    }

    let mut vol_by_index: HashMap<usize, f64> = HashMap::new();
    let mut vol_values = Vec::new();
    for index in 20..bar_returns.len() {
        let returns: Vec<f64> = bar_returns[index - 20..index]
            .iter()
            .map(|(_, value)| *value)
            .collect();
        let vol = std_dev(&returns);
        vol_by_index.insert(index + 1, vol);
        vol_values.push(vol);
    }
    vol_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let q1 = percentile(&vol_values, 0.25);
    let q2 = percentile(&vol_values, 0.50);
    let q3 = percentile(&vol_values, 0.75);

    let equity_return_by_time: HashMap<DateTime<Utc>, f64> =
        equity_returns(account_history).into_iter().collect();
    let mut buckets: BTreeMap<(String, String), (f64, usize)> = BTreeMap::new();

    for index in 50..bars.len() {
        let Some(vol) = vol_by_index.get(&index).copied() else {
            continue;
        };
        let sma50 = bars[index - 50..index]
            .iter()
            .map(|bar| bar.close)
            .sum::<f64>()
            / 50.0;
        if sma50.abs() <= f64::EPSILON {
            continue;
        }
        let slope = (bars[index].close - sma50) / sma50;
        let vol_regime = if vol <= q1 {
            "Low Vol"
        } else if vol <= q2 {
            "Med Vol"
        } else if vol <= q3 {
            "High Vol"
        } else {
            "Crisis"
        };
        let trend_regime = if slope <= -0.02 {
            "Strong Down"
        } else if slope <= -0.005 {
            "Mild Down"
        } else if slope < 0.005 {
            "Flat"
        } else if slope < 0.02 {
            "Mild Up"
        } else {
            "Strong Up"
        };
        let strategy_return = equity_return_by_time
            .get(&bars[index].timestamp)
            .copied()
            .or_else(|| {
                if index > 0 && bars[index - 1].close.abs() > f64::EPSILON {
                    Some((bars[index].close - bars[index - 1].close) / bars[index - 1].close)
                } else {
                    None
                }
            })
            .unwrap_or(0.0)
            * 100.0;

        buckets
            .entry((vol_regime.to_string(), trend_regime.to_string()))
            .and_modify(|(sum, count)| {
                *sum += strategy_return;
                *count += 1;
            })
            .or_insert((strategy_return, 1));
    }

    buckets
        .into_iter()
        .map(
            |((vol_regime, trend_regime), (sum, count))| RegimePerformanceRow {
                vol_regime,
                trend_regime,
                avg_return_pct: if count > 0 { sum / count as f64 } else { 0.0 },
                bar_count: count,
            },
        )
        .collect()
}

fn build_trade_mae(
    trips: &[RoundTripTrade],
    bars_by_symbol: &HashMap<String, Vec<Bar>>,
    tracked_mae: &HashMap<String, f64>,
) -> Vec<TradeMaeRow> {
    let mut rows = Vec::new();
    for trip in trips {
        if let Some(mae_pct) = tracked_mae.get(&trip.order_id).copied() {
            rows.push(TradeMaeRow {
                trade_id: trip
                    .insight_id
                    .clone()
                    .unwrap_or_else(|| trip.order_id.clone()),
                mae_pct,
                final_pnl_pct: trip.return_pct,
                is_winner: trip.pnl >= 0.0,
            });
            continue;
        }

        let Some(bars) = bars_by_symbol.get(&trip.symbol) else {
            continue;
        };
        if trip.entry_price.abs() <= f64::EPSILON {
            continue;
        }
        let mut mae_pct = 0.0_f64;
        for bar in bars {
            if bar.timestamp < trip.entry_time {
                continue;
            }
            if bar.timestamp > trip.exit_time {
                break;
            }
            let adverse = match trip.side {
                crate::core::broker::types::OrderSide::Buy => {
                    ((bar.low - trip.entry_price) / trip.entry_price) * 100.0
                }
                crate::core::broker::types::OrderSide::Sell => {
                    ((trip.entry_price - bar.high) / trip.entry_price) * 100.0
                }
            };
            mae_pct = mae_pct.min(adverse);
        }
        rows.push(TradeMaeRow {
            trade_id: trip
                .insight_id
                .clone()
                .unwrap_or_else(|| trip.order_id.clone()),
            mae_pct,
            final_pnl_pct: trip.return_pct,
            is_winner: trip.pnl >= 0.0,
        });
    }
    rows
}

fn build_setup_performance(trips: &[RoundTripTrade]) -> Vec<SetupPerformanceRow> {
    let mut buckets: BTreeMap<String, Vec<&RoundTripTrade>> = BTreeMap::new();
    for trip in trips {
        let strategy = trip
            .strategy_type
            .clone()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "All Trades".to_string());
        let side = format!("{:?}", trip.side);
        let symbol = trip.symbol.clone();

        buckets.entry(strategy.clone()).or_default().push(trip);
        buckets
            .entry(format!("{strategy} / {side}"))
            .or_default()
            .push(trip);
        buckets
            .entry(format!("{strategy} / {symbol}"))
            .or_default()
            .push(trip);
    }

    buckets
        .into_iter()
        .filter(|(_, trades)| !trades.is_empty())
        .map(|(setup_name, trades)| {
            let trade_count = trades.len();
            let winners: Vec<f64> = trades
                .iter()
                .filter(|trip| trip.pnl > 0.0)
                .map(|trip| trip.pnl)
                .collect();
            let losers: Vec<f64> = trades
                .iter()
                .filter(|trip| trip.pnl < 0.0)
                .map(|trip| trip.pnl.abs())
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
            SetupPerformanceRow {
                setup_name,
                win_rate: if trade_count > 0 {
                    winners.len() as f64 / trade_count as f64 * 100.0
                } else {
                    0.0
                },
                payoff_ratio: if avg_loss > f64::EPSILON {
                    avg_win / avg_loss
                } else {
                    0.0
                },
                trade_count,
                total_pnl: trades.iter().map(|trip| trip.pnl).sum(),
            }
        })
        .collect()
}

fn build_position_concentration(trade_log: &[TradeRecord]) -> Vec<PositionConcentrationRow> {
    let mut open_by_order: HashMap<String, (String, f64)> = HashMap::new();
    let mut rows_by_date: BTreeMap<String, BTreeMap<String, f64>> = BTreeMap::new();

    for record in trade_log {
        let notional = record.price * record.qty;
        match record.trade_type {
            crate::core::broker::types::TradeRecordType::Entry => {
                open_by_order.insert(record.order_id.clone(), (record.symbol.clone(), notional));
            }
            crate::core::broker::types::TradeRecordType::Exit => {
                open_by_order.remove(&record.order_id);
            }
        }

        let mut symbol_notional: BTreeMap<String, f64> = BTreeMap::new();
        for (symbol, value) in open_by_order.values() {
            *symbol_notional.entry(symbol.clone()).or_default() += *value;
        }
        if !symbol_notional.is_empty() {
            rows_by_date.insert(record.date.date_naive().to_string(), symbol_notional);
        }
    }

    rows_by_date
        .into_iter()
        .flat_map(|(date, symbol_notional)| {
            let total: f64 = symbol_notional.values().sum();
            symbol_notional
                .into_iter()
                .filter_map(move |(symbol, value)| {
                    if total <= f64::EPSILON {
                        return None;
                    }
                    Some(PositionConcentrationRow {
                        date: date.clone(),
                        sector: symbol,
                        weight_pct: value / total * 100.0,
                    })
                })
        })
        .collect()
}

fn pearson(a: &[f64], b: &[f64]) -> f64 {
    if a.len() != b.len() || a.len() < 2 {
        return 0.0;
    }
    let mean_a = a.iter().sum::<f64>() / a.len() as f64;
    let mean_b = b.iter().sum::<f64>() / b.len() as f64;
    let mut covariance = 0.0;
    let mut var_a = 0.0;
    let mut var_b = 0.0;
    for (left, right) in a.iter().zip(b.iter()) {
        let da = left - mean_a;
        let db = right - mean_b;
        covariance += da * db;
        var_a += da * da;
        var_b += db * db;
    }
    let denominator = (var_a * var_b).sqrt();
    if denominator <= f64::EPSILON {
        0.0
    } else {
        covariance / denominator
    }
}

fn build_strategy_correlations(trips: &[RoundTripTrade]) -> Vec<StrategyCorrelationRow> {
    let mut by_strategy: BTreeMap<String, BTreeMap<String, f64>> = BTreeMap::new();
    for trip in trips {
        let strategy = trip
            .strategy_type
            .clone()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "Strategy".to_string());
        by_strategy
            .entry(strategy)
            .or_default()
            .entry(trip.exit_time.date_naive().to_string())
            .and_modify(|value| *value += trip.return_pct)
            .or_insert(trip.return_pct);
    }

    if by_strategy.len() < 2 {
        return Vec::new();
    }

    let dates: Vec<String> = by_strategy
        .values()
        .flat_map(|items| items.keys().cloned())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let strategies: Vec<String> = by_strategy.keys().cloned().collect();
    let mut rows = Vec::new();
    for strategy_a in &strategies {
        for strategy_b in &strategies {
            let values_a: Vec<f64> = dates
                .iter()
                .map(|date| by_strategy[strategy_a].get(date).copied().unwrap_or(0.0))
                .collect();
            let values_b: Vec<f64> = dates
                .iter()
                .map(|date| by_strategy[strategy_b].get(date).copied().unwrap_or(0.0))
                .collect();
            rows.push(StrategyCorrelationRow {
                strategy_a: strategy_a.clone(),
                strategy_b: strategy_b.clone(),
                correlation: if strategy_a == strategy_b {
                    1.0
                } else {
                    pearson(&values_a, &values_b)
                },
            });
        }
    }
    rows
}

async fn insert_analysis_tables(
    tx: &Transaction<'_>,
    results: &BacktestResults,
    state: &BacktestState,
    trips: &[RoundTripTrade],
) -> Result<(), String> {
    let mut monthly_returns_stmt = tx
        .prepare(
            "INSERT INTO monthly_returns (year, month, return_pct, payload_json) VALUES (?1, ?2, ?3, ?4)",
        )
        .await
        .map_err(to_storage_err)?;
    for row in build_monthly_returns(&results.account_history) {
        monthly_returns_stmt
            .execute(params![
                row.year,
                row.month as i64,
                row.return_pct,
                serde_json::to_string(&row).map_err(to_storage_err)?
            ])
            .await
            .map_err(to_storage_err)?;
    }

    let mut time_performance_stmt = tx
        .prepare(
            "INSERT INTO time_performance (day_of_week, hour, avg_return_bps, trade_count, payload_json) VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .await
        .map_err(to_storage_err)?;
    for row in build_time_performance(trips) {
        time_performance_stmt
            .execute(params![
                row.day_of_week as i64,
                row.hour as i64,
                row.avg_return_bps,
                row.trade_count as i64,
                serde_json::to_string(&row).map_err(to_storage_err)?
            ])
            .await
            .map_err(to_storage_err)?;
    }

    let mut drawdown_series_stmt = tx
        .prepare(
            "INSERT INTO drawdown_series (strategy_name, period, drawdown_pct, payload_json) VALUES (?1, ?2, ?3, ?4)",
        )
        .await
        .map_err(to_storage_err)?;
    for row in build_drawdown_series(&results.account_history, primary_strategy_name(trips)) {
        drawdown_series_stmt
            .execute(params![
                row.strategy_name.clone(),
                row.period.clone(),
                row.drawdown_pct,
                serde_json::to_string(&row).map_err(to_storage_err)?
            ])
            .await
            .map_err(to_storage_err)?;
    }

    let mut regime_performance_stmt = tx
        .prepare(
            "INSERT INTO regime_performance (vol_regime, trend_regime, avg_return_pct, bar_count, payload_json) VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .await
        .map_err(to_storage_err)?;
    for row in build_regime_performance(&results.account_history, &state.historical_bars) {
        regime_performance_stmt
            .execute(params![
                row.vol_regime.clone(),
                row.trend_regime.clone(),
                row.avg_return_pct,
                row.bar_count as i64,
                serde_json::to_string(&row).map_err(to_storage_err)?
            ])
            .await
            .map_err(to_storage_err)?;
    }

    let mut rolling_sharpe_stmt = tx
        .prepare(
            "INSERT INTO rolling_sharpe (window_days, period, sharpe, payload_json) VALUES (?1, ?2, ?3, ?4)",
        )
        .await
        .map_err(to_storage_err)?;
    for row in build_rolling_sharpe(&results.account_history) {
        rolling_sharpe_stmt
            .execute(params![
                row.window_days,
                row.period.clone(),
                row.sharpe,
                serde_json::to_string(&row).map_err(to_storage_err)?
            ])
            .await
            .map_err(to_storage_err)?;
    }

    let mut trade_mae_stmt = tx
        .prepare(
            "INSERT INTO trade_mae (trade_id, mae_pct, final_pnl_pct, is_winner, payload_json) VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .await
        .map_err(to_storage_err)?;
    for row in build_trade_mae(trips, &state.historical_bars, &state.trade_mae_by_order_id) {
        trade_mae_stmt
            .execute(params![
                row.trade_id.clone(),
                row.mae_pct,
                row.final_pnl_pct,
                row.is_winner,
                serde_json::to_string(&row).map_err(to_storage_err)?
            ])
            .await
            .map_err(to_storage_err)?;
    }

    let mut setup_performance_stmt = tx
        .prepare(
            "INSERT INTO setup_performance (setup_name, win_rate, payoff_ratio, trade_count, total_pnl, payload_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .await
        .map_err(to_storage_err)?;
    for row in build_setup_performance(trips) {
        setup_performance_stmt
            .execute(params![
                row.setup_name.clone(),
                row.win_rate,
                row.payoff_ratio,
                row.trade_count as i64,
                row.total_pnl,
                serde_json::to_string(&row).map_err(to_storage_err)?
            ])
            .await
            .map_err(to_storage_err)?;
    }

    let mut position_concentration_stmt = tx
        .prepare(
            "INSERT INTO position_concentration (date, sector, weight_pct, payload_json) VALUES (?1, ?2, ?3, ?4)",
        )
        .await
        .map_err(to_storage_err)?;
    for row in build_position_concentration(&results.trade_log) {
        position_concentration_stmt
            .execute(params![
                row.date.clone(),
                row.sector.clone(),
                row.weight_pct,
                serde_json::to_string(&row).map_err(to_storage_err)?
            ])
            .await
            .map_err(to_storage_err)?;
    }

    let mut strategy_correlations_stmt = tx
        .prepare(
            "INSERT INTO strategy_correlations (strategy_a, strategy_b, correlation, payload_json) VALUES (?1, ?2, ?3, ?4)",
        )
        .await
        .map_err(to_storage_err)?;
    for row in build_strategy_correlations(trips) {
        strategy_correlations_stmt
            .execute(params![
                row.strategy_a.clone(),
                row.strategy_b.clone(),
                row.correlation,
                serde_json::to_string(&row).map_err(to_storage_err)?
            ])
            .await
            .map_err(to_storage_err)?;
    }

    Ok(())
}

pub async fn write_backtest_db(
    dir_path: &Path,
    results: &BacktestResults,
    state: &BacktestState,
) -> Result<(), String> {
    std::fs::create_dir_all(dir_path).map_err(to_storage_err)?;
    let mut conn = connect_database(dir_path).await?;
    init_schema(&conn).await?;
    let tx = conn.transaction().await.map_err(to_storage_err)?;
    insert_trade_log(&tx, &results.trade_log).await?;
    let round_trips = results.round_trip_trades();
    insert_round_trips(&tx, &round_trips).await?;
    insert_trade_log_rows(&tx, &round_trips, &results.trade_log).await?;
    insert_account_history(&tx, &results.account_history).await?;
    let insights: Vec<InsightSnapshot> = state.insight_snapshots.values().cloned().collect();
    insert_insights(&tx, &insights).await?;
    insert_bars(&tx, &state.historical_bars).await?;
    insert_analysis_tables(&tx, results, state, &round_trips).await?;
    tx.commit().await.map_err(to_storage_err)?;
    Ok(())
}
