use std::path::Path;

use chrono::{DateTime, Utc};
use turso::{Builder, Connection, params};

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
        "#,
    )
    .await
    .map_err(to_storage_err)?;
    Ok(())
}

async fn insert_trade_log(conn: &Connection, trade_log: &[TradeRecord]) -> Result<(), String> {
    for record in trade_log {
        conn.execute(
            "INSERT INTO trade_log (event_at, symbol, side, qty, price, order_id, insight_id, strategy_type, trade_type, payload_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
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
            ],
        )
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
    conn: &Connection,
    round_trips: &[RoundTripTrade],
    trade_events: &[TradeRecord],
) -> Result<(), String> {
    let rows = build_trade_log_rows(round_trips, trade_events);
    for row in rows {
        conn.execute(
            "INSERT INTO trade_log_rows (symbol, entry_time, insight_id, status, payload_json)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                row.symbol.clone(),
                row.entry_time.clone(),
                row.insight_id.clone(),
                row.status.clone(),
                serde_json::to_string(&row).map_err(to_storage_err)?
            ],
        )
        .await
        .map_err(to_storage_err)?;
    }
    Ok(())
}

async fn insert_round_trips(conn: &Connection, trips: &[RoundTripTrade]) -> Result<(), String> {
    for trip in trips {
        conn.execute(
            "INSERT INTO round_trips (symbol, side, insight_id, strategy_type, entry_time, exit_time, entry_price, exit_price, qty, pnl, return_pct, hold_secs, payload_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
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
            ],
        )
        .await
        .map_err(to_storage_err)?;
    }
    Ok(())
}

async fn insert_account_history(
    conn: &Connection,
    account_history: &[(DateTime<Utc>, Account)],
) -> Result<(), String> {
    for (timestamp, account) in account_history {
        let payload = serde_json::to_string(&AccountHistoryItem {
            timestamp: *timestamp,
            equity: account.equity,
        })
        .map_err(to_storage_err)?;
        conn.execute(
            "INSERT INTO account_history (event_at, equity, cash, buying_power, payload_json)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                timestamp.to_rfc3339(),
                account.equity,
                account.cash,
                account.buying_power,
                payload
            ],
        )
        .await
        .map_err(to_storage_err)?;
    }
    Ok(())
}

async fn insert_insights(conn: &Connection, insights: &[InsightSnapshot]) -> Result<(), String> {
    for insight in insights {
        conn.execute(
            "INSERT OR REPLACE INTO insights (insight_id, symbol, strategy_type, state, created_at, updated_at, filled_at, closed_at, payload_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                insight.insight_id.clone(),
                insight.symbol.clone(),
                insight.strategy_type.clone(),
                insight.state.clone(),
                insight.created_at.to_rfc3339(),
                insight.updated_at.to_rfc3339(),
                insight.filled_at.map(|value| value.to_rfc3339()),
                insight.closed_at.map(|value| value.to_rfc3339()),
                serde_json::to_string(insight).map_err(to_storage_err)?
            ],
        )
        .await
        .map_err(to_storage_err)?;
    }
    Ok(())
}

async fn insert_bars(
    conn: &Connection,
    bars_by_symbol: &std::collections::HashMap<String, Vec<Bar>>,
) -> Result<(), String> {
    for bars in bars_by_symbol.values() {
        for bar in bars {
            conn.execute(
                "INSERT INTO bars (symbol, event_at, open, high, low, close, volume, payload_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    bar.symbol.clone(),
                    bar.timestamp.to_rfc3339(),
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.volume,
                    serde_json::to_string(bar).map_err(to_storage_err)?
                ],
            )
            .await
            .map_err(to_storage_err)?;
        }
    }
    Ok(())
}

pub async fn write_backtest_db(
    dir_path: &Path,
    results: &BacktestResults,
    state: &BacktestState,
) -> Result<(), String> {
    std::fs::create_dir_all(dir_path).map_err(to_storage_err)?;
    let conn = connect_database(dir_path).await?;
    init_schema(&conn).await?;
    insert_trade_log(&conn, &results.trade_log).await?;
    let round_trips = results.round_trip_trades();
    insert_round_trips(&conn, &round_trips).await?;
    insert_trade_log_rows(&conn, &round_trips, &results.trade_log).await?;
    insert_account_history(&conn, &results.account_history).await?;
    let insights: Vec<InsightSnapshot> = state.insight_snapshots.values().cloned().collect();
    insert_insights(&conn, &insights).await?;
    insert_bars(&conn, &state.historical_bars).await?;
    Ok(())
}
