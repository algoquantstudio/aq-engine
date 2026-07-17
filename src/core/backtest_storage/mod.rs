use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use chrono::{DateTime, Datelike, Timelike, Utc};
use polars::lazy::dsl::pearson_corr;
use polars::prelude::*;
use turso::{Builder, Connection, Value, params, transaction::Transaction};

mod types;

use crate::core::broker::backtest_state::BacktestState;
use crate::core::broker::backtest_state::{BacktestResults, RoundTripTrade};
use crate::core::broker::types::{Account, Bar, TradeRecord};
use crate::core::insight::InsightSnapshot;

pub use types::BacktestTradeLogRow;

pub const BACKTEST_DB_FILE: &str = "backtest.db";
pub const BACKTEST_DB_APPLICATION_ID: u32 = 0x4151_4254; // "AQBT"
pub const BACKTEST_DB_USER_VERSION: u32 = 1;

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
        PRAGMA application_id = 1095844436;
        PRAGMA user_version = 1;
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
            commission REAL NOT NULL DEFAULT 0.0,
            swap REAL NOT NULL DEFAULT 0.0,
            trade_type TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS trade_log_rows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            strategy_type TEXT,
            parent_id TEXT,
            is_child INTEGER NOT NULL DEFAULT 0,
            base_strategy_type TEXT,
            entry_time TEXT NOT NULL,
            exit_time TEXT,
            insight_id TEXT,
            qty REAL NOT NULL,
            entry_price REAL NOT NULL,
            exit_price REAL,
            return_pct REAL,
            pnl REAL,
            commission REAL NOT NULL DEFAULT 0.0,
            swap REAL NOT NULL DEFAULT 0.0,
            status TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS round_trips (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT NOT NULL,
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
            commission REAL NOT NULL DEFAULT 0.0,
            swap REAL NOT NULL DEFAULT 0.0,
            return_pct REAL NOT NULL,
            hold_secs INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS account_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_at TEXT NOT NULL,
            equity REAL NOT NULL,
            cash REAL NOT NULL,
            buying_power REAL NOT NULL,
            accrued_commission REAL NOT NULL DEFAULT 0.0
        );
        CREATE TABLE IF NOT EXISTS insights (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            insight_id TEXT NOT NULL UNIQUE,
            parent_id TEXT,
            order_id TEXT,
            side TEXT NOT NULL,
            symbol TEXT NOT NULL,
            quantity REAL,
            contracts REAL,
            order_type TEXT NOT NULL,
            order_class TEXT NOT NULL,
            limit_price REAL,
            stop_price REAL,
            take_profit_levels_json TEXT NOT NULL,
            stop_loss_levels_json TEXT NOT NULL,
            trailing_stop_price REAL,
            strategy_type TEXT NOT NULL,
            confidence INTEGER NOT NULL,
            timeframe_json TEXT NOT NULL,
            period_unfilled INTEGER,
            period_till_tp INTEGER,
            execution_depends_json TEXT NOT NULL,
            filled_price REAL,
            close_order_id TEXT,
            close_price REAL,
            broker_realized_pnl REAL,
            commission REAL,
            swap REAL,
            partial_closes_json TEXT NOT NULL,
            state TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            filled_at TEXT,
            closed_at TEXT,
            legs_json TEXT NOT NULL,
            market_changed INTEGER NOT NULL DEFAULT 0,
            submitted INTEGER NOT NULL DEFAULT 0,
            cancelling INTEGER NOT NULL DEFAULT 0,
            closing INTEGER NOT NULL DEFAULT 0,
            first_on_fill INTEGER NOT NULL DEFAULT 0,
            partial_filled_quantity REAL,
            state_history_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS bars (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            history_key TEXT NOT NULL DEFAULT '',
            timeframe_label TEXT NOT NULL DEFAULT '',
            is_feature INTEGER NOT NULL DEFAULT 0,
            allow_trading INTEGER NOT NULL DEFAULT 1,
            event_at TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS monthly_returns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INT NOT NULL,
            month INT NOT NULL,
            return_pct REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS param_sweep (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            param1_name TEXT NOT NULL,
            param1_value REAL NOT NULL,
            param2_name TEXT NOT NULL,
            param2_value REAL NOT NULL,
            sharpe REAL NOT NULL,
            total_return REAL NOT NULL,
            max_drawdown REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS time_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            day_of_week INT NOT NULL,
            hour INT NOT NULL,
            avg_return_bps REAL NOT NULL,
            trade_count INT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS drawdown_series (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_name TEXT NOT NULL,
            period TEXT NOT NULL,
            drawdown_pct REAL NOT NULL,
            series_type TEXT NOT NULL,
            basis TEXT NOT NULL,
            cumulative_pnl REAL,
            cumulative_return_pct REAL,
            drawdown_pnl REAL
        );
        CREATE TABLE IF NOT EXISTS regime_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vol_regime TEXT NOT NULL,
            trend_regime TEXT NOT NULL,
            avg_return_pct REAL NOT NULL,
            bar_count INT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS strategy_regime_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_type TEXT NOT NULL,
            vol_regime TEXT NOT NULL,
            trend_regime TEXT NOT NULL,
            trade_count INT NOT NULL,
            win_rate REAL NOT NULL,
            total_pnl REAL NOT NULL,
            avg_return_pct REAL NOT NULL,
            profit_factor REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS factor_exposure (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            factor_name TEXT NOT NULL,
            beta REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS strategy_correlations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_a TEXT NOT NULL,
            strategy_b TEXT NOT NULL,
            correlation REAL NOT NULL,
            sample_count INT NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS position_concentration (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            sector TEXT NOT NULL,
            weight_pct REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS slippage_analysis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id TEXT NOT NULL,
            expected_cost_bps REAL NOT NULL,
            actual_cost_bps REAL NOT NULL,
            order_size REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS trade_mae (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id TEXT NOT NULL,
            mae_pct REAL NOT NULL,
            final_pnl_pct REAL NOT NULL,
            is_winner BOOL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS setup_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            setup_name TEXT NOT NULL,
            win_rate REAL NOT NULL,
            payoff_ratio REAL NOT NULL,
            trade_count INT NOT NULL,
            total_pnl REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS rolling_sharpe (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            window_days INT NOT NULL,
            period TEXT NOT NULL,
            sharpe REAL NOT NULL
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
            test_end TEXT NOT NULL
        );
        "#,
    )
    .await
    .map_err(to_storage_err)?;
    Ok(())
}

async fn checkpoint_database(conn: &Connection) -> Result<(), String> {
    let mut rows = conn
        .query("PRAGMA wal_checkpoint(TRUNCATE)", ())
        .await
        .map_err(to_storage_err)?;
    while rows.next().await.map_err(to_storage_err)?.is_some() {}
    Ok(())
}

const SECONDARY_INDEXES: &[(&str, &str)] = &[
    ("idx_trade_log_symbol_at", "trade_log(symbol, event_at)"),
    ("idx_trade_log_insight_id", "trade_log(insight_id)"),
    (
        "idx_trade_log_rows_symbol_entry_time",
        "trade_log_rows(symbol, entry_time)",
    ),
    (
        "idx_trade_log_rows_insight_id",
        "trade_log_rows(insight_id)",
    ),
    (
        "idx_trade_log_rows_strategy",
        "trade_log_rows(base_strategy_type, is_child)",
    ),
    (
        "idx_round_trips_symbol_entry_time",
        "round_trips(symbol, entry_time)",
    ),
    ("idx_round_trips_insight_id", "round_trips(insight_id)"),
    ("idx_account_history_event_at", "account_history(event_at)"),
    (
        "idx_insights_symbol_created_at",
        "insights(symbol, created_at)",
    ),
    (
        "idx_insights_strategy_state",
        "insights(strategy_type, state)",
    ),
    ("idx_bars_symbol_event_at", "bars(symbol, event_at)"),
    (
        "idx_bars_history_key_event_at",
        "bars(history_key, event_at)",
    ),
    ("idx_monthly_returns_period", "monthly_returns(year, month)"),
    (
        "idx_time_performance_slot",
        "time_performance(day_of_week, hour)",
    ),
    (
        "idx_drawdown_series_period",
        "drawdown_series(strategy_name, period)",
    ),
    (
        "idx_strategy_regime_performance_key",
        "strategy_regime_performance(strategy_type, vol_regime, trend_regime)",
    ),
    (
        "idx_position_concentration_date",
        "position_concentration(date)",
    ),
];

async fn drop_secondary_indexes(tx: &Transaction<'_>) -> Result<(), String> {
    let sql = SECONDARY_INDEXES
        .iter()
        .map(|(name, _)| format!("DROP INDEX IF EXISTS {name};"))
        .collect::<Vec<_>>()
        .join("\n");
    tx.execute_batch(sql).await.map_err(to_storage_err)
}

async fn create_secondary_indexes(tx: &Transaction<'_>) -> Result<(), String> {
    let sql = SECONDARY_INDEXES
        .iter()
        .map(|(name, target)| format!("CREATE INDEX IF NOT EXISTS {name} ON {target};"))
        .collect::<Vec<_>>()
        .join("\n");
    tx.execute_batch(sql).await.map_err(to_storage_err)
}

const MAX_BATCH_PARAMETERS: usize = 900;

async fn flush_insert_batch(
    tx: &Transaction<'_>,
    insert_prefix: &str,
    columns_per_row: usize,
    rows: &mut Vec<Vec<Value>>,
) -> Result<(), String> {
    if rows.is_empty() {
        return Ok(());
    }

    let placeholders = format!("({})", vec!["?"; columns_per_row].join(", "));
    let sql = format!(
        "{insert_prefix} {}",
        vec![placeholders; rows.len()].join(", ")
    );
    let values = rows.drain(..).flatten().collect::<Vec<_>>();
    tx.execute(sql, values).await.map_err(to_storage_err)?;
    Ok(())
}

async fn insert_trade_log(tx: &Transaction<'_>, trade_log: &[TradeRecord]) -> Result<(), String> {
    const COLUMNS: usize = 11;
    let mut rows = Vec::with_capacity(MAX_BATCH_PARAMETERS / COLUMNS);
    for record in trade_log {
        rows.push(vec![
            Value::Text(record.date.to_rfc3339()),
            Value::Text(record.symbol.clone()),
            Value::Text(format!("{:?}", record.side)),
            Value::Real(record.qty),
            Value::Real(record.price),
            Value::Text(record.order_id.clone()),
            record
                .insight_id
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
            record
                .strategy_type
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
            Value::Real(record.commission),
            Value::Real(record.swap),
            Value::Text(format!("{:?}", record.trade_type)),
        ]);
        if rows.len() >= MAX_BATCH_PARAMETERS / COLUMNS {
            flush_insert_batch(
                tx,
                "INSERT INTO trade_log (event_at, symbol, side, qty, price, order_id, insight_id, strategy_type, commission, swap, trade_type) VALUES",
                COLUMNS,
                &mut rows,
            )
            .await?;
        }
    }
    flush_insert_batch(
        tx,
        "INSERT INTO trade_log (event_at, symbol, side, qty, price, order_id, insight_id, strategy_type, commission, swap, trade_type) VALUES",
        COLUMNS,
        &mut rows,
    )
    .await
}

fn format_backtest_timestamp(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%d %H:%M:%S").to_string()
}

fn normalized_strategy_type(strategy_type: Option<&str>) -> Option<String> {
    strategy_type
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .strip_suffix("-CHILD")
                .unwrap_or(value)
                .trim()
                .to_string()
        })
        .filter(|value| !value.is_empty())
}

fn strategy_label(strategy_type: Option<&str>, fallback: &str) -> String {
    normalized_strategy_type(strategy_type).unwrap_or_else(|| fallback.to_string())
}

#[derive(Clone, Debug)]
struct TradeRowMetadata {
    strategy_type: Option<String>,
    parent_id: Option<String>,
    is_child: bool,
    base_strategy_type: Option<String>,
}

fn trade_row_metadata(
    insight_id: Option<&str>,
    strategy_type: Option<&str>,
    insight_lookup: &HashMap<String, &InsightSnapshot>,
) -> TradeRowMetadata {
    let insight = insight_id.and_then(|id| insight_lookup.get(id).copied());
    let effective_strategy_type = strategy_type
        .map(str::to_string)
        .or_else(|| insight.map(|snapshot| snapshot.strategy_type.clone()));
    let parent_id = insight.and_then(|snapshot| snapshot.parent_id.clone());
    let strategy_marks_child = effective_strategy_type
        .as_deref()
        .map(|value| value.trim().ends_with("-CHILD"))
        .unwrap_or(false);
    let is_child = parent_id.is_some() || strategy_marks_child;
    let base_strategy_type = normalized_strategy_type(effective_strategy_type.as_deref());

    TradeRowMetadata {
        strategy_type: effective_strategy_type,
        parent_id,
        is_child,
        base_strategy_type,
    }
}

fn build_trade_log_rows(
    trade_events: &[TradeRecord],
    insights: &[InsightSnapshot],
) -> Vec<BacktestTradeLogRow> {
    let mut rows = Vec::new();
    let mut entry_remaining_by_order: std::collections::HashMap<String, (TradeRecord, f64)> =
        std::collections::HashMap::new();
    let insight_lookup: HashMap<String, &InsightSnapshot> = insights
        .iter()
        .map(|insight| (insight.insight_id.clone(), insight))
        .collect();

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
                    let gross_pnl = match entry.side {
                        crate::core::broker::types::OrderSide::Buy => {
                            (trade.price - entry.price) * exit_qty
                        }
                        crate::core::broker::types::OrderSide::Sell => {
                            (entry.price - trade.price) * exit_qty
                        }
                    };
                    let entry_commission = if entry.qty.abs() > f64::EPSILON {
                        entry.commission * (exit_qty / entry.qty)
                    } else {
                        0.0
                    };
                    let commission = entry_commission + trade.commission;
                    let swap = trade.swap;
                    let pnl = gross_pnl + swap - commission;
                    let return_pct = if entry.price.abs() > f64::EPSILON {
                        (pnl / (entry.price * exit_qty)) * 100.0
                    } else {
                        0.0
                    };
                    let metadata = trade_row_metadata(
                        trade.insight_id.as_deref().or(entry.insight_id.as_deref()),
                        trade
                            .strategy_type
                            .as_deref()
                            .or(entry.strategy_type.as_deref()),
                        &insight_lookup,
                    );

                    rows.push(BacktestTradeLogRow {
                        id: 0,
                        symbol: trade.symbol.clone(),
                        side: format!("{:?}", trade.side).to_uppercase(),
                        strategy_type: metadata.strategy_type,
                        parent_id: metadata.parent_id,
                        is_child: metadata.is_child,
                        base_strategy_type: metadata.base_strategy_type,
                        insight_id: trade
                            .insight_id
                            .clone()
                            .or_else(|| entry.insight_id.clone()),
                        entry_time: format_backtest_timestamp(entry.date),
                        exit_time: Some(format_backtest_timestamp(trade.date)),
                        qty: exit_qty,
                        entry_price: entry.price,
                        exit_price: Some(trade.price),
                        return_pct: Some(return_pct),
                        pnl: Some(pnl),
                        commission: Some(commission),
                        swap: Some(swap),
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
                    let metadata = trade_row_metadata(
                        trade.insight_id.as_deref(),
                        trade.strategy_type.as_deref(),
                        &insight_lookup,
                    );
                    rows.push(BacktestTradeLogRow {
                        id: 0,
                        symbol: trade.symbol.clone(),
                        side: format!("{:?}", trade.side).to_uppercase(),
                        strategy_type: metadata.strategy_type,
                        parent_id: metadata.parent_id,
                        is_child: metadata.is_child,
                        base_strategy_type: metadata.base_strategy_type,
                        insight_id: trade.insight_id.clone(),
                        entry_time: format_backtest_timestamp(trade.date),
                        exit_time: None,
                        qty: trade.qty,
                        entry_price: trade.price,
                        exit_price: None,
                        return_pct: None,
                        pnl: None,
                        commission: Some(trade.commission),
                        swap: Some(trade.swap),
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
        let metadata = trade_row_metadata(
            trade.insight_id.as_deref(),
            trade.strategy_type.as_deref(),
            &insight_lookup,
        );
        rows.push(BacktestTradeLogRow {
            id: next_id,
            symbol: trade.symbol.clone(),
            side: format!("{:?}", trade.side).to_uppercase(),
            strategy_type: metadata.strategy_type,
            parent_id: metadata.parent_id,
            is_child: metadata.is_child,
            base_strategy_type: metadata.base_strategy_type,
            insight_id: trade.insight_id.clone(),
            entry_time: format_backtest_timestamp(trade.date),
            exit_time: None,
            qty: remaining_qty,
            entry_price: trade.price,
            exit_price: None,
            return_pct: None,
            pnl: None,
            commission: Some(trade.commission),
            swap: Some(trade.swap),
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
    trade_events: &[TradeRecord],
    insights: &[InsightSnapshot],
) -> Result<(), String> {
    let rows = build_trade_log_rows(trade_events, insights);
    let mut stmt = tx
        .prepare(
            "INSERT INTO trade_log_rows (symbol, side, strategy_type, parent_id, is_child, base_strategy_type, entry_time, exit_time, insight_id, qty, entry_price, exit_price, return_pct, pnl, commission, swap, status)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
        )
        .await
        .map_err(to_storage_err)?;
    for row in rows {
        stmt.execute(params![
            row.symbol.clone(),
            row.side.clone(),
            row.strategy_type.clone(),
            row.parent_id.clone(),
            if row.is_child { 1_i64 } else { 0_i64 },
            row.base_strategy_type.clone(),
            row.entry_time.clone(),
            row.exit_time.clone(),
            row.insight_id.clone(),
            row.qty,
            row.entry_price,
            row.exit_price,
            row.return_pct,
            row.pnl,
            row.commission.unwrap_or(0.0),
            row.swap.unwrap_or(0.0),
            row.status.clone()
        ])
        .await
        .map_err(to_storage_err)?;
    }
    Ok(())
}

async fn insert_round_trips(tx: &Transaction<'_>, trips: &[RoundTripTrade]) -> Result<(), String> {
    let mut stmt = tx
        .prepare(
            "INSERT INTO round_trips (order_id, symbol, side, insight_id, strategy_type, entry_time, exit_time, entry_price, exit_price, qty, pnl, commission, swap, return_pct, hold_secs)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
        )
        .await
        .map_err(to_storage_err)?;
    for trip in trips {
        stmt.execute(params![
            trip.order_id.clone(),
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
            trip.commission,
            trip.swap,
            trip.return_pct,
            trip.hold_secs
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
    const COLUMNS: usize = 5;
    let mut rows = Vec::with_capacity(MAX_BATCH_PARAMETERS / COLUMNS);
    for (timestamp, account) in account_history {
        rows.push(vec![
            Value::Text(timestamp.to_rfc3339()),
            Value::Real(account.equity),
            Value::Real(account.cash),
            Value::Real(account.buying_power),
            Value::Real(account.accrued_commission),
        ]);
        if rows.len() >= MAX_BATCH_PARAMETERS / COLUMNS {
            flush_insert_batch(
                tx,
                "INSERT INTO account_history (event_at, equity, cash, buying_power, accrued_commission) VALUES",
                COLUMNS,
                &mut rows,
            )
            .await?;
        }
    }
    flush_insert_batch(
        tx,
        "INSERT INTO account_history (event_at, equity, cash, buying_power, accrued_commission) VALUES",
        COLUMNS,
        &mut rows,
    )
    .await
}

async fn insert_insights(tx: &Transaction<'_>, insights: &[InsightSnapshot]) -> Result<(), String> {
    let mut stmt = tx
        .prepare(
            "INSERT OR REPLACE INTO insights (insight_id, parent_id, order_id, side, symbol, quantity, contracts, order_type, order_class, limit_price, stop_price, take_profit_levels_json, stop_loss_levels_json, trailing_stop_price, strategy_type, confidence, timeframe_json, period_unfilled, period_till_tp, execution_depends_json, filled_price, close_order_id, close_price, broker_realized_pnl, commission, swap, partial_closes_json, state, created_at, updated_at, filled_at, closed_at, legs_json, market_changed, submitted, cancelling, closing, first_on_fill, partial_filled_quantity, state_history_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33, ?34, ?35, ?36, ?37, ?38, ?39, ?40)",
        )
        .await
        .map_err(to_storage_err)?;
    for insight in insights {
        stmt.execute(params![
            insight.insight_id.clone(),
            insight.parent_id.clone(),
            insight.order_id.clone(),
            insight.side.clone(),
            insight.symbol.clone(),
            insight.quantity,
            insight.contracts,
            insight.order_type.clone(),
            insight.order_class.clone(),
            insight.limit_price,
            insight.stop_price,
            serde_json::to_string(&insight.take_profit_levels).map_err(to_storage_err)?,
            serde_json::to_string(&insight.stop_loss_levels).map_err(to_storage_err)?,
            insight.trailing_stop_price,
            insight.strategy_type.clone(),
            insight.confidence as i64,
            serde_json::to_string(&insight.timeframe).map_err(to_storage_err)?,
            insight.period_unfilled.map(|value| value as i64),
            insight.period_till_tp.map(|value| value as i64),
            serde_json::to_string(&insight.execution_depends).map_err(to_storage_err)?,
            insight.filled_price,
            insight.close_order_id.clone(),
            insight.close_price,
            insight.broker_realized_pnl,
            insight.commission,
            insight.swap,
            serde_json::to_string(&insight.partial_closes).map_err(to_storage_err)?,
            insight.state.clone(),
            insight.created_at.to_rfc3339(),
            insight.updated_at.to_rfc3339(),
            insight.filled_at.map(|value| value.to_rfc3339()),
            insight.closed_at.map(|value| value.to_rfc3339()),
            serde_json::to_string(&insight.legs).map_err(to_storage_err)?,
            if insight.market_changed { 1_i64 } else { 0_i64 },
            if insight.submitted { 1_i64 } else { 0_i64 },
            if insight.cancelling { 1_i64 } else { 0_i64 },
            if insight.closing { 1_i64 } else { 0_i64 },
            if insight.first_on_fill { 1_i64 } else { 0_i64 },
            insight.partial_filled_quantity,
            serde_json::to_string(&insight.state_history).map_err(to_storage_err)?
        ])
        .await
        .map_err(to_storage_err)?;
    }
    Ok(())
}

async fn insert_bars(tx: &Transaction<'_>, state: &BacktestState) -> Result<(), String> {
    const COLUMNS: usize = 11;
    const INSERT_PREFIX: &str = "INSERT INTO bars (symbol, history_key, timeframe_label, is_feature, allow_trading, event_at, open, high, low, close, volume) VALUES";
    let mut rows = Vec::with_capacity(MAX_BATCH_PARAMETERS / COLUMNS);

    if !state.event_stream_bars.is_empty() {
        let mut keys = state.event_stream_bars.keys().cloned().collect::<Vec<_>>();
        keys.sort_by(|left, right| {
            left.symbol.cmp(&right.symbol).then(
                left.timeframe
                    .compact_label()
                    .cmp(&right.timeframe.compact_label()),
            )
        });

        for key in keys {
            let Some(bars) = state.event_stream_bars.get(&key) else {
                continue;
            };
            let Some(stream) = state.event_streams.get(&key) else {
                continue;
            };
            for bar in bars {
                rows.push(vec![
                    Value::Text(bar.symbol.clone()),
                    Value::Text(stream.history_key.clone()),
                    Value::Text(key.timeframe.compact_label()),
                    Value::Integer(if stream.is_feature { 1 } else { 0 }),
                    Value::Integer(if stream.allow_trading { 1 } else { 0 }),
                    Value::Text(bar.timestamp.to_rfc3339()),
                    Value::Real(bar.open),
                    Value::Real(bar.high),
                    Value::Real(bar.low),
                    Value::Real(bar.close),
                    Value::Real(bar.volume),
                ]);
                if rows.len() >= MAX_BATCH_PARAMETERS / COLUMNS {
                    flush_insert_batch(tx, INSERT_PREFIX, COLUMNS, &mut rows).await?;
                }
            }
        }
        return flush_insert_batch(tx, INSERT_PREFIX, COLUMNS, &mut rows).await;
    }

    for (symbol, bars) in &state.historical_bars {
        for bar in bars {
            rows.push(vec![
                Value::Text(symbol.clone()),
                Value::Text(symbol.clone()),
                Value::Text(String::new()),
                Value::Integer(0),
                Value::Integer(1),
                Value::Text(bar.timestamp.to_rfc3339()),
                Value::Real(bar.open),
                Value::Real(bar.high),
                Value::Real(bar.low),
                Value::Real(bar.close),
                Value::Real(bar.volume),
            ]);
            if rows.len() >= MAX_BATCH_PARAMETERS / COLUMNS {
                flush_insert_batch(tx, INSERT_PREFIX, COLUMNS, &mut rows).await?;
            }
        }
    }
    flush_insert_batch(tx, INSERT_PREFIX, COLUMNS, &mut rows).await
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

#[derive(Debug, Clone, serde::Serialize)]
struct DrawdownSeriesRow {
    strategy_name: String,
    period: String,
    drawdown_pct: f64,
    series_type: String,
    basis: String,
    cumulative_pnl: Option<f64>,
    cumulative_return_pct: Option<f64>,
    drawdown_pnl: Option<f64>,
}

#[derive(Debug, serde::Serialize)]
struct RegimePerformanceRow {
    vol_regime: String,
    trend_regime: String,
    avg_return_pct: f64,
    bar_count: usize,
}

#[derive(Debug, serde::Serialize)]
struct StrategyRegimePerformanceRow {
    strategy_type: String,
    vol_regime: String,
    trend_regime: String,
    trade_count: usize,
    win_rate: f64,
    total_pnl: f64,
    avg_return_pct: f64,
    profit_factor: f64,
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
    sample_count: usize,
}

fn account_history_frame(account_history: &[(DateTime<Utc>, Account)]) -> PolarsResult<DataFrame> {
    let mut timestamp_ms = Vec::with_capacity(account_history.len());
    let mut years = Vec::with_capacity(account_history.len());
    let mut months = Vec::with_capacity(account_history.len());
    let mut periods = Vec::with_capacity(account_history.len());
    let mut equities = Vec::with_capacity(account_history.len());

    for (timestamp, account) in account_history {
        timestamp_ms.push(timestamp.timestamp_millis());
        years.push(timestamp.year());
        months.push(timestamp.month() as i32);
        periods.push(month_period(*timestamp));
        equities.push(account.equity);
    }

    DataFrame::new(vec![
        Column::new("timestamp_ms".into(), timestamp_ms.as_slice()),
        Column::new("year".into(), years.as_slice()),
        Column::new("month".into(), months.as_slice()),
        Column::new("period".into(), periods.as_slice()),
        Column::new("equity".into(), equities.as_slice()),
    ])
}

fn round_trips_frame(trips: &[RoundTripTrade]) -> PolarsResult<DataFrame> {
    let mut symbols = Vec::with_capacity(trips.len());
    let mut sides = Vec::with_capacity(trips.len());
    let mut strategy_types = Vec::with_capacity(trips.len());
    let mut entry_day_of_week = Vec::with_capacity(trips.len());
    let mut entry_hour = Vec::with_capacity(trips.len());
    let mut entry_ms = Vec::with_capacity(trips.len());
    let mut exit_ms = Vec::with_capacity(trips.len());
    let mut exit_dates = Vec::with_capacity(trips.len());
    let mut periods = Vec::with_capacity(trips.len());
    let mut pnl_values = Vec::with_capacity(trips.len());
    let mut return_pct_values = Vec::with_capacity(trips.len());
    let mut return_bps_values = Vec::with_capacity(trips.len());
    let mut is_win_values = Vec::with_capacity(trips.len());
    let mut is_loss_values = Vec::with_capacity(trips.len());
    let mut gross_profit_values = Vec::with_capacity(trips.len());
    let mut gross_loss_values = Vec::with_capacity(trips.len());

    for trip in trips {
        symbols.push(trip.symbol.clone());
        sides.push(format!("{:?}", trip.side));
        strategy_types.push(strategy_label(trip.strategy_type.as_deref(), "Unknown"));
        entry_day_of_week.push(trip.entry_time.weekday().number_from_monday() as i32);
        entry_hour.push(trip.entry_time.hour() as i32);
        entry_ms.push(trip.entry_time.timestamp_millis());
        exit_ms.push(trip.exit_time.timestamp_millis());
        exit_dates.push(trip.exit_time.date_naive().to_string());
        periods.push(month_period(trip.exit_time));
        pnl_values.push(trip.pnl);
        return_pct_values.push(trip.return_pct);
        return_bps_values.push(trip.return_pct * 100.0);
        is_win_values.push(if trip.pnl > 0.0 { 1_i32 } else { 0_i32 });
        is_loss_values.push(if trip.pnl < 0.0 { 1_i32 } else { 0_i32 });
        gross_profit_values.push(if trip.pnl > 0.0 { trip.pnl } else { 0.0 });
        gross_loss_values.push(if trip.pnl < 0.0 { trip.pnl.abs() } else { 0.0 });
    }

    DataFrame::new(vec![
        Column::new("symbol".into(), symbols.as_slice()),
        Column::new("side".into(), sides.as_slice()),
        Column::new("strategy_type".into(), strategy_types.as_slice()),
        Column::new("entry_day_of_week".into(), entry_day_of_week.as_slice()),
        Column::new("entry_hour".into(), entry_hour.as_slice()),
        Column::new("entry_ms".into(), entry_ms.as_slice()),
        Column::new("exit_ms".into(), exit_ms.as_slice()),
        Column::new("exit_date".into(), exit_dates.as_slice()),
        Column::new("period".into(), periods.as_slice()),
        Column::new("pnl".into(), pnl_values.as_slice()),
        Column::new("return_pct".into(), return_pct_values.as_slice()),
        Column::new("return_bps".into(), return_bps_values.as_slice()),
        Column::new("is_win".into(), is_win_values.as_slice()),
        Column::new("is_loss".into(), is_loss_values.as_slice()),
        Column::new("gross_profit".into(), gross_profit_values.as_slice()),
        Column::new("gross_loss".into(), gross_loss_values.as_slice()),
    ])
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

fn rolling_period(timestamp: DateTime<Utc>, duration_days: f64) -> String {
    if duration_days <= 2.0 {
        timestamp.format("%Y-%m-%d %H:00").to_string()
    } else if duration_days <= 14.0 {
        timestamp.format("%Y-%m-%d").to_string()
    } else {
        quarter_period(timestamp)
    }
}

fn build_monthly_returns_iter(
    account_history: &[(DateTime<Utc>, Account)],
) -> Vec<MonthlyReturnRow> {
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

fn build_monthly_returns_polars(
    account_history: &[(DateTime<Utc>, Account)],
) -> PolarsResult<Vec<MonthlyReturnRow>> {
    if account_history.is_empty() {
        return Ok(Vec::new());
    }

    let df = account_history_frame(account_history)?;

    let out = df
        .lazy()
        .select([col("year"), col("month"), col("equity")])
        .group_by([col("year"), col("month")])
        .agg([
            col("equity").first().alias("first_equity"),
            col("equity").last().alias("last_equity"),
        ])
        .with_columns([when(col("first_equity").abs().gt(lit(f64::EPSILON)))
            .then(((col("last_equity") - col("first_equity")) / col("first_equity")) * lit(100.0))
            .otherwise(lit(0.0))
            .alias("return_pct")])
        .select([col("year"), col("month"), col("return_pct")])
        .sort(["year", "month"], Default::default())
        .collect()?;

    let year_col = out.column("year")?.i32()?;
    let month_col = out.column("month")?.i32()?;
    let return_col = out.column("return_pct")?.f64()?;

    let rows = year_col
        .into_iter()
        .zip(month_col.into_iter())
        .zip(return_col.into_iter())
        .map(|((year, month), return_pct)| MonthlyReturnRow {
            year: year.unwrap_or_default(),
            month: month.unwrap_or_default().max(0) as u32,
            return_pct: return_pct.unwrap_or(0.0),
        })
        .collect();

    Ok(rows)
}

fn build_monthly_returns(account_history: &[(DateTime<Utc>, Account)]) -> Vec<MonthlyReturnRow> {
    build_monthly_returns_polars(account_history)
        .unwrap_or_else(|_| build_monthly_returns_iter(account_history))
}

fn build_time_performance_iter(trips: &[RoundTripTrade]) -> Vec<TimePerformanceRow> {
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

fn build_time_performance_polars(
    trips: &[RoundTripTrade],
) -> PolarsResult<Vec<TimePerformanceRow>> {
    if trips.is_empty() {
        return Ok(Vec::new());
    }

    let df = round_trips_frame(trips)?;

    let out = df
        .lazy()
        .select([
            col("entry_day_of_week").alias("day_of_week"),
            col("entry_hour").alias("hour"),
            col("return_bps"),
        ])
        .group_by([col("day_of_week"), col("hour")])
        .agg([
            col("return_bps").mean().alias("avg_return_bps"),
            col("return_bps")
                .count()
                .cast(DataType::Int64)
                .alias("trade_count"),
        ])
        .sort(["day_of_week", "hour"], Default::default())
        .collect()?;

    let day_col = out.column("day_of_week")?.i32()?;
    let hour_col = out.column("hour")?.i32()?;
    let avg_col = out.column("avg_return_bps")?.f64()?;
    let count_col = out.column("trade_count")?.i64()?;

    let rows = day_col
        .into_iter()
        .zip(hour_col.into_iter())
        .zip(avg_col.into_iter())
        .zip(count_col.into_iter())
        .map(
            |(((day_of_week, hour), avg_return_bps), trade_count)| TimePerformanceRow {
                day_of_week: day_of_week.unwrap_or_default().max(0) as u32,
                hour: hour.unwrap_or_default().max(0) as u32,
                avg_return_bps: avg_return_bps.unwrap_or(0.0),
                trade_count: trade_count.unwrap_or_default().max(0) as usize,
            },
        )
        .collect();

    Ok(rows)
}

fn build_time_performance(trips: &[RoundTripTrade]) -> Vec<TimePerformanceRow> {
    build_time_performance_polars(trips).unwrap_or_else(|_| build_time_performance_iter(trips))
}

fn build_account_drawdown_series_iter(
    account_history: &[(DateTime<Utc>, Account)],
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
            strategy_name: "Portfolio Equity".to_string(),
            period,
            drawdown_pct,
            series_type: "portfolio_equity".to_string(),
            basis: "account_equity".to_string(),
            cumulative_pnl: None,
            cumulative_return_pct: None,
            drawdown_pnl: None,
        })
        .collect()
}

fn build_account_drawdown_series_polars(
    account_history: &[(DateTime<Utc>, Account)],
) -> PolarsResult<Vec<DrawdownSeriesRow>> {
    if account_history.is_empty() {
        return Ok(Vec::new());
    }

    let df = account_history_frame(account_history)?;
    let out = df
        .lazy()
        .select([col("timestamp_ms"), col("period"), col("equity")])
        .sort(["timestamp_ms"], Default::default())
        .with_column(col("equity").cum_max(false).alias("peak_equity"))
        .with_column(
            when(col("peak_equity").abs().gt(lit(f64::EPSILON)))
                .then(((col("equity") - col("peak_equity")) / col("peak_equity")) * lit(100.0))
                .otherwise(lit(0.0))
                .alias("drawdown_pct"),
        )
        .group_by([col("period")])
        .agg([col("drawdown_pct").min().alias("drawdown_pct")])
        .sort(["period"], Default::default())
        .collect()?;

    let period_col = out.column("period")?.str()?;
    let drawdown_col = out.column("drawdown_pct")?.f64()?;

    let rows = period_col
        .into_iter()
        .zip(drawdown_col.into_iter())
        .map(|(period, drawdown_pct)| DrawdownSeriesRow {
            strategy_name: "Portfolio Equity".to_string(),
            period: period.unwrap_or_default().to_string(),
            drawdown_pct: drawdown_pct.unwrap_or(0.0),
            series_type: "portfolio_equity".to_string(),
            basis: "account_equity".to_string(),
            cumulative_pnl: None,
            cumulative_return_pct: None,
            drawdown_pnl: None,
        })
        .collect();

    Ok(rows)
}

fn build_account_drawdown_series(
    account_history: &[(DateTime<Utc>, Account)],
) -> Vec<DrawdownSeriesRow> {
    build_account_drawdown_series_polars(account_history)
        .unwrap_or_else(|_| build_account_drawdown_series_iter(account_history))
}

fn build_strategy_drawdown_series(trips: &[RoundTripTrade]) -> Vec<DrawdownSeriesRow> {
    build_strategy_drawdown_series_polars(trips)
        .unwrap_or_else(|_| build_strategy_drawdown_series_iter(trips))
}

fn build_strategy_drawdown_series_iter(trips: &[RoundTripTrade]) -> Vec<DrawdownSeriesRow> {
    let mut by_strategy: BTreeMap<String, Vec<&RoundTripTrade>> = BTreeMap::new();
    for trip in trips {
        let strategy = strategy_label(trip.strategy_type.as_deref(), "Unknown");
        by_strategy.entry(strategy).or_default().push(trip);
    }

    let mut rows = Vec::new();
    for (strategy, mut strategy_trips) in by_strategy {
        strategy_trips.sort_by(|a, b| a.exit_time.cmp(&b.exit_time));
        let mut cumulative_return_pct = 0.0_f64;
        let mut cumulative_pnl = 0.0_f64;
        let mut peak_return_pct = 0.0_f64;
        let mut peak_pnl = 0.0_f64;
        let mut by_period: BTreeMap<String, DrawdownSeriesRow> = BTreeMap::new();

        for trip in strategy_trips {
            cumulative_return_pct += trip.return_pct;
            cumulative_pnl += trip.pnl;
            peak_return_pct = peak_return_pct.max(cumulative_return_pct);
            peak_pnl = peak_pnl.max(cumulative_pnl);
            let drawdown_pct = cumulative_return_pct - peak_return_pct;
            let drawdown_pnl = cumulative_pnl - peak_pnl;
            let period = month_period(trip.exit_time);
            let row = DrawdownSeriesRow {
                strategy_name: strategy.clone(),
                period: period.clone(),
                drawdown_pct,
                series_type: "strategy_realized".to_string(),
                basis: "cumulative_realized_return_pct".to_string(),
                cumulative_pnl: Some(cumulative_pnl),
                cumulative_return_pct: Some(cumulative_return_pct),
                drawdown_pnl: Some(drawdown_pnl),
            };
            by_period
                .entry(period)
                .and_modify(|current| {
                    if row.drawdown_pct < current.drawdown_pct {
                        *current = row.clone();
                    }
                })
                .or_insert(row);
        }

        rows.extend(by_period.into_values());
    }

    rows
}

fn build_strategy_drawdown_series_polars(
    trips: &[RoundTripTrade],
) -> PolarsResult<Vec<DrawdownSeriesRow>> {
    if trips.is_empty() {
        return Ok(Vec::new());
    }

    let df = round_trips_frame(trips)?;
    let out = df
        .lazy()
        .select([
            col("strategy_type").alias("strategy_name"),
            col("period"),
            col("exit_ms"),
            col("pnl"),
            col("return_pct"),
        ])
        .sort(
            ["strategy_name", "exit_ms"],
            SortMultipleOptions::new().with_maintain_order(true),
        )
        .with_columns([
            col("return_pct")
                .cum_sum(false)
                .over([col("strategy_name")])
                .alias("cumulative_return_pct"),
            col("pnl")
                .cum_sum(false)
                .over([col("strategy_name")])
                .alias("cumulative_pnl"),
        ])
        .with_columns([
            col("cumulative_return_pct")
                .cum_max(false)
                .over([col("strategy_name")])
                .alias("peak_return_pct"),
            col("cumulative_pnl")
                .cum_max(false)
                .over([col("strategy_name")])
                .alias("peak_pnl"),
        ])
        .with_columns([
            (col("cumulative_return_pct") - col("peak_return_pct")).alias("drawdown_pct"),
            (col("cumulative_pnl") - col("peak_pnl")).alias("drawdown_pnl"),
        ])
        .sort(
            ["strategy_name", "period", "drawdown_pct"],
            SortMultipleOptions::new().with_maintain_order(true),
        )
        .group_by([col("strategy_name"), col("period")])
        .agg([
            col("drawdown_pct").first().alias("drawdown_pct"),
            col("cumulative_pnl").first().alias("cumulative_pnl"),
            col("cumulative_return_pct")
                .first()
                .alias("cumulative_return_pct"),
            col("drawdown_pnl").first().alias("drawdown_pnl"),
        ])
        .sort(["strategy_name", "period"], Default::default())
        .collect()?;

    let strategy_col = out.column("strategy_name")?.str()?;
    let period_col = out.column("period")?.str()?;
    let drawdown_col = out.column("drawdown_pct")?.f64()?;
    let cumulative_pnl_col = out.column("cumulative_pnl")?.f64()?;
    let cumulative_return_col = out.column("cumulative_return_pct")?.f64()?;
    let drawdown_pnl_col = out.column("drawdown_pnl")?.f64()?;

    let rows = strategy_col
        .into_iter()
        .zip(period_col.into_iter())
        .zip(drawdown_col.into_iter())
        .zip(cumulative_pnl_col.into_iter())
        .zip(cumulative_return_col.into_iter())
        .zip(drawdown_pnl_col.into_iter())
        .map(
            |(
                ((((strategy_name, period), drawdown_pct), cumulative_pnl), cumulative_return_pct),
                drawdown_pnl,
            )| {
                DrawdownSeriesRow {
                    strategy_name: strategy_name.unwrap_or_default().to_string(),
                    period: period.unwrap_or_default().to_string(),
                    drawdown_pct: drawdown_pct.unwrap_or(0.0),
                    series_type: "strategy_realized".to_string(),
                    basis: "cumulative_realized_return_pct".to_string(),
                    cumulative_pnl,
                    cumulative_return_pct,
                    drawdown_pnl,
                }
            },
        )
        .collect();

    Ok(rows)
}

fn build_drawdown_series(
    account_history: &[(DateTime<Utc>, Account)],
    trips: &[RoundTripTrade],
) -> Vec<DrawdownSeriesRow> {
    let mut rows = build_account_drawdown_series(account_history);
    rows.extend(build_strategy_drawdown_series(trips));
    rows
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

fn sharpe_from_returns<I>(returns: I) -> f64
where
    I: IntoIterator<Item = f64>,
{
    let mut count = 0usize;
    let mut mean = 0.0;
    let mut m2 = 0.0;

    for value in returns {
        count += 1;
        let delta = value - mean;
        mean += delta / count as f64;
        m2 += delta * (value - mean);
    }

    if count < 2 {
        return 0.0;
    }
    let variance = m2 / count as f64;
    let std_dev = variance.sqrt();
    if std_dev <= f64::EPSILON {
        return 0.0;
    }
    (mean / std_dev) * (252.0_f64).sqrt()
}

fn build_rolling_sharpe_iter(
    account_history: &[(DateTime<Utc>, Account)],
) -> Vec<RollingSharpeRow> {
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
        for window in returns.windows(window_len) {
            let Some((timestamp, _)) = window.last() else {
                continue;
            };
            let period = rolling_period(*timestamp, duration_days);
            latest_by_period.insert(
                period,
                sharpe_from_returns(window.iter().map(|(_, value)| *value)),
            );
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

fn build_rolling_sharpe_polars(
    account_history: &[(DateTime<Utc>, Account)],
) -> PolarsResult<Vec<RollingSharpeRow>> {
    if account_history.len() < 3 {
        return Ok(Vec::new());
    }

    let Some((first_timestamp, _)) = account_history.first() else {
        return Ok(Vec::new());
    };
    let Some((last_timestamp, _)) = account_history.last() else {
        return Ok(Vec::new());
    };
    let duration_secs = (*last_timestamp - *first_timestamp).num_seconds().max(1) as f64;
    let duration_days = duration_secs / 86_400.0;

    let mut timestamp_ms = Vec::with_capacity(account_history.len());
    let mut periods = Vec::with_capacity(account_history.len());
    let mut equities = Vec::with_capacity(account_history.len());
    for (timestamp, account) in account_history {
        timestamp_ms.push(timestamp.timestamp_millis());
        periods.push(rolling_period(*timestamp, duration_days));
        equities.push(account.equity);
    }

    let df = DataFrame::new(vec![
        Column::new("timestamp_ms".into(), timestamp_ms.as_slice()),
        Column::new("period".into(), periods.as_slice()),
        Column::new("equity".into(), equities.as_slice()),
    ])?;

    let returns_len = account_history.len().saturating_sub(1);
    if returns_len < 2 {
        return Ok(Vec::new());
    }
    let samples_per_day = (returns_len as f64 / duration_days.max(1.0 / 24.0)).max(1.0);
    let max_window_days = duration_days.ceil().max(1.0) as i32;
    let mut windows: Vec<i32> = [1_i32, 2, 3, 7, 14, 30, 60, 90, 120, 180]
        .into_iter()
        .filter(|window| *window <= max_window_days)
        .collect();
    if windows.is_empty() {
        windows.push(1);
    }

    let mut rolling_exprs = Vec::with_capacity(windows.len() * 2);
    let mut sharpe_exprs = Vec::with_capacity(windows.len());
    for window_days in &windows {
        let window_len = ((*window_days as f64) * samples_per_day).round() as usize;
        let window_len = window_len.clamp(2, returns_len);
        let options = RollingOptionsFixedWindow {
            window_size: window_len,
            min_periods: window_len,
            ..Default::default()
        };
        let mean_name = format!("rolling_mean_{}d", window_days);
        let std_name = format!("rolling_std_{}d", window_days);
        let sharpe_name = format!("sharpe_{}d", window_days);
        rolling_exprs.push(
            col("return_value")
                .rolling_mean(options.clone())
                .alias(&mean_name),
        );
        rolling_exprs.push(col("return_value").rolling_std(options).alias(&std_name));
        sharpe_exprs.push(
            when(col(&std_name).abs().gt(lit(f64::EPSILON)))
                .then((col(&mean_name) / col(&std_name)) * lit((252.0_f64).sqrt()))
                .otherwise(lit(0.0))
                .alias(&sharpe_name),
        );
    }

    let mut select_exprs = Vec::with_capacity(windows.len() + 1);
    select_exprs.push(col("period"));
    select_exprs.extend(
        windows
            .iter()
            .map(|window_days| col(format!("sharpe_{}d", window_days))),
    );

    let out = df
        .lazy()
        .sort(
            ["timestamp_ms"],
            SortMultipleOptions::new().with_maintain_order(true),
        )
        .with_column(col("equity").shift(lit(1_i64)).alias("previous_equity"))
        .filter(
            col("previous_equity")
                .is_not_null()
                .and(col("previous_equity").abs().gt(lit(f64::EPSILON))),
        )
        .with_column(
            ((col("equity") - col("previous_equity")) / col("previous_equity"))
                .alias("return_value"),
        )
        .with_columns(rolling_exprs)
        .with_columns(sharpe_exprs)
        .select(select_exprs)
        .collect()?;

    let period_col = out.column("period")?.str()?;
    let mut rows = Vec::new();
    for window_days in windows {
        let sharpe_col = out.column(&format!("sharpe_{}d", window_days))?.f64()?;
        let latest_by_period: BTreeMap<String, f64> = period_col
            .into_iter()
            .zip(sharpe_col.into_iter())
            .filter_map(|(period, sharpe)| Some((period?.to_string(), sharpe?)))
            .collect();
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

    Ok(rows)
}

fn build_rolling_sharpe(account_history: &[(DateTime<Utc>, Account)]) -> Vec<RollingSharpeRow> {
    build_rolling_sharpe_polars(account_history)
        .unwrap_or_else(|_| build_rolling_sharpe_iter(account_history))
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

#[derive(Clone, Debug)]
struct RegimePoint {
    timestamp: DateTime<Utc>,
    vol_regime: String,
    trend_regime: String,
    market_return_pct: f64,
}

fn volatility_regime(vol: f64, q1: f64, q2: f64, q3: f64) -> &'static str {
    if vol <= q1 {
        "Low Vol"
    } else if vol <= q2 {
        "Med Vol"
    } else if vol <= q3 {
        "High Vol"
    } else {
        "Crisis"
    }
}

fn trend_regime(slope: f64) -> &'static str {
    if slope <= -0.02 {
        "Strong Down"
    } else if slope <= -0.005 {
        "Mild Down"
    } else if slope < 0.005 {
        "Flat"
    } else if slope < 0.02 {
        "Mild Up"
    } else {
        "Strong Up"
    }
}

fn build_regime_points(bars: &[Bar]) -> Vec<RegimePoint> {
    if bars.len() < 51 {
        return Vec::new();
    }

    let mut bar_returns = Vec::with_capacity(bars.len().saturating_sub(1));
    for window in bars.windows(2) {
        if window[0].close.abs() > f64::EPSILON {
            bar_returns.push((window[1].close - window[0].close) / window[0].close);
        } else {
            bar_returns.push(0.0);
        }
    }

    let mut vol_by_index: HashMap<usize, f64> = HashMap::new();
    let mut vol_values = Vec::new();
    for index in 20..bar_returns.len() {
        let vol = std_dev(&bar_returns[index - 20..index]);
        vol_by_index.insert(index + 1, vol);
        vol_values.push(vol);
    }
    if vol_values.is_empty() {
        return Vec::new();
    }
    vol_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let q1 = percentile(&vol_values, 0.25);
    let q2 = percentile(&vol_values, 0.50);
    let q3 = percentile(&vol_values, 0.75);

    let mut points = Vec::new();
    for (start_index, window) in bars.windows(51).enumerate() {
        let index = start_index + 50;
        let Some(vol) = vol_by_index.get(&index).copied() else {
            continue;
        };
        let current = &window[50];
        let previous = &window[49];
        let sma50 = window[..50].iter().map(|bar| bar.close).sum::<f64>() / 50.0;
        if sma50.abs() <= f64::EPSILON {
            continue;
        }
        let slope = (current.close - sma50) / sma50;
        let market_return_pct = if previous.close.abs() > f64::EPSILON {
            (current.close - previous.close) / previous.close * 100.0
        } else {
            0.0
        };
        points.push(RegimePoint {
            timestamp: current.timestamp,
            vol_regime: volatility_regime(vol, q1, q2, q3).to_string(),
            trend_regime: trend_regime(slope).to_string(),
            market_return_pct,
        });
    }

    points
}

fn build_regime_performance_iter(
    account_history: &[(DateTime<Utc>, Account)],
    regime_points: &[RegimePoint],
) -> Vec<RegimePerformanceRow> {
    if regime_points.is_empty() {
        return Vec::new();
    }

    let equity_return_by_time: HashMap<DateTime<Utc>, f64> =
        equity_returns(account_history).into_iter().collect();
    let mut buckets: BTreeMap<(String, String), (f64, usize)> = BTreeMap::new();

    for point in regime_points {
        let strategy_return = equity_return_by_time
            .get(&point.timestamp)
            .map(|value| *value * 100.0)
            .unwrap_or(point.market_return_pct);
        buckets
            .entry((point.vol_regime.clone(), point.trend_regime.clone()))
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

fn build_regime_performance_polars(
    account_history: &[(DateTime<Utc>, Account)],
    regime_points: &[RegimePoint],
) -> PolarsResult<Vec<RegimePerformanceRow>> {
    if regime_points.is_empty() {
        return Ok(Vec::new());
    }

    let equity_return_by_time: HashMap<DateTime<Utc>, f64> =
        equity_returns(account_history).into_iter().collect();
    let mut vol_regimes = Vec::with_capacity(regime_points.len());
    let mut trend_regimes = Vec::with_capacity(regime_points.len());
    let mut returns = Vec::with_capacity(regime_points.len());

    for point in regime_points {
        vol_regimes.push(point.vol_regime.clone());
        trend_regimes.push(point.trend_regime.clone());
        returns.push(
            equity_return_by_time
                .get(&point.timestamp)
                .map(|value| *value * 100.0)
                .unwrap_or(point.market_return_pct),
        );
    }

    let df = DataFrame::new(vec![
        Column::new("vol_regime".into(), vol_regimes.as_slice()),
        Column::new("trend_regime".into(), trend_regimes.as_slice()),
        Column::new("strategy_return".into(), returns.as_slice()),
    ])?;
    let out = df
        .lazy()
        .group_by([col("vol_regime"), col("trend_regime")])
        .agg([
            col("strategy_return").mean().alias("avg_return_pct"),
            col("strategy_return")
                .count()
                .cast(DataType::Int64)
                .alias("bar_count"),
        ])
        .sort(["vol_regime", "trend_regime"], Default::default())
        .collect()?;

    let vol_col = out.column("vol_regime")?.str()?;
    let trend_col = out.column("trend_regime")?.str()?;
    let avg_col = out.column("avg_return_pct")?.f64()?;
    let count_col = out.column("bar_count")?.i64()?;

    let rows = vol_col
        .into_iter()
        .zip(trend_col.into_iter())
        .zip(avg_col.into_iter())
        .zip(count_col.into_iter())
        .map(
            |(((vol_regime, trend_regime), avg_return_pct), bar_count)| RegimePerformanceRow {
                vol_regime: vol_regime.unwrap_or_default().to_string(),
                trend_regime: trend_regime.unwrap_or_default().to_string(),
                avg_return_pct: avg_return_pct.unwrap_or(0.0),
                bar_count: bar_count.unwrap_or_default().max(0) as usize,
            },
        )
        .collect();

    Ok(rows)
}

fn build_regime_performance(
    account_history: &[(DateTime<Utc>, Account)],
    bars_by_symbol: &HashMap<String, Vec<Bar>>,
) -> Vec<RegimePerformanceRow> {
    let Some((_, bars)) = bars_by_symbol.iter().next() else {
        return Vec::new();
    };
    let regime_points = build_regime_points(bars);
    build_regime_performance_polars(account_history, &regime_points)
        .unwrap_or_else(|_| build_regime_performance_iter(account_history, &regime_points))
}

fn regime_at_entry<'a>(
    regime_points: &'a [RegimePoint],
    entry_time: DateTime<Utc>,
) -> Option<&'a RegimePoint> {
    let insertion_index = regime_points.partition_point(|point| point.timestamp <= entry_time);
    if insertion_index == 0 {
        regime_points.first()
    } else {
        regime_points.get(insertion_index - 1)
    }
}

fn build_strategy_regime_performance_iter(
    trips: &[RoundTripTrade],
    regime_points_by_symbol: &HashMap<String, Vec<RegimePoint>>,
) -> Vec<StrategyRegimePerformanceRow> {
    if regime_points_by_symbol.is_empty() {
        return Vec::new();
    }

    let mut buckets: BTreeMap<(String, String, String), Vec<&RoundTripTrade>> = BTreeMap::new();
    for trip in trips {
        let Some(points) = regime_points_by_symbol.get(&trip.symbol) else {
            continue;
        };
        let Some(point) = regime_at_entry(points, trip.entry_time) else {
            continue;
        };
        let strategy = strategy_label(trip.strategy_type.as_deref(), "Unknown");
        buckets
            .entry((
                strategy,
                point.vol_regime.clone(),
                point.trend_regime.clone(),
            ))
            .or_default()
            .push(trip);
    }

    buckets
        .into_iter()
        .map(|((strategy_type, vol_regime, trend_regime), trades)| {
            let trade_count = trades.len();
            let winners = trades.iter().filter(|trip| trip.pnl > 0.0).count();
            let gross_profit: f64 = trades
                .iter()
                .filter(|trip| trip.pnl > 0.0)
                .map(|trip| trip.pnl)
                .sum();
            let gross_loss: f64 = trades
                .iter()
                .filter(|trip| trip.pnl < 0.0)
                .map(|trip| trip.pnl)
                .sum::<f64>()
                .abs();
            StrategyRegimePerformanceRow {
                strategy_type,
                vol_regime,
                trend_regime,
                trade_count,
                win_rate: if trade_count > 0 {
                    winners as f64 / trade_count as f64 * 100.0
                } else {
                    0.0
                },
                total_pnl: trades.iter().map(|trip| trip.pnl).sum(),
                avg_return_pct: if trade_count > 0 {
                    trades.iter().map(|trip| trip.return_pct).sum::<f64>() / trade_count as f64
                } else {
                    0.0
                },
                profit_factor: if gross_loss > f64::EPSILON {
                    gross_profit / gross_loss
                } else {
                    0.0
                },
            }
        })
        .collect()
}

fn build_strategy_regime_performance_polars(
    trips: &[RoundTripTrade],
    regime_points_by_symbol: &HashMap<String, Vec<RegimePoint>>,
) -> PolarsResult<Vec<StrategyRegimePerformanceRow>> {
    if trips.is_empty() || regime_points_by_symbol.is_empty() {
        return Ok(Vec::new());
    }

    let mut strategy_types = Vec::new();
    let mut vol_regimes = Vec::new();
    let mut trend_regimes = Vec::new();
    let mut pnl_values = Vec::new();
    let mut return_values = Vec::new();
    let mut is_win_values = Vec::new();
    let mut gross_profit_values = Vec::new();
    let mut gross_loss_values = Vec::new();

    for trip in trips {
        let Some(points) = regime_points_by_symbol.get(&trip.symbol) else {
            continue;
        };
        let Some(point) = regime_at_entry(points, trip.entry_time) else {
            continue;
        };
        strategy_types.push(strategy_label(trip.strategy_type.as_deref(), "Unknown"));
        vol_regimes.push(point.vol_regime.clone());
        trend_regimes.push(point.trend_regime.clone());
        pnl_values.push(trip.pnl);
        return_values.push(trip.return_pct);
        is_win_values.push(if trip.pnl > 0.0 { 1_i32 } else { 0_i32 });
        gross_profit_values.push(if trip.pnl > 0.0 { trip.pnl } else { 0.0 });
        gross_loss_values.push(if trip.pnl < 0.0 { trip.pnl.abs() } else { 0.0 });
    }

    if strategy_types.is_empty() {
        return Ok(Vec::new());
    }

    let df = DataFrame::new(vec![
        Column::new("strategy_type".into(), strategy_types.as_slice()),
        Column::new("vol_regime".into(), vol_regimes.as_slice()),
        Column::new("trend_regime".into(), trend_regimes.as_slice()),
        Column::new("pnl".into(), pnl_values.as_slice()),
        Column::new("return_pct".into(), return_values.as_slice()),
        Column::new("is_win".into(), is_win_values.as_slice()),
        Column::new("gross_profit".into(), gross_profit_values.as_slice()),
        Column::new("gross_loss".into(), gross_loss_values.as_slice()),
    ])?;
    let out = df
        .lazy()
        .group_by([col("strategy_type"), col("vol_regime"), col("trend_regime")])
        .agg([
            col("pnl").sum().alias("total_pnl"),
            col("return_pct").mean().alias("avg_return_pct"),
            col("pnl")
                .count()
                .cast(DataType::Int64)
                .alias("trade_count"),
            col("is_win")
                .sum()
                .cast(DataType::Int64)
                .alias("winning_trades"),
            col("gross_profit").sum().alias("gross_profit"),
            col("gross_loss").sum().alias("gross_loss"),
        ])
        .sort(
            ["strategy_type", "vol_regime", "trend_regime"],
            Default::default(),
        )
        .collect()?;

    let strategy_col = out.column("strategy_type")?.str()?;
    let vol_col = out.column("vol_regime")?.str()?;
    let trend_col = out.column("trend_regime")?.str()?;
    let total_pnl_col = out.column("total_pnl")?.f64()?;
    let avg_return_col = out.column("avg_return_pct")?.f64()?;
    let trade_count_col = out.column("trade_count")?.i64()?;
    let winning_col = out.column("winning_trades")?.i64()?;
    let gross_profit_col = out.column("gross_profit")?.f64()?;
    let gross_loss_col = out.column("gross_loss")?.f64()?;

    let rows = strategy_col
        .into_iter()
        .zip(vol_col.into_iter())
        .zip(trend_col.into_iter())
        .zip(total_pnl_col.into_iter())
        .zip(avg_return_col.into_iter())
        .zip(trade_count_col.into_iter())
        .zip(winning_col.into_iter())
        .zip(gross_profit_col.into_iter())
        .zip(gross_loss_col.into_iter())
        .map(
            |(
                (
                    (
                        (
                            ((((strategy, vol_regime), trend_regime), total_pnl), avg_return),
                            trade_count,
                        ),
                        winners,
                    ),
                    gross_profit,
                ),
                gross_loss,
            )| {
                let trade_count = trade_count.unwrap_or_default().max(0) as usize;
                let winners = winners.unwrap_or_default().max(0) as usize;
                let gross_profit = gross_profit.unwrap_or(0.0);
                let gross_loss = gross_loss.unwrap_or(0.0);
                StrategyRegimePerformanceRow {
                    strategy_type: strategy.unwrap_or_default().to_string(),
                    vol_regime: vol_regime.unwrap_or_default().to_string(),
                    trend_regime: trend_regime.unwrap_or_default().to_string(),
                    trade_count,
                    win_rate: if trade_count > 0 {
                        winners as f64 / trade_count as f64 * 100.0
                    } else {
                        0.0
                    },
                    total_pnl: total_pnl.unwrap_or(0.0),
                    avg_return_pct: avg_return.unwrap_or(0.0),
                    profit_factor: if gross_loss > f64::EPSILON {
                        gross_profit / gross_loss
                    } else {
                        0.0
                    },
                }
            },
        )
        .collect();

    Ok(rows)
}

fn build_strategy_regime_performance(
    trips: &[RoundTripTrade],
    bars_by_symbol: &HashMap<String, Vec<Bar>>,
) -> Vec<StrategyRegimePerformanceRow> {
    let regime_points_by_symbol: HashMap<String, Vec<RegimePoint>> = bars_by_symbol
        .iter()
        .map(|(symbol, bars)| (symbol.clone(), build_regime_points(bars)))
        .filter(|(_, points)| !points.is_empty())
        .collect();
    build_strategy_regime_performance_polars(trips, &regime_points_by_symbol)
        .unwrap_or_else(|_| build_strategy_regime_performance_iter(trips, &regime_points_by_symbol))
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

fn build_setup_performance_iter(trips: &[RoundTripTrade]) -> Vec<SetupPerformanceRow> {
    let mut buckets: BTreeMap<String, Vec<&RoundTripTrade>> = BTreeMap::new();
    for trip in trips {
        let strategy = strategy_label(trip.strategy_type.as_deref(), "All Trades");
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

fn build_setup_performance_polars(
    trips: &[RoundTripTrade],
) -> PolarsResult<Vec<SetupPerformanceRow>> {
    if trips.is_empty() {
        return Ok(Vec::new());
    }

    let mut setup_names = Vec::with_capacity(trips.len() * 3);
    let mut pnl_values = Vec::with_capacity(trips.len() * 3);
    let mut is_win_values = Vec::with_capacity(trips.len() * 3);
    let mut is_loss_values = Vec::with_capacity(trips.len() * 3);
    let mut gross_profit_values = Vec::with_capacity(trips.len() * 3);
    let mut gross_loss_values = Vec::with_capacity(trips.len() * 3);

    for trip in trips {
        let strategy = strategy_label(trip.strategy_type.as_deref(), "All Trades");
        let side = format!("{:?}", trip.side);
        let labels = [
            strategy.clone(),
            format!("{strategy} / {side}"),
            format!("{strategy} / {}", trip.symbol),
        ];
        for label in labels {
            setup_names.push(label);
            pnl_values.push(trip.pnl);
            is_win_values.push(if trip.pnl > 0.0 { 1_i32 } else { 0_i32 });
            is_loss_values.push(if trip.pnl < 0.0 { 1_i32 } else { 0_i32 });
            gross_profit_values.push(if trip.pnl > 0.0 { trip.pnl } else { 0.0 });
            gross_loss_values.push(if trip.pnl < 0.0 { trip.pnl.abs() } else { 0.0 });
        }
    }

    let df = DataFrame::new(vec![
        Column::new("setup_name".into(), setup_names.as_slice()),
        Column::new("pnl".into(), pnl_values.as_slice()),
        Column::new("is_win".into(), is_win_values.as_slice()),
        Column::new("is_loss".into(), is_loss_values.as_slice()),
        Column::new("gross_profit".into(), gross_profit_values.as_slice()),
        Column::new("gross_loss".into(), gross_loss_values.as_slice()),
    ])?;
    let out = df
        .lazy()
        .group_by([col("setup_name")])
        .agg([
            col("pnl").sum().alias("total_pnl"),
            col("pnl")
                .count()
                .cast(DataType::Int64)
                .alias("trade_count"),
            col("is_win")
                .sum()
                .cast(DataType::Int64)
                .alias("winning_trades"),
            col("is_loss")
                .sum()
                .cast(DataType::Int64)
                .alias("losing_trades"),
            col("gross_profit").sum().alias("gross_profit"),
            col("gross_loss").sum().alias("gross_loss"),
        ])
        .sort(["setup_name"], Default::default())
        .collect()?;

    let setup_col = out.column("setup_name")?.str()?;
    let total_pnl_col = out.column("total_pnl")?.f64()?;
    let trade_count_col = out.column("trade_count")?.i64()?;
    let winning_col = out.column("winning_trades")?.i64()?;
    let losing_col = out.column("losing_trades")?.i64()?;
    let gross_profit_col = out.column("gross_profit")?.f64()?;
    let gross_loss_col = out.column("gross_loss")?.f64()?;

    let rows = setup_col
        .into_iter()
        .zip(total_pnl_col.into_iter())
        .zip(trade_count_col.into_iter())
        .zip(winning_col.into_iter())
        .zip(losing_col.into_iter())
        .zip(gross_profit_col.into_iter())
        .zip(gross_loss_col.into_iter())
        .map(
            |(
                (((((setup_name, total_pnl), trade_count), winners), losers), gross_profit),
                gross_loss,
            )| {
                let trade_count = trade_count.unwrap_or_default().max(0) as usize;
                let winners = winners.unwrap_or_default().max(0) as usize;
                let losers = losers.unwrap_or_default().max(0) as usize;
                let gross_profit = gross_profit.unwrap_or(0.0);
                let gross_loss = gross_loss.unwrap_or(0.0);
                let avg_win = if winners > 0 {
                    gross_profit / winners as f64
                } else {
                    0.0
                };
                let avg_loss = if losers > 0 {
                    gross_loss / losers as f64
                } else {
                    0.0
                };

                SetupPerformanceRow {
                    setup_name: setup_name.unwrap_or_default().to_string(),
                    win_rate: if trade_count > 0 {
                        winners as f64 / trade_count as f64 * 100.0
                    } else {
                        0.0
                    },
                    payoff_ratio: if avg_loss > f64::EPSILON {
                        avg_win / avg_loss
                    } else {
                        0.0
                    },
                    trade_count,
                    total_pnl: total_pnl.unwrap_or(0.0),
                }
            },
        )
        .collect();

    Ok(rows)
}

fn build_setup_performance(trips: &[RoundTripTrade]) -> Vec<SetupPerformanceRow> {
    build_setup_performance_polars(trips).unwrap_or_else(|_| build_setup_performance_iter(trips))
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

fn pearson_pairs<I>(pairs: I) -> (f64, usize)
where
    I: IntoIterator<Item = (f64, f64)>,
{
    let mut count = 0usize;
    let mut mean_a = 0.0;
    let mut mean_b = 0.0;
    let mut covariance = 0.0;
    let mut var_a = 0.0;
    let mut var_b = 0.0;

    for (a, b) in pairs {
        count += 1;
        let delta_a = a - mean_a;
        let delta_b = b - mean_b;
        mean_a += delta_a / count as f64;
        mean_b += delta_b / count as f64;
        covariance += delta_a * (b - mean_b);
        var_a += delta_a * (a - mean_a);
        var_b += delta_b * (b - mean_b);
    }

    if count < 2 {
        return (0.0, count);
    }

    let denominator = (var_a * var_b).sqrt();
    if denominator <= f64::EPSILON {
        (0.0, count)
    } else {
        (covariance / denominator, count)
    }
}

fn build_strategy_correlations_iter(trips: &[RoundTripTrade]) -> Vec<StrategyCorrelationRow> {
    let mut by_strategy: BTreeMap<String, BTreeMap<String, f64>> = BTreeMap::new();
    for trip in trips {
        let strategy = strategy_label(trip.strategy_type.as_deref(), "Strategy");
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

    let dates = by_strategy
        .values()
        .flat_map(|items| items.keys().cloned())
        .collect::<std::collections::BTreeSet<_>>();
    let mut rows = Vec::new();
    for (strategy_a, series_a) in &by_strategy {
        for (strategy_b, series_b) in &by_strategy {
            let (correlation, sample_count) = pearson_pairs(
                dates
                    .iter()
                    .filter_map(|date| Some((*series_a.get(date)?, *series_b.get(date)?))),
            );
            rows.push(StrategyCorrelationRow {
                strategy_a: strategy_a.clone(),
                strategy_b: strategy_b.clone(),
                correlation: if strategy_a == strategy_b {
                    1.0
                } else if sample_count < 2 {
                    0.0
                } else {
                    correlation
                },
                sample_count,
            });
        }
    }
    rows
}

fn build_strategy_correlations_polars(
    trips: &[RoundTripTrade],
) -> PolarsResult<Vec<StrategyCorrelationRow>> {
    if trips.is_empty() {
        return Ok(Vec::new());
    }

    let daily_returns = round_trips_frame(trips)?
        .lazy()
        .select([col("strategy_type"), col("exit_date"), col("return_pct")])
        .group_by([col("strategy_type"), col("exit_date")])
        .agg([col("return_pct").sum().alias("return_pct")])
        .collect()?;

    let strategy_col = daily_returns.column("strategy_type")?.str()?;
    let strategies = strategy_col
        .into_iter()
        .flatten()
        .map(str::to_string)
        .collect::<std::collections::BTreeSet<_>>();

    if strategies.len() < 2 {
        return Ok(Vec::new());
    }

    let mut rows = Vec::new();
    for strategy_a in &strategies {
        for strategy_b in &strategies {
            let left = daily_returns
                .clone()
                .lazy()
                .filter(col("strategy_type").eq(lit(strategy_a.as_str())))
                .select([col("exit_date"), col("return_pct").alias("return_a")]);
            let right = daily_returns
                .clone()
                .lazy()
                .filter(col("strategy_type").eq(lit(strategy_b.as_str())))
                .select([col("exit_date"), col("return_pct").alias("return_b")]);

            let out = left
                .join(
                    right,
                    [col("exit_date")],
                    [col("exit_date")],
                    JoinArgs::new(JoinType::Inner),
                )
                .select([
                    pearson_corr(col("return_a"), col("return_b")).alias("correlation"),
                    col("return_a")
                        .count()
                        .cast(DataType::Int64)
                        .alias("sample_count"),
                ])
                .collect()?;

            let sample_count = out
                .column("sample_count")?
                .i64()?
                .get(0)
                .unwrap_or_default()
                .max(0) as usize;
            let correlation = if strategy_a == strategy_b {
                1.0
            } else if sample_count < 2 {
                0.0
            } else {
                out.column("correlation")?.f64()?.get(0).unwrap_or(0.0)
            };

            rows.push(StrategyCorrelationRow {
                strategy_a: strategy_a.clone(),
                strategy_b: strategy_b.clone(),
                correlation,
                sample_count,
            });
        }
    }

    Ok(rows)
}

fn build_strategy_correlations(trips: &[RoundTripTrade]) -> Vec<StrategyCorrelationRow> {
    build_strategy_correlations_polars(trips)
        .unwrap_or_else(|_| build_strategy_correlations_iter(trips))
}

async fn insert_analysis_tables(
    tx: &Transaction<'_>,
    results: &BacktestResults,
    state: &BacktestState,
    trips: &[RoundTripTrade],
) -> Result<(), String> {
    let mut monthly_returns_stmt = tx
        .prepare("INSERT INTO monthly_returns (year, month, return_pct) VALUES (?1, ?2, ?3)")
        .await
        .map_err(to_storage_err)?;
    for row in build_monthly_returns(&results.account_history) {
        monthly_returns_stmt
            .execute(params![row.year, row.month as i64, row.return_pct])
            .await
            .map_err(to_storage_err)?;
    }

    let mut time_performance_stmt = tx
        .prepare(
            "INSERT INTO time_performance (day_of_week, hour, avg_return_bps, trade_count) VALUES (?1, ?2, ?3, ?4)",
        )
        .await
        .map_err(to_storage_err)?;
    for row in build_time_performance(trips) {
        time_performance_stmt
            .execute(params![
                row.day_of_week as i64,
                row.hour as i64,
                row.avg_return_bps,
                row.trade_count as i64
            ])
            .await
            .map_err(to_storage_err)?;
    }

    let mut drawdown_series_stmt = tx
        .prepare(
            "INSERT INTO drawdown_series (strategy_name, period, drawdown_pct, series_type, basis, cumulative_pnl, cumulative_return_pct, drawdown_pnl) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )
        .await
        .map_err(to_storage_err)?;
    for row in build_drawdown_series(&results.account_history, trips) {
        drawdown_series_stmt
            .execute(params![
                row.strategy_name.clone(),
                row.period.clone(),
                row.drawdown_pct,
                row.series_type.clone(),
                row.basis.clone(),
                row.cumulative_pnl,
                row.cumulative_return_pct,
                row.drawdown_pnl
            ])
            .await
            .map_err(to_storage_err)?;
    }

    let mut regime_performance_stmt = tx
        .prepare(
            "INSERT INTO regime_performance (vol_regime, trend_regime, avg_return_pct, bar_count) VALUES (?1, ?2, ?3, ?4)",
        )
        .await
        .map_err(to_storage_err)?;
    for row in build_regime_performance(&results.account_history, &state.historical_bars) {
        regime_performance_stmt
            .execute(params![
                row.vol_regime.clone(),
                row.trend_regime.clone(),
                row.avg_return_pct,
                row.bar_count as i64
            ])
            .await
            .map_err(to_storage_err)?;
    }

    let mut strategy_regime_performance_stmt = tx
        .prepare(
            "INSERT INTO strategy_regime_performance (strategy_type, vol_regime, trend_regime, trade_count, win_rate, total_pnl, avg_return_pct, profit_factor) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )
        .await
        .map_err(to_storage_err)?;
    for row in build_strategy_regime_performance(trips, &state.historical_bars) {
        strategy_regime_performance_stmt
            .execute(params![
                row.strategy_type.clone(),
                row.vol_regime.clone(),
                row.trend_regime.clone(),
                row.trade_count as i64,
                row.win_rate,
                row.total_pnl,
                row.avg_return_pct,
                row.profit_factor
            ])
            .await
            .map_err(to_storage_err)?;
    }

    let mut rolling_sharpe_stmt = tx
        .prepare("INSERT INTO rolling_sharpe (window_days, period, sharpe) VALUES (?1, ?2, ?3)")
        .await
        .map_err(to_storage_err)?;
    for row in build_rolling_sharpe(&results.account_history) {
        rolling_sharpe_stmt
            .execute(params![row.window_days, row.period.clone(), row.sharpe])
            .await
            .map_err(to_storage_err)?;
    }

    let mut trade_mae_stmt = tx
        .prepare(
            "INSERT INTO trade_mae (trade_id, mae_pct, final_pnl_pct, is_winner) VALUES (?1, ?2, ?3, ?4)",
        )
        .await
        .map_err(to_storage_err)?;
    for row in build_trade_mae(trips, &state.historical_bars, &state.trade_mae_by_order_id) {
        trade_mae_stmt
            .execute(params![
                row.trade_id.clone(),
                row.mae_pct,
                row.final_pnl_pct,
                row.is_winner
            ])
            .await
            .map_err(to_storage_err)?;
    }

    let mut setup_performance_stmt = tx
        .prepare(
            "INSERT INTO setup_performance (setup_name, win_rate, payoff_ratio, trade_count, total_pnl) VALUES (?1, ?2, ?3, ?4, ?5)",
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
                row.total_pnl
            ])
            .await
            .map_err(to_storage_err)?;
    }

    let mut position_concentration_stmt = tx
        .prepare(
            "INSERT INTO position_concentration (date, sector, weight_pct) VALUES (?1, ?2, ?3)",
        )
        .await
        .map_err(to_storage_err)?;
    for row in build_position_concentration(&results.trade_log) {
        position_concentration_stmt
            .execute(params![
                row.date.clone(),
                row.sector.clone(),
                row.weight_pct
            ])
            .await
            .map_err(to_storage_err)?;
    }

    let mut strategy_correlations_stmt = tx
        .prepare(
            "INSERT INTO strategy_correlations (strategy_a, strategy_b, correlation, sample_count) VALUES (?1, ?2, ?3, ?4)",
        )
        .await
        .map_err(to_storage_err)?;
    for row in build_strategy_correlations(trips) {
        strategy_correlations_stmt
            .execute(params![
                row.strategy_a.clone(),
                row.strategy_b.clone(),
                row.correlation,
                row.sample_count as i64
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
    drop_secondary_indexes(&tx).await?;
    let insights: Vec<InsightSnapshot> = state.insight_snapshots.values().cloned().collect();
    insert_trade_log(&tx, &results.trade_log).await?;
    let round_trips = results.round_trip_trades();
    insert_round_trips(&tx, &round_trips).await?;
    insert_trade_log_rows(&tx, &results.trade_log, &insights).await?;
    insert_account_history(&tx, &results.account_history).await?;
    insert_insights(&tx, &insights).await?;
    insert_bars(&tx, state).await?;
    insert_analysis_tables(&tx, results, state, &round_trips).await?;
    create_secondary_indexes(&tx).await?;
    tx.commit().await.map_err(to_storage_err)?;
    checkpoint_database(&conn).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::broker::types::{AccountType, OrderSide, TradeRecordType};
    use crate::core::insight::snapshot::InsightLegsSnapshot;
    use chrono::TimeZone;

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

    fn test_snapshot(
        insight_id: &str,
        parent_id: Option<&str>,
        strategy_type: &str,
    ) -> InsightSnapshot {
        let timestamp = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        InsightSnapshot {
            insight_id: insight_id.to_string(),
            parent_id: parent_id.map(str::to_string),
            state: "Closed".to_string(),
            children: Vec::new(),
            order_id: Some("order-1".to_string()),
            side: "Buy".to_string(),
            symbol: "AAPL".to_string(),
            quantity: Some(1.0),
            contracts: None,
            order_type: "Market".to_string(),
            order_class: "Simple".to_string(),
            limit_price: None,
            stop_price: None,
            take_profit_levels: None,
            stop_loss_levels: None,
            trailing_stop_price: None,
            strategy_type: strategy_type.to_string(),
            confidence: 100,
            timeframe: serde_json::json!({ "amount": 1, "unit": "Day" }),
            period_unfilled: None,
            period_till_tp: None,
            execution_depends: Vec::new(),
            filled_price: Some(100.0),
            close_order_id: None,
            close_price: Some(110.0),
            broker_realized_pnl: None,
            commission: None,
            swap: None,
            partial_closes: Vec::new(),
            created_at: timestamp,
            updated_at: timestamp,
            filled_at: Some(timestamp),
            closed_at: Some(timestamp),
            legs: InsightLegsSnapshot {
                take_profit: None,
                stop_loss: None,
                trailing_stop: None,
            },
            market_changed: false,
            submitted: true,
            cancelling: false,
            closing: false,
            first_on_fill: false,
            partial_filled_quantity: None,
            state_history: Vec::new(),
        }
    }

    #[tokio::test]
    async fn brands_backtest_database_as_an_aqe_artifact() {
        let dir = std::env::temp_dir().join(format!(
            "aqe-backtest-application-id-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let connection = connect_database(&dir).await.unwrap();
        init_schema(&connection).await.unwrap();
        checkpoint_database(&connection).await.unwrap();

        let mut application_id = connection.query("PRAGMA application_id", ()).await.unwrap();
        assert_eq!(
            application_id
                .next()
                .await
                .unwrap()
                .unwrap()
                .get::<i64>(0)
                .unwrap(),
            i64::from(BACKTEST_DB_APPLICATION_ID)
        );
        let mut user_version = connection.query("PRAGMA user_version", ()).await.unwrap();
        assert_eq!(
            user_version
                .next()
                .await
                .unwrap()
                .unwrap()
                .get::<i64>(0)
                .unwrap(),
            i64::from(BACKTEST_DB_USER_VERSION)
        );
        drop(connection);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn old_trade_log_rows_default_parent_child_fields() {
        let row: BacktestTradeLogRow = serde_json::from_value(serde_json::json!({
            "id": 1,
            "symbol": "AAPL",
            "side": "BUY",
            "strategyType": "MeanReversion",
            "insightId": "insight-1",
            "entryTime": "2026-01-01 00:00:00",
            "exitTime": null,
            "qty": 1.0,
            "entryPrice": 100.0,
            "exitPrice": null,
            "returnPct": null,
            "pnl": null,
            "status": "OPEN"
        }))
        .unwrap();

        assert_eq!(row.parent_id, None);
        assert!(!row.is_child);
        assert_eq!(row.base_strategy_type, None);
    }

    #[test]
    fn child_trade_log_rows_include_parent_and_base_strategy_type() {
        let entry_time = Utc.with_ymd_and_hms(2026, 1, 1, 9, 30, 0).unwrap();
        let exit_time = Utc.with_ymd_and_hms(2026, 1, 1, 10, 30, 0).unwrap();
        let trades = vec![
            TradeRecord {
                date: entry_time,
                symbol: "AAPL".to_string(),
                side: OrderSide::Buy,
                qty: 1.0,
                price: 100.0,
                order_id: "order-1".to_string(),
                insight_id: Some("child-1".to_string()),
                strategy_type: Some("MeanReversion-CHILD".to_string()),
                commission: 0.0,
                swap: 0.0,
                trade_type: TradeRecordType::Entry,
            },
            TradeRecord {
                date: exit_time,
                symbol: "AAPL".to_string(),
                side: OrderSide::Buy,
                qty: 1.0,
                price: 110.0,
                order_id: "order-1".to_string(),
                insight_id: Some("child-1".to_string()),
                strategy_type: Some("MeanReversion-CHILD".to_string()),
                commission: 0.0,
                swap: 0.0,
                trade_type: TradeRecordType::Exit,
            },
        ];
        let insights = vec![test_snapshot(
            "child-1",
            Some("parent-1"),
            "MeanReversion-CHILD",
        )];

        let rows = build_trade_log_rows(&trades, &insights);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].parent_id.as_deref(), Some("parent-1"));
        assert!(rows[0].is_child);
        assert_eq!(rows[0].base_strategy_type.as_deref(), Some("MeanReversion"));
    }

    #[tokio::test]
    async fn feature_stream_bars_persist_history_key_metadata() {
        use crate::core::broker::types::Bar;
        use crate::core::events::{EventStreamType, ResolvedEventStream};
        use crate::core::utils::timeframe::{TimeFrame, TimeFrameUnit};

        let dir = std::env::temp_dir().join(format!("aqe-backtest-bars-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();

        let mut state = BacktestState::new();
        let timestamp = Utc.with_ymd_and_hms(2026, 1, 1, 9, 30, 0).unwrap();
        let main_timeframe = TimeFrame::new(5, TimeFrameUnit::Minute);
        let feature_timeframe = TimeFrame::new(15, TimeFrameUnit::Minute);
        let stream = ResolvedEventStream::new(
            EventStreamType::Bar,
            "AAPL",
            feature_timeframe,
            main_timeframe,
            false,
        );
        state.load_event_stream_bars(
            stream,
            vec![Bar {
                symbol: "AAPL".to_string(),
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
                volume: 1_000.0,
                timestamp,
            }],
        );

        let mut conn = connect_database(&dir).await.unwrap();
        init_schema(&conn).await.unwrap();
        let tx = conn.transaction().await.unwrap();
        insert_bars(&tx, &state).await.unwrap();
        tx.commit().await.unwrap();

        let mut rows = conn
            .query(
                "SELECT symbol, history_key, timeframe_label, is_feature, allow_trading, open, high, low, close, volume, event_at FROM bars",
                (),
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        let symbol: String = row.get(0).unwrap();
        let history_key: String = row.get(1).unwrap();
        let timeframe_label: String = row.get(2).unwrap();
        let is_feature: i64 = row.get(3).unwrap();
        let allow_trading: i64 = row.get(4).unwrap();
        let open: f64 = row.get(5).unwrap();
        let high: f64 = row.get(6).unwrap();
        let low: f64 = row.get(7).unwrap();
        let close: f64 = row.get(8).unwrap();
        let volume: f64 = row.get(9).unwrap();
        let event_at: String = row.get(10).unwrap();

        assert_eq!(symbol, "AAPL");
        assert_eq!(history_key, "AAPL:15m");
        assert_eq!(timeframe_label, "15m");
        assert_eq!(is_feature, 1);
        assert_eq!(allow_trading, 0);
        assert_eq!(open, 100.0);
        assert_eq!(high, 101.0);
        assert_eq!(low, 99.0);
        assert_eq!(close, 100.5);
        assert_eq!(volume, 1_000.0);
        assert_eq!(event_at, timestamp.to_rfc3339());

        drop(rows);
        drop(conn);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn account_history_batches_rows_and_rebuilds_indexes() {
        let dir = std::env::temp_dir().join(format!(
            "aqe-backtest-batched-account-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let start = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let history = (0..250)
            .map(|offset| {
                (
                    start + chrono::Duration::minutes(offset),
                    test_account(10_000.0 + offset as f64),
                )
            })
            .collect::<Vec<_>>();

        let mut conn = connect_database(&dir).await.unwrap();
        init_schema(&conn).await.unwrap();
        let tx = conn.transaction().await.unwrap();
        drop_secondary_indexes(&tx).await.unwrap();
        insert_account_history(&tx, &history).await.unwrap();
        create_secondary_indexes(&tx).await.unwrap();
        tx.commit().await.unwrap();

        let mut count_rows = conn
            .query("SELECT COUNT(*) FROM account_history", ())
            .await
            .unwrap();
        let count: i64 = count_rows.next().await.unwrap().unwrap().get(0).unwrap();
        assert_eq!(count, 250);

        let mut index_rows = conn
            .query(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_account_history_event_at'",
                (),
            )
            .await
            .unwrap();
        let index_count: i64 = index_rows.next().await.unwrap().unwrap().get(0).unwrap();
        assert_eq!(index_count, 1);

        drop(count_rows);
        drop(index_rows);
        drop(conn);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn regime_lookup_uses_latest_point_at_or_before_entry() {
        let start = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let points = (0..4)
            .map(|offset| RegimePoint {
                timestamp: start + chrono::Duration::hours(offset * 2),
                vol_regime: format!("vol-{offset}"),
                trend_regime: format!("trend-{offset}"),
                market_return_pct: offset as f64,
            })
            .collect::<Vec<_>>();

        assert_eq!(
            regime_at_entry(&points, start - chrono::Duration::hours(1))
                .unwrap()
                .vol_regime,
            "vol-0"
        );
        assert_eq!(
            regime_at_entry(&points, start + chrono::Duration::hours(5))
                .unwrap()
                .vol_regime,
            "vol-2"
        );
        assert_eq!(
            regime_at_entry(&points, start + chrono::Duration::hours(20))
                .unwrap()
                .vol_regime,
            "vol-3"
        );
    }

    #[test]
    fn drawdown_series_contains_portfolio_and_realized_strategy_rows() {
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let t1 = Utc.with_ymd_and_hms(2026, 1, 2, 0, 0, 0).unwrap();
        let t2 = Utc.with_ymd_and_hms(2026, 1, 3, 0, 0, 0).unwrap();
        let account_history = vec![
            (t0, test_account(10_000.0)),
            (t1, test_account(9_500.0)),
            (t2, test_account(10_250.0)),
        ];
        let trips = vec![
            RoundTripTrade {
                order_id: "alpha-1".to_string(),
                symbol: "AAPL".to_string(),
                side: OrderSide::Buy,
                insight_id: Some("alpha-insight".to_string()),
                strategy_type: Some("Alpha".to_string()),
                entry_time: t0,
                exit_time: t1,
                entry_price: 100.0,
                exit_price: 95.0,
                qty: 1.0,
                pnl: -5.0,
                commission: 0.0,
                swap: 0.0,
                return_pct: -5.0,
                hold_secs: 86_400,
            },
            RoundTripTrade {
                order_id: "beta-1".to_string(),
                symbol: "MSFT".to_string(),
                side: OrderSide::Buy,
                insight_id: Some("beta-insight".to_string()),
                strategy_type: Some("Beta-CHILD".to_string()),
                entry_time: t0,
                exit_time: t2,
                entry_price: 100.0,
                exit_price: 105.0,
                qty: 1.0,
                pnl: 5.0,
                commission: 0.0,
                swap: 0.0,
                return_pct: 5.0,
                hold_secs: 172_800,
            },
        ];

        let rows = build_drawdown_series(&account_history, &trips);
        let names = rows
            .iter()
            .map(|row| row.strategy_name.as_str())
            .collect::<std::collections::BTreeSet<_>>();

        assert!(names.contains("Portfolio Equity"));
        assert!(names.contains("Alpha"));
        assert!(names.contains("Beta"));
        assert!(
            rows.iter()
                .any(|row| row.series_type == "strategy_realized")
        );
    }
}
