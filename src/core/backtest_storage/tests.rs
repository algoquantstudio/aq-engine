use super::*;
use crate::core::broker::types::{AccountType, OrderSide, TradeRecordType};
use crate::core::insight::snapshot::InsightLegsSnapshot;
use chrono::TimeZone;

fn test_account(equity: f64) -> Account {
    Account {
        account_id: "test".to_string(),
        max_total_orders: None,
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
        is_split_insight: false,
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
        entry_filled_quantity: Some(1.0),
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

#[tokio::test]
async fn fill_cash_storage_upgrades_legacy_without_inventing_evidence() {
    let dir = std::env::temp_dir().join(format!("aqe-fill-cash-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut conn = connect_database(&dir).await.unwrap();
    conn.execute_batch("CREATE TABLE trade_log(id INTEGER PRIMARY KEY,event_at TEXT,symbol TEXT,side TEXT,qty REAL,price REAL,order_id TEXT,insight_id TEXT,strategy_type TEXT,commission REAL,swap REAL,trade_type TEXT);
        INSERT INTO trade_log VALUES(1,'2026-01-01T00:00:00Z','CUSTOM','Buy',2,100,'legacy',NULL,NULL,0,0,'Entry');").await.unwrap();
    init_schema(&conn).await.unwrap();
    init_schema(&conn).await.unwrap();
    let mut legacy = conn.query("SELECT execution_cash_json,account_notional,valuation_basis FROM trade_log WHERE id=1", ()).await.unwrap();
    let row = legacy.next().await.unwrap().unwrap();
    assert_eq!(row.get::<Option<String>>(0).unwrap(),None);
    assert_eq!(row.get::<Option<f64>>(1).unwrap(),None);
    assert_eq!(row.get::<Option<String>>(2).unwrap(),None);
    drop(legacy);
    let mut record: TradeRecord = serde_json::from_value(serde_json::json!({
        "date":"2026-01-01T00:05:00Z","symbol":"CUSTOM","side":"Buy","qty":2.,"price":100.,"order_id":"new","commission":-1.,"swap":0.,"trade_type":"Entry"
    })).unwrap();
    assert!(record.execution_cash.is_none());
    assert!(!serde_json::to_string(&record).unwrap().contains("execution_cash"));
    record.execution_cash=Some(aqe_live_protocol::ExecutionFillCash {currency:"EUR".into(),gross_pnl:Some(0.),commission:Some(-1.),carry:Some(0.)});
    record.account_notional=Some(200.);
    record.valuation_basis=Some("paper_price_units".into());
    let tx=conn.transaction().await.unwrap();
    insert_trade_log(&tx,&[record.clone()]).await.unwrap();
    tx.commit().await.unwrap();
    let mut rows=conn.query("SELECT execution_cash_json,account_notional,valuation_basis FROM trade_log WHERE id=2",()).await.unwrap();
    let row=rows.next().await.unwrap().unwrap();
    let cash:aqe_live_protocol::ExecutionFillCash=serde_json::from_str(&row.get::<String>(0).unwrap()).unwrap();
    assert_eq!(Some(cash),record.execution_cash);
    assert_eq!(row.get::<f64>(1).unwrap(),200.);
    assert_eq!(row.get::<String>(2).unwrap(),"paper_price_units");
    drop(rows);drop(conn);std::fs::remove_dir_all(dir).unwrap();
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
            execution_cash: None,
            account_notional: None,
            valuation_basis: None,
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
            execution_cash: None,
            account_notional: None,
            valuation_basis: None,
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
async fn entry_fill_quantity_persists_without_changing_partial_close_quantity() {
    let dir =
        std::env::temp_dir().join(format!("aqe-entry-fill-snapshot-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut conn = connect_database(&dir).await.unwrap();
    init_schema(&conn).await.unwrap();
    let mut snapshot = test_snapshot("entry", None, "Testing");
    snapshot.entry_filled_quantity = Some(4.0);
    snapshot.partial_filled_quantity = Some(1.0);
    let tx = conn.transaction().await.unwrap();
    insert_insights(&tx, &[snapshot]).await.unwrap();
    tx.commit().await.unwrap();
    let mut rows = conn.query("SELECT entry_filled_quantity, partial_filled_quantity FROM insights WHERE insight_id = 'entry'", ()).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<f64>(0).unwrap(), 4.0);
    assert_eq!(row.get::<f64>(1).unwrap(), 1.0);
    drop(rows);
    drop(conn);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn entry_fill_quantity_schema_upgrade_preserves_legacy_rows() {
    let dir = std::env::temp_dir().join(format!("aqe-entry-fill-upgrade-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let conn = connect_database(&dir).await.unwrap();
    conn.execute_batch("CREATE TABLE insights(insight_id TEXT PRIMARY KEY, retained TEXT); INSERT INTO insights VALUES('old', 'unchanged');").await.unwrap();
    init_schema(&conn).await.unwrap();
    init_schema(&conn).await.unwrap();
    let mut rows = conn
        .query(
            "SELECT retained, entry_filled_quantity FROM insights WHERE insight_id = 'old'",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<String>(0).unwrap(), "unchanged");
    assert_eq!(row.get::<Option<f64>>(1).unwrap(), None);
    drop(rows);
    drop(conn);
    std::fs::remove_dir_all(dir).unwrap();
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
