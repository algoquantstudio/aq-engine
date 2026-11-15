// Included in storage::tests so existing schema fixtures remain shared.
async fn bulk_test_connection() -> Connection {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();
    init_schema(&conn).await.unwrap();
    conn
}

fn bulk_fixture(count: usize) -> (Vec<InsightSnapshot>, Vec<TradeRecord>, Vec<RoundTripTrade>) {
    let start = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let mut snapshots = Vec::new();
    let mut events = Vec::new();
    let mut trips = Vec::new();
    for index in 0..count {
        let id = format!("insight-{index:06}");
        let mut snapshot = test_snapshot(&id, Some("parent"), "Bulk-CHILD");
        snapshot.order_id = Some(format!("order-{index}"));
        snapshot.quantity = Some(10.0);
        snapshot.entry_filled_quantity = Some(10.0);
        snapshot.partial_filled_quantity = Some(4.0);
        snapshot.commission = Some(2.0);
        snapshot.swap = Some(-0.5);
        snapshot.take_profit_levels = Some(vec![105.0, 110.0]);
        snapshot.execution_depends = vec!["dependency-1".into(), "dependency-2".into()];
        snapshot
            .state_history
            .push(crate::core::insight::InsightStateHistorySnapshot {
                at: start,
                state: "Filled".into(),
                message: Some("execution evidence".repeat(8)),
                kind: Some("Lifecycle".into()),
                from_state: Some("Executed".into()),
                to_state: Some("Filled".into()),
            });
        snapshots.push(snapshot);
        let entry = TradeRecord {
            date: start + chrono::Duration::minutes(index as i64),
            symbol: "AAPL".into(),
            side: OrderSide::Buy,
            qty: 10.0,
            price: 100.0,
            order_id: format!("order-{index}"),
            insight_id: Some(id.clone()),
            strategy_type: Some("Bulk-CHILD".into()),
            commission: 2.0,
            swap: 0.0,
            execution_cash: None,
            account_notional: None,
            valuation_basis: None,
            trade_type: TradeRecordType::Entry,
        };
        events.push(entry.clone());
        for (seconds, qty, price, commission, swap) in
            [(10, 4.0, 105.0, 1.0, -0.5), (20, 6.0, 95.0, 1.5, 0.25)]
        {
            let date = entry.date + chrono::Duration::seconds(seconds);
            events.push(TradeRecord {
                date,
                qty,
                price,
                commission,
                swap,
                trade_type: TradeRecordType::Exit,
                ..entry.clone()
            });
            let fees = entry.commission * (qty / entry.qty) + commission;
            let pnl = (price - entry.price) * qty + swap - fees;
            trips.push(RoundTripTrade {
                order_id: entry.order_id.clone(),
                symbol: entry.symbol.clone(),
                side: entry.side.clone(),
                insight_id: entry.insight_id.clone(),
                strategy_type: entry.strategy_type.clone(),
                entry_time: entry.date,
                exit_time: date,
                entry_price: entry.price,
                exit_price: price,
                qty,
                pnl,
                commission: fees,
                swap,
                return_pct: pnl / (entry.price * qty) * 100.0,
                hold_secs: seconds,
            });
        }
    }
    // Preserve INSERT OR REPLACE's last-observation semantics inside one batch.
    if let Some(first) = snapshots.first().cloned() {
        let mut replacement = first;
        replacement.confidence = 17;
        replacement.state_history[0].message = Some("replacement evidence".into());
        snapshots.push(replacement);
    }
    (snapshots, events, trips)
}

async fn insert_rowwise(
    tx: &Transaction<'_>,
    prefix: &str,
    columns: usize,
    rows: impl IntoIterator<Item = Result<Vec<Value>, String>>,
) {
    let sql = format!("{prefix} ({})", vec!["?"; columns].join(", "));
    let mut stmt = tx.prepare(&sql).await.unwrap();
    for row in rows {
        stmt.execute(row.unwrap()).await.unwrap();
    }
}

async fn bulk_table_rows(conn: &Connection, table: &str, order: &str) -> Vec<Vec<Value>> {
    let mut result = conn
        .query(&format!("SELECT * FROM {table} ORDER BY {order}"), ())
        .await
        .unwrap();
    let columns = result.column_count();
    let mut rows = Vec::new();
    while let Some(row) = result.next().await.unwrap() {
        rows.push(
            (0..columns)
                .map(|index| row.get_value(index).unwrap())
                .collect(),
        );
    }
    rows
}

#[tokio::test]
async fn bulk_finalization_tables_match_rowwise_ids_json_fees_and_replacement() {
    let (snapshots, events, trips) = bulk_fixture(127);
    let mut reference = bulk_test_connection().await;
    let mut batched = bulk_test_connection().await;
    {
        let tx = reference.transaction().await.unwrap();
        insert_rowwise(
            &tx,
            INSIGHTS_INSERT,
            42,
            snapshots.iter().map(insight_insert_values),
        )
        .await;
        insert_rowwise(
            &tx,
            ROUND_TRIPS_INSERT,
            15,
            trips.iter().map(trip_insert_values),
        )
        .await;
        insert_rowwise(
            &tx,
            TRADE_LOG_ROWS_INSERT,
            17,
            build_trade_log_rows(&events, &snapshots)
                .into_iter()
                .map(row_insert_values),
        )
        .await;
        tx.commit().await.unwrap();
    }
    {
        let tx = batched.transaction().await.unwrap();
        insert_insights(&tx, &snapshots).await.unwrap();
        insert_round_trips(&tx, &trips).await.unwrap();
        insert_trade_log_rows(&tx, &events, &snapshots)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    for (table, order) in [
        ("insights", "insight_id"),
        ("round_trips", "id"),
        ("trade_log_rows", "id"),
    ] {
        assert_eq!(
            bulk_table_rows(&reference, table, order).await,
            bulk_table_rows(&batched, table, order).await,
            "{table}"
        );
    }
    let mut query = batched.query("SELECT confidence, entry_filled_quantity, partial_filled_quantity, state_history_json FROM insights WHERE insight_id='insight-000000'", ()).await.unwrap();
    let row = query.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 17);
    assert_eq!(row.get::<f64>(1).unwrap(), 10.0);
    assert_eq!(row.get::<f64>(2).unwrap(), 4.0);
    assert!(
        row.get::<String>(3)
            .unwrap()
            .contains("replacement evidence")
    );
}

#[tokio::test]
#[ignore = "manual paired finalization bulk persistence performance probe"]
async fn bulk_finalization_performance_probe() {
    use std::time::Instant;
    let (snapshots, events, trips) = bulk_fixture(1000);
    let mut before = Vec::new();
    let mut after = Vec::new();
    for sample in 0..10 {
        let mut reference = bulk_test_connection().await;
        let mut batched = bulk_test_connection().await;
        for use_batch in if sample % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        } {
            let connection = if use_batch {
                &mut batched
            } else {
                &mut reference
            };
            let start = Instant::now();
            let tx = connection.transaction().await.unwrap();
            // Match the writer's index lifecycle, while measuring only these
            // three tables rather than claiming full backtest finalization.
            drop_secondary_indexes(&tx).await.unwrap();
            if use_batch {
                insert_insights(&tx, &snapshots).await.unwrap();
                insert_round_trips(&tx, &trips).await.unwrap();
                insert_trade_log_rows(&tx, &events, &snapshots)
                    .await
                    .unwrap();
            } else {
                insert_rowwise(
                    &tx,
                    INSIGHTS_INSERT,
                    42,
                    snapshots.iter().map(insight_insert_values),
                )
                .await;
                insert_rowwise(
                    &tx,
                    ROUND_TRIPS_INSERT,
                    15,
                    trips.iter().map(trip_insert_values),
                )
                .await;
                insert_rowwise(
                    &tx,
                    TRADE_LOG_ROWS_INSERT,
                    17,
                    build_trade_log_rows(&events, &snapshots)
                        .into_iter()
                        .map(row_insert_values),
                )
                .await;
            }
            create_secondary_indexes(&tx).await.unwrap();
            tx.commit().await.unwrap();
            let elapsed = start.elapsed().as_nanos();
            if use_batch {
                after.push(elapsed);
            } else {
                before.push(elapsed);
            }
        }
        for (table, order) in [
            ("insights", "insight_id"),
            ("round_trips", "id"),
            ("trade_log_rows", "id"),
        ] {
            assert_eq!(
                bulk_table_rows(&reference, table, order).await,
                bulk_table_rows(&batched, table, order).await,
                "{table}"
            );
        }
    }
    before.sort_unstable();
    after.sort_unstable();
    eprintln!(
        "finalization_bulk_component indexes=deferred insights=1001 trips=2000 trade_rows=2000 baseline_p50_ns={} baseline_p95_ns={} batched_p50_ns={} batched_p95_ns={}",
        before[5], before[9], after[5], after[9]
    );
}
