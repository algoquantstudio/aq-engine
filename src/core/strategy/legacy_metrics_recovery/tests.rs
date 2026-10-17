use super::*;
use crate::core::strategy::live_metrics::LivePerformanceTracker;
use std::sync::{Arc, Mutex};

fn record(index: usize) -> HistoricalClosedInsightMetric {
    let at = chrono::DateTime::from_timestamp(1_700_000_000 + index as i64, 0).unwrap();
    HistoricalClosedInsightMetric {
        insight_id: format!("trade-{index}"),
        side: "Buy".into(),
        quantity: Some(2.0),
        limit_price: None,
        stop_price: None,
        filled_price: Some(10.0),
        close_price: Some(12.0),
        broker_realized_pnl: Some(3.5),
        commission: Some(0.25),
        swap: Some(0.1),
        created_at: at - chrono::TimeDelta::minutes(10),
        updated_at: at,
        filled_at: Some(at - chrono::TimeDelta::minutes(5)),
        closed_at: Some(at),
    }
}

fn tracker() -> LivePerformanceTracker {
    let mut tracker = LivePerformanceTracker::default();
    let at = chrono::DateTime::from_timestamp(1_800_000_000, 0).unwrap();
    tracker.initialize_for_account(Some("account".into()), 100.0, 110.0, at, ["ETHUSD".into()]);
    tracker.update_equity(105.0, at + chrono::TimeDelta::seconds(5));
    tracker.update_open_position_metrics(
        3,
        4,
        9.0,
        2,
        1,
        vec!["ETHUSD".into()],
        at + chrono::TimeDelta::seconds(6),
    );
    tracker
}

fn live_close(index: usize) -> crate::core::insight::InsightSnapshot {
    let row = record(index);
    serde_json::from_value(serde_json::json!({
        "insight_id": row.insight_id, "state": "Closed", "side": "Buy", "symbol": "ETHUSD",
        "order_type": "Market", "order_class": "Simple", "strategy_type": "test", "confidence": 100,
        "timeframe": null, "execution_depends": [], "partial_closes": [], "state_history": [],
        "created_at": row.created_at, "updated_at": row.updated_at, "closed_at": row.closed_at,
        "filled_at": row.filled_at, "legs": {}, "market_changed": false, "submitted": true,
        "cancelling": false, "closing": false, "first_on_fill": false,
        "quantity": row.quantity, "filled_price": row.filled_price, "close_price": row.close_price,
        "broker_realized_pnl": row.broker_realized_pnl, "commission": row.commission, "swap": row.swap,
    })).unwrap()
}

#[tokio::test]
async fn legacy_recovery_fenced_commit_keeps_new_closes_and_deduplicates_cloud_overlap() {
    let mut tracker = tracker();
    let token = tracker.begin_trade_history_recovery().unwrap();
    assert!(tracker.begin_trade_history_recovery().is_err());
    let (release, wait) = tokio::sync::oneshot::channel();
    let mut worker = LegacyMetricsRecovery::default();
    worker
        .start(token.clone(), async move {
            wait.await.unwrap();
            load_trade_history_pages("strategy", |_, _| async { Ok(vec![record(0), record(1)]) })
                .await
        })
        .unwrap();
    tracker.record_closed_insight(&live_close(1));
    tracker.record_closed_insight(&live_close(1));
    tracker.record_closed_insight(&live_close(2));
    tracker.refresh_equity(120.0, chrono::Utc::now());
    release.send(()).unwrap();
    let completed = worker.next().await.unwrap();
    tracker
        .try_apply_trade_history_recovery(&completed.token, completed.result.unwrap())
        .unwrap();
    let after = tracker.checkpoint().unwrap();
    assert_eq!(after.total_trades, 3);
    assert_eq!(after.gross_profit, 10.5);
    assert_eq!(after.current_equity, 120.0);
    assert_eq!(after.open_positions_count, 3);
    assert!(after.history_recovery_complete);
}

#[tokio::test]
async fn legacy_recovery_missing_known_close_cannot_replace_current_metrics() {
    let mut tracker = tracker();
    tracker.record_closed_insight(&live_close(99));
    let before = tracker.checkpoint().unwrap();
    let token = tracker.begin_trade_history_recovery().unwrap();
    let rebuilt = load_trade_history_pages("strategy", |_, _| async { Ok(vec![record(0)]) })
        .await
        .unwrap();
    assert!(
        tracker
            .try_apply_trade_history_recovery(&token, rebuilt)
            .is_err()
    );
    assert_eq!(tracker.checkpoint().unwrap(), before);
    let retry = tracker.begin_trade_history_recovery().unwrap();
    let rebuilt =
        load_trade_history_pages("strategy", |_, _| async { Ok(vec![record(0), record(99)]) })
            .await
            .unwrap();
    tracker
        .try_apply_trade_history_recovery(&retry, rebuilt)
        .unwrap();
    assert_eq!(tracker.checkpoint().unwrap().total_trades, 2);
}

#[test]
fn legacy_recovery_obsolete_attempt_and_account_are_fenced_without_cancelling_new_attempt() {
    let mut tracker = tracker();
    let old = tracker.begin_trade_history_recovery().unwrap();
    tracker.cancel_trade_history_recovery(&old);
    let current = tracker.begin_trade_history_recovery().unwrap();
    tracker.cancel_trade_history_recovery(&old);
    assert!(
        tracker
            .try_apply_trade_history_recovery(&old, TradeHistoryRebuild::default())
            .is_err()
    );
    tracker
        .try_apply_trade_history_recovery(&current, TradeHistoryRebuild::default())
        .unwrap();
    let old = tracker.begin_trade_history_recovery().unwrap();
    tracker.initialize_for_account(
        Some("different-account".into()),
        500.0,
        500.0,
        chrono::Utc::now(),
        [],
    );
    let before = tracker.checkpoint().unwrap();
    assert!(
        tracker
            .try_apply_trade_history_recovery(&old, TradeHistoryRebuild::default())
            .is_err()
    );
    assert_eq!(tracker.checkpoint().unwrap(), before);
}

#[test]
fn legacy_recovery_capture_overflow_preserves_live_trades_and_requires_retry() {
    let mut tracker = tracker();
    let token = tracker.begin_trade_history_recovery().unwrap();
    for index in 0..4097 {
        tracker.record_closed_insight(&live_close(index));
    }
    let before = tracker.checkpoint().unwrap();
    assert_eq!(before.total_trades, 4097);
    assert!(
        tracker
            .try_apply_trade_history_recovery(&token, TradeHistoryRebuild::default())
            .is_err()
    );
    assert_eq!(tracker.checkpoint().unwrap(), before);
    assert!(tracker.begin_trade_history_recovery().is_ok());
}

#[tokio::test]
async fn legacy_recovery_pages_commit_once_and_preserve_risk_equity_and_exposure() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let seen = calls.clone();
    let rebuilt = load_trade_history_pages("strategy", move |offset, limit| {
        assert_eq!(limit, 250);
        seen.lock().unwrap().push(offset);
        async move {
            Ok(match offset {
                0 => (0..250).map(record).collect(),
                250 => (250..500).map(record).collect(),
                500 => vec![record(249)], // duplicate across a page boundary
                _ => panic!("unexpected offset"),
            })
        }
    })
    .await
    .unwrap();
    assert_eq!(*calls.lock().unwrap(), [0, 250, 500]);
    assert_eq!(rebuilt.len(), 500);
    let mut tracker = tracker();
    let before = tracker.checkpoint().unwrap();
    tracker.apply_trade_history_rebuild(rebuilt);
    let after = tracker.checkpoint().unwrap();
    assert_eq!(after.total_trades, 500);
    assert_eq!(after.winning_trades, 500);
    assert_eq!(
        after.gross_profit, 1750.0,
        "broker realized P&L takes precedence"
    );
    assert_eq!(after.sum_hold_secs, 150_000);
    assert_eq!(after.current_equity, before.current_equity);
    assert_eq!(after.return_count, before.return_count);
    assert_eq!(after.return_m2, before.return_m2);
    assert_eq!(after.max_drawdown, before.max_drawdown);
    assert_eq!(after.open_positions_count, 3);
    assert_eq!(after.open_insights_count, 4);
    assert_eq!(after.open_positions_unrealized_pnl, 9.0);
    assert_eq!(
        after.updated_at, before.updated_at,
        "old history cannot rewind current snapshot freshness"
    );
    assert!(after.history_recovery_complete);
}

#[tokio::test]
async fn legacy_recovery_failed_later_page_discards_partial_accumulators() {
    let tracker = tracker();
    let before = tracker.checkpoint().unwrap();
    let result = load_trade_history_pages("strategy", |offset, _| async move {
        if offset == 0 {
            Ok((0..250).map(record).collect())
        } else {
            Err("simulated database failure".into())
        }
    })
    .await;
    assert!(result.err().unwrap().contains("offset 250"));
    assert_eq!(tracker.checkpoint().unwrap(), before);
    let rebuilt = load_trade_history_pages("strategy", |_, _| async { Ok(vec![record(1)]) })
        .await
        .unwrap();
    assert_eq!(
        rebuilt.len(),
        1,
        "retry starts with no partial previous history"
    );
}

#[tokio::test]
async fn legacy_recovery_total_deadline_bounds_repeated_successful_pages() {
    let started = Instant::now();
    let result = load_with_limits(
        "strategy",
        1,
        Duration::from_secs(1),
        Duration::from_millis(25),
        |offset, _| async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(vec![record(offset)])
        },
    )
    .await;
    assert!(result.is_err());
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn legacy_recovery_zero_budget_never_starts_query_and_oversized_page_is_rejected() {
    let result = load_with_limits(
        "strategy",
        250,
        Duration::from_secs(1),
        Duration::ZERO,
        |_, _| async {
            panic!("expired recovery cannot start a query");
        },
    )
    .await;
    assert!(result.is_err());
    let result = load_trade_history_pages("strategy", |_, _| async {
        Ok((0..251).map(record).collect())
    })
    .await;
    assert!(result.err().unwrap().contains("page bound"));
}

#[tokio::test]
async fn legacy_recovery_cancelled_attempt_releases_query_and_staging() {
    let retained = Arc::new(());
    let held = retained.clone();
    let result = tokio::time::timeout(
        Duration::from_millis(10),
        load_trade_history_pages("strategy", move |offset, _| {
            let held = held.clone();
            async move {
                let _held = held;
                if offset == 0 {
                    Ok((0..250).map(record).collect())
                } else {
                    std::future::pending().await
                }
            }
        }),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(Arc::strong_count(&retained), 1);
}

#[tokio::test]
async fn legacy_recovery_exact_page_boundary_and_empty_history_terminate() {
    for total in [0, 250, 500] {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let seen = calls.clone();
        let rebuilt = load_trade_history_pages("strategy", move |offset, limit| {
            seen.lock().unwrap().push(offset);
            async move { Ok((offset..total.min(offset + limit)).map(record).collect()) }
        })
        .await
        .unwrap();
        assert_eq!(rebuilt.len(), total);
        assert_eq!(calls.lock().unwrap().len(), total / 250 + 1);
        let mut tracker = tracker();
        tracker.replace_trade_history([record(999)]);
        tracker.apply_trade_history_rebuild(rebuilt);
        assert_eq!(tracker.checkpoint().unwrap().total_trades, total);
    }
}

#[tokio::test]
async fn legacy_recovery_worker_is_single_flight_and_cancelled_wait_keeps_ownership() {
    let mut tracker = tracker();
    let token = tracker.begin_trade_history_recovery().unwrap();
    let mut worker = LegacyMetricsRecovery::default();
    let retained = Arc::new(());
    let held = retained.clone();
    worker
        .start(token.clone(), async move {
            let _held = held;
            std::future::pending().await
        })
        .unwrap();
    assert!(
        worker
            .start(token.clone(), async {
                panic!("duplicate recovery cannot run")
            })
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(10), worker.next())
            .await
            .is_err()
    );
    assert!(worker.is_running());
    let returned_token = worker.shutdown().await.unwrap();
    assert_eq!(returned_token, token);
    tracker.cancel_trade_history_recovery(&returned_token);
    assert!(tracker.begin_trade_history_recovery().is_ok());
    assert!(!worker.is_running());
    assert_eq!(Arc::strong_count(&retained), 1);
}

#[tokio::test]
async fn legacy_recovery_worker_panic_returns_token_and_drop_aborts_owned_read() {
    let mut tracker = tracker();
    let token = tracker.begin_trade_history_recovery().unwrap();
    let mut worker = LegacyMetricsRecovery::default();
    worker
        .start(token.clone(), async { panic!("fake history query panic") })
        .unwrap();
    let completed = worker.next().await.unwrap();
    assert_eq!(completed.token, token);
    assert!(completed.result.is_err());
    tracker.cancel_trade_history_recovery(&completed.token);
    let token = tracker.begin_trade_history_recovery().unwrap();
    let retained = Arc::new(());
    let held = retained.clone();
    worker
        .start(token, async move {
            let _held = held;
            std::future::pending().await
        })
        .unwrap();
    drop(worker);
    tokio::task::yield_now().await;
    assert_eq!(Arc::strong_count(&retained), 1);
}
