use super::*;
use crate::core::strategy::types::live_metrics::HistoricalClosedInsightMetric;

fn at(seconds: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(1_800_000_000 + seconds, 0).unwrap()
}

fn trade(id: &str, seconds: i64) -> HistoricalClosedInsightMetric {
    HistoricalClosedInsightMetric {
        insight_id: id.into(),
        side: "Buy".into(),
        quantity: Some(1.0),
        limit_price: None,
        stop_price: None,
        filled_price: Some(100.0),
        close_price: Some(110.0),
        broker_realized_pnl: Some(10.0),
        commission: Some(0.0),
        swap: Some(0.0),
        created_at: at(seconds - 20),
        updated_at: at(seconds),
        filled_at: Some(at(seconds - 10)),
        closed_at: Some(at(seconds)),
    }
}

fn tracker(start: i64) -> LivePerformanceTracker {
    let mut tracker = LivePerformanceTracker::default();
    tracker.initialize_for_account(
        Some("account".into()),
        100.0,
        100.0,
        at(start),
        ["ETHUSD".into()],
    );
    tracker.update_equity(90.0, at(start + 1));
    tracker.update_equity(110.0, at(start + 2));
    tracker
}

fn baseline() -> LegacyRiskBaseline {
    let mut historical = tracker(0);
    historical.replace_trade_history([trade("old", 3)]);
    LegacyRiskBaseline::prepare(
        historical.snapshot().unwrap(),
        "account".into(),
        Some((95.0, at(-10))),
    )
    .unwrap()
}

#[test]
fn legacy_baseline_preserves_live_activity_and_does_not_add_unidentified_trades() {
    let baseline = baseline();
    assert_eq!(baseline.checkpoint.total_trades, 0);
    assert!(baseline.checkpoint.recorded_closed_insight_ids.is_empty());
    let historical_returns = baseline.checkpoint.return_count;
    let mut current = tracker(100);
    current.replace_trade_history([trade("during-lookup", 103)]);
    current.mark_history_recovery_incomplete();
    current.update_open_position_metrics(3, 4, 7.0, 2, 1, vec!["SOLUSD".into()], at(104));
    let before = current.checkpoint().unwrap();
    assert!(matches!(
        current
            .merge_legacy_risk_baseline(baseline.clone())
            .unwrap(),
        LegacyRiskMerge::NeedsTradeHistory
    ));
    let after = current.checkpoint().unwrap();
    assert_eq!(after.total_trades, before.total_trades);
    assert_eq!(after.gross_profit, before.gross_profit);
    assert_eq!(
        after.recorded_closed_insight_ids,
        before.recorded_closed_insight_ids
    );
    assert_eq!(after.current_equity, before.current_equity);
    assert_eq!(after.previous_equity, before.previous_equity);
    assert_eq!(after.open_positions_count, 3);
    assert_eq!(after.open_insights_count, 4);
    assert_eq!(after.open_positions_unrealized_pnl, 7.0);
    assert_eq!(after.return_count, before.return_count + historical_returns);
    assert_eq!(after.updated_at, before.updated_at);
    assert_eq!(after.starting_cash, 95.0);
    assert_eq!(after.executed_at, at(-10));
    assert!(after.symbols.contains(&"ETHUSD".into()));
    assert!(after.symbols.contains(&"SOLUSD".into()));
    assert!(!after.history_recovery_complete);

    // A retry and a restart from the incomplete checkpoint cannot replay risk.
    current
        .merge_legacy_risk_baseline(baseline.clone())
        .unwrap();
    assert_eq!(current.checkpoint().unwrap(), after);
    let mut restored = LivePerformanceTracker::default();
    restored.restore(after, 110.0, at(105), []).unwrap();
    let restored_before = restored.checkpoint().unwrap();
    restored.merge_legacy_risk_baseline(baseline).unwrap();
    assert_eq!(restored.checkpoint().unwrap(), restored_before);
}

#[test]
fn legacy_baseline_complete_checkpoint_is_preferred_without_reset() {
    let mut current = tracker(100);
    current.replace_trade_history([trade("local", 103)]);
    let before = current.checkpoint().unwrap();
    assert!(matches!(
        current.merge_legacy_risk_baseline(baseline()).unwrap(),
        LegacyRiskMerge::CompleteCheckpointPreferred
    ));
    assert_eq!(current.checkpoint().unwrap(), before);
}

#[test]
fn legacy_baseline_account_and_time_conflicts_leave_current_state_untouched() {
    let mut current = tracker(100);
    current.mark_history_recovery_incomplete();
    let before = current.checkpoint().unwrap();
    let mut wrong_account = baseline();
    wrong_account.checkpoint.account_id = "other".into();
    assert!(current.merge_legacy_risk_baseline(wrong_account).is_err());
    assert_eq!(current.checkpoint().unwrap(), before);
    let overlap =
        LegacyRiskBaseline::prepare(tracker(101).snapshot().unwrap(), "account".into(), None)
            .unwrap();
    assert!(current.merge_legacy_risk_baseline(overlap).is_err());
    assert_eq!(current.checkpoint().unwrap(), before);
}

#[test]
fn legacy_baseline_rejects_invalid_or_future_account_origin() {
    let snapshot = tracker(0).snapshot().unwrap();
    for origin in [(f64::NAN, at(-1)), (f64::INFINITY, at(-1)), (100.0, at(10))] {
        assert!(
            LegacyRiskBaseline::prepare(snapshot.clone(), "account".into(), Some(origin)).is_err()
        );
    }
}

#[test]
fn legacy_baseline_trade_rebuild_must_cover_prelookup_closes() {
    let mut current = tracker(100);
    current.replace_trade_history([trade("during-lookup", 103)]);
    current.mark_history_recovery_incomplete();
    current.merge_legacy_risk_baseline(baseline()).unwrap();
    let before = current.checkpoint().unwrap();
    let token = current.begin_trade_history_recovery().unwrap();
    let mut missing = TradeHistoryRebuild::default();
    missing.push_page([trade("old", 3)]);
    assert!(
        current
            .try_apply_trade_history_recovery(&token, missing)
            .is_err()
    );
    assert_eq!(current.checkpoint().unwrap(), before);

    let token = current.begin_trade_history_recovery().unwrap();
    let mut complete = TradeHistoryRebuild::default();
    complete.push_page([
        trade("old", 3),
        trade("during-lookup", 103),
        trade("old", 3),
    ]);
    current
        .try_apply_trade_history_recovery(&token, complete)
        .unwrap();
    let after = current.checkpoint().unwrap();
    assert_eq!(after.total_trades, 2);
    assert_eq!(after.gross_profit, 20.0);
    assert_eq!(after.return_count, before.return_count);
    assert_eq!(after.current_equity, before.current_equity);
    assert!(after.history_recovery_complete);
}
