use super::*;
use chrono::TimeDelta;

#[test]
fn audited_risk_repair_does_not_lose_newer_local_trade_state() {
    let at = Utc::now();
    let mut tracker = LivePerformanceTracker::default();
    tracker.initialize_for_account(Some("mt5".into()), 1000.0, 1000.0, at, []);
    let mut cloud = tracker.checkpoint().unwrap();
    cloud.max_drawdown = 4.25;
    cloud.updated_at = at + TimeDelta::seconds(10);
    cloud.equity_risk_reconciliation = Some(EquityRiskReconciliation {
        repair_id: "audit".into(),
        through: cloud.updated_at,
    });
    let mut local = cloud.clone();
    local.equity_risk_reconciliation = None;
    local.updated_at = at + TimeDelta::seconds(11);
    local.max_drawdown = 100.0;
    local.total_trades = 876;
    local.recorded_closed_insight_ids = vec!["newest-trade".into()];
    let selected =
        LivePerformanceTracker::newest_compatible_checkpoint("mt5", [local.clone(), cloud.clone()])
            .unwrap();
    assert_eq!(selected.max_drawdown, 4.25);
    assert_eq!(selected.total_trades, 876);
    assert_eq!(
        selected.recorded_closed_insight_ids,
        local.recorded_closed_insight_ids
    );
    // Reconnect acknowledgement repairs risk without merging trades twice.
    let mut contaminated = cloud.clone();
    contaminated.equity_risk_reconciliation = None;
    contaminated.max_drawdown = 100.0;
    tracker
        .restore(contaminated, 1000.0, at + TimeDelta::seconds(12), [])
        .unwrap();
    tracker.merge_historical_checkpoint(cloud.clone()).unwrap();
    assert_eq!(tracker.checkpoint().unwrap().max_drawdown, 4.25);
    tracker
        .restore(selected.clone(), 1000.0, at + TimeDelta::seconds(12), [])
        .unwrap();
    assert_eq!(
        tracker.checkpoint().unwrap().equity_risk_reconciliation,
        cloud.equity_risk_reconciliation
    );
    // A real subsequent loss from a reconciled runtime must never be hidden.
    tracker.update_equity(0.0, at + TimeDelta::seconds(13));
    assert_eq!(
        LivePerformanceTracker::newest_compatible_checkpoint(
            "mt5",
            [cloud.clone(), tracker.checkpoint().unwrap()]
        )
        .unwrap()
        .max_drawdown,
        100.0
    );
    let mut wrong = cloud.clone();
    wrong.account_id = "other".into();
    assert_eq!(
        LivePerformanceTracker::newest_compatible_checkpoint("mt5", [local.clone(), wrong])
            .unwrap()
            .max_drawdown,
        100.0
    );
    let mut other_origin = cloud;
    other_origin.executed_at = at - TimeDelta::days(1);
    assert_eq!(
        LivePerformanceTracker::newest_compatible_checkpoint("mt5", [local, other_origin])
            .unwrap()
            .max_drawdown,
        100.0
    );
}

#[test]
fn persisted_account_equity_origin_survives_restart_without_resetting_metrics() {
    let at = Utc::now() - TimeDelta::hours(1);
    let mut tracker = LivePerformanceTracker::default();
    // Legacy checkpoint used cash even though existing positions made equity
    // different. The strategy-row origin corrects only the account baseline.
    tracker.initialize_for_account(Some("mt5".into()), 1000.0, 900.0, at, []);
    tracker.update_equity(990.0, at + TimeDelta::minutes(1));
    let return_count = tracker.return_count;
    assert!(tracker.apply_account_equity_baseline("mt5", 900.0, at));
    assert!((tracker.snapshot().unwrap().total_return_pct - 10.0).abs() < 1e-10);
    assert_eq!(tracker.return_count, return_count);
    let checkpoint = tracker.checkpoint().unwrap();
    let mut restarted = LivePerformanceTracker::default();
    restarted
        .restore(checkpoint, 1080.0, at + TimeDelta::hours(1), [])
        .unwrap();
    assert_eq!(restarted.snapshot().unwrap().starting_cash, 900.0);
    assert!((restarted.snapshot().unwrap().total_return_pct - 20.0).abs() < 1e-10);
    assert!(restarted.apply_account_equity_baseline("mt5", 900.0, at));
    assert_eq!(restarted.snapshot().unwrap().starting_cash, 900.0);
    for (account, equity, time) in [
        ("paper", 500.0, at),
        ("mt5", f64::NAN, at),
        ("mt5", f64::INFINITY, at),
        ("mt5", 0.0, at),
        ("mt5", 500.0, at + TimeDelta::hours(2)),
    ] {
        assert!(!restarted.apply_account_equity_baseline(account, equity, time));
        assert_eq!(restarted.snapshot().unwrap().starting_cash, 900.0);
    }
}

#[test]
fn paper_account_equity_origin_uses_same_broker_neutral_tracker() {
    let at = Utc::now();
    let mut tracker = LivePerformanceTracker::default();
    tracker.initialize_for_account(Some("paper".into()), 1000.0, 1000.0, at, []);
    tracker.update_equity(1050.0, at + TimeDelta::minutes(1));
    assert!(tracker.apply_account_equity_baseline("paper", 1000.0, at));
    assert!((tracker.snapshot().unwrap().total_return_pct - 5.0).abs() < 1e-10);
}

#[test]
fn very_young_live_session_never_serializes_unbounded_metrics_as_null() {
    let started_at = Utc::now();
    let mut tracker = LivePerformanceTracker::default();
    tracker.initialize(100.0, 100.0, started_at, ["SOLUSD".to_string()]);
    tracker.update_equity(200.0, started_at + TimeDelta::seconds(1));
    tracker.update_open_position_metrics(
        1,
        1,
        f64::INFINITY,
        1,
        0,
        vec!["SOLUSD".to_string()],
        started_at + TimeDelta::seconds(1),
    );

    let snapshot = tracker.snapshot().expect("initialized live metrics");
    assert!(snapshot.cagr.is_finite());
    assert!(snapshot.calmar_ratio.is_finite());
    assert!(snapshot.open_positions_unrealized_pnl.is_finite());

    let json = serde_json::to_value(snapshot).expect("serializable live metrics");
    assert!(json["cagr"].is_number());
    assert!(json["calmar_ratio"].is_number());
    assert!(json["open_positions_unrealized_pnl"].is_number());
}

#[test]
fn fast_equity_refresh_does_not_change_return_sampling_frequency() {
    let started_at = Utc::now();
    let mut tracker = LivePerformanceTracker::default();
    tracker.initialize(100.0, 100.0, started_at, ["SOLUSD".to_string()]);

    tracker.refresh_equity(101.0, started_at + TimeDelta::seconds(1));
    tracker.refresh_equity(102.0, started_at + TimeDelta::seconds(2));
    assert_eq!(tracker.return_count, 0);
    assert_eq!(tracker.previous_equity, Some(100.0));

    tracker.update_equity(103.0, started_at + TimeDelta::seconds(5));
    assert_eq!(tracker.return_count, 1);
    assert_eq!(tracker.previous_equity, Some(103.0));
    assert_eq!(tracker.snapshot().unwrap().final_equity, 103.0);
}

#[test]
fn unchanged_open_position_metrics_do_not_mark_tracker_dirty() {
    let started_at = Utc::now();
    let mut tracker = LivePerformanceTracker::default();
    tracker.initialize(100.0, 100.0, started_at, ["SOLUSD".to_string()]);
    tracker.mark_persisted();

    tracker.update_open_position_metrics(
        0,
        0,
        0.0,
        0,
        0,
        vec!["SOLUSD".to_string()],
        started_at + TimeDelta::seconds(1),
    );

    assert!(!tracker.should_persist());
}

#[test]
fn checkpoint_round_trip_preserves_accumulators_without_restart_sample() {
    let started_at = Utc::now() - TimeDelta::hours(3);
    let mut tracker = LivePerformanceTracker::default();
    tracker.initialize_for_account(
        Some(" broker-42 ".to_string()),
        100_000.0,
        100_000.0,
        started_at,
        ["ETHUSD".to_string()],
    );
    tracker.update_equity(101_000.0, started_at + TimeDelta::minutes(5));
    let checkpoint = tracker.checkpoint().expect("checkpoint");
    let checkpoint: LiveMetricsCheckpoint =
        serde_json::from_slice(&serde_json::to_vec(&checkpoint).expect("serialize checkpoint"))
            .expect("deserialize checkpoint");

    let mut restored = LivePerformanceTracker::default();
    restored
        .restore(
            checkpoint.clone(),
            101_500.0,
            started_at + TimeDelta::hours(3),
            ["SOLUSD".to_string()],
        )
        .expect("restore checkpoint");

    assert_eq!(restored.account_id.as_deref(), Some("broker-42"));
    assert_eq!(restored.return_count, checkpoint.return_count);
    assert_eq!(restored.previous_equity, Some(101_500.0));
    assert_eq!(restored.snapshot().unwrap().executed_at, started_at);
    assert_eq!(restored.snapshot().unwrap().finished_at, None);
    assert_eq!(restored.symbols, vec!["ETHUSD", "SOLUSD"]);
}

#[test]
fn newest_checkpoint_is_selected_without_adding_accumulators() {
    let now = Utc::now();
    let checkpoint = |account: &str, updated_at| LiveMetricsCheckpoint {
        version: LIVE_METRICS_CHECKPOINT_VERSION,
        account_id: account.to_string(),
        equity_risk_reconciliation: None,
        history_recovery_complete: true,
        starting_cash: 100.0,
        current_equity: 100.0,
        peak_equity: 100.0,
        max_drawdown: 0.0,
        total_trades: if updated_at == now { 7 } else { 3 },
        recorded_closed_insight_ids: Vec::new(),
        winning_trades: 0,
        losing_trades: 0,
        gross_profit: 0.0,
        gross_loss_abs: 0.0,
        sum_winner: 0.0,
        sum_loser_abs: 0.0,
        sum_winner_pct: 0.0,
        sum_loser_pct_abs: 0.0,
        best_trade: 0.0,
        worst_trade: 0.0,
        sum_hold_secs: 0,
        longest_winning_trade_held_secs: 0,
        longest_losing_trade_held_secs: 0,
        open_positions_count: 0,
        open_insights_count: 0,
        open_positions_unrealized_pnl: 0.0,
        open_positions_profitable_count: 0,
        open_positions_losing_count: 0,
        symbols: vec![],
        return_count: 0,
        return_mean: 0.0,
        return_m2: 0.0,
        downside_return_count: 0,
        downside_return_sum_sq: 0.0,
        previous_equity: Some(100.0),
        underwater_start: None,
        max_drawdown_duration_secs: 0,
        executed_at: now - TimeDelta::hours(1),
        updated_at,
    };
    let selected = LivePerformanceTracker::newest_compatible_checkpoint(
        "account-a",
        [
            checkpoint("account-a", now - TimeDelta::minutes(1)),
            checkpoint("account-a", now),
            checkpoint("account-b", now + TimeDelta::minutes(1)),
        ],
    )
    .expect("matching checkpoint");
    assert_eq!(selected.total_trades, 7);
}

#[test]
fn closed_trade_history_fixture_is_deterministic_and_idempotent() {
    let now = Utc::now();
    let mut records = vec![
        HistoricalClosedInsightMetric {
            insight_id: "winner".to_string(),
            side: "Buy".to_string(),
            quantity: Some(2.0),
            limit_price: None,
            stop_price: None,
            filled_price: Some(10.0),
            close_price: Some(12.0),
            broker_realized_pnl: Some(3.5),
            commission: Some(0.25),
            swap: Some(0.1),
            created_at: now - TimeDelta::minutes(20),
            updated_at: now - TimeDelta::minutes(10),
            filled_at: Some(now - TimeDelta::minutes(18)),
            closed_at: Some(now - TimeDelta::minutes(10)),
        },
        HistoricalClosedInsightMetric {
            insight_id: "loser".to_string(),
            side: "Sell".to_string(),
            quantity: Some(1.0),
            limit_price: None,
            stop_price: None,
            filled_price: Some(10.0),
            close_price: Some(11.0),
            broker_realized_pnl: None,
            commission: Some(0.2),
            swap: Some(-0.1),
            created_at: now - TimeDelta::minutes(9),
            updated_at: now,
            filled_at: Some(now - TimeDelta::minutes(8)),
            closed_at: Some(now),
        },
    ];
    records.insert(1, records[0].clone());
    let mut tracker = LivePerformanceTracker::default();
    tracker.initialize_for_account(
        Some("account".to_string()),
        100.0,
        102.2,
        now - TimeDelta::hours(1),
        std::iter::empty(),
    );

    tracker.replace_trade_history(records.clone());
    let first = tracker.checkpoint().unwrap();
    tracker.replace_trade_history(records);
    let second = tracker.checkpoint().unwrap();

    assert_eq!(first.total_trades, 2);
    assert_eq!(first.winning_trades, 1);
    assert_eq!(first.losing_trades, 1);
    assert_eq!(first.gross_profit, 3.5);
    assert!((first.gross_loss_abs - 1.3).abs() < 1e-9);
    assert_eq!(first.total_trades, second.total_trades);
    assert_eq!(first.gross_profit, second.gross_profit);
    assert_eq!(first.gross_loss_abs, second.gross_loss_abs);
    assert!(second.history_recovery_complete);

    // Cloud can become reachable only after the local baseline has been restored.
    // This is an acknowledgement of existing history, not a second trade replay.
    let mut restarted = LivePerformanceTracker::default();
    restarted
        .restore(second.clone(), 102.2, now + TimeDelta::hours(1), [])
        .unwrap();
    let before = restarted.checkpoint().unwrap();
    restarted
        .merge_historical_checkpoint(second.clone())
        .unwrap();
    assert_eq!(restarted.checkpoint().unwrap(), before);

    // Matching counters alone cannot establish continuity: unknown identities
    // must still be rejected instead of silently treating them as already recorded.
    let mut unrelated = second;
    unrelated.recorded_closed_insight_ids[0] = "unseen-trade".to_string();
    assert!(restarted.merge_historical_checkpoint(unrelated).is_err());
    assert_eq!(restarted.checkpoint().unwrap(), before);
}

#[test]
fn checkpoint_for_a_different_broker_account_is_not_compatible() {
    let now = Utc::now();
    let mut tracker = LivePerformanceTracker::default();
    tracker.initialize_for_account(
        Some("account-a".to_string()),
        100.0,
        100.0,
        now,
        std::iter::empty(),
    );
    assert!(
        LivePerformanceTracker::newest_compatible_checkpoint(
            "account-b",
            [tracker.checkpoint().unwrap()]
        )
        .is_none()
    );
}

#[test]
fn non_overlapping_checkpoint_merge_combines_online_statistics_once() {
    let now = Utc::now();
    let mut historical_tracker = LivePerformanceTracker::default();
    historical_tracker.initialize_for_account(
        Some("account".to_string()),
        100.0,
        100.0,
        now - TimeDelta::hours(2),
        ["ETHUSD".to_string()],
    );
    historical_tracker.update_equity(101.0, now - TimeDelta::hours(1));
    let historical = historical_tracker.checkpoint().unwrap();

    let mut current = LivePerformanceTracker::default();
    current.initialize_for_account(
        Some("account".to_string()),
        101.0,
        101.0,
        now,
        ["SOLUSD".to_string()],
    );
    current.update_equity(102.0, now + TimeDelta::minutes(5));
    current
        .merge_historical_checkpoint(historical.clone())
        .expect("non-overlapping merge");

    assert_eq!(current.return_count, 2);
    assert_eq!(current.snapshot().unwrap().starting_cash, 100.0);
    assert_eq!(
        current.snapshot().unwrap().executed_at,
        historical.executed_at
    );
    assert_eq!(current.symbols, vec!["ETHUSD", "SOLUSD"]);
    let merged = current.checkpoint().unwrap();
    current
        .merge_historical_checkpoint(historical)
        .expect("an already included baseline is acknowledged without replay");
    assert_eq!(current.checkpoint().unwrap(), merged);
}
