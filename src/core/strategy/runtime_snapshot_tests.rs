#[test]
fn runtime_snapshot_skips_unobserved_details_and_refreshes_every_consumer_attachment() {
    let mut state = StrategyState::new(
        "Runtime preview".into(),
        "1".into(),
        NoopStrategy,
        UnifiedBroker::new(
            PaperBroker::new(AccountType::Paper, 100_000.0, 1),
            FixedScaleOutDataFeed::new(),
        ),
        StrategyMode::Backtest,
        TimeFrame::new(1, TimeFrameUnit::Minute),
    );
    let mut insight = Insight::default();
    insight.symbol = "AAPL".into();
    let id = state.insights.add_insight(insight);
    state.universe.insert("AAPL".into(), test_asset("AAPL"));
    state
        .variables
        .insert("changing".into(), json!({"value": 1}));
    state.publish_runtime_snapshot("offline", Some("saved-result.json".into()));
    // There were no readers during publication. Essential fields still advance.
    let reader = state.runtime_telemetry.snapshot_handle();
    {
        let snapshot = reader.read().unwrap();
        assert!(snapshot.active_insights.is_empty());
        assert!(snapshot.variables.is_empty());
        assert!(snapshot.universe.is_empty());
        assert_eq!(snapshot.insight_counts.get("New"), Some(&1));
        assert_eq!(snapshot.aqs_sync_status, "offline");
        assert_eq!(
            snapshot.saved_result_path.as_deref(),
            Some("saved-result.json")
        );
        assert!(snapshot.backtest_progress.is_some());
    }
    state.publish_runtime_snapshot("connected", None);
    {
        let snapshot = reader.read().unwrap();
        assert_eq!(snapshot.active_insights.len(), 1);
        assert_eq!(snapshot.active_insights[0].symbol, "AAPL");
        assert_eq!(snapshot.variables.len(), 1);
        assert_eq!(snapshot.universe, ["AAPL"]);
        assert!(!snapshot.provider_capabilities.is_empty());
    }
    // No timestamp or state transition accompanies this public-field edit.
    state.insights.get_insight_mut(&id).unwrap().symbol = "BTC-USD".into();
    state
        .variables
        .insert("changing".into(), json!({"value": 2}));
    state.universe.remove("AAPL");
    state
        .universe
        .insert("BTC-USD".into(), test_asset("BTC-USD"));
    state.publish_runtime_snapshot("connected", None);
    assert_eq!(reader.read().unwrap().active_insights[0].symbol, "BTC-USD");
    assert_eq!(reader.read().unwrap().universe, ["BTC-USD"]);
    let expected_variables = state.runtime_variable_snapshot();
    assert_eq!(reader.read().unwrap().variables, expected_variables);
    drop(reader);
    state.insights.get_insight_mut(&id).unwrap().symbol = "EURUSD".into();
    state.publish_runtime_snapshot("disconnected", None);
    let reader = state.runtime_telemetry.snapshot_handle();
    state.publish_runtime_snapshot("reconnected", None);
    assert_eq!(reader.read().unwrap().active_insights[0].symbol, "EURUSD");
    state.insights.remove_insight(&id);
    state.publish_runtime_snapshot("reconnected", None);
    assert!(reader.read().unwrap().active_insights.is_empty());
}
