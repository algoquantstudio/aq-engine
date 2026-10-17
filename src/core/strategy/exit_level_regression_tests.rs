#[test]
fn terminal_exit_fill_does_not_rewrite_configured_bracket() {
    for hit in ["take_profit", "stop_loss"] {
        let (mut state, execution) = trade_update_state();
        let mut insight = Insight::new(
            OrderSide::Sell,
            "AAPL".into(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            100,
            None,
        );
        insight
            .set_quantity(Some(1.0))
            .set_take_profit_levels(Some(vec![80.0]))
            .set_stop_loss(Some(110.0));
        insight.filled_price = Some(100.0);
        insight.order_id = Some("exit-level-regression".into());
        insight.state = InsightState::Filled;
        let id = *insight.insight_id();
        let mut order = test_order(
            "exit-level-regression",
            &insight,
            TradeUpdateEvent::Closed,
            OrderSide::Buy,
            Some(95.0),
        );
        order.legs = Some(
            serde_json::from_value(json!({hit: {
                "order_id":"exit-deal", "limit_price":95.0, "filled_price":95.0,
                "side":"Buy", "order_type":"Limit", "status":"Filled", "order_class":"Bracket",
                "created_at":0,"updated_at":0,"submitted_at":0,"filled_at":0
            }}))
            .unwrap(),
        );
        state.insights.add_insight(insight);
        execution.emit(order, TradeUpdateEvent::Closed);
        state.on_trade_update();
        let row = state.insights.get(&id).unwrap();
        assert_eq!(row.state, InsightState::Closed);
        assert_eq!(row.take_profit_levels(), Some(vec![80.0]));
        assert_eq!(row.stop_loss(), Some(110.0));
        assert_eq!(row.close_price, Some(95.0));
    }
}
