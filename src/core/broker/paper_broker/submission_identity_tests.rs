use super::*;

fn request(client: &str, insight: &str, strategy: &str, symbol: &str) -> BrokerOrderRequest {
    BrokerOrderRequest {
        client_order_id: client.into(),
        insight_id: insight.into(),
        strategy_type: strategy.into(),
        symbol: symbol.into(),
        quantity: Some(2.0),
        side: OrderSide::Buy,
        order_type: OrderType::Limit,
        order_class: OrderClass::Simple,
        limit_price: Some(50.0),
        stop_price: None,
        take_profit_levels: None,
        stop_loss_levels: None,
        trailing_stop_price: None,
    }
}

fn assert_identity(order: &Order, expected: &BrokerOrderRequest) {
    assert_eq!(order.order_id, expected.client_order_id);
    assert_eq!(order.insight_id.as_deref(), Some(expected.insight_id.as_str()));
    assert_eq!(order.strategy_type.as_deref(), Some(expected.strategy_type.as_str()));
    assert_eq!(order.asset.symbol, expected.symbol);
    assert_eq!(order.asset.name, expected.symbol);
}

#[tokio::test]
async fn accepted_submissions_preserve_distinct_custom_identities_and_move_owned_metadata() {
    for (client, insight, strategy, symbol) in [
        ("client/order::one", "insight::different", "Custom strategy / alpha", "BTC-USD.m"),
        ("request/trade:two", "signal/insight::two", "Custom -- Strategy \"Test\"", "ETH_USD/special"),
        ("client-with-spaces ", " insight-with-spaces", " Custom strategy ", "INDEX.symbol"),
    ] {
        let broker = PaperBroker::new(AccountType::Paper, 10_000.0, 1);
        let input = request(client, insight, strategy, symbol);
        let insight_buffer = input.insight_id.as_ptr();
        let strategy_buffer = input.strategy_type.as_ptr();
        let expected = input.clone();
        let order = broker.submit_order(input).await.unwrap();
        assert_identity(&order, &expected);
        assert_eq!(order.status, TradeUpdateEvent::Pending);
        {
            let stored = broker.orders.get(client).unwrap();
            assert_identity(&stored, &expected);
            assert_eq!(stored.insight_id.as_ref().unwrap().as_ptr(), insight_buffer);
            assert_eq!(stored.strategy_type.as_ref().unwrap().as_ptr(), strategy_buffer);
        }
        let events = broker.drain_broker_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event.1, TradeUpdateEvent::PendingNew);
        assert_identity(&events[0].event.0, &expected);
        assert_eq!(serde_json::to_value(&events[0].event.0).unwrap(), serde_json::to_value(&order).unwrap());
    }
}

#[tokio::test]
async fn duplicate_client_identity_does_not_replace_metadata_or_emit_another_event() {
    let broker = PaperBroker::new(AccountType::Paper, 10_000.0, 1);
    let first = request("same-client", "first-insight", "Original / strategy", "BTC-USD.m");
    let expected = first.clone();
    broker.submit_order(first).await.unwrap();
    let conflicting = request("same-client", "other-insight", "Different", "ETH-USD");
    let error = broker.submit_order(conflicting).await.unwrap_err();
    assert!(matches!(error, BrokerError::OrderError(message)
        if message == "Client order identity already exists; reconcile instead of resubmitting"));
    assert_identity(&broker.orders.get("same-client").unwrap(), &expected);
    let events = broker.drain_broker_events();
    assert_eq!(events.len(), 1);
    assert_identity(&events[0].event.0, &expected);
}

#[tokio::test]
async fn rejected_submission_keeps_custom_identity_in_response_and_terminal_evidence() {
    let broker = PaperBroker::new(AccountType::Paper, 1.0, 1);
    let input = request("client/rejected", "insight/rejected", "Custom / refused", "BTC-USD.m");
    let expected = input.clone();
    let order = broker.submit_order(input).await.unwrap();
    assert_eq!(order.status, TradeUpdateEvent::Rejected);
    assert!(order.rejection_reason.is_some());
    assert_identity(&order, &expected);
    assert!(broker.pending_orders.lock().is_empty());
    let events = broker.drain_broker_events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.1, TradeUpdateEvent::Rejected);
    assert_identity(&events[0].event.0, &expected);
    assert_eq!(serde_json::to_value(&events[0].event.0).unwrap(), serde_json::to_value(&order).unwrap());
}
