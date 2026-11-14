use super::*;
use std::sync::atomic::Ordering;

fn request(id: &str, symbol: &str, bracket: bool) -> BrokerOrderRequest {
    BrokerOrderRequest {
        client_order_id: id.into(),
        insight_id: id.into(),
        strategy_type: "routing".into(),
        symbol: symbol.into(),
        quantity: Some(1.0),
        side: OrderSide::Buy,
        order_type: OrderType::Market,
        order_class: if bracket {
            OrderClass::Bracket
        } else {
            OrderClass::Simple
        },
        limit_price: None,
        stop_price: None,
        take_profit_levels: bracket.then(|| vec![120.0]),
        stop_loss_levels: bracket.then(|| vec![80.0]),
        trailing_stop_price: None,
    }
}

fn bar(symbol: &str, price: f64) -> Bar {
    Bar {
        symbol: symbol.into(),
        open: price,
        high: price,
        low: price,
        close: price,
        volume: 1000.0,
        timestamp: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
    }
}

fn step(broker: &PaperBroker, bars: Vec<Bar>) {
    let at = bars
        .first()
        .map(|bar| bar.timestamp)
        .unwrap_or_else(|| broker.get_current_time());
    broker.process_step(
        &bars
            .into_iter()
            .map(|bar| (bar.symbol.clone(), bar))
            .collect(),
        at,
    );
}

fn take_work(broker: &PaperBroker) -> (usize, usize, usize) {
    (
        broker.work_counts.pending.swap(0, Ordering::Relaxed),
        broker.work_counts.active.swap(0, Ordering::Relaxed),
        broker.work_counts.positions.swap(0, Ordering::Relaxed),
    )
}

fn assert_account_matches_positions(broker: &PaperBroker) {
    let account = broker.get_account_sync().unwrap();
    let mut pnl = 0.0;
    let mut margin = 0.0;
    for symbol in broker.positions.iter() {
        for row in symbol.value().iter() {
            pnl += row.unrealized_pnl;
            margin += row.margin_required.unwrap_or(row.cost_basis);
        }
    }
    assert!((account.equity - (account.cash + pnl + margin)).abs() < 1e-8);
}

#[test]
fn paper_symbol_routing_pending_promotion_preserves_survivors_and_cancellation() {
    let mut pending = PendingOrders::default();
    pending.insert("A", "old-survivor".into());
    pending.insert("A", "filled".into());
    pending.insert("A", "cancelled".into());
    assert!(!pending.is_indexed());
    assert_eq!(
        pending.retain_for_symbols(["A".to_owned()].iter(), |id| id != "filled"),
        3
    );

    // The surviving orders keep their admission priority when a second
    // symbol promotes the queue, even if bars arrive in the opposite order.
    pending.insert("B", "new-b".into());
    pending.insert("A", "new-a".into());
    assert!(pending.is_indexed());
    pending.remove_many(&HashSet::from(["cancelled".to_owned()]));
    let mut visited = Vec::new();
    assert_eq!(
        pending.retain_for_symbols(["B".to_owned()].iter(), |id| {
            visited.push(id.to_owned());
            true
        }),
        1,
        "a sparse bar must not visit surviving orders for other symbols"
    );
    assert_eq!(visited, ["new-b"]);
    visited.clear();
    pending.retain_for_symbols(["B".to_owned(), "A".to_owned()].iter(), |id| {
        visited.push(id.to_owned());
        false
    });
    assert_eq!(visited, ["old-survivor", "new-b", "new-a"]);
    assert!(pending.is_empty());
    assert!(!pending.is_indexed());

    pending.insert("C", "next-batch".into());
    assert!(!pending.is_indexed());
    assert_eq!(
        pending.retain_for_symbols(["A".to_owned()].iter(), |_| {
            panic!("a previous batch's symbol must not select the new batch")
        }),
        0
    );
    pending.remove_many(&HashSet::from(["next-batch".to_owned()]));
    assert!(pending.is_empty());
}

#[tokio::test]
async fn paper_symbol_routing_dense_fifo_preserves_fills_and_accounting() {
    let broker = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
    for n in 0..500 {
        broker
            .submit_order(request(&format!("dense-{n}"), "A", false))
            .await
            .unwrap();
    }
    assert!(!broker.pending_orders.lock().is_indexed());
    assert!(broker
        .submit_order(request("dense-0", "B", false))
        .await
        .is_err());
    step(&broker, vec![bar("B", 100.0)]);
    assert_eq!(take_work(&broker), (0, 0, 0));
    step(&broker, vec![bar("A", 100.0)]);
    assert_eq!(take_work(&broker), (500, 0, 500));
    let fills = broker
        .drain_broker_events()
        .into_iter()
        .filter(|event| event.event.1 == TradeUpdateEvent::Filled)
        .map(|event| event.event.0.order_id)
        .collect::<Vec<_>>();
    assert_eq!(
        fills,
        (0..500).map(|n| format!("dense-{n}")).collect::<Vec<_>>()
    );
    assert!(broker.pending_orders.lock().is_empty());
    assert_eq!(broker.get_account_sync().unwrap().cash, 50_000.0);
    assert_eq!(broker.get_account_sync().unwrap().equity, 100_000.0);
    assert_account_matches_positions(&broker);
}

#[tokio::test]
async fn paper_symbol_routing_dense_survivor_keeps_funds_priority_after_promotion() {
    let broker = PaperBroker::new(AccountType::Paper, 250.0, 1);
    let mut old_limit = request("old-limit", "A", false);
    old_limit.order_type = OrderType::Limit;
    old_limit.limit_price = Some(90.0);
    broker.submit_order(old_limit).await.unwrap();
    broker
        .submit_order(request("first-fill", "A", false))
        .await
        .unwrap();
    broker
        .submit_order(request("cancelled", "A", false))
        .await
        .unwrap();
    broker.cancel_order("cancelled").await.unwrap();

    // Release cancellation without an A bar; this must keep the limit order
    // and first market order in their original FIFO positions.
    broker.drain_broker_events();
    assert!(broker.orders.get("cancelled").is_none());
    step(&broker, vec![bar("A", 100.0)]);
    assert_eq!(
        broker.orders.get("old-limit").unwrap().status,
        TradeUpdateEvent::Pending
    );
    assert_eq!(
        broker.orders.get("first-fill").unwrap().status,
        TradeUpdateEvent::Filled
    );
    broker.drain_broker_events();
    broker
        .submit_order(request("new-b", "B", false))
        .await
        .unwrap();
    broker
        .submit_order(request("new-a", "A", false))
        .await
        .unwrap();
    assert!(broker.pending_orders.lock().is_indexed());

    step(&broker, vec![bar("B", 90.0), bar("A", 90.0)]);
    let outcomes = broker
        .drain_broker_events()
        .into_iter()
        .filter(|event| {
            matches!(
                event.event.1,
                TradeUpdateEvent::Filled | TradeUpdateEvent::Rejected
            )
        })
        .map(|event| (event.event.0.order_id, event.event.1))
        .collect::<Vec<_>>();
    assert_eq!(
        outcomes,
        [
            ("old-limit".into(), TradeUpdateEvent::Filled),
            ("new-b".into(), TradeUpdateEvent::Rejected),
            ("new-a".into(), TradeUpdateEvent::Rejected),
        ]
    );
    assert!(broker.pending_orders.lock().is_empty());
    assert_eq!(broker.get_account_sync().unwrap().cash, 60.0);
    assert_account_matches_positions(&broker);
}

#[tokio::test]
async fn paper_symbol_routing_sparse_bar_visits_only_matching_pending_active_and_positions() {
    let broker = PaperBroker::new(AccountType::Paper, 1_000_000.0, 1);
    for n in 0..1000 {
        broker
            .submit_order(request(&format!("open-{n}"), &format!("S{n}"), true))
            .await
            .unwrap();
    }
    step(
        &broker,
        (0..1000).map(|n| bar(&format!("S{n}"), 100.0)).collect(),
    );
    take_work(&broker);
    for n in 0..1000 {
        broker
            .submit_order(request(&format!("pending-{n}"), &format!("S{n}"), false))
            .await
            .unwrap();
    }
    broker
        .work_counts
        .active_reindex
        .store(0, Ordering::Relaxed);
    step(&broker, vec![bar("S500", 105.0)]);
    assert_eq!(take_work(&broker), (1, 1, 2));
    assert_eq!(
        broker.work_counts.active_reindex.load(Ordering::Relaxed),
        0,
        "steady membership needs no global rebuild"
    );
    assert_eq!(
        broker.orders.get("pending-499").unwrap().status,
        TradeUpdateEvent::Pending
    );
    assert_eq!(
        broker.orders.get("pending-500").unwrap().status,
        TradeUpdateEvent::Filled
    );
    assert_eq!(
        broker
            .positions
            .get("S499")
            .unwrap()
            .get("open-499")
            .unwrap()
            .current_price,
        100.0
    );
    assert_account_matches_positions(&broker);
}

#[tokio::test]
async fn paper_symbol_routing_preserves_global_admission_priority_and_cancel_cleanup() {
    let broker = PaperBroker::new(AccountType::Paper, 150.0, 1);
    broker
        .submit_order(request("first", "Z", false))
        .await
        .unwrap();
    broker
        .submit_order(request("cancelled", "A", false))
        .await
        .unwrap();
    broker.cancel_order("cancelled").await.unwrap();
    broker
        .submit_order(request("second", "A", false))
        .await
        .unwrap();
    step(&broker, vec![bar("A", 100.0), bar("Z", 100.0)]);
    assert_eq!(
        broker.orders.get("first").unwrap().status,
        TradeUpdateEvent::Filled
    );
    assert_eq!(
        broker.orders.get("second").unwrap().status,
        TradeUpdateEvent::Rejected
    );
    let events = broker.drain_broker_events();
    let fills: Vec<_> = events
        .iter()
        .filter(|event| event.event.1 == TradeUpdateEvent::Filled)
        .map(|event| event.event.0.order_id.as_str())
        .collect();
    assert_eq!(fills, ["first"]);
    assert!(broker.pending_orders.lock().is_empty());
    assert!(broker.orders.get("cancelled").is_none());
    assert!(broker.orders.get("second").is_none());
    assert_account_matches_positions(&broker);
}

#[tokio::test]
async fn paper_symbol_routing_preserves_bracket_priority_partial_exit_and_sparse_totals() {
    let broker = PaperBroker::new(AccountType::Paper, 10_000.0, 2);
    let mut a = request("a", "A", true);
    a.quantity = Some(2.0);
    broker.submit_order(a).await.unwrap();
    broker.submit_order(request("b", "B", false)).await.unwrap();
    step(&broker, vec![bar("A", 100.0), bar("B", 100.0)]);
    step(&broker, vec![bar("B", 110.0)]);
    assert_account_matches_positions(&broker);
    broker.close_position("a", 1.0, Some(105.0)).await.unwrap();
    broker.process_close_queue_at(
        &HashMap::from([("A".into(), bar("A", 100.0))]),
        broker.get_current_time(),
    );
    assert_account_matches_positions(&broker);
    assert_eq!(
        broker.positions.get("A").unwrap().get("a").unwrap().qty,
        1.0
    );
    let mut both = bar("A", 100.0);
    both.high = 130.0;
    both.low = 70.0;
    step(&broker, vec![both]);
    assert_eq!(
        broker.orders.get("a").unwrap().filled_price,
        Some(80.0),
        "SL still wins when both legs trigger"
    );
    assert!(broker.positions.get("A").is_none());
    assert!(!broker.position_totals.lock().contains_key("A"));
    assert_eq!(
        broker
            .positions
            .get("B")
            .unwrap()
            .get("b")
            .unwrap()
            .unrealized_pnl,
        10.0
    );
    assert_account_matches_positions(&broker);
    broker.drain_broker_events();
    assert!(broker.active_orders.is_empty());
}

#[test]
fn paper_symbol_routing_index_merges_sparse_groups_in_original_fifo_order() {
    let mut index = SymbolOrders::default();
    for n in 0..10_000 {
        index.insert(&format!("S{}", n % 100), format!("{n}"));
    }
    let symbols = ["S99".to_owned(), "S1".to_owned()];
    let selected = index.select(symbols.iter());
    let reference: Vec<_> = (0..10_000)
        .filter(|n| n % 100 == 1 || n % 100 == 99)
        .map(|n| (n, n.to_string()))
        .collect();
    assert_eq!(
        selected
            .iter()
            .map(|(ordinal, id)| (*ordinal, id.to_string()))
            .collect::<Vec<_>>(),
        reference
    );
    assert_eq!(selected.len(), 200);
}

#[tokio::test]
async fn paper_symbol_routing_bracket_closes_keep_canonical_iteration_order() {
    let broker = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
    for n in 0..40 {
        broker
            .submit_order(request(
                &format!("order-{n}"),
                if n % 2 == 0 { "A" } else { "B" },
                true,
            ))
            .await
            .unwrap();
    }
    step(&broker, vec![bar("A", 100.0), bar("B", 100.0)]);
    broker.drain_broker_events();
    let expected: Vec<_> = broker
        .active_orders
        .iter()
        .filter(|entry| entry.value() == "A")
        .map(|entry| entry.key().clone())
        .collect();
    step(&broker, vec![bar("A", 130.0)]);
    let actual: Vec<_> = broker
        .drain_broker_events()
        .into_iter()
        .filter(|event| event.event.1 == TradeUpdateEvent::Closed)
        .map(|event| event.event.0.order_id)
        .collect();
    assert_eq!(actual, expected);
    assert_eq!(broker.active_orders.len(), 20);
    assert_account_matches_positions(&broker);
}

#[tokio::test]
async fn paper_symbol_routing_churn_coalesces_active_rebuild_once_per_phase() {
    let broker = PaperBroker::new(AccountType::Paper, 1_000_000.0, 1);
    for n in 0..1000 {
        broker
            .submit_order(request(&format!("held-{n}"), "HELD", true))
            .await
            .unwrap();
    }
    step(&broker, vec![bar("HELD", 100.0)]);
    broker
        .work_counts
        .active_reindex
        .store(0, Ordering::Relaxed);
    for cycle in 0..5 {
        for n in 0..10 {
            broker
                .submit_order(request(&format!("churn-{cycle}-{n}"), "CHURN", true))
                .await
                .unwrap();
        }
        step(&broker, vec![bar("CHURN", 100.0)]);
        assert_eq!(
            broker.work_counts.active_reindex.swap(0, Ordering::Relaxed),
            1010,
            "ten fills coalesce into one canonical index rebuild"
        );
        step(&broker, vec![bar("CHURN", 130.0)]);
        assert_eq!(
            broker.work_counts.active_reindex.swap(0, Ordering::Relaxed),
            0,
            "closing uses the current index; rebuilding is deferred until the next phase"
        );
        broker.drain_broker_events();
        assert_eq!(broker.active_orders.len(), 1000);
        assert_account_matches_positions(&broker);
    }
}

#[tokio::test]
async fn paper_symbol_routing_public_amendments_and_cloned_broker_share_routes() {
    let broker = PaperBroker::new(AccountType::Paper, 10000.0, 1);
    let handle = broker.clone();
    let mut input = request("amended", "A", false);
    input.order_type = OrderType::Limit;
    input.limit_price = Some(50.0);
    handle.submit_order(input).await.unwrap();
    step(&broker, vec![bar("A", 100.0)]);
    assert_eq!(
        broker.orders.get("amended").unwrap().status,
        TradeUpdateEvent::Pending
    );
    handle
        .amend_entry_prices("amended", Some(105.0), None)
        .await
        .unwrap();
    step(&broker, vec![bar("A", 100.0)]);
    assert_eq!(
        broker.orders.get("amended").unwrap().status,
        TradeUpdateEvent::Filled
    );
    handle
        .close_position("amended", 0.0, Some(110.0))
        .await
        .unwrap();
    step(&broker, vec![bar("A", 110.0)]);
    assert!(broker.positions.is_empty());
    assert!(broker.position_totals.lock().is_empty());
    assert_eq!(broker.get_account_sync().unwrap().equity, 10010.0);
}
