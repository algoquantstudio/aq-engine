use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

async fn queued_order(broker: &PaperBroker) -> Order {
    let mut insight = crate::core::insight::Insight::default();
    insight.symbol = "GENERIC".into();
    insight.quantity = Some(2.0);
    insight.order_type = OrderType::Limit;
    insight.limit_price = Some(50.0);
    let order = broker.submit_order(insight.into()).await.unwrap();
    assert_eq!(broker.drain_broker_events().len(), 1);
    order
}

fn same_event(actual: &BrokerOrderEvent, expected: &BrokerOrderEvent) {
    assert_eq!(serde_json::to_value(actual).unwrap(), serde_json::to_value(expected).unwrap());
}

#[tokio::test]
async fn queued_event_without_subscribers_moves_its_order_buffer_into_the_drain() {
    let broker = PaperBroker::new(AccountType::Paper, 10_000.0, 1);
    let order = queued_order(&broker).await;
    broker.emit_trade_event(&order, TradeUpdateEvent::New);
    let (event_id, symbol_buffer) = {
        let queue = broker.trade_events.lock();
        let event = queue.front().unwrap();
        (event.event_id, event.event.0.asset.symbol.as_ptr())
    };
    let events = broker.drain_broker_events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_id, event_id);
    assert_eq!(events[0].event.0.asset.symbol.as_ptr(), symbol_buffer);
    assert_eq!(events[0].event.0.order_id, order.order_id);
    assert!(!broker.has_pending_trade_events());
}

#[tokio::test]
async fn callbacks_can_unsubscribe_and_drain_without_locks_or_identity_changes() {
    let broker = Arc::new(PaperBroker::new(AccountType::Paper, 10_000.0, 1));
    let mut order = queued_order(&broker).await;
    let seen = Arc::new(Mutex::new(Vec::<(u8, BrokerOrderEvent)>::new()));
    let first_seen = Arc::clone(&seen);
    let weak = Arc::downgrade(&broker);
    broker.subscribe_to_broker_event_stream(Arc::new(move |event| {
        let broker = weak.upgrade().unwrap();
        {
            let queue = broker.trade_events.try_lock().expect("callback must not hold queue lock");
            same_event(queue.back().unwrap(), &event);
        }
        assert!(broker.trade_stream_subscribers.try_lock().is_some(),
            "callback must not hold subscriber lock");
        futures::executor::block_on(broker.unsubscribe_from_trade_stream()).unwrap();
        first_seen.lock().push((1, event));
    })).await.unwrap();
    let second_seen = Arc::clone(&seen);
    let weak = Arc::downgrade(&broker);
    broker.subscribe_to_broker_event_stream(Arc::new(move |event| {
        // The first callback unsubscribed everyone, but this callback was already
        // in this event's snapshot and must still receive the identical evidence.
        let broker = weak.upgrade().unwrap();
        let drained = broker.drain_broker_events();
        assert_eq!(drained.len(), 1);
        same_event(&drained[0], &event);
        second_seen.lock().push((2, event));
    })).await.unwrap();
    order.status = TradeUpdateEvent::Filled;
    order.filled_qty = 2.0;
    let at = DateTime::from_timestamp(1_700_000_040, 0).unwrap();
    broker.set_time(at);
    let fill = broker.entry_execution(&order, 50.0, at);
    fill.validate().unwrap();
    broker.emit_execution_event(&order, TradeUpdateEvent::Filled, Some(fill.clone()));
    {
        let seen = seen.lock();
        assert_eq!(seen.len(), 2);
        assert_eq!((seen[0].0, seen[1].0), (1, 2));
        same_event(&seen[0].1, &seen[1].1);
        assert_eq!(seen[0].1.metadata.execution_fill.as_deref(), Some(&fill));
    }
    assert_eq!(broker.execution_history.lock().rows, 1, "validated history remains recorded");
    broker.emit_trade_event(&order, TradeUpdateEvent::New);
    assert_eq!(seen.lock().len(), 2, "self-unsubscribe affects the next event");
    assert_eq!(broker.drain_broker_events().len(), 1);
}

#[tokio::test]
async fn a_racing_drain_moves_owned_evidence_and_new_subscribers_start_with_next_snapshot() {
    let broker = Arc::new(PaperBroker::new(AccountType::Paper, 10_000.0, 1));
    let order = queued_order(&broker).await;
    let first = Arc::new(AtomicBool::new(true));
    let calls = Arc::new(AtomicUsize::new(0));
    let later_calls = Arc::new(AtomicUsize::new(0));
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
    let callback_event = Arc::new(Mutex::new(None::<BrokerOrderEvent>));
    let seen = Arc::clone(&callback_event);
    let count = Arc::clone(&calls);
    broker.subscribe_to_broker_event_stream(Arc::new(move |event| {
        count.fetch_add(1, Ordering::SeqCst);
        if first.swap(false, Ordering::SeqCst) {
            *seen.lock() = Some(event);
            entered_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv_timeout(Duration::from_secs(10)).unwrap();
        }
    })).await.unwrap();
    let emitting_broker = Arc::clone(&broker);
    let emitted_order = order.clone();
    let emitting = std::thread::spawn(move || {
        emitting_broker.emit_trade_event(&emitted_order, TradeUpdateEvent::New);
    });
    entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    // The emitter is in its first callback: the event is queued and its
    // subscriber snapshot is already fixed, with both broker locks released.
    let queued_symbol = {
        let queue = broker.trade_events.try_lock().unwrap();
        let event = queue.front().unwrap();
        event.event.0.asset.symbol.as_ptr()
    };
    let mut drained = broker.drain_broker_events();
    assert_eq!(drained.len(), 1);
    same_event(&drained[0], callback_event.lock().as_ref().unwrap());
    assert_eq!(drained[0].event.0.asset.symbol.as_ptr(), queued_symbol,
        "racing drain should move the queued owned envelope");
    drained[0].event.0.asset.symbol.push_str("-consumer-change");
    assert_eq!(callback_event.lock().as_ref().unwrap().event.0.asset.symbol, "GENERIC");
    let later = Arc::clone(&later_calls);
    broker.subscribe_to_broker_event_stream(Arc::new(move |_| {
        later.fetch_add(1, Ordering::SeqCst);
    })).await.unwrap();
    release_tx.send(()).unwrap();
    emitting.join().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(later_calls.load(Ordering::SeqCst), 0);
    broker.emit_trade_event(&order, TradeUpdateEvent::New);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(later_calls.load(Ordering::SeqCst), 1);
    assert_eq!(broker.drain_broker_events().len(), 1);
}

#[tokio::test]
async fn a_subscriber_registered_while_emission_waits_for_snapshot_receives_that_event() {
    let broker = Arc::new(PaperBroker::new(AccountType::Paper, 10_000.0, 1));
    let order = queued_order(&broker).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let callback_calls = Arc::clone(&calls);
    let callback: Arc<dyn Fn(BrokerOrderEvent) + Send + Sync> = Arc::new(move |_| {
        callback_calls.fetch_add(1, Ordering::SeqCst);
    });
    let mut registered = broker.trade_stream_subscribers.lock();
    let emitting_broker = Arc::clone(&broker);
    let emitted_order = order.clone();
    let emitting = std::thread::spawn(move || {
        emitting_broker.emit_trade_event(&emitted_order, TradeUpdateEvent::New);
    });
    // The held registry lock forces a stable wait after queue insertion.
    // No timeout is used as a substitute for observing that wait.
    let started = std::time::Instant::now();
    loop {
        if broker.trade_events.try_lock().is_none() {
            break;
        }
        assert!(started.elapsed() < Duration::from_secs(10), "emitter did not reach snapshot wait");
        std::thread::yield_now();
    }
    registered.push(callback);
    drop(registered);
    emitting.join().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(broker.drain_broker_events().len(), 1);
}

struct DrainWhenDropped {
    broker: std::sync::Weak<PaperBroker>,
    entered: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
    drained: Arc<Mutex<Vec<BrokerOrderEvent>>>,
}

// Receiver is Send but not Sync; the captured probe only needs an explicit
// mutex around itself for the callback's required Send + Sync contract.
impl Drop for DrainWhenDropped {
    fn drop(&mut self) {
        let broker = self.broker.upgrade().unwrap();
        assert!(broker.trade_stream_subscribers.try_lock().is_some(),
            "unsubscribe must release registry lock before destroying user captures");
        self.entered.send(()).unwrap();
        self.release.recv_timeout(Duration::from_secs(10)).unwrap();
        self.drained.lock().extend(broker.drain_broker_events());
    }
}

#[tokio::test]
async fn unsubscribe_drops_user_captures_without_blocking_emitters_and_allows_destructor_drain() {
    let broker = Arc::new(PaperBroker::new(AccountType::Paper, 10_000.0, 1));
    let order = queued_order(&broker).await;
    let drained = Arc::new(Mutex::new(Vec::new()));
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let probe = std::sync::Mutex::new(DrainWhenDropped {
        broker: Arc::downgrade(&broker), entered: entered_tx, release: release_rx,
        drained: Arc::clone(&drained),
    });
    broker.subscribe_to_broker_event_stream(Arc::new(move |_| {
        std::hint::black_box(&probe);
    })).await.unwrap();
    let unsubscribing_broker = Arc::clone(&broker);
    let unsubscribing = std::thread::spawn(move || {
        futures::executor::block_on(unsubscribing_broker.unsubscribe_from_trade_stream()).unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let emitting_broker = Arc::clone(&broker);
    let emitted_order = order.clone();
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let emitting = std::thread::spawn(move || {
        emitting_broker.emit_trade_event(&emitted_order, TradeUpdateEvent::New);
        finished_tx.send(()).unwrap();
    });
    // The capture's destructor is still running. It must hold neither registry
    // nor queue lock, otherwise this emit cannot reach its subscriber snapshot.
    finished_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    release_tx.send(()).unwrap();
    emitting.join().unwrap();
    unsubscribing.join().unwrap();
    let drained = drained.lock();
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].event.0.order_id, order.order_id);
    assert_eq!(drained[0].event.1, TradeUpdateEvent::New);
    assert!(!broker.has_pending_trade_events());
}
