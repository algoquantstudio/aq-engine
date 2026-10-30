//! No external brokers or network requests. The current-thread runtime does
//! not poll refresh workers until we stop them at the end of each fixture.
use super::*;
use crate::core::broker::{
    data_feeds::yahoo::YahooFinanceDataFeed,
    snapshots::BrokerSnapshotCache,
    types::{AccountType, Bar},
};
use crate::core::pipeline::{InsightPipe, InsightPipeResult};
use crate::core::utils::timeframe::TimeFrameUnit;
use std::sync::Mutex;

struct Noop;
impl Strategy for Noop {
    fn on_start(&mut self, _: &mut dyn StrategyContext) {}
    fn init(&mut self, _: &mut dyn StrategyContext, _: &Asset) {}
    fn universe(&self, _: &mut dyn StrategyContext) -> HashSet<String> {
        HashSet::new()
    }
    fn on_bar(&mut self, _: &mut dyn StrategyContext, _: &str, _: &BarData) {}
    fn generate_insights(&mut self, _: &mut dyn StrategyContext, _: &str) {}
    fn insight_pipeline(&mut self, _: &mut dyn StrategyContext, _: &Insight) {}
    fn on_teardown(&mut self, _: &mut dyn StrategyContext) {}
}
type Owner = StrategyState<Noop, PaperBroker, YahooFinanceDataFeed>;
type Seen = Arc<Mutex<Vec<(Uuid, f64)>>>;
struct Probe {
    seen: Seen,
    stop: bool,
}
impl InsightPipe for Probe {
    fn version(&self) -> &str {
        "test"
    }
    fn run(&mut self, ctx: &mut dyn StrategyContext, row: &mut Insight) -> InsightPipeResult {
        self.seen
            .lock()
            .unwrap()
            .push((row.insight_id, ctx.latest_quote(&row.symbol).unwrap().bid));
        if self.stop {
            ctx.shutdown();
        }
        InsightPipeResult::new(true, true, None, self.name().into())
    }
}
fn row(id: u128, symbol: &str) -> Insight {
    Insight {
        insight_id: Uuid::from_u128(id),
        symbol: symbol.into(),
        ..Default::default()
    }
}
fn quote(symbol: &str, bid: f64) -> Quote {
    Quote {
        profit_model: None,
        symbol: symbol.into(),
        bid,
        ask: bid + 1.0,
        bid_size: 1.0,
        ask_size: 1.0,
        last: Some(bid),
        last_size: None,
        timestamp: chrono::Utc::now(),
    }
}
async fn owner(count: u128, stop: bool) -> (Owner, Seen) {
    let mut state = StrategyState::new(
        "quotes".into(),
        "test".into(),
        Noop,
        UnifiedBroker::new(
            PaperBroker::new(AccountType::Paper, 100_000.0, 1),
            YahooFinanceDataFeed::new(),
        ),
        StrategyMode::Live,
        TimeFrame::new(1, TimeFrameUnit::Minute),
    );
    state.status = StrategyStatus::Running;
    let account = state.broker.get_account().await.unwrap();
    state
        .broker
        .start_snapshot_reads(
            account,
            Instant::now(),
            &[],
            Default::default(),
            Default::default(),
        )
        .unwrap();
    state
        .broker
        .set_snapshot_symbols(&["ETHUSD".into(), "SOLUSD".into()])
        .unwrap();
    state.execution_quotes.configure(["ETHUSD".into()]);
    let seen = Seen::default();
    state.add_pipe(
        WrappedInsightPipe::builder(Box::new(Probe {
            seen: seen.clone(),
            stop,
        }))
        .build(),
    );
    for id in 1..=count {
        state.insights.add_insight(row(id, "ETHUSD"));
    }
    state.insights.add_insight(row(count + 1, "SOLUSD"));
    (state, seen)
}
fn event(state: &Owner, bid: f64) -> QuoteEvent {
    QuoteEvent {
        quote: quote("ETHUSD", bid),
        source: state
            .broker
            .snapshot_dependency_revision(&SnapshotKind::Quote {
                symbol: "ETHUSD".into(),
            })
            .unwrap(),
        received: Instant::now(),
        enabled: Arc::new(AtomicBool::new(true)),
    }
}
fn drain(state: &mut Owner, sweep: &mut QuoteSweep) {
    for _ in 0..1000 {
        if !sweep.is_pending() {
            return;
        }
        assert!(state.advance_quote_sweep(sweep).unwrap() <= 256);
    }
    panic!("quote sweep did not converge");
}

#[test]
fn native_quote_symbol_index_tracks_moves_terminal_rows_nested_materialization_and_deletion() {
    let mut rows = InsightCollection::new();
    for id in 1..=1000 {
        rows.add_insight(row(id, if id % 2 == 0 { "ETHUSD" } else { "SOLUSD" }));
    }
    let upper = rows.active_symbol_upper_bound("ETHUSD").unwrap();
    let first = rows.active_symbol_page("ETHUSD", None, upper, 1000);
    assert_eq!(first.len(), 256);
    assert!(first.iter().all(|id| id.as_u128() % 2 == 0));
    rows.get_mut(&Uuid::from_u128(1000)).unwrap().symbol = "SOLUSD".into();
    rows.refresh_runtime_tracking(&Uuid::from_u128(1000));
    rows.get_mut(&Uuid::from_u128(998)).unwrap().state = InsightState::Closed;
    rows.refresh_runtime_tracking(&Uuid::from_u128(998));
    rows.remove_insight(&Uuid::from_u128(996));
    assert_eq!(
        rows.active_symbol_upper_bound("ETHUSD"),
        Some(Uuid::from_u128(994))
    );
    assert_eq!(rows.active_symbol_upper_bound("SOLUSD"), Some(upper));
    let mut parent = row(2000, "BTCUSD");
    parent.children.push(row(2001, "BTCUSD"));
    rows.add_insight(parent);
    assert_eq!(
        rows.active_symbol_page("BTCUSD", None, Uuid::from_u128(3000), 256)
            .len(),
        1
    );
    rows.add_insight(row(2001, "BTCUSD"));
    assert_eq!(
        rows.active_symbol_page("BTCUSD", None, Uuid::from_u128(3000), 256)
            .len(),
        2
    );
    let moved = rows.take_for_update(&Uuid::from_u128(2001)).unwrap();
    rows.restore_after_update(moved, false);
    rows.remove_insight(&Uuid::from_u128(2000));
    rows.remove_insight(&Uuid::from_u128(2001));
    assert_eq!(rows.active_symbol_upper_bound("BTCUSD"), None);
}

#[tokio::test]
async fn native_quote_thousand_rows_keep_crossing_price_and_ignore_other_symbols_and_newer_poll() {
    let (mut state, seen) = owner(1000, false).await;
    let first = event(&state, 90.0);
    let second = event(&state, 110.0);
    let mut sweep = QuoteSweep::default();
    sweep.begin(first, &state.insights, &mut state.execution_quotes);
    assert_eq!(
        state
            .advance_quote_sweep_with_budget(&mut sweep, Duration::ZERO)
            .unwrap(),
        1
    );
    state
        .broker
        .live_snapshot_cache()
        .unwrap()
        .seed_quote_for_test(quote("ETHUSD", 200.0));
    assert_eq!(state.latest_quote("ETHUSD").unwrap().bid, 90.0);
    drain(&mut state, &mut sweep);
    assert_eq!(
        *seen.lock().unwrap(),
        (1..=1000)
            .map(|id| (Uuid::from_u128(id), 90.0))
            .collect::<Vec<_>>()
    );
    seen.lock().unwrap().clear();
    sweep.begin(second, &state.insights, &mut state.execution_quotes);
    drain(&mut state, &mut sweep);
    assert_eq!(
        *seen.lock().unwrap(),
        (1..=1000)
            .map(|id| (Uuid::from_u128(id), 110.0))
            .collect::<Vec<_>>()
    );
    state.broker.stop_snapshot_reads().await;
}

#[tokio::test]
async fn native_quote_stop_and_membership_mutations_between_pages() {
    let (mut state, seen) = owner(500, false).await;
    let mut sweep = QuoteSweep::default();
    sweep.begin(
        event(&state, 90.0),
        &state.insights,
        &mut state.execution_quotes,
    );
    state
        .advance_quote_sweep_with_budget(&mut sweep, Duration::ZERO)
        .unwrap();
    state.insights.remove_insight(&Uuid::from_u128(100));
    state.insights.get_mut(&Uuid::from_u128(200)).unwrap().state = InsightState::Cancelled;
    state
        .insights
        .refresh_runtime_tracking(&Uuid::from_u128(200));
    drain(&mut state, &mut sweep);
    assert_eq!(seen.lock().unwrap().len(), 498);
    state.broker.stop_snapshot_reads().await;

    let (mut state, seen) = owner(500, true).await;
    sweep.begin(
        event(&state, 90.0),
        &state.insights,
        &mut state.execution_quotes,
    );
    drain(&mut state, &mut sweep);
    assert_eq!(seen.lock().unwrap().len(), 1);
    state.broker.stop_snapshot_reads().await;
}

#[tokio::test]
async fn native_quote_fences_invalidation_resubscription_and_replacement_cache() {
    let (mut state, seen) = owner(2, false).await;
    assert!(state.latest_quote("ETHUSD").is_err());
    let mut sweep = QuoteSweep::default();
    for change in 0..3 {
        let old = event(&state, 90.0);
        match change {
            0 => state.broker.invalidate_snapshot_reads(),
            1 => {
                state.broker.set_snapshot_symbols(&[]).unwrap();
                state
                    .broker
                    .set_snapshot_symbols(&["ETHUSD".into()])
                    .unwrap();
            }
            _ => old.enabled.store(false, Ordering::Release),
        }
        sweep.begin(old, &state.insights, &mut state.execution_quotes);
        assert_eq!(state.advance_quote_sweep(&mut sweep).unwrap(), 0);
        assert!(state.latest_quote("ETHUSD").is_err());
    }
    let old = event(&state, 90.0);
    state.broker.stop_snapshot_reads().await;
    // Different StrategyState/feed cache with the same symbol and account name.
    let (mut replacement, _) = owner(2, false).await;
    sweep.begin(
        old,
        &replacement.insights,
        &mut replacement.execution_quotes,
    );
    assert_eq!(replacement.advance_quote_sweep(&mut sweep).unwrap(), 0);
    assert!(replacement.latest_quote("ETHUSD").is_err());
    assert!(seen.lock().unwrap().is_empty());
    replacement.broker.stop_snapshot_reads().await;
}

#[tokio::test]
async fn native_quote_backlog_defers_stale_prices_without_stopping_and_recovers() {
    let (mut state, seen) = owner(2, false).await;
    let mut old = event(&state, 90.0);
    old.received = Instant::now() - MAX_QUEUE_AGE - Duration::from_secs(1);
    let mut sweep = QuoteSweep::default();
    sweep.begin(old, &state.insights, &mut state.execution_quotes);
    assert_eq!(state.advance_quote_sweep(&mut sweep).unwrap(), 0);
    assert!(!sweep.is_pending());
    assert!(seen.lock().unwrap().is_empty());
    assert!(state.latest_quote("ETHUSD").is_err());
    assert!(!*state.shutdown_tx.borrow());
    assert!(state.status.is_running());
    assert!(sweep.last_backlog_warning.is_some());
    let fresh = event(&state, 95.0);
    sweep.begin(fresh, &state.insights, &mut state.execution_quotes);
    state.advance_quote_sweep(&mut sweep).unwrap();
    assert_eq!(seen.lock().unwrap().len(), 2);
    assert!(!*state.shutdown_tx.borrow());
    state.broker.stop_snapshot_reads().await;
}

#[test]
fn native_quote_callback_is_bounded_ordered_validated_and_fenced() {
    let cache = Arc::new(BrokerSnapshotCache::new("account", Default::default()).unwrap());
    cache.set_symbols(&["ETHUSD".into()]).unwrap();
    let enabled = Arc::new(AtomicBool::new(true));
    let (tx, mut rx) = mpsc::channel(2);
    let (overflow, mut errors) = mpsc::channel(1);
    let callback = quote_callback("ETHUSD".into(), cache, enabled.clone(), tx, overflow);
    callback(quote("SOLUSD", 90.0));
    callback(quote("ETHUSD", f64::NAN));
    let first = quote("ETHUSD", 90.0);
    callback(first.clone());
    let mut older = first.clone();
    older.timestamp -= chrono::Duration::milliseconds(1);
    callback(older);
    let mut second = first.clone();
    second.bid = 89.0; // Equal timestamp, different price must not be coalesced.
    callback(second);
    callback(quote("ETHUSD", 95.0));
    assert_eq!(errors.try_recv().unwrap(), "native execution quote");
    assert_eq!(rx.try_recv().unwrap().quote.bid, 90.0);
    assert_eq!(rx.try_recv().unwrap().quote.bid, 89.0);
    assert!(rx.try_recv().is_err());
    enabled.store(false, Ordering::Release);
    callback(quote("ETHUSD", 100.0));
    assert!(rx.try_recv().is_err());
}

#[test]
fn native_quote_old_lease_is_fenced_after_symbol_removal_even_for_new_callbacks() {
    let cache = Arc::new(BrokerSnapshotCache::new("account", Default::default()).unwrap());
    cache.set_symbols(&["ETHUSD".into()]).unwrap();
    let (tx, mut rx) = mpsc::channel(2);
    let (overflow, _) = mpsc::channel(1);
    let callback = quote_callback(
        "ETHUSD".into(),
        cache.clone(),
        Arc::new(AtomicBool::new(true)),
        tx,
        overflow,
    );
    cache.set_symbols(&[]).unwrap();
    cache.set_symbols(&["ETHUSD".into()]).unwrap();
    callback(quote("ETHUSD", 90.0));
    assert!(rx.try_recv().is_err());
}

#[derive(Default)]
struct Feed {
    callbacks: Mutex<HashMap<String, crate::core::broker::QuoteCallback>>,
    fail_symbol: Option<String>,
}
impl DataFeed for Feed {
    async fn connect(&self) -> Result<bool, BrokerError> {
        Ok(true)
    }
    async fn disconnect(&self) -> Result<bool, BrokerError> {
        Ok(true)
    }
    fn is_connected(&self) -> bool {
        true
    }
    fn get_features(&self) -> &'static [DataFeedFeature] {
        &[DataFeedFeature::QuoteStream, DataFeedFeature::NativeBidAsk]
    }
}
impl DataProvider for Feed {
    async fn get_ticker_info(&self, _: &str) -> Result<Asset, BrokerError> {
        panic!("unexpected metadata RPC")
    }
    async fn get_history(
        &self,
        _: &str,
        _: chrono::DateTime<chrono::Utc>,
        _: chrono::DateTime<chrono::Utc>,
        _: TimeFrame,
    ) -> Result<DataFrame, BrokerError> {
        panic!("unexpected history RPC")
    }
    async fn get_latest_quote(&self, _: &str) -> Result<Quote, BrokerError> {
        panic!("unexpected quote RPC")
    }
    async fn get_latest_bar(&self, _: &str) -> Result<Bar, BrokerError> {
        panic!("unexpected bar RPC")
    }
    async fn subscribe_to_data_stream(
        &self,
        _: Vec<String>,
        _: TimeFrame,
        _: crate::core::broker::DataStreamMode,
        _: Arc<dyn Fn(Bar) + Send + Sync>,
    ) -> Result<(), BrokerError> {
        panic!("unexpected bar subscription")
    }
    async fn unsubscribe_from_data_stream(&self, _: Vec<String>) -> Result<(), BrokerError> {
        panic!("unexpected bar unsubscription")
    }
    async fn subscribe_to_quote_stream(
        &self,
        id: String,
        symbol: String,
        callback: crate::core::broker::QuoteCallback,
    ) -> Result<(), BrokerError> {
        self.callbacks.lock().unwrap().insert(id, callback);
        if self.fail_symbol.as_ref() == Some(&symbol) {
            return Err(BrokerError::DataFeedError("partial setup".into()));
        }
        Ok(())
    }
    async fn unsubscribe_from_quote_stream(&self, id: &str) -> Result<(), BrokerError> {
        self.callbacks.lock().unwrap().remove(id);
        Ok(())
    }
}
async fn broker(feed: Feed) -> UnifiedBroker<PaperBroker, Feed> {
    let broker = UnifiedBroker::new(PaperBroker::new(AccountType::Paper, 100_000.0, 1), feed);
    let account = broker.get_account().await.unwrap();
    broker
        .start_snapshot_reads(
            account,
            Instant::now(),
            &[],
            Default::default(),
            Default::default(),
        )
        .unwrap();
    broker
        .set_snapshot_symbols(&["ETHUSD".into(), "SOLUSD".into()])
        .unwrap();
    broker
}

#[tokio::test]
async fn native_quote_independent_owners_release_only_their_own_leases() {
    let broker = broker(Feed::default()).await;
    let (tx, _) = mpsc::channel(1);
    let mut first = LiveQuotes::start(&broker, ["ETHUSD".into()], tx.clone())
        .await
        .unwrap();
    let mut second = LiveQuotes::start(&broker, ["ETHUSD".into()], tx)
        .await
        .unwrap();
    assert_eq!(broker.data.callbacks.lock().unwrap().len(), 2);
    let callbacks: Vec<_> = broker
        .data
        .callbacks
        .lock()
        .unwrap()
        .values()
        .cloned()
        .collect();
    first.stop(&broker).await;
    assert_eq!(broker.data.callbacks.lock().unwrap().len(), 1);
    for callback in callbacks {
        callback(quote("ETHUSD", 90.0));
    }
    assert!(first.receiver.try_recv().is_err());
    assert_eq!(second.receiver.try_recv().unwrap().quote.bid, 90.0);
    second.stop(&broker).await;
    assert!(broker.data.callbacks.lock().unwrap().is_empty());
    broker.stop_snapshot_reads().await;
}

#[tokio::test]
async fn native_quote_partial_setup_failure_releases_all_installed_leases() {
    let broker = broker(Feed {
        fail_symbol: Some("SOLUSD".into()),
        ..Default::default()
    })
    .await;
    let (tx, _) = mpsc::channel(1);
    assert!(
        LiveQuotes::start(&broker, ["ETHUSD".into(), "SOLUSD".into()], tx)
            .await
            .is_err()
    );
    assert!(broker.data.callbacks.lock().unwrap().is_empty());
    broker.stop_snapshot_reads().await;
}

#[tokio::test]
async fn native_quote_synthetic_feed_uses_existing_fallback_without_subscribing() {
    let (state, _) = owner(0, false).await;
    let (tx, _) = mpsc::channel(1);
    let mut quotes = LiveQuotes::start(&state.broker, ["ETHUSD".into()], tx)
        .await
        .unwrap();
    assert!(!quotes.is_subscribed());
    assert!(quotes.receiver.recv().await.is_none());
    quotes.stop(&state.broker).await;
    state.broker.stop_snapshot_reads().await;
}
