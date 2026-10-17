use super::*;
use crate::core::broker::snapshots::tests::{account, quote};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Probe {
    limited: bool,
    order_calls: AtomicUsize,
    quote_calls_by_symbol: parking_lot::Mutex<std::collections::HashMap<String, usize>>,
    account_calls: AtomicUsize,
    position_calls: AtomicUsize,
    quote_calls: AtomicUsize,
    active_quotes: AtomicUsize,
    max_quotes: AtomicUsize,
    active_accounts: AtomicUsize,
    hold_account: bool,
    hold_quotes: bool,
    accounts: tokio::sync::Semaphore,
    quotes: tokio::sync::Semaphore,
}
impl Probe {
    fn new(hold_account: bool, hold_quotes: bool) -> Self {
        Self {
            limited: false,
            order_calls: AtomicUsize::new(0),
            quote_calls_by_symbol: Default::default(),
            account_calls: AtomicUsize::new(0),
            position_calls: AtomicUsize::new(0),
            quote_calls: AtomicUsize::new(0),
            active_quotes: AtomicUsize::new(0),
            max_quotes: AtomicUsize::new(0),
            active_accounts: AtomicUsize::new(0),
            hold_account,
            hold_quotes,
            accounts: tokio::sync::Semaphore::new(0),
            quotes: tokio::sync::Semaphore::new(0),
        }
    }
}
struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl SnapshotSource for Probe {
    async fn orders(&self) -> Result<Vec<Order>, BrokerError> {
        self.order_calls.fetch_add(1, Ordering::SeqCst);
        Ok(vec![])
    }
    async fn account(&self) -> Result<Account, BrokerError> {
        self.account_calls.fetch_add(1, Ordering::SeqCst);
        self.active_accounts.fetch_add(1, Ordering::SeqCst);
        let _active = Active(&self.active_accounts);
        if self.hold_account {
            self.accounts.acquire().await.unwrap().forget();
        }
        let mut value = account("account");
        value.max_total_orders = self.limited.then_some(4096);
        Ok(value)
    }
    async fn positions(&self) -> Result<Vec<Position>, BrokerError> {
        self.position_calls.fetch_add(1, Ordering::SeqCst);
        Ok(vec![])
    }
    async fn quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
        *self
            .quote_calls_by_symbol
            .lock()
            .entry(symbol.into())
            .or_default() += 1;
        self.quote_calls.fetch_add(1, Ordering::SeqCst);
        let active = self.active_quotes.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_quotes.fetch_max(active, Ordering::SeqCst);
        let _active = Active(&self.active_quotes);
        if self.hold_quotes {
            self.quotes.acquire().await.unwrap().forget();
        }
        Ok(quote(symbol))
    }
}
fn cache(symbols: &[&str]) -> Arc<BrokerSnapshotCache> {
    let cache = Arc::new(BrokerSnapshotCache::new("account", Default::default()).unwrap());
    cache
        .set_symbols(&symbols.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        .unwrap();
    cache
}
fn config() -> SnapshotRefreshConfig {
    SnapshotRefreshConfig {
        interval: Duration::from_secs(10),
        request_timeout: Duration::from_secs(5),
        quote_concurrency: 2,
    }
}

#[tokio::test]
async fn order_refresh_only_polls_accounts_with_reported_limits() {
    for limited in [false, true] {
        let mut probe = Probe::new(false, false);
        probe.limited = limited;
        let source = Arc::new(probe);
        let cache = Arc::new(BrokerSnapshotCache::new("account", Default::default()).unwrap());
        let worker = SnapshotRefreshWorker::start_source(
            source.clone(),
            cache.clone(),
            SnapshotRefreshConfig {
                interval: Duration::from_millis(10),
                ..config()
            },
        )
        .unwrap();
        until(|| source.account_calls.load(Ordering::SeqCst) >= 3).await;
        if limited {
            until(|| cache.order_capacity().is_ok()).await;
            assert_eq!(cache.order_capacity().unwrap().pending_orders, 0);
            assert!(source.order_calls.load(Ordering::SeqCst) > 0);
        } else {
            assert_eq!(source.order_calls.load(Ordering::SeqCst), 0);
        }
        worker.stop().await;
    }
}

#[tokio::test]
async fn aggregate_symbol_limits_enable_compact_order_capacity_refresh() {
    let source = Arc::new(Probe::new(false, false));
    let cache = Arc::new(BrokerSnapshotCache::new("account", Default::default()).unwrap());
    cache.set_capacity_symbols(&["SOLUSD".into()]).unwrap();
    let worker = SnapshotRefreshWorker::start_source(
        source.clone(),
        cache.clone(),
        SnapshotRefreshConfig {
            interval: Duration::from_millis(10),
            ..config()
        },
    )
    .unwrap();
    until(|| cache.order_capacity().is_ok()).await;
    assert!(source.order_calls.load(Ordering::SeqCst) > 0);
    assert_eq!(cache.order_capacity().unwrap().pending_orders, 0);
    worker.stop().await;
}

#[tokio::test]
async fn native_cache_worker_skips_live_symbol_and_resumes_after_lease_stop() {
    let source = Arc::new(Probe::new(false, false));
    let cache = cache(&["ETHUSD", "SOLUSD"]);
    let worker = SnapshotRefreshWorker::start_source(
        source.clone(),
        cache.clone(),
        SnapshotRefreshConfig {
            interval: Duration::from_millis(10),
            ..config()
        },
    )
    .unwrap();
    let active = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let revision = cache.dependency_revision(&SnapshotKind::Quote {
        symbol: "ETHUSD".into(),
    });
    cache.publish_native_quote(revision, Instant::now(), quote("ETHUSD"), &active);
    let calls = |symbol: &str| {
        source
            .quote_calls_by_symbol
            .lock()
            .get(symbol)
            .copied()
            .unwrap_or(0)
    };
    until(|| calls("SOLUSD") >= 3).await;
    assert_eq!(calls("ETHUSD"), 0);
    active.store(false, Ordering::Release);
    until(|| calls("ETHUSD") >= 1).await;
    worker.stop().await;
    assert_eq!(Arc::strong_count(&source), 1);
}
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn snapshot_refresh_slow_account_does_not_block_quotes_positions_or_cached_reads() {
    let source = Arc::new(Probe::new(true, false));
    let cache = cache(&["ETHUSD"]);
    let worker =
        SnapshotRefreshWorker::start_source(source.clone(), cache.clone(), config()).unwrap();
    until(|| cache.quote("ETHUSD").is_ok() && cache.state.read().positions.is_some()).await;
    assert_eq!(cache.account().unwrap_err(), SnapshotUnavailable::Missing);
    assert_eq!(
        cache.positions().unwrap_err(),
        SnapshotUnavailable::Missing,
        "positions wait for account identity"
    );
    for _ in 0..1_000 {
        assert!(cache.quote("ETHUSD").is_ok());
    }
    assert_eq!(source.quote_calls.load(Ordering::SeqCst), 1);
    source.accounts.add_permits(1);
    until(|| cache.account().is_ok() && cache.positions().is_ok()).await;
    assert_eq!(source.account_calls.load(Ordering::SeqCst), 1);
    worker.stop().await;
    assert_eq!(cache.account().unwrap_err(), SnapshotUnavailable::Missing);
    assert_eq!(
        Arc::strong_count(&source),
        1,
        "stopped tasks must release transport ownership"
    );
}

#[tokio::test]
async fn snapshot_refresh_quote_fanout_is_bounded_and_stop_cancels_only_read_tasks() {
    let source = Arc::new(Probe::new(true, true));
    let cache = cache(&["A", "B", "C", "D", "E"]);
    let worker =
        SnapshotRefreshWorker::start_source(source.clone(), cache.clone(), config()).unwrap();
    until(|| {
        source.active_quotes.load(Ordering::SeqCst) == 2
            && source.active_accounts.load(Ordering::SeqCst) == 1
    })
    .await;
    assert_eq!(source.quote_calls.load(Ordering::SeqCst), 2);
    assert_eq!(source.max_quotes.load(Ordering::SeqCst), 2);
    assert!(SnapshotRefreshWorker::start_source(source.clone(), cache.clone(), config()).is_err());
    tokio::time::timeout(Duration::from_secs(1), worker.stop())
        .await
        .unwrap();
    assert_eq!(source.active_quotes.load(Ordering::SeqCst), 0);
    assert_eq!(source.active_accounts.load(Ordering::SeqCst), 0);
    assert_eq!(
        source.quote_calls.load(Ordering::SeqCst),
        2,
        "stop must not dispatch the pending quote backlog"
    );
    assert_eq!(Arc::strong_count(&source), 1);
}

#[tokio::test]
async fn snapshot_refresh_drop_fences_late_results_and_releases_resources() {
    let source = Arc::new(Probe::new(true, true));
    let cache = cache(&["ETHUSD"]);
    let worker =
        SnapshotRefreshWorker::start_source(source.clone(), cache.clone(), config()).unwrap();
    until(|| source.active_quotes.load(Ordering::SeqCst) == 1).await;
    drop(worker);
    source.accounts.add_permits(1);
    source.quotes.add_permits(1);
    until(|| Arc::strong_count(&source) == 1).await;
    assert_eq!(
        cache.quote("ETHUSD").unwrap_err(),
        SnapshotUnavailable::Missing
    );
    assert_eq!(cache.account().unwrap_err(), SnapshotUnavailable::Missing);
    let next = SnapshotRefreshWorker::start_source(source.clone(), cache, config()).unwrap();
    next.stop().await;
}

#[tokio::test]
async fn snapshot_refresh_timeout_is_not_a_zero_account_or_an_unbounded_retry_queue() {
    let source = Arc::new(Probe::new(true, false));
    let cache = cache(&["ETHUSD"]);
    let mut config = config();
    config.request_timeout = Duration::from_millis(30);
    let mut worker =
        SnapshotRefreshWorker::start_source(source.clone(), cache.clone(), config).unwrap();
    until(|| worker.health().account_failures == 1).await;
    assert_eq!(cache.account().unwrap_err(), SnapshotUnavailable::Missing);
    assert!(cache.quote("ETHUSD").is_ok());
    assert_eq!(source.account_calls.load(Ordering::SeqCst), 1);
    assert_eq!(source.active_accounts.load(Ordering::SeqCst), 0);
    assert_eq!(worker.tasks.len(), 4);
    worker.stop().await;
}

#[tokio::test]
async fn snapshot_refresh_builtin_paper_adapter_uses_the_send_read_contract() {
    use crate::core::broker::{
        data_feeds::yahoo::YahooFinanceDataFeed, paper_broker::PaperBroker,
        traits::OrderManagementProvider, types::AccountType,
    };
    let execution = Arc::new(PaperBroker::new(AccountType::Paper, 100_000.0, 2));
    let expected = execution.get_account().await.unwrap();
    let cache =
        Arc::new(BrokerSnapshotCache::new(&expected.account_id, Default::default()).unwrap());
    // Empty quote membership: the Yahoo adapter makes no network requests.
    let worker = SnapshotRefreshWorker::start(
        execution.clone(),
        Arc::new(YahooFinanceDataFeed::new()),
        cache.clone(),
        config(),
    )
    .unwrap();
    until(|| cache.positions().is_ok()).await;
    assert_eq!(cache.account().unwrap().cash, expected.cash);
    worker.stop().await;
    assert_eq!(Arc::strong_count(&execution), 1);
}

#[tokio::test]
async fn snapshot_refresh_account_mismatch_stops_refresh_and_health_keeps_exit_count() {
    let source = Arc::new(Probe::new(false, false));
    let cache = Arc::new(BrokerSnapshotCache::new("another-account", Default::default()).unwrap());
    let mut config = config();
    config.interval = Duration::from_millis(5);
    let mut worker =
        SnapshotRefreshWorker::start_source(source.clone(), cache.clone(), config).unwrap();
    until(|| worker.health().exited_tasks == 4).await;
    assert_eq!(worker.health().exited_tasks, 4);
    assert_eq!(
        cache.account().unwrap_err(),
        SnapshotUnavailable::AccountChanged
    );
    assert_eq!(source.account_calls.load(Ordering::SeqCst), 1);
    worker.stop().await;
}
