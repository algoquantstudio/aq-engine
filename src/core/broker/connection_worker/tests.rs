use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize};

struct Probe {
    connected: AtomicBool,
    mode: u8,
    attempts: parking_lot::Mutex<Vec<Instant>>,
    active: AtomicUsize,
    maximum: AtomicUsize,
    started: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}
impl Probe {
    fn new(mode: u8) -> Arc<Self> {
        Arc::new(Self {
            connected: AtomicBool::new(false),
            mode,
            attempts: Default::default(),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
            started: Default::default(),
            release: tokio::sync::Semaphore::new(0),
        })
    }
}
impl ConnectionSource for Arc<Probe> {
    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }
    async fn connect(&self) -> Result<bool, BrokerError> {
        self.attempts.lock().push(Instant::now());
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum.fetch_max(active, Ordering::SeqCst);
        struct Active<'a>(&'a AtomicUsize);
        impl Drop for Active<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        let _active = Active(&self.active);
        self.started.notify_one();
        match self.mode {
            1 => self.release.acquire().await.unwrap().forget(),
            2 => return Err(BrokerError::ConnectionError("fixture unavailable".into())),
            3 => panic!("fixture connection task panic"),
            _ => {}
        }
        self.connected.store(true, Ordering::SeqCst);
        Ok(true)
    }
}
fn cache() -> Arc<BrokerSnapshotCache> {
    Arc::new(BrokerSnapshotCache::new("account", Default::default()).unwrap())
}
fn config() -> ProviderConnectionConfig {
    ProviderConnectionConfig {
        check_interval: Duration::from_millis(2),
        attempt_timeout: Duration::from_millis(30),
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(40),
    }
}
async fn wait_until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn slow_broker_connection_does_not_block_datafeed_and_stop_releases_both() {
    let broker = Probe::new(1);
    let data = Probe::new(0);
    let worker =
        ProviderConnectionWorker::start_sources(broker.clone(), data.clone(), cache(), config())
            .unwrap();
    wait_until(|| broker.active.load(Ordering::SeqCst) == 1 && data.is_connected()).await;
    worker.stop().await;
    assert_eq!(broker.active.load(Ordering::SeqCst), 0);
    assert_eq!(data.active.load(Ordering::SeqCst), 0);
    assert_eq!(Arc::strong_count(&broker), 1);
    assert_eq!(Arc::strong_count(&data), 1);
    assert_eq!(data.attempts.lock().len(), 1);
}

#[tokio::test]
async fn connection_timeouts_do_not_overlap_attempts_or_grow_task_count() {
    let broker = Probe::new(1);
    let data = Probe::new(0);
    let mut worker =
        ProviderConnectionWorker::start_sources(broker.clone(), data.clone(), cache(), config())
            .unwrap();
    wait_until(|| broker.attempts.lock().len() >= 3).await;
    let health = worker.health();
    assert!(health.broker.timeouts >= 2);
    assert!(health.broker.failures >= 2);
    assert_eq!(health.datafeed.attempts, 1);
    assert_eq!(worker.tasks.len(), 2);
    assert_eq!(broker.maximum.load(Ordering::SeqCst), 1);
    worker.stop().await;
    assert_eq!(broker.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn failed_connections_use_bounded_exponential_backoff() {
    let broker = Probe::new(2);
    let data = Probe::new(0);
    let cfg = config();
    let worker =
        ProviderConnectionWorker::start_sources(broker.clone(), data.clone(), cache(), cfg)
            .unwrap();
    wait_until(|| broker.attempts.lock().len() >= 5).await;
    worker.stop().await;
    let times = broker.attempts.lock();
    for (pair, minimum) in times.windows(2).zip([10, 20, 40, 40]) {
        assert!(pair[1].duration_since(pair[0]) >= Duration::from_millis(minimum));
    }
    assert_eq!(broker.maximum.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn drop_aborts_pending_connections_instead_of_detaching() {
    let broker = Probe::new(1);
    let data = Probe::new(1);
    let worker =
        ProviderConnectionWorker::start_sources(broker.clone(), data.clone(), cache(), config())
            .unwrap();
    wait_until(|| {
        broker.active.load(Ordering::SeqCst) == 1 && data.active.load(Ordering::SeqCst) == 1
    })
    .await;
    drop(worker);
    wait_until(|| Arc::strong_count(&broker) == 1 && Arc::strong_count(&data) == 1).await;
    assert_eq!(broker.active.load(Ordering::SeqCst), 0);
    assert_eq!(data.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn task_failure_remains_visible_after_health_reaps_it() {
    let broker = Probe::new(3);
    let data = Probe::new(0);
    let mut worker =
        ProviderConnectionWorker::start_sources(broker, data.clone(), cache(), config()).unwrap();
    wait_until(|| worker.health().exited_tasks == 1).await;
    assert_eq!(worker.health().exited_tasks, 1);
    wait_until(|| data.is_connected()).await;
    worker.stop().await;
}

#[tokio::test]
async fn invalid_configuration_does_not_spawn_workers() {
    let broker = Probe::new(0);
    let data = Probe::new(0);
    assert!(
        ProviderConnectionWorker::start_sources(
            broker.clone(),
            data.clone(),
            cache(),
            ProviderConnectionConfig {
                check_interval: Duration::ZERO,
                ..config()
            }
        )
        .is_err()
    );
    assert_eq!(Arc::strong_count(&broker), 1);
    assert_eq!(Arc::strong_count(&data), 1);
    assert!(broker.attempts.lock().is_empty());
}

#[tokio::test]
async fn account_mismatch_stops_reconnects_without_clearing_the_scope_fence() {
    let broker = Probe::new(0);
    let data = Probe::new(0);
    let cache = cache();
    cache.seed_account_for_test(
        super::super::types::Account {
            account_id: "different-account".into(),
            account_type: super::super::types::AccountType::Paper,
            cash: 100.0,
            equity: 100.0,
            buying_power: 100.0,
            currency: "USD".into(),
            accrued_commission: 0.0,
            shorting_enabled: false,
            leverage: 1,
            max_total_orders: None,
        },
        std::time::Instant::now(),
    );
    let mut worker = ProviderConnectionWorker::start_sources(
        broker.clone(),
        data.clone(),
        cache.clone(),
        config(),
    )
    .unwrap();
    wait_until(|| worker.health().exited_tasks == 2).await;
    assert!(broker.attempts.lock().is_empty());
    assert!(data.attempts.lock().is_empty());
    assert_eq!(
        cache.account().unwrap_err(),
        SnapshotUnavailable::AccountChanged
    );
    worker.stop().await;
}
