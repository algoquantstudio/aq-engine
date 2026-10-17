//! Owned connection maintenance, separate from trading decisions and mutations.
use super::{
    UnifiedBroker,
    snapshots::{BrokerSnapshotCache, SnapshotUnavailable},
    traits::{Broker, DataFeed, DataProvider, OrderManagementProvider},
    types::BrokerError,
};
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;

#[derive(Clone, Copy, Debug)]
pub struct ProviderConnectionConfig {
    pub check_interval: Duration,
    pub attempt_timeout: Duration,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}
impl Default for ProviderConnectionConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(1),
            attempt_timeout: Duration::from_secs(30),
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ConnectionLaneHealth {
    pub attempts: u64,
    pub failures: u64,
    pub timeouts: u64,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct ProviderConnectionHealth {
    pub broker: ConnectionLaneHealth,
    pub datafeed: ConnectionLaneHealth,
    pub exited_tasks: usize,
}
#[derive(Default)]
struct Counts {
    attempts: AtomicU64,
    failures: AtomicU64,
    timeouts: AtomicU64,
}
impl Counts {
    fn snapshot(&self) -> ConnectionLaneHealth {
        ConnectionLaneHealth {
            attempts: self.attempts.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
        }
    }
}

trait ConnectionSource: Send + Sync {
    fn is_connected(&self) -> bool;
    fn connect(&self) -> impl Future<Output = Result<bool, BrokerError>> + Send;
}
struct Execution<E>(Arc<E>);
impl<E: Broker> ConnectionSource for Execution<E> {
    fn is_connected(&self) -> bool {
        self.0.is_connected()
    }
    async fn connect(&self) -> Result<bool, BrokerError> {
        self.0.connect().await
    }
}
struct Feed<D>(Arc<D>);
impl<D: DataFeed> ConnectionSource for Feed<D> {
    fn is_connected(&self) -> bool {
        self.0.is_connected()
    }
    async fn connect(&self) -> Result<bool, BrokerError> {
        self.0.connect().await
    }
}

/// Exactly two tasks, one active attempt per adapter. Drop aborts owned reads;
/// explicit stop awaits them before the caller disconnects either transport.
pub struct ProviderConnectionWorker {
    tasks: tokio::task::JoinSet<()>,
    broker: Arc<Counts>,
    datafeed: Arc<Counts>,
    exited_tasks: usize,
}
impl ProviderConnectionWorker {
    pub fn start<E: Broker + 'static, D: DataFeed + 'static>(
        execution: Arc<E>,
        data: Arc<D>,
        cache: Arc<BrokerSnapshotCache>,
        config: ProviderConnectionConfig,
    ) -> Result<Self, BrokerError> {
        Self::start_sources(Execution(execution), Feed(data), cache, config)
    }

    fn start_sources<E: ConnectionSource + 'static, D: ConnectionSource + 'static>(
        execution: E,
        data: D,
        cache: Arc<BrokerSnapshotCache>,
        config: ProviderConnectionConfig,
    ) -> Result<Self, BrokerError> {
        if config.check_interval.is_zero()
            || config.attempt_timeout.is_zero()
            || config.initial_backoff.is_zero()
            || config.max_backoff < config.initial_backoff
            || tokio::runtime::Handle::try_current().is_err()
        {
            return Err(BrokerError::ConnectionError(
                "Invalid provider connection worker configuration".into(),
            ));
        }
        let broker = Arc::new(Counts::default());
        let datafeed = Arc::new(Counts::default());
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(maintain(
            execution,
            Arc::clone(&cache),
            Arc::clone(&broker),
            config,
            "execution broker",
        ));
        tasks.spawn(maintain(
            data,
            cache,
            Arc::clone(&datafeed),
            config,
            "datafeed",
        ));
        Ok(Self {
            tasks,
            broker,
            datafeed,
            exited_tasks: 0,
        })
    }

    pub fn health(&mut self) -> ProviderConnectionHealth {
        while let Some(result) = self.tasks.try_join_next() {
            self.exited_tasks += 1;
            if let Err(error) = result {
                log::warn!("Provider connection task exited: {error}");
            }
        }
        ProviderConnectionHealth {
            broker: self.broker.snapshot(),
            datafeed: self.datafeed.snapshot(),
            exited_tasks: self.exited_tasks,
        }
    }

    pub async fn stop(mut self) {
        self.tasks.shutdown().await;
    }
}

async fn maintain<S: ConnectionSource>(
    source: S,
    cache: Arc<BrokerSnapshotCache>,
    counts: Arc<Counts>,
    config: ProviderConnectionConfig,
    label: &'static str,
) {
    let mut tick = tokio::time::interval(config.check_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut next_attempt = Instant::now();
    let mut backoff = config.initial_backoff;
    loop {
        tick.tick().await;
        if matches!(cache.account(), Err(SnapshotUnavailable::AccountChanged)) {
            return;
        }
        if source.is_connected() {
            backoff = config.initial_backoff;
            next_attempt = Instant::now();
            continue;
        }
        if Instant::now() < next_attempt {
            continue;
        }
        cache.invalidate();
        counts.attempts.fetch_add(1, Ordering::Relaxed);
        let connected = match tokio::time::timeout(config.attempt_timeout, source.connect()).await {
            Ok(Ok(true)) if source.is_connected() => {
                log::info!("Live {label} reconnected");
                true
            }
            Ok(Ok(_)) => {
                log::warn!("Live {label} reconnect did not establish a connection");
                false
            }
            Ok(Err(error)) => {
                log::warn!("Live {label} reconnect failed: {error}");
                false
            }
            Err(_) => {
                counts.timeouts.fetch_add(1, Ordering::Relaxed);
                log::warn!("Live {label} reconnect attempt timed out");
                false
            }
        };
        if connected {
            backoff = config.initial_backoff;
        } else {
            counts.failures.fetch_add(1, Ordering::Relaxed);
        }
        next_attempt = Instant::now() + backoff;
        if !connected {
            backoff = backoff.saturating_mul(2).min(config.max_backoff);
        }
    }
}

impl<E: Broker + OrderManagementProvider, D: DataFeed + DataProvider> UnifiedBroker<E, D> {
    pub fn start_connection_worker(
        &self,
        config: ProviderConnectionConfig,
    ) -> Result<(), BrokerError>
    where
        E: 'static,
        D: 'static,
    {
        let mut slot = self.connection_worker.lock();
        if self.backtest_state.is_some() || slot.is_some() {
            return Err(BrokerError::ConnectionError(
                "Connection worker already installed or backtest execution".into(),
            ));
        }
        let cache = self.live_snapshot_cache().ok_or_else(|| {
            BrokerError::ConnectionError("Connection worker requires live account scope".into())
        })?;
        *slot = Some(ProviderConnectionWorker::start(
            Arc::clone(&self.execution),
            Arc::clone(&self.data),
            cache,
            config,
        )?);
        Ok(())
    }

    pub fn connection_worker_health(&self) -> Option<ProviderConnectionHealth> {
        self.connection_worker
            .lock()
            .as_mut()
            .map(ProviderConnectionWorker::health)
    }

    pub async fn stop_connection_worker(&self) {
        let worker = self.connection_worker.lock().take();
        if let Some(worker) = worker {
            worker.stop().await;
        }
    }
}

#[cfg(test)]
mod tests;
