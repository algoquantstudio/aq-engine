use super::*;
use crate::core::broker::{
    traits::{DataProvider, OrderManagementProvider},
    types::BrokerError,
};
use futures::{StreamExt, stream};
use std::{
    future::Future,
    sync::atomic::{AtomicU64, Ordering},
};

#[derive(Clone, Copy, Debug)]
pub struct SnapshotRefreshConfig {
    pub interval: Duration,
    pub request_timeout: Duration,
    pub quote_concurrency: usize,
}
impl Default for SnapshotRefreshConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(1),
            request_timeout: Duration::from_secs(5),
            quote_concurrency: 8,
        }
    }
}

#[derive(Default)]
struct RefreshCounts {
    order_failures: AtomicU64,
    account_failures: AtomicU64,
    position_failures: AtomicU64,
    quote_failures: AtomicU64,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SnapshotRefreshHealth {
    pub order_failures: u64,
    pub account_failures: u64,
    pub position_failures: u64,
    pub quote_failures: u64,
    pub exited_tasks: usize,
}

/// Four long-lived read-only tasks, regardless of insight count. Account and
/// position completion cannot block one another or the bounded quote fanout.
/// Explicit stop waits for task release; Drop aborts rather than detaching them.
pub struct SnapshotRefreshWorker {
    tasks: tokio::task::JoinSet<()>,
    cache: Arc<BrokerSnapshotCache>,
    counts: Arc<RefreshCounts>,
    identity: u64,
}

trait SnapshotSource: Send + Sync {
    fn orders(&self) -> impl Future<Output = Result<Vec<Order>, BrokerError>> + Send;
    fn account(&self) -> impl Future<Output = Result<Account, BrokerError>> + Send;
    fn positions(&self) -> impl Future<Output = Result<Vec<Position>, BrokerError>> + Send;
    fn quote(&self, symbol: &str) -> impl Future<Output = Result<Quote, BrokerError>> + Send;
}
struct Sources<E, D> {
    execution: Arc<E>,
    data: Arc<D>,
}
impl<E: OrderManagementProvider, D: DataProvider> SnapshotSource for Sources<E, D> {
    async fn orders(&self) -> Result<Vec<Order>, BrokerError> {
        self.execution.get_orders().await
    }
    async fn account(&self) -> Result<Account, BrokerError> {
        self.execution.get_account().await
    }
    async fn positions(&self) -> Result<Vec<Position>, BrokerError> {
        self.execution.get_positions().await
    }
    async fn quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
        self.data.get_latest_quote(symbol).await
    }
}

impl SnapshotRefreshWorker {
    pub fn start<E: OrderManagementProvider + 'static, D: DataProvider + 'static>(
        execution: Arc<E>,
        data: Arc<D>,
        cache: Arc<BrokerSnapshotCache>,
        config: SnapshotRefreshConfig,
    ) -> Result<Self, SnapshotUnavailable> {
        Self::start_source(Arc::new(Sources { execution, data }), cache, config)
    }

    fn start_source<S: SnapshotSource + 'static>(
        source: Arc<S>,
        cache: Arc<BrokerSnapshotCache>,
        config: SnapshotRefreshConfig,
    ) -> Result<Self, SnapshotUnavailable> {
        if config.interval.is_zero()
            || config.request_timeout.is_zero()
            || config.quote_concurrency == 0
            || config.quote_concurrency > 32
            || tokio::runtime::Handle::try_current().is_err()
        {
            return Err(SnapshotUnavailable::Invalid);
        }
        let identity = cache.claim_worker()?;
        let mut tasks = tokio::task::JoinSet::new();
        let counts = Arc::new(RefreshCounts::default());
        let order_source = Arc::clone(&source);
        let order_cache = Arc::clone(&cache);
        let order_counts = Arc::clone(&counts);
        tasks.spawn(async move {
            let mut tick = interval(config.interval);
            loop {
                tick.tick().await;
                let Some(generation) = order_cache.request_generation(identity) else {
                    return;
                };
                // Avoid scanning unlimited paper/history books unnecessarily.
                if !order_cache.requires_order_capacity() {
                    continue;
                }
                let observed = Instant::now();
                match tokio::time::timeout(config.request_timeout, order_source.orders()).await {
                    Ok(Ok(orders)) => order_cache.publish_orders(generation, observed, orders),
                    _ => {
                        order_counts.order_failures.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
        let account_source = Arc::clone(&source);
        let account_cache = Arc::clone(&cache);
        let account_counts = Arc::clone(&counts);
        tasks.spawn(async move {
            let mut tick = interval(config.interval);
            loop {
                tick.tick().await;
                let Some(generation) = account_cache.request_generation(identity) else {
                    return;
                };
                let observed = Instant::now();
                match tokio::time::timeout(config.request_timeout, account_source.account()).await {
                    Ok(Ok(account)) => account_cache.publish_account(generation, observed, account),
                    _ => {
                        account_counts
                            .account_failures
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
        let position_source = Arc::clone(&source);
        let position_cache = Arc::clone(&cache);
        let position_counts = Arc::clone(&counts);
        tasks.spawn(async move {
            let mut tick = interval(config.interval);
            loop {
                tick.tick().await;
                let Some(generation) = position_cache.request_generation(identity) else {
                    return;
                };
                let observed = Instant::now();
                match tokio::time::timeout(config.request_timeout, position_source.positions())
                    .await
                {
                    Ok(Ok(positions)) => {
                        position_cache.publish_positions(generation, observed, positions)
                    }
                    _ => {
                        position_counts
                            .position_failures
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
        let quote_cache = Arc::clone(&cache);
        let quote_counts = Arc::clone(&counts);
        tasks.spawn(async move {
            let mut tick = interval(config.interval);
            loop {
                tick.tick().await;
                let Some(requests) = quote_cache.quote_requests(identity) else {
                    return;
                };
                let mut pending = stream::iter(requests)
                    .map(|(symbol, membership, generation)| {
                        let source = Arc::clone(&source);
                        let cache = Arc::clone(&quote_cache);
                        async move {
                            if !cache
                                .quote_request_current(identity, &symbol, membership, generation)
                            {
                                return None;
                            }
                            let observed = Instant::now();
                            let result =
                                tokio::time::timeout(config.request_timeout, source.quote(&symbol))
                                    .await;
                            Some((symbol, membership, generation, observed, result))
                        }
                    })
                    .buffer_unordered(config.quote_concurrency);
                let mut turn_started = Instant::now();
                let mut turn_count = 0;
                while let Some(result) = pending.next().await {
                    turn_count += 1;
                    if turn_count >= 256 || turn_started.elapsed() >= Duration::from_millis(5) {
                        tokio::task::yield_now().await;
                        turn_started = Instant::now();
                        turn_count = 0;
                    }
                    let Some((symbol, membership, generation, observed, result)) = result else {
                        continue;
                    };
                    match result {
                        Ok(Ok(quote)) => quote_cache
                            .publish_quote(&symbol, membership, generation, observed, quote),
                        _ => {
                            quote_counts.quote_failures.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        });
        Ok(Self {
            tasks,
            cache,
            counts,
            identity,
        })
    }

    pub fn health(&mut self) -> SnapshotRefreshHealth {
        while self.tasks.try_join_next().is_some() {}
        SnapshotRefreshHealth {
            order_failures: self.counts.order_failures.load(Ordering::Relaxed),
            account_failures: self.counts.account_failures.load(Ordering::Relaxed),
            position_failures: self.counts.position_failures.load(Ordering::Relaxed),
            quote_failures: self.counts.quote_failures.load(Ordering::Relaxed),
            exited_tasks: 4 - self.tasks.len(),
        }
    }

    /// Startup already awaited this account to establish the cache's identity.
    /// Preserve its dispatch timestamp rather than making it fresh a second time.
    pub(crate) fn seed_account(&self, account: Account, observed: Instant) {
        if let Some(generation) = self.cache.request_generation(self.identity) {
            self.cache.publish_account(generation, observed, account);
        }
    }

    pub async fn stop(mut self) {
        self.cache.release_worker(self.identity);
        self.tasks.shutdown().await;
    }
}
impl Drop for SnapshotRefreshWorker {
    fn drop(&mut self) {
        // Fence immediately before task cancellation; an in-flight response on
        // another executor thread cannot make retained values fresh after Drop.
        self.cache.release_worker(self.identity);
        self.tasks.abort_all();
    }
}
fn interval(period: Duration) -> tokio::time::Interval {
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick
}

#[cfg(test)]
mod tests;
