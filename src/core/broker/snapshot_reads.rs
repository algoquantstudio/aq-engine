//! The strategy-facing read boundary: no RPC or blocking executor fallback.
use super::{
    UnifiedBroker,
    snapshots::{
        BrokerSnapshotCache, SnapshotFreshness, SnapshotKind, SnapshotRefreshConfig,
        SnapshotRefreshHealth, SnapshotRefreshWorker, SnapshotUnavailable,
    },
    traits::{Broker, DataFeed, DataProvider, OrderManagementProvider},
    types::{Account, BrokerError, Position, Quote},
};
use std::{sync::Arc, time::Instant};

pub(super) struct LiveSnapshotReads {
    cache: Arc<BrokerSnapshotCache>,
    worker: SnapshotRefreshWorker,
}

impl<E: Broker + OrderManagementProvider, D: DataFeed + DataProvider> UnifiedBroker<E, D> {
    pub fn start_snapshot_reads(
        &self,
        account: Account,
        observed: Instant,
        symbols: &[String],
        freshness: SnapshotFreshness,
        config: SnapshotRefreshConfig,
    ) -> Result<(), BrokerError>
    where
        E: 'static,
        D: 'static,
    {
        let mut slot = self.snapshot_reads.lock();
        let error = |reason| BrokerError::SnapshotUnavailable {
            kind: SnapshotKind::Account,
            reason,
        };
        if self.backtest_state.is_some() || slot.is_some() {
            return Err(error(SnapshotUnavailable::Invalid));
        }
        let cache =
            Arc::new(BrokerSnapshotCache::new(&account.account_id, freshness).map_err(error)?);
        cache.set_symbols(symbols).map_err(error)?;
        let worker = SnapshotRefreshWorker::start(
            Arc::clone(&self.execution),
            Arc::clone(&self.data),
            Arc::clone(&cache),
            config,
        )
        .map_err(error)?;
        worker.seed_account(account, observed);
        // Invalid/stale initial identity cannot silently enable strategy startup.
        cache.account().map_err(error)?;
        *slot = Some(LiveSnapshotReads { cache, worker });
        Ok(())
    }

    pub(crate) fn live_snapshot_cache(&self) -> Option<Arc<BrokerSnapshotCache>> {
        self.snapshot_reads
            .lock()
            .as_ref()
            .map(|reads| Arc::clone(&reads.cache))
    }

    pub fn set_snapshot_symbols(&self, symbols: &[String]) -> Result<(), BrokerError> {
        if let Some(cache) = self.live_snapshot_cache() {
            cache
                .set_symbols(symbols)
                .map_err(|reason| BrokerError::SnapshotUnavailable {
                    kind: SnapshotKind::Quote {
                        symbol: String::new(),
                    },
                    reason,
                })?;
        }
        Ok(())
    }

    pub fn set_snapshot_capacity_symbols(&self, symbols: &[String]) -> Result<(), BrokerError> {
        if let Some(cache) = self.live_snapshot_cache() {
            cache.set_capacity_symbols(symbols).map_err(|reason| {
                BrokerError::SnapshotUnavailable {
                    kind: SnapshotKind::Positions,
                    reason,
                }
            })?;
        }
        Ok(())
    }

    pub fn invalidate_snapshot_reads(&self) {
        if let Some(cache) = self.live_snapshot_cache() {
            cache.invalidate();
        }
    }

    pub fn snapshot_refresh_health(&self) -> Option<SnapshotRefreshHealth> {
        self.snapshot_reads
            .lock()
            .as_mut()
            .map(|reads| reads.worker.health())
    }

    pub fn snapshot_notifier(&self) -> Option<Arc<tokio::sync::Notify>> {
        self.live_snapshot_cache().map(|cache| cache.notifier())
    }

    pub fn snapshot_revision(&self) -> u64 {
        self.live_snapshot_cache()
            .map_or(0, |cache| cache.revision())
    }

    pub(crate) fn snapshot_dependency_revision(
        &self,
        kind: &SnapshotKind,
    ) -> Option<super::snapshots::SnapshotReadRevision> {
        self.live_snapshot_cache()
            .map(|cache| cache.dependency_revision(kind))
    }

    pub async fn stop_snapshot_reads(&self) {
        // Never hold the synchronous ownership lock across task shutdown.
        let reads = self.snapshot_reads.lock().take();
        if let Some(reads) = reads {
            reads.worker.stop().await;
        }
    }

    pub fn account_snapshot(&self) -> Result<Arc<Account>, BrokerError> {
        let result = if self.backtest_state.is_some() {
            self.execution
                .get_cached_account()
                .map(Arc::new)
                .ok_or(SnapshotUnavailable::Missing)
        } else {
            self.live_snapshot_cache()
                .ok_or(SnapshotUnavailable::Missing)
                .and_then(|cache| cache.account())
        };
        result.map_err(|reason| BrokerError::SnapshotUnavailable {
            kind: SnapshotKind::Account,
            reason,
        })
    }

    pub fn positions_snapshot(&self) -> Result<Arc<[Position]>, BrokerError> {
        let result = if self.backtest_state.is_some() {
            self.execution
                .get_cached_positions()
                .map(Arc::from)
                .ok_or(SnapshotUnavailable::Missing)
        } else {
            self.live_snapshot_cache()
                .ok_or(SnapshotUnavailable::Missing)
                .and_then(|cache| cache.positions())
        };
        result.map_err(|reason| BrokerError::SnapshotUnavailable {
            kind: SnapshotKind::Positions,
            reason,
        })
    }

    pub fn quote_snapshot(&self, symbol: &str) -> Result<Arc<Quote>, BrokerError> {
        let result = if let Some(state) = &self.backtest_state {
            // Simulated time is authoritative. Never fall back to today's network quote.
            state
                .read()
                .get_current_bar(symbol)
                .map(|bar| {
                    Arc::new(Quote {
                        symbol: symbol.to_owned(),
                        bid: bar.close,
                        ask: bar.close,
                        bid_size: bar.volume,
                        ask_size: bar.volume,
                        last: Some(bar.close),
                        last_size: Some(bar.volume),
                        timestamp: bar.timestamp,
                    })
                })
                .ok_or(SnapshotUnavailable::Missing)
        } else {
            self.live_snapshot_cache()
                .ok_or(SnapshotUnavailable::Missing)
                .and_then(|cache| cache.quote(symbol))
        };
        result.map_err(|reason| BrokerError::SnapshotUnavailable {
            kind: SnapshotKind::Quote {
                symbol: symbol.to_owned(),
            },
            reason,
        })
    }
}
