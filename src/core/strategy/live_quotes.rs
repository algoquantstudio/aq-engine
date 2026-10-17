//! Native quotes are ordered execution inputs, not replaceable UI patches.
//! One event retains its price until its bounded, symbol-indexed pass finishes.
use super::*;
use crate::core::broker::{
    features::DataFeedFeature,
    snapshots::{SnapshotKind, SnapshotReadRevision},
    types::Quote,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

const QUEUE_CAPACITY: usize = 4096;
const MAX_QUEUE_AGE: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub(super) struct QuoteEvent {
    quote: Quote,
    source: SnapshotReadRevision,
    received: Instant,
    enabled: Arc<AtomicBool>,
}

#[derive(Default)]
pub(super) struct ExecutionQuoteReads {
    rows: HashMap<String, Option<QuoteEvent>>,
}

impl ExecutionQuoteReads {
    pub fn configure(&mut self, symbols: impl IntoIterator<Item = String>) {
        self.rows = symbols.into_iter().map(|symbol| (symbol, None)).collect();
    }

    pub fn read<E, D>(
        &self,
        symbol: &str,
        broker: &UnifiedBroker<E, D>,
    ) -> Option<Result<Quote, BrokerError>>
    where
        E: Broker + OrderManagementProvider,
        D: DataFeed + DataProvider,
    {
        let row = self.rows.get(symbol)?;
        let kind = SnapshotKind::Quote {
            symbol: symbol.into(),
        };
        let result = row
            .as_ref()
            .filter(|event| {
                event.enabled.load(Ordering::Acquire)
                    && broker.live_snapshot_cache().is_some_and(|cache| {
                        cache
                            .dependency_revision(&kind)
                            .same_source_epoch(event.source)
                            && cache.execution_quote_fresh(&event.quote, event.received)
                    })
            })
            .map(|event| event.quote.clone())
            .ok_or_else(|| BrokerError::SnapshotUnavailable {
                kind,
                reason: crate::core::broker::snapshots::SnapshotUnavailable::Missing,
            });
        Some(result)
    }
}

/// Scoped to this strategy's UnifiedBroker feed, never shared by symbol alone
/// across strategies/providers. Drop fences callbacks even on startup failure.
pub(super) struct LiveQuotes {
    ids: Vec<String>,
    enabled: Arc<AtomicBool>,
    pub receiver: mpsc::Receiver<QuoteEvent>,
}

impl Drop for LiveQuotes {
    fn drop(&mut self) {
        self.enabled.store(false, Ordering::Release);
    }
}

impl LiveQuotes {
    pub fn is_subscribed(&self) -> bool {
        !self.ids.is_empty()
    }

    pub fn fence(&mut self) {
        self.enabled.store(false, Ordering::Release);
        self.receiver.close();
    }
    pub async fn start<E, D>(
        broker: &UnifiedBroker<E, D>,
        symbols: impl IntoIterator<Item = String>,
        overflow: mpsc::Sender<&'static str>,
    ) -> Result<Self, BrokerError>
    where
        E: Broker + OrderManagementProvider,
        D: DataFeed + DataProvider,
    {
        let (sender, receiver) = mpsc::channel(QUEUE_CAPACITY);
        let mut owner = Self {
            ids: Vec::new(),
            // Do not accumulate pre-start quotes while subsequent subscription
            // RPCs are still installing the rest of the universe.
            enabled: Arc::new(AtomicBool::new(false)),
            receiver,
        };
        if !broker.has_feature(DataFeedFeature::QuoteStream)
            || !broker.has_feature(DataFeedFeature::NativeBidAsk)
        {
            return Ok(owner);
        }
        let cache = broker.live_snapshot_cache().ok_or_else(|| {
            BrokerError::DataFeedError(
                "Native execution quotes require an initialized snapshot cache".into(),
            )
        })?;
        for symbol in symbols {
            let id = format!("aqe-execution-{}", Uuid::new_v4());
            let callback = quote_callback(
                symbol.clone(),
                cache.clone(),
                owner.enabled.clone(),
                sender.clone(),
                overflow.clone(),
            );
            // Record the lease before awaiting: a provider may install its
            // callback and then fail. Cleanup must cover that partial success.
            owner.ids.push(id.clone());
            let result = tokio::time::timeout(
                Duration::from_secs(30),
                broker.subscribe_to_quote_stream(id, symbol, callback),
            )
            .await
            .unwrap_or_else(|_| {
                Err(BrokerError::DataFeedError(
                    "Native execution quote subscription timed out".into(),
                ))
            });
            if let Err(error) = result {
                owner.stop(broker).await;
                return Err(error);
            }
        }
        owner.enabled.store(true, Ordering::Release);
        Ok(owner)
    }

    pub async fn stop<E, D>(&mut self, broker: &UnifiedBroker<E, D>)
    where
        E: Broker + OrderManagementProvider,
        D: DataFeed + DataProvider,
    {
        self.fence();
        for id in self.ids.drain(..) {
            // Callback fencing is immediate; provider cleanup remains bounded.
            match tokio::time::timeout(
                Duration::from_secs(5),
                broker.unsubscribe_from_quote_stream(&id),
            )
            .await
            {
                Ok(Ok(())) => {}
                result => log::warn!("Execution quote lease cleanup failed for {id}: {result:?}"),
            }
        }
    }
}

fn quote_callback(
    symbol: String,
    cache: Arc<crate::core::broker::snapshots::BrokerSnapshotCache>,
    enabled: Arc<AtomicBool>,
    sender: mpsc::Sender<QuoteEvent>,
    overflow: mpsc::Sender<&'static str>,
) -> crate::core::broker::QuoteCallback {
    let last = parking_lot::Mutex::new(None);
    let kind = SnapshotKind::Quote {
        symbol: symbol.clone(),
    };
    let subscription = cache.dependency_revision(&kind);
    Arc::new(move |quote| {
        let received = Instant::now();
        if !enabled.load(Ordering::Acquire)
            || quote.symbol != symbol
            || !quote.valid_market_values()
            || !cache.execution_quote_fresh(&quote, received)
        {
            return;
        }
        // Serialize concurrent callbacks for this lease; reject provider time
        // regressions, but retain distinct updates with equal timestamps.
        let mut last = last.lock();
        if last.is_some_and(|timestamp| quote.timestamp < timestamp) {
            return;
        }
        *last = Some(quote.timestamp);
        let source = cache.dependency_revision(&kind);
        if !source.same_subscription(subscription) {
            return;
        }
        // Cache consumers can see the freshest observation immediately. The
        // execution owner still consumes every queued quote in order and never
        // reads ahead through this replaceable cache.
        cache.publish_native_quote(source, received, quote.clone(), &enabled);
        let event = QuoteEvent {
            quote,
            source,
            received,
            enabled: enabled.clone(),
        };
        match sender.try_send(event) {
            Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                let _ = overflow.try_send("native execution quote");
            }
        }
    })
}

#[derive(Default)]
pub(super) struct QuoteSweep {
    event: Option<QuoteEvent>,
    after: Option<Uuid>,
    upper: Option<Uuid>,
    last_backlog_warning: Option<Instant>,
}

impl QuoteSweep {
    fn clear(&mut self) {
        self.event = None;
        self.after = None;
        self.upper = None;
    }
    pub fn is_pending(&self) -> bool {
        self.event.is_some()
    }

    pub fn begin(
        &mut self,
        event: QuoteEvent,
        insights: &InsightCollection,
        reads: &mut ExecutionQuoteReads,
    ) {
        assert!(
            !self.is_pending(),
            "execution quote must finish before accepting the next"
        );
        if let Some(row) = reads.rows.get_mut(&event.quote.symbol) {
            *row = Some(event.clone());
        }
        self.upper = insights.active_symbol_upper_bound(&event.quote.symbol);
        self.after = None;
        self.event = self.upper.map(|_| event);
    }
}

impl<S, E, D> StrategyState<S, E, D>
where
    S: Strategy,
    E: Broker + OrderManagementProvider,
    D: DataFeed + DataProvider,
{
    pub(super) fn advance_quote_sweep(
        &mut self,
        sweep: &mut QuoteSweep,
    ) -> Result<usize, BrokerError> {
        self.advance_quote_sweep_with_budget(sweep, Duration::from_millis(5))
    }

    fn advance_quote_sweep_with_budget(
        &mut self,
        sweep: &mut QuoteSweep,
        budget: Duration,
    ) -> Result<usize, BrokerError> {
        let Some(event) = sweep.event.as_ref() else {
            return Ok(0);
        };
        let kind = SnapshotKind::Quote {
            symbol: event.quote.symbol.clone(),
        };
        if let Err(
            error @ BrokerError::SnapshotUnavailable {
                reason: crate::core::broker::snapshots::SnapshotUnavailable::AccountChanged,
                ..
            },
        ) = self.broker.account_snapshot()
        {
            sweep.clear();
            return Err(error);
        }
        if !event.enabled.load(Ordering::Acquire)
            || !self.status.is_running()
            || *self.shutdown_tx.borrow()
            || self.runtime_telemetry.shutdown_requested()
            || !self
                .broker
                .snapshot_dependency_revision(&kind)
                .is_some_and(|now| now.same_source_epoch(event.source))
        {
            sweep.clear();
            return Ok(0);
        }
        // Stale execution reads are already fenced by ExecutionQuoteReads.
        // Defer this evaluation until fresh data arrives, not a strategy-wide
        // teardown (which would close unrelated broker-protected positions).
        if event.received.elapsed() > MAX_QUEUE_AGE {
            if sweep
                .last_backlog_warning
                .is_none_or(|last| last.elapsed() >= Duration::from_secs(30))
            {
                log::warn!(
                    "Native execution quote delayed: symbol={} age_ms={}; deferring stale price evaluation until fresh quotes arrive; strategy remains running",
                    event.quote.symbol,
                    event.received.elapsed().as_millis()
                );
                sweep.last_backlog_warning = Some(Instant::now());
            }
            sweep.clear();
            return Ok(0);
        }
        let ids = self.insights.active_symbol_page(
            &event.quote.symbol,
            sweep.after,
            sweep.upper.unwrap(),
            256,
        );
        let (last, count) = self.run_insight_pipeline_budgeted(Some(ids), Some(budget));
        if count == 0 || last == sweep.upper {
            sweep.clear();
        } else {
            sweep.after = last;
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests;
