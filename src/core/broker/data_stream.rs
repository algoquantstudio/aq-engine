use crate::core::broker::DataStreamMode;
use crate::core::broker::types::{Bar, BarData, BrokerError};
use crate::core::utils::timeframe::TimeFrame;
use chrono::{DateTime, Utc};
use log::warn;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const INTRABAR_POLL_INTERVAL: Duration = Duration::from_secs(10);
const ERROR_RETRY_INTERVAL: Duration = Duration::from_secs(10);
const COMPLETED_BAR_PUBLICATION_GRACE: Duration = Duration::from_secs(1);

/// Type alias for the async fetch function that data feeds provide.
/// Accepts (symbol, start, end, timeframe) and returns bars.
pub type FetchBarsFn = Arc<
    dyn Fn(
            String,
            DateTime<Utc>,
            DateTime<Utc>,
            TimeFrame,
        ) -> Pin<Box<dyn Future<Output = Result<BarData, BrokerError>> + Send>>
        + Send
        + Sync,
>;

/// Reusable subscription manager for polling-based data streams.
///
/// Any `DataProvider` implementation (Yahoo, Alpaca, IB, etc.) can compose
/// with this struct to manage live data subscriptions. The polling loop
/// uses `TimeFrame` utilities (`get_next_time_increment`, `add_time_increment`)
/// for smart interval scheduling.
pub struct DataStreamManager {
    /// Active subscription handles, keyed by symbol, timeframe, and mode.
    subscriptions: Arc<Mutex<HashMap<DataStreamSubscriptionKey, tokio::task::JoinHandle<()>>>>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DataStreamSubscriptionKey {
    symbol: String,
    time_frame: TimeFrame,
    mode: DataStreamMode,
}

impl DataStreamManager {
    pub fn new() -> Self {
        Self {
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Start a polling subscription for a symbol.
    ///
    /// - `symbol`: the ticker to subscribe to
    /// - `time_frame`: determines the polling interval via `get_next_time_increment()`
    /// - `on_bar`: callback invoked for each genuinely new bar
    /// - `fetch_fn`: the datafeed's bar-fetching function (e.g. Yahoo's `fetch_bars_raw`)
    pub fn subscribe(
        &self,
        symbol: String,
        time_frame: TimeFrame,
        mode: DataStreamMode,
        on_bar: Arc<dyn Fn(Bar) + Send + Sync>,
        fetch_fn: FetchBarsFn,
    ) {
        let key = DataStreamSubscriptionKey {
            symbol: symbol.clone(),
            time_frame,
            mode,
        };
        if let Some(handle) = self.subscriptions.lock().unwrap().remove(&key) {
            handle.abort();
        }

        let symbol_clone = symbol.clone();

        let handle = tokio::spawn(async move {
            let mut last_bar_timestamp: Option<DateTime<Utc>> = None;
            let mut retry_after_error = false;
            let mut bootstrap_completed_bar = mode == DataStreamMode::CompletedBar;

            loop {
                let sleep_duration = if retry_after_error {
                    ERROR_RETRY_INTERVAL
                } else if bootstrap_completed_bar {
                    bootstrap_completed_bar = false;
                    Duration::ZERO
                } else {
                    next_poll_delay(time_frame, mode, Utc::now())
                };
                tokio::time::sleep(sleep_duration).await;
                retry_after_error = false;

                // Fetch the latest bars (look back 2 intervals to catch edge cases)
                let fetch_end = Utc::now();
                let fetch_start = time_frame
                    .add_time_increment(fetch_end, -3)
                    .unwrap_or(fetch_end - chrono::Duration::hours(1));

                match (fetch_fn)(symbol_clone.clone(), fetch_start, fetch_end, time_frame).await {
                    Ok(bar_data) => {
                        let bars = match bar_data {
                            BarData::Bars(bars) => bars,
                            BarData::Frame(_) => {
                                // If we got a DataFrame, skip (shouldn't happen in polling mode)
                                continue;
                            }
                        };

                        let current_increment = time_frame.get_current_time_increment(Utc::now());

                        let candidate = match mode {
                            DataStreamMode::Intrabar => {
                                bars.into_iter().rev().find(|bar| match last_bar_timestamp {
                                    Some(last_ts) => bar.timestamp >= last_ts,
                                    None => true,
                                })
                            }
                            DataStreamMode::CompletedBar => bars.into_iter().rev().find(|bar| {
                                bar.timestamp < current_increment
                                    && match last_bar_timestamp {
                                        Some(last_ts) => bar.timestamp > last_ts,
                                        None => true,
                                    }
                            }),
                        };

                        if let Some(bar) = candidate {
                            last_bar_timestamp = Some(bar.timestamp);
                            on_bar(bar);
                        }
                    }
                    Err(e) => {
                        warn!(
                            "[DataStreamManager] Error fetching bars for {}: {:?}",
                            symbol_clone, e
                        );
                        retry_after_error = true;
                    }
                }
            }
        });

        // Store the handle
        let mut subs = self.subscriptions.lock().unwrap();
        subs.insert(key, handle);
    }

    /// Cancel all subscriptions for a symbol.
    pub fn unsubscribe(&self, symbol: &str) {
        let mut subs = self.subscriptions.lock().unwrap();
        let keys: Vec<DataStreamSubscriptionKey> = subs
            .keys()
            .filter(|key| key.symbol == symbol)
            .cloned()
            .collect();
        for key in keys {
            if let Some(handle) = subs.remove(&key) {
                handle.abort();
            }
        }
    }

    /// Cancel all active subscriptions.
    pub fn unsubscribe_all(&self) {
        let mut subs = self.subscriptions.lock().unwrap();
        for (_, handle) in subs.drain() {
            handle.abort();
        }
    }

    /// Check if a symbol is currently subscribed.
    pub fn is_subscribed(&self, symbol: &str) -> bool {
        let subs = self.subscriptions.lock().unwrap();
        subs.keys().any(|key| key.symbol == symbol)
    }

    /// Get count of active subscriptions.
    pub fn active_count(&self) -> usize {
        let subs = self.subscriptions.lock().unwrap();
        subs.len()
    }
}

fn next_poll_delay(time_frame: TimeFrame, mode: DataStreamMode, now: DateTime<Utc>) -> Duration {
    match mode {
        DataStreamMode::Intrabar => INTRABAR_POLL_INTERVAL,
        DataStreamMode::CompletedBar => {
            let until_boundary = (time_frame.get_next_time_increment(now) - now)
                .to_std()
                .unwrap_or_default();
            until_boundary + COMPLETED_BAR_PUBLICATION_GRACE
        }
    }
}

impl Default for DataStreamManager {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for DataStreamManager {
    fn drop(&mut self) {
        self.unsubscribe_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::utils::timeframe::TimeFrameUnit;

    #[tokio::test]
    async fn keeps_multiple_timeframes_for_same_symbol() {
        let manager = DataStreamManager::new();
        let fetch_fn: FetchBarsFn = Arc::new(|_symbol, _start, _end, _timeframe| {
            Box::pin(async { Ok(BarData::Bars(Vec::new())) })
        });
        let callback: Arc<dyn Fn(Bar) + Send + Sync> = Arc::new(|_bar| {});

        manager.subscribe(
            "BTC".to_string(),
            TimeFrame::new(1, TimeFrameUnit::Minute),
            DataStreamMode::CompletedBar,
            callback.clone(),
            fetch_fn.clone(),
        );
        manager.subscribe(
            "BTC".to_string(),
            TimeFrame::new(15, TimeFrameUnit::Minute),
            DataStreamMode::CompletedBar,
            callback,
            fetch_fn,
        );

        assert_eq!(manager.active_count(), 2);
        assert!(manager.is_subscribed("BTC"));
        manager.unsubscribe("BTC");
        assert_eq!(manager.active_count(), 0);
    }

    #[test]
    fn completed_bar_polling_aligns_to_timeframe_boundary() {
        let now = DateTime::parse_from_rfc3339("2025-01-01T12:00:05Z")
            .unwrap()
            .with_timezone(&Utc);
        let delay = next_poll_delay(
            TimeFrame::new(15, TimeFrameUnit::Minute),
            DataStreamMode::CompletedBar,
            now,
        );

        assert_eq!(delay, Duration::from_secs(14 * 60 + 56));
    }

    #[test]
    fn intrabar_polling_keeps_short_cadence() {
        let now = Utc::now();
        let delay = next_poll_delay(
            TimeFrame::new(15, TimeFrameUnit::Minute),
            DataStreamMode::Intrabar,
            now,
        );

        assert_eq!(delay, INTRABAR_POLL_INTERVAL);
    }
}
