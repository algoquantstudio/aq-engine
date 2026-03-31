use crate::core::broker::DataStreamMode;
use crate::core::broker::types::{Bar, BarData, BrokerError};
use crate::core::utils::timeframe::TimeFrame;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

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
    /// Active subscription handles, keyed by symbol
    subscriptions: Arc<Mutex<HashMap<String, tokio::task::JoinHandle<()>>>>,
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
        // If already subscribed for this symbol, unsubscribe first
        self.unsubscribe(&symbol);

        let symbol_clone = symbol.clone();
        let _subscriptions = self.subscriptions.clone();

        let handle = tokio::spawn(async move {
            let mut last_bar_timestamp: Option<DateTime<Utc>> = None;

            loop {
                // Poll frequently (e.g. 10 seconds) to catch intrabar price movements
                let sleep_duration = std::time::Duration::from_secs(10);
                tokio::time::sleep(sleep_duration).await;

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
                                bars.iter().rev().find(|bar| match last_bar_timestamp {
                                    Some(last_ts) => bar.timestamp >= last_ts,
                                    None => true,
                                })
                            }
                            DataStreamMode::CompletedBar => bars.iter().rev().find(|bar| {
                                bar.timestamp < current_increment
                                    && match last_bar_timestamp {
                                        Some(last_ts) => bar.timestamp > last_ts,
                                        None => true,
                                    }
                            }),
                        };

                        if let Some(bar) = candidate {
                            last_bar_timestamp = Some(bar.timestamp);
                            on_bar(bar.clone());
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "[DataStreamManager] Error fetching bars for {}: {:?}",
                            symbol_clone, e
                        );
                        // On error, wait a bit before retrying
                        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                    }
                }
            }
        });

        // Store the handle
        let mut subs = self.subscriptions.lock().unwrap();
        subs.insert(symbol, handle);
    }

    /// Cancel the subscription for a symbol.
    pub fn unsubscribe(&self, symbol: &str) {
        let mut subs = self.subscriptions.lock().unwrap();
        if let Some(handle) = subs.remove(symbol) {
            handle.abort();
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
        subs.contains_key(symbol)
    }

    /// Get count of active subscriptions.
    pub fn active_count(&self) -> usize {
        let subs = self.subscriptions.lock().unwrap();
        subs.len()
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
