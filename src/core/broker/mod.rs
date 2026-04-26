#![allow(async_fn_in_trait)]

pub mod backtest_state;
pub mod data_feeds;
pub mod data_stream;
pub mod mt5_bridge;
pub mod mt5_broker;
pub mod paper_broker;
pub mod types;

#[cfg(test)]
mod tests;

pub enum BrokerType {
    Paper(paper_broker::PaperBroker),
    Mt5(mt5_broker::Mt5Broker),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataStreamMode {
    Intrabar,
    CompletedBar,
}

use crate::core::utils::timeframe::TimeFrame;
use log::{debug, info};
use parking_lot::RwLock;
use polars::prelude::*;
use std::sync::Arc;
use types::{AccountType, Bar, BrokerError, Order};

// ─────────────────────── UnifiedBroker ───────────────────────

/// Composes an execution engine (E) and a data provider (D).
/// Optionally holds a shared `BacktestState` for backtesting scenarios.
pub struct UnifiedBroker<E, D>
where
    E: traits::Broker + traits::OrderManagementProvider,
    D: traits::DataFeed + traits::DataProvider,
{
    pub name: String,
    pub execution: E,
    pub data: D,
    pub backtest_state: Option<Arc<RwLock<backtest_state::BacktestState>>>,
}

impl<E, D> UnifiedBroker<E, D>
where
    E: traits::Broker + traits::OrderManagementProvider,
    D: traits::DataFeed + traits::DataProvider,
{
    pub fn new(execution: E, data: D) -> Self {
        Self {
            name: format!("{}", execution.get_name()),
            execution,
            data,
            backtest_state: None,
        }
    }

    /// Create a UnifiedBroker configured for backtesting with a shared BacktestState.
    pub fn new_backtest(execution: E, data: D) -> Self {
        let state = Arc::new(RwLock::new(backtest_state::BacktestState::new()));
        Self {
            name: format!("{}", execution.get_name()),
            execution,
            data,
            backtest_state: Some(state),
        }
    }

    // ─────────────── Unified connect/disconnect (both) ───────────────

    /// Connect both the execution broker and the data feed.
    pub async fn connect(&self) -> Result<bool, BrokerError> {
        self.execution.connect().await?;
        self.data.connect().await?;
        Ok(true)
    }

    /// Disconnect both the execution broker and the data feed.
    pub async fn disconnect(&self) -> Result<bool, BrokerError> {
        self.execution.disconnect().await?;
        self.data.disconnect().await?;
        Ok(true)
    }

    /// True when both execution broker and data feed are connected.
    pub fn is_connected(&self) -> bool {
        self.execution.is_connected() && self.data.is_connected()
    }

    // ─────────────── Individual connect/disconnect ───────────────

    /// Connect only the execution broker.
    pub async fn connect_broker(&self) -> Result<bool, BrokerError> {
        self.execution.connect().await
    }

    /// Disconnect only the execution broker.
    pub async fn disconnect_broker(&self) -> Result<bool, BrokerError> {
        self.execution.disconnect().await
    }

    /// Check if the execution broker is connected.
    pub fn is_broker_connected(&self) -> bool {
        self.execution.is_connected()
    }

    /// Connect only the data feed.
    pub async fn connect_datafeed(&self) -> Result<bool, BrokerError> {
        self.data.connect().await
    }

    /// Disconnect only the data feed.
    pub async fn disconnect_datafeed(&self) -> Result<bool, BrokerError> {
        self.data.disconnect().await
    }

    /// Check if the data feed is connected.
    pub fn is_datafeed_connected(&self) -> bool {
        self.data.is_connected()
    }
}

impl Default for UnifiedBroker<paper_broker::PaperBroker, data_feeds::yahoo::YahooFinanceDataFeed> {
    fn default() -> Self {
        let execution = paper_broker::PaperBroker::new(types::AccountType::Paper, 100_000.0, 4);
        let data = data_feeds::yahoo::YahooFinanceDataFeed::new();

        Self {
            name: "UnifiedBroker (Default)".to_string(),
            execution,
            data,
            backtest_state: None,
        }
    }
}

// ─────────────────────── Broker trait delegation ───────────────────────

impl<E, D> traits::Broker for UnifiedBroker<E, D>
where
    E: traits::Broker + traits::OrderManagementProvider,
    D: traits::DataFeed + traits::DataProvider,
{
    async fn connect(&self) -> Result<bool, BrokerError> {
        self.execution.connect().await
    }

    async fn disconnect(&self) -> Result<bool, BrokerError> {
        self.execution.disconnect().await
    }

    fn is_connected(&self) -> bool {
        self.execution.is_connected()
    }

    fn get_current_time(&self) -> chrono::DateTime<chrono::Utc> {
        self.execution.get_current_time()
    }

    fn get_name(&self) -> String {
        self.execution.get_name()
    }
    fn get_account_type(&self) -> Result<AccountType, BrokerError> {
        self.execution.get_account_type()
    }
}

// ─────────────── Synchronous order helpers (for StrategyContext) ───────────────

impl<E, D> UnifiedBroker<E, D>
where
    E: traits::Broker + traits::OrderManagementProvider,
    D: traits::DataFeed + traits::DataProvider,
{
    /// Synchronous cancel — safe when the underlying broker is PaperBroker
    /// (all operations are non-blocking).
    pub fn cancel_order_sync(&self, order_id: &str) -> Result<bool, BrokerError> {
        // PaperBroker's cancel_order doesn't actually await anything,
        // so blocking here is safe and zero-cost.
        futures::executor::block_on(self.execution.cancel_order(order_id))
    }

    /// Synchronous close — safe when the underlying broker is PaperBroker.
    pub fn close_position_sync(
        &self,
        order_id: &str,
        qty: f64,
        price: Option<f64>,
    ) -> Result<bool, BrokerError> {
        futures::executor::block_on(self.execution.close_position(order_id, qty, price))
    }

    pub fn get_position_sync(&self, symbol: &str) -> Result<types::Position, BrokerError> {
        futures::executor::block_on(self.execution.get_position(symbol))
    }

    pub fn get_positions_sync(&self) -> Result<Vec<types::Position>, BrokerError> {
        futures::executor::block_on(self.execution.get_positions())
    }
}

// ─────────────────────── OMS delegation ───────────────────────

impl<E, D> traits::OrderManagementProvider for UnifiedBroker<E, D>
where
    E: traits::Broker + traits::OrderManagementProvider,
    D: traits::DataFeed + traits::DataProvider,
{
    async fn submit_order(
        &self,
        insight: crate::core::insight::Insight,
    ) -> Result<Order, BrokerError> {
        self.execution.submit_order(insight).await
    }

    async fn cancel_order(&self, order_id: &str) -> Result<bool, BrokerError> {
        self.execution.cancel_order(order_id).await
    }

    async fn update_order(
        &self,
        order_id: &str,
        price: f64,
        qty: f64,
    ) -> Result<bool, BrokerError> {
        self.execution.update_order(order_id, price, qty).await
    }

    async fn close_position(
        &self,
        order_id: &str,
        qty: f64,
        price: Option<f64>,
    ) -> Result<bool, BrokerError> {
        self.execution.close_position(order_id, qty, price).await
    }

    async fn close_all_positions(&self) -> Result<bool, BrokerError> {
        self.execution.close_all_positions().await
    }

    async fn get_orders(&self) -> Result<Vec<Order>, BrokerError> {
        self.execution.get_orders().await
    }

    async fn get_order(&self, order_id: &str) -> Result<Order, BrokerError> {
        self.execution.get_order(order_id).await
    }

    async fn get_positions(&self) -> Result<Vec<types::Position>, BrokerError> {
        self.execution.get_positions().await
    }

    async fn get_position(&self, symbol: &str) -> Result<types::Position, BrokerError> {
        self.execution.get_position(symbol).await
    }

    async fn get_account(&self) -> Result<types::Account, BrokerError> {
        self.execution.get_account().await
    }

    fn drain_trade_events(&self) -> Vec<(types::Order, types::TradeUpdateEvent)> {
        self.execution.drain_trade_events()
    }

    async fn subscribe_to_trade_stream(
        &self,
        on_trade: Arc<dyn Fn((types::Order, types::TradeUpdateEvent)) + Send + Sync>,
    ) -> Result<(), BrokerError> {
        self.execution.subscribe_to_trade_stream(on_trade).await
    }

    async fn unsubscribe_from_trade_stream(&self) -> Result<(), BrokerError> {
        self.execution.unsubscribe_from_trade_stream().await
    }

    fn format_on_trade_update(
        &self,
        order: types::Order,
    ) -> (types::Order, types::TradeUpdateEvent) {
        self.execution.format_on_trade_update(order)
    }

    fn process_live_bar(&self, bar: &Bar) {
        self.execution.process_live_bar(bar)
    }
}

// ─────────────────────── DataFeed delegation ───────────────────────

impl<E, D> traits::DataFeed for UnifiedBroker<E, D>
where
    E: traits::Broker + traits::OrderManagementProvider,
    D: traits::DataFeed + traits::DataProvider,
{
    async fn connect(&self) -> Result<bool, BrokerError> {
        self.data.connect().await
    }

    async fn disconnect(&self) -> Result<bool, BrokerError> {
        self.data.disconnect().await
    }

    fn is_connected(&self) -> bool {
        self.data.is_connected()
    }
}

// ─────────────────────── DataProvider delegation ───────────────────────

impl<E, D> traits::DataProvider for UnifiedBroker<E, D>
where
    E: traits::Broker + traits::OrderManagementProvider,
    D: traits::DataFeed + traits::DataProvider,
{
    async fn get_ticker_info(&self, symbol: &str) -> Result<types::Asset, BrokerError> {
        self.data.get_ticker_info(symbol).await
    }

    async fn get_history(
        &self,
        symbol: &str,
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
        time_frame: TimeFrame,
    ) -> Result<DataFrame, BrokerError> {
        self.data.get_history(symbol, start, end, time_frame).await
    }

    async fn get_latest_quote(&self, symbol: &str) -> Result<types::Quote, BrokerError> {
        self.data.get_latest_quote(symbol).await
    }

    async fn get_latest_bar(&self, symbol: &str) -> Result<types::Bar, BrokerError> {
        self.data.get_latest_bar(symbol).await
    }

    async fn subscribe_to_data_stream(
        &self,
        symbols: Vec<String>,
        time_frame: TimeFrame,
        mode: DataStreamMode,
        on_bar: Arc<dyn Fn(types::Bar) + Send + Sync>,
    ) -> Result<(), BrokerError> {
        self.data
            .subscribe_to_data_stream(symbols, time_frame, mode, on_bar)
            .await
    }

    async fn unsubscribe_from_data_stream(&self, symbols: Vec<String>) -> Result<(), BrokerError> {
        self.data.unsubscribe_from_data_stream(symbols).await
    }
}

// ─────────────────────── Backtest-specific impl (PaperBroker + any DataFeed) ───────────────────────

impl<D> UnifiedBroker<paper_broker::PaperBroker, D>
where
    D: traits::DataFeed + traits::DataProvider,
{
    /// Load historical data from the data feed into the BacktestState.
    /// Creates the BacktestState if it doesn't already exist.
    pub async fn load_backtest_data(
        &mut self,
        symbols: &[String],
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
        time_frame: TimeFrame,
    ) -> Result<(), BrokerError> {
        // Ensure BacktestState exists
        if self.backtest_state.is_none() {
            self.backtest_state = Some(Arc::new(RwLock::new(backtest_state::BacktestState::new())));
        }

        let state = self.backtest_state.as_ref().unwrap().clone();

        for symbol in symbols {
            info!(
                "Loading backtest history for symbol={} timeframe={:?} start={} end={}",
                symbol, time_frame, start, end
            );
            // Fetch history via the data feed (works with any provider: Yahoo, MT5, IB, etc.)
            let df = self
                .data
                .get_history(symbol, start, end, time_frame.clone())
                .await?;
            debug!("Fetched dataframe for {} shape={:?}", symbol, df.shape());

            // Convert DataFrame to Vec<Bar> for fast indexed access in BacktestState
            let bars = Self::dataframe_to_bars(symbol, &df)?;
            info!("Loaded {} bars for {}", bars.len(), symbol);

            let mut state_guard = state.write();
            state_guard.load_bars(symbol.clone(), bars);

            // Set initial time from the first bar
            if let Some(first_bar) = state_guard
                .historical_bars
                .get(symbol)
                .and_then(|v| v.first())
            {
                if state_guard.previous_time.is_none() {
                    state_guard.current_time = first_bar.timestamp;
                }
            }
        }

        // Share the BacktestState with PaperBroker
        self.execution.set_backtest_state(state);

        Ok(())
    }

    /// Convert a Polars DataFrame (with columns: open, high, low, close, volume, timestamp)
    /// into a `Vec<Bar>` for efficient indexed access.
    fn dataframe_to_bars(symbol: &str, df: &DataFrame) -> Result<Vec<types::Bar>, BrokerError> {
        let len = df.height();
        let mut bars = Vec::with_capacity(len);

        let open = df
            .column("open")
            .map_err(|e| BrokerError::DataFeedError(format!("Missing 'open' column: {}", e)))?
            .f64()
            .map_err(|e| BrokerError::DataFeedError(format!("'open' not f64: {}", e)))?;
        let high = df
            .column("high")
            .map_err(|e| BrokerError::DataFeedError(format!("Missing 'high' column: {}", e)))?
            .f64()
            .map_err(|e| BrokerError::DataFeedError(format!("'high' not f64: {}", e)))?;
        let low = df
            .column("low")
            .map_err(|e| BrokerError::DataFeedError(format!("Missing 'low' column: {}", e)))?
            .f64()
            .map_err(|e| BrokerError::DataFeedError(format!("'low' not f64: {}", e)))?;
        let close = df
            .column("close")
            .map_err(|e| BrokerError::DataFeedError(format!("Missing 'close' column: {}", e)))?
            .f64()
            .map_err(|e| BrokerError::DataFeedError(format!("'close' not f64: {}", e)))?;
        let volume = df
            .column("volume")
            .map_err(|e| BrokerError::DataFeedError(format!("Missing 'volume' column: {}", e)))?
            .f64()
            .map_err(|e| BrokerError::DataFeedError(format!("'volume' not f64: {}", e)))?;
        let timestamp_col = df.column("timestamp").map_err(|e| {
            BrokerError::DataFeedError(format!("Missing 'timestamp' column: {}", e))
        })?;
        // Cast datetime to i64 to access the underlying millisecond values
        let timestamp_i64 = timestamp_col
            .cast(&polars::prelude::DataType::Int64)
            .map_err(|e| BrokerError::DataFeedError(format!("Failed to cast timestamp: {}", e)))?;
        let timestamp = timestamp_i64
            .i64()
            .map_err(|e| BrokerError::DataFeedError(format!("'timestamp' not i64: {}", e)))?;

        for i in 0..len {
            let ts_ms = timestamp.get(i).ok_or_else(|| {
                BrokerError::DataFeedError(format!("Null timestamp at row {}", i))
            })?;
            let dt = chrono::DateTime::from_timestamp_millis(ts_ms).ok_or_else(|| {
                BrokerError::DataFeedError(format!("Invalid timestamp at row {}", i))
            })?;

            bars.push(types::Bar {
                symbol: symbol.to_string(),
                open: open.get(i).unwrap_or(0.0),
                high: high.get(i).unwrap_or(0.0),
                low: low.get(i).unwrap_or(0.0),
                close: close.get(i).unwrap_or(0.0),
                volume: volume.get(i).unwrap_or(0.0),
                timestamp: dt,
            });
        }

        Ok(bars)
    }

    /// Advance one bar step: process pending/active orders against the current bar,
    /// then advance the bar index.
    /// Returns the current bars (one per symbol), or `None` if backtest is complete.
    pub fn step(
        &mut self,
    ) -> Result<Option<std::collections::HashMap<String, types::Bar>>, BrokerError> {
        debug!("UnifiedBroker::step enter");
        let state = self
            .backtest_state
            .as_ref()
            .ok_or_else(|| BrokerError::TradeError("BacktestState not initialized".into()))?;

        let state_guard = state.read();
        if state_guard.is_complete() {
            debug!("UnifiedBroker::step complete -> no more bars");
            return Ok(None);
        }

        // Get current bars
        let current_bars = state_guard.get_current_bars();
        let current_time = state_guard.current_time;
        debug!(
            "UnifiedBroker::step processing bars={} current_time={}",
            current_bars.len(),
            current_time
        );
        drop(state_guard);

        // Process orders against current bars
        debug!("UnifiedBroker::step calling execution.process_step");
        self.execution.process_step(&current_bars, current_time);
        debug!("UnifiedBroker::step execution.process_step returned");

        // Advance to next bar
        let mut state_guard = state.write();
        debug!("UnifiedBroker::step advancing backtest state");
        state_guard.advance();
        debug!("UnifiedBroker::step advanced backtest state");

        // Snapshot account
        if let Ok(account) = self.execution.get_account_sync() {
            state_guard.snapshot_account(&account);
        }

        debug!("UnifiedBroker::step exit");
        Ok(Some(current_bars))
    }

    /// Get backtest results after completion.
    pub fn get_results(&self) -> backtest_state::BacktestResults {
        self.execution
            .compute_results(self.backtest_state.as_ref().map(|s| s.read()))
    }
}

// ─────────────────────── Trait Definitions ───────────────────────

pub mod traits {
    use super::types::{
        Account, AccountType, Asset, BAR_DATAFRAME_COLUMNS, Bar, BarData, BrokerError, Order,
        Position, Quote, TradeUpdateEvent,
    };
    use crate::core::insight::Insight;
    use crate::core::utils::timeframe::TimeFrame;
    use chrono::DateTime;
    use polars::prelude::{Column, DataFrame};
    use std::sync::Arc;

    #[allow(async_fn_in_trait)]
    pub trait Broker {
        async fn connect(&self) -> Result<bool, BrokerError>;
        async fn disconnect(&self) -> Result<bool, BrokerError>;
        fn is_connected(&self) -> bool;
        fn get_current_time(&self) -> DateTime<chrono::Utc> {
            chrono::Utc::now()
        }
        fn get_name(&self) -> String;
        fn get_account_type(&self) -> Result<AccountType, BrokerError>;
    }

    pub trait OrderManagementProvider: Broker {
        // Order Execution
        async fn submit_order(&self, insight: Insight) -> Result<Order, BrokerError>;
        async fn cancel_order(&self, order_id: &str) -> Result<bool, BrokerError>;
        async fn update_order(
            &self,
            order_id: &str,
            price: f64,
            qty: f64,
        ) -> Result<bool, BrokerError>;
        async fn close_position(
            &self,
            order_id: &str,
            qty: f64,
            price: Option<f64>,
        ) -> Result<bool, BrokerError>;
        async fn close_all_positions(&self) -> Result<bool, BrokerError>;

        // Order Management
        async fn get_orders(&self) -> Result<Vec<Order>, BrokerError>;
        async fn get_order(&self, order_id: &str) -> Result<Order, BrokerError>;
        async fn get_positions(&self) -> Result<Vec<Position>, BrokerError>;
        async fn get_position(&self, symbol: &str) -> Result<Position, BrokerError>;

        // Account Management
        async fn get_account(&self) -> Result<Account, BrokerError>;

        // Trade Events
        /// Drain all pending trade events since the last call.
        /// Returns `(Order, TradeUpdateEvent)` tuples — one per state change.
        /// Each broker implementation collects events during order processing.
        fn drain_trade_events(&self) -> Vec<(Order, TradeUpdateEvent)>;

        /// Subscribe to a real-time stream of trade events.
        /// The broker will invoke the provided callback whenever an order's state changes.
        async fn subscribe_to_trade_stream(
            &self,
            on_trade: Arc<dyn Fn((Order, TradeUpdateEvent)) + Send + Sync>,
        ) -> Result<(), BrokerError>;

        /// Unsubscribe from the real-time trade event stream.
        async fn unsubscribe_from_trade_stream(&self) -> Result<(), BrokerError>;

        /// Format a raw trade update into `(Order, TradeUpdateEvent)`.
        /// Default implementation extracts the event from the order's status field.
        /// Broker implementations can override to map from native formats.
        fn format_on_trade_update(&self, order: Order) -> (Order, TradeUpdateEvent) {
            let event = order.status.clone();
            (order, event)
        }

        /// Allow execution brokers to advance live paper state from incoming bars.
        /// Real brokers can keep the default no-op behavior because fills are broker-managed.
        fn process_live_bar(&self, _bar: &Bar) {}
    }

    pub trait DataFeed {
        async fn connect(&self) -> Result<bool, BrokerError>;
        async fn disconnect(&self) -> Result<bool, BrokerError>;
        fn is_connected(&self) -> bool;
    }

    pub trait DataProvider: DataFeed {
        /// Ticker metadata
        async fn get_ticker_info(&self, symbol: &str) -> Result<Asset, BrokerError>;

        /// Historical bar data as a Polars DataFrame.
        /// Columns: open (f64), high (f64), low (f64), close (f64), volume (f64), timestamp (datetime)
        async fn get_history(
            &self,
            symbol: &str,
            start: DateTime<chrono::Utc>,
            end: DateTime<chrono::Utc>,
            time_frame: TimeFrame,
        ) -> Result<DataFrame, BrokerError>;

        async fn get_latest_quote(&self, symbol: &str) -> Result<Quote, BrokerError>;
        async fn get_latest_bar(&self, symbol: &str) -> Result<Bar, BrokerError>;

        // Data Subscription
        async fn subscribe_to_data_stream(
            &self,
            symbols: Vec<String>,
            time_frame: TimeFrame,
            mode: crate::core::broker::DataStreamMode,
            on_bar: Arc<dyn Fn(Bar) + Send + Sync>,
        ) -> Result<(), BrokerError>;
        async fn unsubscribe_from_data_stream(
            &self,
            symbols: Vec<String>,
        ) -> Result<(), BrokerError>;

        /// Format raw bar data into Polars DataFrame for the strategy layer.
        ///
        /// Accepts `BarData` which can be either:
        /// - `BarData::Frame(df)` — already a DataFrame; validated for required columns and passed through.
        /// - `BarData::Bars(bars)` — raw bar structs converted into a DataFrame.
        ///
        /// Data feeds can override this to provide custom formatting logic.
        fn format_on_bar(&self, data: BarData) -> Result<DataFrame, String> {
            match data {
                BarData::Frame(df) => {
                    // Validate that the DataFrame has the required bar columns
                    let col_names: Vec<&str> = df
                        .get_column_names()
                        .into_iter()
                        .map(|s| s.as_str())
                        .collect();
                    for required in BAR_DATAFRAME_COLUMNS {
                        if !col_names.contains(required) {
                            return Err(format!(
                                "DataFrame missing required column '{}'. Has: {:?}",
                                required, col_names
                            ));
                        }
                    }
                    Ok(df)
                }
                BarData::Bars(bars) => {
                    // Default: convert Vec<Bar> into a DataFrame
                    let symbols: Vec<&str> = bars.iter().map(|b| b.symbol.as_str()).collect();
                    let opens: Vec<f64> = bars.iter().map(|b| b.open).collect();
                    let highs: Vec<f64> = bars.iter().map(|b| b.high).collect();
                    let lows: Vec<f64> = bars.iter().map(|b| b.low).collect();
                    let closes: Vec<f64> = bars.iter().map(|b| b.close).collect();
                    let volumes: Vec<f64> = bars.iter().map(|b| b.volume).collect();
                    let timestamps: Vec<i64> = bars
                        .iter()
                        .map(|b| b.timestamp.timestamp_millis())
                        .collect();

                    DataFrame::new(vec![
                        Column::new("symbol".into(), &symbols),
                        Column::new("open".into(), &opens),
                        Column::new("high".into(), &highs),
                        Column::new("low".into(), &lows),
                        Column::new("close".into(), &closes),
                        Column::new("volume".into(), &volumes),
                        Column::new("timestamp".into(), &timestamps),
                    ])
                    .map_err(|e| e.to_string())
                }
            }
        }

        fn format_on_quote(&self, data: Vec<Quote>) -> Result<Vec<Quote>, String> {
            Ok(data)
        }
    }
}
