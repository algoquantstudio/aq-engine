#![allow(async_fn_in_trait)]

pub mod backtest_state;
pub mod account_cash_flow;
pub mod command_journal;
pub mod command_worker;
pub mod commands;
pub mod connection_worker;
pub mod data_feeds;
pub mod data_stream;
pub mod features;
pub mod fill_history;
pub mod mt5_bridge;
pub mod mt5_broker;
pub mod paper_broker;
pub mod position_valuation;
mod snapshot_reads;
pub mod snapshots;
pub mod types;

#[cfg(test)]
mod tests;

pub enum BrokerType {
    Paper(paper_broker::PaperBroker),
    Mt5(mt5_broker::Mt5Broker),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataStreamMode {
    Intrabar,
    CompletedBar,
}

pub type QuoteCallback = Arc<dyn Fn(types::Quote) + Send + Sync>;

#[derive(Default)]
struct ReplayQuotes {
    subscriptions: std::collections::HashMap<String, (String, QuoteCallback)>,
    latest: std::collections::HashMap<String, types::Quote>,
}

use crate::core::events::{BacktestMarketStep, ResolvedEventStream};
use crate::core::utils::timeframe::TimeFrame;
pub use commands::BrokerOrderRequest;
use futures::{StreamExt, TryStreamExt, stream};
use log::{debug, info};
use parking_lot::RwLock;
use polars::prelude::*;
use std::sync::Arc;
use types::{AccountType, Asset, Bar, BrokerError, Order};

pub use features::{
    BrokerFeature, DataFeedFeature, UnifiedBrokerFeature, effective_buying_power_from,
};

const MAX_CONCURRENT_HISTORY_LOADS: usize = 8;

pub(crate) fn block_on_broker_future<F>(future: F) -> F::Output
where
    F: std::future::Future,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current()
        && handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
    {
        return tokio::task::block_in_place(|| handle.block_on(future));
    }
    futures::executor::block_on(future)
}

// ─────────────────────── UnifiedBroker ───────────────────────

/// Composes an execution engine (E) and a data provider (D).
/// Optionally holds a shared `BacktestState` for backtesting scenarios.
pub struct UnifiedBroker<E, D>
where
    E: traits::Broker + traits::OrderManagementProvider,
    D: traits::DataFeed + traits::DataProvider,
{
    pub name: String,
    /// Shared transport ownership; the strategy remains the sole Insight owner.
    pub execution: Arc<E>,
    pub data: Arc<D>,
    pub backtest_state: Option<Arc<RwLock<backtest_state::BacktestState>>>,
    replay_quotes: RwLock<ReplayQuotes>,
    snapshot_reads: parking_lot::Mutex<Option<snapshot_reads::LiveSnapshotReads>>,
    connection_worker: parking_lot::Mutex<Option<connection_worker::ProviderConnectionWorker>>,
}

impl<E, D> UnifiedBroker<E, D>
where
    E: traits::Broker + traits::OrderManagementProvider,
    D: traits::DataFeed + traits::DataProvider,
{
    pub fn new(execution: E, data: D) -> Self {
        Self {
            name: format!("{}", execution.get_name()),
            execution: Arc::new(execution),
            data: Arc::new(data),
            backtest_state: None,
            replay_quotes: RwLock::new(ReplayQuotes::default()),
            snapshot_reads: parking_lot::Mutex::new(None),
            connection_worker: parking_lot::Mutex::new(None),
        }
    }

    /// Create a UnifiedBroker configured for backtesting with a shared BacktestState.
    pub fn new_backtest(execution: E, data: D) -> Self {
        let state = Arc::new(RwLock::new(backtest_state::BacktestState::new()));
        Self {
            name: format!("{}", execution.get_name()),
            execution: Arc::new(execution),
            data: Arc::new(data),
            backtest_state: Some(state),
            replay_quotes: RwLock::new(ReplayQuotes::default()),
            snapshot_reads: parking_lot::Mutex::new(None),
            connection_worker: parking_lot::Mutex::new(None),
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
        self.stop_connection_worker().await;
        self.stop_snapshot_reads().await;
        *self.replay_quotes.write() = ReplayQuotes::default();
        let execution = self.execution.disconnect().await;
        let data = self.data.disconnect().await;
        execution?;
        data?;
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

    pub fn configure_live_session(&self, session_id: &str) -> Result<(), BrokerError> {
        self.execution.configure_live_session(session_id)?;
        self.data.configure_live_session(session_id)
    }

    /// Check if the data feed is connected.
    pub fn is_datafeed_connected(&self) -> bool {
        self.data.is_connected()
    }

    /// Return whether the execution broker or data feed supports one feature.
    /// The feature type selects the correct side of the unified broker.
    pub fn has_feature<F>(&self, feature: F) -> bool
    where
        F: UnifiedBrokerFeature,
    {
        feature.is_supported_by(self)
    }

    /// Return true only when every requested feature is supported.
    pub fn has_features<F>(&self, features: &[F]) -> bool
    where
        F: UnifiedBrokerFeature,
    {
        features
            .iter()
            .copied()
            .all(|feature| self.has_feature(feature))
    }
}

impl Default for UnifiedBroker<paper_broker::PaperBroker, data_feeds::yahoo::YahooFinanceDataFeed> {
    fn default() -> Self {
        let execution = paper_broker::PaperBroker::new(types::AccountType::Paper, 100_000.0, 4);
        let data = data_feeds::yahoo::YahooFinanceDataFeed::new();

        Self {
            name: "UnifiedBroker (Default)".to_string(),
            execution: Arc::new(execution),
            data: Arc::new(data),
            backtest_state: None,
            replay_quotes: RwLock::new(ReplayQuotes::default()),
            snapshot_reads: parking_lot::Mutex::new(None),
            connection_worker: parking_lot::Mutex::new(None),
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

    fn get_features(&self) -> &'static [BrokerFeature] {
        self.execution.get_features()
    }

    fn configure_asset_metadata(&self, asset: &Asset) -> Result<(), BrokerError> {
        self.execution.configure_asset_metadata(asset)
    }

    fn get_cached_lifecycle_snapshot(&self) -> Option<types::BrokerLifecycleSnapshot> {
        self.execution.get_cached_lifecycle_snapshot()
    }
}

// ─────────────── Synchronous order helpers (for StrategyContext) ───────────────

impl<E, D> UnifiedBroker<E, D>
where
    E: traits::Broker + traits::OrderManagementProvider,
    D: traits::DataFeed + traits::DataProvider,
{
    /// Returns an owned, broker-neutral lifecycle resolver that may run independently from the
    /// strategy loop. Brokers opt in only when their adapter can safely be shared across tasks.
    pub fn lifecycle_reconciler(&self) -> Option<Arc<dyn traits::OrderLifecycleReconciler>> {
        self.execution.lifecycle_reconciler()
    }

    /// Synchronous cancel — safe when the underlying broker is PaperBroker
    /// (all operations are non-blocking).
    pub fn cancel_order_sync(&self, order_id: &str) -> Result<bool, BrokerError> {
        // PaperBroker's cancel_order doesn't actually await anything,
        // so blocking here is safe and zero-cost.
        block_on_broker_future(self.execution.cancel_order(order_id))
    }

    /// Synchronous order update — used by insight pipes to modify broker-side legs.
    pub fn update_order_sync(
        &self,
        order_id: &str,
        price: f64,
        qty: f64,
    ) -> Result<bool, BrokerError> {
        // StrategyContext's existing update_order is used by TP pipes. Keep that
        // meaning explicit until the receipt-returning context migration replaces it.
        block_on_broker_future(self.execution.update_take_profit(order_id, price, qty))
    }

    pub fn update_stop_loss_sync(
        &self,
        order_id: &str,
        price: f64,
        qty: f64,
    ) -> Result<bool, BrokerError> {
        block_on_broker_future(self.execution.update_stop_loss(order_id, price, qty))
    }

    /// Synchronous close — safe when the underlying broker is PaperBroker.
    pub fn close_position_sync(
        &self,
        order_id: &str,
        qty: f64,
        price: Option<f64>,
    ) -> Result<bool, BrokerError> {
        block_on_broker_future(self.execution.close_position(order_id, qty, price))
    }

    pub fn get_position_sync(&self, symbol: &str) -> Result<types::Position, BrokerError> {
        self.positions_snapshot()?
            .iter()
            .find(|position| position.asset.symbol == symbol)
            .cloned()
            .ok_or_else(|| BrokerError::PositionError("Position not found".into()))
    }

    pub fn get_positions_sync(&self) -> Result<Vec<types::Position>, BrokerError> {
        Ok(self.positions_snapshot()?.to_vec())
    }

    /// Exact entry lookup from a fresh decision snapshot. Never performs I/O.
    pub fn get_position_by_order_id_sync(&self, order_id: &str, symbol: &str) -> Result<Option<types::Position>, BrokerError> {
        self.insight_context().position(order_id,symbol)
    }
}

// ─────────────────────── OMS delegation ───────────────────────

impl<E, D> traits::OrderManagementProvider for UnifiedBroker<E, D>
where
    E: traits::Broker + traits::OrderManagementProvider,
    D: traits::DataFeed + traits::DataProvider,
{
    fn account_cash_flow_reader(&self) -> Option<Arc<dyn account_cash_flow::AccountCashFlowReader>> {
        self.execution.account_cash_flow_reader()
    }
    async fn reconcile_submission(
        &self,
        account_id: &str,
        request: &BrokerOrderRequest,
    ) -> Result<commands::SubmissionResolution, BrokerError> {
        self.execution
            .reconcile_submission(account_id, request)
            .await
    }

    async fn pending_cancellation_recovery(&self, account: &str, order: &str)
        -> Result<Option<types::OrderLifecycleSnapshot>, BrokerError> {
        self.execution.pending_cancellation_recovery(account, order).await
    }

    async fn retry_cancellation(&self, account: &str, order: &str) -> Result<bool, BrokerError> {
        self.execution.retry_cancellation(account, order).await
    }

    async fn submit_order(&self, insight: BrokerOrderRequest) -> Result<Order, BrokerError> {
        self.execution.submit_order(insight).await
    }

    async fn cancel_order(&self, order_id: &str) -> Result<bool, BrokerError> {
        self.execution.cancel_order(order_id).await
    }

    async fn update_stop_loss(
        &self,
        order_id: &str,
        price: f64,
        qty: f64,
    ) -> Result<bool, BrokerError> {
        self.execution.update_stop_loss(order_id, price, qty).await
    }

    async fn amend_entry_prices(
        &self,
        order_id: &str,
        limit_price: Option<f64>,
        stop_price: Option<f64>,
    ) -> Result<bool, BrokerError> {
        self.execution
            .amend_entry_prices(order_id, limit_price, stop_price)
            .await
    }

    async fn replace_order(
        &self,
        order_id: &str,
        price: f64,
        qty: f64,
    ) -> Result<bool, BrokerError> {
        self.execution.replace_order(order_id, price, qty).await
    }

    async fn update_take_profit(
        &self,
        order_id: &str,
        price: f64,
        qty: f64,
    ) -> Result<bool, BrokerError> {
        self.execution
            .update_take_profit(order_id, price, qty)
            .await
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

    async fn reconcile_order_lifecycle(
        &self,
        order_ids: &[String],
    ) -> Result<Vec<types::OrderLifecycleSnapshot>, BrokerError> {
        self.execution.reconcile_order_lifecycle(order_ids).await
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

    async fn get_position_by_order_id(&self, order_id: &str, symbol: &str) -> Result<Option<types::Position>, BrokerError> {
        self.execution.get_position_by_order_id(order_id,symbol).await
    }

    async fn get_account(&self) -> Result<types::Account, BrokerError> {
        self.execution.get_account().await
    }

    fn get_cached_account(&self) -> Option<types::Account> {
        self.execution.get_cached_account()
    }

    fn get_cached_positions(&self) -> Option<Vec<types::Position>> {
        self.execution.get_cached_positions()
    }

    fn drain_trade_events(&self) -> Vec<(types::Order, types::TradeUpdateEvent)> {
        self.execution.drain_trade_events()
    }

    fn drain_trade_events_bounded(
        &self,
        max_events: usize,
    ) -> Vec<(types::Order, types::TradeUpdateEvent)> {
        self.execution.drain_trade_events_bounded(max_events)
    }

    fn has_pending_trade_events(&self) -> bool {
        self.execution.has_pending_trade_events()
    }

    fn drain_broker_events(&self) -> Vec<types::BrokerOrderEvent> {
        self.execution.drain_broker_events()
    }

    async fn subscribe_to_trade_stream(
        &self,
        on_trade: Arc<dyn Fn((types::Order, types::TradeUpdateEvent)) + Send + Sync>,
    ) -> Result<(), BrokerError> {
        self.execution.subscribe_to_trade_stream(on_trade).await
    }

    async fn subscribe_to_broker_event_stream(
        &self,
        on_trade: Arc<dyn Fn(types::BrokerOrderEvent) + Send + Sync>,
    ) -> Result<(), BrokerError> {
        self.execution
            .subscribe_to_broker_event_stream(on_trade)
            .await
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

    fn get_features(&self) -> &'static [DataFeedFeature] {
        if self.backtest_state.is_some() {
            &[
                DataFeedFeature::HistoricalCandles,
                DataFeedFeature::LatestCandle,
                DataFeedFeature::LatestQuote,
                DataFeedFeature::CandleStream,
                DataFeedFeature::QuoteStream,
                DataFeedFeature::SyntheticQuotes,
            ]
        } else {
            self.data.get_features()
        }
    }
}

// ─────────────────────── DataProvider delegation ───────────────────────

impl<E, D> traits::DataProvider for UnifiedBroker<E, D>
where
    E: traits::Broker + traits::OrderManagementProvider,
    D: traits::DataFeed + traits::DataProvider,
{
    async fn supports_historical_stream(&self, symbol: &str, timeframe: TimeFrame) -> Result<bool, BrokerError> {
        self.data.supports_historical_stream(symbol, timeframe).await
    }

    async fn get_ticker_info(&self, symbol: &str) -> Result<types::Asset, BrokerError> {
        let mut asset = self.data.get_ticker_info(symbol).await?;
        // Execution limits belong to the execution broker, not the quote provider.
        // E.g. paper trading against an MT5 feed has no broker aggregate-volume ceiling.
        if !self
            .execution
            .get_features()
            .contains(&BrokerFeature::MaxTotalSize)
        {
            asset.max_total_size = None;
        }
        Ok(asset)
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
        if self.backtest_state.is_some() {
            return self
                .replay_quotes
                .read()
                .latest
                .get(symbol)
                .cloned()
                .ok_or_else(|| {
                    BrokerError::DataFeedError(format!("No replayed quote yet for {symbol}"))
                });
        }
        self.data.get_latest_quote(symbol).await
    }

    async fn subscribe_to_quote_stream(
        &self,
        subscription_id: String,
        symbol: String,
        on_quote: QuoteCallback,
    ) -> Result<(), BrokerError> {
        if subscription_id.is_empty() || symbol.trim().is_empty() {
            return Err(BrokerError::DataFeedError(
                "Quote subscription requires an ID and symbol".into(),
            ));
        }
        if self.backtest_state.is_some() {
            self.replay_quotes
                .write()
                .subscriptions
                .insert(subscription_id, (symbol, on_quote));
            return Ok(());
        }
        self.data
            .subscribe_to_quote_stream(subscription_id, symbol, on_quote)
            .await
    }

    async fn unsubscribe_from_quote_stream(
        &self,
        subscription_id: &str,
    ) -> Result<(), BrokerError> {
        if self.backtest_state.is_some() {
            self.replay_quotes
                .write()
                .subscriptions
                .remove(subscription_id);
            return Ok(());
        }
        self.data
            .unsubscribe_from_quote_stream(subscription_id)
            .await
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
    fn publish_replay_quotes(&self, bars: &std::collections::HashMap<String, Bar>) {
        let (quotes, subscriptions) = {
            let mut replay = self.replay_quotes.write();
            let quotes: Vec<_> = bars.values().map(types::Quote::from_bar_close).collect();
            for quote in &quotes {
                replay.latest.insert(quote.symbol.clone(), quote.clone());
            }
            (
                quotes,
                replay.subscriptions.values().cloned().collect::<Vec<_>>(),
            )
        };
        // No locks held over user callbacks. Only consumed bars, never a live provider or
        // next-bar index. This is a data-provider facility, not a Strategy::on_quote hook.
        for quote in quotes {
            for (symbol, callback) in &subscriptions {
                if *symbol == quote.symbol {
                    callback(quote.clone());
                }
            }
        }
    }

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
        state.write().set_backtest_window(start, end);

        let data = &self.data;
        let mut histories = stream::iter(symbols.iter().cloned().enumerate())
            .map(|(index, symbol)| async move {
                info!(
                    "Loading backtest history for symbol={} timeframe={:?} start={} end={}",
                    symbol, time_frame, start, end
                );
                if !data.supports_historical_stream(&symbol, time_frame).await? {
                    log::warn!("Skipping unavailable historical stream {} {}", symbol, time_frame);
                    return Ok((index, symbol, Vec::new()));
                }
                let df = data.get_history(&symbol, start, end, time_frame).await?;
                debug!("Fetched dataframe for {} shape={:?}", symbol, df.shape());

                let bars = Self::dataframe_to_bars(&symbol, &df)?;
                if bars.is_empty() {
                    return Err(BrokerError::DataFeedError(format!(
                        "Backtest history request returned 0 bars for {} timeframe={:?} start={} end={}",
                        symbol, time_frame, start, end
                    )));
                }
                info!("Loaded {} bars for {}", bars.len(), symbol);
                Ok((index, symbol, bars))
            })
            .buffer_unordered(MAX_CONCURRENT_HISTORY_LOADS)
            .try_collect::<Vec<_>>()
            .await?;
        histories.retain(|(_, _, bars)| !bars.is_empty());
        if histories.is_empty() {
            return Err(BrokerError::DataFeedError("No requested historical streams are available in the selected data feed".into()));
        }
        histories.sort_unstable_by_key(|(index, _, _)| *index);

        for (_, symbol, bars) in histories {
            let mut state_guard = state.write();
            state_guard.load_bars(symbol.clone(), bars);

            // Set initial time from the first bar
            if let Some(first_bar) = state_guard
                .historical_bars
                .get(&symbol)
                .and_then(|v| v.first())
            {
                if state_guard.previous_time.is_none() {
                    state_guard.current_time = first_bar.timestamp;
                }
            }
        }

        // Share the BacktestState with PaperBroker
        Arc::get_mut(&mut self.execution)
            .ok_or_else(|| BrokerError::ConnectionError(
                "Backtest history must be configured before sharing execution with command workers".into()
            ))?
            .set_backtest_state(state);

        Ok(())
    }

    pub async fn load_backtest_event_streams(
        &mut self,
        streams: &[ResolvedEventStream],
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), BrokerError> {
        if self.backtest_state.is_none() {
            self.backtest_state = Some(Arc::new(RwLock::new(backtest_state::BacktestState::new())));
        }

        let state = self.backtest_state.as_ref().unwrap().clone();
        state.write().set_backtest_window(start, end);

        let data = &self.data;
        let mut histories = stream::iter(streams.iter().cloned().enumerate())
            .map(|(index, stream)| async move {
                let symbol = stream.key.symbol.clone();
                let time_frame = stream.key.timeframe;
                if !data.supports_historical_stream(&symbol, time_frame).await? {
                    log::warn!("Skipping unavailable historical stream {} {}", symbol, time_frame);
                    return Ok((index, stream, Vec::new()));
                }
                info!(
                    "Loading backtest stream symbol={} history_key={} timeframe={} feature={} allow_trading={} start={} end={}",
                    &symbol,
                    stream.history_key,
                    time_frame,
                    stream.is_feature,
                    stream.allow_trading,
                    start,
                    end
                );

                let df = data.get_history(&symbol, start, end, time_frame).await?;
                debug!(
                    "Fetched dataframe for {} {} shape={:?}",
                    &symbol,
                    time_frame,
                    df.shape()
                );

                let bars = Self::dataframe_to_bars(&symbol, &df)?;
                if bars.is_empty() {
                    return Err(BrokerError::DataFeedError(format!(
                        "Backtest history request returned 0 bars for {} timeframe={} start={} end={}",
                        &symbol, time_frame, start, end
                    )));
                }

                Ok((index, stream, bars))
            })
            .buffer_unordered(MAX_CONCURRENT_HISTORY_LOADS)
            .try_collect::<Vec<_>>()
            .await?;
        histories.retain(|(_, _, bars)| !bars.is_empty());
        if histories.is_empty() {
            return Err(BrokerError::DataFeedError("No requested historical streams are available in the selected data feed".into()));
        }
        histories.sort_unstable_by_key(|(index, _, _)| *index);

        for (_, stream, bars) in histories {
            let symbol = &stream.key.symbol;
            let mut state_guard = state.write();
            state_guard.load_event_stream_bars(stream.clone(), bars.clone());
            if !stream.is_feature {
                state_guard.load_bars(symbol.clone(), bars);
                if state_guard.previous_time.is_none() {
                    if let Some(first_bar) = state_guard
                        .historical_bars
                        .get(symbol)
                        .and_then(|bars| bars.first())
                    {
                        state_guard.current_time = first_bar.timestamp;
                    }
                }
            }
        }

        Arc::get_mut(&mut self.execution)
            .ok_or_else(|| BrokerError::ConnectionError(
                "Backtest history must be configured before sharing execution with command workers".into()
            ))?
            .set_backtest_state(state);

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

        let rows = timestamp
            .iter()
            .zip(open.iter())
            .zip(high.iter())
            .zip(low.iter())
            .zip(close.iter())
            .zip(volume.iter());

        for (i, (((((ts_ms, open), high), low), close), volume)) in rows.enumerate() {
            let ts_ms = ts_ms.ok_or_else(|| {
                BrokerError::DataFeedError(format!("Null timestamp at row {}", i))
            })?;
            let dt = chrono::DateTime::from_timestamp_millis(ts_ms).ok_or_else(|| {
                BrokerError::DataFeedError(format!("Invalid timestamp at row {}", i))
            })?;

            bars.push(types::Bar {
                symbol: symbol.to_string(),
                open: open.unwrap_or(0.0),
                high: high.unwrap_or(0.0),
                low: low.unwrap_or(0.0),
                close: close.unwrap_or(0.0),
                volume: volume.unwrap_or(0.0),
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
        self.publish_replay_quotes(&current_bars);
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

    pub fn step_market_streams(&mut self) -> Result<Option<BacktestMarketStep>, BrokerError> {
        debug!("UnifiedBroker::step_market_streams enter");
        let state = self
            .backtest_state
            .as_ref()
            .ok_or_else(|| BrokerError::TradeError("BacktestState not initialized".into()))?;

        let mut state_guard = state.write();
        if state_guard.is_market_stream_complete() {
            debug!("UnifiedBroker::step_market_streams complete -> no more events");
            return Ok(None);
        }

        let Some(step) = state_guard.next_market_step() else {
            debug!("UnifiedBroker::step_market_streams no next step");
            return Ok(None);
        };
        drop(state_guard);

        if !step.execution_bars.is_empty() {
            debug!(
                "UnifiedBroker::step_market_streams processing execution bars={} timestamp={}",
                step.execution_bars.len(),
                step.timestamp
            );
            self.execution
                .process_step(&step.execution_bars, step.timestamp);
            self.publish_replay_quotes(&step.execution_bars);
        }

        let mut state_guard = state.write();
        state_guard.advance_market_step(&step);

        if let Ok(account) = self.execution.get_account_sync() {
            state_guard.snapshot_account(&account);
        }

        debug!("UnifiedBroker::step_market_streams exit");
        Ok(Some(step))
    }

    /// Get backtest results after completion.
    pub fn get_results(&self) -> backtest_state::BacktestResults {
        self.execution
            .compute_results(self.backtest_state.as_ref().map(|s| s.read()))
    }
}

impl<D> UnifiedBroker<paper_broker::PaperBroker, D>
where
    D: traits::DataFeed + traits::DataProvider,
{
    pub fn flush_backtest_close_queue_at_last_bars(&mut self) -> Result<usize, BrokerError> {
        let state = self
            .backtest_state
            .as_ref()
            .ok_or_else(|| BrokerError::TradeError("BacktestState not initialized".into()))?;

        let (last_bars, close_time) = {
            let state_guard = state.read();
            let last_bars = state_guard.get_last_bars();
            let close_time = last_bars
                .values()
                .map(|bar| bar.timestamp)
                .max()
                .unwrap_or(state_guard.current_time);
            (last_bars, close_time)
        };

        if last_bars.is_empty() {
            return Ok(0);
        }

        let processed = self
            .execution
            .process_close_queue_at(&last_bars, close_time);
        if processed == 0 {
            return Ok(0);
        }

        let mut state_guard = state.write();
        let previous_time = state_guard.current_time;
        state_guard.previous_time = Some(previous_time);
        state_guard.current_time = close_time;
        if let Ok(account) = self.execution.get_account_sync() {
            state_guard.snapshot_account(&account);
        }

        Ok(processed)
    }
}

// ─────────────────────── Trait Definitions ───────────────────────

pub mod traits {
    use super::commands::BrokerOrderRequest;
    use super::features::{BrokerFeature, DataFeedFeature};
    use super::types::{
        Account, AccountType, Asset, BAR_DATAFRAME_COLUMNS, Bar, BarData, BrokerError,
        BrokerLifecycleSnapshot, BrokerOrderEvent, Order, OrderLifecycleSnapshot,
        OrderLifecycleStatus, Position, Quote, TradeUpdateEvent,
    };
    use crate::core::utils::timeframe::TimeFrame;
    use chrono::DateTime;
    use polars::prelude::{Column, DataFrame};
    use std::sync::Arc;

    #[async_trait::async_trait]
    pub trait OrderLifecycleReconciler: Send + Sync {
        async fn reconcile(
            &self,
            order_ids: &[String],
        ) -> Result<Vec<OrderLifecycleSnapshot>, BrokerError>;
    }

    #[allow(async_fn_in_trait)]
    pub trait Broker: Send + Sync {
        /// Connection attempts run in an owned background task. Cancellation
        /// must leave transport resources owned by the adapter for disconnect.
        fn connect(&self) -> impl std::future::Future<Output = Result<bool, BrokerError>> + Send;
        async fn disconnect(&self) -> Result<bool, BrokerError>;
        fn is_connected(&self) -> bool;
        fn get_current_time(&self) -> DateTime<chrono::Utc> {
            chrono::Utc::now()
        }
        fn get_name(&self) -> String;
        fn get_account_type(&self) -> Result<AccountType, BrokerError>;

        /// Native execution features supported by this broker.
        fn get_features(&self) -> &'static [BrokerFeature] {
            &[]
        }

        fn configure_live_session(&self, _session_id: &str) -> Result<(), BrokerError> {
            Ok(())
        }

        fn configure_asset_metadata(&self, _asset: &Asset) -> Result<(), BrokerError> {
            Ok(())
        }

        /// Return the newest complete broker lifecycle view without transport I/O.
        ///
        /// `None` means the adapter has not acquired an authoritative snapshot yet. Callers must
        /// never infer cancellation or closure from `None` or an incomplete snapshot.
        fn get_cached_lifecycle_snapshot(&self) -> Option<BrokerLifecycleSnapshot> {
            None
        }
    }

    pub trait OrderManagementProvider: Broker {
        /// Estimate margin required for an entry in the account currency.
        /// `None` means the broker does not provide authoritative estimation;
        /// callers must not substitute generic notional arithmetic.
        fn estimate_order_margin(
            &self,
            _request: &BrokerOrderRequest,
        ) -> impl std::future::Future<Output = Result<Option<f64>, BrokerError>> + Send {
            async { Ok(None) }
        }
        /// Read-only, exact client-command lookup. Never infer rejection from
        /// absence in the active-order book; do not submit inside this method.
        fn reconcile_submission(
            &self,
            _account_id: &str,
            _request: &BrokerOrderRequest,
        ) -> impl std::future::Future<
            Output = Result<super::commands::SubmissionResolution, BrokerError>,
        > + Send {
            async { Ok(super::commands::SubmissionResolution::Unsupported) }
        }
        /// Read-only, account/ticket-exact proof that a previously uncertain
        /// cancellation still targets a pending order. Absence is not proof.
        fn pending_cancellation_recovery(&self, _account: &str, _order: &str)
            -> impl std::future::Future<Output = Result<Option<OrderLifecycleSnapshot>, BrokerError>> + Send {
            async { Ok(None) }
        }
        /// Only the durable command worker may call this after recording a
        /// recovery attempt. Must never submit/close/replace an order and must
        /// confirm cancellation from authoritative terminal evidence, not ACK,
        /// delivering that evidence through the normal broker event reducer.
        fn retry_cancellation(&self, _account: &str, _order: &str)
            -> impl std::future::Future<Output = Result<bool, BrokerError>> + Send {
            async { Err(BrokerError::UnsupportedOperation("cancellation recovery unsupported".into())) }
        }
        /// Creates an owned resolver for restart reconciliation. The default is deliberately
        /// unsupported so adding a broker never makes unsafe lifecycle assumptions.
        fn lifecycle_reconciler(&self) -> Option<Arc<dyn OrderLifecycleReconciler>> {
            None
        }

        /// Optional read-only analytics recovery. Implementations must not
        /// reconcile, submit, or otherwise mutate broker orders during a read.
        fn execution_fill_history_reader(
            &self,
        ) -> Option<Arc<dyn super::fill_history::ExecutionFillHistoryReader>> {
            None
        }

        /// Optional read-only account funding/adjustment observations. Never
        /// derive these from changes in account equity or order/Insight state.
        fn account_cash_flow_reader(&self) -> Option<Arc<dyn super::account_cash_flow::AccountCashFlowReader>> {
            None
        }

        /// Optional read-only, exact-position valuation. The legacy Position
        /// model is not sufficient evidence of account-currency exposure.
        fn position_valuation_reader(
            &self,
        ) -> Option<Arc<dyn super::position_valuation::PositionValuationReader>> {
            None
        }

        // Order Execution
        // Send futures are part of the contract: execution workers run on Tokio's
        // multi-thread runtime without boxing each mutation future.
        fn submit_order(
            &self,
            request: BrokerOrderRequest,
        ) -> impl std::future::Future<Output = Result<Order, BrokerError>> + Send;
        fn cancel_order(
            &self,
            order_id: &str,
        ) -> impl std::future::Future<Output = Result<bool, BrokerError>> + Send;
        /// Explicit pending-entry replacement. Never silently interpret this as
        /// a take-profit modification (older adapters used update_order for both).
        fn amend_entry_prices(
            &self,
            _order_id: &str,
            _limit_price: Option<f64>,
            _stop_price: Option<f64>,
        ) -> impl std::future::Future<Output = Result<bool, BrokerError>> + Send {
            async {
                Err(BrokerError::UnsupportedOperation(
                    "Entry price amendment".into(),
                ))
            }
        }

        fn replace_order(
            &self,
            _order_id: &str,
            _price: f64,
            _qty: f64,
        ) -> impl std::future::Future<Output = Result<bool, BrokerError>> + Send {
            async {
                Err(BrokerError::UnsupportedOperation(
                    "Pending order replacement".into(),
                ))
            }
        }

        fn update_take_profit(
            &self,
            order_id: &str,
            price: f64,
            qty: f64,
        ) -> impl std::future::Future<Output = Result<bool, BrokerError>> + Send;

        fn update_stop_loss(
            &self,
            order_id: &str,
            price: f64,
            qty: f64,
        ) -> impl std::future::Future<Output = Result<bool, BrokerError>> + Send;
        /// Owns the position's protective-order cleanup. A full close must cancel
        /// remaining native legs; a partial close must preserve/resize protection
        /// for the remaining exposure. The strategy must not independently race
        /// protective leg cancellations against this operation. `Ok(true)` is
        /// acknowledgement only; terminal trade evidence is emitted separately.
        fn close_position(
            &self,
            order_id: &str,
            qty: f64,
            price: Option<f64>,
        ) -> impl std::future::Future<Output = Result<bool, BrokerError>> + Send;
        async fn close_all_positions(&self) -> Result<bool, BrokerError>;

        // Order Management
        fn get_orders(
            &self,
        ) -> impl std::future::Future<Output = Result<Vec<Order>, BrokerError>> + Send;

        /// Resolve a batch of broker order IDs after a process restart.
        ///
        /// The default implementation uses `get_orders()` and never infers that a filled order's
        /// position is closed. Brokers with native position and order-history identity should
        /// override this method and return `PositionOpen` or a confirmed terminal state.
        async fn reconcile_order_lifecycle(
            &self,
            order_ids: &[String],
        ) -> Result<Vec<OrderLifecycleSnapshot>, BrokerError> {
            let orders = self.get_orders().await?;
            let orders_by_id = orders
                .into_iter()
                .map(|order| (order.order_id.clone(), order))
                .collect::<std::collections::HashMap<_, _>>();
            Ok(order_ids
                .iter()
                .map(|order_id| {
                    let (status, symbol) = orders_by_id
                        .get(order_id)
                        .map(|order| {
                            let status = match order.status {
                                TradeUpdateEvent::Filled | TradeUpdateEvent::PartialFilled => {
                                    OrderLifecycleStatus::Filled
                                }
                                TradeUpdateEvent::Closed => OrderLifecycleStatus::Closed,
                                TradeUpdateEvent::Cancelled => OrderLifecycleStatus::Cancelled,
                                TradeUpdateEvent::Rejected => OrderLifecycleStatus::Rejected,
                                TradeUpdateEvent::Expired => OrderLifecycleStatus::Expired,
                                TradeUpdateEvent::Accepted
                                | TradeUpdateEvent::New
                                | TradeUpdateEvent::PendingNew
                                | TradeUpdateEvent::Pending
                                | TradeUpdateEvent::Replaced => OrderLifecycleStatus::Pending,
                            };
                            (status, Some(order.asset.symbol.clone()))
                        })
                        .unwrap_or((OrderLifecycleStatus::Unknown, None));
                    OrderLifecycleSnapshot {
                        order_id: order_id.clone(),
                        status,
                        position_id: None,
                        position_qty: None,
                        symbol,
                        detail: None,
                        close_details: None,
                    }
                })
                .collect())
        }

        async fn get_order(&self, order_id: &str) -> Result<Order, BrokerError>;
        fn get_positions(
            &self,
        ) -> impl std::future::Future<Output = Result<Vec<Position>, BrokerError>> + Send;
        async fn get_position(&self, symbol: &str) -> Result<Position, BrokerError>;

        /// Explicit asynchronous broker refresh. For hot-path Insight reads use
        /// `insight.get_position()` or UnifiedBroker's snapshot lookup instead.
        async fn get_position_by_order_id(&self, order_id: &str, symbol: &str) -> Result<Option<Position>, BrokerError> {
            let positions=self.get_positions().await?;
            let mut matching=positions.iter().filter(|p|p.order_id.as_deref()==Some(order_id));
            let result=matching.next().cloned();
            if matching.next().is_some() || result.as_ref().is_some_and(|p|p.asset.symbol!=symbol) {
                return Err(BrokerError::PositionError("Ambiguous broker position identity".into()));
            }
            if result.is_none() && positions.iter().any(|p|p.order_id.is_none()) {
                return Err(BrokerError::PositionError("Broker snapshot does not include order IDs".into()));
            }
            Ok(result)
        }

        // Account Management
        fn get_account(
            &self,
        ) -> impl std::future::Future<Output = Result<Account, BrokerError>> + Send;

        /// Return the newest account snapshot without transport I/O. Account-stream brokers
        /// update this cache from their adapter; local brokers can return their in-memory value.
        fn get_cached_account(&self) -> Option<Account> {
            None
        }

        /// Transport-free positions for deterministic in-memory execution. Live strategy
        /// reads use the timestamped refresh cache, not this untimestamped adapter hook.
        fn get_cached_positions(&self) -> Option<Vec<Position>> {
            None
        }

        /// Exact in-memory position lookup for simulation/custom adapters.
        /// Live strategies use the timestamped cache instead of this hook.
        fn position_snapshot_reader(&self) -> Option<super::types::PositionSnapshotReader> {
            None
        }

        // Trade Events
        /// Remove at most `max_events` in source order, leaving all other events
        /// owned by the adapter. Zero must not consume anything. Implement this
        /// at the queue, not by draining everything and truncating the result.
        fn drain_trade_events_bounded(&self, max_events: usize) -> Vec<(Order, TradeUpdateEvent)>;

        /// Non-blocking queue readiness; must not perform network or database I/O.
        fn has_pending_trade_events(&self) -> bool;

        /// Convenience for explicit offline/test consumers. Strategy drivers use
        /// the bounded API and yield between reducer turns.
        fn drain_trade_events(&self) -> Vec<(Order, TradeUpdateEvent)> {
            self.drain_trade_events_bounded(usize::MAX)
        }

        /// Drain broker-neutral envelopes. During the compatibility release, legacy brokers are
        /// wrapped with generated event IDs and unknown source timing metadata.
        fn drain_broker_events(&self) -> Vec<BrokerOrderEvent> {
            self.drain_trade_events()
                .into_iter()
                .map(BrokerOrderEvent::new)
                .collect()
        }

        /// Subscribe to a real-time stream of trade events.
        /// The broker will invoke the provided callback whenever an order's state changes.
        async fn subscribe_to_trade_stream(
            &self,
            on_trade: Arc<dyn Fn((Order, TradeUpdateEvent)) + Send + Sync>,
        ) -> Result<(), BrokerError>;

        /// Subscribe to broker-neutral envelopes. Built-in brokers should override this once
        /// their native adapter can supply source sequence and source timestamps.
        async fn subscribe_to_broker_event_stream(
            &self,
            on_trade: Arc<dyn Fn(BrokerOrderEvent) + Send + Sync>,
        ) -> Result<(), BrokerError> {
            self.subscribe_to_trade_stream(Arc::new(move |event| {
                on_trade(BrokerOrderEvent::new(event));
            }))
            .await
        }

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

    pub trait DataFeed: Send + Sync {
        /// See Broker::connect: no detached resources on cancellation.
        fn connect(&self) -> impl std::future::Future<Output = Result<bool, BrokerError>> + Send;
        async fn disconnect(&self) -> Result<bool, BrokerError>;
        fn is_connected(&self) -> bool;

        /// Market-data features supported by this data feed.
        fn get_features(&self) -> &'static [DataFeedFeature] {
            &[]
        }
    }

    pub trait DataProvider: DataFeed {
        /// Ticker metadata
        async fn get_ticker_info(&self, symbol: &str) -> Result<Asset, BrokerError>;

        /// Finite datasets can declare a symbol/timeframe unavailable so the
        /// backtest loader skips that stream. Network feeds keep their existing
        /// strict request/error behavior. This never substitutes another timeframe.
        async fn supports_historical_stream(
            &self, _symbol: &str, _timeframe: TimeFrame,
        ) -> Result<bool, BrokerError> {
            Ok(true)
        }

        /// Historical bar data as a Polars DataFrame.
        /// Columns: open (f64), high (f64), low (f64), close (f64), volume (f64), timestamp (datetime)
        async fn get_history(
            &self,
            symbol: &str,
            start: DateTime<chrono::Utc>,
            end: DateTime<chrono::Utc>,
            time_frame: TimeFrame,
        ) -> Result<DataFrame, BrokerError>;

        fn get_latest_quote(
            &self,
            symbol: &str,
        ) -> impl std::future::Future<Output = Result<Quote, BrokerError>> + Send;

        /// Subscribe independently of candle timeframes. Use a unique ID per owner;
        /// unsubscribing that ID must not cancel another owner's quotes or candles.
        /// Callbacks must be short/nonblocking. Check QuoteStream plus NativeBidAsk or
        /// SyntheticQuotes capabilities; this API does not promise every exchange tick.
        async fn subscribe_to_quote_stream(
            &self,
            _subscription_id: String,
            _symbol: String,
            _on_quote: super::QuoteCallback,
        ) -> Result<(), BrokerError> {
            Err(BrokerError::DataFeedError(
                "Quote streaming is not supported by this provider".into(),
            ))
        }

        async fn unsubscribe_from_quote_stream(
            &self,
            _subscription_id: &str,
        ) -> Result<(), BrokerError> {
            Ok(())
        }
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

        fn configure_live_session(&self, _session_id: &str) -> Result<(), BrokerError> {
            Ok(())
        }

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

                    DataFrame::new(
                        bars.len(),
                        vec![
                            Column::new("symbol".into(), &symbols),
                            Column::new("open".into(), &opens),
                            Column::new("high".into(), &highs),
                            Column::new("low".into(), &lows),
                            Column::new("close".into(), &closes),
                            Column::new("volume".into(), &volumes),
                            Column::new("timestamp".into(), &timestamps),
                        ],
                    )
                    .map_err(|e| e.to_string())
                }
            }
        }

        fn format_on_quote(&self, data: Vec<Quote>) -> Result<Vec<Quote>, String> {
            Ok(data)
        }
    }
}
