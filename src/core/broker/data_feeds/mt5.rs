use crate::core::broker::traits::{DataFeed, DataProvider};
use crate::core::broker::types::{Account, Asset, Bar, BrokerError, Quote};
use crate::core::broker::{DataStreamMode, mt5_bridge::Mt5Bridge};
use crate::core::utils::timeframe::TimeFrame;
use chrono::{DateTime, Utc};
use polars::prelude::*;
use std::sync::Arc;

pub struct Mt5DataFeed {
    bridge: Arc<Mt5Bridge>,
}

impl Mt5DataFeed {
    pub fn from_env() -> Result<Self, BrokerError> {
        Ok(Self::new(Mt5Bridge::shared_from_env()?))
    }

    pub fn new(bridge: Arc<Mt5Bridge>) -> Self {
        Self { bridge }
    }

    pub fn bridge(&self) -> &Arc<Mt5Bridge> {
        &self.bridge
    }

    pub async fn get_account(&self) -> Result<Account, BrokerError> {
        self.bridge.request_account().await
    }

    pub fn stop_bridge(&self) {
        self.bridge.stop();
    }

    pub async fn shutdown_bridge(&self) {
        self.bridge.shutdown().await;
    }
}

impl DataFeed for Mt5DataFeed {
    async fn connect(&self) -> Result<bool, BrokerError> {
        self.bridge.start().await?;
        self.bridge.wait_for_rpc_poll().await
    }

    async fn disconnect(&self) -> Result<bool, BrokerError> {
        self.bridge.shutdown().await;
        Ok(true)
    }

    fn is_connected(&self) -> bool {
        self.bridge.is_connected()
    }
}

impl DataProvider for Mt5DataFeed {
    fn configure_live_session(&self, session_id: &str) -> Result<(), BrokerError> {
        self.bridge.set_session_id(session_id);
        Ok(())
    }

    async fn get_ticker_info(&self, symbol: &str) -> Result<Asset, BrokerError> {
        self.bridge.request_asset(symbol).await
    }

    async fn get_history(
        &self,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        time_frame: TimeFrame,
    ) -> Result<DataFrame, BrokerError> {
        self.bridge
            .request_history(symbol, start, end, time_frame)
            .await
    }

    async fn get_latest_quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
        self.bridge.request_latest_quote(symbol).await
    }

    async fn get_latest_bar(&self, symbol: &str) -> Result<Bar, BrokerError> {
        self.bridge.request_latest_bar(symbol).await
    }

    async fn subscribe_to_data_stream(
        &self,
        symbols: Vec<String>,
        time_frame: TimeFrame,
        mode: DataStreamMode,
        on_bar: Arc<dyn Fn(Bar) + Send + Sync>,
    ) -> Result<(), BrokerError> {
        self.bridge
            .subscribe_bars_rpc(symbols, time_frame, mode, on_bar)
            .await
    }

    async fn unsubscribe_from_data_stream(&self, symbols: Vec<String>) -> Result<(), BrokerError> {
        self.bridge.unsubscribe_symbol_bars_rpc(symbols).await
    }
}
