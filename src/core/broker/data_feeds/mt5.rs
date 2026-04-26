use crate::core::broker::traits::{DataFeed, DataProvider};
use crate::core::broker::types::{Asset, Bar, BarData, BrokerError, Quote};
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
}

impl DataFeed for Mt5DataFeed {
    async fn connect(&self) -> Result<bool, BrokerError> {
        self.bridge.start().await
    }

    async fn disconnect(&self) -> Result<bool, BrokerError> {
        self.bridge.stop();
        Ok(true)
    }

    fn is_connected(&self) -> bool {
        self.bridge.is_connected()
    }
}

impl DataProvider for Mt5DataFeed {
    async fn get_ticker_info(&self, symbol: &str) -> Result<Asset, BrokerError> {
        Ok(self.bridge.asset(symbol))
    }

    async fn get_history(
        &self,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        _time_frame: TimeFrame,
    ) -> Result<DataFrame, BrokerError> {
        self.format_on_bar(BarData::Bars(self.bridge.history(symbol, start, end)))
            .map_err(BrokerError::DataFeedError)
    }

    async fn get_latest_quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
        self.bridge.latest_quote(symbol)
    }

    async fn get_latest_bar(&self, symbol: &str) -> Result<Bar, BrokerError> {
        self.bridge.latest_bar(symbol)
    }

    async fn subscribe_to_data_stream(
        &self,
        symbols: Vec<String>,
        _time_frame: TimeFrame,
        _mode: DataStreamMode,
        on_bar: Arc<dyn Fn(Bar) + Send + Sync>,
    ) -> Result<(), BrokerError> {
        self.bridge.subscribe_bars(symbols, on_bar);
        Ok(())
    }

    async fn unsubscribe_from_data_stream(&self, symbols: Vec<String>) -> Result<(), BrokerError> {
        self.bridge.unsubscribe_bars(symbols);
        Ok(())
    }
}
