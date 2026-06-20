use crate::core::broker::types::Bar;
use crate::core::utils::timeframe::TimeFrame;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub enum EventStreamType {
    Bar,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct EventStreamOptions {
    pub allow_trading: bool,
}

impl Default for EventStreamOptions {
    fn default() -> Self {
        Self {
            allow_trading: false,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct EventStreamRequest {
    pub event_type: EventStreamType,
    pub timeframe: Option<TimeFrame>,
    pub options: EventStreamOptions,
}

impl EventStreamRequest {
    pub fn new(event_type: EventStreamType, timeframe: Option<TimeFrame>) -> Self {
        Self {
            event_type,
            timeframe,
            options: EventStreamOptions::default(),
        }
    }

    pub fn bar(timeframe: Option<TimeFrame>) -> Self {
        Self::new(EventStreamType::Bar, timeframe)
    }

    pub fn allow_trading(mut self, allow_trading: bool) -> Self {
        self.options.allow_trading = allow_trading;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct MarketStreamKey {
    pub event_type: EventStreamType,
    pub symbol: String,
    pub timeframe: TimeFrame,
}

impl MarketStreamKey {
    pub fn new(
        event_type: EventStreamType,
        symbol: impl Into<String>,
        timeframe: TimeFrame,
    ) -> Self {
        Self {
            event_type,
            symbol: symbol.into(),
            timeframe,
        }
    }

    pub fn history_key(&self, main_timeframe: TimeFrame) -> String {
        if self.timeframe == main_timeframe {
            self.symbol.clone()
        } else {
            format!("{}:{}", self.symbol, self.timeframe.compact_label())
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct StrategyEventContext {
    pub event_type: EventStreamType,
    pub symbol: String,
    pub timeframe: TimeFrame,
    pub history_key: String,
    pub is_feature: bool,
    pub allow_trading: bool,
    pub timestamp: DateTime<Utc>,
}

impl StrategyEventContext {
    pub fn from_stream(stream: &ResolvedEventStream, timestamp: DateTime<Utc>) -> Self {
        Self {
            event_type: stream.key.event_type,
            symbol: stream.key.symbol.clone(),
            timeframe: stream.key.timeframe,
            history_key: stream.history_key.clone(),
            is_feature: stream.is_feature,
            allow_trading: stream.allow_trading,
            timestamp,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ResolvedEventStream {
    pub key: MarketStreamKey,
    pub history_key: String,
    pub is_feature: bool,
    pub allow_trading: bool,
}

impl ResolvedEventStream {
    pub fn new(
        event_type: EventStreamType,
        symbol: impl Into<String>,
        timeframe: TimeFrame,
        main_timeframe: TimeFrame,
        allow_trading: bool,
    ) -> Self {
        let key = MarketStreamKey::new(event_type, symbol, timeframe);
        let is_feature = timeframe != main_timeframe;
        let history_key = key.history_key(main_timeframe);
        Self {
            key,
            history_key,
            is_feature,
            allow_trading: allow_trading || !is_feature,
        }
    }

    pub fn context_at(&self, timestamp: DateTime<Utc>) -> StrategyEventContext {
        StrategyEventContext::from_stream(self, timestamp)
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MarketDataEvent {
    pub context: StrategyEventContext,
    pub bar: Bar,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct BacktestMarketStep {
    pub timestamp: DateTime<Utc>,
    pub events: Vec<MarketDataEvent>,
    pub execution_bars: HashMap<String, Bar>,
    pub has_tradable_events: bool,
}
