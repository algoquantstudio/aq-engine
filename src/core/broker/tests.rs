use super::traits::{DataFeed, DataProvider, OrderManagementProvider};
use super::types::{AccountType, Asset, Bar, BrokerError, OrderSide, OrderType, Quote, TradeUpdateEvent};
use super::{DataStreamMode, UnifiedBroker, paper_broker::PaperBroker};
use crate::core::insight::Insight;
use crate::core::insight::types::StrategyType;
use crate::core::utils::timeframe::TimeFrame;
use crate::core::utils::timeframe::TimeFrameUnit;
use chrono::Utc;
use polars::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

struct MockDataFeed {
    connected: Arc<Mutex<bool>>,
}

impl MockDataFeed {
    fn new() -> Self {
        Self {
            connected: Arc::new(Mutex::new(false)),
        }
    }
}

impl DataFeed for MockDataFeed {
    async fn connect(&self) -> Result<bool, BrokerError> {
        let mut connected = self.connected.lock().unwrap();
        *connected = true;
        Ok(true)
    }

    async fn disconnect(&self) -> Result<bool, BrokerError> {
        let mut connected = self.connected.lock().unwrap();
        *connected = false;
        Ok(true)
    }

    fn is_connected(&self) -> bool {
        *self.connected.lock().unwrap()
    }
}

impl DataProvider for MockDataFeed {
    async fn get_ticker_info(&self, _symbol: &str) -> Result<Asset, BrokerError> {
        Err(BrokerError::TradeError("Not implemented".into()))
    }
    async fn get_history(
        &self,
        _symbol: &str,
        _start: chrono::DateTime<chrono::Utc>,
        _end: chrono::DateTime<chrono::Utc>,
        _time_frame: TimeFrame,
    ) -> Result<DataFrame, BrokerError> {
        // Return empty DataFrame with correct schema
        let df = DataFrame::new(vec![
            Column::new("symbol".into(), Vec::<String>::new()),
            Column::new("open".into(), Vec::<f64>::new()),
            Column::new("high".into(), Vec::<f64>::new()),
            Column::new("low".into(), Vec::<f64>::new()),
            Column::new("close".into(), Vec::<f64>::new()),
            Column::new("volume".into(), Vec::<f64>::new()),
            Column::new("timestamp".into(), Vec::<i64>::new()),
        ])
        .map_err(|e| BrokerError::DataFeedError(e.to_string()))?;
        Ok(df)
    }
    async fn get_latest_quote(&self, _symbol: &str) -> Result<Quote, BrokerError> {
        Err(BrokerError::TradeError("Not implemented".into()))
    }
    async fn get_latest_bar(&self, _symbol: &str) -> Result<Bar, BrokerError> {
        Err(BrokerError::TradeError("Not implemented".into()))
    }
    async fn subscribe_to_data_stream(
        &self,
        _symbols: Vec<String>,
        _time_frame: TimeFrame,
        _mode: DataStreamMode,
        _on_bar: Arc<dyn Fn(Bar) + Send + Sync>,
    ) -> Result<(), BrokerError> {
        Ok(())
    }
    async fn unsubscribe_from_data_stream(&self, _symbols: Vec<String>) -> Result<(), BrokerError> {
        Ok(())
    }
}

#[tokio::test]
async fn test_unified_broker_connect() {
    let paper_broker = PaperBroker::new(AccountType::Paper, 1000.0, 1);
    let mock_data = MockDataFeed::new();

    let unified_broker = UnifiedBroker::new(paper_broker, mock_data);

    // Initial state
    assert!(!unified_broker.is_datafeed_connected());

    // Connect
    let _ = unified_broker.connect().await;

    // Check delegation
    assert!(unified_broker.is_connected());
    assert!(unified_broker.is_datafeed_connected());
}

#[tokio::test]
async fn test_paper_broker_trailing_stop_moves_and_closes_position() {
    let broker = PaperBroker::new(AccountType::Paper, 10_000.0, 1);

    let mut insight = Insight::new(
        OrderSide::Buy,
        "AAPL".to_string(),
        StrategyType::Testing,
        TimeFrame::new(1, TimeFrameUnit::Minute),
        80,
        None,
    );
    insight.set_quantity(Some(1.0));
    insight.set_trailing_stop_price(Some(5.0));

    let order = broker
        .submit_order(insight)
        .await
        .expect("paper broker should accept trailing-stop insight");

    let entry_time = Utc::now();
    let mut entry_bars = HashMap::new();
    entry_bars.insert(
        "AAPL".to_string(),
        Bar {
            symbol: "AAPL".to_string(),
            open: 100.0,
            high: 102.0,
            low: 99.0,
            close: 101.0,
            volume: 1_000.0,
            timestamp: entry_time,
        },
    );
    broker.process_step(&entry_bars, entry_time);

    let active_order = broker
        .get_order(&order.order_id)
        .await
        .expect("filled order should still be retrievable");
    let trailing_leg = active_order
        .legs
        .as_ref()
        .and_then(|legs| legs.trailing_stop.as_ref())
        .expect("trailing leg should be created after fill");
    assert_eq!(active_order.status, TradeUpdateEvent::Filled);
    assert_eq!(trailing_leg.trail_price, Some(5.0));
    assert_eq!(trailing_leg.limit_price, Some(97.0));

    let trend_time = entry_time + chrono::Duration::minutes(1);
    let mut trend_bars = HashMap::new();
    trend_bars.insert(
        "AAPL".to_string(),
        Bar {
            symbol: "AAPL".to_string(),
            open: 106.0,
            high: 110.0,
            low: 106.0,
            close: 109.0,
            volume: 1_000.0,
            timestamp: trend_time,
        },
    );
    broker.process_step(&trend_bars, trend_time);

    let updated_order = broker
        .get_order(&order.order_id)
        .await
        .expect("order should still be open after favorable move");
    let updated_trailing = updated_order
        .legs
        .as_ref()
        .and_then(|legs| legs.trailing_stop.as_ref())
        .expect("trailing leg should remain active");
    assert_eq!(updated_order.status, TradeUpdateEvent::Filled);
    assert_eq!(updated_trailing.limit_price, Some(105.0));

    let reversal_time = trend_time + chrono::Duration::minutes(1);
    let mut reversal_bars = HashMap::new();
    reversal_bars.insert(
        "AAPL".to_string(),
        Bar {
            symbol: "AAPL".to_string(),
            open: 106.0,
            high: 106.0,
            low: 104.0,
            close: 104.5,
            volume: 1_000.0,
            timestamp: reversal_time,
        },
    );
    broker.process_step(&reversal_bars, reversal_time);

    let closed_order = broker
        .get_order(&order.order_id)
        .await
        .expect("closed order should still be queryable");
    let closed_trailing = closed_order
        .legs
        .as_ref()
        .and_then(|legs| legs.trailing_stop.as_ref())
        .expect("trailing leg should remain attached");
    assert_eq!(closed_order.status, TradeUpdateEvent::Closed);
    assert_eq!(closed_order.filled_price, Some(105.0));
    assert_eq!(closed_trailing.status, TradeUpdateEvent::Filled);
    assert_eq!(closed_trailing.filled_price, Some(105.0));
    assert_eq!(closed_trailing.order_type, OrderType::TrailingStop);
}

#[tokio::test]
async fn test_paper_broker_short_trailing_stop_moves_and_closes_position() {
    let broker = PaperBroker::new(AccountType::Paper, 10_000.0, 1);

    let mut insight = Insight::new(
        OrderSide::Sell,
        "AAPL".to_string(),
        StrategyType::Testing,
        TimeFrame::new(1, TimeFrameUnit::Minute),
        80,
        None,
    );
    insight.set_quantity(Some(1.0));
    insight.set_trailing_stop_price(Some(5.0));

    let order = broker
        .submit_order(insight)
        .await
        .expect("paper broker should accept short trailing-stop insight");

    let entry_time = Utc::now();
    let mut entry_bars = HashMap::new();
    entry_bars.insert(
        "AAPL".to_string(),
        Bar {
            symbol: "AAPL".to_string(),
            open: 100.0,
            high: 101.0,
            low: 98.0,
            close: 99.0,
            volume: 1_000.0,
            timestamp: entry_time,
        },
    );
    broker.process_step(&entry_bars, entry_time);

    let active_order = broker
        .get_order(&order.order_id)
        .await
        .expect("filled short order should still be retrievable");
    let trailing_leg = active_order
        .legs
        .as_ref()
        .and_then(|legs| legs.trailing_stop.as_ref())
        .expect("short trailing leg should be created after fill");
    assert_eq!(active_order.status, TradeUpdateEvent::Filled);
    assert_eq!(trailing_leg.trail_price, Some(5.0));
    assert_eq!(trailing_leg.limit_price, Some(103.0));

    let trend_time = entry_time + chrono::Duration::minutes(1);
    let mut trend_bars = HashMap::new();
    trend_bars.insert(
        "AAPL".to_string(),
        Bar {
            symbol: "AAPL".to_string(),
            open: 97.0,
            high: 96.5,
            low: 92.0,
            close: 93.0,
            volume: 1_000.0,
            timestamp: trend_time,
        },
    );
    broker.process_step(&trend_bars, trend_time);

    let updated_order = broker
        .get_order(&order.order_id)
        .await
        .expect("short order should still be open after favorable move");
    let updated_trailing = updated_order
        .legs
        .as_ref()
        .and_then(|legs| legs.trailing_stop.as_ref())
        .expect("short trailing leg should remain active");
    assert_eq!(updated_order.status, TradeUpdateEvent::Filled);
    assert_eq!(updated_trailing.limit_price, Some(97.0));

    let reversal_time = trend_time + chrono::Duration::minutes(1);
    let mut reversal_bars = HashMap::new();
    reversal_bars.insert(
        "AAPL".to_string(),
        Bar {
            symbol: "AAPL".to_string(),
            open: 96.5,
            high: 97.5,
            low: 96.0,
            close: 97.25,
            volume: 1_000.0,
            timestamp: reversal_time,
        },
    );
    broker.process_step(&reversal_bars, reversal_time);

    let closed_order = broker
        .get_order(&order.order_id)
        .await
        .expect("closed short order should still be queryable");
    let closed_trailing = closed_order
        .legs
        .as_ref()
        .and_then(|legs| legs.trailing_stop.as_ref())
        .expect("short trailing leg should remain attached");
    assert_eq!(closed_order.status, TradeUpdateEvent::Closed);
    assert_eq!(closed_order.filled_price, Some(97.0));
    assert_eq!(closed_trailing.status, TradeUpdateEvent::Filled);
    assert_eq!(closed_trailing.filled_price, Some(97.0));
}

#[tokio::test]
async fn test_paper_broker_rejects_market_order_when_fill_would_overdraw_cash() {
    let broker = PaperBroker::new(AccountType::Paper, 100.0, 1);

    let mut insight = Insight::new(
        OrderSide::Buy,
        "AAPL".to_string(),
        StrategyType::Testing,
        TimeFrame::new(1, TimeFrameUnit::Minute),
        80,
        None,
    );
    insight.set_quantity(Some(2.0));

    let order = broker
        .submit_order(insight)
        .await
        .expect("market order should enter the pending queue before fill-time validation");
    assert_eq!(order.status, TradeUpdateEvent::Pending);

    let ts = Utc::now();
    let mut bars = HashMap::new();
    bars.insert(
        "AAPL".to_string(),
        Bar {
            symbol: "AAPL".to_string(),
            open: 120.0,
            high: 121.0,
            low: 119.0,
            close: 120.5,
            volume: 1_000.0,
            timestamp: ts,
        },
    );
    broker.process_step(&bars, ts);

    let rejected_order = broker
        .get_order(&order.order_id)
        .await
        .expect("rejected order should still be queryable");
    assert_eq!(rejected_order.status, TradeUpdateEvent::Rejected);
    assert!(
        rejected_order
            .rejection_reason
            .as_deref()
            .unwrap_or_default()
            .contains("Insufficient funds")
    );

    let account = broker
        .get_account()
        .await
        .expect("paper account should remain readable");
    assert_eq!(account.cash, 100.0);
    assert_eq!(account.buying_power, 100.0);

    let trade_events = broker.drain_trade_events();
    assert!(trade_events.iter().any(|(event_order, event)| {
        *event == TradeUpdateEvent::Rejected
            && event_order.order_id == order.order_id
            && event_order
                .rejection_reason
                .as_deref()
                .unwrap_or_default()
                .contains("Insufficient funds")
    }));
}
