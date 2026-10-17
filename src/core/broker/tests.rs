use super::traits::{Broker, DataFeed, DataProvider, OrderManagementProvider};
use super::types::{
    AccountType, Asset, AssetCommissionFees, AssetExchange, AssetFee, AssetFees, AssetSideFees,
    AssetStatus, AssetSwapFees, AssetType, Bar, BrokerError, OrderSide, OrderType, Quote,
    TradeUpdateEvent,
};
use super::{
    BrokerFeature, DataFeedFeature, DataStreamMode, UnifiedBroker, block_on_broker_future,
    data_feeds::yahoo::YahooFinanceDataFeed, paper_broker::PaperBroker,
};
use crate::core::insight::Insight;
use crate::core::insight::types::StrategyType;
use crate::core::utils::timeframe::TimeFrame;
use crate::core::utils::timeframe::TimeFrameUnit;
use crate::core::utils::tools::dynamic_round_for_asset;
use chrono::Utc;
use polars::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[test]
fn broker_future_bridge_runs_without_a_runtime() {
    assert_eq!(block_on_broker_future(async { 42 }), 42);
}

#[test]
fn unified_broker_routes_typed_feature_checks() {
    let broker = UnifiedBroker::new(
        PaperBroker::new(AccountType::Paper, 100_000.0, 1),
        YahooFinanceDataFeed::new(),
    );

    assert!(broker.has_features(&[
        BrokerFeature::MarketOrder,
        BrokerFeature::LimitOrder,
        BrokerFeature::OcoOrder,
        BrokerFeature::TrailingOrder,
        BrokerFeature::PartialClose,
        BrokerFeature::BuyingPowerLeveraged,
    ]));
    assert!(broker.has_features(&[
        DataFeedFeature::HistoricalCandles,
        DataFeedFeature::LatestCandle,
        DataFeedFeature::LatestQuote,
        DataFeedFeature::CandleStream,
        DataFeedFeature::IntraCandleStream,
    ]));
    assert!(broker.has_feature(DataFeedFeature::QuoteStream));
    assert!(broker.has_feature(DataFeedFeature::SyntheticQuotes));
    assert!(!broker.has_feature(DataFeedFeature::NativeBidAsk));
    assert!(!broker.has_feature(BrokerFeature::MaxTotalOrders));
    assert!(!broker.has_feature(BrokerFeature::MaxTotalSize));
    assert_eq!(broker.get_cached_account().unwrap().max_total_orders, None);
}

#[tokio::test]
async fn quote_replay_uses_only_consumed_bars_and_exact_leases() {
    let broker = UnifiedBroker::new_backtest(
        PaperBroker::new(AccountType::Paper, 100_000.0, 1),
        YahooFinanceDataFeed::new(),
    );
    assert!(broker.has_feature(DataFeedFeature::SyntheticQuotes));
    assert!(!broker.has_feature(DataFeedFeature::NativeBidAsk));
    assert!(broker.get_latest_quote("AAPL").await.is_err()); // Never fetch today's data.
    let seen = Arc::new(Mutex::new(Vec::new()));
    for id in ["one", "two"] {
        let seen = seen.clone();
        broker
            .subscribe_to_quote_stream(
                id.into(),
                "AAPL".into(),
                Arc::new(move |q| seen.lock().unwrap().push((id, q))),
            )
            .await
            .unwrap();
    }
    broker.unsubscribe_from_quote_stream("one").await.unwrap();
    let bar = Bar {
        symbol: "AAPL".into(),
        open: 99.0,
        high: 102.0,
        low: 98.0,
        close: 100.0,
        volume: 1234.0,
        timestamp: Utc::now() - chrono::Duration::days(30),
    };
    broker.publish_replay_quotes(&HashMap::from([("AAPL".into(), bar.clone())]));
    let quotes = seen.lock().unwrap();
    assert_eq!(quotes.len(), 1);
    assert_eq!(quotes[0].0, "two");
    assert_eq!(quotes[0].1.timestamp, bar.timestamp);
    assert_eq!(quotes[0].1.bid, 100.0);
    assert_eq!(quotes[0].1.ask, 100.0);
    assert_eq!(quotes[0].1.bid_size, 0.0);
    assert_eq!(quotes[0].1.last_size, None);
    drop(quotes);
    assert_eq!(broker.get_latest_quote("AAPL").await.unwrap().bid, 100.0);
    broker.unsubscribe_from_quote_stream("two").await.unwrap();
    assert!(broker.replay_quotes.read().subscriptions.is_empty());
}

#[tokio::test]
async fn quote_live_paper_delegates_to_feed_with_independent_lease_cleanup() {
    let broker = UnifiedBroker::new(
        PaperBroker::new(AccountType::Paper, 100_000.0, 1),
        YahooFinanceDataFeed::new(),
    );
    broker
        .subscribe_to_quote_stream("one".into(), "AAPL".into(), Arc::new(|_| {}))
        .await
        .unwrap();
    broker
        .subscribe_to_quote_stream("two".into(), "AAPL".into(), Arc::new(|_| {}))
        .await
        .unwrap();
    broker.unsubscribe_from_quote_stream("one").await.unwrap();
    broker.unsubscribe_from_quote_stream("two").await.unwrap();
    broker
        .unsubscribe_from_quote_stream("missing")
        .await
        .unwrap();
    assert!(broker.replay_quotes.read().subscriptions.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_future_bridge_releases_multithreaded_runtime_worker() {
    assert_eq!(block_on_broker_future(async { 42 }), 42);
}

#[tokio::test(flavor = "current_thread")]
async fn broker_future_bridge_supports_immediate_backtest_futures() {
    assert_eq!(block_on_broker_future(async { 42 }), 42);
}

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
        let df = DataFrame::new(
            0,
            vec![
                Column::new("symbol".into(), Vec::<String>::new()),
                Column::new("open".into(), Vec::<f64>::new()),
                Column::new("high".into(), Vec::<f64>::new()),
                Column::new("low".into(), Vec::<f64>::new()),
                Column::new("close".into(), Vec::<f64>::new()),
                Column::new("volume".into(), Vec::<f64>::new()),
                Column::new("timestamp".into(), Vec::<i64>::new()),
            ],
        )
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

struct ConcurrentHistoryFeed {
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
}

impl ConcurrentHistoryFeed {
    fn new() -> Self {
        Self {
            active: Arc::new(AtomicUsize::new(0)),
            max_active: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl DataFeed for ConcurrentHistoryFeed {
    async fn connect(&self) -> Result<bool, BrokerError> {
        Ok(true)
    }

    async fn disconnect(&self) -> Result<bool, BrokerError> {
        Ok(true)
    }

    fn is_connected(&self) -> bool {
        true
    }
}

impl DataProvider for ConcurrentHistoryFeed {
    async fn get_ticker_info(&self, _symbol: &str) -> Result<Asset, BrokerError> {
        Err(BrokerError::TradeError("Not implemented".into()))
    }

    async fn get_history(
        &self,
        symbol: &str,
        start: chrono::DateTime<chrono::Utc>,
        _end: chrono::DateTime<chrono::Utc>,
        _time_frame: TimeFrame,
    ) -> Result<DataFrame, BrokerError> {
        let index = symbol
            .strip_prefix("SYM")
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or_default();
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        let delay_ms = if index == 0 { 40 } else { 5 };
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        self.active.fetch_sub(1, Ordering::SeqCst);

        let timestamp = (start + chrono::Duration::minutes(index)).timestamp_millis();
        DataFrame::new(
            1,
            vec![
                Column::new("symbol".into(), vec![symbol.to_string()]),
                Column::new("open".into(), vec![100.0]),
                Column::new("high".into(), vec![101.0]),
                Column::new("low".into(), vec![99.0]),
                Column::new("close".into(), vec![100.5]),
                Column::new("volume".into(), vec![10.0]),
                Column::new("timestamp".into(), vec![timestamp]),
            ],
        )
        .map_err(|error| BrokerError::DataFeedError(error.to_string()))
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
async fn backtest_history_loads_are_bounded_concurrent_and_installed_in_order() {
    let feed = ConcurrentHistoryFeed::new();
    let max_active = feed.max_active.clone();
    let paper_broker = PaperBroker::new(AccountType::Paper, 1_000.0, 1);
    let mut broker = UnifiedBroker::new_backtest(paper_broker, feed);
    let symbols = (0..12)
        .map(|index| format!("SYM{index}"))
        .collect::<Vec<_>>();
    let start = Utc::now();
    let end = start + chrono::Duration::days(1);

    broker
        .load_backtest_data(
            &symbols,
            start,
            end,
            TimeFrame::new(1, TimeFrameUnit::Minute),
        )
        .await
        .unwrap();

    let observed_max = max_active.load(Ordering::SeqCst);
    assert!(observed_max > 1);
    assert!(observed_max <= super::MAX_CONCURRENT_HISTORY_LOADS);
    let state = broker.backtest_state.as_ref().unwrap().read();
    assert_eq!(state.historical_bars.len(), symbols.len());
    let expected_current_time = chrono::DateTime::from_timestamp_millis(
        (start + chrono::Duration::minutes((symbols.len() - 1) as i64)).timestamp_millis(),
    )
    .unwrap();
    assert_eq!(state.current_time, expected_current_time);
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
        .submit_order(insight.into())
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
async fn test_paper_broker_trailing_stop_does_not_use_same_bar_extremes() {
    let broker = PaperBroker::new(AccountType::Paper, 10_000.0, 1);
    let mut insight = Insight::new(
        OrderSide::Buy,
        "AAPL".to_string(),
        StrategyType::Testing,
        TimeFrame::new(1, TimeFrameUnit::Minute),
        80,
        None,
    );
    insight
        .set_quantity(Some(1.0))
        .set_trailing_stop_price(Some(5.0));
    let order = broker.submit_order(insight.into()).await.unwrap();

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

    // The bar reaches a new high and later contains a low below the newly
    // calculated trail. OHLC does not establish that the high came first, so
    // only the existing 97.0 stop may be evaluated this bar.
    let mut ambiguous_bars = HashMap::new();
    ambiguous_bars.insert(
        "AAPL".to_string(),
        Bar {
            symbol: "AAPL".to_string(),
            open: 104.0,
            high: 110.0,
            low: 104.0,
            close: 109.0,
            volume: 1_000.0,
            timestamp: entry_time + chrono::Duration::minutes(1),
        },
    );
    broker.process_step(&ambiguous_bars, entry_time + chrono::Duration::minutes(1));

    let active_order = broker.get_order(&order.order_id).await.unwrap();
    let trailing = active_order
        .legs
        .as_ref()
        .and_then(|legs| legs.trailing_stop.as_ref())
        .unwrap();
    assert_eq!(active_order.status, TradeUpdateEvent::Filled);
    assert_eq!(trailing.limit_price, Some(105.0));
}

#[tokio::test]
async fn test_paper_broker_update_stop_loss_moves_active_stop_leg() {
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
    insight.set_stop_loss(Some(95.0));
    insight.set_take_profit_levels(Some(vec![120.0]));

    let order = broker
        .submit_order(insight.into())
        .await
        .expect("paper broker should accept bracket insight");

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

    broker
        .update_stop_loss(&order.order_id, 97.0, 1.0)
        .await
        .expect("stop-loss update should succeed");

    let updated_order = broker
        .get_order(&order.order_id)
        .await
        .expect("filled order should still be retrievable");
    let updated_legs = updated_order.legs.as_ref().expect("legs should remain");
    assert_eq!(
        updated_legs
            .stop_loss
            .as_ref()
            .and_then(|leg| leg.limit_price),
        Some(97.0)
    );
    assert_eq!(
        updated_legs
            .take_profit
            .as_ref()
            .and_then(|leg| leg.limit_price),
        Some(120.0)
    );

    let stop_time = entry_time + chrono::Duration::minutes(1);
    let mut stop_bars = HashMap::new();
    stop_bars.insert(
        "AAPL".to_string(),
        Bar {
            symbol: "AAPL".to_string(),
            open: 98.0,
            high: 99.0,
            low: 96.5,
            close: 97.5,
            volume: 1_000.0,
            timestamp: stop_time,
        },
    );
    broker.process_step(&stop_bars, stop_time);

    let closed_order = broker
        .get_order(&order.order_id)
        .await
        .expect("closed order should still be queryable");
    let closed_stop = closed_order
        .legs
        .as_ref()
        .and_then(|legs| legs.stop_loss.as_ref())
        .expect("stop-loss leg should remain attached");
    assert_eq!(closed_order.status, TradeUpdateEvent::Closed);
    assert_eq!(closed_order.filled_price, Some(97.0));
    assert_eq!(closed_stop.status, TradeUpdateEvent::Filled);
    assert_eq!(closed_stop.filled_price, Some(97.0));
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
        .submit_order(insight.into())
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
        .submit_order(insight.into())
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

#[tokio::test]
async fn test_paper_broker_applies_side_specific_commission_and_swap_fees() {
    let fees = AssetFees {
        commission: AssetCommissionFees {
            entry: AssetSideFees {
                long: AssetFee::PercentagePerContractFee(0.001),
                short: AssetFee::PerContractFee(3.0),
            },
            exit: AssetSideFees {
                long: AssetFee::FixedFee(2.0),
                short: AssetFee::PercentageFee(0.001),
            },
        },
        swap: AssetSwapFees {
            long: AssetFee::Points(-1.0),
            short: AssetFee::PercentagePerContractFee(0.0001),
        },
        ..AssetFees::default()
    };
    let mut broker = PaperBroker::new(AccountType::Paper, 10_000.0, 1).with_asset_fees(fees);
    let saved = Arc::new(parking_lot::RwLock::new(super::backtest_state::BacktestState::new()));
    broker.set_backtest_state(saved.clone());
    broker
        .configure_asset_metadata(&Asset {
            id: "asset-aapl".to_string(),
            symbol: "AAPL".to_string(),
            name: "AAPL".to_string(),
            asset_type: AssetType::Stock,
            status: AssetStatus::Active,
            exchange: AssetExchange::NASDAQ,
            tradable: true,
            marginable: true,
            shortable: true,
            fractional: true,
            min_order_size: None,
            quantity_base: None,
            max_order_size: None,
            max_total_size: None,
            min_price_increment: Some(0.01),
            price_base: None,
            contract_size: Some(10),
            trading_calendar: Some(r#"{"kind":"sessions","source":"fixture_feed","weekly_seconds":[[],[[32400,61200]]]}"#.into()),
            fees: AssetFees::default(),
        })
        .expect("paper broker should accept asset metadata");

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
        .submit_order(insight.into())
        .await
        .expect("paper broker should accept fee test insight");
    // Paper execution must retain the feed's generic metadata, including when
    // applying its own fee model. The same asset goes into lifecycle evidence.
    let calendar = order.asset.trading_calendar.clone().expect("recorded feed calendar");
    assert_eq!(serde_json::from_str::<serde_json::Value>(&calendar).unwrap()["source"],"fixture_feed");
    let mut legacy=serde_json::to_value(&order.asset).unwrap();
    legacy.as_object_mut().unwrap().remove("trading_calendar");
    assert!(serde_json::from_value::<Asset>(legacy).unwrap().trading_calendar.is_none());

    let entry_time = Utc::now();
    let mut entry_bars = HashMap::new();
    entry_bars.insert(
        "AAPL".to_string(),
        Bar {
            symbol: "AAPL".to_string(),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
            volume: 1_000.0,
            timestamp: entry_time,
        },
    );
    broker.process_step(&entry_bars, entry_time);
    let filled_events = broker.drain_trade_events();
    let filled_order = filled_events
        .iter()
        .find_map(|(event_order, event)| {
            (*event == TradeUpdateEvent::Filled).then_some(event_order)
        })
        .expect("filled trade event should be emitted");

    assert_eq!(
        dynamic_round_for_asset(
            filled_order.commission.unwrap_or_default(),
            &filled_order.asset
        ),
        dynamic_round_for_asset(2.0, &filled_order.asset),
        "entry commission should use registered contract size"
    );

    broker
        .close_position(&order.order_id, 0.0, Some(110.0))
        .await
        .expect("paper broker should queue close request");

    let close_time = entry_time + chrono::Duration::days(1);
    let mut close_bars = HashMap::new();
    close_bars.insert(
        "AAPL".to_string(),
        Bar {
            symbol: "AAPL".to_string(),
            open: 110.0,
            high: 111.0,
            low: 109.0,
            close: 110.0,
            volume: 1_000.0,
            timestamp: close_time,
        },
    );
    broker.process_step(&close_bars, close_time);

    let closed_events = broker.drain_trade_events();
    let closed_order = closed_events
        .iter()
        .find_map(|(event_order, event)| {
            (*event == TradeUpdateEvent::Closed).then_some(event_order)
        })
        .expect("closed trade event should be emitted");

    let expected_entry_commission = 2.0;
    let expected_exit_commission = 2.0;
    let expected_swap = -0.2;
    let expected_net_pnl =
        20.0 + expected_swap - expected_entry_commission - expected_exit_commission;

    assert_eq!(
        dynamic_round_for_asset(
            closed_order.commission.unwrap_or_default(),
            &closed_order.asset
        ),
        dynamic_round_for_asset(
            expected_entry_commission + expected_exit_commission,
            &closed_order.asset,
        )
    );
    assert_eq!(
        dynamic_round_for_asset(closed_order.swap.unwrap_or_default(), &closed_order.asset),
        dynamic_round_for_asset(expected_swap, &closed_order.asset)
    );
    assert_eq!(
        dynamic_round_for_asset(
            closed_order.realized_pnl.unwrap_or_default(),
            &closed_order.asset
        ),
        dynamic_round_for_asset(expected_net_pnl, &closed_order.asset)
    );

    let account = broker
        .get_account()
        .await
        .expect("paper account should remain readable");
    assert_eq!(
        dynamic_round_for_asset(account.accrued_commission, &closed_order.asset),
        dynamic_round_for_asset(
            expected_entry_commission + expected_exit_commission,
            &closed_order.asset,
        )
    );
    let state = saved.read();
    assert_eq!(state.trade_log.len(), 2);
    let entry = &state.trade_log[0];
    let exit = &state.trade_log[1];
    assert_eq!(entry.execution_cash.as_ref().unwrap().net(), Some(-2.0));
    assert_eq!(exit.execution_cash.as_ref().unwrap().gross_pnl, Some(20.0));
    assert_eq!(entry.account_notional, Some(200.0));
    assert_eq!(exit.account_notional, Some(220.0));
    assert_eq!(exit.valuation_basis.as_deref(), Some("paper_price_units"));
    let net: f64 = state.trade_log.iter().map(|r| r.execution_cash.as_ref().unwrap().net().unwrap()).sum();
    assert!((net - expected_net_pnl).abs() < 1e-10, "entry fees must not be charged twice");
    assert!((net - (account.cash - 10_000.0)).abs() < 1e-10);
}

#[tokio::test]
async fn paper_fill_cash_records_partial_short_exits_without_recharging_entry_fees() {
    let mut broker=PaperBroker::new(AccountType::Paper,10_000.,1).with_asset_fees(AssetFees {
        commission:AssetCommissionFees {
            entry:AssetSideFees {short:AssetFee::FixedFee(4.),..Default::default()},
            exit:AssetSideFees {short:AssetFee::FixedFee(1.),..Default::default()},
        },..Default::default()
    });
    let saved=Arc::new(parking_lot::RwLock::new(super::backtest_state::BacktestState::new()));
    broker.set_backtest_state(saved.clone());
    let mut insight=Insight::new(OrderSide::Sell,"CUSTOM".into(),StrategyType::Testing,TimeFrame::new(1,TimeFrameUnit::Minute),80,None);
    insight.set_quantity(Some(2.));
    let order=broker.submit_order(insight.into()).await.unwrap();
    let start=Utc::now();
    let step=|price:f64,minute:i64| {
        let at=start+chrono::Duration::minutes(minute);
        let bars=HashMap::from([("CUSTOM".into(),Bar {symbol:"CUSTOM".into(),open:price,high:price,low:price,close:price,volume:1000.,timestamp:at})]);
        broker.process_step(&bars,at);
    };
    step(100.,0);
    broker.close_position(&order.order_id,1.,Some(90.)).await.unwrap();
    step(90.,1);
    {
        let state=saved.read();
        assert_eq!(state.trade_log.len(),2);
        assert_eq!(state.trade_log[0].execution_cash.as_ref().unwrap().net(),Some(-4.));
        assert_eq!(state.trade_log[1].execution_cash.as_ref().unwrap().net(),Some(9.));
        assert_eq!(state.trade_log[1].qty,1.);
    }
    broker.close_position(&order.order_id,0.,Some(95.)).await.unwrap();
    step(95.,2);
    let account=broker.get_account().await.unwrap();
    let state=saved.read();
    assert_eq!(state.trade_log.len(),3);
    assert_eq!(state.trade_log[2].execution_cash.as_ref().unwrap().net(),Some(4.));
    assert_eq!(state.trade_log.iter().map(|r|r.account_notional.unwrap()).sum::<f64>(),385.);
    let net=state.trade_log.iter().map(|r|r.execution_cash.as_ref().unwrap().net().unwrap()).sum::<f64>();
    assert_eq!(net,9.);
    assert!((account.cash-10_000.-net).abs()<1e-10);
}
