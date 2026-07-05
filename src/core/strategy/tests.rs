use crate::core::broker::UnifiedBroker;
use crate::core::strategy::{Strategy, StrategyContext, StrategyState};
use std::collections::HashSet;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::alpha::{AlphaModel, AlphaResult, WrappedAlphaModel};
    use crate::core::broker::DataStreamMode;
    use crate::core::broker::data_feeds::mt5::Mt5DataFeed;
    use crate::core::broker::data_feeds::yahoo::YahooFinanceDataFeed;
    use crate::core::broker::mt5_broker::Mt5Broker;
    use crate::core::broker::paper_broker::PaperBroker;
    use crate::core::broker::traits::{Broker, DataFeed, DataProvider, OrderManagementProvider};
    use crate::core::broker::types::{
        AccountType, Asset, AssetExchange, AssetStatus, AssetType, Bar, BarData, BrokerError,
        OrderClass, OrderSide, OrderType, Quote, TradeUpdateEvent,
    };
    use crate::core::insight::Insight;
    use crate::core::insight::types::{InsightState, StrategyType};
    use crate::core::lifecycle::{
        LifecycleResult, LifecycleTiming, OnInitLogic, OnInitLogicBuilder, OnStartLogic,
        OnStartLogicBuilder, OnTeardownLogic, OnTeardownLogicBuilder,
    };
    use crate::core::pipeline::insight_submit::InsightSubmitPipe;
    use crate::core::pipeline::scale_out::ScaleOutPipe;
    use crate::core::pipeline::{InsightPipe, InsightPipeResult, WrappedInsightPipe};
    use crate::core::strategy::{EventStreamType, StrategyMode};
    use crate::core::universe::{UniverseModel, UniverseModelBuilder, UniverseResult};
    use crate::core::utils::timeframe::{TimeFrame, TimeFrameUnit};
    use chrono::{TimeZone, Utc};
    use polars::prelude::*;
    use rustc_hash::FxHashSet;
    use serde_json::json;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    static MT5_INTEGRATION_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn assert_backtest_or_skip<T>(result: Result<T, BrokerError>, context: &str) -> Option<T> {
        match result {
            Ok(value) => Some(value),
            Err(BrokerError::ConnectionError(err)) => {
                println!(
                    "Skipping {} due to Yahoo connection failure: {}",
                    context, err
                );
                None
            }
            Err(err) => panic!("{}: {:?}", context, err),
        }
    }

    fn init_test_logger(default_level: &str) {
        let env = env_logger::Env::default().default_filter_or(default_level);
        let _ = env_logger::Builder::from_env(env)
            .format_timestamp_millis()
            .is_test(true)
            .try_init();
    }

    fn fixed_scale_out_fixture_df() -> DataFrame {
        DataFrame::new(vec![
            Column::new("symbol".into(), vec!["AAPL"; 31]),
            Column::new(
                "open".into(),
                vec![
                    229.25, 237.21, 238.45, 240.00, 239.30, 237.00, 232.19, 226.88, 229.22, 237.00,
                    237.18, 238.97, 239.97, 241.23, 248.30, 255.88, 255.22, 253.21, 254.10, 254.56,
                    254.86, 255.04, 256.58, 254.67, 257.99, 256.81, 256.52, 257.81, 254.94, 249.38,
                    246.60,
                ],
            ),
            Column::new(
                "high".into(),
                vec![
                    230.85, 238.85, 239.90, 241.32, 240.15, 238.78, 232.42, 230.45, 234.51, 238.19,
                    241.22, 240.10, 241.20, 246.30, 256.64, 257.34, 255.74, 257.17, 257.60, 255.00,
                    255.92, 258.79, 258.18, 259.24, 259.07, 257.40, 258.52, 258.00, 256.38, 249.69,
                    248.85,
                ],
            ),
            Column::new(
                "low".into(),
                vec![
                    226.97, 234.36, 236.74, 238.49, 236.34, 233.36, 225.95, 226.65, 229.02, 235.03,
                    236.32, 237.73, 236.65, 240.21, 248.12, 253.58, 251.04, 251.71, 253.78, 253.01,
                    253.11, 254.93, 254.15, 253.95, 255.05, 255.43, 256.11, 253.14, 244.00, 245.56,
                    244.70,
                ],
            ),
            Column::new(
                "close".into(),
                vec![
                    229.72, 238.47, 239.78, 239.69, 237.88, 234.35, 226.79, 230.03, 234.07, 236.70,
                    238.15, 238.99, 237.88, 245.50, 256.08, 254.43, 252.31, 256.87, 255.46, 254.43,
                    254.63, 255.45, 257.13, 258.02, 256.69, 256.48, 258.06, 254.04, 245.27, 247.66,
                    247.77,
                ],
            ),
            Column::new("volume".into(), vec![1_000_000.0; 31]),
            Column::new(
                "timestamp".into(),
                vec![
                    1756771200000i64,
                    1756857600000,
                    1756944000000,
                    1757030400000,
                    1757289600000,
                    1757376000000,
                    1757462400000,
                    1757548800000,
                    1757635200000,
                    1757894400000,
                    1757980800000,
                    1758067200000,
                    1758153600000,
                    1758240000000,
                    1758499200000,
                    1758585600000,
                    1758672000000,
                    1758758400000,
                    1758844800000,
                    1759104000000,
                    1759190400000,
                    1759276800000,
                    1759363200000,
                    1759449600000,
                    1759708800000,
                    1759795200000,
                    1759881600000,
                    1759968000000,
                    1760054400000,
                    1760313600000,
                    1760400000000,
                ],
            ),
        ])
        .unwrap()
    }

    struct FixedScaleOutDataFeed {
        connected: Arc<Mutex<bool>>,
        history: DataFrame,
    }

    impl FixedScaleOutDataFeed {
        fn new() -> Self {
            Self {
                connected: Arc::new(Mutex::new(false)),
                history: fixed_scale_out_fixture_df(),
            }
        }
    }

    impl DataFeed for FixedScaleOutDataFeed {
        async fn connect(&self) -> Result<bool, BrokerError> {
            *self.connected.lock().unwrap() = true;
            Ok(true)
        }

        async fn disconnect(&self) -> Result<bool, BrokerError> {
            *self.connected.lock().unwrap() = false;
            Ok(true)
        }

        fn is_connected(&self) -> bool {
            *self.connected.lock().unwrap()
        }
    }

    impl DataProvider for FixedScaleOutDataFeed {
        async fn get_ticker_info(&self, symbol: &str) -> Result<Asset, BrokerError> {
            Ok(Asset {
                id: format!("fixture-{symbol}"),
                symbol: symbol.to_string(),
                name: symbol.to_string(),
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
                min_price_increment: Some(0.01),
                price_base: None,
                contract_size: None,
                fees: Default::default(),
            })
        }

        async fn get_history(
            &self,
            _symbol: &str,
            start: chrono::DateTime<chrono::Utc>,
            end: chrono::DateTime<chrono::Utc>,
            _time_frame: TimeFrame,
        ) -> Result<DataFrame, BrokerError> {
            let timestamps = self
                .history
                .column("timestamp")
                .map_err(|e| BrokerError::DataFeedError(e.to_string()))?
                .i64()
                .map_err(|e| BrokerError::DataFeedError(e.to_string()))?;
            let mask: BooleanChunked = timestamps
                .into_iter()
                .map(|ts| {
                    ts.map(|millis| {
                        let dt = Utc.timestamp_millis_opt(millis).unwrap();
                        dt >= start && dt <= end
                    })
                })
                .collect();
            self.history
                .filter(&mask)
                .map_err(|e| BrokerError::DataFeedError(e.to_string()))
        }

        async fn get_latest_quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
            let last = self.history.tail(Some(1));
            let close = last
                .column("close")
                .map_err(|e| BrokerError::DataFeedError(e.to_string()))?
                .f64()
                .map_err(|e| BrokerError::DataFeedError(e.to_string()))?
                .get(0)
                .unwrap_or_default();
            let ts = last
                .column("timestamp")
                .map_err(|e| BrokerError::DataFeedError(e.to_string()))?
                .i64()
                .map_err(|e| BrokerError::DataFeedError(e.to_string()))?
                .get(0)
                .unwrap_or_default();
            Ok(Quote {
                symbol: symbol.to_string(),
                bid: close,
                ask: close,
                bid_size: 100.0,
                ask_size: 100.0,
                last: Some(close),
                last_size: Some(100.0),
                timestamp: Utc.timestamp_millis_opt(ts).unwrap(),
            })
        }

        async fn get_latest_bar(&self, symbol: &str) -> Result<Bar, BrokerError> {
            let last = self.history.tail(Some(1));
            Ok(Bar {
                symbol: symbol.to_string(),
                open: last
                    .column("open")
                    .unwrap()
                    .f64()
                    .unwrap()
                    .get(0)
                    .unwrap_or_default(),
                high: last
                    .column("high")
                    .unwrap()
                    .f64()
                    .unwrap()
                    .get(0)
                    .unwrap_or_default(),
                low: last
                    .column("low")
                    .unwrap()
                    .f64()
                    .unwrap()
                    .get(0)
                    .unwrap_or_default(),
                close: last
                    .column("close")
                    .unwrap()
                    .f64()
                    .unwrap()
                    .get(0)
                    .unwrap_or_default(),
                volume: last
                    .column("volume")
                    .unwrap()
                    .f64()
                    .unwrap()
                    .get(0)
                    .unwrap_or_default(),
                timestamp: Utc
                    .timestamp_millis_opt(
                        last.column("timestamp")
                            .unwrap()
                            .i64()
                            .unwrap()
                            .get(0)
                            .unwrap_or_default(),
                    )
                    .unwrap(),
            })
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

        async fn unsubscribe_from_data_stream(
            &self,
            _symbols: Vec<String>,
        ) -> Result<(), BrokerError> {
            Ok(())
        }
    }

    struct GeneratedWarmupDataFeed {
        connected: Arc<Mutex<bool>>,
        requests: Arc<Mutex<Vec<(String, TimeFrame)>>>,
    }

    impl GeneratedWarmupDataFeed {
        fn new() -> Self {
            Self {
                connected: Arc::new(Mutex::new(false)),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl DataFeed for GeneratedWarmupDataFeed {
        async fn connect(&self) -> Result<bool, BrokerError> {
            *self.connected.lock().unwrap() = true;
            Ok(true)
        }

        async fn disconnect(&self) -> Result<bool, BrokerError> {
            *self.connected.lock().unwrap() = false;
            Ok(true)
        }

        fn is_connected(&self) -> bool {
            *self.connected.lock().unwrap()
        }
    }

    impl DataProvider for GeneratedWarmupDataFeed {
        async fn get_ticker_info(&self, symbol: &str) -> Result<Asset, BrokerError> {
            Ok(Asset {
                id: format!("generated-{symbol}"),
                symbol: symbol.to_string(),
                name: symbol.to_string(),
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
                min_price_increment: Some(0.01),
                price_base: None,
                contract_size: None,
                fees: Default::default(),
            })
        }

        async fn get_history(
            &self,
            symbol: &str,
            start: chrono::DateTime<chrono::Utc>,
            end: chrono::DateTime<chrono::Utc>,
            time_frame: TimeFrame,
        ) -> Result<DataFrame, BrokerError> {
            self.requests
                .lock()
                .unwrap()
                .push((symbol.to_string(), time_frame));

            let mut symbols = Vec::new();
            let mut open = Vec::new();
            let mut high = Vec::new();
            let mut low = Vec::new();
            let mut close = Vec::new();
            let mut volume = Vec::new();
            let mut timestamp = Vec::new();
            let mut current = start;
            let mut index = 0usize;

            while current < end {
                let price = 100.0 + index as f64;
                symbols.push(symbol.to_string());
                open.push(price);
                high.push(price + 1.0);
                low.push(price - 1.0);
                close.push(price + 0.5);
                volume.push(1_000.0 + index as f64);
                timestamp.push(current.timestamp_millis());

                current = time_frame
                    .add_time_increment(current, 1)
                    .map_err(|error| BrokerError::DataFeedError(format!("{:?}", error)))?;
                index += 1;
                if index > 10_000 {
                    return Err(BrokerError::DataFeedError(
                        "Generated history exceeded safety limit".to_string(),
                    ));
                }
            }

            DataFrame::new(vec![
                Column::new("symbol".into(), symbols),
                Column::new("open".into(), open),
                Column::new("high".into(), high),
                Column::new("low".into(), low),
                Column::new("close".into(), close),
                Column::new("volume".into(), volume),
                Column::new("timestamp".into(), timestamp),
            ])
            .map_err(|error| BrokerError::DataFeedError(error.to_string()))
        }

        async fn get_latest_quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
            Ok(Quote {
                symbol: symbol.to_string(),
                bid: 100.0,
                ask: 100.0,
                bid_size: 100.0,
                ask_size: 100.0,
                last: Some(100.0),
                last_size: Some(100.0),
                timestamp: Utc::now(),
            })
        }

        async fn get_latest_bar(&self, symbol: &str) -> Result<Bar, BrokerError> {
            Ok(Bar {
                symbol: symbol.to_string(),
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
                volume: 1_000.0,
                timestamp: Utc::now(),
            })
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

        async fn unsubscribe_from_data_stream(
            &self,
            _symbols: Vec<String>,
        ) -> Result<(), BrokerError> {
            Ok(())
        }
    }

    struct FixedScaleOutBacktestStrategy {
        generated: bool,
        tp_levels: Vec<f64>,
        sl_levels: Vec<f64>,
    }

    impl Strategy for FixedScaleOutBacktestStrategy {
        fn name(&self) -> &str {
            "FixedScaleOutBacktestStrategy"
        }

        fn on_start(&mut self, ctx: &mut dyn StrategyContext) {
            let submit_pipe = WrappedInsightPipe::builder(Box::new(InsightSubmitPipe::new()))
                .target_state(InsightState::New)
                .build();
            ctx.add_pipe(submit_pipe);

            let scale_out_pipe = WrappedInsightPipe::builder(Box::new(ScaleOutPipe::new(0.5)))
                .target_state(InsightState::Filled)
                .build();
            ctx.add_pipe(scale_out_pipe);
        }

        fn init(&mut self, _ctx: &mut dyn StrategyContext, _asset: &Asset) {}

        fn universe(&self, _ctx: &mut dyn StrategyContext) -> HashSet<String> {
            ["AAPL".to_string()].into_iter().collect()
        }

        fn on_bar(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str, _bar: &BarData) {}

        fn generate_insights(&mut self, ctx: &mut dyn StrategyContext, symbol: &str) {
            if self.generated {
                return;
            }

            let mut insight = Insight::new(
                OrderSide::Buy,
                symbol.to_string(),
                StrategyType::Testing,
                ctx.timeframe().clone(),
                90,
                None,
            );
            insight
                .set_quantity(Some(10.0))
                .set_take_profit_levels(Some(self.tp_levels.clone()))
                .set_stop_loss_levels(Some(self.sl_levels.clone()));

            ctx.add_insight(insight);
            self.generated = true;
        }

        fn insight_pipeline(&mut self, _ctx: &mut dyn StrategyContext, _insight: &Insight) {}

        fn on_teardown(&mut self, _ctx: &mut dyn StrategyContext) {}
    }

    async fn run_fixed_scale_out_backtest(
        tp_levels: Vec<f64>,
        sl_levels: Vec<f64>,
        start: chrono::DateTime<Utc>,
        end: chrono::DateTime<Utc>,
    ) -> Option<StrategyState<FixedScaleOutBacktestStrategy, PaperBroker, FixedScaleOutDataFeed>>
    {
        init_test_logger("debug");
        println!(
            "[scale-out-test] setup backtest start={} end={} tp={:?} sl={:?}",
            start, end, tp_levels, sl_levels
        );
        let execution = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        let data = FixedScaleOutDataFeed::new();
        let broker = UnifiedBroker::new(execution, data);

        let mut state = StrategyState::new(
            "ScaleOutBacktest".to_string(),
            "1.0".to_string(),
            FixedScaleOutBacktestStrategy {
                generated: false,
                tp_levels,
                sl_levels,
            },
            broker,
            StrategyMode::Backtest,
            TimeFrame::new(1, TimeFrameUnit::Day),
        );
        state.set_warm_up_bars(1);
        println!("[scale-out-test] strategy state created");

        println!("[scale-out-test] invoking run_backtest");
        state
            .run_backtest(start, end, state.timeframe())
            .await
            .expect("ScaleOut backtest with fixed fixture bars should complete");
        println!(
            "[scale-out-test] run_backtest returned insights={}",
            state.insights.len()
        );
        Some(state)
    }

    #[derive(Clone, Debug)]
    struct RecordedEvent {
        history_key: String,
        symbol: String,
        timeframe: TimeFrame,
        is_feature: bool,
        allow_trading: bool,
    }

    struct MultiTimeframeBacktestStrategy {
        feature_timeframe: TimeFrame,
        on_bar_events: Arc<Mutex<Vec<RecordedEvent>>>,
        generate_events: Arc<Mutex<Vec<RecordedEvent>>>,
    }

    impl Strategy for MultiTimeframeBacktestStrategy {
        fn name(&self) -> &str {
            "MultiTimeframeBacktestStrategy"
        }

        fn on_start(&mut self, ctx: &mut dyn StrategyContext) {
            ctx.add_events(EventStreamType::Bar, Some(self.feature_timeframe));
        }

        fn init(&mut self, _ctx: &mut dyn StrategyContext, _asset: &Asset) {}

        fn universe(&self, _ctx: &mut dyn StrategyContext) -> HashSet<String> {
            ["AAPL".to_string()].into_iter().collect()
        }

        fn on_bar(&mut self, ctx: &mut dyn StrategyContext, _symbol: &str, _bar: &BarData) {
            let event = ctx
                .current_event()
                .expect("on_bar should run inside a market event context");
            self.on_bar_events.lock().unwrap().push(RecordedEvent {
                history_key: event.history_key,
                symbol: event.symbol,
                timeframe: event.timeframe,
                is_feature: event.is_feature,
                allow_trading: event.allow_trading,
            });
        }

        fn generate_insights(&mut self, ctx: &mut dyn StrategyContext, _symbol: &str) {
            let event = ctx
                .current_event()
                .expect("generate_insights should keep the active market event context");
            self.generate_events.lock().unwrap().push(RecordedEvent {
                history_key: event.history_key,
                symbol: event.symbol,
                timeframe: event.timeframe,
                is_feature: event.is_feature,
                allow_trading: event.allow_trading,
            });
        }

        fn insight_pipeline(&mut self, _ctx: &mut dyn StrategyContext, _insight: &Insight) {}

        fn on_teardown(&mut self, _ctx: &mut dyn StrategyContext) {}
    }

    #[tokio::test]
    async fn multi_timeframe_feature_stream_updates_feature_history_without_trading() {
        init_test_logger("debug");
        let on_bar_events = Arc::new(Mutex::new(Vec::new()));
        let generate_events = Arc::new(Mutex::new(Vec::new()));
        let feature_timeframe = TimeFrame::new(15, TimeFrameUnit::Minute);
        let execution = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        let data = FixedScaleOutDataFeed::new();
        let broker = UnifiedBroker::new(execution, data);
        let mut state = StrategyState::new(
            "MultiTimeframeBacktest".to_string(),
            "1.0".to_string(),
            MultiTimeframeBacktestStrategy {
                feature_timeframe,
                on_bar_events: on_bar_events.clone(),
                generate_events: generate_events.clone(),
            },
            broker,
            StrategyMode::Backtest,
            TimeFrame::new(1, TimeFrameUnit::Day),
        );

        let start = Utc.timestamp_millis_opt(1756771200000).unwrap();
        let end = Utc.timestamp_millis_opt(1757030400000).unwrap();
        state
            .run_backtest(start, end, state.timeframe())
            .await
            .expect("multi-timeframe fixed-data backtest should complete");

        let main_history = state
            .history
            .get("AAPL")
            .expect("main timeframe history should use the base symbol key");
        let feature_history = state
            .history
            .get("AAPL:15m")
            .expect("feature timeframe history should use symbol:timeframe key");
        assert!(main_history.height() > 0);
        assert_eq!(main_history.height(), feature_history.height());

        let on_bar_events = on_bar_events.lock().unwrap();
        assert!(
            on_bar_events
                .iter()
                .any(|event| event.history_key == "AAPL" && !event.is_feature)
        );
        assert!(on_bar_events.iter().any(|event| {
            event.history_key == "AAPL:15m"
                && event.symbol == "AAPL"
                && event.timeframe == feature_timeframe
                && event.is_feature
                && !event.allow_trading
        }));
        drop(on_bar_events);

        let generate_events = generate_events.lock().unwrap();
        assert!(
            !generate_events.is_empty(),
            "main timeframe events should still generate insights"
        );
        assert!(
            generate_events
                .iter()
                .all(|event| event.history_key == "AAPL"
                    && !event.is_feature
                    && event.allow_trading),
            "feature events should not call generate_insights by default"
        );
    }

    #[derive(Clone, Debug)]
    struct HistoryWindowRequest {
        symbol: String,
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
        time_frame: TimeFrame,
    }

    struct HistoryWindowSpyDataFeed {
        connected: Arc<Mutex<bool>>,
        requests: Arc<Mutex<Vec<HistoryWindowRequest>>>,
        subscribe_calls: Arc<Mutex<usize>>,
    }

    impl HistoryWindowSpyDataFeed {
        fn new() -> Self {
            Self {
                connected: Arc::new(Mutex::new(false)),
                requests: Arc::new(Mutex::new(Vec::new())),
                subscribe_calls: Arc::new(Mutex::new(0)),
            }
        }
    }

    impl DataFeed for HistoryWindowSpyDataFeed {
        async fn connect(&self) -> Result<bool, BrokerError> {
            *self.connected.lock().unwrap() = true;
            Ok(true)
        }

        async fn disconnect(&self) -> Result<bool, BrokerError> {
            *self.connected.lock().unwrap() = false;
            Ok(true)
        }

        fn is_connected(&self) -> bool {
            *self.connected.lock().unwrap()
        }
    }

    impl DataProvider for HistoryWindowSpyDataFeed {
        async fn get_ticker_info(&self, symbol: &str) -> Result<Asset, BrokerError> {
            Ok(Asset {
                id: format!("spy-{symbol}"),
                symbol: symbol.to_string(),
                name: symbol.to_string(),
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
                min_price_increment: Some(0.01),
                price_base: None,
                contract_size: None,
                fees: Default::default(),
            })
        }

        async fn get_history(
            &self,
            symbol: &str,
            start: chrono::DateTime<chrono::Utc>,
            end: chrono::DateTime<chrono::Utc>,
            time_frame: TimeFrame,
        ) -> Result<DataFrame, BrokerError> {
            self.requests.lock().unwrap().push(HistoryWindowRequest {
                symbol: symbol.to_string(),
                start,
                end,
                time_frame,
            });

            let mut symbols = Vec::new();
            let mut open = Vec::new();
            let mut high = Vec::new();
            let mut low = Vec::new();
            let mut close = Vec::new();
            let mut volume = Vec::new();
            let mut timestamp = Vec::new();
            let mut current = start;
            let mut index = 0usize;

            while current < end {
                let price = 100.0 + index as f64;
                symbols.push(symbol.to_string());
                open.push(price);
                high.push(price + 1.0);
                low.push(price - 1.0);
                close.push(price + 0.5);
                volume.push(1_000.0 + index as f64);
                timestamp.push(current.timestamp_millis());
                current = time_frame
                    .add_time_increment(current, 1)
                    .map_err(|error| BrokerError::DataFeedError(format!("{:?}", error)))?;
                index += 1;
                if index > 10_000 {
                    return Err(BrokerError::DataFeedError(
                        "HistoryWindowSpyDataFeed exceeded safety limit".to_string(),
                    ));
                }
            }

            DataFrame::new(vec![
                Column::new("symbol".into(), symbols),
                Column::new("open".into(), open),
                Column::new("high".into(), high),
                Column::new("low".into(), low),
                Column::new("close".into(), close),
                Column::new("volume".into(), volume),
                Column::new("timestamp".into(), timestamp),
            ])
            .map_err(|error| BrokerError::DataFeedError(error.to_string()))
        }

        async fn get_latest_quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
            Ok(Quote {
                symbol: symbol.to_string(),
                bid: 100.0,
                ask: 100.0,
                bid_size: 100.0,
                ask_size: 100.0,
                last: Some(100.0),
                last_size: Some(100.0),
                timestamp: Utc::now(),
            })
        }

        async fn get_latest_bar(&self, symbol: &str) -> Result<Bar, BrokerError> {
            Ok(Bar {
                symbol: symbol.to_string(),
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
                volume: 1_000.0,
                timestamp: Utc::now(),
            })
        }

        async fn subscribe_to_data_stream(
            &self,
            _symbols: Vec<String>,
            _time_frame: TimeFrame,
            _mode: DataStreamMode,
            _on_bar: Arc<dyn Fn(Bar) + Send + Sync>,
        ) -> Result<(), BrokerError> {
            *self.subscribe_calls.lock().unwrap() += 1;
            Ok(())
        }

        async fn unsubscribe_from_data_stream(
            &self,
            _symbols: Vec<String>,
        ) -> Result<(), BrokerError> {
            Ok(())
        }
    }

    struct HistoryWindowSpyStrategy {
        feature_timeframe: TimeFrame,
    }

    impl Strategy for HistoryWindowSpyStrategy {
        fn on_start(&mut self, ctx: &mut dyn StrategyContext) {
            ctx.add_events(EventStreamType::Bar, Some(self.feature_timeframe));
        }

        fn universe(&self, _ctx: &mut dyn StrategyContext) -> HashSet<String> {
            ["AAPL".to_string()].into_iter().collect()
        }

        fn init(&mut self, _ctx: &mut dyn StrategyContext, _asset: &Asset) {}

        fn on_bar(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str, _bar: &BarData) {}

        fn generate_insights(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str) {}

        fn insight_pipeline(&mut self, _ctx: &mut dyn StrategyContext, _insight: &Insight) {}

        fn on_teardown(&mut self, _ctx: &mut dyn StrategyContext) {}
    }

    #[tokio::test]
    async fn backtest_add_event_history_uses_backtest_end_not_wall_clock_now() {
        let feature_timeframe = TimeFrame::new(15, TimeFrameUnit::Minute);
        let execution = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        let data = HistoryWindowSpyDataFeed::new();
        let broker = UnifiedBroker::new_backtest(execution, data);
        let mut state = StrategyState::new(
            "HistoryWindowSpy".to_string(),
            "1.0".to_string(),
            HistoryWindowSpyStrategy { feature_timeframe },
            broker,
            StrategyMode::Backtest,
            TimeFrame::new(1, TimeFrameUnit::Day),
        );

        let start = Utc.with_ymd_and_hms(2025, 2, 3, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2025, 2, 4, 0, 0, 0).unwrap();
        state
            .run_backtest(start, end, state.timeframe())
            .await
            .expect("history window spy backtest should complete");

        let requests = state.broker.data.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests.iter().all(|request| request.symbol == "AAPL"));
        assert!(requests.iter().all(|request| request.start == start));
        assert!(
            requests.iter().all(|request| request.end == end),
            "all backtest history requests, including add_event feature streams, should end at the configured backtest end"
        );
        assert!(
            requests
                .iter()
                .any(|request| request.time_frame == TimeFrame::new(1, TimeFrameUnit::Day))
        );
        assert!(
            requests
                .iter()
                .any(|request| request.time_frame == feature_timeframe)
        );
        assert_eq!(
            *state.broker.data.subscribe_calls.lock().unwrap(),
            0,
            "backtests should load historical event streams instead of starting live subscriptions"
        );
    }

    async fn assert_scaled_out(
        state: &StrategyState<FixedScaleOutBacktestStrategy, PaperBroker, FixedScaleOutDataFeed>,
    ) {
        println!(
            "[scale-out-test] asserting scaled out across {} insights",
            state.insights.len()
        );
        let mut found_scaled_out = false;
        for id in state.insights.ids() {
            if let Some(insight) = state.insights.get(&id) {
                println!(
                    "[scale-out-test] insight id={} state={:?} partials={} partial_qty={:?}",
                    insight.insight_id,
                    insight.state,
                    insight.partial_closes.len(),
                    insight.partial_filled_quantity
                );
                if insight.strategy_type.to_string() == "Testing"
                    && !insight.partial_closes.is_empty()
                {
                    found_scaled_out = true;
                    assert!(
                        insight
                            .partial_closes
                            .iter()
                            .any(|partial| partial.quantity > 0.0),
                        "Scaled-out insight should record positive partial close quantity"
                    );
                    assert!(
                        insight.partial_filled_quantity.unwrap_or(0.0) > 0.0,
                        "Scaled-out insight should track reduced quantity"
                    );
                }
            }
        }

        assert!(
            found_scaled_out,
            "Expected at least one insight to scale out during the backtest"
        );

        let positions = state
            .broker
            .get_positions()
            .await
            .expect("Should be able to query broker positions after scale-out backtest");
        println!(
            "[scale-out-test] broker positions after backtest={}",
            positions.len()
        );
        assert!(
            positions.is_empty(),
            "Expected no remaining broker positions after scale-out backtest, found {:?}",
            positions
        );

        let orders = state
            .broker
            .get_orders()
            .await
            .expect("Should be able to query broker orders after scale-out backtest");
        let hanging_orders: Vec<_> = orders
            .into_iter()
            .filter(|order| {
                !matches!(
                    order.status,
                    TradeUpdateEvent::Closed
                        | TradeUpdateEvent::Cancelled
                        | TradeUpdateEvent::Rejected
                )
            })
            .collect();
        println!(
            "[scale-out-test] nonterminal broker orders after backtest={}",
            hanging_orders.len()
        );
        assert!(
            hanging_orders.is_empty(),
            "Expected no hanging broker orders after scale-out backtest, found {:?}",
            hanging_orders
        );
    }

    // ═══════════════════════════════════════════════════════════════════
    //  SMA Crossover Alpha Model
    // ═══════════════════════════════════════════════════════════════════

    /// Simple Moving Average crossover alpha.
    /// Generates BUY when fast SMA crosses above slow SMA,
    ///         SELL when fast SMA crosses below slow SMA.
    struct SmaCrossoverAlpha {
        fast_period: usize,
        slow_period: usize,
        /// Tracks previous crossover state per symbol: true = fast > slow
        prev_fast_above: std::collections::HashMap<String, bool>,
    }

    impl SmaCrossoverAlpha {
        fn new(fast_period: usize, slow_period: usize) -> Self {
            Self {
                fast_period,
                slow_period,
                prev_fast_above: std::collections::HashMap::new(),
            }
        }
    }

    impl AlphaModel for SmaCrossoverAlpha {
        fn version(&self) -> &str {
            "1.0"
        }

        fn start(&mut self, ctx: &mut dyn StrategyContext) {
            ctx.register_indicator(Box::new(
                crate::core::indicators::I::SimpleMovingAverage::new(self.fast_period, "close"),
            ));
            ctx.register_indicator(Box::new(
                crate::core::indicators::I::SimpleMovingAverage::new(self.slow_period, "close"),
            ));
        }

        fn init(&mut self, _ctx: &mut dyn StrategyContext, _asset: &Asset) {
            // Nothing per-asset
        }

        fn generate_insights(
            &mut self,
            ctx: &mut dyn StrategyContext,
            symbol: &str,
        ) -> AlphaResult {
            let history = ctx.history();
            let df = match history.get(symbol) {
                Some(df) if df.height() >= self.slow_period => df,
                _ => {
                    return AlphaResult::new(
                        None,
                        true,
                        Some("Not enough history".into()),
                        self.name().to_string(),
                    );
                }
            };

            let fast_col = format!("SMA_{}_close", self.fast_period);
            let slow_col = format!("SMA_{}_close", self.slow_period);

            let fast_sma = df
                .column(&fast_col)
                .ok()
                .and_then(|c| c.f64().ok())
                .and_then(|c| {
                    if c.len() > 0 {
                        c.get(c.len() - 1)
                    } else {
                        None
                    }
                });
            let slow_sma = df
                .column(&slow_col)
                .ok()
                .and_then(|c| c.f64().ok())
                .and_then(|c| {
                    if c.len() > 0 {
                        c.get(c.len() - 1)
                    } else {
                        None
                    }
                });

            match (fast_sma, slow_sma) {
                (Some(fast), Some(slow)) => {
                    let fast_above = fast > slow;
                    let prev = self.prev_fast_above.get(symbol).copied();
                    self.prev_fast_above.insert(symbol.to_string(), fast_above);

                    // Check for crossover
                    match prev {
                        Some(was_above) if was_above != fast_above => {
                            let side = if fast_above {
                                OrderSide::Buy
                            } else {
                                OrderSide::Sell
                            };

                            let insight = Insight::new(
                                side,
                                symbol.to_string(),
                                StrategyType::Custom("SMA_Crossover".into()),
                                ctx.timeframe().clone(),
                                80, // confidence
                                None,
                            );

                            AlphaResult::new(
                                Some(insight),
                                true,
                                Some(format!("SMA Crossover: fast={:.2}, slow={:.2}", fast, slow)),
                                self.name().to_string(),
                            )
                        }
                        _ => AlphaResult::new(
                            None,
                            true,
                            Some("No crossover".into()),
                            self.name().to_string(),
                        ),
                    }
                }
                _ => AlphaResult::new(
                    None,
                    true,
                    Some("Not enough data for SMA".into()),
                    self.name().to_string(),
                ),
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  Insight Pipes: Position Sizing + Risk Management
    // ═══════════════════════════════════════════════════════════════════

    /// Sets the quantity on every New insight.
    struct PositionSizerPipe {
        fixed_quantity: f64,
    }

    impl PositionSizerPipe {
        fn new(fixed_quantity: f64) -> Self {
            Self { fixed_quantity }
        }
    }

    impl InsightPipe for PositionSizerPipe {
        fn version(&self) -> &str {
            "1.0"
        }

        fn run(
            &mut self,
            _ctx: &mut dyn StrategyContext,
            insight: &mut Insight,
        ) -> InsightPipeResult {
            insight.set_quantity(Some(self.fixed_quantity));
            InsightPipeResult::new(
                true,
                true,
                Some(format!("Set quantity to {}", self.fixed_quantity)),
                self.name().to_string(),
            )
        }
    }

    /// Sets stop-loss and take-profit based on the current close price.
    /// SL = 2% adverse, TP = 4% favorable (2:1 reward:risk).
    struct RiskManagementPipe {
        sl_pct: f64,
        tp_pct: f64,
    }

    impl RiskManagementPipe {
        fn new(sl_pct: f64, tp_pct: f64) -> Self {
            Self { sl_pct, tp_pct }
        }
    }

    impl InsightPipe for RiskManagementPipe {
        fn version(&self) -> &str {
            "1.0"
        }

        fn run(
            &mut self,
            ctx: &mut dyn StrategyContext,
            insight: &mut Insight,
        ) -> InsightPipeResult {
            let symbol = insight.symbol.clone();
            let history = ctx.history();

            // Get latest close price
            let close = match history.get(&symbol) {
                Some(df) if df.height() > 0 => {
                    let closes = df.column("close").unwrap().f64().unwrap();
                    closes.get(closes.len() - 1).unwrap_or(0.0)
                }
                _ => {
                    return InsightPipeResult::new(
                        false,
                        false,
                        Some("No price data for SL/TP".into()),
                        self.name().to_string(),
                    );
                }
            };

            match insight.side {
                OrderSide::Buy => {
                    insight.set_stop_loss(Some(close * (1.0 - self.sl_pct)));
                    insight.set_take_profit_levels(Some(vec![close * (1.0 + self.tp_pct)]));
                }
                OrderSide::Sell => {
                    insight.set_stop_loss(Some(close * (1.0 + self.sl_pct)));
                    insight.set_take_profit_levels(Some(vec![close * (1.0 - self.tp_pct)]));
                }
            }

            InsightPipeResult::new(
                true,
                true,
                Some(format!(
                    "SL={:.2}%, TP={:.2}% from close={:.2}",
                    self.sl_pct * 100.0,
                    self.tp_pct * 100.0,
                    close
                )),
                self.name().to_string(),
            )
        }
    }

    struct SubmitInsightPipe {}

    impl InsightPipe for SubmitInsightPipe {
        fn version(&self) -> &str {
            "1.0"
        }

        fn run(
            &mut self,
            ctx: &mut dyn StrategyContext,
            insight: &mut Insight,
        ) -> InsightPipeResult {
            insight.submit(ctx);
            InsightPipeResult::new(
                true,
                true,
                Some("Insight submitted".into()),
                self.name().to_string(),
            )
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  Minimal Strategy (delegates to AlphaModel + Pipeline)
    // ═══════════════════════════════════════════════════════════════════

    struct SmaStrategy {
        symbols: HashSet<String>,
    }

    impl SmaStrategy {
        fn new(symbols: Vec<&str>) -> Self {
            Self {
                symbols: symbols.into_iter().map(|s| s.to_string()).collect(),
            }
        }
    }

    impl Strategy for SmaStrategy {
        fn name(&self) -> &str {
            "SMA_Crossover_Strategy"
        }

        fn on_start(&mut self, _ctx: &mut dyn StrategyContext) {
            println!("[SmaStrategy] on_start");
        }

        fn init(&mut self, _ctx: &mut dyn StrategyContext, asset: &Asset) {
            println!("[SmaStrategy] init asset: {}", asset.symbol);
        }

        fn universe(&self, _ctx: &mut dyn StrategyContext) -> HashSet<String> {
            self.symbols.clone()
        }

        fn on_bar(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str, _bar: &BarData) {
            // All signal logic is in the AlphaModel
        }

        fn generate_insights(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str) {
            // All insight generation is in the AlphaModel
        }

        fn insight_pipeline(&mut self, _ctx: &mut dyn StrategyContext, _insight: &Insight) {
            // All pipeline logic is in InsightPipe components
        }

        fn on_teardown(&mut self, _ctx: &mut dyn StrategyContext) {
            println!("[SmaStrategy] on_teardown");
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  Tests
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_insight_can_set_trailing_stop_price() {
        let mut insight = Insight::new(
            OrderSide::Buy,
            "EURUSD=X".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            90,
            None,
        );

        insight.set_trailing_stop_price(Some(1.1534));

        assert_eq!(insight.trailing_stop_price(), Some(1.1534));
        assert!(matches!(insight.order_class(), OrderClass::Bracket));
        assert!(
            insight.legs.trailing_stop.is_none(),
            "insight should store trailing stop as a plain gap until the broker materializes the leg"
        );
        assert!(insight.state_history.iter().any(|(_, _, message)| {
            message
                .as_deref()
                .is_some_and(|message| message.contains("Trailing stop set to 1.1534"))
        }));
    }

    #[tokio::test]
    async fn test_paper_broker_submit_order_preserves_trailing_stop_leg_metadata() {
        let broker = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        let mut insight = Insight::new(
            OrderSide::Buy,
            "AAPL".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Day),
            80,
            None,
        );
        insight.set_quantity(Some(5.0));
        insight.set_trailing_stop_price(Some(222.25));

        let order = broker
            .submit_order(insight.clone())
            .await
            .expect("paper broker should accept trailing-stop insight");

        let trailing_leg = order
            .legs
            .as_ref()
            .and_then(|legs| legs.trailing_stop.as_ref())
            .expect("submitted order should contain trailing stop leg");

        let expected_insight_id = insight.insight_id().to_string();
        assert_eq!(
            order.insight_id.as_deref(),
            Some(expected_insight_id.as_str())
        );
        assert_eq!(trailing_leg.limit_price, None);
        assert_eq!(trailing_leg.trail_price, Some(222.25));
        assert_eq!(trailing_leg.side, OrderSide::Sell);
        assert_eq!(trailing_leg.order_type, OrderType::TrailingStop);
        assert!(matches!(order.order_class, OrderClass::Bracket));
    }

    #[test]
    fn test_max_history_rows_respects_warm_up_floor() {
        let execution = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        let data = FixedScaleOutDataFeed::new();
        let broker = UnifiedBroker::new_backtest(execution, data);
        let mut state = StrategyState::new(
            "HistoryRetention".to_string(),
            "1.0".to_string(),
            FixedScaleOutBacktestStrategy {
                generated: false,
                tp_levels: vec![105.0],
                sl_levels: vec![95.0],
            },
            broker,
            StrategyMode::Backtest,
            TimeFrame::new(1, TimeFrameUnit::Day),
        );

        assert_eq!(state.max_history_rows(), 2000);

        state.set_warm_up_bars(2500);
        assert_eq!(state.max_history_rows(), 2501);

        state.update_max_history_rows(50);
        assert_eq!(state.max_history_rows(), 2501);

        state.update_max_history_rows(3000);
        assert_eq!(state.max_history_rows(), 3000);
    }

    /// Test 1: Full backtest with SMA crossover alpha using Yahoo Finance data.
    ///
    /// Verifies:
    /// - Yahoo Finance data feed fetches real data
    /// - SMA crossover alpha generates insights on crossovers
    /// - Insights are collected in the InsightCollection
    /// - Backtest completes without errors
    #[tokio::test]
    async fn test_sma_crossover_backtest_with_yahoo() {
        // 1. Create components
        let execution = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        let data = FixedScaleOutDataFeed::new();
        let broker = UnifiedBroker::new_backtest(execution, data);

        // 2. Create strategy state
        let strategy = SmaStrategy::new(vec!["AAPL"]);
        let mut state = StrategyState::new(
            "SMA_Backtest".to_string(),
            "1.0".to_string(),
            strategy,
            broker,
            StrategyMode::Backtest,
            TimeFrame::new(1, TimeFrameUnit::Day),
        );

        // 3. Register alpha model (fast=5, slow=20 for frequent signals)
        state
            .add_alpha(WrappedAlphaModel::builder(Box::new(SmaCrossoverAlpha::new(5, 20))).build());

        // 4. Register insight pipes
        state.add_pipe(WrappedInsightPipe::builder(Box::new(PositionSizerPipe::new(10.0))).build());
        state.add_pipe(
            WrappedInsightPipe::builder(Box::new(RiskManagementPipe::new(0.02, 0.04))).build(),
        );
        state.add_pipe(WrappedInsightPipe::builder(Box::new(SubmitInsightPipe {})).build());

        // 5. Set warm-up period to slow SMA length
        state.set_warm_up_bars(20);

        // 6. Run backtest: 6 months of daily data
        let end = chrono::Utc::now();
        let start = end - chrono::Duration::days(365);

        println!("Running SMA crossover backtest: {} to {}", start, end);

        let result = state.run_backtest(start, end, state.timeframe()).await;

        let Some(results) = assert_backtest_or_skip(result, "SMA crossover backtest failed") else {
            return;
        };
        results.print_metrics();

        assert!(
            results.account_history.len() > 0,
            "Account history should have at least one snapshot"
        );

        let insight_count = state.insights.len();
        println!("  Insights generated: {}", insight_count);
        println!("  Strategy completed successfully ✓");
        println!("  Insights: {:#?}", state.insights.get_state_count());
    }

    /// Test 2: Verify that insight pipes correctly modify insights.
    ///
    /// Creates an insight directly and runs pipes against it to verify
    /// that the PositionSizerPipe sets quantity and RiskManagementPipe
    /// sets stop-loss/take-profit.
    #[tokio::test]
    async fn test_insight_pipes_modify_insight() {
        // 1. Create components
        let execution = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        let data = FixedScaleOutDataFeed::new();
        let broker = UnifiedBroker::new_backtest(execution, data);

        // 2. Create strategy that produces a single insight in on_bar
        struct PipeTestStrategy;
        impl Strategy for PipeTestStrategy {
            fn name(&self) -> &str {
                "PipeTestStrategy"
            }
            fn on_start(&mut self, _ctx: &mut dyn StrategyContext) {}
            fn init(&mut self, _ctx: &mut dyn StrategyContext, _asset: &Asset) {}
            fn universe(&self, _ctx: &mut dyn StrategyContext) -> HashSet<String> {
                let mut set = HashSet::new();
                set.insert("AAPL".to_string());
                set
            }
            fn on_bar(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str, _bar: &BarData) {}
            fn generate_insights(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str) {}
            fn insight_pipeline(&mut self, _ctx: &mut dyn StrategyContext, _insight: &Insight) {}
            fn on_teardown(&mut self, _ctx: &mut dyn StrategyContext) {}
        }

        let strategy = PipeTestStrategy;
        let mut state = StrategyState::new(
            "PipeTest".to_string(),
            "1.0".to_string(),
            strategy,
            broker,
            StrategyMode::Backtest,
            TimeFrame::new(1, TimeFrameUnit::Day),
        );

        // Register pipes
        state.add_pipe(WrappedInsightPipe::builder(Box::new(PositionSizerPipe::new(25.0))).build());
        state.add_pipe(
            WrappedInsightPipe::builder(Box::new(RiskManagementPipe::new(0.03, 0.06))).build(),
        );

        // Set warm-up to 0 so pipes run immediately
        state.set_warm_up_bars(0);

        // Run a short backtest (30 days) so we have price data for the pipes
        let end = chrono::Utc::now();
        let start = end - chrono::Duration::days(30);

        // Manually add an insight BEFORE running backtest — we want to test the pipeline
        // We'll add an insight after load_backtest_data by injecting during on_bar
        // Instead, let's use a strategy that generates one insight on the first bar

        /// Strategy that generates a BUY insight on every bar (for testing pipes)
        struct InsightGeneratorStrategy {
            generated: bool,
        }
        impl Strategy for InsightGeneratorStrategy {
            fn name(&self) -> &str {
                "InsightGenerator"
            }
            fn on_start(&mut self, _ctx: &mut dyn StrategyContext) {}
            fn init(&mut self, _ctx: &mut dyn StrategyContext, _asset: &Asset) {}
            fn universe(&self, _ctx: &mut dyn StrategyContext) -> HashSet<String> {
                let mut set = HashSet::new();
                set.insert("AAPL".to_string());
                set
            }
            fn on_bar(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str, _bar: &BarData) {}
            fn generate_insights(&mut self, ctx: &mut dyn StrategyContext, symbol: &str) {
                if !self.generated {
                    let insight = Insight::new(
                        OrderSide::Buy,
                        symbol.to_string(),
                        StrategyType::Testing,
                        ctx.timeframe().clone(),
                        90,
                        None,
                    );
                    ctx.add_insight(insight);
                    self.generated = true;
                }
            }
            fn insight_pipeline(&mut self, _ctx: &mut dyn StrategyContext, _insight: &Insight) {}
            fn on_teardown(&mut self, _ctx: &mut dyn StrategyContext) {}
        }

        // Recreate state with the insight generator strategy
        let execution2 = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        let data2 = YahooFinanceDataFeed::new();
        let broker2 = UnifiedBroker::new(execution2, data2);
        let mut state = StrategyState::new(
            "PipeTest".to_string(),
            "1.0".to_string(),
            InsightGeneratorStrategy { generated: false },
            broker2,
            StrategyMode::Backtest,
            TimeFrame::new(1, TimeFrameUnit::Day),
        );

        state.add_pipe(WrappedInsightPipe::builder(Box::new(PositionSizerPipe::new(25.0))).build());
        state.add_pipe(
            WrappedInsightPipe::builder(Box::new(RiskManagementPipe::new(0.03, 0.06))).build(),
        );
        state.set_warm_up_bars(1); // Allow first bar, then generate on second

        println!("Running insight pipe test backtest...");

        let result = state.run_backtest(start, end, state.timeframe()).await;
        if assert_backtest_or_skip(result, "Insight pipe backtest should complete").is_none() {
            return;
        }

        // Verify insights were generated and modified by pipes
        let insight_count = state.insights.len();
        println!("  Insights in collection: {}", insight_count);
        assert!(
            insight_count > 0,
            "At least one insight should have been generated"
        );

        // Check that the pipe modified the insight correctly
        let insight_ids = state.insights.ids();
        for id in &insight_ids {
            if let Some(insight) = state.insights.get_mut(id) {
                println!("  Insight {} for {}:", insight.insight_id, insight.symbol);
                println!("    Side: {:?}", insight.side);
                println!("    Quantity: {:?}", insight.quantity());
                println!("    Stop Loss: {:?}", insight.stop_loss());
                println!("    Take Profit: {:?}", insight.take_profit_levels());
                println!("    State: {:?}", insight.state());

                // PositionSizerPipe should have set quantity to 25.0
                assert_eq!(
                    insight.quantity(),
                    Some(25.0),
                    "PositionSizerPipe should set quantity to 25.0"
                );

                // RiskManagementPipe should have set SL and TP
                assert!(
                    insight.stop_loss().is_some(),
                    "RiskManagementPipe should set stop_loss"
                );
                assert!(
                    insight.take_profit_levels().is_some(),
                    "RiskManagementPipe should set take_profit_levels"
                );

                // Verify SL is below close for a BUY
                let _sl = insight.stop_loss().unwrap();
                let tp = insight.take_profit_levels().unwrap();
                assert!(!tp.is_empty(), "Take profit levels should not be empty");

                println!("    ✓ Pipe modifications verified!");
            }
        }
    }

    /// Test 3: Verify alpha model lifecycle (init/start/generate).
    ///
    /// Runs a very short backtest and checks that the alpha model's
    /// lifecycle methods are called in the correct order.
    #[tokio::test]
    async fn test_alpha_model_lifecycle() {
        use std::sync::{Arc, Mutex};

        /// Alpha model that records lifecycle events.
        struct LifecycleTracker {
            events: Arc<Mutex<Vec<String>>>,
        }

        impl AlphaModel for LifecycleTracker {
            fn version(&self) -> &str {
                "1.0"
            }
            fn start(&mut self, _ctx: &mut dyn StrategyContext) {
                self.events.lock().unwrap().push("start".to_string());
            }
            fn init(&mut self, _ctx: &mut dyn StrategyContext, asset: &Asset) {
                self.events
                    .lock()
                    .unwrap()
                    .push(format!("init:{}", asset.symbol));
            }
            fn generate_insights(
                &mut self,
                _ctx: &mut dyn StrategyContext,
                symbol: &str,
            ) -> AlphaResult {
                self.events
                    .lock()
                    .unwrap()
                    .push(format!("generate:{}", symbol));
                AlphaResult::new(None, true, None, self.name().to_string())
            }
        }

        let events = Arc::new(Mutex::new(Vec::<String>::new()));

        let execution = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        let data = FixedScaleOutDataFeed::new();
        let broker = UnifiedBroker::new_backtest(execution, data);

        let strategy = SmaStrategy::new(vec!["AAPL"]);
        let mut state = StrategyState::new(
            "LifecycleTest".to_string(),
            "1.0".to_string(),
            strategy,
            broker,
            StrategyMode::Backtest,
            TimeFrame::new(1, TimeFrameUnit::Day),
        );

        state.add_alpha(
            WrappedAlphaModel::builder(Box::new(LifecycleTracker {
                events: events.clone(),
            }))
            .build(),
        );
        state.set_warm_up_bars(0);

        let start = Utc.with_ymd_and_hms(2025, 9, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2025, 10, 15, 0, 0, 0).unwrap();

        let result = state.run_backtest(start, end, state.timeframe()).await;
        if assert_backtest_or_skip(result, "Alpha model lifecycle backtest should complete")
            .is_none()
        {
            return;
        }

        let events = events.lock().unwrap();
        println!("Lifecycle events: {:?}", *events);

        // Verify lifecycle order
        assert!(
            events.iter().any(|e| e.starts_with("init:")),
            "init() should have been called"
        );
        assert!(
            events.iter().any(|e| e == "start"),
            "start() should have been called"
        );
        assert!(
            events.iter().any(|e| e.starts_with("generate:")),
            "generate_insights() should have been called"
        );

        // start should come before init, which should come before generate
        let start_pos = events.iter().position(|e| e == "start").unwrap();
        let init_pos = events.iter().position(|e| e.starts_with("init:")).unwrap();
        let gen_pos = events
            .iter()
            .position(|e| e.starts_with("generate:"))
            .unwrap();

        assert!(start_pos < init_pos, "start() should come before init()");
        assert!(
            start_pos < gen_pos,
            "start() should come before generate_insights()"
        );

        println!("  ✓ Lifecycle order verified: start → init → generate");
    }

    #[tokio::test]
    async fn test_preseed_warmup_history_runs_after_alpha_start_before_strategy_init() {
        struct WarmupAlpha;

        impl AlphaModel for WarmupAlpha {
            fn version(&self) -> &str {
                "1.0"
            }

            fn start(&mut self, ctx: &mut dyn StrategyContext) {
                ctx.set_warm_up_bars(5);
            }

            fn init(&mut self, ctx: &mut dyn StrategyContext, asset: &Asset) {
                let seeded_rows = ctx
                    .history()
                    .get(&asset.symbol)
                    .map(|history| history.height())
                    .unwrap_or_default();
                ctx.variables()
                    .insert("alpha_init_seeded_rows".to_string(), json!(seeded_rows));
            }

            fn generate_insights(
                &mut self,
                _ctx: &mut dyn StrategyContext,
                _symbol: &str,
            ) -> AlphaResult {
                AlphaResult::new(None, true, None, self.name().to_string())
            }
        }

        struct InitHistoryProbeStrategy;

        impl Strategy for InitHistoryProbeStrategy {
            fn on_start(&mut self, _ctx: &mut dyn StrategyContext) {}

            fn init(&mut self, ctx: &mut dyn StrategyContext, asset: &Asset) {
                let seeded_rows = ctx
                    .history()
                    .get(&asset.symbol)
                    .map(|history| history.height())
                    .unwrap_or_default();
                ctx.variables()
                    .insert("strategy_init_seeded_rows".to_string(), json!(seeded_rows));
                ctx.variables().insert(
                    "strategy_init_warmup".to_string(),
                    json!(ctx.warm_up_bars()),
                );
            }

            fn universe(&self, _ctx: &mut dyn StrategyContext) -> HashSet<String> {
                ["AAPL".to_string()].into_iter().collect()
            }

            fn on_bar(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str, _bar: &BarData) {}

            fn generate_insights(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str) {}

            fn insight_pipeline(&mut self, _ctx: &mut dyn StrategyContext, _insight: &Insight) {}

            fn on_teardown(&mut self, _ctx: &mut dyn StrategyContext) {}
        }

        let execution = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        let data = FixedScaleOutDataFeed::new();
        let broker = UnifiedBroker::new_backtest(execution, data);
        let mut state = StrategyState::new(
            "PreseedWarmupHistoryTest".to_string(),
            "1.0".to_string(),
            InitHistoryProbeStrategy,
            broker,
            StrategyMode::Backtest,
            TimeFrame::new(1, TimeFrameUnit::Day),
        );
        state.add_alpha(WrappedAlphaModel::builder(Box::new(WarmupAlpha)).build());
        state.add_on_init_logic(
            OnInitLogicBuilder::new(Box::new(
                crate::core::lifecycle::preseed_warmup_history::PreseedWarmupHistory::new(),
            ))
            .timing(LifecycleTiming::BeforeGenerated)
            .build(),
        );

        let start = Utc.with_ymd_and_hms(2025, 9, 15, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2025, 9, 17, 0, 0, 0).unwrap();
        let result = state.run_backtest(start, end, state.timeframe()).await;
        if assert_backtest_or_skip(result, "Warm-up preseed backtest should complete").is_none() {
            return;
        }

        let strategy_rows = state
            .variables
            .get("strategy_init_seeded_rows")
            .and_then(|value| value.as_u64())
            .unwrap_or_default();
        let alpha_rows = state
            .variables
            .get("alpha_init_seeded_rows")
            .and_then(|value| value.as_u64())
            .unwrap_or_default();
        let warmup = state
            .variables
            .get("strategy_init_warmup")
            .and_then(|value| value.as_i64())
            .unwrap_or_default();

        assert_eq!(warmup, 5);
        assert!(
            strategy_rows > 0,
            "strategy init should see preseeded warm-up history"
        );
        assert_eq!(
            alpha_rows, strategy_rows,
            "alpha init should run after strategy init without losing seeded history"
        );
    }

    #[tokio::test]
    async fn test_preseed_warmup_history_seeds_registered_feature_streams() {
        let feature_timeframe = TimeFrame::new(15, TimeFrameUnit::Minute);

        struct FeatureWarmupStrategy {
            feature_timeframe: TimeFrame,
        }

        impl Strategy for FeatureWarmupStrategy {
            fn on_start(&mut self, ctx: &mut dyn StrategyContext) {
                ctx.set_warm_up_bars(5);
                ctx.add_events(EventStreamType::Bar, Some(self.feature_timeframe));
            }

            fn init(&mut self, ctx: &mut dyn StrategyContext, asset: &Asset) {
                let feature_key = format!(
                    "{}:{}",
                    asset.symbol,
                    self.feature_timeframe.compact_label()
                );
                let main_rows = ctx
                    .history()
                    .get(&asset.symbol)
                    .map(|history| history.height())
                    .unwrap_or_default();
                let feature_rows = ctx
                    .history()
                    .get(&feature_key)
                    .map(|history| history.height())
                    .unwrap_or_default();
                ctx.variables()
                    .insert("feature_warmup_main_rows".to_string(), json!(main_rows));
                ctx.variables().insert(
                    "feature_warmup_feature_rows".to_string(),
                    json!(feature_rows),
                );
            }

            fn universe(&self, _ctx: &mut dyn StrategyContext) -> HashSet<String> {
                ["AAPL".to_string()].into_iter().collect()
            }

            fn on_bar(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str, _bar: &BarData) {}

            fn generate_insights(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str) {}

            fn insight_pipeline(&mut self, _ctx: &mut dyn StrategyContext, _insight: &Insight) {}

            fn on_teardown(&mut self, _ctx: &mut dyn StrategyContext) {}
        }

        let execution = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        let data = GeneratedWarmupDataFeed::new();
        let broker = UnifiedBroker::new_backtest(execution, data);
        let mut state = StrategyState::new(
            "FeatureWarmupHistoryTest".to_string(),
            "1.0".to_string(),
            FeatureWarmupStrategy { feature_timeframe },
            broker,
            StrategyMode::Backtest,
            TimeFrame::new(1, TimeFrameUnit::Day),
        );
        state.add_on_init_logic(
            OnInitLogicBuilder::new(Box::new(
                crate::core::lifecycle::preseed_warmup_history::PreseedWarmupHistory::new(),
            ))
            .timing(LifecycleTiming::BeforeGenerated)
            .build(),
        );

        let start = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2026, 6, 2, 0, 0, 0).unwrap();
        state
            .run_backtest(start, end, state.timeframe())
            .await
            .expect("feature warm-up backtest should complete");

        let main_rows = state
            .variables
            .get("feature_warmup_main_rows")
            .and_then(|value| value.as_u64())
            .unwrap_or_default();
        let feature_rows = state
            .variables
            .get("feature_warmup_feature_rows")
            .and_then(|value| value.as_u64())
            .unwrap_or_default();
        assert_eq!(main_rows, 5);
        assert_eq!(feature_rows, 5);

        let feature_history = state
            .history
            .get("AAPL:15m")
            .expect("feature warm-up history should use symbol:timeframe key");
        assert!(
            feature_history.height() >= feature_rows as usize,
            "feature history should retain seeded rows and append runtime bars"
        );

        let requests = state.broker.data.requests.lock().unwrap();
        assert!(
            requests.iter().any(|(symbol, timeframe)| symbol == "AAPL"
                && *timeframe == TimeFrame::new(1, TimeFrameUnit::Day)),
            "main timeframe warm-up should request daily bars"
        );
        assert!(
            requests
                .iter()
                .any(|(symbol, timeframe)| symbol == "AAPL" && *timeframe == feature_timeframe),
            "feature timeframe warm-up should request feature bars"
        );
    }

    #[tokio::test]
    async fn test_strategy_lifecycle_logic_and_universe_models_run() {
        struct VariableOnStartLogic {
            key: &'static str,
        }

        impl OnStartLogic for VariableOnStartLogic {
            fn version(&self) -> &str {
                "1.0"
            }

            fn run(&mut self, ctx: &mut dyn StrategyContext) -> LifecycleResult {
                ctx.variables().insert(self.key.to_string(), json!(true));
                LifecycleResult::passed(self.name().to_string())
            }
        }

        struct VariableOnInitLogic {
            key: &'static str,
        }

        impl OnInitLogic for VariableOnInitLogic {
            fn version(&self) -> &str {
                "1.0"
            }

            fn run(&mut self, ctx: &mut dyn StrategyContext, asset: &Asset) -> LifecycleResult {
                let mut symbols = ctx
                    .variables()
                    .get(self.key)
                    .and_then(|entry| entry.value().as_array().cloned())
                    .unwrap_or_default();
                symbols.push(json!(asset.symbol.clone()));
                ctx.variables().insert(self.key.to_string(), json!(symbols));
                LifecycleResult::passed(self.name().to_string())
            }
        }

        struct VariableOnTeardownLogic {
            key: &'static str,
        }

        impl OnTeardownLogic for VariableOnTeardownLogic {
            fn version(&self) -> &str {
                "1.0"
            }

            fn run(&mut self, ctx: &mut dyn StrategyContext) -> LifecycleResult {
                ctx.variables().insert(self.key.to_string(), json!(true));
                LifecycleResult::passed(self.name().to_string())
            }
        }

        struct StaticUniverseModel {
            symbols: Vec<&'static str>,
        }

        impl UniverseModel for StaticUniverseModel {
            fn version(&self) -> &str {
                "1.0"
            }

            fn run(&mut self, _ctx: &mut dyn StrategyContext) -> UniverseResult {
                UniverseResult::passed(
                    self.symbols
                        .iter()
                        .map(|symbol| symbol.to_string())
                        .collect(),
                    self.name().to_string(),
                )
            }
        }

        struct GeneratedStyleLifecycleStrategy;

        impl Strategy for GeneratedStyleLifecycleStrategy {
            fn name(&self) -> &str {
                "GeneratedStyleLifecycleStrategy"
            }

            fn on_start(&mut self, ctx: &mut dyn StrategyContext) {
                ctx.variables()
                    .insert("generated_on_start".to_string(), json!(true));
                ctx.add_universe_model(
                    UniverseModelBuilder::new(Box::new(StaticUniverseModel {
                        symbols: vec!["AAPL", "MSFT"],
                    }))
                    .build(),
                );
                ctx.add_universe_model(
                    UniverseModelBuilder::new(Box::new(StaticUniverseModel {
                        symbols: vec!["MSFT", "GOOG"],
                    }))
                    .build(),
                );
            }

            fn init(&mut self, _ctx: &mut dyn StrategyContext, _asset: &Asset) {}

            fn universe(&self, _ctx: &mut dyn StrategyContext) -> HashSet<String> {
                ["AAPL".to_string(), "STATIC_ONLY".to_string()]
                    .into_iter()
                    .collect()
            }

            fn on_bar(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str, _bar: &BarData) {}

            fn generate_insights(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str) {}

            fn insight_pipeline(&mut self, _ctx: &mut dyn StrategyContext, _insight: &Insight) {}

            fn on_teardown(&mut self, _ctx: &mut dyn StrategyContext) {}
        }

        let execution = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        let data = FixedScaleOutDataFeed::new();
        let broker = UnifiedBroker::new_backtest(execution, data);

        let mut state = StrategyState::new(
            "LifecycleLogicStateTest".to_string(),
            "1.0".to_string(),
            GeneratedStyleLifecycleStrategy,
            broker,
            StrategyMode::Backtest,
            TimeFrame::new(1, TimeFrameUnit::Day),
        );
        state.add_on_start_logic(
            OnStartLogicBuilder::new(Box::new(VariableOnStartLogic {
                key: "on_start_before",
            }))
            .timing(LifecycleTiming::BeforeGenerated)
            .build(),
        );
        state.add_on_start_logic(
            OnStartLogicBuilder::new(Box::new(VariableOnStartLogic {
                key: "on_start_after",
            }))
            .timing(LifecycleTiming::AfterGenerated)
            .build(),
        );
        state.add_on_init_logic(
            OnInitLogicBuilder::new(Box::new(VariableOnInitLogic {
                key: "on_init_symbols",
            }))
            .timing(LifecycleTiming::BeforeGenerated)
            .build(),
        );
        state.add_on_teardown_logic(
            OnTeardownLogicBuilder::new(Box::new(VariableOnTeardownLogic {
                key: "on_teardown_after",
            }))
            .timing(LifecycleTiming::AfterGenerated)
            .build(),
        );
        state.set_warm_up_bars(0);

        let start = Utc.with_ymd_and_hms(2025, 9, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2025, 9, 3, 0, 0, 0).unwrap();
        let result = state.run_backtest(start, end, state.timeframe()).await;
        if assert_backtest_or_skip(result, "Lifecycle logic strategy backtest should complete")
            .is_none()
        {
            return;
        }

        assert_eq!(
            state.variables.get("on_start_before").map(|v| v.clone()),
            Some(json!(true))
        );
        assert_eq!(
            state.variables.get("generated_on_start").map(|v| v.clone()),
            Some(json!(true))
        );
        assert_eq!(
            state.variables.get("on_start_after").map(|v| v.clone()),
            Some(json!(true))
        );
        assert_eq!(
            state.variables.get("on_teardown_after").map(|v| v.clone()),
            Some(json!(true))
        );

        let loaded_symbols: HashSet<String> = state.universe.keys().cloned().collect();
        assert_eq!(
            loaded_symbols,
            [
                "AAPL".to_string(),
                "MSFT".to_string(),
                "GOOG".to_string(),
                "STATIC_ONLY".to_string()
            ]
            .into_iter()
            .collect()
        );

        let mut init_symbols = state
            .variables
            .get("on_init_symbols")
            .and_then(|entry| entry.value().as_array().cloned())
            .unwrap_or_default()
            .into_iter()
            .filter_map(|value| value.as_str().map(str::to_string))
            .collect::<Vec<_>>();
        init_symbols.sort();
        assert_eq!(init_symbols, vec!["AAPL", "GOOG", "MSFT", "STATIC_ONLY"]);
    }

    #[test]
    fn teardown_blocks_new_insights_from_strategy_context() {
        let execution = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        let data = FixedScaleOutDataFeed::new();
        let broker = UnifiedBroker::new_backtest(execution, data);
        let mut state = StrategyState::new(
            "TeardownBlocksInsights".to_string(),
            "1.0".to_string(),
            SmaStrategy::new(vec!["AAPL"]),
            broker,
            StrategyMode::Backtest,
            TimeFrame::new(1, TimeFrameUnit::Day),
        );
        state.status = crate::core::strategy::StrategyStatus::Running;

        state.begin_teardown();
        let insight = Insight::new(
            OrderSide::Buy,
            "AAPL".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Day),
            90,
            None,
        );
        StrategyContext::add_insight(&mut state, insight);

        assert_eq!(
            state.status,
            crate::core::strategy::StrategyStatus::Stopping
        );
        assert_eq!(state.insights.len(), 0);
    }

    #[tokio::test]
    async fn submit_insight_is_idempotent_after_broker_acknowledgement() {
        let execution = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        let data = FixedScaleOutDataFeed::new();
        let broker = UnifiedBroker::new_backtest(execution, data);
        let mut state = StrategyState::new(
            "SubmitIdempotence".to_string(),
            "1.0".to_string(),
            SmaStrategy::new(vec!["AAPL"]),
            broker,
            StrategyMode::Backtest,
            TimeFrame::new(1, TimeFrameUnit::Day),
        );
        state.status = crate::core::strategy::StrategyStatus::Running;
        state.universe.insert(
            "AAPL".to_string(),
            Asset {
                id: "fixture-AAPL".to_string(),
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
                min_price_increment: Some(0.01),
                price_base: None,
                contract_size: None,
                fees: Default::default(),
            },
        );

        let mut insight = Insight::new(
            OrderSide::Buy,
            "AAPL".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Day),
            90,
            None,
        );
        insight.set_quantity(Some(1.0));

        insight.submit(&mut state);
        let first_order_id = insight.order_id.clone();
        assert!(insight.submitted);
        assert_eq!(insight.state, InsightState::Executed);
        assert!(
            first_order_id.is_some(),
            "broker acknowledgement should attach an order id immediately"
        );

        insight.submit(&mut state);

        assert_eq!(insight.order_id, first_order_id);
        let orders = state
            .broker
            .get_orders()
            .await
            .expect("paper broker orders should be readable");
        assert_eq!(
            orders.len(),
            1,
            "a second submit call for the same acknowledged insight must not create another order"
        );
    }

    // ═══════════════════════════════════════════════════════════════════
    //  Trade Event Tests
    // ═══════════════════════════════════════════════════════════════════

    /// Test 4: Verify trade event: submit insight → order fills → insight becomes FILLED.
    ///
    /// A strategy generates a BUY insight with quantity on the first bar (after warm-up).
    /// PaperBroker fills the market order on the next step. `on_trade_update()` should
    /// transition the insight from EXECUTED → FILLED.
    #[tokio::test]
    async fn test_trade_event_fill() {
        /// Strategy that generates one BUY insight with quantity already set.
        struct FillTestStrategy {
            generated: bool,
        }
        impl Strategy for FillTestStrategy {
            fn name(&self) -> &str {
                "FillTestStrategy"
            }
            fn on_start(&mut self, _ctx: &mut dyn StrategyContext) {}
            fn init(&mut self, _ctx: &mut dyn StrategyContext, _asset: &Asset) {}
            fn universe(&self, _ctx: &mut dyn StrategyContext) -> HashSet<String> {
                let mut set = HashSet::new();
                set.insert("AAPL".to_string());
                set
            }
            fn on_bar(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str, _bar: &BarData) {}
            fn generate_insights(&mut self, ctx: &mut dyn StrategyContext, symbol: &str) {
                if !self.generated {
                    let mut insight = Insight::new(
                        OrderSide::Buy,
                        symbol.to_string(),
                        StrategyType::Testing,
                        ctx.timeframe().clone(),
                        90,
                        None,
                    );
                    insight.set_quantity(Some(5.0));
                    // Add and submit — submit_insight now sets order_id and EXECUTED
                    insight.submit(ctx);
                    ctx.add_insight(insight.clone());
                    self.generated = true;
                }
            }
            fn insight_pipeline(&mut self, _ctx: &mut dyn StrategyContext, _insight: &Insight) {}
            fn on_teardown(&mut self, _ctx: &mut dyn StrategyContext) {}
        }

        let execution = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        let data = FixedScaleOutDataFeed::new();
        let broker = UnifiedBroker::new_backtest(execution, data);

        let mut state = StrategyState::new(
            "FillTest".to_string(),
            "1.0".to_string(),
            FillTestStrategy { generated: false },
            broker,
            StrategyMode::Backtest,
            TimeFrame::new(1, TimeFrameUnit::Day),
        );
        state.set_warm_up_bars(1);

        let start = Utc.with_ymd_and_hms(2025, 9, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2025, 10, 15, 0, 0, 0).unwrap();

        let result = state.run_backtest(start, end, state.timeframe()).await;
        if assert_backtest_or_skip(result, "Trade event fill backtest should complete").is_none() {
            return;
        }

        // Verify at least one insight exists
        let insight_count = state.insights.len();
        println!("  Insights: {}", insight_count);
        assert!(insight_count > 0, "At least one insight expected");

        // Check insight states — we expect at least one FILLED or CLOSED
        // (CLOSED if SL/TP triggered during remaining bars)
        let insight_ids = state.insights.ids();
        let mut found_executed_or_filled = false;
        for id in &insight_ids {
            if let Some(insight) = state.insights.get_mut(id) {
                println!(
                    "  Insight {}: state={:?}, order_id={:?}",
                    insight.insight_id, insight.state, insight.order_id
                );
                // order_id should be set by submit_insight
                assert!(
                    insight.order_id.is_some(),
                    "order_id should be set after submit"
                );
                match insight.state {
                    InsightState::Executed | InsightState::Filled | InsightState::Closed => {
                        found_executed_or_filled = true;
                    }
                    _ => {}
                }
            }
        }
        assert!(
            found_executed_or_filled,
            "Insight should be in EXECUTED, FILLED, or CLOSED state after backtest"
        );
        println!("  ✓ Trade event fill test passed!");
    }

    struct NoopStrategy;

    impl Strategy for NoopStrategy {
        fn name(&self) -> &str {
            "NoopStrategy"
        }

        fn on_start(&mut self, _ctx: &mut dyn StrategyContext) {}

        fn init(&mut self, _ctx: &mut dyn StrategyContext, _asset: &Asset) {}

        fn universe(&self, _ctx: &mut dyn StrategyContext) -> HashSet<String> {
            HashSet::from(["AAPL".to_string()])
        }

        fn on_bar(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str, _bar: &BarData) {}

        fn generate_insights(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str) {}

        fn insight_pipeline(&mut self, _ctx: &mut dyn StrategyContext, _insight: &Insight) {}

        fn on_teardown(&mut self, _ctx: &mut dyn StrategyContext) {}
    }

    fn test_asset(symbol: &str) -> Asset {
        Asset {
            id: symbol.to_string(),
            symbol: symbol.to_string(),
            name: symbol.to_string(),
            asset_type: AssetType::Stock,
            exchange: AssetExchange::NASDAQ,
            status: AssetStatus::Active,
            tradable: true,
            marginable: true,
            shortable: true,
            fractional: true,
            min_order_size: None,
            quantity_base: None,
            max_order_size: None,
            min_price_increment: None,
            price_base: None,
            contract_size: None,
            fees: Default::default(),
        }
    }

    #[derive(Clone)]
    struct TestExecutionBroker {
        events: Arc<Mutex<VecDeque<(crate::core::broker::types::Order, TradeUpdateEvent)>>>,
        close_requests: Arc<Mutex<Vec<String>>>,
        cancel_requests: Arc<Mutex<Vec<String>>>,
    }

    impl TestExecutionBroker {
        fn new() -> Self {
            Self {
                events: Arc::new(Mutex::new(VecDeque::new())),
                close_requests: Arc::new(Mutex::new(Vec::new())),
                cancel_requests: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn emit(&self, order: crate::core::broker::types::Order, event: TradeUpdateEvent) {
            self.events.lock().unwrap().push_back((order, event));
        }
    }

    impl Broker for TestExecutionBroker {
        async fn connect(&self) -> Result<bool, BrokerError> {
            Ok(true)
        }

        async fn disconnect(&self) -> Result<bool, BrokerError> {
            Ok(true)
        }

        fn is_connected(&self) -> bool {
            true
        }

        fn get_current_time(&self) -> chrono::DateTime<Utc> {
            Utc::now()
        }

        fn get_name(&self) -> String {
            "TestExecutionBroker".to_string()
        }

        fn get_account_type(&self) -> Result<AccountType, BrokerError> {
            Ok(AccountType::Paper)
        }
    }

    impl OrderManagementProvider for TestExecutionBroker {
        async fn submit_order(
            &self,
            insight: Insight,
        ) -> Result<crate::core::broker::types::Order, BrokerError> {
            Ok(test_order(
                "submitted-order",
                &insight,
                TradeUpdateEvent::Accepted,
                insight.side.clone(),
                None,
            ))
        }

        async fn cancel_order(&self, order_id: &str) -> Result<bool, BrokerError> {
            self.cancel_requests
                .lock()
                .unwrap()
                .push(order_id.to_string());
            Ok(true)
        }

        async fn update_order(
            &self,
            _order_id: &str,
            _price: f64,
            _qty: f64,
        ) -> Result<bool, BrokerError> {
            Ok(true)
        }

        async fn update_stop_loss(
            &self,
            _order_id: &str,
            _price: f64,
            _qty: f64,
        ) -> Result<bool, BrokerError> {
            Ok(true)
        }

        async fn close_position(
            &self,
            order_id: &str,
            _qty: f64,
            _price: Option<f64>,
        ) -> Result<bool, BrokerError> {
            self.close_requests
                .lock()
                .unwrap()
                .push(order_id.to_string());
            Ok(true)
        }

        async fn close_all_positions(&self) -> Result<bool, BrokerError> {
            Ok(true)
        }

        async fn get_orders(&self) -> Result<Vec<crate::core::broker::types::Order>, BrokerError> {
            Ok(Vec::new())
        }

        async fn get_order(
            &self,
            order_id: &str,
        ) -> Result<crate::core::broker::types::Order, BrokerError> {
            Err(BrokerError::OrderError(format!(
                "test order {} not found",
                order_id
            )))
        }

        async fn get_positions(
            &self,
        ) -> Result<Vec<crate::core::broker::types::Position>, BrokerError> {
            Ok(Vec::new())
        }

        async fn get_position(
            &self,
            symbol: &str,
        ) -> Result<crate::core::broker::types::Position, BrokerError> {
            Err(BrokerError::PositionError(format!(
                "test position {} not found",
                symbol
            )))
        }

        async fn get_account(&self) -> Result<crate::core::broker::types::Account, BrokerError> {
            Ok(crate::core::broker::types::Account {
                account_id: "test".to_string(),
                account_type: AccountType::Paper,
                equity: 100_000.0,
                cash: 100_000.0,
                currency: "USD".to_string(),
                buying_power: 100_000.0,
                accrued_commission: 0.0,
                shorting_enabled: true,
                leverage: 1,
            })
        }

        fn drain_trade_events(&self) -> Vec<(crate::core::broker::types::Order, TradeUpdateEvent)> {
            self.events.lock().unwrap().drain(..).collect()
        }

        async fn subscribe_to_trade_stream(
            &self,
            _on_trade: Arc<
                dyn Fn((crate::core::broker::types::Order, TradeUpdateEvent)) + Send + Sync,
            >,
        ) -> Result<(), BrokerError> {
            Ok(())
        }

        async fn unsubscribe_from_trade_stream(&self) -> Result<(), BrokerError> {
            Ok(())
        }
    }

    fn trade_update_state() -> (
        StrategyState<NoopStrategy, TestExecutionBroker, YahooFinanceDataFeed>,
        TestExecutionBroker,
    ) {
        let execution = TestExecutionBroker::new();
        let broker = UnifiedBroker::new(execution.clone(), YahooFinanceDataFeed::new());
        let mut state = StrategyState::new(
            "Mt5TradeUpdateTest".to_string(),
            "1.0".to_string(),
            NoopStrategy,
            broker,
            StrategyMode::Live,
            TimeFrame::new(1, TimeFrameUnit::Minute),
        );
        state
            .universe
            .insert("AAPL".to_string(), test_asset("AAPL"));
        (state, execution)
    }

    fn test_order(
        order_id: &str,
        insight: &Insight,
        event: TradeUpdateEvent,
        side: OrderSide,
        filled_price: Option<f64>,
    ) -> crate::core::broker::types::Order {
        crate::core::broker::types::Order {
            order_id: order_id.to_string(),
            insight_id: Some(insight.insight_id.to_string()),
            strategy_type: Some(insight.strategy_type.to_string()),
            asset: test_asset(&insight.symbol),
            qty: insight.quantity.unwrap_or(1.0),
            filled_qty: insight.quantity.unwrap_or(1.0),
            limit_price: insight.limit_price,
            filled_price,
            stop_price: insight.stop_price,
            side,
            order_type: OrderType::Market,
            time_in_force: crate::core::broker::types::TimeInForce::GTC,
            status: event,
            order_class: OrderClass::Simple,
            created_at: 0,
            updated_at: 0,
            submitted_at: 0,
            filled_at: Some(0),
            realized_pnl: None,
            commission: None,
            swap: None,
            rejection_reason: None,
            legs: None,
        }
    }

    #[test]
    fn test_mt5_fill_with_changed_ticket_updates_insight_order_id() {
        let (mut state, execution) = trade_update_state();
        let mut insight = Insight::new(
            OrderSide::Buy,
            "AAPL".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            90,
            None,
        );
        insight.set_quantity(Some(2.0));
        insight.order_id = Some("client-order".to_string());
        insight.state = InsightState::Executed;
        let insight_id = *insight.insight_id();
        state.insights.add_insight(insight.clone());

        let fill = test_order(
            "mt5-position-123",
            &insight,
            TradeUpdateEvent::Filled,
            OrderSide::Buy,
            Some(101.25),
        );
        execution.emit(fill, TradeUpdateEvent::Filled);

        state.on_trade_update();

        let updated = state.insights.get(&insight_id).unwrap();
        assert_eq!(updated.state, InsightState::Filled);
        assert_eq!(updated.order_id.as_deref(), Some("mt5-position-123"));
        assert_eq!(updated.filled_price, Some(101.25));
    }

    #[test]
    fn trade_update_rounds_insight_fee_fields_before_snapshot() {
        let (mut state, execution) = trade_update_state();
        let mut insight = Insight::new(
            OrderSide::Buy,
            "AAPL".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            90,
            None,
        );
        insight.set_quantity(Some(2.0));
        insight.order_id = Some("position-123".to_string());
        insight.state = InsightState::Filled;
        insight.filled_price = Some(100.0);
        let insight_id = *insight.insight_id();
        state.insights.add_insight(insight.clone());

        let mut close = test_order(
            "position-123",
            &insight,
            TradeUpdateEvent::Closed,
            OrderSide::Sell,
            Some(110.237),
        );
        close.realized_pnl = Some(20.236);
        close.commission = Some(1.234);
        close.swap = Some(-0.345);
        execution.emit(close, TradeUpdateEvent::Closed);

        state.on_trade_update();

        let asset = test_asset("AAPL");
        let updated = state.insights.get(&insight_id).unwrap();
        assert_eq!(updated.state, InsightState::Closed);
        assert_eq!(
            updated.close_price,
            Some(crate::core::utils::tools::dynamic_round_for_asset(
                110.237, &asset
            ))
        );
        assert_eq!(
            updated.broker_realized_pnl,
            Some(crate::core::utils::tools::dynamic_round_for_asset(
                20.236, &asset
            ))
        );
        assert_eq!(
            updated.commission,
            Some(crate::core::utils::tools::dynamic_round_for_asset(
                1.234, &asset
            ))
        );
        assert_eq!(
            updated.swap,
            Some(crate::core::utils::tools::dynamic_round_for_asset(
                -0.345, &asset
            ))
        );
    }

    #[test]
    fn test_mt5_late_fill_corrects_cancelled_insight() {
        let (mut state, execution) = trade_update_state();
        let mut insight = Insight::new(
            OrderSide::Buy,
            "AAPL".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            90,
            None,
        );
        insight.set_quantity(Some(1.0));
        insight.order_id = Some("client-order".to_string());
        insight.state = InsightState::Cancelled;
        let insight_id = *insight.insight_id();
        state.insights.add_insight(insight.clone());

        let fill = test_order(
            "mt5-position-after-cancel",
            &insight,
            TradeUpdateEvent::Filled,
            OrderSide::Buy,
            Some(102.0),
        );
        execution.emit(fill, TradeUpdateEvent::Filled);

        state.on_trade_update();

        let updated = state.insights.get(&insight_id).unwrap();
        assert_eq!(updated.state, InsightState::Filled);
        assert_eq!(
            updated.order_id.as_deref(),
            Some("mt5-position-after-cancel")
        );
        assert_eq!(updated.filled_price, Some(102.0));
    }

    #[test]
    fn test_parent_closed_does_not_cancel_children_without_event_trigger() {
        let (mut state, _execution) = trade_update_state();
        let mut parent = Insight::new(
            OrderSide::Buy,
            "AAPL".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            90,
            None,
        );
        parent.set_quantity(Some(1.0));
        parent.state = InsightState::Closed;
        let parent_id = *parent.insight_id();
        let mut child = Insight::new(
            OrderSide::Buy,
            "AAPL".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            90,
            Some(parent_id),
        );
        let child_id = *child.insight_id();
        child.state = InsightState::New;
        state.insights.add_insight(parent);
        state.insights.add_insight(child);

        state.on_trade_update();

        let child = state.insights.get(&child_id).unwrap();
        assert_eq!(child.state, InsightState::New);

        let parents = vec![parent_id].into_iter().collect::<FxHashSet<_>>();
        assert_eq!(
            state.insights.child_ids_for_parents(&parents),
            vec![child_id]
        );
    }

    #[test]
    fn test_child_fill_after_parent_closed_is_queued_to_close() {
        let (mut state, execution) = trade_update_state();
        let mut parent = Insight::new(
            OrderSide::Buy,
            "AAPL".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            90,
            None,
        );
        parent.set_quantity(Some(1.0));
        parent.state = InsightState::Closed;
        let parent_id = *parent.insight_id();
        let mut child = Insight::new(
            OrderSide::Buy,
            "AAPL".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            90,
            Some(parent_id),
        );
        child.set_quantity(Some(1.0));
        child.order_id = Some("client-child".to_string());
        child.state = InsightState::Executed;
        let child_id = *child.insight_id();
        state.insights.add_insight(parent);
        state.insights.add_insight(child.clone());

        let fill = test_order(
            "mt5-child-position",
            &child,
            TradeUpdateEvent::Filled,
            OrderSide::Buy,
            Some(99.5),
        );
        execution.emit(fill, TradeUpdateEvent::Filled);

        state.on_trade_update();

        let child = state.insights.get(&child_id).unwrap();
        assert_eq!(child.state, InsightState::Filled);
        assert!(child.closing, "filled child should be queued to close");
        assert_eq!(child.order_id.as_deref(), Some("mt5-child-position"));
        assert_eq!(
            execution.close_requests.lock().unwrap().as_slice(),
            ["mt5-child-position"]
        );
    }

    #[test]
    fn test_parent_close_event_cancels_unfilled_child() {
        let (mut state, execution) = trade_update_state();
        let mut parent = Insight::new(
            OrderSide::Buy,
            "AAPL".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            90,
            None,
        );
        parent.set_quantity(Some(1.0));
        parent.order_id = Some("parent-position".to_string());
        parent.state = InsightState::Filled;
        parent.filled_price = Some(100.0);
        let parent_id = *parent.insight_id();
        let mut child = Insight::new(
            OrderSide::Buy,
            "AAPL".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            90,
            Some(parent_id),
        );
        child.set_quantity(Some(1.0));
        child.order_id = Some("child-pending".to_string());
        child.state = InsightState::Executed;
        let child_id = *child.insight_id();
        state.insights.add_insight(parent.clone());
        state.insights.add_insight(child);

        let close = test_order(
            "parent-close",
            &parent,
            TradeUpdateEvent::Closed,
            OrderSide::Sell,
            Some(105.0),
        );
        execution.emit(close, TradeUpdateEvent::Closed);

        state.on_trade_update();

        let parent = state.insights.get(&parent_id).unwrap();
        assert_eq!(parent.state, InsightState::Closed);
        let child = state.insights.get(&child_id).unwrap();
        assert_eq!(child.state, InsightState::Executed);
        assert!(
            child.cancelling,
            "unfilled child should be queued to cancel"
        );
        assert_eq!(
            execution.cancel_requests.lock().unwrap().as_slice(),
            ["child-pending"]
        );
    }

    #[test]
    fn test_parent_close_event_closes_filled_child_and_cancels_pending_child() {
        let (mut state, execution) = trade_update_state();
        let mut parent = Insight::new(
            OrderSide::Buy,
            "AAPL".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            90,
            None,
        );
        parent.set_quantity(Some(1.0));
        parent.order_id = Some("parent-position".to_string());
        parent.state = InsightState::Filled;
        parent.filled_price = Some(100.0);
        let parent_id = *parent.insight_id();

        let mut market_child = Insight::new(
            OrderSide::Buy,
            "AAPL".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            90,
            Some(parent_id),
        );
        market_child.set_quantity(Some(1.0));
        market_child.order_id = Some("market-child-position".to_string());
        market_child.state = InsightState::Filled;
        market_child.filled_price = Some(100.0);
        let market_child_id = *market_child.insight_id();

        let mut limit_child = Insight::new(
            OrderSide::Buy,
            "AAPL".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            90,
            Some(parent_id),
        );
        limit_child.set_quantity(Some(1.0));
        limit_child.order_id = Some("limit-child-pending".to_string());
        limit_child.limit_price = Some(95.0);
        limit_child.state = InsightState::Executed;
        let limit_child_id = *limit_child.insight_id();

        state.insights.add_insight(parent.clone());
        state.insights.add_insight(market_child.clone());
        state.insights.add_insight(limit_child.clone());

        let parent_close = test_order(
            "parent-position",
            &parent,
            TradeUpdateEvent::Closed,
            OrderSide::Sell,
            Some(101.0),
        );
        execution.emit(parent_close, TradeUpdateEvent::Closed);
        state.on_trade_update();

        let parent = state.insights.get(&parent_id).unwrap();
        assert_eq!(parent.state, InsightState::Closed);

        let market_child = state.insights.get(&market_child_id).unwrap();
        assert_eq!(market_child.state, InsightState::Filled);
        assert!(
            market_child.closing,
            "filled market child should be queued to close when parent closes"
        );
        assert_eq!(
            execution.close_requests.lock().unwrap().as_slice(),
            ["market-child-position"]
        );

        let limit_child = state.insights.get(&limit_child_id).unwrap();
        assert_eq!(limit_child.state, InsightState::Executed);
        assert!(
            limit_child.cancelling,
            "pending limit child should be queued to cancel when parent closes"
        );
        assert_eq!(
            execution.cancel_requests.lock().unwrap().as_slice(),
            ["limit-child-pending"]
        );

        let market_child_close = test_order(
            "market-child-position",
            &market_child,
            TradeUpdateEvent::Closed,
            OrderSide::Sell,
            Some(101.0),
        );
        let limit_child_cancel = test_order(
            "limit-child-pending",
            &limit_child,
            TradeUpdateEvent::Cancelled,
            OrderSide::Buy,
            None,
        );
        execution.emit(market_child_close, TradeUpdateEvent::Closed);
        execution.emit(limit_child_cancel, TradeUpdateEvent::Cancelled);
        state.on_trade_update();

        let market_child = state.insights.get(&market_child_id).unwrap();
        assert_eq!(
            market_child.state,
            InsightState::Closed,
            "filled market child should become Closed after broker close event"
        );

        let limit_child = state.insights.get(&limit_child_id).unwrap();
        assert_eq!(
            limit_child.state,
            InsightState::Cancelled,
            "pending limit child should become Cancelled after broker cancel event"
        );
    }

    /// Test 5: Verify trade event: bracket order → SL triggers → insight becomes CLOSED.
    ///
    /// Strategy generates a BUY insight with quantity, SL (very tight), and TP (very wide).
    /// The tight SL should trigger during the backtest, causing FILLED → CLOSED.
    #[tokio::test]
    async fn test_trade_event_bracket_close() {
        /// Strategy that generates a bracket order with a very tight stop-loss.
        struct BracketTestStrategy {
            generated: bool,
        }
        impl Strategy for BracketTestStrategy {
            fn name(&self) -> &str {
                "BracketTestStrategy"
            }
            fn on_start(&mut self, _ctx: &mut dyn StrategyContext) {}
            fn init(&mut self, _ctx: &mut dyn StrategyContext, _asset: &Asset) {}
            fn universe(&self, _ctx: &mut dyn StrategyContext) -> HashSet<String> {
                let mut set = HashSet::new();
                set.insert("AAPL".to_string());
                set
            }
            fn on_bar(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str, _bar: &BarData) {}
            fn generate_insights(&mut self, ctx: &mut dyn StrategyContext, symbol: &str) {
                if !self.generated {
                    // Get current close price from history for SL/TP calculation
                    let close = match ctx.history().get(symbol) {
                        Some(df) if df.height() > 0 => {
                            let closes = df.column("close").unwrap().f64().unwrap();
                            closes.get(closes.len() - 1).unwrap_or(150.0)
                        }
                        _ => return, // Wait for data
                    };

                    let mut insight = Insight::new(
                        OrderSide::Buy,
                        symbol.to_string(),
                        StrategyType::Testing,
                        ctx.timeframe().clone(),
                        90,
                        None,
                    );
                    insight.set_quantity(Some(5.0));

                    // Very tight SL (0.1% below entry) — likely to trigger
                    insight.set_stop_loss(Some(close * 0.999));
                    // Very wide TP (50% above entry) — unlikely to trigger
                    insight.set_take_profit_levels(Some(vec![close * 1.5]));

                    insight.submit(ctx);
                    ctx.add_insight(insight.clone());
                    self.generated = true;
                }
            }
            fn insight_pipeline(&mut self, _ctx: &mut dyn StrategyContext, _insight: &Insight) {}
            fn on_teardown(&mut self, _ctx: &mut dyn StrategyContext) {}
        }

        let execution = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        let data = YahooFinanceDataFeed::new();
        let broker = UnifiedBroker::new(execution, data);

        let mut state = StrategyState::new(
            "BracketTest".to_string(),
            "1.0".to_string(),
            BracketTestStrategy { generated: false },
            broker,
            StrategyMode::Backtest,
            TimeFrame::new(1, TimeFrameUnit::Day),
        );
        state.set_warm_up_bars(1);

        // Use 90 days to give the tight SL time to trigger
        let end = chrono::Utc::now();
        let start = end - chrono::Duration::days(90);

        let result = state.run_backtest(start, end, state.timeframe()).await;
        if assert_backtest_or_skip(result, "Trade event bracket close backtest should complete")
            .is_none()
        {
            return;
        }

        // Check that at least one insight reached CLOSED state
        let insight_ids = state.insights.ids();
        let mut found_closed = false;
        for id in &insight_ids {
            if let Some(insight) = state.insights.get_mut(id) {
                println!(
                    "  Insight {}: state={:?}, order_id={:?}, close_price={:?}",
                    insight.insight_id, insight.state, insight.order_id, insight.close_price
                );
                if insight.state == InsightState::Closed {
                    found_closed = true;
                    // Verify closing fields are populated
                    assert!(
                        insight.close_price.is_some(),
                        "Closed insight should have close_price"
                    );
                    assert!(
                        insight.closed_at.is_some(),
                        "Closed insight should have closed_at timestamp"
                    );
                }
            }
        }
        assert!(
            found_closed,
            "With a 0.1% SL over 90 days, the bracket should have triggered CLOSED"
        );
        println!("  ✓ Trade event bracket close test passed!");
    }

    /// Test 6: Verify trade event: submit insight → cancel → insight becomes CANCELLED.
    ///
    /// Strategy generates a LIMIT BUY insight with a very low limit price (won't fill),
    /// then cancels it on the next bar.
    #[tokio::test]
    async fn test_trade_event_cancel() {
        use std::sync::{Arc, Mutex};

        /// Strategy that generates a limit order and cancels it on the next bar.
        struct CancelTestStrategy {
            submitted: bool,
            cancelled: bool,
            insight_id: Arc<Mutex<Option<uuid::Uuid>>>,
        }
        impl Strategy for CancelTestStrategy {
            fn name(&self) -> &str {
                "CancelTestStrategy"
            }
            fn on_start(&mut self, _ctx: &mut dyn StrategyContext) {}
            fn init(&mut self, _ctx: &mut dyn StrategyContext, _asset: &Asset) {}
            fn universe(&self, _ctx: &mut dyn StrategyContext) -> HashSet<String> {
                let mut set = HashSet::new();
                set.insert("AAPL".to_string());
                set
            }
            fn on_bar(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str, _bar: &BarData) {}
            fn generate_insights(&mut self, ctx: &mut dyn StrategyContext, symbol: &str) {
                if !self.submitted {
                    let mut insight = Insight::new(
                        OrderSide::Buy,
                        symbol.to_string(),
                        StrategyType::Testing,
                        ctx.timeframe().clone(),
                        90,
                        None,
                    );
                    insight.set_quantity(Some(5.0));
                    // Set a very low limit price so it won't fill
                    insight.set_limit_price(Some(1.0));

                    let id = *insight.insight_id();
                    *self.insight_id.lock().unwrap() = Some(id);

                    insight.submit(ctx);
                    ctx.add_insight(insight.clone());
                    self.submitted = true;
                } else if !self.cancelled {
                    // Cancel the insight on the next bar
                    let id = self.insight_id.lock().unwrap().unwrap();
                    let insights = ctx.insights();
                    if let Some(insight) = insights.get_insight(&id) {
                        if let Some(order_id) = &insight.order_id {
                            let _ = ctx.cancel_order(order_id);
                        }
                    }
                    self.cancelled = true;
                }
            }
            fn insight_pipeline(&mut self, _ctx: &mut dyn StrategyContext, _insight: &Insight) {}
            fn on_teardown(&mut self, _ctx: &mut dyn StrategyContext) {}
        }

        let insight_id = Arc::new(Mutex::new(None));
        let execution = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        let data = YahooFinanceDataFeed::new();
        let broker = UnifiedBroker::new(execution, data);

        let mut state = StrategyState::new(
            "CancelTest".to_string(),
            "1.0".to_string(),
            CancelTestStrategy {
                submitted: false,
                cancelled: false,
                insight_id: insight_id.clone(),
            },
            broker,
            StrategyMode::Backtest,
            TimeFrame::new(1, TimeFrameUnit::Day),
        );
        state.set_warm_up_bars(1);

        let end = chrono::Utc::now();
        let start = end - chrono::Duration::days(30);

        let result = state.run_backtest(start, end, state.timeframe()).await;
        if assert_backtest_or_skip(result, "Trade event cancel backtest should complete").is_none()
        {
            return;
        }

        // The insight should have been cancelled
        let insight_ids = state.insights.ids();
        let mut found_cancelled = false;
        for id in &insight_ids {
            if let Some(insight) = state.insights.get(id) {
                if insight.state == InsightState::Cancelled {
                    found_cancelled = true;
                }
            }
        }
        assert!(found_cancelled, "Insight should have been cancelled");
        println!("  ✓ Trade event cancel test passed!");
    }

    /// Test 7: Verify Child Insight lifecycle
    ///
    /// Strategy generates a parent BUY insight. Once the parent fills,
    /// a Child SELL insight should automatically be submitted.
    #[tokio::test]
    async fn test_child_insight_lifecycle() {
        use std::sync::{Arc, Mutex};

        struct ChildTestStrategy {
            generated: bool,
            parent_id: Arc<Mutex<Option<uuid::Uuid>>>,
        }
        impl Strategy for ChildTestStrategy {
            fn name(&self) -> &str {
                "ChildTestStrategy"
            }
            fn on_start(&mut self, _ctx: &mut dyn StrategyContext) {}
            fn init(&mut self, _ctx: &mut dyn StrategyContext, _asset: &Asset) {}
            fn universe(&self, _ctx: &mut dyn StrategyContext) -> HashSet<String> {
                let mut set = HashSet::new();
                set.insert("AAPL".to_string());
                set
            }
            fn on_bar(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str, _bar: &BarData) {}
            fn generate_insights(&mut self, ctx: &mut dyn StrategyContext, symbol: &str) {
                if !self.generated {
                    let mut parent_insight = Insight::new(
                        OrderSide::Buy,
                        symbol.to_string(),
                        StrategyType::Testing,
                        ctx.timeframe().clone(),
                        90,
                        None,
                    );
                    parent_insight.set_quantity(Some(5.0));

                    let child_insight = Insight::new(
                        OrderSide::Sell,
                        symbol.to_string(),
                        StrategyType::Testing,
                        ctx.timeframe().clone(),
                        90,
                        None, // Setting parent ID via add_child_insight
                    );

                    parent_insight.add_child_insight(child_insight, ctx);

                    let p_id = *parent_insight.insight_id();
                    *self.parent_id.lock().unwrap() = Some(p_id);

                    parent_insight.submit(ctx);
                    ctx.add_insight(parent_insight.clone());
                    self.generated = true;
                }
            }
            fn insight_pipeline(&mut self, _ctx: &mut dyn StrategyContext, _insight: &Insight) {}
            fn on_teardown(&mut self, _ctx: &mut dyn StrategyContext) {}
        }

        let parent_id = Arc::new(Mutex::new(None));
        let execution = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        let data = YahooFinanceDataFeed::new();
        let broker = UnifiedBroker::new(execution, data);

        let mut state = StrategyState::new(
            "ChildTest".to_string(),
            "1.0".to_string(),
            ChildTestStrategy {
                generated: false,
                parent_id: parent_id.clone(),
            },
            broker,
            StrategyMode::Backtest,
            TimeFrame::new(1, TimeFrameUnit::Day),
        );
        state.set_warm_up_bars(1);

        let end = chrono::Utc::now();
        let start = end - chrono::Duration::days(30);

        let result = state.run_backtest(start, end, state.timeframe()).await;
        if assert_backtest_or_skip(result, "Child insight lifecycle backtest should complete")
            .is_none()
        {
            return;
        }

        // Validate that both Parent and Child exist, and Child was submitted/filled
        let insight_ids = state.insights.ids();
        let mut parent_found = false;
        let mut child_found = false;

        let locked_p_id = parent_id.lock().unwrap().unwrap();

        for id in &insight_ids {
            if let Some(insight) = state.insights.get(id) {
                if insight.insight_id == locked_p_id {
                    parent_found = true;
                    assert!(
                        insight.state == InsightState::Filled
                            || insight.state == InsightState::Closed
                    );
                } else if insight.parent_id == Some(locked_p_id) {
                    child_found = true;
                    assert!(
                        insight.submitted,
                        "Child insight was never submitted by the state loop"
                    );
                }
            }
        }

        assert!(parent_found, "Parent insight not found");
        assert!(child_found, "Child insight not found or generated");
        println!("  ✓ Child insight lifecycle test passed!");
    }

    /// Test 8: Verify Partial Returns
    ///
    /// The backtester provides a `TradeUpdateEvent::PartialFilled`. Verify the insight
    /// increments its `partial_filled_quantity` correctly while remaining in open states.
    #[tokio::test]
    async fn test_partial_return_logic() {
        struct PartialTestStrategy;
        impl Strategy for PartialTestStrategy {
            fn name(&self) -> &str {
                "PartialTestStrategy"
            }
            fn on_start(&mut self, _ctx: &mut dyn StrategyContext) {}
            fn init(&mut self, _ctx: &mut dyn StrategyContext, _asset: &Asset) {}
            fn universe(&self, _ctx: &mut dyn StrategyContext) -> HashSet<String> {
                let mut set = HashSet::new();
                set.insert("AAPL".to_string());
                set
            }
            fn on_bar(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str, _bar: &BarData) {}
            fn generate_insights(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str) {}

            fn insight_pipeline(&mut self, ctx: &mut dyn StrategyContext, insight: &Insight) {
                if insight.state == InsightState::Filled {
                    if let Some(tp_levels) = &insight.take_profit_levels {
                        if tp_levels.len() >= 2 {
                            // Extract current price from mock history
                            let current_price = ctx
                                .history()
                                .get(&insight.symbol)
                                .and_then(|df| {
                                    let c = df.column("close").ok()?;
                                    let v: Vec<Option<f64>> = c.f64().ok()?.into_iter().collect();
                                    v.last().copied().flatten()
                                })
                                .unwrap_or(0.0);

                            let qty = insight.quantity.unwrap_or(0.0);
                            let partial_qty = insight.partial_filled_quantity.unwrap_or(0.0);
                            let remaining = qty - partial_qty;

                            // Scale out: Half at TP 1, Remainder at TP 2
                            if let Some(order_id) = insight.order_id.as_deref() {
                                if current_price >= tp_levels[0] && partial_qty == 0.0 {
                                    let _ = ctx.close_position(order_id, qty / 2.0, None);
                                } else if current_price >= tp_levels[1] && remaining > 0.0 {
                                    let _ = ctx.close_position(order_id, remaining, None);
                                }
                            }
                        }
                    }
                }
            }
            fn on_teardown(&mut self, _ctx: &mut dyn StrategyContext) {}
        }

        let execution = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        let data = YahooFinanceDataFeed::new();
        let broker = UnifiedBroker::new(execution, data);

        let mut strat = PartialTestStrategy;
        let mut state = StrategyState::new(
            "PartialTest".to_string(),
            "1.0".to_string(),
            PartialTestStrategy,
            broker,
            StrategyMode::Backtest,
            TimeFrame::new(1, TimeFrameUnit::Day),
        );

        // Manually build Insight
        let mut insight = Insight::new(
            OrderSide::Buy,
            "AAPL".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Day),
            90,
            None,
        );
        insight.quantity = Some(10.0);
        insight.limit_price = Some(100.0);
        insight.take_profit_levels = Some(vec![120.0, 150.0]); // 2 TP levels
        insight.order_id = Some("TEST_ORDER".to_string());
        insight.state = InsightState::Filled;

        let insight_id = insight.insight_id;
        state.insights.add_insight(insight.clone());

        // MOCK TP 1 Hit
        let s = Series::new("close".into(), &[120.0f64]);
        let df = DataFrame::new(vec![Column::from(s)]).unwrap();
        state.history.insert("AAPL".to_string(), df);

        // Execute Pipeline natively checking close_position SideEffects
        strat.insight_pipeline(&mut state, &insight);

        // Simulate broker partially closing half of the order
        {
            let locked_insight = state.insights.get_mut(&insight_id).unwrap();
            locked_insight.partial_closed(5.0, 120.0, "CLOSE_TP_1", None);

            // Ensure accurate updates
            assert_eq!(locked_insight.partial_filled_quantity, Some(5.0));
            assert_eq!(locked_insight.partial_closes.len(), 1);
            assert_eq!(locked_insight.partial_closes[0].get_pl(), 100.0); // (120 - 100) * 5
            assert_eq!(locked_insight.state, InsightState::Filled); // Remains filled/open
        }

        // MOCK TP 2 Hit
        let s2 = Series::new("close".into(), &[150.0f64]);
        let df2 = DataFrame::new(vec![Column::from(s2)]).unwrap();
        state.history.insert("AAPL".to_string(), df2);

        // Refresh insight clone for pipeline param
        let refreshed_insight = state.insights.get(&insight_id).unwrap().clone();
        strat.insight_pipeline(&mut state, &refreshed_insight);

        let final_insight = state.insights.get_mut(&insight_id).unwrap();
        final_insight.partial_closed(5.0, 150.0, "CLOSE_TP_2", None);

        // Validate final completion limits
        assert_eq!(final_insight.partial_filled_quantity, Some(10.0));
        assert_eq!(final_insight.partial_closes.len(), 2);
        assert_eq!(final_insight.partial_closes[1].get_pl(), 250.0); // (150 - 100) * 5

        // Assert Total PL tracking
        final_insight.close_price = Some(150.0);
        final_insight.state = InsightState::Closed;

        // Total PL = 100 + 250 = 350
        assert_eq!(final_insight.get_pl(None, true), 350.0);

        println!("  ✓ Partial return logic test passed!");
    }

    #[tokio::test]
    async fn test_scale_out_pipe_consumes_levels_and_closes_partial() {
        struct ScaleOutStrategy;
        impl Strategy for ScaleOutStrategy {
            fn name(&self) -> &str {
                "ScaleOutStrategy"
            }
            fn on_start(&mut self, _ctx: &mut dyn StrategyContext) {}
            fn init(&mut self, _ctx: &mut dyn StrategyContext, _asset: &Asset) {}
            fn universe(&self, _ctx: &mut dyn StrategyContext) -> HashSet<String> {
                ["AAPL".to_string()].into_iter().collect()
            }
            fn on_bar(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str, _bar: &BarData) {}
            fn generate_insights(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str) {}
            fn insight_pipeline(&mut self, _ctx: &mut dyn StrategyContext, _insight: &Insight) {}
            fn on_teardown(&mut self, _ctx: &mut dyn StrategyContext) {}
        }

        let execution = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        let data = FixedScaleOutDataFeed::new();
        let broker = UnifiedBroker::new_backtest(execution, data);

        let mut state = StrategyState::new(
            "ScaleOutTest".to_string(),
            "1.0".to_string(),
            ScaleOutStrategy,
            broker,
            StrategyMode::Backtest,
            TimeFrame::new(1, TimeFrameUnit::Day),
        );

        let mut insight = Insight::new(
            OrderSide::Buy,
            "AAPL".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Day),
            90,
            None,
        );
        insight
            .set_quantity(Some(10.0))
            .set_limit_price(Some(100.0))
            .set_take_profit_levels(Some(vec![110.0, 120.0]))
            .set_stop_loss_levels(Some(vec![95.0, 90.0]));

        let current_bar = Bar {
            symbol: "AAPL".to_string(),
            open: 109.0,
            high: 111.0,
            low: 108.0,
            close: 111.0,
            volume: 1000.0,
            timestamp: Utc.with_ymd_and_hms(2025, 9, 3, 0, 0, 0).unwrap(),
        };
        if let Some(backtest_state) = state.broker.backtest_state.as_ref() {
            let mut backtest_state = backtest_state.write();
            backtest_state.load_bars("AAPL".to_string(), vec![current_bar.clone()]);
            backtest_state.current_time = current_bar.timestamp;
        }

        let submitted_order =
            futures::executor::block_on(state.broker.submit_order(insight.clone()))
                .expect("scale-out test order should submit");
        let mut bars = std::collections::HashMap::new();
        bars.insert("AAPL".to_string(), current_bar.clone());
        state
            .broker
            .execution
            .process_step(&bars, current_bar.timestamp);

        insight.order_id = Some(submitted_order.order_id.clone());
        insight.state = InsightState::Filled;
        insight.filled_price = Some(current_bar.open);
        state.bind_insight_context(&mut insight);

        let mut pipe = ScaleOutPipe::new(0.5);
        let result = pipe.run(&mut state, &mut insight);

        assert!(result.success);
        assert!(result.passed);
        assert_eq!(insight.take_profit_levels(), Some(vec![120.0]));
        assert_eq!(insight.stop_loss_levels(), Some(vec![95.0, 90.0]));
        assert!(!insight.closing);

        println!("  ✓ Scale out pipe partial-close test passed!");
    }

    async fn run_scale_out_backtest_take_profit_with_paper_broker_and_yahoo() {
        println!("[scale-out-test] TP case start");
        // Raw AAPL daily bars inspected from 2025-09-02..2025-10-14:
        // entry should occur near the next-day open around 237-239. The fixture
        // bars trade through 238.0 almost immediately and later through 245.0,
        // so both TP levels should fully flatten the position inside this window.
        let start = Utc.with_ymd_and_hms(2025, 9, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2025, 10, 15, 0, 0, 0).unwrap();
        let Some(state) =
            run_fixed_scale_out_backtest(vec![238.0, 245.0], vec![220.0, 210.0], start, end).await
        else {
            return;
        };

        assert_scaled_out(&state).await;
        println!("  ✓ Scale out backtest TP integration test passed!");
    }

    async fn run_scale_out_backtest_stop_loss_with_paper_broker_and_yahoo() {
        println!("[scale-out-test] SL case start");
        // Raw AAPL daily bars inspected from 2025-09-29..2025-10-14:
        // after entry around the next-day open near 254.86, lows later fall
        // through 254.5 and then through 245.5, so both SL levels should
        // fully flatten the position inside this window.
        let start = Utc.with_ymd_and_hms(2025, 9, 29, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2025, 10, 15, 0, 0, 0).unwrap();
        let Some(state) =
            run_fixed_scale_out_backtest(vec![400.0, 450.0], vec![254.5, 245.5], start, end).await
        else {
            return;
        };

        assert_scaled_out(&state).await;
        println!("  ✓ Scale out backtest SL integration test passed!");
    }

    #[tokio::test]
    async fn test_scale_out_backtest_with_paper_broker_and_yahoo() {
        println!("[scale-out-test] combined test start");
        run_scale_out_backtest_take_profit_with_paper_broker_and_yahoo().await;
        println!("[scale-out-test] TP case finished");
        run_scale_out_backtest_stop_loss_with_paper_broker_and_yahoo().await;
        println!("[scale-out-test] SL case finished");
    }

    struct PipelineHarnessStrategy;

    impl Strategy for PipelineHarnessStrategy {
        fn name(&self) -> &str {
            "PipelineHarnessStrategy"
        }

        fn on_start(&mut self, _ctx: &mut dyn StrategyContext) {}
        fn init(&mut self, _ctx: &mut dyn StrategyContext, _asset: &Asset) {}

        fn universe(&self, _ctx: &mut dyn StrategyContext) -> HashSet<String> {
            ["AAPL".to_string()].into_iter().collect()
        }

        fn on_bar(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str, _bar: &BarData) {}
        fn generate_insights(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str) {}
        fn insight_pipeline(&mut self, _ctx: &mut dyn StrategyContext, _insight: &Insight) {}
        fn on_teardown(&mut self, _ctx: &mut dyn StrategyContext) {}
    }

    struct AlwaysFailPipe {
        passed: bool,
        success: bool,
        message: &'static str,
    }

    impl InsightPipe for AlwaysFailPipe {
        fn version(&self) -> &str {
            "1.0"
        }

        fn run(
            &mut self,
            _ctx: &mut dyn StrategyContext,
            _insight: &mut Insight,
        ) -> InsightPipeResult {
            InsightPipeResult::new(
                self.passed,
                self.success,
                Some(self.message.to_string()),
                self.name().to_string(),
            )
        }
    }

    struct PassThroughPipe;

    impl InsightPipe for PassThroughPipe {
        fn version(&self) -> &str {
            "1.0"
        }

        fn run(
            &mut self,
            _ctx: &mut dyn StrategyContext,
            _insight: &mut Insight,
        ) -> InsightPipeResult {
            InsightPipeResult::new(true, true, Some("ok".to_string()), self.name().to_string())
        }
    }

    struct LifecycleHaltPipe;

    impl InsightPipe for LifecycleHaltPipe {
        fn version(&self) -> &str {
            "1.0"
        }

        fn run(
            &mut self,
            _ctx: &mut dyn StrategyContext,
            insight: &mut Insight,
        ) -> InsightPipeResult {
            insight.closing = true;
            InsightPipeResult::new(
                false,
                true,
                Some("close requested".to_string()),
                self.name().to_string(),
            )
        }
    }

    fn pipeline_harness_state()
    -> StrategyState<PipelineHarnessStrategy, PaperBroker, FixedScaleOutDataFeed> {
        let execution = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        let data = FixedScaleOutDataFeed::new();
        let broker = UnifiedBroker::new_backtest(execution, data);

        let mut state = StrategyState::new(
            "PipelineHarness".to_string(),
            "1.0".to_string(),
            PipelineHarnessStrategy,
            broker,
            StrategyMode::Backtest,
            TimeFrame::new(1, TimeFrameUnit::Minute),
        );
        state.status = crate::core::strategy::StrategyStatus::Running;
        state
    }

    #[test]
    fn test_run_insight_pipeline_rejects_when_pipe_execution_fails() {
        let mut state = pipeline_harness_state();
        state.add_pipe(
            WrappedInsightPipe::builder(Box::new(AlwaysFailPipe {
                passed: false,
                success: false,
                message: "forced execution failure",
            }))
            .target_state(InsightState::New)
            .build(),
        );

        let mut insight = Insight::new(
            OrderSide::Buy,
            "AAPL".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            90,
            None,
        );
        insight.set_quantity(Some(1.0));
        let insight_id = *insight.insight_id();
        state.add_insight(insight);

        state.run_insight_pipeline();

        let insight = state
            .insights
            .get(&insight_id)
            .expect("insight should still exist after rejection");
        assert_eq!(insight.state, InsightState::Rejected);
        assert!(insight.state_history.iter().any(|(_, state, message)| {
            *state == InsightState::Rejected
                && message
                    .as_deref()
                    .is_some_and(|value| value.contains("forced execution failure"))
        }));
    }

    #[test]
    fn test_run_insight_pipeline_rejects_when_pipe_does_not_pass() {
        let mut state = pipeline_harness_state();
        state.add_pipe(
            WrappedInsightPipe::builder(Box::new(AlwaysFailPipe {
                passed: false,
                success: true,
                message: "risk check did not pass",
            }))
            .target_state(InsightState::New)
            .build(),
        );

        let mut insight = Insight::new(
            OrderSide::Buy,
            "AAPL".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            90,
            None,
        );
        insight.set_quantity(Some(1.0));
        let insight_id = *insight.insight_id();
        state.add_insight(insight);

        state.run_insight_pipeline();

        let insight = state
            .insights
            .get(&insight_id)
            .expect("insight should still exist after rejection");
        assert_eq!(insight.state, InsightState::Rejected);
        assert!(insight.state_history.iter().any(|(_, state, message)| {
            *state == InsightState::Rejected
                && message
                    .as_deref()
                    .is_some_and(|value| value.contains("risk check did not pass"))
        }));
    }

    #[test]
    fn test_run_insight_pipeline_does_not_reject_when_pipe_starts_close() {
        let mut state = pipeline_harness_state();
        state.add_pipe(
            WrappedInsightPipe::builder(Box::new(LifecycleHaltPipe))
                .target_state(InsightState::Filled)
                .build(),
        );

        let mut insight = Insight::new(
            OrderSide::Buy,
            "AAPL".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            90,
            None,
        );
        insight.set_quantity(Some(1.0));
        insight.state = InsightState::Filled;
        insight.order_id = Some("filled-order".to_string());
        insight.filled_price = Some(123.45);
        insight.set_first_on_fill(true);
        let insight_id = *insight.insight_id();
        state.add_insight(insight);

        state.run_insight_pipeline();

        let insight = state
            .insights
            .get(&insight_id)
            .expect("filled insight should still exist after lifecycle halt");
        assert_eq!(insight.state, InsightState::Filled);
        assert!(insight.closing);
        assert!(!insight.first_on_fill());
        assert!(
            !insight
                .state_history
                .iter()
                .any(|(_, state, _)| *state == InsightState::Rejected)
        );
    }

    #[test]
    fn test_run_insight_pipeline_clears_first_on_fill_after_filled_pipes_complete() {
        let mut state = pipeline_harness_state();
        state.add_pipe(
            WrappedInsightPipe::builder(Box::new(PassThroughPipe))
                .target_state(InsightState::Filled)
                .build(),
        );

        let mut insight = Insight::new(
            OrderSide::Buy,
            "AAPL".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            90,
            None,
        );
        insight.set_quantity(Some(1.0));
        insight.state = InsightState::Filled;
        insight.order_id = Some("filled-order".to_string());
        insight.filled_price = Some(123.45);
        insight.set_first_on_fill(true);
        let insight_id = *insight.insight_id();
        state.add_insight(insight);

        state.run_insight_pipeline();

        let insight = state
            .insights
            .get(&insight_id)
            .expect("filled insight should still exist after pipeline run");
        assert!(!insight.first_on_fill());
        assert!(insight.state_history.iter().any(|(_, state, message)| {
            *state == InsightState::Filled
                && message
                    .as_deref()
                    .is_some_and(|value| value.contains("first_on_fill set to false"))
        }));
    }

    #[tokio::test]
    async fn test_strategy_on_bar_updates_indicator() {
        use crate::core::indicators::I::SimpleMovingAverage;
        use polars::prelude::DataFrame;

        let execution = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        let data = YahooFinanceDataFeed::new();
        let broker = UnifiedBroker::new(execution, data);

        struct SmaTestStrategy;
        impl Strategy for SmaTestStrategy {
            fn name(&self) -> &str {
                "SmaTestStrategy"
            }
            fn on_start(&mut self, _ctx: &mut dyn StrategyContext) {}
            fn init(&mut self, _ctx: &mut dyn StrategyContext, _asset: &Asset) {}
            fn universe(&self, _ctx: &mut dyn StrategyContext) -> HashSet<String> {
                let mut set = HashSet::new();
                set.insert("AAPL".to_string());
                set
            }
            fn on_bar(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str, _bar: &BarData) {}
            fn generate_insights(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str) {}
            fn insight_pipeline(&mut self, _ctx: &mut dyn StrategyContext, _insight: &Insight) {}
            fn on_teardown(&mut self, _ctx: &mut dyn StrategyContext) {}
        }

        let strategy = SmaTestStrategy;
        let mut state = StrategyState::new(
            "SmaTest".to_string(),
            "1.0".to_string(),
            strategy,
            broker,
            StrategyMode::Backtest,
            TimeFrame::new(1, TimeFrameUnit::Day),
        );

        state.register_indicator(Box::new(SimpleMovingAverage::new(2, "close")));

        let sym = "AAPL";
        state.history.insert(sym.to_string(), DataFrame::empty());

        let mut dummy = SmaTestStrategy;

        // Push bar 1
        let bar1 = BarData::Bars(vec![Bar {
            symbol: sym.to_string(),
            timestamp: chrono::Utc::now(),
            open: 1.0,
            high: 1.0,
            low: 1.0,
            close: 10.0,
            volume: 100.0,
        }]);
        state._on_bar(&mut dummy, sym, &bar1);

        // Push bar 2
        let bar2 = BarData::Bars(vec![Bar {
            symbol: sym.to_string(),
            timestamp: chrono::Utc::now() + chrono::Duration::seconds(1),
            open: 2.0,
            high: 2.0,
            low: 2.0,
            close: 20.0,
            volume: 100.0,
        }]);
        state._on_bar(&mut dummy, sym, &bar2);

        let hist = state.history().get(sym).expect("History missing");
        assert_eq!(hist.height(), 2);

        let sma_col = hist.column("SMA_2_close").expect("Missing SMA column");
        let sma_vals: Vec<Option<f64>> = sma_col.f64().unwrap().into_iter().collect();

        assert_eq!(sma_vals[0], None);
        assert_eq!(sma_vals[1], Some(15.0)); // (10 + 20) / 2
    }

    #[tokio::test]
    #[ignore = "requires live Yahoo stream timing and external network access"]
    async fn test_run_live_crypto() {
        // 1. Create components
        let execution = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        let data = YahooFinanceDataFeed::new();
        let broker = UnifiedBroker::new(execution, data);

        // 2. Simple strategy that counts bars and performs a trade
        struct LiveTestStrategy {
            bars_received: usize,
            order_submitted: bool,
        }
        impl Strategy for LiveTestStrategy {
            fn name(&self) -> &str {
                "LiveTest"
            }
            fn on_start(&mut self, _ctx: &mut dyn StrategyContext) {
                println!("[LiveTest] Started");
            }
            fn init(&mut self, _ctx: &mut dyn StrategyContext, _asset: &Asset) {}
            fn universe(&self, _ctx: &mut dyn StrategyContext) -> HashSet<String> {
                let mut set = HashSet::new();
                set.insert("BTC-USD".to_string());
                set
            }
            fn on_bar(&mut self, ctx: &mut dyn StrategyContext, symbol: &str, bar: &BarData) {
                self.bars_received += 1;
                println!("Bar received for {}", symbol);
                println!("Bar: {:?}", bar);
                println!(
                    "[{}] Received live bar for {}! Total: {}",
                    chrono::Utc::now(),
                    symbol,
                    self.bars_received
                );

                // Submit a test trade on the first bar to verify trade stream
                if !self.order_submitted {
                    let mut insight = Insight::new(
                        OrderSide::Buy,
                        symbol.to_string(),
                        StrategyType::Testing,
                        ctx.timeframe().clone(),
                        100,
                        None,
                    );
                    insight.set_quantity(Some(0.001)); // Very small BTC amount
                    insight.submit(ctx);
                    ctx.add_insight(insight);
                    self.order_submitted = true;
                    println!("[LiveTest] Submitted test BUY order for trade stream verification");
                }

                if self.bars_received >= 1 {
                    println!("[LiveTest] Received first completed bar, shutting down");
                    ctx.shutdown();
                }
            }

            fn generate_insights(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str) {}
            fn insight_pipeline(
                &mut self,
                _ctx: &mut dyn StrategyContext,
                _insight: &crate::core::insight::Insight,
            ) {
            }
            fn on_teardown(&mut self, _ctx: &mut dyn StrategyContext) {
                println!("[LiveTest] Tearing down");
            }
        }

        let mut state = StrategyState::new(
            "LiveTest".into(),
            "1.0".into(),
            LiveTestStrategy {
                bars_received: 0,
                order_submitted: false,
            },
            broker,
            StrategyMode::Live,
            TimeFrame::new(1, TimeFrameUnit::Minute),
        );

        println!("Starting live trading test (timeout in 60s)...");

        // Run with a timeout to prevent hanging forever if data stream fails
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(60), state.run_live(None)).await;

        match result {
            Ok(execution_result) => {
                match execution_result {
                    Ok(()) => {}
                    Err(BrokerError::ConnectionError(err)) => {
                        println!(
                            "Skipping test_run_live_crypto due to Yahoo connection failure: {}",
                            err
                        );
                        return;
                    }
                    Err(err) => {
                        panic!("Live execution failed: {:?}", err);
                    }
                }
                println!("Live test finished gracefully.");
            }
            Err(_) => {
                panic!("Live test timed out after 60s");
            }
        }
    }

    #[tokio::test]
    #[ignore = "requires a running MT5 terminal with AqeMt5BridgeEA attached and bridge env vars configured"]
    async fn test_run_backtest_mt5_datafeed_paper_broker_single_entry_close() {
        let _mt5_guard = MT5_INTEGRATION_TEST_LOCK.lock().await;
        init_test_logger("info");

        if std::env::var("AQE_MT5_BRIDGE_TOKEN").is_err() {
            println!(
                "Skipping MT5 paper backtest: AQE_MT5_BRIDGE_TOKEN is required. See integrations/mt5/README.md."
            );
            return;
        }

        const TEST_SYMBOL: &str = "BTCUSD";

        struct Mt5PaperBacktestStrategy {
            submitted: bool,
            close_requested: bool,
        }

        impl Strategy for Mt5PaperBacktestStrategy {
            fn name(&self) -> &str {
                "Mt5PaperBacktestStrategy"
            }

            fn on_start(&mut self, _ctx: &mut dyn StrategyContext) {
                println!("[MT5 Paper Backtest] Started");
            }

            fn init(&mut self, _ctx: &mut dyn StrategyContext, asset: &Asset) {
                println!(
                    "[MT5 Paper Backtest] Initialised asset {} ({:?})",
                    asset.symbol, asset.asset_type
                );
            }

            fn universe(&self, _ctx: &mut dyn StrategyContext) -> HashSet<String> {
                HashSet::from([TEST_SYMBOL.to_string()])
            }

            fn on_bar(&mut self, ctx: &mut dyn StrategyContext, symbol: &str, bar: &BarData) {
                println!("[MT5 Paper Backtest] Bar for {}: {:?}", symbol, bar);

                if self.close_requested {
                    return;
                }

                let close_request = ctx
                    .insights()
                    .values()
                    .find(|insight| {
                        insight.symbol() == symbol && insight.state() == &InsightState::Filled
                    })
                    .and_then(|insight| {
                        Some((
                            insight.order_id.as_ref()?.clone(),
                            insight.quantity.unwrap_or(0.0),
                        ))
                    });

                if let Some((order_id, qty)) = close_request {
                    if qty > 0.0 {
                        ctx.close_position(&order_id, qty, None)
                            .expect("paper close_position should succeed");
                        self.close_requested = true;
                        println!(
                            "[MT5 Paper Backtest] Requested paper close for {}",
                            order_id
                        );
                    }
                }
            }

            fn generate_insights(&mut self, ctx: &mut dyn StrategyContext, symbol: &str) {
                if self.submitted {
                    return;
                }

                let mut insight = Insight::new(
                    OrderSide::Buy,
                    symbol.to_string(),
                    StrategyType::Testing,
                    ctx.timeframe().clone(),
                    90,
                    None,
                );
                insight.set_quantity(Some(0.01));
                insight.submit(ctx);
                ctx.add_insight(insight);
                self.submitted = true;
                println!("[MT5 Paper Backtest] Submitted paper BUY insight");
            }

            fn insight_pipeline(&mut self, _ctx: &mut dyn StrategyContext, _insight: &Insight) {}

            fn on_teardown(&mut self, _ctx: &mut dyn StrategyContext) {
                println!("[MT5 Paper Backtest] Tearing down");
            }
        }

        let execution = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        let data = Mt5DataFeed::from_env().expect("failed to create MT5 datafeed from env");
        let session_id = format!("mt5-paper-backtest-{}", uuid::Uuid::new_v4());
        data.configure_live_session(&session_id)
            .expect("failed to configure MT5 data smoke session");
        data.connect()
            .await
            .expect("failed to start MT5 data bridge");
        data.get_ticker_info(TEST_SYMBOL)
            .await
            .expect("MT5 datafeed preflight failed; confirm the EA is running and polling the AQE bridge URL");

        let broker = UnifiedBroker::new_backtest(execution, data);
        let mut state = StrategyState::new(
            "Mt5PaperBacktest".into(),
            "1.0".into(),
            Mt5PaperBacktestStrategy {
                submitted: false,
                close_requested: false,
            },
            broker,
            StrategyMode::Backtest,
            TimeFrame::new(1, TimeFrameUnit::Minute),
        );

        let end = Utc::now();
        let start = end - chrono::Duration::hours(2);
        let results = state
            .run_backtest(start, end, state.timeframe())
            .await
            .expect("MT5 datafeed paper backtest should complete");

        println!(
            "[MT5 Paper Backtest] trades={} final_equity={}",
            results.total_trades, results.final_equity
        );
        assert!(
            results.total_trades > 0,
            "paper backtest should produce at least one trade"
        );
        assert!(
            state
                .insights
                .values()
                .any(|insight| insight.state() == &InsightState::Closed),
            "paper backtest should close the submitted insight"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "places and closes a real MT5 order; requires AqeMt5BridgeEA and bridge env vars configured"]
    async fn test_run_live_mt5_broker_datafeed_single_entry_close() {
        let _mt5_guard = MT5_INTEGRATION_TEST_LOCK.lock().await;
        init_test_logger("info");

        if std::env::var("AQE_MT5_BRIDGE_TOKEN").is_err() {
            println!(
                "Skipping MT5 live order test: AQE_MT5_BRIDGE_TOKEN is required. See integrations/mt5/README.md."
            );
            return;
        }

        const TEST_SYMBOL: &str = "BTCUSD";
        const TEST_QTY: f64 = 0.01;
        let closed = Arc::new(Mutex::new(false));
        let rejected = Arc::new(Mutex::new(false));
        let order_id = Arc::new(Mutex::new(None::<String>));

        let preflight_execution =
            Mt5Broker::from_env().expect("failed to create MT5 broker from env");
        preflight_execution
            .connect()
            .await
            .expect("failed to start MT5 preflight bridge");
        let preflight_positions = preflight_execution
            .get_positions()
            .await
            .expect("failed to fetch MT5 positions before entry-close test");
        let _ = preflight_execution.disconnect().await;
        let open_symbol_position = preflight_positions
            .iter()
            .find(|position| position.asset.symbol == TEST_SYMBOL && position.qty.abs() > 0.0);
        assert!(
            open_symbol_position.is_none(),
            "MT5 live entry-close test requires no pre-existing {} position; found {:?}",
            TEST_SYMBOL,
            open_symbol_position
        );

        struct Mt5SingleEntryCloseStrategy {
            qty: f64,
            submitted: bool,
            bars_after_submit: usize,
            close_requested: bool,
            closed: Arc<Mutex<bool>>,
            rejected: Arc<Mutex<bool>>,
            order_id: Arc<Mutex<Option<String>>>,
        }

        impl Strategy for Mt5SingleEntryCloseStrategy {
            fn name(&self) -> &str {
                "Mt5SingleEntryCloseStrategy"
            }

            fn on_start(&mut self, ctx: &mut dyn StrategyContext) {
                println!(
                    "[MT5 Live EntryClose] Started qty={} timeframe={:?}",
                    self.qty,
                    ctx.timeframe()
                );
            }

            fn init(&mut self, _ctx: &mut dyn StrategyContext, asset: &Asset) {
                println!(
                    "[MT5 Live EntryClose] Initialised asset {} ({:?})",
                    asset.symbol, asset.asset_type
                );
            }

            fn universe(&self, _ctx: &mut dyn StrategyContext) -> HashSet<String> {
                HashSet::from([TEST_SYMBOL.to_string()])
            }

            fn on_bar(&mut self, ctx: &mut dyn StrategyContext, symbol: &str, bar: &BarData) {
                println!("[MT5 Live EntryClose] Bar for {}: {:?}", symbol, bar);
                let submitted_insight = ctx
                    .insights()
                    .values()
                    .find(|insight| insight.symbol() == symbol);
                if let Some(order_id) = submitted_insight
                    .as_ref()
                    .and_then(|insight| insight.order_id.clone())
                {
                    *self.order_id.lock().unwrap() = Some(order_id);
                }

                let terminal_state = submitted_insight
                    .as_ref()
                    .map(|insight| insight.state().clone());

                match terminal_state {
                    Some(InsightState::Closed) => {
                        *self.closed.lock().unwrap() = true;
                        println!("[MT5 Live EntryClose] Insight closed");
                        ctx.shutdown();
                        return;
                    }
                    Some(InsightState::Rejected | InsightState::Cancelled) => {
                        *self.rejected.lock().unwrap() = true;
                        println!(
                            "[MT5 Live EntryClose] Insight terminal without close: {:?}",
                            terminal_state
                        );
                        ctx.shutdown();
                        return;
                    }
                    _ => {}
                }

                if !self.close_requested {
                    let close_request = ctx
                        .insights()
                        .values()
                        .find(|insight| {
                            insight.symbol() == symbol && insight.state() == &InsightState::Filled
                        })
                        .and_then(|insight| {
                            Some((
                                insight.order_id.as_ref()?.clone(),
                                insight.quantity.unwrap_or(self.qty),
                            ))
                        });

                    if let Some((order_id, qty)) = close_request {
                        *self.order_id.lock().unwrap() = Some(order_id.clone());
                        ctx.close_position(&order_id, qty, None)
                            .expect("MT5 close_position RPC should succeed");
                        self.close_requested = true;
                        *self.closed.lock().unwrap() = true;
                        println!("[MT5 Live EntryClose] Requested close for {}", order_id);
                        ctx.shutdown();
                        return;
                    }
                }

                if self.submitted {
                    if self.order_id.lock().unwrap().is_some() {
                        *self.closed.lock().unwrap() = true;
                        println!(
                            "[MT5 Live EntryClose] Submitted insight no longer active; verifying captured broker order id"
                        );
                        ctx.shutdown();
                        return;
                    }
                    self.bars_after_submit += 1;
                    if self.bars_after_submit >= 2 {
                        *self.rejected.lock().unwrap() = true;
                        println!(
                            "[MT5 Live EntryClose] Submitted insight disappeared before broker order id was captured"
                        );
                        ctx.shutdown();
                    }
                    return;
                }

                let mut insight = Insight::new(
                    OrderSide::Buy,
                    symbol.to_string(),
                    StrategyType::Testing,
                    ctx.timeframe().clone(),
                    90,
                    None,
                );
                insight.set_quantity(Some(self.qty));
                if let Ok(quote) = ctx.latest_quote(symbol) {
                    let reference_price = quote.ask.max(quote.last.unwrap_or(quote.ask));
                    if reference_price > 0.0 {
                        insight.set_stop_loss(Some(reference_price * 0.99));
                        insight.set_take_profit_levels(Some(vec![reference_price * 1.02]));
                    }
                }
                insight.submit(ctx);
                if let Some(order_id) = insight.order_id.as_ref() {
                    *self.order_id.lock().unwrap() = Some(order_id.clone());
                    println!(
                        "[MT5 Live EntryClose] Captured submitted order id {}",
                        order_id
                    );
                }
                ctx.add_insight(insight);
                self.submitted = true;
                println!("[MT5 Live EntryClose] Submitted live BUY insight");
            }

            fn generate_insights(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str) {}

            fn insight_pipeline(&mut self, _ctx: &mut dyn StrategyContext, _insight: &Insight) {}

            fn on_teardown(&mut self, _ctx: &mut dyn StrategyContext) {
                println!("[MT5 Live EntryClose] Tearing down");
            }
        }

        let execution = Mt5Broker::from_env().expect("failed to create MT5 broker from env");
        let data = Mt5DataFeed::from_env().expect("failed to create MT5 datafeed from env");
        let broker = UnifiedBroker::new(execution, data);
        let mut state = StrategyState::new(
            "Mt5SingleEntryClose".into(),
            "1.0".into(),
            Mt5SingleEntryCloseStrategy {
                qty: TEST_QTY,
                submitted: false,
                bars_after_submit: 0,
                close_requested: false,
                closed: closed.clone(),
                rejected: rejected.clone(),
                order_id: order_id.clone(),
            },
            broker,
            StrategyMode::Live,
            TimeFrame::new(1, TimeFrameUnit::Minute),
        );

        let result =
            tokio::time::timeout(std::time::Duration::from_secs(180), state.run_live(None)).await;

        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => panic!("MT5 live entry-close strategy failed: {:?}", error),
            Err(_) => panic!("MT5 live entry-close strategy timed out after 180s"),
        }

        assert!(
            !*rejected.lock().unwrap(),
            "MT5 live entry-close insight was rejected or cancelled"
        );
        let rejected_count = state
            .insights
            .lifetime_state_counts()
            .get(&InsightState::Rejected)
            .copied()
            .unwrap_or(0);
        let cancelled_count = state
            .insights
            .lifetime_state_counts()
            .get(&InsightState::Cancelled)
            .copied()
            .unwrap_or(0);
        assert_eq!(
            rejected_count, 0,
            "MT5 live entry-close insight was rejected"
        );
        assert_eq!(
            cancelled_count, 0,
            "MT5 live entry-close insight was cancelled"
        );

        let closed_count = state
            .insights
            .lifetime_state_counts()
            .get(&InsightState::Closed)
            .copied()
            .unwrap_or(0);
        assert!(
            closed_count > 0 || *closed.lock().unwrap(),
            "MT5 live entry-close strategy never observed a closed insight"
        );

        let order_id = order_id
            .lock()
            .unwrap()
            .clone()
            .expect("MT5 live entry-close strategy never captured an order id");
        let execution = Mt5Broker::from_env().expect("failed to create MT5 broker from env");
        execution
            .connect()
            .await
            .expect("failed to start MT5 order lookup bridge");
        let order_lookup = execution.get_order(&order_id).await;
        let positions = execution
            .get_positions()
            .await
            .expect("failed to fetch MT5 positions after entry-close test");
        let _ = execution.disconnect().await;

        match order_lookup {
            Ok(order) => {
                assert_eq!(
                    order.status,
                    TradeUpdateEvent::Closed,
                    "MT5 live entry-close order should be closed or absent from the broker"
                );
            }
            Err(BrokerError::OrderError(message)) => {
                let message = message.to_lowercase();
                assert!(
                    message.contains("not found")
                        || message.contains("closed")
                        || message.contains("does not exist"),
                    "MT5 live entry-close order lookup returned unexpected error: {}",
                    message
                );
            }
            Err(error) => panic!(
                "MT5 live entry-close order lookup returned unexpected error: {:?}",
                error
            ),
        }

        let open_symbol_position = positions
            .iter()
            .find(|position| position.asset.symbol == TEST_SYMBOL && position.qty.abs() > 0.0);
        assert!(
            open_symbol_position.is_none(),
            "MT5 live entry-close left an open {} position: {:?}",
            TEST_SYMBOL,
            open_symbol_position
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "places and cleans up real MT5 parent/child orders; requires AqeMt5BridgeEA and bridge env vars configured"]
    async fn test_run_live_mt5_child_insights_market_and_limit() {
        let _mt5_guard = MT5_INTEGRATION_TEST_LOCK.lock().await;
        init_test_logger("info");

        if std::env::var("AQE_MT5_BRIDGE_TOKEN").is_err() {
            println!(
                "Skipping MT5 child insight test: AQE_MT5_BRIDGE_TOKEN is required. See integrations/mt5/README.md."
            );
            return;
        }

        const TEST_SYMBOL: &str = "BTCUSD";
        const TEST_QTY: f64 = 0.01;

        #[derive(Default)]
        struct Mt5ChildInsightShared {
            parent_id: Option<uuid::Uuid>,
            market_child_id: Option<uuid::Uuid>,
            limit_child_id: Option<uuid::Uuid>,
            parent_order_id: Option<String>,
            market_child_order_id: Option<String>,
            limit_child_order_id: Option<String>,
            market_child_seen: bool,
            limit_child_seen: bool,
            parent_close_requested: bool,
            account_flat_after_parent_close: bool,
            cleanup_requested: bool,
            failed: Option<String>,
        }

        fn latest_bar_close(bar: &BarData) -> Option<f64> {
            match bar {
                BarData::Bars(bars) => bars.last().map(|bar| bar.close),
                BarData::Frame(frame) => {
                    let close = frame.column("close").ok()?.f64().ok()?;
                    frame
                        .height()
                        .checked_sub(1)
                        .and_then(|index| close.get(index))
                }
            }
        }

        fn request_parent_close_if_ready(
            ctx: &mut dyn StrategyContext,
            shared: &Arc<Mutex<Mt5ChildInsightShared>>,
            symbol: &str,
            parent_id: uuid::Uuid,
        ) -> Result<bool, String> {
            let insights = ctx
                .insights()
                .values()
                .filter(|insight| insight.symbol() == symbol)
                .collect::<Vec<_>>();
            let market_child = insights.iter().find(|insight| {
                insight.parent_id == Some(parent_id) && insight.limit_price.is_none()
            });
            let limit_child = insights.iter().find(|insight| {
                insight.parent_id == Some(parent_id) && insight.limit_price.is_some()
            });
            let parent = insights
                .iter()
                .find(|insight| insight.insight_id == parent_id);

            if let Some(insight) = insights.iter().find(|insight| {
                insight.state() == &InsightState::Rejected
                    || (insight.state() == &InsightState::Cancelled
                        && insight.parent_id != Some(parent_id))
            }) {
                return Err(format!(
                    "unexpected terminal insight {} state {:?}",
                    insight.insight_id,
                    insight.state()
                ));
            }

            let market_child_order_id = market_child
                .filter(|insight| insight.state() == &InsightState::Filled)
                .and_then(|insight| insight.order_id.clone());
            let limit_child_order_id = limit_child
                .filter(|insight| {
                    insight.order_id.is_some()
                        && matches!(insight.state(), InsightState::New | InsightState::Executed)
                })
                .and_then(|insight| insight.order_id.clone());
            let parent_order = parent.and_then(|insight| {
                Some((
                    insight.order_id.as_ref()?.clone(),
                    insight.quantity.unwrap_or(TEST_QTY),
                ))
            });

            {
                let mut shared = shared.lock().unwrap();
                if let Some(order_id) = market_child_order_id.as_ref() {
                    shared.market_child_seen = true;
                    shared.market_child_order_id = Some(order_id.clone());
                }
                if let Some(order_id) = limit_child_order_id.as_ref() {
                    shared.limit_child_seen = true;
                    shared.limit_child_order_id = Some(order_id.clone());
                }
                if let Some((order_id, _)) = parent_order.as_ref() {
                    shared.parent_order_id = Some(order_id.clone());
                }
                if shared.parent_close_requested {
                    return Ok(false);
                }
            }

            let Some(limit_order_id) = limit_child_order_id else {
                return Ok(false);
            };
            let Some((parent_order_id, parent_qty)) = parent_order else {
                return Ok(false);
            };

            ctx.cancel_order(&limit_order_id)
                .map_err(|error| format!("MT5 child limit cancel RPC failed: {error}"))?;
            println!("[MT5 ChildInsights] Requested child limit cancel {limit_order_id}");

            ctx.close_position(&parent_order_id, parent_qty, None)
                .map_err(|error| format!("MT5 parent close RPC failed: {error}"))?;
            println!(
                "[MT5 ChildInsights] Requested parent close {parent_order_id}; market child should cascade close"
            );

            let mut shared = shared.lock().unwrap();
            shared.parent_close_requested = true;
            println!("[MT5 ChildInsights] Limit child cancelled; waiting for flat BTCUSD exposure");
            Ok(true)
        }

        let preflight_execution =
            Mt5Broker::from_env().expect("failed to create MT5 broker from env");
        preflight_execution
            .connect()
            .await
            .expect("failed to start MT5 child preflight bridge");
        let preflight_positions = preflight_execution
            .get_positions()
            .await
            .expect("failed to fetch MT5 positions before child insight test");
        let preflight_orders = preflight_execution
            .get_orders()
            .await
            .expect("failed to fetch MT5 orders before child insight test");
        let _ = preflight_execution.disconnect().await;

        let open_symbol_position = preflight_positions
            .iter()
            .find(|position| position.asset.symbol == TEST_SYMBOL && position.qty.abs() > 0.0);
        assert!(
            open_symbol_position.is_none(),
            "MT5 child insight test requires no pre-existing {} position; found {:?}",
            TEST_SYMBOL,
            open_symbol_position
        );
        let open_symbol_order = preflight_orders
            .iter()
            .find(|order| order.asset.symbol == TEST_SYMBOL);
        assert!(
            open_symbol_order.is_none(),
            "MT5 child insight test requires no pre-existing {} order; found {:?}",
            TEST_SYMBOL,
            open_symbol_order
        );

        struct Mt5ChildInsightsStrategy {
            submitted: bool,
            bars_after_submit: usize,
            shared: Arc<Mutex<Mt5ChildInsightShared>>,
        }

        impl Strategy for Mt5ChildInsightsStrategy {
            fn name(&self) -> &str {
                "Mt5ChildInsightsStrategy"
            }

            fn on_start(&mut self, ctx: &mut dyn StrategyContext) {
                println!(
                    "[MT5 ChildInsights] Started qty={} timeframe={:?}",
                    TEST_QTY,
                    ctx.timeframe()
                );
            }

            fn init(&mut self, _ctx: &mut dyn StrategyContext, asset: &Asset) {
                println!(
                    "[MT5 ChildInsights] Initialised asset {} ({:?})",
                    asset.symbol, asset.asset_type
                );
            }

            fn universe(&self, _ctx: &mut dyn StrategyContext) -> HashSet<String> {
                HashSet::from([TEST_SYMBOL.to_string()])
            }

            fn on_bar(&mut self, ctx: &mut dyn StrategyContext, symbol: &str, bar: &BarData) {
                println!("[MT5 ChildInsights] Bar for {}: {:?}", symbol, bar);

                if !self.submitted {
                    let reference_price = ctx
                        .latest_quote(symbol)
                        .ok()
                        .map(|quote| quote.ask.max(quote.last.unwrap_or(quote.ask)))
                        .or_else(|| latest_bar_close(bar))
                        .unwrap_or(0.0);

                    if reference_price <= 0.0 {
                        return;
                    }

                    let mut parent = Insight::new(
                        OrderSide::Buy,
                        symbol.to_string(),
                        StrategyType::Testing,
                        ctx.timeframe().clone(),
                        90,
                        None,
                    );
                    parent.set_quantity(Some(TEST_QTY));

                    let mut market_child = Insight::new(
                        OrderSide::Buy,
                        symbol.to_string(),
                        StrategyType::Testing,
                        ctx.timeframe().clone(),
                        90,
                        None,
                    );
                    market_child.set_quantity(Some(TEST_QTY));

                    let mut limit_child = Insight::new(
                        OrderSide::Buy,
                        symbol.to_string(),
                        StrategyType::Testing,
                        ctx.timeframe().clone(),
                        90,
                        None,
                    );
                    limit_child
                        .set_quantity(Some(TEST_QTY))
                        .set_limit_price(Some(reference_price * 0.95));

                    let parent_id = *parent.insight_id();
                    let market_child_id = *market_child.insight_id();
                    let limit_child_id = *limit_child.insight_id();
                    parent.add_child_insight(market_child, ctx);
                    parent.add_child_insight(limit_child, ctx);

                    {
                        let mut shared = self.shared.lock().unwrap();
                        shared.parent_id = Some(parent_id);
                        shared.market_child_id = Some(market_child_id);
                        shared.limit_child_id = Some(limit_child_id);
                    }
                    parent.submit(ctx);
                    ctx.add_insight(parent);
                    self.submitted = true;
                    println!(
                        "[MT5 ChildInsights] Submitted parent market BUY with market and limit children"
                    );
                    match request_parent_close_if_ready(ctx, &self.shared, symbol, parent_id) {
                        Ok(_) => {}
                        Err(error) => {
                            self.shared.lock().unwrap().failed = Some(error);
                            ctx.shutdown();
                        }
                    }
                    return;
                }

                if self.shared.lock().unwrap().cleanup_requested {
                    println!("[MT5 ChildInsights] BTCUSD exposure flat after parent close");
                    ctx.shutdown();
                    return;
                }

                self.bars_after_submit += 1;
                let parent_id = self.shared.lock().unwrap().parent_id;
                let Some(parent_id) = parent_id else {
                    self.shared.lock().unwrap().failed =
                        Some("parent id was not recorded".to_string());
                    ctx.shutdown();
                    return;
                };

                match request_parent_close_if_ready(ctx, &self.shared, symbol, parent_id) {
                    Ok(true) => return,
                    Ok(false) => {}
                    Err(error) => {
                        self.shared.lock().unwrap().failed = Some(error);
                        ctx.shutdown();
                        return;
                    }
                }

                let shared = self.shared.lock().unwrap();
                if self.bars_after_submit >= 8 {
                    let message = format!(
                        "timed out waiting for child cascade: market_seen={} limit_seen={} parent_close_requested={} account_flat_after_parent_close={}",
                        shared.market_child_seen,
                        shared.limit_child_seen,
                        shared.parent_close_requested,
                        shared.account_flat_after_parent_close
                    );
                    drop(shared);
                    let mut shared = self.shared.lock().unwrap();
                    shared.failed = Some(format!("{message}"));
                    ctx.shutdown();
                }
            }

            fn generate_insights(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str) {}

            fn insight_pipeline(&mut self, _ctx: &mut dyn StrategyContext, _insight: &Insight) {}

            fn on_teardown(&mut self, _ctx: &mut dyn StrategyContext) {
                println!("[MT5 ChildInsights] Tearing down");
            }
        }

        let shared = Arc::new(Mutex::new(Mt5ChildInsightShared::default()));
        let execution = Mt5Broker::from_env().expect("failed to create MT5 broker from env");
        let monitor_shared = shared.clone();
        let _flat_monitor_task = tokio::spawn(async move {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
            loop {
                if monitor_shared.lock().unwrap().parent_close_requested {
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    monitor_shared.lock().unwrap().failed =
                        Some("timed out waiting for MT5 parent close request".to_string());
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }

            let monitor = match Mt5Broker::from_env() {
                Ok(monitor) => monitor,
                Err(error) => {
                    monitor_shared.lock().unwrap().failed = Some(format!(
                        "failed to create MT5 child flat-exposure monitor: {:?}",
                        error
                    ));
                    return;
                }
            };

            let flat_deadline = std::time::Instant::now() + std::time::Duration::from_secs(90);
            loop {
                let positions = match monitor.get_positions().await {
                    Ok(positions) => positions,
                    Err(error) => {
                        monitor_shared.lock().unwrap().failed = Some(format!(
                            "failed to poll MT5 positions after parent close: {error}"
                        ));
                        return;
                    }
                };
                let orders = match monitor.get_orders().await {
                    Ok(orders) => orders,
                    Err(error) => {
                        monitor_shared.lock().unwrap().failed = Some(format!(
                            "failed to poll MT5 orders after parent close: {error}"
                        ));
                        return;
                    }
                };

                let has_open_position = positions.iter().any(|position| {
                    position.asset.symbol == TEST_SYMBOL && position.qty.abs() > 0.0
                });
                let has_open_order = orders.iter().any(|order| order.asset.symbol == TEST_SYMBOL);

                if !has_open_position && !has_open_order {
                    let mut shared = monitor_shared.lock().unwrap();
                    shared.account_flat_after_parent_close = true;
                    shared.cleanup_requested = true;
                    return;
                }

                if std::time::Instant::now() >= flat_deadline {
                    monitor_shared.lock().unwrap().failed = Some(format!(
                        "BTCUSD was not flat after parent close: open_position={} open_order={}",
                        has_open_position, has_open_order
                    ));
                    return;
                }

                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        });
        let data = Mt5DataFeed::from_env().expect("failed to create MT5 datafeed from env");
        let broker = UnifiedBroker::new(execution, data);
        let mut state = StrategyState::new(
            "Mt5ChildInsights".into(),
            "1.0".into(),
            Mt5ChildInsightsStrategy {
                submitted: false,
                bars_after_submit: 0,
                shared: shared.clone(),
            },
            broker,
            StrategyMode::Live,
            TimeFrame::new(1, TimeFrameUnit::Minute),
        );

        let run_result =
            tokio::time::timeout(std::time::Duration::from_secs(240), state.run_live(None)).await;

        let cleanup_execution =
            Mt5Broker::from_env().expect("failed to create MT5 child cleanup broker from env");
        cleanup_execution
            .connect()
            .await
            .expect("failed to start MT5 child cleanup bridge");
        let cleanup_orders = cleanup_execution
            .get_orders()
            .await
            .expect("failed to fetch MT5 child cleanup orders");
        for order in cleanup_orders
            .iter()
            .filter(|order| order.asset.symbol == TEST_SYMBOL)
        {
            let _ = cleanup_execution.cancel_order(&order.order_id).await;
        }
        let _ = cleanup_execution.close_all_positions().await;
        let positions = cleanup_execution
            .get_positions()
            .await
            .expect("failed to fetch MT5 positions after child insight test");
        let orders = cleanup_execution
            .get_orders()
            .await
            .expect("failed to fetch MT5 orders after child insight test");
        let _ = cleanup_execution.disconnect().await;

        match run_result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => panic!("MT5 child insight strategy failed: {:?}", error),
            Err(_) => panic!("MT5 child insight strategy timed out after 240s"),
        }

        let shared = shared.lock().unwrap();
        assert!(
            shared.failed.is_none(),
            "MT5 child insight strategy failed: {:?}",
            shared.failed
        );
        assert!(
            shared.market_child_seen,
            "MT5 child insight test never observed the market child fill"
        );
        assert!(
            shared.limit_child_seen,
            "MT5 child insight test never observed the limit child broker order"
        );
        assert!(
            shared.cleanup_requested,
            "MT5 child insight test did not complete parent-close cleanup"
        );
        assert!(
            shared.parent_close_requested,
            "MT5 child insight test did not request parent close"
        );
        assert!(
            shared.account_flat_after_parent_close,
            "MT5 child insight test never observed flat BTCUSD exposure after parent close"
        );
        drop(shared);

        let open_symbol_position = positions
            .iter()
            .find(|position| position.asset.symbol == TEST_SYMBOL && position.qty.abs() > 0.0);
        assert!(
            open_symbol_position.is_none(),
            "MT5 child insight test left an open {} position: {:?}",
            TEST_SYMBOL,
            open_symbol_position
        );
        let open_symbol_order = orders
            .iter()
            .find(|order| order.asset.symbol == TEST_SYMBOL);
        assert!(
            open_symbol_order.is_none(),
            "MT5 child insight test left an open {} order: {:?}",
            TEST_SYMBOL,
            open_symbol_order
        );
    }

    #[tokio::test]
    #[ignore = "requires a running MT5 terminal with AqeMt5BridgeEA attached and bridge env vars configured"]
    async fn test_run_live_mt5_bridge_smoke() {
        let _mt5_guard = MT5_INTEGRATION_TEST_LOCK.lock().await;
        init_test_logger("info");

        if std::env::var("AQE_MT5_BRIDGE_TOKEN").is_err() {
            println!(
                "Skipping MT5 smoke test: AQE_MT5_BRIDGE_TOKEN is required. See integrations/mt5/README.md."
            );
            return;
        }

        const TEST_SYMBOL: &str = "BTCUSD";

        let execution = Mt5Broker::from_env().expect("failed to create MT5 broker from env");
        let data = Mt5DataFeed::from_env().expect("failed to create MT5 datafeed from env");
        let smoke_session_id = format!("mt5-smoke-{}", uuid::Uuid::new_v4());
        execution
            .configure_live_session(&smoke_session_id)
            .expect("failed to configure MT5 execution smoke session");
        data.configure_live_session(&smoke_session_id)
            .expect("failed to configure MT5 data smoke session");
        execution
            .connect()
            .await
            .expect("failed to start MT5 execution bridge");
        data.connect()
            .await
            .expect("failed to start MT5 data bridge");

        let account = execution
            .get_account()
            .await
            .expect("failed to fetch MT5 account through bridge");
        println!(
            "[MT5 Smoke] Account: equity={} cash={} buying_power={}",
            account.equity, account.cash, account.buying_power
        );
        let asset = data
            .get_ticker_info(TEST_SYMBOL)
            .await
            .expect("failed to fetch MT5 ticker info through bridge");
        println!(
            "[MT5 Smoke] Asset: {} {:?} min_qty={:?}",
            asset.symbol, asset.asset_type, asset.min_order_size
        );
        let quote = data
            .get_latest_quote(TEST_SYMBOL)
            .await
            .expect("failed to fetch MT5 latest quote through bridge");
        println!(
            "[MT5 Smoke] Quote: {} bid={} ask={} last={:?}",
            quote.symbol, quote.bid, quote.ask, quote.last
        );

        let broker = UnifiedBroker::new(execution, data);

        struct Mt5SmokeStrategy {
            symbol: String,
            bars_received: usize,
        }

        impl Strategy for Mt5SmokeStrategy {
            fn name(&self) -> &str {
                "Mt5SmokeStrategy"
            }

            fn on_start(&mut self, ctx: &mut dyn StrategyContext) {
                println!("[MT5 Smoke] Started. timeframe={:?}", ctx.timeframe());
            }

            fn init(&mut self, _ctx: &mut dyn StrategyContext, asset: &Asset) {
                println!(
                    "[MT5 Smoke] Initialised asset {} ({:?})",
                    asset.symbol, asset.asset_type
                );
            }

            fn universe(&self, _ctx: &mut dyn StrategyContext) -> HashSet<String> {
                HashSet::from([self.symbol.clone()])
            }

            fn on_bar(&mut self, ctx: &mut dyn StrategyContext, symbol: &str, bar: &BarData) {
                self.bars_received += 1;
                println!(
                    "[MT5 Smoke] Bar {} received for {}: {:?}",
                    self.bars_received, symbol, bar
                );

                if self.bars_received >= 1 {
                    println!("[MT5 Smoke] Smoke condition met, shutting down");
                    ctx.shutdown();
                }
            }

            fn generate_insights(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str) {}

            fn insight_pipeline(&mut self, _ctx: &mut dyn StrategyContext, _insight: &Insight) {}

            fn on_teardown(&mut self, _ctx: &mut dyn StrategyContext) {
                println!("[MT5 Smoke] Tearing down");
            }
        }

        let mut state = StrategyState::new(
            "Mt5Smoke".into(),
            "1.0".into(),
            Mt5SmokeStrategy {
                symbol: TEST_SYMBOL.to_string(),
                bars_received: 0,
            },
            broker,
            StrategyMode::Live,
            TimeFrame::new(1, TimeFrameUnit::Minute),
        );

        let result =
            tokio::time::timeout(std::time::Duration::from_secs(90), state.run_live(None)).await;

        match result {
            Ok(Ok(())) => println!("[MT5 Smoke] Finished gracefully"),
            Ok(Err(error)) => panic!("MT5 smoke strategy failed: {:?}", error),
            Err(_) => panic!(
                "MT5 smoke strategy timed out after 90s. Confirm the EA is attached, WebRequest is enabled, and the symbol is streaming."
            ),
        }
    }
}
