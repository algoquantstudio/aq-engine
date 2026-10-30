//! Companion to engine_performance: unchanged controls plus the real execution path.
//! cargo run -p aq-engine --release --features runtime --example execution_performance -- --output /tmp/execution-performance.json
//! Add --smoke for one 32-order correctness sample (not a throughput baseline).
//! No network or live broker. Each sample owns and removes a private artifact directory.

// Reuse the exact control workloads and report calculations without changing the old example.
#[allow(dead_code)]
mod controls {
    include!("engine_performance.rs");

    pub fn report(mut execution: impl FnMut() -> Duration, smoke: bool) -> Value {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut results = Vec::new();
        if !smoke {
            let bars = synthetic_bars(BAR_ROWS);
            let history = synthetic_bars(64);
            results.push(measure(
                "indicator_batch",
                "SMA + EMA + RSI + ATR batch",
                format!("{BAR_ROWS} deterministic OHLCV bars through four indicators"),
                BAR_ROWS,
                "bars/s",
                || run_indicator_sample(&bars),
            ));
            results.push(measure("insight_pipeline", "Three-stage Insight Pipe chain",
                format!("{PIPELINE_INSIGHTS} New insights through market entry, TTL, and reward/risk pipes"),
                PIPELINE_INSIGHTS * PIPELINE_STAGES, "pipe stages/s", || run_pipeline_sample(&history)));
            results.push(measure(
                "paper_broker",
                "PaperBroker submit + fill (direct control)",
                format!("{PAPER_ORDERS} market insights submitted and filled on one synthetic bar"),
                PAPER_ORDERS,
                "orders/s",
                || run_paper_broker_sample(&runtime),
            ));
            results.push(measure(
                "execution_backtest",
                "Journaled StrategyState backtest",
                "5000 market insights; submit deferral, worker, durable journal, reducer, teardown"
                    .into(),
                PAPER_ORDERS,
                "orders/s",
                &mut execution,
            ));
        } else {
            let elapsed = execution();
            results.push(BenchmarkResult {
                id: "execution_backtest_smoke",
                label: "Execution correctness smoke",
                workload: "32 market insights through full backtest".into(),
                operations_per_sample: 32,
                throughput_unit: "orders/s",
                median_ms: duration_ms(elapsed),
                p95_ms: duration_ms(elapsed),
                throughput_per_second: 32.0 / elapsed.as_secs_f64(),
                raw_sample_ms: vec![duration_ms(elapsed)],
            });
        }
        serde_json::to_value(BenchmarkReport {
            suite: "AQ Engine execution performance suite",
            engine_version: env!("CARGO_PKG_VERSION"),
            engine_revision: engine_revision(),
            generated_at_utc: Utc::now().to_rfc3339(),
            build_profile: "Cargo release; full execution includes durable disk I/O",
            warmup_runs: if smoke { 0 } else { WARMUP_RUNS },
            sample_runs: if smoke { 1 } else { SAMPLE_RUNS },
            host: host_info(),
            results,
        })
        .unwrap()
    }
}

use aq_engine::core::broker::DataStreamMode;
use aq_engine::core::broker::traits::{DataFeed, DataProvider};
use aq_engine::core::broker::types::{
    AssetExchange, AssetStatus, AssetType, Bar, BrokerError, OrderSide, Quote,
};
use aq_engine::core::insight::types::StrategyType;
use aq_engine::core::pipeline::insight_submit::InsightSubmitPipe;
use aq_engine::core::pipeline::{InsightPipe, InsightPipeResult, WrappedInsightPipe};
use aq_engine::prelude::*;
use chrono::{DateTime, Utc};
use polars::prelude::*;
use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

fn epoch() -> DateTime<Utc> {
    DateTime::from_timestamp(1_700_000_000, 0).unwrap()
}

struct SyntheticFeed {
    connected: AtomicBool,
}
impl DataFeed for SyntheticFeed {
    async fn connect(&self) -> Result<bool, BrokerError> {
        self.connected.store(true, Ordering::Relaxed);
        Ok(true)
    }
    async fn disconnect(&self) -> Result<bool, BrokerError> {
        self.connected.store(false, Ordering::Relaxed);
        Ok(true)
    }
    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }
}
impl DataProvider for SyntheticFeed {
    async fn get_ticker_info(&self, symbol: &str) -> Result<Asset, BrokerError> {
        Ok(Asset {
            id: symbol.into(),
            symbol: symbol.into(),
            name: symbol.into(),
            tradable: true,
            marginable: true,
            shortable: true,
            fractional: true,
            min_price_increment: Some(0.01),
            asset_type: AssetType::Stock,
            status: AssetStatus::Active,
            exchange: AssetExchange::NASDAQ,
            min_order_size: None,
            quantity_base: None,
            max_order_size: None,
            max_total_size: None,
            price_base: None,
            contract_size: None,
            trading_calendar: None,
            fees: Default::default(),
        })
    }
    async fn get_history(
        &self,
        _: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        _: TimeFrame,
    ) -> Result<DataFrame, BrokerError> {
        let timestamps: Vec<i64> = (-64..=4)
            .map(|i| epoch().timestamp_millis() + i * 60_000)
            .filter(|t| *t >= start.timestamp_millis() && *t <= end.timestamp_millis())
            .collect();
        let n = timestamps.len();
        DataFrame::new(
            n,
            vec![
                Column::new("timestamp".into(), timestamps),
                Column::new("open".into(), vec![100.0; n]),
                Column::new("high".into(), vec![101.0; n]),
                Column::new("low".into(), vec![99.0; n]),
                Column::new("close".into(), vec![100.5; n]),
                Column::new("volume".into(), vec![1_000_000.0; n]),
            ],
        )
        .map_err(|e| BrokerError::DataFeedError(e.to_string()))
    }
    async fn get_latest_quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
        Ok(Quote {
            profit_model: None,
            symbol: symbol.into(),
            bid: 100.5,
            ask: 100.5,
            bid_size: 1_000_000.0,
            ask_size: 1_000_000.0,
            last: Some(100.5),
            last_size: None,
            timestamp: epoch(),
        })
    }
    async fn get_latest_bar(&self, symbol: &str) -> Result<Bar, BrokerError> {
        Ok(Bar {
            symbol: symbol.into(),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
            volume: 1_000_000.0,
            timestamp: epoch(),
        })
    }
    async fn subscribe_to_data_stream(
        &self,
        _: Vec<String>,
        _: TimeFrame,
        _: DataStreamMode,
        _: Arc<dyn Fn(Bar) + Send + Sync>,
    ) -> Result<(), BrokerError> {
        Err(BrokerError::UnsupportedOperation(
            "offline benchmark only".into(),
        ))
    }
    async fn unsubscribe_from_data_stream(&self, _: Vec<String>) -> Result<(), BrokerError> {
        Ok(())
    }
}

// Observes actual reducer-produced Filled state; queue acceptance cannot pass this check.
struct ObserveFill {
    ids: Arc<std::sync::Mutex<HashSet<uuid::Uuid>>>,
}
impl InsightPipe for ObserveFill {
    fn version(&self) -> &str {
        "1"
    }
    fn run(&mut self, _: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        assert_eq!(insight.state, InsightState::Filled);
        assert!(insight.order_id.is_some());
        assert!(insight.filled_price.is_some_and(|p| p > 0.0));
        self.ids.lock().unwrap().insert(insight.insight_id);
        InsightPipeResult::new(true, true, None, self.name().into())
    }
}
struct FixtureStrategy {
    generated: bool,
    orders: usize,
    filled: Arc<std::sync::Mutex<HashSet<uuid::Uuid>>>,
}
impl Strategy for FixtureStrategy {
    fn name(&self) -> &str {
        "Execution benchmark"
    }
    fn on_start(&mut self, ctx: &mut dyn StrategyContext) {
        ctx.add_pipe(
            WrappedInsightPipe::builder(Box::new(InsightSubmitPipe::new()))
                .target_state(InsightState::New)
                .build(),
        );
        ctx.add_pipe(
            WrappedInsightPipe::builder(Box::new(ObserveFill {
                ids: self.filled.clone(),
            }))
            .target_state(InsightState::Filled)
            .build(),
        );
    }
    fn init(&mut self, _: &mut dyn StrategyContext, _: &Asset) {}
    fn universe(&self, _: &mut dyn StrategyContext) -> HashSet<String> {
        ["AQE".into()].into_iter().collect()
    }
    fn on_bar(&mut self, _: &mut dyn StrategyContext, _: &str, _: &BarData) {}
    fn generate_insights(&mut self, ctx: &mut dyn StrategyContext, symbol: &str) {
        if self.generated {
            return;
        }
        for _ in 0..self.orders {
            let mut insight = Insight::new(
                OrderSide::Buy,
                symbol.into(),
                StrategyType::Testing,
                ctx.timeframe().clone(),
                80,
                None,
            );
            insight.set_quantity(Some(1.0));
            ctx.add_insight(insight);
        }
        self.generated = true;
    }
    fn insight_pipeline(&mut self, _: &mut dyn StrategyContext, _: &Insight) {}
    fn on_teardown(&mut self, _: &mut dyn StrategyContext) {}
}

struct SampleDir(PathBuf);
impl Drop for SampleDir {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_dir_all(&self.0) {
            eprintln!("sample cleanup failed: {e}");
        }
    }
}
async fn execution_sample(orders: usize) -> Duration {
    let directory = SampleDir(
        std::env::temp_dir().join(format!("aqe-execution-bench-{}", uuid::Uuid::new_v4())),
    );
    std::fs::create_dir_all(&directory.0).unwrap();
    let filled = Arc::new(std::sync::Mutex::new(HashSet::new()));
    let broker = UnifiedBroker::new(
        PaperBroker::new(AccountType::Paper, 100_000_000.0, 1),
        SyntheticFeed {
            connected: AtomicBool::new(false),
        },
    );
    let timeframe = TimeFrame::new(1, TimeFrameUnit::Minute);
    let mut state = StrategyState::new(
        "Execution benchmark".into(),
        "1".into(),
        FixtureStrategy {
            generated: false,
            orders,
            filled: filled.clone(),
        },
        broker,
        StrategyMode::Backtest,
        timeframe.clone(),
    );
    state.set_artifact_root(&directory.0);
    state.set_warm_up_bars(1);
    let started = Instant::now();
    tokio::time::timeout(
        Duration::from_secs(300),
        state.run_backtest(epoch(), epoch() + chrono::Duration::minutes(4), timeframe),
    )
    .await
    .expect("execution benchmark exceeded five minutes")
    .expect("execution backtest failed");
    let elapsed = started.elapsed();
    assert!(state.strategy().generated);
    assert_eq!(
        filled.lock().unwrap().len(),
        orders,
        "every requested insight must reach broker-confirmed Filled"
    );
    drop(state);
    drop(directory); // Cleanup and validation are outside the timed interval.
    elapsed
}

fn main() {
    assert!(!cfg!(debug_assertions), "use --release for this benchmark");
    let args: Vec<String> = std::env::args().skip(1).collect();
    let smoke = args.iter().any(|arg| arg == "--smoke");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let mut report = controls::report(
        || runtime.block_on(execution_sample(if smoke { 32 } else { 5000 })),
        smoke,
    );
    report["execution_scope"] = serde_json::json!(
        "Includes run startup, synthetic history, command admission, durable journal/checkpoints, reducer, pipeline resumes, results and teardown. Not comparable to direct orders/s as equivalent work. Excludes fixture construction and artifact deletion; no cloud sync."
    );
    let json = serde_json::to_string_pretty(&report).unwrap();
    if let Some(index) = args.iter().position(|arg| arg == "--output") {
        std::fs::write(
            args.get(index + 1).expect("--output requires a path"),
            format!("{json}\n"),
        )
        .unwrap();
    }
    println!("{json}");
}
