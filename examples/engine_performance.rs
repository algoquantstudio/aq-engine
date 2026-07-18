use aq_engine::core::alpha::WrappedAlphaModel;
use aq_engine::core::broker::paper_broker::PaperBroker;
use aq_engine::core::broker::traits::OrderManagementProvider;
use aq_engine::core::broker::types::{
    Account, AccountType, Asset, Bar, BrokerError, OrderSide, Quote, TradeUpdateEvent,
};
use aq_engine::core::indicators::Indicator;
use aq_engine::core::indicators::atr::AverageTrueRange;
use aq_engine::core::indicators::ema::ExponentialMovingAverage;
use aq_engine::core::indicators::rsi::RelativeStrengthIndex;
use aq_engine::core::indicators::sma::SimpleMovingAverage;
use aq_engine::core::insight::types::{InsightState, StrategyType};
use aq_engine::core::insight::{Insight, InsightCollection};
use aq_engine::core::pipeline::WrappedInsightPipe;
use aq_engine::core::pipeline::insight_ttl_config::InsightTtlConfigPipe;
use aq_engine::core::pipeline::market_order_entry::MarketOrderEntryPipe;
use aq_engine::core::pipeline::minimum_risk_to_reward::MinimumRiskToRewardPipe;
use aq_engine::core::strategy::{StrategyContext, StrategyMode, TeardownCleanupReport};
use aq_engine::core::universe::WrappedUniverseModel;
use aq_engine::core::utils::timeframe::{TimeFrame, TimeFrameUnit};
use aq_engine::core::utils::tools::TradingTools;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use polars::prelude::{Column, DataFrame};
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::hint::black_box;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

const WARMUP_RUNS: usize = 3;
const SAMPLE_RUNS: usize = 15;
const BAR_ROWS: usize = 250_000;
const PIPELINE_INSIGHTS: usize = 20_000;
const PIPELINE_STAGES: usize = 3;
const PAPER_ORDERS: usize = 5_000;

#[derive(Serialize)]
struct BenchmarkReport {
    suite: &'static str,
    engine_version: &'static str,
    engine_revision: String,
    generated_at_utc: String,
    build_profile: &'static str,
    warmup_runs: usize,
    sample_runs: usize,
    host: HostInfo,
    results: Vec<BenchmarkResult>,
}

#[derive(Serialize)]
struct HostInfo {
    model: String,
    cpu: String,
    logical_cpus: String,
    memory_bytes: String,
    os: String,
    rustc: String,
}

#[derive(Serialize)]
struct BenchmarkResult {
    id: &'static str,
    label: &'static str,
    workload: String,
    operations_per_sample: usize,
    throughput_unit: &'static str,
    median_ms: f64,
    p95_ms: f64,
    throughput_per_second: f64,
}

struct MockTools;

impl TradingTools for MockTools {
    fn dynamic_round(&self, value: f64, _symbol: &str) -> f64 {
        value
    }

    fn quantity_round(&self, value: f64, _symbol: &str) -> f64 {
        value
    }

    fn calculate_time_to_live(&self, _price: f64, _entry: f64, _atr: f64, additional: i32) -> i32 {
        additional
    }

    fn get_unrealized_pnl(&self, _symbol: &str) -> Result<f64, BrokerError> {
        Ok(0.0)
    }

    fn get_all_unrealized_pnl(&self) -> Result<f64, BrokerError> {
        Ok(0.0)
    }

    fn get_filled_insights(&self) -> Vec<Insight> {
        Vec::new()
    }
}

struct BenchmarkContext {
    universe: HashMap<String, Asset>,
    history: HashMap<String, DataFrame>,
    insights: InsightCollection,
    variables: DashMap<String, Value>,
    timeframe: TimeFrame,
    minimum_reward_risk: f64,
    current_time: DateTime<Utc>,
}

impl BenchmarkContext {
    fn new(history: DataFrame) -> Self {
        Self {
            universe: HashMap::new(),
            history: HashMap::from([("AQE".to_string(), history)]),
            insights: InsightCollection::new(),
            variables: DashMap::new(),
            timeframe: TimeFrame::new(1, TimeFrameUnit::Minute),
            minimum_reward_risk: 2.0,
            current_time: fixed_time(),
        }
    }
}

impl StrategyContext for BenchmarkContext {
    fn universe(&self) -> &HashMap<String, Asset> {
        &self.universe
    }

    fn history(&self) -> &HashMap<String, DataFrame> {
        &self.history
    }

    fn insights(&self) -> &InsightCollection {
        &self.insights
    }

    fn mode(&self) -> StrategyMode {
        StrategyMode::Backtest
    }

    fn add_insight(&mut self, insight: Insight) {
        self.insights.add_insight(insight);
    }

    fn submit_insight(&mut self, _insight: &mut Insight) {}

    fn register_indicator(&mut self, _indicator: Box<dyn Indicator>) {}

    fn add_alpha(&mut self, _alpha: WrappedAlphaModel) {}

    fn add_pipe(&mut self, _pipe: WrappedInsightPipe) {}

    fn add_universe_model(&mut self, _model: WrappedUniverseModel) {}

    fn set_execution_risk(&mut self, _risk: f64) {}

    fn set_min_reward_risk_ratio(&mut self, ratio: f64) {
        self.minimum_reward_risk = ratio;
    }

    fn set_base_confidence(&mut self, _confidence: f64) {}

    fn execution_risk(&self) -> f64 {
        0.02
    }

    fn min_reward_risk_ratio(&self) -> f64 {
        self.minimum_reward_risk
    }

    fn base_confidence(&self) -> f64 {
        0.8
    }

    fn variables(&self) -> &DashMap<String, Value> {
        &self.variables
    }

    fn tools(&self) -> Box<dyn TradingTools + '_> {
        Box::new(MockTools)
    }

    fn max_history_rows(&self) -> usize {
        2_000
    }

    fn set_max_history_rows(&mut self, _rows: usize) {}

    fn warm_up_bars(&self) -> i32 {
        0
    }

    fn set_warm_up_bars(&mut self, _bars: i32) {}

    fn timeframe(&self) -> &TimeFrame {
        &self.timeframe
    }

    fn account(&self) -> Result<Account, BrokerError> {
        Ok(Account {
            account_id: "benchmark".to_string(),
            account_type: AccountType::Paper,
            equity: 1_000_000.0,
            cash: 1_000_000.0,
            currency: "USD".to_string(),
            buying_power: 1_000_000.0,
            accrued_commission: 0.0,
            shorting_enabled: true,
            leverage: 1,
        })
    }

    fn current_time(&self) -> DateTime<Utc> {
        self.current_time
    }

    fn bind_insight_context(&self, _insight: &mut Insight) {}

    fn latest_quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
        Err(BrokerError::DataFeedError(format!(
            "No quote available for {symbol}"
        )))
    }

    fn cleanup_active_insights_for_teardown(&mut self) -> TeardownCleanupReport {
        TeardownCleanupReport::default()
    }

    fn cancel_order(&self, _order_id: &str) -> Result<bool, BrokerError> {
        Ok(false)
    }

    fn update_order(&self, _order_id: &str, _price: f64, _qty: f64) -> Result<bool, BrokerError> {
        Ok(false)
    }

    fn update_stop_loss_order(
        &self,
        _order_id: &str,
        _price: f64,
        _qty: f64,
    ) -> Result<bool, BrokerError> {
        Ok(false)
    }

    fn close_position(
        &self,
        _order_id: &str,
        _qty: f64,
        _price: Option<f64>,
    ) -> Result<bool, BrokerError> {
        Ok(false)
    }

    fn shutdown(&mut self) {}
}

fn main() {
    let output_path = output_path_from_args();
    let base_frame = synthetic_bars(BAR_ROWS);
    let pipeline_history = synthetic_bars(64);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("benchmark runtime should start");

    let results = vec![
        measure(
            "indicator_batch",
            "SMA + EMA + RSI + ATR batch",
            format!("{BAR_ROWS} deterministic OHLCV bars through four indicators"),
            BAR_ROWS,
            "bars/s",
            || run_indicator_sample(&base_frame),
        ),
        measure(
            "insight_pipeline",
            "Three-stage Insight Pipe chain",
            format!(
                "{PIPELINE_INSIGHTS} New insights through market entry, TTL, and reward/risk pipes"
            ),
            PIPELINE_INSIGHTS * PIPELINE_STAGES,
            "pipe stages/s",
            || run_pipeline_sample(&pipeline_history),
        ),
        measure(
            "paper_broker",
            "PaperBroker submit + fill",
            format!("{PAPER_ORDERS} market insights submitted and filled on one synthetic bar"),
            PAPER_ORDERS,
            "orders/s",
            || run_paper_broker_sample(&runtime),
        ),
    ];

    let report = BenchmarkReport {
        suite: "AQ Engine performance suite",
        engine_version: env!("CARGO_PKG_VERSION"),
        engine_revision: engine_revision(),
        generated_at_utc: Utc::now().to_rfc3339(),
        build_profile: "Cargo release; AQ Engine opt-level=3; workspace LTO; codegen-units=1",
        warmup_runs: WARMUP_RUNS,
        sample_runs: SAMPLE_RUNS,
        host: host_info(),
        results,
    };

    let json = serde_json::to_string_pretty(&report).expect("benchmark report should serialize");
    if let Some(path) = output_path {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("benchmark output directory should be created");
        }
        fs::write(&path, format!("{json}\n")).expect("benchmark report should be written");
        eprintln!("wrote {}", path.display());
    }
    println!("{json}");
}

fn measure<F>(
    id: &'static str,
    label: &'static str,
    workload: String,
    operations_per_sample: usize,
    throughput_unit: &'static str,
    mut sample: F,
) -> BenchmarkResult
where
    F: FnMut() -> Duration,
{
    for _ in 0..WARMUP_RUNS {
        black_box(sample());
    }

    let mut durations = (0..SAMPLE_RUNS).map(|_| sample()).collect::<Vec<_>>();
    durations.sort_unstable();

    let median = percentile(&durations, 0.50);
    let p95 = percentile(&durations, 0.95);
    BenchmarkResult {
        id,
        label,
        workload,
        operations_per_sample,
        throughput_unit,
        median_ms: duration_ms(median),
        p95_ms: duration_ms(p95),
        throughput_per_second: operations_per_sample as f64 / median.as_secs_f64(),
    }
}

fn run_indicator_sample(base_frame: &DataFrame) -> Duration {
    let mut frame = base_frame.clone();
    let mut indicators: Vec<Box<dyn Indicator>> = vec![
        Box::new(SimpleMovingAverage::new(20, "close")),
        Box::new(ExponentialMovingAverage::new(20, "close")),
        Box::new(RelativeStrengthIndex::new(14, "close")),
        Box::new(AverageTrueRange::new(14)),
    ];

    let started = Instant::now();
    for indicator in &mut indicators {
        indicator
            .run(&mut frame)
            .expect("indicator benchmark should succeed");
    }
    let elapsed = started.elapsed();

    black_box(frame.width());
    elapsed
}

fn run_pipeline_sample(history: &DataFrame) -> Duration {
    let mut context = BenchmarkContext::new(history.clone());
    let mut pipes = vec![
        WrappedInsightPipe::builder(Box::new(MarketOrderEntryPipe::new()))
            .target_state(InsightState::New)
            .build(),
        WrappedInsightPipe::builder(Box::new(InsightTtlConfigPipe::new(5, 20)))
            .target_state(InsightState::New)
            .build(),
        WrappedInsightPipe::builder(Box::new(MinimumRiskToRewardPipe::new(Some(2.0))))
            .target_state(InsightState::New)
            .build(),
    ];
    let mut insights = (0..PIPELINE_INSIGHTS)
        .map(|_| {
            let mut insight = benchmark_insight();
            insight
                .set_quantity(Some(1.0))
                .set_stop_loss(Some(98.0))
                .set_take_profit_levels(Some(vec![110.0]));
            insight
        })
        .collect::<Vec<_>>();

    let started = Instant::now();
    for insight in &mut insights {
        for pipe in &mut pipes {
            if pipe.should_run(insight) {
                let result = pipe.run(&mut context, insight);
                assert!(result.success && result.passed);
                black_box(result);
            }
        }
    }
    let elapsed = started.elapsed();

    black_box(insights.last().and_then(|insight| insight.limit_price));
    elapsed
}

fn run_paper_broker_sample(runtime: &tokio::runtime::Runtime) -> Duration {
    let broker = PaperBroker::new(AccountType::Paper, 1_000_000_000.0, 1);
    let insights = (0..PAPER_ORDERS)
        .map(|_| {
            let mut insight = benchmark_insight();
            insight.set_quantity(Some(1.0));
            insight
        })
        .collect::<Vec<_>>();
    let timestamp = fixed_time();
    let bars = HashMap::from([(
        "AQE".to_string(),
        Bar {
            symbol: "AQE".to_string(),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
            volume: 1_000_000.0,
            timestamp,
        },
    )]);

    let started = Instant::now();
    for insight in insights {
        let order = runtime
            .block_on(broker.submit_order(insight))
            .expect("PaperBroker should accept benchmark insight");
        black_box(order.order_id);
    }
    broker.process_step(&bars, timestamp);
    let elapsed = started.elapsed();

    let filled = runtime
        .block_on(broker.get_orders())
        .expect("PaperBroker orders should be readable")
        .into_iter()
        .filter(|order| order.status == TradeUpdateEvent::Filled)
        .count();
    assert_eq!(filled, PAPER_ORDERS);
    black_box(filled);
    elapsed
}

fn benchmark_insight() -> Insight {
    Insight::new(
        OrderSide::Buy,
        "AQE".to_string(),
        StrategyType::Testing,
        TimeFrame::new(1, TimeFrameUnit::Minute),
        80,
        None,
    )
}

fn synthetic_bars(rows: usize) -> DataFrame {
    let mut open = Vec::with_capacity(rows);
    let mut high = Vec::with_capacity(rows);
    let mut low = Vec::with_capacity(rows);
    let mut close = Vec::with_capacity(rows);
    let mut volume = Vec::with_capacity(rows);
    let mut timestamp = Vec::with_capacity(rows);

    for index in 0..rows {
        let trend = index as f64 * 0.000_2;
        let cycle = (index as f64 * 0.017).sin() * 1.4;
        let price = 100.0 + trend + cycle;
        open.push(price - 0.08);
        high.push(price + 0.42);
        low.push(price - 0.37);
        close.push(price);
        volume.push(1_000.0 + (index % 400) as f64);
        timestamp.push(1_700_000_000_000_i64 + index as i64 * 60_000);
    }

    DataFrame::new(vec![
        Column::new("open".into(), open),
        Column::new("high".into(), high),
        Column::new("low".into(), low),
        Column::new("close".into(), close),
        Column::new("volume".into(), volume),
        Column::new("timestamp".into(), timestamp),
    ])
    .expect("synthetic benchmark frame should be valid")
}

fn percentile(sorted: &[Duration], percentile: f64) -> Duration {
    let index = ((sorted.len() - 1) as f64 * percentile).ceil() as usize;
    sorted[index]
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn fixed_time() -> DateTime<Utc> {
    DateTime::from_timestamp(1_700_000_000, 0).expect("fixed benchmark time should be valid")
}

fn output_path_from_args() -> Option<PathBuf> {
    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        if argument == "--json" {
            return args.next().map(PathBuf::from);
        }
    }
    None
}

fn host_info() -> HostInfo {
    HostInfo {
        model: command_output("sysctl", &["-n", "hw.model"]),
        cpu: command_output("sysctl", &["-n", "machdep.cpu.brand_string"]),
        logical_cpus: command_output("sysctl", &["-n", "hw.logicalcpu"]),
        memory_bytes: command_output("sysctl", &["-n", "hw.memsize"]),
        os: command_output("sw_vers", &["-productVersion"]),
        rustc: command_output("rustc", &["--version"]),
    }
}

fn command_output(command: &str, args: &[&str]) -> String {
    Command::new(command)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|output| !output.is_empty())
        .unwrap_or_else(|| "unavailable".to_string())
}

fn engine_revision() -> String {
    Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|output| !output.is_empty())
        .unwrap_or_else(|| "unavailable".to_string())
}
