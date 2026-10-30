//! Offline lifecycle companion. Does not change engine_performance or execution_performance.
//! Build/run only with --release --features runtime; see docs/offline-lifecycle-performance.md.

use aq_engine::core::broker::types::{
    AssetExchange, AssetStatus, AssetType, Bar, OrderSide, Quote, TradeUpdateEvent,
};
use aq_engine::core::broker::{
    DataStreamMode,
    traits::{Broker, DataFeed, DataProvider, OrderManagementProvider},
};
use aq_engine::core::indicators::I::{
    AverageTrueRange, ExponentialMovingAverage, RelativeStrengthIndex, SimpleMovingAverage,
};
use aq_engine::core::insight::types::StrategyType;
use aq_engine::core::pipeline::{WrappedInsightPipe, insight_submit::InsightSubmitPipe};
use aq_engine::core::strategy::StrategyStatus;
use aq_engine::prelude::*;
use chrono::{DateTime, Utc};
use polars::prelude::*;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

const SYMBOL: &str = "AQE_OFFLINE";
const STARTING_CASH: f64 = 100_000_000.0;
const ENTRY_PRICE: f64 = 100.0;
const EXIT_PRICE: f64 = 101.0;
const HISTORY_LIMIT: usize = 512;
const INDICATOR_WARMUP_ROWS: usize = 64;
const WORKER_THREADS: usize = 2;
const MAX_LIVE_BARS: usize = 100_000;
const SAMPLE_TIMEOUT: Duration = Duration::from_secs(300);

type Callback = Arc<dyn Fn(Bar) + Send + Sync>;

/// The only data source is an owned, deterministic frame and a one-bar-at-a-time callback.
/// It never connects to a socket or reads a strategy's data directory.
struct OfflineFeed {
    connected: AtomicBool,
    callback: Mutex<Option<Callback>>,
    anchor: DateTime<Utc>,
    bars: usize,
}
impl OfflineFeed {
    fn bar(&self, index: usize) -> Bar {
        Bar {
            symbol: SYMBOL.into(),
            open: ENTRY_PRICE,
            high: ENTRY_PRICE + 1.0,
            low: ENTRY_PRICE - 1.0,
            close: ENTRY_PRICE,
            volume: 1_000_000.0,
            timestamp: self.anchor + chrono::Duration::minutes(index as i64),
        }
    }
}
impl DataFeed for OfflineFeed {
    async fn connect(&self) -> Result<bool, BrokerError> {
        self.connected.store(true, Ordering::Release);
        Ok(true)
    }
    async fn disconnect(&self) -> Result<bool, BrokerError> {
        self.connected.store(false, Ordering::Release);
        Ok(true)
    }
    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }
}
impl DataProvider for OfflineFeed {
    async fn get_ticker_info(&self, symbol: &str) -> Result<Asset, BrokerError> {
        assert_eq!(symbol, SYMBOL);
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
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        _: TimeFrame,
    ) -> Result<DataFrame, BrokerError> {
        assert_eq!(symbol, SYMBOL);
        let timestamps: Vec<i64> = (-128..self.bars as i64)
            .map(|i| self.anchor.timestamp_millis() + i * 60_000)
            .filter(|t| *t >= start.timestamp_millis() && *t <= end.timestamp_millis())
            .collect();
        let n = timestamps.len();
        DataFrame::new(
            n,
            vec![
                Column::new("timestamp".into(), timestamps),
                Column::new("open".into(), vec![ENTRY_PRICE; n]),
                Column::new("high".into(), vec![ENTRY_PRICE + 1.0; n]),
                Column::new("low".into(), vec![ENTRY_PRICE - 1.0; n]),
                Column::new("close".into(), vec![ENTRY_PRICE; n]),
                Column::new("volume".into(), vec![1_000_000.0; n]),
            ],
        )
        .map_err(|e| BrokerError::DataFeedError(e.to_string()))
    }
    async fn get_latest_quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
        assert_eq!(symbol, SYMBOL);
        Ok(Quote {
            profit_model: None,
            symbol: symbol.into(),
            bid: ENTRY_PRICE,
            ask: ENTRY_PRICE,
            bid_size: 1_000_000.0,
            ask_size: 1_000_000.0,
            last: Some(ENTRY_PRICE),
            last_size: None,
            timestamp: Utc::now(),
        })
    }
    async fn get_latest_bar(&self, symbol: &str) -> Result<Bar, BrokerError> {
        assert_eq!(symbol, SYMBOL);
        Ok(self.bar(0))
    }
    async fn subscribe_to_data_stream(
        &self,
        symbols: Vec<String>,
        _: TimeFrame,
        mode: DataStreamMode,
        callback: Callback,
    ) -> Result<(), BrokerError> {
        assert_eq!(symbols, vec![SYMBOL.to_string()]);
        if mode == DataStreamMode::CompletedBar {
            assert!(
                self.callback.lock().unwrap().replace(callback).is_none(),
                "one completed-bar stream per sample"
            );
        }
        Ok(())
    }
    async fn unsubscribe_from_data_stream(&self, _: Vec<String>) -> Result<(), BrokerError> {
        self.callback.lock().unwrap().take();
        Ok(())
    }
}

#[derive(Clone)]
struct Observation {
    index: usize,
    quantity: f64,
    sign: f64,
    created: Instant,
    close_requested: Option<Instant>,
    order_id: Option<String>,
    broker_fills: usize,
    broker_closes: usize,
    canonical_fill: bool,
    canonical_close: bool,
    entry_latency_ms: Option<f64>,
    close_latency_ms: Option<f64>,
}
#[derive(Default)]
struct Evidence {
    rows: Mutex<HashMap<Uuid, Observation>>,
    order_ids: Mutex<HashSet<String>>,
    bar_count: AtomicUsize,
    teardown_count: AtomicUsize,
    producer_finished: AtomicBool,
    bar_ack: tokio::sync::Notify,
}
fn near(actual: f64, expected: f64, context: &str) {
    assert!(
        actual.is_finite() && (actual - expected).abs() < 0.000_001,
        "{context}: expected {expected}, observed {actual}"
    );
}
fn quantity(index: usize) -> f64 {
    1.0 + (index % 3) as f64 * 0.25
}
fn sign(index: usize) -> f64 {
    if index % 2 == 0 { 1.0 } else { -1.0 }
}

/// This stage observes reducer-confirmed entry state, then uses the public receipt-based
/// close API. A broker ACK cannot satisfy the Closed assertions below.
struct ObserveAndClose(Arc<Evidence>);
impl InsightPipe for ObserveAndClose {
    fn version(&self) -> &str {
        "1"
    }
    fn run(&mut self, ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        assert_eq!(insight.state, InsightState::Filled);
        {
            let mut rows = self.0.rows.lock().unwrap();
            let row = rows
                .get_mut(&insight.insight_id)
                .expect("only owned entries may fill");
            near(
                insight
                    .entry_filled_quantity
                    .expect("canonical fill quantity"),
                row.quantity,
                "canonical entry quantity",
            );
            near(
                insight.filled_price.expect("canonical entry price"),
                ENTRY_PRICE,
                "canonical entry price",
            );
            let order_id = insight
                .order_id
                .as_ref()
                .expect("canonical broker order ID");
            assert_eq!(row.order_id.as_ref(), Some(order_id));
            row.canonical_fill = true;
            row.close_requested.get_or_insert_with(Instant::now);
        }
        InsightPipeResult::from_mutation(
            insight.close_partial(ctx, insight.remaining_quantity(), Some(EXIT_PRICE)),
            self.name(),
        )
    }
}

struct LifecycleStrategy {
    orders: usize,
    generated: bool,
    bar_heavy: bool,
    live: bool,
    seeded_history_rows: usize,
    indicator_checks: usize,
    indicator_checksum: f64,
    evidence: Arc<Evidence>,
}
impl LifecycleStrategy {
    fn observe_closed(&mut self, ctx: &mut dyn StrategyContext) {
        let mut all_closed = self.orders > 0;
        let mut rows = self.evidence.rows.lock().unwrap();
        for (id, row) in rows.iter_mut() {
            if !row.canonical_close {
                if let Some(insight) = ctx.insights().get(id) {
                    assert!(
                        !matches!(
                            insight.state,
                            InsightState::Rejected | InsightState::Cancelled
                        ),
                        "entry must not reject/cancel"
                    );
                    if insight.state == InsightState::Closed {
                        assert!(
                            row.canonical_fill,
                            "closed before canonical Filled observation"
                        );
                        near(
                            insight.close_price.expect("canonical close price"),
                            EXIT_PRICE,
                            "canonical exit price",
                        );
                        near(
                            insight.broker_realized_pnl.expect("broker realized P&L"),
                            row.sign * row.quantity,
                            "canonical realized P&L",
                        );
                        row.canonical_close = true;
                    }
                }
            }
            all_closed &= row.canonical_close;
        }
        all_closed &= rows.len() == self.orders;
        drop(rows);
        if self.live && all_closed {
            ctx.shutdown();
        }
    }
}
impl Strategy for LifecycleStrategy {
    fn name(&self) -> &str {
        "Offline lifecycle benchmark"
    }
    fn on_start(&mut self, ctx: &mut dyn StrategyContext) {
        ctx.set_max_history_rows(HISTORY_LIMIT);
        ctx.add_pipe(
            WrappedInsightPipe::builder(Box::new(InsightSubmitPipe::new()))
                .target_state(InsightState::New)
                .build(),
        );
        ctx.add_pipe(
            WrappedInsightPipe::builder(Box::new(ObserveAndClose(self.evidence.clone())))
                .target_state(InsightState::Filled)
                .build(),
        );
        if self.bar_heavy {
            ctx.register_indicator(Box::new(SimpleMovingAverage::new(20, "close")));
            ctx.register_indicator(Box::new(ExponentialMovingAverage::new(20, "close")));
            ctx.register_indicator(Box::new(RelativeStrengthIndex::new(14, "close")));
            ctx.register_indicator(Box::new(AverageTrueRange::new(14)));
        }
    }
    fn init(&mut self, ctx: &mut dyn StrategyContext, asset: &Asset) {
        if self.bar_heavy {
            // A warmup threshold alone consumes replay bars before invoking on_bar.
            // Seed the owned pre-start history explicitly so every requested replay
            // bar has initialized indicators and reaches the strategy callback.
            self.seeded_history_rows = ctx
                .preseed_warmup_history(&asset.symbol, INDICATOR_WARMUP_ROWS as i32)
                .expect("offline indicator history preseed");
            assert_eq!(self.seeded_history_rows, INDICATOR_WARMUP_ROWS);
        }
    }
    fn universe(&self, _: &mut dyn StrategyContext) -> HashSet<String> {
        [SYMBOL.into()].into_iter().collect()
    }
    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, symbol: &str, _: &BarData) {
        if self.bar_heavy {
            let history = ctx.history().get(symbol).expect("strategy history");
            assert!(history.height() <= HISTORY_LIMIT);
            for (name, expected) in [
                ("SMA_20_close", ENTRY_PRICE),
                ("EMA_20", ENTRY_PRICE),
                ("RSI_14", 50.0),
                ("ATRr_14", 2.0),
            ] {
                let values = history
                    .column(name)
                    .expect("registered indicator output")
                    .f64()
                    .unwrap();
                let value = values.get(values.len() - 1).expect("warm indicator output");
                near(value, expected, name);
                self.indicator_checksum += value;
            }
            self.indicator_checks += 1;
        }
        self.observe_closed(ctx);
        self.evidence.bar_count.fetch_add(1, Ordering::Release);
        self.evidence.bar_ack.notify_one();
    }
    fn generate_insights(&mut self, ctx: &mut dyn StrategyContext, symbol: &str) {
        if self.generated {
            return;
        }
        for index in 0..self.orders {
            let mut insight = Insight::new(
                if sign(index) > 0.0 {
                    OrderSide::Buy
                } else {
                    OrderSide::Sell
                },
                symbol.into(),
                StrategyType::Testing,
                ctx.timeframe().clone(),
                80,
                None,
            );
            insight.set_quantity(Some(quantity(index)));
            self.evidence.rows.lock().unwrap().insert(
                insight.insight_id,
                Observation {
                    index,
                    quantity: quantity(index),
                    sign: sign(index),
                    created: Instant::now(),
                    close_requested: None,
                    order_id: None,
                    broker_fills: 0,
                    broker_closes: 0,
                    canonical_fill: false,
                    canonical_close: false,
                    entry_latency_ms: None,
                    close_latency_ms: None,
                },
            );
            ctx.add_insight(insight);
        }
        self.generated = true;
    }
    fn insight_pipeline(&mut self, _: &mut dyn StrategyContext, _: &Insight) {}
    fn on_teardown(&mut self, ctx: &mut dyn StrategyContext) {
        self.evidence.teardown_count.fetch_add(1, Ordering::Relaxed);
        self.observe_closed(ctx);
        let cleanup = ctx.cleanup_active_insights_for_teardown();
        assert!(
            !cleanup.has_failures(),
            "normal owned teardown: {:?}",
            cleanup.failures
        );
    }
}

/// Failed samples deliberately retain their private files for diagnosis.
struct SampleDir {
    path: PathBuf,
    successful: bool,
}
impl SampleDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("aqe-offline-lifecycle-{}", Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self {
            path,
            successful: false,
        }
    }
}
impl Drop for SampleDir {
    fn drop(&mut self) {
        if self.successful {
            std::fs::remove_dir_all(&self.path).expect("remove only owned sample directory");
        } else {
            eprintln!(
                "Unsuccessful offline fixture retained at {}",
                self.path.display()
            );
        }
    }
}
fn journal_paths(root: &Path, output: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            journal_paths(&path, output);
        } else if path.file_name().is_some_and(|name| name == "commands.db") {
            output.push(path);
        }
    }
}
async fn journal_proof(root: &Path, expected_orders: usize) -> Value {
    let mut paths = Vec::new();
    journal_paths(root, &mut paths);
    assert_eq!(paths.len(), 1, "exactly one private execution journal");
    let database = turso::Builder::new_local(paths[0].to_string_lossy().as_ref())
        .build()
        .await
        .unwrap();
    let connection = database.connect().unwrap();
    let mut rows = connection
        .query(
            "SELECT phase FROM broker_execution_lifecycle WHERE singleton=1",
            (),
        )
        .await
        .unwrap();
    let phase: String = rows.next().await.unwrap().unwrap().get(0).unwrap();
    assert_eq!(
        phase, "stopped",
        "runtime must durably prove normal settlement"
    );
    drop(rows);
    let mut rows = connection
        .query(
            "SELECT COUNT(*),SUM(CASE WHEN state='applied' THEN 0 ELSE 1 END) FROM broker_commands",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    let commands: i64 = row.get(0).unwrap();
    let pending: Option<i64> = row.get(1).unwrap();
    assert_eq!(
        commands,
        (expected_orders * 2) as i64,
        "one entry and one explicit close command per insight"
    );
    assert_eq!(pending.unwrap_or(0), 0, "no pending broker work");
    drop(rows);
    let mut rows = connection
        .query(
            "SELECT COUNT(*) FROM broker_execution_graph WHERE recovery_seed=1",
            (),
        )
        .await
        .unwrap();
    let recovery: i64 = rows.next().await.unwrap().unwrap().get(0).unwrap();
    assert_eq!(
        recovery, 0,
        "no recoverable exposure or continuation remains"
    );
    json!({"phase": phase, "commands": commands, "pending_commands": 0, "recovery_seeds": recovery})
}

struct Sample {
    elapsed: Duration,
    fingerprint: Value,
    entry_ms: Vec<f64>,
    close_ms: Vec<f64>,
    bars: usize,
}
async fn sample(case: &str, orders: usize, bar_rows: usize) -> Sample {
    let live = case == "live_close";
    let bar_heavy = case == "backtest_bars";
    let orders = if bar_heavy { 0 } else { orders };
    let bars = if bar_heavy { bar_rows } else { 8 };
    let mut directory = SampleDir::new();
    let evidence = Arc::new(Evidence::default());
    let paper = PaperBroker::new(AccountType::Paper, STARTING_CASH, 1);
    let probe = paper.clone();
    let observed = evidence.clone();
    paper
        .subscribe_to_trade_stream(Arc::new(move |(order, event)| {
            if !matches!(event, TradeUpdateEvent::Filled | TradeUpdateEvent::Closed) {
                assert!(
                    !matches!(
                        event,
                        TradeUpdateEvent::Rejected
                            | TradeUpdateEvent::Cancelled
                            | TradeUpdateEvent::Expired
                    ),
                    "unexpected broker terminal event: {event:?}"
                );
                return;
            }
            let id = Uuid::parse_str(
                order
                    .insight_id
                    .as_ref()
                    .expect("owned broker event insight ID"),
            )
            .unwrap();
            let mut rows = observed.rows.lock().unwrap();
            let row = rows
                .get_mut(&id)
                .expect("broker may only emit owned orders");
            near(order.filled_qty, row.quantity, "broker fill quantity");
            if event == TradeUpdateEvent::Filled {
                row.broker_fills += 1;
                assert_eq!(row.broker_fills, 1, "duplicate entry execution");
                near(
                    order.filled_price.unwrap(),
                    ENTRY_PRICE,
                    "broker entry price",
                );
                assert!(
                    observed
                        .order_ids
                        .lock()
                        .unwrap()
                        .insert(order.order_id.clone()),
                    "broker reused order ID"
                );
                row.order_id = Some(order.order_id.clone());
                row.entry_latency_ms = Some(row.created.elapsed().as_secs_f64() * 1000.0);
            } else {
                row.broker_closes += 1;
                assert_eq!(row.broker_closes, 1, "duplicate exit execution");
                assert_eq!(row.order_id.as_ref(), Some(&order.order_id));
                near(order.filled_price.unwrap(), EXIT_PRICE, "broker exit price");
                near(
                    order.realized_pnl.unwrap(),
                    row.sign * row.quantity,
                    "broker realized P&L",
                );
                row.close_latency_ms = Some(
                    row.close_requested
                        .expect("explicit close must precede exit")
                        .elapsed()
                        .as_secs_f64()
                        * 1000.0,
                );
            }
        }))
        .await
        .unwrap();
    let anchor = if live {
        DateTime::from_timestamp(Utc::now().timestamp() / 60 * 60, 0).unwrap()
    } else {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    };
    let broker = UnifiedBroker::new(
        paper,
        OfflineFeed {
            connected: AtomicBool::new(false),
            callback: Mutex::new(None),
            anchor,
            bars,
        },
    );
    let feed = broker.data.clone();
    let timeframe = TimeFrame::new(1, TimeFrameUnit::Minute);
    let mut state = StrategyState::new(
        "Offline lifecycle benchmark".into(),
        "1".into(),
        LifecycleStrategy {
            orders,
            generated: false,
            bar_heavy,
            live,
            seeded_history_rows: 0,
            indicator_checks: 0,
            indicator_checksum: 0.0,
            evidence: evidence.clone(),
        },
        broker,
        if live {
            StrategyMode::Live
        } else {
            StrategyMode::Backtest
        },
        timeframe.clone(),
    );
    state.set_artifact_root(&directory.path);
    state.set_warm_up_bars(if bar_heavy {
        INDICATOR_WARMUP_ROWS as i32
    } else {
        1
    });

    let producer = if live {
        let feed = feed.clone();
        let evidence = evidence.clone();
        Some(tokio::spawn(async move {
            // A single queued completed bar prevents admission overflow without changing engine capacities.
            // The live warmup may already contain anchor's current minute.
            for index in 1..=MAX_LIVE_BARS {
                if evidence.producer_finished.load(Ordering::Acquire) {
                    return;
                }
                let callback = loop {
                    if evidence.producer_finished.load(Ordering::Acquire) {
                        return;
                    }
                    let callback = feed.callback.lock().unwrap().clone();
                    if let Some(callback) = callback {
                        break callback;
                    }
                    tokio::task::yield_now().await;
                };
                callback(feed.bar(index));
                tokio::select! {
                    _ = evidence.bar_ack.notified() => {},
                    _ = tokio::time::sleep(SAMPLE_TIMEOUT) => panic!("live owner did not acknowledge its bar"),
                }
                // Let asynchronous durable commands advance between synthetic clock steps.
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            panic!("finite offline feed exhausted before normal lifecycle completion");
        }))
    } else {
        None
    };
    let started = Instant::now();
    if live {
        tokio::time::timeout(SAMPLE_TIMEOUT, state.run_live(None))
            .await
            .expect("offline live deadline; sample is invalid")
            .expect("offline run_live failed");
    } else {
        tokio::time::timeout(
            SAMPLE_TIMEOUT,
            state.run_backtest(
                anchor,
                anchor + chrono::Duration::minutes(bars as i64 - 1),
                timeframe,
            ),
        )
        .await
        .expect("offline backtest deadline; sample is invalid")
        .expect("offline run_backtest failed");
    }
    let elapsed = started.elapsed();
    evidence.producer_finished.store(true, Ordering::Release);
    // A final on_bar can request stop before the producer's next iteration.
    evidence.bar_ack.notify_one();
    if let Some(producer) = producer {
        producer.await.expect("offline callback producer failed");
    }
    assert_eq!(
        state.status,
        if live {
            StrategyStatus::Stopped
        } else {
            StrategyStatus::Completed
        }
    );
    assert_eq!(evidence.teardown_count.load(Ordering::Relaxed), 1);
    assert_eq!(
        state.insights.active_insight_count(),
        0,
        "no active exposure after explicit closes"
    );
    assert!(!probe.is_connected());
    assert!(!feed.is_connected());
    assert!(
        feed.callback.lock().unwrap().is_none(),
        "data subscription retained after disconnect"
    );
    assert!(
        !probe.has_pending_trade_events(),
        "broker event queue was not drained"
    );
    assert!(
        probe.get_positions().await.unwrap().is_empty(),
        "broker exposure remains"
    );
    assert!(
        probe.get_orders().await.unwrap().is_empty(),
        "normal drained shutdown must release every terminal PaperBroker order"
    );
    let account = probe.get_account().await.unwrap();
    let expected_pnl: f64 = (0..orders).map(|i| sign(i) * quantity(i)).sum();
    near(account.cash, STARTING_CASH + expected_pnl, "final cash");
    near(account.equity, STARTING_CASH + expected_pnl, "final equity");
    near(
        account.buying_power,
        STARTING_CASH + expected_pnl,
        "final buying power",
    );
    near(
        account.accrued_commission,
        0.0,
        "zero-fee fixture commission",
    );
    let processed_bars = evidence.bar_count.load(Ordering::Acquire);
    let mut outcomes = Vec::new();
    let mut entry_ms = Vec::new();
    let mut close_ms = Vec::new();
    {
        let rows = evidence.rows.lock().unwrap();
        assert_eq!(rows.len(), orders);
        for row in rows.values() {
            assert_eq!(row.broker_fills, 1);
            assert_eq!(row.broker_closes, 1);
            assert!(
                row.canonical_fill && row.canonical_close,
                "actual reducer states must match broker outcomes"
            );
            entry_ms.push(row.entry_latency_ms.unwrap());
            close_ms.push(row.close_latency_ms.unwrap());
            outcomes.push((row.index, row.quantity, row.sign * row.quantity));
        }
    }
    outcomes.sort_by_key(|row| row.0);
    assert_eq!(evidence.order_ids.lock().unwrap().len(), orders);
    let strategy = state.strategy();
    if bar_heavy {
        assert_eq!(strategy.seeded_history_rows, INDICATOR_WARMUP_ROWS);
        assert_eq!(
            processed_bars, bars,
            "all replay bars after the separate pre-start warmup must reach the strategy"
        );
        assert_eq!(strategy.indicator_checks, bars);
        near(
            strategy.indicator_checksum,
            bars as f64 * 252.0,
            "indicator stream checksum",
        );
    }
    let indicator_checks = strategy.indicator_checks;
    let indicator_checksum = strategy.indicator_checksum;
    let seeded_history_rows = strategy.seeded_history_rows;
    drop(state); // The stopped runtime must release its journal before independent inspection.
    let journal = journal_proof(&directory.path, orders).await;
    let fingerprint = json!({"orders": orders, "unique_entry_orders": orders, "canonical_filled": orders,
        "broker_closed": orders, "canonical_closed": orders, "entry_price": ENTRY_PRICE, "exit_price": EXIT_PRICE,
        "outcomes_index_quantity_pnl": outcomes, "realized_pnl": expected_pnl,
        "cash": account.cash, "equity": account.equity, "buying_power": account.buying_power,
        "open_positions": 0, "active_insights": 0, "teardown_calls": 1,
        "seeded_history_rows": seeded_history_rows,
        "indicator_checks": indicator_checks, "indicator_checksum": indicator_checksum, "journal": journal});
    directory.successful = true;
    Sample {
        elapsed,
        fingerprint,
        entry_ms,
        close_ms,
        bars: processed_bars,
    }
}

fn percentile(values: &mut [f64], fraction: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    Some(values[((values.len() - 1) as f64 * fraction).ceil() as usize])
}
fn latency(mut values: Vec<f64>) -> Value {
    json!({"count": values.len(), "p50_ms": percentile(&mut values, 0.5), "p95_ms": percentile(&mut values, 0.95), "p99_ms": percentile(&mut values, 0.99)})
}
fn argument(args: &[String], name: &str, default: usize) -> usize {
    args.iter()
        .position(|arg| arg == name)
        .map(|i| {
            args.get(i + 1)
                .expect("option requires a value")
                .parse()
                .expect("numeric option")
        })
        .unwrap_or(default)
}
fn main() {
    assert!(
        !cfg!(debug_assertions),
        "use --release for measured lifecycle work"
    );
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--smoke" => index += 1,
            "--case" | "--orders" | "--bars" | "--samples" | "--warmups" | "--output" => index += 2,
            unknown => panic!(
                "unsupported benchmark argument {unknown}; launch/cloud/credential arguments are forbidden"
            ),
        }
    }
    assert!(
        aq_engine::launch_config::from_process().unwrap().is_none(),
        "offline benchmark forbids launch configuration"
    );
    for name in [
        "AQE_EVENT_BUS",
        "AQE_NATS_URL",
        "AQE_NATS_TOKEN",
        "AQE_OUTBOX_PATH",
        "AQE_RUN_ID",
        "AQE_ACCOUNT_COORDINATOR_DIR",
        "AQE_MT5_BRIDGE_TOKEN",
        "AQE_MT5_BRIDGE_BIND_ADDR",
    ] {
        assert!(
            std::env::var_os(name).is_none(),
            "unset {name} before running the offline fixture"
        );
    }
    let smoke = args.iter().any(|arg| arg == "--smoke");
    let orders = if smoke {
        16
    } else {
        argument(&args, "--orders", 1000)
    };
    let bars = if smoke {
        128
    } else {
        argument(&args, "--bars", 10_000)
    };
    let samples = if smoke {
        1
    } else {
        argument(&args, "--samples", 7)
    };
    let warmups = if smoke {
        0
    } else {
        argument(&args, "--warmups", 1)
    };
    assert!(orders > 0 && orders <= 100_000 && bars >= 32 && samples > 0);
    let selected = args
        .iter()
        .position(|a| a == "--case")
        .map(|i| args.get(i + 1).expect("--case requires a value").as_str())
        .unwrap_or("all");
    assert!(["all", "live_close", "backtest_close", "backtest_bars"].contains(&selected));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(WORKER_THREADS)
        .enable_all()
        .build()
        .unwrap();
    let mut results = Vec::new();
    for case in ["backtest_close", "live_close", "backtest_bars"] {
        if selected != "all" && selected != case {
            continue;
        }
        let mut fingerprint = None;
        for _ in 0..warmups {
            let run = runtime.block_on(sample(case, orders, bars));
            if let Some(expected) = &fingerprint {
                assert_eq!(&run.fingerprint, expected);
            }
            fingerprint = Some(run.fingerprint);
        }
        let mut raw_ms = Vec::new();
        let mut bar_counts = Vec::new();
        let mut entries = Vec::new();
        let mut closes = Vec::new();
        for _ in 0..samples {
            let run = runtime.block_on(sample(case, orders, bars));
            if let Some(expected) = &fingerprint {
                assert_eq!(
                    &run.fingerprint, expected,
                    "outcomes changed between samples"
                );
            }
            fingerprint = Some(run.fingerprint);
            raw_ms.push(run.elapsed.as_secs_f64() * 1000.0);
            bar_counts.push(run.bars);
            entries.extend(run.entry_ms);
            closes.extend(run.close_ms);
        }
        let operations = if case == "backtest_bars" {
            bars
        } else {
            orders
        };
        let mut sorted = raw_ms.clone();
        let median = percentile(&mut sorted, 0.5).unwrap();
        results.push(json!({"id": case, "operations_per_sample": operations,
            "throughput_unit": if case == "backtest_bars" {"bars/s"} else {"complete trades/s"},
            "median_ms": median, "p95_ms": percentile(&mut sorted, 0.95),
            "throughput_per_second": operations as f64 / (median / 1000.0),
            "raw_sample_ms": raw_ms, "processed_bars_per_sample": bar_counts,
            "entry_to_broker_fill": latency(entries), "explicit_close_to_broker_exit": latency(closes),
            "outcome_fingerprint": fingerprint}));
    }
    let report = json!({"suite": "AQE offline lifecycle companion", "smoke_correctness_only": smoke,
        "engine_version": env!("CARGO_PKG_VERSION"), "generated_at_utc": Utc::now().to_rfc3339(),
        "warmup_runs": warmups, "sample_runs": samples, "tokio_worker_threads": WORKER_THREADS,
        "polars_max_threads_env": std::env::var("POLARS_MAX_THREADS").ok(), "history_limit": HISTORY_LIMIT,
        "bar_heavy_prestart_warmup_rows": INDICATOR_WARMUP_ROWS,
        "engine_capacity_overrides": false, "paper_leverage": 1, "starting_cash": STARTING_CASH,
        "live_callback_window": 1, "live_interbar_pause_ms": 1, "live_max_bars": MAX_LIVE_BARS,
        "scope": "Actual run_live/run_backtest startup, bars, durable journal/worker/reducer, explicit closes, normal teardown/disconnect. Excludes fixture construction, post-run economic/journal validation and directory removal. No AQS, NATS, database service, MT5 or network.",
        "results": results});
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
