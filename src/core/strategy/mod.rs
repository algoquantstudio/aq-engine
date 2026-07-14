#[cfg(test)]
mod tests;
use crate::core::alpha::WrappedAlphaModel;
use crate::core::broker::backtest_state::BacktestResults;
use crate::core::broker::paper_broker::PaperBroker;
use crate::core::broker::types::{Asset, BarData, BrokerError, Order, TradeUpdateEvent};
use crate::core::indicators::Indicator;
use crate::core::insight::types::InsightState;
use crate::core::insight::{Insight, InsightCollection, InsightSnapshot, InsightStrategyContext};
use crate::core::lifecycle::{
    LifecycleTiming, WrappedOnInitLogic, WrappedOnStartLogic, WrappedOnTeardownLogic,
};
use crate::core::pipeline::WrappedInsightPipe;
use crate::core::tui::{
    BacktestProgressSnapshot, RuntimeInsightSnapshot, RuntimeInsightStateSnapshot,
    RuntimeMetricsSnapshot, RuntimeTelemetry, TuiConfig, summarise_value,
};
use crate::core::universe::WrappedUniverseModel;
use crate::core::utils::tools::{StrategyTools, TradingTools, dynamic_round_for_asset};
use dashmap::DashMap;
use polars::prelude::*;
#[cfg(feature = "runtime")]
use rustc_hash::FxHashSet;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
#[cfg(not(feature = "runtime"))]
type FxHashSet<T> = HashSet<T>;
use std::path::PathBuf;
use std::vec;
use uuid::Uuid;
mod aqs_ops;
mod aqs_sync;
mod aqs_types;
mod live_metrics;
mod types;
pub use types::{InsightPipeline, StrategyMode, StrategyStatus};
pub mod traits;
use crate::core::broker::UnifiedBroker;
use crate::core::broker::traits::{Broker, DataFeed, DataProvider, OrderManagementProvider};
pub use crate::core::events::{
    EventStreamOptions, EventStreamRequest, EventStreamType, MarketStreamKey, StrategyEventContext,
};
use crate::core::events::{MarketDataEvent, ResolvedEventStream};
use crate::core::utils::timeframe::TimeFrame;
pub use traits::{BrokerAccess, Strategy, StrategyContext, TeardownCleanupReport};

pub use aqs_types::AqsAuth;
use live_metrics::LivePerformanceTracker;
use log::{debug, error, info, warn};

pub fn set_logging_level(level: impl AsRef<str>) -> Result<(), log::SetLoggerError> {
    let log_level = level.as_ref().trim().to_lowercase();
    let log_level = if log_level.is_empty() {
        "info".to_string()
    } else {
        log_level
    };
    let default_log_filter = format!("{log_level},tracing::span=warn,turso=warn,libsql=warn");
    let env = env_logger::Env::default().default_filter_or(default_log_filter);
    let mut builder = env_logger::Builder::from_env(env);
    builder.format_timestamp_millis();
    if TuiConfig::from_process_args().enabled {
        builder.target(env_logger::Target::Pipe(crate::core::tui::tui_log_writer()));
    }
    let result = builder.try_init();
    if result.is_ok() {
        info!("Logger initialised with level {}", log_level);
    }
    result
}

fn current_dir_with_aqmeta() -> Option<PathBuf> {
    let mut dir = std::env::current_dir().ok()?;
    loop {
        let has_aqmeta = std::fs::read_dir(&dir).ok()?.flatten().any(|entry| {
            entry
                .path()
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("aqmeta"))
        });
        if has_aqmeta {
            return Some(dir);
        }
        if !dir.pop() {
            return None;
        }
    }
}

// ─────────────────────── StrategyState ───────────────────────

pub struct StrategyState<S, E, D>
where
    S: Strategy,
    E: Broker + OrderManagementProvider,
    D: DataFeed + DataProvider,
{
    pub strategy_id: Uuid,
    pub name: String,
    pub version: String,

    /// Wrapped in Option to enable split-borrow during backtest loop.
    /// Always `Some` outside of active backtest iteration.
    strategy: Option<S>,
    pub timeframe: TimeFrame,
    pub insights: InsightCollection,
    pub history: HashMap<String, DataFrame>,
    pub universe: HashMap<String, Asset>,
    pub broker: UnifiedBroker<E, D>,

    //
    pub status: StrategyStatus,
    pub mode: StrategyMode,

    // Alpha Models
    alpha_models: Vec<WrappedAlphaModel>,

    // Lifecycle logic blocks
    on_start_logic: Vec<WrappedOnStartLogic>,
    on_init_logic: Vec<WrappedOnInitLogic>,
    on_teardown_logic: Vec<WrappedOnTeardownLogic>,

    // Universe models
    universe_models: Vec<WrappedUniverseModel>,

    // Market event streams
    event_stream_requests: Vec<EventStreamRequest>,
    event_context_stack: Vec<StrategyEventContext>,

    // Indicators
    pub indicators: HashMap<String, Box<dyn Indicator>>,

    // Risk Management Parameters
    warm_up_bars: i32,
    warm_up_progress: HashMap<String, i32>,
    pub execution_risk: f64,
    pub min_reward_risk_ratio: f64,
    pub base_confidence: f64,
    pub variables: DashMap<String, Value>,
    pub max_history_rows: usize,
    history_seed_anchor: Option<chrono::DateTime<chrono::Utc>>,
    live_metrics: LivePerformanceTracker,
    default_live_auth: Option<AqsAuth>,
    runtime_telemetry: RuntimeTelemetry,
    artifact_root: Option<PathBuf>,

    // Insight Pipeline
    pub insight_pipeline: InsightPipeline,

    // Internal Shutdown
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl<S, E, D> StrategyState<S, E, D>
where
    S: Strategy,
    E: Broker + OrderManagementProvider,
    D: DataFeed + DataProvider,
{
    fn trade_event_matches_insight(insight: &Insight, order: &Order) -> bool {
        order
            .insight_id
            .as_deref()
            .is_some_and(|value| value == insight.insight_id.to_string())
            || insight.order_id.as_deref() == Some(&order.order_id)
            || insight.close_order_id.as_deref() == Some(&order.order_id)
            || insight
                .legs
                .take_profit
                .as_ref()
                .and_then(|leg| leg.order_id.as_deref())
                == Some(order.order_id.as_str())
            || insight
                .legs
                .stop_loss
                .as_ref()
                .and_then(|leg| leg.order_id.as_deref())
                == Some(order.order_id.as_str())
            || insight
                .legs
                .trailing_stop
                .as_ref()
                .and_then(|leg| leg.order_id.as_deref())
                == Some(order.order_id.as_str())
    }

    fn close_price_from_order(order: &Order) -> f64 {
        order
            .legs
            .as_ref()
            .and_then(|legs| {
                legs.stop_loss
                    .as_ref()
                    .filter(|leg| leg.status == TradeUpdateEvent::Filled)
                    .and_then(|leg| leg.filled_price.or(leg.limit_price))
                    .or_else(|| {
                        legs.take_profit
                            .as_ref()
                            .filter(|leg| leg.status == TradeUpdateEvent::Filled)
                            .and_then(|leg| leg.filled_price.or(leg.limit_price))
                    })
                    .or_else(|| {
                        legs.trailing_stop
                            .as_ref()
                            .filter(|leg| leg.status == TradeUpdateEvent::Filled)
                            .and_then(|leg| leg.filled_price.or(leg.limit_price))
                    })
            })
            .or(order.filled_price)
            .or(order.stop_price)
            .or(order.limit_price)
            .unwrap_or(0.0)
    }

    fn order_log_summary(order: &Order) -> String {
        let entry = order
            .filled_price
            .or(order.limit_price)
            .or(order.stop_price)
            .map(|price| format!("{price:.4}"))
            .unwrap_or_else(|| "MARKET".to_string());

        let tp = order
            .legs
            .as_ref()
            .and_then(|legs| legs.take_profit.as_ref())
            .and_then(|leg| leg.limit_price)
            .map(|price| format!("{price:.4}"))
            .unwrap_or_else(|| "-".to_string());

        let sl = order
            .legs
            .as_ref()
            .and_then(|legs| legs.stop_loss.as_ref())
            .and_then(|leg| leg.limit_price)
            .map(|price| format!("{price:.4}"))
            .unwrap_or_else(|| "-".to_string());

        format!(
            "{} {:?} {:.4} @ {} type={:?} status={:?} strat={} order_id={} tp={} sl={}",
            order.asset.symbol,
            order.side,
            order.qty,
            entry,
            order.order_type,
            order.status,
            order.strategy_type.as_deref().unwrap_or("-"),
            order.order_id,
            tp,
            sl
        )
    }

    fn sync_broker_managed_levels(insight: &mut Insight, order: &Order) {
        if let Some(legs) = &order.legs {
            if let Some(tp) = legs.take_profit.as_ref().and_then(|leg| leg.limit_price) {
                let should_sync_tp = insight
                    .take_profit_levels()
                    .is_none_or(|levels| levels.len() <= 1);
                if should_sync_tp {
                    insight.set_take_profit_levels(Some(vec![Self::round_order_value(order, tp)]));
                }
            }
            if let Some(sl) = legs.stop_loss.as_ref().and_then(|leg| leg.limit_price) {
                let should_sync_sl = insight
                    .stop_loss_levels()
                    .is_none_or(|levels| levels.len() <= 1);
                if should_sync_sl {
                    insight.set_stop_loss_levels(Some(vec![Self::round_order_value(order, sl)]));
                }
            }
            if let Some(trailing_gap) = legs.trailing_stop.as_ref().and_then(|leg| leg.trail_price)
            {
                insight.set_trailing_stop_price(Some(Self::round_order_value(order, trailing_gap)));
            }
            insight.legs = legs.clone();
        }
    }

    fn round_order_value(order: &Order, value: f64) -> f64 {
        dynamic_round_for_asset(value, &order.asset)
    }

    fn round_order_option_value(order: &Order, value: Option<f64>) -> Option<f64> {
        value.map(|value| Self::round_order_value(order, value))
    }

    pub fn new(
        name: String,
        version: String,
        strategy: S,
        broker: UnifiedBroker<E, D>,
        mode: StrategyMode,
        timeframe: TimeFrame,
    ) -> Self {
        Self {
            name,
            version,
            strategy: Some(strategy),
            broker,
            mode,
            status: StrategyStatus::Initialised,
            strategy_id: Uuid::new_v4(),
            insights: InsightCollection::new()
                .with_order_id_index_enabled(matches!(mode, StrategyMode::Live)),
            history: Default::default(),
            universe: Default::default(),
            alpha_models: Vec::new(),
            on_start_logic: Vec::new(),
            on_init_logic: Vec::new(),
            on_teardown_logic: Vec::new(),
            universe_models: Vec::new(),
            event_stream_requests: Vec::new(),
            event_context_stack: Vec::new(),
            indicators: HashMap::new(),
            warm_up_bars: 0,
            warm_up_progress: HashMap::new(),
            execution_risk: 0.02,
            min_reward_risk_ratio: 2.0,
            base_confidence: 1.0,
            variables: DashMap::new(),
            max_history_rows: 2000,
            history_seed_anchor: None,
            live_metrics: LivePerformanceTracker::default(),
            default_live_auth: AqsAuth::from_process_args(),
            runtime_telemetry: RuntimeTelemetry::default(),
            artifact_root: None,
            insight_pipeline: Default::default(),
            timeframe,
            shutdown_tx: tokio::sync::watch::channel(false).0,
        }
    }

    /// Set the project directory used for run artifacts such as `backtests/` and `live/`.
    ///
    /// Generated AQS projects set this to `CARGO_MANIFEST_DIR` so output stays beside the
    /// strategy project even if the binary is launched from another working directory.
    pub fn set_artifact_root(&mut self, root: impl Into<PathBuf>) {
        self.artifact_root = Some(root.into());
    }

    pub fn with_artifact_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.set_artifact_root(root);
        self
    }

    fn artifact_root(&self) -> PathBuf {
        self.artifact_root
            .clone()
            .or_else(|| current_dir_with_aqmeta())
            .or_else(|| std::env::current_dir().ok())
            .unwrap_or_else(|| PathBuf::from("."))
    }

    /// Access the strategy (panics if called during backtest iteration — internal only).
    pub fn strategy(&self) -> &S {
        self.strategy
            .as_ref()
            .expect("strategy is taken during iteration")
    }

    /// Mutably access the strategy.
    pub fn strategy_mut(&mut self) -> &mut S {
        self.strategy
            .as_mut()
            .expect("strategy is taken during iteration")
    }

    // ─────────────────── Registration Helpers ───────────────────

    /// Register an alpha model (Python's `registerAlpha`).
    pub fn add_alpha(&mut self, alpha: WrappedAlphaModel) {
        self.alpha_models.push(alpha);
    }

    /// Register an indicator securely with deduplication (Python's TA registration).
    pub fn register_indicator(&mut self, mut indicator: Box<dyn Indicator>) {
        let name = indicator.name();
        if !self.indicators.contains_key(&name) {
            // Apply `run` to all existing history DataFrames to backfill
            for (_, df) in self.history.iter_mut() {
                if df.height() > 0 {
                    if let Err(e) = indicator.run(df) {
                        warn!(
                            "Failed to run indicator {} on existing history: {}",
                            name, e
                        );
                    }
                }
            }
            self.indicators.insert(name, indicator);
        }
    }

    fn apply_indicators_to_history(&mut self, symbol: &str) {
        let Some(history_df) = self.history.get_mut(symbol) else {
            return;
        };
        if history_df.height() == 0 {
            return;
        }

        let current_time = self.broker.get_current_time();
        for (name, indicator) in self.indicators.iter_mut() {
            if let Err(error) = indicator.run(history_df) {
                warn!(
                    "Failed to run indicator {} on seeded history for {}: {}",
                    name, symbol, error
                );
            }
            indicator.set_last_run_time(current_time);
        }
    }

    fn min_history_rows(&self) -> usize {
        self.warm_up_bars.max(0) as usize + 1
    }

    fn filter_history_before_anchor(
        df: DataFrame,
        anchor: chrono::DateTime<chrono::Utc>,
    ) -> Result<DataFrame, BrokerError> {
        if df.height() == 0 {
            return Ok(df);
        }

        if df.column("timestamp").is_err() {
            return Ok(df);
        }

        let anchor_ms = anchor.timestamp_millis();

        df.lazy()
            .filter(col("timestamp").cast(DataType::Int64).lt(lit(anchor_ms)))
            .collect()
            .map_err(|error| BrokerError::DataFeedError(error.to_string()))
    }

    fn preseed_warmup_history_for_symbol(
        &mut self,
        symbol: &str,
        warmup_bars: i32,
    ) -> Result<usize, BrokerError> {
        if warmup_bars <= 0 {
            return Ok(0);
        }

        let streams = self.event_streams_for_symbol(symbol);
        let mut total_rows = 0;
        for stream in streams {
            total_rows += self.preseed_warmup_history_for_stream(&stream, warmup_bars)?;
        }

        Ok(total_rows)
    }

    fn preseed_warmup_history_for_stream(
        &mut self,
        stream: &ResolvedEventStream,
        warmup_bars: i32,
    ) -> Result<usize, BrokerError> {
        let anchor = self
            .history_seed_anchor
            .unwrap_or_else(|| self.broker.get_current_time());
        let start = stream
            .key
            .timeframe
            .add_time_increment(anchor, -(warmup_bars as i64))
            .map_err(|error| {
                BrokerError::DataFeedError(format!(
                    "Failed to calculate warm-up start for {}: {:?}",
                    stream.history_key, error
                ))
            })?;

        let mut seeded = crate::core::broker::block_on_broker_future(self.broker.get_history(
            &stream.key.symbol,
            start,
            anchor,
            stream.key.timeframe,
        ))?;
        seeded = Self::filter_history_before_anchor(seeded, anchor)?;

        let max_history_rows = self.max_history_rows();
        if seeded.height() > max_history_rows {
            seeded = seeded.tail(Some(max_history_rows));
        }

        let rows = seeded.height();
        self.history.insert(stream.history_key.clone(), seeded);
        self.apply_indicators_to_history(&stream.history_key);
        if rows >= warmup_bars as usize {
            self.warm_up_progress.remove(&stream.history_key);
        } else {
            self.warm_up_progress
                .insert(stream.history_key.clone(), rows as i32);
        }
        Ok(rows)
    }

    fn cleanup_active_insights_for_teardown_internal(&mut self) -> TeardownCleanupReport {
        let mut report = TeardownCleanupReport::default();
        let insight_ids = self.insights.ids();

        for id in insight_ids {
            let Some(mut insight) = self.insights.remove_insight(&id) else {
                continue;
            };

            match insight.state {
                InsightState::New => {
                    insight.order_rejected("Teardown cleanup rejected new insight");
                    report.rejected_new += 1;
                }
                InsightState::Executed => match insight.order_id.clone() {
                    Some(order_id) => match self.broker.cancel_order_sync(&order_id) {
                        Ok(true) => {
                            insight.order_cancelled("Teardown cleanup cancelled executed insight");
                            report.cancelled_executed += 1;
                        }
                        Ok(false) => report.failures.push(format!(
                            "{} {} cancel request returned false for order {}",
                            insight.symbol, insight.insight_id, order_id
                        )),
                        Err(error) => report.failures.push(format!(
                            "{} {} failed to cancel order {}: {:?}",
                            insight.symbol, insight.insight_id, order_id, error
                        )),
                    },
                    None => report.failures.push(format!(
                        "{} {} is executed but has no order id",
                        insight.symbol, insight.insight_id
                    )),
                },
                InsightState::Filled => {
                    let had_order_id = insight.order_id.is_some();
                    let remaining_quantity = insight.remaining_quantity();
                    insight.close(self);
                    if had_order_id && insight.closing {
                        report.closed_filled += 1;
                    } else {
                        report.failures.push(format!(
                            "{} {} failed to queue filled close for remaining quantity {:.4}",
                            insight.symbol, insight.insight_id, remaining_quantity
                        ));
                    }
                }
                _ => {}
            }

            self.add_insight_internal(insight, true);
        }

        report
    }

    fn begin_teardown(&mut self) {
        if !self.status.is_finished() {
            self.status = StrategyStatus::Stopping;
        }
    }

    fn add_insight_internal(&mut self, insight: Insight, allow_during_teardown: bool) {
        if !self.status.is_running() && !allow_during_teardown {
            warn!(
                "Ignoring new insight while strategy status is {}: {}",
                self.status,
                insight.log_summary(),
            );
            return;
        }
        let mut insight = insight;
        self.bind_insight_context(&mut insight);
        let children_to_submit = (!allow_during_teardown
            && insight.state == InsightState::Filled
            && !insight.children.is_empty())
        .then(|| insight.children.clone());
        self.insights.add_insight(insight);

        if let Some(children) = children_to_submit {
            for mut child in children {
                child.submit(self);
                self.add_insight_internal(child, false);
            }
        }
    }

    /// Register a pipe into the correct `InsightState` bucket (Python's `add_executor`).
    pub fn add_pipe(&mut self, pipe: WrappedInsightPipe) {
        self.insight_pipeline.add_pipe(pipe);
    }

    /// Register multiple pipes at once (Python's `add_executors`).
    pub fn add_pipes(&mut self, pipes: Vec<WrappedInsightPipe>) {
        self.insight_pipeline.add_pipes(pipes);
    }

    pub fn add_on_start_logic(&mut self, logic: WrappedOnStartLogic) {
        self.on_start_logic.push(logic);
    }

    pub fn add_on_init_logic(&mut self, logic: WrappedOnInitLogic) {
        self.on_init_logic.push(logic);
    }

    pub fn add_on_teardown_logic(&mut self, logic: WrappedOnTeardownLogic) {
        self.on_teardown_logic.push(logic);
    }

    fn run_on_start_logic(&mut self, timing: LifecycleTiming) {
        let mut logic_blocks = std::mem::take(&mut self.on_start_logic);
        for logic in logic_blocks.iter_mut() {
            if logic.timing() == timing {
                debug!("Running on_start logic {}", logic.name());
                logic.run(self);
            }
        }
        logic_blocks.append(&mut self.on_start_logic);
        self.on_start_logic = logic_blocks;
    }

    fn run_on_init_logic(&mut self, timing: LifecycleTiming, asset: &Asset) {
        let mut logic_blocks = std::mem::take(&mut self.on_init_logic);
        for logic in logic_blocks.iter_mut() {
            if logic.timing() == timing {
                debug!(
                    "Running on_init logic {} for {}",
                    logic.name(),
                    asset.symbol
                );
                logic.run(self, asset);
            }
        }
        logic_blocks.append(&mut self.on_init_logic);
        self.on_init_logic = logic_blocks;
    }

    fn run_on_teardown_logic(&mut self, timing: LifecycleTiming) {
        let mut logic_blocks = std::mem::take(&mut self.on_teardown_logic);
        for logic in logic_blocks.iter_mut() {
            if logic.timing() == timing {
                debug!("Running on_teardown logic {}", logic.name());
                logic.run(self);
            }
        }
        logic_blocks.append(&mut self.on_teardown_logic);
        self.on_teardown_logic = logic_blocks;
    }

    pub fn add_universe_model(&mut self, model: WrappedUniverseModel) {
        self.universe_models.push(model);
    }

    pub fn add_events(&mut self, event_type: EventStreamType, timeframe: Option<TimeFrame>) {
        self.add_events_with_options(EventStreamRequest::new(event_type, timeframe));
    }

    pub fn add_events_with_options(&mut self, request: EventStreamRequest) {
        self.event_stream_requests.push(request);
    }

    fn current_event_context(&self) -> Option<StrategyEventContext> {
        self.event_context_stack.last().cloned()
    }

    fn resolve_event_streams(&mut self) -> Vec<ResolvedEventStream> {
        let mut streams = Vec::new();
        let mut seen = HashSet::new();
        let mut symbols: Vec<String> = self.universe.keys().cloned().collect();
        symbols.sort();

        for symbol in symbols {
            for stream in self.event_streams_for_symbol(&symbol) {
                if seen.insert(stream.key.clone()) {
                    self.history
                        .entry(stream.history_key.clone())
                        .or_insert_with(DataFrame::default);
                    streams.push(stream);
                }
            }
        }

        streams
    }

    fn event_streams_for_symbol(&self, symbol: &str) -> Vec<ResolvedEventStream> {
        let mut streams = Vec::new();
        let mut seen = HashSet::new();

        let main_stream = ResolvedEventStream::new(
            EventStreamType::Bar,
            symbol.to_string(),
            self.timeframe,
            self.timeframe,
            true,
        );
        if seen.insert(main_stream.key.clone()) {
            streams.push(main_stream);
        }

        for request in &self.event_stream_requests {
            let timeframe = request.timeframe.unwrap_or(self.timeframe);
            let allow_trading = if timeframe == self.timeframe {
                true
            } else {
                request.options.allow_trading
            };
            let stream = ResolvedEventStream::new(
                request.event_type,
                symbol.to_string(),
                timeframe,
                self.timeframe,
                allow_trading,
            );
            if seen.insert(stream.key.clone()) {
                streams.push(stream);
            }
        }

        streams
    }

    fn run_universe_models(&mut self) -> Option<HashSet<String>> {
        if self.universe_models.is_empty() {
            return None;
        }

        let mut symbols = HashSet::new();
        let mut models = std::mem::take(&mut self.universe_models);
        for model in models.iter_mut() {
            debug!("Running universe model {}", model.name());
            let result = model.run(self);
            if result.success {
                symbols.extend(result.symbols);
            }
        }
        models.append(&mut self.universe_models);
        self.universe_models = models;

        Some(symbols)
    }

    // ─────────────────── Warm-Up ───────────────────

    /// Get the current warm-up period.
    pub fn warm_up_bars(&self) -> i32 {
        self.warm_up_bars
    }

    /// Set the warm-up period. Only raises the value — never lowers it.
    /// This allows multiple alpha models / strategies to each declare
    /// their own minimum warm-up requirement without conflicting.
    pub fn set_warm_up_bars(&mut self, bars: i32) {
        if bars > self.warm_up_bars {
            self.warm_up_bars = bars;
        }
    }

    pub fn set_execution_risk(&mut self, risk: f64) {
        self.execution_risk = risk;
    }

    pub fn set_min_reward_risk_ratio(&mut self, ratio: f64) {
        self.min_reward_risk_ratio = ratio;
    }

    pub fn set_base_confidence(&mut self, confidence: f64) {
        self.base_confidence = Self::normalize_base_confidence(confidence);
    }

    pub fn base_confidence(&self) -> f64 {
        self.base_confidence
    }

    fn normalize_base_confidence(confidence: f64) -> f64 {
        if confidence <= 0.0 {
            0.0
        } else if confidence <= 1.0 {
            confidence
        } else {
            (confidence / 100.0).clamp(0.0, 1.0)
        }
    }

    fn runtime_variable_snapshot(&self) -> Vec<crate::core::tui::RuntimeVariableSnapshot> {
        let mut variables = self
            .variables
            .iter()
            .map(|entry| {
                let mut snapshot = summarise_value(entry.value());
                snapshot.key = entry.key().clone();
                snapshot
            })
            .collect::<Vec<_>>();
        variables.sort_by(|left, right| left.key.cmp(&right.key));
        variables.truncate(64);
        variables
    }

    fn runtime_insight_counts(&self) -> HashMap<String, usize> {
        self.insights
            .get_state_count()
            .into_iter()
            .map(|(state, count)| (format!("{:?}", state), count))
            .collect()
    }

    fn runtime_active_insights(&self) -> Vec<RuntimeInsightSnapshot> {
        let mut insights = self
            .insights
            .active_insight_ids_unsorted()
            .into_iter()
            .filter_map(|id| {
                let insight = self.insights.get(&id)?;
                Some(RuntimeInsightSnapshot {
                    insight_id: insight.insight_id.to_string(),
                    parent_id: insight.parent_id.map(|id| id.to_string()),
                    symbol: insight.symbol.clone(),
                    state: InsightSnapshot::insight_state_label(insight.state()).to_string(),
                    side: InsightSnapshot::order_side_label(insight.side()).to_string(),
                    strategy_type: InsightSnapshot::strategy_type_label(insight.strategy_type())
                        .into_owned(),
                    order_id: insight.order_id.clone(),
                    close_order_id: insight.close_order_id.clone(),
                    quantity: insight.quantity(),
                    contracts: insight.contracts,
                    order_type: InsightSnapshot::order_type_label(insight.order_type()).to_string(),
                    order_class: InsightSnapshot::order_class_label(insight.order_class())
                        .to_string(),
                    limit_price: insight.limit_price(),
                    stop_price: insight.stop_price(),
                    take_profit_levels: insight.take_profit_levels(),
                    stop_loss_levels: insight.stop_loss_levels(),
                    filled_price: insight.filled_price,
                    close_price: insight.close_price,
                    confidence: insight.confidence(),
                    created_at: Some(insight.created_at),
                    updated_at: Some(insight.updated_at),
                    filled_at: insight.filled_at,
                    closed_at: insight.closed_at,
                    state_history: insight
                        .state_history
                        .iter()
                        .rev()
                        .take(5)
                        .map(|(at, state, message)| RuntimeInsightStateSnapshot {
                            at: *at,
                            state: InsightSnapshot::insight_state_label(state).to_string(),
                            message: message.clone(),
                        })
                        .collect(),
                })
            })
            .collect::<Vec<_>>();
        insights.sort_by(|left, right| {
            left.symbol
                .cmp(&right.symbol)
                .then(left.updated_at.cmp(&right.updated_at))
                .then(left.insight_id.cmp(&right.insight_id))
        });
        insights.truncate(128);
        insights
    }

    fn runtime_live_metrics(&self) -> Option<RuntimeMetricsSnapshot> {
        self.live_metrics
            .snapshot()
            .map(|snapshot| RuntimeMetricsSnapshot {
                final_equity: snapshot.final_equity,
                total_return_pct: snapshot.total_return_pct,
                total_trades: snapshot.total_trades,
                open_positions_count: snapshot.open_positions_count,
                open_insights_count: snapshot.open_insights_count,
                updated_at: Some(snapshot.updated_at),
                summary_lines: Vec::new(),
            })
    }

    fn runtime_backtest_progress(&self) -> Option<BacktestProgressSnapshot> {
        self.broker
            .backtest_state
            .as_ref()
            .map(|state| state.read().progress_snapshot())
    }

    fn publish_runtime_snapshot(
        &mut self,
        aqs_sync_status: impl Into<String>,
        saved_result_path: Option<String>,
    ) {
        let mode = self.mode;
        let strategy_name = self.name.clone();
        let strategy_id = self.strategy_id.to_string();
        let status = self.status.to_string();
        let current_time = Some(self.broker.get_current_time());
        let mut universe = self.universe.keys().cloned().collect::<Vec<_>>();
        universe.sort();
        let variables = self.runtime_variable_snapshot();
        let insight_counts = self.runtime_insight_counts();
        let active_insights = self.runtime_active_insights();
        let metrics = self.runtime_live_metrics();
        let backtest_progress = self.runtime_backtest_progress();
        let broker_status = if self.broker.is_broker_connected() {
            "connected"
        } else {
            "disconnected"
        }
        .to_string();
        let datafeed_status = if self.broker.is_datafeed_connected() {
            "connected"
        } else {
            "disconnected"
        }
        .to_string();
        let aqs_sync_status = aqs_sync_status.into();

        self.runtime_telemetry.update_snapshot(|snapshot| {
            snapshot.mode = mode;
            snapshot.strategy_name = strategy_name;
            snapshot.strategy_id = strategy_id;
            snapshot.status = status;
            snapshot.current_time = current_time;
            snapshot.universe = universe;
            snapshot.variables = variables;
            snapshot.insight_counts = insight_counts;
            snapshot.active_insights = active_insights;
            if metrics.is_some() {
                snapshot.metrics = metrics;
            }
            snapshot.backtest_progress = backtest_progress;
            snapshot.aqs_sync_status = aqs_sync_status;
            snapshot.broker_status = broker_status;
            snapshot.datafeed_status = datafeed_status;
            if saved_result_path.is_some() {
                snapshot.saved_result_path = saved_result_path;
            }
        });
    }

    fn publish_runtime_event(
        &mut self,
        level: impl Into<String>,
        message: impl Into<String>,
        aqs_sync_status: impl Into<String>,
    ) {
        self.runtime_telemetry.push_event(level, message);
        self.publish_runtime_snapshot(aqs_sync_status, None);
    }

    fn push_runtime_event(&mut self, level: impl Into<String>, message: impl Into<String>) {
        self.runtime_telemetry.push_event(level, message);
    }

    pub fn max_history_rows(&self) -> usize {
        self.max_history_rows.max(self.min_history_rows())
    }

    pub fn update_max_history_rows(&mut self, rows: usize) {
        self.max_history_rows = rows;
    }

    /// Trigger strategy shutdown gracefully.
    pub fn shutdown(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Get the current timeframe.
    pub fn timeframe(&self) -> TimeFrame {
        self.timeframe
    }

    /// Set the timeframe.
    pub fn set_timeframe(&mut self, timeframe: TimeFrame) {
        self.timeframe = timeframe;
    }

    // ─────────────────── Universe Loading ───────────────────

    /// Load universe: strategy.universe() + universe models → broker.get_ticker_info() → strategy.init per asset.
    /// Mirrors Python's `_loadUniverse()`.
    async fn load_universe(&mut self, strategy: &mut S) {
        let mut symbol_set = strategy.universe(self);
        if let Some(model_symbols) = self.run_universe_models() {
            symbol_set.extend(model_symbols);
        }
        let mut universe_symbols: Vec<_> = symbol_set.iter().cloned().collect();
        universe_symbols.sort();
        info!("Loading strategy universe: {}", universe_symbols.join(", "));

        for symbol in &symbol_set {
            debug!("Loading asset metadata for {}", symbol);
            match self.broker.get_ticker_info(symbol).await {
                Ok(asset) => {
                    // Initialize empty history for this symbol if its tradable, and add to universe_symbols
                    if !asset.tradable {
                        info!(
                            "Asset {} is not tradable; skipping history initialization and strategy init",
                            asset.symbol
                        );
                        debug!("Asset details: {:?}", asset);
                        continue;
                    }
                    self.history
                        .insert(asset.symbol.clone(), DataFrame::default());
                    if let Err(error) = self.broker.execution.configure_asset_metadata(&asset) {
                        warn!(
                            "Failed to configure execution metadata for {}: {}",
                            asset.symbol, error
                        );
                    }
                    self.universe.insert(asset.symbol.clone(), asset);
                }
                Err(error) => {
                    warn!("Failed to load asset metadata for {}: {}", symbol, error);
                    continue;
                }
            }
        }
    }

    // ─────────────────── Alpha Model Loading ───────────────────

    /// Start all alpha models so indicators and warm-up requirements are registered.
    fn start_alpha_models(&mut self) {
        let mut alphas = std::mem::take(&mut self.alpha_models);

        // alpha.start() — one-time startup
        for alpha in alphas.iter_mut() {
            debug!("Starting alpha model {}", alpha.name());
            alpha.start(self);
        }

        self.alpha_models = alphas;
    }

    /// Run strategy init and OnInit lifecycle logic once per loaded asset.
    fn load_init(&mut self, strategy: &mut S) {
        let assets: Vec<Asset> = self.universe.values().cloned().collect();
        for asset in &assets {
            debug!("Running strategy init for {}", asset.symbol);
            self.run_on_init_logic(LifecycleTiming::BeforeGenerated, asset);
            strategy.init(self, asset);
            self.run_on_init_logic(LifecycleTiming::AfterGenerated, asset);
        }
    }

    /// Init all alpha models once per asset after strategy init has completed.
    fn init_alpha_models(&mut self) {
        let mut alphas = std::mem::take(&mut self.alpha_models);

        // alpha.init(asset) — per-asset initialization
        let assets: Vec<Asset> = self.universe.values().cloned().collect();
        for asset in &assets {
            for alpha in alphas.iter_mut() {
                debug!(
                    "Initialising alpha model {} for {}",
                    alpha.name(),
                    asset.symbol
                );
                alpha.init(self, asset);
            }
        }

        self.alpha_models = alphas;
    }

    // ─────────────────── Bar Processing ───────────────────

    #[cfg(test)]
    fn bar_data_timestamp(&self, bar: &BarData) -> chrono::DateTime<chrono::Utc> {
        match bar {
            BarData::Bars(bars) => bars
                .last()
                .map(|bar| bar.timestamp)
                .unwrap_or_else(|| self.broker.get_current_time()),
            BarData::Frame(frame) => frame
                .column("timestamp")
                .ok()
                .and_then(|column| column.cast(&DataType::Int64).ok())
                .and_then(|column| column.i64().ok().and_then(|values| values.get(0)))
                .and_then(chrono::DateTime::from_timestamp_millis)
                .unwrap_or_else(|| self.broker.get_current_time()),
        }
    }

    #[cfg(test)]
    fn main_event_context(&self, symbol: &str, bar: &BarData) -> StrategyEventContext {
        StrategyEventContext {
            event_type: EventStreamType::Bar,
            symbol: symbol.to_string(),
            timeframe: self.timeframe,
            history_key: symbol.to_string(),
            is_feature: false,
            allow_trading: true,
            timestamp: self.bar_data_timestamp(bar),
        }
    }

    fn _on_market_data_event(&mut self, strategy: &mut S, event: MarketDataEvent) {
        let symbol = event.context.symbol.clone();
        self._on_bar_with_context(
            strategy,
            &symbol,
            &BarData::Bars(vec![event.bar]),
            event.context,
        );
    }

    /// Process a bar callback: append to HISTORY, warm-up check, on_bar → generate insights.
    /// Mirrors Python's `_on_bar()`.
    #[cfg(test)]
    fn _on_bar(&mut self, strategy: &mut S, symbol: &str, bar: &BarData) {
        let context = self.main_event_context(symbol, bar);
        self._on_bar_with_context(strategy, symbol, bar, context);
    }

    fn _on_bar_with_context(
        &mut self,
        strategy: &mut S,
        symbol: &str,
        bar: &BarData,
        context: StrategyEventContext,
    ) {
        match bar {
            BarData::Bars(bars) => {
                if let Some(last_bar) = bars.last() {
                    debug!(
                        "on_bar symbol={} history_key={} timeframe={} feature={} o={:.4} h={:.4} l={:.4} c={:.4} v={:.0}",
                        symbol,
                        context.history_key,
                        context.timeframe,
                        context.is_feature,
                        last_bar.open,
                        last_bar.high,
                        last_bar.low,
                        last_bar.close,
                        last_bar.volume
                    );
                } else {
                    debug!("on_bar symbol={} bars=empty", symbol);
                }
            }
            BarData::Frame(frame) => {
                debug!(
                    "on_bar symbol={} history_key={} timeframe={} feature={} frame_shape={:?}",
                    symbol,
                    context.history_key,
                    context.timeframe,
                    context.is_feature,
                    frame.shape()
                );
            }
        }
        let max_history_rows = self.max_history_rows();
        let history_key = context.history_key.clone();
        let allow_trading = context.allow_trading;
        self.event_context_stack.push(context);

        // Append to history DataFrame
        {
            let history_df = self
                .history
                .entry(history_key.clone())
                .or_insert_with(DataFrame::default);
            let bar_df_res = self.broker.data.format_on_bar(bar.clone());

            if let Ok(mut bar_df) = bar_df_res {
                let appended_rows = bar_df.height();
                // Pre-fill bar_df with Null values for indicators to match schema
                for (name, _ind) in self.indicators.iter() {
                    let null_series =
                        Series::full_null(name.into(), bar_df.height(), &DataType::Float64);
                    let _ = bar_df.with_column(null_series);
                }

                let current_time = self.broker.get_current_time();
                for row_index in 0..appended_rows {
                    for (_name, ind) in self.indicators.iter_mut() {
                        let window_size = ind.window_size();
                        let available_rows = history_df.height() + row_index + 1;
                        if available_rows < window_size {
                            ind.set_last_run_time(current_time);
                            continue;
                        }

                        // Indicators only need their bounded lookback. Build that window from the
                        // retained history and the incoming rows, then write results into the small
                        // incoming frame before appending it to the full history frame.
                        let prior_rows_needed = window_size.saturating_sub(row_index + 1);
                        let mut window = history_df.tail(Some(prior_rows_needed));
                        let incoming = bar_df.slice(0, row_index + 1);
                        if window.height() == 0 {
                            window = incoming;
                        } else if window.vstack_mut(&incoming).is_err() {
                            ind.set_last_run_time(current_time);
                            continue;
                        }
                        if window.height() > window_size {
                            window = window.tail(Some(window_size));
                        }

                        if let Ok(result) = ind.run_bar(&window) {
                            for (column_name, value) in result {
                                let Ok(series) = bar_df.column(&column_name) else {
                                    continue;
                                };
                                let Ok(values) = series.f64() else {
                                    continue;
                                };
                                let mut values = values.into_iter().collect::<Vec<Option<f64>>>();
                                if let Some(slot) = values.get_mut(row_index) {
                                    *slot = Some(value);
                                    let updated = Series::new(column_name.into(), values);
                                    let _ = bar_df.with_column(updated);
                                }
                            }
                        }
                        ind.set_last_run_time(current_time);
                    }
                }

                if history_df.height() == 0 {
                    *history_df = bar_df;
                } else {
                    let _ = history_df.vstack_mut(&bar_df);
                }
                if history_df.height() > max_history_rows {
                    *history_df = history_df.tail(Some(max_history_rows));
                }
            }
        }

        // Warm-up check: skip strategy callbacks until enough bars accumulated
        if self.warm_up_bars > 0 {
            if let Some(history_df) = self.history.get(&history_key) {
                let loaded = history_df.height() as i32;
                if loaded < self.warm_up_bars {
                    let should_log = self
                        .warm_up_progress
                        .get(&history_key)
                        .copied()
                        .unwrap_or(-1)
                        != loaded;
                    self.warm_up_progress.insert(history_key.clone(), loaded);
                    if should_log {
                        info!(
                            "[warm up] {} {}/{} candles",
                            history_key, loaded, self.warm_up_bars
                        );
                    }
                    debug!(
                        "Warm-up active for {}: {}/{} bars loaded, skipping strategy callbacks",
                        history_key, loaded, self.warm_up_bars
                    );
                    self.event_context_stack.pop();
                    return;
                }
                if self.warm_up_progress.remove(&history_key).is_some() {
                    info!(
                        "[warm up] {} complete ({}/{})",
                        history_key, loaded, self.warm_up_bars
                    );
                }
            }
        }

        // Call strategy callbacks
        debug!(
            "Warm-up complete for {}, invoking strategy callbacks",
            history_key
        );
        strategy.on_bar(self, symbol, bar);

        // Generate insights from alpha models then strategy
        if allow_trading {
            self._generate_insights(strategy, symbol);
        }
        self.event_context_stack.pop();
    }

    // ─────────────────── Insight Generation ───────────────────

    /// Loop alpha models → generate insights → submit successful ones.
    /// Then call strategy.generate_insights().
    /// Mirrors Python's `_generateInsights()`.
    fn _generate_insights(&mut self, strategy: &mut S, symbol: &str) {
        // Take alpha_models out to avoid borrow conflict
        let mut alphas = std::mem::take(&mut self.alpha_models);

        for alpha in alphas.iter_mut() {
            if !alpha.is_allowed_asset(symbol) {
                continue;
            }
            debug!("Running alpha model {} for {}", alpha.name(), symbol);
            let result = alpha.generate_insights(self, symbol);
            debug!(
                "Alpha model {} result for {}: success={} message={:?} created_insight={}",
                alpha.name(),
                symbol,
                result.success,
                result.message,
                result.insight.is_some()
            );
            if result.success {
                if let Some(insight) = result.insight {
                    info!("New insight created: {}", insight.log_summary());
                    self.add_insight(insight);
                }
            }
        }

        // Put alpha_models back
        self.alpha_models = alphas;

        // Strategy's own generate_insights
        debug!("Running strategy generate_insights for {}", symbol);
        strategy.generate_insights(self, symbol);
        debug!("Completed strategy generate_insights for {}", symbol);
    }

    // ─────────────────── Insight Pipeline ───────────────────

    /// Run insight pipeline: for each active insight, run pipes matching its state.
    /// Mirrors Python's `_insightListener` (the per-step portion).
    fn run_insight_pipeline(&mut self) {
        // Take pipeline and insights out to avoid borrow conflicts
        let mut pipeline = std::mem::take(&mut self.insight_pipeline);
        let mut insights = std::mem::take(&mut self.insights);
        let mut touched_insight_ids = FxHashSet::default();

        let insight_ids = insights.active_insight_ids_unsorted();

        for id in insight_ids {
            let mut insight = match insights.get_mut(&id) {
                Some(ins) => ins,
                None => continue,
            };

            let state = insight.state.clone();
            let can_expire = insight.can_expire();
            let should_clear_first_on_fill =
                state == InsightState::Filled && insight.first_on_fill();
            let mut before_fingerprint = if can_expire || should_clear_first_on_fill {
                Some(insight.snapshot_fingerprint_hash())
            } else {
                None
            };

            if can_expire && insight.has_expired(self) {
                debug!(
                    "Skipping expired insight {} in state {:?}",
                    insight.insight_id(),
                    insight.state()
                );
                if before_fingerprint
                    .is_some_and(|before| before != insight.snapshot_fingerprint_hash())
                {
                    touched_insight_ids.insert(id);
                }
                continue;
            }

            if let Some(pipes) = pipeline.get_pipes_for_state(&state) {
                let should_run_any_pipe = pipes.iter().any(|pipe| pipe.should_run(&insight));
                if should_run_any_pipe && before_fingerprint.is_none() {
                    before_fingerprint = Some(insight.snapshot_fingerprint_hash());
                }
                let mut failed = false;
                let mut rejection_reason: Option<String> = None;
                for pipe in pipes.iter_mut() {
                    if !pipe.should_run(&insight) {
                        continue;
                    }
                    debug!(
                        "Running insight pipe {} for insight {} in state {:?}",
                        pipe.name(),
                        insight.insight_id(),
                        state
                    );
                    let result = pipe.run(self, &mut insight);
                    debug!(
                        "Insight pipe {} result for insight {}: success={} passed={} message={:?}",
                        pipe.name(),
                        insight.insight_id(),
                        result.success,
                        result.passed,
                        result.message
                    );
                    if !result.success {
                        failed = true;
                        rejection_reason = Some(result.message.unwrap_or_else(|| {
                            format!("Insight pipe {} failed", result.pipe_name)
                        }));
                        break;
                    }
                    if !result.passed {
                        if insight.closing || insight.cancelling || insight.state.is_inactive() {
                            break;
                        }
                        failed = true;
                        rejection_reason = Some(result.message.unwrap_or_else(|| {
                            format!("Insight pipe {} did not pass", result.pipe_name)
                        }));
                        break;
                    }
                }
                if failed {
                    if !insight.state.is_inactive() {
                        insight.order_rejected(
                            rejection_reason
                                .as_deref()
                                .unwrap_or("Insight pipeline failed"),
                        );
                    }
                    if before_fingerprint
                        .is_some_and(|before| before != insight.snapshot_fingerprint_hash())
                    {
                        touched_insight_ids.insert(id);
                    }
                    continue;
                }
            }

            if should_clear_first_on_fill {
                debug!(
                    "Filled-state insight pipeline completed for insight {}; clearing first_on_fill",
                    insight.insight_id()
                );
                insight.set_first_on_fill(false);
            }
            if before_fingerprint
                .is_some_and(|before| before != insight.snapshot_fingerprint_hash())
            {
                touched_insight_ids.insert(id);
            }
        }

        // Put them back
        self.insight_pipeline = pipeline;
        self.insights = insights;
        for insight_id in touched_insight_ids {
            self.insights.refresh_runtime_tracking(&insight_id);
        }
    }

    fn sync_backtest_insight_snapshots(&mut self) {
        let Some(backtest_state) = self.broker.backtest_state.as_ref() else {
            return;
        };

        let dirty_insight_ids = self.insights.take_dirty_insight_ids();
        if dirty_insight_ids.is_empty() {
            return;
        }

        let strategy_id = self.strategy_id.to_string();
        let snapshots = dirty_insight_ids
            .into_iter()
            .filter_map(|insight_id| {
                self.insights
                    .get(&insight_id)
                    .map(|insight| InsightSnapshot::from_insight(&insight, &strategy_id))
            })
            .collect::<Vec<_>>();

        if !snapshots.is_empty() {
            backtest_state.write().record_insight_snapshots(snapshots);
        }
    }

    // ─────────────────── Trade Event Processing ───────────────────

    /// Process trade events from the broker. Mirrors Python's `_on_trade_update`.
    /// Handles all event types: New, PendingNew, Filled, PartialFilled,
    /// Cancelled, Rejected, Closed, Replaced.
    /// Works with any broker implementing `OrderManagementProvider`.
    fn on_trade_update(&mut self) {
        let events = self.broker.drain_trade_events();
        let mut children_to_submit = Vec::new();
        let mut parents_to_reconcile = Vec::new();
        let mut touched_insight_ids = FxHashSet::default();

        for (order, event) in events {
            debug!(
                "Trade event: event={:?} {}",
                event,
                Self::order_log_summary(&order)
            );
            if !self.universe.contains_key(&order.asset.symbol) {
                continue;
            }
            let mut insight_ids = self.insights.candidate_insight_ids_for_trade_event(&order);
            if insight_ids.is_empty() {
                insight_ids = self.insights.ids();
            }
            for id in insight_ids {
                let mut insight = match self.insights.get_mut(&id) {
                    Some(i) => i,
                    None => continue,
                };
                if insight.symbol != order.asset.symbol {
                    continue;
                }
                let matched = Self::trade_event_matches_insight(&insight, &order);

                match (&insight.state, &event) {
                    // NEW → EXECUTED on accept
                    (
                        InsightState::New,
                        TradeUpdateEvent::Accepted
                        | TradeUpdateEvent::PendingNew
                        | TradeUpdateEvent::New,
                    ) => {
                        if matched {
                            insight.order_accepted(&order.order_id);
                            touched_insight_ids.insert(id);
                            break;
                        }
                    }
                    // NEW can jump straight to fill in live brokers
                    (InsightState::New, TradeUpdateEvent::Filled) => {
                        if matched {
                            insight.order_accepted(&order.order_id);
                            let price = Self::round_order_value(
                                &order,
                                order.filled_price.or(order.limit_price).unwrap_or(0.0),
                            );
                            insight.position_filled(
                                price,
                                order.filled_qty,
                                &order.order_id,
                                Self::round_order_option_value(&order, order.commission),
                            );
                            Self::sync_broker_managed_levels(&mut insight, &order);
                            touched_insight_ids.insert(id);
                            if let Some(parent_id) = insight.parent_id {
                                parents_to_reconcile.push(parent_id);
                            }
                            if !insight.children.is_empty() {
                                children_to_submit.extend(insight.children.clone());
                            }
                            break;
                        }
                    }
                    // NEW can jump straight to partial fill in live brokers
                    (InsightState::New, TradeUpdateEvent::PartialFilled) => {
                        if matched {
                            insight.order_accepted(&order.order_id);
                            let price = Self::round_order_value(
                                &order,
                                order.filled_price.or(order.limit_price).unwrap_or(0.0),
                            );
                            insight.partial_filled(
                                order.filled_qty,
                                price,
                                &order.order_id,
                                Self::round_order_option_value(&order, order.commission),
                            );
                            Self::sync_broker_managed_levels(&mut insight, &order);
                            touched_insight_ids.insert(id);
                            break;
                        }
                    }
                    // EXECUTED → FILLED
                    (InsightState::Executed, TradeUpdateEvent::Filled) => {
                        if matched {
                            let price = Self::round_order_value(
                                &order,
                                order.filled_price.or(order.limit_price).unwrap_or(0.0),
                            );
                            insight.position_filled(
                                price,
                                order.filled_qty,
                                &order.order_id,
                                Self::round_order_option_value(&order, order.commission),
                            );
                            Self::sync_broker_managed_levels(&mut insight, &order);
                            touched_insight_ids.insert(id);
                            if let Some(parent_id) = insight.parent_id {
                                parents_to_reconcile.push(parent_id);
                            }

                            // If this insight has children, queue them for submission
                            if !insight.children.is_empty() {
                                children_to_submit.extend(insight.children.clone());
                            }
                            break;
                        }
                    }
                    // EXECUTED partial fill
                    (InsightState::Executed, TradeUpdateEvent::PartialFilled) => {
                        if matched {
                            let price = Self::round_order_value(
                                &order,
                                order.filled_price.or(order.limit_price).unwrap_or(0.0),
                            );
                            insight.partial_filled(
                                order.filled_qty,
                                price,
                                &order.order_id,
                                Self::round_order_option_value(&order, order.commission),
                            );
                            Self::sync_broker_managed_levels(&mut insight, &order);
                            touched_insight_ids.insert(id);
                            break;
                        }
                    }
                    // FILLED partial close / scale-out
                    (InsightState::Filled, TradeUpdateEvent::PartialFilled) => {
                        if matched {
                            let price = Self::round_order_value(
                                &order,
                                Self::close_price_from_order(&order),
                            );
                            insight.partial_closed(
                                order.filled_qty,
                                price,
                                &order.order_id,
                                Self::round_order_option_value(&order, order.commission),
                            );
                            touched_insight_ids.insert(id);
                            // We dont need to reset this when closing the insight - its kind of a
                            // design choice
                            // Self::sync_broker_managed_levels(insight, &order);
                            break;
                        }
                    }
                    // EXECUTED → CANCELED
                    (InsightState::Executed | InsightState::New, TradeUpdateEvent::Cancelled) => {
                        if matched {
                            Self::sync_broker_managed_levels(&mut insight, &order);
                            insight.order_cancelled("Order cancelled");
                            touched_insight_ids.insert(id);
                            // Cancel any open children
                            parents_to_reconcile.push(insight.insight_id);
                            break;
                        }
                    }
                    // EXECUTED → REJECTED
                    (InsightState::Executed | InsightState::New, TradeUpdateEvent::Rejected) => {
                        if matched {
                            Self::sync_broker_managed_levels(&mut insight, &order);
                            insight.order_rejected(
                                order
                                    .rejection_reason
                                    .as_deref()
                                    .unwrap_or("Order rejected"),
                            );
                            touched_insight_ids.insert(id);
                            // Cancel any open children
                            parents_to_reconcile.push(insight.insight_id);
                            break;
                        }
                    }
                    // EXECUTED → REPLACED (order modified)
                    (InsightState::Executed, TradeUpdateEvent::Replaced) => {
                        if matched {
                            if order.limit_price != insight.limit_price {
                                insight.limit_price =
                                    Self::round_order_option_value(&order, order.limit_price);
                            }
                            if insight.quantity != Some(order.qty) {
                                insight.quantity = Some(order.qty);
                            }
                            Self::sync_broker_managed_levels(&mut insight, &order);
                            touched_insight_ids.insert(id);
                            break;
                        }
                    }
                    // FILLED → CLOSED (SL/TP/manual close)
                    (InsightState::Filled, TradeUpdateEvent::Filled | TradeUpdateEvent::Closed) => {
                        if matched {
                            if event == TradeUpdateEvent::Filled && order.side == insight.side {
                                continue;
                            }
                            let price = Self::round_order_value(
                                &order,
                                Self::close_price_from_order(&order),
                            );
                            Self::sync_broker_managed_levels(&mut insight, &order);
                            insight.position_closed(
                                price,
                                &order.order_id,
                                order.filled_qty,
                                Self::round_order_option_value(&order, order.realized_pnl),
                                Self::round_order_option_value(&order, order.commission),
                                Self::round_order_option_value(&order, order.swap),
                            );
                            touched_insight_ids.insert(id);

                            // Close/Cancel any open children
                            parents_to_reconcile.push(insight.insight_id);
                            break;
                        }
                    }
                    // MT5 can deliver terminal messages out of order. If a real fill arrives
                    // after a stale cancel/reject, the broker fill is authoritative.
                    (
                        InsightState::Cancelled | InsightState::Rejected,
                        TradeUpdateEvent::Filled,
                    ) => {
                        if matched {
                            let price = Self::round_order_value(
                                &order,
                                order.filled_price.or(order.limit_price).unwrap_or(0.0),
                            );
                            insight.position_filled(
                                price,
                                order.filled_qty,
                                &order.order_id,
                                Self::round_order_option_value(&order, order.commission),
                            );
                            Self::sync_broker_managed_levels(&mut insight, &order);
                            touched_insight_ids.insert(id);
                            if let Some(parent_id) = insight.parent_id {
                                parents_to_reconcile.push(parent_id);
                            }
                            break;
                        }
                    }
                    _ => {}
                }
            }
        }

        // Process deferred child events
        for mut child in children_to_submit {
            child.submit(self);
            self.add_insight(child);
        }

        if !parents_to_reconcile.is_empty() {
            let parents_to_reconcile = parents_to_reconcile
                .into_iter()
                .filter(|parent_id| {
                    self.insights
                        .get(parent_id)
                        .is_some_and(|parent| parent.state.is_inactive())
                })
                .collect::<FxHashSet<_>>();
            let children_to_process = self.insights.child_ids_for_parents(&parents_to_reconcile);

            // Second pass: apply closures (requires mutable self)
            for id in children_to_process {
                if let Some(mut child) = self.insights.remove_insight(&id) {
                    match child.state {
                        InsightState::Filled => {
                            child.close(self);
                            touched_insight_ids.insert(id);
                            self.add_insight_internal(child, true);
                        }
                        InsightState::New if child.order_id.is_none() => {
                            child.order_cancelled(
                                "Parent insight is terminal before child submission",
                            );
                            touched_insight_ids.insert(id);
                            self.add_insight_internal(child, true);
                        }
                        InsightState::New | InsightState::Executed => {
                            child.cancel(self);
                            touched_insight_ids.insert(id);
                            self.add_insight_internal(child, true);
                        }
                        _ => {
                            self.add_insight_internal(child, true);
                        }
                    }
                }
            }
        }

        for insight_id in touched_insight_ids {
            self.insights.refresh_runtime_tracking(&insight_id);
        }
    }
}

// ─────────────────────── StrategyContext impl ───────────────────────

impl<S, E, D> StrategyContext for StrategyState<S, E, D>
where
    S: Strategy,
    E: Broker + OrderManagementProvider,
    D: DataFeed + DataProvider,
{
    fn universe(&self) -> &HashMap<String, Asset> {
        &self.universe
    }

    fn history(&self) -> &HashMap<String, DataFrame> {
        &self.history
    }

    fn insights(&self) -> &InsightCollection {
        &self.insights
    }

    fn add_insight(&mut self, insight: Insight) {
        self.add_insight_internal(insight, false);
    }

    fn submit_insight(&mut self, insight: &mut Insight) {
        info!("Submitting order to broker: {}", insight.log_summary());
        let result = crate::core::broker::block_on_broker_future(
            self.broker.execution.submit_order(insight.clone()),
        );
        if let Ok(order) = result {
            debug!(
                "Broker accepted: insight_id={} {}",
                insight.insight_id(),
                Self::order_log_summary(&order)
            );
            if order.order_id.is_empty() {
                error!(
                    "Broker returned an empty order id after submit: insight_id={} {}",
                    insight.insight_id(),
                    Self::order_log_summary(&order)
                );
            } else {
                match order.status {
                    TradeUpdateEvent::Accepted
                    | TradeUpdateEvent::PendingNew
                    | TradeUpdateEvent::New
                    | TradeUpdateEvent::Pending => {
                        insight.order_accepted(&order.order_id);
                    }
                    TradeUpdateEvent::Filled => {
                        insight.order_accepted(&order.order_id);
                        let price = Self::round_order_value(
                            &order,
                            order.filled_price.or(order.limit_price).unwrap_or(0.0),
                        );
                        insight.position_filled(
                            price,
                            order.filled_qty,
                            &order.order_id,
                            Self::round_order_option_value(&order, order.commission),
                        );
                        Self::sync_broker_managed_levels(insight, &order);
                    }
                    TradeUpdateEvent::PartialFilled => {
                        insight.order_accepted(&order.order_id);
                        let price = Self::round_order_value(
                            &order,
                            order.filled_price.or(order.limit_price).unwrap_or(0.0),
                        );
                        insight.partial_filled(
                            order.filled_qty,
                            price,
                            &order.order_id,
                            Self::round_order_option_value(&order, order.commission),
                        );
                        Self::sync_broker_managed_levels(insight, &order);
                    }
                    TradeUpdateEvent::Rejected => {
                        insight.order_rejected(
                            order
                                .rejection_reason
                                .as_deref()
                                .unwrap_or("Order rejected"),
                        );
                    }
                    TradeUpdateEvent::Cancelled | TradeUpdateEvent::Expired => {
                        insight.order_cancelled("Order cancelled before acknowledgement");
                    }
                    TradeUpdateEvent::Closed | TradeUpdateEvent::Replaced => {
                        insight.order_id = Some(order.order_id.clone());
                    }
                }
            }
        } else if let Err(error) = result {
            error!(
                "Broker submit_order failed for insight_id={} {}: {:?}",
                insight.insight_id(),
                insight.log_summary(),
                error
            );
        }
    }

    fn register_indicator(&mut self, indicator: Box<dyn crate::core::indicators::Indicator>) {
        // self refers to StrategyState, which has the implementation
        self.register_indicator(indicator);
    }

    fn add_alpha(&mut self, alpha: crate::core::alpha::WrappedAlphaModel) {
        self.add_alpha(alpha);
    }

    fn add_pipe(&mut self, pipe: crate::core::pipeline::WrappedInsightPipe) {
        self.add_pipe(pipe);
    }

    fn add_universe_model(&mut self, model: crate::core::universe::WrappedUniverseModel) {
        self.add_universe_model(model);
    }

    fn add_events(&mut self, event_type: EventStreamType, timeframe: Option<TimeFrame>) {
        StrategyState::add_events(self, event_type, timeframe);
    }

    fn add_events_with_options(&mut self, request: EventStreamRequest) {
        StrategyState::add_events_with_options(self, request);
    }

    fn current_event(&self) -> Option<StrategyEventContext> {
        self.current_event_context()
    }

    fn set_execution_risk(&mut self, risk: f64) {
        self.set_execution_risk(risk);
    }

    fn set_min_reward_risk_ratio(&mut self, ratio: f64) {
        self.set_min_reward_risk_ratio(ratio);
    }

    fn set_base_confidence(&mut self, confidence: f64) {
        self.set_base_confidence(confidence);
    }

    fn execution_risk(&self) -> f64 {
        self.execution_risk
    }

    fn min_reward_risk_ratio(&self) -> f64 {
        self.min_reward_risk_ratio
    }

    fn base_confidence(&self) -> f64 {
        self.base_confidence
    }

    fn variables(&self) -> &DashMap<String, Value> {
        &self.variables
    }

    fn max_history_rows(&self) -> usize {
        StrategyState::max_history_rows(self)
    }

    fn set_max_history_rows(&mut self, rows: usize) {
        self.update_max_history_rows(rows);
    }

    fn tools(&self) -> Box<dyn TradingTools + '_> {
        Box::new(StrategyTools::new(
            &self.universe,
            &self.insights,
            &self.broker,
        ))
    }

    fn mode(&self) -> StrategyMode {
        self.mode.clone()
    }

    fn warm_up_bars(&self) -> i32 {
        self.warm_up_bars
    }

    fn set_warm_up_bars(&mut self, bars: i32) {
        self.set_warm_up_bars(bars);
    }

    fn timeframe(&self) -> &TimeFrame {
        &self.timeframe
    }

    fn account(&self) -> Result<crate::core::broker::types::Account, BrokerError> {
        crate::core::broker::block_on_broker_future(self.broker.get_account())
    }

    fn current_time(&self) -> chrono::DateTime<chrono::Utc> {
        if matches!(self.mode, StrategyMode::Backtest) {
            if let Some(state) = &self.broker.backtest_state {
                return state.read().current_time;
            }
            if let Some(anchor) = self.history_seed_anchor {
                return anchor;
            }
        }
        self.broker.get_current_time()
    }

    fn bind_insight_context(&self, insight: &mut Insight) {
        if let Some(state) = self.broker.backtest_state.clone() {
            insight.bind_context(InsightStrategyContext::new(move || {
                state.read().current_time
            }));
        } else {
            insight.bind_context(InsightStrategyContext::new(chrono::Utc::now));
        }
    }

    fn latest_quote(&self, symbol: &str) -> Result<crate::core::broker::types::Quote, BrokerError> {
        if matches!(self.mode, StrategyMode::Backtest) {
            if let Some(state) = &self.broker.backtest_state {
                let state = state.read();
                if let Some(bar) = state.get_current_bar(symbol) {
                    return Ok(crate::core::broker::types::Quote {
                        symbol: symbol.to_string(),
                        bid: bar.close,
                        ask: bar.close,
                        bid_size: bar.volume,
                        ask_size: bar.volume,
                        last: Some(bar.close),
                        last_size: Some(bar.volume),
                        timestamp: bar.timestamp,
                    });
                }
            }
        }
        crate::core::broker::block_on_broker_future(self.broker.get_latest_quote(symbol))
    }

    fn preseed_warmup_history(
        &mut self,
        symbol: &str,
        warmup_bars: i32,
    ) -> Result<usize, BrokerError> {
        self.preseed_warmup_history_for_symbol(symbol, warmup_bars)
    }

    fn cleanup_active_insights_for_teardown(&mut self) -> TeardownCleanupReport {
        self.cleanup_active_insights_for_teardown_internal()
    }

    fn cancel_order(&self, order_id: &str) -> Result<bool, BrokerError> {
        self.broker.cancel_order_sync(order_id)
    }

    fn update_order(&self, order_id: &str, price: f64, qty: f64) -> Result<bool, BrokerError> {
        self.broker.update_order_sync(order_id, price, qty)
    }

    fn update_stop_loss_order(
        &self,
        order_id: &str,
        price: f64,
        qty: f64,
    ) -> Result<bool, BrokerError> {
        self.broker.update_stop_loss_sync(order_id, price, qty)
    }

    fn close_position(
        &self,
        order_id: &str,
        qty: f64,
        price: Option<f64>,
    ) -> Result<bool, BrokerError> {
        self.broker.close_position_sync(order_id, qty, price)
    }

    fn shutdown(&mut self) {
        self.shutdown_tx.send(true).ok();
    }
}

// ─────────────────────── BrokerAccess impl ───────────────────────

impl<S, E, D> BrokerAccess<E, D> for StrategyState<S, E, D>
where
    S: Strategy,
    E: Broker + OrderManagementProvider,
    D: DataFeed + DataProvider,
{
    fn broker(&self) -> &UnifiedBroker<E, D> {
        &self.broker
    }

    fn broker_mut(&mut self) -> &mut UnifiedBroker<E, D> {
        &mut self.broker
    }
}

// ─────────────────────── Backtest Runner (PaperBroker only) ───────────────────────

impl<S, D> StrategyState<S, PaperBroker, D>
where
    S: Strategy,
    D: DataFeed + DataProvider,
{
    fn finalize_backtest_results(&self, results: &BacktestResults) -> Option<String> {
        info!("═══════════════════ Backtest Results ═══════════════════");
        let terminal_output_suspended = crate::core::tui::terminal_output_suspended();
        if !terminal_output_suspended {
            results.print_metrics();
        }
        info!("Insights generated: {}", self.insights.len());
        info!("Insights: {:#?}", self.insights.get_state_count());

        let run_id = Uuid::new_v4().to_string();
        let out_dir = self.artifact_root().join("backtests").join(&run_id);

        let Some(backtest_state) = self.broker.backtest_state.as_ref() else {
            warn!("Backtest completed but no backtest state was available for result persistence");
            return None;
        };

        let state = backtest_state.read();
        if let Err(error) = results.save_to_disk(&out_dir, &*state) {
            error!("Failed to save results to disk: {}", error);
            if !terminal_output_suspended {
                eprintln!("Failed to save results to disk: {}", error);
            }
            return None;
        }

        if let Ok(abs_path) = std::fs::canonicalize(&out_dir) {
            if !terminal_output_suspended {
                println!("RESULTS_SAVED_TO: {}", abs_path.display());
            }
            Some(abs_path.display().to_string())
        } else {
            if !terminal_output_suspended {
                println!("RESULTS_SAVED_TO: {}", out_dir.display());
            }
            Some(out_dir.display().to_string())
        }
    }

    /// Run a full backtest.
    ///
    /// Flow:
    /// 1. `strategy.on_start(ctx)`
    /// 2. `load_universe()` — strategy.universe() → broker.get_ticker_info()
    /// 3. `alpha.start()` per alpha — registers indicators and warm-up requirements
    /// 4. `load_init()` — OnInit lifecycle logic → strategy.init per asset
    /// 5. `alpha.init(asset)` per alpha per asset
    /// 6. `broker.load_backtest_data()` — fills BacktestState
    /// 7. Loop: `broker.step()` → for each bar: `_on_bar()` → `run_insight_pipeline()`
    /// 8. `strategy.on_teardown(ctx)`
    /// 9. Return `BacktestResults`
    pub async fn run_backtest(
        &mut self,
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
        time_frame: TimeFrame,
    ) -> Result<BacktestResults, BrokerError> {
        // Take strategy out to avoid split-borrow (self.strategy vs self as ctx)
        let mut strategy = self
            .strategy
            .take()
            .expect("strategy must be Some before run_backtest");

        self.runtime_telemetry
            .start_tui(TuiConfig::from_process_args());
        self.publish_runtime_snapshot("not configured", None);

        // ── 0. Connect broker (execution + data feed) ──
        info!(
            "Starting backtest strategy={} timeframe={:?} start={} end={}",
            self.name, time_frame, start, end
        );
        self.timeframe = time_frame.clone();
        self.history_seed_anchor = Some(start);
        if let Err(error) = self.broker.connect().await {
            error!("Backtest failed while connecting broker: {}", error);
            self.runtime_telemetry.stop_tui();
            return Err(error);
        }
        self.status = StrategyStatus::Running;
        self.publish_runtime_snapshot("not configured", None);

        // ── 1. Strategy lifecycle: on_start ──
        debug!("Invoking strategy on_start");
        self.run_on_start_logic(LifecycleTiming::BeforeGenerated);
        strategy.on_start(self);
        self.run_on_start_logic(LifecycleTiming::AfterGenerated);
        self.publish_runtime_snapshot("not configured", None);

        // ── 2. Load universe assets ──
        self.load_universe(&mut strategy).await;

        if self.universe.is_empty() {
            error!(
                "No tradable universe assets were loaded; check data-feed connectivity, symbol mapping, and asset metadata responses"
            );
            self.runtime_telemetry.stop_tui();
            return Err(BrokerError::DataFeedError(
                "No tradable universe assets were loaded; check data-feed connectivity, symbol mapping, and asset metadata responses"
                    .to_string(),
            ));
        }

        // ── 3. Start alpha models so indicators and warm-up are configured ──
        self.start_alpha_models();

        // ── 4. Run OnInit lifecycle logic and strategy init per asset ──
        self.load_init(&mut strategy);

        // ── 5. Init alpha models per asset ──
        self.init_alpha_models();

        // ── 6. Load historical event streams into BacktestState ──
        let event_streams = self.resolve_event_streams();
        use std::fmt::Write as _;
        let mut event_stream_summary = String::new();
        for stream in &event_streams {
            if !event_stream_summary.is_empty() {
                event_stream_summary.push_str(", ");
            }
            let _ = write!(
                event_stream_summary,
                "{} {} feature={} trading={}",
                stream.history_key, stream.key.timeframe, stream.is_feature, stream.allow_trading
            );
        }
        info!("Loading backtest event streams: [{}]", event_stream_summary);
        if let Err(error) = self
            .broker
            .load_backtest_event_streams(&event_streams, start, end)
            .await
        {
            error!("Backtest failed while loading event streams: {}", error);
            self.runtime_telemetry.stop_tui();
            return Err(error);
        }
        info!("Backtest event streams loaded successfully");
        self.publish_runtime_event("info", "Backtest started", "not configured");

        // ── 7. Main backtest loop ──
        let mut last_runtime_publish = std::time::Instant::now();
        loop {
            if self.runtime_telemetry.shutdown_requested() {
                warn!("Backtest stop requested from AQE TUI");
                self.publish_runtime_event(
                    "warn",
                    "Backtest stop requested from TUI",
                    "not configured",
                );
                break;
            }
            // Advance one market step: process orders → get current stream events
            let step_result = match self.broker.step_market_streams() {
                Ok(step) => step,
                Err(error) => {
                    self.publish_runtime_event(
                        "error",
                        format!("Backtest failed: {}", error),
                        "not configured",
                    );
                    self.runtime_telemetry.stop_tui();
                    return Err(error);
                }
            };

            match step_result {
                None => break, // Backtest complete
                Some(step) => {
                    debug!(
                        "Backtest step events received timestamp={} events={} execution_bars={}",
                        step.timestamp,
                        step.events.len(),
                        step.execution_bars.len()
                    );
                    // Process trade events from the broker (fills, closes, cancels, etc.)
                    debug!("Backtest loop calling on_trade_update");
                    self.on_trade_update();
                    debug!("Backtest loop on_trade_update returned");
                    self.sync_backtest_insight_snapshots();

                    // Call _on_bar for each market data event
                    for event in step.events {
                        self._on_market_data_event(&mut strategy, event);
                    }
                    // Run insight pipeline after processing tradable events for this step
                    if step.has_tradable_events {
                        debug!("Backtest loop calling run_insight_pipeline");
                        self.run_insight_pipeline();
                        debug!("Backtest loop run_insight_pipeline returned");
                        self.sync_backtest_insight_snapshots();
                    }
                    if last_runtime_publish.elapsed() >= std::time::Duration::from_millis(500) {
                        self.publish_runtime_snapshot("not configured", None);
                        last_runtime_publish = std::time::Instant::now();
                    }
                }
            }
        }

        // ── 8. Teardown ──
        debug!("Invoking strategy on_teardown");
        self.begin_teardown();
        self.run_on_teardown_logic(LifecycleTiming::BeforeGenerated);
        strategy.on_teardown(self);
        self.run_on_teardown_logic(LifecycleTiming::AfterGenerated);
        loop {
            let final_close_requests = match self.broker.flush_backtest_close_queue_at_last_bars() {
                Ok(requests) => requests,
                Err(error) => {
                    self.publish_runtime_event(
                        "error",
                        format!("Backtest teardown failed: {}", error),
                        "not configured",
                    );
                    self.runtime_telemetry.stop_tui();
                    return Err(error);
                }
            };
            if final_close_requests == 0 {
                break;
            }

            debug!(
                "Backtest teardown flushed {} final close request(s); draining trade events",
                final_close_requests
            );
            self.on_trade_update();
            self.sync_backtest_insight_snapshots();
        }
        self.sync_backtest_insight_snapshots();

        // Disconnect data feed
        let _ = self.broker.disconnect().await;

        // Put strategy back
        self.strategy = Some(strategy);
        self.history_seed_anchor = None;

        // ── 9. Results ──
        let results = self.broker.get_results();
        let saved_result_path = self.finalize_backtest_results(&results);
        self.status = StrategyStatus::Completed;
        let open_insights_count = self.insights.active_insight_ids_unsorted().len();
        let mut summary_lines = results.metric_summary_lines();
        summary_lines.push(format!("Insights generated: {}", self.insights.len()));
        summary_lines.extend(
            format!("Insights: {:#?}", self.insights.get_state_count())
                .lines()
                .map(str::to_string),
        );
        if let Some(path) = saved_result_path.as_ref() {
            summary_lines.push(format!("RESULTS_SAVED_TO: {path}"));
        }
        self.runtime_telemetry.update_snapshot(|snapshot| {
            snapshot.metrics = Some(RuntimeMetricsSnapshot {
                final_equity: results.final_equity,
                total_return_pct: results.total_return_pct,
                total_trades: results.total_trades,
                open_positions_count: 0,
                open_insights_count,
                updated_at: Some(results.finished_at),
                summary_lines,
            });
        });
        self.publish_runtime_event("info", "Backtest completed", "not configured");
        self.publish_runtime_snapshot("not configured", saved_result_path);
        self.runtime_telemetry.wait_for_tui_close();
        Ok(results)
    }
}
