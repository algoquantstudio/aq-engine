#[cfg(test)]
mod snapshot_context_tests;
#[cfg(test)]
mod position_read_tests;
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
use std::path::{Path, PathBuf};
use std::vec;
use uuid::Uuid;
mod aqs_actions;
mod aqs_connection;
mod aqs_ops;
mod aqs_reader;
mod aqs_types;
pub mod command_driver;
mod execution_persistence;
mod execution_recovery;
mod execution_runtime;
pub mod hyperparameters;
mod insight_work;
mod lifecycle_reconciliation;
mod live_capture_recovery;
mod live_event_projection;
mod live_analytics_export;
mod live_execution;
mod live_fill_history;
mod live_position_marks;
mod live_metrics;
mod live_projection;
mod live_quotes;
mod local_live_artifacts;
mod metrics_checkpoint_writer;
mod metrics_risk_fence;
mod metrics_continuity;
mod metrics_continuity_lookup;
mod order_reducer;
mod pipeline_sweep;
mod snapshot_dependencies;
use order_reducer::{OrderEvidenceSource, OrderReduction};
mod types;
use types::HyperparameterSelection;
pub(crate) use types::InsightPipelineCheckpoint;
pub use types::{InsightPipeline, StrategyMode, StrategyStatus};
pub mod traits;
use crate::core::broker::traits::{Broker, DataFeed, DataProvider, OrderManagementProvider};
use crate::core::broker::{BrokerFeature, DataFeedFeature, UnifiedBroker};
pub use crate::core::events::{
    EventStreamOptions, EventStreamRequest, EventStreamType, MarketStreamKey, StrategyEventContext,
};
use crate::core::events::{MarketDataEvent, ResolvedEventStream};
use crate::core::utils::timeframe::TimeFrame;
pub use hyperparameters::{
    HYPERPARAMETER_SEED_KEY, HYPERPARAMETER_VALUES_KEY, HyperParameter, HyperParameterConfig,
    HyperParameterSelection,
};
pub use traits::{BrokerAccess, Strategy, StrategyContext, TeardownCleanupReport};

pub use aqs_types::{
    AqsAuth, StrategyActionKind, StrategyActionStatus, StrategyEventLevel, StrategyEventTitle,
    StrategyEventType,
};
use live_metrics::LivePerformanceTracker;
use log::{debug, error, info, warn};

/// Select AQE's TLS provider before any dependency constructs a Rustls client.
///
/// Polars' object-store stack can enable Ring while SurrealDB and Reqwest enable AWS-LC.
/// Rustls deliberately refuses to choose automatically when both features are present.
#[cfg(feature = "runtime")]
fn ensure_rustls_crypto_provider() {
    crate::core::live_sync::install_default_rustls_provider();
}

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
    // At most one 256-event broker page is retained across reducer yields.
    pending_broker_events: std::collections::VecDeque<(Order, TradeUpdateEvent)>,
    command_driver: Option<command_driver::StrategyCommandDriver>,
    execution_error: Option<crate::core::broker::commands::BrokerCommandError>,
    insight_work: insight_work::InsightWorkQueue,
    execution_persistence: execution_persistence::ExecutionPersistence,

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
    // Native execution reads advance only when the owner accepts the next event,
    // never when a concurrent polling worker observes a later price.
    execution_quotes: live_quotes::ExecutionQuoteReads,

    // Indicators
    pub indicators: HashMap<String, Box<dyn Indicator>>,
    indicator_order: Vec<String>,

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
    live_metrics_cloud_write_enabled: bool,
    metrics_checkpoint_writer: Option<metrics_checkpoint_writer::MetricsCheckpointWriter>,
    default_live_auth: Option<AqsAuth>,
    hosted_sync: Option<crate::core::live_sync::HostedSyncConfig>,
    runtime_telemetry: RuntimeTelemetry,
    artifact_root: Option<PathBuf>,
    backtest_result_dir: Option<PathBuf>,
    last_backtest_result_path: Option<PathBuf>,
    hyperparameter_config: Option<HyperParameterConfig>,
    hyperparameter_selection: Option<HyperparameterSelection>,

    // Insight Pipeline
    pub insight_pipeline: InsightPipeline,

    // Internal Shutdown
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    teardown_cleanup_requested: bool,
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
            // A terminal leg carries execution evidence, not a new target. Some
            // adapters only know the exit fill and use it as the leg limit.
            // Preserve the last configured levels; keep the fill in `legs`.
            if order.status == TradeUpdateEvent::Closed
                || legs.take_profit.as_ref().is_some_and(|leg| leg.status == TradeUpdateEvent::Filled)
                || legs.stop_loss.as_ref().is_some_and(|leg| leg.status == TradeUpdateEvent::Filled)
                || legs.trailing_stop.as_ref().is_some_and(|leg| leg.status == TradeUpdateEvent::Filled)
            {
                insight.legs = legs.clone();
                return;
            }
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
        mut broker: UnifiedBroker<E, D>,
        mode: StrategyMode,
        timeframe: TimeFrame,
    ) -> Self {
        if matches!(mode, StrategyMode::Backtest) && broker.backtest_state.is_none() {
            broker.backtest_state = Some(Arc::new(parking_lot::RwLock::new(
                crate::core::broker::backtest_state::BacktestState::new(),
            )));
        }
        Self {
            name,
            version,
            strategy: Some(strategy),
            broker,
            pending_broker_events: std::collections::VecDeque::new(),
            command_driver: None,
            execution_error: None,
            insight_work: Default::default(),
            execution_persistence: Default::default(),
            mode,
            status: StrategyStatus::Initialised,
            strategy_id: Uuid::new_v4(),
            insights: InsightCollection::new(),
            history: Default::default(),
            universe: Default::default(),
            alpha_models: Vec::new(),
            on_start_logic: Vec::new(),
            on_init_logic: Vec::new(),
            on_teardown_logic: Vec::new(),
            universe_models: Vec::new(),
            event_stream_requests: Vec::new(),
            event_context_stack: Vec::new(),
            execution_quotes: Default::default(),
            indicators: HashMap::new(),
            indicator_order: Vec::new(),
            warm_up_bars: 0,
            warm_up_progress: HashMap::new(),
            execution_risk: 0.02,
            min_reward_risk_ratio: 2.0,
            base_confidence: 1.0,
            variables: DashMap::new(),
            max_history_rows: 2000,
            history_seed_anchor: None,
            live_metrics: LivePerformanceTracker::default(),
            live_metrics_cloud_write_enabled: true,
            metrics_checkpoint_writer: None,
            default_live_auth: AqsAuth::from_process_args(),
            hosted_sync: None,
            runtime_telemetry: RuntimeTelemetry::default(),
            artifact_root: None,
            backtest_result_dir: None,
            last_backtest_result_path: None,
            hyperparameter_config: None,
            hyperparameter_selection: None,
            insight_pipeline: Default::default(),
            timeframe,
            shutdown_tx: tokio::sync::watch::channel(false).0,
            teardown_cleanup_requested: false,
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

    /// Opt live execution into the hosted gateway with an embedded background sync
    /// worker. Backtests and unconfigured standalone live execution are unchanged.
    pub fn set_hosted_sync(&mut self, config: crate::core::live_sync::HostedSyncConfig) {
        self.hosted_sync = Some(config);
    }

    pub fn with_hosted_sync(mut self, config: crate::core::live_sync::HostedSyncConfig) -> Self {
        self.set_hosted_sync(config);
        self
    }

    /// Attach optional hyperparameter definitions. With this configuration,
    /// `run_backtest` and `run_live` resolve `--hyper-seed` themselves.
    pub fn set_hyper_parameter_config(&mut self, config: HyperParameterConfig) {
        self.hyperparameter_config = Some(config);
    }

    pub fn with_hyper_parameter_config(mut self, config: HyperParameterConfig) -> Self {
        self.set_hyper_parameter_config(config);
        self
    }

    /// Apply an already-enumerated sweep member to this state. Full sweeps still require a
    /// fresh state per seed because a completed state owns mutable broker and strategy state.
    pub fn set_hyper_parameter_run(
        &mut self,
        selection: &HyperParameterSelection,
        is_sweep_run: bool,
    ) -> Result<(), String> {
        if is_sweep_run {
            let config = self
                .hyperparameter_config
                .clone()
                .ok_or("Attach a HyperParameterConfig before selecting a sweep run")?;
            self.set_hyper_parameter_sweep_seed(&config, selection)
        } else {
            self.set_hyper_parameter_seed(selection)
        }
    }

    fn apply_hyper_parameters_from_process_args(&mut self) -> Result<(), BrokerError> {
        if self.hyperparameter_selection.is_some() {
            return Ok(());
        }
        let Some(config) = self.hyperparameter_config.clone() else {
            return Ok(());
        };
        let args = std::env::args().collect::<Vec<_>>();
        if config.is_sweep_requested(&args) {
            let runner = match self.mode {
                StrategyMode::Backtest => "run_backtest",
                StrategyMode::Live => "run_live",
            };
            return Err(BrokerError::DataFeedError(format!(
                "--hyper-sweep requires a fresh StrategyState for every seed; enumerate the sweep before calling {runner}"
            )));
        }
        if let Some(selection) = config
            .selection_from_process_args(&args)
            .map_err(BrokerError::DataFeedError)?
        {
            self.set_hyper_parameter_seed(&selection)
                .map_err(BrokerError::DataFeedError)?;
        }
        Ok(())
    }

    /// Override the next backtest result directory. Sweep runs use this so every seed
    /// retains the ordinary `metrics.json` and `backtest.db` artifact pair in one group.
    pub fn set_backtest_result_dir(&mut self, path: impl Into<PathBuf>) {
        self.backtest_result_dir = Some(path.into());
    }

    pub fn last_backtest_result_path(&self) -> Option<&Path> {
        self.last_backtest_result_path.as_deref()
    }

    /// Apply a saved hyperparameter seed to an ordinary run. The result is written to the
    /// normal `backtests/<run-id>` location, while the exact seed and values are persisted in
    /// its metrics for reproducibility.
    pub fn set_hyper_parameter_seed(
        &mut self,
        selection: &HyperParameterSelection,
    ) -> Result<(), String> {
        self.apply_hyper_parameter_selection(None, selection, false)
    }

    /// Apply a seed as one member of a full sweep. Unlike [`Self::set_hyper_parameter_seed`],
    /// this keeps the result in the sweep's `backtests/hyper/<sweep>/<seed>` folder.
    pub fn set_hyper_parameter_sweep_seed(
        &mut self,
        config: &HyperParameterConfig,
        selection: &HyperParameterSelection,
    ) -> Result<(), String> {
        self.apply_hyper_parameter_selection(Some(config), selection, true)
    }

    fn apply_hyper_parameter_selection(
        &mut self,
        config: Option<&HyperParameterConfig>,
        selection: &HyperParameterSelection,
        is_sweep_run: bool,
    ) -> Result<(), String> {
        selection.validate()?;
        if let Some(config) = config.filter(|_| is_sweep_run) {
            config.write_sweep_manifest(&self.artifact_root())?;
        }
        self.variables.insert(
            HYPERPARAMETER_VALUES_KEY.to_string(),
            selection.values.clone(),
        );
        self.variables.insert(
            HYPERPARAMETER_SEED_KEY.to_string(),
            Value::String(selection.seed.clone()),
        );
        self.hyperparameter_selection = Some(HyperparameterSelection {
            seed: selection.seed.clone(),
            values: selection.values.clone(),
            sweep_id: config
                .map(|config| config.sweep_id.clone())
                .unwrap_or_default(),
            strategy_fingerprint: config
                .map(|config| config.strategy_fingerprint.clone())
                .unwrap_or_default(),
            source_targets: config
                .map(|config| config.source_targets.clone())
                .unwrap_or(Value::Null),
            is_sweep_run,
        });
        self.backtest_result_dir = config.filter(|_| is_sweep_run).map(|config| {
            self.artifact_root()
                .join("backtests")
                .join("hyper")
                .join(&config.sweep_id)
                .join(&selection.seed)
        });
        Ok(())
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
    pub fn register_indicator(&mut self, indicator: Box<dyn Indicator>) {
        let name = indicator.name();
        if !self.indicators.contains_key(&name) {
            self.indicators.insert(name.clone(), indicator);
            match crate::core::indicators::execution_order(&self.indicators) {
                Ok(order) => self.indicator_order = order,
                Err(error) => {
                    self.indicators.remove(&name);
                    warn!("Cannot register indicator {}: {}", name, error);
                    return;
                }
            }
            // Registration is cold-path work. Reseed in dependency order so an indicator
            // attached to another indicator's numeric output sees the completed backfill.
            let keys: Vec<_> = self.history.keys().cloned().collect();
            for key in keys { self.apply_indicators_to_history(&key); }
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
        for name in &self.indicator_order {
            let Some(indicator) = self.indicators.get_mut(name) else { continue; };
            for output in indicator.output_columns() {
                if history_df.column(&output).is_err() {
                    let _ = history_df.with_column(Series::full_null(output.into(), history_df.height(), &DataType::Float64).into());
                }
            }
            if let Err(error) = indicator.seed_stream(symbol, history_df) {
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
        self.teardown_cleanup_requested = true;
        let mut report = TeardownCleanupReport::default();
        for id in self.insights.active_insight_ids() {
            let Some(insight) = self.insights.get(&id) else {
                continue;
            };
            match insight.state {
                InsightState::New => report.rejected_new += 1,
                InsightState::Executed => report.cancelled_executed += 1,
                InsightState::Filled => report.closed_filled += 1,
                _ => continue,
            }
            self.schedule_insight_cleanup(id);
        }
        // Counts describe requested work, not broker-confirmed closures.
        self.process_insight_work(256, std::time::Duration::from_millis(5));
        report
    }

    fn cancel_unsubmitted_insights_for_early_stop(&mut self) {
        for id in self.insights.active_insight_ids() {
            if self.insights.get(&id).is_some_and(|insight| {
                insight.state == InsightState::New
                    && !insight.submitted
                    && insight.order_id.is_none()
                    && insight.pending_mutations.is_empty()
                    && insight.entry_filled_quantity.unwrap_or(0.0) == 0.0
            }) {
                self.schedule_insight_cleanup(id);
            }
        }
        // Requests are IDs only; the ordinary bounded owner turns settle them.
        self.process_insight_work(256, std::time::Duration::from_millis(5));
    }

    fn begin_teardown(&mut self) {
        if let Some(driver) = &self.command_driver {
            driver.request_execution_stop();
        }
        self.teardown_cleanup_requested = false;
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
        let id = insight.insight_id;
        self.insights.add_insight(insight);
        if !allow_during_teardown {
            self.schedule_insight_children(id);
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
                info!("Starting teardown logic {}", logic.name());
                logic.run(self);
                info!("Finished teardown logic {}", logic.name());
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
        let mut keys = self
            .variables
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        keys.sort();
        keys.into_iter()
            .take(64)
            .filter_map(|key| {
                let entry = self.variables.get(&key)?;
                let mut snapshot = summarise_value(entry.value());
                snapshot.key = key;
                Some(snapshot)
            })
            .collect()
    }

    fn runtime_insight_counts(&self) -> HashMap<String, usize> {
        self.insights
            .get_state_count()
            .into_iter()
            .map(|(state, count)| (format!("{:?}", state), count))
            .collect()
    }

    fn runtime_active_insights(&self) -> Vec<RuntimeInsightSnapshot> {
        // Bound expensive detail/history cloning, not just the final displayed vector.
        let mut candidates = self
            .insights
            .active_insight_ids_unsorted()
            .into_iter()
            .filter_map(|id| self.insights.get(&id))
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            right
                .updated_at
                .cmp(&left.updated_at)
                .then(left.insight_id.cmp(&right.insight_id))
        });
        candidates
            .into_iter()
            .take(crate::core::tui::INSIGHT_PREVIEW_LIMIT)
            .map(|insight| RuntimeInsightSnapshot {
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
                order_class: InsightSnapshot::order_class_label(insight.order_class()).to_string(),
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
                    .enumerate()
                    .rev()
                    .take(25)
                    .map(
                        |(index, (at, state, message))| RuntimeInsightStateSnapshot {
                            at: *at,
                            state: InsightSnapshot::insight_state_label(state).to_string(),
                            message: message
                                .as_ref()
                                .map(|message| message.chars().take(512).collect()),
                            kind: insight
                                .state_history_kinds
                                .get(index)
                                .map(|kind| format!("{kind:?}"))
                                .unwrap_or_else(|| "Diagnostic".into()),
                        },
                    )
                    .collect(),
            })
            .collect()
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
                summary_lines: vec![
                    format!("Equity: {:.2}", snapshot.final_equity),
                    format!("Return: {:.2}%", snapshot.total_return_pct),
                    format!("Closed trades: {}", snapshot.total_trades),
                    format!("Win rate: {:.2}%", snapshot.win_rate),
                    format!("Max drawdown: {:.2}%", snapshot.max_drawdown),
                    format!("Profit factor: {:.2}", snapshot.profit_factor),
                    format!("Expectancy: {:.2}", snapshot.expectancy),
                    format!("Open positions: {}", snapshot.open_positions_count),
                    format!("Open insights: {}", snapshot.open_insights_count),
                    format!(
                        "Unrealized P&L: {:.2}",
                        snapshot.open_positions_unrealized_pnl
                    ),
                    format!(
                        "Winners / losers: {} / {}",
                        snapshot.winning_trades, snapshot.losing_trades
                    ),
                    format!(
                        "Metrics updated: {} UTC",
                        snapshot.updated_at.format("%H:%M:%S")
                    ),
                ],
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
        let mut provider_capabilities =
            crate::core::broker::traits::Broker::get_features(&self.broker)
                .iter()
                .map(|feature| format!("Broker: {feature:?}"))
                .collect::<Vec<_>>();
        provider_capabilities.extend(
            crate::core::broker::traits::DataFeed::get_features(&self.broker)
                .iter()
                .map(|feature| format!("Feed: {feature:?}")),
        );

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
            snapshot.provider_capabilities = provider_capabilities;
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
        if let Some(driver) = &self.command_driver {
            driver.request_execution_stop();
        }
        // A stop can arrive before the live/backtest listener subscribes.
        // `send` discards the value when there are no receivers; retain it so a
        // later startup/drain cannot accidentally continue entry processing.
        self.shutdown_tx.send_replace(true);
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
                // Materialize every declared output, including multi-output MACD/bands/profile.
                // Built-ins update bounded, per-history-key state once per incoming batch.
                for name in &self.indicator_order {
                    if let Some(indicator) = self.indicators.get(name) {
                        for output in indicator.output_columns() {
                            let null_series = Series::full_null(output.into(), appended_rows, &DataType::Float64);
                            let _ = bar_df.with_column(null_series.into());
                        }
                    }
                }
                let current_time = self.broker.get_current_time();
                for name in &self.indicator_order {
                    let Some(indicator) = self.indicators.get_mut(name) else { continue; };
                    if let Err(error) = indicator.update_stream(&history_key, history_df, &mut bar_df) {
                        warn!("Indicator {} update failed for {}: {}", name, history_key, error);
                    }
                    indicator.set_last_run_time(current_time);
                }

                if history_df.height() == 0 {
                    *history_df = bar_df;
                } else {
                    // Registration/dependency order can differ from physical backfill order.
                    // Align by name; Polars vstack requires identical physical column order.
                    let appended = bar_df.select(history_df.get_column_names().into_iter().map(|s| s.as_str()))
                        .and_then(|aligned| history_df.vstack_mut(&aligned).map(|_| ()));
                    if let Err(error) = appended {
                        warn!("Cannot append indicator history for {}: {}", history_key, error);
                        self.event_context_stack.pop();
                        return;
                    }
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
        self.run_insight_pipeline_selected(None);
    }

    /// A completion resumes only its selected owners. A market-data turn still
    /// supplies the full active set until feed-specific routing is installed.
    fn run_insight_pipeline_selected(&mut self, selected: Option<Vec<Uuid>>) {
        self.run_insight_pipeline_budgeted(selected, None);
    }

    /// A live sweep can yield only between complete insight turns. The returned
    /// cursor identifies the last consumed ID, including removed/inactive rows.
    fn run_insight_pipeline_budgeted(
        &mut self,
        selected: Option<Vec<Uuid>>,
        budget: Option<std::time::Duration>,
    ) -> (Option<Uuid>, usize) {
        let started = std::time::Instant::now();
        let mut last_processed = None;
        let mut processed = 0;
        let mut snapshot_membership_changed = false;
        // Move only the row being updated. Other insights and the identity index
        // remain available to DCA/risk pipes through StrategyContext.
        let mut pipeline = std::mem::take(&mut self.insight_pipeline);
        let mut touched_insight_ids = FxHashSet::default();

        let full_pass = selected.is_none();
        let insight_ids = selected.unwrap_or_else(|| self.insights.active_insight_ids_unsorted());
        if full_pass && !pipeline.deferred.is_empty() {
            let active_ids = insight_ids.iter().copied().collect::<FxHashSet<_>>();
            let stale = pipeline
                .deferred
                .iter()
                .filter(|(id, resume)| {
                    !self.insights.get(id).is_some_and(|insight| {
                        active_ids.contains(id) && insight.state == resume.state
                    })
                })
                .map(|(id, _)| *id)
                .collect::<Vec<_>>();
            for id in stale {
                pipeline.forget_continuation(&id);
                self.insights.mark_execution_dirty(id);
            }
            for pipes in pipeline.pipeline.values_mut() {
                for pipe in pipes {
                    pipe.retain_active_insights(&active_ids);
                }
            }
        }

        for id in insight_ids {
            if budget.is_some_and(|budget| processed > 0 && started.elapsed() >= budget)
                || (budget.is_some()
                    && (*self.shutdown_tx.borrow() || self.runtime_telemetry.shutdown_requested()))
            {
                break;
            }
            last_processed = Some(id);
            processed += 1;
            if !self
                .insights
                .get(&id)
                .is_some_and(|row| !row.state.is_inactive())
            {
                if pipeline.forget_continuation(&id) {
                    self.insights.mark_execution_dirty(id);
                }
                continue;
            }
            if pipeline.deferred.get(&id).is_some_and(|resume| {
                self.insights
                    .get(&id)
                    .is_some_and(|row| row.state != resume.state)
            }) {
                if pipeline.forget_continuation(&id) {
                    self.insights.mark_execution_dirty(id);
                }
            }
            let before_continuation = pipeline.execution_checkpoint(&id);
            let mut insight = match self.insights.take_for_update(&id) {
                Some(ins) => ins,
                None => continue,
            };

            'insight_turn: {
                let state = insight.state.clone();
                let expiration_managed_by_pipe =
                    pipeline.pipeline.get(&state).is_some_and(|pipes| {
                        pipes.iter().any(|pipe| {
                            pipe.controls_expiration() && pipe.matches_asset_and_alpha(&insight)
                        })
                    });
                let can_expire = insight.can_expire();
                let should_clear_first_on_fill =
                    state == InsightState::Filled && insight.first_on_fill();
                let mut before_fingerprint = if can_expire || should_clear_first_on_fill {
                    Some(insight.snapshot_fingerprint_hash())
                } else {
                    None
                };

                if can_expire && !expiration_managed_by_pipe && insight.has_expired(self) {
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
                    break 'insight_turn;
                }

                let previous_resume = pipeline.deferred.remove(&id);
                let resume_index = previous_resume.as_ref().map_or(0, |resume| resume.index);
                let mut deferred = None;
                if let Some(pipes) = pipeline.get_pipes_for_state(&state) {
                    let should_run_any_pipe = pipes.iter().any(|pipe| pipe.should_run(&insight));
                    if should_run_any_pipe && before_fingerprint.is_none() {
                        before_fingerprint = Some(insight.snapshot_fingerprint_hash());
                    }
                    let mut failed = false;
                    let mut rejection_reason: Option<String> = None;
                    for (pipe_index, pipe) in pipes.iter_mut().enumerate().skip(resume_index) {
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
                        if let Some(dependency) = result.deferred {
                            let snapshot_attempted = previous_resume
                                .as_ref()
                                .filter(|resume| resume.dependency == dependency)
                                .and_then(|resume| resume.snapshot_attempted);
                            deferred = Some(types::DeferredPipeResume {
                                state: state.clone(),
                                index: pipe_index,
                                dependency,
                                snapshot_attempted,
                            });
                            break;
                        }
                        if !result.success {
                            failed = true;
                            rejection_reason = Some(result.message.unwrap_or_else(|| {
                                format!("Insight pipe {} failed", result.pipe_name)
                            }));
                            break;
                        }
                        if !result.passed {
                            if insight.closing || insight.cancelling || insight.state.is_inactive()
                            {
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
                        let reason = rejection_reason
                            .as_deref()
                            .unwrap_or("Insight pipeline failed");
                        if insight.state == InsightState::Filled {
                            insight.state_log(Some(format!(
                                "Filled insight pipeline failed; position remains active: {reason}"
                            )));
                        } else if insight.has_pending_submit()
                            || insight.state == InsightState::Executed
                        {
                            insight.state_log(Some(format!(
                                "Insight pipeline failed; broker cleanup requested: {reason}"
                            )));
                            self.schedule_insight_cleanup(id);
                        } else if !insight.state.is_inactive() {
                            insight.order_rejected(reason);
                        }
                        if before_fingerprint
                            .is_some_and(|before| before != insight.snapshot_fingerprint_hash())
                        {
                            touched_insight_ids.insert(id);
                        }
                        break 'insight_turn;
                    }
                }

                if let Some(resume) = deferred {
                    pipeline.deferred.insert(id, resume);
                    if before_fingerprint
                        .is_some_and(|before| before != insight.snapshot_fingerprint_hash())
                    {
                        touched_insight_ids.insert(id);
                    }
                    // Initial configuration and completed side effects are not replayed.
                    // first_on_fill remains true until this continuation completes.
                    break 'insight_turn;
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
            self.insights
                .restore_after_update(insight, touched_insight_ids.contains(&id));
            if before_continuation != pipeline.execution_checkpoint(&id) {
                self.insights.mark_execution_dirty(id);
                // A later stage (including inside And/Or) may wait on the same
                // snapshot kind. It has not attempted this observation yet.
                if let Some(resume) = pipeline.deferred.get_mut(&id) {
                    resume.snapshot_attempted = None;
                }
                pipeline.snapshot_dependencies.remove(&id);
            }
            snapshot_membership_changed |= pipeline.snapshot_dependencies.update(
                id,
                pipeline.deferred.get(&id).map(|resume| &resume.dependency),
            );
            self.schedule_insight_children(id);
        }

        self.insight_pipeline = pipeline;
        if snapshot_membership_changed {
            if let Some(ready) = self.broker.snapshot_notifier() {
                ready.notify_one();
            }
        }
        // Completions reduce only after every borrowed row has been restored.
        // Budgeted live sweeps leave ready reducer work to the next owner turn,
        // so another five-millisecond drain cannot hide inside this budget.
        if budget.is_none() {
            self.on_trade_update();
        }
        (last_processed, processed)
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
    fn has_pending_broker_events(&self) -> bool {
        !self.pending_broker_events.is_empty()
            || self.broker.has_pending_trade_events()
            || self.command_driver.as_ref().is_some_and(|driver| {
                driver.has_ready_events_with_admission(
                    self.status.is_running() && !*self.shutdown_tx.borrow(),
                )
            })
            || self
                .insight_work
                .is_ready_for_execution(self.insight_execution_enabled())
            || ((self.execution_persistence.is_ready() || self.insights.has_execution_dirty())
                && self
                    .command_driver
                    .as_ref()
                    .is_some_and(|driver| !driver.checkpoint_in_flight()))
    }

    /// Bound reducer work between strategy scheduling points. Individual legacy
    /// mutation calls in child cleanup are migrated separately; this budget
    /// never interrupts one state transition half way through.
    fn on_trade_update(&mut self) {
        let started = std::time::Instant::now();
        let budget = std::time::Duration::from_millis(5);
        let commands = match self.reduce_ready_commands(256, budget) {
            Ok(count) => count,
            Err(error) => {
                error!("Broker command completion driver failed; stopping new dispatch: {error}");
                self.execution_error.get_or_insert(error);
                self.shutdown();
                return;
            }
        };
        let trades = if let Some(remaining) = budget.checked_sub(started.elapsed()) {
            self.on_trade_update_with_budget(256 - commands, remaining)
        } else {
            0
        };
        let work = if let Some(remaining) = budget.checked_sub(started.elapsed()) {
            self.process_insight_work(256 - commands - trades, remaining)
        } else {
            0
        };
        if let Some(remaining) = budget.checked_sub(started.elapsed()) {
            if let Err(error) = self.advance_execution_persistence_with_budget(
                256 - commands - trades - work,
                remaining,
            ) {
                error!("Execution checkpoint failed; stopping new dispatch: {error}");
                self.execution_error.get_or_insert(error);
                self.shutdown();
            }
        }
    }

    #[cfg(test)]
    async fn drain_ready_broker_events(&mut self) {
        // Fixtures drain all ready evidence between assertions. Yield between
        // bounded turns so stop/timers and transport tasks run.
        while self.has_pending_broker_events() {
            self.on_trade_update();
            tokio::task::yield_now().await;
        }
    }

    fn on_trade_update_with_budget(
        &mut self,
        max_events: usize,
        budget: std::time::Duration,
    ) -> usize {
        let started = std::time::Instant::now();
        let mut processed = 0;
        while processed < max_events {
            if processed != 0 && started.elapsed() >= budget {
                break;
            }
            if self.pending_broker_events.is_empty() {
                self.pending_broker_events.extend(
                    self.broker
                        .drain_trade_events_bounded((max_events - processed).min(256)),
                );
            }
            let Some((order, event)) = self.pending_broker_events.pop_front() else {
                break;
            };
            self.process_trade_update(order, event);
            processed += 1;
        }
        processed
    }

    fn process_trade_update(&mut self, order: Order, event: TradeUpdateEvent) {
        self.process_order_evidence(order, event, OrderEvidenceSource::TradeStream, None);
    }

    fn process_order_evidence(
        &mut self,
        order: Order,
        event: TradeUpdateEvent,
        source: OrderEvidenceSource,
        response_insight_id: Option<Uuid>,
    ) -> bool {
        debug!(
            "Trade event: event={:?} {}",
            event,
            Self::order_log_summary(&order)
        );
        let mut parent_to_reconcile = None;
        let mut changed_id = None;
        if !self.universe.contains_key(&order.asset.symbol) {
            return false;
        }
        // A submit response may omit the client ID. Its receipt is the only
        // authoritative route; never guess among other New insights by symbol.
        let mut insight_ids = if source == OrderEvidenceSource::SubmitResponse {
            let Some(id) = response_insight_id else {
                return false;
            };
            vec![id]
        } else {
            self.insights.candidate_insight_ids_for_trade_event(&order)
        };
        if insight_ids.is_empty() && source == OrderEvidenceSource::TradeStream {
            insight_ids = self.insights.ids();
        }
        for id in insight_ids {
            let Some(insight) = self.insights.get_mut(&id) else {
                continue;
            };
            let reduction = Self::reduce_broker_order(insight, &order, &event, source);
            if reduction == OrderReduction::Ignored {
                if source == OrderEvidenceSource::SubmitResponse
                    && insight.symbol == order.asset.symbol
                    && insight.side == order.side
                    && (insight.order_id.as_deref() == Some(order.order_id.as_str())
                        || matches!(insight.state, InsightState::Filled | InsightState::Closed))
                    && !order.order_id.is_empty()
                    && order
                        .insight_id
                        .as_deref()
                        .is_none_or(|value| value == id.to_string())
                {
                    // The stream already applied this entry; the late response
                    // is consumed without resetting first-fill or appending history.
                    return true;
                }
                continue;
            }
            if reduction == OrderReduction::AccountingUpdated {
                // Persist only the corrected evidence. Do not reopen/reclose a
                // trade, wake pipes, retire children or dispatch broker work.
                self.insights.refresh_runtime_tracking(&id);
                return true;
            }
            changed_id = Some(id);
            match reduction {
                OrderReduction::EntryFilled => {
                    parent_to_reconcile = insight.parent_id;
                }
                OrderReduction::Terminal => parent_to_reconcile = Some(insight.insight_id),
                OrderReduction::LateEntryFilled => {
                    // A cancelled/rejected entry filling late is exposure to
                    // reconcile, not permission to launch its dormant DCA stack.
                    parent_to_reconcile = insight.parent_id;
                }
                _ => {}
            }
            break;
        }
        let Some(changed_id) = changed_id else {
            return false;
        };
        self.insights.refresh_runtime_tracking(&changed_id);
        if self
            .insights
            .get(&changed_id)
            .is_some_and(|insight| insight.state.is_inactive())
        {
            self.reduce_terminal_command_receipts(changed_id);
            self.insight_work.retire_children(changed_id);
        }

        self.insight_work.wake(changed_id);
        if self.status == StrategyStatus::Stopping {
            self.schedule_insight_cleanup(changed_id);
        } else {
            self.schedule_insight_children(changed_id);
        }
        if let Some(parent_id) = parent_to_reconcile.filter(|parent_id| {
            self.insights
                .get(parent_id)
                .is_some_and(|parent| parent.state.is_inactive())
        }) {
            self.schedule_descendant_cleanup(parent_id);
        }
        true
    }
}

// ─────────────────────── StrategyContext impl ───────────────────────

impl<S, E, D> StrategyContext for StrategyState<S, E, D>
where
    S: Strategy,
    E: Broker + OrderManagementProvider,
    D: DataFeed + DataProvider,
{
    fn enqueue_broker_command(
        &mut self,
        insight_id: Uuid,
        action: crate::core::broker::commands::BrokerCommandAction,
        group: Option<crate::core::broker::commands::BrokerOperationGroup>,
    ) -> Result<
        crate::core::broker::commands::BrokerCommandReceipt,
        crate::core::broker::commands::BrokerCommandError,
    > {
        let constraints = match &action {
            crate::core::broker::commands::BrokerCommandAction::Submit { request } => {
                crate::core::broker::commands::BrokerDispatchConstraints {
                    max_total_symbol_quantity: self
                        .universe
                        .get(&request.symbol)
                        .and_then(|asset| asset.max_total_size),
                }
            }
            _ => Default::default(),
        };
        let receipt = self
            .command_driver
            .as_mut()
            .ok_or(crate::core::broker::commands::BrokerCommandError::Closed)?
            .enqueue_with_constraints(&insight_id.to_string(), action, group, constraints)?;
        self.insights.execution_pin(insight_id);
        Ok(receipt)
    }

    fn broker_command_progress(
        &self,
        receipt: &crate::core::broker::commands::BrokerCommandReceipt,
    ) -> Option<command_driver::BrokerCommandProgress> {
        self.command_driver.as_ref()?.progress(receipt)
    }

    fn broker_command_reduced(
        &mut self,
        receipt: &crate::core::broker::commands::BrokerCommandReceipt,
    ) {
        if let Some(driver) = self.command_driver.as_mut() {
            if let Err(error) = driver.mark_reduced(receipt) {
                error!("Cannot release broker command reducer ownership: {error}");
                self.shutdown();
            }
        }
    }

    fn cancel_pending_entry(
        &mut self,
        receipt: &crate::core::broker::commands::BrokerCommandReceipt,
    ) -> Result<(), crate::core::broker::commands::BrokerCommandError> {
        self.command_driver
            .as_ref()
            .ok_or(crate::core::broker::commands::BrokerCommandError::Closed)?
            .cancel_pending_entry(receipt)
    }

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

    fn request_insight_cancellation(
        &mut self,
        id: Uuid,
    ) -> Result<bool, crate::core::broker::commands::BrokerCommandError> {
        let Some(row) = self.insights.get(&id) else {
            return Ok(false);
        };
        if !matches!(row.state, InsightState::New | InsightState::Executed) {
            return Ok(false);
        }
        // Persist the entry gate before queuing cleanup so neither root nor child
        // submission can race the next pipeline turn or restart recovery.
        if let Some(mut row) = self.insights.take_for_update(&id) {
            row.suppress_child_entries = true;
            self.insights.restore_after_update(row, true);
        }
        self.schedule_insight_cleanup(id);
        self.schedule_descendant_cleanup(id);
        Ok(true)
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

    fn broker_features(&self) -> &'static [BrokerFeature] {
        self.broker.execution.get_features()
    }

    fn datafeed_features(&self) -> &'static [DataFeedFeature] {
        // Backtests expose replay capabilities supplied by UnifiedBroker, not
        // just the underlying provider (e.g. a historical-only CSV file).
        DataFeed::get_features(&self.broker)
    }

    fn tools(&self) -> Box<dyn TradingTools + '_> {
        Box::new(
            StrategyTools::new(&self.universe, &self.insights, &self.broker).with_context(self),
        )
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
        self.broker
            .account_snapshot()
            .map(|account| (*account).clone())
    }

    fn get_positions(&self) -> Result<Vec<crate::core::broker::types::Position>, BrokerError> {
        self.broker.get_positions_sync()
    }

    fn cached_account(&self) -> Option<crate::core::broker::types::Account> {
        self.broker
            .account_snapshot()
            .ok()
            .map(|account| (*account).clone())
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
        insight.bind_context(self.broker.insight_context());
    }

    fn latest_quote(&self, symbol: &str) -> Result<crate::core::broker::types::Quote, BrokerError> {
        if let Some(result) = self.execution_quotes.read(symbol, &self.broker) {
            return result;
        }
        self.broker
            .quote_snapshot(symbol)
            .map(|quote| (*quote).clone())
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

    fn shutdown(&mut self) {
        StrategyState::shutdown(self);
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
    async fn finalize_backtest_results(&mut self, results: &BacktestResults) -> Option<String> {
        info!("═══════════════════ Backtest Results ═══════════════════");
        let terminal_output_suspended = crate::core::tui::terminal_output_suspended();
        if !terminal_output_suspended {
            results.print_metrics();
        }
        info!("Insights generated: {}", self.insights.len());
        info!("Insights: {:#?}", self.insights.get_state_count());

        let run_id = Uuid::new_v4().to_string();
        let out_dir = self
            .backtest_result_dir
            .clone()
            .unwrap_or_else(|| self.artifact_root().join("backtests").join(&run_id));

        let Some(backtest_state) = self.broker.backtest_state.clone() else {
            warn!("Backtest completed but no backtest state was available for result persistence");
            return None;
        };

        let state = backtest_state.read();
        let persistence_started = std::time::Instant::now();
        info!("Persisting backtest artifacts to {}", out_dir.display());
        if let Err(error) = results.save_to_disk_async(&out_dir, &*state).await {
            error!("Failed to save results to disk: {}", error);
            if !terminal_output_suspended {
                eprintln!("Failed to save results to disk: {}", error);
            }
            return None;
        }
        drop(state);
        info!(
            "Backtest artifacts persisted in {}ms",
            persistence_started.elapsed().as_millis()
        );

        if let Some(selection) = self.hyperparameter_selection.as_ref() {
            if let Err(error) = append_hyperparameter_metrics(&out_dir, selection, &self.timeframe)
            {
                warn!(
                    "Backtest saved but hyperparameter metadata could not be persisted: {}",
                    error
                );
            }
        }

        self.last_backtest_result_path = Some(out_dir.clone());
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
        ensure_rustls_crypto_provider();
        self.apply_hyper_parameters_from_process_args()?;
        // Take strategy out to avoid split-borrow (self.strategy vs self as ctx)
        let mut strategy = self
            .strategy
            .take()
            .expect("strategy must be Some before run_backtest");

        self.runtime_telemetry
            .start_tui(TuiConfig::from_process_args());
        self.publish_runtime_snapshot("not configured", None);

        let mut lifecycle_started = false;
        let execution_result: Result<(), BrokerError> = async {
            // ── 0. Connect broker (execution + data feed) ──
            info!(
                "Starting backtest strategy={} timeframe={:?} start={} end={}",
                self.name, time_frame, start, end
            );
            self.timeframe = time_frame.clone();
            self.history_seed_anchor = Some(start);
            if let Some(state) = &self.broker.backtest_state {
                state.write().set_backtest_window(start, end);
            }
            if let Err(error) = self.broker.connect().await {
                error!("Backtest failed while connecting broker: {}", error);
                self.runtime_telemetry.stop_tui();
                return Err(error);
            }
            self.status = StrategyStatus::Running;
            self.publish_runtime_snapshot("not configured", None);

            // ── 1. Strategy lifecycle: on_start ──
            lifecycle_started = true;
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
            // Attaching history requires exclusive access to PaperBroker's Arc.
            // Only share it with the command worker after the simulated clock is
            // installed. Every backtest gets a private journal, never a live run's.
            let account = self.broker.get_account().await?;
            let journal_path = self.artifact_root().join(".aqs/execution/backtests")
                .join(Uuid::new_v4().to_string()).join("commands.db");
            self.start_command_driver(
                &journal_path, &self.strategy_id.to_string(), &account.account_id, Default::default(),
            ).await.map_err(|error| BrokerError::OrderError(format!("backtest execution startup failed: {error}")))?;
            self.drain_backtest_execution().await?;
            self.publish_runtime_event("info", "Backtest started", "not configured");

            // ── 7. Main backtest loop ──
            let mut last_runtime_publish = std::time::Instant::now();
            loop {
                if self.runtime_telemetry.shutdown_requested() || *self.shutdown_tx.borrow() {
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
                        self.drain_backtest_execution().await?;
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
                        self.drain_backtest_execution().await?;
                        self.sync_backtest_insight_snapshots();
                        if last_runtime_publish.elapsed() >= std::time::Duration::from_millis(500) {
                            self.publish_runtime_snapshot("not configured", None);
                            last_runtime_publish = std::time::Instant::now();
                        }
                    }
                }
            }
            Ok(())
        }
        .await;

        // ── 8. Teardown ──
        // Keep one cleanup exit for success, stop and all fallible startup/loop
        // paths. Never leave the strategy taken or a journal worker detached.
        let mut teardown_result: Result<(), BrokerError> = async {
            if !lifecycle_started {
                return Ok(());
            }
            debug!("Invoking strategy on_teardown");
            self.begin_teardown();
            self.run_on_teardown_logic(LifecycleTiming::BeforeGenerated);
            strategy.on_teardown(self);
            self.run_on_teardown_logic(LifecycleTiming::AfterGenerated);
            self.drain_backtest_execution().await?;
            loop {
                let final_close_requests =
                    match self.broker.flush_backtest_close_queue_at_last_bars() {
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
                self.drain_backtest_execution().await?;
                self.sync_backtest_insight_snapshots();
            }
            self.sync_backtest_insight_snapshots();
            Ok(())
        }
        .await;

        if lifecycle_started && teardown_result.is_ok() {
            if let Some(driver) = &self.command_driver {
                match driver.complete_execution_stop().await {
                    Ok(false) if self.teardown_cleanup_requested => {
                        teardown_result = Err(BrokerError::OrderError(
                            "backtest teardown still owns unresolved execution work".into(),
                        ));
                    }
                    Err(error) => {
                        teardown_result = Err(BrokerError::OrderError(format!(
                            "backtest execution stop completion was not durable: {error}"
                        )));
                    }
                    // Backtests may intentionally retain open exposure when
                    // their strategy does not request liquidation. Keep that
                    // journal Stopping instead of claiming broker settlement.
                    _ => {}
                }
            }
        }

        // The worker can still own an in-flight mutation. Release it before
        // disconnecting the transports it uses, including after a failed drain.
        let worker_result = match self.command_driver.take() {
            Some(driver) => driver.stop().await.map(drop).map_err(|error| {
                BrokerError::OrderError(format!("backtest execution shutdown failed: {error}"))
            }),
            None => Ok(()),
        };
        let disconnect_result = self.broker.disconnect().await.map(|_| ());

        // Put strategy back
        self.strategy = Some(strategy);
        self.history_seed_anchor = None;
        for result in [
            &execution_result,
            &teardown_result,
            &worker_result,
            &disconnect_result,
        ] {
            if let Err(error) = result {
                error!("Backtest did not finish cleanly: {error}");
            }
        }
        if let Err(error) = execution_result
            .and(teardown_result)
            .and(worker_result)
            .and(disconnect_result)
        {
            self.status = StrategyStatus::Stopped;
            self.publish_runtime_event(
                "error",
                format!("Backtest failed: {error}"),
                "not configured",
            );
            self.runtime_telemetry.stop_tui();
            return Err(error);
        }

        // ── 9. Results ──
        let results = self.broker.get_results();
        let saved_result_path = self.finalize_backtest_results(&results).await;
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

fn append_hyperparameter_metrics(
    result_dir: &Path,
    selection: &HyperparameterSelection,
    timeframe: &TimeFrame,
) -> Result<(), String> {
    let metrics_path = result_dir.join("metrics.json");
    let content = std::fs::read_to_string(&metrics_path).map_err(|error| error.to_string())?;
    let mut metrics: Value = serde_json::from_str(&content).map_err(|error| error.to_string())?;
    let object = metrics
        .as_object_mut()
        .ok_or_else(|| "Backtest metrics must be a JSON object".to_string())?;
    let mut hyperparameter = serde_json::json!({
        "seed": selection.seed,
        "values": selection.values,
        "source_targets": selection.source_targets,
        "strategy_fingerprint": selection.strategy_fingerprint,
        "datafeed": { "type": "configured", "timeframe": timeframe.to_string() },
    });
    if selection.is_sweep_run {
        hyperparameter
            .as_object_mut()
            .expect("hyperparameter metadata is an object")
            .insert(
                "sweep_id".to_string(),
                Value::String(selection.sweep_id.clone()),
            );
    }
    object.insert("hyperparameter".to_string(), hyperparameter);
    std::fs::write(
        metrics_path,
        serde_json::to_string_pretty(&metrics).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod hyperparameter_metrics_tests {
    use super::*;

    #[test]
    fn ordinary_seed_metadata_has_no_sweep_id() {
        let directory =
            std::env::temp_dir().join(format!("aqe_hyperparameter_metrics_{}", Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join("metrics.json"), "{}").unwrap();
        let selection = HyperparameterSelection {
            seed: "seed-id".into(),
            values: serde_json::json!({ "period": 14 }),
            sweep_id: "sweep-id".into(),
            strategy_fingerprint: "fingerprint".into(),
            source_targets: serde_json::json!([]),
            is_sweep_run: false,
        };

        append_hyperparameter_metrics(
            &directory,
            &selection,
            &TimeFrame::new(1, crate::core::utils::timeframe::TimeFrameUnit::Minute),
        )
        .unwrap();

        let metrics: Value =
            serde_json::from_str(&std::fs::read_to_string(directory.join("metrics.json")).unwrap())
                .unwrap();
        assert_eq!(metrics["hyperparameter"]["seed"], "seed-id");
        assert_eq!(metrics["hyperparameter"]["values"]["period"], 14);
        assert!(metrics["hyperparameter"].get("sweep_id").is_none());
    }
}
