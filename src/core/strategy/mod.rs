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
use crate::core::universe::WrappedUniverseModel;
use crate::core::utils::tools::{StrategyTools, TradingTools};
use dashmap::DashMap;
use polars::prelude::*;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
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
    let result = env_logger::Builder::from_env(env)
        .format_timestamp_millis()
        .try_init();
    if result.is_ok() {
        info!("Logger initialised with level {}", log_level);
    }
    result
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
                insight.add_take_profit_levels(vec![tp]);
            }
            if let Some(sl) = legs.stop_loss.as_ref().and_then(|leg| leg.limit_price) {
                insight.add_stop_loss_levels(vec![sl]);
            }
            if let Some(trailing_gap) = legs.trailing_stop.as_ref().and_then(|leg| leg.trail_price)
            {
                insight.set_trailing_stop_price(Some(trailing_gap));
            }
            insight.legs = legs.clone();
        }
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
            insights: Default::default(),
            history: Default::default(),
            universe: Default::default(),
            alpha_models: Vec::new(),
            on_start_logic: Vec::new(),
            on_init_logic: Vec::new(),
            on_teardown_logic: Vec::new(),
            universe_models: Vec::new(),
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
            insight_pipeline: Default::default(),
            timeframe,
            shutdown_tx: tokio::sync::watch::channel(false).0,
        }
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
                        eprintln!(
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

    fn filter_history_before_anchor(
        df: DataFrame,
        anchor: chrono::DateTime<chrono::Utc>,
    ) -> Result<DataFrame, BrokerError> {
        if df.height() == 0 {
            return Ok(df);
        }

        let timestamp_col = match df.column("timestamp") {
            Ok(column) => column,
            Err(_) => return Ok(df),
        };
        let timestamp_i64 = timestamp_col.cast(&DataType::Int64).map_err(|error| {
            BrokerError::DataFeedError(format!("Failed to cast timestamp column: {}", error))
        })?;
        let timestamps = timestamp_i64.i64().map_err(|error| {
            BrokerError::DataFeedError(format!("Timestamp column is not Int64: {}", error))
        })?;
        let anchor_ms = anchor.timestamp_millis();
        let mask: BooleanChunked = timestamps
            .into_iter()
            .map(|value| value.map(|millis| millis < anchor_ms))
            .collect();
        df.filter(&mask)
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

        let anchor = self
            .history_seed_anchor
            .unwrap_or_else(|| self.broker.get_current_time());
        let start = self
            .timeframe
            .add_time_increment(anchor, -(warmup_bars as i64))
            .map_err(|error| {
                BrokerError::DataFeedError(format!(
                    "Failed to calculate warm-up start for {}: {:?}",
                    symbol, error
                ))
            })?;

        let mut seeded = futures::executor::block_on(self.broker.get_history(
            symbol,
            start,
            anchor,
            self.timeframe.clone(),
        ))?;
        seeded = Self::filter_history_before_anchor(seeded, anchor)?;

        if seeded.height() > self.max_history_rows {
            seeded = seeded.tail(Some(self.max_history_rows));
        }

        let rows = seeded.height();
        self.history.insert(symbol.to_string(), seeded);
        self.apply_indicators_to_history(symbol);
        if rows >= warmup_bars as usize {
            self.warm_up_progress.remove(symbol);
        } else {
            self.warm_up_progress
                .insert(symbol.to_string(), rows as i32);
        }
        Ok(rows)
    }

    fn cleanup_active_insights_for_teardown_internal(&mut self) -> TeardownCleanupReport {
        let mut report = TeardownCleanupReport::default();
        let insight_ids: Vec<Uuid> = self.insights.keys().copied().collect();

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
                            insight.order_canceled("Teardown cleanup cancelled executed insight");
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
        self.insights.add_insight(insight);
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

    pub fn max_history_rows(&self) -> usize {
        self.max_history_rows
    }

    pub fn update_max_history_rows(&mut self, rows: usize) {
        self.max_history_rows = rows.max(100);
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

    /// Process a bar callback: append to HISTORY, warm-up check, on_bar → generate insights.
    /// Mirrors Python's `_on_bar()`.
    fn _on_bar(&mut self, strategy: &mut S, symbol: &str, bar: &BarData) {
        match bar {
            BarData::Bars(bars) => {
                if let Some(last_bar) = bars.last() {
                    debug!(
                        "on_bar symbol={} o={:.4} h={:.4} l={:.4} c={:.4} v={:.0}",
                        symbol,
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
                debug!("on_bar symbol={} frame_shape={:?}", symbol, frame.shape());
            }
        }
        let max_history_rows = self.max_history_rows();
        // Append to history DataFrame
        if let Some(history_df) = self.history.get_mut(symbol) {
            let bar_df_res = self.broker.data.format_on_bar(bar.clone());

            if let Ok(mut bar_df) = bar_df_res {
                let appended_rows = bar_df.height();
                // Pre-fill bar_df with Null values for indicators to match schema
                for (name, _ind) in self.indicators.iter() {
                    let null_series =
                        Series::full_null(name.into(), bar_df.height(), &DataType::Float64);
                    let _ = bar_df.with_column(null_series);
                }

                if history_df.height() == 0 {
                    *history_df = bar_df; // Just taking the newly formed df
                } else {
                    let _ = history_df.vstack_mut(&bar_df);
                }
                if history_df.height() > max_history_rows {
                    *history_df = history_df.tail(Some(max_history_rows));
                }

                let current_time = self.broker.get_current_time();
                for (_name, ind) in self.indicators.iter_mut() {
                    let ws = ind.window_size();
                    let h = history_df.height();
                    if h >= ws {
                        let tail = history_df.tail(Some(ws));
                        if let Ok(res) = ind.run_bar(&tail) {
                            for (col_name, val) in res {
                                if let Ok(s) = history_df.column(&col_name) {
                                    if let Ok(ca) = s.f64() {
                                        let mut vec: Vec<Option<f64>> = ca.into_iter().collect();
                                        if !vec.is_empty() {
                                            let last_idx = vec.len().saturating_sub(appended_rows);
                                            for i in last_idx..vec.len() {
                                                vec[i] = Some(val);
                                            }
                                            let new_s = Series::new(col_name.into(), &vec);
                                            let _ = history_df.with_column(new_s);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    ind.set_last_run_time(current_time);
                }
            }
        }

        // Warm-up check: skip strategy callbacks until enough bars accumulated
        if self.warm_up_bars > 0 {
            if let Some(history_df) = self.history.get(symbol) {
                let loaded = history_df.height() as i32;
                if loaded < self.warm_up_bars {
                    let should_log =
                        self.warm_up_progress.get(symbol).copied().unwrap_or(-1) != loaded;
                    self.warm_up_progress.insert(symbol.to_string(), loaded);
                    if should_log {
                        info!(
                            "[warm up] {} {}/{} candles",
                            symbol, loaded, self.warm_up_bars
                        );
                    }
                    debug!(
                        "Warm-up active for {}: {}/{} bars loaded, skipping strategy callbacks",
                        symbol, loaded, self.warm_up_bars
                    );
                    return;
                }
                if self.warm_up_progress.remove(symbol).is_some() {
                    info!(
                        "[warm up] {} complete ({}/{})",
                        symbol, loaded, self.warm_up_bars
                    );
                }
            }
        }

        // Call strategy callbacks
        debug!(
            "Warm-up complete for {}, invoking strategy callbacks",
            symbol
        );
        strategy.on_bar(self, symbol, bar);

        // Generate insights from alpha models then strategy
        self._generate_insights(strategy, symbol);
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
        let mut touched_insight_ids = HashSet::new();

        let insight_ids = insights.active_insight_ids();

        for id in insight_ids {
            let insight = match insights.get_mut(&id) {
                Some(ins) => ins,
                None => continue,
            };
            let before_fingerprint = insight.snapshot_fingerprint_hash();

            if insight.has_expired(self) {
                debug!(
                    "Skipping expired insight {} in state {:?}",
                    insight.insight_id(),
                    insight.state()
                );
                if before_fingerprint != insight.snapshot_fingerprint_hash() {
                    touched_insight_ids.insert(id);
                }
                continue;
            }

            let state = insight.state.clone();
            let should_clear_first_on_fill =
                state == InsightState::Filled && insight.first_on_fill();

            if let Some(pipes) = pipeline.get_pipes_for_state(&state) {
                let mut passed = true;
                let mut rejection_reason: Option<String> = None;
                for pipe in pipes.iter_mut() {
                    if !pipe.should_run(insight) {
                        continue;
                    }
                    debug!(
                        "Running insight pipe {} for insight {} in state {:?}",
                        pipe.name(),
                        insight.insight_id(),
                        state
                    );
                    let result = pipe.run(self, insight);
                    debug!(
                        "Insight pipe {} result for insight {}: success={} passed={} message={:?}",
                        pipe.name(),
                        insight.insight_id(),
                        result.success,
                        result.passed,
                        result.message
                    );
                    if !result.success {
                        passed = false;
                        rejection_reason = Some(result.message.unwrap_or_else(|| {
                            format!("Insight pipe {} failed", result.pipe_name)
                        }));
                        break;
                    }
                    if !result.passed {
                        passed = false;
                        rejection_reason = Some(result.message.unwrap_or_else(|| {
                            format!("Insight pipe {} did not pass", result.pipe_name)
                        }));
                        break;
                    }
                }
                if !passed {
                    if !insight.state.is_inactive() {
                        insight.order_rejected(
                            rejection_reason
                                .as_deref()
                                .unwrap_or("Insight pipeline failed"),
                        );
                    }
                    if before_fingerprint != insight.snapshot_fingerprint_hash() {
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
            if before_fingerprint != insight.snapshot_fingerprint_hash() {
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

        let dirty_insight_ids = self.insights.dirty_insight_ids();
        if dirty_insight_ids.is_empty() {
            return;
        }

        let mut state = backtest_state.write();
        let strategy_id = self.strategy_id.to_string();
        let mut synced_ids = Vec::with_capacity(dirty_insight_ids.len());
        for insight_id in dirty_insight_ids {
            let Some(insight) = self.insights.get(&insight_id) else {
                continue;
            };
            state.record_insight_snapshot(InsightSnapshot::from_insight(insight, &strategy_id));
            synced_ids.push(insight_id);
        }
        drop(state);

        for insight_id in synced_ids {
            self.insights.remove_dirty(&insight_id);
        }
    }

    // ─────────────────── Trade Event Processing ───────────────────

    /// Process trade events from the broker. Mirrors Python's `_on_trade_update`.
    /// Handles all event types: New, PendingNew, Filled, PartialFilled,
    /// Canceled, Rejected, Closed, Replaced.
    /// Works with any broker implementing `OrderManagementProvider`.
    fn on_trade_update(&mut self) {
        let events = self.broker.drain_trade_events();
        let mut children_to_submit = Vec::new();
        let mut parents_to_close = Vec::new();
        let mut touched_insight_ids = HashSet::new();

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
                insight_ids = self.insights.keys().cloned().collect();
            }
            for id in insight_ids {
                let insight = match self.insights.get_mut(&id) {
                    Some(i) => i,
                    None => continue,
                };
                if insight.symbol != order.asset.symbol {
                    continue;
                }
                let matched = Self::trade_event_matches_insight(insight, &order);

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
                            let price = order.filled_price.or(order.limit_price).unwrap_or(0.0);
                            insight.position_filled(price, order.filled_qty, &order.order_id);
                            Self::sync_broker_managed_levels(insight, &order);
                            touched_insight_ids.insert(id);
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
                            let price = order.filled_price.or(order.limit_price).unwrap_or(0.0);
                            insight.partial_filled(order.filled_qty, price, &order.order_id);
                            Self::sync_broker_managed_levels(insight, &order);
                            touched_insight_ids.insert(id);
                            break;
                        }
                    }
                    // EXECUTED → FILLED
                    (InsightState::Executed, TradeUpdateEvent::Filled) => {
                        if matched {
                            let price = order.filled_price.or(order.limit_price).unwrap_or(0.0);
                            insight.position_filled(price, order.filled_qty, &order.order_id);
                            Self::sync_broker_managed_levels(insight, &order);
                            touched_insight_ids.insert(id);

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
                            let price = order.filled_price.or(order.limit_price).unwrap_or(0.0);
                            insight.partial_filled(order.filled_qty, price, &order.order_id);
                            Self::sync_broker_managed_levels(insight, &order);
                            touched_insight_ids.insert(id);
                            break;
                        }
                    }
                    // FILLED partial close / scale-out
                    (InsightState::Filled, TradeUpdateEvent::PartialFilled) => {
                        if matched {
                            let price = Self::close_price_from_order(&order);
                            insight.partial_closed(order.filled_qty, price, &order.order_id);
                            touched_insight_ids.insert(id);
                            // We dont need to reset this when closing the insight - its kind of a
                            // design choice
                            // Self::sync_broker_managed_levels(insight, &order);
                            break;
                        }
                    }
                    // EXECUTED → CANCELED
                    (InsightState::Executed | InsightState::New, TradeUpdateEvent::Canceled) => {
                        if matched {
                            Self::sync_broker_managed_levels(insight, &order);
                            insight.order_canceled("Order canceled");
                            touched_insight_ids.insert(id);
                            // Cancel any open children
                            parents_to_close.push(insight.insight_id);
                            break;
                        }
                    }
                    // EXECUTED → REJECTED
                    (InsightState::Executed | InsightState::New, TradeUpdateEvent::Rejected) => {
                        if matched {
                            Self::sync_broker_managed_levels(insight, &order);
                            insight.order_rejected(
                                order
                                    .rejection_reason
                                    .as_deref()
                                    .unwrap_or("Order rejected"),
                            );
                            touched_insight_ids.insert(id);
                            // Cancel any open children
                            parents_to_close.push(insight.insight_id);
                            break;
                        }
                    }
                    // EXECUTED → REPLACED (order modified)
                    (InsightState::Executed, TradeUpdateEvent::Replaced) => {
                        if matched {
                            if order.limit_price != insight.limit_price {
                                insight.limit_price = order.limit_price;
                            }
                            if insight.quantity != Some(order.qty) {
                                insight.quantity = Some(order.qty);
                            }
                            Self::sync_broker_managed_levels(insight, &order);
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
                            let price = Self::close_price_from_order(&order);
                            // again we dont need to sync the TP/SL again on close because we will
                            // have the close price.
                            // Self::sync_broker_managed_levels(insight, &order);
                            insight.position_closed(
                                price,
                                &order.order_id,
                                order.filled_qty,
                                order.realized_pnl,
                            );
                            touched_insight_ids.insert(id);

                            // Close/Cancel any open children
                            parents_to_close.push(insight.insight_id);
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

        // Process orphaned children cleanup
        if !parents_to_close.is_empty() {
            let mut children_to_process = Vec::new();
            let parents_to_close = parents_to_close.into_iter().collect::<HashSet<_>>();

            // First pass: identify which children need which action (needs immutable/short mutable borrow)
            let insight_ids: Vec<Uuid> = self.insights.keys().cloned().collect();
            for id in &insight_ids {
                if let Some(child) = self.insights.get(id) {
                    if let Some(parent_id) = child.parent_id {
                        if parents_to_close.contains(&parent_id) {
                            children_to_process.push(*id);
                        }
                    }
                }
            }

            // Second pass: apply closures (requires mutable self)
            for id in children_to_process {
                if let Some(mut child) = self.insights.remove_insight(&id) {
                    match child.state {
                        InsightState::Filled => {
                            child.close(self);
                            self.add_insight(child);
                        }
                        InsightState::New | InsightState::Executed => {
                            child.cancel(self);
                            self.add_insight(child);
                        }
                        _ => {
                            self.add_insight(child);
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
        let result =
            futures::executor::block_on(self.broker.execution.submit_order(insight.clone()));
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
                        let price = order.filled_price.or(order.limit_price).unwrap_or(0.0);
                        insight.position_filled(price, order.filled_qty, &order.order_id);
                        Self::sync_broker_managed_levels(insight, &order);
                    }
                    TradeUpdateEvent::PartialFilled => {
                        insight.order_accepted(&order.order_id);
                        let price = order.filled_price.or(order.limit_price).unwrap_or(0.0);
                        insight.partial_filled(order.filled_qty, price, &order.order_id);
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
                    TradeUpdateEvent::Canceled | TradeUpdateEvent::Expired => {
                        insight.order_canceled("Order canceled before acknowledgement");
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
        self.max_history_rows
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
        futures::executor::block_on(self.broker.get_account())
    }

    fn current_time(&self) -> chrono::DateTime<chrono::Utc> {
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
        futures::executor::block_on(self.broker.get_latest_quote(symbol))
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
    fn finalize_backtest_results(&self, results: &BacktestResults) {
        info!("═══════════════════ Backtest Results ═══════════════════");
        results.print_metrics();
        info!("Insights generated: {}", self.insights.len());
        info!("Insights: {:#?}", self.insights.get_state_count());

        let run_id = Uuid::new_v4().to_string();
        let out_dir = std::env::current_dir()
            .unwrap_or_default()
            .join("backtests")
            .join(&run_id);

        let Some(backtest_state) = self.broker.backtest_state.as_ref() else {
            warn!("Backtest completed but no backtest state was available for result persistence");
            return;
        };

        let state = backtest_state.read();
        if let Err(error) = results.save_to_disk(&out_dir, &*state) {
            eprintln!("Failed to save results to disk: {}", error);
            return;
        }

        if let Ok(abs_path) = std::fs::canonicalize(&out_dir) {
            println!("RESULTS_SAVED_TO: {}", abs_path.display());
        } else {
            println!("RESULTS_SAVED_TO: {}", out_dir.display());
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

        // ── 0. Connect broker (execution + data feed) ──
        info!(
            "Starting backtest strategy={} timeframe={:?} start={} end={}",
            self.name, time_frame, start, end
        );
        self.timeframe = time_frame.clone();
        self.history_seed_anchor = Some(start);
        self.broker.connect().await?;
        self.status = StrategyStatus::Running;

        // ── 1. Strategy lifecycle: on_start ──
        debug!("Invoking strategy on_start");
        self.run_on_start_logic(LifecycleTiming::BeforeGenerated);
        strategy.on_start(self);
        self.run_on_start_logic(LifecycleTiming::AfterGenerated);

        // ── 2. Load universe assets ──
        self.load_universe(&mut strategy).await;

        if self.universe.is_empty() {
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

        // ── 6. Load historical data into BacktestState ──
        let symbols: Vec<String> = self.universe.keys().cloned().collect();
        info!("Loading backtest history for symbols: {:?}", symbols);
        self.broker
            .load_backtest_data(&symbols, start, end, time_frame)
            .await?;
        info!("Backtest history loaded successfully");

        // ── 7. Main backtest loop ──
        loop {
            // Advance one step: process orders → get current bars
            let step_result = self.broker.step()?;

            match step_result {
                None => break, // Backtest complete
                Some(current_bars) => {
                    debug!(
                        "Backtest step bars received for {} symbols",
                        current_bars.len()
                    );
                    // Process trade events from the broker (fills, closes, cancels, etc.)
                    debug!("Backtest loop calling on_trade_update");
                    self.on_trade_update();
                    debug!("Backtest loop on_trade_update returned");
                    self.sync_backtest_insight_snapshots();

                    // Call _on_bar for each symbol's bar
                    for (symbol, bar) in &current_bars {
                        self._on_bar(&mut strategy, symbol, &BarData::Bars(vec![bar.clone()]));
                    }
                    // Run insight pipeline after processing all bars for this step
                    debug!("Backtest loop calling run_insight_pipeline");
                    self.run_insight_pipeline();
                    debug!("Backtest loop run_insight_pipeline returned");
                    self.sync_backtest_insight_snapshots();
                }
            }
        }

        // ── 8. Teardown ──
        debug!("Invoking strategy on_teardown");
        self.begin_teardown();
        self.run_on_teardown_logic(LifecycleTiming::BeforeGenerated);
        strategy.on_teardown(self);
        self.run_on_teardown_logic(LifecycleTiming::AfterGenerated);
        self.sync_backtest_insight_snapshots();

        // Disconnect data feed
        let _ = self.broker.disconnect().await;

        // Put strategy back
        self.strategy = Some(strategy);
        self.history_seed_anchor = None;

        // ── 9. Results ──
        let results = self.broker.get_results();
        self.finalize_backtest_results(&results);
        self.status = StrategyStatus::Completed;
        Ok(results)
    }
}
