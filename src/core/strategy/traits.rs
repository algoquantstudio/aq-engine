use crate::core::alpha::WrappedAlphaModel;
use crate::core::broker::UnifiedBroker;
use crate::core::broker::traits::{Broker, DataFeed, DataProvider, OrderManagementProvider};
use crate::core::broker::types::BrokerError;
use crate::core::broker::types::{Account, Asset, BarData, Quote};
use crate::core::indicators::Indicator;
use crate::core::insight::{Insight, InsightCollection};
use crate::core::pipeline::WrappedInsightPipe;
use crate::core::strategy::StrategyMode;
use crate::core::universe::WrappedUniverseModel;
use crate::core::utils::timeframe::TimeFrame;
use crate::core::utils::tools::TradingTools;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use polars::prelude::DataFrame;
use serde_json::Value;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Default)]
pub struct TeardownCleanupReport {
    pub rejected_new: usize,
    pub cancelled_executed: usize,
    pub closed_filled: usize,
    pub failures: Vec<String>,
}

impl TeardownCleanupReport {
    pub fn total_actions(&self) -> usize {
        self.rejected_new + self.cancelled_executed + self.closed_filled
    }

    pub fn has_failures(&self) -> bool {
        !self.failures.is_empty()
    }

    pub fn summary(&self) -> String {
        format!(
            "rejected_new={}, cancelled_executed={}, closed_filled={}",
            self.rejected_new, self.cancelled_executed, self.closed_filled
        )
    }
}
// ─────────────────────── Object-safe Strategy Context ───────────────────────

/// Core context available to **all** components (Strategy, AlphaModel, InsightPipe).
///
/// Object-safe — can be used as `&mut dyn StrategyContext`.
/// For typed broker access, see the `BrokerAccess` extension trait.
pub trait StrategyContext {
    fn universe(&self) -> &HashMap<String, Asset>;
    fn history(&self) -> &HashMap<String, DataFrame>;
    fn insights(&self) -> &InsightCollection;
    fn mode(&self) -> StrategyMode;
    fn add_insight(&mut self, insight: Insight);
    fn submit_insight(&mut self, insight: &mut Insight);
    fn register_indicator(&mut self, indicator: Box<dyn Indicator>);

    fn add_alpha(&mut self, alpha: WrappedAlphaModel);
    fn add_pipe(&mut self, pipe: WrappedInsightPipe);
    fn add_universe_model(&mut self, model: WrappedUniverseModel);
    fn set_execution_risk(&mut self, risk: f64);
    fn set_min_reward_risk_ratio(&mut self, ratio: f64);
    fn set_base_confidence(&mut self, confidence: f64);
    fn execution_risk(&self) -> f64;
    fn min_reward_risk_ratio(&self) -> f64;
    fn base_confidence(&self) -> f64;
    fn variables(&self) -> &DashMap<String, Value>;
    fn tools(&self) -> Box<dyn TradingTools + '_>;
    fn max_history_rows(&self) -> usize;
    fn set_max_history_rows(&mut self, rows: usize);

    /// Get the current warm-up period (number of bars to skip before generating signals).
    fn warm_up_bars(&self) -> i32;
    /// Set the warm-up period. Only raises the value — never lowers it.
    fn set_warm_up_bars(&mut self, bars: i32);
    fn timeframe(&self) -> &TimeFrame;
    fn account(&self) -> Result<Account, BrokerError>;
    fn current_time(&self) -> DateTime<Utc>;
    fn bind_insight_context(&self, insight: &mut Insight);
    fn latest_quote(&self, symbol: &str) -> Result<Quote, BrokerError>;
    fn preseed_warmup_history(
        &mut self,
        symbol: &str,
        warmup_bars: i32,
    ) -> Result<usize, BrokerError> {
        let _ = (symbol, warmup_bars);
        Err(BrokerError::DataFeedError(
            "preseed_warmup_history is not supported by this strategy context".to_string(),
        ))
    }
    fn cleanup_active_insights_for_teardown(&mut self) -> TeardownCleanupReport {
        TeardownCleanupReport::default()
    }

    // Order operations (delegated to broker internally)
    fn cancel_order(&self, order_id: &str) -> Result<bool, BrokerError>;
    fn close_position(
        &self,
        order_id: &str,
        qty: f64,
        price: Option<f64>,
    ) -> Result<bool, BrokerError>;
    fn shutdown(&mut self);
}

// ─────────────────────── Generic Broker Extension ───────────────────────

/// Extension trait for code that needs typed access to the broker.
/// Not object-safe (has associated types).
pub trait BrokerAccess<E, D>: StrategyContext
where
    E: Broker + OrderManagementProvider,
    D: DataFeed + DataProvider,
{
    fn broker(&self) -> &UnifiedBroker<E, D>;
    fn broker_mut(&mut self) -> &mut UnifiedBroker<E, D>;
}

// ─────────────────────── Strategy Trait ───────────────────────

pub trait Strategy {
    fn name(&self) -> &str {
        "Base Strategy"
    }

    /// Called once when the strategy starts (Python's `start()`).
    fn on_start(&mut self, ctx: &mut dyn StrategyContext);

    /// Called once per asset during universe loading (Python's `init(asset)`).
    fn init(&mut self, ctx: &mut dyn StrategyContext, asset: &Asset);

    /// Returns the set of symbols this strategy trades (Python's `universe()`).
    fn universe(&self, ctx: &mut dyn StrategyContext) -> HashSet<String>;

    /// Called each time a new bar arrives for a symbol (Python's `on_bar(symbol, bar)`).
    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, symbol: &str, bar: &BarData);

    /// Called after on_bar to generate trading insights (Python's `generateInsights(symbol)`).
    fn generate_insights(&mut self, ctx: &mut dyn StrategyContext, symbol: &str);

    /// Called for each active insight to manage its execution pipeline.
    fn insight_pipeline(&mut self, ctx: &mut dyn StrategyContext, insight: &Insight);

    /// Called once at the end of the strategy lifecycle.
    fn on_teardown(&mut self, ctx: &mut dyn StrategyContext);
}
