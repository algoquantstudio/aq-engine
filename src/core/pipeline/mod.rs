use crate::core::broker::types::{Asset, Quote};
use crate::core::insight::Insight;
use crate::core::insight::types::InsightState;
use crate::core::strategy::StrategyContext;
use polars::prelude::DataFrame;
use std::collections::HashSet;

// Built-in pipe implementations
pub mod allow_trading_window;
pub mod and_pipe;
pub mod basic_stop_loss;
pub mod basic_take_profit;
pub mod cancel_opposite;
pub mod close_market_changed;
pub mod dynamic_quantity_to_risk;
pub mod end_of_day_close;
pub mod full_account_quantity_to_risk;
pub mod insight_submit;
pub mod insight_ttl_config;
pub mod market_order_entry;
pub mod minimum_risk_to_reward;
pub mod or_pipe;
pub mod percentage_dca_levels;
pub mod percentage_risk_to_quantity;
pub mod quantity_sizing;
pub mod reject_expired_insight;
pub mod scale_out;

// ─────────────────────── InsightPipeResult ───────────────────────

/// Result returned by `InsightPipe::run()`.
/// Mirrors Python's `ExecutorResults`.
#[derive(Clone, Debug)]
pub struct InsightPipeResult {
    /// Did the pipe's logic pass (e.g., validation checks succeeded)?
    pub passed: bool,
    /// Was the pipe execution itself successful (no errors)?
    pub success: bool,
    /// Optional message describing the result.
    pub message: Option<String>,
    /// Name of the pipe that produced this result.
    pub pipe_name: String,
}

impl InsightPipeResult {
    /// Convenience constructor (mirrors Python's `returnResults`).
    pub fn new(passed: bool, success: bool, message: Option<String>, pipe_name: String) -> Self {
        let (passed, success) = if passed {
            (true, true)
        } else if !success {
            (false, false)
        } else {
            (passed, success)
        };
        Self {
            passed,
            success,
            message,
            pipe_name,
        }
    }
}

// ─────────────────────── InsightPipe Trait ───────────────────────

/// A pipeline stage that processes an insight at a specific `InsightState`.
///
/// Mirrors Python's `BaseExecutor`. Stored as `Box<dyn InsightPipe>`.
/// Receives `&mut dyn StrategyContext` for full access to strategy state.
pub trait InsightPipe {
    /// Unique name of this pipe.
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
            .rsplit("::")
            .next()
            .unwrap_or("InsightPipe")
    }

    /// Version string.
    fn version(&self) -> &str;

    /// Execute logic on an insight (Python's `executor.run(insight)`).
    fn run(&mut self, ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult;

    fn get_latest_bar(&self, ctx: &dyn StrategyContext, symbol: &str) -> Result<DataFrame, String> {
        let history = ctx
            .history()
            .get(symbol)
            .ok_or_else(|| format!("No history found for {}", symbol))?;
        if history.height() == 0 {
            return Err(format!("No bars available for {}", symbol));
        }
        Ok(history.tail(Some(1)))
    }

    fn get_previos_bar(
        &self,
        ctx: &dyn StrategyContext,
        symbol: &str,
    ) -> Result<DataFrame, String> {
        let history = ctx
            .history()
            .get(symbol)
            .ok_or_else(|| format!("No history found for {}", symbol))?;
        if history.height() < 2 {
            return Err(format!("Not enough bars available for {}", symbol));
        }
        Ok(history.slice((history.height() - 2) as i64, 1))
    }

    fn get_asset<'a>(&self, ctx: &'a dyn StrategyContext, symbol: &str) -> Option<&'a Asset> {
        ctx.universe().get(symbol)
    }

    fn get_latest_quote(&self, ctx: &dyn StrategyContext, symbol: &str) -> Result<Quote, String> {
        ctx.latest_quote(symbol).map_err(|e| e.to_string())
    }

    fn get_previos_quote(&self, ctx: &dyn StrategyContext, symbol: &str) -> Result<Quote, String> {
        self.get_latest_quote(ctx, symbol)
    }
}

// ─────────────────────── WrappedInsightPipe & Builder ───────────────────────

/// Builder for `WrappedInsightPipe`.
pub struct InsightPipeBuilder {
    inner: Box<dyn InsightPipe>,
    target_state: InsightState,
    allow_state_change: bool,
    allowed_assets: Option<HashSet<String>>,
    allowed_alphas: Option<HashSet<String>>,
}

impl InsightPipeBuilder {
    pub fn new(inner: Box<dyn InsightPipe>) -> Self {
        Self {
            inner,
            target_state: InsightState::New, // Default to New, like Python
            allow_state_change: true,
            allowed_assets: None,
            allowed_alphas: None,
        }
    }

    pub fn target_state(mut self, state: InsightState) -> Self {
        self.target_state = state;
        self
    }

    pub fn allow_state_change(mut self, allow: bool) -> Self {
        self.allow_state_change = allow;
        self
    }

    pub fn allowed_assets(mut self, assets: HashSet<String>) -> Self {
        self.allowed_assets = Some(assets);
        self
    }

    pub fn allowed_alphas(mut self, alphas: HashSet<String>) -> Self {
        self.allowed_alphas = Some(alphas);
        self
    }

    pub fn build(self) -> WrappedInsightPipe {
        WrappedInsightPipe {
            inner: self.inner,
            target_state: self.target_state,
            allow_state_change: self.allow_state_change,
            allowed_assets: self.allowed_assets,
            allowed_alphas: self.allowed_alphas,
            runs_count: 0,
            passed_count: 0,
        }
    }
}

/// A wrapper around an `InsightPipe` that tracks metadata and manages execution state.
pub struct WrappedInsightPipe {
    pub inner: Box<dyn InsightPipe>,
    pub target_state: InsightState,
    pub allow_state_change: bool,
    pub allowed_assets: Option<HashSet<String>>,
    pub allowed_alphas: Option<HashSet<String>>,
    pub runs_count: usize,
    pub passed_count: usize,
}

impl WrappedInsightPipe {
    pub fn builder(inner: Box<dyn InsightPipe>) -> InsightPipeBuilder {
        InsightPipeBuilder::new(inner)
    }

    pub fn name(&self) -> &str {
        self.inner.name()
    }

    pub fn version(&self) -> &str {
        self.inner.version()
    }

    pub fn should_run(&self, insight: &Insight) -> bool {
        let asset_ok = match &self.allowed_assets {
            None => true,
            Some(set) if set.is_empty() => true,
            Some(set) => set.contains(&insight.symbol),
        };
        let alpha_ok = match &self.allowed_alphas {
            None => true,
            Some(set) if set.is_empty() => true,
            Some(set) => match &insight.strategy_type {
                crate::core::insight::types::StrategyType::Custom(name) => set.contains(name),
                _ => true,
            },
        };
        asset_ok && alpha_ok
    }

    pub fn run(
        &mut self,
        ctx: &mut dyn StrategyContext,
        insight: &mut Insight,
    ) -> InsightPipeResult {
        self.runs_count += 1;
        let result = self.inner.run(ctx, insight);
        if result.passed {
            self.passed_count += 1;
        }
        result
    }
}
