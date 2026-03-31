use crate::core::broker::types::Asset;
use crate::core::insight::Insight;
use crate::core::strategy::StrategyContext;
use dashmap::DashMap;
use polars::prelude::DataFrame;
use serde_json::Value;
use std::collections::HashSet;

pub mod ema_price_crossover;
pub mod rsi_divergance_alpha;
pub mod test_entry;
pub use ema_price_crossover::EmaPriceCrossover;
pub use rsi_divergance_alpha::RsiDiverganceAlpha;
pub use test_entry::TestEntry;

// ─────────────────────── AlphaResult ───────────────────────

/// Result returned by `AlphaModel::generate_insights()`.
/// Mirrors Python's `AlphaResults`.
#[derive(Clone, Debug)]
pub struct AlphaResult {
    /// Generated insight, if any.
    pub insight: Option<Insight>,
    /// Whether the alpha ran successfully.
    pub success: bool,
    /// Optional message describing the result.
    pub message: Option<String>,
    /// Name of the alpha that produced this result.
    pub alpha_name: String,
}

impl AlphaResult {
    /// Convenience constructor (mirrors Python's `returnResults`).
    pub fn new(
        insight: Option<Insight>,
        success: bool,
        message: Option<String>,
        alpha_name: String,
    ) -> Self {
        let (insight, success) = if let Some(ins) = insight {
            // If insight is provided, always mark as success
            (Some(ins), true)
        } else if !success {
            (None, false)
        } else {
            (None, success)
        };
        Self {
            insight,
            success,
            message,
            alpha_name,
        }
    }
}

// ─────────────────────── AlphaModel Trait ───────────────────────

/// Abstract alpha model — generates trading insights.
///
/// Mirrors Python's `BaseAlpha`. Stored as `Box<dyn AlphaModel>`.
/// Receives `&mut dyn StrategyContext` for full access to strategy state.
pub trait AlphaModel {
    /// Unique name of this alpha model.
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
            .rsplit("::")
            .next()
            .unwrap_or("AlphaModel")
    }

    /// Version string.
    fn version(&self) -> &str;

    /// One-time startup after universe is loaded (Python's `alpha.start()`).
    fn start(&mut self, ctx: &mut dyn StrategyContext);

    /// Per-asset initialization during universe loading (Python's `alpha.init(asset)`).
    fn init(&mut self, ctx: &mut dyn StrategyContext, asset: &Asset);

    /// Generate insight for a given symbol (Python's `alpha.generateInsights(symbol)`).
    fn generate_insights(&mut self, ctx: &mut dyn StrategyContext, symbol: &str) -> AlphaResult;

    fn get_history<'a>(&self, ctx: &'a dyn StrategyContext, symbol: &str) -> Option<&'a DataFrame> {
        ctx.history().get(symbol)
    }

    fn get_latest_bar(&self, ctx: &dyn StrategyContext, symbol: &str) -> Result<DataFrame, String> {
        let history = self
            .get_history(ctx, symbol)
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
        let history = self
            .get_history(ctx, symbol)
            .ok_or_else(|| format!("No history found for {}", symbol))?;
        if history.height() < 2 {
            return Err(format!("Not enough bars available for {}", symbol));
        }
        Ok(history.slice((history.height() - 2) as i64, 1))
    }

    fn get_asset<'a>(&self, ctx: &'a dyn StrategyContext, symbol: &str) -> Option<&'a Asset> {
        ctx.universe().get(symbol)
    }

    fn get_variables<'a>(&self, ctx: &'a dyn StrategyContext) -> &'a DashMap<String, Value> {
        ctx.variables()
    }
}

// ─────────────────────── WrappedAlphaModel & Builder ───────────────────────

/// Builder for `WrappedAlphaModel`.
pub struct AlphaModelBuilder {
    inner: Box<dyn AlphaModel>,
    allowed_assets: Option<HashSet<String>>,
}

impl AlphaModelBuilder {
    pub fn new(inner: Box<dyn AlphaModel>) -> Self {
        Self {
            inner,
            allowed_assets: None,
        }
    }

    pub fn allowed_assets(mut self, assets: HashSet<String>) -> Self {
        self.allowed_assets = Some(assets);
        self
    }

    pub fn build(self) -> WrappedAlphaModel {
        WrappedAlphaModel {
            inner: self.inner,
            allowed_assets: self.allowed_assets,
            runs_count: 0,
            insights_generated: 0,
        }
    }
}

/// A wrapper around an `AlphaModel` that tracks metadata and manages execution state.
pub struct WrappedAlphaModel {
    pub inner: Box<dyn AlphaModel>,
    pub allowed_assets: Option<HashSet<String>>,
    pub runs_count: usize,
    pub insights_generated: usize,
}

impl WrappedAlphaModel {
    pub fn builder(inner: Box<dyn AlphaModel>) -> AlphaModelBuilder {
        AlphaModelBuilder::new(inner)
    }

    pub fn name(&self) -> &str {
        self.inner.name()
    }

    pub fn version(&self) -> &str {
        self.inner.version()
    }

    pub fn start(&mut self, ctx: &mut dyn StrategyContext) {
        self.inner.start(ctx);
    }

    pub fn init(&mut self, ctx: &mut dyn StrategyContext, asset: &Asset) {
        self.inner.init(ctx, asset);
    }

    pub fn is_allowed_asset(&self, symbol: &str) -> bool {
        match &self.allowed_assets {
            None => true,
            Some(set) if set.is_empty() => true,
            Some(set) => set.contains(symbol),
        }
    }

    pub fn generate_insights(
        &mut self,
        ctx: &mut dyn StrategyContext,
        symbol: &str,
    ) -> AlphaResult {
        self.runs_count += 1;
        let result = self.inner.generate_insights(ctx, symbol);
        if result.insight.is_some() {
            self.insights_generated += 1;
        }
        result
    }
}
