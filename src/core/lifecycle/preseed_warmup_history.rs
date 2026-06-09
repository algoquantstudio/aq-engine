use crate::core::broker::types::Asset;
use crate::core::lifecycle::{LifecycleResult, OnInitLogic};
use crate::core::strategy::StrategyContext;

/// Fetches warm-up bars into strategy history before per-asset strategy init.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - None.
///
/// Behaviour:
/// Reads `ctx.warm_up_bars()`. When warm-up is positive, it asks the strategy context to fetch
/// and insert pre-run history for the asset symbol using the strategy timeframe.
pub struct PreseedWarmupHistory;

impl PreseedWarmupHistory {
    pub fn new() -> Self {
        Self
    }
}

impl OnInitLogic for PreseedWarmupHistory {
    fn version(&self) -> &str {
        "1.0"
    }

    fn run(&mut self, ctx: &mut dyn StrategyContext, asset: &Asset) -> LifecycleResult {
        let warmup_bars = ctx.warm_up_bars();
        if warmup_bars <= 0 {
            return LifecycleResult::new(
                true,
                Some(format!("No warm-up history requested for {}", asset.symbol)),
                self.name().to_string(),
            );
        }

        match ctx.preseed_warmup_history(&asset.symbol, warmup_bars) {
            Ok(rows) => LifecycleResult::new(
                true,
                Some(format!(
                    "Preseeded {} warm-up rows for {}",
                    rows, asset.symbol
                )),
                self.name().to_string(),
            ),
            Err(error) => LifecycleResult::new(
                false,
                Some(format!(
                    "Failed to preseed warm-up history for {}: {}",
                    asset.symbol, error
                )),
                self.name().to_string(),
            ),
        }
    }
}
