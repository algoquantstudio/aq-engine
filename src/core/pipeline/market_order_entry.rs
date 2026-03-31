use crate::core::insight::Insight;
use crate::core::pipeline::InsightPipeResult;
use crate::core::strategy::StrategyContext;

use super::InsightPipe;

/// Sets the limit price for market orders to the current close price.
/// Port of Python's `MarketOrderEntryPriceExecutor`.
/// Only applies to market orders without a limit price set.
/// Targets `InsightState::New`.
pub struct MarketOrderEntryPipe;

impl MarketOrderEntryPipe {
    pub fn new() -> Self {
        Self
    }
}

impl InsightPipe for MarketOrderEntryPipe {
    fn version(&self) -> &str {
        "1.0"
    }

    fn run(&mut self, ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        // If insight already has a limit price, pass through
        if insight.limit_price.is_some() {
            return InsightPipeResult::new(
                true,
                true,
                Some("Insight already has limit price set".to_string()),
                self.name().to_string(),
            );
        }

        // Get latest bar close price for this symbol
        let history = ctx.history();
        if let Some(df) = history.get(&insight.symbol) {
            if df.height() > 0 {
                // Get last close price from the history DataFrame
                if let Ok(close_col) = df.column("close") {
                    if let Ok(close_vals) = close_col.f64() {
                        if let Some(price) = close_vals.get(df.height() - 1) {
                            insight.set_limit_price(Some(price));
                            return InsightPipeResult::new(
                                true,
                                true,
                                Some(format!("Limit price set to close: {}", price)),
                                self.name().to_string(),
                            );
                        }
                    }
                }
            }
        }

        InsightPipeResult::new(
            false,
            false,
            Some("Failed to get close price for limit".to_string()),
            self.name().to_string(),
        )
    }
}
