use crate::core::insight::Insight;
use crate::core::pipeline::InsightPipeResult;
use crate::core::strategy::StrategyContext;

use super::InsightPipe;

/// Converts a market-style insight into a close-priced limit entry when no entry is set.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - None.
///
/// Behaviour:
/// Passes through insights that already have a limit price. Otherwise it reads the latest
/// `close` value from the symbol history and writes it to `insight.limit_price`. The pipe
/// fails when history is missing, empty, or the close column cannot be read as a finite value.
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
