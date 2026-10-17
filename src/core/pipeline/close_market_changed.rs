use crate::core::insight::Insight;
use crate::core::pipeline::InsightPipeResult;
use crate::core::strategy::StrategyContext;

use super::InsightPipe;

/// Closes an insight when its market-change flag has been set.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - None.
///
/// Behaviour:
/// Checks `insight.market_changed` and calls `insight.close(ctx)` when the flag is true. Insights
/// already in a closing state return `passed=false`; otherwise unchanged insights pass through.
pub struct CloseMarketChangedPipe;

impl CloseMarketChangedPipe {
    pub fn new() -> Self {
        Self
    }
}

impl InsightPipe for CloseMarketChangedPipe {
    fn version(&self) -> &str {
        "1.0"
    }

    fn run(&mut self, ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        if insight.has_pending_close() {
            return InsightPipeResult::from_exit_mutation(insight.close(ctx), self.name());
        }
        if insight.closing {
            return InsightPipeResult::new(
                false,
                true,
                Some("Insight is already being closed".to_string()),
                self.name().to_string(),
            );
        }

        if insight.market_changed {
            return InsightPipeResult::from_exit_mutation(insight.close(ctx), self.name());
        }

        InsightPipeResult::new(true, true, None, self.name().to_string())
    }
}
