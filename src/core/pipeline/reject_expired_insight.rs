use crate::core::insight::Insight;
use crate::core::pipeline::InsightPipeResult;
use crate::core::strategy::StrategyContext;

use super::InsightPipe;

/// Rejects insights whose configured lifetime has expired.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - None.
///
/// Behaviour:
/// Calls `insight.has_expired(ctx)` using the strategy clock/context. Expired insights are marked
/// rejected with `insight.order_rejected(...)` and return `passed=false`; non-expired insights
/// pass through unchanged.
pub struct RejectExpiredInsightPipe;

impl RejectExpiredInsightPipe {
    pub fn new() -> Self {
        Self
    }
}

impl InsightPipe for RejectExpiredInsightPipe {
    fn version(&self) -> &str {
        "1.0"
    }

    fn run(&mut self, ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        if insight.has_expired(ctx) {
            insight.order_rejected("Insight has expired");
            return InsightPipeResult::new(
                false,
                true,
                Some("Insight has expired".to_string()),
                self.name().to_string(),
            );
        }

        InsightPipeResult::new(true, true, None, self.name().to_string())
    }
}
