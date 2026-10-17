use crate::core::insight::Insight;
use crate::core::pipeline::InsightPipeResult;
use crate::core::strategy::StrategyContext;

use super::InsightPipe;

/// Rejects insights whose configured lifetime has expired.
///
/// Author: @isaac-diaby
///
/// Behaviour:
/// Calls `insight.expire_if_due(ctx)` using the strategy clock/context. A new insight is rejected
/// immediately; an executed order is cancelled and a filled position is closed through the
/// configured broker, with the terminal state applied by the broker event. Non-expired insights
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

    fn controls_expiration(&self) -> bool {
        true
    }

    fn run(&mut self, ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        if let Some(result) = insight.expire_if_due(ctx) {
            return InsightPipeResult::from_exit_mutation(result, self.name());
        }

        InsightPipeResult::new(true, true, None, self.name().to_string())
    }
}
