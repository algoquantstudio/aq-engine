use crate::core::insight::Insight;
use crate::core::pipeline::InsightPipeResult;
use crate::core::strategy::StrategyContext;

use super::InsightPipe;

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
