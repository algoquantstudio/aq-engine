use crate::core::insight::Insight;
use crate::core::insight::types::InsightState;
use crate::core::pipeline::InsightPipeResult;
use crate::core::strategy::StrategyContext;

use super::InsightPipe;

/// Submits the insight as an order via `ctx.submit_insight()`.
/// Targets `InsightState::New`.
pub struct InsightSubmitPipe;

impl InsightSubmitPipe {
    pub fn new() -> Self {
        Self
    }
}

impl InsightPipe for InsightSubmitPipe {
    fn version(&self) -> &str {
        "1.0"
    }

    fn run(&mut self, ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        if insight.state == InsightState::New {
            insight.submit(ctx);
        }
        InsightPipeResult::new(
            true,
            true,
            Some("Insight submitted".to_string()),
            self.name().to_string(),
        )
    }
}
