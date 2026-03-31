use crate::core::insight::Insight;
use crate::core::pipeline::InsightPipeResult;
use crate::core::pipeline::WrappedInsightPipe;
use crate::core::strategy::StrategyContext;

use super::InsightPipe;

/// All sub-pipes must pass (port of Python's `AllExecutor`).
/// If any sub-pipe fails, the insight is rejected.
pub struct AndPipe {
    pipes: Vec<WrappedInsightPipe>,
}

impl AndPipe {
    pub fn new(pipes: Vec<WrappedInsightPipe>) -> Self {
        assert!(!pipes.is_empty(), "AndPipe requires at least one sub-pipe");
        Self { pipes }
    }
}

impl InsightPipe for AndPipe {
    fn version(&self) -> &str {
        "1.0"
    }

    fn run(&mut self, ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        let mut messages = Vec::new();

        for pipe in self.pipes.iter_mut() {
            if !pipe.should_run(insight) {
                continue;
            }
            let result = pipe.run(ctx, insight);
            if let Some(msg) = &result.message {
                messages.push(msg.clone());
            }
            if !result.success || !result.passed {
                return InsightPipeResult::new(
                    false,
                    true,
                    Some(format!("AndPipe failed: {:?}", messages)),
                    self.name().to_string(),
                );
            }
        }

        InsightPipeResult::new(
            true,
            true,
            Some(format!("All pipes passed: {:?}", messages)),
            self.name().to_string(),
        )
    }
}
