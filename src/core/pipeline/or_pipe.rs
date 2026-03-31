use crate::core::insight::Insight;
use crate::core::pipeline::InsightPipeResult;
use crate::core::pipeline::WrappedInsightPipe;
use crate::core::strategy::StrategyContext;

use super::InsightPipe;

/// Any sub-pipe passes (port of Python's `AnyExecutor`).
/// If no sub-pipe passes, the insight is rejected.
pub struct OrPipe {
    pipes: Vec<WrappedInsightPipe>,
}

impl OrPipe {
    pub fn new(pipes: Vec<WrappedInsightPipe>) -> Self {
        assert!(!pipes.is_empty(), "OrPipe requires at least one sub-pipe");
        Self { pipes }
    }
}

impl InsightPipe for OrPipe {
    fn version(&self) -> &str {
        "1.0"
    }

    fn run(&mut self, ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        let mut errors = Vec::new();

        for pipe in self.pipes.iter_mut() {
            if !pipe.should_run(insight) {
                continue;
            }
            let result = pipe.run(ctx, insight);
            if !result.success {
                if let Some(msg) = &result.message {
                    errors.push(msg.clone());
                }
                continue;
            }
            if result.passed {
                return InsightPipeResult::new(
                    true,
                    true,
                    Some(format!("Pipe {} passed: {:?}", pipe.name(), result.message)),
                    self.name().to_string(),
                );
            }
        }

        InsightPipeResult::new(
            false,
            true,
            Some(format!("No pipe passed. Errors: {:?}", errors)),
            self.name().to_string(),
        )
    }
}
