use crate::core::lifecycle::{LifecycleResult, OnTeardownLogic};
use crate::core::strategy::StrategyContext;

/// Cleans up active insights before strategy teardown completes.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - None.
///
/// Behaviour:
/// Delegates to `ctx.cleanup_active_insights_for_teardown()`, which rejects new insights,
/// cancels executed insights, and closes filled positions with their remaining quantity.
pub struct CloseAllFilledPositions;

impl CloseAllFilledPositions {
    pub fn new() -> Self {
        Self
    }
}

impl OnTeardownLogic for CloseAllFilledPositions {
    fn version(&self) -> &str {
        "1.0"
    }

    fn run(&mut self, ctx: &mut dyn StrategyContext) -> LifecycleResult {
        let report = ctx.cleanup_active_insights_for_teardown();
        let summary = report.summary();

        if report.has_failures() {
            LifecycleResult::new(
                false,
                Some(format!(
                    "{}; failures={}",
                    summary,
                    report.failures.join("; ")
                )),
                self.name().to_string(),
            )
        } else if report.total_actions() == 0 {
            LifecycleResult::new(
                true,
                Some("No active insights to clean up on teardown".to_string()),
                self.name().to_string(),
            )
        } else {
            LifecycleResult::new(true, Some(summary), self.name().to_string())
        }
    }
}
