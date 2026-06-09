use crate::core::insight::Insight;
use crate::core::pipeline::InsightPipeResult;
use crate::core::strategy::StrategyContext;

use super::InsightPipe;

/// Reports active insights on the opposite side for the same symbol.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - None.
///
/// Behaviour:
/// Reads the strategy insight collection for the current symbol, counts insights whose side is
/// opposite to the incoming insight, and returns that count in the result message. The current
/// implementation is informational and does not cancel or mutate the matching insights.
pub struct CancelOppositePipe;

impl CancelOppositePipe {
    pub fn new() -> Self {
        Self
    }
}

impl InsightPipe for CancelOppositePipe {
    fn version(&self) -> &str {
        "1.0"
    }

    fn run(&mut self, ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        // Count opposite-side insights for the same symbol
        let insights = ctx.insights();
        let same_symbol = insights.get_insights_by_symbol(&insight.symbol);
        let affected = same_symbol
            .iter()
            .filter(|other| other.side != insight.side)
            .count();

        InsightPipeResult::new(
            true,
            true,
            Some(format!(
                "Flagged {} opposite-side insights for {}",
                affected, insight.symbol
            )),
            self.name().to_string(),
        )
    }
}
