use crate::core::insight::Insight;
use crate::core::pipeline::InsightPipeResult;
use crate::core::strategy::StrategyContext;

use super::InsightPipe;

/// Cancels pending opposite-side entries for the same symbol.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - None.
///
/// Behaviour:
/// Requests runtime-owned cancellation for New/Executed opposite-side entries. Filled positions
/// and terminal insights are left alone. Broker acknowledgements are handled by the execution
/// queue; scheduling cancellation is not reported as an already-completed broker operation.
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
        let ids: Vec<_> = ctx
            .insights()
            .iter_active_insights()
            .filter(|other| {
                other.symbol == insight.symbol
                    && other.side != insight.side
                    && other.insight_id != insight.insight_id
                    && matches!(
                        other.state,
                        crate::core::insight::types::InsightState::New
                            | crate::core::insight::types::InsightState::Executed
                    )
            })
            .map(|other| other.insight_id)
            .collect();
        let mut affected = 0;
        for id in ids {
            match ctx.request_insight_cancellation(id) {
                Ok(true) => affected += 1,
                Ok(false) => {}
                Err(error) => return InsightPipeResult::from_mutation(Err(error), self.name()),
            }
        }

        InsightPipeResult::new(
            true,
            true,
            Some(format!(
                "Requested cancellation of {} opposite-side entries for {}",
                affected, insight.symbol
            )),
            self.name().to_string(),
        )
    }
}
