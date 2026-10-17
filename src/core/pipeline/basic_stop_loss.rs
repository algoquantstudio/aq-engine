use crate::core::broker::BrokerFeature;
use crate::core::insight::Insight;
use crate::core::pipeline::InsightPipeResult;
use crate::core::strategy::StrategyContext;

use super::{InsightPipe, exit_trigger::ExitTrigger};

/// Closes an active insight when its executable quote or simulated bar breaches its stop loss.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - None.
///
/// Behaviour:
/// Passes through insights without a stop loss and leaves native OCO protection to the broker.
/// Without OCO, native live feeds use bid for buy insights and ask for sell insights.
/// Missing/stale quotes defer execution. Backtests and bar-only feeds retain low/high
/// history semantics. Insights already being closed do not issue another close request.
pub struct BasicStopLossPipe;

impl BasicStopLossPipe {
    pub fn new() -> Self {
        Self
    }
}

impl InsightPipe for BasicStopLossPipe {
    fn version(&self) -> &str {
        // The continuation/receipt layout is unchanged; existing pending closes
        // must remain resumable after this decision-source fix.
        "1.0"
    }

    fn run(&mut self, ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        if insight.has_pending_close() {
            return InsightPipeResult::from_exit_mutation(insight.close(ctx), self.name());
        }
        let Some(stop_loss) = insight.stop_loss() else {
            return InsightPipeResult::new(
                true,
                true,
                Some("Insight does not have stop loss set".to_string()),
                self.name().to_string(),
            );
        };
        if ctx.has_broker_feature(BrokerFeature::OcoOrder) {
            return InsightPipeResult::new(
                true,
                true,
                Some("Stop loss is managed by the broker's native OCO protection".to_string()),
                self.name().to_string(),
            );
        }
        if insight.closing {
            return InsightPipeResult::new(
                false,
                true,
                Some("Insight is already being closed".to_string()),
                self.name().to_string(),
            );
        }

        let should_close = match ExitTrigger::StopLoss.reached(ctx, insight, stop_loss) {
            Ok(reached) => reached,
            Err(error) => return InsightPipeResult::from_read_error(error, self.name()),
        };

        if should_close {
            return InsightPipeResult::from_exit_mutation(insight.close(ctx), self.name());
        }

        InsightPipeResult::new(true, true, None, self.name().to_string())
    }
}
