use crate::core::broker::BrokerFeature;
use crate::core::insight::Insight;
use crate::core::insight::types::InsightState;
use crate::core::pipeline::{InsightPipeDeferral, InsightPipeResult};
use crate::core::strategy::StrategyContext;

use super::InsightPipe;

/// Submits new insights to the broker through the strategy context.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - None.
///
/// Behaviour:
/// Targets insights in `InsightState::New`. When an insight is new, it calls
/// `insight.submit(ctx)`, allowing the broker/runtime implementation behind the strategy
/// context to place the order. Insights in other states pass through unchanged.
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
        // Admission checks only apply before submission. Replaying them while
        // waiting for broker evidence can turn a disconnected account snapshot
        // into a failure that cancels an order already accepted by the broker.
        // Poll even if a trade event advanced the insight before the RPC reply.
        if insight.has_pending_submit() {
            return InsightPipeResult::from_mutation(
                insight.poll_pending_submission(ctx),
                self.name(),
            );
        }
        if insight.state == InsightState::New {
            if ctx.has_broker_feature(BrokerFeature::MaxTotalOrders)
                && ctx.cached_account().is_none()
            {
                return InsightPipeResult::deferred(
                    InsightPipeDeferral::AccountSnapshot,
                    self.name(),
                );
            }
            if let Err(message) =
                crate::core::utils::quantity_sizing::validate_pending_tree_limits(ctx, insight)
            {
                insight.state_log(Some(message.clone()));
                return InsightPipeResult::new(false, true, Some(message), self.name().to_string());
            }
            return InsightPipeResult::from_mutation(insight.submit(ctx), self.name());
        }
        InsightPipeResult::new(
            true,
            true,
            Some("Insight submitted".to_string()),
            self.name().to_string(),
        )
    }
}
