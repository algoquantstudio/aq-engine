use crate::core::broker::BrokerFeature;
use crate::core::insight::Insight;
use crate::core::pipeline::InsightPipeResult;
use crate::core::strategy::StrategyContext;

use super::{InsightPipe, exit_trigger::ExitTrigger};

/// Closes or partially closes an insight when its executable quote or simulated bar reaches take profit.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - None.
///
/// Behaviour:
/// Passes through insights without take-profit levels. Native OCO protection is left to the
/// broker. Otherwise, native live feeds use bid for buy insights and ask for sell insights;
/// missing/stale quotes defer. Backtests and bar-only feeds retain low/high history semantics.
/// Multiple levels close half the remaining quantity when partial closes are
/// supported; without partial-close support, the first hit closes the remaining position.
pub struct BasicTakeProfitPipe;

impl BasicTakeProfitPipe {
    pub fn new() -> Self {
        Self
    }
}

impl InsightPipe for BasicTakeProfitPipe {
    fn version(&self) -> &str {
        // The continuation/receipt layout is unchanged; existing pending closes
        // must remain resumable after this decision-source fix.
        "1.0"
    }

    fn run(&mut self, ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        let Some(tp_levels) = insight.take_profit_levels() else {
            if insight.has_pending_close() {
                return InsightPipeResult::from_exit_mutation(insight.close(ctx), self.name());
            }
            return InsightPipeResult::new(
                true,
                true,
                Some("Insight does not have take profit set".to_string()),
                self.name().to_string(),
            );
        };
        let Some(current_tp) = tp_levels.first().copied() else {
            if insight.has_pending_close() {
                return InsightPipeResult::from_exit_mutation(insight.close(ctx), self.name());
            }
            return InsightPipeResult::new(true, true, None, self.name().to_string());
        };
        let native_oco = ctx.has_broker_feature(BrokerFeature::OcoOrder);
        let partial_close_supported = ctx.has_broker_feature(BrokerFeature::PartialClose);
        if !insight.has_pending_close()
            && native_oco
            && (tp_levels.len() == 1 || !partial_close_supported)
        {
            return InsightPipeResult::new(
                true,
                true,
                Some("Take profit is managed by the broker's native OCO protection".to_string()),
                self.name().to_string(),
            );
        }
        if insight.closing && !insight.has_pending_close() {
            return InsightPipeResult::new(
                false,
                true,
                Some("Insight is already being closed".to_string()),
                self.name().to_string(),
            );
        }

        let should_close = if insight.has_pending_close() {
            // Completion belongs to the original decision. Missing/new market
            // data must not prevent it from being consumed on a deferred resume.
            true
        } else {
            match ExitTrigger::TakeProfit.reached(ctx, insight, current_tp) {
                Ok(reached) => reached,
                Err(error) => return InsightPipeResult::from_read_error(error, self.name()),
            }
        };

        if should_close {
            if tp_levels.len() > 1 {
                if !partial_close_supported {
                    return InsightPipeResult::from_exit_mutation(insight.close(ctx), self.name());
                }
                let quantity_to_close = ctx
                    .tools()
                    .quantity_round(insight.remaining_quantity() / 2.0, &insight.symbol);
                match insight.try_close_partial(ctx, quantity_to_close, None) {
                    Ok(crate::core::insight::InsightMutationState::Applied) => {}
                    pending_or_error => {
                        return InsightPipeResult::from_mutation(pending_or_error, self.name());
                    }
                }
                insight.set_take_profit_levels(Some(tp_levels.into_iter().skip(1).collect()));
                return InsightPipeResult::new(
                    false,
                    true,
                    Some(format!("Partial take profit hit for {}", insight.symbol)),
                    self.name().to_string(),
                );
            }

            return InsightPipeResult::from_exit_mutation(insight.close(ctx), self.name());
        }

        InsightPipeResult::new(true, true, None, self.name().to_string())
    }
}
