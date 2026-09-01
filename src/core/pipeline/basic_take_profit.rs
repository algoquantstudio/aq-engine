use crate::core::broker::{BrokerFeature, types::OrderSide};
use crate::core::insight::Insight;
use crate::core::pipeline::InsightPipeResult;
use crate::core::strategy::StrategyContext;

use super::InsightPipe;

/// Closes or partially closes an active insight when the latest candle reaches take profit.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - None.
///
/// Behaviour:
/// Passes through insights without take-profit levels. Native OCO protection is left to the
/// broker. Otherwise, it reads the latest `low` and `high` from strategy history: buy insights
/// trigger on high at or above the first take-profit level, and sell insights trigger on low at
/// or below it. Multiple levels close half the remaining quantity when partial closes are
/// supported; without partial-close support, the first hit closes the remaining position.
pub struct BasicTakeProfitPipe;

impl BasicTakeProfitPipe {
    pub fn new() -> Self {
        Self
    }
}

impl InsightPipe for BasicTakeProfitPipe {
    fn version(&self) -> &str {
        "1.0"
    }

    fn run(&mut self, ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        let Some(tp_levels) = insight.take_profit_levels() else {
            return InsightPipeResult::new(
                true,
                true,
                Some("Insight does not have take profit set".to_string()),
                self.name().to_string(),
            );
        };
        let Some(current_tp) = tp_levels.first().copied() else {
            return InsightPipeResult::new(true, true, None, self.name().to_string());
        };
        let native_oco = ctx.has_broker_feature(BrokerFeature::OcoOrder);
        let partial_close_supported = ctx.has_broker_feature(BrokerFeature::PartialClose);
        if native_oco && (tp_levels.len() == 1 || !partial_close_supported) {
            return InsightPipeResult::new(
                true,
                true,
                Some("Take profit is managed by the broker's native OCO protection".to_string()),
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

        let Some(df) = ctx.history().get(&insight.symbol) else {
            return InsightPipeResult::new(
                false,
                false,
                Some("No history found".to_string()),
                self.name().to_string(),
            );
        };
        let row = df.tail(Some(1));
        let low = row
            .column("low")
            .ok()
            .and_then(|c| c.f64().ok())
            .and_then(|c| c.get(0));
        let high = row
            .column("high")
            .ok()
            .and_then(|c| c.f64().ok())
            .and_then(|c| c.get(0));
        let should_close = match insight.side {
            OrderSide::Buy => high.map(|v| v >= current_tp).unwrap_or(false),
            OrderSide::Sell => low.map(|v| v <= current_tp).unwrap_or(false),
        };

        if should_close {
            if tp_levels.len() > 1 {
                if !partial_close_supported {
                    insight.close(ctx);
                    return InsightPipeResult::new(
                        false,
                        true,
                        Some(format!(
                            "Take profit hit for {}; broker does not support partial closes, so the remaining position was closed",
                            insight.symbol
                        )),
                        self.name().to_string(),
                    );
                }
                let quantity_to_close = ctx
                    .tools()
                    .quantity_round(insight.remaining_quantity() / 2.0, &insight.symbol);
                if !insight.try_close_partial(ctx, quantity_to_close, None) {
                    return InsightPipeResult::new(
                        false,
                        false,
                        Some(format!(
                            "Broker rejected partial take profit for {}",
                            insight.symbol
                        )),
                        self.name().to_string(),
                    );
                }
                insight.set_take_profit_levels(Some(tp_levels.into_iter().skip(1).collect()));
                return InsightPipeResult::new(
                    false,
                    true,
                    Some(format!("Partial take profit hit for {}", insight.symbol)),
                    self.name().to_string(),
                );
            }

            insight.close(ctx);
            return InsightPipeResult::new(
                false,
                true,
                Some(format!("Take profit hit for {}", insight.symbol)),
                self.name().to_string(),
            );
        }

        InsightPipeResult::new(true, true, None, self.name().to_string())
    }
}
