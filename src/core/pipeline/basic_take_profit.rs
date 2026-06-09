use crate::core::broker::types::OrderSide;
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
/// Passes through insights without take-profit levels. For open insights, it reads the latest
/// `low` and `high` from strategy history: buy insights trigger on high at or above the first
/// take-profit level, and sell insights trigger on low at or below it. Multiple take-profit
/// levels close half the current quantity; a final level closes the remaining insight.
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
                let quantity_to_close = ctx
                    .tools()
                    .quantity_round(insight.quantity.unwrap_or(0.0) / 2.0, &insight.symbol);
                insight.close_partial(ctx, quantity_to_close, None);
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
