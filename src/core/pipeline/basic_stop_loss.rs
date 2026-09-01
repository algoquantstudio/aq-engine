use crate::core::broker::{BrokerFeature, types::OrderSide};
use crate::core::insight::Insight;
use crate::core::pipeline::InsightPipeResult;
use crate::core::strategy::StrategyContext;

use super::InsightPipe;

/// Closes an active insight when the latest candle breaches its stop loss.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - None.
///
/// Behaviour:
/// Passes through insights without a stop loss and leaves native OCO protection to the broker.
/// For brokers without OCO, it reads the latest `low` and `high` from strategy history: buy
/// insights close when low is at or below stop loss, and sell insights close when high is at or
/// above stop loss. Insights already being closed do not issue another close request.
pub struct BasicStopLossPipe;

impl BasicStopLossPipe {
    pub fn new() -> Self {
        Self
    }
}

impl InsightPipe for BasicStopLossPipe {
    fn version(&self) -> &str {
        "1.0"
    }

    fn run(&mut self, ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
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
            OrderSide::Buy => low.map(|v| v <= stop_loss).unwrap_or(false),
            OrderSide::Sell => high.map(|v| v >= stop_loss).unwrap_or(false),
        };

        if should_close {
            insight.close(ctx);
            return InsightPipeResult::new(
                false,
                true,
                Some(format!("Price broke stop loss for {}", insight.symbol)),
                self.name().to_string(),
            );
        }

        InsightPipeResult::new(true, true, None, self.name().to_string())
    }
}
