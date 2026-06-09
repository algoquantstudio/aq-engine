use crate::core::insight::Insight;
use crate::core::pipeline::InsightPipeResult;
use crate::core::strategy::StrategyContext;

use super::InsightPipe;

/// Rejects insights whose latest reward-to-risk ratio is below the configured minimum.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - `minimum_rr`: Optional minimum reward-to-risk ratio to write into the strategy context
///   before evaluating the insight. When `None`, the current context value is used.
///
/// Behaviour:
/// Resolves entry from limit/stop price or the latest quote, then requires stop-loss and
/// take-profit levels to be present. The final take-profit level is compared with stop-loss
/// distance to calculate reward-to-risk. Insights below `ctx.min_reward_risk_ratio()` are marked
/// rejected; passing insights are left unchanged.
pub struct MinimumRiskToRewardPipe {
    minimum_rr: Option<f64>,
}

impl MinimumRiskToRewardPipe {
    pub fn new(minimum_rr: Option<f64>) -> Self {
        Self { minimum_rr }
    }
}

impl InsightPipe for MinimumRiskToRewardPipe {
    fn version(&self) -> &str {
        "1.0"
    }

    fn run(&mut self, ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        if let Some(minimum_rr) = self.minimum_rr {
            ctx.set_min_reward_risk_ratio(minimum_rr);
        }

        let entry = if let Some(entry) = insight.limit_price.or(insight.stop_price) {
            entry
        } else if let Ok(quote) = self.get_latest_quote(ctx, &insight.symbol) {
            match insight.side {
                crate::core::broker::types::OrderSide::Buy => quote.ask,
                crate::core::broker::types::OrderSide::Sell => quote.bid,
            }
        } else {
            0.0
        };

        if entry <= 0.0 || insight.stop_loss().is_none() || insight.take_profit_levels().is_none() {
            return InsightPipeResult::new(
                false,
                false,
                Some(
                    "Insight does not have limit price, stop loss or take profit set.".to_string(),
                ),
                self.name().to_string(),
            );
        }

        let minimum_rr = ctx.min_reward_risk_ratio();
        if minimum_rr <= 0.0 {
            return InsightPipeResult::new(
                false,
                false,
                Some("Minimum reward risk ratio is not configured".to_string()),
                self.name().to_string(),
            );
        }

        let stop_loss = insight.stop_loss().unwrap_or(entry);
        let take_profit = insight
            .take_profit_levels
            .clone()
            .and_then(|levels| levels.last().copied())
            .unwrap_or(entry);
        let risk = (entry - stop_loss).abs();
        let reward = (take_profit - entry).abs();
        let rr = if risk > 0.0 { reward / risk } else { 0.0 };
        if rr < minimum_rr {
            insight.order_rejected(&format!(
                "Risk to reward ratio {:.2} is below minimum {:.2}",
                rr, minimum_rr
            ));
            return InsightPipeResult::new(
                false,
                true,
                Some("Risk to reward ratio below minimum".to_string()),
                self.name().to_string(),
            );
        }

        InsightPipeResult::new(true, true, None, self.name().to_string())
    }
}
