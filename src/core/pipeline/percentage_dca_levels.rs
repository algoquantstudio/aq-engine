use crate::core::broker::types::OrderSide;
use crate::core::insight::Insight;
use crate::core::insight::types::StrategyDependentConfirmation;
use crate::core::pipeline::InsightPipeResult;
use crate::core::strategy::StrategyContext;

use super::InsightPipe;

/// Splits a parent insight into percentage-spaced DCA child entries.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - `dca_percentage`: Fractional distance between each DCA price level and the original entry.
/// - `dca_levels`: Number of child DCA levels to create. Values below `1` are clamped to `1`.
///
/// Behaviour:
/// Requires a parent insight with quantity, an entry price, and a stop loss. The pipe builds DCA
/// price levels below entry for buys and above entry for sells, redistributes quantity across the
/// parent and children, adjusts the stop loss beyond the final DCA level, and adds child insights
/// with side-specific confirmation dependencies. Existing child insights are not used as DCA
/// parents.
pub struct PercentageDcaLevelsPipe {
    dca_percentage: f64,
    dca_levels: usize,
}

impl PercentageDcaLevelsPipe {
    pub fn new(dca_percentage: f64, dca_levels: i64) -> Self {
        Self {
            dca_percentage,
            dca_levels: dca_levels.max(1) as usize,
        }
    }

    fn calculate_dca_quantities(&self, total_quantity: f64, price_levels: &[f64]) -> Vec<f64> {
        let denominator: f64 = price_levels.iter().map(|price| 1.0 / price).sum();
        price_levels
            .iter()
            .map(|price| (total_quantity / denominator) * (1.0 / price))
            .collect()
    }
}

impl InsightPipe for PercentageDcaLevelsPipe {
    fn version(&self) -> &str {
        "1.0"
    }

    fn run(&mut self, ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        if insight.quantity.is_none() {
            return InsightPipeResult::new(
                true,
                true,
                Some("Insight does not have a desired quantity".to_string()),
                self.name().to_string(),
            );
        }
        if insight.parent_id().is_some() {
            return InsightPipeResult::new(
                true,
                true,
                Some("Insight is already a child insight".to_string()),
                self.name().to_string(),
            );
        }

        let entry = if let Some(entry) = insight.limit_price.or(insight.stop_price) {
            entry
        } else {
            match self.get_latest_quote(ctx, &insight.symbol) {
                Ok(quote) => match insight.side {
                    OrderSide::Buy => quote.ask,
                    OrderSide::Sell => quote.bid,
                },
                Err(_) => {
                    return InsightPipeResult::new(
                        false,
                        false,
                        Some("Insight does not have an entry price".to_string()),
                        self.name().to_string(),
                    );
                }
            }
        };
        let Some(stop_loss) = insight.stop_loss() else {
            return InsightPipeResult::new(
                false,
                false,
                Some("Insight does not have stop loss set".to_string()),
                self.name().to_string(),
            );
        };

        let desired_quantity = insight.quantity.unwrap_or(0.0);
        let mut price_levels = vec![entry];
        for level in 1..=self.dca_levels {
            let price = match insight.side {
                OrderSide::Buy => entry * (1.0 - self.dca_percentage * level as f64),
                OrderSide::Sell => entry * (1.0 + self.dca_percentage * level as f64),
            };
            price_levels.push(ctx.tools().dynamic_round(price, &insight.symbol));
        }

        let quantities = self.calculate_dca_quantities(desired_quantity, &price_levels);
        insight.set_quantity(Some(
            ctx.tools().quantity_round(quantities[0], &insight.symbol),
        ));

        let dca_stop_loss = match insight.side {
            OrderSide::Buy => {
                price_levels.last().copied().unwrap_or(entry) - ((entry - stop_loss) / 2.0)
            }
            OrderSide::Sell => {
                price_levels.last().copied().unwrap_or(entry) + ((stop_loss - entry) / 2.0)
            }
        };
        insight.set_stop_loss(Some(
            ctx.tools().dynamic_round(dca_stop_loss, &insight.symbol),
        ));

        for (index, level_price) in price_levels.iter().enumerate().skip(1) {
            let quantity_index = if insight.side == OrderSide::Buy {
                index
            } else {
                quantities.len().saturating_sub(index)
            };
            let quantity = quantities
                .get(quantity_index)
                .copied()
                .unwrap_or(*quantities.last().unwrap_or(&desired_quantity));
            let mut child = Insight::new(
                insight.side.clone(),
                insight.symbol.clone(),
                insight.strategy_type.clone(),
                *insight.timeframe(),
                insight.confidence,
                Some(*insight.insight_id()),
            );
            child
                .set_quantity(Some(ctx.tools().quantity_round(quantity, &insight.symbol)))
                .set_limit_price(Some(*level_price))
                .set_stop_loss_levels(insight.stop_loss_levels())
                .set_take_profit_levels(insight.take_profit_levels())
                .set_execution_depends(vec![match insight.side {
                    OrderSide::Buy => StrategyDependentConfirmation::UpStateConfirmationModel,
                    OrderSide::Sell => StrategyDependentConfirmation::DownStateConfirmationModel,
                }]);
            insight.add_child_insight(child, ctx);
        }

        InsightPipeResult::new(
            true,
            true,
            Some("Added DCA levels to insight".to_string()),
            self.name().to_string(),
        )
    }
}
