use crate::core::broker::types::OrderSide;
use crate::core::insight::Insight;
use crate::core::insight::types::StrategyDependentConfirmation;
use crate::core::pipeline::InsightPipeResult;
use crate::core::strategy::StrategyContext;
use crate::core::utils::quantity_sizing::{
    AssetQuantityLimits, apply_max_order_size_with_child_legs, cap_asset_total_quantity,
    cap_quantity_levels, floor_quantity, validate_pending_tree_limits,
};

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
/// with side-specific confirmation dependencies. Every logical DCA level observes the asset's
/// maximum single-order quantity. Excess quantity is represented by nested split insights at the
/// same price; these execution fragments are not additional DCA levels.
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
        if !self.dca_percentage.is_finite() || self.dca_percentage <= 0.0 || self.dca_levels > 1024
        {
            return InsightPipeResult::new(
                false,
                false,
                Some("DCA requires a positive finite spacing and at most 1024 levels".into()),
                self.name().into(),
            );
        }
        if insight.state != crate::core::insight::types::InsightState::New
            || insight.submitted
            || insight.has_pending_submit()
            || insight.children.iter().any(|child| {
                child.submitted || child.has_pending_submit() || child.order_id.is_some()
            })
        {
            return InsightPipeResult::new(
                false,
                false,
                Some("DCA must be configured before submission".into()),
                self.name().into(),
            );
        }
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
                Err(error) => return InsightPipeResult::from_read_error(error, self.name()),
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

        let Some(asset) = self.get_asset(ctx, &insight.symbol).cloned() else {
            return InsightPipeResult::new(
                false,
                false,
                Some(format!(
                    "Asset {} is not in the strategy universe",
                    insight.symbol
                )),
                self.name().to_string(),
            );
        };
        let mut limits = AssetQuantityLimits::from_asset(&asset);

        // A quantity-sizing pipe may already have split the original order before this DCA pipe
        // runs. Restore that logical total and rebuild the execution fragments after distributing
        // it across the DCA levels. Ordinary children mean DCA has already been configured and
        // must not be duplicated.
        if insight
            .children
            .iter()
            .any(|child| !child.is_split_insight())
        {
            return InsightPipeResult::new(
                true,
                true,
                Some("Insight already has DCA child levels".to_string()),
                self.name().to_string(),
            );
        }
        let desired_quantity = insight.total_insight_quantity_size();
        let desired_quantity =
            match cap_asset_total_quantity(ctx, insight, desired_quantity, limits) {
                Ok(quantity) => quantity,
                Err(message) => {
                    return InsightPipeResult::new(
                        false,
                        true,
                        Some(message),
                        self.name().to_string(),
                    );
                }
            };
        // Cap the complete budget once; each level must not claim the same free capacity.
        limits.max_total_size = None;

        let mut price_levels = vec![entry];
        for level in 1..=self.dca_levels {
            let price = match insight.side {
                OrderSide::Buy => entry * (1.0 - self.dca_percentage * level as f64),
                OrderSide::Sell => entry * (1.0 + self.dca_percentage * level as f64),
            };
            price_levels.push(ctx.tools().dynamic_round(price, &insight.symbol));
        }

        if price_levels
            .iter()
            .any(|price| !price.is_finite() || *price <= 0.0)
        {
            return InsightPipeResult::new(
                false,
                true,
                Some("Invalid DCA price levels".into()),
                self.name().to_string(),
            );
        }
        let mut quantities = self.calculate_dca_quantities(desired_quantity, &price_levels);
        if let Err(message) = cap_quantity_levels(ctx, insight, &mut quantities, limits) {
            return InsightPipeResult::new(false, true, Some(message), self.name().to_string());
        }

        let dca_stop_loss = match insight.side {
            OrderSide::Buy => {
                price_levels.last().copied().unwrap_or(entry) - ((entry - stop_loss) / 2.0)
            }
            OrderSide::Sell => {
                price_levels.last().copied().unwrap_or(entry) + ((stop_loss - entry) / 2.0)
            }
        };
        let dca_stop_loss = ctx.tools().dynamic_round(dca_stop_loss, &insight.symbol);
        // Validate the entire plan before replacing existing execution fragments.
        if let Err(message) =
            crate::core::utils::quantity_sizing::validate_stop_for_side(insight, entry, stop_loss)
                .and_then(|_| {
                    crate::core::utils::quantity_sizing::validate_stop_for_side(
                        insight,
                        *price_levels.last().unwrap(),
                        dca_stop_loss,
                    )
                })
        {
            return InsightPipeResult::new(false, false, Some(message), self.name().into());
        }
        for quantity in &quantities {
            if let Err(message) =
                crate::core::utils::quantity_sizing::plan_quantity_parts(*quantity, limits, None)
            {
                return InsightPipeResult::new(false, false, Some(message), self.name().into());
            }
        }
        insight.children.retain(|child| !child.is_split_insight());
        insight.set_stop_loss(Some(dca_stop_loss));

        let parent_target_quantity = floor_quantity(quantities[0], limits);
        let parent_split = match apply_max_order_size_with_child_legs(
            ctx,
            insight,
            parent_target_quantity,
            limits,
        ) {
            Ok(split) => split,
            Err(message) => {
                insight.order_rejected(&message);
                return InsightPipeResult::new(false, true, Some(message), self.name().to_string());
            }
        };
        insight.set_quantity(Some(parent_split.parent_quantity));

        let mut split_fragment_count = parent_split.child_count;

        for (index, level_price) in price_levels.iter().enumerate().skip(1) {
            let quantity = quantities
                .get(index)
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
            let level_target_quantity = floor_quantity(quantity, limits);
            child
                .set_limit_price(Some(*level_price))
                .set_stop_loss_levels(insight.stop_loss_levels())
                .set_take_profit_levels(insight.take_profit_levels())
                .set_execution_depends(vec![match insight.side {
                    OrderSide::Buy => StrategyDependentConfirmation::UpStateConfirmationModel,
                    OrderSide::Sell => StrategyDependentConfirmation::DownStateConfirmationModel,
                }]);

            let level_split = match apply_max_order_size_with_child_legs(
                ctx,
                &mut child,
                level_target_quantity,
                limits,
            ) {
                Ok(split) => split,
                Err(message) => {
                    insight.order_rejected(&message);
                    return InsightPipeResult::new(
                        false,
                        true,
                        Some(message),
                        self.name().to_string(),
                    );
                }
            };
            child.set_quantity(Some(level_split.parent_quantity));
            split_fragment_count += level_split.child_count;
            insight.add_child_insight(child, None, ctx);
        }

        if let Err(message) = validate_pending_tree_limits(ctx, insight) {
            insight.state_log(Some(message.clone()));
            return InsightPipeResult::new(false, true, Some(message), self.name().to_string());
        }
        InsightPipeResult::new(
            true,
            true,
            Some(format!(
                "Added {} DCA levels to insight with {} broker-size split fragments",
                self.dca_levels, split_fragment_count
            )),
            self.name().to_string(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::utils::quantity_sizing::tests::{MockContext, sample_asset, sample_insight};

    fn execution_quantity(insight: &Insight) -> f64 {
        insight.quantity().unwrap_or_default()
            + insight
                .children
                .iter()
                .filter(|child| child.is_split_insight())
                .map(execution_quantity)
                .sum::<f64>()
    }

    #[test]
    fn splits_every_dca_level_at_the_broker_maximum() {
        let mut asset = sample_asset();
        asset.max_order_size = Some(40.0);
        let mut ctx = MockContext::new(asset);
        let mut insight = sample_insight();
        insight.set_quantity(Some(300.0));

        let result = PercentageDcaLevelsPipe::new(0.10, 2).run(&mut ctx, &mut insight);

        assert!(result.passed, "{:?}", result.message);
        assert!(insight.quantity().unwrap() <= 40.0);
        assert_eq!(
            insight
                .children
                .iter()
                .filter(|child| !child.is_split_insight())
                .count(),
            2
        );

        let logical_levels = std::iter::once(&insight).chain(
            insight
                .children
                .iter()
                .filter(|child| !child.is_split_insight()),
        );
        let mut total_quantity = 0.0;
        for level in logical_levels {
            assert!(level.quantity().unwrap() <= 40.0);
            assert!(
                level
                    .children
                    .iter()
                    .filter(|child| child.is_split_insight())
                    .all(|fragment| fragment.quantity().unwrap() <= 40.0)
            );
            assert!(
                level
                    .children
                    .iter()
                    .filter(|child| child.is_split_insight())
                    .all(|fragment| fragment.limit_price() == level.limit_price())
            );
            total_quantity += execution_quantity(level);
        }
        assert!(total_quantity <= 300.0 && total_quantity > 297.0);
    }

    #[test]
    fn redistributes_preexisting_split_quantity_without_counting_it_as_a_dca_level() {
        let mut asset = sample_asset();
        asset.max_order_size = Some(100.0);
        let limits = AssetQuantityLimits::from_asset(&asset);
        let mut ctx = MockContext::new(asset);
        let mut insight = sample_insight();

        let initial_split =
            apply_max_order_size_with_child_legs(&mut ctx, &mut insight, 250.0, limits).unwrap();
        insight.set_quantity(Some(initial_split.parent_quantity));
        assert_eq!(insight.children.len(), 2);

        let result = PercentageDcaLevelsPipe::new(0.05, 2).run(&mut ctx, &mut insight);

        assert!(result.passed, "{:?}", result.message);
        assert_eq!(
            insight
                .children
                .iter()
                .filter(|child| !child.is_split_insight())
                .count(),
            2
        );
        let total_quantity = execution_quantity(&insight)
            + insight
                .children
                .iter()
                .filter(|child| !child.is_split_insight())
                .map(execution_quantity)
                .sum::<f64>();
        assert!(total_quantity <= 250.0 && total_quantity > 247.0);
    }
}
