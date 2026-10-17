use crate::core::insight::Insight;
use crate::core::pipeline::InsightPipeResult;
use crate::core::strategy::StrategyContext;
use crate::core::utils::quantity_sizing::{
    AssetQuantityLimits, apply_max_order_size_with_child_legs, contract_multiplier, entry_price,
    round_whole_or_fractional_quantity, split_summary, validate_stop_for_side,
};

use super::InsightPipe;

/// Sizes an insight to use the full available account buying power or cash.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - None.
///
/// Behaviour:
/// Requires an entry price, stop loss, asset definition, and account snapshot. It uses effective
/// buying power, including leverage only when the broker has not already included it and the asset
/// is marginable. The pipe divides available capital by the entry price and contract multiplier,
/// rounds down according to whole/fractional asset rules, sets the parent quantity, and creates child
/// insights when the target size exceeds the asset maximum single-entry quantity.
pub struct FullAccountQuantityToRiskPipe;

impl FullAccountQuantityToRiskPipe {
    pub fn new() -> Self {
        Self
    }
}

impl InsightPipe for FullAccountQuantityToRiskPipe {
    fn version(&self) -> &str {
        "1.0"
    }

    fn run(&mut self, ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        let entry = match entry_price(ctx, insight) {
            Ok(value) if value.is_finite() && value > 0.0 => value,
            Ok(_) => {
                return InsightPipeResult::new(
                    false,
                    false,
                    Some("Entry price must be positive".to_string()),
                    self.name().to_string(),
                );
            }
            Err(error) => return InsightPipeResult::from_read_error(error, self.name()),
        };
        if insight.stop_loss().is_none() {
            return InsightPipeResult::new(
                false,
                false,
                Some("Insight does not have stop loss set.".to_string()),
                self.name().to_string(),
            );
        }
        if let Err(message) = validate_stop_for_side(insight, entry, insight.stop_loss().unwrap()) {
            return InsightPipeResult::new(false, false, Some(message), self.name().into());
        }

        let Some(asset) = self.get_asset(ctx, &insight.symbol) else {
            return InsightPipeResult::new(
                false,
                false,
                Some(format!("Asset {} not found", insight.symbol)),
                self.name().to_string(),
            );
        };
        let limits = AssetQuantityLimits::from_asset(asset);
        let fractional = asset.fractional;
        let account = match ctx.account() {
            Ok(account) => account,
            Err(error) => return InsightPipeResult::from_read_error(error, self.name()),
        };

        let capital = ctx.effective_buying_power(&account, asset);
        if !capital.is_finite() || capital <= 0.0 {
            return InsightPipeResult::new(
                false,
                false,
                Some("Available capital must be positive".to_string()),
                self.name().to_string(),
            );
        }

        let target_quantity = match round_whole_or_fractional_quantity(
            ctx,
            &insight.symbol,
            capital / (entry * contract_multiplier(asset)),
            fractional,
        ) {
            Ok(quantity) => quantity,
            Err(message) => {
                insight.order_rejected(&message);
                return InsightPipeResult::new(false, true, Some(message), self.name().to_string());
            }
        };

        let split =
            match apply_max_order_size_with_child_legs(ctx, insight, target_quantity, limits) {
                Ok(result) => result,
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

        insight.set_quantity(Some(split.parent_quantity));
        InsightPipeResult::new(
            true,
            true,
            Some(format!(
                "Quantity set to {:.4}{}",
                split.total_quantity(),
                split_summary(&split)
            )),
            self.name().to_string(),
        )
    }
}
