use crate::core::insight::Insight;
use crate::core::pipeline::InsightPipeResult;
use crate::core::pipeline::quantity_sizing::{
    AssetQuantityLimits, apply_max_order_size_with_child_legs, entry_price,
    round_whole_or_fractional_quantity, split_summary,
};
use crate::core::strategy::StrategyContext;

use super::InsightPipe;

/// Sizes an insight from the strategy's configured execution risk.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - `maximum_cost_basis`: Upper bound on the working capital used for the position.
/// - `minimum_cost_basis`: Minimum working capital required before the insight can be sized.
///
/// Behaviour:
/// Requires an unset quantity, entry price, stop loss, asset definition, and account snapshot.
/// Working capital comes from buying power for marginable assets or cash for non-marginable
/// assets, scaled by insight confidence, bounded by the configured cost-basis limits, and then
/// multiplied by `ctx.execution_risk()`. The pipe calculates quantity from stop-loss distance,
/// caps it by what working capital can buy, applies asset rounding/contract-size rules, sets the
/// parent insight quantity, and creates child insights when the target size exceeds the asset
/// maximum single-entry quantity.
pub struct DynamicQuantityToRiskPipe {
    maximum_cost_basis: f64,
    minimum_cost_basis: f64,
}

impl DynamicQuantityToRiskPipe {
    pub fn new(maximum_cost_basis: f64, minimum_cost_basis: f64) -> Self {
        Self {
            maximum_cost_basis,
            minimum_cost_basis,
        }
    }
}

impl InsightPipe for DynamicQuantityToRiskPipe {
    fn version(&self) -> &str {
        "1.0"
    }

    fn run(&mut self, ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        let entry = match entry_price(ctx, insight) {
            Ok(value) if value > 0.0 => value,
            Ok(_) => {
                return InsightPipeResult::new(
                    false,
                    false,
                    Some("Entry price must be positive".to_string()),
                    self.name().to_string(),
                );
            }
            Err(_) => {
                return InsightPipeResult::new(
                    false,
                    false,
                    Some("Insight does not have an entry price set.".to_string()),
                    self.name().to_string(),
                );
            }
        };
        let Some(stop_loss) = insight.stop_loss() else {
            return InsightPipeResult::new(
                false,
                false,
                Some("Insight does not have stop loss set.".to_string()),
                self.name().to_string(),
            );
        };
        if insight.quantity.is_some() {
            return InsightPipeResult::new(
                true,
                true,
                Some("Quantity already set".to_string()),
                self.name().to_string(),
            );
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
        let Ok(account) = ctx.account() else {
            return InsightPipeResult::new(
                false,
                false,
                Some("Failed to load account".to_string()),
                self.name().to_string(),
            );
        };

        let mut working_capital = if asset.marginable {
            account.buying_power
        } else {
            account.cash
        } * (insight.confidence as f64 / 100.0);

        if working_capital < self.minimum_cost_basis {
            insight.order_rejected(&format!(
                "Working capital {:.2} is below minimum cost basis {:.2}",
                working_capital, self.minimum_cost_basis
            ));
            return InsightPipeResult::new(
                false,
                true,
                Some("Working capital below minimum cost basis".to_string()),
                self.name().to_string(),
            );
        }

        if working_capital > self.maximum_cost_basis {
            working_capital = self.maximum_cost_basis;
        }

        let risk_per_share = (entry - stop_loss).abs();
        if risk_per_share <= 0.0 {
            return InsightPipeResult::new(
                false,
                false,
                Some("Risk per share must be positive".to_string()),
                self.name().to_string(),
            );
        }

        let account_size_at_risk = working_capital * ctx.execution_risk();
        let maximum_can_buy = working_capital / entry;
        let mut size_should_buy = (account_size_at_risk / risk_per_share).min(maximum_can_buy);

        size_should_buy = match round_whole_or_fractional_quantity(
            ctx,
            &insight.symbol,
            size_should_buy,
            fractional,
        ) {
            Ok(quantity) => quantity,
            Err(message) => {
                insight.order_rejected(&message);
                return InsightPipeResult::new(false, true, Some(message), self.name().to_string());
            }
        };

        if let Some(contract_size) = asset.contract_size {
            if contract_size > 0 {
                size_should_buy = (size_should_buy / contract_size as f64).round();
            }
        }

        let split =
            match apply_max_order_size_with_child_legs(ctx, insight, size_should_buy, limits) {
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
