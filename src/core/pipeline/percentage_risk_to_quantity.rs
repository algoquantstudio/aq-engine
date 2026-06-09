use crate::core::insight::Insight;
use crate::core::pipeline::InsightPipeResult;
use crate::core::pipeline::quantity_sizing::{
    AssetQuantityLimits, DEFAULT_RISK_PERCENT, account_size_from_equity_or_cash,
    apply_max_order_size_with_child_legs, entry_price, normalised_percent_rate, split_summary,
    validate_stop_for_side,
};
use crate::core::strategy::StrategyContext;

use super::InsightPipe;

/// Sizes an insight from a configured percentage of account equity at risk.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - `risk_percent`: Percentage of account size to risk. Values greater than `1` are treated as
///   percentages, values `0` or below use the built-in default, and values `0..=1` are treated as
///   already-normalised rates.
///
/// Behaviour:
/// Requires an entry price, side-valid stop loss, asset definition, and account snapshot. Account
/// size is taken from equity when available, otherwise cash or buying power. The pipe converts
/// `risk_percent` into a risk amount, divides by entry-to-stop distance, rounds quantity with
/// strategy tools, sets the parent insight quantity, and creates child insights when the target
/// size exceeds the asset maximum single-entry quantity.
pub struct PercentageRiskToQuantityPipe {
    pub risk_percent: i64,
}

impl PercentageRiskToQuantityPipe {
    pub fn new(risk_percent: i64) -> Self {
        Self { risk_percent }
    }

    fn risk_rate(&self) -> f64 {
        normalised_percent_rate(self.risk_percent, DEFAULT_RISK_PERCENT)
    }
}

impl InsightPipe for PercentageRiskToQuantityPipe {
    fn version(&self) -> &str {
        "1.0"
    }

    fn run(&mut self, ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        let Some(asset) = self.get_asset(ctx, &insight.symbol) else {
            return InsightPipeResult::new(
                false,
                false,
                Some(format!("Asset {} not found", insight.symbol)),
                self.name().to_string(),
            );
        };
        let limits = AssetQuantityLimits::from_asset(asset);

        let Ok(account) = ctx.account() else {
            return InsightPipeResult::new(
                false,
                false,
                Some("Failed to load account".to_string()),
                self.name().to_string(),
            );
        };

        let account_size = account_size_from_equity_or_cash(&account);
        if account_size <= 0.0 {
            return InsightPipeResult::new(
                false,
                false,
                Some("Account size must be positive".to_string()),
                self.name().to_string(),
            );
        }

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
            Err(message) => {
                return InsightPipeResult::new(
                    false,
                    false,
                    Some(message),
                    self.name().to_string(),
                );
            }
        };

        let stop_loss = match insight.stop_loss() {
            Some(value) if value > 0.0 => value,
            Some(_) => {
                return InsightPipeResult::new(
                    false,
                    false,
                    Some("Stop loss must be positive before position sizing".to_string()),
                    self.name().to_string(),
                );
            }
            None => {
                return InsightPipeResult::new(
                    false,
                    false,
                    Some("Stop loss must be set before position sizing".to_string()),
                    self.name().to_string(),
                );
            }
        };

        if let Err(message) = validate_stop_for_side(insight, entry, stop_loss) {
            return InsightPipeResult::new(false, false, Some(message), self.name().to_string());
        }

        let risk_per_unit = (entry - stop_loss).abs();
        if risk_per_unit <= f64::EPSILON {
            return InsightPipeResult::new(
                false,
                false,
                Some("Entry and stop loss cannot be the same price".to_string()),
                self.name().to_string(),
            );
        }

        let risk_amount = account_size * self.risk_rate();
        let target_quantity = ctx
            .tools()
            .quantity_round(risk_amount / risk_per_unit, &insight.symbol);
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

        let total_quantity = split.total_quantity();
        let actual_risk_amount = risk_per_unit * total_quantity;
        let actual_risk_percent = (actual_risk_amount / account_size) * 100.0;

        InsightPipeResult::new(
            true,
            true,
            Some(format!(
                "Position sized: qty={:.4}{}, risk={:.2} ({:.2}%)",
                total_quantity,
                split_summary(&split),
                actual_risk_amount,
                actual_risk_percent
            )),
            self.name().to_string(),
        )
    }
}
