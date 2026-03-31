use crate::core::insight::Insight;
use crate::core::pipeline::InsightPipeResult;
use crate::core::strategy::StrategyContext;

use super::InsightPipe;

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
        let entry = if let Some(entry) = insight.limit_price.or(insight.stop_price) {
            entry
        } else {
            match self.get_latest_quote(ctx, &insight.symbol) {
                Ok(quote) => match insight.side {
                    crate::core::broker::types::OrderSide::Buy => quote.ask,
                    crate::core::broker::types::OrderSide::Sell => quote.bid,
                },
                Err(_) => {
                    return InsightPipeResult::new(
                        false,
                        false,
                        Some("Insight does not have an entry price set.".to_string()),
                        self.name().to_string(),
                    );
                }
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

        if size_should_buy > 1.0 {
            size_should_buy = size_should_buy.floor();
        } else if asset.fractional {
            size_should_buy = ctx.tools().quantity_round(size_should_buy, &insight.symbol);
        } else {
            insight.order_rejected(&format!(
                "Asset {} is not fractionable for quantity {:.4}",
                insight.symbol, size_should_buy
            ));
            return InsightPipeResult::new(
                false,
                true,
                Some("Asset is not fractionable".to_string()),
                self.name().to_string(),
            );
        }

        if let Some(contract_size) = asset.contract_size {
            if contract_size > 0 {
                size_should_buy = (size_should_buy / contract_size as f64).round();
            }
        }

        if let Some(min_order_size) = asset.min_order_size {
            if size_should_buy < min_order_size {
                insight.order_rejected(&format!(
                    "Quantity {:.4} is below minimum order size {:.4}",
                    size_should_buy, min_order_size
                ));
                return InsightPipeResult::new(
                    false,
                    true,
                    Some("Quantity below minimum order size".to_string()),
                    self.name().to_string(),
                );
            }
        }

        if let Some(max_order_size) = asset.max_order_size {
            size_should_buy = size_should_buy.min(max_order_size);
        }

        insight.set_quantity(Some(size_should_buy));
        InsightPipeResult::new(
            true,
            true,
            Some(format!("Quantity set to {:.4}", size_should_buy)),
            self.name().to_string(),
        )
    }
}
