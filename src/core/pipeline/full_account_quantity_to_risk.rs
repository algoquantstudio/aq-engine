use crate::core::insight::Insight;
use crate::core::pipeline::InsightPipeResult;
use crate::core::strategy::StrategyContext;

use super::InsightPipe;

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
        if insight.stop_loss().is_none() {
            return InsightPipeResult::new(
                false,
                false,
                Some("Insight does not have stop loss set.".to_string()),
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

        let capital = if asset.marginable {
            account.buying_power
        } else {
            account.cash
        };
        let mut quantity = capital / entry;

        if quantity > 1.0 {
            quantity = quantity.floor();
        } else if asset.fractional {
            quantity = ctx.tools().quantity_round(quantity, &insight.symbol);
        }

        if let Some(min_order_size) = asset.min_order_size {
            if quantity < min_order_size {
                insight.order_rejected(&format!(
                    "Quantity {:.4} is below minimum order size {:.4}",
                    quantity, min_order_size
                ));
                return InsightPipeResult::new(
                    false,
                    true,
                    Some("Quantity below minimum order size".to_string()),
                    self.name().to_string(),
                );
            }
        }

        insight.set_quantity(Some(quantity));
        InsightPipeResult::new(
            true,
            true,
            Some(format!("Quantity set to {:.4}", quantity)),
            self.name().to_string(),
        )
    }
}
