use crate::core::broker::types::OrderSide;
use crate::core::insight::Insight;
use crate::core::pipeline::InsightPipeResult;
use crate::core::strategy::StrategyContext;

use super::InsightPipe;

pub struct ScaleOutPipe {
    sell_off_percentage: f64,
}

impl ScaleOutPipe {
    pub fn new(sell_off_percentage: f64) -> Self {
        Self {
            sell_off_percentage: sell_off_percentage.clamp(0.0, 1.0),
        }
    }

    fn quote_price(&self, ctx: &dyn StrategyContext, insight: &Insight) -> Result<f64, String> {
        let quote = self.get_latest_quote(ctx, &insight.symbol)?;
        quote
            .last
            .or(Some(match insight.side {
                OrderSide::Buy => quote.bid,
                OrderSide::Sell => quote.ask,
            }))
            .filter(|price| price.is_finite())
            .ok_or_else(|| format!("No valid quote price available for {}", insight.symbol))
    }
}

impl InsightPipe for ScaleOutPipe {
    fn version(&self) -> &str {
        "1.0"
    }

    fn run(&mut self, ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        if insight.state != crate::core::insight::types::InsightState::Filled {
            return InsightPipeResult::new(
                true,
                true,
                Some("Insight is not filled".to_string()),
                self.name().to_string(),
            );
        }

        if insight.closing {
            return InsightPipeResult::new(
                false,
                true,
                Some("Insight is already closing".to_string()),
                self.name().to_string(),
            );
        }

        let remaining_qty = insight.remaining_quantity();
        if remaining_qty <= 0.0 {
            return InsightPipeResult::new(
                false,
                true,
                Some("No remaining quantity to scale out".to_string()),
                self.name().to_string(),
            );
        }

        let current_price = match self.quote_price(ctx, insight) {
            Ok(price) => price,
            Err(error) => {
                return InsightPipeResult::new(false, false, Some(error), self.name().to_string());
            }
        };

        let mut tp_levels = insight.take_profit_levels().unwrap_or_default();
        let mut sl_levels = insight.stop_loss_levels().unwrap_or_default();

        let tp_hit = tp_levels
            .first()
            .copied()
            .filter(|level| match insight.side {
                OrderSide::Buy => current_price >= *level,
                OrderSide::Sell => current_price <= *level,
            });

        let sl_hit = sl_levels
            .first()
            .copied()
            .filter(|level| match insight.side {
                OrderSide::Buy => current_price <= *level,
                OrderSide::Sell => current_price >= *level,
            });

        let (level_kind, level_price) = if let Some(level) = sl_hit {
            ("stop loss", level)
        } else if let Some(level) = tp_hit {
            ("take profit", level)
        } else {
            return InsightPipeResult::new(true, true, None, self.name().to_string());
        };

        let is_final_tp = tp_hit.is_some() && tp_levels.len() == 1;
        let is_final_sl = sl_hit.is_some() && sl_levels.len() == 1;
        let close_qty = if is_final_tp || is_final_sl {
            remaining_qty
        } else {
            let scaled = ctx
                .tools()
                .quantity_round(remaining_qty * self.sell_off_percentage, &insight.symbol);
            if scaled <= 0.0 {
                remaining_qty
            } else {
                scaled.min(remaining_qty)
            }
        };

        insight.close_partial(ctx, close_qty, Some(level_price));

        if tp_hit.is_some() && !tp_levels.is_empty() {
            tp_levels.remove(0);
            insight.set_take_profit_levels(Some(tp_levels));
        }
        if sl_hit.is_some() && !sl_levels.is_empty() {
            sl_levels.remove(0);
            insight.set_stop_loss_levels(Some(sl_levels));
        }

        InsightPipeResult::new(
            true,
            false,
            Some(format!(
                "Scale out triggered on {} for {} qty {:.4} @ {:.4}",
                level_kind, insight.symbol, close_qty, level_price
            )),
            self.name().to_string(),
        )
    }
}
