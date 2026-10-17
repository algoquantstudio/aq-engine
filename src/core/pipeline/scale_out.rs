use crate::core::broker::{
    BrokerFeature,
    types::{BrokerError, OrderSide},
};
use crate::core::insight::Insight;
use crate::core::pipeline::InsightPipeResult;
use crate::core::strategy::StrategyContext;

use super::InsightPipe;

/// Scales out filled insights as quote price reaches stop-loss or take-profit levels.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - `sell_off_percentage`: Fraction of remaining quantity to close at each non-final level,
///   clamped to `0.0..=1.0` when finite. Zero and non-finite fractions do not trigger closes.
///
/// Behaviour:
/// Runs only on filled insights with remaining quantity. It resolves the latest quote price,
/// checks the first stop-loss and take-profit levels, and issues a partial close at the hit
/// level. Broker-native OCO handles final levels. Non-final levels use partial closes where the
/// broker supports them; otherwise AQE waits for native final protection or closes the remaining
/// position when no native OCO is available.
pub struct ScaleOutPipe {
    sell_off_percentage: f64,
}

impl ScaleOutPipe {
    pub fn new(sell_off_percentage: f64) -> Self {
        Self {
            sell_off_percentage: if sell_off_percentage.is_finite() {
                sell_off_percentage.clamp(0.0, 1.0)
            } else {
                sell_off_percentage
            },
        }
    }

    fn quote_price(
        &self,
        ctx: &dyn StrategyContext,
        insight: &Insight,
    ) -> Result<f64, BrokerError> {
        let quote = self.get_latest_quote(ctx, &insight.symbol)?;
        if quote.symbol != insight.symbol || !quote.valid_market_values() {
            return Err(BrokerError::SnapshotUnavailable {
                kind: crate::core::broker::snapshots::SnapshotKind::Quote {
                    symbol: insight.symbol.clone(),
                },
                reason: crate::core::broker::snapshots::SnapshotUnavailable::Invalid,
            });
        }
        Ok(match insight.side {
            OrderSide::Buy => quote.bid,
            OrderSide::Sell => quote.ask,
        })
    }
}

impl InsightPipe for ScaleOutPipe {
    fn version(&self) -> &str {
        "1.0"
    }

    fn run(&mut self, ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        if !insight.has_pending_close()
            && (!self.sell_off_percentage.is_finite() || self.sell_off_percentage <= 0.0)
        {
            return InsightPipeResult::new(
                false,
                false,
                Some("Scale-out fraction must be finite and greater than zero".into()),
                self.name().into(),
            );
        }
        if insight.state != crate::core::insight::types::InsightState::Filled {
            return InsightPipeResult::new(
                true,
                true,
                Some("Insight is not filled".to_string()),
                self.name().to_string(),
            );
        }

        if insight.closing && !insight.has_pending_close() {
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

        let current_price = match insight
            .pending_close_price()
            .map(Ok)
            .unwrap_or_else(|| self.quote_price(ctx, insight))
        {
            Ok(price) => price,
            Err(error) => {
                return InsightPipeResult::from_read_error(error, self.name());
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
        let native_oco = ctx.has_broker_feature(BrokerFeature::OcoOrder);
        let partial_close_supported = ctx.has_broker_feature(BrokerFeature::PartialClose);

        if native_oco && (is_final_tp || is_final_sl) {
            return InsightPipeResult::new(
                true,
                true,
                Some(format!(
                    "Final {} for {} is managed by the broker's native OCO protection",
                    level_kind, insight.symbol
                )),
                self.name().to_string(),
            );
        }

        if native_oco && !partial_close_supported {
            return InsightPipeResult::new(
                true,
                true,
                Some(format!(
                    "Intermediate {} for {} requires partial-close support; waiting for broker-managed final protection",
                    level_kind, insight.symbol
                )),
                self.name().to_string(),
            );
        }

        let close_qty = if is_final_tp || is_final_sl || !partial_close_supported {
            remaining_qty
        } else {
            let Some(asset) = self.get_asset(ctx, &insight.symbol) else {
                return InsightPipeResult::new(
                    false,
                    false,
                    Some("Asset not found".into()),
                    self.name().into(),
                );
            };
            let scaled = crate::core::utils::quantity_sizing::floor_quantity(
                remaining_qty * self.sell_off_percentage,
                crate::core::utils::quantity_sizing::AssetQuantityLimits::from_asset(asset),
            );
            if scaled <= 0.0 {
                return InsightPipeResult::new(
                    true,
                    true,
                    Some("Scale-out quantity is below the asset quantity step".into()),
                    self.name().into(),
                );
            } else {
                scaled.min(remaining_qty)
            }
        };

        match insight.try_close_partial(ctx, close_qty, Some(level_price)) {
            Ok(crate::core::insight::InsightMutationState::Applied) => {}
            pending_or_error => {
                return InsightPipeResult::from_mutation(pending_or_error, self.name());
            }
        }

        if sl_hit.is_none() && tp_hit.is_some() && !tp_levels.is_empty() {
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
                "Scale out triggered on {} for {} qty {:.4} @ {:.4}{}",
                level_kind,
                insight.symbol,
                close_qty,
                level_price,
                if partial_close_supported {
                    ""
                } else {
                    "; broker does not support partial closes, so the remaining position was closed"
                }
            )),
            self.name().to_string(),
        )
    }
}
