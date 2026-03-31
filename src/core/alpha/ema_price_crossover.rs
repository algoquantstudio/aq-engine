use crate::core::alpha::{AlphaModel, AlphaResult};
use crate::core::broker::types::{Asset, OrderSide};
use crate::core::indicators::{atr::AverageTrueRange, ema::ExponentialMovingAverage};
use crate::core::insight::{Insight, types::StrategyDependentConfirmation, types::StrategyType};
use crate::core::strategy::StrategyContext;

pub struct EmaPriceCrossover {
    atr_period: usize,
    ema_period: usize,
    atr_column: String,
    ema_column: String,
    base_confidence_modifier_field: Option<String>,
}

impl EmaPriceCrossover {
    pub fn new(
        atr_period: usize,
        ema_period: usize,
        base_confidence_modifier_field: String,
    ) -> Self {
        Self {
            atr_period,
            ema_period,
            atr_column: format!("ATRr_{}", atr_period),
            ema_column: format!("EMA_{}", ema_period),
            base_confidence_modifier_field: (!base_confidence_modifier_field.trim().is_empty())
                .then_some(base_confidence_modifier_field),
        }
    }

    fn latest_value(
        df: &polars::prelude::DataFrame,
        column: &str,
        offset: usize,
    ) -> Result<f64, String> {
        let idx = df
            .height()
            .checked_sub(1 + offset)
            .ok_or_else(|| "Not enough rows".to_string())?;
        df.column(column)
            .map_err(|e| format!("Missing column '{}': {}", column, e))?
            .f64()
            .map_err(|e| format!("Column '{}' is not Float64: {}", column, e))?
            .get(idx)
            .ok_or_else(|| format!("Value for '{}' is null", column))
    }

    fn confidence_for_symbol(&self, ctx: &dyn StrategyContext, symbol: &str) -> Result<u8, String> {
        let mut confidence = ctx.base_confidence();
        if let Some(field) = &self.base_confidence_modifier_field {
            let history = ctx
                .history()
                .get(symbol)
                .ok_or_else(|| format!("No history for {}", symbol))?;
            confidence *= Self::latest_value(history, field, 0)?.abs();
        }
        if confidence <= 0.0 {
            return Err("Base Confidence is 0.".to_string());
        }
        Ok((confidence * 100.0).round().clamp(0.0, 100.0) as u8)
    }
}

impl AlphaModel for EmaPriceCrossover {
    fn version(&self) -> &str {
        "0.1"
    }

    fn start(&mut self, ctx: &mut dyn StrategyContext) {
        ctx.register_indicator(Box::new(AverageTrueRange::new(self.atr_period)));
        ctx.register_indicator(Box::new(ExponentialMovingAverage::new(
            self.ema_period,
            "close",
        )));
        ctx.set_warm_up_bars(self.atr_period.max(self.ema_period) as i32);
    }

    fn init(&mut self, _ctx: &mut dyn StrategyContext, _asset: &Asset) {}

    fn generate_insights(&mut self, ctx: &mut dyn StrategyContext, symbol: &str) -> AlphaResult {
        let Some(asset) = self.get_asset(ctx, symbol) else {
            return AlphaResult::new(
                None,
                false,
                Some(format!("Asset {} not found", symbol)),
                self.name().to_string(),
            );
        };
        let history = match self.get_history(ctx, symbol) {
            Some(history) if history.height() >= 2 => history,
            _ => {
                return AlphaResult::new(
                    None,
                    true,
                    Some("Not enough history".to_string()),
                    self.name().to_string(),
                );
            }
        };

        let confidence = match self.confidence_for_symbol(ctx, symbol) {
            Ok(value) => value,
            Err(message) => {
                return AlphaResult::new(None, true, Some(message), self.name().to_string());
            }
        };

        let latest_close = match Self::latest_value(history, "close", 0) {
            Ok(v) => v,
            Err(e) => return AlphaResult::new(None, false, Some(e), self.name().to_string()),
        };
        let latest_high = match Self::latest_value(history, "high", 0) {
            Ok(v) => v,
            Err(e) => return AlphaResult::new(None, false, Some(e), self.name().to_string()),
        };
        let latest_low = match Self::latest_value(history, "low", 0) {
            Ok(v) => v,
            Err(e) => return AlphaResult::new(None, false, Some(e), self.name().to_string()),
        };
        let previous_high = match Self::latest_value(history, "high", 1) {
            Ok(v) => v,
            Err(e) => return AlphaResult::new(None, false, Some(e), self.name().to_string()),
        };
        let previous_low = match Self::latest_value(history, "low", 1) {
            Ok(v) => v,
            Err(e) => return AlphaResult::new(None, false, Some(e), self.name().to_string()),
        };
        let latest_atr = match Self::latest_value(history, &self.atr_column, 0) {
            Ok(v) => v,
            Err(e) => return AlphaResult::new(None, false, Some(e), self.name().to_string()),
        };
        let latest_ema = match Self::latest_value(history, &self.ema_column, 0) {
            Ok(v) => v,
            Err(e) => return AlphaResult::new(None, false, Some(e), self.name().to_string()),
        };
        let previous_ema = match Self::latest_value(history, &self.ema_column, 1) {
            Ok(v) => v,
            Err(e) => return AlphaResult::new(None, false, Some(e), self.name().to_string()),
        };

        let long_signal = latest_ema < latest_close
            && previous_ema > previous_high
            && (latest_close - latest_ema).abs() < latest_atr;
        let short_signal = asset.shortable
            && latest_ema > latest_close
            && previous_ema < previous_low
            && (latest_ema - latest_close).abs() < latest_atr;

        let (side, tp, sl, entry) = if long_signal {
            (
                OrderSide::Buy,
                ctx.tools()
                    .dynamic_round(latest_high + (latest_atr * 3.5), symbol),
                ctx.tools().dynamic_round(
                    (previous_low - latest_atr).max(latest_ema - latest_atr * 1.5),
                    symbol,
                ),
                ctx.tools().dynamic_round(latest_ema, symbol),
            )
        } else if short_signal {
            (
                OrderSide::Sell,
                ctx.tools()
                    .dynamic_round(latest_low - (latest_atr * 3.5), symbol),
                ctx.tools().dynamic_round(
                    (previous_high + latest_atr).min(latest_ema + latest_atr * 1.5),
                    symbol,
                ),
                ctx.tools().dynamic_round(latest_ema, symbol),
            )
        } else {
            return AlphaResult::new(None, true, None, self.name().to_string());
        };

        let ttluf = ctx
            .tools()
            .calculate_time_to_live(latest_close, entry, latest_atr, 0)
            .max(1);
        let ttlf = ctx
            .tools()
            .calculate_time_to_live(tp, entry, latest_atr, 0)
            .max(1);
        let mut insight = Insight::new(
            side,
            symbol.to_string(),
            StrategyType::Custom(self.name().to_string()),
            ctx.timeframe().clone(),
            confidence,
            None,
        );
        insight
            .set_limit_price(Some(entry))
            .set_take_profit_levels(Some(vec![tp]))
            .set_stop_loss(Some(sl))
            .set_period_unfilled(Some(ttluf as u32))
            .set_period_till_tp(Some(ttlf as u32))
            .set_execution_depends(vec![StrategyDependentConfirmation::None]);
        AlphaResult::new(Some(insight), true, None, self.name().to_string())
    }
}
