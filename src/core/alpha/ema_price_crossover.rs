use super::{
    BuiltinAlphaOptions,
    options::{self, EntryGate},
};
use crate::core::alpha::{AlphaModel, AlphaResult};
use crate::core::broker::types::{Asset, OrderSide};
use crate::core::indicators::{atr::AverageTrueRange, ema::ExponentialMovingAverage};
use crate::core::insight::{Insight, types::StrategyDependentConfirmation, types::StrategyType};
use crate::core::strategy::StrategyContext;

/// Generates limit-entry insights when price crosses back through an EMA near ATR support.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - `atr_period`: Number of bars used by the registered ATR indicator.
/// - `ema_period`: Number of bars used by the registered EMA indicator on `close`.
/// - `base_confidence_modifier_field`: Optional history column whose latest absolute value
///   scales `ctx.base_confidence()` before it is converted to insight confidence.
///
/// Behaviour:
/// Registers ATR and EMA indicators during `start`, preserving enough warm-up for the
/// previous EMA, then reads the latest and previous price and indicator values.
/// A buy insight is emitted when price is above EMA after the previous high was below the EMA;
/// a sell insight is emitted only for shortable assets under the inverse condition. Entry,
/// stop loss, take profit, and expiry periods are rounded with strategy tools before the
/// insight is returned.
pub struct EmaPriceCrossover {
    atr_period: usize,
    ema_period: usize,
    atr_column: String,
    ema_column: String,
    base_confidence_modifier_field: Option<String>,
    options: BuiltinAlphaOptions,
    max_entry_distance_atr: f64,
    swing_stop_atr: f64,
    gate: EntryGate,
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
                .then(|| base_confidence_modifier_field.trim().to_string()),
            options: BuiltinAlphaOptions::default(),
            max_entry_distance_atr: 1.0,
            swing_stop_atr: 1.0,
            gate: EntryGate::default(),
        }
    }

    pub fn with_options(mut self, options: BuiltinAlphaOptions) -> Self {
        self.options = options;
        self
    }

    /// Maximum close-to-EMA entry distance and previous-candle stop buffer, in ATR units.
    pub fn with_signal_options(mut self, max_entry_distance_atr: f64, swing_stop_atr: f64) -> Self {
        self.max_entry_distance_atr = max_entry_distance_atr;
        self.swing_stop_atr = swing_stop_atr;
        self
    }

    fn validate(&self) -> Result<(), String> {
        options::validate_period("ATR period", self.atr_period)?;
        options::validate_period("EMA period", self.ema_period)?;
        self.options.validate()?;
        if !self.max_entry_distance_atr.is_finite()
            || self.max_entry_distance_atr <= 0.0
            || !self.swing_stop_atr.is_finite()
            || self.swing_stop_atr <= 0.0
        {
            return Err(
                "EMA entry distance and swing stop buffer must be finite and positive".into(),
            );
        }
        Ok(())
    }
}

impl AlphaModel for EmaPriceCrossover {
    fn version(&self) -> &str {
        "0.2"
    }

    fn start(&mut self, ctx: &mut dyn StrategyContext) {
        self.gate.clear();
        if self.validate().is_err() {
            return;
        }
        ctx.register_indicator(Box::new(AverageTrueRange::new(self.atr_period)));
        ctx.register_indicator(Box::new(ExponentialMovingAverage::new(
            self.ema_period,
            "close",
        )));
        ctx.set_warm_up_bars(
            ctx.warm_up_bars()
                .max(self.atr_period.max(self.ema_period + 1) as i32),
        );
    }

    fn init(&mut self, _ctx: &mut dyn StrategyContext, _asset: &Asset) {}

    fn generate_insights(&mut self, ctx: &mut dyn StrategyContext, symbol: &str) -> AlphaResult {
        if let Err(message) = self.validate() {
            return AlphaResult::new(None, false, Some(message), self.name().into());
        }
        if !self
            .gate
            .allows(ctx, symbol, "EmaPriceCrossover", &self.options)
        {
            return AlphaResult::new(None, true, None, self.name().into());
        }
        let Some(asset) = self.get_asset(ctx, symbol) else {
            return AlphaResult::new(
                None,
                false,
                Some(format!("Asset {} not found", symbol)),
                self.name().to_string(),
            );
        };
        let history = match self.get_history(ctx, symbol) {
            Some(history) if history.height() >= self.atr_period.max(self.ema_period + 1) => {
                history
            }
            _ => {
                return AlphaResult::new(
                    None,
                    true,
                    Some("Not enough history".to_string()),
                    self.name().to_string(),
                );
            }
        };

        let confidence =
            match options::confidence(ctx, history, self.base_confidence_modifier_field.as_deref())
            {
                Ok(value) => value,
                Err(message) => {
                    return AlphaResult::new(None, true, Some(message), self.name().to_string());
                }
            };

        let latest_close = match options::numeric(history, "close", 0) {
            Ok(v) => v,
            Err(e) => return AlphaResult::new(None, false, Some(e), self.name().to_string()),
        };
        let latest_high = match options::numeric(history, "high", 0) {
            Ok(v) => v,
            Err(e) => return AlphaResult::new(None, false, Some(e), self.name().to_string()),
        };
        let latest_low = match options::numeric(history, "low", 0) {
            Ok(v) => v,
            Err(e) => return AlphaResult::new(None, false, Some(e), self.name().to_string()),
        };
        let previous_high = match options::numeric(history, "high", 1) {
            Ok(v) => v,
            Err(e) => return AlphaResult::new(None, false, Some(e), self.name().to_string()),
        };
        let previous_low = match options::numeric(history, "low", 1) {
            Ok(v) => v,
            Err(e) => return AlphaResult::new(None, false, Some(e), self.name().to_string()),
        };
        let latest_atr = match options::numeric(history, &self.atr_column, 0) {
            Ok(v) => v,
            Err(e) => return AlphaResult::new(None, false, Some(e), self.name().to_string()),
        };
        let latest_ema = match options::numeric(history, &self.ema_column, 0) {
            Ok(v) => v,
            Err(e) => return AlphaResult::new(None, false, Some(e), self.name().to_string()),
        };
        let previous_ema = match options::numeric(history, &self.ema_column, 1) {
            Ok(v) => v,
            Err(e) => return AlphaResult::new(None, false, Some(e), self.name().to_string()),
        };

        if !asset.tradable
            || [
                latest_close,
                latest_high,
                latest_low,
                previous_high,
                previous_low,
                latest_atr,
                latest_ema,
                previous_ema,
            ]
            .into_iter()
            .any(|v| v <= 0.0)
            || latest_low > latest_close
            || latest_high < latest_close
            || previous_low > previous_high
        {
            return AlphaResult::new(
                None,
                true,
                Some("Waiting for valid prices and ATR".into()),
                self.name().into(),
            );
        }
        let long_signal = self.options.allow_long
            && latest_ema < latest_close
            && previous_ema > previous_high
            && (latest_close - latest_ema).abs() < latest_atr * self.max_entry_distance_atr;
        let short_signal = self.options.allow_short
            && asset.shortable
            && latest_ema > latest_close
            && previous_ema < previous_low
            && (latest_ema - latest_close).abs() < latest_atr * self.max_entry_distance_atr;

        if !long_signal && !short_signal {
            return AlphaResult::new(None, true, None, self.name().to_string());
        }

        let tools = ctx.tools();
        let (side, tp, sl, entry) = if long_signal {
            (
                OrderSide::Buy,
                tools.dynamic_round(
                    latest_high + (latest_atr * self.options.take_profit_atr),
                    symbol,
                ),
                tools.dynamic_round(
                    (previous_low - latest_atr * self.swing_stop_atr)
                        .max(latest_ema - latest_atr * self.options.stop_loss_atr),
                    symbol,
                ),
                tools.dynamic_round(latest_ema, symbol),
            )
        } else {
            (
                OrderSide::Sell,
                tools.dynamic_round(
                    latest_low - (latest_atr * self.options.take_profit_atr),
                    symbol,
                ),
                tools.dynamic_round(
                    (previous_high + latest_atr * self.swing_stop_atr)
                        .min(latest_ema + latest_atr * self.options.stop_loss_atr),
                    symbol,
                ),
                tools.dynamic_round(latest_ema, symbol),
            )
        };

        if !options::valid_levels(&side, entry, sl, tp) {
            return AlphaResult::new(
                None,
                true,
                Some("ATR levels are invalid after price rounding".into()),
                self.name().into(),
            );
        }
        let ttluf = tools
            .calculate_time_to_live(latest_close, entry, latest_atr, 0)
            .max(1);
        let ttlf = tools
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
            .set_period_unfilled(Some(self.options.unfilled_ttl(ttluf as u32)))
            .set_period_till_tp(Some(self.options.filled_ttl(ttlf as u32)))
            .set_execution_depends(vec![StrategyDependentConfirmation::None]);
        self.gate.emitted(symbol);
        AlphaResult::new(Some(insight), true, None, self.name().to_string())
    }
}
