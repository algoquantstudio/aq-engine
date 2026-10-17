use super::{
    BuiltinAlphaOptions,
    options::{self, EntryGate},
};
use crate::core::alpha::{AlphaModel, AlphaResult};
use crate::core::broker::types::{Asset, OrderSide};
use crate::core::indicators::atr::AverageTrueRange;
use crate::core::insight::{Insight, types::StrategyType};
use crate::core::strategy::StrategyContext;

/// Generates deterministic test insights from the latest candle direction.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - `atr_period`: Number of bars used by the registered ATR indicator.
/// - `limit_entries`: Whether generated insights should use a limit price at the latest close.
/// - `max_spawn`: Maximum number of active testing insights allowed per symbol.
/// - `base_confidence_modifier_field`: Optional history column whose latest absolute value
///   scales `ctx.base_confidence()` before it is converted to insight confidence.
///
/// Behaviour:
/// Registers ATR during `start`, initialises a per-symbol spawn counter in strategy variables,
/// and keeps that counter synchronised with active testing insights. A bullish latest candle
/// creates a buy insight; a bearish candle creates a sell insight only when the asset is
/// shortable. ATR is used to place basic stop-loss and take-profit levels, and successful
/// generation increments the stored spawn counter.
pub struct TestEntry {
    atr_period: usize,
    atr_column: String,
    limit_entries: bool,
    max_spawn: usize,
    base_confidence_modifier_field: Option<String>,
    options: BuiltinAlphaOptions,
    gate: EntryGate,
}

impl TestEntry {
    pub fn new(
        atr_period: usize,
        limit_entries: bool,
        max_spawn: usize,
        base_confidence_modifier_field: String,
    ) -> Self {
        let modifier = if base_confidence_modifier_field.trim().is_empty() {
            None
        } else {
            Some(base_confidence_modifier_field.trim().to_string())
        };

        Self {
            atr_period,
            atr_column: format!("ATRr_{}", atr_period),
            limit_entries,
            max_spawn,
            base_confidence_modifier_field: modifier,
            options: BuiltinAlphaOptions {
                take_profit_atr: 2.0,
                ..Default::default()
            },
            gate: EntryGate::default(),
        }
    }

    pub fn with_options(mut self, options: BuiltinAlphaOptions) -> Self {
        self.options = options;
        self
    }

    fn validate(&self) -> Result<(), String> {
        options::validate_period("ATR period", self.atr_period)?;
        self.options.validate()
    }

    fn symbol_spawn_count_key(&self, symbol: &str) -> String {
        format!(
            "{}_{}_spawn_count",
            self.name().to_lowercase(),
            symbol.to_lowercase()
        )
    }

    fn active_spawn_count(&self, ctx: &dyn StrategyContext, symbol: &str) -> usize {
        options::active_roots(ctx, symbol, "TestEntry")
    }

    fn sync_symbol_spawn_count(&self, ctx: &dyn StrategyContext, symbol: &str) -> usize {
        let spawn_count = self.active_spawn_count(ctx, symbol);
        ctx.variables().insert(
            self.symbol_spawn_count_key(symbol),
            serde_json::Value::from(spawn_count as u64),
        );
        spawn_count
    }
}

impl AlphaModel for TestEntry {
    fn version(&self) -> &str {
        "1.1"
    }

    fn start(&mut self, ctx: &mut dyn StrategyContext) {
        self.gate.clear();
        if self.validate().is_err() {
            return;
        }
        ctx.register_indicator(Box::new(AverageTrueRange::new(self.atr_period)));
        ctx.set_warm_up_bars(ctx.warm_up_bars().max(self.atr_period.max(2) as i32));
    }

    fn init(&mut self, ctx: &mut dyn StrategyContext, asset: &Asset) {
        ctx.variables().insert(
            self.symbol_spawn_count_key(&asset.symbol),
            serde_json::Value::from(0u64),
        );
    }

    fn generate_insights(&mut self, ctx: &mut dyn StrategyContext, symbol: &str) -> AlphaResult {
        if let Err(message) = self.validate() {
            return AlphaResult::new(None, false, Some(message), self.name().into());
        }
        if !self.gate.allows(ctx, symbol, "TestEntry", &self.options) {
            return AlphaResult::new(None, true, None, self.name().into());
        }
        let spawn_count = self.sync_symbol_spawn_count(ctx, symbol);

        let spawn_key = self.symbol_spawn_count_key(symbol);

        if spawn_count >= self.max_spawn {
            return AlphaResult::new(
                None,
                true,
                Some("Insight already exists.".to_string()),
                self.name().to_string(),
            );
        }

        let history = match self.get_history(ctx, symbol) {
            Some(history) if history.height() >= self.atr_period.max(2) => history,
            _ => {
                return AlphaResult::new(
                    None,
                    true,
                    Some("Not enough history".into()),
                    self.name().into(),
                );
            }
        };
        let confidence =
            match options::confidence(ctx, history, self.base_confidence_modifier_field.as_deref())
            {
                Ok(value) => value,
                Err(message) => {
                    return AlphaResult::new(None, true, Some(message), self.name().into());
                }
            };

        let close = match options::numeric(history, "close", 0) {
            Ok(value) => value,
            Err(message) => {
                return AlphaResult::new(None, false, Some(message), self.name().to_string());
            }
        };
        let open = match options::numeric(history, "open", 0) {
            Ok(value) => value,
            Err(message) => {
                return AlphaResult::new(None, false, Some(message), self.name().to_string());
            }
        };
        let atr = match options::numeric(history, &self.atr_column, 0) {
            Ok(value) => value,
            Err(message) => {
                return AlphaResult::new(None, false, Some(message), self.name().to_string());
            }
        };
        if close <= 0.0 || open <= 0.0 || atr <= 0.0 {
            return AlphaResult::new(
                None,
                true,
                Some("Waiting for positive prices and ATR".into()),
                self.name().into(),
            );
        }

        let Some(asset) = self.get_asset(ctx, symbol) else {
            return AlphaResult::new(
                None,
                false,
                Some(format!("Asset {} not found in universe", symbol)),
                self.name().to_string(),
            );
        };
        if !asset.tradable || close == open {
            return AlphaResult::new(None, true, None, self.name().into());
        }
        let signal_side = if close > open {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        if !self.options.allows(signal_side, asset.shortable) {
            return AlphaResult::new(None, true, None, self.name().into());
        }

        let (side, tp, sl, entry) = {
            let tools = ctx.tools();
            let entry = if self.limit_entries {
                Some(tools.dynamic_round(close, symbol))
            } else {
                None
            };

            if close > open {
                (
                    OrderSide::Buy,
                    tools.dynamic_round(close + (atr * self.options.take_profit_atr), symbol),
                    tools.dynamic_round(close - (atr * self.options.stop_loss_atr), symbol),
                    entry,
                )
            } else if asset.shortable {
                (
                    OrderSide::Sell,
                    tools.dynamic_round(close - (atr * self.options.take_profit_atr), symbol),
                    tools.dynamic_round(close + (atr * self.options.stop_loss_atr), symbol),
                    entry,
                )
            } else {
                return AlphaResult::new(
                    None,
                    true,
                    Some("Asset is not shortable.".to_string()),
                    self.name().to_string(),
                );
            }
        };

        let priced_entry = entry.unwrap_or(close);
        if !options::valid_levels(&side, priced_entry, sl, tp) {
            return AlphaResult::new(
                None,
                true,
                Some("ATR levels are invalid after price rounding".into()),
                self.name().into(),
            );
        }
        let mut insight = Insight::new(
            side,
            symbol.to_string(),
            StrategyType::Testing,
            ctx.timeframe().clone(),
            confidence,
            None,
        );
        insight
            .set_limit_price(entry)
            .set_take_profit_levels(Some(vec![tp]))
            .set_stop_loss(Some(sl))
            .set_period_unfilled(Some(self.options.unfilled_ttl(5)))
            .set_period_till_tp(Some(self.options.filled_ttl(10)));
        ctx.variables()
            .insert(spawn_key, serde_json::Value::from((spawn_count + 1) as u64));
        self.gate.emitted(symbol);

        AlphaResult::new(Some(insight), true, None, self.name().to_string())
    }
}
