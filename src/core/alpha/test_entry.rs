use crate::core::alpha::{AlphaModel, AlphaResult};
use crate::core::broker::types::{Asset, OrderSide};
use crate::core::indicators::atr::AverageTrueRange;
use crate::core::insight::{Insight, types::StrategyType};
use crate::core::strategy::StrategyContext;

pub struct TestEntry {
    atr_period: usize,
    atr_column: String,
    limit_entries: bool,
    max_spawn: usize,
    base_confidence_modifier_field: Option<String>,
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
            Some(base_confidence_modifier_field)
        };

        Self {
            atr_period,
            atr_column: format!("ATRr_{}", atr_period),
            limit_entries,
            max_spawn,
            base_confidence_modifier_field: modifier,
        }
    }

    fn latest_numeric(df: &polars::prelude::DataFrame, col: &str) -> Result<f64, String> {
        let series = df
            .column(col)
            .map_err(|e| format!("Failed to find column '{}': {}", col, e))?
            .f64()
            .map_err(|e| format!("Column '{}' is not Float64: {}", col, e))?;

        series
            .get(df.height().saturating_sub(1))
            .ok_or_else(|| format!("Latest value for '{}' is null", col))
    }

    fn symbol_spawn_count_key(&self, symbol: &str) -> String {
        format!(
            "{}_{}_spawn_count",
            self.name().to_lowercase(),
            symbol.to_lowercase()
        )
    }

    fn confidence_for_symbol(&self, ctx: &dyn StrategyContext, symbol: &str) -> Result<u8, String> {
        let mut confidence = ctx.base_confidence();
        if let Some(field) = &self.base_confidence_modifier_field {
            let history = ctx
                .history()
                .get(symbol)
                .ok_or_else(|| format!("No history found for {}", symbol))?;
            confidence *= Self::latest_numeric(history, field)?.abs();
        }

        if confidence <= 0.0 {
            return Err("Base Confidence is 0.".to_string());
        }

        Ok((confidence * 100.0).round().clamp(0.0, 100.0) as u8)
    }
}

impl AlphaModel for TestEntry {
    fn version(&self) -> &str {
        "1.0"
    }

    fn start(&mut self, ctx: &mut dyn StrategyContext) {
        ctx.register_indicator(Box::new(AverageTrueRange::new(self.atr_period)));
        ctx.set_warm_up_bars(self.atr_period as i32);
    }

    fn init(&mut self, ctx: &mut dyn StrategyContext, asset: &Asset) {
        ctx.variables().insert(
            self.symbol_spawn_count_key(&asset.symbol),
            serde_json::Value::from(0u64),
        );
    }

    fn generate_insights(&mut self, ctx: &mut dyn StrategyContext, symbol: &str) -> AlphaResult {
        let confidence = match self.confidence_for_symbol(ctx, symbol) {
            Ok(value) => value,
            Err(message) => {
                return AlphaResult::new(None, true, Some(message), self.name().to_string());
            }
        };

        let spawn_key = self.symbol_spawn_count_key(symbol);

        // Reset the per-symbol spawn counter once no insight for this symbol remains active.
        let active_for_symbol = ctx.insights().values().any(|insight| {
            insight.symbol() == symbol && insight.strategy_type().to_string() == self.name()
        });
        if !active_for_symbol {
            ctx.variables()
                .insert(spawn_key.clone(), serde_json::Value::from(0u64));
        }

        let spawn_count = self
            .get_variables(ctx)
            .get(&spawn_key)
            .and_then(|value| value.value().as_u64())
            .unwrap_or(0) as usize;

        if spawn_count >= self.max_spawn {
            return AlphaResult::new(
                None,
                true,
                Some("Insight already exists.".to_string()),
                self.name().to_string(),
            );
        }

        let latest_bar = match self.get_latest_bar(ctx, symbol) {
            Ok(bar) => bar,
            Err(message) => {
                return AlphaResult::new(None, true, Some(message), self.name().to_string());
            }
        };
        let _previous_bar = match self.get_previos_bar(ctx, symbol) {
            Ok(bar) => bar,
            Err(message) => {
                return AlphaResult::new(None, true, Some(message), self.name().to_string());
            }
        };

        let close = match Self::latest_numeric(&latest_bar, "close") {
            Ok(value) => value,
            Err(message) => {
                return AlphaResult::new(None, false, Some(message), self.name().to_string());
            }
        };
        let open = match Self::latest_numeric(&latest_bar, "open") {
            Ok(value) => value,
            Err(message) => {
                return AlphaResult::new(None, false, Some(message), self.name().to_string());
            }
        };
        let atr = match Self::latest_numeric(&latest_bar, &self.atr_column) {
            Ok(value) => value,
            Err(message) => {
                return AlphaResult::new(None, false, Some(message), self.name().to_string());
            }
        };

        let Some(asset) = self.get_asset(ctx, symbol) else {
            return AlphaResult::new(
                None,
                false,
                Some(format!("Asset {} not found in universe", symbol)),
                self.name().to_string(),
            );
        };

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
                    tools.dynamic_round(close + (atr * 2.0), symbol),
                    tools.dynamic_round(close - (atr * 1.5), symbol),
                    entry,
                )
            } else if asset.shortable {
                (
                    OrderSide::Sell,
                    tools.dynamic_round(close - (atr * 2.0), symbol),
                    tools.dynamic_round(close + (atr * 1.5), symbol),
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
            .set_period_unfilled(Some(5))
            .set_period_till_tp(Some(10));
        ctx.variables()
            .insert(spawn_key, serde_json::Value::from((spawn_count + 1) as u64));

        AlphaResult::new(Some(insight), true, None, self.name().to_string())
    }
}
