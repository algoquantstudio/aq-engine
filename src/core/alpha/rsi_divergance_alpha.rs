use crate::core::alpha::{AlphaModel, AlphaResult};
use crate::core::broker::types::{Asset, OrderSide};
use crate::core::indicators::{atr::AverageTrueRange, rsi::RelativeStrengthIndex};
use crate::core::insight::{Insight, types::StrategyDependentConfirmation, types::StrategyType};
use crate::core::strategy::StrategyContext;

pub struct RsiDiverganceAlpha {
    local_window: usize,
    divergance_window: usize,
    atr_period: usize,
    rsi_period: usize,
    atr_column: String,
    rsi_column: String,
    base_confidence_modifier_field: Option<String>,
}

impl RsiDiverganceAlpha {
    pub fn new(
        local_window: usize,
        divergance_window: usize,
        atr_period: usize,
        rsi_period: usize,
        base_confidence_modifier_field: String,
    ) -> Self {
        Self {
            local_window,
            divergance_window,
            atr_period,
            rsi_period,
            atr_column: format!("ATRr_{}", atr_period),
            rsi_column: format!("RSI_{}", rsi_period),
            base_confidence_modifier_field: (!base_confidence_modifier_field.trim().is_empty())
                .then_some(base_confidence_modifier_field),
        }
    }

    fn series_values(df: &polars::prelude::DataFrame, column: &str) -> Result<Vec<f64>, String> {
        Ok(df
            .column(column)
            .map_err(|e| format!("Missing column '{}': {}", column, e))?
            .f64()
            .map_err(|e| format!("Column '{}' is not Float64: {}", column, e))?
            .into_iter()
            .flatten()
            .collect())
    }

    fn aligned_indicator_window<'a>(
        base: &'a [f64],
        indicator_column: &polars::prelude::Float64Chunked,
        column_name: &str,
    ) -> Result<(&'a [f64], Vec<f64>), String> {
        let indicator_values: Vec<Option<f64>> = indicator_column.into_iter().collect();
        let first_valid_index = indicator_values
            .iter()
            .position(|value| value.is_some())
            .ok_or_else(|| format!("No values for {}", column_name))?;

        if base.len() < first_valid_index {
            return Err(format!(
                "Column '{}' is misaligned with base series",
                column_name
            ));
        }

        let aligned_base = &base[first_valid_index..];
        let aligned_indicator = indicator_values[first_valid_index..]
            .iter()
            .map(|value| {
                value.ok_or_else(|| {
                    format!(
                        "Unexpected null in aligned indicator series '{}'",
                        column_name
                    )
                })
            })
            .collect::<Result<Vec<f64>, String>>()?;

        Ok((aligned_base, aligned_indicator))
    }

    fn confidence_for_symbol(&self, ctx: &dyn StrategyContext, symbol: &str) -> Result<u8, String> {
        let mut confidence = ctx.base_confidence();
        if let Some(field) = &self.base_confidence_modifier_field {
            let history = ctx
                .history()
                .get(symbol)
                .ok_or_else(|| format!("No history for {}", symbol))?;
            let last = Self::series_values(history, field)?
                .last()
                .copied()
                .ok_or_else(|| format!("No values for {}", field))?;
            confidence *= last.abs();
        }
        if confidence <= 0.0 {
            return Err("Base Confidence is 0.".to_string());
        }
        Ok((confidence * 100.0).round().clamp(0.0, 100.0) as u8)
    }

    fn local_low_pivots(values: &[f64], local_window: usize) -> Vec<usize> {
        if values.len() < local_window.saturating_mul(2) + 1 {
            return Vec::new();
        }

        let first_index = local_window;
        let last_confirmed_index = values.len().saturating_sub(local_window + 1);
        if first_index > last_confirmed_index {
            return Vec::new();
        }

        let mut pivots = Vec::new();
        for i in first_index..=last_confirmed_index {
            let from = i - local_window;
            let to = i + local_window;
            let current = values[i];
            let is_pivot = values[from..=to].iter().enumerate().all(|(offset, value)| {
                let idx = from + offset;
                idx == i || current < *value
            });
            if is_pivot {
                pivots.push(i);
            }
        }
        pivots
    }

    fn local_high_pivots(values: &[f64], local_window: usize) -> Vec<usize> {
        if values.len() < local_window.saturating_mul(2) + 1 {
            return Vec::new();
        }

        let first_index = local_window;
        let last_confirmed_index = values.len().saturating_sub(local_window + 1);
        if first_index > last_confirmed_index {
            return Vec::new();
        }

        let mut pivots = Vec::new();
        for i in first_index..=last_confirmed_index {
            let from = i - local_window;
            let to = i + local_window;
            let current = values[i];
            let is_pivot = values[from..=to].iter().enumerate().all(|(offset, value)| {
                let idx = from + offset;
                idx == i || current > *value
            });
            if is_pivot {
                pivots.push(i);
            }
        }
        pivots
    }

    fn lower_low_with_higher_rsi(
        lows: &[f64],
        rsi: &[f64],
        local_window: usize,
        divergance_window: usize,
    ) -> bool {
        if lows.len() != rsi.len() || lows.len() < local_window.saturating_mul(2) + 2 {
            return false;
        }

        let pivots = Self::local_low_pivots(lows, local_window);
        if pivots.len() < 2 {
            return false;
        }

        let last = pivots[pivots.len() - 1];
        let last_confirmed_index = lows.len().saturating_sub(local_window + 1);
        let Some(prev) = pivots[..pivots.len() - 1]
            .iter()
            .rev()
            .find(|&&pivot| last.saturating_sub(pivot) <= divergance_window)
            .copied()
        else {
            return false;
        };

        last == last_confirmed_index && lows[last] < lows[prev] && rsi[last] > rsi[prev]
    }

    fn higher_high_with_lower_rsi(
        highs: &[f64],
        rsi: &[f64],
        local_window: usize,
        divergance_window: usize,
    ) -> bool {
        if highs.len() != rsi.len() || highs.len() < local_window.saturating_mul(2) + 2 {
            return false;
        }

        let pivots = Self::local_high_pivots(highs, local_window);
        if pivots.len() < 2 {
            return false;
        }

        let last = pivots[pivots.len() - 1];
        let last_confirmed_index = highs.len().saturating_sub(local_window + 1);
        let Some(prev) = pivots[..pivots.len() - 1]
            .iter()
            .rev()
            .find(|&&pivot| last.saturating_sub(pivot) <= divergance_window)
            .copied()
        else {
            return false;
        };

        last == last_confirmed_index && highs[last] > highs[prev] && rsi[last] < rsi[prev]
    }
}

impl AlphaModel for RsiDiverganceAlpha {
    fn version(&self) -> &str {
        "0.2"
    }

    fn start(&mut self, ctx: &mut dyn StrategyContext) {
        ctx.register_indicator(Box::new(AverageTrueRange::new(self.atr_period)));
        ctx.register_indicator(Box::new(RelativeStrengthIndex::new(
            self.rsi_period,
            "close",
        )));
        ctx.set_warm_up_bars(self.atr_period.max(self.rsi_period) as i32);
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
            Some(history)
                if history.height() >= self.local_window.max(self.divergance_window).max(3) =>
            {
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
        let confidence = match self.confidence_for_symbol(ctx, symbol) {
            Ok(value) => value,
            Err(message) => {
                return AlphaResult::new(None, true, Some(message), self.name().to_string());
            }
        };

        let highs = match Self::series_values(history, "high") {
            Ok(v) => v,
            Err(e) => return AlphaResult::new(None, false, Some(e), self.name().to_string()),
        };
        let lows = match Self::series_values(history, "low") {
            Ok(v) => v,
            Err(e) => return AlphaResult::new(None, false, Some(e), self.name().to_string()),
        };
        let closes = match Self::series_values(history, "close") {
            Ok(v) => v,
            Err(e) => return AlphaResult::new(None, false, Some(e), self.name().to_string()),
        };
        let atr_values = match Self::series_values(history, &self.atr_column) {
            Ok(v) => v,
            Err(e) => return AlphaResult::new(None, false, Some(e), self.name().to_string()),
        };
        let rsi_column = match history.column(&self.rsi_column) {
            Ok(column) => match column.f64() {
                Ok(values) => values,
                Err(e) => {
                    return AlphaResult::new(
                        None,
                        false,
                        Some(format!(
                            "Column '{}' is not Float64: {}",
                            self.rsi_column, e
                        )),
                        self.name().to_string(),
                    );
                }
            },
            Err(e) => {
                return AlphaResult::new(
                    None,
                    false,
                    Some(format!("Missing column '{}': {}", self.rsi_column, e)),
                    self.name().to_string(),
                );
            }
        };
        let (aligned_lows, rsi_values) =
            match Self::aligned_indicator_window(&lows, rsi_column, &self.rsi_column) {
                Ok(values) => values,
                Err(e) => return AlphaResult::new(None, true, Some(e), self.name().to_string()),
            };
        let (aligned_highs, aligned_rsi_for_highs) =
            match Self::aligned_indicator_window(&highs, rsi_column, &self.rsi_column) {
                Ok(values) => values,
                Err(e) => return AlphaResult::new(None, true, Some(e), self.name().to_string()),
            };

        let latest_close = *closes.last().unwrap_or(&0.0);
        let latest_atr = *atr_values.last().unwrap_or(&0.0);
        if latest_atr <= 0.0 {
            return AlphaResult::new(
                None,
                true,
                Some("ATR is not ready".to_string()),
                self.name().to_string(),
            );
        }

        let bullish = Self::lower_low_with_higher_rsi(
            aligned_lows,
            &rsi_values,
            self.local_window,
            self.divergance_window,
        );
        let bearish = asset.shortable
            && Self::higher_high_with_lower_rsi(
                aligned_highs,
                &aligned_rsi_for_highs,
                self.local_window,
                self.divergance_window,
            );

        let (side, entry, tp, sl) = if bullish {
            let entry = ctx
                .tools()
                .dynamic_round(*highs.last().unwrap_or(&latest_close), symbol);
            (
                OrderSide::Buy,
                entry,
                ctx.tools().dynamic_round(entry + latest_atr * 3.5, symbol),
                ctx.tools().dynamic_round(entry - latest_atr * 1.5, symbol),
            )
        } else if bearish {
            let entry = ctx
                .tools()
                .dynamic_round(*lows.last().unwrap_or(&latest_close), symbol);
            (
                OrderSide::Sell,
                entry,
                ctx.tools().dynamic_round(entry - latest_atr * 3.5, symbol),
                ctx.tools().dynamic_round(entry + latest_atr * 1.5, symbol),
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
            .set_execution_depends(vec![
                StrategyDependentConfirmation::LowRelativeVolumeConfirmationModel,
            ]);
        AlphaResult::new(Some(insight), true, None, self.name().to_string())
    }
}
