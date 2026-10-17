//! Shared, backwards-compatible controls for the built-in alpha models.
use crate::core::{
    broker::types::OrderSide, insight::types::StrategyType, strategy::StrategyContext,
};
use polars::prelude::*;
use std::collections::HashMap;

/// Optional controls; existing constructors keep their original ATR and expiry defaults.
#[derive(Clone, Debug)]
pub struct BuiltinAlphaOptions {
    pub allow_long: bool,
    pub allow_short: bool,
    pub stop_loss_atr: f64,
    pub take_profit_atr: f64,
    /// Maximum active root insights from this alpha family per symbol; 0 means unlimited.
    pub max_active_insights: usize,
    /// Minimum distinct observed bars between emitted entries; 0 disables the extra cooldown.
    pub cooldown_bars: usize,
    /// 0 keeps the model's automatic/default expiry (in strategy bars).
    pub period_unfilled: u32,
    pub period_till_tp: u32,
}

impl Default for BuiltinAlphaOptions {
    fn default() -> Self {
        Self {
            allow_long: true,
            allow_short: true,
            stop_loss_atr: 1.5,
            take_profit_atr: 3.5,
            max_active_insights: 0,
            cooldown_bars: 0,
            period_unfilled: 0,
            period_till_tp: 0,
        }
    }
}

impl BuiltinAlphaOptions {
    pub(super) fn validate(&self) -> Result<(), String> {
        if !self.stop_loss_atr.is_finite()
            || self.stop_loss_atr <= 0.0
            || !self.take_profit_atr.is_finite()
            || self.take_profit_atr <= 0.0
        {
            return Err(
                "ATR stop-loss and take-profit multipliers must be finite and positive".into(),
            );
        }
        Ok(())
    }

    pub(super) fn allows(&self, side: OrderSide, shortable: bool) -> bool {
        match side {
            OrderSide::Buy => self.allow_long,
            OrderSide::Sell => self.allow_short && shortable,
        }
    }

    pub(super) fn unfilled_ttl(&self, automatic: u32) -> u32 {
        if self.period_unfilled == 0 {
            automatic
        } else {
            self.period_unfilled
        }
    }

    pub(super) fn filled_ttl(&self, automatic: u32) -> u32 {
        if self.period_till_tp == 0 {
            automatic
        } else {
            self.period_till_tp
        }
    }
}

// Prevent overflow/pathological indicator allocations from invalid constructor or sweep inputs.
pub(super) fn validate_period(name: &str, period: usize) -> Result<(), String> {
    if (1..=100_000).contains(&period) {
        Ok(())
    } else {
        Err(format!("{name} must be between 1 and 100000 bars"))
    }
}

pub(super) fn numeric(df: &DataFrame, column: &str, offset: usize) -> Result<f64, String> {
    let idx = df
        .height()
        .checked_sub(offset + 1)
        .ok_or("Not enough history")?;
    df.column(column)
        .map_err(|_| format!("Missing column '{column}'"))?
        .f64()
        .map_err(|_| format!("Column '{column}' must be Float64"))?
        .get(idx)
        .filter(|value| value.is_finite())
        .ok_or_else(|| format!("Value for '{column}' is null or non-finite"))
}

pub(super) fn confidence(
    ctx: &dyn StrategyContext,
    history: &DataFrame,
    modifier: Option<&str>,
) -> Result<u8, String> {
    let mut value = ctx.base_confidence();
    if let Some(field) = modifier {
        value *= numeric(history, field, 0)?.abs();
    }
    if !value.is_finite() || value <= 0.0 {
        return Err("Confidence must be finite and positive".into());
    }
    let rounded = (value.min(1.0) * 100.0).round() as u8;
    if rounded == 0 {
        return Err("Confidence rounds to zero".into());
    }
    Ok(rounded)
}

pub(super) fn valid_levels(side: &OrderSide, entry: f64, sl: f64, tp: f64) -> bool {
    [entry, sl, tp]
        .into_iter()
        .all(|v| v.is_finite() && v > 0.0)
        && match side {
            OrderSide::Buy => sl < entry && tp > entry,
            OrderSide::Sell => sl > entry && tp < entry,
        }
}

pub(super) fn active_roots(ctx: &dyn StrategyContext, symbol: &str, family: &str) -> usize {
    ctx.insights()
        .iter_active_insights()
        .filter(|insight| {
            insight.symbol == symbol
                && insight.parent_id.is_none()
                && match &insight.strategy_type {
                    StrategyType::Testing => family == "TestEntry",
                    StrategyType::Custom(name) => name == family,
                    _ => false,
                }
        })
        .count()
}

#[derive(Default)]
struct SymbolGate {
    timestamp: Option<i64>,
    elapsed: usize,
    emitted: bool,
}

/// O(symbols) memory; no per-bar history clones or unbounded timestamp sets.
#[derive(Default)]
pub(super) struct EntryGate {
    symbols: HashMap<String, SymbolGate>,
}

impl EntryGate {
    pub(super) fn clear(&mut self) {
        self.symbols.clear();
    }

    pub(super) fn allows(
        &mut self,
        ctx: &dyn StrategyContext,
        symbol: &str,
        family: &str,
        options: &BuiltinAlphaOptions,
    ) -> bool {
        if !options.allow_long && !options.allow_short {
            return false;
        }
        let timestamp = ctx.history().get(symbol).and_then(|history| {
            // Engine history timestamps are Int64 milliseconds. Read only the final scalar.
            history
                .column("timestamp")
                .ok()?
                .i64()
                .ok()?
                .get(history.height().checked_sub(1)?)
        });
        if let Some(timestamp) = timestamp {
            let gate = self.symbols.entry(symbol.into()).or_default();
            match gate.timestamp {
                Some(previous) if timestamp < previous => return false,
                Some(previous) if timestamp == previous => {}
                _ => {
                    gate.timestamp = Some(timestamp);
                    gate.elapsed = gate.elapsed.saturating_add(1);
                }
            }
            if gate.emitted && gate.elapsed < options.cooldown_bars.max(1) {
                return false;
            }
        }
        options.max_active_insights == 0
            || active_roots(ctx, symbol, family) < options.max_active_insights
    }

    pub(super) fn emitted(&mut self, symbol: &str) {
        if let Some(gate) = self.symbols.get_mut(symbol) {
            gate.elapsed = 0;
            gate.emitted = true;
        }
    }
}
