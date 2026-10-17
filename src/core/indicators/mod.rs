//! Built-in indicators over Polars numeric columns.
//!
//! Use [`I`] for all constructors. Existing constructors remain supported; `Default` chooses
//! close where a single price source is needed. Every indicator accepts an output prefix;
//! MACD, Bollinger Bands and Volume Profile expose multiple columns via `output_columns()`.
//! For example, `I::SimpleMovingAverage::new(20, "open")` averages opening prices, while
//! `I::Macd::default().with_output_prefix("trend")` emits trend, trend_signal and trend_histogram.
//!
//! `run` scans column chunks once for historical/research work. The strategy runtime seeds
//! independent symbol/timeframe streams and processes only incoming rows with `update_stream`.
//! Standard kernels take O(1) work per row and bounded rolling memory (recursive smoothers O(1)).
//! Volume Profile uses sparse fixed-price bins: O(log bins) insertion/removal and O(bins) POC/
//! value-area output, bounded by its lookback. Its final histogram API skips historical outputs.
//! Non-finite input is missing data; arithmetic overflow resets the affected kernel's warm-up.
//!
//! Formula references: [MACD](https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/macd),
//! [Q-stick](https://tulipindicators.org/qstick),
//! [VWMA](https://www.tradingview.com/support/solutions/43000592293-volume-weighted-moving-average-vwma/).
//! Volume Profile uses the usual POC/value-area concepts but explicitly assigns each bar's
//! volume to its selected price; it does not claim tick-level or TradingView implementation parity.
use chrono::{DateTime, Utc};
use polars::prelude::*;
use std::collections::HashMap;

/// Core trait for all technical indicators.
pub trait Indicator: Send + Sync {
    /// Returns the unique name of this indicator instance (e.g., "SMA_14_close").
    /// This is used for deduplication when registering to a strategy.
    fn name(&self) -> String;

    /// Returns the names of columns this indicator requires as input (e.g., vec!["close"]).
    fn input_columns(&self) -> Vec<String>;

    /// Returns the names of columns this indicator will output/append to the DataFrame.
    fn output_columns(&self) -> Vec<String>;

    /// The number of previous data points needed to compute the current value.
    fn window_size(&self) -> usize;

    /// Computes the indicator over a bulk DataFrame.
    /// Mutates the DataFrame by appending the `output_columns`.
    fn run(&mut self, df: &mut DataFrame) -> Result<(), String>;

    /// Computes the latest value from the supplied history (stateless for built-ins).
    /// Recursive indicators require their full seed history for exact results. The engine uses
    /// update_stream instead, which preserves recursive state across bounded history windows.
    /// Returns a map of column name to computed value.
    fn run_bar(&mut self, slice: &DataFrame) -> Result<HashMap<String, f64>, String>;

    /// Backfill and reset a symbol/timeframe stream. Built-ins retain smoothing state per key;
    /// custom indicators keep their existing run/run_bar implementation through these defaults.
    fn seed_stream(&mut self, _key: &str, df: &mut DataFrame) -> Result<(), String> {
        self.run(df)
    }

    /// Append-only batch update. `history` excludes `incoming` and belongs to `key`.
    /// Built-ins process new rows only, including warm-up, and emit nullable Float64 columns.
    /// Call seed_stream after correcting historical rows; use a distinct key per symbol/timeframe.
    fn update_stream(
        &mut self,
        _key: &str,
        history: &DataFrame,
        incoming: &mut DataFrame,
    ) -> Result<(), String> {
        let names = self.output_columns();
        let mut outputs: Vec<Vec<Option<f64>>> = names
            .iter()
            .map(|_| vec![None; incoming.height()])
            .collect();
        for index in 0..incoming.height() {
            let window_size = self.window_size();
            if window_size == 0 || history.height() + index + 1 < window_size {
                continue;
            }
            let mut window = history.tail(Some(window_size.saturating_sub(index + 1)));
            let next = incoming.slice(0, index + 1);
            if window.height() == 0 {
                window = next;
            } else {
                let next = next
                    .select(window.get_column_names().into_iter().map(|s| s.as_str()))
                    .map_err(|e| e.to_string())?;
                window.vstack_mut(&next).map_err(|e| e.to_string())?;
            }
            if window.height() > window_size {
                window = window.tail(Some(window_size));
            }
            let values = self.run_bar(&window)?;
            for (name, output) in names.iter().zip(&mut outputs) {
                output[index] = values.get(name).copied().filter(|v| v.is_finite());
            }
        }
        for (name, values) in names.into_iter().zip(outputs) {
            frame::upsert_column(incoming, Column::new(name.into(), values))
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// Release cached state when removing a dynamic history stream.
    fn reset_stream(&mut self, _key: &str) {}

    /// Returns the last time this indicator was run.
    fn last_run_time(&self) -> Option<DateTime<Utc>>;

    /// Updates the last time this indicator was run.
    fn set_last_run_time(&mut self, time: DateTime<Utc>);
}

mod api;
pub mod atr;
pub mod ema;
mod engine;
mod frame;
mod kernels;
pub mod rsi;
pub mod sma;
#[cfg(test)]
mod tests;
pub mod volume_profile;
pub use api::{
    BollingerBands, Macd, MovingAverageConvergenceDivergence, QStick, RateOfChange,
    VolumeWeightedMovingAverage,
};
pub use kernels::Smoothing;
#[allow(non_snake_case)]
pub mod I {
    pub use super::atr::AverageTrueRange;
    pub use super::ema::ExponentialMovingAverage;
    pub use super::rsi::RelativeStrengthIndex;
    pub use super::sma::SimpleMovingAverage;
    pub use super::volume_profile::{VolumeProfile, VolumeProfileBin};
    pub use super::{
        BollingerBands, Macd, MovingAverageConvergenceDivergence, QStick, RateOfChange, Smoothing,
        VolumeWeightedMovingAverage,
    };
}

/// Stable topological order for chained numeric sources, computed at registration, not per bar.
pub(crate) fn execution_order(
    indicators: &HashMap<String, Box<dyn Indicator>>,
) -> Result<Vec<String>, String> {
    let mut producers = HashMap::new();
    for (name, indicator) in indicators {
        for output in indicator.output_columns() {
            if producers.insert(output.clone(), name.clone()).is_some() {
                return Err(format!("Duplicate indicator output '{output}'"));
            }
        }
    }
    let mut pending: Vec<_> = indicators.keys().cloned().collect();
    pending.sort();
    let mut ordered = Vec::with_capacity(pending.len());
    while !pending.is_empty() {
        let Some(index) = pending.iter().position(|name| {
            indicators[name].input_columns().iter().all(|input| {
                producers
                    .get(input)
                    .is_none_or(|producer| ordered.contains(producer))
            })
        }) else {
            return Err("Cyclic indicator input dependencies".into());
        };
        ordered.push(pending.remove(index));
    }
    Ok(ordered)
}
