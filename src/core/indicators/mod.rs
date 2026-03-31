use chrono::{DateTime, Utc};
use polars::prelude::DataFrame;
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

    /// Incrementally computes the indicator for the latest bar given a slice
    /// of the historical DataFrame (of length at least `window_size()`).
    /// Returns a map of column name to computed value.
    fn run_bar(&mut self, slice: &DataFrame) -> Result<HashMap<String, f64>, String>;

    /// Returns the last time this indicator was run.
    fn last_run_time(&self) -> Option<DateTime<Utc>>;

    /// Updates the last time this indicator was run.
    fn set_last_run_time(&mut self, time: DateTime<Utc>);
}

pub mod atr;
pub mod ema;
pub mod rsi;
pub mod sma;
#[allow(non_snake_case)]
pub mod I {
    pub use super::atr::AverageTrueRange;
    pub use super::ema::ExponentialMovingAverage;
    pub use super::rsi::RelativeStrengthIndex;
    pub use super::sma::SimpleMovingAverage;
}
