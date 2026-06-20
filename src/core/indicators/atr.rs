use super::Indicator;
use chrono::{DateTime, Utc};
use polars::prelude::*;
use std::collections::HashMap;

/// Computes average true range from high, low, and close columns.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - `period`: Number of true-range values required in the rolling average window.
///
/// Behaviour:
/// Produces an output column named `ATRr_{period}`. Full-history runs calculate true range from
/// `high`, `low`, and previous `close`, then write a simple rolling average once `period` valid
/// true-range values are available. `run_bar` recalculates on the supplied slice and returns the
/// latest ATR value.
pub struct AverageTrueRange {
    period: usize,
    last_run_time: Option<DateTime<Utc>>,
}

impl AverageTrueRange {
    pub fn new(period: usize) -> Self {
        Self {
            period,
            last_run_time: None,
        }
    }
}

impl Indicator for AverageTrueRange {
    fn name(&self) -> String {
        format!("ATRr_{}", self.period)
    }

    fn input_columns(&self) -> Vec<String> {
        vec!["high".to_string(), "low".to_string(), "close".to_string()]
    }

    fn output_columns(&self) -> Vec<String> {
        vec![self.name()]
    }

    fn window_size(&self) -> usize {
        self.period
    }

    fn run(&mut self, df: &mut DataFrame) -> Result<(), String> {
        let highs = df
            .column("high")
            .map_err(|e| format!("Failed to find column 'high': {}", e))?
            .f64()
            .map_err(|e| format!("Column 'high' is not Float64: {}", e))?;
        let lows = df
            .column("low")
            .map_err(|e| format!("Failed to find column 'low': {}", e))?
            .f64()
            .map_err(|e| format!("Column 'low' is not Float64: {}", e))?;
        let closes = df
            .column("close")
            .map_err(|e| format!("Failed to find column 'close': {}", e))?
            .f64()
            .map_err(|e| format!("Column 'close' is not Float64: {}", e))?;

        let highs: Vec<Option<f64>> = highs.into_iter().collect();
        let lows: Vec<Option<f64>> = lows.into_iter().collect();
        let closes: Vec<Option<f64>> = closes.into_iter().collect();

        let mut true_ranges = Vec::with_capacity(df.height());
        let previous_closes = std::iter::once(None).chain(closes.iter().copied());
        for ((high, low), prev_close) in highs
            .iter()
            .copied()
            .zip(lows.iter().copied())
            .zip(previous_closes)
        {
            let (Some(high), Some(low)) = (high, low) else {
                true_ranges.push(None);
                continue;
            };

            let tr = if let Some(prev_close) = prev_close {
                let hl = high - low;
                let hc = (high - prev_close).abs();
                let lc = (low - prev_close).abs();
                hl.max(hc).max(lc)
            } else {
                high - low
            };
            true_ranges.push(Some(tr));
        }

        let mut atr_values = Vec::with_capacity(true_ranges.len());
        let mut rolling_sum = 0.0;
        let mut valid_count = 0usize;

        let outgoing_ranges = std::iter::repeat(None)
            .take(self.period)
            .chain(true_ranges.iter().copied());
        for (value, outgoing) in true_ranges.iter().copied().zip(outgoing_ranges) {
            if let Some(value) = value {
                rolling_sum += value;
                valid_count += 1;
            }

            if let Some(value) = outgoing {
                rolling_sum -= value;
                valid_count -= 1;
            }

            if valid_count == self.period {
                atr_values.push(Some(rolling_sum / self.period as f64));
            } else {
                atr_values.push(None);
            }
        }

        df.with_column(Series::new(self.name().into(), atr_values))
            .map_err(|e| format!("Failed to add ATR column: {}", e))?;

        Ok(())
    }

    fn run_bar(&mut self, slice: &DataFrame) -> Result<HashMap<String, f64>, String> {
        if slice.height() < self.period {
            return Err(format!(
                "Not enough data to compute ATR (need {}, got {})",
                self.period,
                slice.height()
            ));
        }

        let mut tmp = slice.clone();
        self.run(&mut tmp)?;

        let value = tmp
            .column(&self.name())
            .map_err(|e| format!("Failed to find ATR column: {}", e))?
            .f64()
            .map_err(|e| format!("ATR column is not Float64: {}", e))?
            .get(tmp.height().saturating_sub(1))
            .ok_or_else(|| "Latest ATR value is null".to_string())?;

        let mut result = HashMap::new();
        result.insert(self.name(), value);
        Ok(result)
    }

    fn last_run_time(&self) -> Option<DateTime<Utc>> {
        self.last_run_time
    }

    fn set_last_run_time(&mut self, time: DateTime<Utc>) {
        self.last_run_time = Some(time);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computes_atr_column() {
        let mut df = DataFrame::new(vec![
            Series::new("high".into(), &[10.0, 12.0, 13.0, 14.0]).into(),
            Series::new("low".into(), &[8.0, 9.0, 11.0, 12.0]).into(),
            Series::new("close".into(), &[9.0, 11.0, 12.0, 13.0]).into(),
        ])
        .unwrap();

        let mut atr = AverageTrueRange::new(3);
        atr.run(&mut df).unwrap();

        let values: Vec<Option<f64>> = df
            .column("ATRr_3")
            .unwrap()
            .f64()
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(values[0], None);
        assert_eq!(values[1], None);
        assert_eq!(values[2], Some(7.0 / 3.0));
        assert_eq!(values[3], Some(7.0 / 3.0));
    }

    #[test]
    fn computes_latest_atr_from_run_bar() {
        let df = DataFrame::new(vec![
            Series::new("high".into(), &[10.0, 12.0, 13.0]).into(),
            Series::new("low".into(), &[8.0, 9.0, 11.0]).into(),
            Series::new("close".into(), &[9.0, 11.0, 12.0]).into(),
        ])
        .unwrap();

        let mut atr = AverageTrueRange::new(3);
        let values = atr.run_bar(&df).unwrap();
        assert_eq!(values.get("ATRr_3").copied(), Some(7.0 / 3.0));
    }
}
