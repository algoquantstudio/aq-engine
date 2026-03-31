use super::Indicator;
use chrono::{DateTime, Utc};
use polars::prelude::*;
use std::collections::HashMap;

/// Simple Moving Average
pub struct SimpleMovingAverage {
    period: usize,
    target_column: String,
    last_run_time: Option<DateTime<Utc>>,
}

impl SimpleMovingAverage {
    pub fn new(period: usize, target_column: &str) -> Self {
        Self {
            period,
            target_column: target_column.to_string(),
            last_run_time: None,
        }
    }
}

impl Indicator for SimpleMovingAverage {
    fn name(&self) -> String {
        format!("SMA_{}_{}", self.period, self.target_column)
    }

    fn input_columns(&self) -> Vec<String> {
        vec![self.target_column.clone()]
    }

    fn output_columns(&self) -> Vec<String> {
        vec![self.name()]
    }

    fn window_size(&self) -> usize {
        self.period
    }

    fn run(&mut self, df: &mut DataFrame) -> Result<(), String> {
        let input_col = &self.target_column;
        let output_col = self.name();

        let s = df
            .column(input_col)
            .map_err(|e| format!("Failed to find column '{}': {}", input_col, e))?;

        let floats = s
            .f64()
            .map_err(|e| format!("Column '{}' is not of type Float64: {}", input_col, e))?;

        let mut sma_values: Vec<Option<f64>> = Vec::with_capacity(floats.len());
        let period = self.period;

        let values: Vec<Option<f64>> = floats.into_iter().collect();
        let mut sum = 0.0;
        let mut valid_count = 0;

        for i in 0..values.len() {
            if let Some(v) = values[i] {
                sum += v;
                valid_count += 1;
            }

            if i >= period {
                if let Some(v) = values[i - period] {
                    sum -= v;
                    valid_count -= 1;
                }
            }

            if valid_count == period {
                sma_values.push(Some(sum / period as f64));
            } else {
                sma_values.push(None);
            }
        }

        let sma_series = Series::new(output_col.clone().into(), &sma_values);
        df.with_column(sma_series)
            .map_err(|e| format!("Failed to add column '{}': {}", output_col, e))?;

        Ok(())
    }

    fn run_bar(&mut self, slice: &DataFrame) -> Result<HashMap<String, f64>, String> {
        let input_col = &self.target_column;

        // slice is expected to contain at least `window_size()` rows at the end.
        let s = slice
            .column(input_col)
            .map_err(|e| format!("Failed to find column '{}': {}", input_col, e))?;

        let tail_series = s.tail(Some(self.period));
        let floats = tail_series
            .f64()
            .map_err(|e| format!("Column '{}' is not of type Float64: {}", input_col, e))?;

        if floats.len() < self.period {
            return Err(format!(
                "Not enough data to compute SMA (need {}, got {})",
                self.period,
                floats.len()
            ));
        }

        let mean = floats
            .mean()
            .ok_or("Not enough valid float values for SMA".to_string())?;

        let mut result = HashMap::new();
        result.insert(self.name(), mean);
        Ok(result)
    }

    fn last_run_time(&self) -> Option<DateTime<Utc>> {
        self.last_run_time
    }

    fn set_last_run_time(&mut self, time: DateTime<Utc>) {
        self.last_run_time = Some(time);
    }
}
