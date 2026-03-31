use super::Indicator;
use chrono::{DateTime, Utc};
use polars::prelude::*;
use std::collections::HashMap;

pub struct ExponentialMovingAverage {
    period: usize,
    target_column: String,
    last_run_time: Option<DateTime<Utc>>,
}

impl ExponentialMovingAverage {
    pub fn new(period: usize, target_column: &str) -> Self {
        Self {
            period,
            target_column: target_column.to_string(),
            last_run_time: None,
        }
    }
}

impl Indicator for ExponentialMovingAverage {
    fn name(&self) -> String {
        format!("EMA_{}", self.period)
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
        let s = df
            .column(&self.target_column)
            .map_err(|e| format!("Failed to find column '{}': {}", self.target_column, e))?
            .f64()
            .map_err(|e| format!("Column '{}' is not Float64: {}", self.target_column, e))?;

        let multiplier = 2.0 / (self.period as f64 + 1.0);
        let mut out: Vec<Option<f64>> = Vec::with_capacity(s.len());
        let mut ema: Option<f64> = None;
        let mut seed_sum = 0.0;
        let mut seed_count = 0usize;

        for value in s.into_iter() {
            match (value, ema) {
                (Some(v), Some(prev)) => {
                    let next = ((v - prev) * multiplier) + prev;
                    ema = Some(next);
                    out.push(Some(next));
                }
                (Some(v), None) => {
                    seed_sum += v;
                    seed_count += 1;
                    if seed_count == self.period {
                        let seed = seed_sum / self.period as f64;
                        ema = Some(seed);
                        out.push(Some(seed));
                    } else {
                        out.push(None);
                    }
                }
                (None, Some(prev)) => out.push(Some(prev)),
                (None, None) => out.push(None),
            }
        }

        df.with_column(Series::new(self.name().into(), out))
            .map_err(|e| format!("Failed to append EMA column: {}", e))?;
        Ok(())
    }

    fn run_bar(&mut self, slice: &DataFrame) -> Result<HashMap<String, f64>, String> {
        let mut cloned = slice.clone();
        self.run(&mut cloned)?;
        let value = cloned
            .column(&self.name())
            .map_err(|e| format!("Failed to get EMA column: {}", e))?
            .f64()
            .map_err(|e| format!("EMA column is not Float64: {}", e))?
            .get(cloned.height().saturating_sub(1))
            .ok_or_else(|| "Latest EMA value is null".to_string())?;
        Ok(HashMap::from([(self.name(), value)]))
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
    fn ema_appends_column() {
        let mut df = df!("close" => &[1.0, 2.0, 3.0, 4.0, 5.0]).unwrap();
        let mut ema = ExponentialMovingAverage::new(3, "close");
        ema.run(&mut df).unwrap();
        assert!(df.column("EMA_3").is_ok());
    }
}
