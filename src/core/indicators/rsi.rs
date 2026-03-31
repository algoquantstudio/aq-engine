use super::Indicator;
use chrono::{DateTime, Utc};
use polars::prelude::*;
use std::collections::HashMap;

pub struct RelativeStrengthIndex {
    period: usize,
    target_column: String,
    last_run_time: Option<DateTime<Utc>>,
}

impl RelativeStrengthIndex {
    pub fn new(period: usize, target_column: &str) -> Self {
        Self {
            period,
            target_column: target_column.to_string(),
            last_run_time: None,
        }
    }
}

impl Indicator for RelativeStrengthIndex {
    fn name(&self) -> String {
        format!("RSI_{}", self.period)
    }

    fn input_columns(&self) -> Vec<String> {
        vec![self.target_column.clone()]
    }

    fn output_columns(&self) -> Vec<String> {
        vec![self.name()]
    }

    fn window_size(&self) -> usize {
        self.period + 1
    }

    fn run(&mut self, df: &mut DataFrame) -> Result<(), String> {
        let s = df
            .column(&self.target_column)
            .map_err(|e| format!("Failed to find column '{}': {}", self.target_column, e))?
            .f64()
            .map_err(|e| format!("Column '{}' is not Float64: {}", self.target_column, e))?;

        let values: Vec<Option<f64>> = s.into_iter().collect();
        let mut out = vec![None; values.len()];
        if values.len() <= self.period {
            df.with_column(Series::new(self.name().into(), out))
                .map_err(|e| format!("Failed to append RSI column: {}", e))?;
            return Ok(());
        }

        let mut gains = 0.0;
        let mut losses = 0.0;
        for i in 1..=self.period {
            let current = values[i].ok_or_else(|| "Null value in RSI window".to_string())?;
            let previous = values[i - 1].ok_or_else(|| "Null value in RSI window".to_string())?;
            let delta = current - previous;
            if delta >= 0.0 {
                gains += delta;
            } else {
                losses += -delta;
            }
        }

        let mut avg_gain = gains / self.period as f64;
        let mut avg_loss = losses / self.period as f64;
        out[self.period] = Some(if avg_loss == 0.0 {
            100.0
        } else {
            100.0 - (100.0 / (1.0 + avg_gain / avg_loss))
        });

        for i in (self.period + 1)..values.len() {
            let current = values[i].ok_or_else(|| "Null value in RSI series".to_string())?;
            let previous = values[i - 1].ok_or_else(|| "Null value in RSI series".to_string())?;
            let delta = current - previous;
            let gain = delta.max(0.0);
            let loss = (-delta).max(0.0);
            avg_gain = ((avg_gain * (self.period as f64 - 1.0)) + gain) / self.period as f64;
            avg_loss = ((avg_loss * (self.period as f64 - 1.0)) + loss) / self.period as f64;
            out[i] = Some(if avg_loss == 0.0 {
                100.0
            } else {
                100.0 - (100.0 / (1.0 + avg_gain / avg_loss))
            });
        }

        df.with_column(Series::new(self.name().into(), out))
            .map_err(|e| format!("Failed to append RSI column: {}", e))?;
        Ok(())
    }

    fn run_bar(&mut self, slice: &DataFrame) -> Result<HashMap<String, f64>, String> {
        let mut cloned = slice.clone();
        self.run(&mut cloned)?;
        let value = cloned
            .column(&self.name())
            .map_err(|e| format!("Failed to get RSI column: {}", e))?
            .f64()
            .map_err(|e| format!("RSI column is not Float64: {}", e))?
            .get(cloned.height().saturating_sub(1))
            .ok_or_else(|| "Latest RSI value is null".to_string())?;
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
    fn rsi_appends_column() {
        let mut df = df!("close" => &[1.0, 2.0, 1.5, 2.5, 2.0, 3.0, 2.8]).unwrap();
        let mut rsi = RelativeStrengthIndex::new(3, "close");
        rsi.run(&mut df).unwrap();
        assert!(df.column("RSI_3").is_ok());
    }
}
