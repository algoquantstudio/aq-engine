//! Polars adapter: borrow Float64 buffers, cast other numeric types once per batch, iterate
//! chunks directly and allocate only output columns. No intermediate full-frame clones.
use super::{
    frame::{column, upsert_column},
    kernels::{Kernel, Kind, Values},
    volume_profile::VolumeProfileBin,
};
use chrono::{DateTime, Utc};
use polars::prelude::*;
use std::collections::HashMap;

struct Stream {
    kernel: Kernel,
    last: Option<RowKey>,
}
#[derive(PartialEq)]
struct RowKey {
    timestamp: Option<i64>,
    values: [Option<u64>; 4],
}

pub(super) struct Calculator {
    pub(super) kind: Kind,
    pub(super) inputs: Vec<String>,
    pub(super) outputs: Vec<String>,
    pub(super) explicit_prefix: Option<String>,
    streams: HashMap<String, Stream>,
    pub(super) last_run: Option<DateTime<Utc>>,
}
impl Calculator {
    pub(super) fn new(kind: Kind, inputs: Vec<String>, outputs: Vec<String>) -> Self {
        Self {
            kind,
            inputs,
            outputs,
            explicit_prefix: None,
            streams: HashMap::new(),
            last_run: None,
        }
    }
    pub(super) fn validate(&self) -> Result<(), String> {
        self.kind.validate()?;
        if self.inputs.iter().any(|n| n.trim().is_empty())
            || self
                .outputs
                .iter()
                .any(|n| n.trim().is_empty() || self.inputs.contains(n))
        {
            return Err(
                "Indicator column names must be non-empty and outputs must not overwrite inputs"
                    .into(),
            );
        }
        Ok(())
    }
    fn read(&self, df: &DataFrame) -> Result<Vec<Column>, String> {
        self.inputs
            .iter()
            .map(|name| {
                let source =
                    column(df, name).map_err(|e| format!("Indicator input '{name}': {e}"))?;
                if !source.dtype().is_primitive_numeric() {
                    return Err(format!("Indicator input '{name}' must be numeric"));
                }
                source.cast(&DataType::Float64).map_err(|e| e.to_string())
            })
            .collect()
    }
    fn last_key(df: &DataFrame, inputs: &[Column]) -> Option<RowKey> {
        let index = df.height().checked_sub(1)?;
        let mut values = [None; 4];
        for (slot, col) in values.iter_mut().zip(inputs) {
            *slot = col.f64().ok()?.get(index).map(f64::to_bits);
        }
        Some(RowKey {
            timestamp: column(df, "timestamp")
                .ok()
                .and_then(|c| c.i64().ok())
                .and_then(|c| c.get(index)),
            values,
        })
    }
    fn compute(
        kernel: &mut Kernel,
        inputs: &[Column],
        height: usize,
        count: usize,
        collect: bool,
    ) -> (Vec<Vec<Option<f64>>>, Values) {
        let mut iterators: Vec<_> = inputs
            .iter()
            .map(|c| c.f64().expect("cast Float64").iter())
            .collect();
        let mut output: Vec<_> = (0..if collect { count } else { 0 })
            .map(|_| Vec::with_capacity(height))
            .collect();
        let mut row = [None; 4];
        let mut latest = [None; 4];
        for _ in 0..height {
            for (value, it) in row.iter_mut().zip(&mut iterators) {
                *value = it.next().flatten().filter(|v| v.is_finite());
            }
            latest = kernel.next(&row[..inputs.len()]);
            for (column, value) in output.iter_mut().zip(latest) {
                column.push(value);
            }
        }
        (output, latest)
    }
    fn write(&self, df: &mut DataFrame, values: Vec<Vec<Option<f64>>>) -> Result<(), String> {
        for (name, values) in self.outputs.iter().zip(values) {
            upsert_column(df, Column::new(name.clone().into(), values))
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
    pub(super) fn run(&mut self, df: &mut DataFrame) -> Result<(), String> {
        self.validate()?;
        let inputs = self.read(df)?;
        let (values, _) = Self::compute(
            &mut self.kind.kernel(),
            &inputs,
            df.height(),
            self.outputs.len(),
            true,
        );
        self.write(df, values)
    }
    pub(super) fn run_bar(&mut self, df: &DataFrame) -> Result<HashMap<String, f64>, String> {
        self.validate()?;
        let tail = self.kind.stateless_tail().map(|rows| df.tail(Some(rows)));
        let df = tail.as_ref().unwrap_or(df);
        let inputs = self.read(df)?;
        let (_, latest) = Self::compute(
            &mut self.kind.kernel(),
            &inputs,
            df.height(),
            self.outputs.len(),
            false,
        );
        Ok(self
            .outputs
            .iter()
            .cloned()
            .zip(latest)
            .filter_map(|(n, v)| v.map(|v| (n, v)))
            .collect())
    }
    pub(super) fn seed(&mut self, key: &str, df: &mut DataFrame) -> Result<(), String> {
        self.validate()?;
        let inputs = self.read(df)?;
        let mut stream = Stream {
            kernel: self.kind.kernel(),
            last: Self::last_key(df, &inputs),
        };
        let (values, _) = Self::compute(
            &mut stream.kernel,
            &inputs,
            df.height(),
            self.outputs.len(),
            true,
        );
        self.write(df, values)?;
        self.streams.insert(key.into(), stream);
        Ok(())
    }
    pub(super) fn update(
        &mut self,
        key: &str,
        history: &DataFrame,
        incoming: &mut DataFrame,
    ) -> Result<(), String> {
        self.validate()?;
        let inputs = self.read(incoming)?;
        // Read only the final retained row for continuity, not the entire history's chunks.
        let tail = history.tail(Some(1));
        let history_last = if tail.height() == 0 {
            None
        } else {
            Self::last_key(&tail, &self.read(&tail)?)
        };
        if !self
            .streams
            .get(key)
            .is_some_and(|s| s.last == history_last)
        {
            let mut stream = Stream {
                kernel: self.kind.kernel(),
                last: history_last,
            };
            if history.height() > 0 {
                Self::compute(
                    &mut stream.kernel,
                    &self.read(history)?,
                    history.height(),
                    0,
                    false,
                );
            }
            self.streams.insert(key.into(), stream);
        }
        let stream = self.streams.get_mut(key).expect("initialized stream");
        let (values, _) = Self::compute(
            &mut stream.kernel,
            &inputs,
            incoming.height(),
            self.outputs.len(),
            true,
        );
        if incoming.height() > 0 {
            stream.last = Self::last_key(incoming, &inputs);
        }
        self.write(incoming, values)
    }
    pub(super) fn reset(&mut self, key: &str) {
        self.streams.remove(key);
    }
    pub(super) fn histogram(&self, df: &DataFrame) -> Result<Vec<VolumeProfileBin>, String> {
        self.validate()?;
        match self.kind.kernel() {
            Kernel::Profile(mut p) => {
                // A final histogram only needs the retained window, not all historical
                // POC/value-area calculations. Keep research snapshots O(lookback log bins).
                let tail = df.tail(Some(self.kind.window()));
                let columns = self.read(&tail)?;
                for (price, volume) in columns[0]
                    .f64()
                    .unwrap()
                    .iter()
                    .zip(columns[1].f64().unwrap().iter())
                {
                    p.push(price, volume);
                }
                Ok(p.histogram())
            }
            _ => Err("Not a volume profile".into()),
        }
    }
}
