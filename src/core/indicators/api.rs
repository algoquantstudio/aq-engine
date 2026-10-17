use super::{
    Indicator,
    engine::Calculator,
    kernels::{Kind, Smoothing},
    volume_profile::VolumeProfileBin,
};
use chrono::{DateTime, Utc};
use polars::prelude::DataFrame;
use std::collections::HashMap;

fn target(value: &str) -> String {
    if value.trim().is_empty() {
        "close".into()
    } else {
        value.trim().into()
    }
}
fn columns(names: &[&str]) -> Vec<String> {
    names.iter().map(|s| s.trim().to_string()).collect()
}
fn output(base: String, suffixes: &[&str]) -> Vec<String> {
    suffixes
        .iter()
        .map(|suffix| format!("{base}{suffix}"))
        .collect()
}
fn mode_name(mode: Smoothing) -> &'static str {
    match mode {
        Smoothing::Simple => "SMA",
        Smoothing::Exponential => "EMA",
        Smoothing::Wilder => "RMA",
    }
}

macro_rules! indicator {
    ($name:ident, $doc:literal) => {
        #[doc=$doc]
        pub struct $name {
            inner: Calculator,
        }
        impl $name {
            /// Override output names as a group. The first output uses the prefix itself.
            /// Set before registration; use distinct prefixes for otherwise identical instances.
            pub fn with_output_prefix(mut self, prefix: &str) -> Self {
                let suffixes: &[&str] = match self.inner.kind {
                    Kind::Macd(..) => &["", "_signal", "_histogram"],
                    Kind::Bands(..) => &["", "_upper", "_lower"],
                    Kind::Profile(..) => &["", "_val", "_vah", "_volume"],
                    _ => &[""],
                };
                self.inner.outputs = output(prefix.into(), suffixes);
                self.inner.explicit_prefix = Some(prefix.into());
                self
            }
            /// Validate configuration without needing market data.
            pub fn validate(&self) -> Result<(), String> {
                self.inner.validate()
            }
            #[allow(dead_code)]
            fn configured_with_prefix(&self, configured: Self) -> Self {
                match self.inner.explicit_prefix.as_deref() {
                    Some(prefix) => configured.with_output_prefix(prefix),
                    None => configured,
                }
            }
        }
        impl Indicator for $name {
            fn name(&self) -> String {
                self.inner.outputs[0].clone()
            }
            fn input_columns(&self) -> Vec<String> {
                self.inner.inputs.clone()
            }
            fn output_columns(&self) -> Vec<String> {
                self.inner.outputs.clone()
            }
            fn window_size(&self) -> usize {
                self.inner.kind.window()
            }
            fn run(&mut self, df: &mut DataFrame) -> Result<(), String> {
                self.inner.run(df)
            }
            fn run_bar(&mut self, df: &DataFrame) -> Result<HashMap<String, f64>, String> {
                self.inner.run_bar(df)
            }
            fn seed_stream(&mut self, key: &str, df: &mut DataFrame) -> Result<(), String> {
                self.inner.seed(key, df)
            }
            fn update_stream(
                &mut self,
                key: &str,
                history: &DataFrame,
                incoming: &mut DataFrame,
            ) -> Result<(), String> {
                self.inner.update(key, history, incoming)
            }
            fn reset_stream(&mut self, key: &str) {
                self.inner.reset(key);
            }
            fn last_run_time(&self) -> Option<DateTime<Utc>> {
                self.inner.last_run
            }
            fn set_last_run_time(&mut self, time: DateTime<Utc>) {
                self.inner.last_run = Some(time);
            }
        }
    };
}

indicator!(
    SimpleMovingAverage,
    "Rolling SMA, with a full valid window required. Numeric source defaults to close; legacy constructor and output name are preserved."
);
impl SimpleMovingAverage {
    pub fn new(period: usize, target_column: &str) -> Self {
        let field = target(target_column);
        Self {
            inner: Calculator::new(
                Kind::Average(period, Smoothing::Simple),
                vec![field.clone()],
                vec![format!("SMA_{period}_{field}")],
            ),
        }
    }
    pub fn close(period: usize) -> Self {
        Self::new(period, "close")
    }
}
impl Default for SimpleMovingAverage {
    fn default() -> Self {
        Self::close(14)
    }
}

indicator!(
    ExponentialMovingAverage,
    "SMA-seeded EMA using alpha=2/(period+1). Missing/non-finite samples carry the last value after seeding. Close retains EMA_{period}; other fields receive distinct names."
);
impl ExponentialMovingAverage {
    pub fn new(period: usize, target_column: &str) -> Self {
        let field = target(target_column);
        let name = if field == "close" {
            format!("EMA_{period}")
        } else {
            format!("EMA_{period}_{field}")
        };
        Self {
            inner: Calculator::new(
                Kind::Average(period, Smoothing::Exponential),
                vec![field],
                vec![name],
            ),
        }
    }
    pub fn close(period: usize) -> Self {
        Self::new(period, "close")
    }
}
impl Default for ExponentialMovingAverage {
    fn default() -> Self {
        Self::close(14)
    }
}

indicator!(
    RelativeStrengthIndex,
    "Wilder RSI, seeded with period valid adjacent deltas. Flat prices return neutral 50. Missing observations break the delta chain and carry the last RSI; close retains RSI_{period}."
);
impl RelativeStrengthIndex {
    pub fn new(period: usize, target_column: &str) -> Self {
        let field = target(target_column);
        let name = if field == "close" {
            format!("RSI_{period}")
        } else {
            format!("RSI_{period}_{field}")
        };
        Self {
            inner: Calculator::new(Kind::Rsi(period), vec![field], vec![name]),
        }
    }
    pub fn close(period: usize) -> Self {
        Self::new(period, "close")
    }
}
impl Default for RelativeStrengthIndex {
    fn default() -> Self {
        Self::close(14)
    }
}

indicator!(
    AverageTrueRange,
    "True range with configurable OHLC columns and smoothing. new(period) preserves AQE's legacy simple-average ATRr_{period}; use with_smoothing(Wilder) for conventional Wilder ATR."
);
impl AverageTrueRange {
    pub fn new(period: usize) -> Self {
        Self::configured(period, Smoothing::Simple, &["high", "low", "close"])
    }
    fn configured(period: usize, mode: Smoothing, fields: &[&str]) -> Self {
        let mut name = match mode {
            Smoothing::Simple => format!("ATRr_{period}"),
            Smoothing::Wilder => format!("ATRw_{period}"),
            Smoothing::Exponential => format!("ATRe_{period}"),
        };
        if fields != ["high", "low", "close"] {
            name.push_str(&format!("_{}", fields.join("_")));
        }
        Self {
            inner: Calculator::new(Kind::Atr(period, mode), columns(fields), vec![name]),
        }
    }
    pub fn with_smoothing(self, mode: Smoothing) -> Self {
        let Kind::Atr(p, _) = self.inner.kind else {
            unreachable!()
        };
        self.configured_with_prefix(Self::configured(
            p,
            mode,
            &self
                .inner
                .inputs
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
        ))
    }
    pub fn with_columns(self, high: &str, low: &str, close: &str) -> Self {
        let Kind::Atr(p, m) = self.inner.kind else {
            unreachable!()
        };
        self.configured_with_prefix(Self::configured(p, m, &[high, low, close]))
    }
}
impl Default for AverageTrueRange {
    fn default() -> Self {
        Self::new(14)
    }
}

indicator!(
    QStick,
    "Moving average of close minus open. Default: 14-bar SMA. Supports custom open/close columns and Simple, Exponential or Wilder smoothing."
);
impl QStick {
    pub fn new(period: usize) -> Self {
        Self::configured(period, Smoothing::Simple, "open", "close")
    }
    fn configured(period: usize, mode: Smoothing, open: &str, close: &str) -> Self {
        Self {
            inner: Calculator::new(
                Kind::Qstick(period, mode),
                columns(&[open, close]),
                vec![format!(
                    "QSTICK_{period}_{}_{open}_{close}",
                    mode_name(mode)
                )],
            ),
        }
    }
    pub fn with_smoothing(self, mode: Smoothing) -> Self {
        let Kind::Qstick(p, _) = self.inner.kind else {
            unreachable!()
        };
        self.configured_with_prefix(Self::configured(
            p,
            mode,
            &self.inner.inputs[0],
            &self.inner.inputs[1],
        ))
    }
    pub fn with_columns(self, open: &str, close: &str) -> Self {
        let Kind::Qstick(p, m) = self.inner.kind else {
            unreachable!()
        };
        self.configured_with_prefix(Self::configured(p, m, open, close))
    }
}
impl Default for QStick {
    fn default() -> Self {
        Self::new(14)
    }
}

indicator!(
    MovingAverageConvergenceDivergence,
    "MACD: fast EMA minus slow EMA, signal EMA, and line-minus-signal histogram. All EMAs use SMA seeds. Defaults: 12/26/9, close. Outputs: name(), name()_signal, name()_histogram."
);
impl MovingAverageConvergenceDivergence {
    pub fn new(fast: usize, slow: usize, signal: usize, target_column: &str) -> Self {
        let field = target(target_column);
        let name = format!("MACD_{fast}_{slow}_{signal}_{field}");
        Self {
            inner: Calculator::new(
                Kind::Macd(fast, slow, signal),
                vec![field],
                output(name, &["", "_signal", "_histogram"]),
            ),
        }
    }
}
impl Default for MovingAverageConvergenceDivergence {
    fn default() -> Self {
        Self::new(12, 26, 9, "close")
    }
}
pub type Macd = MovingAverageConvergenceDivergence;

indicator!(
    BollingerBands,
    "SMA with upper/lower population-standard-deviation bands. Defaults: 20, 2 deviations, close. Rolling Welford arithmetic avoids catastrophic variance cancellation. Outputs: middle, _upper, _lower."
);
impl BollingerBands {
    pub fn new(period: usize, deviations: f64, target_column: &str) -> Self {
        let field = target(target_column);
        let name = format!("BB_{period}_{deviations}_{field}");
        Self {
            inner: Calculator::new(
                Kind::Bands(period, deviations),
                vec![field],
                output(name, &["", "_upper", "_lower"]),
            ),
        }
    }
}
impl Default for BollingerBands {
    fn default() -> Self {
        Self::new(20, 2.0, "close")
    }
}

indicator!(
    VolumeWeightedMovingAverage,
    "Rolling sum(price*volume)/sum(volume), not session-anchored VWAP. Defaults: 20, close, volume. Zero total volume yields null; negative/missing volume invalidates its window."
);
impl VolumeWeightedMovingAverage {
    pub fn new(period: usize, target_column: &str) -> Self {
        Self::configured(period, target_column, "volume")
    }
    fn configured(period: usize, field: &str, volume: &str) -> Self {
        let field = target(field);
        let name = format!("VWMA_{period}_{field}_{volume}");
        Self {
            inner: Calculator::new(Kind::Vwma(period), vec![field, volume.into()], vec![name]),
        }
    }
    pub fn with_volume_column(self, volume: &str) -> Self {
        let Kind::Vwma(p) = self.inner.kind else {
            unreachable!()
        };
        self.configured_with_prefix(Self::configured(p, &self.inner.inputs[0], volume))
    }
}
impl Default for VolumeWeightedMovingAverage {
    fn default() -> Self {
        Self::new(20, "close")
    }
}

indicator!(
    RateOfChange,
    "Percent change from period bars ago: 100*(current/previous-1). Defaults: 12, close. Zero or missing reference prices produce null, never infinity."
);
impl RateOfChange {
    pub fn new(period: usize, target_column: &str) -> Self {
        let field = target(target_column);
        let name = format!("ROC_{period}_{field}");
        Self {
            inner: Calculator::new(Kind::Roc(period), vec![field], vec![name]),
        }
    }
}
impl Default for RateOfChange {
    fn default() -> Self {
        Self::new(12, "close")
    }
}

indicator!(
    VolumeProfile,
    "Rolling sparse fixed-price-bin profile. Each row's volume is assigned to its chosen price: a bar-data approximation, not tick reconstruction. Defaults: 100 rows, bin width 1 price unit, 70% value area, close/volume. Outputs: POC at name(), _val, _vah, _volume. Bins are anchored at zero. Ties choose the lower bin; area expands until at least the target is covered."
);
impl VolumeProfile {
    pub fn new(period: usize, bin_size: f64) -> Self {
        Self::configured(period, bin_size, 0.7, "close", "volume")
    }
    fn configured(period: usize, size: f64, area: f64, price: &str, volume: &str) -> Self {
        let price = target(price);
        let name = format!("VP_{period}_{size}_{area}_{price}_{volume}");
        Self {
            inner: Calculator::new(
                Kind::Profile(period, size, area),
                vec![price, volume.into()],
                output(name, &["", "_val", "_vah", "_volume"]),
            ),
        }
    }
    pub fn with_value_area(self, fraction: f64) -> Self {
        let Kind::Profile(p, s, _) = self.inner.kind else {
            unreachable!()
        };
        self.configured_with_prefix(Self::configured(
            p,
            s,
            fraction,
            &self.inner.inputs[0],
            &self.inner.inputs[1],
        ))
    }
    pub fn with_columns(self, price: &str, volume: &str) -> Self {
        let Kind::Profile(p, s, a) = self.inner.kind else {
            unreachable!()
        };
        self.configured_with_prefix(Self::configured(p, s, a, price, volume))
    }
    /// Current occupied bins over the last lookback rows, for research/plotting. No future rows
    /// influence earlier run() outputs. This snapshot includes available valid rows even during
    /// warm-up; scalar outputs require a full valid window and strictly positive total volume.
    pub fn histogram(&self, df: &DataFrame) -> Result<Vec<VolumeProfileBin>, String> {
        self.inner.histogram(df)
    }
}
impl Default for VolumeProfile {
    fn default() -> Self {
        Self::new(100, 1.0)
    }
}
