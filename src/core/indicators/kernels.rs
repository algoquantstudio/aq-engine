//! Streaming kernels with no per-row frame copies. Rolling storage is bounded by lookback.
use super::volume_profile::ProfileState;
use std::collections::VecDeque;

pub(super) const OUTPUTS: usize = 4;
pub(super) type Values = [Option<f64>; OUTPUTS];

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Smoothing {
    #[default]
    Simple,
    Exponential,
    Wilder,
}

/// Neumaier compensation limits accumulated cancellation error in long-running rolling sums.
#[derive(Default)]
pub(super) struct Sum {
    sum: f64,
    correction: f64,
}
impl Sum {
    pub(super) fn add(&mut self, x: f64) {
        let next = self.sum + x;
        self.correction += if self.sum.abs() >= x.abs() {
            (self.sum - next) + x
        } else {
            (x - next) + self.sum
        };
        self.sum = next;
    }
    pub(super) fn value(&self) -> f64 {
        self.sum + self.correction
    }
}

pub(super) struct Average {
    period: usize,
    mode: Smoothing,
    alpha: f64,
    queue: VecDeque<Option<f64>>,
    sum: Sum,
    count: usize,
    previous: Option<f64>,
}
impl Average {
    pub(super) fn new(period: usize, mode: Smoothing) -> Self {
        Self {
            period,
            mode,
            alpha: if mode == Smoothing::Wilder {
                1.0 / period as f64
            } else {
                2.0 / (period as f64 + 1.0)
            },
            queue: VecDeque::new(),
            sum: Sum::default(),
            count: 0,
            previous: None,
        }
    }
    pub(super) fn next(&mut self, value: Option<f64>) -> Option<f64> {
        let result = match self.mode {
            Smoothing::Simple => {
                if self.queue.len() == self.period {
                    if let Some(Some(old)) = self.queue.pop_front() {
                        self.sum.add(-old);
                        self.count -= 1;
                    }
                }
                self.queue.push_back(value);
                if let Some(value) = value {
                    self.sum.add(value);
                    self.count += 1;
                }
                (self.count == self.period).then(|| self.sum.value() / self.period as f64)
            }
            _ => {
                // Missing observations do not invent a new sample or alter smoothing state.
                let Some(value) = value else {
                    return self.previous;
                };
                if let Some(previous) = self.previous {
                    let alpha = self.alpha;
                    self.previous = Some(previous * (1.0 - alpha) + value * alpha);
                } else {
                    self.sum.add(value);
                    self.count += 1;
                    if self.count == self.period {
                        self.previous = Some(self.sum.value() / self.period as f64);
                    }
                }
                self.previous
            }
        };
        if !self.sum.value().is_finite() || result.is_some_and(|v| !v.is_finite()) {
            // Out-of-range arithmetic must not permanently poison a long-running stream.
            // Treat the overflow as a gap and require a fresh seed/window.
            self.queue.clear();
            self.sum = Sum::default();
            self.count = 0;
            self.previous = None;
            None
        } else {
            result
        }
    }
}

/// Rolling Welford updates/removals avoid E[x²] - E[x]² cancellation for large prices.
pub(super) struct Moments {
    period: usize,
    queue: VecDeque<Option<f64>>,
    count: usize,
    mean: f64,
    m2: f64,
}
impl Moments {
    fn new(period: usize) -> Self {
        Self {
            period,
            queue: VecDeque::new(),
            count: 0,
            mean: 0.0,
            m2: 0.0,
        }
    }
    fn next(&mut self, value: Option<f64>) -> Option<(f64, f64)> {
        if self.queue.len() == self.period {
            if let Some(Some(old)) = self.queue.pop_front() {
                if self.count <= 1 {
                    self.count = 0;
                    self.mean = 0.0;
                    self.m2 = 0.0;
                } else {
                    let old_mean = self.mean;
                    self.count -= 1;
                    self.mean -= (old - self.mean) / self.count as f64;
                    self.m2 = (self.m2 - (old - old_mean) * (old - self.mean)).max(0.0);
                }
            }
        }
        self.queue.push_back(value);
        if let Some(value) = value {
            self.count += 1;
            let delta = value - self.mean;
            self.mean += delta / self.count as f64;
            self.m2 += delta * (value - self.mean);
        }
        if !self.mean.is_finite() || !self.m2.is_finite() {
            self.queue.clear();
            self.count = 0;
            self.mean = 0.0;
            self.m2 = 0.0;
            return None;
        }
        (self.count == self.period)
            .then(|| (self.mean, (self.m2.max(0.0) / self.period as f64).sqrt()))
    }
}

pub(super) enum Kernel {
    Average(Average),
    Atr {
        average: Average,
        previous: Option<f64>,
    },
    Rsi {
        gain: Average,
        loss: Average,
        previous: Option<f64>,
    },
    Qstick(Average),
    Macd {
        fast: Average,
        slow: Average,
        signal: Average,
    },
    Bands {
        moments: Moments,
        deviations: f64,
    },
    Vwma {
        price_volume: Average,
        volume: Average,
    },
    Roc {
        period: usize,
        queue: VecDeque<Option<f64>>,
    },
    Profile(ProfileState),
}

#[derive(Clone)]
pub(super) enum Kind {
    Average(usize, Smoothing),
    Atr(usize, Smoothing),
    Rsi(usize),
    Qstick(usize, Smoothing),
    Macd(usize, usize, usize),
    Bands(usize, f64),
    Vwma(usize),
    Roc(usize),
    Profile(usize, f64, f64),
}
impl Kind {
    /// Finite-window indicators can answer stateless latest-value queries from a tail.
    /// Recursive averages must retain their complete seed history instead.
    pub(super) fn stateless_tail(&self) -> Option<usize> {
        match *self {
            Self::Average(p, Smoothing::Simple)
            | Self::Qstick(p, Smoothing::Simple)
            | Self::Bands(p, _)
            | Self::Vwma(p)
            | Self::Profile(p, _, _) => Some(p),
            Self::Atr(p, Smoothing::Simple) | Self::Roc(p) => Some(p.saturating_add(1)),
            _ => None,
        }
    }
    pub(super) fn validate(&self) -> Result<(), String> {
        let period = |p: usize| {
            if (1..=1_000_000).contains(&p) {
                Ok(())
            } else {
                Err("Indicator periods must be between 1 and 1000000".to_string())
            }
        };
        match *self {
            Self::Average(p, _)
            | Self::Atr(p, _)
            | Self::Rsi(p)
            | Self::Qstick(p, _)
            | Self::Vwma(p)
            | Self::Roc(p) => period(p),
            Self::Macd(f, s, signal) => {
                period(f)?;
                period(s)?;
                period(signal)?;
                if f >= s {
                    Err("MACD fast period must be less than slow period".into())
                } else {
                    Ok(())
                }
            }
            Self::Bands(p, d) => {
                period(p)?;
                if d.is_finite() && d > 0.0 {
                    Ok(())
                } else {
                    Err("Band deviations must be finite and positive".into())
                }
            }
            Self::Profile(p, size, area) => {
                period(p)?;
                if size.is_finite() && size > 0.0 && area.is_finite() && area > 0.0 && area <= 1.0 {
                    Ok(())
                } else {
                    Err("Volume profile requires a positive finite bin size and value-area fraction in (0, 1]".into())
                }
            }
        }
    }
    pub(super) fn window(&self) -> usize {
        match *self {
            Self::Rsi(p) | Self::Roc(p) => p.saturating_add(1),
            Self::Macd(_, s, signal) => s.saturating_add(signal).saturating_sub(1),
            Self::Average(p, _)
            | Self::Atr(p, _)
            | Self::Qstick(p, _)
            | Self::Bands(p, _)
            | Self::Vwma(p)
            | Self::Profile(p, _, _) => p,
        }
    }
    pub(super) fn kernel(&self) -> Kernel {
        match *self {
            Self::Average(p, m) => Kernel::Average(Average::new(p, m)),
            Self::Atr(p, m) => Kernel::Atr {
                average: Average::new(p, m),
                previous: None,
            },
            Self::Rsi(p) => Kernel::Rsi {
                gain: Average::new(p, Smoothing::Wilder),
                loss: Average::new(p, Smoothing::Wilder),
                previous: None,
            },
            Self::Qstick(p, m) => Kernel::Qstick(Average::new(p, m)),
            Self::Macd(f, s, g) => Kernel::Macd {
                fast: Average::new(f, Smoothing::Exponential),
                slow: Average::new(s, Smoothing::Exponential),
                signal: Average::new(g, Smoothing::Exponential),
            },
            Self::Bands(p, d) => Kernel::Bands {
                moments: Moments::new(p),
                deviations: d,
            },
            Self::Vwma(p) => Kernel::Vwma {
                price_volume: Average::new(p, Smoothing::Simple),
                volume: Average::new(p, Smoothing::Simple),
            },
            Self::Roc(p) => Kernel::Roc {
                period: p,
                queue: VecDeque::new(),
            },
            Self::Profile(p, s, a) => Kernel::Profile(ProfileState::new(p, s, a)),
        }
    }
}
impl Kernel {
    pub(super) fn next(&mut self, row: &[Option<f64>]) -> Values {
        let mut out = [None; OUTPUTS];
        out[0] = match self {
            Self::Average(average) => average.next(row[0]),
            Self::Qstick(average) => average.next(
                row[1]
                    .zip(row[0])
                    .map(|(close, open)| close - open)
                    .filter(|v| v.is_finite()),
            ),
            Self::Atr { average, previous } => {
                let tr = row[0].zip(row[1]).filter(|(h, l)| h >= l).map(|(h, l)| {
                    previous.map_or(h - l, |c| (h - l).max((h - c).abs()).max((l - c).abs()))
                });
                *previous = row[2];
                average.next(tr.filter(|v| v.is_finite()))
            }
            Self::Rsi {
                gain,
                loss,
                previous,
            } => {
                let delta = previous
                    .zip(row[0])
                    .map(|(p, c)| c - p)
                    .filter(|v| v.is_finite());
                *previous = row[0];
                let g = gain.next(delta.map(|d| d.max(0.0)));
                let l = loss.next(delta.map(|d| (-d).max(0.0)));
                g.zip(l).map(|(g, l)| {
                    if g == 0.0 && l == 0.0 {
                        50.0
                    } else if l == 0.0 {
                        100.0
                    } else if g >= l {
                        100.0 / (1.0 + l / g)
                    } else {
                        let ratio = g / l;
                        100.0 * ratio / (1.0 + ratio)
                    }
                })
            }
            Self::Macd { fast, slow, signal } => {
                let line = fast
                    .next(row[0])
                    .zip(slow.next(row[0]))
                    .map(|(f, s)| f - s)
                    .filter(|v| v.is_finite());
                // A missing source must not repeatedly seed/advance the signal EMA.
                let sig = signal.next(if row[0].is_some() { line } else { None });
                out[1] = sig;
                out[2] = line.zip(sig).map(|(l, s)| l - s);
                line
            }
            Self::Bands {
                moments,
                deviations,
            } => moments.next(row[0]).map(|(mean, std)| {
                out[1] = Some(mean + *deviations * std);
                out[2] = Some(mean - *deviations * std);
                mean
            }),
            Self::Vwma {
                price_volume,
                volume,
            } => {
                let pair = row[0]
                    .zip(row[1])
                    .filter(|(p, v)| *v >= 0.0 && (p * v).is_finite());
                let pv = price_volume.next(pair.map(|(p, v)| p * v));
                let v = volume.next(pair.map(|(_, v)| v));
                pv.zip(v).filter(|(_, v)| *v > 0.0).map(|(pv, v)| pv / v)
            }
            Self::Roc { period, queue } => {
                let old = if queue.len() == *period {
                    queue.pop_front().flatten()
                } else {
                    None
                };
                queue.push_back(row[0]);
                old.zip(row[0])
                    .filter(|(old, _)| *old != 0.0)
                    .map(|(old, now)| (now / old - 1.0) * 100.0)
            }
            Self::Profile(profile) => return profile.next(row[0], row[1]),
        };
        for value in &mut out {
            *value = value.filter(|v| v.is_finite());
        }
        out
    }
}
