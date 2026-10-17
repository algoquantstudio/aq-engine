//! Rolling fixed-price-bin volume profile. Each row's volume is assigned to its selected
//! price (close by default): this is a bar-data approximation, not reconstructed trade flow.
//! Feed trade-price/size rows or smaller-timeframe bars for finer volume-at-price resolution.
pub use super::api::VolumeProfile;
use super::kernels::{Sum, Values};
use std::collections::{BTreeMap, VecDeque};

#[derive(Clone, Debug, PartialEq)]
pub struct VolumeProfileBin {
    pub low: f64,
    pub high: f64,
    pub volume: f64,
}

pub(super) struct ProfileState {
    period: usize,
    size: f64,
    area: f64,
    queue: VecDeque<Option<(i64, f64)>>,
    bins: BTreeMap<i64, (Sum, usize)>,
    ordered: Vec<(i64, f64)>,
    total: Sum,
    valid: usize,
}
impl ProfileState {
    pub(super) fn new(period: usize, size: f64, area: f64) -> Self {
        Self {
            period,
            size,
            area,
            queue: VecDeque::new(),
            bins: BTreeMap::new(),
            ordered: Vec::new(),
            total: Sum::default(),
            valid: 0,
        }
    }
    pub(super) fn push(&mut self, price: Option<f64>, volume: Option<f64>) {
        if self.queue.len() == self.period {
            if let Some(Some((key, volume))) = self.queue.pop_front() {
                self.valid -= 1;
                self.total.add(-volume);
                if let Some((sum, count)) = self.bins.get_mut(&key) {
                    sum.add(-volume);
                    *count -= 1;
                    if *count == 0 {
                        self.bins.remove(&key);
                    }
                }
            }
        }
        let entry = price
            .zip(volume)
            .filter(|(p, v)| p.is_finite() && v.is_finite() && *v >= 0.0)
            .and_then(|(p, v)| {
                let key = (p / self.size).floor();
                // Exact integer bin identities and finite boundaries, without saturated casts.
                (key.is_finite()
                    && key.abs() < 4_503_599_627_370_496.0
                    && (key * self.size).is_finite()
                    && ((key + 1.0) * self.size).is_finite())
                .then_some((key as i64, v))
            });
        self.queue.push_back(entry);
        if let Some((key, volume)) = entry {
            self.valid += 1;
            self.total.add(volume);
            let (sum, count) = self.bins.entry(key).or_insert_with(|| (Sum::default(), 0));
            sum.add(volume);
            *count += 1;
        }
        if !self.total.value().is_finite() {
            self.queue.clear();
            self.bins.clear();
            self.ordered.clear();
            self.total = Sum::default();
            self.valid = 0;
        }
    }
    pub(super) fn next(&mut self, price: Option<f64>, volume: Option<f64>) -> Values {
        self.push(price, volume);
        if self.valid != self.period {
            return [None; 4];
        }
        let total = self.total.value();
        if !total.is_finite() || total <= 0.0 {
            return [None; 4];
        }
        self.ordered.clear();
        self.ordered.extend(
            self.bins
                .iter()
                .map(|(&key, (sum, _))| (key, sum.value().max(0.0))),
        );
        let mut poc = 0;
        for index in 1..self.ordered.len() {
            if self.ordered[index].1 > self.ordered[poc].1 {
                poc = index;
            }
        }
        let (mut low, mut high) = (poc, poc);
        let mut covered = self.ordered[poc].1;
        // Contiguous expansion to AT LEAST the requested fraction. Equal volumes favor
        // the lower-price bin. Empty bins between occupied levels are implicitly included.
        while covered < total * self.area && (low > 0 || high + 1 < self.ordered.len()) {
            if low > 0
                && (high + 1 == self.ordered.len()
                    || self.ordered[low - 1].1 >= self.ordered[high + 1].1)
            {
                low -= 1;
                covered += self.ordered[low].1;
            } else {
                high += 1;
                covered += self.ordered[high].1;
            }
        }
        [
            Some((self.ordered[poc].0 as f64 + 0.5) * self.size),
            Some(self.ordered[low].0 as f64 * self.size),
            Some((self.ordered[high].0 as f64 + 1.0) * self.size),
            Some(total),
        ]
        .map(|value| value.filter(|v| v.is_finite()))
    }
    pub(super) fn histogram(&self) -> Vec<VolumeProfileBin> {
        self.bins
            .iter()
            .map(|(&key, (sum, _))| VolumeProfileBin {
                low: key as f64 * self.size,
                high: (key as f64 + 1.0) * self.size,
                volume: sum.value().max(0.0),
            })
            .collect()
    }
}
