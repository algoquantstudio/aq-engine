//! Columnar history construction shared by built-in feeds and the default API.
use super::types::Bar;
use polars::prelude::*;

pub(super) fn from_bars(bars: &[Bar]) -> PolarsResult<DataFrame> {
    DataFrame::new(
        bars.len(),
        vec![
            StringChunked::from_iter_values(
                "symbol".into(),
                bars.iter().map(|b| b.symbol.as_str()),
            )
            .into_column(),
            Float64Chunked::from_iter_values("open".into(), bars.iter().map(|b| b.open))
                .into_column(),
            Float64Chunked::from_iter_values("high".into(), bars.iter().map(|b| b.high))
                .into_column(),
            Float64Chunked::from_iter_values("low".into(), bars.iter().map(|b| b.low))
                .into_column(),
            Float64Chunked::from_iter_values("close".into(), bars.iter().map(|b| b.close))
                .into_column(),
            Float64Chunked::from_iter_values("volume".into(), bars.iter().map(|b| b.volume))
                .into_column(),
            Int64Chunked::from_iter_values(
                "timestamp".into(),
                bars.iter().map(|b| b.timestamp.timestamp_millis()),
            )
            .into_column(),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(count: usize) -> Vec<Bar> {
        (0..count)
            .map(|i| Bar {
                symbol: "TEST".into(),
                open: i as f64 + 2.0,
                high: i as f64 + 3.0,
                low: i as f64 + 1.0,
                close: i as f64 + 2.5,
                volume: i as f64,
                timestamp: chrono::DateTime::from_timestamp_millis(
                    1_700_000_000_000 + i as i64 * 60_000,
                )
                .unwrap(),
            })
            .collect()
    }

    // Previous get_history path: clone cached rows and rebuild every column.
    fn previous_history(bars: &[Bar]) -> DataFrame {
        let bars = bars.to_vec();
        let symbols: Vec<&str> = bars.iter().map(|b| b.symbol.as_str()).collect();
        let opens: Vec<f64> = bars.iter().map(|b| b.open).collect();
        let highs: Vec<f64> = bars.iter().map(|b| b.high).collect();
        let lows: Vec<f64> = bars.iter().map(|b| b.low).collect();
        let closes: Vec<f64> = bars.iter().map(|b| b.close).collect();
        let volumes: Vec<f64> = bars.iter().map(|b| b.volume).collect();
        let timestamps: Vec<i64> = bars
            .iter()
            .map(|b| b.timestamp.timestamp_millis())
            .collect();
        DataFrame::new(
            bars.len(),
            vec![
                Column::new("symbol".into(), &symbols),
                Column::new("open".into(), &opens),
                Column::new("high".into(), &highs),
                Column::new("low".into(), &lows),
                Column::new("close".into(), &closes),
                Column::new("volume".into(), &volumes),
                Column::new("timestamp".into(), &timestamps),
            ],
        )
        .unwrap()
    }

    #[test]
    fn columnar_history_matches_bar_api_schema_and_values() {
        for count in [0, 1, 1024] {
            let bars = fixture(count);
            let frame = from_bars(&bars).unwrap();
            assert!(frame.equals_missing(&previous_history(&bars)));
        }
    }

    #[test]
    #[ignore = "manual paired columnar history performance probe"]
    fn columnar_history_reuse_performance_probe() {
        use std::{hint::black_box, time::Instant};
        let bars = fixture(100_000);
        let frame = from_bars(&bars).unwrap();
        let range = 10_000..60_000;
        assert!(
            frame
                .slice(range.start as i64, range.len())
                .equals_missing(&previous_history(&bars[range.clone()]))
        );
        let mut samples: [Vec<u128>; 2] = Default::default();
        for iteration in 0..60 {
            for variant in [iteration % 2, 1 - iteration % 2] {
                let started = Instant::now();
                if variant == 0 {
                    black_box(previous_history(black_box(&bars[range.clone()])));
                } else {
                    black_box(black_box(&frame).slice(range.start as i64, range.len()));
                }
                samples[variant].push(started.elapsed().as_nanos());
            }
        }
        for sample in &mut samples {
            sample.sort_unstable();
        }
        eprintln!(
            "columnar_history rows=100000 selected=50000 before_p50_ns={} before_p95_ns={} after_p50_ns={} after_p95_ns={} frame_bytes={}",
            samples[0][30],
            samples[0][57],
            samples[1][30],
            samples[1][57],
            frame.estimated_size()
        );
    }
}
