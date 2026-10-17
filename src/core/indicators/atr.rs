//! Backwards-compatible import path for the optimized built-in indicator.
pub use super::api::AverageTrueRange;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::indicators::Indicator;
    use polars::prelude::*;

    #[test]
    fn computes_atr_column() {
        let mut df = DataFrame::new(
            4,
            vec![
                Series::new("high".into(), &[10.0, 12.0, 13.0, 14.0]).into(),
                Series::new("low".into(), &[8.0, 9.0, 11.0, 12.0]).into(),
                Series::new("close".into(), &[9.0, 11.0, 12.0, 13.0]).into(),
            ],
        )
        .unwrap();

        let mut atr = AverageTrueRange::new(3);
        atr.run(&mut df).unwrap();

        let values: Vec<Option<f64>> = df.column("ATRr_3").unwrap().f64().unwrap().iter().collect();

        assert_eq!(values[0], None);
        assert_eq!(values[1], None);
        assert_eq!(values[2], Some(7.0 / 3.0));
        assert_eq!(values[3], Some(7.0 / 3.0));
    }

    #[test]
    fn computes_latest_atr_from_run_bar() {
        let df = DataFrame::new(
            3,
            vec![
                Series::new("high".into(), &[10.0, 12.0, 13.0]).into(),
                Series::new("low".into(), &[8.0, 9.0, 11.0]).into(),
                Series::new("close".into(), &[9.0, 11.0, 12.0]).into(),
            ],
        )
        .unwrap();

        let mut atr = AverageTrueRange::new(3);
        let values = atr.run_bar(&df).unwrap();
        assert_eq!(values.get("ATRr_3").copied(), Some(7.0 / 3.0));
    }
}
