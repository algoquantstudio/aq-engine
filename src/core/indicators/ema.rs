//! Backwards-compatible import path for the optimized built-in indicator.
pub use super::api::ExponentialMovingAverage;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::indicators::Indicator;
    use polars::prelude::*;

    #[test]
    fn ema_appends_column() {
        let mut df = df!("close" => &[1.0, 2.0, 3.0, 4.0, 5.0]).unwrap();
        let mut ema = ExponentialMovingAverage::new(3, "close");
        ema.run(&mut df).unwrap();
        assert!(df.column("EMA_3").is_ok());
    }
}
