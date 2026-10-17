//! Backwards-compatible import path for the optimized built-in indicator.
pub use super::api::RelativeStrengthIndex;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::indicators::Indicator;
    use polars::prelude::*;

    #[test]
    fn rsi_appends_column() {
        let mut df = df!("close" => &[1.0, 2.0, 1.5, 2.5, 2.0, 3.0, 2.8]).unwrap();
        let mut rsi = RelativeStrengthIndex::new(3, "close");
        rsi.run(&mut df).unwrap();
        assert!(df.column("RSI_3").is_ok());
    }
}
