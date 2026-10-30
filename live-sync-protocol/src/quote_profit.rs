//! Read-only pricing coefficients, calibrated by the execution broker for the
//! current quote and account. They are not lifecycle or historical evidence.
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BrokerQuoteProfit {
    pub version: u8,
    pub account_id: String,
    pub currency: String,
    pub symbol: String,
    pub as_of_us: i64,
    /// Cash per one price unit and one broker quantity unit. Separate native
    /// gain/loss coefficients retain the broker's asymmetric FX conversion.
    pub buy_gain: f64,
    pub buy_loss: f64,
    pub sell_gain: f64,
    pub sell_loss: f64,
}

impl BrokerQuoteProfit {
    pub fn profit(
        &self,
        account: &str,
        currency: &str,
        symbol: &str,
        side: &str,
        entry: f64,
        exit: f64,
        quantity: f64,
        quote_at_us: i64,
        now_us: i64,
    ) -> Option<f64> {
        let fresh = |at: i64, age: i64| at > 0 && now_us >= at && now_us.saturating_sub(at) <= age;
        if self.version != 1
            || self.account_id != account
            || self.currency != currency
            || !self.symbol.eq_ignore_ascii_case(symbol)
            || account.is_empty()
            || currency.is_empty()
            || !fresh(self.as_of_us, 15_000_000)
            || !fresh(quote_at_us, 30_000_000)
            || ![
                entry,
                exit,
                quantity,
                self.buy_gain,
                self.buy_loss,
                self.sell_gain,
                self.sell_loss,
            ]
            .into_iter()
            .all(|v| v.is_finite() && v > 0.0)
        {
            return None;
        }
        let delta = match side {
            "Buy" => exit - entry,
            "Sell" => entry - exit,
            _ => return None,
        };
        let coefficient = match (side, delta >= 0.0) {
            ("Buy", true) => self.buy_gain,
            ("Buy", false) => self.buy_loss,
            (_, true) => self.sell_gain,
            (_, false) => self.sell_loss,
        };
        let result = delta * quantity * coefficient;
        result.is_finite().then_some(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_coefficients_cover_contract_fx_side_zero_and_scope() {
        let mut m = BrokerQuoteProfit {
            version: 1,
            account_id: "GBP-account".into(),
            currency: "GBP".into(),
            symbol: "BTCUSD".into(),
            as_of_us: 100_000_000,
            buy_gain: 0.75,
            buy_loss: 0.76,
            sell_gain: 0.75,
            sell_loss: 0.76,
        };
        let calc = |m: &BrokerQuoteProfit, side, entry, exit, quantity| {
            m.profit(
                "GBP-account",
                "GBP",
                "BTCUSD",
                side,
                entry,
                exit,
                quantity,
                100_000_000,
                101_000_000,
            )
        };
        assert_eq!(
            calc(&m, "Sell", 80360.39, 80287.81, 10.).unwrap().round(),
            544.
        );
        assert_eq!(calc(&m, "Buy", 100., 90., 2.), Some(-15.2));
        assert_eq!(calc(&m, "Sell", 100., 100., 2.), Some(0.));
        m.buy_gain = 75000.; // e.g. 100,000-unit FX contract, converted to GBP.
        assert!((calc(&m, "Buy", 1.1, 1.101, 0.5).unwrap() - 37.5).abs() < 1e-8);
        assert!(
            m.profit(
                "other",
                "GBP",
                "BTCUSD",
                "Buy",
                1.,
                2.,
                1.,
                100_000_000,
                101_000_000
            )
            .is_none()
        );
        assert!(
            m.profit(
                "GBP-account",
                "USD",
                "BTCUSD",
                "Buy",
                1.,
                2.,
                1.,
                100_000_000,
                101_000_000
            )
            .is_none()
        );
        assert!(
            m.profit(
                "GBP-account",
                "GBP",
                "SOLUSD",
                "Buy",
                1.,
                2.,
                1.,
                100_000_000,
                101_000_000
            )
            .is_none()
        );
        assert!(
            m.profit(
                "GBP-account",
                "GBP",
                "BTCUSD",
                "Buy",
                1.,
                2.,
                1.,
                100_000_000,
                116_000_000
            )
            .is_none()
        );
        assert!(
            m.profit(
                "GBP-account",
                "GBP",
                "BTCUSD",
                "Buy",
                1.,
                2.,
                1.,
                50_000_000,
                101_000_000
            )
            .is_none()
        );
        m.buy_gain = f64::NAN;
        assert!(calc(&m, "Buy", 1., 2., 1.).is_none());
    }
}
