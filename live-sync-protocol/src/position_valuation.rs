//! Broker-neutral, read-only marks for exact physical positions. Ownership is
//! supplied by the consumer's execution journal, never inferred from a symbol.
use crate::ExecutionFillSide;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub const POSITION_VALUATION_VERSION: u16 = 1;
pub const MAX_POSITION_VALUATIONS: usize = 32;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PositionValuations {
    pub version: u16,
    pub provider: String,
    pub account_id: String,
    pub account_currency: String,
    pub symbol: String,
    pub as_of_us: i64,
    pub positions: Vec<PositionValuation>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PositionValuation {
    pub position_id: String,
    #[serde(flatten)]
    pub state: PositionValuationState,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum PositionValuationState {
    /// Observed absence in a complete bounded open-position read. This is NOT
    /// a close fill, proof of realized cash, or an instruction to close an Insight.
    NotOpen,
    Open {
        quantity: f64,
        side: ExecutionFillSide,
        average_entry_price: f64,
        current_price: f64,
        /// Broker-reported open trading P&L in account currency. Entry/exit
        /// commissions are excluded, and carry is kept separate to avoid reuse
        /// as both a cash-ledger fee and an open-position adjustment.
        gross_unrealized_pnl: f64,
        carry: f64,
        /// None means the source cannot establish quote freshness. Receiving
        /// a response now must never turn an old/unknown quote into a fresh mark.
        quote_time: Option<PositionQuoteTime>,
        notional: PositionNotional,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PositionQuoteTime {
    pub occurred_at_us: i64,
    pub clock: PositionValuationClock,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "basis", rename_all = "snake_case")]
pub enum PositionValuationClock {
    Utc,
    DeclaredFixedOffset {
        seconds: i32,
        valid_from_us: i64,
        valid_through_us: i64,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "basis", rename_all = "snake_case")]
pub enum PositionNotional {
    /// The paper execution model settles price x quantity directly in account
    /// units. This intentionally does not claim real-instrument FX/multipliers.
    PaperPriceUnits {
        account_value: f64,
    },
    /// Only valid for linear price x contract-size instruments whose price
    /// currency already equals the account currency; this is not an FX model.
    LinearContractSameCurrency {
        account_value: f64,
        units_per_quantity: f64,
        price_currency: String,
    },
    Unavailable {
        reason: PositionNotionalUnavailable,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionNotionalUnavailable {
    UnsupportedContract,
    CurrencyConversionUnavailable,
    InvalidContractMetadata,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PositionValuationFreshness {
    Fresh,
    Stale,
    UnknownQuoteTime,
    Future,
}

fn identity(value: &str, max: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max && !value.chars().any(char::is_control)
}

pub fn validate_position_valuation_request(
    account: &str,
    symbol: &str,
    positions: &[String],
) -> Result<(), &'static str> {
    if !identity(account, 128)
        || !identity(symbol, 256)
        || positions.is_empty()
        || positions.len() > MAX_POSITION_VALUATIONS
        || positions.iter().any(|p| !identity(p, 128))
        || positions.iter().collect::<HashSet<_>>().len() != positions.len()
    {
        return Err("invalid position valuation request");
    }
    Ok(())
}

impl PositionValuations {
    pub fn validate(
        &self,
        account: &str,
        symbol: &str,
        positions: &[String],
    ) -> Result<(), &'static str> {
        validate_position_valuation_request(account, symbol, positions)?;
        if self.version != POSITION_VALUATION_VERSION
            || !identity(&self.provider, 128)
            || self.account_id != account
            || self.symbol != symbol
            || !identity(&self.account_currency, 32)
            || self.as_of_us <= 0
            || self.positions.len() != positions.len()
        {
            return Err("invalid position valuation scope");
        }
        let mut expected: HashSet<_> = positions.iter().map(String::as_str).collect();
        for p in &self.positions {
            if !expected.remove(p.position_id.as_str()) {
                return Err("unexpected position valuation identity");
            }
            if let PositionValuationState::Open {
                quantity,
                average_entry_price,
                current_price,
                gross_unrealized_pnl,
                carry,
                quote_time,
                notional,
                ..
            } = &p.state
            {
                if !quantity.is_finite()
                    || *quantity <= 0.
                    || [
                        average_entry_price,
                        current_price,
                        gross_unrealized_pnl,
                        carry,
                    ]
                    .iter()
                    .any(|v| !v.is_finite())
                {
                    return Err("invalid open position valuation");
                }
                if let Some(q) = quote_time {
                    // A second-granularity broker observation can contain a
                    // quote from later in that same second, never a later one.
                    if q.occurred_at_us <= 0
                        || q.occurred_at_us > self.as_of_us.saturating_add(999_999)
                    {
                        return Err("invalid position quote time");
                    }
                    if let PositionValuationClock::DeclaredFixedOffset {
                        seconds,
                        valid_from_us,
                        valid_through_us,
                    } = q.clock
                    {
                        if seconds.unsigned_abs() > 14 * 3600
                            || valid_from_us <= 0
                            || valid_through_us < valid_from_us
                            || q.occurred_at_us < valid_from_us
                            || q.occurred_at_us > valid_through_us
                        {
                            return Err("invalid position quote clock");
                        }
                    }
                }
                if let PositionNotional::LinearContractSameCurrency {
                    account_value,
                    units_per_quantity,
                    price_currency,
                } = notional
                {
                    let expected = (quantity * current_price * units_per_quantity).abs();
                    if !account_value.is_finite()
                        || *account_value < 0.
                        || !units_per_quantity.is_finite()
                        || *units_per_quantity <= 0.
                        || price_currency != &self.account_currency
                        || !expected.is_finite()
                        || (account_value - expected).abs()
                            > expected.abs().max(account_value.abs()) * 1e-9
                    {
                        return Err("invalid position notional basis");
                    }
                }
                if let PositionNotional::PaperPriceUnits { account_value } = notional {
                    let expected = (quantity * current_price).abs();
                    if !account_value.is_finite()
                        || *account_value < 0.
                        || !expected.is_finite()
                        || (account_value - expected).abs()
                            > expected.abs().max(account_value.abs()) * 1e-9
                    {
                        return Err("invalid paper position valuation");
                    }
                }
            }
        }
        Ok(())
    }

    /// Call after validation. Freshness considers both observation and quote
    /// age; unknown prices are not silently treated as current. NotOpen has
    /// no quote but still expires with the observation.
    pub fn freshness(
        &self,
        position: &PositionValuation,
        now_us: i64,
        max_age_us: i64,
    ) -> PositionValuationFreshness {
        use PositionValuationFreshness::*;
        if now_us < self.as_of_us {
            return Future;
        }
        if max_age_us <= 0 || now_us.saturating_sub(self.as_of_us) > max_age_us {
            return Stale;
        }
        match &position.state {
            PositionValuationState::NotOpen => Fresh,
            PositionValuationState::Open {
                quote_time: None, ..
            } => UnknownQuoteTime,
            PositionValuationState::Open {
                quote_time: Some(q),
                ..
            } => {
                if now_us < q.occurred_at_us {
                    Future
                } else if now_us.saturating_sub(q.occurred_at_us) > max_age_us {
                    Stale
                } else {
                    Fresh
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> PositionValuations {
        serde_json::from_value(serde_json::json!({
            "version":1,"provider":"broker","account_id":"account","account_currency":"GBP","symbol":"GENERIC","as_of_us":2_000_000,
            "positions":[{"position_id":"1","status":"open","quantity":2.,"side":"sell","average_entry_price":95.,"current_price":100.,
                "gross_unrealized_pnl":-1000.,"carry":-4.,"quote_time":{"occurred_at_us":1_000_000,"clock":{"basis":"utc"}},
                "notional":{"basis":"linear_contract_same_currency","account_value":20000.,"units_per_quantity":100.,"price_currency":"GBP"}},
                {"position_id":"2","status":"not_open"}]
        })).unwrap()
    }
    #[test]
    fn bounded_exact_scope_and_numeric_evidence() {
        let p = fixture();
        let ids = vec!["1".into(), "2".into()];
        assert!(p.validate("account", "GENERIC", &ids).is_ok());
        assert!(p.validate("other", "GENERIC", &ids).is_err());
        assert!(p.validate("account", "OTHER", &ids).is_err());
        assert!(
            p.validate("account", "GENERIC", &["1".into(), "1".into()])
                .is_err()
        );
        assert!(validate_position_valuation_request("account", "GENERIC", &[]).is_err());
        assert!(
            validate_position_valuation_request(
                "account",
                "GENERIC",
                &(0..33).map(|x| x.to_string()).collect::<Vec<_>>()
            )
            .is_err()
        );
        let mut bad = p.clone();
        bad.positions[1] = bad.positions[0].clone();
        assert!(bad.validate("account", "GENERIC", &ids).is_err());
        for value in [f64::NAN, f64::INFINITY, -1., 0.] {
            let mut bad = p.clone();
            if let PositionValuationState::Open { quantity, .. } = &mut bad.positions[0].state {
                *quantity = value;
            }
            assert!(bad.validate("account", "GENERIC", &ids).is_err());
        }
        for (value, currency) in [(100., "GBP"), (20000., "USD"), (f64::INFINITY, "GBP")] {
            let mut bad = p.clone();
            if let PositionValuationState::Open { notional, .. } = &mut bad.positions[0].state {
                *notional = PositionNotional::LinearContractSameCurrency {
                    account_value: value,
                    units_per_quantity: 100.,
                    price_currency: currency.into(),
                };
            }
            assert!(bad.validate("account", "GENERIC", &ids).is_err());
        }
    }
    #[test]
    fn quote_age_unknown_and_unsupported_are_not_fresh_zeroes() {
        use PositionValuationFreshness::*;
        let mut p = fixture();
        assert_eq!(p.freshness(&p.positions[0], 2_000_000, 1_000_000), Fresh);
        assert_eq!(p.freshness(&p.positions[0], 2_000_001, 1_000_000), Stale);
        assert_eq!(p.freshness(&p.positions[1], 2_000_001, 1_000_000), Fresh);
        assert_eq!(p.freshness(&p.positions[1], 3_000_001, 1_000_000), Stale);
        assert_eq!(p.freshness(&p.positions[0], 1_000_000, 1_000_000), Future);
        if let PositionValuationState::Open {
            quote_time,
            notional,
            ..
        } = &mut p.positions[0].state
        {
            *quote_time = None;
            *notional = PositionNotional::Unavailable {
                reason: PositionNotionalUnavailable::CurrencyConversionUnavailable,
            };
        }
        assert!(
            p.validate("account", "GENERIC", &["1".into(), "2".into()])
                .is_ok()
        );
        assert_eq!(
            p.freshness(&p.positions[0], 2_000_000, 1_000_000),
            UnknownQuoteTime
        );
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains("currency_conversion_unavailable"));
        assert_eq!(
            serde_json::from_str::<PositionValuations>(&json).unwrap(),
            p
        );
    }
    #[test]
    fn fixed_clock_window_and_future_quote_are_validated() {
        for (time, seconds, from, through, valid) in [
            (1_000_000, 3600, 1, 2_000_000, true),
            (1_000_000, 3600, 1_000_001, 2_000_000, false),
            (1_000_000, 86400, 1, 2_000_000, false),
            (1_000_000, 3600, 1, 999_999, false),
            (3_000_000, 3600, 1, 4_000_000, false),
        ] {
            let mut p = fixture();
            if let PositionValuationState::Open { quote_time, .. } = &mut p.positions[0].state {
                *quote_time = Some(PositionQuoteTime {
                    occurred_at_us: time,
                    clock: PositionValuationClock::DeclaredFixedOffset {
                        seconds,
                        valid_from_us: from,
                        valid_through_us: through,
                    },
                });
            }
            assert_eq!(
                p.validate("account", "GENERIC", &["1".into(), "2".into()])
                    .is_ok(),
                valid
            );
        }
    }
}
