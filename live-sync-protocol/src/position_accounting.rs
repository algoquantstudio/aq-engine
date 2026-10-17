//! Explicit economics for one fully closed, single-owner broker position.
//! This replaces previous revisions of the same (provider, position_id); it is
//! never an additional fill and must not be added to per-deal/partial-exit P&L.
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

pub const POSITION_CLOSE_ACCOUNTING_VERSION: u16 = 1;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PositionCloseAccounting {
    pub version: u16,
    /// Versioned adapter/source contract, not a strategy or alpha label.
    pub provider: String,
    /// Account-scoped durable broker position identity.
    pub position_id: String,
    /// One distinct entry order. Multi-owner/netted histories are unsupported.
    pub entry_order_id: String,
    pub currency: String,
    pub opened_at_us: i64,
    pub closed_at_us: i64,
    /// Time this complete broker history was read. Older deliveries cannot
    /// overwrite a newer accounting observation for the same position.
    pub as_of_us: i64,
    /// Lifetime realized amount including commissions and signed carry.
    pub net_pnl: f64,
    /// Positive expense, negative rebate; never the absolute value of a credit.
    pub commission: f64,
    /// Signed carry/financing; positive credits increase net P&L.
    pub swap: f64,
}

/// Shared historical/live accounting equation. Missing/invalid inputs must not
/// become economic zero, including overflow of otherwise finite source values.
pub fn gross_from_net(net: f64, commission: f64, swap: f64) -> Option<f64> {
    let gross = net + commission - swap;
    [net, commission, swap, gross]
        .into_iter()
        .all(f64::is_finite)
        .then_some(gross)
}

impl PositionCloseAccounting {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.version != POSITION_CLOSE_ACCOUNTING_VERSION {
            return Err("unsupported position accounting version");
        }
        if [&self.provider, &self.position_id, &self.entry_order_id]
            .into_iter()
            .any(|value| value.trim().is_empty() || value.len() > 128)
            || self.currency.trim().is_empty()
            || self.currency.len() > 32
        {
            return Err("invalid position accounting identity/currency");
        }
        if self.opened_at_us <= 0
            || self.closed_at_us < self.opened_at_us
            || self.as_of_us < self.closed_at_us
        {
            return Err("invalid position accounting chronology");
        }
        if gross_from_net(self.net_pnl, self.commission, self.swap).is_none() {
            return Err("invalid position accounting values");
        }
        Ok(())
    }

    pub fn owns_order(&self, order_id: &str) -> bool {
        self.entry_order_id == order_id || self.position_id == order_id
    }
}

// Checkpoint fingerprints run on the owner loop: hash the small typed record
// directly instead of allocating/serializing JSON on every fingerprint check.
impl Hash for PositionCloseAccounting {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.version.hash(state);
        self.provider.hash(state);
        self.position_id.hash(state);
        self.entry_order_id.hash(state);
        self.currency.hash(state);
        self.opened_at_us.hash(state);
        self.closed_at_us.hash(state);
        self.as_of_us.hash(state);
        for value in [self.net_pnl, self.commission, self.swap] {
            // PartialEq treats signed zero as equal, so Hash must do likewise.
            (if value == 0.0 { 0.0 } else { value })
                .to_bits()
                .hash(state);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gross_equation_preserves_carry_and_rebates_without_double_costs() {
        assert_eq!(gross_from_net(95., 7., 2.), Some(100.));
        assert_eq!(gross_from_net(103., -2., 1.), Some(100.));
        assert_eq!(gross_from_net(0., 0., 0.), Some(0.));
        assert_eq!(gross_from_net(f64::NAN, 0., 0.), None);
        assert_eq!(gross_from_net(f64::MAX, f64::MAX, 0.), None);
    }
    #[test]
    fn position_accounting_validation_and_hash_are_deterministic() {
        let a = PositionCloseAccounting {
            version: 1,
            provider: "generic-v1".into(),
            position_id: "p".into(),
            entry_order_id: "e".into(),
            currency: "USD".into(),
            opened_at_us: 100,
            closed_at_us: 200,
            as_of_us: 300,
            net_pnl: 0.,
            commission: 0.,
            swap: 0.,
        };
        assert!(a.validate().is_ok());
        assert!(a.owns_order("p") && a.owns_order("e"));
        assert!(!a.owns_order("other"));
        for mut invalid in [a.clone(), a.clone(), a.clone(), a.clone(), a.clone()]
            .into_iter()
            .enumerate()
        {
            match invalid.0 {
                0 => invalid.1.version = 0,
                1 => invalid.1.currency = " ".into(),
                2 => invalid.1.closed_at_us = 99,
                3 => invalid.1.as_of_us = 199,
                _ => invalid.1.commission = f64::INFINITY,
            }
            assert!(invalid.1.validate().is_err());
        }
        let hash = |v: &PositionCloseAccounting| {
            let mut h = std::collections::hash_map::DefaultHasher::new();
            v.hash(&mut h);
            h.finish()
        };
        let mut b = a.clone();
        b.net_pnl = -0.;
        b.commission = -0.;
        assert_eq!(a, b);
        assert_eq!(hash(&a), hash(&b));
    }
}
