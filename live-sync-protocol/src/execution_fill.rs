//! Broker execution facts, independent of Insight and strategy graph concepts.
//! A fill is an executed delta, never a cumulative order/position snapshot.
use serde::{Deserialize, Serialize};

pub const EXECUTION_FILL_VERSION: u16 = 1;
/// Version 2 adds explicit broker invalidations. Legacy pages remain readable;
/// old consumers must reject v2 rather than count the retained audit fact.
pub const EXECUTION_FILL_PAGE_VERSION: u16 = 2;
/// Version 3 adds ordered, non-economic position comparison updates.
pub const EXECUTION_POSITION_CHECK_PAGE_VERSION: u16 = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionFillSide {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionFillEffect {
    Entry,
    Exit,
    /// Netting reversal: do not guess the opening/closing quantity split.
    Reversal,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionFillCash {
    pub currency: String,
    /// Realized price P&L posted by this fill, excluding commission and carry.
    pub gross_pnl: Option<f64>,
    /// Positive expense, negative rebate. Includes broker execution fees.
    pub commission: Option<f64>,
    /// Signed financing credit/debit posted by this fill, not lifetime carry.
    pub carry: Option<f64>,
}

impl ExecutionFillCash {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !identity(&self.currency, 32)
            || [self.gross_pnl, self.commission, self.carry]
                .into_iter()
                .flatten()
                .any(|n| !n.is_finite())
            || (self.gross_pnl.is_some()
                && self.commission.is_some()
                && self.carry.is_some()
                && self.net().is_none())
        {
            return Err("invalid execution fill cash values");
        }
        Ok(())
    }

    pub fn net(&self) -> Option<f64> {
        let net = self.gross_pnl? - self.commission? + self.carry?;
        net.is_finite().then_some(net)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionFill {
    pub version: u16,
    /// Versioned source semantics; uniqueness is (account, provider, fill_id).
    pub provider: String,
    /// Stable broker deal/execution identity, not a transport event UUID.
    pub fill_id: String,
    pub order_id: String,
    pub position_id: Option<String>,
    pub symbol: String,
    pub side: ExecutionFillSide,
    pub effect: ExecutionFillEffect,
    pub quantity: f64,
    pub price: f64,
    pub occurred_at_us: i64,
    /// Source observation watermark. A newer correction replaces this fill.
    pub as_of_us: i64,
    pub cash: Option<ExecutionFillCash>,
    /// Already converted executed-side notional in cash.currency. Absent when
    /// the adapter cannot establish multiplier/FX/valuation semantics.
    pub account_notional: Option<f64>,
}

fn identity(value: &str, max: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max && !value.chars().any(char::is_control)
}

impl ExecutionFill {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.version != EXECUTION_FILL_VERSION {
            return Err("unsupported execution fill version");
        }
        if [&self.provider, &self.fill_id, &self.order_id]
            .into_iter()
            .any(|s| !identity(s, 128))
            || !identity(&self.symbol, 256)
            || self
                .position_id
                .as_deref()
                .is_some_and(|s| !identity(s, 128))
        {
            return Err("invalid execution fill identity");
        }
        if self.occurred_at_us <= 0 || self.as_of_us < self.occurred_at_us {
            return Err("invalid execution fill chronology");
        }
        // Negative/zero traded prices are valid for some instruments.
        if !self.quantity.is_finite()
            || self.quantity <= 0.
            || !self.price.is_finite()
            || self
                .account_notional
                .is_some_and(|n| !n.is_finite() || n < 0.)
        {
            return Err("invalid execution fill quantity/price/notional");
        }
        if let Some(cash) = &self.cash {
            cash.validate()?;
        } else if self.account_notional.is_some() {
            return Err("execution fill notional requires a declared currency");
        }
        Ok(())
    }

    /// Ignore refresh timestamps when deciding whether to append a revision.
    pub fn same_facts(&self, other: &Self) -> bool {
        let mut other = other.clone();
        other.as_of_us = self.as_of_us;
        *self == other
    }
}

/// Ownership is separate from the broker fact. Unresolved ownership is retained,
/// not silently assigned from symbol or alpha class.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionFillInput {
    pub insight_id: Option<String>,
    pub fill: ExecutionFill,
}

impl ExecutionFillInput {
    pub fn validate(&self) -> Result<(), &'static str> {
        self.fill.validate()?;
        if self
            .insight_id
            .as_deref()
            .is_some_and(|id| !identity(id, 128))
        {
            return Err("invalid execution fill owner");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionFillRecord {
    pub sequence: u64,
    /// Original epoch is retained when late corrections arrive after restart.
    pub execution_epoch: u64,
    pub recorded_at_us: i64,
    pub observation: ExecutionFillInput,
    /// The observation remains the original audit evidence, not an active fill.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invalidation: Option<ExecutionFillInvalidation>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionFillInvalidationReason {
    BrokerCancelled,
    BrokerDeleted,
}

/// Authoritative removal of an execution, never a synthetic zero-quantity fill.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionFillInvalidation {
    pub provider: String,
    pub fill_id: String,
    pub order_id: String,
    pub position_id: Option<String>,
    pub symbol: String,
    pub side: ExecutionFillSide,
    pub effect: ExecutionFillEffect,
    pub occurred_at_us: i64,
    pub as_of_us: i64,
    pub reason: ExecutionFillInvalidationReason,
}

impl ExecutionFillInvalidation {
    pub fn validate(&self) -> Result<(), &'static str> {
        if [&self.provider, &self.fill_id, &self.order_id]
            .into_iter()
            .any(|v| !identity(v, 128))
            || !identity(&self.symbol, 256)
            || self
                .position_id
                .as_deref()
                .is_some_and(|v| !identity(v, 128))
            || self.occurred_at_us <= 0
            || self.as_of_us < self.occurred_at_us
        {
            return Err("invalid execution fill invalidation");
        }
        Ok(())
    }
    pub fn matches(&self, fill: &ExecutionFill) -> bool {
        self.provider == fill.provider
            && self.fill_id == fill.fill_id
            && self.order_id == fill.order_id
            && self.position_id == fill.position_id
            && self.symbol == fill.symbol
            && self.side == fill.side
            && self.effect == fill.effect
    }
}

impl ExecutionFillRecord {
    pub fn as_of_us(&self) -> i64 {
        self.invalidation
            .as_ref()
            .map_or(self.observation.fill.as_of_us, |v| v.as_of_us)
    }
    pub fn validate(&self) -> Result<(), &'static str> {
        self.observation.validate()?;
        if let Some(v) = &self.invalidation {
            v.validate()?;
            if !v.matches(&self.observation.fill) || v.as_of_us <= self.observation.fill.as_of_us {
                return Err("invalidation does not supersede its original fill");
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionFillPage {
    pub version: u16,
    pub source_id: String,
    pub strategy_id: String,
    pub account_id: String,
    pub after_sequence: u64,
    pub through_sequence: u64,
    pub next_sequence: u64,
    pub has_more: bool,
    pub records: Vec<ExecutionFillRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub position_checks: Vec<crate::PositionQuantityRecord>,
    pub generated_at_us: i64,
    pub coverage: String,
    pub valuation_basis: String,
    /// This ledger alone cannot prove broker history backfill/reconciliation.
    pub recovery_complete: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fill() -> ExecutionFill {
        ExecutionFill {
            version: 1,
            provider: "generic-deals-v1".into(),
            fill_id: "deal".into(),
            order_id: "order".into(),
            position_id: None,
            symbol: "FUTURE".into(),
            side: ExecutionFillSide::Buy,
            effect: ExecutionFillEffect::Entry,
            quantity: 2.,
            price: -10.,
            occurred_at_us: 100,
            as_of_us: 200,
            cash: Some(ExecutionFillCash {
                currency: "USD".into(),
                gross_pnl: Some(0.),
                commission: Some(-2.),
                carry: Some(1.),
            }),
            account_notional: None,
        }
    }
    #[test]
    fn execution_fill_preserves_rebates_unknown_costs_and_negative_prices() {
        let mut f = fill();
        assert!(f.validate().is_ok());
        assert_eq!(f.cash.as_ref().unwrap().net(), Some(3.));
        f.cash.as_mut().unwrap().commission = None;
        assert!(f.validate().is_ok());
        assert_eq!(f.cash.as_ref().unwrap().net(), None);
        f.cash = None;
        f.account_notional = Some(1.);
        assert!(f.validate().is_err());
    }
    #[test]
    fn execution_fill_rejects_unsafe_values_and_ignores_only_refresh_time() {
        let f = fill();
        let mut next = f.clone();
        next.as_of_us += 1;
        assert!(f.same_facts(&next));
        next.quantity += 1.;
        assert!(!f.same_facts(&next));
        next.quantity = f64::NAN;
        assert!(next.validate().is_err());
        next = f.clone();
        next.as_of_us = 99;
        assert!(next.validate().is_err());
        next = f.clone();
        next.fill_id = " ".into();
        assert!(next.validate().is_err());
        next = f;
        next.cash.as_mut().unwrap().carry = Some(f64::INFINITY);
        assert!(next.validate().is_err());
    }
}
