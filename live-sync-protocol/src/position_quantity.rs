//! Read-only broker quantity evidence. This is not account cash reconciliation,
//! an execution fill, a position mark, or an instruction to change an order.
use crate::ExecutionFillSide;
use serde::{Deserialize, Serialize};

pub const POSITION_QUANTITY_DEFINITION: &str = "aqe-owned-position-quantity-1";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PositionQuantitySnapshot {
    pub position_id: String,
    pub symbol: String,
    pub quantity: f64,
    pub side: Option<ExecutionFillSide>,
    pub as_of_us: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PositionQuantityCheck {
    pub definition_version: String,
    pub snapshot: PositionQuantitySnapshot,
    pub ledger_quantity: Option<f64>,
    pub ledger_side: Option<ExecutionFillSide>,
    pub matched: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PositionQuantityObservation {
    pub provider: String,
    pub position_id: String,
    pub symbol: String,
    pub insight_id: String,
    /// Latest position fill revision included in this comparison, not the
    /// transport cursor or evidence that the whole account was recovered.
    pub ledger_sequence: u64,
    pub as_of_us: i64,
    /// None explicitly revokes a previous comparison. It never means flat.
    pub check: Option<PositionQuantityCheck>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PositionQuantityRecord {
    pub sequence: u64,
    pub execution_epoch: u64,
    pub recorded_at_us: i64,
    pub observation: PositionQuantityObservation,
}

impl PositionQuantityRecord {
    pub fn validate(&self) -> Result<(), &'static str> {
        let o = &self.observation;
        let identity = |value: &str, max| {
            !value.trim().is_empty() && value.len() <= max && !value.chars().any(char::is_control)
        };
        if self.sequence == 0
            || self.sequence > i64::MAX as u64
            || self.execution_epoch == 0
            || self.execution_epoch > i64::MAX as u64
            || self.recorded_at_us <= 0
            || o.as_of_us <= 0
            || o.ledger_sequence == 0
            || o.ledger_sequence >= self.sequence
            || !identity(&o.provider, 128)
            || !identity(&o.position_id, 128)
            || !identity(&o.symbol, 256)
            || !identity(&o.insight_id, 128)
        {
            return Err("invalid position quantity record scope");
        }
        if let Some(c) = &o.check {
            let s = &c.snapshot;
            if c.definition_version != POSITION_QUANTITY_DEFINITION
                || s.position_id != o.position_id
                || s.symbol != o.symbol
                || s.as_of_us != o.as_of_us
                || !s.quantity.is_finite()
                || s.quantity < 0.
                || (s.quantity == 0.) != s.side.is_none()
                || c.ledger_quantity.is_some_and(|q| !q.is_finite() || q < 0.)
                || c.ledger_quantity.is_some_and(|q| q > 0.) != c.ledger_side.is_some()
            {
                return Err("invalid position quantity comparison");
            }
            let matched = c.ledger_quantity.is_some_and(|q| {
                c.ledger_side == s.side
                    && (q - s.quantity).abs() <= q.abs().max(s.quantity.abs()) * 1e-9
            });
            if matched != c.matched {
                return Err("inconsistent position quantity result");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExecutionFillPage, ExecutionFillTransfer};
    fn page() -> ExecutionFillPage {
        serde_json::from_value(serde_json::json!({
            "version":3,"source_id":"source","strategy_id":"generic","account_id":"account",
            "after_sequence":1,"through_sequence":2,"next_sequence":2,"has_more":false,
            "generated_at_us":300,"coverage":"owned_position","valuation_basis":"broker_cash","recovery_complete":null,
            "records":[],"position_checks":[{"sequence":2,"execution_epoch":1,"recorded_at_us":300,"observation":{
                "provider":"generic-provider","position_id":"position","symbol":"GENERIC","insight_id":"insight",
                "ledger_sequence":1,"as_of_us":200,"check":{"definition_version":POSITION_QUANTITY_DEFINITION,
                    "snapshot":{"position_id":"position","symbol":"GENERIC","quantity":2.,"side":"buy","as_of_us":200},
                    "ledger_quantity":2.,"ledger_side":"buy","matched":true}}}]
        })).unwrap()
    }
    #[test]
    fn comparison_only_pages_are_versioned_bounded_and_do_not_invent_fills() {
        let p = page();
        let transfer = ExecutionFillTransfer::from_page(&p).unwrap();
        assert_eq!(transfer.page().unwrap(), p);
        assert!(p.records.is_empty());
        for version in [1, 2, 4] {
            let mut wrong = p.clone();
            wrong.version = version;
            assert!(ExecutionFillTransfer::from_page(&wrong).is_err());
        }
        let mut invalidated = p.clone();
        invalidated.position_checks[0].observation.check = None;
        assert!(ExecutionFillTransfer::from_page(&invalidated).is_ok());
        let mut false_match = p.clone();
        false_match.position_checks[0]
            .observation
            .check
            .as_mut()
            .unwrap()
            .snapshot
            .quantity = 1.;
        assert!(ExecutionFillTransfer::from_page(&false_match).is_err());
        false_match.position_checks[0]
            .observation
            .check
            .as_mut()
            .unwrap()
            .matched = false;
        assert!(ExecutionFillTransfer::from_page(&false_match).is_ok());
        let mut wrong_identity = p.clone();
        wrong_identity.position_checks[0].observation.symbol = "OTHER".into();
        assert!(ExecutionFillTransfer::from_page(&wrong_identity).is_err());
        let mut missing = p.clone();
        missing.position_checks.clear();
        assert!(ExecutionFillTransfer::from_page(&missing).is_err());
        let mut duplicate = p.clone();
        duplicate.position_checks.push(p.position_checks[0].clone());
        assert!(ExecutionFillTransfer::from_page(&duplicate).is_err());
        let mut future = p.clone();
        future.position_checks[0].observation.ledger_sequence = 2;
        assert!(ExecutionFillTransfer::from_page(&future).is_err());
    }
}
