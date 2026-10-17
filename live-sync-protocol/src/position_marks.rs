//! Scheduled broker valuations, not another Insight state/history projection.
use crate::{PositionValuationState, PositionValuations};
use serde::{Deserialize, Serialize};
pub const POSITION_MARK_VERSION: u16 = 1;
pub const POSITION_MARK_MINUTE_US: i64 = 60_000_000;
pub const MAX_POSITION_MARK_ROWS: usize = 32;
pub const POSITION_MARK_TRANSFER_BYTES: usize = 32 * 1024;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PositionMarkObservation {
    pub insight_id: String,
    /// The fill-provider namespace of the owned ledger, not the valuation provider.
    pub fill_provider: String,
    pub ledger_sequence: u64,
    /// UTC sampling slot at admission. Source as-of/quote times stay in `values`.
    pub sample_minute_us: i64,
    /// Exactly one physical position, in a declared account currency/basis.
    pub values: PositionValuations,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PositionMarkRecord {
    pub sequence: u64,
    pub execution_epoch: u64,
    pub recorded_at_us: i64,
    pub observation: PositionMarkObservation,
}
impl PositionMarkRecord {
    pub fn validate(&self, account: &str) -> Result<(), String> {
        let o = &self.observation;
        let id = |v: &str, max| {
            !v.trim().is_empty() && v.len() <= max && !v.chars().any(char::is_control)
        };
        if self.sequence == 0
            || self.sequence > i64::MAX as u64
            || self.execution_epoch == 0
            || self.execution_epoch > i64::MAX as u64
            || self.recorded_at_us <= 0
            || !id(&o.insight_id, 128)
            || !id(&o.fill_provider, 128)
            || o.ledger_sequence == 0
            || o.ledger_sequence > i64::MAX as u64
            || o.sample_minute_us <= 0
            || o.sample_minute_us % POSITION_MARK_MINUTE_US != 0
            || o.sample_minute_us > self.recorded_at_us
            || o.values.positions.len() != 1
            || o.values.as_of_us > self.recorded_at_us.saturating_add(5_000_000)
            || o.values.as_of_us >= o.sample_minute_us.saturating_add(POSITION_MARK_MINUTE_US)
        {
            return Err("Invalid position mark identity/sampling slot".into());
        }
        o.values
            .validate(
                account,
                &o.values.symbol,
                &[o.values.positions[0].position_id.clone()],
            )
            .map_err(str::to_owned)?;
        Ok(())
    }
    pub fn is_open(&self) -> bool {
        self.observation
            .values
            .positions
            .first()
            .is_some_and(|p| matches!(&p.state, PositionValuationState::Open { .. }))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PositionMarkPage {
    pub version: u16,
    pub source_id: String,
    pub strategy_id: String,
    pub account_id: String,
    pub after_sequence: u64,
    pub through_sequence: u64,
    pub next_sequence: u64,
    pub has_more: bool,
    pub records: Vec<PositionMarkRecord>,
    pub generated_at_us: i64,
}
impl PositionMarkPage {
    pub fn validate(&self) -> Result<(), String> {
        let id = |v: &str, max| {
            !v.trim().is_empty() && v.len() <= max && !v.chars().any(char::is_control)
        };
        if self.version != POSITION_MARK_VERSION
            || !id(&self.source_id, 128)
            || !id(&self.strategy_id, 256)
            || !id(&self.account_id, 128)
            || self.after_sequence > self.next_sequence
            || self.next_sequence > self.through_sequence
            || self.through_sequence > i64::MAX as u64
            || self.records.len() > MAX_POSITION_MARK_ROWS
            || self.generated_at_us <= 0
            || self.has_more != (self.next_sequence < self.through_sequence)
            || self.next_sequence - self.after_sequence != self.records.len() as u64
            || self.records.is_empty() && self.next_sequence < self.through_sequence
        {
            return Err("Invalid position mark page boundary".into());
        }
        for (i, r) in self.records.iter().enumerate() {
            r.validate(&self.account_id)?;
            if r.sequence != self.after_sequence + i as u64 + 1 {
                return Err("Incomplete position mark page".into());
            }
        }
        if serde_json::to_vec(self).map_err(|e| e.to_string())?.len() > POSITION_MARK_TRANSFER_BYTES
        {
            return Err("Position mark page byte limit".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn record() -> PositionMarkRecord {
        serde_json::from_value(serde_json::json!({"sequence":1,"execution_epoch":4,"recorded_at_us":120_000_001,
            "observation":{"insight_id":"owned-insight","fill_provider":"deal-provider","ledger_sequence":9,"sample_minute_us":120_000_000,
            "values":{"version":1,"provider":"value-provider","account_id":"account","account_currency":"GBP","symbol":"ANY","as_of_us":120_000_000,
            "positions":[{"position_id":"position","status":"open","quantity":2.0,"side":"sell","average_entry_price":100.0,"current_price":90.0,
                "gross_unrealized_pnl":20.0,"carry":-1.0,"quote_time":null,"notional":{"basis":"unavailable","reason":"currency_conversion_unavailable"}}]}}})).unwrap()
    }
    #[test]
    fn marks_preserve_unknown_fx_quote_age_and_distinct_broker_economics() {
        let r = record();
        assert!(r.validate("account").is_ok());
        assert!(r.is_open());
        assert!(r.validate("foreign").is_err());
        assert!(
            serde_json::to_string(&r)
                .unwrap()
                .contains("currency_conversion_unavailable")
        );
        for field in 0..5 {
            let mut bad = r.clone();
            match field {
                0 => bad.observation.sample_minute_us += 1,
                1 => bad.observation.values.positions.clear(),
                2 => bad.execution_epoch = 0,
                3 => bad.observation.values.as_of_us += 6_000_000,
                _ => bad.observation.insight_id = "\n".into(),
            };
            assert!(bad.validate("account").is_err());
        }
        let mut stale = r.clone();
        stale.observation.values.as_of_us = 60_000_000;
        assert!(
            stale.validate("account").is_ok(),
            "Old source marks remain explicitly old, not fresh on receipt"
        );
        let mut page = PositionMarkPage {
            version: 1,
            source_id: "source".into(),
            strategy_id: "generic".into(),
            account_id: "account".into(),
            after_sequence: 0,
            through_sequence: 1,
            next_sequence: 1,
            has_more: false,
            records: vec![r],
            generated_at_us: 120_000_002,
        };
        assert!(page.validate().is_ok());
        page.records[0].sequence = 2;
        assert!(page.validate().is_err());
    }
}
