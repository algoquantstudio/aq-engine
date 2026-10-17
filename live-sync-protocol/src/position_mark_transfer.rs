//! Whole, immutable mark pages with a distinct cursor from fills and state.
use crate::{POSITION_MARK_TRANSFER_BYTES, POSITION_MARK_VERSION, PositionMarkPage};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PositionMarkTransfer {
    pub schema_version: u16,
    pub source_id: String,
    pub strategy_id: String,
    pub account_id: String,
    pub after_sequence: u64,
    pub next_sequence: u64,
    pub transfer_id: String,
    pub body: String,
}
/// Durable local outbox admission only, not cloud persistence/reconciliation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PositionMarkExportReceipt {
    pub source_id: String,
    pub transfer_id: String,
    pub after_sequence: u64,
    pub next_sequence: u64,
}
impl PositionMarkTransfer {
    pub fn from_page(page: &PositionMarkPage) -> Result<Self, String> {
        page.validate()?;
        if page.records.is_empty() {
            return Err("Empty position mark transfer".into());
        }
        let body = serde_json::to_string(page).map_err(|e| e.to_string())?;
        Ok(Self {
            schema_version: page.version,
            source_id: page.source_id.clone(),
            strategy_id: page.strategy_id.clone(),
            account_id: page.account_id.clone(),
            after_sequence: page.after_sequence,
            next_sequence: page.next_sequence,
            transfer_id: crate::execution_evidence_transfer::checksum(body.as_bytes()),
            body,
        })
    }
    pub fn page(&self) -> Result<PositionMarkPage, String> {
        if self.schema_version != POSITION_MARK_VERSION
            || self.body.is_empty()
            || self.body.len() > POSITION_MARK_TRANSFER_BYTES
            || self.transfer_id
                != crate::execution_evidence_transfer::checksum(self.body.as_bytes())
        {
            return Err("Invalid position mark transfer size/checksum/version".into());
        }
        let page: PositionMarkPage = serde_json::from_str(&self.body).map_err(|e| e.to_string())?;
        page.validate()?;
        if page.records.is_empty()
            || (
                self.schema_version,
                &self.source_id,
                &self.strategy_id,
                &self.account_id,
                self.after_sequence,
                self.next_sequence,
            ) != (
                page.version,
                &page.source_id,
                &page.strategy_id,
                &page.account_id,
                page.after_sequence,
                page.next_sequence,
            )
        {
            return Err("Position mark transfer scope/boundary mismatch".into());
        }
        Ok(page)
    }
    pub fn receipt(&self) -> PositionMarkExportReceipt {
        PositionMarkExportReceipt {
            source_id: self.source_id.clone(),
            transfer_id: self.transfer_id.clone(),
            after_sequence: self.after_sequence,
            next_sequence: self.next_sequence,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mark_transfer_is_checksummed_scoped_nonempty_and_complete() {
        let page:PositionMarkPage = serde_json::from_value(serde_json::json!({
            "version":1,"source_id":"source","strategy_id":"generic","account_id":"account","after_sequence":0,"through_sequence":1,"next_sequence":1,"has_more":false,"generated_at_us":120000002,
            "records":[{"sequence":1,"execution_epoch":4,"recorded_at_us":120000001,"observation":{"insight_id":"insight","fill_provider":"fills","ledger_sequence":8,"sample_minute_us":120000000,
                "values":{"version":1,"provider":"marks","account_id":"account","account_currency":"GBP","symbol":"ANY","as_of_us":120000000,"positions":[{"position_id":"position","status":"not_open"}]}}}]
        })).unwrap();
        let t = PositionMarkTransfer::from_page(&page).unwrap();
        assert_eq!(t.page().unwrap(), page);
        for field in 0..5 {
            let mut bad = t.clone();
            match field {
                0 => bad.body.push(' '),
                1 => bad.source_id = "foreign".into(),
                2 => bad.account_id = "foreign".into(),
                3 => bad.schema_version = 2,
                _ => bad.next_sequence = 2,
            };
            assert!(bad.page().is_err());
        }
        let mut empty = page;
        empty.after_sequence = 1;
        empty.records.clear();
        assert!(empty.validate().is_ok());
        assert!(PositionMarkTransfer::from_page(&empty).is_err());
    }
}
