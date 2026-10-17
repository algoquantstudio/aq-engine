//! Small, whole-page transfers: no partial fill page can advance a consumer.
use crate::{
    EXECUTION_FILL_PAGE_VERSION, EXECUTION_FILL_VERSION, EXECUTION_POSITION_CHECK_PAGE_VERSION,
    ExecutionFillPage,
};
use serde::{Deserialize, Serialize};

pub const EXECUTION_FILL_TRANSFER_BYTES: usize = 32 * 1024;
pub const EXECUTION_FILL_TRANSFER_ROWS: usize = 32;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionFillTransfer {
    pub schema_version: u16,
    pub source_id: String,
    pub strategy_id: String,
    pub account_id: String,
    pub after_sequence: u64,
    pub next_sequence: u64,
    pub transfer_id: String,
    pub body: String,
}

/// Complete durable local handoff; not a cloud or broker-recovery acknowledgement.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionFillExportReceipt {
    pub source_id: String,
    pub transfer_id: String,
    pub after_sequence: u64,
    pub next_sequence: u64,
}

fn identity(value: &str, limit: usize) -> bool {
    !value.trim().is_empty() && value.len() <= limit && !value.chars().any(char::is_control)
}

pub fn validate_execution_fill_page(page: &ExecutionFillPage) -> Result<(), String> {
    if ![
        EXECUTION_FILL_VERSION,
        EXECUTION_FILL_PAGE_VERSION,
        EXECUTION_POSITION_CHECK_PAGE_VERSION,
    ]
    .contains(&page.version)
        || !identity(&page.source_id, 128)
        || !identity(&page.strategy_id, 256)
        || !identity(&page.account_id, 256)
        || !identity(&page.coverage, 256)
        || !identity(&page.valuation_basis, 256)
        || page.generated_at_us <= 0
        || page.records.is_empty() && page.position_checks.is_empty()
        || page.records.len() + page.position_checks.len() > EXECUTION_FILL_TRANSFER_ROWS
        || !page.position_checks.is_empty() && page.version < EXECUTION_POSITION_CHECK_PAGE_VERSION
        || page.after_sequence >= page.next_sequence
        || page.next_sequence > page.through_sequence
        || page.through_sequence > i64::MAX as u64
        || page.has_more != (page.next_sequence < page.through_sequence)
    {
        return Err("invalid execution fill page scope/boundary".into());
    }
    let mut sequences = Vec::with_capacity(page.records.len() + page.position_checks.len());
    let mut previous = page.after_sequence;
    for record in &page.records {
        // The journal is append-only with a source-local sequence. Missing a
        // record inside a page must not let consumers acknowledge past it.
        if record.sequence <= previous
            || record.sequence > page.next_sequence
            || record.execution_epoch == 0
            || record.execution_epoch > i64::MAX as u64
            || record.recorded_at_us <= 0
        {
            return Err("invalid execution fill record ordering".into());
        }
        record.validate().map_err(str::to_owned)?;
        if record.invalidation.is_some() && page.version < EXECUTION_FILL_PAGE_VERSION {
            return Err("execution fill invalidation requires page version 2".into());
        }
        previous = record.sequence;
        sequences.push(record.sequence);
    }
    previous = page.after_sequence;
    for record in &page.position_checks {
        record.validate().map_err(str::to_owned)?;
        if record.sequence <= previous || record.sequence > page.next_sequence {
            return Err("invalid position quantity record ordering".into());
        }
        previous = record.sequence;
        sequences.push(record.sequence);
    }
    sequences.sort_unstable();
    if sequences
        .iter()
        .enumerate()
        .any(|(i, sequence)| *sequence != page.after_sequence + i as u64 + 1)
        || sequences.last() != Some(&page.next_sequence)
    {
        return Err("incomplete execution fill page".into());
    }
    Ok(())
}

impl ExecutionFillTransfer {
    pub fn from_page(page: &ExecutionFillPage) -> Result<Self, String> {
        validate_execution_fill_page(page)?;
        let body = serde_json::to_string(page).map_err(|e| e.to_string())?;
        if body.len() > EXECUTION_FILL_TRANSFER_BYTES {
            return Err("execution fill transfer exceeds byte limit".into());
        }
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

    pub fn page(&self) -> Result<ExecutionFillPage, String> {
        if self.body.is_empty()
            || self.body.len() > EXECUTION_FILL_TRANSFER_BYTES
            || ![
                EXECUTION_FILL_VERSION,
                EXECUTION_FILL_PAGE_VERSION,
                EXECUTION_POSITION_CHECK_PAGE_VERSION,
            ]
            .contains(&self.schema_version)
            || self.transfer_id
                != crate::execution_evidence_transfer::checksum(self.body.as_bytes())
        {
            return Err("invalid execution fill transfer size/checksum/version".into());
        }
        let page: ExecutionFillPage =
            serde_json::from_str(&self.body).map_err(|e| e.to_string())?;
        validate_execution_fill_page(&page)?;
        if self.schema_version != page.version {
            return Err("execution fill envelope version mismatch".into());
        }
        if (
            &self.source_id,
            &self.strategy_id,
            &self.account_id,
            self.after_sequence,
            self.next_sequence,
        ) != (
            &page.source_id,
            &page.strategy_id,
            &page.account_id,
            page.after_sequence,
            page.next_sequence,
        ) {
            return Err("execution fill envelope scope mismatch".into());
        }
        Ok(page)
    }

    pub fn receipt(&self) -> ExecutionFillExportReceipt {
        ExecutionFillExportReceipt {
            source_id: self.source_id.clone(),
            transfer_id: self.transfer_id.clone(),
            after_sequence: self.after_sequence,
            next_sequence: self.next_sequence,
        }
    }
}
