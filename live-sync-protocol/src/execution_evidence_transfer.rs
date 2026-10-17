//! Bounded, content-addressed transfer of sanitized execution evidence. A page
//! is immutable while it is awaiting durable admission; chunks may arrive in
//! any order, but no partial page is ever presented as an analytics snapshot.
use crate::{EXECUTION_EVIDENCE_VERSION, ExecutionEvidenceFeedPage};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub const EXECUTION_EVIDENCE_CHUNK_BYTES: usize = 32 * 1024;
pub const EXECUTION_EVIDENCE_TRANSFER_BYTES: usize = 1152 * 1024;
pub const EXECUTION_EVIDENCE_MAX_CHUNKS: usize = 37;

/// Proof of a complete page's durable handoff, not of cloud persistence or
/// broker reconciliation. The exporter advances only after every chunk commits.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionEvidenceExportReceipt {
    pub source_id: String,
    pub transfer_id: String,
    pub after_sequence: u64,
    pub next_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionEvidenceChunk {
    pub schema_version: u16,
    pub source_id: String,
    pub strategy_id: String,
    pub account_id: String,
    pub after_sequence: u64,
    pub next_sequence: u64,
    pub transfer_id: String,
    pub index: usize,
    pub count: usize,
    pub body: String,
}

fn encoded_page(page: &ExecutionEvidenceFeedPage) -> Result<String, String> {
    if page.schema_version != EXECUTION_EVIDENCE_VERSION
        || page.observations.is_empty()
        || page.observations.len() > 128
        || page.source_id.is_empty()
        || page.source_id.len() > 128
        || page.strategy_id.is_empty()
        || page.strategy_id.len() > 256
        || page.account_id.is_empty()
        || page.account_id.len() > 256
        || page.after_sequence >= page.next_sequence
        || page.next_sequence > page.through_sequence
        || page.through_sequence > i64::MAX as u64
        || (page.has_more && page.next_sequence == page.through_sequence)
        || (!page.has_more && page.next_sequence != page.through_sequence)
    {
        return Err("invalid execution evidence transfer scope or boundary".into());
    }
    let mut previous = page.after_sequence;
    for observation in &page.observations {
        if observation.execution_epoch == 0
            || observation.execution_epoch > i64::MAX as u64
            || observation.record.sequence <= previous
            || observation.record.sequence > page.next_sequence
            || observation.record.checkpoint_revision == 0
            || observation.record.state.insight_id.is_empty()
        {
            return Err("invalid execution evidence observation ordering".into());
        }
        previous = observation.record.sequence;
        if let Some(accounting) = &observation.record.state.position_accounting {
            accounting.validate().map_err(str::to_owned)?;
            if observation.record.state.state != "Closed"
                || !observation.record.state.entry_order_id.as_deref()
                    .is_some_and(|id| accounting.owns_order(id))
            {
                return Err("position accounting must belong to a closed insight".into());
            }
        }
    }
    if previous != page.next_sequence {
        return Err("incomplete execution evidence transfer".into());
    }
    let json =
        serde_json::to_string(page).map_err(|_| "execution evidence serialization failed")?;
    if json.len() > EXECUTION_EVIDENCE_TRANSFER_BYTES {
        return Err("execution evidence transfer exceeds byte limit".into());
    }
    Ok(json)
}

pub(crate) fn checksum(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    Sha256::digest(bytes)
        .iter()
        .flat_map(|byte| {
            [
                HEX[(byte >> 4) as usize] as char,
                HEX[(byte & 15) as usize] as char,
            ]
        })
        .collect()
}

pub fn execution_evidence_transfer_id(page: &ExecutionEvidenceFeedPage) -> Result<String, String> {
    Ok(checksum(encoded_page(page)?.as_bytes()))
}

pub fn execution_evidence_chunks(
    page: &ExecutionEvidenceFeedPage,
) -> Result<Vec<ExecutionEvidenceChunk>, String> {
    let json = encoded_page(page)?;
    let transfer_id = checksum(json.as_bytes());
    let mut bodies = Vec::new();
    let mut start = 0;
    while start < json.len() {
        let mut end = (start + EXECUTION_EVIDENCE_CHUNK_BYTES).min(json.len());
        while !json.is_char_boundary(end) {
            end -= 1;
        }
        bodies.push(json[start..end].to_owned());
        start = end;
    }
    let count = bodies.len();
    if count > EXECUTION_EVIDENCE_MAX_CHUNKS {
        return Err("too many evidence chunks".into());
    }
    Ok(bodies
        .into_iter()
        .enumerate()
        .map(|(index, body)| ExecutionEvidenceChunk {
            schema_version: page.schema_version,
            source_id: page.source_id.clone(),
            strategy_id: page.strategy_id.clone(),
            account_id: page.account_id.clone(),
            after_sequence: page.after_sequence,
            next_sequence: page.next_sequence,
            transfer_id: transfer_id.clone(),
            index,
            count,
            body,
        })
        .collect())
}

impl ExecutionEvidenceChunk {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != EXECUTION_EVIDENCE_VERSION
            || self.source_id.is_empty()
            || self.source_id.len() > 128
            || self.strategy_id.is_empty()
            || self.strategy_id.len() > 256
            || self.account_id.is_empty()
            || self.account_id.len() > 256
            || self.after_sequence >= self.next_sequence
            || self.next_sequence > i64::MAX as u64
            || self.count == 0
            || self.count > EXECUTION_EVIDENCE_MAX_CHUNKS
            || self.index >= self.count
            || self.body.is_empty()
            || self.body.len() > EXECUTION_EVIDENCE_CHUNK_BYTES
            || self.transfer_id.len() != 64
            || !self
                .transfer_id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err("invalid execution evidence chunk".into());
        }
        Ok(())
    }
}

/// Exact duplicate chunks are harmless. Missing, conflicting or mixed-scope
/// chunks fail closed and must be backfilled before advancing the source cursor.
pub fn assemble_execution_evidence(
    chunks: &[ExecutionEvidenceChunk],
) -> Result<ExecutionEvidenceFeedPage, String> {
    let first = chunks
        .first()
        .ok_or("missing execution evidence transfer")?;
    if chunks.len() > EXECUTION_EVIDENCE_MAX_CHUNKS * 2 {
        return Err("too many evidence chunks".into());
    }
    let mut parts = BTreeMap::new();
    let mut bytes = 0;
    for chunk in chunks {
        chunk.validate()?;
        if (
            &chunk.source_id,
            &chunk.strategy_id,
            &chunk.account_id,
            chunk.after_sequence,
            chunk.next_sequence,
            &chunk.transfer_id,
            chunk.count,
        ) != (
            &first.source_id,
            &first.strategy_id,
            &first.account_id,
            first.after_sequence,
            first.next_sequence,
            &first.transfer_id,
            first.count,
        ) {
            return Err("mixed execution evidence transfer".into());
        }
        match parts.insert(chunk.index, chunk.body.as_str()) {
            Some(previous) if previous != chunk.body => {
                return Err("conflicting execution evidence chunk".into());
            }
            Some(_) => {}
            None => bytes += chunk.body.len(),
        }
        if bytes > EXECUTION_EVIDENCE_TRANSFER_BYTES {
            return Err("execution evidence transfer exceeds byte limit".into());
        }
    }
    if parts.len() != first.count {
        return Err("incomplete execution evidence transfer".into());
    }
    let json = parts.into_values().collect::<String>();
    if checksum(json.as_bytes()) != first.transfer_id {
        return Err("execution evidence checksum mismatch".into());
    }
    let page: ExecutionEvidenceFeedPage =
        serde_json::from_str(&json).map_err(|_| "invalid execution evidence JSON")?;
    encoded_page(&page)?;
    if (
        &page.source_id,
        &page.strategy_id,
        &page.account_id,
        page.after_sequence,
        page.next_sequence,
    ) != (
        &first.source_id,
        &first.strategy_id,
        &first.account_id,
        first.after_sequence,
        first.next_sequence,
    ) {
        return Err("execution evidence envelope mismatch".into());
    }
    Ok(page)
}
