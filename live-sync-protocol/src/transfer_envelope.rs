//! A borrowed view of an already decoded JSON transfer. Validating a retained
//! event must not allocate another copy of its encoded page body.
use crate::{ExecutionFillTransfer, PositionMarkTransfer};
use serde::Deserialize;

#[derive(Deserialize)]
pub(crate) struct Envelope<'a> {
    pub schema_version: u16,
    #[serde(borrow)]
    pub source_id: &'a str,
    #[serde(borrow)]
    pub strategy_id: &'a str,
    #[serde(borrow)]
    pub account_id: &'a str,
    pub after_sequence: u64,
    pub next_sequence: u64,
    #[serde(borrow)]
    pub transfer_id: &'a str,
    #[serde(borrow)]
    pub body: &'a str,
}

impl<'a> From<&'a ExecutionFillTransfer> for Envelope<'a> {
    fn from(value: &'a ExecutionFillTransfer) -> Self {
        Self {
            schema_version: value.schema_version,
            source_id: &value.source_id,
            strategy_id: &value.strategy_id,
            account_id: &value.account_id,
            after_sequence: value.after_sequence,
            next_sequence: value.next_sequence,
            transfer_id: &value.transfer_id,
            body: &value.body,
        }
    }
}

impl<'a> From<&'a PositionMarkTransfer> for Envelope<'a> {
    fn from(value: &'a PositionMarkTransfer) -> Self {
        Self {
            schema_version: value.schema_version,
            source_id: &value.source_id,
            strategy_id: &value.strategy_id,
            account_id: &value.account_id,
            after_sequence: value.after_sequence,
            next_sequence: value.next_sequence,
            transfer_id: &value.transfer_id,
            body: &value.body,
        }
    }
}

#[cfg(test)]
mod tests;
