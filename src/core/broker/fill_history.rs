//! Read-only execution-history recovery. Ownership stays in the journal, not
//! the broker fact; no strategy graph or alpha-instance identity is involved.
use super::types::BrokerError;
use aqe_live_protocol::{ExecutionFill, ExecutionFillEffect, ExecutionFillInvalidation};
pub use aqe_live_protocol::{PositionQuantityCheck, PositionQuantitySnapshot};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub const MAX_HISTORY_FILLS: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FillHistoryOwner {
    pub insight_id: String,
    pub execution_epoch: u64,
    pub entry_order_id: String,
    pub symbol: String,
}

pub(crate) struct FillHistoryOwnerPage {
    pub owners: Vec<FillHistoryOwner>,
    pub next_cursor: Option<String>,
}

/// A complete bounded snapshot for one provably single-entry-order position.
/// Unsupported/netted/missing histories are unavailable, never an empty proof.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionFillHistory {
    pub account_id: String,
    pub entry_order_id: String,
    pub position_id: String,
    pub symbol: String,
    pub as_of_us: i64,
    pub clock: ExecutionHistoryClock,
    pub fills: Vec<ExecutionFill>,
    #[serde(default)]
    pub invalidations: Vec<ExecutionFillInvalidation>,
    /// Optional broker observation, separate from the reconstructed ledger.
    /// Old adapters remain readable but cannot claim position reconciliation.
    #[serde(default)]
    pub position_snapshot: Option<PositionQuantitySnapshot>,
}

/// Historical clock evidence is independent of the broker's current offset.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "basis", rename_all = "snake_case")]
pub enum ExecutionHistoryClock {
    Utc,
    DeclaredFixedOffset {
        seconds: i32,
        valid_from_us: i64,
        valid_through_us: i64,
    },
}

impl ExecutionFillHistory {
    pub fn validate(&self, account: &str, order: &str, symbol: &str) -> Result<(), BrokerError> {
        let invalid =
            || BrokerError::TradeError("invalid or ambiguous execution fill history".into());
        if self.account_id != account
            || self.entry_order_id != order
            || self.symbol != symbol
            || self.position_id.is_empty()
            || self.position_id.len() > 128
            || (self.fills.is_empty() && self.invalidations.is_empty())
            || self.fills.len().saturating_add(self.invalidations.len()) > MAX_HISTORY_FILLS
            || self.as_of_us <= 0
        {
            return Err(invalid());
        }
        let mut ids = HashSet::new();
        let mut entry_side = None;
        let mut first_entry_us = i64::MAX;
        let mut currency = None;
        let mut entered = 0.;
        let mut exited = 0.;
        let provider = self
            .fills
            .first()
            .map(|v| &v.provider)
            .or_else(|| self.invalidations.first().map(|v| &v.provider))
            .ok_or_else(invalid)?;
        if let ExecutionHistoryClock::DeclaredFixedOffset {
            seconds,
            valid_from_us,
            valid_through_us,
        } = self.clock
        {
            if seconds.unsigned_abs() > 14 * 3600
                || valid_from_us <= 0
                || valid_through_us < valid_from_us
            {
                return Err(invalid());
            }
        }
        for fill in &self.fills {
            fill.validate().map_err(|_| invalid())?;
            if fill.position_id.as_deref() != Some(self.position_id.as_str())
                || fill.symbol != symbol
                || fill.as_of_us != self.as_of_us
                || &fill.provider != provider
                || !ids.insert((&fill.provider, &fill.fill_id))
            {
                return Err(invalid());
            }
            if let ExecutionHistoryClock::DeclaredFixedOffset {
                valid_from_us,
                valid_through_us,
                ..
            } = self.clock
            {
                if fill.occurred_at_us < valid_from_us || fill.occurred_at_us > valid_through_us {
                    return Err(invalid());
                }
            }
            if let Some(cash) = &fill.cash {
                if currency.is_some_and(|c| c != &cash.currency) {
                    return Err(invalid());
                }
                currency = Some(&cash.currency);
            }
            match fill.effect {
                ExecutionFillEffect::Entry if fill.order_id == order => {
                    if entry_side.is_some_and(|side| side != fill.side) {
                        return Err(invalid());
                    }
                    entry_side = Some(fill.side);
                    first_entry_us = first_entry_us.min(fill.occurred_at_us);
                    entered += fill.quantity;
                }
                ExecutionFillEffect::Exit => exited += fill.quantity,
                _ => return Err(invalid()),
            }
        }
        let mut cancelled_entry = false;
        for v in &self.invalidations {
            v.validate().map_err(|_| invalid())?;
            if v.position_id.as_deref() != Some(self.position_id.as_str())
                || v.symbol != symbol
                || v.as_of_us != self.as_of_us
                || &v.provider != provider
                || !ids.insert((&v.provider, &v.fill_id))
            {
                return Err(invalid());
            }
            if let ExecutionHistoryClock::DeclaredFixedOffset {
                valid_from_us,
                valid_through_us,
                ..
            } = self.clock
            {
                if v.occurred_at_us < valid_from_us || v.occurred_at_us > valid_through_us {
                    return Err(invalid());
                }
            }
            match v.effect {
                ExecutionFillEffect::Entry if v.order_id == order => {
                    if entry_side.is_some_and(|side| side != v.side) {
                        return Err(invalid());
                    }
                    entry_side = Some(v.side);
                    first_entry_us = first_entry_us.min(v.occurred_at_us);
                    cancelled_entry = true;
                }
                ExecutionFillEffect::Exit => {}
                _ => return Err(invalid()),
            }
        }
        if entry_side.is_none()
            || !entered.is_finite()
            || !exited.is_finite()
            || (!cancelled_entry && exited > entered + entered.abs().max(1.) * 1e-9)
            || self.fills.iter().any(|fill| {
                fill.effect == ExecutionFillEffect::Exit
                    && (Some(fill.side) == entry_side || fill.occurred_at_us < first_entry_us)
            })
            || self.invalidations.iter().any(|v| {
                v.effect == ExecutionFillEffect::Exit
                    && (Some(v.side) == entry_side || v.occurred_at_us < first_entry_us)
            })
        {
            return Err(invalid());
        }
        if let Some(position) = &self.position_snapshot {
            if position.position_id != self.position_id
                || position.symbol != self.symbol
                || position.as_of_us != self.as_of_us
                || !position.quantity.is_finite()
                || position.quantity < 0.
                || (position.quantity == 0.) != position.side.is_none()
            {
                return Err(invalid());
            }
        }
        Ok(())
    }

    /// A mismatch is diagnostic evidence, not a reason to discard genuine
    /// broker fills or mutate orders. This is one owned position, never proof
    /// of account-wide cash/FX reconciliation or ownership of external trades.
    pub fn position_check(&self) -> Option<PositionQuantityCheck> {
        self.validate(&self.account_id, &self.entry_order_id, &self.symbol)
            .ok()?;
        let snapshot = self.position_snapshot.clone()?;
        let mut buckets = std::collections::BTreeMap::<i64, (f64, f64)>::new();
        let mut side = None;
        let mut valid = true;
        for fill in &self.fills {
            let bucket = buckets.entry(fill.occurred_at_us).or_default();
            match fill.effect {
                ExecutionFillEffect::Entry => {
                    valid &= side.is_none_or(|s| s == fill.side);
                    side = Some(fill.side);
                    bucket.0 += fill.quantity;
                }
                ExecutionFillEffect::Exit => bucket.1 += fill.quantity,
                _ => valid = false,
            }
        }
        let mut remaining = 0.;
        for (entries, exits) in buckets.into_values() {
            let before_exit = remaining + entries;
            remaining = before_exit - exits;
            valid &= before_exit.is_finite()
                && remaining.is_finite()
                && remaining >= -before_exit.abs().max(exits.abs()) * 1e-9;
            if remaining.abs() <= before_exit.abs().max(exits.abs()) * 1e-9 {
                remaining = 0.;
            }
        }
        let ledger_quantity = valid.then_some(remaining);
        let ledger_side = (valid && remaining > 0.).then_some(side).flatten();
        let tolerance = remaining.abs().max(snapshot.quantity.abs()) * 1e-9;
        let matched = valid
            && ledger_side == snapshot.side
            && (remaining - snapshot.quantity).abs() <= tolerance;
        Some(PositionQuantityCheck {
            definition_version: aqe_live_protocol::POSITION_QUANTITY_DEFINITION.into(),
            snapshot,
            ledger_quantity,
            ledger_side,
            matched,
        })
    }
}

#[async_trait::async_trait]
pub trait ExecutionFillHistoryReader: Send + Sync {
    /// No order mutation, lifecycle reconciliation, or account-wide attribution.
    async fn read(
        &self,
        account_id: &str,
        entry_order_id: &str,
        symbol: &str,
    ) -> Result<Option<ExecutionFillHistory>, BrokerError>;
}
