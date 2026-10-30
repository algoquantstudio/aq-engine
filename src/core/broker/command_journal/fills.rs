//! Append-only broker fill revisions, separate from recoverable trading state.
use super::*;
use aqe_live_protocol::{
    EXECUTION_FILL_PAGE_VERSION, EXECUTION_FILL_VERSION, EXECUTION_POSITION_CHECK_PAGE_VERSION,
    ExecutionFillInput, ExecutionFillInvalidation, ExecutionFillPage, ExecutionFillRecord,
    PositionQuantityObservation, PositionQuantityRecord,
};
mod export;
mod history;
mod invalidations;
use crate::core::broker::fill_history::{ExecutionFillHistory, FillHistoryOwner};

pub const MAX_FILL_BATCH_ROWS: usize = 256;
const MAX_FILL_BYTES: usize = 512 * 1024;
const MAX_FILL_PAGE_ROWS: usize = 128;

pub(super) const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS broker_execution_fill_history_clocks (
    clock_json TEXT PRIMARY KEY, first_sequence INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS broker_execution_fill_export (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    cursor INTEGER NOT NULL CHECK(cursor>=0), last_transfer_id TEXT,
    pending_transfer_id TEXT, pending_next INTEGER, pending_json TEXT
);
INSERT INTO broker_execution_fill_export(singleton,cursor) VALUES(1,0)
    ON CONFLICT(singleton) DO NOTHING;
CREATE TABLE IF NOT EXISTS broker_execution_fills (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_epoch INTEGER NOT NULL, recorded_at_us INTEGER NOT NULL,
    observation_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS broker_execution_fill_heads (
    provider TEXT NOT NULL, fill_id TEXT NOT NULL, sequence INTEGER NOT NULL,
    as_of_us INTEGER NOT NULL, PRIMARY KEY(provider,fill_id)
);
CREATE TABLE IF NOT EXISTS broker_execution_fill_invalidated (
    provider TEXT NOT NULL, fill_id TEXT NOT NULL, body TEXT NOT NULL,
    PRIMARY KEY(provider,fill_id)
);
CREATE INDEX IF NOT EXISTS idx_fill_position_identity ON broker_execution_fills(
    COALESCE(json_extract(observation_json,'$.fill.position_id'),json_extract(observation_json,'$.invalidated_observation.fill.position_id'))
);
CREATE INDEX IF NOT EXISTS idx_fill_head_sequence ON broker_execution_fill_heads(sequence);
CREATE TABLE IF NOT EXISTS broker_execution_position_checks (
    provider TEXT NOT NULL,position_id TEXT NOT NULL,insight_id TEXT NOT NULL,
    execution_epoch INTEGER NOT NULL,ledger_sequence INTEGER NOT NULL,
    as_of_us INTEGER NOT NULL,check_json TEXT NOT NULL,
    PRIMARY KEY(provider,position_id)
);
CREATE INDEX IF NOT EXISTS idx_execution_position_check_ledger ON broker_execution_position_checks(ledger_sequence);";

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
enum SavedFill {
    PositionCheck {
        position_check: PositionQuantityObservation,
    },
    // Deliberately not the legacy input shape: an older runtime must fail to
    // decode this record rather than re-export the audit fact as an active fill.
    Invalidated {
        invalidated_observation: ExecutionFillInput,
        invalidation: ExecutionFillInvalidation,
    },
    Observation(ExecutionFillInput),
}
impl SavedFill {
    fn into_parts(
        self,
    ) -> Result<(ExecutionFillInput, Option<ExecutionFillInvalidation>), BrokerCommandError> {
        match self {
            Self::PositionCheck { .. } => {
                Err(journal_error("position check cannot be a fill head"))
            }
            Self::Invalidated {
                invalidated_observation,
                invalidation,
            } => Ok((invalidated_observation, Some(invalidation))),
            Self::Observation(observation) => Ok((observation, None)),
        }
    }
}

/// Prevalidated, bounded admission. Not a checkpoint and never restores orders.
pub struct ExecutionFillBatch {
    rows: Vec<(ExecutionFillInput, String)>,
    history_owner: Option<(String, FillHistoryOwner)>,
    history_clock: Option<String>,
    invalidations: Vec<ExecutionFillInvalidation>,
    position_check: Option<crate::core::broker::fill_history::PositionQuantityCheck>,
}
impl ExecutionFillBatch {
    pub(crate) fn from_history(
        owner: FillHistoryOwner,
        history: ExecutionFillHistory,
    ) -> Result<Self, BrokerCommandError> {
        history
            .validate(&history.account_id, &owner.entry_order_id, &owner.symbol)
            .map_err(journal_error)?;
        let clock = serde_json::to_string(&history.clock).map_err(journal_error)?;
        let position_check = history.position_check();
        let mut batch = Self::new(
            history
                .fills
                .into_iter()
                .map(|fill| ExecutionFillInput {
                    insight_id: Some(owner.insight_id.clone()),
                    fill,
                })
                .collect(),
        )?;
        batch.invalidations = history.invalidations;
        let invalidation_bytes = serde_json::to_vec(&batch.invalidations)
            .map_err(journal_error)?
            .len();
        if batch.rows.iter().map(|(_, json)| json.len()).sum::<usize>() + invalidation_bytes
            > MAX_FILL_BYTES
        {
            return Err(journal_error("execution history batch byte limit"));
        }
        batch.history_owner = Some((history.account_id, owner));
        batch.history_clock = Some(clock);
        batch.position_check = position_check;
        Ok(batch)
    }

    pub fn new(rows: Vec<ExecutionFillInput>) -> Result<Self, BrokerCommandError> {
        if rows.len() > MAX_FILL_BATCH_ROWS {
            return Err(journal_error("execution fill batch row limit"));
        }
        let mut encoded = Vec::with_capacity(rows.len());
        let mut bytes = 0;
        for row in rows {
            row.validate().map_err(journal_error)?;
            let json = serde_json::to_string(&row).map_err(journal_error)?;
            bytes += json.len();
            if bytes > MAX_FILL_BYTES {
                return Err(journal_error("execution fill batch byte limit"));
            }
            encoded.push((row, json));
        }
        Ok(Self {
            rows: encoded,
            history_owner: None,
            history_clock: None,
            invalidations: vec![],
            position_check: None,
        })
    }

    pub fn invalidations(rows: Vec<ExecutionFillInvalidation>) -> Result<Self, BrokerCommandError> {
        if rows.len() > MAX_FILL_BATCH_ROWS {
            return Err(journal_error("execution invalidation row limit"));
        }
        let mut bytes = 0;
        for row in &rows {
            row.validate().map_err(journal_error)?;
            bytes += serde_json::to_vec(row).map_err(journal_error)?.len();
        }
        if bytes > MAX_FILL_BYTES {
            return Err(journal_error("execution invalidation byte limit"));
        }
        let mut batch = Self::new(vec![])?;
        batch.invalidations = rows;
        Ok(batch)
    }
}

impl BrokerCommandJournal {
    /// Atomic deduplication/correction by durable broker fill identity. Refreshes
    /// update the comparison watermark without expanding the history. A stale
    /// replay cannot overwrite a correction, including across execution epochs.
    pub async fn record_execution_fills(
        &mut self,
        batch: &ExecutionFillBatch,
    ) -> Result<(), BrokerCommandError> {
        self.check_analytics_admission().await?;
        let tx = turso::transaction::Transaction::new_unchecked(
            self.analytics_connection(),turso::transaction::TransactionBehavior::Immediate)
            .await
            .map_err(journal_error)?;
        if let Some((account, owner)) = &batch.history_owner {
            if account != &self.account_id {
                return Err(journal_error("history account scope changed"));
            }
            // The one command worker owns both connections. Optional history
            // may read canonical ownership, but never writes broker recovery.
            history::validate_owner(&self.connection, owner).await?;
        }
        for (input, encoded) in &batch.rows {
            let mut effective = input.clone();
            let fill = &input.fill;
            if invalidations::suppressed(&tx, fill).await? {
                continue;
            }
            let mut rows=tx.query("SELECT h.as_of_us,f.execution_epoch,f.observation_json FROM broker_execution_fill_heads h
                JOIN broker_execution_fills f ON f.sequence=h.sequence WHERE h.provider=?1 AND h.fill_id=?2",
                params![fill.provider.clone(),fill.fill_id.clone()]).await.map_err(journal_error)?;
            let previous = rows.next().await.map_err(journal_error)?;
            let mut epoch = batch
                .history_owner
                .as_ref()
                .map(|(_, owner)| owner.execution_epoch)
                .unwrap_or(self.lifecycle.epoch) as i64;
            if let Some(row) = previous {
                let as_of: i64 = row.get(0).map_err(journal_error)?;
                epoch = row.get(1).map_err(journal_error)?;
                if batch
                    .history_owner
                    .as_ref()
                    .is_some_and(|(_, owner)| owner.execution_epoch as i64 != epoch)
                {
                    return Err(journal_error("history fill execution epoch conflict"));
                }
                let saved: SavedFill =
                    serde_json::from_str(&row.get::<String>(2).map_err(journal_error)?)
                        .map_err(journal_error)?;
                let (old, invalidation) = saved.into_parts()?;
                drop(rows);
                if fill.as_of_us < as_of {
                    continue;
                }
                // A reconnect may deliver the same deal before its client
                // correlation is restored. Do not erase an established owner.
                if effective.insight_id.is_none() {
                    effective.insight_id = old.insight_id.clone();
                }
                if old.fill.order_id != fill.order_id
                    || old.fill.position_id != fill.position_id
                    || old.fill.symbol != fill.symbol
                    || old.fill.side != fill.side
                    || old.fill.effect != fill.effect
                    || (old.insight_id.is_some() && old.insight_id != effective.insight_id)
                {
                    return Err(journal_error("execution fill identity/ownership conflict"));
                }
                if invalidation.is_none()
                    && old.fill.same_facts(fill)
                    && old.insight_id == effective.insight_id
                {
                    tx.execute("UPDATE broker_execution_fill_heads SET as_of_us=?1 WHERE provider=?2 AND fill_id=?3",
                        params![fill.as_of_us,fill.provider.clone(),fill.fill_id.clone()]).await.map_err(journal_error)?;
                    continue;
                }
                if fill.as_of_us == as_of {
                    return Err(journal_error(
                        "conflicting execution fill at same observation watermark",
                    ));
                }
            } else {
                drop(rows);
            }
            let json = if effective.insight_id == input.insight_id {
                encoded.clone()
            } else {
                serde_json::to_string(&effective).map_err(journal_error)?
            };
            tx.execute("INSERT INTO broker_execution_fills(execution_epoch,recorded_at_us,observation_json) VALUES(?1,?2,?3)",
                params![epoch,chrono::Utc::now().timestamp_micros(),json]).await.map_err(journal_error)?;
            tx.execute("INSERT INTO broker_execution_fill_heads(provider,fill_id,sequence,as_of_us) VALUES(?1,?2,last_insert_rowid(),?3)
                ON CONFLICT(provider,fill_id) DO UPDATE SET sequence=excluded.sequence,as_of_us=excluded.as_of_us",
                params![fill.provider.clone(),fill.fill_id.clone(),fill.as_of_us]).await.map_err(journal_error)?;
            history::invalidate_position_check(
                &tx,
                &fill.provider,
                fill.position_id.as_deref(),
                fill.as_of_us,
            )
            .await?;
            if let Some(clock) = &batch.history_clock {
                tx.execute(
                    "INSERT INTO broker_execution_fill_history_clocks(clock_json,first_sequence)
                    SELECT ?1,MAX(sequence) FROM broker_execution_fills WHERE 1
                    ON CONFLICT(clock_json) DO NOTHING",
                    params![clock.clone()],
                )
                .await
                .map_err(journal_error)?;
            }
        }
        let invalidated = invalidations::apply(
            &tx,
            &batch.invalidations,
            batch.history_owner.as_ref().map(|(_, owner)| owner),
        )
        .await?;
        if invalidated {
            if let Some(clock) = &batch.history_clock {
                tx.execute(
                    "INSERT INTO broker_execution_fill_history_clocks(clock_json,first_sequence)
                    SELECT ?1,MAX(sequence) FROM broker_execution_fills WHERE 1
                    ON CONFLICT(clock_json) DO NOTHING",
                    params![clock.clone()],
                )
                .await
                .map_err(journal_error)?;
            }
        }
        history::persist_position_check(&tx, batch).await?;
        super::analytics_capture::commit_observations(tx,self.analytics_connection(),self.analytics_path(),self.analytics_capture_budget,self.lifecycle.epoch).await
    }

    /// Indexed, fixed-watermark feed. Corrections are replacement records, not
    /// additional economic fills. This read cannot change broker recovery state.
    pub async fn execution_fill_page(
        &self,
        after: u64,
        through: Option<u64>,
        limit: usize,
    ) -> Result<ExecutionFillPage, BrokerCommandError> {
        let mut rows = self.analytics_connection()
            .query(
                "SELECT COALESCE(MAX(sequence),0) FROM broker_execution_fills",
                (),
            )
            .await
            .map_err(journal_error)?;
        let head: u64 = rows
            .next()
            .await
            .map_err(journal_error)?
            .ok_or_else(|| journal_error("missing fill watermark"))?
            .get::<i64>(0)
            .map_err(journal_error)?
            .try_into()
            .map_err(journal_error)?;
        drop(rows);
        let through = through.unwrap_or(head);
        if after > through || through > head {
            return Err(journal_error(
                "execution fill cursor exceeds retained watermark",
            ));
        }
        let limit = limit.clamp(1, MAX_FILL_PAGE_ROWS);
        let mut rows=self.analytics_connection().query("SELECT sequence,execution_epoch,recorded_at_us,observation_json FROM broker_execution_fills
            WHERE sequence>?1 AND sequence<=?2 ORDER BY sequence LIMIT ?3",
            params![after as i64,through as i64,(limit+1) as i64]).await.map_err(journal_error)?;
        let mut records = Vec::new();
        let mut position_checks = Vec::new();
        let mut bytes = 0;
        let mut next = after;
        let mut has_more = false;
        while let Some(row) = rows.next().await.map_err(journal_error)? {
            let json: String = row.get(3).map_err(journal_error)?;
            if json.len() > MAX_FILL_BYTES {
                return Err(journal_error("oversized saved execution fill"));
            }
            if records.len() + position_checks.len() == limit || bytes + json.len() > MAX_FILL_BYTES
            {
                has_more = true;
                break;
            }
            let saved: SavedFill = serde_json::from_str(&json).map_err(journal_error)?;
            next = row
                .get::<i64>(0)
                .map_err(journal_error)?
                .try_into()
                .map_err(journal_error)?;
            if let SavedFill::PositionCheck { position_check } = saved {
                let record = PositionQuantityRecord {
                    sequence: next,
                    execution_epoch: row
                        .get::<i64>(1)
                        .map_err(journal_error)?
                        .try_into()
                        .map_err(journal_error)?,
                    recorded_at_us: row.get(2).map_err(journal_error)?,
                    observation: position_check,
                };
                record.validate().map_err(journal_error)?;
                position_checks.push(record);
                bytes += json.len();
                continue;
            }
            let (observation, invalidation) = saved.into_parts()?;
            let record = ExecutionFillRecord {
                sequence: next,
                execution_epoch: row
                    .get::<i64>(1)
                    .map_err(journal_error)?
                    .try_into()
                    .map_err(journal_error)?,
                recorded_at_us: row.get(2).map_err(journal_error)?,
                observation,
                invalidation,
            };
            record.validate().map_err(journal_error)?;
            records.push(record);
            bytes += json.len();
        }
        drop(rows);
        let mut clocks = self.analytics_connection()
            .query(
                "SELECT clock_json FROM broker_execution_fill_history_clocks
            WHERE first_sequence<=?1 ORDER BY first_sequence LIMIT 2",
                params![through as i64],
            )
            .await
            .map_err(journal_error)?;
        let mut coverage = "observed_broker_fills_since_ledger_installation".to_string();
        if let Some(row) = clocks.next().await.map_err(journal_error)? {
            coverage = format!(
                "observed_fills_and_bounded_owned_history;clock={}",
                row.get::<String>(0).map_err(journal_error)?
            );
            if clocks.next().await.map_err(journal_error)?.is_some() {
                coverage = "observed_fills_and_bounded_owned_history;multiple_explicit_clock_windows;recovery_not_reconciled".into();
            }
        }
        Ok(ExecutionFillPage {
            version: if !position_checks.is_empty() {
                EXECUTION_POSITION_CHECK_PAGE_VERSION
            } else if records.iter().any(|r| r.invalidation.is_some()) {
                EXECUTION_FILL_PAGE_VERSION
            } else {
                EXECUTION_FILL_VERSION
            },
            source_id: self.evidence_source_id.clone(),
            strategy_id: self.strategy_id.clone(),
            account_id: self.account_id.clone(),
            after_sequence: after,
            through_sequence: through,
            next_sequence: next,
            has_more,
            records,
            position_checks,
            generated_at_us: chrono::Utc::now().timestamp_micros(),
            coverage,
            valuation_basis: "broker_reported_cash_no_inferred_notional_multiplier_or_fx".into(),
            recovery_complete: None,
        })
    }
}

#[cfg(test)]
mod tests;
