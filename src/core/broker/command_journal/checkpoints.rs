//! Atomic state + completion ownership. No applied marker can be written without
//! the matching row in the same FULL-synchronous transaction.
use super::*;
use crate::core::{
    broker::commands::BrokerCommandReceipt,
    insight::execution_checkpoint::InsightExecutionCheckpoint,
};
use std::{collections::HashSet, io::Write};

pub(crate) const MAX_CHECKPOINT_ROWS: usize = 32;
pub(crate) const MAX_CHECKPOINT_BYTES: usize = 1024 * 1024;

/// Private journal metadata preserves original execution ownership across
/// restarts. Existing Insight fields remain the source of order and symbol.
pub(super) async fn install_owner_epochs(
    connection: &Connection,
    current_epoch: u64,
) -> Result<(), BrokerCommandError> {
    let mut columns = connection
        .query("PRAGMA table_info(broker_insight_checkpoints)", ())
        .await
        .map_err(journal_error)?;
    let mut installed = HashSet::new();
    while let Some(row) = columns.next().await.map_err(journal_error)? {
        installed.insert(row.get::<String>(1).map_err(journal_error)?);
    }
    drop(columns);
    let mut version_rows = connection
        .query("PRAGMA user_version", ())
        .await
        .map_err(journal_error)?;
    let version = version_rows
        .next()
        .await
        .map_err(journal_error)?
        .ok_or_else(|| journal_error("missing private journal schema version"))?
        .get::<i64>(0)
        .map_err(journal_error)?;
    drop(version_rows);
    let mut backfill = version < 1;
    for (name, definition) in [
        ("execution_epoch", "INTEGER NOT NULL DEFAULT 0"),
        ("order_id", "TEXT"),
        ("symbol", "TEXT"),
        ("materialized", "INTEGER NOT NULL DEFAULT 0"),
    ] {
        if !installed.contains(name) {
            connection
                .execute_batch(&format!(
                    "ALTER TABLE broker_insight_checkpoints ADD COLUMN {name} {definition};"
                ))
                .await
                .map_err(journal_error)?;
            backfill = true;
        }
    }
    // Scalar lookup columns index the existing checkpoint, rather than copying
    // its execution state/history. Reads do not parse private JSON per owner.
    let mut index = connection.query("SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_fill_history_owned_order'", ())
        .await.map_err(journal_error)?;
    let legacy_index = match index.next().await.map_err(journal_error)? {
        Some(row) => row
            .get::<String>(0)
            .map_err(journal_error)?
            .contains("json_extract"),
        None => false,
    };
    drop(index);
    if legacy_index {
        connection
            .execute_batch("DROP INDEX idx_fill_history_owned_order;")
            .await
            .map_err(journal_error)?;
    }
    connection.execute_batch("CREATE INDEX IF NOT EXISTS idx_fill_history_owned_order ON broker_insight_checkpoints(order_id);")
        .await.map_err(journal_error)?;
    if backfill {
        connection.execute_batch("UPDATE broker_insight_checkpoints SET
            order_id=json_extract(checkpoint_json,'$.order_id'),symbol=json_extract(checkpoint_json,'$.symbol'),
            materialized=COALESCE(json_extract(checkpoint_json,'$.materialized'),0)
            WHERE json_valid(checkpoint_json);")
            .await.map_err(journal_error)?;
    }
    connection.execute_batch("CREATE INDEX IF NOT EXISTS idx_checkpoint_owner_epoch ON broker_insight_checkpoints(execution_epoch);")
        .await.map_err(journal_error)?;
    // Prefer an established archive epoch. Older journals without the archive
    // use the retained lifecycle epoch; no historical epoch is invented.
    connection
        .execute(
            "UPDATE broker_insight_checkpoints SET execution_epoch=COALESCE(
        (SELECT execution_epoch FROM broker_execution_evidence e
            WHERE e.insight_id=broker_insight_checkpoints.insight_id ORDER BY sequence LIMIT 1),
        ?1)
        WHERE execution_epoch=0 AND materialized=1",
            params![i64::try_from(current_epoch).map_err(journal_error)?],
        )
        .await
        .map_err(journal_error)?;
    if version < 1 {
        // Written last: an interrupted ALTER/backfill resumes on next open.
        connection
            .execute_batch("PRAGMA user_version=1;")
            .await
            .map_err(journal_error)?;
    }
    Ok(())
}

struct EncodedCheckpoint {
    id: String,
    revision: i64,
    json: String,
    metadata_json: String,
    recovery_seed: bool,
    materialized: bool,
    order_id: Option<String>,
    symbol: String,
    observed_at_us: i64,
}

/// Intent and owning Insight state share one commit before broker dispatch.
/// This is consumed by startup/runtime admission, never sent to a broker.
pub(crate) struct ExecutionIntent {
    pub command: BrokerCommand,
    pub queued_at_us: i64,
    pub admission_order: u64,
}

struct EncodedIntent {
    command: BrokerCommand,
    queued_at_us: i64,
    admission_order: i64,
    json: String,
}

pub(crate) struct ExecutionCheckpointBatch {
    rows: Vec<EncodedCheckpoint>,
    pub applied: Vec<BrokerCommandReceipt>,
    intents: Vec<EncodedIntent>,
}

/// Encoding is bounded while writing, not after allocating an oversized String.
struct BoundedJson {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for BoundedJson {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other(
                "execution checkpoint exceeds byte limit",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl ExecutionCheckpointBatch {
    pub fn new(
        rows: Vec<InsightExecutionCheckpoint>,
        applied: Vec<BrokerCommandReceipt>,
    ) -> Result<Self, BrokerCommandError> {
        Self::with_intents(rows, applied, Vec::new())
    }

    pub fn with_intents(
        rows: Vec<InsightExecutionCheckpoint>,
        applied: Vec<BrokerCommandReceipt>,
        intents: Vec<ExecutionIntent>,
    ) -> Result<Self, BrokerCommandError> {
        if rows.is_empty() || rows.len() > MAX_CHECKPOINT_ROWS || applied.len() > 256 {
            return Err(BrokerCommandError::Invalid(
                "execution checkpoint batch exceeds row/receipt limit".into(),
            ));
        }
        let mut ids = HashSet::with_capacity(rows.len());
        let mut commands = HashSet::with_capacity(applied.len());
        for receipt in &applied {
            let row = rows
                .iter()
                .find(|row| row.insight_id.to_string() == receipt.insight_id)
                .ok_or_else(|| {
                    journal_error("applied receipt is missing its reduced insight checkpoint")
                })?;
            if row.retains(receipt) || !commands.insert(receipt.id.clone()) || !row.materialized {
                return Err(journal_error(
                    "applied receipt is still pending, duplicated, or unmaterialized",
                ));
            }
        }
        let mut encoded = Vec::with_capacity(rows.len());
        let mut receipt_bytes = BoundedJson {
            bytes: Vec::new(),
            limit: MAX_CHECKPOINT_BYTES,
        };
        serde_json::to_writer(&mut receipt_bytes, &applied).map_err(journal_error)?;
        let mut remaining = MAX_CHECKPOINT_BYTES - receipt_bytes.bytes.len();
        if intents.len() > 256 {
            return Err(journal_error("too many checkpoint intents"));
        }
        let mut encoded_intents = Vec::with_capacity(intents.len());
        for intent in intents {
            let admission_order = i64::try_from(intent.admission_order).map_err(journal_error)?;
            if admission_order == 0 {
                return Err(journal_error("invalid owner admission order"));
            }
            let receipt = intent.command.receipt();
            if !commands.insert(receipt.id.clone()) {
                return Err(journal_error(
                    "duplicate or already applied checkpoint intent",
                ));
            }
            let row = rows
                .iter()
                .find(|row| row.insight_id.to_string() == receipt.insight_id)
                .ok_or_else(|| journal_error("intent is missing its pending insight checkpoint"))?;
            if !row.materialized
                || (!row
                    .pending_mutations
                    .iter()
                    .any(|pending| pending.receipt == receipt)
                    && !row.owner_receipts.contains(&receipt))
            {
                return Err(journal_error(
                    "intent is not retained by its insight checkpoint",
                ));
            }
            let json = intent.command.encode()?;
            remaining = remaining
                .checked_sub(json.len())
                .ok_or_else(|| journal_error("execution checkpoint exceeds byte limit"))?;
            encoded_intents.push(EncodedIntent {
                command: intent.command,
                queued_at_us: intent.queued_at_us,
                admission_order,
                json,
            });
        }
        for row in rows {
            row.validate()?;
            let id = row.insight_id.to_string();
            if !ids.insert(id.clone()) {
                return Err(journal_error("duplicate checkpoint row"));
            }
            let metadata = super::recovery::InsightRecoveryMetadata::from_row(&row);
            let metadata_json = serde_json::to_string(&metadata).map_err(journal_error)?;
            remaining = remaining
                .checked_sub(metadata_json.len())
                .ok_or_else(|| journal_error("execution checkpoint metadata exceeds byte limit"))?;
            let mut writer = BoundedJson {
                bytes: Vec::new(),
                limit: remaining,
            };
            serde_json::to_writer(&mut writer, &row).map_err(journal_error)?;
            remaining -= writer.bytes.len();
            let json = String::from_utf8(writer.bytes).map_err(journal_error)?;
            encoded.push(EncodedCheckpoint {
                id,
                revision: row.revision as i64,
                json,
                metadata_json,
                recovery_seed: metadata.recovery_seed,
                materialized: row.materialized,
                order_id: row.order_id,
                symbol: row.symbol,
                observed_at_us: row.updated_at.timestamp_micros(),
            });
        }
        Ok(Self {
            rows: encoded,
            applied,
            intents: encoded_intents,
        })
    }
}

pub(crate) struct ExecutionCheckpointPage {
    pub rows: Vec<InsightExecutionCheckpoint>,
    pub next_cursor: Option<String>,
}

impl BrokerCommandJournal {
    /// A same-revision retry is accepted only for byte-identical state. A newer
    /// revision must be exactly previous + 1. Failed validation rolls back ALL
    /// rows and receipts, including rows written earlier in this batch.
    pub(crate) async fn persist_execution_batch(
        &mut self,
        batch: &ExecutionCheckpointBatch,
    ) -> Result<(), BrokerCommandError> {
        if batch.intents.iter().any(|intent| {
            intent.command.strategy_id != self.strategy_id
                || intent.command.account_id != self.account_id
        }) {
            return Err(journal_error(
                "checkpoint intent does not match journal scope",
            ));
        }
        let tx = self
            .connection
            .transaction_with_behavior(turso::transaction::TransactionBehavior::Immediate)
            .await
            .map_err(journal_error)?;
        for intent in &batch.intents {
            let mut admission = tx
                .query(
                    "SELECT admission_order FROM broker_command_admission WHERE command_id=?1",
                    params![intent.command.id.0.clone()],
                )
                .await
                .map_err(journal_error)?;
            let existing = admission.next().await.map_err(journal_error)?;
            if let Some(row) = existing {
                if row.get::<i64>(0).map_err(journal_error)? != intent.admission_order {
                    return Err(BrokerCommandError::IdentityConflict);
                }
                drop(admission);
            } else {
                drop(admission);
                tx.execute("INSERT INTO broker_command_admission(command_id,admission_order) VALUES(?1,?2)",
                    params![intent.command.id.0.clone(), intent.admission_order]).await.map_err(journal_error)?;
            }
            let mut rows = tx
                .query(
                    "SELECT command_json FROM broker_commands WHERE command_id=?1",
                    params![intent.command.id.0.clone()],
                )
                .await
                .map_err(journal_error)?;
            if let Some(row) = rows.next().await.map_err(journal_error)? {
                let existing: BrokerCommand =
                    serde_json::from_str(&row.get::<String>(0).map_err(journal_error)?)
                        .map_err(journal_error)?;
                if existing != intent.command {
                    return Err(BrokerCommandError::IdentityConflict);
                }
                // Identical replay must never reset Dispatching/Completed/Applied.
            } else {
                drop(rows);
                tx.execute("INSERT INTO broker_commands(command_id,command_json,state,queued_at_us) VALUES(?1,?2,'queued',?3)",
                    params![intent.command.id.0.clone(), intent.json.clone(), intent.queued_at_us])
                    .await.map_err(journal_error)?;
            }
        }
        let mut evidence_remaining = MAX_CHECKPOINT_BYTES;
        for checkpoint in &batch.rows {
            let mut rows = tx.query("SELECT revision,checkpoint_json FROM broker_insight_checkpoints WHERE insight_id=?1",
                params![checkpoint.id.clone()]).await.map_err(journal_error)?;
            let existing = rows.next().await.map_err(journal_error)?;
            let (previous, unchanged) = if let Some(row) = existing {
                let previous: i64 = row.get(0).map_err(journal_error)?;
                let unchanged = previous == checkpoint.revision
                    && row.get::<String>(1).map_err(journal_error)? == checkpoint.json;
                (previous, unchanged)
            } else {
                (0, false)
            };
            drop(rows);
            if unchanged {
                continue;
            }
            if previous.checked_add(1) != Some(checkpoint.revision) {
                return Err(journal_error(
                    "stale/conflicting execution checkpoint revision",
                ));
            }
            let epoch = if checkpoint.materialized {
                self.lifecycle.epoch as i64
            } else {
                0
            };
            tx.execute("INSERT INTO broker_insight_checkpoints(insight_id,revision,checkpoint_json,execution_epoch,order_id,symbol,materialized) VALUES(?1,?2,?3,?4,?5,?6,?7)
                ON CONFLICT(insight_id) DO UPDATE SET revision=excluded.revision,checkpoint_json=excluded.checkpoint_json,
                    order_id=excluded.order_id,symbol=excluded.symbol,materialized=excluded.materialized,
                    execution_epoch=CASE WHEN broker_insight_checkpoints.execution_epoch>0 THEN broker_insight_checkpoints.execution_epoch ELSE excluded.execution_epoch END",
                params![checkpoint.id.clone(), checkpoint.revision, checkpoint.json.clone(), epoch, checkpoint.order_id.clone(), checkpoint.symbol.clone(), i64::from(checkpoint.materialized)])
                .await.map_err(journal_error)?;
            tx.execute("INSERT INTO broker_execution_graph(insight_id,revision,recovery_seed,metadata_json) VALUES(?1,?2,?3,?4)
                ON CONFLICT(insight_id) DO UPDATE SET revision=excluded.revision,recovery_seed=excluded.recovery_seed,metadata_json=excluded.metadata_json",
                params![checkpoint.id.clone(), checkpoint.revision, i64::from(checkpoint.recovery_seed), checkpoint.metadata_json.clone()])
                .await.map_err(journal_error)?;
            if self.state_history_enabled && checkpoint.materialized {
                // This compatibility projection runs on the journal worker,
                // only when explicitly enabled; never on the decision loop.
                let row: InsightExecutionCheckpoint =
                    serde_json::from_str(&checkpoint.json).map_err(journal_error)?;
                let mut writer = BoundedJson {
                    bytes: Vec::new(),
                    limit: evidence_remaining,
                };
                serde_json::to_writer(&mut writer, &super::evidence::project(&row))
                    .map_err(journal_error)?;
                evidence_remaining -= writer.bytes.len();
                let evidence = String::from_utf8(writer.bytes).map_err(journal_error)?;
                let mut origin = tx.query("SELECT execution_epoch FROM broker_insight_checkpoints WHERE insight_id=?1",
                    params![checkpoint.id.clone()]).await.map_err(journal_error)?;
                let epoch = origin
                    .next()
                    .await
                    .map_err(journal_error)?
                    .ok_or_else(|| journal_error("missing checkpoint ownership epoch"))?
                    .get::<i64>(0)
                    .map_err(journal_error)?;
                drop(origin);
                super::evidence::append(
                    &tx,
                    u64::try_from(epoch).map_err(journal_error)?,
                    &checkpoint.id,
                    checkpoint.revision,
                    checkpoint.observed_at_us,
                    &evidence,
                )
                .await?;
            }
        }
        for receipt in &batch.applied {
            let mut rows = tx.query("SELECT sequence,command_json,state,queued_at_us,dispatched_at_us,completion_json
                FROM broker_commands WHERE command_id=?1", params![receipt.id.0.clone()])
                .await.map_err(journal_error)?;
            let row = rows
                .next()
                .await
                .map_err(journal_error)?
                .ok_or_else(|| journal_error("cannot apply an unknown command"))?;
            let record = Self::decode(row)?;
            drop(rows);
            if record.command.receipt() != *receipt {
                return Err(BrokerCommandError::IdentityConflict);
            }
            let ambiguous = record.completion.as_ref().is_some_and(|completion| {
                matches!(completion.outcome, BrokerCommandOutcome::Ambiguous { .. })
            });
            let terminal_settlement = if ambiguous {
                batch
                    .rows
                    .iter()
                    .find(|row| row.id == receipt.insight_id)
                    .map(|row| serde_json::from_str::<InsightExecutionCheckpoint>(&row.json))
                    .transpose()
                    .map_err(journal_error)?
                    .is_some_and(|row| {
                        record
                            .command
                            .action
                            .settled_by_terminal(row.order_id.as_deref(), &row.state)
                    })
            } else {
                false
            };
            if record.completion.as_ref().is_none_or(|completion| {
                matches!(completion.outcome, BrokerCommandOutcome::Ambiguous { .. })
                    && !terminal_settlement
            }) {
                return Err(journal_error(
                    "unresolved broker command cannot be marked applied",
                ));
            }
            tx.execute("UPDATE broker_commands SET state='applied' WHERE command_id=?1 AND state='completed'",
                params![receipt.id.0.clone()]).await.map_err(journal_error)?;
        }
        tx.commit().await.map_err(journal_error)
    }

    /// Primary-key pagination with both a row and byte ceiling. No full table
    /// materialization, and no history/decode allocation beyond one bounded page.
    pub(crate) async fn execution_checkpoint_page(
        &self,
        after: &str,
        limit: usize,
    ) -> Result<ExecutionCheckpointPage, BrokerCommandError> {
        let mut rows = self
            .connection
            .query(
                "SELECT insight_id,revision,checkpoint_json FROM broker_insight_checkpoints
            WHERE insight_id>?1 ORDER BY insight_id LIMIT ?2",
                params![after.to_owned(), limit.clamp(1, MAX_CHECKPOINT_ROWS) as i64],
            )
            .await
            .map_err(journal_error)?;
        let mut page = Vec::new();
        let mut bytes = 0usize;
        let mut cursor = None;
        while let Some(row) = rows.next().await.map_err(journal_error)? {
            let id: String = row.get(0).map_err(journal_error)?;
            let revision: i64 = row.get(1).map_err(journal_error)?;
            let json: String = row.get(2).map_err(journal_error)?;
            if json.len() > MAX_CHECKPOINT_BYTES {
                return Err(journal_error("oversized saved execution checkpoint"));
            }
            if bytes + json.len() > MAX_CHECKPOINT_BYTES {
                break;
            }
            let checkpoint: InsightExecutionCheckpoint =
                serde_json::from_str(&json).map_err(journal_error)?;
            checkpoint.validate()?;
            if checkpoint.insight_id.to_string() != id || checkpoint.revision as i64 != revision {
                return Err(journal_error(
                    "saved execution checkpoint identity/revision mismatch",
                ));
            }
            bytes += json.len();
            cursor = Some(id);
            page.push(checkpoint);
        }
        Ok(ExecutionCheckpointPage {
            rows: page,
            next_cursor: cursor,
        })
    }
}

#[cfg(test)]
mod tests;
