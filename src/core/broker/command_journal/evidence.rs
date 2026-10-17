//! Append-only, sanitized state evidence, committed with its source checkpoint.
//! No broker mutation, strategy callback, full-tree scan, or cloud I/O occurs here.
use super::*;
use crate::core::insight::execution_checkpoint::InsightExecutionCheckpoint;
use aqe_live_protocol::{
    EXECUTION_EVIDENCE_VERSION, ExecutionEvidenceFeedPage, ExecutionEvidenceObservation,
    ExecutionEvidencePage, ExecutionEvidencePartialClose, ExecutionEvidenceRecord,
    ExecutionEvidenceState,
};

const MAX_EVIDENCE_PAGE_ROWS: usize = 128;
const MAX_EVIDENCE_PAGE_BYTES: usize = 1024 * 1024;

pub(super) const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS broker_execution_evidence_source (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
        source_id TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS broker_execution_evidence_export (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
        cursor INTEGER NOT NULL CHECK(cursor>=0),
        last_transfer_id TEXT,
        pending_transfer_id TEXT,
        pending_next INTEGER,
        pending_json TEXT
    );
    INSERT INTO broker_execution_evidence_export(singleton,cursor) VALUES(1,0)
        ON CONFLICT(singleton) DO NOTHING;
    CREATE TABLE IF NOT EXISTS broker_execution_evidence (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
        execution_epoch INTEGER NOT NULL CHECK(execution_epoch>0),
        insight_id TEXT NOT NULL,
        checkpoint_revision INTEGER NOT NULL CHECK(checkpoint_revision>0),
        observed_at_us INTEGER NOT NULL,
        recorded_at_us INTEGER NOT NULL,
        state_json TEXT NOT NULL,
        UNIQUE(execution_epoch,insight_id,checkpoint_revision)
    );
    CREATE INDEX IF NOT EXISTS idx_execution_evidence_epoch_sequence
        ON broker_execution_evidence(execution_epoch,sequence);
    CREATE INDEX IF NOT EXISTS idx_execution_evidence_owner_sequence
        ON broker_execution_evidence(insight_id,sequence DESC);
";

pub(super) async fn install_source(connection: &Connection) -> Result<String, BrokerCommandError> {
    connection
        .execute(
            "INSERT INTO broker_execution_evidence_source(singleton,source_id)
         VALUES(1,?1) ON CONFLICT(singleton) DO NOTHING",
            params![uuid::Uuid::new_v4().to_string()],
        )
        .await
        .map_err(journal_error)?;
    let mut rows = connection
        .query(
            "SELECT source_id FROM broker_execution_evidence_source WHERE singleton=1",
            (),
        )
        .await
        .map_err(journal_error)?;
    let source: String = rows
        .next()
        .await
        .map_err(journal_error)?
        .ok_or_else(|| journal_error("missing execution evidence source identity"))?
        .get(0)
        .map_err(journal_error)?;
    let id = uuid::Uuid::parse_str(&source).map_err(journal_error)?;
    if id.is_nil() {
        return Err(journal_error("invalid execution evidence source identity"));
    }
    Ok(source)
}

fn finite(value: Option<f64>, invalid: &mut usize) -> Option<f64> {
    value.filter(|value| {
        if value.is_finite() {
            true
        } else {
            *invalid += 1;
            false
        }
    })
}

fn levels(values: &Option<Vec<f64>>, invalid: &mut usize) -> Option<Vec<f64>> {
    // A partially valid protection list must not silently become a different
    // set of orders. Mark the whole list unavailable if any element is invalid.
    values.as_ref().and_then(|values| {
        let count = values.iter().filter(|v| !v.is_finite()).count();
        *invalid += count;
        (count == 0).then(|| values.clone())
    })
}

pub(super) fn project(row: &InsightExecutionCheckpoint) -> ExecutionEvidenceState {
    let mut invalid = 0;
    ExecutionEvidenceState {
        insight_id: row.insight_id.to_string(),
        parent_id: row.parent_id.map(|id| id.to_string()),
        is_split_insight: row.is_split_insight,
        symbol: row.symbol.clone(),
        strategy_label: row.strategy_type.to_string(),
        state: format!("{:?}", row.state),
        side: format!("{:?}", row.side),
        entry_order_id: row.order_id.clone(),
        close_order_id: row.close_order_id.clone(),
        intended_quantity: finite(row.quantity, &mut invalid),
        entry_filled_quantity: finite(row.entry_filled_quantity, &mut invalid),
        partial_closed_quantity: finite(row.partial_filled_quantity, &mut invalid),
        entry_price: finite(row.filled_price, &mut invalid),
        close_price: finite(row.close_price, &mut invalid),
        broker_realized_pnl: finite(row.broker_realized_pnl, &mut invalid),
        commission: finite(row.commission, &mut invalid),
        swap: finite(row.swap, &mut invalid),
        position_accounting: row
            .position_accounting
            .clone()
            .filter(|value| value.validate().is_ok()),
        order_type: format!("{:?}", row.order_type),
        order_class: format!("{:?}", row.order_class),
        limit_price: finite(row.limit_price, &mut invalid),
        stop_price: finite(row.stop_price, &mut invalid),
        take_profit_levels: levels(&row.take_profit_levels, &mut invalid),
        stop_loss_levels: levels(&row.stop_loss_levels, &mut invalid),
        trailing_stop_price: finite(row.trailing_stop_price, &mut invalid),
        created_at_us: row.created_at.timestamp_micros(),
        filled_at_us: row.filled_at.map(|at| at.timestamp_micros()),
        closed_at_us: row.closed_at.map(|at| at.timestamp_micros()),
        partial_closes: row
            .partial_closes
            .iter()
            .map(|partial| ExecutionEvidencePartialClose {
                order_id: partial.order_id.clone(),
                side: format!("{:?}", partial.side),
                quantity: finite(Some(partial.quantity), &mut invalid),
                entry_price: finite(Some(partial.entry_price), &mut invalid),
                filled_price: finite(partial.filled_price, &mut invalid),
            })
            .collect(),
        invalid_numeric_fields: invalid,
    }
}

pub(super) async fn append(
    tx: &turso::transaction::Transaction<'_>,
    epoch: u64,
    insight_id: &str,
    revision: i64,
    observed_at_us: i64,
    state_json: &str,
) -> Result<(), BrokerCommandError> {
    let mut rows = tx
        .query(
            "SELECT state_json,execution_epoch FROM broker_execution_evidence
         WHERE insight_id=?1 ORDER BY sequence DESC LIMIT 1",
            params![insight_id.to_owned()],
        )
        .await
        .map_err(journal_error)?;
    let owner_epoch = if let Some(row) = rows.next().await.map_err(journal_error)? {
        if row.get::<String>(0).map_err(journal_error)? == state_json {
            // Private-only revisions do not emit another analytics observation.
            return Ok(());
        }
        // Late cost/fill corrections belong to the insight's original observed
        // epoch, never the subsequently started deployment's analytics.
        row.get::<i64>(1).map_err(journal_error)?
    } else {
        i64::try_from(epoch).map_err(journal_error)?
    };
    drop(rows);
    tx.execute(
        "INSERT INTO broker_execution_evidence
        (execution_epoch,insight_id,checkpoint_revision,observed_at_us,recorded_at_us,state_json)
        VALUES(?1,?2,?3,?4,?5,?6)",
        params![
            owner_epoch,
            insight_id.to_owned(),
            revision,
            observed_at_us,
            chrono::Utc::now().timestamp_micros(),
            state_json.to_owned()
        ],
    )
    .await
    .map_err(journal_error)?;
    Ok(())
}

impl BrokerCommandJournal {
    /// Freeze the next bounded export before handing it to an asynchronous
    /// publisher. A restart returns exactly the same bytes and read boundary,
    /// even if additional execution observations have committed since then.
    pub async fn prepare_execution_evidence_export(
        &self,
    ) -> Result<Option<ExecutionEvidenceFeedPage>, BrokerCommandError> {
        let mut rows = self.connection.query(
            "SELECT cursor,pending_json,pending_transfer_id,pending_next FROM broker_execution_evidence_export WHERE singleton=1", (),
        ).await.map_err(journal_error)?;
        let row = rows
            .next()
            .await
            .map_err(journal_error)?
            .ok_or_else(|| journal_error("missing execution evidence export cursor"))?;
        let cursor =
            u64::try_from(row.get::<i64>(0).map_err(journal_error)?).map_err(journal_error)?;
        let pending: Option<String> = row.get(1).map_err(journal_error)?;
        let pending_id: Option<String> = row.get(2).map_err(journal_error)?;
        let pending_next: Option<i64> = row.get(3).map_err(journal_error)?;
        drop(rows);
        if let Some(pending) = pending {
            if pending.len() > aqe_live_protocol::EXECUTION_EVIDENCE_TRANSFER_BYTES {
                return Err(journal_error("oversized pending execution evidence export"));
            }
            let page: ExecutionEvidenceFeedPage =
                serde_json::from_str(&pending).map_err(journal_error)?;
            if page.source_id != self.evidence_source_id
                || page.after_sequence != cursor
                || page.strategy_id != self.strategy_id
                || page.account_id != self.account_id
            {
                return Err(journal_error(
                    "pending execution evidence export scope mismatch",
                ));
            }
            let checksum =
                aqe_live_protocol::execution_evidence_transfer_id(&page).map_err(journal_error)?;
            if pending_id.as_deref() != Some(&checksum)
                || pending_next != Some(page.next_sequence as i64)
            {
                return Err(journal_error(
                    "pending execution evidence export checksum mismatch",
                ));
            }
            return Ok(Some(page));
        }
        let page = self
            .execution_evidence_feed_page(cursor, None, MAX_EVIDENCE_PAGE_ROWS)
            .await?;
        if page.observations.is_empty() {
            return Ok(None);
        }
        let transfer_id =
            aqe_live_protocol::execution_evidence_transfer_id(&page).map_err(journal_error)?;
        self.connection.execute(
            "UPDATE broker_execution_evidence_export SET pending_transfer_id=?1,pending_next=?2,pending_json=?3 WHERE singleton=1",
            params![transfer_id, page.next_sequence as i64, serde_json::to_string(&page).map_err(journal_error)?],
        ).await.map_err(journal_error)?;
        Ok(Some(page))
    }

    /// Called only after durable outbox receipts for ALL chunks. This changes
    /// analytics delivery metadata, never command disposition or recovery state.
    pub async fn acknowledge_execution_evidence_export(
        &self,
        receipt: &aqe_live_protocol::ExecutionEvidenceExportReceipt,
    ) -> Result<(), BrokerCommandError> {
        if receipt.source_id != self.evidence_source_id
            || receipt.after_sequence >= receipt.next_sequence
            || receipt.next_sequence > i64::MAX as u64
        {
            return Err(journal_error("invalid execution evidence export receipt"));
        }
        let mut rows = self.connection.query(
            "SELECT cursor,last_transfer_id,pending_transfer_id,pending_next FROM broker_execution_evidence_export WHERE singleton=1", (),
        ).await.map_err(journal_error)?;
        let row = rows
            .next()
            .await
            .map_err(journal_error)?
            .ok_or_else(|| journal_error("missing execution evidence export cursor"))?;
        let cursor: i64 = row.get(0).map_err(journal_error)?;
        let last: Option<String> = row.get(1).map_err(journal_error)?;
        let pending: Option<String> = row.get(2).map_err(journal_error)?;
        let next: Option<i64> = row.get(3).map_err(journal_error)?;
        drop(rows);
        if cursor == receipt.next_sequence as i64 && last.as_deref() == Some(&receipt.transfer_id) {
            return Ok(());
        }
        if cursor != receipt.after_sequence as i64
            || pending.as_deref() != Some(&receipt.transfer_id)
            || next != Some(receipt.next_sequence as i64)
        {
            return Err(journal_error(
                "execution evidence export receipt does not match pending page",
            ));
        }
        self.connection.execute(
            "UPDATE broker_execution_evidence_export SET cursor=?1,last_transfer_id=pending_transfer_id,
             pending_transfer_id=NULL,pending_next=NULL,pending_json=NULL WHERE singleton=1",
            params![receipt.next_sequence as i64],
        ).await.map_err(journal_error)?;
        Ok(())
    }

    /// Bounded read-only feed. Capture `through_sequence` on the first page and
    /// pass it unchanged thereafter, even if the writer commits new observations.
    /// Reading evidence neither marks commands applied nor claims broker recovery.
    pub async fn execution_evidence_page(
        &self,
        execution_epoch: u64,
        after_sequence: u64,
        through_sequence: Option<u64>,
        limit: usize,
    ) -> Result<ExecutionEvidencePage, BrokerCommandError> {
        if execution_epoch == 0 || execution_epoch > self.lifecycle.epoch {
            return Err(journal_error("invalid execution evidence epoch"));
        }
        let epoch = i64::try_from(execution_epoch).map_err(journal_error)?;
        let after = i64::try_from(after_sequence).map_err(journal_error)?;
        let mut head = self.connection.query(
            "SELECT COALESCE(MAX(sequence),0) FROM broker_execution_evidence WHERE execution_epoch=?1",
            params![epoch],
        ).await.map_err(journal_error)?;
        let high: i64 = head
            .next()
            .await
            .map_err(journal_error)?
            .ok_or_else(|| journal_error("missing execution evidence watermark"))?
            .get(0)
            .map_err(journal_error)?;
        drop(head);
        let through = through_sequence
            .map(i64::try_from)
            .transpose()
            .map_err(journal_error)?
            .unwrap_or(high);
        if through > high || after > through {
            return Err(journal_error(
                "execution evidence cursor exceeds retained watermark",
            ));
        }
        let limit = limit.clamp(1, MAX_EVIDENCE_PAGE_ROWS);
        let mut rows = self.connection.query(
            "SELECT sequence,checkpoint_revision,observed_at_us,recorded_at_us,state_json,insight_id
             FROM broker_execution_evidence WHERE execution_epoch=?1 AND sequence>?2 AND sequence<=?3
             ORDER BY sequence LIMIT ?4",
            params![epoch, after, through, (limit+1) as i64],
        ).await.map_err(journal_error)?;
        let mut records = Vec::new();
        let mut bytes = 0;
        let mut next = after_sequence;
        let mut has_more = false;
        while let Some(row) = rows.next().await.map_err(journal_error)? {
            let json: String = row.get(4).map_err(journal_error)?;
            if json.len() > MAX_EVIDENCE_PAGE_BYTES {
                return Err(journal_error("oversized execution evidence record"));
            }
            if records.len() == limit || bytes + json.len() > MAX_EVIDENCE_PAGE_BYTES {
                has_more = true;
                break;
            }
            let state: ExecutionEvidenceState =
                serde_json::from_str(&json).map_err(journal_error)?;
            if state.insight_id != row.get::<String>(5).map_err(journal_error)? {
                return Err(journal_error("execution evidence identity mismatch"));
            }
            next =
                u64::try_from(row.get::<i64>(0).map_err(journal_error)?).map_err(journal_error)?;
            records.push(ExecutionEvidenceRecord {
                sequence: next,
                checkpoint_revision: u64::try_from(row.get::<i64>(1).map_err(journal_error)?)
                    .map_err(journal_error)?,
                observed_at_us: row.get(2).map_err(journal_error)?,
                recorded_at_us: row.get(3).map_err(journal_error)?,
                state,
            });
            bytes += json.len();
        }
        Ok(ExecutionEvidencePage {
            schema_version: EXECUTION_EVIDENCE_VERSION,
            source_id: self.evidence_source_id.clone(),
            strategy_id: self.strategy_id.clone(),
            account_id: self.account_id.clone(),
            execution_epoch,
            run_id: None,
            session_id: None,
            configuration_version: None,
            after_sequence,
            through_sequence: through as u64,
            next_sequence: next,
            has_more,
            records,
            generated_at_us: chrono::Utc::now().timestamp_micros(),
            coverage: "canonical_state_changes_since_projection_installation".into(),
            recovery_complete: None,
            valuation_basis: "raw_broker_fields_no_inferred_multiplier_fx_or_fee_semantics".into(),
        })
    }

    /// Source-wide keyset export. Only one bounded page is read, without joining
    /// checkpoints, traversing the insight graph or touching command state.
    pub async fn execution_evidence_feed_page(
        &self,
        after_sequence: u64,
        through_sequence: Option<u64>,
        limit: usize,
    ) -> Result<ExecutionEvidenceFeedPage, BrokerCommandError> {
        let after = i64::try_from(after_sequence).map_err(journal_error)?;
        let mut rows = self
            .connection
            .query(
                "SELECT COALESCE(MAX(sequence),0) FROM broker_execution_evidence",
                (),
            )
            .await
            .map_err(journal_error)?;
        let high: i64 = rows
            .next()
            .await
            .map_err(journal_error)?
            .ok_or_else(|| journal_error("missing execution evidence watermark"))?
            .get(0)
            .map_err(journal_error)?;
        drop(rows);
        let through = through_sequence
            .map(i64::try_from)
            .transpose()
            .map_err(journal_error)?
            .unwrap_or(high);
        if through > high || after > through {
            return Err(journal_error(
                "execution evidence cursor exceeds retained watermark",
            ));
        }
        let limit = limit.clamp(1, MAX_EVIDENCE_PAGE_ROWS);
        let mut rows = self.connection.query(
            "SELECT sequence,checkpoint_revision,observed_at_us,recorded_at_us,state_json,insight_id,execution_epoch
             FROM broker_execution_evidence WHERE sequence>?1 AND sequence<=?2 ORDER BY sequence LIMIT ?3",
            params![after, through, (limit + 1) as i64],
        ).await.map_err(journal_error)?;
        let mut observations = Vec::new();
        let mut bytes = 0;
        let mut next = after_sequence;
        let mut has_more = false;
        while let Some(row) = rows.next().await.map_err(journal_error)? {
            let json: String = row.get(4).map_err(journal_error)?;
            if json.len() > MAX_EVIDENCE_PAGE_BYTES {
                return Err(journal_error("oversized execution evidence record"));
            }
            if observations.len() == limit || bytes + json.len() > MAX_EVIDENCE_PAGE_BYTES {
                has_more = true;
                break;
            }
            let state: ExecutionEvidenceState =
                serde_json::from_str(&json).map_err(journal_error)?;
            if state.insight_id != row.get::<String>(5).map_err(journal_error)? {
                return Err(journal_error("execution evidence identity mismatch"));
            }
            let epoch =
                u64::try_from(row.get::<i64>(6).map_err(journal_error)?).map_err(journal_error)?;
            if epoch == 0 || epoch > self.lifecycle.epoch {
                return Err(journal_error("invalid execution evidence owner epoch"));
            }
            next =
                u64::try_from(row.get::<i64>(0).map_err(journal_error)?).map_err(journal_error)?;
            observations.push(ExecutionEvidenceObservation {
                execution_epoch: epoch,
                record: ExecutionEvidenceRecord {
                    sequence: next,
                    checkpoint_revision: u64::try_from(row.get::<i64>(1).map_err(journal_error)?)
                        .map_err(journal_error)?,
                    observed_at_us: row.get(2).map_err(journal_error)?,
                    recorded_at_us: row.get(3).map_err(journal_error)?,
                    state,
                },
            });
            bytes += json.len();
        }
        Ok(ExecutionEvidenceFeedPage {
            schema_version: EXECUTION_EVIDENCE_VERSION,
            source_id: self.evidence_source_id.clone(),
            strategy_id: self.strategy_id.clone(),
            account_id: self.account_id.clone(),
            after_sequence,
            through_sequence: through as u64,
            next_sequence: next,
            has_more,
            observations,
            generated_at_us: chrono::Utc::now().timestamp_micros(),
            coverage: "canonical_state_changes_since_projection_installation".into(),
            recovery_complete: None,
            valuation_basis: "raw_broker_fields_no_inferred_multiplier_fx_or_fee_semantics".into(),
        })
    }
}

#[cfg(test)]
mod tests;
