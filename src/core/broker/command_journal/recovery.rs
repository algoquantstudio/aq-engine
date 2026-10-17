//! Indexed, bounded startup reads. Completed unrelated stacks are not decoded
//! into memory; only live owners and their connected recovery graph are loaded.
use super::*;
use crate::core::insight::execution_checkpoint::InsightExecutionCheckpoint;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct InsightRecoveryMetadata {
    pub id: Uuid,
    pub revision: u64,
    pub parent: Option<Uuid>,
    pub children: Vec<Uuid>,
    pub materialized: bool,
    pub recovery_seed: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dependencies: Vec<Uuid>,
}

impl InsightRecoveryMetadata {
    pub fn from_row(row: &InsightExecutionCheckpoint) -> Self {
        Self {
            id: row.insight_id,
            revision: row.revision,
            parent: row.parent_id,
            children: row.child_ids.clone(),
            materialized: row.materialized,
            dependencies: if row.state.is_active() {
                row.execution_data
                    .recovery_dependencies()
                    .expect("validated checkpoint dependencies")
            } else {
                Vec::new()
            },
            recovery_seed: row.materialized
                && (row.state.is_active()
                    || !row.owner_receipts.is_empty()
                    || !row.pending_mutations.is_empty()),
        }
    }
}

pub(crate) struct ExecutionRecovery {
    pub rows: HashMap<Uuid, InsightExecutionCheckpoint>,
    pub commands: Vec<RecoveryCommand>,
    pub stopping: bool,
}

impl ExecutionRecovery {
    /// A recovered queued entry cannot outlive cancellation of its stack or
    /// dispatch through an orphaned ownership link. Dispatched commands still
    /// replay their durable/ambiguous evidence regardless of this admission gate.
    pub fn can_dispatch_entry(&self, owner: Uuid) -> bool {
        if self.stopping
            || self
                .rows
                .get(&owner)
                .is_none_or(|row| row.state != crate::core::insight::types::InsightState::New)
        {
            return false;
        }
        let mut current = owner;
        for _ in 0..64 {
            let Some(row) = self.rows.get(&current) else {
                return false;
            };
            if !row.materialized
                || row.state.is_inactive()
                || row.cancelling
                || row.closing
                || row.suppress_child_entries
            {
                return false;
            }
            let Some(parent) = row.parent_id else {
                return true;
            };
            let Some(parent_row) = self.rows.get(&parent) else {
                return false;
            };
            if !parent_row.child_ids.contains(&current) {
                return false;
            }
            let parent_ready = if row.is_split_insight {
                parent_row.submitted
                    || parent_row.order_id.is_some()
                    || parent_row.pending_mutations.iter().any(|pending| {
                        pending.kind == crate::core::insight::operations::MutationKind::Submit
                    })
            } else {
                parent_row.state == crate::core::insight::types::InsightState::Filled
            };
            if !parent_ready {
                return false;
            }
            current = parent;
        }
        false
    }
}

impl BrokerCommandJournal {
    /// Additive local-index recovery for journals written before the graph index.
    /// Each transaction is bounded and idempotent. No original row is rewritten.
    pub(crate) async fn ensure_execution_graph_index(&mut self) -> Result<(), BrokerCommandError> {
        let mut state = self.connection.query("SELECT graph_version,graph_cursor,graph_complete FROM broker_execution_recovery_meta WHERE singleton=1", ()).await.map_err(journal_error)?;
        let state_row = state
            .next()
            .await
            .map_err(journal_error)?
            .ok_or_else(|| journal_error("missing recovery index version"))?;
        if state_row.get::<i64>(0).map_err(journal_error)? != 1 {
            return Err(journal_error("unsupported recovery graph index version"));
        }
        if state_row.get::<i64>(2).map_err(journal_error)? == 1 {
            return Ok(());
        }
        let mut cursor: String = state_row.get(1).map_err(journal_error)?;
        drop(state);
        let mut indexed = 0;
        loop {
            let mut query = self.connection.query(
                "SELECT c.insight_id,c.revision,c.checkpoint_json FROM broker_insight_checkpoints c
                 LEFT JOIN broker_execution_graph g ON c.insight_id=g.insight_id
                 WHERE c.insight_id>?1 AND (g.insight_id IS NULL OR g.revision<>c.revision)
                 ORDER BY c.insight_id LIMIT 32", params![cursor.clone()],
            ).await.map_err(journal_error)?;
            let mut metadata = Vec::new();
            let mut bytes = 0;
            while let Some(row) = query.next().await.map_err(journal_error)? {
                let json: String = row.get(2).map_err(journal_error)?;
                if json.len() > checkpoints::MAX_CHECKPOINT_BYTES {
                    return Err(journal_error(
                        "oversized checkpoint during graph index recovery",
                    ));
                }
                if bytes + json.len() > checkpoints::MAX_CHECKPOINT_BYTES {
                    break;
                }
                bytes += json.len();
                let checkpoint: InsightExecutionCheckpoint =
                    serde_json::from_str(&json).map_err(journal_error)?;
                checkpoint.validate()?;
                if checkpoint.insight_id.to_string()
                    != row.get::<String>(0).map_err(journal_error)?
                    || checkpoint.revision as i64 != row.get::<i64>(1).map_err(journal_error)?
                {
                    return Err(journal_error(
                        "checkpoint identity/revision mismatch during graph index recovery",
                    ));
                }
                metadata.push(InsightRecoveryMetadata::from_row(&checkpoint));
            }
            drop(query);
            if metadata.is_empty() {
                // All subsequent writes maintain rows and graph atomically. A
                // completed additive upgrade must not scan retired rows again
                // on every restart, and an interrupted upgrade resumes its key.
                self.connection.execute("UPDATE broker_execution_recovery_meta SET graph_complete=1 WHERE singleton=1", ()).await.map_err(journal_error)?;
                return Ok(());
            }
            cursor = metadata.last().expect("nonempty metadata").id.to_string();
            indexed += metadata.len();
            let tx = self
                .connection
                .transaction_with_behavior(turso::transaction::TransactionBehavior::Immediate)
                .await
                .map_err(journal_error)?;
            for row in metadata {
                tx.execute("INSERT INTO broker_execution_graph(insight_id,revision,recovery_seed,metadata_json) VALUES(?1,?2,?3,?4)
                    ON CONFLICT(insight_id) DO UPDATE SET revision=excluded.revision,recovery_seed=excluded.recovery_seed,metadata_json=excluded.metadata_json",
                    params![row.id.to_string(), row.revision as i64, i64::from(row.recovery_seed), serde_json::to_string(&row).map_err(journal_error)?]).await.map_err(journal_error)?;
            }
            tx.execute(
                "UPDATE broker_execution_recovery_meta SET graph_cursor=?1 WHERE singleton=1",
                params![cursor.clone()],
            )
            .await
            .map_err(journal_error)?;
            tx.commit().await.map_err(journal_error)?;
            log::info!(
                "Execution recovery graph index: indexed_rows={indexed} batch_bytes={bytes}"
            );
            tokio::task::yield_now().await;
        }
    }

    async fn recovery_metadata_query(
        &self,
        sql: &str,
        params: Vec<String>,
    ) -> Result<Vec<InsightRecoveryMetadata>, BrokerCommandError> {
        let mut query = self
            .connection
            .query(sql, params)
            .await
            .map_err(journal_error)?;
        let mut result = Vec::new();
        let mut bytes = 0;
        while let Some(row) = query.next().await.map_err(journal_error)? {
            let json: String = row.get(2).map_err(journal_error)?;
            if json.len() > checkpoints::MAX_CHECKPOINT_BYTES {
                return Err(journal_error("oversized execution graph metadata"));
            }
            if bytes + json.len() > checkpoints::MAX_CHECKPOINT_BYTES {
                break;
            }
            let metadata: InsightRecoveryMetadata =
                serde_json::from_str(&json).map_err(journal_error)?;
            if metadata.id.to_string() != row.get::<String>(0).map_err(journal_error)?
                || metadata.revision as i64 != row.get::<i64>(1).map_err(journal_error)?
            {
                return Err(journal_error(
                    "execution graph metadata identity/revision mismatch",
                ));
            }
            bytes += json.len();
            result.push(metadata);
        }
        Ok(result)
    }

    pub(crate) async fn load_execution_recovery(
        &mut self,
        max_pending: usize,
    ) -> Result<ExecutionRecovery, BrokerCommandError> {
        let started = std::time::Instant::now();
        self.verify_recovery_ordering().await?;
        self.ensure_execution_graph_index().await?;
        let mut commands = Vec::new();
        let mut after = 0;
        loop {
            let page = self.ordered_recovery_page(after, 256).await?;
            if page.is_empty() {
                break;
            }
            if commands.len() + page.len() > max_pending {
                return Err(journal_error(
                    "pending recovery exceeds owner capacity; increase capacity before recovery",
                ));
            }
            after = page.last().expect("nonempty page").admission_order;
            commands.extend(page);
            log::debug!("Execution recovery commands: loaded={}", commands.len());
            tokio::task::yield_now().await;
        }
        let mut wanted = BTreeSet::new();
        let mut metadata = HashMap::new();
        let mut cursor = String::new();
        loop {
            let page = self
                .recovery_metadata_query(
                    "SELECT insight_id,revision,metadata_json FROM broker_execution_graph
                 WHERE recovery_seed=1 AND insight_id>?1 ORDER BY insight_id LIMIT 32",
                    vec![cursor],
                )
                .await?;
            if page.is_empty() {
                break;
            }
            cursor = page.last().expect("nonempty page").id.to_string();
            for row in page {
                wanted.insert(row.id);
            }
            tokio::task::yield_now().await;
        }
        for command in &commands {
            if command.record.command.strategy_id != self.strategy_id
                || command.record.command.account_id != self.account_id
            {
                return Err(journal_error(
                    "recovered command belongs to another strategy/account",
                ));
            }
            wanted.insert(
                Uuid::parse_str(&command.record.command.insight_id).map_err(journal_error)?,
            );
        }
        let mut missing = BTreeSet::new();
        while !wanted.is_empty() {
            let ids = wanted.iter().take(32).copied().collect::<Vec<_>>();
            let sql = format!(
                "SELECT insight_id,revision,metadata_json FROM broker_execution_graph WHERE insight_id IN ({}) ORDER BY insight_id",
                (1..=ids.len())
                    .map(|index| format!("?{index}"))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let page = self
                .recovery_metadata_query(&sql, ids.iter().map(Uuid::to_string).collect())
                .await?;
            // When a byte boundary ends this page, leave higher IDs queued.
            let end = page.last().map(|row| row.id);
            let byte_limited = page.len() < ids.len() && end.is_some();
            let returned = page.iter().map(|row| row.id).collect::<BTreeSet<_>>();
            for id in ids {
                if returned.contains(&id) {
                    wanted.remove(&id);
                } else if !byte_limited || end.is_some_and(|last| id < last) {
                    wanted.remove(&id);
                    missing.insert(id);
                }
            }
            for row in page {
                let links = row
                    .children
                    .iter()
                    .copied()
                    .chain(row.parent)
                    .chain(row.dependencies.iter().copied());
                for id in links {
                    if !metadata.contains_key(&id) && !missing.contains(&id) {
                        wanted.insert(id);
                    }
                }
                wanted.remove(&row.id);
                metadata.insert(row.id, row);
            }
            tokio::task::yield_now().await;
        }
        // A missing legacy ancestor can still require closing an orphan. Missing
        // child templates or conflicting direct parents cannot safely resume.
        for row in metadata.values() {
            if row
                .dependencies
                .iter()
                .any(|owner| !metadata.contains_key(owner))
            {
                return Err(journal_error(
                    "saved execution recovery dependency is missing",
                ));
            }
            for child in &row.children {
                if metadata
                    .get(child)
                    .is_none_or(|child| child.parent != Some(row.id))
                {
                    return Err(journal_error(
                        "saved child graph is incomplete or has conflicting ownership",
                    ));
                }
            }
            let mut ancestors = BTreeSet::new();
            let mut current = Some(row.id);
            while let Some(id) = current {
                if !ancestors.insert(id) || ancestors.len() > 64 {
                    return Err(journal_error(
                        "cyclic or excessive-depth saved insight graph",
                    ));
                }
                current = metadata.get(&id).and_then(|row| row.parent);
            }
        }
        let mut rows = HashMap::new();
        let mut ids = metadata.keys().copied().collect::<BTreeSet<_>>();
        // Read only the selected connected graph through exact primary keys.
        // Each request is byte bounded, so large histories never create one
        // unbounded all-history allocation before ownership has been checked.
        while !ids.is_empty() {
            let requested = ids.iter().take(32).copied().collect::<Vec<_>>();
            let sql = format!(
                "SELECT insight_id,checkpoint_json FROM broker_insight_checkpoints WHERE insight_id IN ({}) ORDER BY insight_id",
                (1..=requested.len())
                    .map(|index| format!("?{index}"))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let mut query = self
                .connection
                .query(
                    sql,
                    requested.iter().map(Uuid::to_string).collect::<Vec<_>>(),
                )
                .await
                .map_err(journal_error)?;
            let mut bytes = 0;
            let mut count = 0;
            while let Some(value) = query.next().await.map_err(journal_error)? {
                let id = Uuid::parse_str(&value.get::<String>(0).map_err(journal_error)?)
                    .map_err(journal_error)?;
                let json: String = value.get(1).map_err(journal_error)?;
                if json.len() > checkpoints::MAX_CHECKPOINT_BYTES {
                    return Err(journal_error("oversized selected execution checkpoint"));
                }
                if bytes + json.len() > checkpoints::MAX_CHECKPOINT_BYTES {
                    break;
                }
                if requested.get(count) != Some(&id) {
                    return Err(journal_error("missing selected execution checkpoint"));
                }
                bytes += json.len();
                count += 1;
                let row: InsightExecutionCheckpoint =
                    serde_json::from_str(&json).map_err(journal_error)?;
                row.validate()?;
                if InsightRecoveryMetadata::from_row(&row) != metadata[&id] {
                    return Err(journal_error("selected checkpoint changed during recovery"));
                }
                rows.insert(id, row);
                ids.remove(&id);
            }
            if count == 0 {
                return Err(journal_error("missing selected execution checkpoint page"));
            }
            drop(query);
            log::info!(
                "Execution recovery checkpoints: loaded_rows={} remaining_rows={} batch_rows={count} batch_bytes={bytes}",
                rows.len(),
                ids.len()
            );
            tokio::task::yield_now().await;
        }
        let mut receipts = HashMap::new();
        for command in &commands {
            let receipt = command.record.command.receipt();
            let id = Uuid::parse_str(&receipt.insight_id).map_err(journal_error)?;
            let owner = rows
                .get(&id)
                .ok_or_else(|| journal_error("pending command owner checkpoint is missing"))?;
            if !owner.materialized || !owner.retains(&receipt) {
                return Err(journal_error(
                    "pending command is not retained by its materialized owner",
                ));
            }
            receipts.insert(receipt.id.clone(), receipt);
        }
        for row in rows.values() {
            for receipt in row
                .owner_receipts
                .iter()
                .chain(row.pending_mutations.iter().map(|pending| &pending.receipt))
            {
                if receipts.get(&receipt.id) != Some(receipt) {
                    return Err(journal_error(
                        "saved owner receipt is missing or conflicts with pending command",
                    ));
                }
            }
        }
        log::info!(
            "Execution recovery loaded rows={} pending_commands={} elapsed_ms={}",
            rows.len(),
            commands.len(),
            started.elapsed().as_millis()
        );
        Ok(ExecutionRecovery {
            rows,
            commands,
            stopping: self.execution_lifecycle().phase
                == super::lifecycle::ExecutionPhase::Stopping,
        })
    }
}

#[cfg(test)]
mod tests;
