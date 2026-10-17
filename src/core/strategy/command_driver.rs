//! Strategy-owned command receipts and bounded completion delivery.
//!
//! This owner performs no broker or database I/O in `enqueue`, `progress`, or
//! `drain_ready`. It is deliberately separate from the transport task: only the
//! strategy reducer is allowed to apply the returned evidence to Insights.
use crate::core::broker::{
    command_journal::{
        BrokerCommandJournal, ExecutionCheckpointBatch, ExecutionCheckpointPage, ExecutionIntent,
        JournalCommandRecord,
    },
    command_worker::{
        BrokerCommandSender, BrokerCommandWorker, BrokerCommandWorkerConfig,
        BrokerCommandWorkerEvent, BrokerJournalReply,
    },
    commands::{
        BrokerCommand, BrokerCommandAction, BrokerCommandCompletion, BrokerCommandError,
        BrokerCommandId, BrokerCommandOutcome, BrokerCommandReceipt, BrokerOperationGroup,
    },
    traits::OrderManagementProvider,
};
use std::{
    collections::{HashMap, VecDeque},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Clone, Debug)]
pub enum BrokerCommandProgress {
    Queued,
    Completed(Arc<BrokerCommandCompletion>),
    /// Admission succeeded but journal preparation refused this intent. This is
    /// not broker evidence and must not transition a Filled insight to Rejected.
    Failed(BrokerCommandError),
}

struct PendingCommand {
    owner_id: uuid::Uuid,
    command: Arc<BrokerCommand>,
    progress: BrokerCommandProgress,
    reduced: bool,
    applying: bool,
    queued_at_us: i64,
    admission_order: u64,
    durable: bool,
    dispatch_queued: bool,
    recovery_replay: bool,
}

struct PersistingExecution {
    applied: VecDeque<BrokerCommandId>,
    prepared: Vec<BrokerCommandId>,
    rows: Vec<(uuid::Uuid, SavedExecutionRow)>,
    reply: Option<BrokerJournalReply<()>>,
}

struct SavedExecutionRow {
    revision: u64,
    fingerprint: u64,
    parent: Option<uuid::Uuid>,
    children: rustc_hash::FxHashSet<uuid::Uuid>,
    materialized: bool,
    pipeline: Option<super::InsightPipelineCheckpoint>,
    entry_deadline: Option<chrono::DateTime<chrono::Utc>>,
}

impl SavedExecutionRow {
    fn from_checkpoint(
        row: &crate::core::insight::execution_checkpoint::InsightExecutionCheckpoint,
    ) -> Self {
        Self {
            revision: row.revision,
            fingerprint: row.source_fingerprint,
            parent: row.parent_id,
            children: row.child_ids.iter().copied().collect(),
            materialized: row.materialized,
            pipeline: row.pipeline.clone(),
            entry_deadline: row.period_unfilled.map(|periods| {
                row.timeframe
                    .add_time_increment(row.created_at, i64::from(periods))
                    .unwrap_or(row.created_at)
            }),
        }
    }
}

#[derive(Clone, Debug)]
pub struct BrokerCommandEvidence {
    pub command: Arc<BrokerCommand>,
    pub progress: BrokerCommandProgress,
}

/// Exactly one owner belongs to a strategy/account execution context. Completed
/// receipts count against capacity until durably acknowledged; a slow reducer
/// cannot turn a bounded transport queue into an unbounded completion cache.
pub struct StrategyCommandDriver {
    worker: BrokerCommandWorker,
    pending: HashMap<BrokerCommandId, PendingCommand>,
    persisting: Option<PersistingExecution>,
    by_insight: HashMap<uuid::Uuid, Vec<BrokerCommandId>>,
    entry_admission: VecDeque<BrokerCommandId>,
    control_admission: VecDeque<BrokerCommandId>,
    saved_rows: HashMap<uuid::Uuid, SavedExecutionRow>,
    max_pending: usize,
    ordinary_pending: usize,
    max_ordinary_pending: usize,
    awaiting_completion: usize,
    progress_revision: u64,
    last_admission_order: u64,
    restoring: bool,
}

impl StrategyCommandDriver {
    /// Raw low-level fixtures deliberately exercise incomplete legacy journals.
    /// Production ownership must install the recovered graph through StrategyState.
    #[cfg(test)]
    pub(crate) async fn start<E: OrderManagementProvider + 'static>(
        broker: Arc<E>,
        path: impl AsRef<Path>,
        strategy_id: &str,
        account_id: &str,
        config: BrokerCommandWorkerConfig,
    ) -> Result<Self, BrokerCommandError> {
        let journal = BrokerCommandJournal::open(path, strategy_id, account_id).await?;
        let last_admission_order = journal.last_admission_order().await?;
        Self::from_journal(broker, journal, config, last_admission_order)
    }

    fn from_journal<E: OrderManagementProvider + 'static>(
        broker: Arc<E>,
        journal: BrokerCommandJournal,
        config: BrokerCommandWorkerConfig,
        last_admission_order: u64,
    ) -> Result<Self, BrokerCommandError> {
        let max_pending = config
            .entry_capacity
            .saturating_add(config.control_capacity)
            .saturating_add(config.completion_capacity);
        let max_ordinary_pending = config
            .entry_capacity
            .saturating_add(config.completion_capacity);
        let worker = BrokerCommandWorker::start(broker, journal, config)?;
        Ok(Self {
            worker,
            pending: HashMap::new(),
            persisting: None,
            by_insight: HashMap::new(),
            entry_admission: VecDeque::new(),
            control_admission: VecDeque::new(),
            saved_rows: HashMap::new(),
            max_pending,
            ordinary_pending: 0,
            max_ordinary_pending,
            awaiting_completion: 0,
            progress_revision: 0,
            last_admission_order,
            restoring: false,
        })
    }

    pub(crate) async fn start_recovering<E: OrderManagementProvider + 'static>(
        broker: Arc<E>,
        path: impl AsRef<Path>,
        strategy_id: &str,
        account_id: &str,
        config: BrokerCommandWorkerConfig,
    ) -> Result<
        (
            Self,
            crate::core::broker::command_journal::recovery::ExecutionRecovery,
        ),
        BrokerCommandError,
    > {
        let mut journal = BrokerCommandJournal::open(path, strategy_id, account_id).await?;
        journal.begin_execution_epoch().await?;
        let capacity = config
            .entry_capacity
            .saturating_add(config.control_capacity)
            .saturating_add(config.completion_capacity);
        let recovery = journal.load_execution_recovery(capacity).await?;
        let order = journal.last_admission_order().await?;
        let mut driver = Self::from_journal(broker, journal, config, order)?;
        driver.restoring = true;
        for row in recovery.rows.values() {
            driver
                .saved_rows
                .insert(row.insight_id, SavedExecutionRow::from_checkpoint(row));
        }
        for command in &recovery.commands {
            let record = &command.record;
            let owner_id = uuid::Uuid::parse_str(&record.command.insight_id)
                .map_err(|error| BrokerCommandError::Journal(error.to_string()))?;
            let id = record.command.id.clone();
            let cancel_entry =
                record.command.action.is_entry() && !recovery.can_dispatch_entry(owner_id);
            if cancel_entry {
                driver.worker.sender.cancel_pending_entry(id.clone());
            }
            if record.command.action.is_teardown() {
                driver.control_admission.push_back(id.clone());
            } else {
                driver.ordinary_pending += 1;
                driver.entry_admission.push_back(id.clone());
            }
            driver
                .by_insight
                .entry(owner_id)
                .or_default()
                .push(id.clone());
            // Every recovered command is delivered through the existing worker
            // and reducer. Completed/Dispatching rows replay evidence only; the
            // worker cannot perform another broker mutation for those states.
            driver.pending.insert(
                id.clone(),
                PendingCommand {
                    owner_id,
                    command: Arc::new(record.command.clone()),
                    progress: BrokerCommandProgress::Queued,
                    reduced: false,
                    applying: false,
                    queued_at_us: record.queued_at_us,
                    admission_order: command.admission_order,
                    durable: true,
                    dispatch_queued: false,
                    recovery_replay: cancel_entry
                        || record.state
                            != crate::core::broker::command_journal::JournalCommandState::Queued,
                },
            );
            driver.awaiting_completion += 1;
        }
        Ok((driver, recovery))
    }

    pub(super) fn finish_recovery(&mut self) {
        self.restoring = false;
        self.worker.ready.notify_one();
    }

    pub fn sender(&self) -> BrokerCommandSender {
        self.worker.sender.clone()
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Includes staged intents and journal commits, not just events already in
    /// the delivery queue. Simulated time must not advance across this boundary.
    pub(super) fn has_in_flight_work(&self) -> bool {
        self.awaiting_completion != 0
            || self.persisting.is_some()
            || self.worker.sender.execution_stop_pending()
    }

    pub(super) fn progress_revision(&self) -> u64 {
        self.progress_revision
    }

    pub(super) fn has_admission_capacity(&self) -> bool {
        self.pending.len() < self.max_pending && self.ordinary_pending < self.max_ordinary_pending
    }

    pub(super) fn has_external_execution_work(&self) -> bool {
        self.persisting.is_some()
            || self.worker.sender.execution_stop_pending()
            || self.pending.values().any(|pending| {
                pending.dispatch_queued && matches!(pending.progress, BrokerCommandProgress::Queued)
            })
    }

    pub(super) fn operation_group_ready(&self, group: &BrokerOperationGroup) -> bool {
        let mut found = false;
        for pending in self
            .pending
            .values()
            .filter(|pending| pending.command.group.as_ref() == Some(group))
        {
            found = true;
            if matches!(pending.progress, BrokerCommandProgress::Queued) {
                return false;
            }
        }
        found
    }

    pub(super) fn unresolved_execution_error(&self) -> Option<BrokerCommandError> {
        self.execution_failure(true)
    }

    pub(super) fn non_reconcilable_execution_error(&self) -> Option<BrokerCommandError> {
        self.execution_failure(false)
    }

    fn execution_failure(&self, include_ambiguous: bool) -> Option<BrokerCommandError> {
        if let Some(error) = self.worker.sender.execution_stop_error() {
            return Some(error);
        }
        self.pending
            .values()
            .find_map(|pending| match &pending.progress {
                BrokerCommandProgress::Failed(error) => Some(error.clone()),
                BrokerCommandProgress::Completed(completion)
                    if include_ambiguous
                        && !pending.reduced
                        && matches!(completion.outcome, BrokerCommandOutcome::Ambiguous { .. }) =>
                {
                    Some(BrokerCommandError::Invalid(format!(
                        "command {} has an ambiguous outcome; broker reconciliation is required",
                        pending.command.id.0
                    )))
                }
                _ => None,
            })
    }

    pub fn is_worker_finished(&self) -> bool {
        self.worker.task.is_finished()
    }

    pub fn has_ready_events(&self) -> bool {
        self.has_ready_events_with_admission(true)
    }

    pub(super) fn has_ready_events_with_admission(&self, allow_entries: bool) -> bool {
        self.worker.sender.execution_stop_error().is_some()
            || !self.worker.events.is_empty()
            || self.next_dispatch_id(allow_entries).is_some()
    }
    pub(crate) fn has_completion_events(&self) -> bool {
        !self.worker.events.is_empty()
    }

    pub fn completion_notifier(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.worker.ready)
    }

    pub fn stop_entries(&self) {
        self.worker.sender.stop_entries();
    }

    pub(super) fn install_snapshot_guard(
        &self,
        cache: Arc<crate::core::broker::snapshots::BrokerSnapshotCache>,
    ) -> Result<(), BrokerCommandError> {
        self.worker.sender.install_snapshot_guard(cache)
    }

    pub(super) fn request_execution_stop(&self) {
        self.worker.sender.request_execution_stop();
    }

    /// After the owner has drained cleanup and checkpoint receipts, ask the
    /// journal to prove the epoch settled before acknowledging stop completion.
    pub(super) async fn complete_execution_stop(&self) -> Result<bool, BrokerCommandError> {
        if let Some(error) = self.unresolved_execution_error() {
            return Err(error);
        }
        // The journal cannot see intents still staged in the owner, or a
        // checkpoint currently awaiting its durable acknowledgement.
        if !self.pending.is_empty() || self.has_in_flight_work() || self.restoring {
            return Ok(false);
        }
        self.worker
            .sender
            .try_complete_execution_stop()?
            .wait()
            .await
    }

    pub fn cancel_pending_entry(
        &self,
        receipt: &BrokerCommandReceipt,
    ) -> Result<(), BrokerCommandError> {
        let pending = self
            .pending
            .get(&receipt.id)
            .filter(|pending| {
                pending.command.receipt() == *receipt && pending.command.action.is_entry()
            })
            .ok_or(BrokerCommandError::IdentityConflict)?;
        self.worker
            .sender
            .cancel_pending_entry(pending.command.id.clone());
        Ok(())
    }

    pub fn enqueue(
        &mut self,
        insight_id: &str,
        action: BrokerCommandAction,
        group: Option<BrokerOperationGroup>,
    ) -> Result<BrokerCommandReceipt, BrokerCommandError> {
        self.enqueue_with_constraints(
            insight_id,
            action,
            group,
            crate::core::broker::commands::BrokerDispatchConstraints::default(),
        )
    }

    pub fn enqueue_with_constraints(
        &mut self,
        insight_id: &str,
        action: BrokerCommandAction,
        group: Option<BrokerOperationGroup>,
        constraints: crate::core::broker::commands::BrokerDispatchConstraints,
    ) -> Result<BrokerCommandReceipt, BrokerCommandError> {
        let (strategy_id, account_id) = self.worker.sender.scope();
        let command = BrokerCommand::new_with_constraints(
            strategy_id,
            account_id,
            insight_id,
            action,
            group,
            constraints,
        );
        self.enqueue_command(command)
    }

    /// Retried admission with the same immutable command returns its original
    /// receipt. Reusing an ID for changed content is always an error.
    pub fn enqueue_command(
        &mut self,
        command: BrokerCommand,
    ) -> Result<BrokerCommandReceipt, BrokerCommandError> {
        if self.restoring {
            return Err(BrokerCommandError::Invalid(
                "execution recovery is not installed".into(),
            ));
        }
        command.encode()?;
        let owner_id = uuid::Uuid::parse_str(&command.insight_id).map_err(|_| {
            BrokerCommandError::Invalid("command owner is not an Insight UUID".into())
        })?;
        let (strategy, account) = self.worker.sender.scope();
        if command.strategy_id != strategy || command.account_id != account {
            return Err(BrokerCommandError::Invalid(
                "command belongs to another driver scope".into(),
            ));
        }
        if let Some(existing) = self.pending.get(&command.id) {
            return if existing.command.as_ref() == &command {
                Ok(existing.command.receipt())
            } else {
                Err(BrokerCommandError::IdentityConflict)
            };
        }
        if self.pending.len() >= self.max_pending
            || (!command.action.is_teardown() && self.ordinary_pending >= self.max_ordinary_pending)
        {
            return Err(BrokerCommandError::QueueFull);
        }
        if self.worker.sender.is_closed() {
            return Err(BrokerCommandError::Closed);
        }
        if command.action.is_entry() && self.worker.sender.entries_stopped() {
            return Err(BrokerCommandError::Stopping);
        }
        let receipt = command.receipt();
        let admission_order = self
            .last_admission_order
            .checked_add(1)
            .filter(|order| *order <= i64::MAX as u64)
            .ok_or_else(|| BrokerCommandError::Invalid("owner admission order exhausted".into()))?;
        self.last_admission_order = admission_order;
        if !command.action.is_teardown() {
            self.ordinary_pending += 1;
        }
        self.by_insight
            .entry(owner_id)
            .or_default()
            .push(command.id.clone());
        if command.action.is_teardown() {
            self.control_admission.push_back(command.id.clone());
        } else {
            self.entry_admission.push_back(command.id.clone());
        }
        self.pending.insert(
            command.id.clone(),
            PendingCommand {
                owner_id,
                command: Arc::new(command),
                progress: BrokerCommandProgress::Queued,
                reduced: false,
                applying: false,
                queued_at_us: chrono::Utc::now().timestamp_micros(),
                admission_order,
                durable: false,
                dispatch_queued: false,
                recovery_replay: false,
            },
        );
        self.awaiting_completion += 1;
        Ok(receipt)
    }

    pub fn progress(&self, receipt: &BrokerCommandReceipt) -> Option<BrokerCommandProgress> {
        self.pending
            .get(&receipt.id)
            .filter(|pending| pending.command.receipt() == *receipt)
            .map(|pending| pending.progress.clone())
    }

    /// Transfer at most one bounded turn of evidence. No acknowledgement is
    /// inferred here: the caller must first reduce and durably save its state.
    pub fn drain_ready(
        &mut self,
        max_events: usize,
        budget: Duration,
    ) -> Result<Vec<BrokerCommandEvidence>, BrokerCommandError> {
        if max_events != 0 {
            self.poll_acknowledgements(max_events)?;
            self.dispatch_prepared(max_events)?;
        }
        let started = Instant::now();
        let limit = max_events.min(256);
        let mut ready = Vec::with_capacity(limit.min(self.worker.events.len()));
        for index in 0..limit {
            if index != 0 && started.elapsed() >= budget {
                break;
            }
            match self.next_ready() {
                Ok(Some(evidence)) => ready.push(evidence),
                Ok(None) => break,
                Err(BrokerCommandError::Closed) if !ready.is_empty() => break,
                Err(error) => return Err(error),
            }
        }
        Ok(ready)
    }

    /// One dequeue boundary for the strategy's combined reducer budget, without
    /// a temporary batch vector. Order/request payloads are shared, not cloned.
    fn next_ready(&mut self) -> Result<Option<BrokerCommandEvidence>, BrokerCommandError> {
        if let Some(error) = self.worker.sender.execution_stop_error() {
            return Err(error);
        }
        let event = match self.worker.events.try_recv() {
            Ok(event) => event,
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => return Ok(None),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                self.request_execution_stop();
                // Retain receipts: owner failure is not proof of non-execution.
                return Err(BrokerCommandError::Closed);
            }
        };
        let (receipt, progress) = match event {
            BrokerCommandWorkerEvent::Completion(completion) => (
                completion.receipt.clone(),
                BrokerCommandProgress::Completed(Arc::new(completion)),
            ),
            BrokerCommandWorkerEvent::Failed { receipt, error } => {
                (receipt, BrokerCommandProgress::Failed(error))
            }
        };
        let Some(pending) = self.pending.get_mut(&receipt.id) else {
            self.request_execution_stop();
            return Err(BrokerCommandError::IdentityConflict);
        };
        if pending.command.receipt() != receipt {
            self.request_execution_stop();
            return Err(BrokerCommandError::IdentityConflict);
        }
        if matches!(pending.progress, BrokerCommandProgress::Queued) {
            self.awaiting_completion -= 1;
        }
        pending.progress = progress.clone();
        self.progress_revision = self.progress_revision.wrapping_add(1);
        Ok(Some(BrokerCommandEvidence {
            command: Arc::clone(&pending.command),
            progress,
        }))
    }

    pub fn mark_reduced(
        &mut self,
        receipt: &BrokerCommandReceipt,
    ) -> Result<(), BrokerCommandError> {
        let pending = self
            .pending
            .get_mut(&receipt.id)
            .filter(|pending| pending.command.receipt() == *receipt)
            .ok_or(BrokerCommandError::IdentityConflict)?;
        if !matches!(pending.progress, BrokerCommandProgress::Completed(_)) {
            return Err(BrokerCommandError::Invalid(
                "command has no completion to reduce".into(),
            ));
        }
        pending.reduced = true;
        Ok(())
    }

    pub(crate) fn checkpoint_revision(&self, id: &uuid::Uuid) -> u64 {
        self.saved_rows.get(id).map_or(0, |row| row.revision)
    }

    pub(crate) fn checkpoint_matches(&self, id: &uuid::Uuid, fingerprint: u64) -> bool {
        self.saved_rows
            .get(id)
            .is_some_and(|row| row.fingerprint == fingerprint)
    }

    pub(crate) fn checkpoint_in_flight(&self) -> bool {
        self.persisting.is_some()
    }

    pub(crate) fn checkpoint_execution_matches(
        &self,
        id: &uuid::Uuid,
        fingerprint: u64,
        materialized: bool,
        pipeline: Option<&super::InsightPipelineCheckpoint>,
    ) -> bool {
        self.saved_rows.get(id).is_some_and(|row| {
            row.fingerprint == fingerprint
                && row.materialized == materialized
                && row.pipeline.as_ref() == pipeline
        })
    }

    /// Conservative encoded budget, including driver receipts and new intents.
    /// Count through serde's writer without allocating a second serialized row.
    pub(crate) fn checkpoint_byte_budget(
        &self,
        row: &crate::core::insight::execution_checkpoint::InsightExecutionCheckpoint,
    ) -> Result<usize, BrokerCommandError> {
        struct Count(usize);
        impl std::io::Write for Count {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0 = self.0.saturating_add(bytes.len());
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut count = Count(64);
        serde_json::to_writer(&mut count, row)
            .map_err(|error| BrokerCommandError::Journal(error.to_string()))?;
        serde_json::to_writer(
            &mut count,
            &crate::core::broker::command_journal::recovery::InsightRecoveryMetadata::from_row(row),
        )
        .map_err(|error| BrokerCommandError::Journal(error.to_string()))?;
        for id in self.by_insight.get(&row.insight_id).into_iter().flatten() {
            let pending = self.pending.get(id).expect("indexed receipt remains owned");
            let mut receipt = Count(64);
            serde_json::to_writer(&mut receipt, &pending.command.receipt())
                .map_err(|error| BrokerCommandError::Journal(error.to_string()))?;
            count.0 = count.0.saturating_add(receipt.0.saturating_mul(2));
            if !pending.durable {
                serde_json::to_writer(&mut count, pending.command.as_ref())
                    .map_err(|error| BrokerCommandError::Journal(error.to_string()))?;
            }
        }
        Ok(count.0)
    }

    pub(crate) fn has_unpersisted_intents(&self, id: &uuid::Uuid) -> bool {
        self.by_insight.get(id).is_some_and(|ids| {
            ids.iter()
                .any(|id| self.pending.get(id).is_some_and(|pending| !pending.durable))
        })
    }

    pub(crate) fn pending_owner_ids(&self) -> Vec<uuid::Uuid> {
        self.by_insight
            .iter()
            .filter(|(_, ids)| {
                ids.iter().any(|id| {
                    self.pending.get(id).is_some_and(|pending|
            !pending.durable || (pending.reduced && !pending.applying &&
                matches!(&pending.progress, BrokerCommandProgress::Completed(completion)
                    if !matches!(completion.outcome, BrokerCommandOutcome::Ambiguous { .. }) || pending.reduced)))
                })
            })
            .map(|(id, _)| *id)
            .collect()
    }

    pub(crate) fn has_pending_owner(&self, id: &uuid::Uuid) -> bool {
        self.by_insight.contains_key(id)
    }

    pub(crate) fn has_consumable_receipts(&self, insight: &crate::core::insight::Insight) -> bool {
        self.by_insight
            .get(&insight.insight_id)
            .into_iter()
            .flatten()
            .filter_map(|id| self.pending.get(id))
            .any(|pending| {
                pending.reduced
                    && !pending.applying
                    && !insight
                        .pending_mutations
                        .iter()
                        .any(|mutation| mutation.receipt.id == pending.command.id)
                    && matches!(&pending.progress, BrokerCommandProgress::Completed(completion)
                    if !matches!(completion.outcome, BrokerCommandOutcome::Ambiguous { .. }) || pending.reduced)
            })
    }

    pub(crate) fn consumable_receipts(
        &self,
        row: &crate::core::insight::execution_checkpoint::InsightExecutionCheckpoint,
    ) -> Vec<BrokerCommandReceipt> {
        self.by_insight
            .get(&row.insight_id)
            .into_iter()
            .flatten()
            .filter_map(|id| self.pending.get(id))
            .filter(|pending| {
                pending.reduced
                    && !pending.applying
                    && !row.retains(&pending.command.receipt())
                    && matches!(&pending.progress, BrokerCommandProgress::Completed(completion)
                    if !matches!(completion.outcome, BrokerCommandOutcome::Ambiguous { .. }) || pending.reduced)
            })
            .map(|pending| pending.command.receipt())
            .collect()
    }

    /// New admission is only staged in memory. This is the sole boundary that
    /// grants dispatch eligibility: pending state and intents commit together.
    pub(crate) fn persist_execution_checkpoints(
        &mut self,
        mut rows: Vec<crate::core::insight::execution_checkpoint::InsightExecutionCheckpoint>,
        receipts: Vec<BrokerCommandReceipt>,
    ) -> Result<(), BrokerCommandError> {
        if self.persisting.is_some() {
            return Err(BrokerCommandError::QueueFull);
        }
        let applied = receipts
            .iter()
            .map(|receipt| receipt.id.clone())
            .collect::<std::collections::HashSet<_>>();
        for receipt in &receipts {
            let pending = self
                .pending
                .get(&receipt.id)
                .filter(|pending| pending.command.receipt() == *receipt)
                .ok_or(BrokerCommandError::IdentityConflict)?;
            if !pending.reduced || pending.applying {
                return Err(BrokerCommandError::Invalid(
                    "command has not been reduced or is already persisting".into(),
                ));
            }
            if !matches!(&pending.progress, BrokerCommandProgress::Completed(completion)
                if !matches!(completion.outcome, BrokerCommandOutcome::Ambiguous { .. }) || rows.iter().any(|row|
                    row.insight_id.to_string() == receipt.insight_id && pending.command.action.settled_by_terminal(row.order_id.as_deref(), &row.state)))
            {
                return Err(BrokerCommandError::Invalid(
                    "ambiguous command requires broker reconciliation".into(),
                ));
            }
        }
        let mut intents = Vec::new();
        for row in &mut rows {
            row.owner_receipts.clear();
            for id in self.by_insight.get(&row.insight_id).into_iter().flatten() {
                let pending = self.pending.get(id).expect("indexed command remains owned");
                if !applied.contains(id) {
                    row.owner_receipts.push(pending.command.receipt());
                }
                if !pending.durable {
                    intents.push(ExecutionIntent {
                        command: pending.command.as_ref().clone(),
                        queued_at_us: pending.queued_at_us,
                        admission_order: pending.admission_order,
                    });
                }
            }
        }
        let prepared = intents
            .iter()
            .map(|intent| intent.command.id.clone())
            .collect();
        let saved_rows = rows
            .iter()
            .map(|row| (row.insight_id, SavedExecutionRow::from_checkpoint(row)))
            .collect();
        let batch = ExecutionCheckpointBatch::with_intents(rows, receipts, intents)?;
        let ids = batch
            .applied
            .iter()
            .map(|receipt| receipt.id.clone())
            .collect::<VecDeque<_>>();
        let reply = self.worker.sender.try_persist_execution(batch)?;
        for id in &ids {
            self.pending
                .get_mut(id)
                .expect("validated receipt")
                .applying = true;
        }
        self.persisting = Some(PersistingExecution {
            applied: ids,
            prepared,
            rows: saved_rows,
            reply: Some(reply),
        });
        Ok(())
    }

    pub fn poll_acknowledgements(
        &mut self,
        max_events: usize,
    ) -> Result<usize, BrokerCommandError> {
        if max_events == 0 {
            return Ok(0);
        }
        let Some(persisting) = self.persisting.as_mut() else {
            return Ok(0);
        };
        if let Some(waiting) = persisting.reply.as_mut() {
            let Some(result) = waiting.try_recv() else {
                return Ok(0);
            };
            if let Err(error) = result {
                let persisting = self.persisting.take().expect("polled batch remains owned");
                for id in persisting.applied {
                    self.pending
                        .get_mut(&id)
                        .expect("persisting receipt remains owned")
                        .applying = false;
                }
                self.request_execution_stop();
                return Err(error);
            }
            persisting.reply = None;
            for (id, row) in persisting.rows.drain(..) {
                // A confirmed configuration update also updates entries already
                // waiting in the worker. Never use an unacknowledged checkpoint.
                for command_id in self.by_insight.get(&id).into_iter().flatten() {
                    if self
                        .pending
                        .get(command_id)
                        .is_some_and(|pending| pending.command.action.is_entry())
                    {
                        self.worker
                            .sender
                            .set_entry_deadline(command_id.clone(), row.entry_deadline);
                    }
                }
                self.saved_rows.insert(id, row);
            }
            for id in persisting.prepared.drain(..) {
                let pending = self
                    .pending
                    .get_mut(&id)
                    .expect("prepared receipt remains owned");
                pending.durable = true;
            }
        }
        let applied = persisting.applied.len().min(max_events).min(256);
        for _ in 0..applied {
            let id = persisting
                .applied
                .pop_front()
                .expect("bounded by batch length");
            let pending = self
                .pending
                .remove(&id)
                .expect("persisting receipt remains owned");
            if !pending.command.action.is_teardown() {
                self.ordinary_pending -= 1;
            }
            if let Some(ids) = self.by_insight.get_mut(&pending.owner_id) {
                ids.retain(|existing| *existing != id);
                if ids.is_empty() {
                    self.by_insight.remove(&pending.owner_id);
                }
            }
            self.worker.sender.release_entry_identity(&id);
        }
        if persisting.applied.is_empty() {
            self.persisting = None;
        }
        if applied != 0 {
            self.progress_revision = self.progress_revision.wrapping_add(1);
        }
        Ok(applied)
    }

    fn checkpoint_chain_ready(&self, pending: &PendingCommand) -> bool {
        if !pending.durable {
            return false;
        }
        if pending.recovery_replay {
            return true;
        }
        // Cleanup is broker-evidence based. A legacy/orphaned child must still
        // be cancellable/closable when its old parent graph is incomplete.
        // Its own pending state remains mandatory; this never admits an entry.
        if pending.command.action.is_teardown() {
            return self
                .saved_rows
                .get(&pending.owner_id)
                .is_some_and(|row| row.materialized);
        }
        let mut id = pending.owner_id;
        for _ in 0..64 {
            let Some(row) = self.saved_rows.get(&id) else {
                return false;
            };
            let Some(parent) = row.parent else {
                return true;
            };
            if !self
                .saved_rows
                .get(&parent)
                .is_some_and(|row| row.children.contains(&id))
            {
                return false;
            }
            id = parent;
        }
        false
    }

    fn next_dispatch_id(&self, allow_entries: bool) -> Option<&BrokerCommandId> {
        if self.restoring {
            return None;
        }
        self.control_admission
            .front()
            .filter(|id| {
                self.pending
                    .get(*id)
                    .is_some_and(|pending| self.checkpoint_chain_ready(pending))
            })
            .or_else(|| {
                self.entry_admission.front().filter(|id| {
                    self.pending.get(*id).is_some_and(|pending| {
                        self.checkpoint_chain_ready(pending)
                            && (allow_entries
                                || !pending.command.action.is_entry()
                                || pending.recovery_replay
                                || self.worker.sender.entry_is_gated(&pending.command.id))
                    })
                })
            })
    }

    pub(crate) fn dispatch_prepared(
        &mut self,
        max_events: usize,
    ) -> Result<usize, BrokerCommandError> {
        self.dispatch_prepared_with_admission(max_events, true)
    }

    pub(super) fn dispatch_prepared_with_admission(
        &mut self,
        max_events: usize,
        allow_entries: bool,
    ) -> Result<usize, BrokerCommandError> {
        let mut dispatched = 0;
        while dispatched < max_events.min(256) {
            let Some(id) = self.next_dispatch_id(allow_entries).cloned() else {
                break;
            };
            let pending = self
                .pending
                .get_mut(&id)
                .expect("ready receipt remains owned");
            if !pending.durable {
                self.request_execution_stop();
                return Err(BrokerCommandError::Journal(
                    "dispatch without checkpoint ownership".into(),
                ));
            }
            if pending.command.action.is_entry() {
                self.worker.sender.set_entry_deadline(
                    id.clone(),
                    self.saved_rows
                        .get(&pending.owner_id)
                        .and_then(|row| row.entry_deadline),
                );
            }
            match self
                .worker
                .sender
                .try_enqueue_prepared(pending.command.as_ref().clone())
            {
                Ok(_) => {
                    pending.dispatch_queued = true;
                    if pending.command.action.is_teardown() {
                        self.control_admission.pop_front();
                    } else {
                        self.entry_admission.pop_front();
                    }
                    dispatched += 1;
                }
                Err(BrokerCommandError::QueueFull) => break,
                Err(error) => {
                    self.request_execution_stop();
                    return Err(error);
                }
            }
        }
        Ok(dispatched)
    }

    pub(crate) async fn execution_checkpoint_page(
        &self,
        after: String,
        limit: usize,
    ) -> Result<ExecutionCheckpointPage, BrokerCommandError> {
        self.worker
            .sender
            .try_execution_checkpoint_page(after, limit)?
            .wait()
            .await
    }

    /// Startup/reconnect control-plane operation, never called by a synchronous
    /// pipe. Reading recovery does not re-dispatch queued or ambiguous commands.
    pub async fn recovery_page(
        &self,
        after_sequence: i64,
        limit: usize,
    ) -> Result<Vec<JournalCommandRecord>, BrokerCommandError> {
        self.worker
            .sender
            .try_recovery_page(after_sequence, limit)?
            .wait()
            .await
    }

    /// Stop dispatch and release ownership. Dropping the completion receiver
    /// preserves an in-flight result in the journal before the worker exits.
    pub async fn stop(mut self) -> Result<BrokerCommandJournal, BrokerCommandError> {
        self.stop_entries();
        self.worker.events.close();
        let mut journal = (&mut self.worker.task)
            .await
            .map_err(|error| BrokerCommandError::Journal(error.to_string()))??;
        // The worker may already have returned before a stop request arrived.
        // Once ownership transfers back, persist that request here rather than
        // depending on a notification with no remaining consumer.
        if self.worker.sender.execution_stop_pending() {
            journal.request_execution_stop().await?;
        }
        Ok(journal)
    }
}

impl Drop for StrategyCommandDriver {
    fn drop(&mut self) {
        self.stop_entries();
    }
}

impl<S, E, D> super::StrategyState<S, E, D>
where
    S: super::Strategy,
    E: crate::core::broker::traits::Broker + OrderManagementProvider + 'static,
    D: crate::core::broker::traits::DataFeed + crate::core::broker::traits::DataProvider,
{
    /// Startup-only installation. Backtest history must have been attached before
    /// this shares execution ownership. Drivers must not install a second owner.
    pub(super) async fn start_command_driver(
        &mut self,
        path: impl AsRef<Path>,
        strategy_id: &str,
        account_id: &str,
        config: BrokerCommandWorkerConfig,
    ) -> Result<(), BrokerCommandError> {
        self.start_command_driver_with_recovery_signal(path, strategy_id, account_id, config, true)
            .await
    }

    pub(super) async fn start_command_driver_with_recovery_signal(
        &mut self,
        path: impl AsRef<Path>,
        strategy_id: &str,
        account_id: &str,
        config: BrokerCommandWorkerConfig,
        signal_recovered_stop: bool,
    ) -> Result<(), BrokerCommandError> {
        if self.command_driver.is_some() {
            return Err(BrokerCommandError::Invalid(
                "strategy already owns a command driver".into(),
            ));
        }
        let (mut driver, recovery) = StrategyCommandDriver::start_recovering(
            Arc::clone(&self.broker.execution),
            path,
            strategy_id,
            account_id,
            config,
        )
        .await?;
        let recovering_stop = recovery.stopping;
        if let Err(error) = self.install_execution_recovery(recovery) {
            // A failed restoration never opened dispatch. Await worker teardown
            // so a retry cannot race the journal's exclusive process lock.
            if let Err(stop_error) = driver.stop().await {
                log::warn!("Execution recovery worker shutdown failed: {stop_error}");
            }
            return Err(error);
        }
        if self.status == super::StrategyStatus::Stopping || *self.shutdown_tx.borrow() {
            driver.request_execution_stop();
        }
        if recovering_stop {
            log::warn!(
                "Resuming an unfinished durable stop; reconciling broker cleanup before allowing a new live run"
            );
            if signal_recovered_stop {
                self.shutdown_tx.send_replace(true);
            }
            self.status = super::StrategyStatus::Stopping;
            driver.request_execution_stop();
        }
        driver.finish_recovery();
        self.insights.enable_execution_persistence();
        // Recovered owners need the same lifetime guarantee as newly admitted
        // commands. A terminal snapshot can be published before its replayed
        // completion arrives; pruning it then would strand the durable receipt.
        for id in driver.by_insight.keys() {
            self.insights.execution_pin(*id);
        }
        self.command_driver = Some(driver);
        Ok(())
    }
}

impl<S, E, D> super::StrategyState<S, E, D>
where
    S: super::Strategy,
    E: crate::core::broker::traits::Broker + OrderManagementProvider,
    D: crate::core::broker::traits::DataFeed + crate::core::broker::traits::DataProvider,
{
    /// Both evidence orders are valid: a terminal trade can precede the RPC
    /// response or follow it. In either case no active-state pipe will resume,
    /// so the owner consumes definite receipts at both reducer boundaries.
    pub(super) fn reduce_terminal_command_receipts(&mut self, id: uuid::Uuid) {
        if !self
            .insights
            .get(&id)
            .is_some_and(|insight| insight.state.is_inactive())
        {
            return;
        }
        if let Some(mut insight) = self.insights.take_for_update(&id) {
            if let Some(driver) = self.command_driver.as_mut() {
                for command_id in driver.by_insight.get(&id).into_iter().flatten() {
                    if let Some(pending) = driver.pending.get_mut(command_id) {
                        if matches!(&pending.progress, BrokerCommandProgress::Completed(completion)
                            if matches!(completion.outcome, BrokerCommandOutcome::Ambiguous { .. }))
                            && pending
                                .command
                                .action
                                .settled_by_terminal(insight.order_id.as_deref(), &insight.state)
                        {
                            pending.reduced = true;
                        }
                    }
                }
            }
            let before = insight.snapshot_fingerprint_hash();
            insight.poll_terminal_mutations(self);
            let changed = before != insight.snapshot_fingerprint_hash();
            self.insights.restore_after_update(insight, changed);
        }
    }

    /// Apply submit completions through exactly the same reducer as trade events.
    /// Other operation completions remain available to their deferred pipe. They
    /// are not fictitious fills/cancels and are not automatically acknowledged.
    pub(super) fn reduce_ready_commands(
        &mut self,
        max_events: usize,
        budget: Duration,
    ) -> Result<usize, BrokerCommandError> {
        let started = Instant::now();
        let mut count = 0;
        while count < max_events.min(256) {
            if count != 0 && started.elapsed() >= budget {
                break;
            }
            let Some(driver) = self.command_driver.as_mut() else {
                break;
            };
            let Some(evidence) = driver.next_ready()? else {
                break;
            };
            count += 1;
            if let Ok(id) = uuid::Uuid::parse_str(&evidence.command.insight_id) {
                self.insight_work.wake(id);
            }
            if let BrokerCommandProgress::Completed(completion) = evidence.progress {
                if let BrokerCommandOutcome::OrderResponse { order } = &completion.outcome {
                    let consumed = self.process_order_evidence(
                        order.clone(),
                        order.status.clone(),
                        super::OrderEvidenceSource::SubmitResponse,
                        uuid::Uuid::parse_str(&completion.receipt.insight_id).ok(),
                    );
                    // This is in-memory ownership only. Publication/checkpoint
                    // persistence and the applied marker commit atomically.
                    if let Some(driver) = self.command_driver.as_mut().filter(|_| consumed) {
                        driver.mark_reduced(&completion.receipt)?;
                    }
                }
                if evidence.command.action.is_entry() {
                    if let Ok(id) = uuid::Uuid::parse_str(&evidence.command.insight_id) {
                        if let Some(mut insight) = self.insights.take_for_update(&id) {
                            let before = insight.snapshot_fingerprint_hash();
                            // Submit belongs to the owner, not a Filled-state pipe:
                            // the streamed fill may already have changed buckets.
                            let _result = insight.poll_pending_submission(self);
                            let changed = before != insight.snapshot_fingerprint_hash();
                            self.insights.restore_after_update(insight, changed);
                            self.schedule_insight_children(id);
                        }
                    }
                }
                if let Ok(id) = uuid::Uuid::parse_str(&evidence.command.insight_id) {
                    self.reduce_terminal_command_receipts(id);
                }
            }
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests;
