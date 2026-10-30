//! Bounded per-strategy workers share a process-local mutation slot per account.
//! The strategy alone reduces
//! completions into insight state; this task owns transport and journal I/O only.
use super::{
    command_journal::{
        BrokerCommandJournal, ExecutionCheckpointBatch, ExecutionCheckpointPage,
        ExecutionFillBatch,
        JournalCommandRecord, JournalCommandState,
    },
    commands::{
        BrokerCommand, BrokerCommandAction, BrokerCommandCompletion, BrokerCommandError,
        BrokerCommandId, BrokerCommandOutcome, BrokerCommandReceipt, BrokerOrderRequest,
        SubmissionResolution,
    },
    snapshots::{BrokerSnapshotCache, SnapshotUnavailable},
    traits::OrderManagementProvider,
};
use crate::core::live_sync::CloudCaptureGate;
use std::{collections::VecDeque, future::Future, sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot};
mod account_lane;
mod host_account_lock;
mod stop;
mod analytics_capture;
use stop::StopFence;

fn check_analytics_capture(capture: &Option<CloudCaptureGate>) -> Result<(), BrokerCommandError> {
    if capture.as_ref().is_some_and(CloudCaptureGate::is_paused) {
        Err(BrokerCommandError::CapturePaused)
    } else {
        Ok(())
    }
}

fn entry_account_rejection(
    action: &BrokerCommandAction,
    account: &super::types::Account,
) -> Option<&'static str> {
    let BrokerCommandAction::Submit { request } = action else {
        return None;
    };
    if !account.equity.is_finite() || account.equity <= 0.0 {
        return Some("entry refused: account equity is not positive");
    }
    if !account.buying_power.is_finite() || account.buying_power <= 0.0 {
        return Some("entry refused: account buying power is exhausted");
    }
    if request.side == super::types::OrderSide::Sell && !account.shorting_enabled {
        return Some("entry refused: account shorting is disabled");
    }
    None
}

enum JournalRequest {
    CompleteAnalyticsCapture {
        receipt: super::command_journal::AnalyticsCaptureHandoffReceipt,
        _permit: tokio::sync::OwnedSemaphorePermit,
        reply: oneshot::Sender<Result<bool,BrokerCommandError>>,
    },
    AnalyticsCaptureStatus {
        _permit: tokio::sync::OwnedSemaphorePermit,
        reply: oneshot::Sender<Result<Option<super::command_journal::AnalyticsCaptureStatus>, BrokerCommandError>>,
    },
    AnalyticsCapturePause {
        _permit: tokio::sync::OwnedSemaphorePermit,
        reply: oneshot::Sender<Result<Option<super::command_journal::AnalyticsCapturePause>, BrokerCommandError>>,
    },
    AcknowledgeAnalyticsCaptureHandoff {
        receipt: super::command_journal::AnalyticsCaptureHandoffReceipt,
        _permit: tokio::sync::OwnedSemaphorePermit,
        reply: oneshot::Sender<Result<bool, BrokerCommandError>>,
    },
    PrepareMarkExport {
        _permit:tokio::sync::OwnedSemaphorePermit,
        reply:oneshot::Sender<Result<Option<aqe_live_protocol::PositionMarkTransfer>,BrokerCommandError>>,
    },
    AcknowledgeMarkExport {
        receipt:aqe_live_protocol::PositionMarkExportReceipt,
        _permit:tokio::sync::OwnedSemaphorePermit,
        reply:oneshot::Sender<Result<(),BrokerCommandError>>,
    },
    PositionMarkOwners {
        minute:i64,after:Option<(String,String)>,_permit:tokio::sync::OwnedSemaphorePermit,
        reply:oneshot::Sender<Result<super::command_journal::marks::PositionMarkOwners,BrokerCommandError>>,
    },
    RecordPositionMarks {
        batch:super::command_journal::marks::PositionMarkBatch,_permit:tokio::sync::OwnedSemaphorePermit,
        capture: Option<CloudCaptureGate>,
        reply:oneshot::Sender<Result<usize,BrokerCommandError>>,
    },
    FillHistoryOwners {
        after: String,
        active_only: bool,
        _permit: tokio::sync::OwnedSemaphorePermit,
        reply: oneshot::Sender<Result<super::fill_history::FillHistoryOwnerPage, BrokerCommandError>>,
    },
    PrepareFillExport {
        _permit: tokio::sync::OwnedSemaphorePermit,
        reply: oneshot::Sender<Result<Option<aqe_live_protocol::ExecutionFillTransfer>, BrokerCommandError>>,
    },
    AcknowledgeFillExport {
        receipt: aqe_live_protocol::ExecutionFillExportReceipt,
        _permit: tokio::sync::OwnedSemaphorePermit,
        reply: oneshot::Sender<Result<(), BrokerCommandError>>,
    },
    RecordFills {
        batch: ExecutionFillBatch,
        capture: Option<CloudCaptureGate>,
        _permit: tokio::sync::OwnedSemaphorePermit,
        reply: oneshot::Sender<Result<(), BrokerCommandError>>,
    },
    FillPage {
        after: u64,
        through: Option<u64>,
        limit: usize,
        _permit: tokio::sync::OwnedSemaphorePermit,
        reply: oneshot::Sender<Result<aqe_live_protocol::ExecutionFillPage, BrokerCommandError>>,
    },
    CompleteStop {
        reply: oneshot::Sender<Result<bool, BrokerCommandError>>,
    },
    RecoveryPage {
        after_sequence: i64,
        limit: usize,
        reply: oneshot::Sender<Result<Vec<JournalCommandRecord>, BrokerCommandError>>,
    },
    Checkpoint {
        batch: ExecutionCheckpointBatch,
        _permit: tokio::sync::OwnedSemaphorePermit,
        reply: oneshot::Sender<Result<(), BrokerCommandError>>,
    },
    CheckpointPage {
        after: String,
        limit: usize,
        reply: oneshot::Sender<Result<ExecutionCheckpointPage, BrokerCommandError>>,
    },
}

/// A bounded, non-blocking request to the journal owner. Await it only outside
/// the strategy's synchronous decision/reducer turn.
pub struct BrokerJournalReply<T>(oneshot::Receiver<Result<T, BrokerCommandError>>);

impl<T> BrokerJournalReply<T> {
    pub async fn wait(self) -> Result<T, BrokerCommandError> {
        self.0.await.map_err(|_| BrokerCommandError::Closed)?
    }

    pub fn try_recv(&mut self) -> Option<Result<T, BrokerCommandError>> {
        match self.0.try_recv() {
            Ok(result) => Some(result),
            Err(oneshot::error::TryRecvError::Empty) => None,
            Err(oneshot::error::TryRecvError::Closed) => Some(Err(BrokerCommandError::Closed)),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct BrokerCommandWorkerConfig {
    pub entry_capacity: usize,
    pub control_capacity: usize,
    pub completion_capacity: usize,
    pub mutation_timeout: Duration,
}

impl Default for BrokerCommandWorkerConfig {
    fn default() -> Self {
        Self {
            entry_capacity: 1024,
            control_capacity: 256,
            completion_capacity: 256,
            mutation_timeout: Duration::from_secs(30),
        }
    }
}

struct QueuedCommand {
    command: BrokerCommand,
    queued_at_us: i64,
}

const MAX_COMMAND_RECONCILIATIONS: usize = 256;
const MAX_CANCEL_RECOVERY_ATTEMPTS: usize = 6;
const COMMAND_RECONCILIATION_DELAYS: [Duration; 6] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(5),
    Duration::from_secs(15),
    Duration::from_secs(30),
    Duration::from_secs(60),
];

struct AmbiguousCommandReconciliation {
    command: BrokerCommand,
    attempt: usize,
    due: tokio::time::Instant,
}

enum MarginPreflight {
    Unsupported,
    Required(f64),
    Declined(String),
}

fn schedule_command_reconciliation(
    pending: &mut VecDeque<AmbiguousCommandReconciliation>,
    command: &BrokerCommand,
) {
    if pending.len() >= MAX_COMMAND_RECONCILIATIONS
        || pending.iter().any(|item| item.command.id == command.id)
        || !matches!(command.action, BrokerCommandAction::Submit { .. } | BrokerCommandAction::Cancel { .. })
    {
        return;
    }
    pending.push_back(AmbiguousCommandReconciliation {
        command: command.clone(),
        attempt: 0,
        due: tokio::time::Instant::now() + COMMAND_RECONCILIATION_DELAYS[0],
    });
}

fn next_command_reconciliation_at(
    pending: &VecDeque<AmbiguousCommandReconciliation>,
) -> tokio::time::Instant {
    pending
        .iter()
        .map(|item| item.due)
        .min()
        .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(3600))
}

fn take_due_command_reconciliation(
    pending: &mut VecDeque<AmbiguousCommandReconciliation>,
) -> Option<AmbiguousCommandReconciliation> {
    let now = tokio::time::Instant::now();
    let index = pending
        .iter()
        .enumerate()
        .filter(|(_, item)| item.due <= now)
        .min_by_key(|(_, item)| item.due)
        .map(|(index, _)| index)?;
    pending.remove(index)
}

fn reschedule_command_reconciliation(
    pending: &mut VecDeque<AmbiguousCommandReconciliation>,
    mut item: AmbiguousCommandReconciliation,
) {
    item.attempt = item.attempt.saturating_add(1);
    let Some(delay) = COMMAND_RECONCILIATION_DELAYS.get(item.attempt) else {
        return;
    };
    item.due = tokio::time::Instant::now() + *delay;
    pending.push_back(item);
}

/// No broker state is implied by this event. Failed admission/persistence is
/// distinct from a completed broker command and must not reject a filled insight.
#[derive(Debug)]
pub enum BrokerCommandWorkerEvent {
    Completion(BrokerCommandCompletion),
    Failed {
        receipt: BrokerCommandReceipt,
        error: BrokerCommandError,
    },
}

/// Short synchronous gate linearizes entry admission, first broker poll and stop.
/// It is never held across an await, journal I/O, or the broker's pending future.
#[derive(Default)]
struct EntryGate {
    stopped: bool,
    cancelled: std::collections::HashSet<BrokerCommandId>,
    deadlines: std::collections::HashMap<BrokerCommandId, chrono::DateTime<chrono::Utc>>,
    snapshots: Option<Arc<BrokerSnapshotCache>>,
}

impl EntryGate {
    fn cancels_entry(
        &self,
        id: &BrokerCommandId,
        now: impl FnOnce() -> chrono::DateTime<chrono::Utc>,
    ) -> bool {
        self.stopped
            || self.cancelled.contains(id)
            || self
                .deadlines
                .get(id)
                .is_some_and(|deadline| now() >= *deadline)
    }
}

#[derive(Clone)]
pub struct BrokerCommandSender {
    entries: mpsc::Sender<QueuedCommand>,
    controls: mpsc::Sender<QueuedCommand>,
    journal_requests: mpsc::Sender<JournalRequest>,
    gate: Arc<parking_lot::Mutex<EntryGate>>,
    stop: Arc<StopFence>,
    scope: Arc<(String, String)>,
    checkpoint_capacity: Arc<tokio::sync::Semaphore>,
    evidence_capacity: Arc<tokio::sync::Semaphore>,
}

impl BrokerCommandSender {
    /// Startup-only installation, before the strategy dispatches restored work.
    pub(crate) fn install_snapshot_guard(
        &self,
        cache: Arc<BrokerSnapshotCache>,
    ) -> Result<(), BrokerCommandError> {
        cache
            .with_fresh_account(&self.scope.1, || ())
            .map_err(|error| {
                BrokerCommandError::Invalid(format!(
                    "live mutation account validation failed: {error}"
                ))
            })?;
        let mut gate = self.gate.lock();
        if gate.snapshots.is_some() {
            return Err(BrokerCommandError::Invalid(
                "live mutation guard already installed".into(),
            ));
        }
        gate.snapshots = Some(cache);
        Ok(())
    }
    /// Immediately gates broker polling, then wakes the journal owner. The stop
    /// is durable only after `execution_stop_pending()` becomes false without an
    /// error. Repeated requests occupy one coalesced notification, not a queue.
    pub(crate) fn request_execution_stop(&self) {
        self.stop_entries();
        self.stop.request();
    }

    pub(crate) fn execution_stop_pending(&self) -> bool {
        self.stop.pending()
    }
    pub(crate) fn execution_stop_error(&self) -> Option<BrokerCommandError> {
        self.stop.error()
    }

    pub(crate) fn try_complete_execution_stop(
        &self,
    ) -> Result<BrokerJournalReply<bool>, BrokerCommandError> {
        let (reply, receiver) = oneshot::channel();
        self.try_journal_request(JournalRequest::CompleteStop { reply })?;
        Ok(BrokerJournalReply(receiver))
    }
    pub(crate) fn cancel_pending_entry(&self, id: BrokerCommandId) {
        self.gate.lock().cancelled.insert(id);
    }

    pub(crate) fn release_entry_identity(&self, id: &BrokerCommandId) {
        let mut gate = self.gate.lock();
        gate.cancelled.remove(id);
        gate.deadlines.remove(id);
    }

    /// Derived from the owner's durable insight checkpoint, not duplicated in
    /// broker command packets. The receipt remains owned until durable reduction.
    pub(crate) fn set_entry_deadline(
        &self,
        id: BrokerCommandId,
        deadline: Option<chrono::DateTime<chrono::Utc>>,
    ) {
        let mut gate = self.gate.lock();
        if let Some(deadline) = deadline {
            gate.deadlines.insert(id, deadline);
        } else {
            gate.deadlines.remove(&id);
        }
    }
    pub fn scope(&self) -> (&str, &str) {
        (&self.scope.0, &self.scope.1)
    }

    /// One bounded snapshot request may be outstanding across ALL sender clones.
    /// Receipts and resulting state commit atomically in the journal owner.
    pub(crate) fn try_persist_execution(
        &self,
        batch: ExecutionCheckpointBatch,
    ) -> Result<BrokerJournalReply<()>, BrokerCommandError> {
        let permit = Arc::clone(&self.checkpoint_capacity)
            .try_acquire_owned()
            .map_err(|_| BrokerCommandError::QueueFull)?;
        let (reply, receiver) = oneshot::channel();
        self.try_journal_request(JournalRequest::Checkpoint {
            batch,
            _permit: permit,
            reply,
        })?;
        Ok(BrokerJournalReply(receiver))
    }

    pub(crate) fn try_execution_checkpoint_page(
        &self,
        after: String,
        limit: usize,
    ) -> Result<BrokerJournalReply<ExecutionCheckpointPage>, BrokerCommandError> {
        let (reply, receiver) = oneshot::channel();
        self.try_journal_request(JournalRequest::CheckpointPage {
            after,
            limit,
            reply,
        })?;
        Ok(BrokerJournalReply(receiver))
    }

    pub fn try_recovery_page(
        &self,
        after_sequence: i64,
        limit: usize,
    ) -> Result<BrokerJournalReply<Vec<JournalCommandRecord>>, BrokerCommandError> {
        let (reply, receiver) = oneshot::channel();
        self.try_journal_request(JournalRequest::RecoveryPage {
            after_sequence,
            limit: limit.clamp(1, 256),
            reply,
        })?;
        Ok(BrokerJournalReply(receiver))
    }

    /// One-shot fill evidence uses the same bounded analytics permit as reads.
    /// Await outside the synchronous strategy/reducer turn.
    pub async fn record_execution_fills(
        &self,
        batch: ExecutionFillBatch,
    ) -> Result<(), BrokerCommandError> {
        self.record_execution_fills_for_capture(batch, None).await
    }

    pub(crate) async fn record_execution_fills_for_capture(
        &self,
        batch: ExecutionFillBatch,
        capture: Option<CloudCaptureGate>,
    ) -> Result<(), BrokerCommandError> {
        check_analytics_capture(&capture)?;
        // Admission may wait for an existing analytics read, but never drops a
        // one-shot fill merely because the inspector/exporter held the permit.
        let permit = Arc::clone(&self.evidence_capacity)
            .acquire_owned().await.map_err(|_| BrokerCommandError::Closed)?;
        check_analytics_capture(&capture)?;
        let (reply, receiver) = oneshot::channel();
        self.journal_requests.send(JournalRequest::RecordFills {
            batch, capture, _permit: permit, reply,
        }).await.map_err(|_| BrokerCommandError::Closed)?;
        BrokerJournalReply(receiver).wait().await
    }

    pub(crate) fn try_position_mark_owners(&self,minute:i64,after:Option<(String,String)>)->Result<BrokerJournalReply<super::command_journal::marks::PositionMarkOwners>,BrokerCommandError> {
        let permit=self.try_analytics_permit()?;
        let (reply,receiver)=oneshot::channel();
        self.try_analytics_request(JournalRequest::PositionMarkOwners{minute,after,_permit:permit,reply})?;Ok(BrokerJournalReply(receiver))
    }
    pub(crate) async fn record_position_marks_for_capture(
        &self,
        batch: super::command_journal::marks::PositionMarkBatch,
        capture: Option<CloudCaptureGate>,
    ) -> Result<usize, BrokerCommandError> {
        check_analytics_capture(&capture)?;
        // Scheduled analytics may skip a busy minute; never wait ahead of an
        // actual fill/correction for the shared bounded evidence permit.
        let permit = self.try_analytics_permit()?;
        let (reply, receiver) = oneshot::channel();
        self.try_analytics_request(JournalRequest::RecordPositionMarks {
            batch, capture, _permit: permit, reply,
        })?;
        BrokerJournalReply(receiver).wait().await
    }
    pub(crate) fn try_prepare_position_mark_export(&self)->Result<BrokerJournalReply<Option<aqe_live_protocol::PositionMarkTransfer>>,BrokerCommandError> {
        let permit=self.try_analytics_permit()?;
        let (reply,receiver)=oneshot::channel();
        self.try_analytics_request(JournalRequest::PrepareMarkExport{_permit:permit,reply})?;
        Ok(BrokerJournalReply(receiver))
    }
    pub(crate) fn try_acknowledge_position_mark_export(&self,receipt:aqe_live_protocol::PositionMarkExportReceipt)->Result<BrokerJournalReply<()>,BrokerCommandError> {
        let permit=self.try_analytics_permit()?;
        let (reply,receiver)=oneshot::channel();
        self.try_analytics_request(JournalRequest::AcknowledgeMarkExport{receipt,_permit:permit,reply})?;
        Ok(BrokerJournalReply(receiver))
    }
    pub(crate) fn try_fill_history_owners(&self, after: String)
        -> Result<BrokerJournalReply<super::fill_history::FillHistoryOwnerPage>, BrokerCommandError> {
        let permit = self.try_analytics_permit()?;
        let (reply, receiver) = oneshot::channel();
        self.try_analytics_request(JournalRequest::FillHistoryOwners { after, active_only:false, _permit:permit, reply })?;
        Ok(BrokerJournalReply(receiver))
    }

    pub(crate) fn try_active_fill_history_owners(&self, after: String)
        -> Result<BrokerJournalReply<super::fill_history::FillHistoryOwnerPage>, BrokerCommandError> {
        let permit = self.try_analytics_permit()?;
        let (reply, receiver) = oneshot::channel();
        self.try_analytics_request(JournalRequest::FillHistoryOwners { after, active_only:true, _permit:permit, reply })?;
        Ok(BrokerJournalReply(receiver))
    }

    pub fn try_execution_fill_page(
        &self, after: u64, through: Option<u64>, limit: usize,
    ) -> Result<BrokerJournalReply<aqe_live_protocol::ExecutionFillPage>, BrokerCommandError> {
        let permit = self.try_analytics_permit()?;
        let (reply, receiver) = oneshot::channel();
        self.try_analytics_request(JournalRequest::FillPage {
            after, through, limit: limit.clamp(1, 128), _permit: permit, reply,
        })?;
        Ok(BrokerJournalReply(receiver))
    }

    pub fn try_prepare_execution_fill_export(&self)
        -> Result<BrokerJournalReply<Option<aqe_live_protocol::ExecutionFillTransfer>>, BrokerCommandError> {
        let permit = self.try_analytics_permit()?;
        let (reply, receiver) = oneshot::channel();
        self.try_analytics_request(JournalRequest::PrepareFillExport { _permit: permit, reply })?;
        Ok(BrokerJournalReply(receiver))
    }

    pub fn try_acknowledge_execution_fill_export(&self, receipt: aqe_live_protocol::ExecutionFillExportReceipt)
        -> Result<BrokerJournalReply<()>, BrokerCommandError> {
        let permit = self.try_analytics_permit()?;
        let (reply, receiver) = oneshot::channel();
        self.try_analytics_request(JournalRequest::AcknowledgeFillExport { receipt, _permit: permit, reply })?;
        Ok(BrokerJournalReply(receiver))
    }

    fn try_analytics_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, BrokerCommandError> {
        Arc::clone(&self.evidence_capacity).try_acquire_owned().map_err(|error| match error {
            tokio::sync::TryAcquireError::NoPermits => BrokerCommandError::AnalyticsBusy,
            tokio::sync::TryAcquireError::Closed => BrokerCommandError::Closed,
        })
    }

    fn try_analytics_request(&self, request: JournalRequest) -> Result<(), BrokerCommandError> {
        self.try_journal_request(request).map_err(|error| match error {
            BrokerCommandError::QueueFull => BrokerCommandError::AnalyticsQueueFull,
            other => other,
        })
    }

    fn try_journal_request(&self, request: JournalRequest) -> Result<(), BrokerCommandError> {
        self.journal_requests
            .try_send(request)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => BrokerCommandError::QueueFull,
                mpsc::error::TrySendError::Closed(_) => BrokerCommandError::Closed,
            })
    }

    pub fn try_enqueue(
        &self,
        command: BrokerCommand,
    ) -> Result<BrokerCommandReceipt, BrokerCommandError> {
        self.try_enqueue_inner(command, false)
    }

    /// A previously accepted, durably checkpointed entry still needs a
    /// CancelledBeforeDispatch result after stop. The first-poll gate remains
    /// authoritative; this only permits delivery to that gate.
    pub(crate) fn try_enqueue_prepared(
        &self,
        command: BrokerCommand,
    ) -> Result<BrokerCommandReceipt, BrokerCommandError> {
        self.try_enqueue_inner(command, true)
    }

    fn try_enqueue_inner(
        &self,
        command: BrokerCommand,
        prepared: bool,
    ) -> Result<BrokerCommandReceipt, BrokerCommandError> {
        // Validate the byte limit before admission so the queue has a known memory ceiling.
        command.encode()?;
        if command.strategy_id != self.scope.0 || command.account_id != self.scope.1 {
            return Err(BrokerCommandError::Invalid(
                "command belongs to another worker scope".into(),
            ));
        }
        let gate = self.gate.lock();
        if gate.stopped && command.action.is_entry() && !prepared {
            return Err(BrokerCommandError::Stopping);
        }
        let receipt = command.receipt();
        let sender = if command.action.is_teardown() {
            &self.controls
        } else {
            &self.entries
        };
        sender
            .try_send(QueuedCommand {
                command,
                queued_at_us: chrono::Utc::now().timestamp_micros(),
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => BrokerCommandError::QueueFull,
                mpsc::error::TrySendError::Closed(_) => BrokerCommandError::Closed,
            })?;
        Ok(receipt)
    }

    /// Returns only after future entry dispatches have been disabled. In-flight
    /// calls are not aborted: their eventual outcome still needs reconciliation.
    pub fn stop_entries(&self) {
        self.gate.lock().stopped = true;
    }

    pub fn entries_stopped(&self) -> bool {
        self.gate.lock().stopped
    }

    pub(crate) fn entry_is_gated(&self, id: &BrokerCommandId) -> bool {
        let gate = self.gate.lock();
        gate.stopped || gate.cancelled.contains(id)
    }

    pub fn is_closed(&self) -> bool {
        self.entries.is_closed() && self.controls.is_closed()
    }
}

pub struct BrokerCommandWorker {
    pub sender: BrokerCommandSender,
    pub events: mpsc::Receiver<BrokerCommandWorkerEvent>,
    pub ready: Arc<tokio::sync::Notify>,
    pub task: tokio::task::JoinHandle<Result<BrokerCommandJournal, BrokerCommandError>>,
}

impl BrokerCommandWorker {
    pub fn start<E: OrderManagementProvider + 'static>(
        broker: Arc<E>,
        journal: BrokerCommandJournal,
        config: BrokerCommandWorkerConfig,
    ) -> Result<Self, BrokerCommandError> {
        let provider = broker.get_name();
        let lane = account_lane::AccountLane::shared(&provider, journal.scope().1)?;
        let host_account = host_account_lock::HostAccountCoordinator::from_environment(
            &provider,
            journal.scope().1,
        )?;
        Self::start_executor_with_lane(broker, journal, config, Some(lane), host_account)
    }

    #[cfg(test)]
    fn start_executor<E: CommandExecutor + 'static>(
        executor: Arc<E>,
        journal: BrokerCommandJournal,
        config: BrokerCommandWorkerConfig,
    ) -> Result<Self, BrokerCommandError> {
        Self::start_executor_with_lane(executor, journal, config, None, None)
    }

    fn start_executor_with_lane<E: CommandExecutor + 'static>(
        executor: Arc<E>,
        journal: BrokerCommandJournal,
        config: BrokerCommandWorkerConfig,
        lane: Option<Arc<account_lane::AccountLane>>,
        host_account: Option<host_account_lock::HostAccountCoordinator>,
    ) -> Result<Self, BrokerCommandError> {
        if config.entry_capacity == 0
            || config.control_capacity == 0
            || config.completion_capacity == 0
            || config.entry_capacity > 4096
            || config.control_capacity > 4096
            || config.completion_capacity > 4096
            || config.mutation_timeout.is_zero()
        {
            return Err(BrokerCommandError::Invalid(
                "invalid bounded worker configuration".into(),
            ));
        }
        let (entry_tx, entry_rx) = mpsc::channel(config.entry_capacity);
        let (control_tx, control_rx) = mpsc::channel(config.control_capacity);
        let (event_tx, events) = mpsc::channel(config.completion_capacity);
        let ready = Arc::new(tokio::sync::Notify::new());
        let (journal_tx, journal_rx) = mpsc::channel(config.completion_capacity);
        let phase = journal.execution_lifecycle().phase;
        let stop = Arc::new(StopFence::new(phase));
        let gate = Arc::new(parking_lot::Mutex::new(EntryGate {
            stopped: phase != super::command_journal::lifecycle::ExecutionPhase::Running,
            ..Default::default()
        }));
        let (strategy_id, account_id) = journal.scope();
        let sender = BrokerCommandSender {
            entries: entry_tx,
            controls: control_tx,
            journal_requests: journal_tx,
            gate: gate.clone(),
            stop: Arc::clone(&stop),
            scope: Arc::new((strategy_id.to_owned(), account_id.to_owned())),
            checkpoint_capacity: Arc::new(tokio::sync::Semaphore::new(1)),
            evidence_capacity: Arc::new(tokio::sync::Semaphore::new(1)),
        };
        let task = tokio::spawn(run(
            executor,
            journal,
            config,
            entry_rx,
            control_rx,
            journal_rx,
            CompletionSender {
                events: event_tx,
                ready: Arc::clone(&ready),
                stop,
            },
            gate,
            lane,
            host_account,
        ));
        Ok(Self {
            sender,
            events,
            ready,
            task,
        })
    }
}

/// Readiness is a single coalesced permit, not a second event backlog. Publish
/// the event before waking its owner; a notification remains observable even
/// when it arrives between the owner's readiness check and select registration.
struct CompletionSender {
    events: mpsc::Sender<BrokerCommandWorkerEvent>,
    ready: Arc<tokio::sync::Notify>,
    stop: Arc<StopFence>,
}

impl CompletionSender {
    fn is_closed(&self) -> bool {
        self.events.is_closed()
    }
    async fn closed(&self) {
        self.events.closed().await;
    }
    async fn send(&self, event: BrokerCommandWorkerEvent) -> bool {
        let delivered = self.events.send(event).await.is_ok();
        self.ready.notify_one();
        delivered
    }
}

impl Drop for CompletionSender {
    fn drop(&mut self) {
        self.ready.notify_one();
    }
}

/// Statically dispatched test seam; production uses exactly the OMS adapter.
trait CommandExecutor: Send + Sync {
    fn is_connected(&self) -> bool {
        true
    }
    fn current_time(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
    fn execute(
        &self,
        action: BrokerCommandAction,
    ) -> impl Future<Output = BrokerCommandOutcome> + Send;
    fn reconcile_submission(
        &self,
        _account_id: &str,
        _request: &BrokerOrderRequest,
    ) -> impl Future<Output = Result<SubmissionResolution, super::types::BrokerError>> + Send {
        async { Ok(SubmissionResolution::Unsupported) }
    }
    fn pending_cancellation_recovery(&self, _account: &str, _order: &str)
        -> impl Future<Output = Result<Option<super::types::OrderLifecycleSnapshot>, super::types::BrokerError>> + Send {
        async { Ok(None) }
    }
    fn retry_cancellation(&self, _account: &str, _order: &str)
        -> impl Future<Output = BrokerCommandOutcome> + Send {
        async { BrokerCommandOutcome::Ambiguous { reason: "cancellation recovery unsupported".into() } }
    }
    fn estimate_order_margin(
        &self,
        _request: &BrokerOrderRequest,
    ) -> impl Future<Output = Result<Option<f64>, super::types::BrokerError>> + Send {
        async { Ok(None) }
    }
}

impl<E: OrderManagementProvider> CommandExecutor for E {
    fn is_connected(&self) -> bool {
        super::traits::Broker::is_connected(self)
    }
    fn current_time(&self) -> chrono::DateTime<chrono::Utc> {
        super::traits::Broker::get_current_time(self)
    }
    async fn execute(&self, action: BrokerCommandAction) -> BrokerCommandOutcome {
        let result = match action {
            BrokerCommandAction::Submit { request } => {
                return match self.submit_order(request).await {
                    Ok(order) => BrokerCommandOutcome::OrderResponse { order },
                    Err(super::types::BrokerError::UnsupportedOperation(reason)) => {
                        BrokerCommandOutcome::Declined { reason }
                    }
                    Err(super::types::BrokerError::SubmissionRejected(reason)) => {
                        BrokerCommandOutcome::Declined { reason }
                    }
                    Err(error) => BrokerCommandOutcome::Ambiguous {
                        reason: error.to_string(),
                    },
                };
            }
            BrokerCommandAction::AmendEntry {
                order_id,
                limit_price,
                stop_price,
            } => {
                self.amend_entry_prices(&order_id, limit_price, stop_price)
                    .await
            }
            BrokerCommandAction::Cancel { order_id } => self.cancel_order(&order_id).await,
            BrokerCommandAction::Close {
                order_id,
                quantity,
                price,
            } => self.close_position(&order_id, quantity, price).await,
            BrokerCommandAction::Replace {
                order_id,
                price,
                quantity,
            } => self.replace_order(&order_id, price, quantity).await,
            BrokerCommandAction::TakeProfit {
                order_id,
                price,
                quantity,
            } => self.update_take_profit(&order_id, price, quantity).await,
            BrokerCommandAction::StopLoss {
                order_id,
                price,
                quantity,
            } => self.update_stop_loss(&order_id, price, quantity).await,
        };
        match result {
            Ok(true) => BrokerCommandOutcome::Acknowledged,
            Ok(false) => BrokerCommandOutcome::Declined {
                reason: "broker did not acknowledge the mutation".into(),
            },
            Err(super::types::BrokerError::UnsupportedOperation(reason)) => {
                BrokerCommandOutcome::Declined { reason }
            }
            // Existing BrokerError variants do not establish whether bytes reached
            // the broker. Conservatively reconcile errors; never blindly repeat.
            Err(error) => BrokerCommandOutcome::Ambiguous {
                reason: error.to_string(),
            },
        }
    }

    async fn reconcile_submission(
        &self,
        account_id: &str,
        request: &BrokerOrderRequest,
    ) -> Result<SubmissionResolution, super::types::BrokerError> {
        OrderManagementProvider::reconcile_submission(self, account_id, request).await
    }

    async fn pending_cancellation_recovery(&self, account: &str, order: &str)
        -> Result<Option<super::types::OrderLifecycleSnapshot>, super::types::BrokerError> {
        OrderManagementProvider::pending_cancellation_recovery(self, account, order).await
    }

    async fn retry_cancellation(&self, account: &str, order: &str) -> BrokerCommandOutcome {
        match OrderManagementProvider::retry_cancellation(self, account, order).await {
            Ok(true) => BrokerCommandOutcome::Acknowledged,
            Ok(false) => BrokerCommandOutcome::Ambiguous { reason: "cancellation still lacks terminal broker evidence".into() },
            Err(error) => BrokerCommandOutcome::Ambiguous { reason: error.to_string() },
        }
    }

    async fn estimate_order_margin(
        &self,
        request: &BrokerOrderRequest,
    ) -> Result<Option<f64>, super::types::BrokerError> {
        OrderManagementProvider::estimate_order_margin(self, request).await
    }
}

/// Resolve a lost submit response through the broker's immutable client-order receipt.
/// This path never resubmits. Unknown, unsupported, failed, or slow lookups retain the
/// original ambiguous evidence so capacity and insight state remain conservatively fenced.
async fn reconcile_ambiguous_submission<E: CommandExecutor>(
    executor: &E,
    journal: &mut BrokerCommandJournal,
    command: &BrokerCommand,
    completion: BrokerCommandCompletion,
) -> Result<BrokerCommandCompletion, BrokerCommandError> {
    if !matches!(completion.outcome, BrokerCommandOutcome::Ambiguous { .. }) {
        return Ok(completion);
    }
    let BrokerCommandAction::Submit { request } = &command.action else {
        return Ok(completion);
    };
    match tokio::time::timeout(
        Duration::from_secs(2),
        executor.reconcile_submission(&command.account_id, request),
    )
    .await
    {
        Ok(Ok(SubmissionResolution::Confirmed(order))) => {
            journal.confirm_ambiguous_submission(command, order).await
        }
        Ok(Ok(SubmissionResolution::Unknown | SubmissionResolution::Unsupported))
        | Ok(Err(_))
        | Err(_) => Ok(completion),
    }
}

async fn run<E: CommandExecutor>(
    executor: Arc<E>,
    mut journal: BrokerCommandJournal,
    config: BrokerCommandWorkerConfig,
    mut entries: mpsc::Receiver<QueuedCommand>,
    mut controls: mpsc::Receiver<QueuedCommand>,
    mut journal_requests: mpsc::Receiver<JournalRequest>,
    events: CompletionSender,
    gate: Arc<parking_lot::Mutex<EntryGate>>,
    lane: Option<Arc<account_lane::AccountLane>>,
    host_account: Option<host_account_lock::HostAccountCoordinator>,
) -> Result<BrokerCommandJournal, BrokerCommandError> {
    // At most one parked command per bounded lane. A stale account neither
    // consumes all queue slots into an unbounded side buffer nor spins on reads.
    let mut parked_entry: Option<QueuedCommand> = None;
    let mut parked_control: Option<QueuedCommand> = None;
    let mut command_reconciliations = VecDeque::new();
    let mut cancellation_attempts = std::collections::HashMap::<BrokerCommandId, usize>::new();
    let mut admission_retry = tokio::time::interval(Duration::from_millis(25));
    admission_retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    'commands: loop {
        let reconciliation_due = next_command_reconciliation_at(&command_reconciliations);
        let (account_ready, entry_ready, entry_cancelled, account_wake) = {
            let gate = gate.lock();
            let ready = gate
                .snapshots
                .as_ref()
                .is_none_or(|cache| match cache.account() {
                    Err(SnapshotUnavailable::AccountChanged) => true,
                    Ok(_) => executor.is_connected(),
                    Err(_) => false,
                });
            let cancelled = parked_entry.as_ref().is_some_and(|entry| {
                entry.command.action.is_entry()
                    && gate.cancels_entry(&entry.command.id, || executor.current_time())
            });
            let entry_ready = ready && parked_entry.as_ref().is_none_or(|entry| {
                gate.snapshots.as_ref().is_none_or(|cache| {
                    let needs_slot = matches!(&entry.command.action, BrokerCommandAction::Submit { request }
                        if request.order_type != super::types::OrderType::Market);
                    matches!(cache.with_fresh_account_capacity(&entry.command.account_id, needs_slot, |_, _| ()),
                        Ok(()) | Err(SnapshotUnavailable::AccountChanged))
                })
            });
            let wake = if parked_entry.is_some() || parked_control.is_some() {
                gate.snapshots
                    .as_ref()
                    .map(|cache| cache.mutation_notifier())
            } else {
                None
            };
            (ready, entry_ready, cancelled, wake)
        };
        let queued = tokio::select! {
            biased;
            _ = events.closed() => break,
            _ = events.stop.notify.notified() => {
                events.stop.persist(&mut journal, &events.ready).await?;
                continue;
            }
            command = async { parked_control.take() }, if parked_control.is_some() && account_ready => command,
            command = controls.recv(), if parked_control.is_none() && (!controls.is_closed() || !controls.is_empty()) => command,
            Some(request) = journal_requests.recv() => {
                handle_journal_request(&mut journal, request, &events.ready).await;
                continue;
            }
            _ = tokio::time::sleep_until(reconciliation_due), if !command_reconciliations.is_empty() => {
                let Some(item) = take_due_command_reconciliation(&mut command_reconciliations) else {
                    continue;
                };
                let saved = journal.get(&item.command.id).await?;
                let still_ambiguous = saved.as_ref().is_some_and(|record| {
                    record.state == JournalCommandState::Completed
                        && record.completion.as_ref().is_some_and(|completion| {
                            matches!(completion.outcome, BrokerCommandOutcome::Ambiguous { .. })
                        })
                });
                if !still_ambiguous {
                    cancellation_attempts.remove(&item.command.id);
                    continue;
                }
                if matches!(item.command.action, BrokerCommandAction::Cancel { .. }) {
                    // Re-enter the ordinary control lane, with the same immutable
                    // command. No broker mutation occurs in this timer branch.
                    if parked_control.is_none() {
                        parked_control = Some(QueuedCommand { command: item.command.clone(),
                            queued_at_us: saved.expect("checked above").queued_at_us });
                    }
                    reschedule_command_reconciliation(&mut command_reconciliations, item);
                    continue;
                }
                let BrokerCommandAction::Submit { request } = &item.command.action else {
                    continue;
                };
                let resolution = tokio::time::timeout(
                    Duration::from_millis(500),
                    executor.reconcile_submission(&item.command.account_id, request),
                ).await;
                match resolution {
                    Ok(Ok(SubmissionResolution::Confirmed(order))) => {
                        let completion = journal
                            .confirm_ambiguous_submission(&item.command, order)
                            .await?;
                        if let Some(lane) = &lane {
                            lane.settle_entry(&completion.receipt.id, &completion.outcome);
                        }
                        if !deliver_event(
                            &events,
                            BrokerCommandWorkerEvent::Completion(completion),
                            &mut journal,
                            &mut journal_requests,
                        ).await {
                            break;
                        }
                    }
                    Ok(Ok(SubmissionResolution::Unsupported)) => {}
                    Ok(Ok(SubmissionResolution::Unknown)) | Ok(Err(_)) | Err(_) => {
                        reschedule_command_reconciliation(&mut command_reconciliations, item);
                    }
                }
                continue;
            }
            command = async { parked_entry.take() }, if parked_entry.is_some() && (entry_ready || entry_cancelled) => command,
            command = entries.recv(), if parked_entry.is_none() && (!entries.is_closed() || !entries.is_empty()) => command,
            _ = async {
                match account_wake.as_ref() {
                    Some(wake) => wake.notified().await,
                    None => std::future::pending::<()>().await,
                }
            }, if account_wake.is_some() => continue,
            _ = admission_retry.tick(), if parked_entry.is_some() || parked_control.is_some() => continue,
            else => break,
        };
        let Some(QueuedCommand {
            command,
            queued_at_us,
        }) = queued
        else {
            continue;
        };
        let receipt = command.receipt();
        let record = match journal.prepare(&command, queued_at_us).await {
            Ok(record) => record,
            Err(error) => {
                let fatal = matches!(error, BrokerCommandError::Journal(_));
                deliver_event(
                    &events,
                    BrokerCommandWorkerEvent::Failed {
                        receipt,
                        error: error.clone(),
                    },
                    &mut journal,
                    &mut journal_requests,
                )
                .await;
                if fatal {
                    return Err(error);
                }
                continue;
            }
        };
        // Durable result replay is safe in both event-before-response and crash
        // recovery paths. The strategy reducer uses receipt.id for deduplication.
        let mut cancellation_proof = None;
        if let Some(completion) = record.completion.clone() {
            let completion = reconcile_ambiguous_submission(
                executor.as_ref(),
                &mut journal,
                &command,
                completion,
            )
            .await?;
            if matches!(completion.outcome, BrokerCommandOutcome::Ambiguous { .. })
                && cancellation_attempts.get(&command.id).copied().unwrap_or(0) < MAX_CANCEL_RECOVERY_ATTEMPTS
                && (cancellation_attempts.contains_key(&command.id) || cancellation_attempts.len() < MAX_COMMAND_RECONCILIATIONS)
                && let BrokerCommandAction::Cancel { order_id } = &command.action {
                let read = tokio::time::timeout(Duration::from_secs(10),
                    executor.pending_cancellation_recovery(&command.account_id, order_id));
                tokio::pin!(read);
                cancellation_proof = loop {
                    tokio::select! {
                        biased;
                        _ = events.closed() => break None,
                        _ = events.stop.notify.notified() => {
                            events.stop.persist(&mut journal, &events.ready).await?;
                        }
                        result = &mut read => break result.ok().and_then(Result::ok).flatten(),
                        Some(request) = journal_requests.recv() => {
                            handle_journal_request(&mut journal, request, &events.ready).await;
                        }
                    }
                }
                    .filter(|proof| proof.order_id == *order_id
                        && proof.status == super::types::OrderLifecycleStatus::Pending);
            }
            if cancellation_proof.is_none() {
                if matches!(completion.outcome, BrokerCommandOutcome::Ambiguous { .. })
                    && cancellation_attempts.get(&command.id).copied().unwrap_or(0) < MAX_CANCEL_RECOVERY_ATTEMPTS {
                    schedule_command_reconciliation(&mut command_reconciliations, &command);
                }
                if !deliver_event(
                    &events, BrokerCommandWorkerEvent::Completion(completion),
                    &mut journal, &mut journal_requests,
                ).await {
                    break;
                }
                continue;
            }
            // Keep the old completion intact until the guarded retry obtains
            // authoritative terminal evidence. Never retry an entry Submit.
        }
        let mut cancellation_attempt = None;
        let mut dispatched_at_us = record.dispatched_at_us;
        let mut account_permit = None;
        let mut host_account_permit = None;
        let mut entry_reservation = account_lane::EntryReservation::default();
        if events.is_closed() {
            gate.lock().stopped = true;
        }
        let cancelled_before_preflight = command.action.is_entry() && {
            let gate = gate.lock();
            gate.cancels_entry(&command.id, || executor.current_time())
        };
        let margin_preflight = if record.state != JournalCommandState::Dispatching
            && !cancelled_before_preflight
            && let BrokerCommandAction::Submit { request } = &command.action
        {
            match tokio::time::timeout(
                Duration::from_secs(2),
                executor.estimate_order_margin(request),
            )
            .await
            {
                Ok(Ok(Some(value))) if value.is_finite() && value >= 0.0 => {
                    MarginPreflight::Required(value)
                }
                Ok(Ok(None)) => MarginPreflight::Unsupported,
                Ok(Ok(Some(_))) => MarginPreflight::Declined(
                    "entry refused: broker returned an invalid margin estimate".into(),
                ),
                Ok(Err(error)) => MarginPreflight::Declined(format!(
                    "entry refused: authoritative broker margin estimate failed: {error}"
                )),
                Err(_) => MarginPreflight::Declined(
                    "entry refused: authoritative broker margin estimate timed out".into(),
                ),
            }
        } else {
            MarginPreflight::Unsupported
        };
        let outcome = if record.state == JournalCommandState::Dispatching {
            BrokerCommandOutcome::Ambiguous {
                reason: "recovered dispatched intent; broker reconciliation is required".into(),
            }
        } else if command.action.is_entry() && {
            let gate = gate.lock();
            gate.cancels_entry(&command.id, || executor.current_time())
        } {
            BrokerCommandOutcome::CancelledBeforeDispatch
        } else if let MarginPreflight::Declined(reason) = &margin_preflight {
            BrokerCommandOutcome::Declined {
                reason: reason.clone(),
            }
        } else {
            let account = {
                let gate = gate.lock();
                gate.snapshots.as_ref().map_or(Ok(()), |cache| {
                    cache.with_fresh_account(&command.account_id, || ())?;
                    if executor.is_connected() {
                        Ok(())
                    } else {
                        Err(SnapshotUnavailable::Missing)
                    }
                })
            };
            if let Err(reason) = account {
                if reason == SnapshotUnavailable::AccountChanged {
                    gate.lock().stopped = true;
                    events.stop.request();
                    events.stop.persist(&mut journal, &events.ready).await?;
                    return Err(BrokerCommandError::Invalid(
                        "broker account changed; mutation dispatch refused".into(),
                    ));
                }
                let slot = if command.action.is_teardown() {
                    &mut parked_control
                } else {
                    &mut parked_entry
                };
                *slot = Some(QueuedCommand {
                    command,
                    queued_at_us,
                });
                continue;
            }
            if let Some(lane) = &lane {
                let acquire = Arc::clone(lane).acquire(command.action.is_teardown());
                tokio::pin!(acquire);
                // Waiting is not dispatch. Continue serving durable checkpoints
                // and stop, and let cleanup preempt this worker's waiting entry.
                let permit = loop {
                    tokio::select! {
                        biased;
                        _ = events.closed() => {
                            gate.lock().stopped = true;
                            break None;
                        }
                        _ = events.stop.notify.notified() => {
                            events.stop.persist(&mut journal, &events.ready).await?;
                            if command.action.is_entry() { break None; }
                        }
                        Some(control) = controls.recv(), if !command.action.is_teardown() && parked_control.is_none() => {
                            parked_control = Some(control);
                            break None;
                        }
                        permit = &mut acquire => break Some(permit?),
                        Some(request) = journal_requests.recv() => {
                            handle_journal_request(&mut journal, request, &events.ready).await;
                        }
                    }
                };
                let Some(permit) = permit else {
                    let slot = if command.action.is_teardown() {
                        &mut parked_control
                    } else {
                        &mut parked_entry
                    };
                    *slot = Some(QueuedCommand {
                        command,
                        queued_at_us,
                    });
                    continue 'commands;
                };
                account_permit = Some(permit);
            }
            if let Some(coordinator) = &host_account {
                match coordinator.try_acquire().await? {
                    Some(permit) => host_account_permit = Some(permit),
                    None => {
                        // Do not hold the process lane while another process owns
                        // the host fence. Requeue so local teardown can preempt.
                        drop(account_permit.take());
                        let slot = if command.action.is_teardown() {
                            &mut parked_control
                        } else {
                            &mut parked_entry
                        };
                        *slot = Some(QueuedCommand {
                            command,
                            queued_at_us,
                        });
                        continue 'commands;
                    }
                }
            }
            let dispatch_time = chrono::Utc::now().timestamp_micros();
            if let Some(proof) = cancellation_proof.as_ref() {
                cancellation_attempt = journal.begin_cancellation_recovery(&command, proof, dispatch_time).await?;
                if cancellation_attempt.is_none() {
                    cancellation_attempts.remove(&command.id);
                    continue 'commands;
                }
                *cancellation_attempts.entry(command.id.clone()).or_default() += 1;
                log::info!("Retrying broker-confirmed pending cancellation for command {} under its original durable intent", command.id);
            } else {
                journal.mark_dispatched(&command.id, dispatch_time).await?;
                dispatched_at_us = Some(dispatch_time);
            }
            // Stop can arrive while the dispatch journal is being fsynced. Gate
            // the first poll as well as admission, without holding a lock over I/O.
            let is_entry = command.action.is_entry();
            let command_id = command.id.clone();
            let account_id = command.account_id.clone();
            let execution_action = command.action.clone();
            let execution_account = command.account_id.clone();
            let retry_cancel = cancellation_attempt.is_some();
            let executor_ref = executor.as_ref();
            // Construct both kinds of future outside the snapshot lock. An
            // adapter may invalidate its cache while constructing a future;
            // only the transport's first poll belongs under the freshness gate.
            let future = if retry_cancel && let BrokerCommandAction::Cancel { order_id } = &execution_action {
                futures::future::Either::Left(executor_ref.retry_cancellation(&execution_account, order_id))
            } else {
                futures::future::Either::Right(executor_ref.execute(execution_action.clone()))
            };
            let mut future = std::pin::pin!(future);
            let mut started = false;
            let operation = std::future::poll_fn(|cx| {
                if !started {
                    let admission = gate.lock();
                    if is_entry
                        && (admission.cancels_entry(&command_id, || executor.current_time())
                            || events.is_closed())
                    {
                        return std::task::Poll::Ready(Ok(
                            BrokerCommandOutcome::CancelledBeforeDispatch,
                        ));
                    }
                    let mut poll = || {
                        started = true;
                        future.as_mut().poll(cx).map(Ok)
                    };
                    return match admission.snapshots.as_ref() {
                        Some(_) if !executor.is_connected() => {
                            std::task::Poll::Ready(Err(SnapshotUnavailable::Missing))
                        }
                        Some(cache) => {
                            let pending_order = matches!(&command.action, BrokerCommandAction::Submit { request } if request.order_type != super::types::OrderType::Market);
                            let quantity_limit =
                                command.dispatch_constraints.max_total_symbol_quantity;
                            let symbol = match &command.action {
                                BrokerCommandAction::Submit { request }
                                    if quantity_limit.is_some() =>
                                {
                                    Some(request.symbol.as_str())
                                }
                                _ => None,
                            };
                            cache.with_fresh_entry_capacity(&account_id, pending_order, symbol,
                            |account, account_observed, pending_orders, observed_symbol_quantity| {
                            if let Some(reason) = entry_account_rejection(&command.action, account) {
                                return std::task::Poll::Ready(Ok(BrokerCommandOutcome::Declined { reason: reason.into() }));
                            }
                            let pending = account.max_total_orders.zip(pending_orders)
                                .map(|(limit, (count, observed))| (limit as usize, count, observed));
                            let symbol = match (&command.action, quantity_limit, observed_symbol_quantity) {
                                (BrokerCommandAction::Submit { request }, Some(limit), Some((observed, at))) => {
                                    Some((request.symbol.as_str(), request.quantity.unwrap_or_default(), limit, observed, at))
                                }
                                _ => None,
                            };
                            let margin = match &margin_preflight {
                                MarginPreflight::Required(required) => {
                                    Some((*required, account.buying_power, account_observed))
                                }
                                MarginPreflight::Unsupported => None,
                                MarginPreflight::Declined(_) => unreachable!("declined before dispatch"),
                            };
                            let reservation = match &account_permit {
                                Some(permit) => permit.reserve_entry(&command_id, pending, symbol, margin),
                                None if pending.is_some_and(|(limit, count, _)| count >= limit) => Err(account_lane::CapacityRejection::PendingOrders),
                                None if symbol.is_some_and(|(_, requested, limit, observed, _)| observed + requested > limit) => Err(account_lane::CapacityRejection::SymbolQuantity),
                                None if margin.is_some_and(|(required, available, _)| required > available) => Err(account_lane::CapacityRejection::Margin),
                                None => Ok(account_lane::EntryReservation::default()),
                            };
                            match reservation {
                                Ok(reservation) => entry_reservation = reservation,
                                Err(account_lane::CapacityRejection::PendingOrders) => {
                                    return std::task::Poll::Ready(Ok(BrokerCommandOutcome::Declined {
                                        reason: "entry refused: broker pending-order limit reached (including local reservations)".into(),
                                    }));
                                }
                                Err(account_lane::CapacityRejection::SymbolQuantity) => {
                                    return std::task::Poll::Ready(Ok(BrokerCommandOutcome::Declined {
                                        reason: "entry refused: broker aggregate symbol quantity limit reached (including pending orders and local reservations)".into(),
                                    }));
                                }
                                Err(account_lane::CapacityRejection::Margin) => {
                                    return std::task::Poll::Ready(Ok(BrokerCommandOutcome::Declined {
                                        reason: "entry refused: authoritative broker margin requirement exceeds available buying power (including local reservations)".into(),
                                    }));
                                }
                            }
                            poll()
                        }).unwrap_or_else(|reason| std::task::Poll::Ready(Err(reason)))
                        }
                        None => poll(),
                    };
                }
                future.as_mut().poll(cx).map(Ok)
            });
            let operation = tokio::time::timeout(config.mutation_timeout, operation);
            tokio::pin!(operation);
            let attempt = loop {
                tokio::select! {
                    biased;
                    _ = events.stop.notify.notified() => {
                        // Failure is exposed to the owner, but must not abort a
                        // mutation already sent to the broker. Preserve its
                        // result/ambiguity before releasing transport ownership.
                        let _ = events.stop.persist(&mut journal, &events.ready).await;
                    }
                    result = &mut operation => break match result {
                        Ok(outcome) => outcome,
                        Err(_) => Ok(BrokerCommandOutcome::Ambiguous {
                            reason: "broker mutation timed out; reconciliation is required".into(),
                        }),
                    },
                    // Checkpoint/recovery I/O must not wait for a remote broker
                    // response. No additional broker command is dispatched here.
                    Some(request) = journal_requests.recv() => {
                        handle_journal_request(&mut journal, request, &events.ready).await;
                    }
                }
            };
            match attempt {
                Ok(outcome) => {
                    if !started && cancellation_attempt.is_none() {
                        // Local risk refusal has proof that transport was never
                        // polled. Restore journal evidence before completing it.
                        journal
                            .defer_unpolled_dispatch(&command.id, dispatch_time)
                            .await?;
                        dispatched_at_us = None;
                    }
                    outcome
                }
                Err(reason) => {
                    // The future was never polled. Only this live in-process
                    // evidence permits returning the intent to Queued; a crash
                    // before this write is conservatively recovered as ambiguous.
                    if cancellation_attempt.is_none() {
                        journal.defer_unpolled_dispatch(&command.id, dispatch_time).await?;
                    }
                    if reason == SnapshotUnavailable::AccountChanged {
                        gate.lock().stopped = true;
                        events.stop.request();
                        events.stop.persist(&mut journal, &events.ready).await?;
                        return Err(BrokerCommandError::Invalid(
                            "broker account changed before mutation poll".into(),
                        ));
                    }
                    let slot = if command.action.is_teardown() {
                        &mut parked_control
                    } else {
                        &mut parked_entry
                    };
                    *slot = Some(QueuedCommand {
                        command,
                        queued_at_us,
                    });
                    continue 'commands;
                }
            }
        };
        if matches!(outcome, BrokerCommandOutcome::CancelledBeforeDispatch) {
            dispatched_at_us = None;
        }
        let completion = BrokerCommandCompletion {
            receipt,
            queued_at_us: record.queued_at_us,
            dispatched_at_us,
            completed_at_us: chrono::Utc::now().timestamp_micros(),
            outcome,
        };
        let completion = if let Some(attempt) = cancellation_attempt {
            let Some(completion) = journal.finish_cancellation_recovery(&command, attempt, &completion).await?
                else { cancellation_attempts.remove(&command.id); continue 'commands; };
            if !matches!(completion.outcome, BrokerCommandOutcome::Ambiguous { .. }) {
                cancellation_attempts.remove(&command.id);
            }
            completion
        } else {
            journal.complete(&completion).await?;
            completion
        };
        let completion =
            reconcile_ambiguous_submission(executor.as_ref(), &mut journal, &command, completion)
                .await?;
        if matches!(completion.outcome, BrokerCommandOutcome::Ambiguous { .. })
            && cancellation_attempts.get(&command.id).copied().unwrap_or(0) < MAX_CANCEL_RECOVERY_ATTEMPTS {
            schedule_command_reconciliation(&mut command_reconciliations, &command);
        }
        if let Some(permit) = &account_permit {
            permit.finish_entry(
                &completion.receipt.id,
                entry_reservation,
                &completion.outcome,
            );
        }
        // A slow strategy/UI receiver must not monopolize the account after
        // the broker result has become durable.
        drop(host_account_permit);
        drop(account_permit);
        if !deliver_event(
            &events,
            BrokerCommandWorkerEvent::Completion(completion),
            &mut journal,
            &mut journal_requests,
        )
        .await
        {
            break;
        }
    }
    // Dropping the UI/strategy receiver never discards an executed result; it was
    // persisted before delivery. Never dispatch buffered entries after owner loss.
    gate.lock().stopped = true;
    // A receiver may close before the coalesced stop notification is selected.
    // Resource shutdown cannot acknowledge success while that stop is unsaved.
    events.stop.persist(&mut journal, &events.ready).await?;
    Ok(journal)
}

async fn handle_journal_request(
    journal: &mut BrokerCommandJournal,
    request: JournalRequest,
    ready: &tokio::sync::Notify,
) {
    match request {
        JournalRequest::CompleteAnalyticsCapture { receipt, _permit, reply }=>{
            let result=journal.complete_analytics_capture_with_handoff(&receipt).await;
            drop(_permit);let _=reply.send(result);
        }
        JournalRequest::AnalyticsCaptureStatus { _permit, reply } => {
            if !reply.is_closed() {
                let result=journal.analytics_capture_status().await;
                drop(_permit);
                let _=reply.send(result);
            }
        }
        JournalRequest::AnalyticsCapturePause { _permit, reply } => {
            if !reply.is_closed() {
                let result = journal.analytics_capture_pause().await;
                drop(_permit);
                let _ = reply.send(result);
            }
        }
        JournalRequest::AcknowledgeAnalyticsCaptureHandoff { receipt, _permit, reply } => {
            // Persist an exact local quality receipt even if its observer goes
            // away. This never resumes capture or mutates a broker command.
            let result = journal.acknowledge_analytics_capture_handoff(&receipt).await;
            drop(_permit);
            let _ = reply.send(result);
        }
        JournalRequest::PrepareMarkExport{_permit,reply}=>{
            let result=journal.prepare_position_mark_export().await;
            drop(_permit);let _=reply.send(result);
        }
        JournalRequest::AcknowledgeMarkExport{receipt,_permit,reply}=>{
            let result=journal.acknowledge_position_mark_export(&receipt).await;
            drop(_permit);let _=reply.send(result);
        }
        JournalRequest::PositionMarkOwners{minute,after,_permit,reply}=>{
            if !reply.is_closed(){let result=journal.position_mark_owners(minute,after).await;drop(_permit);let _=reply.send(result);}
        },
        JournalRequest::RecordPositionMarks { batch, capture, _permit, reply } => {
            let result = match check_analytics_capture(&capture) {
                Ok(()) => {
                    let result=journal.record_position_marks(batch).await;
                    analytics_capture::observe_refusal(journal,&capture,&result).await;
                    result
                },
                Err(error) => Err(error),
            };
            drop(_permit);
            let _ = reply.send(result);
        }
        JournalRequest::FillHistoryOwners { after, active_only, _permit, reply } => {
            if !reply.is_closed() {
                let result = if active_only { journal.active_fill_history_owner_page(&after).await }
                    else { journal.fill_history_owner_page(&after).await };
                drop(_permit);
                let _ = reply.send(result);
            }
        }
        JournalRequest::PrepareFillExport { _permit, reply } => {
            if !reply.is_closed() {
                let result = journal.prepare_execution_fill_export().await;
                drop(_permit);
                let _ = reply.send(result);
            }
        }
        JournalRequest::AcknowledgeFillExport { receipt, _permit, reply } => {
            if !reply.is_closed() {
                let result = journal.acknowledge_execution_fill_export(&receipt).await;
                drop(_permit);
                let _ = reply.send(result);
            }
        }
        JournalRequest::RecordFills { batch, capture, _permit, reply } => {
            // Recheck after queue/permit waits, before admitting new analytics.
            // Once the transaction begins it still commits if its observer
            // disappears; previously accepted facts are never pruned here.
            let result = match check_analytics_capture(&capture) {
                Ok(()) => {
                    let result=journal.record_execution_fills(&batch).await;
                    analytics_capture::observe_refusal(journal,&capture,&result).await;
                    result
                },
                Err(error) => Err(error),
            };
            drop(_permit);
            let _ = reply.send(result);
        }
        JournalRequest::FillPage { after, through, limit, _permit, reply } => {
            if !reply.is_closed() {
                let _ = reply.send(journal.execution_fill_page(after, through, limit).await);
            }
            drop(_permit);
        }
        JournalRequest::CompleteStop { reply } => {
            let _ = reply.send(journal.complete_execution_stop().await);
        }
        JournalRequest::RecoveryPage {
            after_sequence,
            limit,
            reply,
        } => {
            let _ = reply.send(journal.recovery_page(after_sequence, limit).await);
        }
        JournalRequest::Checkpoint {
            batch,
            _permit,
            reply,
        } => {
            let result = journal.persist_execution_batch(&batch).await;
            drop(_permit);
            let _ = reply.send(result);
        }
        JournalRequest::CheckpointPage {
            after,
            limit,
            reply,
        } => {
            let _ = reply.send(journal.execution_checkpoint_page(&after, limit).await);
        }
    }
    ready.notify_one();
}

/// The journal owner must not become unavailable behind a full completion
/// channel. A reducer may be waiting for its durable ack/recovery page before it
/// can admit or consume more work. Keep control I/O live without dispatching any
/// additional broker mutation or allocating an overflow completion queue.
async fn deliver_event(
    events: &CompletionSender,
    event: BrokerCommandWorkerEvent,
    journal: &mut BrokerCommandJournal,
    requests: &mut mpsc::Receiver<JournalRequest>,
) -> bool {
    let send = events.send(event);
    tokio::pin!(send);
    loop {
        tokio::select! {
            biased;
            _ = events.stop.notify.notified() => {
                if events.stop.persist(journal, &events.ready).await.is_err() { return false; }
            }
            result = &mut send => return result,
            Some(request) = requests.recv() => handle_journal_request(journal, request, &events.ready).await,
        }
    }
}

#[cfg(test)]
mod tests;
