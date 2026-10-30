//! One explicitly owned, paced source-quality bridge. Dropping its handle
//! cancels in-flight observation/handoff work and closes new capture, not trading.
use super::*;
use crate::core::broker::command_worker::BrokerCommandSender;

enum SourceObservationError {
    /// No request was admitted. Keep the previous observation's original
    /// freshness deadline; contention must neither extend it nor invent a gap.
    Deferred,
    Failed(EventBusError),
}
impl From<crate::core::broker::commands::BrokerCommandError> for SourceObservationError {
    fn from(error: crate::core::broker::commands::BrokerCommandError) -> Self {
        if error.is_analytics_backpressure() {
            Self::Deferred
        } else {
            Self::Failed(EventBusError::Publish(format!("source journal observation failed: {error}")))
        }
    }
}
impl From<EventBusError> for SourceObservationError {
    fn from(error: EventBusError) -> Self { Self::Failed(error) }
}
impl SourceObservationError {
    fn event(self) -> EventBusError {
        match self {
            Self::Deferred => EventBusError::Publish("source journal observation temporarily busy; retry without replacing its freshness deadline".into()),
            Self::Failed(error) => error,
        }
    }
}

pub struct SourceJournalMonitor {
    task: Option<tokio::task::JoinHandle<()>>,
    capture: Arc<CaptureController>,
    handover: bool,
    handover_requested: Arc<AtomicBool>,
}
/// Final, read-only journal ownership. The exited command worker has returned
/// this exact file; retaining it prevents another writer from invalidating the
/// sealed observation while final cloud snapshots drain. Drop closes capture.
pub(crate) struct FinalSourceJournal {
    _journal: crate::core::broker::command_journal::BrokerCommandJournal,
    capture: Arc<CaptureController>,
}
impl Drop for FinalSourceJournal {
    fn drop(&mut self) {
        self.capture.source_unavailable();
    }
}
struct MonitorClaim {
    capture: Arc<CaptureController>,
    handover_requested: Arc<AtomicBool>,
}
impl Drop for MonitorClaim {
    fn drop(&mut self) {
        if !self.handover_requested.load(Ordering::Acquire) {
            self.capture.source_unavailable();
        }
        // Close capture before advertising that a replacement may take over.
        self.capture
            .source_monitor_active
            .store(false, Ordering::Release);
    }
}
impl SourceJournalMonitor {
    #[cfg(test)]
    pub(crate) async fn abort_task_for_test(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
    /// The strategy is no longer processing broker events. Keep the last
    /// observation's normal freshness deadline while transferring the exclusive
    /// journal back from its worker; this is not permission to resume capture.
    pub(crate) async fn quiesce(mut self) {
        self.handover_requested.store(true, Ordering::Release);
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
        self.handover = true;
    }
    /// Await cancellation before transferring observation ownership to another
    /// monitor. Drop also cancels, but never advertises a still-running task as
    /// stopped merely because its JoinHandle was released.
    pub async fn shutdown(mut self) {
        self.capture.source_unavailable();
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}
impl Drop for SourceJournalMonitor {
    fn drop(&mut self) {
        if !self.handover {
            self.capture.source_unavailable();
        }
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

impl LocalEventPublisher {
    #[cfg(test)]
    pub(crate) async fn hold_source_operation_for_test(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.source_operation_gate.clone().lock_owned().await
    }
    pub(crate) async fn retain_journal_recovery(&self,journal:&crate::core::broker::command_journal::BrokerCommandJournal)->Result<(),EventBusError>{
        let owner=self.capture.owner().ok_or_else(||EventBusError::Publish("journal recovery source owner unavailable".into()))?;
        let limit=self.capture.limit().ok_or_else(||EventBusError::Publish("journal recovery source limit unavailable".into()))?;
        let link=journal.recovery_link(&owner,limit).map_err(|error|EventBusError::Publish(error.to_string()))?;
        owner.retain_journal_recovery(&link).await.map_err(|error|EventBusError::Publish(error.to_string()))
    }
    pub(crate) async fn suspend_source_observation(&self) -> Result<(), EventBusError> {
        self.capture.source_unavailable();
        tokio::time::timeout(Duration::from_secs(1), self.persist_source_suspension())
            .await
            .map_err(|_| {
                EventBusError::Publish("source observation suspension commit timed out".into())
            })?
    }
    /// Called only after the live execution worker exited. No journal mutation
    /// is available through the returned guard. This neither clears a real
    /// pause nor supplies a replay/commit barrier.
    pub(crate) async fn seal_final_source_journal(
        &self,
        journal: crate::core::broker::command_journal::BrokerCommandJournal,
    ) -> Result<FinalSourceJournal, EventBusError> {
        // Own cancellation before the first await: dropping this future must
        // close capture before releasing the exclusive journal file.
        let final_owner = FinalSourceJournal {
            _journal: journal,
            capture: self.capture.clone(),
        };
        let _operation = self.source_operation_gate.lock().await;
        let result = async {
            if self.capture.source_monitor_active.load(Ordering::Acquire) {
                return Err(EventBusError::Publish(
                    "source journal monitor still owns observation".into(),
                ));
            }
            let status = final_owner
                ._journal
                .analytics_capture_status()
                .await
                .map_err(|error| EventBusError::Publish(error.to_string()))?
                .ok_or_else(|| {
                    EventBusError::Publish("final source partition is unavailable".into())
                })?;
            // Existing observation interruptions are first recorded durably;
            // a final status must never erase a real pause or a missing window.
            self.observe_source_journal_inner(status).await?;
            self.capture
                .seal_source()
                .map_err(|error| EventBusError::Publish(error.to_string()))?;
            self.capture.close_sealed_source_observation(self.dead_letter_outbox.as_ref().ok_or_else(||EventBusError::Publish("final source observation store unavailable".into()))?)
                .await.map_err(|error|EventBusError::Publish(error.to_string()))?;
            Ok(())
        }
        .await;
        if let Err(error) = result {
            self.capture.source_unavailable();
            let _ = tokio::time::timeout(Duration::from_secs(1), self.persist_source_suspension())
                .await;
            return Err(error);
        }
        Ok(final_owner)
    }
    pub(super) async fn persist_source_suspension(&self) -> Result<(), EventBusError> {
        let outbox = self.dead_letter_outbox.as_ref().ok_or_else(|| {
            EventBusError::Publish("source suspension storage unavailable".into())
        })?;
        self.capture
            .persist_source_suspension(outbox)
            .await
            .map_err(|error| EventBusError::Publish(error.to_string()))
    }
    /// Finite owner operation: all journal permits are released before another
    /// file is touched. Exact target commit precedes the source quality ACK.
    pub async fn refresh_source_journal(
        &self,
        sender: &BrokerCommandSender,
    ) -> Result<(), EventBusError> {
        self.try_refresh_source_journal(sender).await.map_err(SourceObservationError::event)
    }

    /// One optional owner attempt. False means nothing was admitted because
    /// another analytics/recovery operation is busy; the old deadline remains.
    pub(crate) async fn refresh_source_journal_if_available(
        &self, sender: &BrokerCommandSender,
    ) -> Result<bool, EventBusError> {
        match self.try_refresh_source_journal(sender).await {
            Ok(()) => Ok(true),
            Err(SourceObservationError::Deferred) => Ok(false),
            Err(SourceObservationError::Failed(error)) => Err(error),
        }
    }

    async fn try_refresh_source_journal(
        &self,
        sender: &BrokerCommandSender,
    ) -> Result<(), SourceObservationError> {
        // Recovery owns this same operation gate. A monitor tick must not time
        // out behind it and create a new pause that fences the recovery itself.
        let _operation = self.source_operation_gate.try_lock()
            .map_err(|_| SourceObservationError::Deferred)?;
        let status = sender
            .try_analytics_capture_status()?
            .wait()
            .await?
            .ok_or_else(|| {
                EventBusError::Publish("required source journal partition is not installed".into())
            })?;
        let has_pause = status.pause.is_some();
        let receipt = self.observe_source_journal_inner(status).await?;
        if has_pause {
            let receipt = receipt.ok_or_else(|| {
                EventBusError::Publish("source journal quality handoff is at capacity".into())
            })?;
            if !sender
                .try_acknowledge_analytics_capture_handoff(receipt)?
                .wait()
                .await?
            {
                return Err(EventBusError::Publish(
                    "source journal quality acknowledgement was fenced".into(),
                ).into());
            }
            let status = sender
                .try_analytics_capture_status()?
                .wait()
                .await?
                .ok_or_else(|| {
                    EventBusError::Publish("required source journal partition disappeared".into())
                })?;
            self.observe_source_journal_inner(status).await?;
        }
        Ok(())
    }

    /// Explicit opt-in for an already configured single journal owner. This
    /// does not install a budget, clear a pause, or perform broker I/O. Default
    /// publishers create no task and make no extra journal requests.
    pub fn monitor_source_journal(
        &self,
        sender: BrokerCommandSender,
    ) -> Result<SourceJournalMonitor, EventBusError> {
        self.capture
            .validate_source_policy()
            .map_err(|error| EventBusError::Publish(error.to_string()))?;
        if self.source_journal_limit().is_none()
            || self.capture.source_is_sealed()
            || self.capture.owner().is_none()
            || self.dead_letter_outbox.is_none()
            || self.counters.shutting_down.load(Ordering::Acquire)
        {
            return Err(EventBusError::Publish(
                "source journal monitor requires an enabled owned publisher".into(),
            ));
        }
        if self
            .capture
            .source_monitor_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(EventBusError::Publish(
                "source journal monitor already has an owner".into(),
            ));
        }
        let publisher = self.clone();
        let mut shutdown = self.shutdown_sender.subscribe();
        let handover_requested = Arc::new(AtomicBool::new(false));
        let claim = MonitorClaim {
            capture: self.capture.clone(),
            handover_requested: handover_requested.clone(),
        };
        let task = tokio::spawn(async move {
            let _claim = claim;
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    changed=shutdown.changed()=>{if changed.is_err() || *shutdown.borrow(){break;}},
                    _=interval.tick()=>{
                        if *shutdown.borrow(){break;}
                        match tokio::time::timeout(Duration::from_secs(2),publisher.try_refresh_source_journal(&sender)).await {
                            Ok(Ok(()))=>{},
                            Ok(Err(SourceObservationError::Deferred))=>{
                                // A healthy cached sample remains valid through
                                // its original ten-second deadline. At expiry,
                                // the ordinary gate closes and persists a gap.
                                publisher.capture.qualify_source_liveness();
                                let _=tokio::time::timeout(Duration::from_secs(1),publisher.persist_source_suspension()).await;
                            },
                            failure=>{
                                publisher.capture.source_unavailable();
                                let reason = match failure {
                                    Ok(Err(SourceObservationError::Failed(error))) => error.to_string(),
                                    _ => "source journal observation exceeded its two-second deadline".into(),
                                };
                                publisher.capture.note_source_error(reason);
                                let _=tokio::time::timeout(Duration::from_secs(1),publisher.persist_source_suspension()).await;
                            },
                        }
                    }
                }
            }
        });
        Ok(SourceJournalMonitor {
            task: Some(task),
            capture: self.capture.clone(),
            handover: false,
            handover_requested,
        })
    }
}
