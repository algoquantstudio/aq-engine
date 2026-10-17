//! One coalesced stop request, independent of full command/completion channels.
use super::*;
use crate::core::broker::command_journal::lifecycle::ExecutionPhase;
use std::sync::atomic::{AtomicBool, Ordering};

pub(super) struct StopFence {
    requested: AtomicBool,
    persisted: AtomicBool,
    failure: parking_lot::Mutex<Option<BrokerCommandError>>,
    pub notify: tokio::sync::Notify,
}

impl StopFence {
    pub fn new(phase: ExecutionPhase) -> Self {
        let stopped = phase != ExecutionPhase::Running;
        Self {
            requested: AtomicBool::new(stopped),
            persisted: AtomicBool::new(stopped),
            failure: parking_lot::Mutex::new(None),
            notify: tokio::sync::Notify::new(),
        }
    }

    pub fn request(&self) {
        if !self.requested.swap(true, Ordering::AcqRel) {
            self.notify.notify_one();
        }
    }

    pub fn pending(&self) -> bool {
        self.requested.load(Ordering::Acquire) && !self.persisted.load(Ordering::Acquire)
    }

    pub fn error(&self) -> Option<BrokerCommandError> {
        if self.pending() {
            self.failure.lock().clone()
        } else {
            None
        }
    }

    pub async fn persist(
        &self,
        journal: &mut BrokerCommandJournal,
        ready: &tokio::sync::Notify,
    ) -> Result<(), BrokerCommandError> {
        if !self.pending() {
            return Ok(());
        }
        if let Some(error) = self.error() {
            return Err(error);
        }
        match journal.request_execution_stop().await {
            Ok(()) => self.persisted.store(true, Ordering::Release),
            Err(error) => {
                *self.failure.lock() = Some(error.clone());
                ready.notify_one();
                return Err(error);
            }
        }
        ready.notify_one();
        Ok(())
    }
}
