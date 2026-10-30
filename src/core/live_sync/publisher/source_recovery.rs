//! Source-first local completion. The destination pause remains the durable
//! gate if completion is cancelled or the process exits between transitions.
use super::*;
use crate::core::broker::command_worker::BrokerCommandSender;

impl LocalEventPublisher {
    fn source_recovery_destination(&self, generation: u64) -> bool {
        let status = self.capture.status();
        let Some(owner) = self.capture.owner() else {
            return false;
        };
        status.state == CloudCaptureState::ReconciliationRequired
            && status.pause.as_ref().is_some_and(|pause| {
                pause.generation == generation
                    && pause.runtime_id == owner.runtime
                    && pause.strategy_id == owner.strategy
                    && pause.live_session_id == owner.session
                    && pause.owner_run_id.as_deref() == Some(owner.run.as_str())
            })
    }

    /// Call only after this generation's current-state replay and quality
    /// notice have durable local receipts. Existing server-side replay barriers
    /// remain separate; the diagnostic receipt is not a server commit receipt.
    pub async fn complete_capture_reconciliation_with_source(
        &self,
        generation: u64,
        sender: &BrokerCommandSender,
    ) -> Result<bool, EventBusError> {
        if self.source_journal_limit().is_none() {
            return self.complete_capture_reconciliation(generation).await;
        }
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            self.complete_source_inner(generation, sender),
        )
        .await;
        match result {
            Ok(result) => result,
            Err(_) => {
                self.capture.source_unavailable();
                let _ =
                    tokio::time::timeout(Duration::from_secs(1), self.persist_source_suspension())
                        .await;
                Err(EventBusError::Publish(
                    "source capture completion timed out; capture remains paused".into(),
                ))
            }
        }
    }

    async fn complete_source_inner(
        &self,
        generation: u64,
        sender: &BrokerCommandSender,
    ) -> Result<bool, EventBusError> {
        let _operation = self.source_operation_gate.lock().await;
        if !self.source_recovery_destination(generation) {
            return Ok(false);
        }
        let failure =
            |_| EventBusError::Publish("source capture completion request unavailable".into());
        let source = sender
            .try_analytics_capture_status()
            .map_err(failure)?
            .wait()
            .await
            .map_err(failure)?
            .ok_or_else(|| {
                EventBusError::Publish("required source capture partition is absent".into())
            })?;
        // Preserve any observer interruption before a new status can qualify.
        // This can advance the destination fence, so recheck it afterwards.
        self.observe_source_journal_inner(source.clone()).await?;
        if !self.source_recovery_destination(generation) {
            return Ok(false);
        }
        if let Some(pause) = source.pause.as_ref() {
            let Some(receipt) = source
                .handoff_receipt
                .as_ref()
                .filter(|receipt| receipt.covers(pause))
            else {
                return Ok(false);
            };
            if !source.physical.below_resume_threshold() {
                return Ok(false);
            }
            if !sender
                .try_complete_analytics_capture(receipt.clone())
                .map_err(failure)?
                .wait()
                .await
                .map_err(failure)?
            {
                return Ok(false);
            }
            // The source is now clear, but capture remains blocked by the
            // destination. Cancellation here is recovered from that open gap.
            let current = sender
                .try_analytics_capture_status()
                .map_err(failure)?
                .wait()
                .await
                .map_err(failure)?
                .ok_or_else(|| {
                    EventBusError::Publish(
                        "source capture partition disappeared during completion".into(),
                    )
                })?;
            if current.journal_source_id != source.journal_source_id
                || current.generation != source.generation
                || current.pause.is_some()
            {
                return Ok(false);
            }
            self.observe_source_journal_inner(current).await?;
        }
        if !self.source_recovery_destination(generation) {
            return Ok(false);
        }
        self.complete_capture_reconciliation(generation).await
    }
}
