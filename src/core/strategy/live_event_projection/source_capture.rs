//! Source observation belongs to the live owner, not a detached worker or the
//! broker's mutation lane. No extra task or journal access on default launches.
use super::*;
use crate::core::broker::{
    command_journal::BrokerCommandJournal, command_worker::BrokerCommandSender,
};

impl LocalBatchSyncState {
    pub(in crate::core::strategy) async fn start_source_observation(
        &mut self,
        sender: BrokerCommandSender,
        preparation: Option<&super::super::command_driver::SourceObservationPreparation>,
    ) -> Result<(), String> {
        if self.publisher.source_journal_limit().is_none() {
            return Ok(());
        }
        if self.source_monitor.is_some() || self.final_source_journal.is_some() {
            return Err("Source observation already has an owner".into());
        }
        if !preparation.is_some_and(|proof|proof.covers(&self.publisher,&self.strategy_id)) {
            return Err("Source observation has no matching pre-dispatch continuity proof".into());
        }
        if !matches!(
            tokio::time::timeout(
                Duration::from_secs(2),
                self.publisher.refresh_source_journal_if_available(&sender)
            )
            .await,
            Ok(Ok(_))
        ) {
            // The pre-dispatch witness or journal pause already makes an
            // unobserved interval recoverable. Close capture and retry without
            // rolling back healthy local execution for this optional store.
            if let Err(error)=self.publisher.suspend_source_observation().await {
                log::warn!("Cloud source observation deferred; durable startup continuity is retained: {error}");
            }
        }
        // A busy attempt is not a broken source: pre-dispatch continuity is
        // already durable, and the monitor retains the original freshness
        // deadline while retrying. No new gap is invented at worker startup.
        self.source_monitor = Some(
            self.publisher
                .monitor_source_journal(sender)
                .map_err(|error| error.to_string())?,
        );
        Ok(())
    }

    pub(in crate::core::strategy) async fn quiesce_source_observation(&mut self) {
        self.capture_recovery.quiesce().await;
        if let Some(monitor) = self.source_monitor.take() {
            monitor.quiesce().await;
        }
    }

    pub(in crate::core::strategy) async fn retain_final_source_journal(
        &mut self,
        journal: BrokerCommandJournal,
    ) -> Result<(), String> {
        if self.publisher.source_journal_limit().is_none() {
            return Ok(());
        }
        match tokio::time::timeout(
            Duration::from_secs(2),
            self.publisher.seal_final_source_journal(journal),
        )
        .await
        {
            Ok(Ok(owner)) => self.final_source_journal = Some(owner),
            Ok(Err(error)) => return Err(error.to_string()),
            Err(_) => {
                self.publisher
                    .suspend_source_observation()
                    .await
                    .map_err(|error| error.to_string())?;
                return Err(
                    "Final source observation timed out; cloud capture remains paused".into(),
                );
            }
        }
        Ok(())
    }
}
