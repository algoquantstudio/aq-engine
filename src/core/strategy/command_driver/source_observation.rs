//! Startup-only continuity proof. No network, per-event queue or billing state.
use crate::core::{
    broker::{command_journal::{AnalyticsCapturePause, BrokerCommandJournal}, commands::BrokerCommandError},
    live_sync::{LocalEventPublisher, PhysicalCaptureLimit},
};
use std::time::Duration;
#[cfg(test)]
mod tests;

#[derive(Clone)]
enum Proof {
    Publisher(uuid::Uuid),
    Journal(AnalyticsCapturePause),
}

/// Created only before the command worker can dispatch. Later observation may
/// retry without turning a cloud-only failure into a trading startup failure.
#[derive(Clone)]
pub(crate) struct SourceObservationPreparation {
    source: String,
    strategy: String,
    account: String,
    limit: PhysicalCaptureLimit,
    publisher: uuid::Uuid,
    proof: Proof,
}

impl SourceObservationPreparation {
    pub(crate) fn covers(&self, publisher: &LocalEventPublisher, strategy: &str) -> bool {
        fn scope(value: &str) -> &str {
            value.trim().strip_prefix("strategy:").unwrap_or(value.trim())
        }
        if scope(strategy) != scope(&self.strategy)
            || publisher.source_observation_instance_for(&self.strategy, self.limit) != Some(self.publisher)
        {
            return false;
        }
        match &self.proof {
            Proof::Publisher(token) => publisher.prepared_source_observation_id(
                &self.source, &self.strategy, &self.account,
            ) == Some(*token),
            Proof::Journal(pause) => pause.validate().is_ok()
                && pause.journal_source_id == self.source
                && pause.strategy_id == self.strategy
                && pause.account_id == self.account,
        }
    }
}

pub(super) async fn prepare(
    journal: &BrokerCommandJournal,
    publisher: &LocalEventPublisher,
) -> Result<SourceObservationPreparation, BrokerCommandError> {
    let invalid = |message: &str| BrokerCommandError::Journal(message.into());
    let limit = journal.analytics_capture_budget()
        .ok_or_else(|| invalid("source observation requires a journal capture partition"))?;
    let (source, strategy, account) = journal.analytics_capture_identity();
    let instance = publisher.source_observation_instance_for(strategy, limit)
        .ok_or_else(|| invalid("source observation owner or partition does not match the journal"))?;
    // Release every source read/permit before awaiting the separate publisher
    // file. The deadline is for optional quality work, never a broker request.
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        let report = journal.analytics_capture_status().await?
            .ok_or_else(|| invalid("source journal capture partition is unavailable"))?;
        publisher.observe_source_journal(report).await
            .map_err(|error| invalid(&error.to_string()))?;
        Ok::<(), BrokerCommandError>(())
    }).await;
    let proof = match publisher.prepared_source_observation_id(source, strategy, account) {
        Some(token) => Proof::Publisher(token),
        None => {
            let pause = journal.suspend_analytics_for_source_initialization().await?;
            // The local pause survives process death, but the importer must
            // also be able to find that exact journal without scanning projects.
            tokio::time::timeout(Duration::from_secs(2),publisher.retain_journal_recovery(journal)).await
                .map_err(|_|invalid("journal recovery link commit timed out"))?
                .map_err(|_|invalid("journal recovery link is not durably discoverable"))?;
            publisher.cloud_capture_gate().note_journal_pause(pause.clone());
            Proof::Journal(pause)
        }
    };
    Ok(SourceObservationPreparation {
        source: source.into(), strategy: strategy.into(), account: account.into(),
        limit, publisher: instance, proof,
    })
}
