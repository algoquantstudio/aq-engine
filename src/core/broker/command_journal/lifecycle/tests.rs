use super::*;
use crate::core::{
    broker::commands::BrokerCommandAction,
    insight::{Insight, execution_checkpoint::InsightExecutionCheckpoint, types::InsightState},
};

struct Fixture(std::path::PathBuf);
impl Fixture {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("aqe-lifecycle-{}", uuid::Uuid::new_v4())))
    }
    async fn open(&self) -> BrokerCommandJournal {
        BrokerCommandJournal::open(self.0.join("commands.db"), "strategy", "account")
            .await
            .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn insight(state: InsightState) -> Insight {
    let mut row = Insight::default();
    row.symbol = "ETHUSD".into();
    row.quantity = Some(1.0);
    row.state = state;
    row
}
async fn save(journal: &mut BrokerCommandJournal, row: &Insight, revision: u64) {
    journal
        .persist_execution_batch(
            &ExecutionCheckpointBatch::new(
                vec![InsightExecutionCheckpoint::capture(
                    row, revision, true, None,
                )],
                vec![],
            )
            .unwrap(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn stop_is_idempotent_and_only_completed_epochs_can_restart() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    assert_eq!(journal.execution_lifecycle().epoch, 1);
    assert!(journal.complete_execution_stop().await.is_err());
    journal.request_execution_stop().await.unwrap();
    let stopped = journal.execution_lifecycle();
    journal.request_execution_stop().await.unwrap();
    assert_eq!(stopped, journal.execution_lifecycle());
    drop(journal);
    let mut journal = fixture.open().await;
    assert_eq!(journal.begin_execution_epoch().await.unwrap(), stopped);
    assert_eq!(
        journal.execution_lifecycle().phase,
        ExecutionPhase::Stopping
    );
    assert!(journal.complete_execution_stop().await.unwrap());
    drop(journal);
    let mut journal = fixture.open().await;
    assert_eq!(journal.execution_lifecycle().phase, ExecutionPhase::Stopped);
    let next = journal.begin_execution_epoch().await.unwrap();
    assert_eq!(next.epoch, 2);
    assert_eq!(next.phase, ExecutionPhase::Running);
    assert!(next.stop_requested_at_us.is_none());
}

#[tokio::test]
async fn unresolved_commands_and_exposure_prevent_stop_completion() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let mut entry = insight(InsightState::New);
    let mut exposure = insight(InsightState::Filled);
    save(&mut journal, &exposure, 1).await;
    let command = BrokerCommand::new(
        "strategy",
        "account",
        &entry.insight_id.to_string(),
        BrokerCommandAction::Submit {
            request: (&entry).into(),
        },
        None,
    );
    journal.prepare(&command, 1).await.unwrap();
    journal.request_execution_stop().await.unwrap();
    assert!(!journal.complete_execution_stop().await.unwrap());
    let completion = BrokerCommandCompletion {
        receipt: command.receipt(),
        queued_at_us: 1,
        dispatched_at_us: None,
        completed_at_us: 2,
        outcome: BrokerCommandOutcome::CancelledBeforeDispatch,
    };
    journal.complete(&completion).await.unwrap();
    assert!(
        !journal.complete_execution_stop().await.unwrap(),
        "unapplied evidence is still owned"
    );
    entry.state = InsightState::Cancelled;
    journal
        .persist_execution_batch(
            &ExecutionCheckpointBatch::new(
                vec![InsightExecutionCheckpoint::capture(&entry, 1, true, None)],
                vec![command.receipt()],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        !journal.complete_execution_stop().await.unwrap(),
        "filled exposure still needs broker evidence"
    );
    exposure.state = InsightState::Closed;
    save(&mut journal, &exposure, 2).await;
    assert!(journal.complete_execution_stop().await.unwrap());
    assert_eq!(
        journal
            .execution_checkpoint_page("", 32)
            .await
            .unwrap()
            .rows
            .len(),
        2,
        "lifecycle completion must not delete history/checkpoints"
    );
}

#[tokio::test]
async fn next_epoch_rechecks_settlement_instead_of_trusting_a_phase_label() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    journal.request_execution_stop().await.unwrap();
    assert!(journal.complete_execution_stop().await.unwrap());
    save(&mut journal, &insight(InsightState::New), 1).await;
    assert!(journal.begin_execution_epoch().await.is_err());
    assert_eq!(journal.execution_lifecycle().epoch, 1);
    assert!(!journal.complete_execution_stop().await.unwrap());
}

#[tokio::test]
async fn failed_or_conflicting_stop_writes_are_never_acknowledged() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    journal
        .connection
        .execute("PRAGMA query_only=1", ())
        .await
        .unwrap();
    assert!(journal.request_execution_stop().await.is_err());
    assert_eq!(journal.execution_lifecycle().phase, ExecutionPhase::Running);
    journal
        .connection
        .execute("PRAGMA query_only=0", ())
        .await
        .unwrap();
    journal
        .connection
        .execute("UPDATE broker_execution_lifecycle SET version=99", ())
        .await
        .unwrap();
    assert!(journal.request_execution_stop().await.is_err());
    assert_eq!(journal.execution_lifecycle().phase, ExecutionPhase::Running);
    drop(journal);
    assert!(
        BrokerCommandJournal::open(fixture.0.join("commands.db"), "strategy", "account")
            .await
            .is_err()
    );
}
