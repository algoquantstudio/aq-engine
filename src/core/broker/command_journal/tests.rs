use super::*;
use crate::core::{
    broker::commands::BrokerCommandAction,
    insight::{Insight, execution_checkpoint::InsightExecutionCheckpoint},
};

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("aqe-journal-replay-{}", uuid::Uuid::new_v4())))
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

fn entry() -> (Insight, BrokerCommand) {
    let mut insight = Insight::default();
    insight.symbol = "ETHUSD".into();
    insight.quantity = Some(1.0);
    let command = BrokerCommand::new(
        "strategy",
        "account",
        &insight.insight_id.to_string(),
        BrokerCommandAction::Submit {
            request: (&insight).into(),
        },
        None,
    );
    (insight, command)
}

async fn sequence(journal: &BrokerCommandJournal) -> i64 {
    let mut rows = journal
        .connection
        .query(
            "SELECT seq FROM sqlite_sequence WHERE name='broker_commands'",
            (),
        )
        .await
        .unwrap();
    rows.next().await.unwrap().unwrap().get(0).unwrap()
}

#[tokio::test]
async fn checkpointed_prepare_replays_all_states_without_another_sequence_write() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let (insight, command) = entry();
    let mut checkpoint = InsightExecutionCheckpoint::capture(&insight, 1, true, None);
    checkpoint.owner_receipts.push(command.receipt());
    journal
        .persist_execution_batch(
            &ExecutionCheckpointBatch::with_intents(
                vec![checkpoint],
                vec![],
                vec![ExecutionIntent {
                    command: command.clone(),
                    queued_at_us: 11,
                    admission_order: 1,
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let original_sequence = sequence(&journal).await;
    let completion = BrokerCommandCompletion {
        receipt: command.receipt(),
        queued_at_us: 11,
        dispatched_at_us: Some(12),
        completed_at_us: 13,
        outcome: BrokerCommandOutcome::Acknowledged,
    };
    for expected in [
        JournalCommandState::Queued,
        JournalCommandState::Dispatching,
        JournalCommandState::Completed,
        JournalCommandState::Applied,
    ] {
        match expected {
            JournalCommandState::Queued => {}
            JournalCommandState::Dispatching => {
                journal.mark_dispatched(&command.id, 12).await.unwrap();
            }
            JournalCommandState::Completed => {
                journal.complete(&completion).await.unwrap();
            }
            JournalCommandState::Applied => {
                journal
                    .persist_execution_batch(
                        &ExecutionCheckpointBatch::new(
                            vec![InsightExecutionCheckpoint::capture(&insight, 2, true, None)],
                            vec![command.receipt()],
                        )
                        .unwrap(),
                    )
                    .await
                    .unwrap();
            }
        }
        let record = journal.prepare(&command, 999).await.unwrap();
        assert_eq!(record.command, command);
        assert_eq!(record.state, expected);
        assert_eq!(record.sequence, original_sequence);
        assert_eq!(record.queued_at_us, 11);
        assert_eq!(
            record.dispatched_at_us,
            (expected != JournalCommandState::Queued).then_some(12)
        );
        if matches!(
            expected,
            JournalCommandState::Completed | JournalCommandState::Applied
        ) {
            assert_eq!(
                serde_json::to_string(record.completion.as_ref().unwrap()).unwrap(),
                serde_json::to_string(&completion).unwrap()
            );
        }
        assert_eq!(sequence(&journal).await, original_sequence);
    }
    drop(journal);
    let mut journal = fixture.open().await;
    let record = journal.prepare(&command, 1000).await.unwrap();
    assert_eq!(record.state, JournalCommandState::Applied);
    assert_eq!(record.queued_at_us, 11);
    assert_eq!(sequence(&journal).await, original_sequence);
    let (_, next) = entry();
    let next_record = journal.prepare(&next, 20).await.unwrap();
    assert!(next_record.sequence > original_sequence);
    let page = journal.recovery_page(original_sequence, 1).await.unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].command, next);
    assert_eq!(page[0].queued_at_us, 20);
}

#[tokio::test]
async fn prepare_replay_keeps_identity_validation_and_payload_limit() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let (_, command) = entry();
    let original = journal.prepare(&command, 11).await.unwrap();
    let mut conflicting = command.clone();
    if let BrokerCommandAction::Submit { request } = &mut conflicting.action {
        request.quantity = Some(2.0);
    }
    assert_eq!(
        journal.prepare(&conflicting, 99).await.unwrap_err(),
        BrokerCommandError::IdentityConflict
    );
    let mut invalid = command.clone();
    invalid.version = 0;
    assert!(matches!(
        journal.prepare(&invalid, 99).await,
        Err(BrokerCommandError::Invalid(_))
    ));
    let mut oversized = command.clone();
    if let BrokerCommandAction::Submit { request } = &mut oversized.action {
        request.strategy_type = "\u{1}".repeat(512);
        request.symbol = "\u{1}".repeat(128);
        request.take_profit_levels = Some(vec![f64::MAX; 256]);
        request.stop_loss_levels = Some(vec![f64::MAX; 256]);
    }
    oversized.validate().unwrap();
    assert_eq!(
        oversized.encode().unwrap_err(),
        BrokerCommandError::TooLarge
    );
    assert_eq!(
        journal.prepare(&oversized, 99).await.unwrap_err(),
        BrokerCommandError::TooLarge
    );
    let saved = journal.get(&command.id).await.unwrap().unwrap();
    assert_eq!(saved.command, command);
    assert_eq!(saved.state, JournalCommandState::Queued);
    assert_eq!(saved.queued_at_us, 11);
    assert_eq!(sequence(&journal).await, original.sequence);
}

#[tokio::test]
async fn prepare_does_not_replay_an_intent_from_an_abandoned_checkpoint() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let (insight, command) = entry();
    // The intent is inserted before its row fails the first-revision check.
    let mut checkpoint = InsightExecutionCheckpoint::capture(&insight, 2, true, None);
    checkpoint.owner_receipts.push(command.receipt());
    let failed = ExecutionCheckpointBatch::with_intents(
        vec![checkpoint],
        vec![],
        vec![ExecutionIntent {
            command: command.clone(),
            queued_at_us: 11,
            admission_order: 1,
        }],
    )
    .unwrap();
    assert!(journal.persist_execution_batch(&failed).await.is_err());
    // No intervening query may perform the deferred rollback for prepare.
    let saved = journal.prepare(&command, 22).await.unwrap();
    assert_eq!(saved.queued_at_us, 22);
    assert_eq!(saved.state, JournalCommandState::Queued);
    assert_eq!(journal.last_admission_order().await.unwrap(), 0);
    assert!(
        journal
            .execution_checkpoint_page("", 32)
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    drop(journal);
    let mut journal = fixture.open().await;
    let replay = journal.prepare(&command, 33).await.unwrap();
    assert_eq!(replay.sequence, saved.sequence);
    assert_eq!(replay.queued_at_us, 22);
}

#[tokio::test]
async fn cached_command_queries_discard_failed_checkpoints_before_read_and_dispatch() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let (_, first) = entry();
    let (_, second) = entry();
    journal.prepare(&first, 11).await.unwrap();
    journal.prepare(&second, 21).await.unwrap();
    journal.mark_dispatched(&first.id, 12).await.unwrap();

    let (insight, abandoned) = entry();
    let mut checkpoint = InsightExecutionCheckpoint::capture(&insight, 2, true, None);
    checkpoint.owner_receipts.push(abandoned.receipt());
    let failed = ExecutionCheckpointBatch::with_intents(
        vec![checkpoint],
        vec![],
        vec![ExecutionIntent {
            command: abandoned.clone(),
            queued_at_us: 31,
            admission_order: 1,
        }],
    )
    .unwrap();
    assert!(journal.persist_execution_batch(&failed).await.is_err());
    // The cached read itself must perform the abandoned transaction's rollback.
    assert!(journal.get(&abandoned.id).await.unwrap().is_none());
    assert!(journal.persist_execution_batch(&failed).await.is_err());
    // A direct cached mutation must also roll back first, then commit on its own.
    journal.mark_dispatched(&second.id, 22).await.unwrap();
    assert!(journal.get(&abandoned.id).await.unwrap().is_none());
    assert_eq!(journal.last_admission_order().await.unwrap(), 0);
    drop(journal);

    let journal = fixture.open().await;
    for (command, queued, dispatched) in [(&first, 11, 12), (&second, 21, 22)] {
        let saved = journal.get(&command.id).await.unwrap().unwrap();
        assert_eq!(saved.command, *command);
        assert_eq!(saved.state, JournalCommandState::Dispatching);
        assert_eq!(saved.queued_at_us, queued);
        assert_eq!(saved.dispatched_at_us, Some(dispatched));
    }
    assert!(journal.get(&abandoned.id).await.unwrap().is_none());
    assert!(
        journal
            .execution_checkpoint_page("", 32)
            .await
            .unwrap()
            .rows
            .is_empty()
    );
}

#[tokio::test]
async fn cached_command_completions_keep_distinct_bindings_and_null_dispatch_times() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let (_, first) = entry();
    let (_, second) = entry();
    journal.prepare(&first, 11).await.unwrap();
    journal.prepare(&second, 21).await.unwrap();
    journal.mark_dispatched(&first.id, 12).await.unwrap();
    let completed = BrokerCommandCompletion {
        receipt: first.receipt(),
        queued_at_us: 11,
        dispatched_at_us: Some(12),
        completed_at_us: 13,
        outcome: BrokerCommandOutcome::Acknowledged,
    };
    let cancelled = BrokerCommandCompletion {
        receipt: second.receipt(),
        queued_at_us: 21,
        dispatched_at_us: None,
        completed_at_us: 23,
        outcome: BrokerCommandOutcome::CancelledBeforeDispatch,
    };
    journal.complete(&completed).await.unwrap();
    journal.complete(&cancelled).await.unwrap();
    journal.complete(&completed).await.unwrap();
    journal.complete(&cancelled).await.unwrap();
    let mut conflicting = cancelled.clone();
    conflicting.queued_at_us = 11;
    assert_eq!(
        journal.complete(&conflicting).await.unwrap_err(),
        BrokerCommandError::IdentityConflict
    );
    drop(journal);

    let journal = fixture.open().await;
    for completion in [completed, cancelled] {
        let saved = journal.get(&completion.receipt.id).await.unwrap().unwrap();
        assert_eq!(saved.state, JournalCommandState::Completed);
        assert_eq!(saved.command.receipt(), completion.receipt);
        assert_eq!(saved.queued_at_us, completion.queued_at_us);
        assert_eq!(saved.dispatched_at_us, completion.dispatched_at_us);
        assert_eq!(
            serde_json::to_string(saved.completion.as_ref().unwrap()).unwrap(),
            serde_json::to_string(&completion).unwrap()
        );
    }
}
