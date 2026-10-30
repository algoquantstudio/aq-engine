use super::*;
use crate::core::broker::commands::BrokerCommandAction;

fn pending(order: &str) -> OrderLifecycleSnapshot {
    OrderLifecycleSnapshot {
        order_id: order.into(),
        status: OrderLifecycleStatus::Pending,
        position_id: None,
        position_qty: None,
        symbol: Some("BTCUSD".into()),
        detail: None,
        close_details: None,
    }
}

#[tokio::test]
async fn cancel_recovery_preserves_original_ambiguity_until_confirmed_and_survives_restart() {
    let root = std::env::temp_dir().join(format!("aqe-cancel-recovery-{}", uuid::Uuid::new_v4()));
    let path = root.join("commands.db");
    let command = BrokerCommand::new(
        "strategy",
        "account",
        "owner",
        BrokerCommandAction::Cancel {
            order_id: "2025202590".into(),
        },
        None,
    );
    let mut journal = BrokerCommandJournal::open(&path, "strategy", "account")
        .await
        .unwrap();
    journal.prepare(&command, 1).await.unwrap();
    journal.mark_dispatched(&command.id, 2).await.unwrap();
    let old = BrokerCommandCompletion {
        receipt: command.receipt(),
        queued_at_us: 1,
        dispatched_at_us: Some(2),
        completed_at_us: 3,
        outcome: BrokerCommandOutcome::Ambiguous {
            reason: "old MT5 timeout".into(),
        },
    };
    journal.complete(&old).await.unwrap();
    journal.request_execution_stop().await.unwrap();
    let proof = pending("2025202590");
    for mut invalid in [pending("other-ticket"), pending("2025202590")] {
        if invalid.order_id == "2025202590" {
            invalid.status = OrderLifecycleStatus::Unknown;
        }
        assert!(
            journal
                .begin_cancellation_recovery(&command, &invalid, 4)
                .await
                .is_err()
        );
    }
    let mut foreign = command.clone();
    foreign.account_id = "other-account".into();
    assert!(
        journal
            .begin_cancellation_recovery(&foreign, &proof, 4)
            .await
            .is_err()
    );
    let mut submit = command.clone();
    submit.action = BrokerCommandAction::Close {
        order_id: "2025202590".into(),
        quantity: 1.,
        price: None,
    };
    assert!(
        journal
            .begin_cancellation_recovery(&submit, &proof, 4)
            .await
            .is_err()
    );
    let attempt = journal
        .begin_cancellation_recovery(&command, &proof, 4)
        .await
        .unwrap()
        .unwrap();
    assert!(
        journal
            .finish_cancellation_recovery(&command, attempt, &old)
            .await
            .unwrap()
            .is_some()
    );
    assert!(!journal.complete_execution_stop().await.unwrap());
    let attempt = journal
        .begin_cancellation_recovery(&command, &proof, 5)
        .await
        .unwrap()
        .unwrap();
    drop(journal); // crash before retry returns: retain old ambiguity and stop fence.
    let mut journal = BrokerCommandJournal::open(&path, "strategy", "account")
        .await
        .unwrap();
    let saved = journal.get(&command.id).await.unwrap().unwrap();
    assert!(matches!(
        saved.completion.unwrap().outcome,
        BrokerCommandOutcome::Ambiguous { .. }
    ));
    assert_eq!(
        journal.execution_lifecycle().phase,
        super::super::lifecycle::ExecutionPhase::Stopping
    );
    let mut confirmed = old.clone();
    confirmed.completed_at_us = 6;
    confirmed.outcome = BrokerCommandOutcome::Acknowledged;
    let result = journal
        .finish_cancellation_recovery(&command, attempt, &confirmed)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.receipt, old.receipt);
    assert_eq!(result.dispatched_at_us, Some(2));
    let mut rows = journal
        .connection
        .query(
            "SELECT COUNT(*),MIN(previous_completion_json) FROM broker_cancel_recovery_attempts",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 2);
    assert!(row.get::<String>(1).unwrap().contains("old MT5 timeout"));
    drop(rows);
    assert_eq!(
        journal.get(&command.id).await.unwrap().unwrap().state,
        JournalCommandState::Completed,
        "confirmation must still await owner checkpoint acknowledgement"
    );
    drop(journal);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn native_terminal_checkpoint_wins_over_a_late_cancellation_retry_result() {
    use crate::core::insight::{
        Insight, execution_checkpoint::InsightExecutionCheckpoint, types::InsightState,
    };
    let root =
        std::env::temp_dir().join(format!("aqe-cancel-terminal-race-{}", uuid::Uuid::new_v4()));
    let mut journal = BrokerCommandJournal::open(root.join("commands.db"), "strategy", "account")
        .await
        .unwrap();
    let mut row = Insight::default();
    row.symbol = "BTCUSD".into();
    row.order_id = Some("2025202590".into());
    row.state = InsightState::Cancelled;
    let command = BrokerCommand::new(
        "strategy",
        "account",
        &row.insight_id.to_string(),
        BrokerCommandAction::Cancel {
            order_id: "2025202590".into(),
        },
        None,
    );
    journal.prepare(&command, 1).await.unwrap();
    journal.mark_dispatched(&command.id, 2).await.unwrap();
    let old = BrokerCommandCompletion {
        receipt: command.receipt(),
        queued_at_us: 1,
        dispatched_at_us: Some(2),
        completed_at_us: 3,
        outcome: BrokerCommandOutcome::Ambiguous {
            reason: "lost response".into(),
        },
    };
    journal.complete(&old).await.unwrap();
    let attempt = journal
        .begin_cancellation_recovery(&command, &pending("2025202590"), 4)
        .await
        .unwrap()
        .unwrap();
    journal
        .persist_execution_batch(
            &ExecutionCheckpointBatch::new(
                vec![InsightExecutionCheckpoint::capture(&row, 1, true, None)],
                vec![command.receipt()],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let mut result = old.clone();
    result.outcome = BrokerCommandOutcome::Acknowledged;
    assert!(
        journal
            .finish_cancellation_recovery(&command, attempt, &result)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        journal.get(&command.id).await.unwrap().unwrap().state,
        JournalCommandState::Applied
    );
    assert!(
        journal
            .begin_cancellation_recovery(&command, &pending("2025202590"), 5)
            .await
            .unwrap()
            .is_none()
    );
    drop(journal);
    std::fs::remove_dir_all(root).unwrap();
}
