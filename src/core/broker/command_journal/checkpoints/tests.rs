use super::*;

#[tokio::test]
async fn pending_state_and_intent_are_atomic_and_replay_does_not_reset_dispatch() {
    use crate::core::insight::operations::{MutationEffect, MutationKind, PendingInsightMutation};
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let mut row = insight();
    let command = BrokerCommand::new(
        "strategy",
        "account",
        &row.insight_id.to_string(),
        BrokerCommandAction::Submit {
            request: (&row).into(),
        },
        None,
    );
    row.pending_mutations.push(PendingInsightMutation {
        kind: MutationKind::Submit,
        receipt: command.receipt(),
        effect: MutationEffect::Submit,
    });
    let batch = ExecutionCheckpointBatch::with_intents(
        vec![checkpoint(&row, 1)],
        vec![],
        vec![ExecutionIntent {
            command: command.clone(),
            queued_at_us: 123,
            admission_order: 1,
        }],
    )
    .unwrap();
    journal.persist_execution_batch(&batch).await.unwrap();
    drop(journal);
    let mut journal = fixture.open().await;
    let record = journal.get(&command.id).await.unwrap().unwrap();
    assert_eq!(record.state, JournalCommandState::Queued);
    assert_eq!(record.queued_at_us, 123);
    let page = journal.execution_checkpoint_page("", 32).await.unwrap();
    assert_eq!(page.rows[0].pending_mutations[0].receipt, command.receipt());
    journal.mark_dispatched(&command.id, 124).await.unwrap();
    journal.persist_execution_batch(&batch).await.unwrap();
    assert_eq!(
        journal.get(&command.id).await.unwrap().unwrap().state,
        JournalCommandState::Dispatching
    );
    // A conflicted state revision after inserting a NEW command must roll back
    // that new intent too; it must never become dispatchable without its owner.
    let second = BrokerCommand::new(
        "strategy",
        "account",
        &row.insight_id.to_string(),
        BrokerCommandAction::Cancel {
            order_id: "ticket".into(),
        },
        None,
    );
    row.pending_mutations.push(PendingInsightMutation {
        kind: MutationKind::Cancel,
        receipt: second.receipt(),
        effect: MutationEffect::Cancel,
    });
    let conflict = ExecutionCheckpointBatch::with_intents(
        vec![checkpoint(&row, 1)],
        vec![],
        vec![ExecutionIntent {
            command: second.clone(),
            queued_at_us: 125,
            admission_order: 2,
        }],
    )
    .unwrap();
    assert!(journal.persist_execution_batch(&conflict).await.is_err());
    assert!(journal.get(&second.id).await.unwrap().is_none());
    assert_eq!(
        journal
            .execution_checkpoint_page("", 32)
            .await
            .unwrap()
            .rows[0]
            .pending_mutations
            .len(),
        1
    );
}

#[tokio::test]
async fn checkpoint_intent_requires_matching_scope_and_pending_owner() {
    use crate::core::insight::operations::{MutationEffect, MutationKind, PendingInsightMutation};
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let mut row = insight();
    let command = BrokerCommand::new(
        "different-strategy",
        "account",
        &row.insight_id.to_string(),
        BrokerCommandAction::Submit {
            request: (&row).into(),
        },
        None,
    );
    let missing_owner = ExecutionCheckpointBatch::with_intents(
        vec![checkpoint(&row, 1)],
        vec![],
        vec![ExecutionIntent {
            command: command.clone(),
            queued_at_us: 1,
            admission_order: 1,
        }],
    );
    assert!(missing_owner.is_err());
    row.pending_mutations.push(PendingInsightMutation {
        kind: MutationKind::Submit,
        receipt: command.receipt(),
        effect: MutationEffect::Submit,
    });
    let wrong_scope = ExecutionCheckpointBatch::with_intents(
        vec![checkpoint(&row, 1)],
        vec![],
        vec![ExecutionIntent {
            command: command.clone(),
            queued_at_us: 1,
            admission_order: 1,
        }],
    )
    .unwrap();
    assert!(journal.persist_execution_batch(&wrong_scope).await.is_err());
    assert!(journal.recovery_page(0, 256).await.unwrap().is_empty());
    assert!(
        journal
            .execution_checkpoint_page("", 32)
            .await
            .unwrap()
            .rows
            .is_empty()
    );
}
use crate::core::{
    broker::commands::{BrokerCommandAction, BrokerCommandCompletion, BrokerCommandOutcome},
    insight::{Insight, types::InsightState},
};

struct Fixture(std::path::PathBuf);
impl Fixture {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("aqe-checkpoint-test-{}", uuid::Uuid::new_v4())))
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

fn insight() -> Insight {
    let mut insight = Insight::default();
    insight.symbol = "ETHUSD".into();
    insight.quantity = Some(2.0);
    insight.limit_price = Some(2490.1234567890123);
    insight
}
fn checkpoint(insight: &Insight, revision: u64) -> InsightExecutionCheckpoint {
    InsightExecutionCheckpoint::capture(insight, revision, true, None)
}
async fn completed(
    journal: &mut BrokerCommandJournal,
    insight: &Insight,
    ambiguous: bool,
) -> BrokerCommandReceipt {
    let command = BrokerCommand::new(
        "strategy",
        "account",
        &insight.insight_id.to_string(),
        BrokerCommandAction::Submit {
            request: insight.into(),
        },
        None,
    );
    journal.prepare(&command, 1).await.unwrap();
    journal.mark_dispatched(&command.id, 2).await.unwrap();
    let receipt = command.receipt();
    journal
        .complete(&BrokerCommandCompletion {
            receipt: receipt.clone(),
            queued_at_us: 1,
            dispatched_at_us: Some(2),
            completed_at_us: 3,
            outcome: if ambiguous {
                BrokerCommandOutcome::Ambiguous {
                    reason: "timeout".into(),
                }
            } else {
                BrokerCommandOutcome::Acknowledged
            },
        })
        .await
        .unwrap();
    receipt
}

#[tokio::test]
async fn checkpoint_and_receipts_commit_together_and_survive_reopen() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let mut row = insight();
    let receipt = completed(&mut journal, &row, false).await;
    row.state = InsightState::Executed;
    row.order_id = Some("broker-ticket".into());
    row.submitted = true;
    row.suppress_child_entries = true;
    let batch =
        ExecutionCheckpointBatch::new(vec![checkpoint(&row, 1)], vec![receipt.clone()]).unwrap();
    journal.persist_execution_batch(&batch).await.unwrap();
    drop(journal);
    let mut journal = fixture.open().await;
    let page = journal.execution_checkpoint_page("", 32).await.unwrap();
    assert_eq!(page.rows.len(), 1);
    let (restored, children, pipeline, materialized) =
        page.rows.into_iter().next().unwrap().into_parts();
    assert!(materialized && children.is_empty() && pipeline.is_none());
    assert!(restored.context.is_none());
    assert_eq!(restored.state, InsightState::Executed);
    assert_eq!(restored.order_id, row.order_id);
    assert_eq!(
        restored.limit_price.unwrap().to_bits(),
        row.limit_price.unwrap().to_bits()
    );
    assert!(restored.suppress_child_entries);
    assert_eq!(
        journal.get(&receipt.id).await.unwrap().unwrap().state,
        JournalCommandState::Applied
    );
    assert!(journal.recovery_page(0, 256).await.unwrap().is_empty());
    // Lost acknowledgement: an identical retry after reopen is idempotent.
    journal.persist_execution_batch(&batch).await.unwrap();
}

#[tokio::test]
async fn failed_second_receipt_rolls_back_prior_rows_and_applied_markers() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let first = insight();
    let second = insight();
    let good = completed(&mut journal, &first, false).await;
    let ambiguous = completed(&mut journal, &second, true).await;
    let batch = ExecutionCheckpointBatch::new(
        vec![checkpoint(&first, 1), checkpoint(&second, 1)],
        vec![good.clone(), ambiguous.clone()],
    )
    .unwrap();
    assert!(journal.persist_execution_batch(&batch).await.is_err());
    // The next access must roll back the abandoned transaction, not expose an
    // earlier statement's row/marker on this connection.
    assert!(
        journal
            .execution_checkpoint_page("", 32)
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(
        journal.get(&good.id).await.unwrap().unwrap().state,
        JournalCommandState::Completed
    );
    drop(journal);
    let journal = fixture.open().await;
    assert!(
        journal
            .execution_checkpoint_page("", 32)
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(journal.recovery_page(0, 256).await.unwrap().len(), 2);
}

#[tokio::test]
async fn stale_or_conflicting_revision_cannot_regress_execution_state() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let mut row = insight();
    let initial = ExecutionCheckpointBatch::new(vec![checkpoint(&row, 1)], vec![]).unwrap();
    journal.persist_execution_batch(&initial).await.unwrap();
    row.state = InsightState::Filled;
    let conflicting = ExecutionCheckpointBatch::new(vec![checkpoint(&row, 1)], vec![]).unwrap();
    assert!(journal.persist_execution_batch(&conflicting).await.is_err());
    let skipped = ExecutionCheckpointBatch::new(vec![checkpoint(&row, 3)], vec![]).unwrap();
    assert!(journal.persist_execution_batch(&skipped).await.is_err());
    let next = ExecutionCheckpointBatch::new(vec![checkpoint(&row, 2)], vec![]).unwrap();
    journal.persist_execution_batch(&next).await.unwrap();
    assert!(journal.persist_execution_batch(&initial).await.is_err());
    let page = journal.execution_checkpoint_page("", 1).await.unwrap();
    assert_eq!(page.rows[0].state, InsightState::Filled);
    assert_eq!(page.rows[0].revision, 2);
}

#[tokio::test]
async fn checkpoint_pages_are_keyed_bounded_and_do_not_skip_rows_at_byte_boundary() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let mut rows = (0..35).map(|_| insight()).collect::<Vec<_>>();
    rows.sort_by_key(|row| row.insight_id);
    for chunk in rows.chunks(32) {
        journal
            .persist_execution_batch(
                &ExecutionCheckpointBatch::new(
                    chunk.iter().map(|row| checkpoint(row, 1)).collect(),
                    vec![],
                )
                .unwrap(),
            )
            .await
            .unwrap();
    }
    let first = journal.execution_checkpoint_page("", 999).await.unwrap();
    assert_eq!(first.rows.len(), 32);
    let second = journal
        .execution_checkpoint_page(first.next_cursor.as_deref().unwrap(), 999)
        .await
        .unwrap();
    assert_eq!(second.rows.len(), 3);
    assert_eq!(second.rows[0].insight_id, rows[32].insight_id);
    assert!(
        journal
            .execution_checkpoint_page(second.next_cursor.as_deref().unwrap(), 999)
            .await
            .unwrap()
            .rows
            .is_empty()
    );

    // Each row fits, but the pair does not. Pagination must return the first and
    // leave the second for the next cursor, not consume/skip it.
    for row in rows.iter_mut().take(2) {
        row.state_history.push((
            chrono::Utc::now(),
            InsightState::New,
            Some("x".repeat(600_000)),
        ));
        journal
            .persist_execution_batch(
                &ExecutionCheckpointBatch::new(vec![checkpoint(row, 2)], vec![]).unwrap(),
            )
            .await
            .unwrap();
    }
    let first = journal.execution_checkpoint_page("", 32).await.unwrap();
    assert_eq!(first.rows.len(), 1);
    let second = journal
        .execution_checkpoint_page(first.next_cursor.as_deref().unwrap(), 32)
        .await
        .unwrap();
    assert_eq!(second.rows[0].insight_id, rows[1].insight_id);
}

#[test]
fn checkpoint_owner_receipts_must_be_unique_materialized_and_match_pending_identity() {
    let mut insight = insight();
    let command = BrokerCommand::new(
        "strategy",
        "account",
        &insight.insight_id.to_string(),
        BrokerCommandAction::Submit {
            request: (&insight).into(),
        },
        None,
    );
    let receipt = command.receipt();
    let mut row = checkpoint(&insight, 1);
    row.owner_receipts.push(receipt.clone());
    assert!(row.validate().is_ok());
    row.owner_receipts.push(receipt.clone());
    assert!(row.validate().is_err());
    row.owner_receipts.pop();
    row.materialized = false;
    assert!(row.validate().is_err());
    row.materialized = true;
    row.owner_receipts[0].insight_id = uuid::Uuid::new_v4().to_string();
    assert!(row.validate().is_err());
    insight
        .pending_mutations
        .push(crate::core::insight::operations::PendingInsightMutation {
            kind: crate::core::insight::operations::MutationKind::Submit,
            receipt: receipt.clone(),
            effect: crate::core::insight::operations::MutationEffect::Submit,
        });
    let mut row = checkpoint(&insight, 1);
    row.owner_receipts.push(receipt);
    assert!(
        row.validate().is_ok(),
        "the same receipt may be owned by helper and driver"
    );
    row.owner_receipts[0].group = Some(crate::core::broker::commands::BrokerOperationGroup {
        id: "forged".into(),
        version: 1,
    });
    assert!(row.validate().is_err());
}

#[test]
fn checkpoint_admission_rejects_pending_receipts_nonfinite_data_and_oversize_rows() {
    use crate::core::insight::operations::{MutationEffect, MutationKind, PendingInsightMutation};
    let mut row = insight();
    let command = BrokerCommand::new(
        "strategy",
        "account",
        &row.insight_id.to_string(),
        BrokerCommandAction::Submit {
            request: (&row).into(),
        },
        None,
    );
    let receipt = command.receipt();
    row.pending_mutations.push(PendingInsightMutation {
        kind: MutationKind::Submit,
        receipt: receipt.clone(),
        effect: MutationEffect::Submit,
    });
    assert!(ExecutionCheckpointBatch::new(vec![checkpoint(&row, 1)], vec![receipt]).is_err());
    assert!(ExecutionCheckpointBatch::new(vec![checkpoint(&row, 1)], vec![]).is_ok());
    row.quantity = Some(f64::NAN);
    assert!(ExecutionCheckpointBatch::new(vec![checkpoint(&row, 1)], vec![]).is_err());
    row.quantity = Some(2.0);
    row.state_history.push((
        chrono::Utc::now(),
        InsightState::New,
        Some("x".repeat(MAX_CHECKPOINT_BYTES)),
    ));
    assert!(ExecutionCheckpointBatch::new(vec![checkpoint(&row, 1)], vec![]).is_err());
    assert!(ExecutionCheckpointBatch::new(vec![], vec![]).is_err());
    assert!(
        ExecutionCheckpointBatch::new((0..33).map(|_| checkpoint(&insight(), 1)).collect(), vec![])
            .is_err()
    );
}

#[test]
fn checkpoint_roundtrips_entry_amendments_and_rejects_invalid_prices() {
    use crate::core::insight::operations::{MutationEffect, MutationKind, PendingInsightMutation};
    for (limit_price, stop_price) in [
        (Some(100.0), None),
        (None, Some(101.0)),
        (Some(100.0), Some(101.0)),
    ] {
        let mut row = insight();
        let command = BrokerCommand::new(
            "strategy",
            "account",
            &row.insight_id.to_string(),
            BrokerCommandAction::AmendEntry {
                order_id: "ticket".into(),
                limit_price,
                stop_price,
            },
            None,
        );
        row.pending_mutations.push(PendingInsightMutation {
            kind: MutationKind::AmendEntry,
            receipt: command.receipt(),
            effect: MutationEffect::AmendEntry {
                limit_price,
                stop_price,
            },
        });
        let original = checkpoint(&row, 1);
        let mut decoded: InsightExecutionCheckpoint =
            serde_json::from_str(&serde_json::to_string(&original).unwrap()).unwrap();
        decoded.validate().unwrap();
        assert_eq!(decoded.pending_mutations[0].receipt, command.receipt());
        assert!(matches!(decoded.pending_mutations[0].effect,
            MutationEffect::AmendEntry { limit_price: l, stop_price: s } if l == limit_price && s == stop_price));
        for bad in [
            None,
            Some(0.0),
            Some(-1.0),
            Some(f64::NAN),
            Some(f64::INFINITY),
        ] {
            decoded.pending_mutations[0].effect = MutationEffect::AmendEntry {
                limit_price: bad,
                stop_price: None,
            };
            assert!(decoded.validate().is_err());
        }
    }
}

#[test]
fn checkpoint_preserves_pending_effects_flags_history_and_flat_child_identity() {
    use crate::core::insight::operations::{MutationEffect, MutationKind, PendingInsightMutation};
    let mut row = insight();
    row.parent_id = Some(uuid::Uuid::new_v4());
    row.is_split_insight = true;
    row.suppress_child_entries = true;
    row.first_on_fill = true;
    row.cancelling = true;
    row.entry_filled_quantity = Some(1.234567890123456);
    row.state_log(Some("execution recovery fixture".into()));
    let mut child = insight();
    child.parent_id = Some(row.insight_id);
    child.state_history.push((
        chrono::Utc::now(),
        InsightState::New,
        Some("nested history is not duplicated".repeat(10_000)),
    ));
    let child_id = child.insight_id;
    row.children.push(child);
    let command = BrokerCommand::new(
        "strategy",
        "account",
        &row.insight_id.to_string(),
        BrokerCommandAction::TakeProfit {
            order_id: "ticket".into(),
            price: 2500.0,
            quantity: 2.0,
        },
        None,
    );
    row.pending_mutations.push(PendingInsightMutation {
        kind: MutationKind::TakeProfit,
        receipt: command.receipt(),
        effect: MutationEffect::TakeProfit(Some(vec![2500.0])),
    });
    let original = checkpoint(&row, 1);
    let json = serde_json::to_string(&original).unwrap();
    assert!(
        json.len() < 10_000,
        "flat checkpoint duplicated child history"
    );
    let decoded: InsightExecutionCheckpoint = serde_json::from_str(&json).unwrap();
    decoded.validate().unwrap();
    let (restored, child_ids, _, materialized) = decoded.into_parts();
    assert!(materialized);
    assert_eq!(child_ids, vec![child_id]);
    assert!(restored.children.is_empty());
    assert_eq!(restored.parent_id, row.parent_id);
    assert!(
        restored.is_split_insight
            && restored.first_on_fill
            && restored.suppress_child_entries
            && restored.cancelling
    );
    assert_eq!(restored.state_history, row.state_history);
    assert_eq!(restored.state_history_kinds, row.state_history_kinds);
    assert_eq!(restored.pending_mutations[0].receipt, command.receipt());
    assert!(
        matches!(&restored.pending_mutations[0].effect, MutationEffect::TakeProfit(Some(levels)) if *levels == vec![2500.0])
    );
}
