use super::*;
use crate::core::{
    broker::{
        command_journal::{ExecutionCheckpointBatch, ExecutionIntent},
        commands::BrokerCommandAction,
    },
    insight::{Insight, types::InsightState},
};

struct Fixture(std::path::PathBuf);
impl Fixture {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("aqe-recovery-{}", Uuid::new_v4())))
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
fn row(state: InsightState, materialized: bool) -> InsightExecutionCheckpoint {
    let mut insight = Insight::default();
    insight.symbol = "ETHUSD".into();
    insight.quantity = Some(1.0);
    insight.state = state;
    InsightExecutionCheckpoint::capture(&insight, 1, materialized, None)
}
async fn save(journal: &mut BrokerCommandJournal, rows: Vec<InsightExecutionCheckpoint>) {
    journal
        .persist_execution_batch(&ExecutionCheckpointBatch::new(rows, vec![]).unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn recovery_loads_explicit_closed_dependency_then_releases_it_with_consumer() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let target = row(InsightState::Closed, true);
    let unrelated = row(InsightState::Closed, true);
    let mut consumer = row(InsightState::Filled, true).into_parts().0;
    consumer
        .set_recovery_dependencies(&[target.insight_id])
        .unwrap();
    let active = InsightExecutionCheckpoint::capture(&consumer, 1, true, None);
    let expected: BTreeSet<_> = [target.insight_id, consumer.insight_id]
        .into_iter()
        .collect();
    save(&mut journal, vec![target, unrelated.clone(), active]).await;
    // Keep SQL JSON valid for expression indexes, but invalid as a checkpoint.
    journal.connection.execute(r#"UPDATE broker_insight_checkpoints SET checkpoint_json='{"fixture_corruption":true}' WHERE insight_id=?1"#, params![unrelated.insight_id.to_string()]).await.unwrap();
    drop(journal);
    let mut journal = fixture.open().await;
    let recovered = journal.load_execution_recovery(32).await.unwrap();
    assert_eq!(
        recovered.rows.keys().copied().collect::<BTreeSet<_>>(),
        expected
    );
    consumer.state = InsightState::Closed;
    save(
        &mut journal,
        vec![InsightExecutionCheckpoint::capture(
            &consumer, 2, true, None,
        )],
    )
    .await;
    assert!(
        journal
            .load_execution_recovery(32)
            .await
            .unwrap()
            .rows
            .is_empty()
    );
}

#[tokio::test]
async fn recovery_refuses_missing_explicit_dependency_without_discarding_consumer() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let target = row(InsightState::Closed, true);
    let mut consumer = row(InsightState::New, true).into_parts().0;
    consumer
        .set_recovery_dependencies(&[target.insight_id])
        .unwrap();
    save(
        &mut journal,
        vec![InsightExecutionCheckpoint::capture(
            &consumer, 1, true, None,
        )],
    )
    .await;
    let error = journal.load_execution_recovery(32).await.err().unwrap();
    assert!(error.to_string().contains("recovery dependency is missing"));
    save(&mut journal, vec![target]).await;
    assert_eq!(
        journal
            .load_execution_recovery(32)
            .await
            .unwrap()
            .rows
            .len(),
        2
    );
}
fn intent(
    mut row: InsightExecutionCheckpoint,
    order: u64,
) -> (InsightExecutionCheckpoint, ExecutionIntent) {
    let command = BrokerCommand::new(
        "strategy",
        "account",
        &row.insight_id.to_string(),
        BrokerCommandAction::Submit {
            request: (&row.clone().into_parts().0).into(),
        },
        None,
    );
    row.owner_receipts.push(command.receipt());
    (
        row,
        ExecutionIntent {
            command,
            admission_order: order,
            queued_at_us: 10,
        },
    )
}

#[tokio::test]
async fn recovery_preserves_admission_order_not_commit_sequence_and_advances_high_watermark() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let (first, a) = intent(row(InsightState::New, true), 1);
    let (second, b) = intent(row(InsightState::New, true), 2);
    let first_id = a.command.id.clone();
    let second_id = b.command.id.clone();
    journal
        .persist_execution_batch(
            &ExecutionCheckpointBatch::with_intents(vec![second], vec![], vec![b]).unwrap(),
        )
        .await
        .unwrap();
    journal
        .persist_execution_batch(
            &ExecutionCheckpointBatch::with_intents(vec![first], vec![], vec![a]).unwrap(),
        )
        .await
        .unwrap();
    drop(journal);
    let mut journal = fixture.open().await;
    assert_eq!(journal.last_admission_order().await.unwrap(), 2);
    let recovered = journal.load_execution_recovery(32).await.unwrap();
    assert_eq!(
        recovered
            .commands
            .iter()
            .map(|command| command.record.command.id.clone())
            .collect::<Vec<_>>(),
        vec![first_id, second_id]
    );
    assert!(recovered.commands[0].record.sequence > recovered.commands[1].record.sequence);
    assert_eq!(
        journal.ordered_recovery_page(1, 1).await.unwrap()[0].admission_order,
        2
    );
}

#[tokio::test]
async fn recovery_loads_connected_live_graph_but_not_retired_stack_histories() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let mut parent = row(InsightState::Filled, true);
    let mut child = row(InsightState::New, false);
    let mut nested = row(InsightState::New, false);
    let mut closed = row(InsightState::Closed, true);
    child.parent_id = Some(parent.insight_id);
    nested.parent_id = Some(child.insight_id);
    closed.parent_id = Some(parent.insight_id);
    child.child_ids.push(nested.insight_id);
    parent
        .child_ids
        .extend([child.insight_id, closed.insight_id]);
    let expected = [
        parent.insight_id,
        child.insight_id,
        nested.insight_id,
        closed.insight_id,
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    let mut retired = row(InsightState::Closed, true);
    let mut unused = row(InsightState::New, false);
    unused.parent_id = Some(retired.insight_id);
    retired.child_ids.push(unused.insight_id);
    let retired_id = retired.insight_id;
    save(
        &mut journal,
        vec![parent, child, nested, closed, retired, unused],
    )
    .await;
    // A cold historical blob must not be decoded on active execution startup.
    // Corruption injection is confined to this disposable test journal.
    journal.connection.execute(r#"UPDATE broker_insight_checkpoints SET checkpoint_json='{"fixture_corruption":true}' WHERE insight_id=?1"#, params![retired_id.to_string()]).await.unwrap();
    let recovered = journal.load_execution_recovery(32).await.unwrap();
    assert_eq!(
        recovered.rows.keys().copied().collect::<BTreeSet<_>>(),
        expected
    );
    assert!(recovered.commands.is_empty());
}

#[tokio::test]
async fn legacy_index_backfill_is_additive_and_recovery_keeps_orphans_for_cleanup() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let mut orphan = row(InsightState::Filled, true);
    orphan.parent_id = Some(Uuid::new_v4());
    let id = orphan.insight_id;
    let expected = serde_json::to_string(&orphan).unwrap();
    save(&mut journal, vec![orphan]).await;
    journal
        .connection
        .execute("DELETE FROM broker_execution_graph", ())
        .await
        .unwrap();
    let recovered = journal.load_execution_recovery(32).await.unwrap();
    assert_eq!(recovered.rows.len(), 1);
    assert_eq!(
        serde_json::to_string(&recovered.rows[&id]).unwrap(),
        expected
    );
    assert!(journal.load_execution_recovery(32).await.is_ok());
}

#[tokio::test]
async fn incomplete_child_graph_and_missing_command_owners_are_not_silently_recovered() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let mut parent = row(InsightState::Filled, true);
    parent.child_ids.push(Uuid::new_v4());
    save(&mut journal, vec![parent]).await;
    assert!(journal.load_execution_recovery(32).await.is_err());
    drop(journal);
    let other = Fixture::new();
    let mut journal = other.open().await;
    let (row, command) = intent(row(InsightState::New, true), 1);
    let id = row.insight_id;
    journal
        .persist_execution_batch(
            &ExecutionCheckpointBatch::with_intents(vec![row], vec![], vec![command]).unwrap(),
        )
        .await
        .unwrap();
    journal
        .connection
        .execute(
            "DELETE FROM broker_insight_checkpoints WHERE insight_id=?1",
            params![id.to_string()],
        )
        .await
        .unwrap();
    assert!(journal.load_execution_recovery(32).await.is_err());
}

#[tokio::test]
async fn recovery_never_omits_unordered_legacy_commands_or_exceeds_pending_capacity() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let (row, command) = intent(row(InsightState::New, true), 1);
    journal.prepare(&command.command, 10).await.unwrap();
    assert!(journal.load_execution_recovery(32).await.is_err());
    journal
        .persist_execution_batch(
            &ExecutionCheckpointBatch::with_intents(vec![row], vec![], vec![command]).unwrap(),
        )
        .await
        .unwrap();
    assert!(journal.load_execution_recovery(0).await.is_err());
    assert_eq!(
        journal
            .load_execution_recovery(1)
            .await
            .unwrap()
            .commands
            .len(),
        1
    );
}

#[tokio::test]
async fn conflicting_admission_order_rolls_back_state_intent_and_graph_together() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let (first, a) = intent(row(InsightState::New, true), 1);
    journal
        .persist_execution_batch(
            &ExecutionCheckpointBatch::with_intents(vec![first], vec![], vec![a]).unwrap(),
        )
        .await
        .unwrap();
    let (second, b) = intent(row(InsightState::New, true), 1);
    let id = second.insight_id;
    let command_id = b.command.id.clone();
    assert!(
        journal
            .persist_execution_batch(
                &ExecutionCheckpointBatch::with_intents(vec![second], vec![], vec![b]).unwrap()
            )
            .await
            .is_err()
    );
    assert!(journal.get(&command_id).await.unwrap().is_none());
    let recovered = journal.load_execution_recovery(32).await.unwrap();
    assert!(!recovered.rows.contains_key(&id));
    assert_eq!(recovered.commands.len(), 1);
}

#[tokio::test]
async fn interrupted_graph_index_upgrade_resumes_committed_key_and_runs_only_once() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let mut saved = Vec::new();
    for index in 1..=33 {
        let mut checkpoint = row(InsightState::Closed, true);
        checkpoint.insight_id = Uuid::from_u128(index);
        saved.push(checkpoint);
    }
    save(&mut journal, saved[..32].to_vec()).await;
    save(&mut journal, saved[32..].to_vec()).await;
    journal
        .connection
        .execute("DELETE FROM broker_execution_graph", ())
        .await
        .unwrap();
    let last = saved[32].insight_id.to_string();
    journal.connection.execute(r#"UPDATE broker_insight_checkpoints SET checkpoint_json='{"fixture_corruption":true}' WHERE insight_id=?1"#, params![last.clone()]).await.unwrap();
    assert!(journal.ensure_execution_graph_index().await.is_err());
    let mut state = journal
        .connection
        .query(
            "SELECT graph_cursor,graph_complete FROM broker_execution_recovery_meta",
            (),
        )
        .await
        .unwrap();
    let marker = state.next().await.unwrap().unwrap();
    assert_eq!(
        marker.get::<String>(0).unwrap(),
        saved[31].insight_id.to_string()
    );
    assert_eq!(marker.get::<i64>(1).unwrap(), 0);
    drop(state);
    // Earlier pages need not be decoded again after a process interruption.
    journal.connection.execute(r#"UPDATE broker_insight_checkpoints SET checkpoint_json='{"fixture_corruption":true}' WHERE insight_id=?1"#, params![saved[0].insight_id.to_string()]).await.unwrap();
    journal
        .connection
        .execute(
            "UPDATE broker_insight_checkpoints SET checkpoint_json=?1 WHERE insight_id=?2",
            params![serde_json::to_string(&saved[32]).unwrap(), last],
        )
        .await
        .unwrap();
    drop(journal);
    let mut journal = fixture.open().await;
    journal.ensure_execution_graph_index().await.unwrap();
    journal.ensure_execution_graph_index().await.unwrap();
    assert!(
        journal
            .load_execution_recovery(32)
            .await
            .unwrap()
            .rows
            .is_empty()
    );
}

#[tokio::test]
async fn selected_graph_history_pages_do_not_skip_the_row_crossing_the_byte_budget() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let mut expected = BTreeSet::new();
    for index in 1..=3 {
        let mut checkpoint = row(InsightState::Filled, true);
        checkpoint.insight_id = Uuid::from_u128(index);
        checkpoint.state_history.push((
            chrono::Utc::now(),
            InsightState::Filled,
            Some("h".repeat(600_000)),
        ));
        expected.insert(checkpoint.insight_id);
        save(&mut journal, vec![checkpoint]).await;
    }
    let recovered = journal.load_execution_recovery(32).await.unwrap();
    assert_eq!(
        recovered.rows.keys().copied().collect::<BTreeSet<_>>(),
        expected
    );
    assert!(
        recovered.rows.values().all(|row| row
            .state_history
            .last()
            .unwrap()
            .2
            .as_ref()
            .unwrap()
            .len()
            == 600_000)
    );
}
