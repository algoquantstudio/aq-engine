use super::*;
use crate::core::insight::{
    Insight,
    types::{InsightState, PartialCloseResult},
};

struct Fixture(std::path::PathBuf);
impl Fixture {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("aqe-evidence-{}", uuid::Uuid::new_v4())))
    }
    async fn open(&self) -> BrokerCommandJournal {
        BrokerCommandJournal::open_with_state_history(
            self.0.join("commands.db"),
            "strategy",
            "account",
            true,
        )
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
    let mut row = Insight::default();
    row.symbol = "GENERIC-FUTURE".into();
    row.quantity = Some(4.0);
    row.limit_price = Some(100.0);
    row
}
fn checkpoint(row: &Insight, revision: u64) -> InsightExecutionCheckpoint {
    InsightExecutionCheckpoint::capture(row, revision, true, None)
}
async fn save(journal: &mut BrokerCommandJournal, row: &Insight, revision: u64) {
    journal
        .persist_execution_batch(
            &ExecutionCheckpointBatch::new(vec![checkpoint(row, revision)], vec![]).unwrap(),
        )
        .await
        .unwrap();
}
async fn page(journal: &BrokerCommandJournal) -> ExecutionEvidencePage {
    journal
        .execution_evidence_page(1, 0, None, 128)
        .await
        .unwrap()
}

#[tokio::test]
async fn canonical_setter_history_survives_without_a_duplicate_state_archive() {
    use crate::core::insight::InsightSnapshot;
    let fixture = Fixture::new();
    let mut journal = BrokerCommandJournal::open_with_state_history(
        fixture.0.join("commands.db"),
        "strategy",
        "account",
        false,
    )
    .await
    .unwrap();
    let parent = uuid::Uuid::new_v4();
    let mut row = Insight::default();
    row.symbol = "GENERIC".into();
    row.set_quantity(Some(2.))
        .set_limit_price(Some(100.))
        .set_stop_price(Some(101.))
        .set_take_profit_levels(Some(vec![110.]))
        .set_stop_loss_levels(Some(vec![90.]))
        .set_trailing_stop_price(Some(5.))
        .set_parent_id(parent);
    let history = row.state_history.clone();
    for prefix in [
        "Quantity set to",
        "Limit price set to",
        "Stop price set to",
        "Take profit levels set to",
        "Stop loss levels set to",
        "Trailing stop set to",
        "Parent ID set to",
    ] {
        assert_eq!(
            history
                .iter()
                .filter(|(_, _, message)| message.as_deref().is_some_and(|m| m.starts_with(prefix)))
                .count(),
            1,
            "{prefix}"
        );
    }
    // Setting the same values is a no-op, not a second analytics event.
    row.set_quantity(Some(2.))
        .set_limit_price(Some(100.))
        .set_parent_id(parent);
    assert_eq!(row.state_history, history);
    save(&mut journal, &row, 1).await;
    drop(journal);
    let journal = BrokerCommandJournal::open_with_state_history(
        fixture.0.join("commands.db"),
        "strategy",
        "account",
        false,
    )
    .await
    .unwrap();
    assert!(
        journal
            .execution_evidence_feed_page(0, None, 128)
            .await
            .unwrap()
            .observations
            .is_empty()
    );
    let saved = journal
        .execution_checkpoint_page("", 32)
        .await
        .unwrap()
        .rows
        .remove(0);
    let (restored, _, _, _) = saved.into_parts();
    assert_eq!(restored.state_history, history);
    let snapshot = InsightSnapshot::from_insight(&restored, "strategy");
    assert_eq!(snapshot.parent_id, Some(parent.to_string()));
    assert_eq!(snapshot.quantity, Some(2.));
    assert_eq!(snapshot.limit_price, Some(100.));
    assert_eq!(snapshot.state_history.len(), history.len());
    assert_eq!(
        snapshot
            .state_history
            .iter()
            .map(|h| h.message.clone())
            .collect::<Vec<_>>(),
        history
            .iter()
            .map(|(_, _, m)| m.clone())
            .collect::<Vec<_>>()
    );
}

#[test]
fn projection_is_allowlisted_and_never_promotes_intended_prices_to_fills() {
    let mut row = checkpoint(&insight(), 1);
    row.execution_data =
        serde_json::from_value(serde_json::json!({"private": "\"secret\""})).unwrap();
    let state = project(&row);
    assert_eq!(state.intended_quantity, Some(4.0));
    assert_eq!(state.limit_price, Some(100.0));
    assert_eq!(state.entry_filled_quantity, None);
    assert_eq!(state.entry_price, None);
    assert_eq!(state.broker_realized_pnl, None);
    let encoded = serde_json::to_string(&state).unwrap();
    for forbidden in [
        "secret",
        "execution_data",
        "pending_mutations",
        "owner_receipts",
        "histories",
        "pipeline_checkpoint",
    ] {
        assert!(!encoded.contains(forbidden), "unexpected {forbidden}");
    }
}

#[test]
fn invalid_numbers_are_unavailable_without_changing_protection_meaning() {
    let mut row = checkpoint(&insight(), 1);
    row.commission = Some(f64::NAN);
    row.swap = Some(f64::INFINITY);
    row.take_profit_levels = Some(vec![101.0, f64::NEG_INFINITY]);
    let state = project(&row);
    assert_eq!(state.commission, None);
    assert_eq!(state.swap, None);
    assert_eq!(state.take_profit_levels, None);
    assert_eq!(state.invalid_numeric_fields, 3);
    assert!(serde_json::to_string(&state).is_ok());
    // The canonical writer is stricter; it must still refuse invalid recovery data.
    assert!(ExecutionCheckpointBatch::new(vec![row], vec![]).is_err());
}

#[tokio::test]
async fn legacy_checkpoint_metadata_is_ignored_and_strategy_type_survives_restart() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    for legacy_node in ["instance-a", "instance-b"] {
        let mut row = insight();
        row.strategy_type = crate::core::insight::types::StrategyType::Custom("SameClass".into());
        let mut legacy = serde_json::to_value(checkpoint(&row, 1)).unwrap();
        legacy["version"] = serde_json::json!(5);
        legacy["alpha_node_id"] = serde_json::json!(legacy_node);
        let restored: InsightExecutionCheckpoint = serde_json::from_value(legacy).unwrap();
        restored.validate().unwrap();
        let row = restored.into_parts().0;
        let current = checkpoint(&row, 1);
        assert_eq!(current.version, 1);
        assert_eq!(current.strategy_type.to_string(), "SameClass");
        assert!(
            !serde_json::to_string(&current)
                .unwrap()
                .contains("alpha_node_id")
        );
        save(&mut journal, &row, 1).await;
    }
    drop(journal);
    let journal = fixture.open().await;
    let page = page(&journal).await;
    assert_eq!(page.records.len(), 2);
    assert!(
        page.records
            .iter()
            .all(|row| row.state.strategy_label == "SameClass")
    );
    assert!(
        !serde_json::to_string(&page)
            .unwrap()
            .contains("alpha_node_id")
    );
}

#[tokio::test]
async fn meaningful_revisions_are_durable_and_private_changes_do_not_duplicate_them() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let row = insight();
    let initial = ExecutionCheckpointBatch::new(vec![checkpoint(&row, 1)], vec![]).unwrap();
    journal.persist_execution_batch(&initial).await.unwrap();
    journal.persist_execution_batch(&initial).await.unwrap();
    let mut private_source = row.clone();
    private_source.updated_at += chrono::Duration::seconds(1);
    private_source.execution_data =
        serde_json::from_value(serde_json::json!({"private": "42"})).unwrap();
    let private = checkpoint(&private_source, 2);
    journal
        .persist_execution_batch(&ExecutionCheckpointBatch::new(vec![private], vec![]).unwrap())
        .await
        .unwrap();
    assert_eq!(page(&journal).await.records.len(), 1);
    let mut filled = row.clone();
    filled.state = InsightState::Filled;
    filled.entry_filled_quantity = Some(2.0);
    filled.filled_price = Some(100.25);
    save(&mut journal, &filled, 3).await;
    let before = page(&journal).await;
    assert_eq!(before.records.len(), 2);
    assert_eq!(before.records[1].checkpoint_revision, 3);
    drop(journal);
    let journal = fixture.open().await;
    let after = page(&journal).await;
    assert_eq!(before.records, after.records);
    assert_eq!(before.through_sequence, after.through_sequence);
    assert_eq!(after.strategy_id, "strategy");
    assert_eq!(after.account_id, "account");
    assert_eq!(after.recovery_complete, None);
    assert!(after.run_id.is_none() && after.configuration_version.is_none());
}

#[tokio::test]
async fn conflicting_batch_rolls_back_earlier_evidence_and_checkpoint_rows() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let row = insight();
    save(&mut journal, &row, 1).await;
    let another = insight();
    let mut conflict = checkpoint(&row, 1);
    conflict.quantity = Some(7.0);
    let batch =
        ExecutionCheckpointBatch::new(vec![checkpoint(&another, 1), conflict], vec![]).unwrap();
    assert!(journal.persist_execution_batch(&batch).await.is_err());
    assert_eq!(page(&journal).await.records.len(), 1);
    assert_eq!(
        journal
            .execution_checkpoint_page("", 32)
            .await
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[tokio::test]
async fn partial_exits_and_cost_corrections_replace_observations_not_fill_identity() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let mut row = insight();
    row.state = InsightState::Filled;
    row.entry_filled_quantity = Some(4.0);
    row.filled_price = Some(100.0);
    row.partial_filled_quantity = Some(1.0);
    row.partial_closes.push(PartialCloseResult {
        order_id: "exit-ticket".into(),
        side: row.side.clone(),
        quantity: 1.0,
        entry_price: 100.0,
        filled_price: Some(101.0),
    });
    save(&mut journal, &row, 1).await;
    row.partial_closes.push(PartialCloseResult {
        order_id: "exit-ticket".into(),
        side: row.side.clone(),
        quantity: 1.0,
        entry_price: 100.0,
        filled_price: Some(102.0),
    });
    row.partial_filled_quantity = Some(2.0);
    row.state = InsightState::Closed;
    row.close_price = Some(103.0);
    row.broker_realized_pnl = Some(150.0); // Contract-valued; no price*qty reconstruction.
    row.commission = Some(2.0);
    save(&mut journal, &row, 2).await;
    row.commission = Some(3.5);
    row.swap = Some(-1.25);
    save(&mut journal, &row, 3).await;
    let records = page(&journal).await.records;
    assert_eq!(records.len(), 3);
    assert_eq!(records[2].state.partial_closes.len(), 2);
    assert_eq!(
        records[2].state.partial_closes[0].order_id,
        records[2].state.partial_closes[1].order_id
    );
    assert_eq!(records[2].state.broker_realized_pnl, Some(150.0));
    assert_eq!(records[2].state.commission, Some(3.5));
    assert_eq!(records[2].state.swap, Some(-1.25));
}

#[tokio::test]
async fn page_watermark_is_stable_during_appends_and_cursors_are_validated() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    for _ in 0..3 {
        save(&mut journal, &insight(), 1).await;
    }
    let first = journal
        .execution_evidence_page(1, 0, None, 1)
        .await
        .unwrap();
    assert!(first.has_more);
    assert_eq!(first.records.len(), 1);
    save(&mut journal, &insight(), 1).await;
    let rest = journal
        .execution_evidence_page(1, first.next_sequence, Some(first.through_sequence), 128)
        .await
        .unwrap();
    assert_eq!(rest.records.len(), 2);
    assert!(!rest.has_more);
    assert_eq!(rest.next_sequence, first.through_sequence);
    let delta = journal
        .execution_evidence_page(1, rest.next_sequence, None, 128)
        .await
        .unwrap();
    assert_eq!(delta.records.len(), 1);
    assert!(
        journal
            .execution_evidence_page(1, delta.through_sequence + 1, None, 1)
            .await
            .is_err()
    );
    assert!(
        journal
            .execution_evidence_page(1, 0, Some(delta.through_sequence + 1), 1)
            .await
            .is_err()
    );
    assert!(
        journal
            .execution_evidence_page(0, 0, None, 1)
            .await
            .is_err()
    );
    assert!(
        journal
            .execution_evidence_page(2, 0, None, 1)
            .await
            .is_err()
    );
    assert_eq!(
        journal
            .execution_evidence_page(1, 0, None, 0)
            .await
            .unwrap()
            .records
            .len(),
        1
    );
}

#[tokio::test]
async fn stopped_epochs_stay_readable_but_never_mix_with_new_execution() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let mut closed = insight();
    closed.state = InsightState::Closed;
    save(&mut journal, &closed, 1).await;
    journal.request_execution_stop().await.unwrap();
    assert!(journal.complete_execution_stop().await.unwrap());
    assert_eq!(journal.begin_execution_epoch().await.unwrap().epoch, 2);
    save(&mut journal, &insight(), 1).await;
    assert_eq!(page(&journal).await.records.len(), 1);
    let new = journal
        .execution_evidence_page(2, 0, None, 128)
        .await
        .unwrap();
    assert_eq!(new.records.len(), 1);
    assert_ne!(
        new.records[0].state.insight_id,
        closed.insight_id.to_string()
    );
    closed.commission = Some(3.5);
    save(&mut journal, &closed, 2).await;
    let old = page(&journal).await;
    assert_eq!(old.records.len(), 2);
    assert_eq!(old.records[1].state.commission, Some(3.5));
    assert_eq!(
        journal
            .execution_evidence_page(2, 0, None, 128)
            .await
            .unwrap()
            .records,
        new.records
    );
    drop(journal);
    assert!(
        BrokerCommandJournal::open(fixture.0.join("commands.db"), "another-strategy", "account")
            .await
            .is_err()
    );
    assert!(
        BrokerCommandJournal::open(fixture.0.join("commands.db"), "strategy", "another-account")
            .await
            .is_err()
    );
    let reopened = fixture.open().await;
    assert_eq!(
        reopened
            .execution_evidence_page(2, 0, None, 128)
            .await
            .unwrap()
            .records,
        new.records
    );
}

#[tokio::test]
async fn dormant_blueprints_are_not_execution_evidence() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let mut row = checkpoint(&insight(), 1);
    row.materialized = false;
    journal
        .persist_execution_batch(&ExecutionCheckpointBatch::new(vec![row.clone()], vec![]).unwrap())
        .await
        .unwrap();
    assert!(page(&journal).await.records.is_empty());
    row.materialized = true;
    row.revision = 2;
    journal
        .persist_execution_batch(&ExecutionCheckpointBatch::new(vec![row], vec![]).unwrap())
        .await
        .unwrap();
    assert_eq!(page(&journal).await.records.len(), 1);
}

#[tokio::test]
async fn pages_enforce_row_and_byte_bounds_without_skipping_records() {
    let fixture = Fixture::new();
    let journal = fixture.open().await;
    // Direct fixture inserts avoid making the writer's smaller checkpoint batch
    // limit the subject of this read-page regression test.
    let mut state = project(&checkpoint(&insight(), 1));
    for index in 0..130 {
        state.insight_id = format!("fixture-{index}");
        let encoded = serde_json::to_string(&state).unwrap();
        journal.connection.execute("INSERT INTO broker_execution_evidence
            (execution_epoch,insight_id,checkpoint_revision,observed_at_us,recorded_at_us,state_json)
            VALUES(1,?1,1,1,1,?2)", params![state.insight_id.clone(), encoded]).await.unwrap();
    }
    let first = journal
        .execution_evidence_page(1, 0, None, usize::MAX)
        .await
        .unwrap();
    assert_eq!(first.records.len(), 128);
    assert!(first.has_more);
    let rest = journal
        .execution_evidence_page(1, first.next_sequence, Some(first.through_sequence), 128)
        .await
        .unwrap();
    assert_eq!(rest.records.len(), 2);
    state.symbol = "x".repeat(MAX_EVIDENCE_PAGE_BYTES / 2);
    for index in 130..132 {
        state.insight_id = format!("fixture-{index}");
        journal.connection.execute("INSERT INTO broker_execution_evidence
            (execution_epoch,insight_id,checkpoint_revision,observed_at_us,recorded_at_us,state_json)
            VALUES(1,?1,1,1,1,?2)", params![state.insight_id.clone(), serde_json::to_string(&state).unwrap()]).await.unwrap();
    }
    let bounded = journal
        .execution_evidence_page(1, rest.next_sequence, None, 128)
        .await
        .unwrap();
    assert_eq!(bounded.records.len(), 1);
    assert!(bounded.has_more);
    let last = journal
        .execution_evidence_page(
            1,
            bounded.next_sequence,
            Some(bounded.through_sequence),
            128,
        )
        .await
        .unwrap();
    assert_eq!(last.records.len(), 1);
    assert!(!last.has_more);
    assert_eq!(last.next_sequence, bounded.through_sequence);
}

#[tokio::test]
async fn source_identity_survives_reopen_but_a_new_journal_is_a_new_stream() {
    let fixture = Fixture::new();
    let journal = fixture.open().await;
    let original = page(&journal).await.source_id;
    assert!(!uuid::Uuid::parse_str(&original).unwrap().is_nil());
    drop(journal);
    assert_eq!(page(&fixture.open().await).await.source_id, original);
    let other = Fixture::new();
    assert_ne!(page(&other.open().await).await.source_id, original);
}

#[tokio::test]
async fn global_export_keeps_late_corrections_in_original_epochs() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let mut closed = insight();
    closed.state = InsightState::Closed;
    save(&mut journal, &closed, 1).await;
    journal.request_execution_stop().await.unwrap();
    assert!(journal.complete_execution_stop().await.unwrap());
    journal.begin_execution_epoch().await.unwrap();
    save(&mut journal, &insight(), 1).await;
    closed.commission = Some(7.0);
    save(&mut journal, &closed, 2).await;
    let page = journal
        .execution_evidence_feed_page(0, None, 128)
        .await
        .unwrap();
    assert_eq!(
        page.observations
            .iter()
            .map(|o| o.execution_epoch)
            .collect::<Vec<_>>(),
        vec![1, 2, 1]
    );
    assert_eq!(page.observations[2].record.state.commission, Some(7.0));
    let first = journal
        .execution_evidence_feed_page(0, None, 1)
        .await
        .unwrap();
    save(&mut journal, &insight(), 1).await;
    let rest = journal
        .execution_evidence_feed_page(first.next_sequence, Some(first.through_sequence), 128)
        .await
        .unwrap();
    assert_eq!(rest.observations, page.observations[1..]);
    assert_eq!(rest.next_sequence, first.through_sequence);
    assert!(!rest.has_more);
    assert!(
        journal
            .execution_evidence_feed_page(99, None, 1)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn legacy_checkpoint_epoch_migration_preserves_original_ownership_with_archive_disabled() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let mut first = insight();
    first.order_id = Some("first-order".into());
    first.state = InsightState::Closed;
    save(&mut journal, &first, 1).await;
    journal.request_execution_stop().await.unwrap();
    assert!(journal.complete_execution_stop().await.unwrap());
    journal.begin_execution_epoch().await.unwrap();
    let mut second = insight();
    second.order_id = Some("second-order".into());
    save(&mut journal, &second, 1).await;
    // Simulate the previous private journal schema. Both source histories are
    // retained so the migration can recover their established original epochs.
    journal.connection.execute_batch("DROP INDEX idx_checkpoint_owner_epoch;
        DROP INDEX idx_fill_history_owned_order;
        ALTER TABLE broker_insight_checkpoints RENAME TO legacy_checkpoints;
        CREATE TABLE broker_insight_checkpoints(insight_id TEXT PRIMARY KEY,revision INTEGER NOT NULL,checkpoint_json TEXT NOT NULL);
        INSERT INTO broker_insight_checkpoints SELECT insight_id,revision,checkpoint_json FROM legacy_checkpoints;
        DROP TABLE legacy_checkpoints;").await.unwrap();
    drop(journal);
    let mut journal = BrokerCommandJournal::open_with_state_history(
        fixture.0.join("commands.db"),
        "strategy",
        "account",
        false,
    )
    .await
    .unwrap();
    let owners = journal.fill_history_owner_page("").await.unwrap().owners;
    assert_eq!(
        owners
            .iter()
            .find(|o| o.entry_order_id == "first-order")
            .unwrap()
            .execution_epoch,
        1
    );
    assert_eq!(
        owners
            .iter()
            .find(|o| o.entry_order_id == "second-order")
            .unwrap()
            .execution_epoch,
        2
    );
    first.commission = Some(8.);
    save(&mut journal, &first, 2).await;
    assert_eq!(
        journal
            .execution_evidence_feed_page(0, None, 128)
            .await
            .unwrap()
            .observations
            .len(),
        2
    );
    drop(journal);
    let journal = BrokerCommandJournal::open_with_state_history(
        fixture.0.join("commands.db"),
        "strategy",
        "account",
        false,
    )
    .await
    .unwrap();
    assert_eq!(
        journal
            .fill_history_owner_page("")
            .await
            .unwrap()
            .owners
            .iter()
            .find(|o| o.entry_order_id == "first-order")
            .unwrap()
            .execution_epoch,
        1
    );
}

#[tokio::test]
async fn interrupted_owner_metadata_upgrade_retries_backfill_without_rebuilding_every_restart() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let mut row = insight();
    row.order_id = Some("original-order".into());
    row.state = InsightState::Closed;
    save(&mut journal, &row, 1).await;
    // Columns were added but initialization did not finish before interruption.
    journal.connection.execute_batch("UPDATE broker_insight_checkpoints SET execution_epoch=0,order_id=NULL,symbol=NULL,materialized=0;
        PRAGMA user_version=0;").await.unwrap();
    drop(journal);
    let journal = BrokerCommandJournal::open_with_state_history(
        fixture.0.join("commands.db"),
        "strategy",
        "account",
        false,
    )
    .await
    .unwrap();
    let owner = journal
        .fill_history_owner_page("")
        .await
        .unwrap()
        .owners
        .remove(0);
    assert_eq!(owner.entry_order_id, "original-order");
    assert_eq!(owner.execution_epoch, 1);
    // This retired checkpoint is intentionally unreadable. Subsequent opens
    // must neither scan its JSON for ownership nor rebuild its scalar index.
    journal.connection.execute("UPDATE broker_insight_checkpoints SET checkpoint_json='retired-invalid-json' WHERE insight_id=?1", params![owner.insight_id.clone()]).await.unwrap();
    drop(journal);
    let journal = BrokerCommandJournal::open_with_state_history(
        fixture.0.join("commands.db"),
        "strategy",
        "account",
        false,
    )
    .await
    .unwrap();
    assert_eq!(
        journal.fill_history_owner_page("").await.unwrap().owners[0],
        owner
    );
}

#[tokio::test]
async fn export_freezes_exact_page_until_matching_durable_receipt_even_across_restart() {
    use aqe_live_protocol::{ExecutionEvidenceExportReceipt, execution_evidence_transfer_id};
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    assert!(
        journal
            .prepare_execution_evidence_export()
            .await
            .unwrap()
            .is_none()
    );
    save(&mut journal, &insight(), 1).await;
    let frozen = journal
        .prepare_execution_evidence_export()
        .await
        .unwrap()
        .unwrap();
    save(&mut journal, &insight(), 1).await;
    assert_eq!(
        journal
            .prepare_execution_evidence_export()
            .await
            .unwrap()
            .unwrap(),
        frozen
    );
    drop(journal);
    let journal = fixture.open().await;
    assert_eq!(
        journal
            .prepare_execution_evidence_export()
            .await
            .unwrap()
            .unwrap(),
        frozen
    );
    let receipt = ExecutionEvidenceExportReceipt {
        source_id: frozen.source_id.clone(),
        transfer_id: execution_evidence_transfer_id(&frozen).unwrap(),
        after_sequence: frozen.after_sequence,
        next_sequence: frozen.next_sequence,
    };
    for bad in [
        ExecutionEvidenceExportReceipt {
            source_id: "different-journal".into(),
            ..receipt.clone()
        },
        ExecutionEvidenceExportReceipt {
            transfer_id: "wrong-page".into(),
            ..receipt.clone()
        },
        ExecutionEvidenceExportReceipt {
            next_sequence: receipt.next_sequence + 1,
            ..receipt.clone()
        },
    ] {
        assert!(
            journal
                .acknowledge_execution_evidence_export(&bad)
                .await
                .is_err()
        );
    }
    assert_eq!(
        journal
            .prepare_execution_evidence_export()
            .await
            .unwrap()
            .unwrap(),
        frozen
    );
    journal
        .acknowledge_execution_evidence_export(&receipt)
        .await
        .unwrap();
    journal
        .acknowledge_execution_evidence_export(&receipt)
        .await
        .unwrap();
    let next = journal
        .prepare_execution_evidence_export()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.after_sequence, receipt.next_sequence);
    assert_eq!(next.observations.len(), 1);
    // An old duplicate receipt cannot discard a newer pending page.
    journal
        .acknowledge_execution_evidence_export(&receipt)
        .await
        .unwrap();
    assert_eq!(
        journal
            .prepare_execution_evidence_export()
            .await
            .unwrap()
            .unwrap(),
        next
    );
}

#[tokio::test]
async fn large_unicode_evidence_roundtrips_out_of_order_and_fails_closed_on_missing_or_corrupt_chunks()
 {
    use aqe_live_protocol::{
        EXECUTION_EVIDENCE_CHUNK_BYTES, assemble_execution_evidence, execution_evidence_chunks,
    };
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let mut row = insight();
    row.symbol = "α商品🧪".repeat(10_000);
    save(&mut journal, &row, 1).await;
    let page = journal
        .prepare_execution_evidence_export()
        .await
        .unwrap()
        .unwrap();
    let mut chunks = execution_evidence_chunks(&page).unwrap();
    assert!(chunks.len() > 1);
    assert!(
        chunks
            .iter()
            .all(|c| c.body.len() <= EXECUTION_EVIDENCE_CHUNK_BYTES)
    );
    chunks.reverse();
    chunks.push(chunks[0].clone());
    assert_eq!(assemble_execution_evidence(&chunks).unwrap(), page);
    let mut missing = chunks.clone();
    missing.remove(1);
    assert!(assemble_execution_evidence(&missing).is_err());
    let mut corrupt = chunks.clone();
    corrupt[1].body.push('x');
    assert!(assemble_execution_evidence(&corrupt).is_err());
    let mut mixed = chunks.clone();
    mixed[1].source_id = "another-journal".into();
    assert!(assemble_execution_evidence(&mixed).is_err());
    let mut conflicting = chunks.clone();
    conflicting.last_mut().unwrap().body = "tampered".into();
    assert!(assemble_execution_evidence(&conflicting).is_err());
}
