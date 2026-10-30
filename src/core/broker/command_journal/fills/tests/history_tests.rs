use super::*;
use crate::core::{
    broker::fill_history::ExecutionHistoryClock,
    insight::{Insight, execution_checkpoint::InsightExecutionCheckpoint},
};

fn snapshot() -> ExecutionFillHistory {
    let entry = input("entry", ExecutionFillEffect::Entry, 2., 0.).fill;
    let mut exit = input("exit", ExecutionFillEffect::Exit, 1., 5.).fill;
    exit.side = ExecutionFillSide::Sell;
    exit.order_id = "exit-order".into();
    ExecutionFillHistory {
        account_id: "account".into(),
        entry_order_id: "order".into(),
        position_id: "position".into(),
        symbol: "GENERIC-FUTURE".into(),
        as_of_us: 200,
        clock: ExecutionHistoryClock::Utc,
        fills: vec![entry, exit],
        invalidations: vec![],
        position_snapshot: None,
    }
}

fn with_position(mut history: ExecutionFillHistory, quantity: f64) -> ExecutionFillHistory {
    history.position_snapshot = Some(
        crate::core::broker::fill_history::PositionQuantitySnapshot {
            position_id: history.position_id.clone(),
            symbol: history.symbol.clone(),
            quantity,
            side: (quantity > 0.).then_some(ExecutionFillSide::Buy),
            as_of_us: history.as_of_us,
        },
    );
    history
}

#[test]
fn position_quantity_check_distinguishes_legacy_mismatch_and_invalid_snapshots() {
    assert!(snapshot().position_check().is_none());
    let h = with_position(snapshot(), 1.);
    let check = h.position_check().unwrap();
    assert!(check.matched);
    assert_eq!(check.ledger_quantity, Some(1.));
    assert!(
        !with_position(snapshot(), 1.25)
            .position_check()
            .unwrap()
            .matched
    );
    assert!(
        !with_position(snapshot(), 0.)
            .position_check()
            .unwrap()
            .matched
    );
    let mut h = h;
    h.position_snapshot.as_mut().unwrap().side = Some(ExecutionFillSide::Sell);
    assert!(!h.position_check().unwrap().matched);
    h.position_snapshot.as_mut().unwrap().as_of_us = 199;
    assert!(h.position_check().is_none());
    let mut h = with_position(snapshot(), 0.);
    h.fills[1].quantity = 2.;
    assert!(h.position_check().unwrap().matched);
    let mut old = serde_json::to_value(&h).unwrap();
    old.as_object_mut().unwrap().remove("position_snapshot");
    assert!(
        serde_json::from_value::<ExecutionFillHistory>(old)
            .unwrap()
            .position_check()
            .is_none()
    );
}

#[test]
fn position_quantity_check_is_order_independent_but_rejects_negative_prefixes() {
    let mut h = snapshot();
    h.fills[0].quantity = 0.5;
    h.fills[0].occurred_at_us = 100;
    h.fills[1].occurred_at_us = 150;
    let mut later_entry = h.fills[0].clone();
    later_entry.fill_id = "later-entry".into();
    later_entry.quantity = 1.5;
    later_entry.occurred_at_us = 175;
    h.fills.push(later_entry);
    h = with_position(h, 1.);
    // The final sum matches the broker, but the earlier exit exceeded inventory.
    let invalid = h.position_check().unwrap();
    assert!(!invalid.matched);
    assert_eq!(invalid.ledger_quantity, None);
    assert_eq!(invalid.ledger_side, None);

    // The source has only timestamp ordering. Simultaneous partial executions
    // must not acquire an arbitrary order from broker response order or IDs.
    h.fills[2].occurred_at_us = 150;
    let simultaneous = h.position_check().unwrap();
    assert!(simultaneous.matched);
    assert_eq!(simultaneous.ledger_quantity, Some(1.));
    h.fills.reverse();
    assert_eq!(h.position_check().unwrap(), simultaneous);
}

#[test]
fn position_quantity_check_handles_short_partial_exits_and_rounding_without_dust_loss() {
    let mut h = snapshot();
    h.fills[0].side = ExecutionFillSide::Sell;
    h.fills[0].quantity = 0.3;
    h.fills[1].side = ExecutionFillSide::Buy;
    h.fills[1].quantity = 0.1;
    h = with_position(h, 0.2);
    h.position_snapshot.as_mut().unwrap().side = Some(ExecutionFillSide::Sell);
    assert!(h.position_check().unwrap().matched);

    let mut final_exit = h.fills[1].clone();
    final_exit.fill_id = "final-exit".into();
    final_exit.quantity = 0.2;
    h.fills.push(final_exit);
    h = with_position(h, 0.);
    assert!(h.position_check().unwrap().matched);
    assert_eq!(h.position_check().unwrap().ledger_quantity, Some(0.));

    let mut tiny = snapshot();
    tiny.fills.truncate(1);
    tiny.fills[0].quantity = 1e-12;
    let tiny = with_position(tiny, 0.);
    let check = tiny.position_check().unwrap();
    assert!(
        !check.matched,
        "a small but real position was rounded to flat"
    );
    assert_eq!(check.ledger_quantity, Some(1e-12));
    assert_eq!(check.ledger_side, Some(ExecutionFillSide::Buy));
}

async fn saved_position_check(
    j: &BrokerCommandJournal,
) -> Option<crate::core::broker::fill_history::PositionQuantityCheck> {
    let mut rows = j
        .connection
        .query(
            "SELECT check_json FROM broker_execution_position_checks",
            (),
        )
        .await
        .unwrap();
    rows.next()
        .await
        .unwrap()
        .map(|r| serde_json::from_str(&r.get::<String>(0).unwrap()).unwrap())
}

#[tokio::test]
async fn position_checks_export_without_new_fills_and_revoke_in_sequence_after_restart() {
    let fixture = Fixture::new();
    let mut j = fixture.open().await;
    let owned = owner(&mut j).await;
    let h = with_position(snapshot(), 1.);
    j.record_execution_fills(&ExecutionFillBatch::from_history(owned.clone(), h.clone()).unwrap())
        .await
        .unwrap();
    let first = j.prepare_execution_fill_export().await.unwrap().unwrap();
    let page = first.page().unwrap();
    assert_eq!(
        (
            page.version,
            page.records.len(),
            page.position_checks.len(),
            page.next_sequence
        ),
        (3, 2, 1, 3)
    );
    assert_eq!(page.position_checks[0].observation.ledger_sequence, 2);
    assert!(page.recovery_complete.is_none());
    j.acknowledge_execution_fill_export(&first.receipt())
        .await
        .unwrap();
    j.record_execution_fills(&ExecutionFillBatch::from_history(owned.clone(), h.clone()).unwrap())
        .await
        .unwrap();
    assert!(
        j.prepare_execution_fill_export().await.unwrap().is_none(),
        "same check appended twice"
    );
    let mut h = h;
    h.as_of_us = 400;
    h.position_snapshot.as_mut().unwrap().as_of_us = 400;
    for f in &mut h.fills {
        f.as_of_us = 400;
    }
    j.record_execution_fills(&ExecutionFillBatch::from_history(owned.clone(), h.clone()).unwrap())
        .await
        .unwrap();
    assert!(
        j.prepare_execution_fill_export().await.unwrap().is_none(),
        "timestamp-only refresh appended another check"
    );
    assert_eq!(
        saved_position_check(&j).await.unwrap().snapshot.as_of_us,
        400
    );
    drop(j);
    let mut j = fixture.open().await;
    j.record_execution_fills(&ExecutionFillBatch::from_history(owned.clone(), h.clone()).unwrap())
        .await
        .unwrap();
    assert!(
        j.prepare_execution_fill_export().await.unwrap().is_none(),
        "restart lost check deduplication"
    );
    // Actual quantity/match changes still export, even without a new fill.
    h.as_of_us = 450;
    h.position_snapshot.as_mut().unwrap().as_of_us = 450;
    h.position_snapshot.as_mut().unwrap().quantity = 1.25;
    for f in &mut h.fills {
        f.as_of_us = 450;
    }
    j.record_execution_fills(&ExecutionFillBatch::from_history(owned, h.clone()).unwrap())
        .await
        .unwrap();
    let refresh = j.prepare_execution_fill_export().await.unwrap().unwrap();
    assert!(refresh.page().unwrap().records.is_empty());
    assert_eq!(
        refresh.page().unwrap().position_checks[0]
            .observation
            .as_of_us,
        450
    );
    drop(j);
    let mut j = fixture.open().await;
    assert_eq!(
        j.prepare_execution_fill_export().await.unwrap().unwrap(),
        refresh
    );
    j.acknowledge_execution_fill_export(&refresh.receipt())
        .await
        .unwrap();
    let mut correction = h.fills[1].clone();
    correction.as_of_us = 500;
    correction.quantity = 0.5;
    j.record_execution_fills(
        &ExecutionFillBatch::new(vec![ExecutionFillInput {
            insight_id: Some(page.position_checks[0].observation.insight_id.clone()),
            fill: correction,
        }])
        .unwrap(),
    )
    .await
    .unwrap();
    let revoked = j
        .prepare_execution_fill_export()
        .await
        .unwrap()
        .unwrap()
        .page()
        .unwrap();
    assert_eq!(
        (revoked.records.len(), revoked.position_checks.len()),
        (1, 1)
    );
    assert!(revoked.position_checks[0].sequence > revoked.records[0].sequence);
    assert!(revoked.position_checks[0].observation.check.is_none());
    assert!(j.recovery_page(0, 1).await.unwrap().is_empty());
}

#[tokio::test]
async fn timestamp_only_checks_keep_frozen_exports_and_do_not_hide_side_changes() {
    let fixture = Fixture::new();
    let mut j = fixture.open().await;
    let owned = owner(&mut j).await;
    let mut h = with_position(snapshot(), 1.);
    j.record_execution_fills(&ExecutionFillBatch::from_history(owned.clone(), h.clone()).unwrap())
        .await
        .unwrap();
    let frozen = j.prepare_execution_fill_export().await.unwrap().unwrap();
    for at in 201..=220 {
        h.as_of_us = at;
        h.position_snapshot.as_mut().unwrap().as_of_us = at;
        for fill in &mut h.fills {
            fill.as_of_us = at;
        }
        j.record_execution_fills(
            &ExecutionFillBatch::from_history(owned.clone(), h.clone()).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            j.prepare_execution_fill_export().await.unwrap().unwrap(),
            frozen
        );
    }
    assert_eq!(
        saved_position_check(&j).await.unwrap().snapshot.as_of_us,
        220
    );
    j.acknowledge_execution_fill_export(&frozen.receipt())
        .await
        .unwrap();
    assert!(j.prepare_execution_fill_export().await.unwrap().is_none());
    // Same quantity, different side is a meaningful mismatch, not a duplicate.
    h.as_of_us = 230;
    h.position_snapshot.as_mut().unwrap().as_of_us = 230;
    h.position_snapshot.as_mut().unwrap().side = Some(ExecutionFillSide::Sell);
    for fill in &mut h.fills {
        fill.as_of_us = 230;
    }
    j.record_execution_fills(&ExecutionFillBatch::from_history(owned.clone(), h.clone()).unwrap())
        .await
        .unwrap();
    let changed = j.prepare_execution_fill_export().await.unwrap().unwrap();
    let page = changed.page().unwrap();
    assert!(page.records.is_empty());
    assert_eq!(page.position_checks.len(), 1);
    assert!(
        !page.position_checks[0]
            .observation
            .check
            .as_ref()
            .unwrap()
            .matched
    );
    j.acknowledge_execution_fill_export(&changed.receipt())
        .await
        .unwrap();
    // The local freshness watermark still rejects conflicting same-time facts.
    h.position_snapshot.as_mut().unwrap().quantity = 1.5;
    assert!(
        j.record_execution_fills(&ExecutionFillBatch::from_history(owned, h).unwrap())
            .await
            .is_err()
    );
    assert!(j.prepare_execution_fill_export().await.unwrap().is_none());
    assert_eq!(
        saved_position_check(&j).await.unwrap().snapshot.quantity,
        1.
    );
}

#[tokio::test]
async fn position_quantity_check_survives_restart_but_not_newer_or_unreported_fills() {
    let fixture = Fixture::new();
    let mut j = fixture.open().await;
    let owned = owner(&mut j).await;
    let h = with_position(snapshot(), 1.);
    j.record_execution_fills(&ExecutionFillBatch::from_history(owned.clone(), h).unwrap())
        .await
        .unwrap();
    assert!(saved_position_check(&j).await.unwrap().matched);
    drop(j);
    let mut j = fixture.open().await;
    assert!(saved_position_check(&j).await.unwrap().matched);
    let mut h = snapshot();
    h.as_of_us = 300;
    for f in &mut h.fills {
        f.as_of_us = 300;
    }
    j.record_execution_fills(
        &ExecutionFillBatch::from_history(owned.clone(), with_position(h.clone(), 1.)).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        saved_position_check(&j).await.unwrap().snapshot.as_of_us,
        300
    );
    assert_eq!(
        j.execution_fill_page(0, None, 32)
            .await
            .unwrap()
            .records
            .len(),
        2,
        "unchanged position refresh duplicated fill history"
    );

    let mut correction = h.fills[1].clone();
    correction.as_of_us = 400;
    correction.quantity = 0.5;
    j.record_execution_fills(
        &ExecutionFillBatch::new(vec![ExecutionFillInput {
            insight_id: Some(owned.insight_id.clone()),
            fill: correction,
        }])
        .unwrap(),
    )
    .await
    .unwrap();
    assert!(
        saved_position_check(&j).await.is_none(),
        "newer fill retained stale position proof"
    );
    j.record_execution_fills(
        &ExecutionFillBatch::from_history(owned.clone(), with_position(h.clone(), 1.)).unwrap(),
    )
    .await
    .unwrap();
    assert!(
        saved_position_check(&j).await.is_none(),
        "stale history certified corrected ledger"
    );
    h.as_of_us = 500;
    for f in &mut h.fills {
        f.as_of_us = 500;
    }
    h.fills[1].quantity = 0.5;
    j.record_execution_fills(
        &ExecutionFillBatch::from_history(owned.clone(), with_position(h.clone(), 1.4)).unwrap(),
    )
    .await
    .unwrap();
    assert!(
        !saved_position_check(&j).await.unwrap().matched,
        "diagnostic mismatch was discarded"
    );
    let mut extra = h.fills[0].clone();
    extra.fill_id = "additional".into();
    extra.as_of_us = 600;
    j.record_execution_fills(
        &ExecutionFillBatch::new(vec![ExecutionFillInput {
            insight_id: Some(owned.insight_id.clone()),
            fill: extra,
        }])
        .unwrap(),
    )
    .await
    .unwrap();
    h.as_of_us = 700;
    for f in &mut h.fills {
        f.as_of_us = 700;
    }
    j.record_execution_fills(
        &ExecutionFillBatch::from_history(owned, with_position(h, 1.5)).unwrap(),
    )
    .await
    .unwrap();
    assert!(
        saved_position_check(&j).await.is_none(),
        "history omitted a known position fill"
    );
    assert!(
        j.recovery_page(0, 1).await.unwrap().is_empty(),
        "analysis created trading commands"
    );
}

#[tokio::test]
async fn position_quantity_check_is_invalidated_by_cancelled_deals_and_can_be_reobserved() {
    let fixture = Fixture::new();
    let mut j = fixture.open().await;
    let owned = owner(&mut j).await;
    j.record_execution_fills(
        &ExecutionFillBatch::from_history(owned.clone(), with_position(snapshot(), 1.)).unwrap(),
    )
    .await
    .unwrap();
    let mut h = snapshot();
    let exit = h.fills.pop().unwrap();
    let cancelled = super::invalidation_tests::invalidate(&exit, 300);
    j.record_execution_fills(&ExecutionFillBatch::invalidations(vec![cancelled.clone()]).unwrap())
        .await
        .unwrap();
    assert!(saved_position_check(&j).await.is_none());
    h.as_of_us = 400;
    h.fills[0].as_of_us = 400;
    h.invalidations = vec![cancelled];
    h.invalidations[0].as_of_us = 400;
    j.record_execution_fills(
        &ExecutionFillBatch::from_history(owned, with_position(h, 2.)).unwrap(),
    )
    .await
    .unwrap();
    assert!(saved_position_check(&j).await.unwrap().matched);
    let mut unseen = super::invalidation_tests::invalidate(&exit, 500);
    unseen.fill_id = "unobserved-cancelled-deal".into();
    j.record_execution_fills(&ExecutionFillBatch::invalidations(vec![unseen]).unwrap())
        .await
        .unwrap();
    assert!(
        saved_position_check(&j).await.is_none(),
        "new unknown cancellation retained old proof"
    );
}
async fn owner(j: &mut BrokerCommandJournal) -> FillHistoryOwner {
    let mut insight = Insight::default();
    insight.symbol = "GENERIC-FUTURE".into();
    insight.order_id = Some("order".into());
    insight.state = crate::core::insight::types::InsightState::Closed;
    let checkpoint = InsightExecutionCheckpoint::capture(&insight, 1, true, None);
    j.persist_execution_batch(&ExecutionCheckpointBatch::new(vec![checkpoint], vec![]).unwrap())
        .await
        .unwrap();
    j.fill_history_owner_page("")
        .await
        .unwrap()
        .owners
        .remove(0)
}

#[tokio::test]
async fn canonical_checkpoint_owns_history_without_a_second_state_archive() {
    let fixture = Fixture::new();
    let mut j = fixture.open().await;
    let owned = owner(&mut j).await;
    async fn count(j: &BrokerCommandJournal) -> i64 {
        let mut rows = j
            .connection
            .query("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='broker_execution_evidence'", ())
            .await
            .unwrap();
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap()
    }
    assert_eq!(count(&j).await, 0);
    j.record_execution_fills(
        &ExecutionFillBatch::from_history(owned.clone(), with_position(snapshot(), 1.)).unwrap(),
    )
    .await
    .unwrap();
    assert!(saved_position_check(&j).await.unwrap().matched);
    let frozen = j.prepare_execution_fill_export().await.unwrap().unwrap();
    assert!(!frozen.page().unwrap().records.is_empty());
    j.request_execution_stop().await.unwrap();
    assert!(j.complete_execution_stop().await.unwrap());
    assert_eq!(j.begin_execution_epoch().await.unwrap().epoch, 2);
    let mut checkpoint = j
        .execution_checkpoint_page("", 1)
        .await
        .unwrap()
        .rows
        .remove(0);
    checkpoint.revision += 1;
    checkpoint.commission = Some(7.);
    j.persist_execution_batch(&ExecutionCheckpointBatch::new(vec![checkpoint], vec![]).unwrap())
        .await
        .unwrap();
    assert_eq!(
        j.fill_history_owner_page("").await.unwrap().owners[0],
        owned
    );
    assert_eq!(count(&j).await, 0);
    drop(j);
    let j = fixture.open().await;
    assert_eq!(
        j.fill_history_owner_page("").await.unwrap().owners[0],
        owned
    );
    assert_eq!(
        j.prepare_execution_fill_export().await.unwrap().unwrap(),
        frozen
    );
    assert_eq!(
        j.execution_checkpoint_page("", 1).await.unwrap().rows[0].commission,
        Some(7.)
    );
    assert_eq!(count(&j).await, 0);
}

#[test]
fn history_rejects_mixed_ownership_currency_sides_quantity_and_clock() {
    let valid = snapshot();
    let check = |s: &ExecutionFillHistory| s.validate("account", "order", "GENERIC-FUTURE");
    check(&valid).unwrap();
    let mut bad = valid.clone();
    bad.account_id = "foreign".into();
    assert!(check(&bad).is_err());
    let mut bad = valid.clone();
    bad.fills[0].order_id = "other-entry".into();
    assert!(check(&bad).is_err());
    let mut bad = valid.clone();
    bad.fills[1].side = ExecutionFillSide::Buy;
    assert!(check(&bad).is_err());
    let mut bad = valid.clone();
    bad.fills[1].quantity = 3.;
    assert!(check(&bad).is_err());
    let mut bad = valid.clone();
    bad.fills[1].effect = ExecutionFillEffect::Reversal;
    assert!(check(&bad).is_err());
    let mut bad = valid.clone();
    bad.fills[1].cash.as_mut().unwrap().currency = "GBP".into();
    assert!(check(&bad).is_err());
    let mut bad = valid.clone();
    bad.fills.push(bad.fills[0].clone());
    assert!(check(&bad).is_err());
    let mut bad = valid.clone();
    bad.clock = ExecutionHistoryClock::DeclaredFixedOffset {
        seconds: i32::MIN,
        valid_from_us: 1,
        valid_through_us: 300,
    };
    assert!(check(&bad).is_err());
    let mut bad = valid.clone();
    bad.clock = ExecutionHistoryClock::DeclaredFixedOffset {
        seconds: 3600,
        valid_from_us: 101,
        valid_through_us: 300,
    };
    assert!(check(&bad).is_err());
    let mut bad = valid;
    bad.fills = vec![bad.fills[0].clone(); 257];
    assert!(check(&bad).is_err());
}

#[tokio::test]
async fn owned_history_cancellation_retracts_existing_fill_without_inventing_economics() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let owned = owner(&mut journal).await;
    journal
        .record_execution_fills(
            &ExecutionFillBatch::from_history(owned.clone(), snapshot()).unwrap(),
        )
        .await
        .unwrap();
    let mut history = snapshot();
    history.as_of_us = 300;
    let cancelled = history.fills.pop().unwrap();
    history
        .invalidations
        .push(super::invalidation_tests::invalidate(&cancelled, 300));
    history.fills[0].as_of_us = 300;
    journal
        .record_execution_fills(
            &ExecutionFillBatch::from_history(owned.clone(), history.clone()).unwrap(),
        )
        .await
        .unwrap();
    journal
        .record_execution_fills(&ExecutionFillBatch::from_history(owned, history.clone()).unwrap())
        .await
        .unwrap();
    let page = journal.execution_fill_page(0, None, 32).await.unwrap();
    assert_eq!(page.records.len(), 3);
    assert!(page.records[2].invalidation.is_some());
    assert_eq!(page.records[2].observation.fill, cancelled);
    history.invalidations[0].order_id = "wrong-entry".into();
    history.invalidations[0].effect = ExecutionFillEffect::Entry;
    assert!(
        history
            .validate("account", "order", "GENERIC-FUTURE")
            .is_err()
    );
}

#[tokio::test]
async fn history_recovers_missing_fills_once_at_original_epoch_after_restart() {
    let fixture = Fixture::new();
    let mut j = fixture.open().await;
    let owner = owner(&mut j).await;
    j.request_execution_stop().await.unwrap();
    assert!(j.complete_execution_stop().await.unwrap());
    j.begin_execution_epoch().await.unwrap();
    drop(j);
    let mut j = fixture.open().await;
    let mut initial_history = snapshot();
    initial_history.clock = ExecutionHistoryClock::DeclaredFixedOffset {
        seconds: -50400,
        valid_from_us: 1,
        valid_through_us: i64::MAX,
    };
    let batch = ExecutionFillBatch::from_history(owner.clone(), initial_history).unwrap();
    j.record_execution_fills(&batch).await.unwrap();
    j.record_execution_fills(&batch).await.unwrap();
    let page = j.execution_fill_page(0, None, 128).await.unwrap();
    assert_eq!(page.records.len(), 2);
    assert!(page.records.iter().all(|r| r.execution_epoch == 1
        && r.observation.insight_id.as_deref() == Some(owner.insight_id.as_str())));
    assert!(page.coverage.contains("bounded_owned_history"));
    assert!(page.coverage.contains("declared_fixed_offset"));
    assert_eq!(page.recovery_complete, None);
    aqe_live_protocol::ExecutionFillTransfer::from_page(&page).unwrap();
    let mut correction = snapshot();
    correction.as_of_us = 300;
    for fill in &mut correction.fills {
        fill.as_of_us = 300;
    }
    correction.fills[1].cash.as_mut().unwrap().gross_pnl = Some(6.);
    j.record_execution_fills(&ExecutionFillBatch::from_history(owner, correction).unwrap())
        .await
        .unwrap();
    let page = j.execution_fill_page(0, None, 128).await.unwrap();
    assert_eq!(page.records.len(), 3);
    assert!(page.coverage.contains("multiple_explicit_clock_windows"));
    aqe_live_protocol::ExecutionFillTransfer::from_page(&page).unwrap();
    assert!(
        j.execution_fill_page(0, Some(2), 128)
            .await
            .unwrap()
            .coverage
            .contains("declared_fixed_offset")
    );
    assert_eq!(page.records[2].execution_epoch, 1);
    assert_eq!(
        page.records[2]
            .observation
            .fill
            .cash
            .as_ref()
            .unwrap()
            .gross_pnl,
        Some(6.)
    );
    assert!(
        j.recovery_page(0, 1).await.unwrap().is_empty(),
        "analytics created trading commands"
    );
}

#[tokio::test]
async fn history_rechecks_account_epoch_and_ambiguous_ownership_atomically() {
    let fixture = Fixture::new();
    let mut j = fixture.open().await;
    let owned = owner(&mut j).await;
    let mut foreign = snapshot();
    foreign.account_id = "foreign".into();
    assert!(
        j.record_execution_fills(
            &ExecutionFillBatch::from_history(owned.clone(), foreign).unwrap()
        )
        .await
        .is_err()
    );
    let mut wrong_epoch = owned.clone();
    wrong_epoch.execution_epoch = 2;
    assert!(
        j.record_execution_fills(
            &ExecutionFillBatch::from_history(wrong_epoch, snapshot()).unwrap()
        )
        .await
        .is_err()
    );
    // Two independently saved Insights claiming one order are not a valid proof.
    owner(&mut j).await;
    assert!(
        j.record_execution_fills(&ExecutionFillBatch::from_history(owned, snapshot()).unwrap())
            .await
            .is_err()
    );
    assert!(
        j.execution_fill_page(0, None, 128)
            .await
            .unwrap()
            .records
            .is_empty()
    );
}

#[tokio::test]
async fn history_owner_pages_include_terminal_rows_and_are_bounded() {
    let fixture = Fixture::new();
    let mut j = fixture.open().await;
    for i in 0..65 {
        let mut insight = Insight::default();
        insight.symbol = "GENERIC-FUTURE".into();
        insight.order_id = Some(format!("order-{i}"));
        insight.state = crate::core::insight::types::InsightState::Closed;
        j.persist_execution_batch(
            &ExecutionCheckpointBatch::new(
                vec![InsightExecutionCheckpoint::capture(&insight, 1, true, None)],
                vec![],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    }
    let mut cursor = String::new();
    let mut ids = std::collections::HashSet::new();
    loop {
        let page = j.fill_history_owner_page(&cursor).await.unwrap();
        assert!(page.owners.len() <= 32);
        for owner in page.owners {
            assert!(ids.insert(owner.insight_id));
        }
        match page.next_cursor {
            Some(next) => {
                assert!(next > cursor);
                cursor = next;
            }
            None => break,
        }
    }
    assert_eq!(ids.len(), 65);
}

#[tokio::test]
async fn active_fill_history_bypasses_archive_without_losing_historical_owners() {
    use crate::core::insight::types::InsightState;
    let fixture=Fixture::new();
    let mut j=fixture.open().await;
    for n in 1..=65 {
        let mut row=Insight::default();row.insight_id=uuid::Uuid::from_u128(n);
        row.symbol="ANY".into();row.order_id=Some(format!("archive-{n}"));row.state=InsightState::Closed;
        j.persist_execution_batch(&ExecutionCheckpointBatch::new(vec![InsightExecutionCheckpoint::capture(&row,1,true,None)],vec![]).unwrap()).await.unwrap();
    }
    let mut live=Insight::default();live.insight_id=uuid::Uuid::from_u128(1000);
    live.symbol="ANY".into();live.order_id=Some("newly-filled".into());live.state=InsightState::Filled;
    let mut partial=live.clone();partial.insight_id=uuid::Uuid::from_u128(1001);
    partial.order_id=Some("partial".into());partial.state=InsightState::Executed;partial.entry_filled_quantity=Some(1.);
    let mut pending=live.clone();pending.insight_id=uuid::Uuid::from_u128(1002);
    pending.order_id=Some("unfilled".into());pending.state=InsightState::Executed;
    pending.entry_filled_quantity=None;
    for row in [&live,&partial,&pending] {
        j.persist_execution_batch(&ExecutionCheckpointBatch::new(vec![InsightExecutionCheckpoint::capture(row,1,true,None)],vec![]).unwrap()).await.unwrap();
    }
    let archive=j.fill_history_owner_page("").await.unwrap();
    assert_eq!(archive.owners.len(),32);
    assert!(archive.owners.iter().all(|o|o.entry_order_id.starts_with("archive-")),"reproduce why a new fill was stuck behind old UUIDs");
    let active=j.active_fill_history_owner_page("").await.unwrap();
    assert_eq!(active.owners.iter().map(|o|o.entry_order_id.as_str()).collect::<Vec<_>>(),["newly-filled","partial"]);
    assert!(j.active_fill_history_owner_page(active.next_cursor.as_deref().unwrap()).await.unwrap().owners.is_empty());
    partial.state=InsightState::Closed;
    j.persist_execution_batch(&ExecutionCheckpointBatch::new(vec![InsightExecutionCheckpoint::capture(&partial,2,true,None)],vec![]).unwrap()).await.unwrap();
    assert_eq!(j.active_fill_history_owner_page("").await.unwrap().owners.len(),1,"closed and unfilled orders must not consume the active lane");
    drop(j);
    let j=fixture.open().await;
    assert_eq!(j.active_fill_history_owner_page("").await.unwrap().owners[0].entry_order_id,"newly-filled");
    let mut cursor=String::new();let mut total=0;
    loop {
        let page=j.fill_history_owner_page(&cursor).await.unwrap();total+=page.owners.len();
        match page.next_cursor {Some(next)=>cursor=next,None=>break}
    }
    assert_eq!(total,68,"historical recovery is retained independently");
}
