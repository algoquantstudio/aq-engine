use super::*;
use crate::core::{
    broker::{
        command_journal::ExecutionFillBatch,
        fill_history::{ExecutionFillHistory, ExecutionHistoryClock},
    },
    insight::{Insight, execution_checkpoint::InsightExecutionCheckpoint},
};
use aqe_live_protocol::{
    ExecutionFill, ExecutionFillCash, ExecutionFillEffect, ExecutionFillSide,
    PositionQuantitySnapshot,
};

struct Fixture(std::path::PathBuf);
impl Fixture {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("aqe-marks-{}", uuid::Uuid::new_v4())))
    }
    async fn open(&self) -> BrokerCommandJournal {
        BrokerCommandJournal::open(
            self.0.join("journal.db"),
            "generic",
            "account")
        .await
        .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn minute() -> i64 {
    chrono::Utc::now().timestamp_micros() / POSITION_MARK_MINUTE_US * POSITION_MARK_MINUTE_US
        - 2 * POSITION_MARK_MINUTE_US
}
async fn seed(
    j: &mut BrokerCommandJournal,
    order: &str,
    position: &str,
) -> crate::core::broker::fill_history::FillHistoryOwner {
    let mut insight = Insight::default();
    insight.symbol = "ANY".into();
    insight.order_id = Some(order.into());
    let checkpoint = InsightExecutionCheckpoint::capture(&insight, 1, true, None);
    j.persist_execution_batch(&ExecutionCheckpointBatch::new(vec![checkpoint], vec![]).unwrap())
        .await
        .unwrap();
    let owner = crate::core::broker::fill_history::FillHistoryOwner {
        insight_id: insight.insight_id.to_string(),
        execution_epoch: j.lifecycle.epoch,
        entry_order_id: order.into(),
        symbol: "ANY".into(),
    };
    let fill = ExecutionFill {
        version: 1,
        provider: "deals-v1".into(),
        fill_id: format!("deal-{order}"),
        order_id: order.into(),
        position_id: Some(position.into()),
        symbol: "ANY".into(),
        side: ExecutionFillSide::Buy,
        effect: ExecutionFillEffect::Entry,
        quantity: 2.,
        price: 100.,
        occurred_at_us: 100,
        as_of_us: 200,
        cash: Some(ExecutionFillCash {
            currency: "GBP".into(),
            gross_pnl: Some(0.),
            commission: Some(1.),
            carry: Some(0.),
        }),
        account_notional: None,
    };
    let h = ExecutionFillHistory {
        account_id: "account".into(),
        entry_order_id: order.into(),
        position_id: position.into(),
        symbol: "ANY".into(),
        as_of_us: 200,
        clock: ExecutionHistoryClock::Utc,
        fills: vec![fill],
        invalidations: vec![],
        position_snapshot: Some(PositionQuantitySnapshot {
            position_id: position.into(),
            symbol: "ANY".into(),
            quantity: 2.,
            side: Some(ExecutionFillSide::Buy),
            as_of_us: 200,
        }),
    };
    j.record_execution_fills(&ExecutionFillBatch::from_history(owner.clone(), h).unwrap())
        .await
        .unwrap();
    owner
}
fn values(position: &str, at: i64) -> PositionValuations {
    serde_json::from_value(serde_json::json!({"version":1,"provider":"values-v1","account_id":"account","account_currency":"GBP","symbol":"ANY","as_of_us":at,
    "positions":[{"position_id":position,"status":"open","quantity":2.0,"side":"buy","average_entry_price":100.0,"current_price":110.0,"gross_unrealized_pnl":20.0,"carry":-0.5,"quote_time":null,"notional":{"basis":"unavailable","reason":"currency_conversion_unavailable"}}]})).unwrap()
}

#[tokio::test]
async fn split_cloud_history_keeps_canonical_mark_ownership_and_frozen_transfer() {
    use crate::core::live_sync::{PhysicalCaptureLimit,MIN_OPERATIONAL_HEADROOM_BYTES};
    let fixture=Fixture::new();
    let mut journal=fixture.open().await;
    seed(&mut journal,"order","position").await;
    let mut journal=journal.with_separate_analytics_capture_budget(PhysicalCaptureLimit {
        physical_limit_bytes:64*1024*1024,operational_headroom_bytes:MIN_OPERATIONAL_HEADROOM_BYTES,
    }).await.unwrap();
    let minute=minute();
    let owners=journal.position_mark_owners(minute,None).await.unwrap().owners;
    assert_eq!(owners.len(),1);
    assert_eq!(journal.record_position_marks(PositionMarkBatch::new(owners.clone(),values("position",minute+100),minute).unwrap()).await.unwrap(),1);
    let frozen=journal.prepare_position_mark_export().await.unwrap().unwrap();
    drop(journal);
    let mut journal=fixture.open().await;
    assert_eq!(journal.prepare_position_mark_export().await.unwrap().unwrap(),frozen);
    // A canonical checkpoint change revokes a stale mark even though the
    // optional store still has its prior position comparison.
    journal.connection.execute("UPDATE broker_insight_checkpoints SET materialized=0",()).await.unwrap();
    assert!(journal.position_mark_owners(minute+POSITION_MARK_MINUTE_US,None).await.unwrap().owners.is_empty());
    assert_eq!(journal.record_position_marks(PositionMarkBatch::new(owners,values("position",minute+POSITION_MARK_MINUTE_US+100),minute+POSITION_MARK_MINUTE_US).unwrap()).await.unwrap(),0);
    assert_eq!(journal.position_mark_page(0).await.unwrap().records.len(),1);
}

#[tokio::test]
async fn scheduled_marks_are_owned_minute_bounded_and_survive_without_state_archive() {
    let f = Fixture::new();
    let mut j = f.open().await;
    seed(&mut j, "order", "position").await;
    let slot = minute();
    let page = j.position_mark_owners(slot, None).await.unwrap();
    assert_eq!(page.owners.len(), 1);
    let owner = page.owners[0].clone();
    let batch = || {
        PositionMarkBatch::new(vec![owner.clone()], values("position", slot + 100), slot).unwrap()
    };
    assert_eq!(j.record_position_marks(batch()).await.unwrap(), 1);
    assert_eq!(j.record_position_marks(batch()).await.unwrap(), 0);
    assert!(
        j.position_mark_owners(slot, None)
            .await
            .unwrap()
            .owners
            .is_empty()
    );
    let record = j.position_mark_page(0).await.unwrap();
    assert_eq!(record.records.len(), 1);
    assert_eq!(record.records[0].observation.values.account_currency, "GBP");
    assert_eq!(record.records[0].observation.values.as_of_us, slot + 100);
    let mut archive = j
        .connection
        .query("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='broker_execution_evidence'", ())
        .await
        .unwrap();
    assert_eq!(
        archive
            .next()
            .await
            .unwrap()
            .unwrap()
            .get::<i64>(0)
            .unwrap(),
        0
    );
    drop(archive);
    drop(j);
    let mut j = f.open().await;
    assert_eq!(
        j.position_mark_page(0).await.unwrap().records,
        record.records
    );
    assert_eq!(
        j.record_position_marks(
            PositionMarkBatch::new(
                vec![owner.clone()],
                values("position", slot + POSITION_MARK_MINUTE_US + 100),
                slot + POSITION_MARK_MINUTE_US
            )
            .unwrap()
        )
        .await
        .unwrap(),
        1
    );
    let second = j.position_mark_page(1).await.unwrap();
    assert_eq!(second.records.len(), 1);
    assert_eq!(second.records[0].sequence, 2);
    let mut foreign = values("position", slot);
    foreign.account_id = "foreign".into();
    assert!(
        j.record_position_marks(PositionMarkBatch::new(vec![owner], foreign, slot).unwrap())
            .await
            .is_err()
    );
    assert_eq!(j.position_mark_page(0).await.unwrap().records.len(), 2);
}

#[tokio::test]
async fn physical_mark_refusal_keeps_the_source_lease_and_export_head_unchanged() {
    use crate::core::live_sync::{PhysicalCaptureLimit,MIN_OPERATIONAL_HEADROOM_BYTES};
    let fixture=Fixture::new();let mut journal=fixture.open().await;
    seed(&mut journal,"order","position").await;
    journal=journal.with_analytics_capture_budget(PhysicalCaptureLimit {
        physical_limit_bytes:64*1024*1024,operational_headroom_bytes:MIN_OPERATIONAL_HEADROOM_BYTES,
    }).await.unwrap();
    let mut rows=journal.connection.query("PRAGMA wal_checkpoint(TRUNCATE)",()).await.unwrap();
    while rows.next().await.unwrap().is_some() {}drop(rows);
    let usage=journal.analytics_capture_usage().await.unwrap().unwrap();
    journal.analytics_capture_budget=Some(PhysicalCaptureLimit {
        physical_limit_bytes:MIN_OPERATIONAL_HEADROOM_BYTES+usage.projected_commit_bytes-1,
        operational_headroom_bytes:MIN_OPERATIONAL_HEADROOM_BYTES,
    });
    let slot=minute();let owner=journal.position_mark_owners(slot,None).await.unwrap().owners.remove(0);
    assert!(matches!(journal.record_position_marks(PositionMarkBatch::new(vec![owner.clone()],values("position",slot+100),slot).unwrap()).await,Err(BrokerCommandError::CapturePaused)));
    assert!(journal.position_mark_page(0).await.unwrap().records.is_empty());
    assert_eq!(journal.position_mark_owners(slot,None).await.unwrap().owners.len(),1,"a refused mark did not consume its source sampling lease");
    assert!(journal.prepare_position_mark_export().await.unwrap().is_none());
    let pause=journal.analytics_capture_pause().await.unwrap().unwrap();
    assert_eq!(pause.execution_epoch,1);
    // Mandatory canonical checkpoint persistence remains available while new
    // marks/fills are paused. It is not charged to a cloud analytics partition.
    let mut checkpoint=Insight::default();checkpoint.symbol="ANY".into();
    journal.persist_execution_batch(&ExecutionCheckpointBatch::new(vec![InsightExecutionCheckpoint::capture(&checkpoint,1,true,None)],vec![]).unwrap()).await.unwrap();
    assert_eq!(journal.analytics_capture_pause().await.unwrap(),Some(pause));
}

#[tokio::test]
async fn changed_quantity_ownership_or_fill_lease_never_certifies_a_newer_book() {
    let f = Fixture::new();
    let mut j = f.open().await;
    seed(&mut j, "order", "position").await;
    let slot = minute();
    let owner = j
        .position_mark_owners(slot, None)
        .await
        .unwrap()
        .owners
        .remove(0);
    assert_eq!(
        j.record_position_marks(
            PositionMarkBatch::new(vec![owner.clone()], values("position", 199), slot).unwrap()
        )
        .await
        .unwrap(),
        0,
        "A valuation older than the owned broker comparison cannot certify it"
    );
    let mut changed = values("position", slot);
    if let PositionValuationState::Open { quantity, .. } = &mut changed.positions[0].state {
        *quantity = 3.;
    }
    assert_eq!(
        j.record_position_marks(
            PositionMarkBatch::new(vec![owner.clone()], changed, slot).unwrap()
        )
        .await
        .unwrap(),
        0
    );
    let mut wrong = owner.clone();
    wrong.insight_id = "another-insight".into();
    assert_eq!(
        j.record_position_marks(
            PositionMarkBatch::new(vec![wrong], values("position", slot), slot).unwrap()
        )
        .await
        .unwrap(),
        0
    );
    // The existing fill path revokes the comparison atomically on any revision.
    let mut fill = j.execution_fill_page(0, None, 32).await.unwrap().records[0]
        .observation
        .clone();
    fill.fill.as_of_us = 300;
    fill.fill.cash.as_mut().unwrap().commission = Some(2.);
    j.record_execution_fills(&ExecutionFillBatch::new(vec![fill]).unwrap())
        .await
        .unwrap();
    assert_eq!(
        j.record_position_marks(
            PositionMarkBatch::new(vec![owner], values("position", slot), slot).unwrap()
        )
        .await
        .unwrap(),
        0
    );
    assert!(j.position_mark_page(0).await.unwrap().records.is_empty());
}

#[tokio::test]
async fn mark_owner_pages_and_byte_pages_remain_bounded_and_fair() {
    let f = Fixture::new();
    let mut j = f.open().await;
    for n in 0..35 {
        seed(&mut j, &format!("o-{n:02}"), &format!("p-{n:02}")).await;
    }
    let slot = minute();
    let first = j.position_mark_owners(slot, None).await.unwrap();
    assert_eq!(first.owners.len(), 32);
    let next = j
        .position_mark_owners(slot, first.next_after)
        .await
        .unwrap();
    assert_eq!(next.owners.len(), 3);
    assert!(next.next_after.is_none());
    for owner in first.owners.into_iter().chain(next.owners) {
        let v = values(&owner.position_id, slot);
        j.record_position_marks(PositionMarkBatch::new(vec![owner], v, slot).unwrap())
            .await
            .unwrap();
    }
    let first = j.position_mark_page(0).await.unwrap();
    assert!(first.records.len() <= 32);
    assert!(first.has_more);
    assert!(serde_json::to_vec(&first).unwrap().len() <= POSITION_MARK_TRANSFER_BYTES);
    let next = j.position_mark_page(first.next_sequence).await.unwrap();
    assert_eq!(first.records.len() + next.records.len(), 35);
    assert!(!next.has_more);
}

#[tokio::test]
async fn mark_admission_is_small_bounded_and_rejects_ambiguous_namespaces() {
    let f = Fixture::new();
    let mut j = f.open().await;
    seed(&mut j, "order", "position").await;
    let slot = minute();
    let owner = j
        .position_mark_owners(slot, None)
        .await
        .unwrap()
        .owners
        .remove(0);
    // A valuation position ID has no fill-provider mapping. Never choose one
    // namespace or certify the same physical position twice.
    j.connection.execute("INSERT INTO broker_execution_position_checks(provider,position_id,insight_id,execution_epoch,ledger_sequence,as_of_us,check_json) SELECT 'other-deals',position_id,insight_id,execution_epoch,ledger_sequence,as_of_us,check_json FROM broker_execution_position_checks", ()).await.unwrap();
    assert!(
        j.position_mark_owners(slot, None)
            .await
            .unwrap()
            .owners
            .is_empty()
    );
    assert_eq!(
        j.record_position_marks(
            PositionMarkBatch::new(vec![owner.clone()], values("position", slot), slot).unwrap()
        )
        .await
        .unwrap(),
        0
    );
    let oversized = PositionMarkBatch {
        owners: vec![owner; MAX_MARK_ADMISSION + 1],
        values: values("position", slot),
        sample_minute_us: slot,
    };
    assert!(j.record_position_marks(oversized).await.is_err());
    assert!(j.position_mark_page(0).await.unwrap().records.is_empty());
}

#[tokio::test]
async fn source_time_regression_and_invalid_sampling_minute_do_not_append() {
    let f = Fixture::new();
    let mut j = f.open().await;
    seed(&mut j, "order", "position").await;
    let slot = minute();
    let owner = j
        .position_mark_owners(slot, None)
        .await
        .unwrap()
        .owners
        .remove(0);
    assert_eq!(
        j.record_position_marks(
            PositionMarkBatch::new(vec![owner.clone()], values("position", slot + 100), slot)
                .unwrap()
        )
        .await
        .unwrap(),
        1
    );
    let later = slot + POSITION_MARK_MINUTE_US;
    assert_eq!(
        j.record_position_marks(
            PositionMarkBatch::new(vec![owner.clone()], values("position", slot), later).unwrap()
        )
        .await
        .unwrap(),
        0
    );
    let forged = PositionMarkBatch {
        owners: vec![owner],
        values: values("position", later),
        sample_minute_us: later + 1,
    };
    assert!(j.record_position_marks(forged).await.is_err());
    assert_eq!(j.position_mark_page(0).await.unwrap().records.len(), 1);
}

#[tokio::test]
async fn mark_export_freezes_restart_bytes_and_only_acks_the_exact_pending_page() {
    let f = Fixture::new();
    let mut j = f.open().await;
    seed(&mut j, "order", "position").await;
    assert!(j.prepare_position_mark_export().await.unwrap().is_none());
    let slot = minute();
    let owner = j
        .position_mark_owners(slot, None)
        .await
        .unwrap()
        .owners
        .remove(0);
    j.record_position_marks(
        PositionMarkBatch::new(vec![owner.clone()], values("position", slot), slot).unwrap(),
    )
    .await
    .unwrap();
    let frozen = j.prepare_position_mark_export().await.unwrap().unwrap();
    j.record_position_marks(
        PositionMarkBatch::new(
            vec![owner],
            values("position", slot + POSITION_MARK_MINUTE_US),
            slot + POSITION_MARK_MINUTE_US,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        j.prepare_position_mark_export().await.unwrap().unwrap(),
        frozen
    );
    drop(j);
    let j = f.open().await;
    assert_eq!(
        j.prepare_position_mark_export().await.unwrap().unwrap(),
        frozen
    );
    for field in 0..4 {
        let mut bad = frozen.receipt();
        match field {
            0 => bad.source_id = "foreign".into(),
            1 => bad.transfer_id = "foreign".into(),
            2 => bad.after_sequence = 1,
            _ => bad.next_sequence = 2,
        };
        assert!(j.acknowledge_position_mark_export(&bad).await.is_err());
        assert_eq!(
            j.prepare_position_mark_export().await.unwrap().unwrap(),
            frozen
        );
    }
    j.acknowledge_position_mark_export(&frozen.receipt())
        .await
        .unwrap();
    j.acknowledge_position_mark_export(&frozen.receipt())
        .await
        .unwrap();
    let next = j.prepare_position_mark_export().await.unwrap().unwrap();
    assert_eq!((next.after_sequence, next.next_sequence), (1, 2));
    assert!(
        j.acknowledge_position_mark_export(&frozen.receipt())
            .await
            .is_ok(),
        "Repeated old receipt must not clear a newer pending page"
    );
    assert_eq!(
        j.prepare_position_mark_export().await.unwrap().unwrap(),
        next
    );
    j.acknowledge_position_mark_export(&next.receipt())
        .await
        .unwrap();
    assert!(j.prepare_position_mark_export().await.unwrap().is_none());
    // State and fill cursors were not advanced by mark receipts.
    assert!(j.prepare_execution_fill_export().await.unwrap().is_some());
}

#[tokio::test]
async fn a_confirmed_flat_comparison_allows_one_final_absence_mark_not_a_close_event() {
    let f = Fixture::new();
    let mut j = f.open().await;
    let fill_owner = seed(&mut j, "order", "position").await;
    let slot = minute();
    let owner = j
        .position_mark_owners(slot, None)
        .await
        .unwrap()
        .owners
        .remove(0);
    j.record_position_marks(
        PositionMarkBatch::new(vec![owner], values("position", slot), slot).unwrap(),
    )
    .await
    .unwrap();
    let mut entry = j.execution_fill_page(0, None, 32).await.unwrap().records[0]
        .observation
        .fill
        .clone();
    entry.as_of_us = 500;
    let mut exit = entry.clone();
    exit.fill_id = "exit-deal".into();
    exit.order_id = "exit-order".into();
    exit.effect = ExecutionFillEffect::Exit;
    exit.side = ExecutionFillSide::Sell;
    exit.occurred_at_us = 400;
    exit.as_of_us = 500;
    exit.price = 110.;
    exit.cash.as_mut().unwrap().gross_pnl = Some(20.);
    let history = ExecutionFillHistory {
        account_id: "account".into(),
        entry_order_id: "order".into(),
        position_id: "position".into(),
        symbol: "ANY".into(),
        as_of_us: 500,
        clock: ExecutionHistoryClock::Utc,
        fills: vec![entry, exit],
        invalidations: vec![],
        position_snapshot: Some(PositionQuantitySnapshot {
            position_id: "position".into(),
            symbol: "ANY".into(),
            quantity: 0.,
            side: None,
            as_of_us: 500,
        }),
    };
    j.record_execution_fills(&ExecutionFillBatch::from_history(fill_owner, history).unwrap())
        .await
        .unwrap();
    let later = slot + POSITION_MARK_MINUTE_US;
    let owner = j
        .position_mark_owners(later, None)
        .await
        .unwrap()
        .owners
        .remove(0);
    assert_eq!(owner.quantity, 0.);
    let mut absence = values("position", later);
    absence.positions[0].state = PositionValuationState::NotOpen;
    assert_eq!(
        j.record_position_marks(PositionMarkBatch::new(vec![owner], absence, later).unwrap())
            .await
            .unwrap(),
        1
    );
    assert!(
        j.position_mark_owners(later + POSITION_MARK_MINUTE_US, None)
            .await
            .unwrap()
            .owners
            .is_empty()
    );
    let marks = j.position_mark_page(0).await.unwrap();
    assert_eq!(marks.records.len(), 2);
    assert!(!marks.records[1].is_open());
    let fills = j.execution_fill_page(0, None, 32).await.unwrap();
    assert_eq!(
        fills
            .records
            .iter()
            .filter(|r| r.observation.fill.effect == ExecutionFillEffect::Exit)
            .count(),
        1
    );
}
