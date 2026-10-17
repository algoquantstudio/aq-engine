use super::*;
use aqe_live_protocol::{ExecutionFill, ExecutionFillCash, ExecutionFillEffect, ExecutionFillSide};
mod history_tests;
mod invalidation_tests;

struct Fixture(std::path::PathBuf);
impl Fixture {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("aqe-fill-ledger-{}", uuid::Uuid::new_v4())))
    }
    async fn open(&self) -> BrokerCommandJournal {
        BrokerCommandJournal::open_with_state_history(
            self.0.join("commands.db"),
            "strategy",
            "account",
            false,
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
fn input(id: &str, effect: ExecutionFillEffect, q: f64, gross: f64) -> ExecutionFillInput {
    ExecutionFillInput {
        insight_id: Some("insight".into()),
        fill: ExecutionFill {
            version: 1,
            provider: "broker-deals-v1".into(),
            fill_id: id.into(),
            order_id: "order".into(),
            position_id: Some("position".into()),
            symbol: "GENERIC-FUTURE".into(),
            side: ExecutionFillSide::Buy,
            effect,
            quantity: q,
            price: 100.,
            occurred_at_us: 100,
            as_of_us: 200,
            cash: Some(ExecutionFillCash {
                currency: "EUR".into(),
                gross_pnl: Some(gross),
                commission: Some(1.),
                carry: Some(0.),
            }),
            account_notional: None,
        },
    }
}
async fn save(j: &mut BrokerCommandJournal, rows: Vec<ExecutionFillInput>) {
    j.record_execution_fills(&ExecutionFillBatch::new(rows).unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn fill_exports_are_bounded_frozen_across_restart_and_acknowledged_independently() {
    let fixture = Fixture::new();
    let mut j = fixture.open().await;
    assert!(j.prepare_execution_fill_export().await.unwrap().is_none());
    let rows = (0..80)
        .map(|i| {
            let mut row = input(&i.to_string(), ExecutionFillEffect::Entry, 1., 0.);
            // Worst-case escaping plus multibyte characters exercises byte caps.
            row.fill.order_id = "\"".repeat(128);
            row.fill.symbol = "契".repeat(85);
            row
        })
        .collect::<Vec<_>>();
    save(&mut j, rows).await;
    let frozen = j.prepare_execution_fill_export().await.unwrap().unwrap();
    assert!(frozen.body.len() <= aqe_live_protocol::EXECUTION_FILL_TRANSFER_BYTES);
    assert!(frozen.page().unwrap().has_more);
    assert!(frozen.page().unwrap().records.len() <= 32);
    assert!(
        j.prepare_execution_evidence_export()
            .await
            .unwrap()
            .is_none()
    );
    save(
        &mut j,
        vec![input("late", ExecutionFillEffect::Exit, 1., 8.)],
    )
    .await;
    drop(j);
    let j = fixture.open().await;
    assert_eq!(
        j.prepare_execution_fill_export().await.unwrap().unwrap(),
        frozen
    );
    for receipt in [
        aqe_live_protocol::ExecutionFillExportReceipt {
            source_id: "foreign".into(),
            ..frozen.receipt()
        },
        aqe_live_protocol::ExecutionFillExportReceipt {
            transfer_id: "wrong".into(),
            ..frozen.receipt()
        },
        aqe_live_protocol::ExecutionFillExportReceipt {
            after_sequence: 1,
            ..frozen.receipt()
        },
        aqe_live_protocol::ExecutionFillExportReceipt {
            next_sequence: 80,
            ..frozen.receipt()
        },
    ] {
        assert!(j.acknowledge_execution_fill_export(&receipt).await.is_err());
    }
    assert_eq!(
        j.prepare_execution_fill_export().await.unwrap().unwrap(),
        frozen
    );
    let mut count = 0;
    let mut after = 0;
    while let Some(transfer) = j.prepare_execution_fill_export().await.unwrap() {
        assert_eq!(transfer.after_sequence, after);
        let page = transfer.page().unwrap();
        count += page.records.len();
        after = page.next_sequence;
        j.acknowledge_execution_fill_export(&transfer.receipt())
            .await
            .unwrap();
        j.acknowledge_execution_fill_export(&transfer.receipt())
            .await
            .unwrap();
    }
    assert_eq!(count, 81);
    assert_eq!(after, 81);
    assert_eq!(
        j.execution_fill_page(0, None, 128)
            .await
            .unwrap()
            .records
            .len(),
        81
    );
    assert!(j.recovery_page(0, 1).await.unwrap().is_empty());
}

#[tokio::test]
async fn fill_transfer_rejects_corrupt_mixed_and_incomplete_pages() {
    let fixture = Fixture::new();
    let mut j = fixture.open().await;
    save(
        &mut j,
        vec![
            input("one", ExecutionFillEffect::Entry, 3., 0.),
            input("two", ExecutionFillEffect::Exit, 1., 2.),
            input("three", ExecutionFillEffect::Exit, 1., 2.),
        ],
    )
    .await;
    let transfer = j.prepare_execution_fill_export().await.unwrap().unwrap();
    let mut bad = transfer.clone();
    bad.body.push(' ');
    assert!(bad.page().is_err());
    bad = transfer.clone();
    bad.account_id = "another-account".into();
    assert!(bad.page().is_err());
    bad = transfer.clone();
    bad.source_id = "other-source".into();
    assert!(bad.page().is_err());
    let mut page = transfer.page().unwrap();
    page.records.clear();
    assert!(aqe_live_protocol::ExecutionFillTransfer::from_page(&page).is_err());
    page = transfer.page().unwrap();
    page.has_more = true;
    assert!(aqe_live_protocol::ExecutionFillTransfer::from_page(&page).is_err());
    page = transfer.page().unwrap();
    page.records[0].observation.fill.quantity = f64::NAN;
    assert!(aqe_live_protocol::ExecutionFillTransfer::from_page(&page).is_err());
    page = transfer.page().unwrap();
    page.records[0].execution_epoch = 0;
    assert!(aqe_live_protocol::ExecutionFillTransfer::from_page(&page).is_err());
    for index in 0..3 {
        page = transfer.page().unwrap();
        page.records.remove(index);
        assert!(aqe_live_protocol::ExecutionFillTransfer::from_page(&page).is_err());
    }
    // A damaged pending page must not silently reset/skip its cursor.
    j.connection
        .execute(
            "UPDATE broker_execution_fill_export SET pending_transfer_id='invalid'",
            (),
        )
        .await
        .unwrap();
    assert!(j.prepare_execution_fill_export().await.is_err());
}

#[tokio::test]
async fn exact_fills_preserve_partial_exits_rebates_and_survive_restart() {
    let fixture = Fixture::new();
    let mut j = fixture.open().await;
    let entry = input("entry", ExecutionFillEffect::Entry, 10., 0.);
    let first = input("partial-1", ExecutionFillEffect::Exit, 3., 30.);
    let mut second = input("partial-2", ExecutionFillEffect::Exit, 2., 10.);
    second.fill.cash.as_mut().unwrap().commission = Some(-2.);
    second.fill.cash.as_mut().unwrap().carry = Some(1.);
    for _ in 0..2 {
        save(&mut j, vec![entry.clone(), first.clone(), second.clone()]).await;
    }
    let before = j.execution_fill_page(0, None, 128).await.unwrap();
    assert_eq!(before.records.len(), 3);
    assert_eq!(before.through_sequence, 3);
    assert_eq!(
        before
            .records
            .iter()
            .map(|r| r.observation.fill.cash.as_ref().unwrap().net().unwrap())
            .sum::<f64>(),
        41.
    );
    let remaining = before
        .records
        .iter()
        .map(|r| match r.observation.fill.effect {
            ExecutionFillEffect::Entry => r.observation.fill.quantity,
            ExecutionFillEffect::Exit => -r.observation.fill.quantity,
            _ => panic!(),
        })
        .sum::<f64>();
    assert_eq!(remaining, 5.);
    assert!(before.recovery_complete.is_none());
    assert!(
        j.execution_checkpoint_page("", 32)
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    assert!(j.recovery_page(0, 32).await.unwrap().is_empty());
    drop(j);
    let j = fixture.open().await;
    let after = j.execution_fill_page(0, None, 128).await.unwrap();
    assert_eq!(after.records, before.records);
    assert_eq!(after.source_id, before.source_id);
    assert_eq!(after.account_id, "account");
    assert_eq!(after.strategy_id, "strategy");
}

#[tokio::test]
async fn corrections_retain_original_epoch_refresh_watermark_and_roll_back_conflicts() {
    let fixture = Fixture::new();
    let mut j = fixture.open().await;
    let first = input("one", ExecutionFillEffect::Exit, 1., 20.);
    save(&mut j, vec![first.clone()]).await;
    let mut refresh = first.clone();
    refresh.fill.as_of_us = 400;
    save(&mut j, vec![refresh]).await;
    let mut stale = first.clone();
    stale.fill.as_of_us = 300;
    stale.fill.quantity = 99.;
    save(&mut j, vec![stale]).await;
    assert_eq!(
        j.execution_fill_page(0, None, 128)
            .await
            .unwrap()
            .records
            .len(),
        1
    );
    j.request_execution_stop().await.unwrap();
    assert!(j.complete_execution_stop().await.unwrap());
    assert_eq!(j.begin_execution_epoch().await.unwrap().epoch, 2);
    let mut correction = first.clone();
    correction.fill.as_of_us = 500;
    correction.fill.cash.as_mut().unwrap().commission = Some(2.);
    save(&mut j, vec![correction.clone()]).await;
    let page = j.execution_fill_page(0, None, 128).await.unwrap();
    assert_eq!(page.records.len(), 2);
    assert_eq!(page.records[1].execution_epoch, 1);
    assert_eq!(
        page.records[1]
            .observation
            .fill
            .cash
            .as_ref()
            .unwrap()
            .net(),
        Some(18.)
    );
    // Earlier valid rows in the same transaction must not survive a conflict.
    let mut conflict = correction;
    conflict.fill.price = 123.;
    let batch = ExecutionFillBatch::new(vec![
        input("rollback", ExecutionFillEffect::Entry, 1., 0.),
        conflict,
    ])
    .unwrap();
    assert!(j.record_execution_fills(&batch).await.is_err());
    assert_eq!(
        j.execution_fill_page(0, None, 128).await.unwrap().records,
        page.records
    );
    drop(j);
    let mut j = fixture.open().await;
    save(&mut j, vec![first]).await;
    assert_eq!(
        j.execution_fill_page(0, None, 128).await.unwrap().records,
        page.records
    );
}

#[tokio::test]
async fn bounded_pages_keep_frozen_watermark_and_unknown_ownership() {
    let fixture = Fixture::new();
    let mut j = fixture.open().await;
    let rows = (0..200)
        .map(|i| {
            let mut row = input(&i.to_string(), ExecutionFillEffect::Entry, 1., 0.);
            row.insight_id = None;
            row.fill.cash = None;
            row
        })
        .collect();
    save(&mut j, rows).await;
    let first = j.execution_fill_page(0, None, 17).await.unwrap();
    assert_eq!(first.records.len(), 17);
    assert!(first.has_more);
    save(
        &mut j,
        vec![input("new", ExecutionFillEffect::Entry, 1., 0.)],
    )
    .await;
    let mut cursor = first.next_sequence;
    let mut all = first.records;
    while cursor < first.through_sequence {
        let page = j
            .execution_fill_page(cursor, Some(first.through_sequence), 500)
            .await
            .unwrap();
        assert!(page.records.len() <= 128);
        cursor = page.next_sequence;
        all.extend(page.records);
    }
    assert_eq!(all.len(), 200);
    assert!(
        all.iter()
            .all(|r| r.observation.insight_id.is_none() && r.observation.fill.cash.is_none())
    );
    assert!(j.execution_fill_page(202, None, 1).await.is_err());
    assert!(j.execution_fill_page(0, Some(u64::MAX), 1).await.is_err());
    assert!(
        ExecutionFillBatch::new(vec![
            input("too-many", ExecutionFillEffect::Entry, 1., 0.);
            257
        ])
        .is_err()
    );
}

#[tokio::test]
async fn fill_identity_cannot_switch_owner_but_unknown_can_be_resolved() {
    let fixture = Fixture::new();
    let mut j = fixture.open().await;
    let mut row = input("one", ExecutionFillEffect::Entry, 1., 0.);
    row.insight_id = None;
    save(&mut j, vec![row.clone()]).await;
    row.insight_id = Some("resolved".into());
    row.fill.as_of_us += 1;
    save(&mut j, vec![row.clone()]).await;
    let before = j.execution_fill_page(0, None, 128).await.unwrap();
    let mut uncorrelated = row.clone();
    uncorrelated.insight_id = None;
    uncorrelated.fill.as_of_us += 1;
    save(&mut j, vec![uncorrelated]).await;
    assert_eq!(
        j.execution_fill_page(0, None, 128).await.unwrap().records,
        before.records
    );
    row.insight_id = Some("foreign".into());
    row.fill.as_of_us += 2;
    assert!(
        j.record_execution_fills(&ExecutionFillBatch::new(vec![row]).unwrap())
            .await
            .is_err()
    );
    assert_eq!(
        j.execution_fill_page(0, None, 128).await.unwrap().records,
        before.records
    );
}
