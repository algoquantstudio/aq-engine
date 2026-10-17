use super::*;
use aqe_live_protocol::ExecutionFillInvalidationReason;

pub(super) fn invalidate(f: &ExecutionFill, at: i64) -> ExecutionFillInvalidation {
    ExecutionFillInvalidation {
        provider: f.provider.clone(),
        fill_id: f.fill_id.clone(),
        order_id: f.order_id.clone(),
        position_id: f.position_id.clone(),
        symbol: f.symbol.clone(),
        side: f.side,
        effect: f.effect,
        occurred_at_us: f.occurred_at_us,
        as_of_us: at,
        reason: ExecutionFillInvalidationReason::BrokerCancelled,
    }
}

#[tokio::test]
async fn invalidations_are_versioned_auditable_deduplicated_and_fence_stale_replays() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let original = input("cancelled", ExecutionFillEffect::Entry, 2., 10.);
    save(&mut journal, vec![original.clone()]).await;
    let invalidation = invalidate(&original.fill, 300);
    journal
        .record_execution_fills(
            &ExecutionFillBatch::invalidations(vec![invalidation.clone()]).unwrap(),
        )
        .await
        .unwrap();
    journal
        .record_execution_fills(
            &ExecutionFillBatch::invalidations(vec![invalidation.clone()]).unwrap(),
        )
        .await
        .unwrap();
    let page = journal.execution_fill_page(0, None, 32).await.unwrap();
    assert_eq!((page.version, page.records.len()), (2, 2));
    assert_eq!(page.records[1].observation, original);
    assert_eq!(page.records[1].invalidation, Some(invalidation.clone()));
    let mut transfer = aqe_live_protocol::ExecutionFillTransfer::from_page(&page).unwrap();
    transfer.schema_version = 1;
    assert!(transfer.page().is_err());
    let mut downgraded = page.clone();
    downgraded.version = 1;
    assert!(aqe_live_protocol::ExecutionFillTransfer::from_page(&downgraded).is_err());
    let mut bad = page.clone();
    bad.records[1].invalidation.as_mut().unwrap().order_id = "other".into();
    assert!(aqe_live_protocol::ExecutionFillTransfer::from_page(&bad).is_err());
    let mut rows = journal
        .connection
        .query(
            "SELECT observation_json FROM broker_execution_fills WHERE sequence=2",
            (),
        )
        .await
        .unwrap();
    let stored = rows
        .next()
        .await
        .unwrap()
        .unwrap()
        .get::<String>(0)
        .unwrap();
    assert!(
        serde_json::from_str::<ExecutionFillInput>(&stored).is_err(),
        "older runtime could count tombstone audit evidence"
    );
    drop(rows);
    drop(journal);
    let mut journal = fixture.open().await;
    let mut delayed = original.clone();
    delayed.fill.as_of_us = 250;
    save(&mut journal, vec![delayed]).await;
    let mut refreshed = invalidation;
    refreshed.as_of_us = 400;
    journal
        .record_execution_fills(&ExecutionFillBatch::invalidations(vec![refreshed]).unwrap())
        .await
        .unwrap();
    let mut delayed = original.clone();
    delayed.fill.as_of_us = 350;
    save(&mut journal, vec![delayed]).await;
    assert_eq!(
        journal
            .execution_fill_page(0, None, 32)
            .await
            .unwrap()
            .records
            .len(),
        2
    );
    let mut restored = original;
    restored.fill.as_of_us = 500;
    save(&mut journal, vec![restored]).await;
    let page = journal.execution_fill_page(2, None, 32).await.unwrap();
    assert_eq!(page.records.len(), 1);
    assert!(page.records[0].invalidation.is_none());
    assert_eq!(page.records[0].execution_epoch, 1);
    assert!(journal.recovery_page(0, 32).await.unwrap().is_empty());
}

#[tokio::test]
async fn unseen_invalidations_survive_restart_and_conflicts_roll_back_atomically() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    let original = input("unseen", ExecutionFillEffect::Entry, 1., 0.);
    let invalidation = invalidate(&original.fill, 300);
    journal
        .record_execution_fills(
            &ExecutionFillBatch::invalidations(vec![invalidation.clone()]).unwrap(),
        )
        .await
        .unwrap();
    assert!(
        journal
            .prepare_execution_fill_export()
            .await
            .unwrap()
            .is_none()
    );
    drop(journal);
    let mut journal = fixture.open().await;
    save(&mut journal, vec![original.clone()]).await;
    assert!(
        journal
            .prepare_execution_fill_export()
            .await
            .unwrap()
            .is_none()
    );
    let other = input("other", ExecutionFillEffect::Entry, 1., 1.);
    save(&mut journal, vec![other.clone()]).await;
    let mut bad = invalidation;
    bad.order_id = "wrong".into();
    bad.as_of_us += 1;
    assert!(
        journal
            .record_execution_fills(
                &ExecutionFillBatch::invalidations(vec![invalidate(&other.fill, 400), bad])
                    .unwrap()
            )
            .await
            .is_err()
    );
    assert_eq!(
        journal
            .execution_fill_page(0, None, 32)
            .await
            .unwrap()
            .records
            .len(),
        1
    );
    let mut newer = original;
    newer.fill.as_of_us = 301;
    save(&mut journal, vec![newer]).await;
    assert_eq!(
        journal
            .execution_fill_page(0, None, 32)
            .await
            .unwrap()
            .records
            .len(),
        2
    );
}
