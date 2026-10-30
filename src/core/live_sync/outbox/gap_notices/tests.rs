use super::*;

fn notice(index: u64) -> CaptureGapNotice {
    let mut notice = CaptureGapNotice {
        version: 1,
        notice_id: uuid::Uuid::nil(),
        strategy_id: "strategy".into(),
        live_session_id: "session".into(),
        source_runtime_id: "runtime".into(),
        source_run_id: Some("run".into()),
        source_generation: index + 1,
        started_at_us: 1,
        last_blocked_at_us: 2,
        origin_runtime_id: Some("runtime".into()),
        origin_run_id: Some("run".into()),
        last_blocked_run_id: Some("run".into()),
        ownership_handoffs: 0,
    };
    notice.notice_id = notice.stable_id();
    notice
}

#[tokio::test]
async fn fixed_gap_mailbox_capacity_duplicates_receipts_and_reopen() {
    let directory = std::env::temp_dir().join(format!("aqe-gap-mailbox-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join("outbox.db");
    let store = OutboxStore::open(&path).await.unwrap();
    for index in 0..MAX_PENDING_CAPTURE_GAP_NOTICES {
        assert!(
            store
                .accept_capture_gap_notice(&notice(index as u64))
                .await
                .unwrap()
        );
    }
    assert!(!store.accept_capture_gap_notice(&notice(256)).await.unwrap());
    assert!(
        store.accept_capture_gap_notice(&notice(0)).await.unwrap(),
        "exact replay never needs another slot"
    );
    assert_eq!(
        store
            .retirement_uncommitted_for_scope("strategy", "session")
            .await
            .unwrap(),
        256
    );
    assert!(
        store
            .pending_capture_gap_notices("another-strategy")
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .pending_capture_gap_notices("strategy")
            .await
            .unwrap()
            .len(),
        16,
        "bounded indexed pages"
    );
    drop(store);
    let store = OutboxStore::open(&path).await.unwrap();
    let first = notice(0);
    let mut receipt = CaptureGapReceipt::for_notice(&first);
    receipt.live_session_id = "wrong".into();
    store
        .settle_capture_gap_notice(&first, Some(&receipt))
        .await
        .unwrap();
    assert_eq!(
        store
            .retirement_uncommitted_for_scope("strategy", "session")
            .await
            .unwrap(),
        256
    );
    store
        .settle_capture_gap_notice(&first, Some(&CaptureGapReceipt::for_notice(&first)))
        .await
        .unwrap();
    assert!(store.accept_capture_gap_notice(&notice(256)).await.unwrap());
    assert_eq!(store.pending_count().await.unwrap(), 0);
    store
        .discard_gap_notices_for_deleted_scope("strategy", "wrong")
        .await
        .unwrap();
    assert_eq!(
        store
            .retirement_uncommitted_for_scope("strategy", "session")
            .await
            .unwrap(),
        256
    );
    drop(store);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn later_refusal_fences_old_notice_and_receipt_does_not_resume_capture() {
    let directory =
        std::env::temp_dir().join(format!("aqe-gap-generation-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let store = OutboxStore::open(directory.join("outbox.db"))
        .await
        .unwrap();
    let mut event = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "entity",
        1,
        EventPriority::Critical,
        EventKind::InsightProjection,
        serde_json::json!({"data":"payload"}),
    );
    event.run_id = Some("run".into());
    assert!(matches!(
        store
            .capture_events_with_capacity(&[event.clone()], 1)
            .await,
        Err(OutboxStoreError::CapturePaused(_))
    ));
    let old = store
        .local_capture_gap_notices("strategy", "session")
        .await
        .unwrap()
        .remove(0);
    assert!(
        store
            .capture_events_with_capacity(&[event], 1)
            .await
            .is_err()
    );
    assert!(
        !store
            .settle_local_capture_gap_notice(&old, &CaptureGapReceipt::for_notice(&old))
            .await
            .unwrap()
    );
    let latest = store
        .local_capture_gap_notices("strategy", "session")
        .await
        .unwrap()
        .remove(0);
    assert!(latest.source_generation > old.source_generation);
    assert!(
        store
            .settle_local_capture_gap_notice(&latest, &CaptureGapReceipt::for_notice(&latest))
            .await
            .unwrap()
    );
    assert!(
        !store
            .has_untransferred_capture_pause_for_scope("strategy", "session")
            .await
            .unwrap()
    );
    assert!(
        store
            .has_unresolved_capture_pause_for_scope("strategy", "session")
            .await
            .unwrap(),
        "a quality notice is not reconciliation or capture resume"
    );
    assert_eq!(store.pending_count().await.unwrap(), 0);
    drop(store);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn concurrent_mailbox_connections_cannot_overspend_the_slot_cap() {
    let directory = std::env::temp_dir().join(format!("aqe-gap-race-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join("outbox.db");
    let first = OutboxStore::open(&path).await.unwrap();
    for index in 0..255 {
        assert!(
            first
                .accept_capture_gap_notice(&notice(index))
                .await
                .unwrap()
        );
    }
    let second = OutboxStore::open(&path).await.unwrap();
    let a = notice(255);
    let b = notice(256);
    let (a, b) = tokio::join!(
        first.accept_capture_gap_notice(&a),
        second.accept_capture_gap_notice(&b)
    );
    assert!([a, b].into_iter().filter(|r| matches!(r, Ok(true))).count() <= 1);
    assert!(
        first
            .retirement_uncommitted_for_scope("strategy", "session")
            .await
            .unwrap()
            <= 256
    );
    drop(first);
    drop(second);
    std::fs::remove_dir_all(directory).unwrap();
}
