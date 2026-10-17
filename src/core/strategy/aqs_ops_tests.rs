use super::*;

#[test]
fn live_sync_background_retry_uses_bounded_exponential_backoff() {
    let initial = Duration::from_secs(LIVE_SYNC_BACKGROUND_RETRY_INITIAL_SECS);
    assert_eq!(next_live_sync_retry_delay(initial), Duration::from_secs(10));
    assert_eq!(
        next_live_sync_retry_delay(Duration::from_secs(40)),
        Duration::from_secs(LIVE_SYNC_BACKGROUND_RETRY_MAX_SECS)
    );
    assert_eq!(
        next_live_sync_retry_delay(Duration::from_secs(LIVE_SYNC_BACKGROUND_RETRY_MAX_SECS)),
        Duration::from_secs(LIVE_SYNC_BACKGROUND_RETRY_MAX_SECS)
    );
}

#[test]
fn live_sync_payloads_omit_nested_json_nulls_without_dropping_falsey_values() {
    let payload = without_json_nulls(serde_json::json!({
        "missing": null,
        "zero": 0,
        "disabled": false,
        "empty": "",
        "nested": {
            "missing": null,
            "value": 3.5,
        },
        "array": [
            { "missing": null, "value": 7 },
            null,
        ],
    }));

    assert_eq!(payload.get("missing"), None);
    assert_eq!(payload["zero"], 0);
    assert_eq!(payload["disabled"], false);
    assert_eq!(payload["empty"], "");
    assert_eq!(payload["nested"].get("missing"), None);
    assert_eq!(payload["nested"]["value"], 3.5);
    assert_eq!(payload["array"][0].get("missing"), None);
    assert!(payload["array"][1].is_null());
}

#[test]
fn entity_revisions_are_monotonic_and_restart_safe() {
    let mut revision = 0;
    assert_eq!(advance_entity_revision(&mut revision, 1_000), 1_000);
    assert_eq!(advance_entity_revision(&mut revision, 1_000), 1_001);

    // A new process starts without the in-memory counter, but the wall-clock floor remains
    // above the previous run and therefore produces a distinct deterministic event ID.
    let mut restarted_revision = 0;
    assert_eq!(
        advance_entity_revision(&mut restarted_revision, 2_000),
        2_000
    );
}

#[tokio::test]
async fn broker_lifecycle_revisions_do_not_reuse_equal_or_regressing_source_sequences() {
    use crate::core::live_sync::{InProcessEventBus, LocalEventPublisher};

    let publisher = LocalEventPublisher::spawn(
        Arc::new(InProcessEventBus::default()),
        None,
        Default::default(),
    );
    let auth = AqsAuth {
        access_method: "aqe_live".into(),
        session_id: "session".into(),
        session_secret: "test-only".into(),
        strategy_id: "strategy".into(),
        user_id: "users:test".into(),
        node_id: None,
        live_session_id: None,
        url: None,
    };
    let mut sync = LocalBatchSyncState::new(publisher, &auth);
    let entity = "broker-order:1956345309";

    assert_eq!(sync.next_revision_with_floor(entity, 1), 1);
    assert_eq!(sync.next_revision_with_floor(entity, 1), 2);
    assert_eq!(sync.next_revision_with_floor(entity, 0), 3);
    assert_eq!(sync.next_revision_with_floor(entity, 8), 8);
    assert_eq!(sync.next_revision_with_floor(entity, 2), 9);
}

#[tokio::test]
async fn changed_insight_projections_at_one_domain_time_drain_without_reservation_conflicts() {
    use crate::core::live_sync::{InProcessEventBus, LocalEventBus, OutboxStore};
    let directory =
        std::env::temp_dir().join(format!("aqe-projection-revision-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let outbox = OutboxStore::open(directory.join("publisher.db"))
        .await
        .unwrap();
    let bus = Arc::new(InProcessEventBus::default());
    let publisher =
        LocalEventPublisher::spawn(bus.clone(), Some(outbox.clone()), Default::default());
    let auth = AqsAuth {
        access_method: "aqe_live".into(),
        session_id: "session".into(),
        session_secret: "test-only".into(),
        strategy_id: "strategy".into(),
        user_id: "users:test".into(),
        node_id: None,
        live_session_id: None,
        url: None,
    };
    let mut sync = LocalBatchSyncState::new(publisher.clone(), &auth);
    let subject =
        EventSubject::for_priority(&sync.runtime_id, &sync.strategy_id, EventPriority::Critical);
    let mut subscription = bus.subscribe(subject.clone()).await.unwrap();
    // This reproduces the actual DLQ pair: same updated_at, changed limit price.
    let mut payload = serde_json::json!({
        "insight_id": "insight", "state": "Executed", "limit_price": 104.5645,
        "quantity": 100.0, "updated_at": "2026-09-08T10:45:09.630048Z",
    });
    let source_revision = 1_788_864_309_630_048;
    let first = sync
        .insight_projection_event("insight", source_revision, payload.clone())
        .unwrap();
    for _ in 0..1000 {
        assert!(
            sync.insight_projection_event("insight", source_revision, payload.clone())
                .is_none()
        );
    }
    let mut equivalent = payload.clone();
    equivalent["revision"] = serde_json::json!(123);
    equivalent["missing_optional"] = serde_json::Value::Null;
    assert!(
        sync.insight_projection_event("insight", source_revision, equivalent)
            .is_none()
    );
    sync.publish_critical(first.clone());
    // A transport retry retains the exact envelope and deduplicates durably.
    sync.publish_critical(first.clone());
    let mut previous = first;
    for (key, value) in [
        ("limit_price", serde_json::Value::Null),
        ("quantity", serde_json::json!(99.0)),
        ("state", serde_json::json!("Filled")),
        ("state", serde_json::json!("Closed")),
    ] {
        payload[key] = value;
        // A backwards domain clock must not regress the synchronization cursor either.
        let next = sync
            .insight_projection_event("insight", source_revision - 1, payload.clone())
            .unwrap();
        assert!(next.entity_revision > previous.entity_revision);
        assert_ne!(next.event_id, previous.event_id);
        assert_eq!(next.payload["revision"], next.entity_revision);
        assert_eq!(next.payload["updated_at"], payload["updated_at"]);
        sync.publish_critical(next.clone());
        previous = next;
    }
    publisher.shutdown(Duration::from_secs(5)).await;
    assert_eq!(publisher.health().pending_critical, 0);
    assert_eq!(publisher.health().publish_failures, 0);
    assert_eq!(publisher.health().deduplicated_critical, 1);
    assert_eq!(outbox.health().await.unwrap().dead_letters, 0);
    for sequence in 1..=5 {
        let delivery = tokio::time::timeout(Duration::from_secs(1), subscription.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delivery.event.critical_sequence, Some(sequence));
    }
    assert!(subscription.try_recv().is_err());
    assert_eq!(sync.insight_projection_fingerprints.len(), 1);
    drop(sync);
    drop(publisher);
    drop(outbox);
    std::fs::remove_dir_all(directory).unwrap();
}
