use super::*;

#[tokio::test]
async fn legacy_handoff_verifies_exact_payload_without_installing_capacity_inventory() {
    let target = store().await;
    let first = event(1);
    assert_eq!(
        target
            .handoff_accepted_events(&[first.clone()])
            .await
            .unwrap(),
        1
    );
    let mut retry = first.clone();
    retry.latency.persistence_ack_received_at_us = Some(now_epoch_micros());
    assert_eq!(target.handoff_accepted_events(&[retry]).await.unwrap(), 0);
    let mut changed = first;
    changed.payload = serde_json::json!({"changed":true});
    // The first row is inserted before the second conflicts. The complete
    // transaction, including that first write, must roll back.
    assert!(matches!(
        target.handoff_accepted_events(&[event(2), changed]).await,
        Err(OutboxStoreError::InvalidRecord(_))
    ));
    assert!(
        !target
            .capture_initialized
            .load(std::sync::atomic::Ordering::Acquire)
    );
    assert_eq!(target.pending_count().await.unwrap(), 1);
}

#[tokio::test]
async fn bounded_accepted_handoff_preserves_the_source_without_manufacturing_a_gap() {
    let target = store().await;
    let first = event(1);
    let second = event(2);
    let limit = bytes(&first);
    target
        .handoff_accepted_events_with_capacity(&[first.clone()], limit)
        .await
        .unwrap();
    assert!(matches!(
        target
            .handoff_accepted_events_with_capacity(&[second.clone()], limit)
            .await,
        Err(OutboxStoreError::CapturePaused(_))
    ));
    assert!(
        target.latest_capture_pause().await.unwrap().is_none(),
        "waiting for ownership transfer is not a new capture gap"
    );
    // An exact lost-ACK retry can transfer ownership even after lowering the
    // destination budget below its retained payloads; it consumes no bytes.
    assert_eq!(
        target
            .handoff_accepted_events_with_capacity(&[first.clone()], 1)
            .await
            .unwrap(),
        0
    );
    assert_eq!(target.capture_retained_bytes().await.unwrap(), limit);
    let mut changed = first.clone();
    changed.payload = serde_json::json!({"different":true});
    assert!(matches!(
        target
            .handoff_accepted_events_with_capacity(&[changed], limit)
            .await,
        Err(OutboxStoreError::InvalidRecord(_))
    ));
    let mut changed = second.clone();
    changed.payload = serde_json::json!({"different":true});
    assert!(matches!(
        target
            .handoff_accepted_events_with_capacity(&[second, changed], limit * 3)
            .await,
        Err(OutboxStoreError::InvalidRecord(_))
    ));
    assert_eq!(target.pending_count().await.unwrap(), 1);
    assert_eq!(
        target.capture_retained_bytes().await.unwrap(),
        inventory(&target).await
    );
}

#[tokio::test]
async fn bounded_handoff_concurrent_owners_cannot_overspend_the_shared_target() {
    let target = store().await;
    let a = event(1);
    let b = event(2);
    let limit = bytes(&a);
    let (left, right) = tokio::join!(
        target.handoff_accepted_events_with_capacity(std::slice::from_ref(&a), limit),
        target.handoff_accepted_events_with_capacity(std::slice::from_ref(&b), limit)
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    assert!(target.capture_retained_bytes().await.unwrap() <= limit);
    assert_eq!(target.pending_count().await.unwrap(), 1);
    assert!(target.latest_capture_pause().await.unwrap().is_none());
}

#[tokio::test]
async fn accepted_prefix_commits_the_largest_fit_and_retries_duplicates_at_the_cap() {
    let target = store().await;
    let events = (1..=3).map(event).collect::<Vec<_>>();
    let limit = bytes(&events[0]) + bytes(&events[1]);
    let (accepted, deferred) = target
        .handoff_accepted_prefix_with_capacity(&events, limit)
        .await
        .unwrap();
    assert_eq!(accepted, 2);
    assert!(deferred.is_some());
    assert_eq!(target.pending_count().await.unwrap(), 2);
    assert_eq!(target.capture_retained_bytes().await.unwrap(), limit);
    // Ownership can transfer after a lost ACK without another byte charge,
    // even if an administrator subsequently lowers the local budget.
    let (accepted, deferred) = target
        .handoff_accepted_prefix_with_capacity(&events, 1)
        .await
        .unwrap();
    assert_eq!(accepted, 2);
    assert!(deferred.is_some());
    assert!(target.latest_capture_pause().await.unwrap().is_none());
    assert_eq!(
        target.capture_retained_bytes().await.unwrap(),
        inventory(&target).await
    );
}

#[tokio::test]
async fn accepted_prefix_rolls_back_new_rows_when_a_duplicate_payload_conflicts() {
    let target = store().await;
    let first = event(1);
    target
        .handoff_accepted_events(std::slice::from_ref(&first))
        .await
        .unwrap();
    let mut changed = first.clone();
    changed.payload = serde_json::json!({"changed":true});
    assert!(matches!(
        target
            .handoff_accepted_prefix_with_capacity(&[event(2), changed], u64::from(u32::MAX))
            .await,
        Err(OutboxStoreError::InvalidRecord(_))
    ));
    assert_eq!(target.pending_count().await.unwrap(), 1);
    assert_eq!(
        target.capture_retained_bytes().await.unwrap(),
        bytes(&first)
    );
    assert!(target.latest_capture_pause().await.unwrap().is_none());
}

#[tokio::test]
async fn accepted_prefix_concurrent_sources_cannot_overspend_shared_capacity() {
    let target = store().await;
    let a = [event(1)];
    let b = [event(2)];
    let limit = bytes(&a[0]).max(bytes(&b[0]));
    let (left, right) = tokio::join!(
        target.handoff_accepted_prefix_with_capacity(&a, limit),
        target.handoff_accepted_prefix_with_capacity(&b, limit)
    );
    let left = left.unwrap();
    let right = right.unwrap();
    assert_eq!(left.0 + right.0, 1);
    assert_eq!(
        usize::from(left.1.is_some()) + usize::from(right.1.is_some()),
        1
    );
    assert_eq!(target.pending_count().await.unwrap(), 1);
    assert!(target.capture_retained_bytes().await.unwrap() <= limit);
    assert!(target.latest_capture_pause().await.unwrap().is_none());
}

fn event(revision: u64) -> CloudEvent {
    EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        format!("insight:{revision}"),
        revision,
        EventPriority::Critical,
        EventKind::InsightProjection,
        serde_json::json!({"revision":revision,"data":"x".repeat(1024)}),
    )
}

async fn store() -> OutboxStore {
    OutboxStore::open(
        std::env::temp_dir().join(format!("capture-test-{}.db", uuid::Uuid::new_v4())),
    )
    .await
    .unwrap()
}

fn bytes(event: &CloudEvent) -> u64 {
    FlatbufferEventCodec::encode_with_local_commit(event, now_epoch_micros())
        .unwrap()
        .len() as u64
}

async fn inventory(store: &OutboxStore) -> u64 {
    let _operation = store.operation_gate.lock().await;
    let mut rows = store
        .connection
        .query(
            "SELECT (SELECT COALESCE(SUM(length(wire_payload)),0) FROM outbox_events) +
                (SELECT COALESCE(SUM(length(wire_payload)),0) FROM dead_letters)",
            (),
        )
        .await
        .unwrap();
    rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap() as u64
}

async fn cursor(store: &OutboxStore) -> u64 {
    store
        .critical_sequence_cursor_for_runtime_scope("runtime", "strategy", "session")
        .await
        .unwrap()
}

#[tokio::test]
async fn atomic_cap_preserves_accepted_evidence_and_exact_duplicates() {
    let store = store().await;
    let first = event(1);
    let second = event(2);
    let limit = bytes(&first);
    assert_eq!(
        store
            .capture_events_with_capacity(&[first.clone()], limit)
            .await
            .unwrap(),
        1
    );
    assert_eq!(store.capture_retained_bytes().await.unwrap(), limit);
    assert_eq!(
        store
            .capture_events_with_capacity(&[first.clone(), first.clone()], limit)
            .await
            .unwrap(),
        0
    );
    assert!(
        matches!(store.capture_events_with_capacity(&[first.clone(), second.clone()], limit).await,
        Err(OutboxStoreError::CapturePaused(CaptureCapacity { requested_bytes, .. })) if requested_bytes == bytes(&second))
    );
    let pending = store
        .pending_batch(EventPriority::Critical, 100, 1_000_000)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].event.event_id, first.event_id);
    assert_eq!(
        store.capture_retained_bytes().await.unwrap(),
        inventory(&store).await
    );
    assert_eq!(store.health().await.unwrap().dead_letters, 0);
    let pause = store
        .capture_pause_for_scope("runtime", "strategy", "session")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pause.generation, 1);
    assert!(pause.resumed_at_us.is_none());
    assert!(
        !store
            .complete_capture_reconciliation("runtime", "strategy", "session", pause.generation)
            .await
            .unwrap()
    );
    // Committing is not storage credit: full retained bytes are credited only
    // after the payload is actually deleted by committed-row pruning.
    store
        .mark_remote_committed(&[first.event_id], None)
        .await
        .unwrap();
    assert_eq!(store.capture_retained_bytes().await.unwrap(), limit);
    store.prune_committed(0).await.unwrap();
    assert_eq!(store.capture_retained_bytes().await.unwrap(), 0);
    assert!(
        store
            .complete_capture_reconciliation("runtime", "strategy", "session", pause.generation)
            .await
            .unwrap()
    );
    assert!(
        store
            .capture_pause_for_scope("runtime", "strategy", "session")
            .await
            .unwrap()
            .unwrap()
            .resumed_at_us
            .is_some()
    );
}

#[tokio::test]
async fn publisher_refusal_never_allocates_an_identity_or_sequence_hole() {
    let store = store().await;
    let first = event(1);
    let second = event(2);
    let third = event(3);
    let accepted = store
        .reserve_publisher_events_with_capacity(&[first.clone()], 0, 1_000_000)
        .await
        .unwrap();
    let limit = store.capture_retained_bytes().await.unwrap();
    assert_eq!(accepted[0].critical_sequence, Some(1));
    assert!(matches!(
        store
            .reserve_publisher_events_with_capacity(&[second.clone(), third], 0, limit)
            .await,
        Err(OutboxStoreError::CapturePaused(_))
    ));
    assert_eq!(cursor(&store).await, 1);
    assert_eq!(store.capture_retained_bytes().await.unwrap(), limit);
    store
        .mark_remote_committed(&[first.event_id], Some(1))
        .await
        .unwrap();
    store.prune_committed(0).await.unwrap();
    // A pruned duplicate still has a durable identity reservation and needs no
    // capacity, even with a limit too small for a new payload.
    assert!(
        store
            .reserve_publisher_events_with_capacity(&[first], 0, 1)
            .await
            .unwrap()
            .is_empty()
    );
    let accepted = store
        .reserve_publisher_events_with_capacity(&[second], 0, limit)
        .await
        .unwrap();
    assert_eq!(accepted[0].critical_sequence, Some(2));
}

#[tokio::test]
async fn pause_survives_restart_and_stale_reconciliation_cannot_clear_it() {
    let store = store().await;
    let path = store.path().to_path_buf();
    let event = event(1);
    for _ in 0..3 {
        assert!(matches!(
            store
                .capture_events_with_capacity(&[event.clone()], 1)
                .await,
            Err(OutboxStoreError::CapturePaused(_))
        ));
    }
    let pause = store
        .capture_pause_for_scope("runtime", "strategy", "session")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pause.generation, 3);
    assert_eq!(pause.blocked_attempts, 3);
    assert_eq!(store.capture_retained_bytes().await.unwrap(), 0);
    drop(store);
    let store = OutboxStore::open(path).await.unwrap();
    assert_eq!(
        store
            .capture_pause_for_scope("runtime", "strategy", "session")
            .await
            .unwrap(),
        Some(pause.clone())
    );
    assert!(
        !store
            .complete_capture_reconciliation("runtime", "strategy", "session", 2)
            .await
            .unwrap()
    );
    assert!(
        store
            .complete_capture_reconciliation("runtime", "strategy", "session", 3)
            .await
            .unwrap()
    );
    assert!(matches!(
        store.capture_events_with_capacity(&[event], 1).await,
        Err(OutboxStoreError::CapturePaused(_))
    ));
    let next = store
        .capture_pause_for_scope("runtime", "strategy", "session")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.generation, 4);
    assert!(next.resumed_at_us.is_none());
}

#[tokio::test]
async fn aggregate_accounts_for_legacy_resize_coalesce_dlq_and_delete() {
    let store = store().await;
    let first = event(1);
    store.insert_events(&[first.clone()]).await.unwrap();
    assert_eq!(
        store.capture_retained_bytes().await.unwrap(),
        inventory(&store).await
    );
    let mut telemetry = event(2);
    telemetry.priority = EventPriority::Telemetry;
    telemetry.entity_id = "latest-account".into();
    store.insert_telemetry_latest(&telemetry).await.unwrap();
    telemetry.entity_revision += 1;
    telemetry.event_id = uuid::Uuid::new_v4();
    telemetry.payload["data"] = serde_json::json!("larger".repeat(1024));
    store.insert_telemetry_latest(&telemetry).await.unwrap();
    store
        .move_to_dead_letter(first.event_id, "test retained evidence")
        .await
        .unwrap();
    assert_eq!(
        store.capture_retained_bytes().await.unwrap(),
        inventory(&store).await
    );
    // Existing SQL update paths, including a resized pending payload, cannot
    // leave a stale aggregate. Rollback must also roll back aggregate changes.
    let _operation = store.operation_gate.lock().await;
    let before = inventory_without_gate(&store).await;
    store
        .connection
        .execute_batch(
            "BEGIN IMMEDIATE; UPDATE outbox_events SET wire_payload=x'01020304'; ROLLBACK;",
        )
        .await
        .unwrap();
    assert_eq!(
        store.capture_retained_bytes_in_transaction().await.unwrap(),
        before
    );
    store
        .connection
        .execute_batch("DELETE FROM outbox_events; DELETE FROM dead_letters;")
        .await
        .unwrap();
    assert_eq!(
        store.capture_retained_bytes_in_transaction().await.unwrap(),
        0
    );
}

async fn inventory_without_gate(store: &OutboxStore) -> u64 {
    let mut rows = store
        .connection
        .query(
            "SELECT retained_bytes FROM capture_capacity WHERE singleton=1",
            (),
        )
        .await
        .unwrap();
    rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap() as u64
}

#[tokio::test]
async fn concurrent_captures_share_the_same_transactional_capacity() {
    let store = store().await;
    let first = event(1);
    let second = event(2);
    let limit = bytes(&first).max(bytes(&second));
    store.capture_retained_bytes().await.unwrap();
    let first_batch = [first];
    let second_batch = [second];
    let (a, b) = tokio::join!(
        store.capture_events_with_capacity(&first_batch, limit),
        store.capture_events_with_capacity(&second_batch, limit)
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert_eq!(
        usize::from(matches!(a, Err(OutboxStoreError::CapturePaused(_))))
            + usize::from(matches!(b, Err(OutboxStoreError::CapturePaused(_)))),
        1
    );
    assert!(store.capture_retained_bytes().await.unwrap() <= limit);
    assert_eq!(
        store.capture_retained_bytes().await.unwrap(),
        inventory(&store).await
    );
}

#[tokio::test]
async fn invalid_and_oversized_batches_do_not_change_capacity_or_sequence() {
    let store = store().await;
    assert!(matches!(
        store.capture_events_with_capacity(&[event(1)], 0).await,
        Err(OutboxStoreError::InvalidRecord(_))
    ));
    let batch = (1..=257).map(event).collect::<Vec<_>>();
    assert!(matches!(
        store.capture_events_with_capacity(&batch, 1_000_000).await,
        Err(OutboxStoreError::InvalidRecord(_))
    ));
    assert_eq!(store.capture_retained_bytes().await.unwrap(), 0);
    assert!(
        store
            .capture_pause_for_scope("runtime", "strategy", "session")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn capped_telemetry_replaces_atomically_and_never_erases_the_last_accepted_value_on_refusal()
{
    let store = store().await;
    let mut first = event(1);
    first.priority = EventPriority::Telemetry;
    first.entity_id = "account".into();
    let limit = bytes(&first);
    store
        .capture_telemetry_latest_with_capacity(&first, limit)
        .await
        .unwrap();
    let mut second = first.clone();
    second.event_id = uuid::Uuid::new_v4();
    second.entity_revision = 2;
    store
        .capture_telemetry_latest_with_capacity(&second, limit)
        .await
        .unwrap();
    let mut too_large = second.clone();
    too_large.event_id = uuid::Uuid::new_v4();
    too_large.entity_revision = 3;
    too_large.payload["data"] = serde_json::json!("x".repeat(4096));
    assert!(matches!(
        store
            .capture_telemetry_latest_with_capacity(&too_large, limit)
            .await,
        Err(OutboxStoreError::CapturePaused(_))
    ));
    let pending = store
        .pending_batch(EventPriority::Telemetry, 10, 100_000)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].event.event_id, second.event_id);
    assert_eq!(store.capture_retained_bytes().await.unwrap(), limit);
    assert_eq!(
        store.capture_retained_bytes().await.unwrap(),
        inventory(&store).await
    );
}

#[tokio::test]
async fn pause_covers_the_earliest_unaccepted_envelope_and_latest_lookup_is_indexed() {
    let store = store().await;
    let mut first = event(1);
    let mut earlier = event(2);
    first.created_at = chrono::Utc::now() - chrono::Duration::minutes(5);
    earlier.created_at = first.created_at - chrono::Duration::minutes(5);
    assert!(matches!(
        store
            .capture_events_with_capacity(&[first, earlier.clone()], 1)
            .await,
        Err(OutboxStoreError::CapturePaused(_))
    ));
    let pause = store.latest_capture_pause().await.unwrap().unwrap();
    assert_eq!(pause.started_at_us, earlier.created_at.timestamp_micros());
    assert!(pause.last_blocked_at_us >= pause.started_at_us);
    let _operation = store.operation_gate.lock().await;
    let mut rows=store.connection.query("EXPLAIN QUERY PLAN SELECT runtime_id FROM capture_pauses INDEXED BY capture_pauses_open WHERE resumed_at_us IS NULL ORDER BY last_blocked_at_us DESC LIMIT 1",()).await.unwrap();
    let mut plan = String::new();
    while let Some(row) = rows.next().await.unwrap() {
        plan.push_str(&row.get::<String>(3).unwrap());
    }
    assert!(plan.contains("capture_pauses_open"), "{plan}");
    assert!(!plan.to_ascii_lowercase().contains("temp b-tree"), "{plan}");
}
