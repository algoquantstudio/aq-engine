use super::*;
use crate::core::live_sync::{EventEnvelope, EventKind};
use uuid::Uuid;

#[tokio::test]
async fn runtime_batches_use_ordered_index_without_payload_sorting() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    let mut rows = store
        .connection
        .query(
            &format!(
                "EXPLAIN QUERY PLAN {}",
                pending_batch_query(true, true, true)
            ),
            params![
                "critical",
                now_epoch_micros(),
                32_i64,
                "runtime",
                "strategy",
                "session"
            ],
        )
        .await
        .unwrap();
    let mut plan = String::new();
    while let Some(row) = rows.next().await.unwrap() {
        plan.push_str(&row.get::<String>(3).unwrap());
        plan.push('\n');
    }
    assert!(plan.contains("idx_outbox_runtime_ordered"), "{plan}");
    assert!(!plan.contains("TEMP B-TREE"), "{plan}");
    drop(rows);
    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn critical_backpressure_is_bounded_exact_and_scope_isolated() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    let mut events: Vec<_> = (1..=102)
        .map(|n| event(EventPriority::Critical, n))
        .collect();
    events[101].runtime_id = "unrelated".into();
    store.insert_events(&events).await.unwrap();
    store
        .connection
        .execute(
            "UPDATE outbox_events SET state = 'in_flight' WHERE event_id = ?1",
            params![events[100].event_id.to_string()],
        )
        .await
        .unwrap();
    for (limit, expected) in [(0, true), (100, true), (101, false), (u64::MAX, false)] {
        assert_eq!(
            store
                .critical_backlog_exceeds_for_runtime_scope("runtime", "strategy", "session", limit)
                .await
                .unwrap(),
            expected
        );
    }
    assert!(
        !store
            .critical_backlog_exceeds_for_runtime_scope("runtime", "strategy", "other-session", 0)
            .await
            .unwrap()
    );
    store
        .mark_remote_committed(&[events[0].event_id], None)
        .await
        .unwrap();
    assert!(
        !store
            .critical_backlog_exceeds_for_runtime_scope("runtime", "strategy", "session", 100)
            .await
            .unwrap()
    );
    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn supervisor_and_backpressure_queries_use_scope_indexes() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    for (query, values, index) in [
        (
            PENDING_SCOPE_COUNT_QUERY,
            vec!["strategy".into(), "session".into()],
            "idx_outbox_scope_state",
        ),
        (
            PENDING_RUNTIME_IDS_QUERY,
            vec!["strategy".into(), "session".into()],
            "idx_outbox_scope_state",
        ),
        (
            PENDING_SCOPE_EXISTS_QUERY,
            vec!["strategy".into(), "session".into()],
            "idx_outbox_scope_state",
        ),
        (
            PENDING_RUNTIME_ID_AFTER_QUERY,
            vec!["strategy".into(), "session".into(), "runtime-one".into()],
            "idx_outbox_scope_state",
        ),
        (
            CRITICAL_BACKLOG_EXCEEDS_QUERY,
            vec![
                "runtime".into(),
                "strategy".into(),
                "session".into(),
                turso::Value::Integer(100),
            ],
            "idx_outbox_runtime_pending",
        ),
    ] {
        let mut rows = store
            .connection
            .query(&format!("EXPLAIN QUERY PLAN {query}"), values)
            .await
            .unwrap();
        let mut plan = String::new();
        while let Some(row) = rows.next().await.unwrap() {
            plan.push_str(&row.get::<String>(3).unwrap());
            plan.push('\n');
        }
        assert!(plan.contains(index), "{plan}");
        // Turso labels range seeks "USING INDEX" even when the index
        // contains every referenced column; verify the scoped seek itself.
        assert!(plan.contains("SEARCH outbox_events"), "{plan}");
        assert!(!plan.contains("SCAN outbox_events"), "{plan}");
        assert!(!plan.contains("TEMP B-TREE"), "{plan}");
    }
    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn supervisor_pending_probes_preserve_scope_state_and_generation_order() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    assert!(!store.has_pending_for_scope("strategy", "session").await.unwrap());
    let mut events = Vec::new();
    for (index, runtime) in ["z", "a", "Ω", "middle", "a", "", "legacy"].iter().enumerate() {
        events.push(event_for_runtime(EventPriority::Audit, index as u64 + 1, runtime));
    }
    let mut unrelated = event_for_runtime(EventPriority::Critical, 100, "first");
    unrelated.strategy_id = "other".into();
    events.push(unrelated);
    events.push(event_for_session(EventPriority::Audit, 101, "other"));
    store.insert_events(&events).await.unwrap();
    store.connection.execute(
        "UPDATE outbox_events SET runtime_id = NULL WHERE runtime_id = 'legacy'", (),
    ).await.unwrap();
    assert!(store.has_pending_for_scope("strategy", "session").await.unwrap());
    assert!(!store.has_pending_for_scope("missing", "session").await.unwrap());
    assert!(!store.has_pending_for_scope("strategy", "missing").await.unwrap());
    let full = store.pending_runtime_ids_for_scope("strategy", "session").await.unwrap();
    assert_eq!(full, ["a", "middle", "z", "Ω"]);
    for limit in [0, 1, 2, 3, 4, 20] {
        assert_eq!(
            store.pending_runtime_ids_for_scope_limited("strategy", "session", limit).await.unwrap(),
            full.iter().take(limit).cloned().collect::<Vec<_>>()
        );
    }
    assert!(store.pending_runtime_ids_for_scope_limited("strategy", "missing", 3).await.unwrap().is_empty());

    // Null/empty legacy generations still block cleanup even though no worker ID can be derived.
    store.connection.execute(
        "UPDATE outbox_events SET state = 'committed' WHERE runtime_id IS NOT NULL AND runtime_id != ''", (),
    ).await.unwrap();
    assert!(store.has_pending_for_scope("strategy", "session").await.unwrap());
    assert!(store.pending_runtime_ids_for_scope_limited("strategy", "session", 3).await.unwrap().is_empty());
    // Non-pending rows use the independent, stronger retirement proof.
    store.connection.execute("UPDATE outbox_events SET state = 'dead_letter' WHERE state = 'pending'", ()).await.unwrap();
    assert!(!store.has_pending_for_scope("strategy", "session").await.unwrap());
    assert_eq!(store.retirement_uncommitted_for_scope("strategy", "session").await.unwrap(), 2);
    store.connection.execute("DROP TABLE outbox_events", ()).await.unwrap();
    assert!(store.has_pending_for_scope("strategy", "session").await.is_err());
    assert!(store.pending_runtime_ids_for_scope_limited("strategy", "session", 3).await.is_err());
    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
#[ignore = "isolated paired supervisor backlog benchmark"]
async fn benchmark_supervisor_pending_probes() {
    async fn sample(store: &OutboxStore, bounded: bool, expected_ids: &[String]) -> f64 {
        let started = Instant::now();
        if bounded {
            assert!(store.has_pending_for_scope("strategy", "session").await.unwrap());
            assert_eq!(store.pending_runtime_ids_for_scope_limited("strategy", "session", 3).await.unwrap(),
                expected_ids.iter().take(3).cloned().collect::<Vec<_>>());
        } else {
            assert_eq!(store.pending_count_for_scope("strategy", "session").await.unwrap(), 30000);
            assert_eq!(store.pending_runtime_ids_for_scope("strategy", "session").await.unwrap(), expected_ids);
        }
        started.elapsed().as_secs_f64() * 1000.0
    }
    for generations in [1, 60] {
        let path = temp_database();
        let store = OutboxStore::open(&path).await.unwrap();
        let events: Vec<_> = (0..30000).map(|index| {
            event_for_runtime(EventPriority::Audit, index + 1, &format!("runtime-{:03}", index % generations))
        }).collect();
        store.insert_events(&events).await.unwrap();
        drop(events);
        let expected_ids = store.pending_runtime_ids_for_scope("strategy", "session").await.unwrap();
        let mut before = Vec::new();
        let mut after = Vec::new();
        for pair in 0..34 {
            for bounded in if pair % 2 == 0 { [false, true] } else { [true, false] } {
                let elapsed = sample(&store, bounded, &expected_ids).await;
                if pair >= 3 {
                    if bounded { after.push(elapsed); } else { before.push(elapsed); }
                }
            }
        }
        let stats = |values: &[f64]| {
            let mut sorted = values.to_vec();
            sorted.sort_by(f64::total_cmp);
            serde_json::json!({"median_ms": sorted[15], "p95_ms": sorted[29], "samples_ms": values})
        };
        eprintln!("supervisor-probes {}", serde_json::json!({
            "backlog": 30000, "generations": generations, "pairs":31,
            "before":stats(&before), "after":stats(&after),
        }));
        drop(store);
        let _ = std::fs::remove_file(path);
    }
}

#[tokio::test]
#[ignore = "isolated large-backlog threshold benchmark"]
async fn benchmark_bounded_critical_backpressure() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    let events: Vec<_> = (1..=10000)
        .map(|n| event(EventPriority::Critical, n))
        .collect();
    store.insert_events(&events).await.unwrap();
    drop(events);
    let start = Instant::now();
    for _ in 0..100 {
        assert_eq!(
            store
                .pending_critical_for_runtime_scope("runtime", "strategy", "session")
                .await
                .unwrap(),
            10000
        );
    }
    let counted = start.elapsed();
    let start = Instant::now();
    for _ in 0..100 {
        assert!(
            store
                .critical_backlog_exceeds_for_runtime_scope("runtime", "strategy", "session", 100)
                .await
                .unwrap()
        );
    }
    eprintln!(
        "runtime-backpressure backlog=10000 threshold=100 iterations=100 count_ms={} bounded_ms={}",
        counted.as_millis(),
        start.elapsed().as_millis()
    );
    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn runtime_deadlines_use_scoped_indexes_without_sorting() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    for priority in [
        EventPriority::Critical,
        EventPriority::Telemetry,
        EventPriority::Audit,
        EventPriority::Reconciliation,
    ] {
        let mut rows = store
            .connection
            .query(
                &format!("EXPLAIN QUERY PLAN {}", pending_deadline_query(priority)),
                params![priority_label(priority), "runtime", "strategy", "session"],
            )
            .await
            .unwrap();
        let mut plan = String::new();
        while let Some(row) = rows.next().await.unwrap() {
            plan.push_str(&row.get::<String>(3).unwrap());
            plan.push('\n');
        }
        let index = if priority == EventPriority::Critical {
            "idx_outbox_runtime_ordered"
        } else {
            "idx_outbox_runtime_pending"
        };
        assert!(plan.contains(index), "{plan}");
        assert!(!plan.contains("TEMP B-TREE"), "{plan}");
        assert!(!plan.contains("SCAN outbox_events"), "{plan}");
    }
    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn shared_outbox_wakes_all_registered_scope_workers() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    let notifier = store.change_notifier();
    let a = notifier.notified();
    let b = notifier.notified();
    tokio::pin!(a, b);
    a.as_mut().enable();
    b.as_mut().enable();
    store
        .insert_events(&[event(EventPriority::Critical, 1)])
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(a, b);
    })
    .await
    .unwrap();
    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn background_notifications_are_priority_scoped_and_telemetry_wakes_after_commit() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    let audit = store.priority_change_notifier(EventPriority::Audit);
    let telemetry = store.priority_change_notifier(EventPriority::Telemetry);
    let a = audit.notified();
    let t = telemetry.notified();
    tokio::pin!(a, t);
    a.as_mut().enable();
    t.as_mut().enable();
    store
        .insert_telemetry_latest(&event(EventPriority::Telemetry, 1))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_millis(100), &mut t)
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut a)
            .await
            .is_err()
    );
    store
        .insert_events(&[event(EventPriority::Audit, 2)])
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_millis(100), a)
        .await
        .unwrap();
    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn background_deadline_uses_earliest_retry_not_the_oldest_created_row() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    for priority in [
        EventPriority::Telemetry,
        EventPriority::Audit,
        EventPriority::Reconciliation,
    ] {
        let first = event(priority, 1);
        let mut second = event(priority, 2);
        second.event_id = Uuid::new_v4();
        store.insert_events(&[first.clone(), second]).await.unwrap();
        store
            .schedule_retry(&[first.event_id], 60, "older delayed")
            .await
            .unwrap();
        let deadline = store
            .next_pending_deadline_for_runtime_scope(priority, "runtime", "strategy", "session")
            .await
            .unwrap()
            .unwrap();
        assert!(
            deadline <= now_epoch_micros(),
            "ready background work must not wait for older backoff"
        );
    }
    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn cloned_agents_share_maintenance_and_diagnostic_cache() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    let other = store.clone();
    let now = Instant::now();
    assert!(store.claim_maintenance(now, Duration::from_secs(60)));
    assert!(!other.claim_maintenance(now + Duration::from_secs(59), Duration::from_secs(60)));
    assert!(other.claim_maintenance(now + Duration::from_secs(60), Duration::from_secs(60)));
    assert_eq!(
        store
            .cached_health(Duration::from_secs(60))
            .await
            .unwrap()
            .pending_critical,
        0
    );
    store
        .insert_events(&[event(EventPriority::Critical, 1)])
        .await
        .unwrap();
    assert_eq!(
        other
            .cached_health(Duration::from_secs(60))
            .await
            .unwrap()
            .pending_critical,
        0
    );
    // Exact counters must never inherit the diagnostic cache's staleness.
    assert_eq!(store.pending_count().await.unwrap(), 1);
    assert_eq!(store.health().await.unwrap().pending_critical, 1);
    assert_eq!(
        store
            .cached_health(Duration::ZERO)
            .await
            .unwrap()
            .pending_critical,
        1
    );
    drop(other);
    drop(store);
    let _ = std::fs::remove_file(path);
}

fn temp_database() -> PathBuf {
    std::env::temp_dir().join(format!("aqe-outbox-{}.db", Uuid::new_v4()))
}

fn event(priority: EventPriority, revision: u64) -> CloudEvent {
    event_for_session(priority, revision, "session")
}

fn event_for_session(priority: EventPriority, revision: u64, live_session_id: &str) -> CloudEvent {
    EventEnvelope::new(
        "runtime",
        "strategy",
        live_session_id,
        "entity",
        revision,
        priority,
        EventKind::InsightProjection,
        serde_json::json!({"revision": revision}),
    )
}

fn event_for_runtime(priority: EventPriority, revision: u64, runtime_id: &str) -> CloudEvent {
    EventEnvelope::new(
        runtime_id,
        "strategy",
        "session",
        "entity",
        revision,
        priority,
        EventKind::InsightProjection,
        serde_json::json!({"revision": revision}),
    )
}

#[tokio::test]
async fn critical_insert_is_idempotent_and_ordered() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    let mut first = event(EventPriority::Critical, 1);
    first.critical_sequence = Some(1);
    let mut second = event(EventPriority::Critical, 2);
    second.critical_sequence = Some(2);
    assert_eq!(
        store
            .insert_events(&[second.clone(), first.clone()])
            .await
            .unwrap(),
        2
    );
    assert_eq!(store.insert_events(&[first.clone()]).await.unwrap(), 0);
    let batch = store
        .pending_batch(EventPriority::Critical, 32, 64 * 1024)
        .await
        .unwrap();
    assert_eq!(batch[0].event.critical_sequence, Some(1));
    assert_eq!(batch[1].event.critical_sequence, Some(2));
    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn retryable_non_reconciliation_conflict_is_requeued_on_open() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    let queued = event(EventPriority::Critical, 1);
    store.insert_events(&[queued.clone()]).await.unwrap();
    store
        .move_to_dead_letter(
            queued.event_id,
            "Transaction conflict: Resource busy. This transaction can be retried",
        )
        .await
        .unwrap();
    assert_eq!(store.health().await.unwrap().dead_letters, 1);
    drop(store);

    let reopened = OutboxStore::open(&path).await.unwrap();
    assert_eq!(reopened.health().await.unwrap().dead_letters, 0);
    let pending = reopened
        .pending_batch(EventPriority::Critical, 10, 64 * 1024)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].event.event_id, queued.event_id);
    assert_eq!(pending[0].attempt_count, 0);
    drop(reopened);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn pending_contention_backoff_is_reset_on_open() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    let mut queued = event(EventPriority::Critical, 1);
    queued.critical_sequence = Some(1);
    store.insert_events(&[queued.clone()]).await.unwrap();
    store
        .schedule_retry(
            &[queued.event_id],
            300,
            "Transaction conflict: Resource busy. This transaction can be retried",
        )
        .await
        .unwrap();
    assert!(
        store
            .pending_batch(EventPriority::Critical, 10, 64 * 1024)
            .await
            .unwrap()
            .is_empty()
    );
    drop(store);

    let reopened = OutboxStore::open(&path).await.unwrap();
    let pending = reopened
        .pending_batch(EventPriority::Critical, 10, 64 * 1024)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].event.event_id, queued.event_id);
    assert_eq!(pending[0].attempt_count, 0);
    drop(reopened);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn maintenance_bounds_raw_latency_samples() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    let now = now_epoch_micros();
    store
        .connection
        .execute(
            "INSERT INTO latency_samples(event_id, stage, duration_us, observed_at_us)
             VALUES ('expired', 'local_ack', 10, ?1),
                    ('recent', 'local_ack', 20, ?2)",
            params![now.saturating_sub(RAW_LATENCY_SAMPLE_RETENTION_US + 1), now,],
        )
        .await
        .unwrap();

    store.refresh_latency_rollups().await.unwrap();

    let mut rows = store
        .connection
        .query("SELECT event_id FROM latency_samples ORDER BY event_id", ())
        .await
        .unwrap();
    let mut retained = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        retained.push(row.get::<String>(0).unwrap());
    }
    assert_eq!(retained, vec!["recent"]);
    drop(rows);
    store.checkpoint_wal(true).await.unwrap();
    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn terminal_log_events_are_never_stored() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    let log = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "runtime-log:stdout:1",
        1,
        EventPriority::Telemetry,
        EventKind::Log,
        serde_json::json!({"stream": "stdout", "line": "local only"}),
    );

    assert_eq!(store.insert_events(&[log.clone()]).await.unwrap(), 0);
    store.insert_telemetry_latest(&log).await.unwrap();
    assert!(
        store
            .pending_batch(EventPriority::Telemetry, 10, 64 * 1024)
            .await
            .unwrap()
            .is_empty()
    );

    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn delayed_earlier_critical_sequence_blocks_later_due_events() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    let mut first = event(EventPriority::Critical, 1);
    first.critical_sequence = Some(1);
    let mut second = event(EventPriority::Critical, 2);
    second.critical_sequence = Some(2);
    store.insert_events(&[first.clone(), second]).await.unwrap();
    store
        .schedule_retry(&[first.event_id], 60, "retry sequence one")
        .await
        .unwrap();

    let batch = store
        .pending_batch_for_scope(
            EventPriority::Critical,
            "strategy",
            "session",
            32,
            64 * 1024,
        )
        .await
        .unwrap();
    assert!(batch.is_empty());
    let deadline = store
        .next_pending_deadline_for_runtime_scope(
            EventPriority::Critical,
            "runtime",
            "strategy",
            "session",
        )
        .await
        .unwrap()
        .unwrap();
    assert!(deadline > now_epoch_micros().saturating_add(50_000_000));
    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn duplicate_event_id_with_different_sequence_is_rejected() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    let mut first = event(EventPriority::Critical, 1);
    first.critical_sequence = Some(1);
    assert_eq!(store.insert_events(&[first.clone()]).await.unwrap(), 1);
    assert_eq!(store.insert_events(&[first.clone()]).await.unwrap(), 0);

    let mut conflicting = first;
    conflicting.critical_sequence = Some(2);
    assert!(matches!(
        store.insert_events(&[conflicting]).await,
        Err(OutboxStoreError::InvalidRecord(_))
    ));
    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn remote_critical_cursors_are_isolated_by_runtime() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    let mut first_runtime = event_for_runtime(EventPriority::Critical, 1, "runtime-one");
    first_runtime.critical_sequence = Some(1);
    let mut second_runtime = event_for_runtime(EventPriority::Critical, 1, "runtime-two");
    second_runtime.critical_sequence = Some(1);
    store
        .insert_events(&[first_runtime.clone(), second_runtime.clone()])
        .await
        .unwrap();
    store
        .mark_remote_committed(&[first_runtime.event_id], Some(1))
        .await
        .unwrap();

    assert_eq!(
        store
            .remote_critical_cursor_for_runtime_scope("runtime-one", "strategy", "session")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .remote_critical_cursor_for_runtime_scope("runtime-two", "strategy", "session")
            .await
            .unwrap(),
        0
    );
    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn runtime_scoped_batches_never_mix_critical_generations() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    let mut first_runtime = event_for_runtime(EventPriority::Critical, 1, "runtime-one");
    first_runtime.critical_sequence = Some(1);
    let mut second_runtime = event_for_runtime(EventPriority::Critical, 1, "runtime-two");
    second_runtime.critical_sequence = Some(1);
    store
        .insert_events(&[first_runtime.clone(), second_runtime.clone()])
        .await
        .unwrap();

    let first = store
        .pending_batch_for_runtime_scope(
            EventPriority::Critical,
            "runtime-one",
            "strategy",
            "session",
            32,
            64 * 1024,
        )
        .await
        .unwrap();
    let second = store
        .pending_batch_for_runtime_scope(
            EventPriority::Critical,
            "runtime-two",
            "strategy",
            "session",
            32,
            64 * 1024,
        )
        .await
        .unwrap();

    assert_eq!(first.len(), 1);
    assert_eq!(first[0].event.runtime_id, "runtime-one");
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].event.runtime_id, "runtime-two");
    assert_eq!(
        store
            .pending_runtime_ids_for_scope("strategy", "session")
            .await
            .unwrap(),
        vec!["runtime-one".to_string(), "runtime-two".to_string()]
    );
    drop(store);
    let _ = std::fs::remove_file(path);
}



#[tokio::test]
async fn exited_runtime_gets_one_terminal_lifecycle_event() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    let mut running = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "strategy-lifecycle",
        1,
        EventPriority::Critical,
        EventKind::StrategyLifecycle,
        serde_json::json!({"state": "running", "status": "Running"}),
    );
    running.critical_sequence = Some(1);
    running.run_id = Some("original-run".into());
    store.insert_events(&[running]).await.unwrap();

    assert!(
        store
            .ensure_runtime_stopped_lifecycle("runtime", "strategy", "session")
            .await
            .unwrap()
    );
    assert!(
        !store
            .ensure_runtime_stopped_lifecycle("runtime", "strategy", "session")
            .await
            .unwrap()
    );
    let batch = store
        .pending_batch_for_runtime_scope(
            EventPriority::Critical,
            "runtime",
            "strategy",
            "session",
            32,
            64 * 1024,
        )
        .await
        .unwrap();
    assert_eq!(batch.len(), 2);
    assert_eq!(batch[1].event.critical_sequence, Some(2));
    assert_eq!(batch[1].event.run_id.as_deref(), Some("original-run"));
    assert_eq!(
        batch[1]
            .event
            .payload
            .get("state")
            .and_then(serde_json::Value::as_str),
        Some("stopped")
    );
    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn retirement_count_includes_dead_letters_and_excludes_other_sessions() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    let local = event(EventPriority::Audit, 1);
    let other = event_for_session(EventPriority::Audit, 2, "other");
    store.insert_events(&[local.clone(), other]).await.unwrap();
    assert_eq!(
        store
            .retirement_uncommitted_for_scope("strategy", "session")
            .await
            .unwrap(),
        1
    );
    store
        .move_to_dead_letter(local.event_id, "invalid event")
        .await
        .unwrap();
    assert_eq!(
        store
            .pending_count_for_scope("strategy", "session")
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .retirement_uncommitted_for_scope("strategy", "session")
            .await
            .unwrap(),
        1
    );
    store
        .mark_remote_committed(&[local.event_id], None)
        .await
        .unwrap();
    assert_eq!(
        store
            .retirement_uncommitted_for_scope("strategy", "session")
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .retirement_uncommitted_for_scope("strategy", "other")
            .await
            .unwrap(),
        1
    );
    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn publisher_sequence_cursor_survives_store_reopen() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    store
        .record_critical_sequence_cursor("strategy", "session", 41)
        .await
        .unwrap();
    drop(store);

    let reopened = OutboxStore::open(&path).await.unwrap();
    assert_eq!(
        reopened
            .critical_sequence_cursor("strategy", "session")
            .await
            .unwrap(),
        41
    );
    drop(reopened);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn publisher_sequence_cursors_are_isolated_by_runtime_generation() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    store
        .record_critical_sequence_cursor_for_runtime_scope("runtime-one", "strategy", "session", 41)
        .await
        .unwrap();
    store
        .record_critical_sequence_cursor_for_runtime_scope("runtime-two", "strategy", "session", 7)
        .await
        .unwrap();

    assert_eq!(
        store
            .critical_sequence_cursor_for_runtime_scope("runtime-one", "strategy", "session",)
            .await
            .unwrap(),
        41
    );
    assert_eq!(
        store
            .critical_sequence_cursor_for_runtime_scope("runtime-two", "strategy", "session",)
            .await
            .unwrap(),
        7
    );
    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn uncommitted_critical_event_survives_store_reopen() {
    let path = temp_database();
    let mut critical = event(EventPriority::Critical, 1);
    critical.critical_sequence = Some(1);

    let store = OutboxStore::open(&path).await.unwrap();
    store
        .insert_events(std::slice::from_ref(&critical))
        .await
        .unwrap();
    drop(store);

    let reopened = OutboxStore::open(&path).await.unwrap();
    let batch = reopened
        .pending_batch_for_scope(
            EventPriority::Critical,
            "strategy",
            "session",
            32,
            64 * 1024,
        )
        .await
        .unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].event.event_id, critical.event_id);
    assert_eq!(batch[0].event.critical_sequence, Some(1));
    assert_eq!(reopened.health().await.unwrap().pending_critical, 1);
    drop(reopened);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn storage_pressure_never_prunes_uncommitted_critical_events() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    let mut critical = event(EventPriority::Critical, 1);
    critical.critical_sequence = Some(1);
    store
        .insert_events(std::slice::from_ref(&critical))
        .await
        .unwrap();

    // A one-byte soft limit guarantees the pressure path executes regardless of the
    // filesystem's SQLite allocation granularity.
    store.enforce_storage_policy(1).await.unwrap();

    let health = store.health().await.unwrap();
    assert_eq!(health.pending_critical, 1);
    let batch = store
        .pending_batch(EventPriority::Critical, 32, 64 * 1024)
        .await
        .unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].event.event_id, critical.event_id);
    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn telemetry_keeps_only_the_latest_pending_revision() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    store
        .insert_telemetry_latest(&event(EventPriority::Telemetry, 1))
        .await
        .unwrap();
    store
        .insert_telemetry_latest(&event(EventPriority::Telemetry, 2))
        .await
        .unwrap();
    let batch = store
        .pending_batch(EventPriority::Telemetry, 100, 128 * 1024)
        .await
        .unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].event.entity_revision, 2);
    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn telemetry_coalescing_never_crosses_live_session_boundaries() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    store
        .insert_telemetry_latest(&event_for_session(
            EventPriority::Telemetry,
            7,
            "session-one",
        ))
        .await
        .unwrap();
    store
        .insert_telemetry_latest(&event_for_session(
            EventPriority::Telemetry,
            8,
            "session-two",
        ))
        .await
        .unwrap();

    let first = store
        .pending_batch_for_scope(
            EventPriority::Telemetry,
            "strategy",
            "session-one",
            100,
            128 * 1024,
        )
        .await
        .unwrap();
    let second = store
        .pending_batch_for_scope(
            EventPriority::Telemetry,
            "strategy",
            "session-two",
            100,
            128 * 1024,
        )
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    assert_eq!(first[0].event.live_session_id, "session-one");
    assert_eq!(second[0].event.live_session_id, "session-two");
    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn critical_ack_records_trace_and_stage_rollup() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    let now = now_epoch_micros();
    let mut critical = event(EventPriority::Critical, 1);
    critical.latency.source_occurred_at_us = Some(now - 30_000);
    critical.latency.adapter_queued_at_us = Some(now - 25_000);
    critical.latency.runtime_received_at_us = Some(now - 20_000);
    critical.latency.local_bus_published_at_us = Some(now - 15_000);
    critical.latency.local_outbox_committed_at_us = Some(now - 10_000);
    critical.latency.persistence_request_sent_at_us = Some(now - 5_000);

    store
        .record_persistence_acknowledgements(std::slice::from_ref(&critical), now)
        .await
        .unwrap();
    store.refresh_latency_rollups().await.unwrap();

    let mut traces = store
        .connection
        .query(
            "SELECT COUNT(*) FROM latency_traces WHERE event_id = ?1",
            params![critical.event_id.to_string()],
        )
        .await
        .unwrap();
    assert_eq!(
        traces.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        1
    );
    let mut rollups = store
        .connection
        .query(
            "SELECT sample_count FROM latency_rollups WHERE stage = 'source_to_persistence_ack'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rollups
            .next()
            .await
            .unwrap()
            .unwrap()
            .get::<i64>(0)
            .unwrap(),
        1
    );
    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn accepted_action_cursor_survives_store_reopen() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    assert!(
        !store
            .action_was_accepted("strategy_action:one", "strategy", "session")
            .await
            .unwrap()
    );
    store
        .mark_action_accepted("strategy_action:one", "strategy", "session")
        .await
        .unwrap();
    drop(store);

    let reopened = OutboxStore::open(&path).await.unwrap();
    assert!(
        reopened
            .action_was_accepted("strategy_action:one", "strategy", "session")
            .await
            .unwrap()
    );
    assert!(
        !reopened
            .action_was_accepted("strategy_action:one", "strategy", "other-session")
            .await
            .unwrap()
    );
    drop(reopened);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn cloned_store_serializes_concurrent_connection_use() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    let critical = event(EventPriority::Critical, 1);
    store
        .insert_events(std::slice::from_ref(&critical))
        .await
        .unwrap();

    for _ in 0..25 {
        let health_store = store.clone();
        let count_store = store.clone();
        let batch_store = store.clone();
        let (health, count, batch) = tokio::join!(
            health_store.health(),
            count_store.pending_count_for_scope("strategy", "session"),
            batch_store.pending_batch_for_scope(
                EventPriority::Critical,
                "strategy",
                "session",
                32,
                64 * 1024,
            ),
        );
        assert_eq!(health.unwrap().pending_critical, 1);
        assert_eq!(count.unwrap(), 1);
        assert_eq!(batch.unwrap().len(), 1);
    }

    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn pending_entity_lookup_is_exact_and_returns_only_unconfirmed_history() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    let history = |entity_id: &str, revision: u64| {
        EventEnvelope::new(
            "runtime",
            "strategy",
            "session",
            entity_id,
            revision,
            EventPriority::Audit,
            EventKind::InsightStateHistory,
            serde_json::json!({
                "insight_id": entity_id.trim_start_matches("insight-history:"),
                "state": "Filled",
                "timestamp": chrono::Utc::now(),
            }),
        )
    };
    let expected = history("insight-history:target", 2);
    let committed = history("insight-history:target", 1);
    let other = history("insight-history:other", 3);
    store
        .insert_events(&[expected.clone(), committed.clone(), other])
        .await
        .unwrap();
    store
        .mark_remote_committed(&[committed.event_id], None)
        .await
        .unwrap();

    let pending = store
        .pending_events_for_entity(
            "strategy",
            "session",
            "insight-history:target",
            EventKind::InsightStateHistory,
            25,
        )
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].event_id, expected.event_id);
    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn new_run_supersedes_old_reconciliation_and_preserves_retry_progress() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    let start = |generation: &str, run_id: &str| ReconciliationStart {
        purpose: aqe_live_protocol::ReconciliationPurpose::Startup,
        critical_replay_through: None,
        generation: generation.to_string(),
        runtime_id: "runtime".to_string(),
        strategy_id: "strategy".to_string(),
        live_session_id: "session".to_string(),
        run_id: Some(run_id.to_string()),
        snapshot_complete: true,
        broker_features: vec!["order_lifecycle_snapshot".to_string()],
        critical_cutoff: Some(10),
        total_chunks: 0,
        total_records: 0,
    };
    let old_start = start("generation-old", "run-old");
    let mut old_event = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "reconciliation:generation-old:start",
        1,
        EventPriority::Reconciliation,
        EventKind::ReconciliationStart,
        serde_json::to_value(&old_start).unwrap(),
    );
    old_event.run_id = old_start.run_id.clone();
    store.insert_events(&[old_event.clone()]).await.unwrap();
    store.begin_reconciliation_job(&old_start).await.unwrap();

    let current_start = start("generation-current", "run-current");
    store
        .begin_reconciliation_job(&current_start)
        .await
        .unwrap();
    store
        .record_reconciliation_retry(&current_start.generation, "temporary failure")
        .await
        .unwrap();
    store
        .update_reconciliation_progress(&ReconciliationJobProgress {
            generation: current_start.generation.clone(),
            run_id: current_start.run_id.clone(),
            status: "processing".to_string(),
            candidate_count: 50,
            classified_count: 25,
            unresolved_count: 2,
            current_cursor: Some("insight-25".to_string()),
            retry_count: 0,
            last_error: None,
        })
        .await
        .unwrap();

    let mut old_rows = store
        .connection
        .query(
            "SELECT state FROM outbox_events WHERE event_id = ?1",
            params![old_event.event_id.to_string()],
        )
        .await
        .unwrap();
    assert_eq!(
        old_rows
            .next()
            .await
            .unwrap()
            .unwrap()
            .get::<String>(0)
            .unwrap(),
        "committed"
    );
    drop(old_rows);
    let progress = store
        .health()
        .await
        .unwrap()
        .reconciliation
        .expect("current reconciliation progress");
    assert_eq!(progress.generation, "generation-current");
    assert_eq!(progress.candidate_count, 50);
    assert_eq!(progress.classified_count, 25);
    assert_eq!(progress.unresolved_count, 2);
    assert_eq!(progress.retry_count, 1);

    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn reconciliation_generation_cannot_change_purpose_or_accept_an_unfenced_recovery() {
    let path = temp_database();
    let store = OutboxStore::open(&path).await.unwrap();
    let start = ReconciliationStart {
        purpose: aqe_live_protocol::ReconciliationPurpose::Startup,
        critical_replay_through: None,
        generation: "immutable-generation".into(),
        runtime_id: "runtime".into(),
        strategy_id: "strategy".into(),
        live_session_id: "session".into(),
        run_id: Some("run".into()),
        snapshot_complete: true,
        broker_features: vec![],
        critical_cutoff: Some(10),
        total_chunks: 0,
        total_records: 0,
    };
    store.begin_reconciliation_job(&start).await.unwrap();
    let before = store.health().await.unwrap().reconciliation.unwrap();
    let mut changed = start.clone();
    changed.purpose = aqe_live_protocol::ReconciliationPurpose::CaptureRecovery;
    changed.critical_replay_through = Some(10);
    assert!(matches!(store.begin_reconciliation_job(&changed).await,
        Err(OutboxStoreError::InvalidRecord(_))));
    changed.generation = "unfenced".into();
    changed.critical_cutoff = None;
    assert!(matches!(store.begin_reconciliation_job(&changed).await,
        Err(OutboxStoreError::InvalidRecord(_))));
    let after = store.health().await.unwrap().reconciliation.unwrap();
    assert_eq!(after.generation, before.generation);
    assert_eq!(after.status, before.status);
    assert_eq!(after.candidate_count, before.candidate_count);
    drop(store);
    let _ = std::fs::remove_file(path);
}
