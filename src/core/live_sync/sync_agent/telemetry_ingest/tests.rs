use super::*;
use crate::core::live_sync::{EventEnvelope, InProcessEventBus};

fn event(revision: u64, entity: &str) -> CloudEvent {
    let mut event = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        entity,
        revision,
        EventPriority::Telemetry,
        EventKind::Heartbeat,
        serde_json::json!({"sent_at":"2026-10-27T12:00:00Z"}),
    );
    event.run_id = Some("run".into());
    event
}

async fn queue(events: &[CloudEvent]) -> EventSubscription {
    let bus = InProcessEventBus::default();
    let subject = EventSubject::for_priority("runtime", "strategy", EventPriority::Telemetry);
    let subscription = bus.subscribe(subject.clone()).await.unwrap();
    for event in events {
        bus.publish_telemetry(subject.clone(), event.clone())
            .unwrap();
    }
    subscription
}

#[tokio::test]
async fn buffered_heartbeats_keep_latest_original_timestamp_and_payload() {
    let mut events: Vec<_> = (1..=200)
        .map(|revision| event(revision, "heartbeat"))
        .collect();
    // A delayed replay or duplicate revision cannot replace the newer head.
    events.push(event(2, "heartbeat"));
    let expected = events[199].clone();
    let mut subscription = queue(&events).await;
    let first = subscription.recv().await.unwrap();
    let (heads, carry) = collect(first, &mut subscription).unwrap();
    assert!(carry.is_none());
    assert_eq!(heads.len(), 1);
    assert_eq!(heads[0].event, expected);
}

#[tokio::test]
async fn coalescing_never_crosses_scope_kind_or_durable_delivery() {
    let base = event(1, "same-entity");
    let mut events = vec![base.clone()];
    for boundary in [
        "runtime",
        "strategy",
        "session",
        "run",
        "kind",
        "critical",
        "audit",
        "reconciliation",
        "reply",
        "sequence",
        "custom",
    ] {
        let mut next = base.clone();
        next.entity_revision = 2;
        next.event_id = uuid::Uuid::new_v4();
        match boundary {
            "runtime" => next.runtime_id = "other-runtime".into(),
            "strategy" => next.strategy_id = "other-strategy".into(),
            "session" => next.live_session_id = "other-session".into(),
            "run" => next.run_id = Some("other-run".into()),
            "kind" => next.kind = EventKind::AccountProjection,
            "critical" => next.priority = EventPriority::Critical,
            "audit" => next.priority = EventPriority::Audit,
            "reconciliation" => next.priority = EventPriority::Reconciliation,
            "sequence" => next.critical_sequence = Some(1),
            "custom" => next.kind = EventKind::Custom("append-only".into()),
            _ => {}
        }
        events.push(next.clone());
        if matches!(
            boundary,
            "critical" | "audit" | "reconciliation" | "reply" | "sequence" | "custom"
        ) {
            next.event_id = uuid::Uuid::new_v4();
            next.entity_revision = 3;
            events.push(next);
        }
    }
    let mut subscription = queue(&events).await;
    let mut first = subscription.recv().await.unwrap();
    first.reply_to = Some("reply-inbox".into());
    // Reply-bearing deliveries remain distinct, including the initial head.
    let (heads, _) = collect(first, &mut subscription).unwrap();
    // The pair called "reply" above is ordinary telemetry and may coalesce;
    // every other protected delivery and all distinct scopes must survive.
    assert_eq!(heads.len(), events.len() - 1);
    assert_eq!(heads[0].reply_to.as_deref(), Some("reply-inbox"));
    assert_eq!(
        heads
            .iter()
            .filter(|d| d.event.priority != EventPriority::Telemetry)
            .count(),
        6
    );
    assert_eq!(
        heads
            .iter()
            .filter(|d| d.event.critical_sequence.is_some())
            .count(),
        2
    );
    assert_eq!(
        heads
            .iter()
            .filter(|d| matches!(d.event.kind, EventKind::Custom(_)))
            .count(),
        2
    );
    let left = EventDelivery {
        subject: heads[0].subject.clone(),
        event: base.clone(),
        reply_to: Some("reply".into()),
    };
    let right = EventDelivery {
        subject: heads[0].subject.clone(),
        event: event(3, "same-entity"),
        reply_to: Some("reply".into()),
    };
    assert!(!same_head(&left, &right));
}

#[tokio::test]
async fn count_and_byte_boundaries_carry_every_distinct_head_without_loss() {
    for payload_size in [1, 48 * 1024] {
        let events: Vec<_> = (0..100)
            .map(|n| {
                let mut event = event(n + 1, &format!("head-{n}"));
                event.payload = serde_json::json!({"data":"x".repeat(payload_size)});
                event
            })
            .collect();
        let mut subscription = queue(&events).await;
        let mut next = subscription.recv().await;
        let mut actual = Vec::new();
        while let Some(first) = next {
            let (heads, carry) = collect(first, &mut subscription).unwrap();
            assert!(heads.len() <= MAX_HEADS);
            assert!(
                heads
                    .iter()
                    .map(|d| FlatbufferEventCodec::encode(&d.event).unwrap().len())
                    .sum::<usize>()
                    <= MAX_BYTES
            );
            actual.extend(heads.into_iter().map(|d| d.event));
            next = carry.or_else(|| subscription.try_recv().ok());
        }
        assert_eq!(actual, events);
    }
}

#[tokio::test]
async fn runtime_ingestion_catches_up_to_latest_heads_and_respects_capacity() {
    for capacity in [None, Some(1)] {
        let dir =
            std::env::temp_dir().join(format!("aqe-telemetry-heads-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let outbox = OutboxStore::open(dir.join("outbox.db")).await.unwrap();
        let mut accepted = event(1, "accepted-audit");
        accepted.priority = EventPriority::Audit;
        outbox.insert_events(&[accepted.clone()]).await.unwrap();
        let retained = outbox.capture_retained_bytes().await.unwrap();
        let events: Vec<_> = (1..=800)
            .map(|revision| event(revision, &format!("head-{}", revision % 4)))
            .collect();
        let mut subscription = queue(&events).await;
        let task_store = outbox.clone();
        let (_shutdown, receiver) = watch::channel(false);
        let task =
            tokio::spawn(
                async move { run(task_store, &mut subscription, receiver, capacity).await },
            );
        // queue() drops its sender after seeding; completion proves all 800
        // buffered messages were consumed, including the capacity-refused case.
        tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let rows = outbox
            .pending_batch(EventPriority::Telemetry, 100, MAX_BYTES)
            .await
            .unwrap();
        if capacity.is_none() {
            assert_eq!(rows.len(), 4);
            assert!(rows.iter().all(|row| row.event.entity_revision >= 797));
        } else {
            assert!(rows.is_empty());
            assert_eq!(outbox.capture_retained_bytes().await.unwrap(), retained);
        }
        let audit = outbox
            .pending_batch(EventPriority::Audit, 100, MAX_BYTES)
            .await
            .unwrap();
        assert_eq!(audit.len(), 1);
        assert_eq!(audit[0].event.event_id, accepted.event_id);
        drop(outbox);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[tokio::test]
#[ignore = "isolated local-journal component profile; no application or broker connections"]
async fn buffered_telemetry_journal_profile() {
    let events: Vec<_> = (1..=800)
        .map(|revision| event(revision, &format!("head-{}", revision % 4)))
        .collect();
    let mut phases = Vec::new();
    for coalesced in [false, true, true, false] {
        let dir =
            std::env::temp_dir().join(format!("aqe-heartbeat-profile-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let outbox = OutboxStore::open(dir.join("outbox.db")).await.unwrap();
        let mut subscription = queue(&events).await;
        let start = std::time::Instant::now();
        let mut writes = 0;
        let mut next = subscription.recv().await;
        while let Some(first) = next {
            let (heads, carry) = if coalesced {
                collect(first, &mut subscription).unwrap()
            } else {
                (vec![first], None)
            };
            for head in heads {
                outbox.insert_telemetry_latest(&head.event).await.unwrap();
                writes += 1;
            }
            next = carry.or_else(|| subscription.try_recv().ok());
        }
        let elapsed_us = start.elapsed().as_micros();
        let rows = outbox
            .pending_batch(EventPriority::Telemetry, 100, MAX_BYTES)
            .await
            .unwrap();
        assert_eq!(rows.len(), 4);
        for row in rows {
            let expected = events
                .iter()
                .rev()
                .find(|event| event.entity_id == row.event.entity_id)
                .unwrap();
            let mut actual = row.event;
            actual.latency = expected.latency.clone();
            assert_eq!(&actual, expected);
        }
        assert_eq!(writes, if coalesced { 16 } else { 800 });
        phases.push(serde_json::json!({"coalesced":coalesced,"input_events":800,"sqlite_calls":writes,"elapsed_us":elapsed_us,"exact_final_heads":true}));
        drop(outbox);
        std::fs::remove_dir_all(dir).unwrap();
    }
    println!(
        "HEARTBEAT_COMPONENT_PROFILE {}",
        serde_json::json!({"scope":"debug component only; not end-to-end or production timing","phases":phases})
    );
}
