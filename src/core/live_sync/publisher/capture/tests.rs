use super::*;
use crate::core::live_sync::{EventEnvelope, EventKind, LocalDeliveryHandle};
use async_trait::async_trait;

#[derive(Default)]
struct SwitchableBus {
    online: AtomicBool,
    local: InProcessEventBus,
}
#[async_trait]
impl LocalEventBus for SwitchableBus {
    async fn publish_critical(
        &self,
        subject: EventSubject,
        event: CloudEvent,
    ) -> Result<LocalDeliveryHandle, EventBusError> {
        if !self.online.load(Ordering::Acquire) {
            return Err(EventBusError::Publish("fixture transport offline".into()));
        }
        self.local.publish_critical(subject, event).await
    }
    fn publish_telemetry(
        &self,
        subject: EventSubject,
        event: CloudEvent,
    ) -> Result<(), EventBusError> {
        self.local.publish_telemetry(subject, event)
    }
    async fn subscribe(&self, subject: EventSubject) -> Result<EventSubscription, EventBusError> {
        self.local.subscribe(subject).await
    }
    async fn acknowledge(
        &self,
        delivery: &EventDelivery,
        ack: SyncAcknowledgement,
    ) -> Result<(), EventBusError> {
        self.local.acknowledge(delivery, ack).await
    }
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
        serde_json::json!({"revision":revision,"data":"x".repeat(2048)}),
    )
}
fn subject() -> EventSubject {
    EventSubject::for_priority("runtime", "strategy", EventPriority::Critical)
}
fn size(event: &CloudEvent) -> u64 {
    let mut event = event.clone();
    event.critical_sequence = Some(1);
    FlatbufferEventCodec::encode_with_local_commit(
        &event,
        crate::core::live_sync::now_epoch_micros(),
    )
    .unwrap()
    .len() as u64
}
async fn store() -> OutboxStore {
    OutboxStore::open(
        std::env::temp_dir().join(format!("publisher-capture-{}.db", uuid::Uuid::new_v4())),
    )
    .await
    .unwrap()
}
async fn wait_state(publisher: &LocalEventPublisher, state: CloudCaptureState) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while publisher.cloud_capture_status().state != state && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        publisher.cloud_capture_status().state,
        state,
        "{:?}",
        publisher.cloud_capture_status()
    );
}
async fn durable(publisher: &LocalEventPublisher, event: CloudEvent) {
    let id = event.event_id;
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(3),
            publisher.queue_durable(subject(), event).unwrap()
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap(),
        id
    );
}

#[tokio::test]
async fn full_capture_is_nonfatal_bounded_and_requires_explicit_reconciliation() {
    let outbox = store().await;
    let bus = Arc::new(SwitchableBus::default());
    let first = event(1);
    let second = event(2);
    let refused = event(3);
    let limit = size(&first) + size(&second);
    let publisher = LocalEventPublisher::spawn(
        bus.clone(),
        Some(outbox.clone()),
        LocalEventPublisherConfig {
            capture_limit_bytes: Some(limit),
            critical_batch_delay: Duration::from_millis(1),
            ..Default::default()
        },
    );
    let source_gate = publisher.cloud_capture_gate();
    assert!(source_gate.is_paused(), "initializing capture cannot start optional source reads");
    assert!(
        publisher
            .wait_for_capture_initialization(Duration::from_secs(3))
            .await
    );
    wait_state(&publisher, CloudCaptureState::Active).await;
    assert!(!source_gate.is_paused());
    durable(&publisher, first.clone()).await;
    durable(&publisher, second.clone()).await;
    let receipt = publisher.queue_durable(subject(), refused).unwrap();
    assert!(matches!(
        receipt.await.unwrap(),
        Err(EventBusError::CloudCapturePaused)
    ));
    wait_state(&publisher, CloudCaptureState::Paused).await;
    assert!(source_gate.is_paused());
    assert!(!publisher.admission_failed());
    let retained = outbox.capture_retained_bytes().await.unwrap();
    assert_eq!(retained, limit);
    assert_eq!(publisher.critical_sequence_cursor(), 2);
    // Sustained producers cannot fill another RAM queue, reserve identities,
    // append another pending payload, or poison trading with a fatal error.
    for revision in 4..1004 {
        assert!(matches!(
            publisher.queue_durable(subject(), event(revision)),
            Err(EventBusError::CloudCapturePaused)
        ));
    }
    assert_eq!(outbox.capture_retained_bytes().await.unwrap(), retained);
    assert_eq!(publisher.critical_sequence_cursor(), 2);
    assert_eq!(publisher.health().queued_encoded_bytes, 0);
    assert_eq!(outbox.health().await.unwrap().pending_critical, 2);
    // Local preview still updates while cloud capture is paused. It is not a
    // durable/cloud acknowledgement and does not enter the accepted sequence.
    let mut preview = bus
        .subscribe(EventSubject::provisional("runtime", "strategy"))
        .await
        .unwrap();
    let local = event(1004);
    let local_id = local.event_id;
    assert!(matches!(
        publisher.try_publish_critical(subject(), local),
        Err(EventBusError::CloudCapturePaused)
    ));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), preview.recv())
            .await
            .unwrap()
            .unwrap()
            .event
            .event_id,
        local_id
    );
    bus.online.store(true, Ordering::Release);
    wait_state(&publisher, CloudCaptureState::ReconciliationRequired).await;
    assert!(source_gate.is_paused(), "free space alone is not a resume receipt");
    assert!(publisher.cloud_capture_paused());
    assert_eq!(outbox.health().await.unwrap().pending_critical, 0);
    assert!(matches!(
        publisher.queue_durable(subject(), event(1005)),
        Err(EventBusError::CloudCapturePaused)
    ));
    let pause = publisher.cloud_capture_status().pause.unwrap();
    assert!(
        !publisher
            .complete_capture_reconciliation(pause.generation.saturating_sub(1))
            .await
            .unwrap()
    );
    // The owner publishes a gap notice and a bounded current-state snapshot,
    // awaiting their local durable receipts before completing reconciliation.
    let mut gap = event(1006);
    gap.kind = EventKind::StrategyEvent;
    gap.payload = serde_json::json!({"message":"cloud history gap","from":pause.started_at_us});
    publisher
        .queue_recovery_durable(subject(), gap)
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    publisher
        .queue_recovery_durable(subject(), event(1007))
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while publisher.pending_critical_count() != 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    outbox.prune_committed(0).await.unwrap();
    assert!(
        publisher
            .complete_capture_reconciliation(pause.generation)
            .await
            .unwrap()
    );
    wait_state(&publisher, CloudCaptureState::Active).await;
    assert!(!source_gate.is_paused(), "the shared view resumes only after the owner CAS");
    durable(&publisher, event(1008)).await;
    assert_eq!(publisher.critical_sequence_cursor(), 5);
    assert!(!publisher.admission_failed());
    publisher.shutdown(Duration::from_secs(2)).await;
    assert!(publisher.shutdown_complete());
}

#[tokio::test]
async fn publisher_restart_restores_the_pause_and_retains_old_accepted_payloads() {
    let outbox = store().await;
    let first = event(1);
    let limit = size(&first);
    outbox
        .reserve_publisher_events_with_capacity(&[first.clone()], 0, limit)
        .await
        .unwrap();
    assert!(matches!(
        outbox
            .reserve_publisher_events_with_capacity(&[event(2)], 0, limit)
            .await,
        Err(OutboxStoreError::CapturePaused(_))
    ));
    let original = outbox.latest_capture_pause().await.unwrap().unwrap();
    let path = outbox.path().to_path_buf();
    drop(outbox);
    let outbox = OutboxStore::open(path).await.unwrap();
    let bus = Arc::new(SwitchableBus::default());
    let publisher = LocalEventPublisher::spawn(
        bus.clone(),
        Some(outbox.clone()),
        LocalEventPublisherConfig {
            capture_limit_bytes: Some(limit),
            ..Default::default()
        },
    );
    assert!(
        publisher
            .wait_for_capture_initialization(Duration::from_secs(3))
            .await
    );
    assert_eq!(
        publisher.cloud_capture_status().state,
        CloudCaptureState::Paused
    );
    assert_eq!(
        publisher.cloud_capture_status().pause,
        Some(original.clone())
    );
    assert_eq!(outbox.health().await.unwrap().pending_critical, 1);
    assert!(matches!(
        publisher.queue_recovery_durable(subject(), event(3)),
        Err(EventBusError::CloudCapturePaused)
    ));
    bus.online.store(true, Ordering::Release);
    wait_state(&publisher, CloudCaptureState::ReconciliationRequired).await;
    assert_eq!(
        publisher.cloud_capture_status().pause.unwrap().generation,
        original.generation
    );
    assert_eq!(outbox.health().await.unwrap().pending_critical, 0);
    assert!(!publisher.admission_failed());
    publisher.shutdown(Duration::from_secs(2)).await;
    assert!(publisher.shutdown_complete());
}

#[test]
fn default_standalone_health_remains_compatible_and_stale_refresh_cannot_clear_a_pause() {
    let health:PublisherHealth=serde_json::from_value(serde_json::json!({
        "pending_critical":0,"pending_telemetry_entities":0,"locally_accepted_critical":0,
        "deduplicated_critical":0,"emergency_outbox_writes":0,"publish_failures":0,"shutting_down":false
    })).unwrap();
    assert_eq!(health.cloud_capture.state, CloudCaptureState::Disabled);
    assert!(
        LocalEventPublisherConfig::default()
            .capture_limit_bytes
            .is_none()
    );
    let controller = CaptureController::new(Some(100));
    let revision = controller.revision.load(Ordering::Acquire);
    controller.refused(CaptureCapacity {
        retained_bytes: 100,
        requested_bytes: 1,
        limit_bytes: 100,
        physical:None,
    });
    controller.publish_if_current(
        CloudCaptureStatus {
            state: CloudCaptureState::Active,
            ..Default::default()
        },
        Some(revision),
    );
    assert_eq!(controller.status().state, CloudCaptureState::Paused);
    assert!(controller.blocked(true));
}
