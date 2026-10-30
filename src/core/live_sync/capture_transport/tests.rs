use super::*;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

struct StoreGuard(std::path::PathBuf);
impl Drop for StoreGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
async fn store(label: &str) -> (StoreGuard, OutboxStore) {
    let path = std::env::temp_dir().join(format!("gap-transport-{label}-{}", uuid::Uuid::new_v4()));
    let store = OutboxStore::open(path.join("outbox.db")).await.unwrap();
    (StoreGuard(path), store)
}
fn event() -> CloudEvent {
    let mut event = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "insight",
        1,
        EventPriority::Critical,
        EventKind::InsightProjection,
        serde_json::json!({"account":"not copied into the notice"}),
    );
    event.run_id = Some("run".into());
    event
}
fn notice(generation: u64) -> CaptureGapNotice {
    let mut notice = CaptureGapNotice {
        version: 1,
        notice_id: uuid::Uuid::nil(),
        strategy_id: "strategy".into(),
        live_session_id: "session".into(),
        source_runtime_id: "runtime".into(),
        source_run_id: Some("run".into()),
        source_generation: generation,
        started_at_us: 1,
        last_blocked_at_us: 2,
        origin_runtime_id: None,
        origin_run_id: None,
        last_blocked_run_id: None,
        ownership_handoffs: 0,
    };
    notice.notice_id = notice.stable_id();
    notice
}

#[derive(Default)]
struct PausedDataSink {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    stopped: tokio::sync::Notify,
    ready: AtomicBool,
    notices: Mutex<Vec<CaptureGapNotice>>,
}
#[async_trait]
impl RemoteBatchSink for PausedDataSink {
    fn safety_run_id(&self) -> Option<&str> {
        Some("run")
    }
    async fn persist_batch(
        &self,
        _: &LiveSyncBatch,
    ) -> Result<LiveSyncBatchResult, RemoteSyncError> {
        Err(RemoteSyncError {
            kind: RemoteSyncErrorKind::CloudPaused,
            message: "fixture cloud paused".into(),
        })
    }
    async fn persist_capture_gap(
        &self,
        notice: &CaptureGapNotice,
    ) -> Result<Option<CaptureGapReceipt>, RemoteSyncError> {
        self.notices.lock().push(notice.clone());
        if !self.ready.load(Ordering::Acquire) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        Ok(Some(CaptureGapReceipt::for_notice(notice)))
    }
    async fn persist_safety(
        &self,
        _: &aqe_live_protocol::SafetyObservation,
    ) -> Result<bool, RemoteSyncError> {
        self.stopped.notify_one();
        Ok(true)
    }
}

#[tokio::test]
async fn running_producer_hands_off_gap_while_remote_data_is_paused_and_stop_is_independent() {
    running_fixture(Arc::new(InProcessEventBus::acknowledged())).await;
}

async fn running_fixture(bus: Arc<dyn LocalEventBus>) {
    let (_source_guard, source) = store("source").await;
    let (_target_guard, target) = store("target").await;
    let lease = Arc::new(
        CaptureOwnerLease::acquire(source.path(), "runtime", "strategy", "session", "run").unwrap(),
    );
    let mut retained = event();
    retained.critical_sequence = Some(1);
    source
        .handoff_accepted_events(&[retained.clone()])
        .await
        .unwrap();
    let source_limit = source.capture_retained_bytes().await.unwrap();
    let mut target_filler = event();
    target_filler.runtime_id = "other-runtime".into();
    target_filler.strategy_id = "other-strategy".into();
    target_filler.critical_sequence = Some(1);
    target.insert_events(&[target_filler]).await.unwrap();
    let target_limit = target.capture_retained_bytes().await.unwrap();
    let sink = Arc::new(PausedDataSink::default());
    let mut config = SyncAgentConfig::new("runtime", "strategy", "session");
    config.capture_limit_bytes = Some(target_limit);
    let agent = SyncAgent::new(config, bus.clone(), target.clone(), sink.clone());
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(agent.run(receiver));
    let publisher = LocalEventPublisher::spawn_owned_with_capture_lease(
        bus,
        source.clone(),
        LocalEventPublisherConfig {
            capture_limit_bytes: Some(source_limit),
            nats_unavailable_fallback_after: Duration::from_millis(20),
            ..Default::default()
        },
        1,
        Arc::new(()),
        lease.clone(),
    );
    assert!(
        publisher
            .wait_for_capture_initialization(Duration::from_secs(2))
            .await
    );
    assert!(matches!(
        publisher
            .queue_durable(
                EventSubject::for_priority("runtime", "strategy", EventPriority::Critical),
                event()
            )
            .unwrap()
            .await
            .unwrap(),
        Err(EventBusError::CloudCapturePaused)
    ));
    tokio::time::timeout(Duration::from_secs(8), sink.entered.notified())
        .await
        .unwrap();
    let handed_off = tokio::time::timeout(Duration::from_secs(2), async {
        while source
            .has_untransferred_capture_pause_for_scope("strategy", "session")
            .await
            .unwrap()
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(
        handed_off.is_ok(),
        "source={:?}, target={:?}, remote={:?}",
        source
            .capture_pause_for_scope("runtime", "strategy", "session")
            .await
            .unwrap(),
        target
            .pending_capture_gap_notices("strategy")
            .await
            .unwrap(),
        &*sink.notices.lock()
    );
    assert!(!task.is_finished());
    assert!(
        source
            .has_unresolved_capture_pause_for_scope("strategy", "session")
            .await
            .unwrap()
    );
    assert!(
        !source
            .has_untransferred_capture_pause_for_scope("strategy", "session")
            .await
            .unwrap()
    );
    assert_eq!(
        target
            .retirement_uncommitted_for_scope("strategy", "session")
            .await
            .unwrap(),
        1
    );
    assert_eq!(source.pending_count().await.unwrap(), 1);
    assert_eq!(target.pending_count().await.unwrap(), 1);
    assert_eq!(source.capture_retained_bytes().await.unwrap(), source_limit);
    assert_eq!(target.capture_retained_bytes().await.unwrap(), target_limit);
    assert_eq!(
        source
            .pending_batch(EventPriority::Critical, 10, 1024 * 1024)
            .await
            .unwrap()[0]
            .event
            .event_id,
        retained.event_id
    );
    assert_eq!(publisher.critical_sequence_cursor(), 1);
    let mut stopped = event();
    stopped.kind = EventKind::StrategyLifecycle;
    stopped.payload = serde_json::json!({"state":"stopped"});
    publisher
        .persist_stop_facts(&[aqe_live_protocol::SafetyObservation::from_event(&stopped).unwrap()])
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(8), sink.stopped.notified())
        .await
        .unwrap();
    assert_eq!(
        publisher.critical_sequence_cursor(),
        1,
        "operational receipts never skip accepted data"
    );
    sink.ready.store(true, Ordering::Release);
    sink.release.notify_waiters();
    tokio::time::timeout(Duration::from_secs(8), async {
        while target
            .retirement_uncommitted_for_scope("strategy", "session")
            .await
            .unwrap()
            != 0
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        source
            .has_unresolved_capture_pause_for_scope("strategy", "session")
            .await
            .unwrap(),
        "receipt is not capture resume or broker recovery"
    );
    assert_eq!(sink.notices.lock()[0].source_run_id.as_deref(), Some("run"));
    assert!(
        !serde_json::to_string(&*sink.notices.lock())
            .unwrap()
            .contains("account")
    );
    publisher.shutdown(Duration::from_secs(3)).await;
    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(publisher);
    drop(lease);
}

struct ReplyFaultBus {
    inner: InProcessEventBus,
    fault: AtomicU8,
    subscribed: tokio::sync::Notify,
}
#[async_trait]
impl LocalEventBus for ReplyFaultBus {
    async fn acknowledge(
        &self,
        delivery: &EventDelivery,
        acknowledgement: SyncAcknowledgement,
    ) -> Result<(), EventBusError> {
        self.inner.acknowledge(delivery, acknowledgement).await
    }
    async fn publish_critical(
        &self,
        s: EventSubject,
        e: CloudEvent,
    ) -> Result<LocalDeliveryHandle, EventBusError> {
        self.inner.publish_critical(s, e).await
    }
    fn publish_telemetry(&self, s: EventSubject, e: CloudEvent) -> Result<(), EventBusError> {
        self.inner.publish_telemetry(s, e)
    }
    async fn subscribe(&self, s: EventSubject) -> Result<EventSubscription, EventBusError> {
        let subscription = self.inner.subscribe(s).await?;
        self.subscribed.notify_one();
        Ok(subscription)
    }
    async fn request_event(
        &self,
        s: EventSubject,
        e: CloudEvent,
        t: Duration,
    ) -> Result<CloudEvent, EventBusError> {
        self.inner.request_event(s, e, t).await
    }
    async fn respond_event(
        &self,
        d: &EventDelivery,
        mut e: CloudEvent,
    ) -> Result<(), EventBusError> {
        match self.fault.swap(0, Ordering::AcqRel) {
            1 => return Ok(()),
            2 => e.payload["noticeId"] = serde_json::json!(uuid::Uuid::new_v4()),
            _ => {}
        }
        self.inner.respond_event(d, e).await
    }
}

#[tokio::test]
async fn lost_or_wrong_local_receipt_retains_source_and_exact_retry_fits_a_full_mailbox() {
    for fault in [1, 2] {
        let (_source_guard, source) = store("receipt-source").await;
        let (_target_guard, target) = store("receipt-target").await;
        let owner =
            CaptureOwnerLease::acquire(source.path(), "runtime", "strategy", "session", "run")
                .unwrap();
        assert!(
            source
                .capture_events_with_capacity(&[event()], 1)
                .await
                .is_err()
        );
        for generation in 1..=255 {
            let mut n = notice(generation);
            n.strategy_id = "another".into();
            n.notice_id = n.stable_id();
            assert!(target.accept_capture_gap_notice(&n).await.unwrap());
        }
        let bus = Arc::new(ReplyFaultBus {
            inner: InProcessEventBus::acknowledged(),
            fault: AtomicU8::new(fault),
            subscribed: tokio::sync::Notify::new(),
        });
        let (shutdown, receiver) = watch::channel(false);
        let worker = tokio::spawn(run_receiver(
            target.clone(),
            bus.clone(),
            "runtime".into(),
            "strategy".into(),
            "session".into(),
            "run".into(),
            receiver,
        ));
        tokio::time::timeout(Duration::from_secs(2), bus.subscribed.notified())
            .await
            .unwrap();
        // Target commit is durable even though its first reply is lost/invalid.
        flush(&source, bus.as_ref(), &owner).await.unwrap();
        assert_eq!(
            target
                .retirement_uncommitted_for_scope("strategy", "session")
                .await
                .unwrap(),
            1
        );
        assert!(
            source
                .has_untransferred_capture_pause_for_scope("strategy", "session")
                .await
                .unwrap()
        );
        flush(&source, bus.as_ref(), &owner).await.unwrap();
        assert!(
            !source
                .has_untransferred_capture_pause_for_scope("strategy", "session")
                .await
                .unwrap()
        );
        assert_eq!(
            target
                .retirement_uncommitted_for_scope("strategy", "session")
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            target
                .retirement_uncommitted_for_scope("another", "session")
                .await
                .unwrap(),
            255
        );
        assert_eq!(target.pending_count().await.unwrap(), 0);
        shutdown.send(true).unwrap();
        worker.await.unwrap();
    }
}

#[tokio::test]
async fn receiver_rejects_foreign_scope_and_non_closed_shape_payloads() {
    let (_target_guard, target) = store("scope").await;
    let bus = Arc::new(ReplyFaultBus {
        inner: InProcessEventBus::acknowledged(),
        fault: AtomicU8::new(0),
        subscribed: tokio::sync::Notify::new(),
    });
    let (shutdown, receiver) = watch::channel(false);
    let worker = tokio::spawn(run_receiver(
        target.clone(),
        bus.clone(),
        "runtime".into(),
        "strategy".into(),
        "session".into(),
        "run".into(),
        receiver,
    ));
    let n = notice(1);
    let subject = EventSubject::capture_gap_handoff("runtime", "strategy");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if bus
                .request_event(subject.clone(), envelope(&n), Duration::from_millis(30))
                .await
                .is_ok()
            {
                break;
            }
        }
    })
    .await
    .unwrap();
    let mut bad = Vec::new();
    let mut e = envelope(&n);
    e.run_id = Some("foreign".into());
    bad.push(e);
    let mut e = envelope(&n);
    e.live_session_id = "foreign".into();
    bad.push(e);
    let mut e = envelope(&n);
    e.payload["account"] = serde_json::json!({"arbitrary":"data"});
    bad.push(e);
    let mut e = envelope(&n);
    e.critical_sequence = Some(1);
    bad.push(e);
    let mut e = envelope(&n);
    e.event_id = uuid::Uuid::new_v4();
    bad.push(e);
    let mut e = envelope(&n);
    e.payload["lastBlockedAtUs"] = serde_json::json!(i64::MAX);
    bad.push(e);
    for e in bad {
        assert!(
            bus.request_event(subject.clone(), e, Duration::from_millis(30))
                .await
                .is_err()
        );
    }
    assert_eq!(
        target
            .retirement_uncommitted_for_scope("strategy", "session")
            .await
            .unwrap(),
        1
    );
    assert_eq!(target.pending_count().await.unwrap(), 0);
    shutdown.send(true).unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn receiver_full_mailbox_does_not_acknowledge_a_new_gap() {
    let (_source_guard, source) = store("full-source").await;
    let (_target_guard, target) = store("full-target").await;
    let owner =
        CaptureOwnerLease::acquire(source.path(), "runtime", "strategy", "session", "run").unwrap();
    assert!(
        source
            .capture_events_with_capacity(&[event()], 1)
            .await
            .is_err()
    );
    for generation in 1..=256 {
        let mut n = notice(generation);
        n.strategy_id = "another".into();
        n.notice_id = n.stable_id();
        assert!(target.accept_capture_gap_notice(&n).await.unwrap());
    }
    let bus = Arc::new(ReplyFaultBus {
        inner: InProcessEventBus::acknowledged(),
        fault: AtomicU8::new(0),
        subscribed: tokio::sync::Notify::new(),
    });
    let (shutdown, receiver) = watch::channel(false);
    let worker = tokio::spawn(run_receiver(
        target.clone(),
        bus.clone(),
        "runtime".into(),
        "strategy".into(),
        "session".into(),
        "run".into(),
        receiver,
    ));
    tokio::time::timeout(Duration::from_secs(2), bus.subscribed.notified())
        .await
        .unwrap();
    flush(&source, bus.as_ref(), &owner).await.unwrap();
    assert!(
        source
            .has_untransferred_capture_pause_for_scope("strategy", "session")
            .await
            .unwrap()
    );
    assert_eq!(
        target
            .retirement_uncommitted_for_scope("strategy", "session")
            .await
            .unwrap(),
        0
    );
    shutdown.send(true).unwrap();
    worker.await.unwrap();
}

#[tokio::test]
#[ignore = "requires an explicit owned NATS binary; never uses the user's server"]
async fn owned_nats_running_gap_handoff_and_stop_do_not_depend_on_data_admission() {
    struct OwnedServer(std::process::Child);
    impl Drop for OwnedServer {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let binary =
        std::env::var_os("AQE_TEST_NATS_SERVER_BIN").expect("set AQE_TEST_NATS_SERVER_BIN");
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let token = format!("owned-gap-fixture-{}", uuid::Uuid::new_v4());
    let _server = OwnedServer(
        std::process::Command::new(binary)
            .args([
                "--addr",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--auth",
                &token,
            ])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    let bus = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match NatsEventBus::connect(&format!("nats://127.0.0.1:{port}"), &token).await {
                Ok(bus) => break Arc::new(bus),
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .unwrap();
    running_fixture(bus).await;
}
