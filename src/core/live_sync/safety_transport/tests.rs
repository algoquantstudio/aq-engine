use super::*;
use async_trait::async_trait;
use parking_lot::Mutex;

fn event(revision: u64, state: &str) -> CloudEvent {
    let mut event = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "lifecycle",
        revision,
        EventPriority::Critical,
        EventKind::StrategyLifecycle,
        serde_json::json!({"state":state,"account":{"private":"x".repeat(2048)}}),
    );
    event.run_id = Some("run".into());
    event
}
async fn store() -> OutboxStore {
    OutboxStore::open(
        std::env::temp_dir().join(format!("safety-transport-{}.db", uuid::Uuid::new_v4())),
    )
    .await
    .unwrap()
}
#[derive(Default)]
struct PausedSink(Mutex<Vec<SafetyObservation>>);
#[async_trait]
impl RemoteBatchSink for PausedSink {
    fn safety_run_id(&self) -> Option<&str> {
        Some("run")
    }
    async fn persist_safety(&self, fact: &SafetyObservation) -> Result<bool, RemoteSyncError> {
        self.0.lock().push(fact.clone());
        Ok(true)
    }
    async fn persist_batch(
        &self,
        _: &LiveSyncBatch,
    ) -> Result<LiveSyncBatchResult, RemoteSyncError> {
        Err(RemoteSyncError {
            kind: RemoteSyncErrorKind::CloudPaused,
            message: "fixture quota exhausted".into(),
        })
    }
}

#[tokio::test]
async fn full_source_and_receiver_buffers_still_handoff_and_deliver_stop_facts() {
    full_buffers_fixture(Arc::new(InProcessEventBus::acknowledged())).await;
}

async fn full_buffers_fixture(bus: Arc<dyn LocalEventBus>) {
    let source = store().await;
    let target = store().await;
    let mut old = event(1, "running");
    old.runtime_id = "other".into();
    old.critical_sequence = Some(1);
    let limit = FlatbufferEventCodec::encode_with_local_commit(&old, now_epoch_micros())
        .unwrap()
        .len() as u64;
    target.insert_events(&[old]).await.unwrap();
    let sink = Arc::new(PausedSink::default());
    let mut config = SyncAgentConfig::new("runtime", "strategy", "session");
    config.capture_limit_bytes = Some(limit);
    let agent = SyncAgent::new(config, bus.clone(), target.clone(), sink.clone());
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(agent.run(receiver));
    let retained = event(1, "running");
    let mut sized = retained.clone();
    sized.critical_sequence = Some(1);
    let source_limit = FlatbufferEventCodec::encode_with_local_commit(&sized, now_epoch_micros())
        .unwrap()
        .len() as u64;
    let publisher = LocalEventPublisher::spawn(
        bus.clone(),
        Some(source.clone()),
        LocalEventPublisherConfig {
            capture_limit_bytes: Some(source_limit),
            nats_unavailable_fallback_after: Duration::from_millis(20),
            ..Default::default()
        },
    );
    assert!(
        publisher
            .wait_for_capture_initialization(Duration::from_secs(2))
            .await
    );
    let subject = EventSubject::for_priority("runtime", "strategy", EventPriority::Critical);
    publisher
        .queue_durable(subject.clone(), retained)
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        publisher
            .queue_durable(subject, event(2, "running"))
            .unwrap()
            .await
            .unwrap(),
        Err(EventBusError::CloudCapturePaused)
    ));
    let stopped = SafetyObservation::from_event(&event(3, "stopped")).unwrap();
    publisher
        .persist_stop_facts(&[stopped.clone()])
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(8), async {
        while !sink.0.lock().iter().any(|fact| fact == &stopped) {
            assert!(!task.is_finished());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(source.pending_count().await.unwrap(), 1);
    assert_eq!(target.pending_count().await.unwrap(), 1);
    assert_eq!(source.capture_retained_bytes().await.unwrap(), source_limit);
    assert_eq!(target.capture_retained_bytes().await.unwrap(), limit);
    assert_eq!(publisher.critical_sequence_cursor(), 1);
    assert!(!publisher.admission_failed());
    assert!(
        serde_json::to_string(&*sink.0.lock())
            .unwrap()
            .find("account")
            .is_none()
    );
    publisher.shutdown(Duration::from_secs(3)).await;
    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
#[ignore = "requires an explicit owned NATS binary; never connects to the user's server"]
async fn owned_nats_stop_handoff_preserves_full_buffers_and_independent_receipts() {
    struct OwnedServer(std::process::Child);
    impl Drop for OwnedServer {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let binary = std::env::var_os("AQE_TEST_NATS_SERVER_BIN")
        .expect("set AQE_TEST_NATS_SERVER_BIN to a local binary");
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let _server = OwnedServer(
        std::process::Command::new(binary)
            .args([
                "--addr",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--auth",
                "owned-stop-fixture",
            ])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    let bus = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match NatsEventBus::connect(&format!("nats://127.0.0.1:{port}"), "owned-stop-fixture")
                .await
            {
                Ok(bus) => break Arc::new(bus),
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .unwrap();
    full_buffers_fixture(bus).await;
}

#[tokio::test]
async fn receiver_rejects_other_runs_and_customer_payloads_without_an_operational_receipt() {
    let target = store().await;
    let bus = Arc::new(InProcessEventBus::default());
    let subject = EventSubject::operational_safety("runtime", "strategy");
    let subscription = bus.subscribe(subject.clone()).await.unwrap();
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(run_receiver(
        target.clone(),
        bus.clone(),
        subscription,
        "runtime".into(),
        "strategy".into(),
        "session".into(),
        "run".into(),
        receiver,
    ));
    let fact = SafetyObservation::from_event(&event(1, "stopped")).unwrap();
    let mut other = fact.clone();
    other.run_id = "other-run".into();
    assert!(
        bus.request_event(subject.clone(), envelope(&other), Duration::from_millis(30))
            .await
            .is_err()
    );
    let mut forged = envelope(&fact);
    forged.payload["account"] = serde_json::json!({"private":true});
    assert!(
        bus.request_event(subject.clone(), forged, Duration::from_millis(30))
            .await
            .is_err()
    );
    assert_eq!(
        target
            .retirement_uncommitted_for_scope("strategy", "session")
            .await
            .unwrap(),
        0
    );
    let response = bus
        .request_event(subject, envelope(&fact), Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(decode(&response), Some(fact));
    assert_eq!(target.pending_count().await.unwrap(), 0);
    assert_eq!(
        target
            .retirement_uncommitted_for_scope("strategy", "session")
            .await
            .unwrap(),
        1
    );
    shutdown.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_full_event_ack_is_not_an_operational_receipt() {
    let source = store().await;
    let bus = Arc::new(InProcessEventBus::default());
    let fact = SafetyObservation::from_event(&event(1, "stopped")).unwrap();
    source.record_safety_only(&[fact.clone()]).await.unwrap();
    let mut subscription = bus
        .subscribe(EventSubject::operational_safety("runtime", "strategy"))
        .await
        .unwrap();
    let responder = bus.clone();
    let task = tokio::spawn(async move {
        let delivery = subscription.recv().await.unwrap();
        let mut response = delivery.event.clone();
        response.payload =
            serde_json::json!({"status":"local_accepted","event_ids":[fact.event_id]});
        responder.respond_event(&delivery, response).await.unwrap();
    });
    flush(&source, bus.as_ref()).await.unwrap();
    task.await.unwrap();
    assert_eq!(
        source
            .retirement_uncommitted_for_scope("strategy", "session")
            .await
            .unwrap(),
        1
    );
}
