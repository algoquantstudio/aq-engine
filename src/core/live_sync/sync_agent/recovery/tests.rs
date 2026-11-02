use super::*;
use crate::core::live_sync::InProcessEventBus;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct TempRoot(std::path::PathBuf);
impl TempRoot {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("aqe-delivery-recovery-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    async fn store(&self) -> OutboxStore {
        OutboxStore::open(self.0.join("outbox.db")).await.unwrap()
    }
}
impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Default)]
struct ProbeSink {
    reachable: AtomicBool,
    heartbeat_proof: AtomicBool,
    heartbeats: AtomicUsize,
    accepted: Mutex<Vec<EventId>>,
}
#[async_trait]
impl RemoteBatchSink for ProbeSink {
    fn heartbeat_can_wake_delivery(&self) -> bool {
        self.heartbeat_proof.load(Ordering::SeqCst)
    }
    async fn heartbeat(&self) -> Result<(), RemoteSyncError> {
        self.heartbeats.fetch_add(1, Ordering::SeqCst);
        if !self.reachable.load(Ordering::SeqCst) {
            return Err(RemoteSyncError {
                kind: RemoteSyncErrorKind::Transient,
                message: "cloud heartbeat unavailable".into(),
            });
        }
        Ok(())
    }
    async fn persist_batch(
        &self,
        batch: &LiveSyncBatch,
    ) -> Result<LiveSyncBatchResult, RemoteSyncError> {
        if !self.reachable.load(Ordering::SeqCst) {
            return Err(RemoteSyncError {
                kind: RemoteSyncErrorKind::Transient,
                message: "event gateway returned HTTP 503".into(),
            });
        }
        self.accepted
            .lock()
            .extend(batch.events().map(|event| event.event_id));
        Ok(LiveSyncBatchResult {
            accepted_critical_through: batch.last_critical_sequence,
            durability: Some("cloud_stream".into()),
            applied: batch.events().count(),
            ..Default::default()
        })
    }
}

fn event(priority: EventPriority, sequence: u64) -> CloudEvent {
    let kind = if priority == EventPriority::Critical {
        EventKind::InsightProjection
    } else {
        EventKind::EquityProjection
    };
    let mut event = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        format!("entity-{sequence}"),
        sequence,
        priority,
        kind,
        serde_json::json!({"owned_fixture":true}),
    );
    if priority == EventPriority::Critical {
        event.critical_sequence = Some(sequence);
    }
    event
}
fn agent(outbox: &OutboxStore, sink: &Arc<ProbeSink>) -> SyncAgent {
    SyncAgent::new(
        SyncAgentConfig::new("runtime", "strategy", "session"),
        Arc::new(InProcessEventBus::default()),
        outbox.clone(),
        sink.clone(),
    )
}
const TRANSIENT: &str = "event gateway returned HTTP 503";

#[tokio::test]
async fn existing_heartbeat_recovers_restarted_backoff_without_resetting_attempts() {
    let root = TempRoot::new();
    let outbox = root.store().await;
    let event = event(EventPriority::Critical, 1);
    outbox.insert_events(&[event.clone()]).await.unwrap();
    for _ in 0..7 {
        outbox
            .schedule_retry(&[event.event_id], 300, TRANSIENT)
            .await
            .unwrap();
    }
    let sink = Arc::new(ProbeSink::default());
    sink.reachable.store(true, Ordering::SeqCst);
    sink.heartbeat_proof.store(true, Ordering::SeqCst);
    // A new agent has no in-memory hint from the failing process.
    let agent = agent(&outbox, &sink);
    agent.restore_delivery_recovery().await.unwrap();
    assert!(
        outbox
            .pending_batch(EventPriority::Critical, 32, 65536)
            .await
            .unwrap()
            .is_empty()
    );
    let original = outbox
        .deferred_retry_hints(EventPriority::Critical, "runtime", "strategy", "session")
        .await
        .unwrap();
    let remaining_ms = original[0].deadline_us.saturating_sub(now_epoch_micros()) / 1000;
    assert!(remaining_ms > 290_000);
    let started = std::time::Instant::now();
    agent
        .periodic_step(periodic::Job::CloudHeartbeat)
        .await
        .unwrap();
    let due = outbox
        .pending_batch(EventPriority::Critical, 32, 65536)
        .await
        .unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].attempt_count, 7);
    assert_eq!(due[0].last_error.as_deref(), Some(TRANSIENT));
    assert_eq!(due[0].event.event_id, event.event_id);
    agent
        .flush_priority(EventPriority::Critical, 32, 65536)
        .await
        .unwrap();
    assert_eq!(outbox.pending_count().await.unwrap(), 0);
    assert_eq!(*sink.accepted.lock(), vec![event.event_id]);
    assert_eq!(sink.heartbeats.load(Ordering::SeqCst), 1);
    assert!(started.elapsed() < Duration::from_secs(2));
    eprintln!(
        "DELIVERY_RECOVERY_REHEARSAL baseline_remaining_ms={remaining_ms} recovered_ms={} events=1 heartbeat_requests=1",
        started.elapsed().as_millis()
    );
}

#[tokio::test]
async fn failed_or_noop_heartbeats_do_not_release_deferred_work() {
    let root = TempRoot::new();
    let outbox = root.store().await;
    let event = event(EventPriority::Critical, 1);
    outbox.insert_events(&[event.clone()]).await.unwrap();
    outbox
        .schedule_retry(&[event.event_id], 300, TRANSIENT)
        .await
        .unwrap();
    let sink = Arc::new(ProbeSink::default());
    sink.heartbeat_proof.store(true, Ordering::SeqCst);
    let agent = agent(&outbox, &sink);
    agent.restore_delivery_recovery().await.unwrap();
    agent
        .periodic_step(periodic::Job::CloudHeartbeat)
        .await
        .unwrap();
    assert!(
        outbox
            .pending_batch(EventPriority::Critical, 32, 65536)
            .await
            .unwrap()
            .is_empty()
    );
    sink.reachable.store(true, Ordering::SeqCst);
    sink.heartbeat_proof.store(false, Ordering::SeqCst);
    agent
        .periodic_step(periodic::Job::CloudHeartbeat)
        .await
        .unwrap();
    assert!(
        outbox
            .pending_batch(EventPriority::Critical, 32, 65536)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(sink.accepted.lock().is_empty());
}

#[tokio::test]
async fn recovery_notification_wakes_the_sleeping_critical_worker() {
    let root = TempRoot::new();
    let outbox = root.store().await;
    let event = event(EventPriority::Critical, 1);
    outbox.insert_events(&[event.clone()]).await.unwrap();
    outbox
        .schedule_retry(&[event.event_id], 300, TRANSIENT)
        .await
        .unwrap();
    let sink = Arc::new(ProbeSink::default());
    sink.heartbeat_proof.store(true, Ordering::SeqCst);
    let agent = agent(&outbox, &sink);
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(agent.clone().run(receiver));
    tokio::time::timeout(Duration::from_secs(2), async {
        while sink.heartbeats.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    // The initial, failed heartbeat left the five-minute deadline intact.
    assert!(sink.accepted.lock().is_empty());
    sink.reachable.store(true, Ordering::SeqCst);
    agent
        .periodic_step(periodic::Job::CloudHeartbeat)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while outbox.pending_count().await.unwrap() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the registered critical waiter must wake, not sleep until the old deadline");
    shutdown.send(true).unwrap();
    task.await.unwrap().unwrap();
    assert_eq!(*sink.accepted.lock(), vec![event.event_id]);
    assert_eq!(sink.heartbeats.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn successful_delivery_wakes_other_lane_without_extra_heartbeat() {
    let root = TempRoot::new();
    let outbox = root.store().await;
    let telemetry = event(EventPriority::Telemetry, 2);
    let critical = event(EventPriority::Critical, 1);
    outbox
        .insert_events(&[telemetry.clone(), critical.clone()])
        .await
        .unwrap();
    outbox
        .schedule_retry(&[telemetry.event_id], 300, TRANSIENT)
        .await
        .unwrap();
    let sink = Arc::new(ProbeSink::default());
    sink.reachable.store(true, Ordering::SeqCst);
    let agent = agent(&outbox, &sink);
    agent.restore_delivery_recovery().await.unwrap();
    agent
        .flush_priority(EventPriority::Critical, 32, 65536)
        .await
        .unwrap();
    agent
        .flush_priority(EventPriority::Telemetry, 100, 131072)
        .await
        .unwrap();
    assert_eq!(
        *sink.accepted.lock(),
        vec![critical.event_id, telemetry.event_id]
    );
    assert_eq!(outbox.pending_count().await.unwrap(), 0);
    assert_eq!(sink.heartbeats.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn recovery_cannot_cross_scope_or_override_a_later_auth_failure() {
    let root = TempRoot::new();
    let outbox = root.store().await;
    let own = event(EventPriority::Critical, 1);
    let mut foreign = event(EventPriority::Critical, 2);
    foreign.runtime_id = "another-runtime".into();
    let mut foreign_session = event(EventPriority::Critical, 3);
    foreign_session.live_session_id = "another-session".into();
    let mut foreign_strategy = event(EventPriority::Critical, 4);
    foreign_strategy.strategy_id = "another-strategy".into();
    let events = vec![own.clone(), foreign, foreign_session, foreign_strategy];
    let ids = events.iter().map(|e| e.event_id).collect::<Vec<_>>();
    outbox.insert_events(&events).await.unwrap();
    outbox.schedule_retry(&ids, 300, TRANSIENT).await.unwrap();
    // The error changed after the recovery hint was captured.
    outbox
        .schedule_retry(&[own.event_id], 300, "event gateway returned HTTP 403")
        .await
        .unwrap();
    assert_eq!(
        outbox
            .wake_retry_events(
                EventPriority::Critical,
                "runtime",
                "strategy",
                "session",
                &ids,
                TRANSIENT
            )
            .await
            .unwrap(),
        0
    );
    assert!(
        outbox
            .pending_batch(EventPriority::Critical, 32, 65536)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(outbox.pending_count().await.unwrap(), 4);
    assert!(
        outbox
            .wake_retry_events(
                EventPriority::Critical,
                "runtime",
                "strategy",
                "session",
                &vec![own.event_id; 257],
                TRANSIENT
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn explicit_cloud_pause_and_critical_sequence_order_remain_authoritative() {
    let root = TempRoot::new();
    let outbox = root.store().await;
    let first = event(EventPriority::Critical, 1);
    let second = event(EventPriority::Critical, 2);
    outbox
        .insert_events(&[first.clone(), second.clone()])
        .await
        .unwrap();
    outbox
        .schedule_retry(&[first.event_id], 300, "sequence repair required")
        .await
        .unwrap();
    outbox
        .schedule_retry(&[second.event_id], 300, TRANSIENT)
        .await
        .unwrap();
    let sink = Arc::new(ProbeSink::default());
    sink.reachable.store(true, Ordering::SeqCst);
    sink.heartbeat_proof.store(true, Ordering::SeqCst);
    let agent = agent(&outbox, &sink);
    agent.restore_delivery_recovery().await.unwrap();
    agent.pause_cloud_delivery("explicit fixture pause".into());
    agent
        .periodic_step(periodic::Job::CloudHeartbeat)
        .await
        .unwrap();
    agent.wake_delivery_retries().await;
    assert_eq!(sink.heartbeats.load(Ordering::SeqCst), 0);
    assert!(
        outbox
            .pending_batch(EventPriority::Critical, 32, 65536)
            .await
            .unwrap()
            .is_empty()
    );
    agent.health_state.lock().cloud_pause_until = None;
    agent.wake_delivery_retries().await;
    // Sequence two is due, but it cannot pass the still-backed-off predecessor.
    assert!(
        outbox
            .pending_batch(EventPriority::Critical, 32, 65536)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(outbox.pending_count().await.unwrap(), 2);
}

#[test]
fn hints_are_bounded_deduplicated_paced_and_do_not_promote_rate_limits() {
    let recovery = Recovery::default();
    let ids = (0..1000).map(|_| uuid::Uuid::new_v4()).collect::<Vec<_>>();
    let until = now_epoch_micros() + 300_000_000;
    recovery.remember(EventPriority::Critical, &ids, TRANSIENT, until);
    recovery.remember(EventPriority::Critical, &ids, TRANSIENT, until);
    recovery.remember(
        EventPriority::Telemetry,
        &ids,
        "event gateway returned HTTP 429",
        until,
    );
    let first = recovery.proven_reachable();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].ids.len(), LIMIT);
    recovery.remember(EventPriority::Critical, &ids[..1], TRANSIENT, until);
    assert!(
        recovery.proven_reachable().is_empty(),
        "another success must not cause a recovery storm"
    );
    assert!(known_transport_failure("event gateway returned HTTP 503"));
    assert!(known_transport_failure(
        "event gateway publication unavailable"
    ));
    for error in [
        "event gateway returned HTTP 429",
        "event gateway returned HTTP 403",
        "sequence repair required",
        "Cloud delivery is paused by the service; local execution continues.",
        "arbitrary unavailable message",
    ] {
        assert!(!known_transport_failure(error));
    }
}
