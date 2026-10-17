use super::*;
use crate::core::live_sync::{EventEnvelope, EventKind, InProcessEventBus, ReconciliationRecord};
use parking_lot::Mutex;
use uuid::Uuid;

#[test]
fn runtime_health_interval_is_one_second() {
    assert_eq!(
        SyncAgentConfig::new("runtime", "strategy", "session").health_interval,
        Duration::from_secs(1)
    );
}

#[tokio::test]
async fn idle_delivery_lanes_avoid_hot_polling_and_wake_on_new_audit_work() {
    let path = std::env::temp_dir().join(format!("aqe-idle-performance-{}.db", Uuid::new_v4()));
    let outbox = OutboxStore::open(&path).await.unwrap();
    let sink = Arc::new(RecordingSink::default());
    let agent = SyncAgent::new(
        SyncAgentConfig::new("runtime", "strategy", "session"),
        Arc::new(InProcessEventBus::default()),
        outbox.clone(),
        sink.clone(),
    );
    let jobs = [
        periodic::Job::Telemetry,
        periodic::Job::Audit,
        periodic::Job::Reconciliation,
    ];
    let before = outbox.diagnostic_query_counts();
    let started = std::time::Instant::now();
    // The previous implementation executed these steps on every 100 ms tick,
    // even for empty lanes. Exercise exactly that path for the baseline.
    for _ in 0..20 {
        for job in jobs {
            agent.periodic_step(job).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let middle = outbox.diagnostic_query_counts();
    let old = std::array::from_fn::<_, 3, _>(|n| middle[n] - before[n]);
    let old_elapsed = started.elapsed();
    let (shutdown, receiver) = watch::channel(false);
    let mut tasks = tokio::task::JoinSet::new();
    for job in jobs {
        let agent = agent.clone();
        let receiver = receiver.clone();
        tasks.spawn(async move { agent.run_periodic(job, receiver).await });
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let after = outbox.diagnostic_query_counts();
    let new = std::array::from_fn::<_, 3, _>(|n| after[n] - middle[n]);
    assert_eq!(new[0], 0, "idle lanes must not select full batches");
    assert_eq!(
        new[2], 0,
        "idle audit/reconciliation must not count critical work"
    );
    assert!(new.iter().sum::<usize>() < old.iter().sum::<usize>() / 2);
    let mut audit = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "wake",
        1,
        EventPriority::Audit,
        EventKind::InsightStateHistory,
        serde_json::json!({"state":"New"}),
    );
    audit.run_id = Some("run".into());
    outbox.insert_events(&[audit]).await.unwrap();
    tokio::time::timeout(Duration::from_millis(500), async {
        while sink.batches.lock().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("an idle audit lane must wake promptly");
    shutdown.send_replace(true);
    while let Some(result) = tasks.join_next().await {
        result.unwrap().unwrap();
    }
    eprintln!(
        "runtime-idle-delivery old_queries={old:?} new_queries={new:?} old_sample_ms={} new_sample_ms=2000",
        old_elapsed.as_millis()
    );
    drop(outbox);
    drop(agent);
    let _ = std::fs::remove_file(path);
}

fn http_test_config(strategy_id: &str) -> HttpBatchSinkConfig {
    HttpBatchSinkConfig {
        batch_url: "https://db.example.com/api/aqs/aqs/aqs/live/v1/batch".into(),
        actions_url: None,
        bearer_token: "test-token".into(),
        session_secret: Some("test-secret".into()),
        signin_url: None,
        namespace: "aqs".into(),
        database: "aqs".into(),
        access_method: "aqe_live".into(),
        user_id: "users:user".into(),
        strategy_id: strategy_id.into(),
        live_session_id: "session".into(),
        runtime_id: Uuid::new_v4().to_string(),
        run_id: Uuid::new_v4().to_string(),
        server_node_id: "server_node:node".into(),
        request_timeout: Duration::from_secs(1),
    }
}

#[test]
fn gateway_is_required_at_sink_construction() {
    for url in ["", "   ", "file:///tmp/legacy"] {
        assert!(HttpBatchSink::new(http_test_config("strategy:one"), url).is_err());
    }
    assert!(
        HttpBatchSink::new(
            http_test_config("strategy:one"),
            "https://events.example.com"
        )
        .is_ok()
    );
}

#[tokio::test]
async fn unavailable_gateway_never_posts_to_direct_batch_api() {
    let direct = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gateway = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", gateway.local_addr().unwrap());
    drop(gateway); // Refused connection, not a slow remote service.
    let mut config = http_test_config("strategy:one");
    config.batch_url = format!("http://{}/batch", direct.local_addr().unwrap());
    let sink = HttpBatchSink::new(config, &endpoint).unwrap();
    let event = EventEnvelope::new(
        "runtime",
        "strategy:one",
        "session",
        "insight",
        1,
        EventPriority::Audit,
        EventKind::ExecutionFillPage,
        serde_json::json!({}),
    );
    let batch = LiveSyncBatch::from_events(vec![event]);
    let error = sink.persist_batch(&batch).await.unwrap_err();
    assert_eq!(error.kind, RemoteSyncErrorKind::Transient);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), direct.accept())
            .await
            .is_err(),
        "gateway failure contacted the removed direct batch route"
    );
}

#[test]
fn critical_batch_preflight_detects_an_internal_sequence_gap() {
    let record = |sequence| {
        let mut event = EventEnvelope::new(
            "runtime",
            "strategy",
            "session",
            format!("entity-{sequence}"),
            sequence,
            EventPriority::Critical,
            EventKind::InsightProjection,
            serde_json::json!({}),
        );
        event.critical_sequence = Some(sequence);
        OutboxEventRecord {
            event,
            attempt_count: 0,
            next_attempt_at_us: 0,
            local_committed_at_us: 0,
            last_error: None,
        }
    };
    assert_eq!(
        first_internal_critical_gap(&[record(10), record(11), record(13)]),
        Some((12, 13))
    );
    assert_eq!(
        first_internal_critical_gap(&[record(10), record(11), record(12)]),
        None
    );
}

#[test]
fn http_sinks_share_a_remote_write_gate_per_strategy_session() {
    let first = HttpBatchSink::new(
        http_test_config("strategy:one"),
        "https://events.example.com",
    )
    .unwrap();
    let second = HttpBatchSink::new(
        http_test_config("strategy:one"),
        "https://events.example.com",
    )
    .unwrap();
    let other = HttpBatchSink::new(
        http_test_config("strategy:two"),
        "https://events.example.com",
    )
    .unwrap();

    assert!(Arc::ptr_eq(
        &first.remote_write_gate,
        &second.remote_write_gate
    ));
    assert!(!Arc::ptr_eq(
        &first.remote_write_gate,
        &other.remote_write_gate
    ));
}

#[derive(Default)]
struct RecordingSink {
    batches: Mutex<Vec<LiveSyncBatch>>,
    reconciliations: Mutex<Vec<String>>,
}

struct CursorConflictSink {
    remote_cursor: u64,
}

struct SelectiveRejectSink {
    rejected_entity: &'static str,
    accepted_entities: Mutex<Vec<String>>,
}

struct TransientFailureSink;

struct PanickingHealthSink;

#[async_trait]
impl RemoteBatchSink for PanickingHealthSink {
    async fn persist_batch(
        &self,
        _: &LiveSyncBatch,
    ) -> Result<LiveSyncBatchResult, RemoteSyncError> {
        Ok(LiveSyncBatchResult::default())
    }

    async fn report_health(&self, _: &RemoteSyncHealth) -> Result<(), RemoteSyncError> {
        panic!("fixture health worker panic");
    }
}

#[derive(Default)]
struct LifecycleResolutionSink {
    process_calls: std::sync::atomic::AtomicUsize,
    results: Mutex<Vec<ReconciliationLifecycleResult>>,
}

#[derive(Default)]
struct PagedReconciliationSink {
    process_calls: std::sync::atomic::AtomicUsize,
}

#[derive(Default)]
struct BlockingSink {
    blocked_once: std::sync::atomic::AtomicBool,
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[derive(Default)]
struct TelemetryBlockingSink {
    telemetry_started: tokio::sync::Notify,
    release_telemetry: tokio::sync::Notify,
    critical_completed: tokio::sync::Notify,
    audit_completed: tokio::sync::Notify,
    telemetry_calls: std::sync::atomic::AtomicUsize,
    health_calls: std::sync::atomic::AtomicUsize,
    health_reported: tokio::sync::Notify,
}

#[async_trait]
impl RemoteBatchSink for RecordingSink {
    async fn reconcile_start(&self, start: &ReconciliationStart) -> Result<(), RemoteSyncError> {
        self.reconciliations.lock().push(start.generation.clone());
        Ok(())
    }

    async fn persist_batch(
        &self,
        batch: &LiveSyncBatch,
    ) -> Result<LiveSyncBatchResult, RemoteSyncError> {
        self.batches.lock().push(batch.clone());
        Ok(LiveSyncBatchResult {
            persisted_critical_through: batch.last_critical_sequence.unwrap_or(0),
            applied: batch.events().count(),
            ..Default::default()
        })
    }
}

#[tokio::test]
async fn unrelated_backlog_does_not_starve_history_or_startup_reconciliation() {
    let path = std::env::temp_dir().join(format!("aqe-scoped-backlog-{}.db", Uuid::new_v4()));
    let outbox = OutboxStore::open(&path).await.unwrap();
    let mut backlog = Vec::new();
    for (runtime, strategy, session) in [
        ("other-runtime", "strategy", "session"),
        ("runtime", "other-strategy", "session"),
        ("runtime", "strategy", "other-session"),
    ] {
        for revision in 1..=101 {
            let mut event = EventEnvelope::new(
                runtime,
                strategy,
                session,
                format!("old-{revision}"),
                revision,
                EventPriority::Critical,
                EventKind::InsightProjection,
                serde_json::json!({}),
            );
            event.critical_sequence = Some(revision);
            backlog.push(event);
        }
    }
    outbox.insert_events(&backlog).await.unwrap();
    let history = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "insight-history:one",
        1,
        EventPriority::Audit,
        EventKind::InsightStateHistory,
        serde_json::json!({"state":"Filled"}),
    );
    let start = ReconciliationStart {
        generation: "startup".into(),
        runtime_id: "runtime".into(),
        strategy_id: "strategy".into(),
        live_session_id: "session".into(),
        run_id: Some("new-run".into()),
        snapshot_complete: true,
        broker_features: vec![],
        critical_cutoff: Some(0),
        total_chunks: 0,
        total_records: 0,
    };
    let reconciliation = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "reconciliation:startup:start",
        1,
        EventPriority::Reconciliation,
        EventKind::ReconciliationStart,
        serde_json::to_value(start).unwrap(),
    );
    outbox
        .insert_events(&[history, reconciliation])
        .await
        .unwrap();
    assert_eq!(outbox.health().await.unwrap().pending_critical, 303);
    assert_eq!(
        outbox
            .pending_critical_for_runtime_scope("runtime", "strategy", "session")
            .await
            .unwrap(),
        0
    );
    let sink = Arc::new(RecordingSink::default());
    let agent = SyncAgent::new(
        SyncAgentConfig::new("runtime", "strategy", "session"),
        Arc::new(InProcessEventBus::default()),
        outbox.clone(),
        sink.clone(),
    );
    let (shutdown, rx) = watch::channel(false);
    let task = tokio::spawn(agent.run(rx));
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let audit = sink
                .batches
                .lock()
                .iter()
                .any(|b| b.events().any(|e| e.kind == EventKind::InsightStateHistory));
            if audit && !sink.reconciliations.lock().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        result.is_ok(),
        "unrelated critical backlog starved this strategy"
    );
    assert_eq!(outbox.health().await.unwrap().pending_critical, 303);
    let mut own = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "own-critical",
        1,
        EventPriority::Critical,
        EventKind::InsightProjection,
        serde_json::json!({}),
    );
    own.critical_sequence = Some(1);
    outbox.insert_events(&[own]).await.unwrap();
    assert_eq!(
        outbox
            .pending_critical_for_runtime_scope("runtime", "strategy", "session")
            .await
            .unwrap(),
        1
    );
    drop(outbox);
    let _ = std::fs::remove_file(path);
}

#[async_trait]
impl RemoteBatchSink for CursorConflictSink {
    async fn persist_batch(
        &self,
        _batch: &LiveSyncBatch,
    ) -> Result<LiveSyncBatchResult, RemoteSyncError> {
        Err(RemoteSyncError {
            kind: RemoteSyncErrorKind::SequenceConflict,
            message: "critical sequence conflict".to_string(),
        })
    }

    async fn fetch_remote_cursor(&self) -> Result<Option<u64>, RemoteSyncError> {
        Ok(Some(self.remote_cursor))
    }
}

#[async_trait]
impl RemoteBatchSink for SelectiveRejectSink {
    async fn persist_batch(
        &self,
        batch: &LiveSyncBatch,
    ) -> Result<LiveSyncBatchResult, RemoteSyncError> {
        if batch
            .events()
            .any(|event| event.entity_id == self.rejected_entity)
        {
            return Err(RemoteSyncError {
                kind: RemoteSyncErrorKind::InvalidBatch,
                message: "invalid singleton payload".to_string(),
            });
        }
        self.accepted_entities
            .lock()
            .extend(batch.events().map(|event| event.entity_id.clone()));
        Ok(LiveSyncBatchResult {
            persisted_critical_through: batch.last_critical_sequence.unwrap_or(0),
            applied: batch.events().count(),
            ..Default::default()
        })
    }
}

#[async_trait]
impl RemoteBatchSink for TransientFailureSink {
    async fn persist_batch(
        &self,
        _batch: &LiveSyncBatch,
    ) -> Result<LiveSyncBatchResult, RemoteSyncError> {
        Err(RemoteSyncError {
            kind: RemoteSyncErrorKind::Transient,
            message: "database temporarily unavailable".to_string(),
        })
    }
}

#[async_trait]
impl RemoteBatchSink for LifecycleResolutionSink {
    async fn persist_batch(
        &self,
        _batch: &LiveSyncBatch,
    ) -> Result<LiveSyncBatchResult, RemoteSyncError> {
        Ok(LiveSyncBatchResult::default())
    }

    async fn reconcile_complete(
        &self,
        _complete: &ReconciliationComplete,
    ) -> Result<ReconciliationProcessResult, RemoteSyncError> {
        if self
            .process_calls
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            == 0
        {
            Ok(ReconciliationProcessResult {
                lifecycle_order_ids: vec!["order-1".to_string()],
                candidate_count: 1,
                status: "processing".to_string(),
                remaining: 1,
                ..Default::default()
            })
        } else {
            Ok(ReconciliationProcessResult {
                candidate_count: 1,
                classified_count: 1,
                status: "completed".to_string(),
                ..Default::default()
            })
        }
    }

    async fn reconcile_lifecycle_result(
        &self,
        result: &ReconciliationLifecycleResult,
    ) -> Result<(), RemoteSyncError> {
        self.results.lock().push(result.clone());
        Ok(())
    }
}

#[async_trait]
impl RemoteBatchSink for PagedReconciliationSink {
    async fn persist_batch(
        &self,
        _batch: &LiveSyncBatch,
    ) -> Result<LiveSyncBatchResult, RemoteSyncError> {
        Ok(LiveSyncBatchResult::default())
    }

    async fn reconcile_complete(
        &self,
        _complete: &ReconciliationComplete,
    ) -> Result<ReconciliationProcessResult, RemoteSyncError> {
        let call = self
            .process_calls
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Ok(if call == 0 {
            ReconciliationProcessResult {
                candidate_count: 50,
                classified_count: 25,
                remaining: 1,
                status: "processing".to_string(),
                ..Default::default()
            }
        } else {
            ReconciliationProcessResult {
                candidate_count: 50,
                classified_count: 50,
                remaining: 0,
                status: "completed".to_string(),
                ..Default::default()
            }
        })
    }
}

#[async_trait]
impl RemoteBatchSink for BlockingSink {
    async fn persist_batch(
        &self,
        batch: &LiveSyncBatch,
    ) -> Result<LiveSyncBatchResult, RemoteSyncError> {
        if !self
            .blocked_once
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            self.started.notify_waiters();
            self.release.notified().await;
        }
        Ok(LiveSyncBatchResult {
            persisted_critical_through: batch.last_critical_sequence.unwrap_or(0),
            applied: batch.events().count(),
            ..Default::default()
        })
    }
}

#[async_trait]
impl RemoteBatchSink for TelemetryBlockingSink {
    async fn report_health(&self, _: &RemoteSyncHealth) -> Result<(), RemoteSyncError> {
        self.health_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.health_reported.notify_one();
        Ok(())
    }

    async fn persist_batch(
        &self,
        batch: &LiveSyncBatch,
    ) -> Result<LiveSyncBatchResult, RemoteSyncError> {
        if batch
            .events()
            .any(|event| event.priority == EventPriority::Telemetry)
        {
            self.telemetry_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.telemetry_started.notify_one();
            self.release_telemetry.notified().await;
        } else if !batch.critical_events.is_empty() {
            self.critical_completed.notify_one();
        } else {
            self.audit_completed.notify_one();
        }
        Ok(LiveSyncBatchResult {
            persisted_critical_through: batch.last_critical_sequence.unwrap_or(0),
            applied: batch.events().count(),
            ..Default::default()
        })
    }
}

#[test]
fn retry_schedule_matches_contract() {
    assert_eq!(retry_delay_seconds(1), 1);
    assert_eq!(retry_delay_seconds(2), 2);
    assert_eq!(retry_delay_seconds(3), 5);
    assert_eq!(retry_delay_seconds(4), 15);
    assert_eq!(retry_delay_seconds(7), 300);
    assert_eq!(retry_delay_seconds(99), 300);

    assert_eq!(reconciliation_retry_delay_seconds(1), 1);
    assert_eq!(reconciliation_retry_delay_seconds(4), 10);
    assert_eq!(reconciliation_retry_delay_seconds(6), 30);
    assert_eq!(reconciliation_retry_delay_seconds(99), 30);
}

#[test]
fn derives_database_websocket_endpoint_from_batch_api() {
    assert_eq!(
        database_websocket_url("https://db.example.com/api/aqs/aqs/aqs/live/v1/batch").unwrap(),
        "wss://db.example.com"
    );
    assert_eq!(
        database_websocket_url("http://127.0.0.1:8000/api/aqs/aqs/aqs/live/v1/batch").unwrap(),
        "ws://127.0.0.1:8000"
    );
    assert!(database_websocket_url("file:///tmp/surreal").is_err());
}

#[test]
fn gateway_sink_retains_operational_health_and_cursor_reads() {
    let sink = HttpBatchSink::new(
        http_test_config("strategy:one"),
        "https://events.example.com",
    )
    .unwrap();
    assert_eq!(
        sink.health_url(),
        "https://db.example.com/api/aqs/aqs/aqs/live/v1/health"
    );
    assert_eq!(
        sink.cursor_url(),
        "https://db.example.com/api/aqs/aqs/aqs/live/v1/cursor/session"
    );
}

#[test]
fn decodes_action_and_strategy_record_ids_in_both_wire_shapes() {
    let action = RemoteStrategyAction {
        id: serde_json::json!({"tb": "strategy_actions", "id": "action-1"}),
        action: serde_json::json!("stop"),
        status: serde_json::json!("pending"),
        strategy_id: Some(serde_json::json!("strategy:strategy-1")),
        runtime_id: Some("runtime".to_string()),
        run_id: Some("run".to_string()),
        requested_at: None,
    };
    assert_eq!(
        action.record_id().as_deref(),
        Some("strategy_actions:action-1")
    );
    assert_eq!(
        action.strategy_record_id().as_deref(),
        Some("strategy:strategy-1")
    );
}

#[tokio::test]
async fn reconciliation_requests_missing_lifecycle_evidence_from_the_local_broker() {
    let path = std::env::temp_dir().join(format!("aqe-sync-lifecycle-{}.db", Uuid::new_v4()));
    let outbox = OutboxStore::open(&path).await.unwrap();
    let bus = Arc::new(InProcessEventBus::default());
    let request_subject = EventSubject::reconciliation_lifecycle_request("runtime", "strategy");
    let mut requests = bus.subscribe(request_subject).await.unwrap();
    let responder_bus = bus.clone();
    tokio::spawn(async move {
        let delivery = requests.recv().await.unwrap();
        let request = serde_json::from_value::<ReconciliationLifecycleRequest>(
            delivery.event.payload.clone(),
        )
        .unwrap();
        let result = ReconciliationLifecycleResult {
            generation: request.generation,
            request_id: request.request_id,
            results: vec![ReconciliationRecord {
                record_type: "terminal_order".to_string(),
                entity_id: "order-1".to_string(),
                payload: serde_json::json!({"orderId": "order-1", "status": "Closed"}),
            }],
        };
        let response = EventEnvelope::new(
            "runtime",
            "strategy",
            "session",
            "lifecycle-result",
            2,
            EventPriority::Reconciliation,
            EventKind::ReconciliationLifecycleResult,
            serde_json::to_value(result).unwrap(),
        );
        responder_bus
            .respond_event(&delivery, response)
            .await
            .unwrap();
    });
    let sink = Arc::new(LifecycleResolutionSink::default());
    let agent = SyncAgent::new(
        SyncAgentConfig::new("runtime", "strategy", "session"),
        bus,
        outbox,
        sink.clone(),
    );
    let complete = ReconciliationComplete {
        generation: "generation-1".to_string(),
        total_chunks: 1,
        total_records: 1,
        snapshot_complete: true,
    };
    let event = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "reconciliation:generation-1:complete",
        1,
        EventPriority::Reconciliation,
        EventKind::ReconciliationComplete,
        serde_json::to_value(complete).unwrap(),
    );

    agent.send_reconciliation_event(&event).await.unwrap();

    let results = sink.results.lock();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].results[0].entity_id, "order-1");
    drop(results);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn reconciliation_processes_only_one_remote_page_per_pacing_interval() {
    let path = std::env::temp_dir().join(format!("aqe-sync-paged-{}.db", Uuid::new_v4()));
    let outbox = OutboxStore::open(&path).await.unwrap();
    let sink = Arc::new(PagedReconciliationSink::default());
    let agent = SyncAgent::new(
        SyncAgentConfig::new("runtime", "strategy", "session"),
        Arc::new(InProcessEventBus::default()),
        outbox,
        sink.clone(),
    );
    let complete = ReconciliationComplete {
        generation: "generation-paged".to_string(),
        total_chunks: 0,
        total_records: 0,
        snapshot_complete: false,
    };
    let event = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "reconciliation:generation-paged:complete",
        1,
        EventPriority::Reconciliation,
        EventKind::ReconciliationComplete,
        serde_json::to_value(complete).unwrap(),
    );

    let started = tokio::time::Instant::now();
    agent.send_reconciliation_event(&event).await.unwrap();

    assert_eq!(
        sink.process_calls
            .load(std::sync::atomic::Ordering::Acquire),
        2
    );
    assert!(started.elapsed() >= Duration::from_millis(90));
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn critical_ack_follows_sqlite_commit_and_remote_flush() {
    let path = std::env::temp_dir().join(format!("aqe-sync-agent-{}.db", Uuid::new_v4()));
    let outbox = OutboxStore::open(&path).await.unwrap();
    let bus = Arc::new(InProcessEventBus::acknowledged());
    let sink = Arc::new(RecordingSink::default());
    let config = SyncAgentConfig {
        flush_interval: Duration::from_millis(5),
        ..SyncAgentConfig::new("runtime", "strategy", "session")
    };
    let agent = SyncAgent::new(config, bus.clone(), outbox.clone(), sink.clone());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(agent.run(shutdown_rx));
    tokio::task::yield_now().await;

    let mut event = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "insight:one",
        1,
        EventPriority::Critical,
        EventKind::InsightProjection,
        serde_json::json!({"state": "filled"}),
    );
    event.critical_sequence = Some(1);
    let handle = bus
        .publish_critical(
            EventSubject::for_priority("runtime", "strategy", EventPriority::Critical),
            event,
        )
        .await
        .unwrap();
    handle.wait().await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if !sink.batches.lock().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(outbox.health().await.unwrap().pending_critical, 0);
    let _ = shutdown_tx.send(true);
    task.await.unwrap().unwrap();
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn restarted_runtime_drains_contiguous_run_scoped_batches() {
    let path = std::env::temp_dir().join(format!("aqe-sync-runs-{}.db", Uuid::new_v4()));
    let outbox = OutboxStore::open(&path).await.unwrap();
    let sink = Arc::new(RecordingSink::default());
    let agent = SyncAgent::new(
        SyncAgentConfig::new("runtime", "strategy", "session"),
        Arc::new(InProcessEventBus::acknowledged()),
        outbox.clone(),
        sink.clone(),
    );
    let events = ["old-run", "old-run", "new-run"]
        .into_iter()
        .enumerate()
        .map(|(index, run)| {
            let sequence = index as u64 + 1;
            let mut event = EventEnvelope::new(
                "runtime",
                "strategy",
                "session",
                format!("insight:{sequence}"),
                sequence,
                EventPriority::Critical,
                EventKind::InsightProjection,
                serde_json::json!({"state":"New"}),
            );
            event.run_id = Some(run.into());
            event.critical_sequence = Some(sequence);
            event
        })
        .collect::<Vec<_>>();
    outbox.insert_events(&events).await.unwrap();
    agent
        .flush_priority(EventPriority::Critical, 32, 64 * 1024)
        .await
        .unwrap();
    assert_eq!(outbox.health().await.unwrap().pending_critical, 1);
    agent
        .flush_priority(EventPriority::Critical, 32, 64 * 1024)
        .await
        .unwrap();
    assert_eq!(outbox.health().await.unwrap().pending_critical, 0);
    {
        let batches = sink.batches.lock();
        assert_eq!(batches.len(), 2);
        assert_eq!(
            (
                batches[0].first_critical_sequence,
                batches[0].last_critical_sequence
            ),
            (Some(1), Some(2))
        );
        assert_eq!(
            (
                batches[1].first_critical_sequence,
                batches[1].last_critical_sequence
            ),
            (Some(3), Some(3))
        );
        assert!(
            batches[0]
                .events()
                .all(|event| event.run_id.as_deref() == Some("old-run"))
        );
        assert!(
            batches[1]
                .events()
                .all(|event| event.run_id.as_deref() == Some("new-run"))
        );
    }
    drop(agent);
    drop(outbox);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn remote_persistence_cannot_block_local_critical_acknowledgements() {
    let path = std::env::temp_dir().join(format!("aqe-sync-blocked-{}.db", Uuid::new_v4()));
    let outbox = OutboxStore::open(&path).await.unwrap();
    let bus = Arc::new(InProcessEventBus::acknowledged());
    let sink = Arc::new(BlockingSink::default());
    let config = SyncAgentConfig {
        flush_interval: Duration::from_millis(5),
        ..SyncAgentConfig::new("runtime", "strategy", "session")
    };
    let agent = SyncAgent::new(config, bus.clone(), outbox, sink.clone());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(agent.run(shutdown_rx));
    tokio::task::yield_now().await;

    let subject = EventSubject::for_priority("runtime", "strategy", EventPriority::Critical);
    let mut first = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "insight:one",
        1,
        EventPriority::Critical,
        EventKind::InsightProjection,
        serde_json::json!({"state": "new"}),
    );
    first.critical_sequence = Some(1);
    let remote_started = sink.started.notified();
    bus.publish_critical(subject.clone(), first)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), remote_started)
        .await
        .unwrap();

    let mut second = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "insight:two",
        1,
        EventPriority::Critical,
        EventKind::InsightProjection,
        serde_json::json!({"state": "executed"}),
    );
    second.critical_sequence = Some(2);
    let handle = bus.publish_critical(subject, second).await.unwrap();
    tokio::time::timeout(Duration::from_millis(200), handle.wait())
        .await
        .expect("local acknowledgement must not wait for the remote sink")
        .unwrap();

    sink.release.notify_waiters();
    let _ = shutdown_tx.send(true);
    task.await.unwrap().unwrap();
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn cancelling_agent_releases_workers_and_retains_uncommitted_events() {
    let path = std::env::temp_dir().join(format!("aqe-sync-cancel-{}.db", Uuid::new_v4()));
    let outbox = OutboxStore::open(&path).await.unwrap();
    let bus = Arc::new(InProcessEventBus::acknowledged());
    let sink = Arc::new(BlockingSink::default());
    let agent = SyncAgent::new(
        SyncAgentConfig::new("runtime", "strategy", "session"),
        bus.clone(),
        outbox.clone(),
        sink.clone(),
    );
    let (_shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(agent.run(receiver));
    tokio::task::yield_now().await;
    let mut event = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "insight",
        1,
        EventPriority::Critical,
        EventKind::InsightProjection,
        serde_json::json!({"state":"New"}),
    );
    event.critical_sequence = Some(1);
    let started = sink.started.notified();
    bus.publish_critical(
        EventSubject::for_priority("runtime", "strategy", EventPriority::Critical),
        event,
    )
    .await
    .unwrap()
    .wait()
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), started)
        .await
        .unwrap();
    assert_eq!(outbox.health().await.unwrap().pending_critical, 1);
    // Simulate startup rollback/cancellation while the remote write is stalled.
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(1), async {
        while Arc::strong_count(&sink) != 1 || Arc::strong_count(&bus) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled sync agent retained detached workers");
    assert_eq!(outbox.health().await.unwrap().pending_critical, 1);
    drop(outbox);
    let reopened = OutboxStore::open(&path).await.unwrap();
    assert_eq!(reopened.health().await.unwrap().pending_critical, 1);
    drop(reopened);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn normal_shutdown_joins_workers_and_pre_stopped_agent_does_no_work() {
    let path = std::env::temp_dir().join(format!("aqe-sync-owned-stop-{}.db", Uuid::new_v4()));
    let outbox = OutboxStore::open(&path).await.unwrap();
    let bus = Arc::new(InProcessEventBus::acknowledged());
    let sink = Arc::new(RecordingSink::default());
    for already_stopped in [true, false] {
        let agent = SyncAgent::new(
            SyncAgentConfig::new("runtime", "strategy", "session"),
            bus.clone(),
            outbox.clone(),
            sink.clone(),
        );
        let (shutdown, receiver) = watch::channel(already_stopped);
        let task = tokio::spawn(agent.run(receiver));
        if !already_stopped {
            tokio::task::yield_now().await;
            shutdown.send_replace(true);
        }
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        // No yield/poll loop: successful run completion is the cleanup barrier.
        assert_eq!(Arc::strong_count(&sink), 1);
        assert_eq!(Arc::strong_count(&bus), 1);
        assert!(sink.batches.lock().is_empty());
    }
    drop(outbox);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn ingestion_error_joins_sibling_workers_before_returning() {
    let path = std::env::temp_dir().join(format!("aqe-sync-owned-error-{}.db", Uuid::new_v4()));
    let outbox = OutboxStore::open(&path).await.unwrap();
    let bus = Arc::new(InProcessEventBus::acknowledged());
    let sink = Arc::new(RecordingSink::default());
    let agent = SyncAgent::new(
        SyncAgentConfig::new("runtime", "strategy", "session"),
        bus.clone(),
        outbox.clone(),
        sink.clone(),
    );
    let (_shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(agent.run(receiver));
    tokio::task::yield_now().await;
    let invalid = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "bad-reconciliation",
        1,
        EventPriority::Reconciliation,
        EventKind::ReconciliationStart,
        serde_json::json!({}),
    );
    let delivery = bus
        .publish_critical(
            EventSubject::for_priority("runtime", "strategy", EventPriority::Reconciliation),
            invalid,
        )
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(SyncAgentError::InvalidEvent(_))));
    assert_eq!(Arc::strong_count(&sink), 1);
    assert_eq!(Arc::strong_count(&bus), 1);
    drop(delivery);
    drop(outbox);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn independent_log_subject_is_not_persisted() {
    let path = std::env::temp_dir().join(format!("aqe-sync-log-{}.db", Uuid::new_v4()));
    let outbox = OutboxStore::open(&path).await.unwrap();
    let bus = Arc::new(InProcessEventBus::default());
    let sink = Arc::new(RecordingSink::default());
    let config = SyncAgentConfig {
        telemetry_flush_interval: Duration::from_millis(5),
        ..SyncAgentConfig::new("runtime", "strategy", "session")
    };
    let agent = SyncAgent::new(config, bus.clone(), outbox, sink.clone());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(agent.run(shutdown_rx));
    tokio::task::yield_now().await;

    let event = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "runtime-log:stdout:0",
        1,
        EventPriority::Telemetry,
        EventKind::Log,
        serde_json::json!({"stream": "stdout", "line": "strategy started"}),
    );
    bus.publish_telemetry(EventSubject::logs("runtime", "strategy"), event)
        .unwrap();
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(sink.batches.lock().iter().all(|batch| {
        batch
            .telemetry
            .iter()
            .all(|event| event.kind != EventKind::Log)
    }));

    let _ = shutdown_tx.send(true);
    task.await.unwrap().unwrap();
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn blocked_telemetry_does_not_block_critical_drain() {
    let path = std::env::temp_dir().join(format!("aqe-sync-priority-{}.db", Uuid::new_v4()));
    let outbox = OutboxStore::open(&path).await.unwrap();
    let telemetry = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "account",
        1,
        EventPriority::Telemetry,
        EventKind::AccountProjection,
        serde_json::json!({"equity": 100_000}),
    );
    outbox.insert_telemetry_latest(&telemetry).await.unwrap();

    let sink = Arc::new(TelemetryBlockingSink::default());
    let config = SyncAgentConfig {
        flush_interval: Duration::from_millis(5),
        telemetry_flush_interval: Duration::from_millis(1),
        health_interval: Duration::from_secs(60),
        ..SyncAgentConfig::new("runtime", "strategy", "session")
    };
    let agent = SyncAgent::new(
        config,
        Arc::new(InProcessEventBus::default()),
        outbox.clone(),
        sink.clone(),
    );
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(agent.run(shutdown_rx));
    tokio::time::timeout(Duration::from_secs(1), sink.telemetry_started.notified())
        .await
        .expect("telemetry request should start");

    let mut critical = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "insight:one",
        1,
        EventPriority::Critical,
        EventKind::InsightProjection,
        serde_json::json!({"state": "filled"}),
    );
    critical.critical_sequence = Some(1);
    outbox.insert_events(&[critical]).await.unwrap();
    tokio::time::timeout(
        Duration::from_millis(250),
        sink.critical_completed.notified(),
    )
    .await
    .expect("critical persistence must run independently of blocked telemetry");

    sink.release_telemetry.notify_waiters();
    let _ = shutdown_tx.send(true);
    task.await.unwrap().unwrap();
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn stalled_telemetry_keeps_health_and_audit_live_without_spawning_more_requests() {
    use std::sync::atomic::Ordering;
    let path = std::env::temp_dir().join(format!("aqe-independent-lanes-{}.db", Uuid::new_v4()));
    let outbox = OutboxStore::open(&path).await.unwrap();
    let telemetry = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "account",
        1,
        EventPriority::Telemetry,
        EventKind::AccountProjection,
        serde_json::json!({"equity": 100_000}),
    );
    outbox.insert_telemetry_latest(&telemetry).await.unwrap();
    let sink = Arc::new(TelemetryBlockingSink::default());
    let config = SyncAgentConfig {
        telemetry_flush_interval: Duration::from_millis(2),
        health_interval: Duration::from_millis(10),
        ..SyncAgentConfig::new("runtime", "strategy", "session")
    };
    let agent = SyncAgent::new(
        config,
        Arc::new(InProcessEventBus::default()),
        outbox.clone(),
        sink.clone(),
    );
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(agent.run(receiver));
    tokio::time::timeout(Duration::from_secs(1), sink.telemetry_started.notified())
        .await
        .unwrap();
    let target_health_count = sink.health_calls.load(Ordering::SeqCst) + 3;
    let audit = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "history",
        1,
        EventPriority::Audit,
        EventKind::InsightStateHistory,
        serde_json::json!({"state":"New"}),
    );
    outbox.insert_events(&[audit]).await.unwrap();
    tokio::time::timeout(Duration::from_millis(500), sink.audit_completed.notified())
        .await
        .expect("audit/history must not await telemetry");
    tokio::time::timeout(Duration::from_millis(500), async {
        while sink.health_calls.load(Ordering::SeqCst) < target_health_count {
            sink.health_reported.notified().await;
        }
    })
    .await
    .expect("health must continue while telemetry is pending");
    assert_eq!(
        sink.telemetry_calls.load(Ordering::SeqCst),
        1,
        "missed ticks must not start concurrent telemetry requests"
    );
    assert_eq!(outbox.health().await.unwrap().pending_telemetry, 1);
    // No release: bounded final flush must retain the event rather than hang.
    shutdown.send_replace(true);
    tokio::time::timeout(Duration::from_secs(4), task)
        .await
        .expect("shutdown flush was unbounded")
        .unwrap()
        .unwrap();
    assert_eq!(outbox.health().await.unwrap().pending_telemetry, 1);
    assert_eq!(Arc::strong_count(&sink), 1);
    drop(outbox);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn unexpected_periodic_worker_exit_stops_siblings_and_is_reported() {
    let path = std::env::temp_dir().join(format!("aqe-worker-failure-{}.db", Uuid::new_v4()));
    let outbox = OutboxStore::open(&path).await.unwrap();
    let sink = Arc::new(PanickingHealthSink);
    let bus = Arc::new(InProcessEventBus::default());
    let agent = SyncAgent::new(
        SyncAgentConfig::new("runtime", "strategy", "session"),
        bus.clone(),
        outbox.clone(),
        sink.clone(),
    );
    let (_shutdown, receiver) = watch::channel(false);
    let result = tokio::time::timeout(Duration::from_secs(2), agent.run(receiver))
        .await
        .unwrap();
    assert!(matches!(result, Err(SyncAgentError::WorkerStopped)));
    assert_eq!(Arc::strong_count(&sink), 1);
    assert_eq!(Arc::strong_count(&bus), 1);
    drop(outbox);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sequence_conflict_commits_only_events_covered_by_the_remote_cursor() {
    let path = std::env::temp_dir().join(format!("aqe-sync-cursor-{}.db", Uuid::new_v4()));
    let outbox = OutboxStore::open(&path).await.unwrap();
    let bus = Arc::new(InProcessEventBus::default());
    let sink = Arc::new(CursorConflictSink { remote_cursor: 1 });
    let agent = SyncAgent::new(
        SyncAgentConfig::new("runtime", "strategy", "session"),
        bus,
        outbox.clone(),
        sink,
    );

    let mut first = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "insight:one",
        1,
        EventPriority::Critical,
        EventKind::InsightProjection,
        serde_json::json!({"state": "executed"}),
    );
    first.critical_sequence = Some(1);
    let mut second = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "insight:two",
        1,
        EventPriority::Critical,
        EventKind::InsightProjection,
        serde_json::json!({"state": "filled"}),
    );
    second.critical_sequence = Some(2);
    outbox
        .insert_events(&[first.clone(), second.clone()])
        .await
        .unwrap();
    let records = outbox
        .pending_batch_for_scope(
            EventPriority::Critical,
            "strategy",
            "session",
            32,
            64 * 1024,
        )
        .await
        .unwrap();

    agent.persist_with_bisection(records).await.unwrap();

    let health = outbox.health().await.unwrap();
    assert_eq!(health.pending_critical, 1);
    assert_eq!(
        outbox
            .critical_sequence_cursor("strategy", "session")
            .await
            .unwrap(),
        2,
        "the local reservation remains at two while remote durability is at one"
    );
    drop(outbox);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn invalid_batch_is_bisected_and_only_the_bad_singleton_enters_dlq() {
    let path = std::env::temp_dir().join(format!("aqe-sync-dlq-{}.db", Uuid::new_v4()));
    let outbox = OutboxStore::open(&path).await.unwrap();
    let sink = Arc::new(SelectiveRejectSink {
        rejected_entity: "insight:bad",
        accepted_entities: Mutex::new(Vec::new()),
    });
    let agent = SyncAgent::new(
        SyncAgentConfig::new("runtime", "strategy", "session"),
        Arc::new(InProcessEventBus::default()),
        outbox.clone(),
        sink.clone(),
    );
    let events = ["insight:one", "insight:bad", "insight:two"]
        .into_iter()
        .enumerate()
        .map(|(index, entity_id)| {
            EventEnvelope::new(
                "runtime",
                "strategy",
                "session",
                entity_id,
                (index + 1) as u64,
                EventPriority::Telemetry,
                EventKind::InsightProjection,
                serde_json::json!({"state": "filled"}),
            )
        })
        .collect::<Vec<_>>();
    outbox.insert_events(&events).await.unwrap();
    let records = outbox
        .pending_batch_for_scope(
            EventPriority::Telemetry,
            "strategy",
            "session",
            100,
            128 * 1024,
        )
        .await
        .unwrap();

    agent.persist_with_bisection(records).await.unwrap();

    let health = outbox.health().await.unwrap();
    assert_eq!(health.pending_telemetry, 0);
    assert_eq!(health.dead_letters, 1);
    let mut accepted = sink.accepted_entities.lock().clone();
    accepted.sort();
    assert_eq!(accepted, vec!["insight:one", "insight:two"]);
    drop(outbox);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn transient_remote_failure_retains_event_and_schedules_backoff() {
    let path = std::env::temp_dir().join(format!("aqe-sync-retry-{}.db", Uuid::new_v4()));
    let outbox = OutboxStore::open(&path).await.unwrap();
    let agent = SyncAgent::new(
        SyncAgentConfig::new("runtime", "strategy", "session"),
        Arc::new(InProcessEventBus::default()),
        outbox.clone(),
        Arc::new(TransientFailureSink),
    );
    let event = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "account:one",
        1,
        EventPriority::Telemetry,
        EventKind::AccountProjection,
        serde_json::json!({"equity": 100_000}),
    );
    outbox.insert_events(&[event]).await.unwrap();
    let records = outbox
        .pending_batch_for_scope(
            EventPriority::Telemetry,
            "strategy",
            "session",
            100,
            128 * 1024,
        )
        .await
        .unwrap();

    agent.persist_with_bisection(records).await.unwrap();

    let health = outbox.health().await.unwrap();
    assert_eq!(health.pending_telemetry, 1);
    assert!(health.current_retry_delay_ms > 0);
    assert_eq!(
        health.last_error.as_deref(),
        Some("database temporarily unavailable")
    );
    drop(outbox);
    let _ = std::fs::remove_file(path);
}
