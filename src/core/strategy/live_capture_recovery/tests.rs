use super::*;
use crate::core::{
    insight::Insight,
    live_sync::{
        EventBusError, EventDelivery, EventSubscription, InProcessEventBus, LocalDeliveryHandle,
        LocalEventBus, LocalEventPublisher, LocalEventPublisherConfig, OutboxStore,
        SyncAcknowledgement,
    },
};
use async_trait::async_trait;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[derive(Default)]
struct FixtureBus {
    local: InProcessEventBus,
    online: AtomicBool,
    events: parking_lot::Mutex<Vec<CloudEvent>>,
}
#[async_trait]
impl LocalEventBus for FixtureBus {
    async fn publish_critical(
        &self,
        subject: EventSubject,
        event: CloudEvent,
    ) -> Result<LocalDeliveryHandle, EventBusError> {
        if !self.online.load(Ordering::Acquire) {
            return Err(EventBusError::Publish("isolated offline fixture".into()));
        }
        self.events.lock().push(event.clone());
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
async fn fixture() -> (Arc<FixtureBus>, OutboxStore, LocalBatchSyncState, AqsAuth) {
    fixture_at(None).await
}
async fn fixture_at(path: Option<std::path::PathBuf>) -> (Arc<FixtureBus>, OutboxStore, LocalBatchSyncState, AqsAuth) {
    let bus = Arc::new(FixtureBus::default());
    let store = OutboxStore::open(
        path.unwrap_or_else(||std::env::temp_dir().join(format!("owner-capture-recovery-{}.db", Uuid::new_v4()))),
    )
    .await
    .unwrap();
    let publisher = LocalEventPublisher::spawn(
        bus.clone(),
        Some(store.clone()),
        LocalEventPublisherConfig {
            capture_limit_bytes: Some(2 * 1024 * 1024),
            critical_batch_delay: Duration::from_millis(1),
            ..Default::default()
        },
    );
    assert!(
        publisher
            .wait_for_capture_initialization(Duration::from_secs(2))
            .await
    );
    let auth = AqsAuth {
        access_method: "aqe_live".into(),
        session_id: "session".into(),
        session_secret: "fixture".into(),
        strategy_id: "strategy".into(),
        user_id: "users:fixture".into(),
        node_id: None,
        live_session_id: None,
        url: None,
    };
    let mut sync = LocalBatchSyncState::new(publisher, &auth);
    sync.runtime_id = "runtime".into();
    sync.run_id = "run".into();
    for n in 0..12 {
        let event = sync.event(
            format!("filler:{n}"),
            EventPriority::Critical,
            EventKind::InsightProjection,
            serde_json::json!({"data":"x".repeat(180_000)}),
            None,
        );
        let receipt = sync
            .publisher
            .queue_durable(
                EventSubject::for_priority("runtime", "strategy", EventPriority::Critical),
                event,
            )
            .unwrap();
        let result = receipt.await.unwrap();
        if n < 11 {
            assert!(result.is_ok());
        } else {
            assert!(matches!(result, Err(EventBusError::CloudCapturePaused)));
        }
    }
    bus.online.store(true, Ordering::Release);
    let deadline = Instant::now() + Duration::from_secs(5);
    while sync.publisher.cloud_capture_status().state != CloudCaptureState::ReconciliationRequired {
        store.prune_committed(0).await.unwrap();
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    (bus, store, sync, auth)
}

#[tokio::test]
async fn owner_capture_recovery_replays_bounded_current_state_and_seals_the_gap_before_resume() {
    let (bus, store, mut sync, auth) = fixture().await;
    let mut insights = InsightCollection::new();
    for n in 1..=100 {
        let mut row = Insight::default();
        row.insight_id = Uuid::from_u128(n);
        if n % 10 == 0 {
            row.state = crate::core::insight::types::InsightState::Closed;
        }
        row.state_log(Some("history must not be claimed as exported".into()));
        insights.add_insight(row);
    }
    let mut recovery = CaptureRecovery::default();
    let deadline = Instant::now() + Duration::from_secs(10);
    while sync.publisher.cloud_capture_paused() || recovery.needs_poll(&sync.publisher) {
        recovery.poll(&mut sync, &insights, &auth, || {
            vec!["order_lifecycle_snapshot".into()]
        },None);
        store.prune_committed(0).await.unwrap();
        assert!(
            Instant::now() < deadline,
            "error={:?}, replayed={}, capture={:?}",
            recovery.health.last_error,
            recovery.health.replayed_insights,
            sync.publisher.cloud_capture_status()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(!sync.publisher.admission_failed());
    assert_eq!(recovery.health.replayed_insights, 100);
    assert!(
        insights.has_dirty_insights(),
        "state replay is not a full history export receipt"
    );
    assert!(
        !insights
            .get(&Uuid::from_u128(1))
            .unwrap()
            .state_history
            .is_empty()
    );
    let events = bus.events.lock();
    let current = events
        .iter()
        .filter(|e| e.kind == EventKind::InsightProjection && e.payload.get("insight_id").is_some())
        .collect::<Vec<_>>();
    assert_eq!(current.len(), 100);
    let starts = events
        .iter()
        .filter(|e| e.kind == EventKind::ReconciliationStart)
        .collect::<Vec<_>>();
    assert_eq!(starts.len(), 1);
    let start: ReconciliationStart = serde_json::from_value(starts[0].payload.clone()).unwrap();
    assert!(start.valid_purpose());
    assert!(!start.snapshot_complete);
    assert!(current.iter().all(|event| event.critical_sequence.unwrap()
        > start.critical_cutoff.unwrap()
        && event.critical_sequence <= start.critical_replay_through));
    assert_eq!(
        events
            .iter()
            .filter(|event| event.payload["title"] == "Cloud history gap")
            .count(),
        1
    );
    let gap = events
        .iter()
        .find(|event| event.payload["title"] == "Cloud history gap")
        .unwrap();
    assert_eq!(gap.payload["payload"]["history_complete"], false);
    assert_eq!(gap.payload["payload"]["accepted_evidence_retained"], true);
    assert!(gap.critical_sequence > start.critical_replay_through);
    assert_eq!(
        events
            .iter()
            .map(|e| e.event_id)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        events.len()
    );
    drop(events);
    sync.publisher.shutdown(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn leased_cross_generation_restart_replays_current_state_and_preserves_gap_provenance() {
    let directory=std::env::temp_dir().join(format!("restart-capture-recovery-{}",Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let path=directory.join("publisher.db");
    let (bus,store,old_sync,auth)=fixture_at(Some(path.clone())).await;
    let old_pause=old_sync.publisher.cloud_capture_status().pause.unwrap();
    old_sync.publisher.shutdown(Duration::from_secs(2)).await;
    drop(old_sync);
    let lease=Arc::new(crate::core::live_sync::CaptureOwnerLease::acquire(
        &path,"new-runtime","strategy","session","new-run").unwrap());
    let publisher=LocalEventPublisher::spawn_owned_with_capture_lease(
        bus.clone(),store.clone(),LocalEventPublisherConfig {
            capture_limit_bytes:Some(2*1024*1024),critical_batch_delay:Duration::from_millis(1),..Default::default()
        },0,Arc::new(()),lease,
    );
    assert!(publisher.wait_for_capture_initialization(Duration::from_secs(2)).await);
    let adopted=publisher.cloud_capture_status().pause.unwrap();
    assert_eq!(adopted.runtime_id,"new-runtime");
    assert_eq!(adopted.owner_run_id.as_deref(),Some("new-run"));
    assert_eq!(adopted.origin_runtime_id.as_deref(),Some("runtime"));
    assert_eq!(adopted.origin_run_id.as_deref(),Some("run"));
    assert_eq!(adopted.started_at_us,old_pause.started_at_us);
    assert!(adopted.generation>old_pause.generation);
    assert!(!store.complete_capture_reconciliation("runtime","strategy","session",old_pause.generation).await.unwrap());
    let mut sync=LocalBatchSyncState::new(publisher,&auth);
    sync.runtime_id="new-runtime".into();sync.run_id="new-run".into();
    let mut insights=InsightCollection::new();
    for _ in 0..5 { insights.add_insight(Insight::default()); }
    let mut recovery=CaptureRecovery::default();
    let deadline=Instant::now()+Duration::from_secs(5);
    while recovery.needs_poll(&sync.publisher) {
        recovery.poll(&mut sync,&insights,&auth,Vec::new,None);
        store.prune_committed(0).await.unwrap();
        assert!(Instant::now()<deadline,"{:?}",recovery.health.last_error);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(recovery.health.replayed_insights,5);
    let events=bus.events.lock();
    let gap=events.iter().find(|e|e.payload["title"]=="Cloud history gap").unwrap();
    assert_eq!(gap.run_id.as_deref(),Some("new-run"));
    assert_eq!(gap.payload["payload"]["version"],2);
    assert_eq!(gap.payload["payload"]["origin_runtime_id"],"runtime");
    assert_eq!(gap.payload["payload"]["origin_run_id"],"run");
    assert_eq!(gap.payload["payload"]["last_blocked_run_id"],"run");
    assert_eq!(gap.payload["payload"]["history_complete"],false);
    assert_eq!(gap.payload["payload"]["ownership_handoffs"],1);
    drop(events);
    sync.publisher.shutdown(Duration::from_secs(1)).await;
    drop(recovery);drop(sync);drop(store);drop(bus);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn owner_capture_recovery_never_clears_another_scope_or_drops_oversized_state() {
    let (_bus, store, mut sync, auth) = fixture().await;
    let mut insights = InsightCollection::new();
    let mut recovery = CaptureRecovery::default();
    sync.runtime_id = "other-runtime".into();
    recovery.poll(&mut sync, &insights, &auth, Vec::new,None);
    assert!(
        recovery
            .health
            .last_error
            .as_deref()
            .unwrap()
            .contains("another source scope")
    );
    assert!(sync.publisher.cloud_capture_paused());
    sync.runtime_id = "runtime".into();
    let mut row = Insight::default();
    row.insight_id = Uuid::from_u128(1);
    row.symbol = "x".repeat(PAGE_BYTES + 1);
    insights.add_insight(row);
    recovery.poll(&mut sync, &insights, &auth, Vec::new,None);
    assert!(
        recovery
            .health
            .last_error
            .as_deref()
            .unwrap()
            .contains("exceeds")
    );
    assert!(sync.publisher.cloud_capture_paused());
    assert!(!sync.publisher.admission_failed());
    assert!(insights.has_dirty_insights());
    assert!(recovery.work.is_empty());
    store.prune_committed(0).await.unwrap();
    sync.publisher.shutdown(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn owner_capture_recovery_cancelled_after_seal_uses_a_new_generation_not_changed_content() {
    let (bus, store, mut sync, auth) = fixture().await;
    let mut insights = InsightCollection::new();
    insights.add_insight(Insight::default());
    let mut first = CaptureRecovery::default();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if bus
            .events
            .lock()
            .iter()
            .any(|event| event.kind == EventKind::ReconciliationComplete)
        {
            break;
        }
        first.poll(&mut sync, &insights, &auth, Vec::new,None);
        store.prune_committed(0).await.unwrap();
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(sync.publisher.cloud_capture_paused());
    let old = first.health.generation.clone().unwrap();
    drop(first);
    let mut replacement = CaptureRecovery::default();
    while replacement.needs_poll(&sync.publisher) {
        replacement.poll(&mut sync, &insights, &auth, Vec::new,None);
        store.prune_committed(0).await.unwrap();
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_ne!(replacement.health.generation.as_deref(), Some(old.as_str()));
    let events = bus.events.lock();
    let generations = events
        .iter()
        .filter(|event| event.kind == EventKind::ReconciliationStart)
        .map(|event| event.payload["generation"].as_str().unwrap().to_owned())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(generations.len(), 2);
    assert!(generations.contains(&old));
    assert!(!sync.publisher.admission_failed());
    drop(events);
    sync.publisher.shutdown(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn sealed_owner_replay_coordinates_the_running_journal_without_broker_mutations() {
    use crate::core::{
        broker::{command_journal::{BrokerCommandJournal,ExecutionFillBatch},command_worker::BrokerCommandWorker,
            commands::BrokerCommandError,paper_broker::PaperBroker,traits::OrderManagementProvider,types::AccountType},
        live_sync::{CaptureOwnerLease,PhysicalCaptureLimit,MIN_OPERATIONAL_HEADROOM_BYTES},
    };
    let directory=std::env::temp_dir().join(format!("joint-owner-recovery-{}",Uuid::new_v4()));std::fs::create_dir_all(&directory).unwrap();
    let policy=PhysicalCaptureLimit{physical_limit_bytes:32*1024*1024,operational_headroom_bytes:MIN_OPERATIONAL_HEADROOM_BYTES};
    let mut journal=BrokerCommandJournal::open(directory.join("journal.db"),"strategy","account").await.unwrap()
        .with_analytics_capture_budget(PhysicalCaptureLimit{physical_limit_bytes:MIN_OPERATIONAL_HEADROOM_BYTES+1,..policy}).await.unwrap();
    let input=serde_json::from_value(serde_json::json!({"insight_id":null,"fill":{
        "version":1,"provider":"fixture","fill_id":"refused","order_id":"order","position_id":null,"symbol":"ANY",
        "side":"buy","effect":"entry","quantity":1.0,"price":1.0,"occurred_at_us":100,"as_of_us":200,"cash":null,"account_notional":null
    }})).unwrap();
    assert_eq!(journal.record_execution_fills(&ExecutionFillBatch::new(vec![input]).unwrap()).await,Err(BrokerCommandError::CapturePaused));
    let journal=journal.with_analytics_capture_budget(policy).await.unwrap();
    let broker=Arc::new(PaperBroker::new(AccountType::Paper,100_000.0,1));
    let mut worker=BrokerCommandWorker::start(broker.clone(),journal,Default::default()).unwrap();
    let bus=Arc::new(FixtureBus::default());bus.online.store(true,Ordering::Release);
    let path=directory.join("publisher.db");let store=OutboxStore::open(&path).await.unwrap();
    let lease=Arc::new(CaptureOwnerLease::acquire(&path,"runtime","strategy","session","run").unwrap());
    let publisher=LocalEventPublisher::spawn_owned_with_capture_lease(bus.clone(),store.clone(),
        LocalEventPublisherConfig{capture_limit_bytes:Some(2*1024*1024),source_journal_limit:Some(policy),critical_batch_delay:Duration::from_millis(1),..Default::default()},0,Arc::new(()),lease);
    let monitor=publisher.monitor_source_journal(worker.sender.clone()).unwrap();
    let deadline=Instant::now()+Duration::from_secs(10);
    while !publisher.cloud_capture_reconciliation_required(){assert!(Instant::now()<deadline);tokio::time::sleep(Duration::from_millis(5)).await;}
    let auth=AqsAuth{access_method:"aqe_live".into(),session_id:"session".into(),session_secret:"fixture".into(),strategy_id:"strategy".into(),user_id:"users:fixture".into(),node_id:None,live_session_id:None,url:None};
    let mut sync=LocalBatchSyncState::new(publisher,&auth);sync.runtime_id="runtime".into();sync.run_id="run".into();
    let mut insights=InsightCollection::new();
    for n in 1..=20 {let mut row=Insight::default();row.insight_id=Uuid::from_u128(n);row.state_log(Some("unexported history".into()));insights.add_insight(row);}
    let mut recovery=CaptureRecovery::default();
    // Missing source ownership cannot silently fall back to destination-only
    // completion after the replay has sealed.
    loop {
        recovery.poll(&mut sync,&insights,&auth,Vec::new,None);
        store.prune_committed(0).await.unwrap();
        if recovery.health.last_error.as_deref().is_some_and(|error|error.contains("no source journal owner")){break;}
        assert!(Instant::now()<deadline,"{:?}",recovery.health.last_error);tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(sync.publisher.cloud_capture_paused());
    assert!(sync.publisher.cloud_capture_status().source_journal.as_ref().unwrap().pause.is_some());
    while recovery.needs_poll(&sync.publisher) {
        recovery.poll(&mut sync,&insights,&auth,Vec::new,Some(worker.sender.clone()));
        store.prune_committed(0).await.unwrap();
        assert!(Instant::now()<deadline,"error={:?}, state={:?}",recovery.health.last_error,sync.publisher.cloud_capture_status());
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(recovery.health.replayed_insights,20);assert!(!sync.publisher.cloud_capture_paused());assert!(insights.has_dirty_insights());
    let events=bus.events.lock();
    assert_eq!(events.iter().filter(|event|event.kind==EventKind::InsightProjection && event.payload.get("insight_id").is_some()).count(),20);
    assert!(events.iter().any(|event|event.kind==EventKind::ReconciliationComplete));
    let gap=events.iter().find(|event|event.payload["title"]=="Cloud history gap").unwrap();
    assert_eq!(gap.payload["payload"]["origin_run_id"],serde_json::Value::Null);
    assert_eq!(gap.payload["payload"]["history_complete"],false);drop(events);
    assert!(broker.get_orders().await.unwrap().is_empty());assert!(broker.get_positions().await.unwrap().is_empty());
    monitor.shutdown().await;sync.publisher.shutdown(Duration::from_secs(1)).await;
    worker.events.close();let journal=worker.task.await.unwrap().unwrap();
    assert!(journal.analytics_capture_pause().await.unwrap().is_none());assert!(journal.execution_fill_page(0,None,128).await.unwrap().records.is_empty());
    drop(journal);drop(sync);drop(store);drop(recovery);drop(bus);drop(worker.sender);drop(broker);std::fs::remove_dir_all(directory).unwrap();
}
