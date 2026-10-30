use super::*;
use crate::core::{
    broker::{paper_broker::PaperBroker, traits::OrderManagementProvider, types::AccountType},
    live_sync::{CaptureOwnerLease, InProcessEventBus, LocalEventPublisherConfig, OutboxStore,
        MIN_OPERATIONAL_HEADROOM_BYTES},
    strategy::command_driver::StrategyCommandDriver,
};
use std::{path::{Path, PathBuf}, sync::Arc};

fn limit() -> PhysicalCaptureLimit {
    PhysicalCaptureLimit { physical_limit_bytes: 32 * 1024 * 1024,
        operational_headroom_bytes: MIN_OPERATIONAL_HEADROOM_BYTES }
}
struct Fixture {
    root: PathBuf,
    publisher: LocalEventPublisher,
    store: OutboxStore,
    writer: turso::Connection,
}
impl Fixture {
    async fn new(root: PathBuf, fail_witness: bool) -> Self {
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("publisher.db");
        let store = OutboxStore::open(&path).await.unwrap();
        store.latest_capture_pause().await.unwrap(); // lazy local schema
        let db = turso::Builder::new_local(path.to_string_lossy().as_ref()).build().await.unwrap();
        let writer = db.connect().unwrap();
        if fail_witness {
            writer.execute_batch("CREATE TRIGGER refuse_initial_witness BEFORE INSERT ON capture_source_observation BEGIN SELECT RAISE(ABORT,'owned initialization fixture'); END;").await.unwrap();
        }
        let lease = Arc::new(CaptureOwnerLease::acquire(&path,"runtime","strategy","session","run").unwrap());
        let publisher = LocalEventPublisher::spawn_owned_with_capture_lease(
            Arc::new(InProcessEventBus::default()), store.clone(),
            LocalEventPublisherConfig { capture_limit_bytes: Some(1024 * 1024),
                source_journal_limit: Some(limit()), ..Default::default() }, 0, Arc::new(()), lease);
        Self { root, publisher, store, writer }
    }
    fn path(&self) -> PathBuf { self.root.join("commands.db") }
    fn cloud_path(&self) -> PathBuf { self.root.join("commands.db.cloud-history.db") }
    async fn journal(&self) -> BrokerCommandJournal {
        BrokerCommandJournal::open(self.path(),"strategy","account")
            .await.unwrap().with_separate_analytics_capture_budget(limit()).await.unwrap()
    }
    async fn finish(self) {
        self.publisher.shutdown(Duration::from_secs(2)).await;
        let root = self.root.clone();
        drop(self);
        std::fs::remove_dir_all(root).unwrap();
    }
}
fn root() -> PathBuf {
    std::env::temp_dir().join(format!("source-pre-dispatch-{}",uuid::Uuid::new_v4()))
}
fn broker() -> Arc<PaperBroker> {
    Arc::new(PaperBroker::new(AccountType::Paper,100_000.0,1))
}
async fn count(path: &Path, sql: &str) -> i64 {
    let db = turso::Builder::new_local(path.to_string_lossy().as_ref()).build().await.unwrap();
    let connection = db.connect().unwrap();
    connection.query(sql,()).await.unwrap().next().await.unwrap().unwrap().get(0).unwrap()
}

#[tokio::test]
async fn startup_witness_is_prepared_before_command_worker_admission() {
    let fixture = Fixture::new(root(),false).await;
    let broker = broker();
    let (driver, _) = StrategyCommandDriver::start_recovering_with_source_observation(
        broker.clone(), fixture.path(), "strategy", "account", Default::default(),
        Some(limit()), Some(&fixture.publisher)).await.unwrap();
    assert!(driver.source_observation_preparation().unwrap().covers(&fixture.publisher,"strategy:strategy"));
    let status = driver.sender().try_analytics_capture_status().unwrap().wait().await.unwrap().unwrap();
    assert!(status.pause.is_none());
    assert!(fixture.publisher.prepared_source_observation_id(&status.journal_source_id,"strategy","account").is_some());
    assert_eq!(count(&fixture.root.join("publisher.db"),"SELECT count(*) FROM capture_source_observation WHERE closed_at_us IS NULL").await,1);
    assert!(broker.get_orders().await.unwrap().is_empty());
    assert!(!CaptureOwnerLease::journal_recovery_path(&fixture.root.join("publisher.db")).exists(),"healthy first witnesses need no fallback registry");
    drop(driver.stop().await.unwrap());
    fixture.finish().await;
}

#[tokio::test]
async fn startup_monitor_contention_keeps_the_pre_dispatch_witness_without_a_false_gap() {
    use crate::core::strategy::{aqs_types::AqsAuth, live_event_projection::LocalBatchSyncState};
    let fixture = Fixture::new(root(), false).await;
    let (driver, _) = StrategyCommandDriver::start_recovering_with_source_observation(
        broker(), fixture.path(), "strategy", "account", Default::default(),
        Some(limit()), Some(&fixture.publisher)).await.unwrap();
    assert!(!fixture.publisher.cloud_capture_paused());
    let auth = AqsAuth { access_method: "aqe_live".into(), session_id: "session".into(),
        session_secret: "fixture".into(), strategy_id: "strategy".into(),
        user_id: "users:fixture".into(), node_id: None, live_session_id: None, url: None };
    let mut sync = LocalBatchSyncState::new(fixture.publisher.clone(), &auth);
    let operation = fixture.publisher.hold_source_operation_for_test().await;
    sync.start_source_observation(driver.sender(), driver.source_observation_preparation()).await.unwrap();
    assert!(!fixture.publisher.cloud_capture_paused());
    assert!(fixture.store.latest_capture_pause().await.unwrap().is_none());
    drop(operation);
    sync.quiesce_source_observation().await;
    drop(sync);
    drop(driver.stop().await.unwrap());
    fixture.finish().await;
}

#[tokio::test]
async fn unavailable_publisher_witness_uses_durable_journal_pause_without_blocking_worker() {
    let fixture = Fixture::new(root(),true).await;
    let broker = broker();
    let (driver, _) = StrategyCommandDriver::start_recovering_with_source_observation(
        broker.clone(), fixture.path(), "strategy", "account", Default::default(),
        Some(limit()), Some(&fixture.publisher)).await.unwrap();
    assert!(driver.source_observation_preparation().unwrap().covers(&fixture.publisher,"strategy"));
    let status = driver.sender().try_analytics_capture_status().unwrap().wait().await.unwrap().unwrap();
    let pause = status.pause.unwrap();
    assert_eq!(pause.generation,1);
    assert!(fixture.publisher.cloud_capture_paused());
    assert_eq!(count(&fixture.root.join("publisher.db"),"SELECT count(*) FROM capture_source_observation").await,0);
    driver.request_execution_stop();
    let journal = driver.stop().await.unwrap();
    assert_eq!(journal.analytics_capture_pause().await.unwrap(),Some(pause));
    assert!(broker.get_orders().await.unwrap().is_empty());
    assert_eq!(fixture.store.pending_count().await.unwrap(),0);
    drop(journal);
    fixture.finish().await;
}

#[tokio::test]
async fn repeated_initialization_preserves_the_exact_journal_pause_and_handoff() {
    let fixture = Fixture::new(root(),true).await;
    let journal = fixture.journal().await;
    let first = prepare(&journal,&fixture.publisher).await.unwrap();
    let before = journal.analytics_capture_pause().await.unwrap().unwrap();
    let second = prepare(&journal,&fixture.publisher).await.unwrap();
    assert!(first.covers(&fixture.publisher,"strategy") && second.covers(&fixture.publisher,"strategy"));
    assert_eq!(journal.analytics_capture_pause().await.unwrap(),Some(before));
    assert_eq!(journal.execution_fill_page(0,None,128).await.unwrap().records.len(),0);
    assert_eq!(count(&fixture.cloud_path(),"SELECT count(*) FROM broker_analytics_capture").await,1);
    drop(journal);
    fixture.finish().await;
}

#[tokio::test]
async fn source_initialization_without_either_durable_fact_refuses_before_worker_dispatch() {
    let fixture = Fixture::new(root(),true).await;
    drop(fixture.journal().await);
    let db = turso::Builder::new_local(fixture.cloud_path().to_string_lossy().as_ref()).build().await.unwrap();
    let connection = db.connect().unwrap();
    connection.execute_batch("CREATE TRIGGER refuse_source_pause BEFORE UPDATE ON broker_analytics_capture BEGIN SELECT RAISE(ABORT,'owned journal durability fixture'); END;").await.unwrap();
    drop(connection);
    let broker = broker();
    let result = StrategyCommandDriver::start_recovering_with_source_observation(
        broker.clone(),fixture.path(),"strategy","account",Default::default(),
        Some(limit()),Some(&fixture.publisher)).await;
    assert!(result.is_err());
    assert!(broker.get_orders().await.unwrap().is_empty());
    assert_eq!(count(&fixture.path(),"SELECT count(*) FROM broker_commands").await,0);
    assert!(fixture.journal().await.analytics_capture_pause().await.unwrap().is_none());
    fixture.finish().await;
}

#[tokio::test]
async fn undiscoverable_initialization_pause_refuses_before_worker_dispatch() {
    let fixture=Fixture::new(root(),true).await;
    std::fs::create_dir(CaptureOwnerLease::journal_recovery_path(&fixture.root.join("publisher.db"))).unwrap();
    let broker=broker();
    let result=StrategyCommandDriver::start_recovering_with_source_observation(broker.clone(),fixture.path(),"strategy","account",Default::default(),Some(limit()),Some(&fixture.publisher)).await;
    assert!(result.is_err());assert!(broker.get_orders().await.unwrap().is_empty());
    assert_eq!(count(&fixture.path(),"SELECT count(*) FROM broker_commands").await,0);
    let journal=fixture.journal().await;assert!(journal.analytics_capture_pause().await.unwrap().is_some());drop(journal);
    fixture.finish().await;
}

#[tokio::test]
async fn source_initialization_rejects_mismatched_policy_without_creating_a_pause() {
    let fixture = Fixture::new(root(),false).await;
    let journal = fixture.journal().await.with_analytics_capture_budget(PhysicalCaptureLimit {
        physical_limit_bytes: limit().physical_limit_bytes + 4096, ..limit()
    }).await.unwrap();
    assert!(prepare(&journal,&fixture.publisher).await.is_err());
    assert!(journal.analytics_capture_pause().await.unwrap().is_none());
    drop(journal);
    fixture.finish().await;
}

#[tokio::test]
async fn initialization_recovery_preserves_its_receipt_until_explicit_reconciliation() {
    let fixture = Fixture::new(root(),true).await;
    let journal = fixture.journal().await;
    let prepared = prepare(&journal,&fixture.publisher).await.unwrap();
    let pause = journal.analytics_capture_pause().await.unwrap().unwrap();
    fixture.writer.execute_batch("DROP TRIGGER refuse_initial_witness").await.unwrap();
    let receipt = fixture.publisher.observe_source_journal(
        journal.analytics_capture_status().await.unwrap().unwrap()).await.unwrap().unwrap();
    assert!(journal.acknowledge_analytics_capture_handoff(&receipt).await.unwrap());
    assert!(prepared.covers(&fixture.publisher,"strategy"));
    // Source receipt is diagnostic ownership, not permission to resume trading
    // analytics before current-state replay and its remote commit barrier.
    assert!(fixture.publisher.cloud_capture_paused());
    assert_eq!(journal.suspend_analytics_for_source_initialization().await.unwrap(),pause);
    assert_eq!(journal.analytics_capture_handoff_receipt().await.unwrap(),Some(receipt));
    assert_eq!(fixture.store.pending_count().await.unwrap(),0);
    drop(journal);
    fixture.finish().await;
}

#[tokio::test]
#[ignore = "owned crash child invoked by its parent test"]
async fn source_preparation_crash_child() {
    let fixture = Fixture::new(PathBuf::from(std::env::var_os("AQE_DISPOSABLE_PREARM_ROOT").unwrap()),true).await;
    let (driver,_) = StrategyCommandDriver::start_recovering_with_source_observation(
        broker(),fixture.path(),"strategy","account",Default::default(),Some(limit()),
        Some(&fixture.publisher)).await.unwrap();
    assert!(driver.source_observation_preparation().unwrap().covers(&fixture.publisher,"strategy"));
    std::process::exit(88); // no destructors, monitor, final seal or shutdown
}

#[tokio::test]
async fn pre_first_witness_process_exit_retains_journal_quality_without_fabricated_fills() {
    let root = root();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap());
    child.args(["--exact","core::strategy::command_driver::source_observation::tests::source_preparation_crash_child","--ignored","--nocapture"])
        .env_clear().envs(std::env::vars_os().filter(|(key,_)| {
            let key=key.to_string_lossy(); !key.starts_with("AQE_") && !key.starts_with("AQS_")
        })).env("AQE_DISPOSABLE_PREARM_ROOT",&root);
    let output=child.output().unwrap();
    assert_eq!(output.status.code(),Some(88),"{}",String::from_utf8_lossy(&output.stderr));
    let journal=BrokerCommandJournal::open(root.join("commands.db"),"strategy","account")
        .await.unwrap().with_analytics_capture_budget(limit()).await.unwrap();
    let pause=journal.analytics_capture_pause().await.unwrap().unwrap();
    assert_eq!(pause.generation,1);
    assert_eq!(pause.execution_epoch,1);
    assert_eq!(journal.execution_fill_page(0,None,128).await.unwrap().records.len(),0);
    assert_eq!(count(&root.join("commands.db"),"SELECT count(*) FROM broker_commands").await,0);
    drop(journal);
    let publisher_path=root.join("publisher.db");
    let import=CaptureOwnerLease::acquire_for_import(&publisher_path).unwrap();
    let links=import.journal_recovery_links("strategy","session").await.unwrap();assert_eq!(links.len(),1);
    let exited=crate::core::broker::command_journal::ExitedAnalyticsCapture::open(&links[0]).await.unwrap();
    let retained=exited.pause().await.unwrap().unwrap();assert_eq!(retained,pause);
    let source=OutboxStore::open(&publisher_path).await.unwrap();
    let receipt=source.handoff_exited_journal_pause(&import,&links[0],&retained).await.unwrap().unwrap();
    assert!(exited.acknowledge_handoff(&retained,&receipt).await.unwrap());
    assert_eq!(exited.pause().await.unwrap(),Some(retained));
    import.forget_journal_recovery(&links[0]).await.unwrap();assert!(import.retire_empty_journal_recovery().await.unwrap());
    assert_eq!(source.local_capture_gap_notices("strategy","session").await.unwrap().len(),1);
    assert_eq!(source.pending_count().await.unwrap(),0);
    assert_eq!(count(&root.join("commands.db"),"SELECT epoch FROM broker_execution_lifecycle WHERE singleton=1").await,1);
    drop(exited);drop(source);drop(import);
    std::fs::remove_dir_all(root).unwrap();
}
