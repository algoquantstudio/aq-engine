use super::*;
use crate::core::{
    broker::command_journal::{
        AnalyticsCaptureHandoffReceipt, AnalyticsCapturePause, AnalyticsCaptureStatus,
        BrokerCommandJournal, ExecutionFillBatch,
    },
    live_sync::{
        CaptureOwnerLease, MIN_OPERATIONAL_HEADROOM_BYTES, PhysicalCaptureLimit,
        PhysicalCaptureUsage,
    },
};

struct Fixture {
    root: std::path::PathBuf,
    store: OutboxStore,
    owner: Arc<CaptureOwnerLease>,
}
impl Fixture {
    async fn new() -> Self {
        let root = std::env::temp_dir().join(format!("source-controller-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("publisher.db");
        let store = OutboxStore::open(&path).await.unwrap();
        let owner = Arc::new(
            CaptureOwnerLease::acquire(&path, "runtime", "strategy", "session", "run").unwrap(),
        );
        Self { root, store, owner }
    }
    fn controller(&self) -> CaptureController {
        CaptureController::with_source_owner(Some(100_000), Some(self.owner.clone()), Some(limit()))
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
fn limit() -> PhysicalCaptureLimit {
    PhysicalCaptureLimit {
        physical_limit_bytes: 32 * 1024 * 1024,
        operational_headroom_bytes: MIN_OPERATIONAL_HEADROOM_BYTES,
    }
}
fn sample() -> AnalyticsCaptureStatus {
    AnalyticsCaptureStatus {
        version: 1,
        journal_source_id: "journal".into(),
        strategy_id: "strategy".into(),
        account_id: "account".into(),
        generation: 0,
        pause: None,
        handoff_receipt: None,
        physical: PhysicalCaptureUsage {
            database_bytes: 4096,
            wal_bytes: 0,
            shared_memory_bytes: 0,
            logical_database_bytes: 4096,
            projected_commit_bytes: 10_000,
            limit_bytes: limit().physical_limit_bytes,
            operational_headroom_bytes: limit().operational_headroom_bytes,
        },
        observed_at_us: crate::core::live_sync::now_epoch_micros(),
    }
}
fn paused(mut status: AnalyticsCaptureStatus) -> AnalyticsCaptureStatus {
    status.generation = 1;
    status.pause = Some(AnalyticsCapturePause {
        version: 1,
        journal_source_id: status.journal_source_id.clone(),
        strategy_id: status.strategy_id.clone(),
        account_id: status.account_id.clone(),
        generation: 1,
        execution_epoch: 1,
        started_at_us: status.observed_at_us - 1,
        last_blocked_at_us: status.observed_at_us - 1,
    });
    status
}
fn receipt(pause: &AnalyticsCapturePause) -> AnalyticsCaptureHandoffReceipt {
    AnalyticsCaptureHandoffReceipt {
        version: 1,
        pause_id: pause.stable_id(),
        journal_source_id: pause.journal_source_id.clone(),
        source_generation: pause.generation,
        strategy_id: pause.strategy_id.clone(),
        account_id: pause.account_id.clone(),
        target_runtime_id: "runtime".into(),
        target_live_session_id: "session".into(),
        target_run_id: "run".into(),
        target_pause_generation: 1,
    }
}

#[tokio::test]
async fn unclean_source_restart_retains_a_gap_before_first_interruption_write() {
    let fixture=Fixture::new().await;
    let capture=fixture.controller();
    capture.observe_source(&fixture.store,sample()).await.unwrap();capture.refresh(&fixture.store).await.unwrap();
    assert!(!capture.blocked(false));
    capture.source_unavailable();
    assert!(fixture.store.latest_capture_pause().await.unwrap().is_none());
    // Deliberately do not run maintenance/persist the new interruption.
    drop(capture);
    let recovered=fixture.controller();
    recovered.observe_source(&fixture.store,sample()).await.unwrap();recovered.refresh(&fixture.store).await.unwrap();
    assert!(recovered.blocked(false),"a fresh sample after an unclean source exit is not proof that omitted history was recovered");
    assert!(fixture.store.latest_capture_pause().await.unwrap().is_some());
}

#[tokio::test]
async fn maintenance_preserves_a_source_failure_reason_until_a_fresh_observation() {
    let fixture = Fixture::new().await;
    let capture = fixture.controller();
    capture.observe_source(&fixture.store, sample()).await.unwrap();
    capture.refresh(&fixture.store).await.unwrap();
    capture.source_unavailable();
    capture.note_source_error("source journal worker closed".into());
    capture.refresh(&fixture.store).await.unwrap();
    assert_eq!(capture.status().last_error.as_deref(), Some("source journal worker closed"));
    capture.observe_source(&fixture.store, sample()).await.unwrap();
    capture.refresh(&fixture.store).await.unwrap();
    assert!(capture.status().last_error.is_none());
    assert_eq!(capture.status().state, CloudCaptureState::ReconciliationRequired);
}

#[tokio::test]
async fn independently_activated_observers_cannot_share_one_source_lease() {
    let fixture=Fixture::new().await;let first=fixture.controller();let second=fixture.controller();
    first.observe_source(&fixture.store,sample()).await.unwrap();first.refresh(&fixture.store).await.unwrap();
    assert!(second.observe_source(&fixture.store,sample()).await.is_err());
    second.source_unavailable();
    assert!(second.persist_source_suspension(&fixture.store).await.is_err());
    assert!(!first.blocked(false));assert!(second.blocked(false));
    assert!(fixture.store.latest_capture_pause().await.unwrap().is_none());
}

#[tokio::test]
async fn source_observation_witness_is_idempotent_and_not_rewritten_per_sample() {
    let fixture=Fixture::new().await;let capture=fixture.controller();
    capture.observe_source(&fixture.store,sample()).await.unwrap();capture.refresh(&fixture.store).await.unwrap();
    let wal=fixture.root.join("publisher.db-wal");let before=std::fs::read(&wal).unwrap_or_default();
    capture.source_observation_armed.store(false,Ordering::Release); // lost commit ACK, exact token retry
    capture.observe_source(&fixture.store,sample()).await.unwrap();
    for _ in 0..20{capture.observe_source(&fixture.store,sample()).await.unwrap();}
    assert_eq!(std::fs::read(wal).unwrap_or_default(),before);
    assert!(fixture.store.latest_capture_pause().await.unwrap().is_none());
    assert!(capture.close_sealed_source_observation(&fixture.store).await.is_err(),"an ordinary healthy sample is not final journal ownership");
}

#[tokio::test]
async fn interrupted_witness_commit_never_opens_capture() {
    let fixture=Fixture::new().await;let capture=fixture.controller();
    capture.refresh(&fixture.store).await.unwrap();
    let database=turso::Builder::new_local(fixture.root.join("publisher.db").to_string_lossy().as_ref()).build().await.unwrap();
    let writer=database.connect().unwrap();writer.execute_batch("BEGIN IMMEDIATE").await.unwrap();
    let result=tokio::time::timeout(std::time::Duration::from_millis(20),capture.observe_source(&fixture.store,sample())).await;
    assert!(!matches!(result,Ok(Ok(()))));assert!(capture.blocked(false));
    assert!(!capture.source_observation_armed.load(Ordering::Acquire));
    writer.execute_batch("ROLLBACK").await.unwrap();
    capture.observe_source(&fixture.store,sample()).await.unwrap();capture.refresh(&fixture.store).await.unwrap();
    assert!(!capture.blocked(false));assert!(fixture.store.latest_capture_pause().await.unwrap().is_none());
}

#[tokio::test]
async fn recovery_pause_and_replacement_witness_roll_back_together() {
    let fixture=Fixture::new().await;let first=fixture.controller();
    first.observe_source(&fixture.store,sample()).await.unwrap();drop(first);
    let database=turso::Builder::new_local(fixture.root.join("publisher.db").to_string_lossy().as_ref()).build().await.unwrap();
    let writer=database.connect().unwrap();
    writer.execute_batch("CREATE TRIGGER fixture_reject_witness BEFORE UPDATE ON capture_source_observation BEGIN SELECT RAISE(ABORT,'fixture replacement failed'); END;").await.unwrap();
    let recovered=fixture.controller();
    assert!(recovered.observe_source(&fixture.store,sample()).await.is_err());
    assert!(recovered.blocked(false));
    assert!(fixture.store.latest_capture_pause().await.unwrap().is_none(),"a failed witness replacement cannot leave half a recovery transaction");
    writer.execute_batch("DROP TRIGGER fixture_reject_witness;").await.unwrap();
    recovered.observe_source(&fixture.store,sample()).await.unwrap();recovered.refresh(&fixture.store).await.unwrap();
    let pause=fixture.store.latest_capture_pause().await.unwrap().unwrap();assert_eq!(pause.generation,1);
    recovered.observe_source(&fixture.store,sample()).await.unwrap();recovered.refresh(&fixture.store).await.unwrap();
    assert_eq!(fixture.store.latest_capture_pause().await.unwrap(),Some(pause));
}

#[tokio::test]
async fn observation_lifecycle_has_one_witness_row_across_restarts() {
    let fixture=Fixture::new().await;
    for _ in 0..40 {
        let capture=fixture.controller();capture.observe_source(&fixture.store,sample()).await.unwrap();
    }
    let database=turso::Builder::new_local(fixture.root.join("publisher.db").to_string_lossy().as_ref()).build().await.unwrap();
    let reader=database.connect().unwrap();
    for table in ["capture_source_observation","capture_pauses"] {
        let mut rows=reader.query(&format!("SELECT COUNT(*) FROM {table}"),()).await.unwrap();
        assert_eq!(rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),1);
    }
    assert_eq!(fixture.store.pending_count().await.unwrap(),0);
}

#[tokio::test]
#[ignore="owned child of source_witness_survives_a_real_process_exit_without_destructors"]
async fn source_witness_crash_child() {
    let root=std::path::PathBuf::from(std::env::var("AQE_DISPOSABLE_SOURCE_CRASH_ROOT").expect("owned fixture directory"));
    assert!(root.starts_with(std::env::temp_dir()));
    assert!(root.file_name().unwrap().to_string_lossy().starts_with("source-crash-"));
    let path=root.join("publisher.db");let store=OutboxStore::open(&path).await.unwrap();
    let owner=Arc::new(CaptureOwnerLease::acquire(&path,"old-runtime","strategy","session","old-run").unwrap());
    if std::env::var("AQE_DISPOSABLE_SOURCE_CRASH_MODE").as_deref()==Ok("sealed") {
        let journal=BrokerCommandJournal::open(root.join("final-journal.db"),"strategy","account").await.unwrap()
            .with_analytics_capture_budget(limit()).await.unwrap();
        let publisher=LocalEventPublisher::spawn_owned_with_capture_lease(Arc::new(InProcessEventBus::default()),store.clone(),
            LocalEventPublisherConfig{capture_limit_bytes:Some(100_000),source_journal_limit:Some(limit()),..Default::default()},0,Arc::new(()),owner);
        let _final=publisher.seal_final_source_journal(journal).await.unwrap();
        assert!(!publisher.cloud_capture_paused());assert!(store.latest_capture_pause().await.unwrap().is_none());
        std::process::exit(87);
    }
    let capture=CaptureController::with_source_owner(Some(100_000),Some(owner),Some(limit()));
    capture.observe_source(&store,sample()).await.unwrap();capture.refresh(&store).await.unwrap();
    assert!(!capture.blocked(false));capture.source_unavailable();
    assert!(store.latest_capture_pause().await.unwrap().is_none());
    std::process::exit(86); // no Drop, asynchronous flush, shutdown or interruption commit
}

#[tokio::test]
async fn source_witness_survives_a_real_process_exit_without_destructors() {
    let root=std::env::temp_dir().join(format!("source-crash-{}",uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let output=std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact","core::live_sync::publisher::capture::source_tests::source_witness_crash_child","--ignored","--nocapture"])
        .env("AQE_DISPOSABLE_SOURCE_CRASH_ROOT",&root).output().unwrap();
    assert_eq!(output.status.code(),Some(86),"{}",String::from_utf8_lossy(&output.stderr));
    let path=root.join("publisher.db");let store=OutboxStore::open(&path).await.unwrap();
    let owner=Arc::new(CaptureOwnerLease::acquire(&path,"new-runtime","strategy","session","new-run").unwrap());
    let capture=CaptureController::with_source_owner(Some(100_000),Some(owner),Some(limit()));
    capture.observe_source(&store,sample()).await.unwrap();capture.refresh(&store).await.unwrap();
    let pause=store.latest_capture_pause().await.unwrap().unwrap();
    assert_eq!(pause.origin_runtime_id.as_deref(),Some("old-runtime"));
    assert_eq!(pause.origin_run_id.as_deref(),Some("old-run"));
    assert_eq!(pause.owner_run_id.as_deref(),Some("new-run"));
    assert_eq!(pause.runtime_id,"new-runtime");assert!(capture.blocked(false));
    assert_eq!(capture.status().state,CloudCaptureState::ReconciliationRequired);
    assert_eq!(store.pending_count().await.unwrap(),0);
    assert_eq!(store.critical_sequence_cursor_for_runtime_scope("new-runtime","strategy","session").await.unwrap(),0);
    drop(capture);drop(store);let _=std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn closed_witness_survives_a_real_process_exit_after_final_journal_seal() {
    let root=std::env::temp_dir().join(format!("source-crash-{}",uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let output=std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact","core::live_sync::publisher::capture::source_tests::source_witness_crash_child","--ignored","--nocapture"])
        .env("AQE_DISPOSABLE_SOURCE_CRASH_ROOT",&root).env("AQE_DISPOSABLE_SOURCE_CRASH_MODE","sealed").output().unwrap();
    assert_eq!(output.status.code(),Some(87),"{}",String::from_utf8_lossy(&output.stderr));
    let path=root.join("publisher.db");let store=OutboxStore::open(&path).await.unwrap();
    let owner=Arc::new(CaptureOwnerLease::acquire(&path,"new-runtime","strategy","session","new-run").unwrap());
    let capture=CaptureController::with_source_owner(Some(100_000),Some(owner),Some(limit()));
    capture.observe_source(&store,sample()).await.unwrap();capture.refresh(&store).await.unwrap();
    assert!(!capture.blocked(false));assert!(store.latest_capture_pause().await.unwrap().is_none());
    drop(capture);drop(store);let _=std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn required_journal_starts_closed_without_fabricating_a_history_gap() {
    let fixture = Fixture::new().await;
    let capture = fixture.controller();
    capture.refresh(&fixture.store).await.unwrap();
    assert!(capture.blocked(false));
    assert_eq!(capture.status().state, CloudCaptureState::Starting);
    assert!(
        fixture
            .store
            .latest_capture_pause()
            .await
            .unwrap()
            .is_none()
    );
    capture.observe_source(&fixture.store,sample()).await.unwrap();
    capture.refresh(&fixture.store).await.unwrap();
    assert_eq!(capture.status().state, CloudCaptureState::Active);
    assert!(capture.status().source_journal.is_some());
    assert_eq!(fixture.store.pending_count().await.unwrap(), 0);
    let old = capture.source.snapshot().unwrap();
    capture.source_unavailable();
    capture.refresh(&fixture.store).await.unwrap();
    assert!(capture.blocked(false));
    assert!(
        capture.observe_source(&fixture.store,old).await.is_err(),
        "a pre-failure cached observation cannot clear an unavailable-source fence"
    );
    capture.observe_source(&fixture.store,sample()).await.unwrap();
    capture.refresh(&fixture.store).await.unwrap();
    assert!(capture.blocked(false),"a restored observation is not current-state replay");
    assert_eq!(capture.status().state,CloudCaptureState::ReconciliationRequired);
    let generation=capture.status().pause.unwrap().generation;
    assert!(capture.complete(&fixture.store,generation).await.unwrap());
}

#[tokio::test]
async fn ready_journal_gap_allows_replay_but_cannot_be_cleared_by_outbox_completion_alone() {
    let fixture = Fixture::new().await;
    let capture = fixture.controller();
    let mut status = paused(sample());
    let source = status.pause.clone().unwrap();
    capture.note_source_pause(source.clone()).unwrap();
    let exact = fixture
        .store
        .handoff_analytics_capture_pause(&fixture.owner, &source, 100_000)
        .await
        .unwrap()
        .unwrap();
    capture.observe_source(&fixture.store,status.clone()).await.unwrap();
    capture.refresh(&fixture.store).await.unwrap();
    assert_eq!(
        capture.status().state,
        CloudCaptureState::Paused,
        "no source receipt yet"
    );
    status.observed_at_us += 1;
    status.handoff_receipt = Some(exact.clone());
    capture.observe_source(&fixture.store,status.clone()).await.unwrap();
    capture.refresh(&fixture.store).await.unwrap();
    assert_eq!(
        capture.status().state,
        CloudCaptureState::ReconciliationRequired
    );
    assert!(!capture.blocked(true));
    assert!(capture.blocked(false));
    let generation = capture.status().pause.unwrap().generation;
    assert!(!capture.complete(&fixture.store, generation).await.unwrap());
    assert!(
        fixture
            .store
            .latest_capture_pause()
            .await
            .unwrap()
            .is_some()
    );
    // A later qualified journal observation proves its own pause cleared, but
    // does not automatically clear the independent destination reconciliation.
    status.observed_at_us += 1;
    status.pause = None;
    status.handoff_receipt = None;
    capture.observe_source(&fixture.store,status).await.unwrap();
    capture.refresh(&fixture.store).await.unwrap();
    assert_eq!(
        capture.status().state,
        CloudCaptureState::ReconciliationRequired
    );
    assert!(capture.complete(&fixture.store, generation).await.unwrap());
    assert_eq!(capture.status().state, CloudCaptureState::Active);
}

#[tokio::test]
async fn a_durable_refusal_hint_fences_an_older_healthy_sample() {
    let fixture = Fixture::new().await;
    let capture = fixture.controller();
    let mut healthy = sample();
    healthy.observed_at_us -= 100;
    capture.observe_source(&fixture.store,healthy.clone()).await.unwrap();
    capture.refresh(&fixture.store).await.unwrap();
    assert!(!capture.blocked(false));
    let status = paused(sample());
    let source = status.pause.clone().unwrap();
    capture.note_source_pause(source).unwrap();
    assert!(capture.blocked(false));
    healthy.observed_at_us += 1;
    capture.observe_source(&fixture.store,healthy).await.unwrap();
    capture.refresh(&fixture.store).await.unwrap();
    assert!(
        capture.blocked(false),
        "a sampled healthy generation cannot erase a newer refusal"
    );
    capture.observe_source(&fixture.store,status).await.unwrap();
    capture.refresh(&fixture.store).await.unwrap();
    assert!(capture.blocked(false));
}

#[tokio::test]
async fn source_observations_expire_and_reject_scope_generation_and_receipt_regression() {
    let fixture = Fixture::new().await;
    let guard = SourceGuard::new(Some(limit()));
    let original = sample();
    let now = original.observed_at_us;
    guard
        .observe(original.clone(), &fixture.owner, now)
        .unwrap();
    assert_eq!(guard.qualification(now), Qualification::Clear);
    assert_eq!(
        guard.qualification(now + source::SOURCE_FRESHNESS_US + 1),
        Qualification::Blocked
    );
    assert!(guard.suspension().is_some());
    // Scope/generation validation below uses a separate uninterrupted owner;
    // the expired owner cannot be cleared without a persisted interruption.
    let guard=SourceGuard::new(Some(limit()));
    guard.observe(original.clone(),&fixture.owner,now).unwrap();
    let mut qualified = paused(original.clone());
    qualified.observed_at_us += 1;
    qualified.handoff_receipt = Some(receipt(qualified.pause.as_ref().unwrap()));
    guard
        .observe(qualified.clone(), &fixture.owner, now + 1)
        .unwrap();
    assert_eq!(guard.qualification(now + 1), Qualification::Ready);
    let mut changed = qualified.clone();
    changed.observed_at_us += 1;
    changed.account_id = "other".into();
    changed.pause.as_mut().unwrap().account_id = "other".into();
    changed.handoff_receipt = None;
    assert!(guard.observe(changed, &fixture.owner, now + 2).is_err());
    let mut changed = qualified.clone();
    changed.observed_at_us += 1;
    changed.handoff_receipt = None;
    assert!(guard.observe(changed, &fixture.owner, now + 2).is_err());
    let mut changed = qualified.clone();
    changed.observed_at_us += 1;
    changed.pause.as_mut().unwrap().last_blocked_at_us += 1;
    changed.handoff_receipt = None;
    assert!(guard.observe(changed, &fixture.owner, now + 2).is_err());
    assert!(guard.observe(original, &fixture.owner, now + 2).is_err());
    assert_eq!(guard.snapshot(), Some(qualified));
}

#[tokio::test]
async fn invalid_source_configuration_never_silently_becomes_unlimited() {
    let fixture = Fixture::new().await;
    for (payload, owner, policy) in [
        (None, Some(fixture.owner.clone()), Some(limit())),
        (Some(100_000), None, Some(limit())),
        (
            Some(100_000),
            Some(fixture.owner.clone()),
            Some(PhysicalCaptureLimit {
                physical_limit_bytes: 1,
                ..limit()
            }),
        ),
    ] {
        let capture = CaptureController::with_source_owner(payload, owner, policy);
        assert!(capture.refresh(&fixture.store).await.is_err());
        assert!(capture.blocked(false));
    }
}

#[tokio::test]
async fn observation_suspension_is_durable_idempotent_and_allocates_no_event() {
    let fixture = Fixture::new().await;
    let at = crate::core::live_sync::now_epoch_micros();
    fixture
        .store
        .record_source_observation_suspension(&fixture.owner, at, 100_000)
        .await
        .unwrap();
    let original = fixture.store.latest_capture_pause().await.unwrap().unwrap();
    let path = fixture.root.join("publisher.db-wal");
    let wal = std::fs::read(&path).unwrap_or_default();
    for _ in 0..10 {
        fixture
            .store
            .record_source_observation_suspension(&fixture.owner, at, 100_000)
            .await
            .unwrap();
    }
    assert_eq!(std::fs::read(path).unwrap_or_default(), wal);
    assert_eq!(
        fixture.store.latest_capture_pause().await.unwrap(),
        Some(original.clone())
    );
    assert_eq!(fixture.store.pending_count().await.unwrap(), 0);
    assert_eq!(
        fixture
            .store
            .critical_sequence_cursor_for_runtime_scope("runtime", "strategy", "session")
            .await
            .unwrap(),
        0
    );
    let reopened = OutboxStore::open(fixture.root.join("publisher.db"))
        .await
        .unwrap();
    assert_eq!(
        reopened.latest_capture_pause().await.unwrap(),
        Some(original.clone())
    );
    reopened
        .record_source_observation_suspension(&fixture.owner, at + 1, 100_000)
        .await
        .unwrap();
    assert_eq!(
        reopened
            .latest_capture_pause()
            .await
            .unwrap()
            .unwrap()
            .generation,
        original.generation + 1
    );
    assert!(
        !reopened
            .complete_capture_reconciliation("runtime", "strategy", "session", original.generation)
            .await
            .unwrap()
    );
    let wrong = CaptureOwnerLease::acquire(
        &fixture.root.join("wrong.db"),
        "runtime",
        "strategy",
        "session",
        "run",
    )
    .unwrap();
    assert!(
        reopened
            .record_source_observation_suspension(&wrong, at + 2, 100_000)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn public_owner_bridge_hands_off_real_source_status_without_resuming_it() {
    let fixture = Fixture::new().await;
    let mut journal = BrokerCommandJournal::open(
        fixture.root.join("journal.db"),
        "strategy",
        "account")
    .await
    .unwrap();
    let tiny = PhysicalCaptureLimit {
        physical_limit_bytes: MIN_OPERATIONAL_HEADROOM_BYTES + 1,
        ..limit()
    };
    journal = journal.with_analytics_capture_budget(tiny).await.unwrap();
    let publisher = LocalEventPublisher::spawn_owned_with_capture_lease(
        Arc::new(InProcessEventBus::default()),
        fixture.store.clone(),
        LocalEventPublisherConfig {
            capture_limit_bytes: Some(100_000),
            source_journal_limit: Some(tiny),
            ..Default::default()
        },
        0,
        Arc::new(()),
        fixture.owner.clone(),
    );
    let healthy = journal.analytics_capture_status().await.unwrap().unwrap();
    assert!(
        publisher
            .observe_source_journal(healthy)
            .await
            .unwrap()
            .is_none()
    );
    assert!(!publisher.cloud_capture_paused());
    let input=serde_json::from_value(serde_json::json!({"insight_id":null,"fill":{
        "version":1,"provider":"fixture","fill_id":"pressure","order_id":"order","position_id":null,"symbol":"ANY",
        "side":"buy","effect":"entry","quantity":1.0,"price":1.0,"occurred_at_us":100,"as_of_us":200,"cash":null,"account_notional":null
    }})).unwrap();
    assert!(matches!(
        journal
            .record_execution_fills(&ExecutionFillBatch::new(vec![input]).unwrap())
            .await,
        Err(crate::core::broker::commands::BrokerCommandError::CapturePaused)
    ));
    let source = journal.analytics_capture_status().await.unwrap().unwrap();
    let exact = publisher
        .observe_source_journal(source.clone())
        .await
        .unwrap()
        .unwrap();
    assert!(publisher.cloud_capture_paused());
    assert!(!publisher.admission_failed());
    assert!(
        journal
            .acknowledge_analytics_capture_handoff(&exact)
            .await
            .unwrap()
    );
    publisher
        .observe_source_journal(journal.analytics_capture_status().await.unwrap().unwrap())
        .await
        .unwrap();
    assert!(
        publisher.cloud_capture_paused(),
        "physical source pressure still exceeds low water"
    );
    assert_eq!(
        journal.analytics_capture_pause().await.unwrap(),
        source.pause
    );
    assert_eq!(fixture.store.pending_count().await.unwrap(), 0);
    assert_eq!(publisher.critical_sequence_cursor(), 0);
    publisher.shutdown(std::time::Duration::from_secs(1)).await;
}

#[tokio::test]
async fn direct_source_observations_share_the_recovery_operation_lane() {
    let fixture = Fixture::new().await;
    let publisher = LocalEventPublisher::spawn_owned_with_capture_lease(
        Arc::new(InProcessEventBus::default()),
        fixture.store.clone(),
        LocalEventPublisherConfig {
            capture_limit_bytes: Some(100_000),
            source_journal_limit: Some(limit()),
            ..Default::default()
        },
        0,
        Arc::new(()),
        fixture.owner.clone(),
    );
    let operation = publisher.source_operation_gate.lock().await;
    let writer = publisher.clone();
    let report = sample();
    let task = tokio::spawn(async move { writer.observe_source_journal(report).await });
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert!(
        !task.is_finished(),
        "a direct observation cannot interleave with source-first completion"
    );
    assert!(publisher.cloud_capture_paused());
    drop(operation);
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .is_none()
    );
    assert!(!publisher.cloud_capture_paused());
    publisher.shutdown(std::time::Duration::from_secs(1)).await;
}

#[tokio::test]
async fn final_source_owner_retains_exclusivity_without_inventing_a_gap() {
    let fixture=Fixture::new().await;
    let path=fixture.root.join("final-journal.db");
    let journal=BrokerCommandJournal::open(&path,"strategy","account").await.unwrap()
        .with_analytics_capture_budget(limit()).await.unwrap();
    let publisher=LocalEventPublisher::spawn_owned_with_capture_lease(
        Arc::new(InProcessEventBus::default()),fixture.store.clone(),
        LocalEventPublisherConfig{capture_limit_bytes:Some(100_000),source_journal_limit:Some(limit()),..Default::default()},
        0,Arc::new(()),fixture.owner.clone());
    let report=journal.analytics_capture_status().await.unwrap().unwrap();
    let final_owner=publisher.seal_final_source_journal(journal).await.unwrap();
    assert!(!publisher.cloud_capture_paused());
    assert!(fixture.store.latest_capture_pause().await.unwrap().is_none());
    assert!(BrokerCommandJournal::open(&path,"strategy","account").await.is_err(),
        "the returned journal must stay exclusive until final cloud publication ends");
    assert!(publisher.observe_source_journal(report).await.is_err(),"a sealed owner cannot accept a later observer");
    assert!(publisher.observe_source_journal(paused(sample())).await.is_err());
    assert!(fixture.store.latest_capture_pause().await.unwrap().is_none(),"late reports cannot mutate the outbox before sealed-owner rejection");
    // A stopped immutable owner does not become stale while final snapshots
    // wait on delivery. An unavailable signal still closes it immediately.
    assert_eq!(publisher.capture.source.qualification(crate::core::live_sync::now_epoch_micros()+2*source::SOURCE_FRESHNESS_US),source::Qualification::Clear);
    publisher.shutdown(std::time::Duration::from_secs(1)).await;
    drop(final_owner);
    assert!(publisher.cloud_capture_paused());
    assert!(BrokerCommandJournal::open(&path,"strategy","account").await.is_ok());
    drop(publisher);
    let restarted=fixture.controller();restarted.observe_source(&fixture.store,sample()).await.unwrap();restarted.refresh(&fixture.store).await.unwrap();
    assert!(!restarted.blocked(false));
    assert!(fixture.store.latest_capture_pause().await.unwrap().is_none(),"verified immutable final journal closes only its observer witness without inventing a pause");
}

#[tokio::test]
async fn cancelled_final_source_handoff_closes_capture_before_releasing_the_journal() {
    let fixture=Fixture::new().await;
    let path=fixture.root.join("cancel-final.db");
    let journal=BrokerCommandJournal::open(&path,"strategy","account").await.unwrap()
        .with_analytics_capture_budget(limit()).await.unwrap();
    let publisher=LocalEventPublisher::spawn_owned_with_capture_lease(
        Arc::new(InProcessEventBus::default()),fixture.store.clone(),
        LocalEventPublisherConfig{capture_limit_bytes:Some(100_000),source_journal_limit:Some(limit()),..Default::default()},
        0,Arc::new(()),fixture.owner.clone());
    publisher.observe_source_journal(journal.analytics_capture_status().await.unwrap().unwrap()).await.unwrap();
    let operation=publisher.source_operation_gate.lock().await;
    let writer=publisher.clone();
    let task=tokio::spawn(async move{writer.seal_final_source_journal(journal).await});
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert!(!task.is_finished());
    task.abort();let _=task.await;
    assert!(publisher.cloud_capture_paused());
    assert!(BrokerCommandJournal::open(&path,"strategy","account").await.is_ok());
    drop(operation);
    publisher.shutdown(std::time::Duration::from_secs(1)).await;
}

#[tokio::test]
async fn expired_source_is_persisted_before_a_fresh_sample_can_qualify_recovery(){
    let fixture=Fixture::new().await;let capture=fixture.controller();
    let mut expired=sample();expired.observed_at_us-=source::SOURCE_FRESHNESS_US+10;
    capture.source.observe(expired.clone(),&fixture.owner,expired.observed_at_us).unwrap();
    let mut previous=capture.status();previous.state=CloudCaptureState::Active;capture.publish(previous);
    // Detection on the producer gate itself closes capture without waiting for
    // the one-second maintenance task or doing SQLite I/O on that hot path.
    assert!(capture.blocked(false));
    assert!(capture.observe_source(&fixture.store,sample()).await.is_err(),"freshness alone is not a persistence ACK");
    capture.refresh(&fixture.store).await.unwrap();
    let gap=fixture.store.latest_capture_pause().await.unwrap().unwrap();
    assert_eq!(capture.status().pause.as_ref().unwrap().generation,gap.generation,"the first completed refresh exposes its committed interruption");
    assert_eq!(gap.capacity.requested_bytes,0);
    capture.observe_source(&fixture.store,sample()).await.unwrap();capture.refresh(&fixture.store).await.unwrap();
    assert_eq!(capture.status().state,CloudCaptureState::ReconciliationRequired);
    assert_eq!(capture.status().pause.unwrap().generation,gap.generation);
    assert_eq!(fixture.store.pending_count().await.unwrap(),0);
    assert_eq!(fixture.store.critical_sequence_cursor_for_runtime_scope("runtime","strategy","session").await.unwrap(),0);
}

#[tokio::test]
async fn failed_interruption_commit_does_not_grant_a_later_observation_permission(){
    let fixture=Fixture::new().await;let capture=fixture.controller();
    capture.observe_source(&fixture.store,sample()).await.unwrap();capture.refresh(&fixture.store).await.unwrap();
    let database=turso::Builder::new_local(fixture.root.join("publisher.db").to_string_lossy().as_ref()).build().await.unwrap();
    let writer=database.connect().unwrap();writer.execute_batch("BEGIN IMMEDIATE").await.unwrap();
    capture.source_unavailable();
    let result=tokio::time::timeout(std::time::Duration::from_millis(20),capture.persist_source_suspension(&fixture.store)).await;
    assert!(!matches!(result,Ok(Ok(()))),"the conflicting writer must prevent a confirmed interruption commit");
    assert!(capture.observe_source(&fixture.store,sample()).await.is_err());assert!(capture.blocked(false));
    writer.execute_batch("ROLLBACK").await.unwrap();
    capture.refresh(&fixture.store).await.unwrap();
    capture.observe_source(&fixture.store,sample()).await.unwrap();capture.refresh(&fixture.store).await.unwrap();
    assert_eq!(capture.status().state,CloudCaptureState::ReconciliationRequired);
    assert!(fixture.store.latest_capture_pause().await.unwrap().is_some());
}

#[tokio::test]
async fn a_second_failure_fences_a_sample_taken_after_the_first_persisted_interruption(){
    let fixture=Fixture::new().await;let capture=fixture.controller();
    capture.observe_source(&fixture.store,sample()).await.unwrap();capture.refresh(&fixture.store).await.unwrap();
    capture.source_unavailable();capture.refresh(&fixture.store).await.unwrap();
    let cached=sample();let original=capture.source_suspension().unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    capture.source_unavailable();
    assert_eq!(capture.source_suspension(),Some(original),"one outage retains one fixed identity");
    assert!(capture.observe_source(&fixture.store,cached).await.is_err(),"a later failure cannot borrow an earlier healthy sample");
    capture.observe_source(&fixture.store,sample()).await.unwrap();capture.refresh(&fixture.store).await.unwrap();
    assert_eq!(capture.status().pause.unwrap().generation,1,"repeated failures in one interruption do not allocate unbounded generations");
    assert_eq!(capture.status().state,CloudCaptureState::ReconciliationRequired);
}

#[tokio::test]
async fn acknowledged_interruption_does_not_rewrite_the_wal_on_every_maintenance_tick(){
    let fixture=Fixture::new().await;let capture=fixture.controller();
    capture.observe_source(&fixture.store,sample()).await.unwrap();capture.refresh(&fixture.store).await.unwrap();
    capture.source_unavailable();capture.refresh(&fixture.store).await.unwrap();
    let original=fixture.store.latest_capture_pause().await.unwrap().unwrap();
    let wal=fixture.root.join("publisher.db-wal");let before=std::fs::read(&wal).unwrap_or_default();
    for _ in 0..20 {capture.refresh(&fixture.store).await.unwrap();}
    assert_eq!(std::fs::read(&wal).unwrap_or_default(),before,"a confirmed fixed interruption needs no repeated WAL writes");
    assert_eq!(fixture.store.latest_capture_pause().await.unwrap(),Some(original));
}

#[tokio::test]
async fn a_new_interruption_cannot_reuse_the_previous_durable_acknowledgement(){
    let fixture=Fixture::new().await;let capture=fixture.controller();
    capture.observe_source(&fixture.store,sample()).await.unwrap();capture.refresh(&fixture.store).await.unwrap();
    capture.source_unavailable();let first=capture.source_suspension().unwrap();
    capture.refresh(&fixture.store).await.unwrap();capture.observe_source(&fixture.store,sample()).await.unwrap();capture.refresh(&fixture.store).await.unwrap();
    let old_generation=capture.status().pause.unwrap().generation;
    assert!(capture.complete(&fixture.store,old_generation).await.unwrap());
    capture.source_unavailable();assert!(capture.source_suspension().unwrap()>first);
    assert!(capture.observe_source(&fixture.store,sample()).await.is_err(),"the previous interruption's commit is not permission for a new one");
    capture.refresh(&fixture.store).await.unwrap();capture.observe_source(&fixture.store,sample()).await.unwrap();capture.refresh(&fixture.store).await.unwrap();
    assert_eq!(capture.status().pause.unwrap().generation,old_generation+1);
    assert!(!capture.complete(&fixture.store,old_generation).await.unwrap());
    assert!(capture.blocked(false));
}
