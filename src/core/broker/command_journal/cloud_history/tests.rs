use super::*;

#[tokio::test]
async fn interrupted_legacy_split_can_resume_with_explicit_capacity_before_reopen() {
    let fixture=Fixture::new();
    let mut source=fixture.open().await;
    source.record_execution_fills(&ExecutionFillBatch::new((0..64).map(|i|fill(&format!("legacy-{i}"))).collect()).unwrap()).await.unwrap();
    source.connection.execute("UPDATE broker_execution_fills SET observation_json=json_set(observation_json,'$.fixture_padding',?1)",params!["x".repeat(128*1024)]).await.unwrap();
    let expected=digest(&source.connection,"broker_execution_fills").await.unwrap();
    let source_id=source.evidence_source_id.clone();
    let path=source.path.clone();
    drop(source);
    let small=PhysicalCaptureLimit{physical_limit_bytes:9*1024*1024,operational_headroom_bytes:8*1024*1024};
    assert!(matches!(BrokerCommandJournal::open_with_cloud_history_capacity(&path,"strategy","account",small).await,Err(BrokerCommandError::CapturePaused)));
    // Recovery must take the NEW approved capacity before trying the durable
    // 'copying' marker's smaller old value. No manual SQL or deletion required.
    let resumed=BrokerCommandJournal::open_with_cloud_history_capacity(&path,"strategy","account",limit()).await.unwrap();
    assert_eq!(resumed.evidence_source_id,source_id);
    assert_eq!(digest(resumed.analytics_connection(),"broker_execution_fills").await.unwrap(),expected);
    assert!(resumed.cloud_history.is_some());
    assert!(BrokerCommandJournal::open_with_cloud_history_capacity(&path,"strategy","account",limit()).await.is_err(),"a capacity change must not bypass the live owner lock");
    drop(resumed);
    let reopened=fixture.open().await;
    assert_eq!(digest(reopened.analytics_connection(),"broker_execution_fills").await.unwrap(),expected);
}

#[tokio::test]
async fn legacy_copy_is_physically_bounded_and_resumes_without_losing_source() {
    let fixture=Fixture::new();
    let source=fixture.open().await;
    source.connection.execute_batch("CREATE TABLE fixture_optional(payload TEXT NOT NULL)").await.unwrap();
    for _ in 0..64 {
        source.connection.execute("INSERT INTO fixture_optional VALUES(?1)",params!["x".repeat(128*1024)]).await.unwrap();
    }
    let expected=digest(&source.connection,"fixture_optional").await.unwrap();
    let path=fixture.0.join("owned-copy.db");
    std::fs::File::create(&path).unwrap();
    let target=connect(&path).await.unwrap();
    target.execute_batch(STORE_META).await.unwrap();
    target.execute_batch("CREATE TABLE fixture_optional(payload TEXT NOT NULL)").await.unwrap();
    let small=PhysicalCaptureLimit{physical_limit_bytes:9*1024*1024,operational_headroom_bytes:8*1024*1024};
    assert!(matches!(copy_table(&source.connection,&target,"fixture_optional",&path,small).await,
        Err(BrokerCommandError::CapturePaused)));
    let before=crate::core::live_sync::measure_physical_capture(&target,&path,small).await.unwrap().actual_bytes();
    assert!(before<small.physical_limit_bytes);
    for _ in 0..3 {
        assert!(matches!(copy_table(&source.connection,&target,"fixture_optional",&path,small).await,
            Err(BrokerCommandError::CapturePaused)));
        assert_eq!(crate::core::live_sync::measure_physical_capture(&target,&path,small).await.unwrap().actual_bytes(),before,
            "a refused copy must not append WAL or advance durable progress");
        assert_eq!(digest(&source.connection,"fixture_optional").await.unwrap(),expected,"accepted source changed");
    }
    copy_table(&source.connection,&target,"fixture_optional",&path,limit()).await.unwrap();
    assert_eq!(digest(&target,"fixture_optional").await.unwrap(),expected);
    copy_table(&source.connection,&target,"fixture_optional",&path,limit()).await.unwrap();
    assert_eq!(digest(&target,"fixture_optional").await.unwrap(),expected,"resumed copy duplicated or changed rows");
}

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("aqe-split-history-{}", uuid::Uuid::new_v4())))
    }
    async fn open(&self) -> BrokerCommandJournal {
        BrokerCommandJournal::open_with_state_history(
            self.0.join("commands.db"),
            "strategy",
            "account",
            false,
        )
        .await
        .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn limit() -> PhysicalCaptureLimit {
    PhysicalCaptureLimit {
        physical_limit_bytes: 64 * 1024 * 1024,
        operational_headroom_bytes: 8 * 1024 * 1024,
    }
}
fn fill(id: &str) -> aqe_live_protocol::ExecutionFillInput {
    serde_json::from_value(serde_json::json!({"insight_id":"insight","fill":{
        "version":1,"provider":"fixture","fill_id":id,"order_id":"order","position_id":"position","symbol":"ANY",
        "side":"buy","effect":"entry","quantity":1.0,"price":100.0,"occurred_at_us":100,"as_of_us":200,
        "cash":{"currency":"GBP","gross_pnl":0.0,"commission":1.0,"carry":0.0},"account_notional":null
    }})).unwrap()
}

#[tokio::test]
async fn migration_preserves_frozen_exports_identity_cursors_and_recovery() {
    let fixture = Fixture::new();
    let mut j = fixture.open().await;
    j.begin_execution_epoch().await.unwrap();
    j.record_execution_fills(
        &ExecutionFillBatch::new((0..40).map(|i| fill(&i.to_string())).collect()).unwrap(),
    )
    .await
    .unwrap();
    let frozen = j.prepare_execution_fill_export().await.unwrap().unwrap();
    let recovery = digest(&j.connection, "broker_execution_recovery_meta")
        .await
        .unwrap();
    let source = j.evidence_source_id.clone();
    let j = j
        .with_separate_analytics_capture_budget(limit())
        .await
        .unwrap();
    assert_ne!(j.analytics_path(), j.path);
    assert_eq!(
        j.prepare_execution_fill_export().await.unwrap().unwrap(),
        frozen
    );
    assert_eq!(
        digest(&j.connection, "broker_execution_recovery_meta")
            .await
            .unwrap(),
        recovery
    );
    assert!(
        j.connection.execute_batch(fills::SCHEMA).await.is_err(),
        "old binaries must not recreate a legacy writer"
    );
    drop(j);
    let mut j = fixture.open().await;
    assert_eq!(j.evidence_source_id, source);
    assert_eq!(j.analytics_capture_budget, Some(limit()));
    assert_eq!(
        j.prepare_execution_fill_export().await.unwrap().unwrap(),
        frozen
    );
    j.acknowledge_execution_fill_export(&frozen.receipt())
        .await
        .unwrap();
    j.record_execution_fills(&ExecutionFillBatch::new(vec![fill("after-split")]).unwrap())
        .await
        .unwrap();
    let next = j.prepare_execution_fill_export().await.unwrap().unwrap();
    assert!(next.next_sequence > frozen.next_sequence);
    assert_eq!(next.after_sequence, frozen.next_sequence);
}

#[tokio::test]
async fn cloud_pressure_does_not_restrict_canonical_broker_checkpoints() {
    use crate::core::insight::{Insight, execution_checkpoint::InsightExecutionCheckpoint};
    let fixture = Fixture::new();
    let j = fixture.open().await;
    let mut j = j
        .with_separate_analytics_capture_budget(limit())
        .await
        .unwrap();
    j.begin_execution_epoch().await.unwrap();
    let footprint = j.analytics_capture_usage().await.unwrap().unwrap();
    j.analytics_capture_budget = Some(PhysicalCaptureLimit {
        physical_limit_bytes: 8 * 1024 * 1024 + footprint.projected_commit_bytes - 1,
        ..limit()
    });
    assert!(matches!(
        j.record_execution_fills(&ExecutionFillBatch::new(vec![fill("blocked")]).unwrap())
            .await,
        Err(BrokerCommandError::CapturePaused)
    ));
    let pause = j.analytics_capture_pause().await.unwrap().unwrap();
    let mut insight = Insight::default();
    insight.symbol = "ANY".into();
    insight.order_id = Some("order".into());
    j.persist_execution_batch(
        &ExecutionCheckpointBatch::new(
            vec![InsightExecutionCheckpoint::capture(&insight, 1, true, None)],
            vec![],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(j.analytics_capture_pause().await.unwrap(), Some(pause));
    assert_eq!(
        j.execution_fill_page(0, None, 128)
            .await
            .unwrap()
            .next_sequence,
        0
    );
    let rows = j.fill_history_owner_page("").await.unwrap();
    assert_eq!(rows.owners.len(), 1);
}

#[tokio::test]
async fn interrupted_copy_resumes_with_no_duplicate_fill_rows() {
    let fixture = Fixture::new();
    let mut j = fixture.open().await;
    j.record_execution_fills(
        &ExecutionFillBatch::new((0..40).map(|i| fill(&i.to_string())).collect()).unwrap(),
    )
    .await
    .unwrap();
    // Stage the same durable progress boundary as a killed migration.
    j.connection.execute_batch(META).await.unwrap();
    j.connection
        .execute(
            "INSERT INTO broker_cloud_history_location VALUES(1,1,'copying',?1,?2)",
            params![
                limit().physical_limit_bytes as i64,
                limit().operational_headroom_bytes as i64
            ],
        )
        .await
        .unwrap();
    let path = path_for(&j.path);
    std::fs::File::create(&path).unwrap();
    let target = connect(&path).await.unwrap();
    target.execute_batch(STORE_META).await.unwrap();
    target
        .execute(
            "INSERT INTO cloud_history_identity VALUES(1,1,?1,?2,?3)",
            params![
                j.strategy_id.clone(),
                j.account_id.clone(),
                j.evidence_source_id.clone()
            ],
        )
        .await
        .unwrap();
    target.execute_batch(fills::SCHEMA).await.unwrap();
    copy_table(&j.connection, &target, "broker_execution_fills", &path, limit())
        .await
        .unwrap();
    drop(target);
    drop(j);
    let j = fixture.open().await;
    assert!(j.cloud_history.is_some());
    assert_eq!(
        j.execution_fill_page(0, None, 128)
            .await
            .unwrap()
            .records
            .len(),
        40
    );
}

#[tokio::test]
async fn standalone_creates_no_cloud_store_and_foreign_destination_is_rejected() {
    let fixture = Fixture::new();
    let j = fixture.open().await;
    assert!(!path_for(&j.path).exists());
    std::fs::write(path_for(&j.path), b"unowned fixture").unwrap();
    assert!(
        j.with_separate_analytics_capture_budget(limit())
            .await
            .is_err()
    );
    let j = fixture.open().await;
    assert!(j.cloud_history.is_none());
    assert!(!has_location(&j.connection).await.unwrap());
}

#[tokio::test]
async fn missing_split_store_never_falls_back_to_empty_history() {
    let fixture = Fixture::new();
    let j = fixture
        .open()
        .await
        .with_separate_analytics_capture_budget(limit())
        .await
        .unwrap();
    let path = j.analytics_path().to_owned();
    drop(j);
    std::fs::rename(&path, path.with_extension("held")).unwrap();
    assert!(
        BrokerCommandJournal::open_with_state_history(
            fixture.0.join("commands.db"),
            "strategy",
            "account",
            false
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn retirement_keeps_current_facts_unexported_revisions_and_monotonic_sequences() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    for revision in 0..100 {
        let mut input = fill("corrected-deal");
        input.fill.price += revision as f64;
        input.fill.as_of_us += revision * 100;
        journal
            .record_execution_fills(&ExecutionFillBatch::new(vec![input]).unwrap())
            .await
            .unwrap();
    }
    let mut journal = journal
        .with_separate_analytics_capture_budget(limit())
        .await
        .unwrap();
    let first = journal
        .prepare_execution_fill_export()
        .await
        .unwrap()
        .unwrap();
    journal
        .acknowledge_execution_fill_export(&first.receipt())
        .await
        .unwrap();
    let remaining = journal
        .execution_fill_page(first.next_sequence, None, 128)
        .await
        .unwrap();
    assert_eq!(
        remaining.records.len(),
        100 - first.next_sequence as usize,
        "an ACK must not prune later revisions"
    );
    while let Some(transfer) = journal.prepare_execution_fill_export().await.unwrap() {
        journal
            .acknowledge_execution_fill_export(&transfer.receipt())
            .await
            .unwrap();
    }
    let mut rows = journal
        .analytics_connection()
        .query("SELECT count(*) FROM broker_execution_fills", ())
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        1,
        "latest deduplication fact must be retained"
    );
    drop(rows);
    journal
        .record_execution_fills(&ExecutionFillBatch::new(vec![fill("new-deal")]).unwrap())
        .await
        .unwrap();
    let next = journal
        .prepare_execution_fill_export()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.after_sequence, 100);
    assert_eq!(next.next_sequence, 101);
    // A stale replay still compares against the retained latest fact.
    journal
        .record_execution_fills(&ExecutionFillBatch::new(vec![fill("corrected-deal")]).unwrap())
        .await
        .unwrap();
    assert_eq!(
        journal
            .prepare_execution_fill_export()
            .await
            .unwrap()
            .unwrap(),
        next
    );
}

#[tokio::test]
async fn corrupt_copy_never_selects_destination_or_deletes_source() {
    let fixture = Fixture::new();
    let mut journal = fixture.open().await;
    journal
        .record_execution_fills(&ExecutionFillBatch::new(vec![fill("owned")]).unwrap())
        .await
        .unwrap();
    journal.connection.execute_batch(META).await.unwrap();
    journal
        .connection
        .execute(
            "INSERT INTO broker_cloud_history_location VALUES(1,1,'copying',?1,?2)",
            params![
                limit().physical_limit_bytes as i64,
                limit().operational_headroom_bytes as i64
            ],
        )
        .await
        .unwrap();
    let path = path_for(&journal.path);
    std::fs::File::create(&path).unwrap();
    let target = connect(&path).await.unwrap();
    target.execute_batch(STORE_META).await.unwrap();
    target
        .execute(
            "INSERT INTO cloud_history_identity VALUES(1,1,?1,?2,?3)",
            params![
                journal.strategy_id.clone(),
                journal.account_id.clone(),
                journal.evidence_source_id.clone()
            ],
        )
        .await
        .unwrap();
    target.execute_batch(fills::SCHEMA).await.unwrap();
    copy_table(&journal.connection, &target, "broker_execution_fills", &path, limit())
        .await
        .unwrap();
    target
        .execute(
            "UPDATE broker_execution_fills SET recorded_at_us=recorded_at_us+1",
            (),
        )
        .await
        .unwrap();
    drop(target);
    assert!(migrate(&mut journal, limit()).await.is_err());
    assert!(journal.cloud_history.is_none());
    assert_eq!(
        journal
            .execution_fill_page(0, None, 128)
            .await
            .unwrap()
            .records
            .len(),
        1
    );
    let mut rows = journal
        .connection
        .query(
            "SELECT state FROM broker_cloud_history_location WHERE singleton=1",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next()
            .await
            .unwrap()
            .unwrap()
            .get::<String>(0)
            .unwrap(),
        "copying"
    );
}

#[tokio::test]
async fn pinned_reader_pressure_bounds_new_capture_and_does_not_grow_on_paused_retries() {
    let fixture=Fixture::new();
    let policy=PhysicalCaptureLimit {physical_limit_bytes:16*1024*1024,..limit()};
    let mut journal=fixture.open().await.with_separate_analytics_capture_budget(policy).await.unwrap();
    journal.begin_execution_epoch().await.unwrap();
    let reader=connect(journal.analytics_path()).await.unwrap();
    reader.execute_batch("BEGIN;").await.unwrap();
    let mut rows=reader.query("SELECT source_id FROM cloud_history_identity",()).await.unwrap();
    assert!(rows.next().await.unwrap().is_some()); drop(rows);
    let mut accepted=0;
    let mut paused=false;
    for batch in 0..100 {
        let inputs=(0..256).map(|i|fill(&format!("pressure-{batch}-{i}"))).collect();
        match journal.record_execution_fills(&ExecutionFillBatch::new(inputs).unwrap()).await {
            Ok(())=>accepted+=256,
            Err(BrokerCommandError::CapturePaused)=>{paused=true;break;},
            Err(error)=>panic!("unexpected pressure error: {error}"),
        }
        assert!(journal.analytics_capture_usage().await.unwrap().unwrap().actual_bytes()<=policy.physical_limit_bytes);
    }
    assert!(paused && accepted>0,"pressure must refuse new optional evidence after retaining a nonempty prefix");
    let pause=journal.analytics_capture_pause().await.unwrap().unwrap();
    let size=journal.analytics_capture_usage().await.unwrap().unwrap().actual_bytes();
    eprintln!("SPLIT_CAPTURE_PRESSURE {}",serde_json::json!({"accepted_rows":accepted,"actual_bytes_at_pause":size,
        "physical_limit_bytes":policy.physical_limit_bytes,"headroom_bytes":policy.operational_headroom_bytes,
        "pinned_reader":true,"rejected_retry_count":128}));
    assert!(size<=policy.physical_limit_bytes);
    for _ in 0..128 {
        assert!(matches!(journal.record_execution_fills(&ExecutionFillBatch::new(vec![fill("refused-retry")]).unwrap()).await,Err(BrokerCommandError::CapturePaused)));
    }
    assert_eq!(journal.analytics_capture_usage().await.unwrap().unwrap().actual_bytes(),size);
    // Reclamation must be nonblocking, and cannot clear a real gap.
    tokio::time::timeout(std::time::Duration::from_secs(1),journal.try_reclaim_cloud_wal()).await.unwrap().unwrap();
    assert_eq!(journal.analytics_capture_pause().await.unwrap(),Some(pause.clone()));
    reader.execute_batch("ROLLBACK;").await.unwrap(); drop(reader);
    journal.try_reclaim_cloud_wal().await.unwrap();
    assert_eq!(journal.analytics_capture_pause().await.unwrap(),Some(pause));
    assert_eq!(journal.execution_fill_page(0,None,128).await.unwrap().records.len(),128);
}

#[tokio::test]
async fn operational_cap_refusal_keeps_exact_export_cursor_and_frozen_bytes() {
    let fixture=Fixture::new();
    let mut journal=fixture.open().await.with_separate_analytics_capture_budget(limit()).await.unwrap();
    journal.begin_execution_epoch().await.unwrap();
    journal.record_execution_fills(&ExecutionFillBatch::new(vec![fill("accepted")]).unwrap()).await.unwrap();
    let frozen=journal.prepare_execution_fill_export().await.unwrap().unwrap();
    // Model a lowered allocation beneath already-retained history. Accepted
    // evidence must be retained, not deleted or cursor-advanced to fit it.
    journal.analytics_connection().execute_batch("CREATE TABLE fixture_retained_padding(body BLOB); INSERT INTO fixture_retained_padding VALUES(zeroblob(6291456));").await.unwrap();
    journal.analytics_capture_budget=Some(PhysicalCaptureLimit {physical_limit_bytes:8*1024*1024+1,..limit()});
    assert!(matches!(journal.acknowledge_execution_fill_export(&frozen.receipt()).await,Err(BrokerCommandError::CapturePaused)));
    assert_eq!(journal.prepare_execution_fill_export().await.unwrap().unwrap(),frozen);
    let mut rows=journal.analytics_connection().query("SELECT cursor FROM broker_execution_fill_export WHERE singleton=1",()).await.unwrap();
    assert_eq!(rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),0);
}
