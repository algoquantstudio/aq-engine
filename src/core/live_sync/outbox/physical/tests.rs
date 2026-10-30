use super::*;

#[tokio::test]
async fn reconciliation_cleanup_refuses_oversized_legacy_blob_before_materializing_it() {
    let directory=Directory::new();let store=OutboxStore::open(directory.path()).await.unwrap();
    let mut legacy=event(1);legacy.priority=EventPriority::Reconciliation;legacy.critical_sequence=None;
    legacy.kind=EventKind::ReconciliationProgress;legacy.payload=serde_json::json!({"generation":"old"});
    store.insert_events(&[legacy.clone()]).await.unwrap();
    let bytes=(LOCAL_EVENT_HARD_LIMIT_BYTES as i64)*2;
    store.connection.execute("UPDATE outbox_events SET wire_payload=zeroblob(?1) WHERE event_id=?2",params![bytes,legacy.event_id.to_string()]).await.unwrap();
    let start=ReconciliationStart{purpose:aqe_live_protocol::ReconciliationPurpose::Startup,critical_replay_through:None,
        generation:"bounded-new".into(),runtime_id:"runtime".into(),strategy_id:"strategy".into(),live_session_id:"session".into(),
        run_id:Some("run".into()),snapshot_complete:true,broker_features:Vec::new(),critical_cutoff:None,total_chunks:0,total_records:0};
    assert!(matches!(store.begin_reconciliation_job(&start).await,Err(OutboxStoreError::InvalidRecord(message)) if message.contains("offline legacy-envelope")));
    assert_eq!(scalar(&store,"SELECT count(*) FROM outbox_events WHERE state='pending'").await,1);
    assert_eq!(scalar(&store,"SELECT length(wire_payload) FROM outbox_events").await,bytes);
    let mut rows=store.connection.query("SELECT CASE WHEN length(wire_payload)<=?1 THEN wire_payload END FROM outbox_events",params![LOCAL_EVENT_HARD_LIMIT_BYTES as i64]).await.unwrap();
    assert!(matches!(rows.next().await.unwrap().unwrap().get_value(0).unwrap(),turso::Value::Null));
}

#[tokio::test]
async fn reconciliation_cleanup_resumes_after_cancelled_page_with_durable_job_witness() {
    let directory=Directory::new();
    let store=OutboxStore::open_with_physical_capture_budget(directory.path(),budget()).await.unwrap();
    let start=ReconciliationStart{purpose:aqe_live_protocol::ReconciliationPurpose::Startup,critical_replay_through:None,
        generation:"new-generation".into(),runtime_id:"runtime".into(),strategy_id:"strategy".into(),live_session_id:"session".into(),
        run_id:Some("run".into()),snapshot_complete:true,broker_features:Vec::new(),critical_cutoff:Some(1),total_chunks:0,total_records:0};
    let old=ReconciliationStart{generation:"old-generation".into(),..start.clone()};
    store.begin_reconciliation_job(&old).await.unwrap();
    for offset in [0,100,200] {
        let rows=(offset+1..=offset+100).map(|i|{
            let mut e=event(i);e.priority=EventPriority::Reconciliation;e.critical_sequence=None;
            e.kind=EventKind::ReconciliationProgress;e.payload=serde_json::json!({"generation":"old-generation"});e
        }).collect::<Vec<_>>();
        store.insert_events(&rows).await.unwrap();
    }
    let entered=Arc::new(tokio::sync::Notify::new());let resume=Arc::new(tokio::sync::Notify::new());
    *store.mutation_pause.lock()=Some((entered.clone(),resume.clone()));
    let target=store.clone();let input=start.clone();
    let task=tokio::spawn(async move {target.begin_reconciliation_job(&input).await});
    // Job, old job status, empty status page, first envelope page, second page.
    for step in 0..5 {
        tokio::time::timeout(Duration::from_secs(5),entered.notified()).await.unwrap();
        if step<4 {*store.mutation_pause.lock()=Some((entered.clone(),resume.clone()));resume.notify_one();}
    }
    task.abort();assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(scalar(&store,"SELECT count(*) FROM reconciliation_jobs WHERE generation='new-generation'").await,1);
    let committed=scalar(&store,"SELECT count(*) FROM outbox_events WHERE state='committed'").await;
    assert_eq!(committed,64,"only a fully committed bounded page can retire old snapshots");
    store.begin_reconciliation_job(&start).await.unwrap();
    assert_eq!(scalar(&store,"SELECT count(*) FROM outbox_events WHERE state='committed'").await,300);
    store.begin_reconciliation_job(&start).await.unwrap();
    assert_eq!(scalar(&store,"SELECT count(*) FROM outbox_events").await,300);
}

#[tokio::test]
async fn latency_rollups_stream_exact_quantiles_and_page_expired_metadata() {
    let directory=Directory::new();
    let store=OutboxStore::open_with_physical_capture_budget(directory.path(),budget()).await.unwrap();
    let minute=60_000_000;
    let now=now_epoch_micros();
    let bucket=now-now.rem_euclid(minute)-minute;
    let tx=turso::transaction::Transaction::new_unchecked(&store.connection,turso::transaction::TransactionBehavior::Immediate).await.unwrap();
    let mut values=Vec::new();
    for i in 0..1025i64 {
        let duration=(i*7919)%10007;values.push(duration);
        tx.execute("INSERT INTO latency_samples VALUES(?1,'fixture-stage',?2,?3)",params![format!("sample-{i}"),duration,bucket+i]).await.unwrap();
        if i<600 {
            tx.execute("INSERT INTO latency_samples VALUES(?1,'fixture-stage',99,?2)",params![format!("expired-{i}"),now-RAW_LATENCY_SAMPLE_RETENTION_US-1000]).await.unwrap();
        }
    }
    tx.commit().await.unwrap();values.sort_unstable();
    store.refresh_latency_rollups().await.unwrap();
    let mut rows=store.connection.query("SELECT sample_count,p50_us,p95_us,p99_us,max_us FROM latency_rollups WHERE bucket_start_us=?1 AND stage='fixture-stage'",params![bucket]).await.unwrap();
    let row=rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(),1025);
    for (index,p) in [50,95,99,100].into_iter().enumerate(){assert_eq!(row.get::<i64>(index+1).unwrap(),percentile(&values,p));}
    drop(rows);
    assert_eq!(scalar(&store,"SELECT count(*) FROM latency_samples WHERE event_id LIKE 'expired-%'").await,344);
    for remaining in [88,0] {
        store.refresh_latency_rollups().await.unwrap();
        assert_eq!(scalar(&store,"SELECT count(*) FROM latency_samples WHERE event_id LIKE 'expired-%'").await,remaining);
    }
    let mut plan=store.connection.query("EXPLAIN QUERY PLAN SELECT duration_us FROM latency_samples INDEXED BY idx_latency_samples_stage_duration WHERE stage=?1 AND observed_at_us>=?2 AND observed_at_us<?3 ORDER BY duration_us",params!["fixture-stage",bucket,bucket+minute]).await.unwrap();
    while let Some(row)=plan.next().await.unwrap(){assert!(!row.get::<String>(3).unwrap().contains("TEMP B-TREE"));}
}

#[tokio::test]
async fn startup_contention_repair_pages_every_row_without_touching_poison_data() {
    let directory=Directory::new();
    let store=OutboxStore::open_with_physical_capture_budget(directory.path(),budget()).await.unwrap();
    for start in [1,101,201,301] {
        store.insert_events(&(start..start+100).map(event).collect::<Vec<_>>()).await.unwrap();
    }
    store.connection.execute("UPDATE outbox_events SET state='dead_letter',attempt_count=7,last_error='transaction conflict' WHERE critical_sequence<=300",()).await.unwrap();
    store.connection.execute("INSERT INTO dead_letters(event_id,strategy_id,reason,wire_payload,failed_at_us) SELECT event_id,strategy_id,'transaction conflict',wire_payload,0 FROM outbox_events WHERE state='dead_letter'",()).await.unwrap();
    store.connection.execute("UPDATE outbox_events SET last_error='database is locked',attempt_count=7,next_attempt_at_us=?1 WHERE state='pending'",params![i64::MAX]).await.unwrap();
    store.requeue_retryable_dead_letters().await.unwrap();
    store.reset_pending_contention_backoff().await.unwrap();
    assert_eq!(scalar(&store,"SELECT count(*) FROM outbox_events WHERE state='pending' AND last_error IS NULL AND attempt_count=0").await,400);
    assert_eq!(scalar(&store,"SELECT count(*) FROM dead_letters").await,0);
    let poison=store.pending_batch(EventPriority::Critical,1,1024*1024).await.unwrap().remove(0).event.event_id;
    store.move_to_dead_letter(poison,"invalid payload").await.unwrap();
    store.requeue_retryable_dead_letters().await.unwrap();
    assert_eq!(scalar(&store,"SELECT count(*) FROM dead_letters").await,1);
}

#[tokio::test]
async fn cancelled_cloud_mutations_rollback_before_the_next_writer() {
    for operation in 0..6 {
        let directory = Directory::new();
        let store = OutboxStore::open_with_physical_capture_budget(directory.path(), budget()).await.unwrap();
        let accepted = event(1);
        store.insert_events(&[accepted.clone()]).await.unwrap();
        if operation == 3 {
            store.connection.execute("INSERT INTO latency_samples(event_id,stage,duration_us,observed_at_us) VALUES('fixture','test',1,?1)",params![now_epoch_micros()]).await.unwrap();
        }
        if operation == 4 { store.mark_remote_committed(&[accepted.event_id],None).await.unwrap(); }
        let signature = |store: OutboxStore| async move {
            let mut values = Vec::new();
            for sql in ["SELECT count(*) FROM outbox_events", "SELECT count(*) FROM outbox_events WHERE state='pending'",
                "SELECT count(*) FROM dead_letters", "SELECT count(*) FROM latency_traces", "SELECT count(*) FROM latency_rollups",
                "SELECT COALESCE(sum(critical_through),0) FROM sync_cursors", "SELECT count(*) FROM reconciliation_jobs"] {
                values.push(scalar(&store,sql).await);
            }
            values
        };
        let before = signature(store.clone()).await;
        let entered = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        *store.mutation_pause.lock() = Some((entered.clone(),resume));
        let write = |store: OutboxStore, accepted: CloudEvent| async move {
            match operation {
                0 => store.move_to_dead_letter(accepted.event_id,"fixture").await,
                1 => store.mark_remote_committed(&[accepted.event_id],Some(1)).await,
                2 => store.record_persistence_acknowledgements(&[accepted],now_epoch_micros()).await,
                3 => store.refresh_latency_rollups().await,
                4 => store.enforce_storage_policy(1).await.map(|_|()),
                _ => store.begin_reconciliation_job(&ReconciliationStart {
                    purpose: aqe_live_protocol::ReconciliationPurpose::Startup, critical_replay_through:None,
                    generation:"fixture-cancel".into(),runtime_id:"runtime".into(),strategy_id:"strategy".into(),
                    live_session_id:"session".into(),run_id:Some("run".into()),snapshot_complete:true,
                    broker_features:Vec::new(),critical_cutoff:Some(1),total_chunks:0,total_records:0,
                }).await,
            }
        };
        let writer = tokio::spawn(write(store.clone(),accepted.clone()));
        tokio::time::timeout(Duration::from_secs(5),entered.notified()).await.unwrap();
        writer.abort();
        assert!(writer.await.unwrap_err().is_cancelled());
        assert_eq!(signature(store.clone()).await,before,"cancelled operation {operation} leaked a partial transaction");
        write(store.clone(),accepted).await.expect("next writer must not inherit a stranded transaction");
    }
}

#[tokio::test]
async fn action_receipts_cannot_grow_pinned_wal_beyond_the_partition() {
    let directory = Directory::new();
    let policy = PhysicalCaptureBudget { physical_limit_bytes: 16*1024*1024, ..budget() };
    let store = OutboxStore::open_with_physical_capture_budget(directory.path(), policy).await.unwrap();
    store.try_reclaim_physical_wal().await.unwrap();
    let reader_db = Builder::new_local(directory.path().to_string_lossy().as_ref()).build().await.unwrap();
    let reader = reader_db.connect().unwrap();
    reader.execute_batch("BEGIN").await.unwrap();
    let mut rows = reader.query("SELECT count(*) FROM processed_actions", ()).await.unwrap();
    rows.next().await.unwrap().unwrap(); drop(rows);
    let mut accepted = 0;
    for i in 0..4096 {
        match store.mark_action_accepted(&format!("action-{i}"), "strategy", "session").await {
            Ok(()) => accepted += 1,
            Err(OutboxStoreError::CapturePaused(_)) => break,
            Err(e) => panic!("{e}"),
        }
    }
    assert!(accepted > 0 && accepted < 4096);
    let bytes = store.physical_capture_usage().await.unwrap().unwrap().actual_bytes();
    for _ in 0..32 {
        assert!(matches!(store.mark_action_accepted("refused", "strategy", "session").await,
            Err(OutboxStoreError::CapturePaused(_))));
    }
    assert_eq!(store.physical_capture_usage().await.unwrap().unwrap().actual_bytes(), bytes);
    assert_eq!(scalar(&store, "SELECT count(*) FROM processed_actions").await, accepted);
    reader.execute_batch("ROLLBACK").await.unwrap();
    store.try_reclaim_physical_wal().await.unwrap();
    store.mark_action_accepted("refused", "strategy", "session").await.unwrap();
    store.mark_action_accepted("refused", "strategy", "session").await.unwrap();
    assert_eq!(scalar(&store, "SELECT count(*) FROM processed_actions").await, accepted + 1);
}

#[tokio::test]
async fn legacy_schema_growth_is_refused_before_commit_and_keeps_accepted_rows() {
    let directory = Directory::new();
    let store = OutboxStore::open(directory.path()).await.unwrap();
    for i in 1..=64 {
        let mut accepted = event(i);
        accepted.payload = serde_json::json!({"fixture":"x".repeat(128*1024)});
        store.insert_events(&[accepted]).await.unwrap();
    }
    store.connection.execute_batch("DROP INDEX idx_outbox_pending;").await.unwrap();
    store.checkpoint_wal(true).await.unwrap();
    drop(store);
    let policy = PhysicalCaptureBudget {
        physical_limit_bytes: MIN_OPERATIONAL_HEADROOM_BYTES+1,
        payload_limit_bytes:32*1024*1024,
        operational_headroom_bytes:MIN_OPERATIONAL_HEADROOM_BYTES,
    };
    for _ in 0..3 {
        assert!(matches!(OutboxStore::open_with_physical_capture_budget(directory.path(),policy).await,
            Err(OutboxStoreError::CapturePaused(_))));
    }
    let database=Builder::new_local(directory.path().to_string_lossy().as_ref()).build().await.unwrap();
    let connection=database.connect().unwrap();
    let mut rows=connection.query("SELECT count(*) FROM outbox_events",()).await.unwrap();
    assert_eq!(rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),64);drop(rows);
    let mut rows=connection.query("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_outbox_pending'",()).await.unwrap();
    assert!(rows.next().await.unwrap().is_none(),"refused legacy DDL must roll back its index, not commit before the physical policy");
}

#[tokio::test]
async fn retention_is_paged_and_does_not_orphan_an_unsettled_safety_receipt() {
    use aqe_live_protocol::SafetyObservation;
    let directory=Directory::new();
    let store=OutboxStore::open_with_physical_capture_budget(directory.path(),budget()).await.unwrap();
    for start in [1,101,201,301,401,501] {
        let batch:Vec<_>=(start..start+100).map(event).collect();
        store.insert_events(&batch).await.unwrap();
        store.mark_remote_committed(&batch.iter().map(|e|e.event_id).collect::<Vec<_>>(),None).await.unwrap();
    }
    assert_eq!(store.prune_committed(0).await.unwrap(),256);
    assert_eq!(store.prune_committed(0).await.unwrap(),256);
    assert_eq!(store.prune_committed(0).await.unwrap(),88);
    let mut stopped=event(1000);stopped.kind=EventKind::StrategyLifecycle;stopped.payload=serde_json::json!({"state":"stopped"});
    store.insert_events(&[stopped.clone()]).await.unwrap();
    store.mark_remote_committed(&[stopped.event_id],None).await.unwrap();
    assert_eq!(store.prune_committed(0).await.unwrap(),0,"retain the handoff witness until its safety receipt commits");
    store.settle_safety_observation(&SafetyObservation::from_event(&stopped).unwrap(),true).await.unwrap();
    assert_eq!(store.prune_committed(0).await.unwrap(),1);
    assert_eq!(scalar(&store,"SELECT count(*) FROM safety_observations").await,0);
}

#[tokio::test]
async fn safety_and_gap_retries_are_bounded_without_losing_receipt_ownership() {
    use aqe_live_protocol::{CaptureGapNotice, CaptureGapReceipt, SafetyObservation};
    for gap in [false, true] {
        let directory = Directory::new();
        let policy = PhysicalCaptureBudget { physical_limit_bytes: 16*1024*1024, ..budget() };
        let store = OutboxStore::open_with_physical_capture_budget(directory.path(),policy).await.unwrap();
        let mut accepted = event(1);
        accepted.kind=EventKind::StrategyLifecycle;
        accepted.payload=serde_json::json!({"state":"stopped"});
        store.insert_events(&[accepted.clone()]).await.unwrap();
        let safety=SafetyObservation::from_event(&accepted).unwrap();
        let mut notice=CaptureGapNotice {version:1,notice_id:uuid::Uuid::nil(),strategy_id:"strategy".into(),live_session_id:"session".into(),
            source_runtime_id:"runtime".into(),source_run_id:Some("run".into()),source_generation:1,started_at_us:1,last_blocked_at_us:2,
            origin_runtime_id:Some("runtime".into()),origin_run_id:Some("run".into()),last_blocked_run_id:Some("run".into()),ownership_handoffs:0};
        notice.notice_id=notice.stable_id();
        store.accept_capture_gap_notice(&notice).await.unwrap();
        store.try_reclaim_physical_wal().await.unwrap();
        let reader_db=Builder::new_local(directory.path().to_string_lossy().as_ref()).build().await.unwrap();
        let reader=reader_db.connect().unwrap();
        reader.execute_batch("BEGIN").await.unwrap();
        let mut rows=reader.query("SELECT count(*) FROM outbox_events",()).await.unwrap();
        rows.next().await.unwrap().unwrap();drop(rows);
        let mut blocked=false;
        for _ in 0..4096 {
            let result=if gap {store.settle_capture_gap_notice(&notice,None).await}
                else {store.settle_safety_observation(&safety,false).await};
            match result {Ok(())=>{},Err(OutboxStoreError::CapturePaused(_))=>{blocked=true;break;},Err(e)=>panic!("{e:?}")}
        }
        assert!(blocked,"operational retry WAL must be bounded");
        let before=store.physical_capture_usage().await.unwrap().unwrap().actual_bytes();
        assert!(before<policy.physical_limit_bytes);
        for _ in 0..32 {
            let result=if gap {store.settle_capture_gap_notice(&notice,Some(&CaptureGapReceipt::for_notice(&notice))).await}
                else {store.settle_safety_observation(&safety,true).await};
            assert!(matches!(result,Err(OutboxStoreError::CapturePaused(_))));
        }
        assert_eq!(store.physical_capture_usage().await.unwrap().unwrap().actual_bytes(),before);
        for _ in 0..8 {
            assert!(matches!(OutboxStore::open_with_physical_capture_budget(directory.path(),policy).await,
                Err(OutboxStoreError::CapturePaused(_))));
        }
        assert_eq!(store.physical_capture_usage().await.unwrap().unwrap().actual_bytes(),before,"refused recovery reopens must not append WAL frames");
        assert_eq!(scalar(&store,"SELECT count(*) FROM capture_gap_notices").await,1);
        assert_eq!(scalar(&store,"SELECT delivered FROM safety_observations").await,0);
        assert_eq!(store.pending_count().await.unwrap(),1,"full accepted evidence is retained");
        reader.execute_batch("ROLLBACK").await.unwrap();
        store.try_reclaim_physical_wal().await.unwrap();
        store.settle_capture_gap_notice(&notice,Some(&CaptureGapReceipt::for_notice(&notice))).await.unwrap();
        store.settle_safety_observation(&safety,true).await.unwrap();
        assert_eq!(scalar(&store,"SELECT count(*) FROM capture_gap_notices").await,0);
        assert_eq!(scalar(&store,"SELECT delivered FROM safety_observations").await,1);
        assert_eq!(store.pending_count().await.unwrap(),1);
    }
}

#[tokio::test]
async fn pinned_reader_cannot_grow_optional_latency_metadata_past_data_ceiling() {
    let directory = Directory::new();
    let policy = PhysicalCaptureBudget {
        payload_limit_bytes: 4 * 1024 * 1024,
        physical_limit_bytes: 16 * 1024 * 1024,
        ..budget()
    };
    let store = OutboxStore::open_with_physical_capture_budget(directory.path(), policy).await.unwrap();
    let accepted = event(1);
    store.insert_events(&[accepted.clone()]).await.unwrap();
    store.try_reclaim_physical_wal().await.unwrap();
    let reader_db = Builder::new_local(directory.path().to_string_lossy().as_ref()).build().await.unwrap();
    let reader = reader_db.connect().unwrap();
    reader.execute_batch("BEGIN").await.unwrap();
    let mut rows = reader.query("SELECT count(*) FROM outbox_events",()).await.unwrap();
    assert_eq!(rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),1);
    drop(rows);
    let mut blocked = false;
    for step in 0..4096 {
        match store.record_persistence_acknowledgements(&[accepted.clone()],now_epoch_micros()+step).await {
            Ok(()) => {},
            Err(OutboxStoreError::CapturePaused(_)) => { blocked=true; break; },
            Err(error) => panic!("unexpected metadata refusal: {error:?}"),
        }
    }
    assert!(blocked,"pinned metadata writer must stop before its data ceiling");
    let before = store.physical_capture_usage().await.unwrap().unwrap();
    assert!(before.actual_bytes() < policy.physical_limit_bytes);
    for _ in 0..128 {
        assert!(matches!(store.record_persistence_acknowledgements(&[accepted.clone()],now_epoch_micros()).await,Err(OutboxStoreError::CapturePaused(_))));
    }
    assert_eq!(store.physical_capture_usage().await.unwrap().unwrap().actual_bytes(),before.actual_bytes(),"refused diagnostics cannot append WAL frames");
    assert_eq!(store.pending_count().await.unwrap(),1,"best-effort diagnostics never discard accepted evidence");
    assert!(store.latest_capture_pause().await.unwrap().is_none(),"refused instrumentation is not a history gap");
    // A real handoff may still spend the operational headroom. The accepted
    // event changes state only after its exact durable transport receipt.
    store.mark_remote_committed(&[accepted.event_id],Some(1)).await.unwrap();
    assert_eq!(store.pending_count().await.unwrap(),0);
    reader.execute_batch("ROLLBACK").await.unwrap();
    assert!(store.try_reclaim_physical_wal().await.unwrap());
}

#[tokio::test]
async fn publisher_and_receiver_retry_metadata_stop_growing_under_pinned_readers() {
    let directory = Directory::new();
    let policy = PhysicalCaptureBudget {
        physical_limit_bytes: 16 * 1024 * 1024,
        ..budget()
    };
    let mut combined = 0;
    for name in ["publisher.db", "receiver.db"] {
        let path = directory.0.join(name);
        let store = OutboxStore::open_with_physical_capture_budget(&path,policy).await.unwrap();
        let accepted = event(1);
        store.insert_events(&[accepted.clone()]).await.unwrap();
        store.try_reclaim_physical_wal().await.unwrap();
        let reader_db = Builder::new_local(path.to_string_lossy().as_ref()).build().await.unwrap();
        let reader = reader_db.connect().unwrap();
        reader.execute_batch("BEGIN").await.unwrap();
        let mut rows = reader.query("SELECT count(*) FROM outbox_events",()).await.unwrap();
        assert_eq!(rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),1);
        drop(rows);
        let mut blocked = false;
        for _ in 0..4096 {
            match store.schedule_retry(&[accepted.event_id],60,"cloud unavailable").await {
                Ok(()) => {},
                Err(OutboxStoreError::CapturePaused(_)) => { blocked=true; break; },
                Err(error) => panic!("unexpected retry error: {error:?}"),
            }
        }
        assert!(blocked,"{name}: retry metadata must not extend WAL indefinitely");
        let before = store.physical_capture_usage().await.unwrap().unwrap();
        assert!(before.actual_bytes() <= policy.physical_limit_bytes);
        combined += before.actual_bytes();
        let attempts = scalar(&store,"SELECT attempt_count FROM outbox_events LIMIT 1").await;
        for _ in 0..128 {
            assert!(matches!(store.schedule_retry(&[accepted.event_id],60,"cloud unavailable").await,Err(OutboxStoreError::CapturePaused(_))));
        }
        assert_eq!(scalar(&store,"SELECT attempt_count FROM outbox_events LIMIT 1").await,attempts,"refused retries roll back metadata too");
        assert_eq!(store.physical_capture_usage().await.unwrap().unwrap().actual_bytes(),before.actual_bytes());
        assert_eq!(store.pending_count().await.unwrap(),1);
        assert!(store.latest_capture_pause().await.unwrap().is_none());
        reader.execute_batch("ROLLBACK").await.unwrap();
        assert!(store.try_reclaim_physical_wal().await.unwrap());
        store.mark_remote_committed(&[accepted.event_id],Some(1)).await.unwrap();
        assert_eq!(store.pending_count().await.unwrap(),0);
    }
    assert!(combined <= policy.physical_limit_bytes*2);
}

#[tokio::test]
async fn a_transaction_that_crosses_the_physical_limit_never_spills_its_payload() {
    let directory = Directory::new();
    let mut store = OutboxStore::open_with_physical_capture_budget(directory.path(), budget())
        .await
        .unwrap();
    store.try_reclaim_physical_wal().await.unwrap();
    let before = store.physical_capture_usage().await.unwrap().unwrap();
    let mut policy = budget();
    policy.physical_limit_bytes =
        policy.operational_headroom_bytes + before.projected_commit_bytes + 64 * 1024;
    store.physical_budget = Some(policy);
    assert!(
        !store
            .physical_capture_usage()
            .await
            .unwrap()
            .unwrap()
            .exceeds_data_limit()
    );
    let mut large = event(1);
    large.payload = serde_json::json!({"data":"x".repeat(128*1024)});
    assert!(
        matches!(store.reserve_publisher_events(&[large],0).await,Err(OutboxStoreError::CapturePaused(capacity)) if capacity.physical.unwrap().exceeds_data_limit() && capacity.retained_bytes==0)
    );
    assert_eq!(store.pending_count().await.unwrap(), 0);
    assert_eq!(
        scalar(&store, "SELECT count(*) FROM publisher_event_reservations").await,
        0
    );
    assert_eq!(
        store
            .critical_sequence_cursor_for_runtime_scope("runtime", "strategy", "session")
            .await
            .unwrap(),
        0
    );
    let after = store.physical_capture_usage().await.unwrap().unwrap();
    assert!(
        after.wal_bytes < 256 * 1024,
        "refused data was not spilled; only the compact pause was committed"
    );
}

#[tokio::test]
async fn independent_writers_serialize_physical_admission_before_acceptance() {
    let directory = Directory::new();
    let policy = PhysicalCaptureBudget {
        payload_limit_bytes: 16 * 1024 * 1024,
        physical_limit_bytes: 12 * 1024 * 1024,
        ..budget()
    };
    let a = OutboxStore::open_with_physical_capture_budget(directory.path(), policy)
        .await
        .unwrap();
    let b = OutboxStore::open_with_physical_capture_budget(directory.path(), policy)
        .await
        .unwrap();
    a.try_reclaim_physical_wal().await.unwrap();
    let batch = |from| {
        (from..from + 8)
            .map(|revision| {
                let mut event = event(revision);
                event.payload = serde_json::json!({"data":"x".repeat(128*1024)});
                event
            })
            .collect::<Vec<_>>()
    };
    let first = batch(1);
    let second = batch(9);
    let (first, second) = tokio::join!(
        a.reserve_publisher_events(&first, 0),
        b.reserve_publisher_events(&second, 0)
    );
    assert_eq!(
        usize::from(first.is_ok()) + usize::from(second.is_ok()),
        1,
        "{first:?} {second:?}"
    );
    assert!(
        matches!(first, Err(OutboxStoreError::CapturePaused(_)))
            || matches!(second, Err(OutboxStoreError::CapturePaused(_)))
    );
    assert_eq!(a.pending_count().await.unwrap(), 8);
    assert_eq!(
        scalar(&a, "SELECT count(*) FROM publisher_event_reservations").await,
        8
    );
}

#[tokio::test]
async fn a_deferred_legacy_identity_upgrade_is_not_a_new_history_gap() {
    let directory = Directory::new();
    let mut store = OutboxStore::open_with_physical_capture_budget(directory.path(), budget())
        .await
        .unwrap();
    let mut legacy = event(1);
    legacy.critical_sequence = Some(1);
    store.insert_events(&[legacy.clone()]).await.unwrap();
    pause_for_physical_pressure(&mut store).await;
    legacy.critical_sequence = None;
    assert!(
        matches!(store.reserve_publisher_events(&[legacy],0).await,Err(OutboxStoreError::CapturePaused(capacity)) if capacity.requested_bytes==0)
    );
    assert!(store.latest_capture_pause().await.unwrap().is_none());
    assert_eq!(
        scalar(&store, "SELECT count(*) FROM publisher_event_reservations").await,
        0
    );
    assert_eq!(store.pending_count().await.unwrap(), 1);
}

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("aqe-physical-capture-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> PathBuf {
        self.0.join("outbox.db")
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn budget() -> PhysicalCaptureBudget {
    PhysicalCaptureBudget {
        payload_limit_bytes: 4 * 1024 * 1024,
        physical_limit_bytes: 64 * 1024 * 1024,
        operational_headroom_bytes: MIN_OPERATIONAL_HEADROOM_BYTES,
    }
}
fn event(revision: u64) -> CloudEvent {
    let mut event = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        format!("entity:{revision}"),
        revision,
        EventPriority::Critical,
        EventKind::InsightProjection,
        serde_json::json!({"data":"x".repeat(2048)}),
    );
    event.run_id = Some("run".into());
    event
}
async fn pause_for_physical_pressure(store: &mut OutboxStore) {
    store.try_reclaim_physical_wal().await.unwrap();
    let usage = store.physical_capture_usage().await.unwrap().unwrap();
    let mut policy = store.physical_capture_budget().unwrap();
    policy.physical_limit_bytes =
        policy.operational_headroom_bytes + usage.projected_commit_bytes - 1;
    store.physical_budget = Some(policy.validate().unwrap());
}
async fn scalar(store: &OutboxStore, sql: &str) -> i64 {
    store
        .connection
        .query(sql, ())
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap()
        .get(0)
        .unwrap()
}

#[tokio::test]
async fn physical_policy_is_explicit_validated_and_spill_control_is_verified() {
    let directory = Directory::new();
    let plain = OutboxStore::open(directory.path()).await.unwrap();
    assert!(plain.physical_capture_budget().is_none());
    assert!(plain.physical_capture_usage().await.unwrap().is_none());
    drop(plain);
    for invalid in [
        PhysicalCaptureBudget {
            payload_limit_bytes: 0,
            ..budget()
        },
        PhysicalCaptureBudget {
            operational_headroom_bytes: 1024,
            ..budget()
        },
        PhysicalCaptureBudget {
            physical_limit_bytes: MIN_OPERATIONAL_HEADROOM_BYTES,
            ..budget()
        },
    ] {
        assert!(
            OutboxStore::open_with_physical_capture_budget(directory.path(), invalid)
                .await
                .is_err()
        );
    }
    let store = OutboxStore::open_with_physical_capture_budget(directory.path(), budget())
        .await
        .unwrap();
    assert_eq!(store.physical_capture_budget(), Some(budget()));
    assert_eq!(scalar(&store, "PRAGMA cache_spill").await, 0);
    let usage = store.physical_capture_usage().await.unwrap().unwrap();
    assert!(usage.database_bytes > 0);
    assert!(usage.logical_database_bytes > 0);
    assert!(usage.projected_commit_bytes > usage.actual_bytes());
    assert_eq!(store.health().await.unwrap().physical_capture, Some(usage));
    drop(store);
    let plain = OutboxStore::open(directory.path()).await.unwrap();
    assert!(
        plain.physical_capture_budget().is_none(),
        "an old process's policy is not silently inherited"
    );
}

#[tokio::test]
async fn physical_refusal_rolls_back_payload_identity_and_cursor_but_retains_exact_retries() {
    let directory = Directory::new();
    let mut store = OutboxStore::open_with_physical_capture_budget(directory.path(), budget())
        .await
        .unwrap();
    let first = event(1);
    let saved = store
        .reserve_publisher_events(&[first.clone()], 0)
        .await
        .unwrap();
    assert_eq!(saved[0].critical_sequence, Some(1));
    let retained = store.capture_retained_bytes().await.unwrap();
    pause_for_physical_pressure(&mut store).await;
    let refusal = store
        .reserve_publisher_events(&[event(2)], 1)
        .await
        .unwrap_err();
    let OutboxStoreError::CapturePaused(capacity) = refusal else {
        panic!("{refusal:?}")
    };
    assert_eq!(capacity.retained_bytes, retained);
    assert!(capacity.requested_bytes > 0);
    assert!(capacity.physical.unwrap().exceeds_data_limit());
    assert_eq!(store.capture_retained_bytes().await.unwrap(), retained);
    assert_eq!(store.pending_count().await.unwrap(), 1);
    assert_eq!(
        scalar(&store, "SELECT count(*) FROM publisher_event_reservations").await,
        1
    );
    assert_eq!(
        store
            .critical_sequence_cursor_for_runtime_scope("runtime", "strategy", "session")
            .await
            .unwrap(),
        1
    );
    assert!(store.latest_capture_pause().await.unwrap().is_some());
    assert!(
        store
            .reserve_publisher_events(&[first], 1)
            .await
            .unwrap()
            .is_empty(),
        "an exact accepted retry is not new capture"
    );
    store.physical_budget = Some(budget());
    let next = store
        .reserve_publisher_events(&[event(2)], 1)
        .await
        .unwrap();
    assert_eq!(
        next[0].critical_sequence,
        Some(2),
        "a refused transaction never consumes a sequence"
    );
}

#[tokio::test]
async fn accepted_handoff_keeps_source_ownership_and_does_not_create_a_history_gap() {
    let directory = Directory::new();
    let mut store = OutboxStore::open_with_physical_capture_budget(directory.path(), budget())
        .await
        .unwrap();
    let mut first = event(1);
    first.critical_sequence = Some(1);
    store
        .handoff_accepted_events(&[first.clone()])
        .await
        .unwrap();
    let retained = store.capture_retained_bytes().await.unwrap();
    pause_for_physical_pressure(&mut store).await;
    let mut second = event(2);
    second.critical_sequence = Some(2);
    let (accepted, deferred) = store
        .handoff_accepted_prefix_with_capacity(&[second.clone()], budget().payload_limit_bytes)
        .await
        .unwrap();
    assert_eq!(accepted, 0);
    assert!(deferred.unwrap().physical.is_some());
    assert!(store.latest_capture_pause().await.unwrap().is_none());
    assert_eq!(store.capture_retained_bytes().await.unwrap(), retained);
    assert_eq!(store.handoff_accepted_events(&[first]).await.unwrap(), 0);
    assert!(
        matches!(store.insert_events(&[second]).await,Err(OutboxStoreError::CapturePaused(capacity)) if capacity.retained_bytes==retained)
    );
    assert_eq!(store.pending_count().await.unwrap(), 1);
}

#[tokio::test]
async fn pruned_payloads_do_not_hide_durable_identity_overhead_or_resume_capture() {
    let directory = Directory::new();
    let store = OutboxStore::open_with_physical_capture_budget(directory.path(), budget())
        .await
        .unwrap();
    let events = (1..=32).map(event).collect::<Vec<_>>();
    let accepted = store.reserve_publisher_events(&events, 0).await.unwrap();
    store
        .mark_remote_committed(
            &accepted
                .iter()
                .map(|event| event.event_id)
                .collect::<Vec<_>>(),
            Some(32),
        )
        .await
        .unwrap();
    assert_eq!(store.prune_committed(0).await.unwrap(), 32);
    assert_eq!(store.capture_retained_bytes().await.unwrap(), 0);
    assert_eq!(
        scalar(&store, "SELECT count(*) FROM publisher_event_reservations").await,
        32
    );
    store.try_reclaim_physical_wal().await.unwrap();
    let usage = store.physical_capture_usage().await.unwrap().unwrap();
    drop(store);
    let small = PhysicalCaptureBudget {
        physical_limit_bytes: MIN_OPERATIONAL_HEADROOM_BYTES + usage.projected_commit_bytes - 1,
        ..budget()
    };
    let store = OutboxStore::open_with_physical_capture_budget(directory.path(), small)
        .await
        .unwrap();
    assert!(matches!(
        store.reserve_publisher_events(&[event(33)], 32).await,
        Err(OutboxStoreError::CapturePaused(_))
    ));
    let publisher = crate::core::live_sync::LocalEventPublisher::spawn(
        Arc::new(crate::core::live_sync::InProcessEventBus::default()),
        Some(store.clone()),
        Default::default(),
    );
    assert!(
        publisher
            .wait_for_capture_initialization(Duration::from_secs(3))
            .await
    );
    let status = publisher.cloud_capture_status();
    assert_eq!(
        status.state,
        crate::core::live_sync::CloudCaptureState::Paused
    );
    assert_eq!(status.retained_bytes, 0);
    assert!(status.physical.unwrap().exceeds_data_limit());
    assert!(publisher.cloud_capture_paused());
    assert!(!publisher.admission_failed());
    publisher.shutdown(Duration::from_secs(2)).await;
    assert_eq!(
        scalar(&store, "SELECT count(*) FROM publisher_event_reservations").await,
        32
    );
}

#[tokio::test]
async fn exhausted_data_admission_keeps_compact_stop_and_gap_mailboxes_available() {
    let directory = Directory::new();
    let mut store = OutboxStore::open_with_physical_capture_budget(directory.path(), budget())
        .await
        .unwrap();
    pause_for_physical_pressure(&mut store).await;
    assert!(matches!(
        store
            .capture_events_with_capacity(&[event(1)], budget().payload_limit_bytes)
            .await,
        Err(OutboxStoreError::CapturePaused(_))
    ));
    let facts=(0..256).map(|index| {
        let mut event=event(index+10);event.kind=EventKind::StrategyActionAcknowledgement;
        event.payload=serde_json::json!({"action":"stop","action_id":format!("strategy_actions:{index}"),"status":"acknowledged"});
        aqe_live_protocol::SafetyObservation::from_event(&event).unwrap()
    }).collect::<Vec<_>>();
    store.record_safety_only(&facts).await.unwrap();
    let mut stopped = event(999);
    stopped.kind = EventKind::StrategyLifecycle;
    stopped.payload = serde_json::json!({"state":"stopped"});
    store
        .record_safety_only(&[aqe_live_protocol::SafetyObservation::from_event(&stopped).unwrap()])
        .await
        .unwrap();
    let notice = store
        .local_capture_gap_notices("strategy", "session")
        .await
        .unwrap()
        .remove(0);
    assert!(store.accept_capture_gap_notice(&notice).await.unwrap());
    assert_eq!(
        scalar(&store, "SELECT count(*) FROM safety_observations").await,
        257
    );
    assert_eq!(
        store
            .pending_capture_gap_notices("strategy")
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(store.pending_count().await.unwrap(), 0);
    assert_eq!(store.capture_retained_bytes().await.unwrap(), 0);
    let usage = store.physical_capture_usage().await.unwrap().unwrap();
    assert!(
        usage.actual_bytes() < usage.limit_bytes,
        "fixture operational facts fit the reserved headroom"
    );
}

#[tokio::test]
async fn missing_footprint_fails_closed_and_wal_reclaim_does_not_delete_data() {
    let directory = Directory::new();
    let mut store = OutboxStore::open_with_physical_capture_budget(directory.path(), budget())
        .await
        .unwrap();
    store
        .reserve_publisher_events(&[event(1)], 0)
        .await
        .unwrap();
    let before = store.physical_capture_usage().await.unwrap().unwrap();
    assert!(store.try_reclaim_physical_wal().await.unwrap());
    let after = store.physical_capture_usage().await.unwrap().unwrap();
    assert_eq!(store.pending_count().await.unwrap(), 1);
    assert!(after.wal_bytes < before.wal_bytes);
    let path = store.path.clone();
    store.path = directory.0.join("missing.db");
    assert!(store.physical_capture_usage().await.is_err());
    assert!(
        store
            .reserve_publisher_events(&[event(2)], 1)
            .await
            .is_err()
    );
    store.path = path;
    assert_eq!(store.pending_count().await.unwrap(), 1);
    assert_eq!(
        store
            .critical_sequence_cursor_for_runtime_scope("runtime", "strategy", "session")
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn external_reader_cannot_make_pressure_reclamation_wait_for_busy_timeout() {
    let directory = Directory::new();
    let store = OutboxStore::open_with_physical_capture_budget(directory.path(), budget())
        .await
        .unwrap();
    store
        .reserve_publisher_events(&[event(1)], 0)
        .await
        .unwrap();
    let database = Builder::new_local(directory.path().to_str().unwrap())
        .build()
        .await
        .unwrap();
    let reader = database.connect().unwrap();
    reader.execute_batch("BEGIN").await.unwrap();
    let mut rows = reader
        .query("SELECT count(*) FROM outbox_events", ())
        .await
        .unwrap();
    assert!(rows.next().await.unwrap().is_some());
    drop(rows);
    store
        .reserve_publisher_events(&[event(2)], 1)
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_millis(750), store.try_reclaim_physical_wal())
        .await
        .unwrap()
        .unwrap();
    assert!(
        !result,
        "a pinned snapshot must not be reported as a completed truncation"
    );
    reader.execute_batch("ROLLBACK").await.unwrap();
    assert!(store.try_reclaim_physical_wal().await.unwrap());
    assert_eq!(store.pending_count().await.unwrap(), 2);
}

#[tokio::test]
async fn repeated_exact_handoffs_do_not_append_health_only_wal_frames() {
    let directory = Directory::new();
    let store = OutboxStore::open_with_physical_capture_budget(directory.path(), budget())
        .await
        .unwrap();
    let mut first = event(1);
    first.critical_sequence = Some(1);
    store
        .handoff_accepted_events(&[first.clone()])
        .await
        .unwrap();
    let before = store.physical_capture_usage().await.unwrap().unwrap();
    for _ in 0..100 {
        assert_eq!(
            store
                .handoff_accepted_events(&[first.clone()])
                .await
                .unwrap(),
            0
        );
    }
    let after = store.physical_capture_usage().await.unwrap().unwrap();
    assert_eq!(after.wal_bytes, before.wal_bytes);
    assert_eq!(after.logical_database_bytes, before.logical_database_bytes);
}

#[tokio::test]
async fn rejected_payload_diagnostics_cannot_bypass_physical_admission() {
    let directory = Directory::new();
    let mut store = OutboxStore::open_with_physical_capture_budget(directory.path(), budget())
        .await
        .unwrap();
    let original = event(1);
    store
        .reserve_publisher_events(&[original.clone()], 0)
        .await
        .unwrap();
    pause_for_physical_pressure(&mut store).await;
    let retained = store.capture_retained_bytes().await.unwrap();
    let mut changed = original;
    changed.payload = serde_json::json!({"changed":true});
    assert!(
        matches!(store.reject_publisher_event(&changed,"identity conflict").await,Err(OutboxStoreError::CapturePaused(capacity)) if capacity.physical.is_some())
    );
    assert_eq!(store.health().await.unwrap().dead_letters, 0);
    assert_eq!(store.capture_retained_bytes().await.unwrap(), retained);
    assert_eq!(store.pending_count().await.unwrap(), 1);
}

#[tokio::test]
async fn physical_factory_payload_ceiling_cannot_be_bypassed_by_older_entry_points() {
    for route in 0..5 {
        let directory = Directory::new();
        let policy = PhysicalCaptureBudget {
            payload_limit_bytes: 1024,
            ..budget()
        };
        let store = OutboxStore::open_with_physical_capture_budget(directory.path(), policy)
            .await
            .unwrap();
        let mut large = event(1);
        let denied = match route {
            0 => matches!(
                store.insert_events(&[large]).await,
                Err(OutboxStoreError::CapturePaused(_))
            ),
            1 => matches!(
                store.reserve_publisher_events(&[large], 0).await,
                Err(OutboxStoreError::CapturePaused(_))
            ),
            2 => matches!(
                store.handoff_accepted_events(&[large]).await,
                Err(OutboxStoreError::CapturePaused(_))
            ),
            3 => {
                large.priority = EventPriority::Telemetry;
                matches!(
                    store.insert_telemetry_latest(&large).await,
                    Err(OutboxStoreError::CapturePaused(_))
                )
            }
            4 => {
                let result = store
                    .handoff_accepted_prefix_with_capacity(&[large], u32::MAX as u64)
                    .await
                    .unwrap();
                result.0 == 0 && result.1.is_some()
            }
            _ => unreachable!(),
        };
        assert!(
            denied,
            "entry point {route} must use the configured payload ceiling"
        );
        assert_eq!(store.pending_count().await.unwrap(), 0);
        assert_eq!(store.capture_retained_bytes().await.unwrap(), 0);
        assert_eq!(
            scalar(&store, "SELECT count(*) FROM publisher_event_reservations").await,
            0
        );
    }
}
