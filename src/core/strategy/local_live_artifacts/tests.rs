use super::*;

pub(super) async fn fixture() -> LocalLiveRunArtifacts {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let artifacts = LocalLiveRunArtifacts {
        run_id: "test-run".into(),
        db_path: PathBuf::from(":memory:"),
        conn: db.connect().unwrap(),
        event_conn: db.connect().unwrap(),
        event_writes: tokio::task::JoinSet::new(),
        event_ready: Arc::new(tokio::sync::Notify::new()),
        event_retry_at: Instant::now(),
        metrics_conn: Some(db.connect().unwrap()),
        metrics_writes: tokio::task::JoinSet::new(),
        metrics_inflight: None,
        metrics_pending: None,
        metrics_retry_at: Instant::now(),
        last_metrics: None,
    };
    artifacts.init_schema().await.unwrap();
    artifacts
}

pub(super) fn event(index: usize) -> PendingLocalLiveEvent {
    PendingLocalLiveEvent {
        event_id: format!("test-event-{index}"),
        created_at: chrono::DateTime::from_timestamp(1_700_000_000 + index as i64, 0).unwrap(),
        event_type: StrategyEventType::TradeEvent,
        level: StrategyEventLevel::Info,
        title: "Broker trade event".into(),
        message: format!("event {index}"),
        details: LocalLiveEventDetails {
            symbol: Some("ETHUSD".into()),
            ..Default::default()
        },
    }
}

async fn count(artifacts: &LocalLiveRunArtifacts) -> i64 {
    artifacts
        .conn
        .query("SELECT count(*) FROM events", ())
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap()
        .get::<i64>(0)
        .unwrap()
}

#[tokio::test]
async fn local_event_batches_are_bounded_ordered_and_preserve_capture_time() {
    let artifacts = fixture().await;
    let mut pending = (0..130).map(event).collect::<VecDeque<_>>();
    assert_eq!(
        flush_pending_local_live_events(&artifacts, &mut pending)
            .await
            .unwrap(),
        64
    );
    assert_eq!(pending.len(), 66);
    assert_eq!(count(&artifacts).await, 64);
    let first = event(0);
    artifacts.append_event_batch(&[&first]).await.unwrap();
    assert_eq!(
        count(&artifacts).await,
        64,
        "same immutable retry is idempotent"
    );
    while !pending.is_empty() {
        flush_pending_local_live_events(&artifacts, &mut pending)
            .await
            .unwrap();
    }
    let mut rows = artifacts
        .conn
        .query("SELECT event_key,created_at FROM events ORDER BY id", ())
        .await
        .unwrap();
    for index in 0..130 {
        let row = rows.next().await.unwrap().unwrap();
        assert_eq!(row.get::<String>(0).unwrap(), event(index).event_id);
        assert_eq!(
            row.get::<String>(1).unwrap(),
            event(index).created_at.to_rfc3339()
        );
    }
    assert!(rows.next().await.unwrap().is_none());
}

#[tokio::test]
async fn local_event_batch_failure_retains_queue_and_rolls_back_all_rows() {
    let artifacts = fixture().await;
    artifacts.conn.execute_batch("CREATE TRIGGER fail_test_event BEFORE INSERT ON events WHEN NEW.event_key = 'test-event-1' BEGIN SELECT RAISE(ABORT, 'fake disk constraint'); END;").await.unwrap();
    let mut pending = (0..3).map(event).collect::<VecDeque<_>>();
    assert!(
        flush_pending_local_live_events(&artifacts, &mut pending)
            .await
            .is_err()
    );
    assert_eq!(pending.len(), 3);
    assert_eq!(count(&artifacts).await, 0);
    artifacts
        .conn
        .execute_batch("DROP TRIGGER fail_test_event;")
        .await
        .unwrap();
    assert_eq!(
        flush_pending_local_live_events(&artifacts, &mut pending)
            .await
            .unwrap(),
        3
    );
    assert!(pending.is_empty());
}

#[tokio::test]
async fn local_event_conflicting_retry_preserves_original_and_rejects_whole_batch() {
    let artifacts = fixture().await;
    artifacts.append_event_batch(&[&event(0)]).await.unwrap();
    let mut changed = event(0);
    changed.message = "different immutable payload".into();
    let mut pending = VecDeque::from([event(1), changed]);
    assert!(
        flush_pending_local_live_events(&artifacts, &mut pending)
            .await
            .is_err()
    );
    assert_eq!(pending.len(), 2);
    assert_eq!(count(&artifacts).await, 1);
    let row = artifacts
        .conn
        .query("SELECT message FROM events", ())
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.get::<String>(0).unwrap(), event(0).message);
    pending[1] = event(0);
    assert_eq!(
        flush_pending_local_live_events(&artifacts, &mut pending)
            .await
            .unwrap(),
        2
    );
    assert_eq!(count(&artifacts).await, 2);
}

#[tokio::test]
async fn local_event_byte_budget_bounds_batches_without_discarding_large_events() {
    let artifacts = fixture().await;
    let mut first = event(0);
    first.message = "x".repeat(130 * 1024);
    let mut second = event(1);
    second.message = first.message.clone();
    let mut pending = VecDeque::from([first, second]);
    assert_eq!(
        flush_pending_local_live_events(&artifacts, &mut pending)
            .await
            .unwrap(),
        1
    );
    assert_eq!(pending.len(), 1);
    pending[0].message = "x".repeat(MAX_LOCAL_EVENT_BATCH_BYTES);
    assert!(
        flush_pending_local_live_events(&artifacts, &mut pending)
            .await
            .is_err()
    );
    assert_eq!(pending.len(), 1);
    assert_eq!(count(&artifacts).await, 1);
}

#[test]
fn local_event_queue_capacity_returns_unaccepted_event_without_eviction() {
    let mut pending = (0..MAX_PENDING_LOCAL_LIVE_EVENTS)
        .map(event)
        .collect::<VecDeque<_>>();
    let refused = enqueue_pending_local_live_event(&mut pending, event(999)).unwrap_err();
    assert_eq!(refused.event_id, "test-event-999");
    assert_eq!(pending.len(), MAX_PENDING_LOCAL_LIVE_EVENTS);
    assert_eq!(pending.front().unwrap().event_id, "test-event-0");
    assert_eq!(pending.back().unwrap().event_id, "test-event-511");
}

#[tokio::test]
async fn local_event_worker_slow_write_and_cancelled_drain_keep_one_owned_batch() {
    let mut artifacts = fixture().await;
    let mut pending = (0..65).map(event).collect::<VecDeque<_>>();
    let batch = pending.iter().take(64).cloned().collect::<Vec<_>>();
    let ids = batch.iter().map(|event| event.event_id.clone()).collect();
    let conn = artifacts.event_conn.clone();
    let (release, wait) = tokio::sync::oneshot::channel();
    artifacts.event_writes.spawn(async move {
        wait.await.unwrap();
        let result = LocalLiveRunArtifacts::write_event_batch(
            &conn,
            std::path::Path::new(":memory:"),
            &batch.iter().collect::<Vec<_>>(),
        )
        .await;
        (ids, result)
    });
    for _ in 0..10 {
        artifacts.poll_event_writes(&mut pending).unwrap();
    }
    assert_eq!(artifacts.event_writes.len(), 1);
    assert_eq!(pending.len(), 65);
    {
        let drain = artifacts.drain_event_writes(&mut pending);
        tokio::pin!(drain);
        assert!(futures::poll!(&mut drain).is_pending());
    }
    assert_eq!(artifacts.event_writes.len(), 1);
    assert_eq!(pending.len(), 65);
    tokio::time::timeout(Duration::from_secs(1), tokio::task::yield_now())
        .await
        .unwrap();
    release.send(()).unwrap();
    artifacts.drain_event_writes(&mut pending).await.unwrap();
    assert!(pending.is_empty());
    assert!(artifacts.event_writes.is_empty());
    assert_eq!(count(&artifacts).await, 65);
}

#[tokio::test]
async fn local_event_worker_retries_lost_commit_ack_without_duplicates() {
    let mut artifacts = fixture().await;
    let mut pending = (0..3).map(event).collect::<VecDeque<_>>();
    artifacts
        .append_event_batch(&pending.iter().collect::<Vec<_>>())
        .await
        .unwrap();
    let ids = pending.iter().map(|event| event.event_id.clone()).collect();
    artifacts.event_writes.spawn(async move {
        (
            ids,
            Err(BrokerError::ConnectionError(
                "simulated lost commit acknowledgement".into(),
            )),
        )
    });
    assert!(artifacts.drain_event_writes(&mut pending).await.is_err());
    assert_eq!(pending.len(), 3);
    assert_eq!(count(&artifacts).await, 3);
    artifacts.event_retry_at = Instant::now();
    artifacts.drain_event_writes(&mut pending).await.unwrap();
    assert!(pending.is_empty());
    assert_eq!(count(&artifacts).await, 3);
}

#[tokio::test]
async fn local_event_worker_drop_aborts_owned_task_instead_of_detaching() {
    let mut artifacts = fixture().await;
    let retained = Arc::new(());
    let held = retained.clone();
    artifacts.event_writes.spawn(async move {
        let _held = held;
        std::future::pending().await
    });
    drop(artifacts);
    tokio::task::yield_now().await;
    assert_eq!(Arc::strong_count(&retained), 1);
}
