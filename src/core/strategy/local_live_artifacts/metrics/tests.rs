use super::super::tests::fixture;
use super::*;
use crate::core::strategy::live_metrics::LivePerformanceTracker;

fn snapshot(equity: f64, symbols: usize) -> LiveMetricsSnapshot {
    let mut tracker = LivePerformanceTracker::default();
    tracker.initialize(
        100_000.0,
        equity,
        chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
        (0..symbols).map(|index| format!("SYM{index:04}")),
    );
    tracker.snapshot().unwrap()
}

async fn scalar(conn: &Connection, query: &str) -> i64 {
    conn.query(query, ())
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
async fn local_metrics_transaction_commits_all_symbol_chunks_and_retry_is_idempotent() {
    let mut artifacts = fixture().await;
    let job = MetricsWrite {
        key: "same-job".into(),
        snapshot: Arc::new(snapshot(101_000.0, 600)),
    };
    let conn = artifacts.metrics_conn.as_mut().unwrap();
    for _ in 0..2 {
        LocalLiveRunArtifacts::persist_metrics_job(
            conn,
            &artifacts.db_path,
            &artifacts.run_id,
            &job,
        )
        .await
        .unwrap();
    }
    assert_eq!(
        scalar(conn, "SELECT count(*) FROM metrics_snapshots").await,
        1
    );
    assert_eq!(
        scalar(conn, "SELECT count(*) FROM metrics_snapshot_symbols").await,
        600
    );
}

#[tokio::test]
async fn local_metrics_symbol_failure_rolls_back_snapshot_and_earlier_chunks() {
    let mut artifacts = fixture().await;
    let conn = artifacts.metrics_conn.as_mut().unwrap();
    conn.execute_batch("CREATE TRIGGER fail_symbol BEFORE INSERT ON metrics_snapshot_symbols WHEN NEW.symbol = 'SYM0300' BEGIN SELECT RAISE(ABORT, 'fake disk failure'); END;").await.unwrap();
    let job = MetricsWrite {
        key: "retry-job".into(),
        snapshot: Arc::new(snapshot(101_000.0, 600)),
    };
    assert!(
        LocalLiveRunArtifacts::persist_metrics_job(
            conn,
            &artifacts.db_path,
            &artifacts.run_id,
            &job
        )
        .await
        .is_err()
    );
    // Access the original connection, not a clone: dropped Turso transactions
    // roll back on the next access through their owning connection wrapper.
    assert_eq!(
        scalar(conn, "SELECT count(*) FROM metrics_snapshots").await,
        0
    );
    assert_eq!(
        scalar(conn, "SELECT count(*) FROM metrics_snapshot_symbols").await,
        0
    );
    conn.execute_batch("DROP TRIGGER fail_symbol;")
        .await
        .unwrap();
    LocalLiveRunArtifacts::persist_metrics_job(conn, &artifacts.db_path, &artifacts.run_id, &job)
        .await
        .unwrap();
    assert_eq!(
        scalar(conn, "SELECT count(*) FROM metrics_snapshots").await,
        1
    );
    assert_eq!(
        scalar(conn, "SELECT count(*) FROM metrics_snapshot_symbols").await,
        600
    );
}

#[tokio::test]
async fn local_metrics_worker_coalesces_only_pending_and_cancelled_drain_keeps_ownership() {
    let mut artifacts = fixture().await;
    let initial = snapshot(101_000.0, 2);
    let job = MetricsWrite {
        key: "dispatched".into(),
        snapshot: Arc::new(initial.clone()),
    };
    artifacts.metrics_inflight = Some(job.clone());
    let mut conn = artifacts.metrics_conn.take().unwrap();
    let (release, wait) = tokio::sync::oneshot::channel();
    artifacts.metrics_writes.spawn(async move {
        wait.await.unwrap();
        let result = LocalLiveRunArtifacts::persist_metrics_job(
            &mut conn,
            std::path::Path::new(":memory:"),
            "test-run",
            &job,
        )
        .await;
        (conn, result)
    });
    for equity in 102_000..102_100 {
        artifacts
            .queue_metrics_if_changed(Some(snapshot(equity as f64, 2)), false)
            .unwrap();
    }
    assert_eq!(artifacts.metrics_writes.len(), 1);
    assert_eq!(
        artifacts.metrics_inflight.as_ref().unwrap().key,
        "dispatched"
    );
    assert_eq!(
        artifacts
            .metrics_pending
            .as_ref()
            .unwrap()
            .snapshot
            .final_equity,
        102_099.0
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(10), artifacts.drain_metrics_writes())
            .await
            .is_err()
    );
    assert_eq!(artifacts.metrics_writes.len(), 1);
    // The independent artifact event connection remains usable while metrics
    // work is waiting; the current-thread owner also continues to yield.
    artifacts
        .append_event_batch(&[&super::super::tests::event(1)])
        .await
        .unwrap();
    release.send(()).unwrap();
    artifacts.drain_metrics_writes().await.unwrap();
    assert_eq!(
        scalar(&artifacts.conn, "SELECT count(*) FROM metrics_snapshots").await,
        2
    );
    assert_eq!(
        artifacts.last_metrics.as_ref().unwrap().final_equity,
        102_099.0
    );
    assert!(artifacts.metrics_pending.is_none() && artifacts.metrics_inflight.is_none());
    assert!(artifacts.metrics_writes.is_empty() && artifacts.metrics_conn.is_some());
    artifacts
        .queue_metrics_if_changed(Some(snapshot(102_099.0, 2)), false)
        .unwrap();
    assert!(
        artifacts.metrics_writes.is_empty(),
        "unchanged snapshot is not dispatched"
    );
}

#[tokio::test]
async fn local_metrics_worker_lost_ack_retries_original_before_newer_snapshot() {
    let mut artifacts = fixture().await;
    let job = MetricsWrite {
        key: "lost-ack".into(),
        snapshot: Arc::new(snapshot(101_000.0, 2)),
    };
    let mut conn = artifacts.metrics_conn.take().unwrap();
    LocalLiveRunArtifacts::persist_metrics_job(
        &mut conn,
        &artifacts.db_path,
        &artifacts.run_id,
        &job,
    )
    .await
    .unwrap();
    artifacts.metrics_inflight = Some(job);
    artifacts.metrics_pending = Some(MetricsWrite {
        key: "newer".into(),
        snapshot: Arc::new(snapshot(102_000.0, 3)),
    });
    artifacts
        .metrics_writes
        .spawn(async move { (conn, Err(BrokerError::ConnectionError("lost ack".into()))) });
    assert!(artifacts.drain_metrics_writes().await.is_err());
    assert!(artifacts.last_metrics.is_none());
    assert_eq!(artifacts.metrics_inflight.as_ref().unwrap().key, "lost-ack");
    artifacts.metrics_retry_at = Instant::now();
    artifacts.drain_metrics_writes().await.unwrap();
    assert_eq!(
        scalar(&artifacts.conn, "SELECT count(*) FROM metrics_snapshots").await,
        2
    );
    assert_eq!(
        scalar(
            &artifacts.conn,
            "SELECT count(*) FROM metrics_snapshot_symbols"
        )
        .await,
        5
    );
    assert_eq!(
        artifacts.last_metrics.as_ref().unwrap().final_equity,
        102_000.0
    );
}

#[tokio::test]
async fn local_metrics_cancelled_transaction_reuses_owning_connection_and_rolls_back() {
    let mut artifacts = fixture().await;
    let conn = artifacts.metrics_conn.as_mut().unwrap();
    conn.execute("CREATE TABLE rollback_probe(value INTEGER)", ())
        .await
        .unwrap();
    let cancelled = tokio::time::timeout(Duration::from_millis(10), async {
        let tx = conn
            .transaction_with_behavior(turso::transaction::TransactionBehavior::Immediate)
            .await
            .unwrap();
        tx.execute("INSERT INTO rollback_probe(value) VALUES (1)", ())
            .await
            .unwrap();
        std::future::pending::<()>().await;
        drop(tx);
    })
    .await;
    assert!(cancelled.is_err());
    assert_eq!(scalar(conn, "SELECT count(*) FROM rollback_probe").await, 0);
    let job = MetricsWrite {
        key: "after-timeout".into(),
        snapshot: Arc::new(snapshot(101_000.0, 2)),
    };
    LocalLiveRunArtifacts::persist_metrics_job(conn, &artifacts.db_path, &artifacts.run_id, &job)
        .await
        .unwrap();
    assert_eq!(
        scalar(conn, "SELECT count(*) FROM metrics_snapshot_symbols").await,
        2
    );
}

#[tokio::test]
async fn local_metrics_failed_final_drain_does_not_publish_completed_artifact() {
    let mut artifacts = fixture().await;
    artifacts.conn.execute_batch("CREATE TRIGGER fail_metrics BEFORE INSERT ON metrics_snapshots BEGIN SELECT RAISE(ABORT, 'disk unavailable'); END;").await.unwrap();
    assert!(
        artifacts
            .finish("completed", "finished", Some(snapshot(101_000.0, 2)))
            .await
            .is_err()
    );
    assert!(artifacts.last_metrics.is_none());
    assert!(artifacts.metrics_inflight.is_some());
    // Clear the deferred rollback through its owning connection before another
    // connection attempts DDL or checks the persisted artifact.
    let conn = artifacts.metrics_conn.as_ref().unwrap();
    assert_eq!(
        scalar(conn, "SELECT count(*) FROM metrics_snapshots").await,
        0
    );
    assert_eq!(
        scalar(&artifacts.conn, "SELECT count(*) FROM events").await,
        0
    );
    artifacts
        .conn
        .execute_batch("DROP TRIGGER fail_metrics;")
        .await
        .unwrap();
    artifacts.metrics_retry_at = Instant::now();
    artifacts
        .finish("completed", "finished", None)
        .await
        .unwrap();
    assert_eq!(
        scalar(&artifacts.conn, "SELECT count(*) FROM metrics_snapshots").await,
        1
    );
    assert_eq!(
        scalar(&artifacts.conn, "SELECT count(*) FROM events").await,
        1
    );
}

#[tokio::test]
async fn local_metrics_panicked_worker_never_acknowledges_snapshot() {
    let mut artifacts = fixture().await;
    artifacts.metrics_inflight = Some(MetricsWrite {
        key: "panic-job".into(),
        snapshot: Arc::new(snapshot(101_000.0, 2)),
    });
    let conn = artifacts.metrics_conn.take().unwrap();
    artifacts.metrics_writes.spawn(async move {
        let _owned_connection = conn;
        panic!("fake writer panic");
    });
    assert!(artifacts.drain_metrics_writes().await.is_err());
    assert!(artifacts.last_metrics.is_none());
    assert_eq!(
        artifacts.metrics_inflight.as_ref().unwrap().key,
        "panic-job"
    );
    artifacts.metrics_retry_at = Instant::now();
    assert!(artifacts.poll_metrics_writes().is_err());
    assert!(artifacts.metrics_writes.is_empty());
}
