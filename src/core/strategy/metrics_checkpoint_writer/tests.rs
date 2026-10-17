use super::*;

struct TestDirectory(std::path::PathBuf);
impl TestDirectory {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("aqe-checkpoint-writer-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn slow_writer_coalesces_only_pending_snapshots_and_does_not_block_owner() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let values = seen.clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let mut started_tx = Some(started_tx);
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let mut writer = CoalescedWriter::start(move |value: &u64| {
        if *value == 1 {
            started_tx.take().unwrap().send(()).unwrap();
            release_rx.recv().unwrap();
        }
        values.lock().push(*value);
        Ok(())
    })
    .unwrap();
    writer.submit(1).unwrap();
    started_rx.await.unwrap();
    writer.submit(2).unwrap();
    writer.submit(3).unwrap();
    assert_eq!(writer.shared.0.lock().pending, Some(3));
    tokio::time::timeout(Duration::from_secs(1), tokio::task::yield_now())
        .await
        .unwrap();
    {
        let shutdown = writer.shutdown();
        tokio::pin!(shutdown);
        assert!(futures::poll!(&mut shutdown).is_pending());
    }
    assert!(
        writer.thread.is_some(),
        "cancelled shutdown retains ownership"
    );
    release_tx.send(()).unwrap();
    writer.shutdown().await.unwrap();
    assert_eq!(*seen.lock(), vec![1, 3]);
    assert!(writer.submit(4).is_err());
}

#[tokio::test]
async fn failed_checkpoint_is_retried_on_shutdown_and_failure_is_reported() {
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = attempts.clone();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let mut tx = Some(tx);
    let mut writer = CoalescedWriter::start(move |_: &u64| {
        count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Some(tx) = tx.take() {
            let _ = tx.send(());
        }
        Err("fake disk failure".into())
    })
    .unwrap();
    writer.submit(1).unwrap();
    rx.await.unwrap();
    assert_eq!(writer.shutdown().await.unwrap_err(), "fake disk failure");
    assert!(attempts.load(std::sync::atomic::Ordering::Relaxed) >= 1);
    assert!(writer.thread.is_none());
}

#[tokio::test]
async fn writer_panic_is_reported_without_retaining_thread() {
    let mut writer = CoalescedWriter::start(|_: &u64| -> Result<(), String> {
        panic!("fake writer panic");
    })
    .unwrap();
    writer.submit(1).unwrap();
    assert!(writer.shutdown().await.unwrap_err().contains("panicked"));
    assert!(writer.thread.is_none());
}

#[tokio::test]
async fn checkpoint_round_trip_replaces_atomically_and_drains_latest_value() {
    let dir = TestDirectory::new();
    let path = dir.path().join("nested/checkpoint.json");
    let target = path.clone();
    let mut tracker = super::super::live_metrics::LivePerformanceTracker::default();
    let now = chrono::Utc::now();
    tracker.initialize_for_account(
        Some("account".into()),
        100_000.0,
        100_000.0,
        now,
        vec!["ETHUSD".into()],
    );
    let mut writer =
        MetricsCheckpointWriter::start(move |stored| write_checkpoint(&target, stored)).unwrap();
    writer
        .submit(LocalLiveMetricsCheckpoint {
            strategy_key: "strategy".into(),
            checkpoint: tracker.checkpoint().unwrap(),
        })
        .unwrap();
    tracker.update_equity(100_123.0, now + chrono::Duration::seconds(1));
    let expected = tracker.checkpoint().unwrap();
    writer
        .submit(LocalLiveMetricsCheckpoint {
            strategy_key: "strategy".into(),
            checkpoint: expected.clone(),
        })
        .unwrap();
    writer.shutdown().await.unwrap();
    let restored: LocalLiveMetricsCheckpoint =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(restored.strategy_key, "strategy");
    assert_eq!(
        serde_json::to_value(restored.checkpoint).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
    assert_eq!(
        std::fs::read_dir(path.parent().unwrap()).unwrap().count(),
        1
    );
}

#[test]
fn failed_replacement_preserves_existing_destination_and_cleans_temp_file() {
    let dir = TestDirectory::new();
    let path = dir.path().join("checkpoint.json");
    std::fs::create_dir(&path).unwrap();
    let mut tracker = super::super::live_metrics::LivePerformanceTracker::default();
    tracker.initialize_for_account(Some("account".into()), 1.0, 1.0, chrono::Utc::now(), vec![]);
    let stored = LocalLiveMetricsCheckpoint {
        strategy_key: "strategy".into(),
        checkpoint: tracker.checkpoint().unwrap(),
    };
    assert!(write_checkpoint(&path, &stored).is_err());
    assert!(path.is_dir());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
}
