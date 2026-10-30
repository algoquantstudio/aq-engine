use super::*;
use aqe_live_protocol::{CaptureGapNotice, CaptureGapReceipt, SafetyKind, SafetyObservation};

struct BlockedDiagnostic {
    entered: Arc<tokio::sync::Notify>,
    stopped: Arc<tokio::sync::Notify>,
}
#[async_trait]
impl RemoteBatchSink for BlockedDiagnostic {
    fn safety_run_id(&self) -> Option<&str> {
        Some("run")
    }
    async fn persist_batch(
        &self,
        _: &LiveSyncBatch,
    ) -> Result<LiveSyncBatchResult, RemoteSyncError> {
        unreachable!()
    }
    async fn persist_capture_gap(
        &self,
        _: &CaptureGapNotice,
    ) -> Result<Option<CaptureGapReceipt>, RemoteSyncError> {
        self.entered.notify_one();
        std::future::pending().await
    }
    async fn persist_safety(&self, _: &SafetyObservation) -> Result<bool, RemoteSyncError> {
        self.stopped.notify_one();
        Ok(true)
    }
}

#[tokio::test]
async fn blocked_gap_endpoint_cannot_delay_compact_stop_or_shutdown() {
    let directory = std::env::temp_dir().join(format!("aqe-gap-stop-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let store = OutboxStore::open(directory.join("outbox.db"))
        .await
        .unwrap();
    let mut gap = CaptureGapNotice {
        version: 1,
        notice_id: uuid::Uuid::nil(),
        strategy_id: "strategy".into(),
        live_session_id: "session".into(),
        source_runtime_id: "previous".into(),
        source_run_id: None,
        source_generation: 1,
        started_at_us: 1,
        last_blocked_at_us: 2,
        origin_runtime_id: None,
        origin_run_id: None,
        last_blocked_run_id: None,
        ownership_handoffs: 0,
    };
    gap.notice_id = gap.stable_id();
    store.accept_capture_gap_notice(&gap).await.unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let stopped = Arc::new(tokio::sync::Notify::new());
    let sink: Arc<dyn RemoteBatchSink> = Arc::new(BlockedDiagnostic {
        entered: entered.clone(),
        stopped: stopped.clone(),
    });
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(super::super::safety::run(
        store.clone(),
        sink,
        "runtime".into(),
        "strategy".into(),
        "session".into(),
        receiver,
    ));
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    store
        .record_safety_only(&[SafetyObservation {
            version: 1,
            strategy_id: "strategy".into(),
            live_session_id: "session".into(),
            runtime_id: "runtime".into(),
            run_id: "run".into(),
            event_id: uuid::Uuid::new_v4(),
            revision: 1,
            created_at: chrono::Utc::now(),
            kind: SafetyKind::Lifecycle,
            state: "stopped".into(),
            action_id: None,
            message: None,
        }])
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), stopped.notified())
        .await
        .unwrap();
    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        store.pending_capture_gap_notices("strategy").await.unwrap(),
        [gap]
    );
    assert_eq!(store.pending_count().await.unwrap(), 0);
    drop(store);
    std::fs::remove_dir_all(directory).unwrap();
}
