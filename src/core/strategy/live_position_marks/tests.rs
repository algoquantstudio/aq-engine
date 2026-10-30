use super::*;
use crate::core::{
    broker::{
        command_journal::{BrokerCommandJournal, ExecutionCheckpointBatch, ExecutionFillBatch},
        command_worker::BrokerCommandWorker,
        fill_history::{ExecutionFillHistory, FillHistoryOwner},
        paper_broker::PaperBroker,
        types::{AccountType, BrokerError},
    },
    insight::{Insight, execution_checkpoint::InsightExecutionCheckpoint},
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[test]
fn capture_defaults_on_and_standalone_can_explicitly_opt_out() {
    assert!(capture_enabled(None));
    assert!(capture_enabled(Some("true")));
    assert!(!capture_enabled(Some("false")));
    assert!(!capture_enabled(Some("0")));
}

struct Reader {
    calls: AtomicUsize,
    entered: tokio::sync::Notify,
    dropped: Arc<AtomicBool>,
}
struct ReadGuard(Arc<AtomicBool>);
impl Drop for ReadGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
#[async_trait::async_trait]
impl PositionValuationReader for Reader {
    async fn read_positions(
        &self,
        account: &str,
        symbol: &str,
        ids: &[String],
    ) -> Result<Option<aqe_live_protocol::PositionValuations>, BrokerError> {
        assert_eq!((account, symbol), ("account", "GENERIC"));
        assert_eq!(ids, ["physical-position"]);
        self.calls.fetch_add(1, Ordering::SeqCst);
        let _guard = ReadGuard(self.dropped.clone());
        self.entered.notify_one();
        std::future::pending().await
    }
}

async fn worker_fixture() -> (std::path::PathBuf, BrokerCommandWorker) {
    let dir = std::env::temp_dir().join(format!("aqe-mark-reader-{}", uuid::Uuid::new_v4()));
    let mut journal = BrokerCommandJournal::open(
        dir.join("commands.db"),
        "strategy",
        "account")
    .await
    .unwrap();
    let mut insight = Insight::default();
    insight.symbol = "GENERIC".into();
    insight.order_id = Some("saved-order".into());
    journal
        .persist_execution_batch(
            &ExecutionCheckpointBatch::new(
                vec![InsightExecutionCheckpoint::capture(&insight, 1, true, None)],
                vec![],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let owner = FillHistoryOwner {
        insight_id: insight.insight_id.to_string(),
        execution_epoch: journal.execution_lifecycle().epoch,
        entry_order_id: "saved-order".into(),
        symbol: "GENERIC".into(),
    };
    let history:ExecutionFillHistory = serde_json::from_value(serde_json::json!({
        "account_id":"account","entry_order_id":"saved-order","position_id":"physical-position","symbol":"GENERIC","as_of_us":200,"clock":{"basis":"utc"},
        "fills":[{"version":1,"provider":"fixture","fill_id":"actual-fill","order_id":"saved-order","position_id":"physical-position","symbol":"GENERIC","side":"buy","effect":"entry","quantity":2.0,"price":100.0,"occurred_at_us":100,"as_of_us":200,"cash":null,"account_notional":null}],
        "position_snapshot":{"position_id":"physical-position","symbol":"GENERIC","quantity":2.0,"side":"buy","as_of_us":200}
    })).unwrap();
    journal
        .record_execution_fills(&ExecutionFillBatch::from_history(owner, history).unwrap())
        .await
        .unwrap();
    let worker = BrokerCommandWorker::start(
        Arc::new(PaperBroker::new(AccountType::Paper, 100_000., 1)),
        journal,
        Default::default(),
    )
    .unwrap();
    (dir, worker)
}

#[tokio::test]
async fn mark_sampling_is_owned_paced_cancelled_and_never_submits_trades() {
    let (dir, worker) = worker_fixture().await;
    let reader = Arc::new(Reader {
        calls: AtomicUsize::new(0),
        entered: tokio::sync::Notify::new(),
        dropped: Arc::new(AtomicBool::new(false)),
    });
    let capture = crate::core::live_sync::capture_test_fixture::CaptureFixture::active().await;
    capture.pause().await;
    let paused_task = tokio::spawn(sample(
        reader.clone(),
        worker.sender.clone(),
        Some(capture.publisher.cloud_capture_gate()),
    ));
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(
        reader.calls.load(Ordering::SeqCst),
        0,
        "a durable cloud pause must not start optional mark reads"
    );
    paused_task.abort();
    assert!(paused_task.await.unwrap_err().is_cancelled());
    capture.shutdown().await;
    // Standalone sampling retains its existing behavior without cloud policy.
    let task = tokio::spawn(sample(reader.clone(), worker.sender.clone(), None));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(reader.calls.load(Ordering::SeqCst), 0);
    tokio::time::timeout(Duration::from_secs(5), reader.entered.notified())
        .await
        .unwrap();
    assert_eq!(reader.calls.load(Ordering::SeqCst), 1);
    // Read-only analytics remains available during an outstanding broker read.
    assert_eq!(
        worker
            .sender
            .try_execution_fill_page(0, None, 32)
            .unwrap()
            .wait()
            .await
            .unwrap()
            .records
            .len(),
        1
    );
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(reader.dropped.load(Ordering::SeqCst));
    drop(worker.sender);
    drop(worker.events);
    drop(worker.ready);
    let journal = tokio::time::timeout(Duration::from_secs(2), worker.task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(journal.recovery_page(0, 1).await.unwrap().is_empty());
    assert!(
        journal
            .position_mark_page(0)
            .await
            .unwrap()
            .records
            .is_empty()
    );
    drop(journal);
    std::fs::remove_dir_all(dir).unwrap();
}

struct LateReader {
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}
#[async_trait::async_trait]
impl PositionValuationReader for LateReader {
    async fn read_positions(
        &self,
        account: &str,
        symbol: &str,
        ids: &[String],
    ) -> Result<Option<aqe_live_protocol::PositionValuations>, BrokerError> {
        assert_eq!((account, symbol), ("account", "GENERIC"));
        assert_eq!(ids, ["physical-position"]);
        self.entered.notify_one();
        self.release.acquire().await.unwrap().forget();
        Ok(Some(serde_json::from_value(serde_json::json!({"version":1,"provider":"values-v1","account_id":"account","account_currency":"GBP","symbol":"GENERIC","as_of_us":chrono::Utc::now().timestamp_micros(),
          "positions":[{"position_id":"physical-position","status":"open","quantity":2.0,"side":"buy","average_entry_price":100.0,"current_price":110.0,"gross_unrealized_pnl":20.0,"carry":0.0,"quote_time":null,"notional":{"basis":"paper_price_units","account_value":220.0}}]})).unwrap()))
    }
}

#[tokio::test]
async fn source_capture_pause_during_mark_read_cannot_append_a_late_sample() {
    let (dir, worker) = worker_fixture().await;
    let capture = crate::core::live_sync::capture_test_fixture::CaptureFixture::active().await;
    let reader = Arc::new(LateReader {
        entered: Default::default(),
        release: tokio::sync::Semaphore::new(0),
    });
    let task = tokio::spawn(sample(
        reader.clone(),
        worker.sender.clone(),
        Some(capture.publisher.cloud_capture_gate()),
    ));
    tokio::time::timeout(Duration::from_secs(5), reader.entered.notified())
        .await
        .unwrap();
    capture.pause().await;
    reader.release.add_permits(1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    drop(worker.sender);
    drop(worker.events);
    drop(worker.ready);
    let journal = tokio::time::timeout(Duration::from_secs(2), worker.task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        journal
            .position_mark_page(0)
            .await
            .unwrap()
            .records
            .is_empty()
    );
    assert_eq!(
        journal
            .execution_fill_page(0, None, 32)
            .await
            .unwrap()
            .records
            .len(),
        1,
        "previously accepted fill evidence remains owned"
    );
    drop(journal);
    capture.shutdown().await;
    std::fs::remove_dir_all(dir).unwrap();
}
