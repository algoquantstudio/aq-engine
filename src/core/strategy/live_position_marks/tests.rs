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

#[tokio::test]
async fn mark_sampling_is_owned_paced_cancelled_and_never_submits_trades() {
    let dir = std::env::temp_dir().join(format!("aqe-mark-reader-{}", uuid::Uuid::new_v4()));
    let mut journal = BrokerCommandJournal::open_with_state_history(
        dir.join("commands.db"),
        "strategy",
        "account",
        false,
    )
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
    let reader = Arc::new(Reader {
        calls: AtomicUsize::new(0),
        entered: tokio::sync::Notify::new(),
        dropped: Arc::new(AtomicBool::new(false)),
    });
    let task = tokio::spawn(sample(reader.clone(), worker.sender.clone()));
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
