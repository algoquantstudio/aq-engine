//! Low-priority read-only recovery, independent of the strategy trading turn.
use crate::core::broker::{
    command_journal::ExecutionFillBatch, command_worker::BrokerCommandSender,
    commands::BrokerCommandError, fill_history::ExecutionFillHistoryReader,
};
use std::{sync::Arc, time::Duration};

/// One bounded owner page and one broker request at a time. Cancellation drops
/// the outstanding read; the journal remains the sole owner of admitted facts.
pub(super) async fn recover(
    reader: Arc<dyn ExecutionFillHistoryReader>,
    sender: BrokerCommandSender,
    capture: Option<crate::core::live_sync::CloudCaptureGate>,
) {
    recover_scoped(reader,sender,capture,false).await;
}

/// Active physical positions must not queue behind years of terminal orders.
/// This independent, serial read lane retains the same journal ownership and
/// admission checks. It can never submit/cancel/replace a broker order.
pub(super) async fn recover_active(
    reader: Arc<dyn ExecutionFillHistoryReader>,
    sender: BrokerCommandSender,
    capture: Option<crate::core::live_sync::CloudCaptureGate>,
) {
    recover_scoped(reader,sender,capture,true).await;
}

async fn recover_scoped(
    reader: Arc<dyn ExecutionFillHistoryReader>,
    sender: BrokerCommandSender,
    capture: Option<crate::core::live_sync::CloudCaptureGate>,
    active_only: bool,
) {
    let account = sender.scope().1.to_owned();
    let mut cursor = String::new();
    'sweep: loop {
        // Pace even unavailable histories. This is never on the trading path.
        tokio::time::sleep(Duration::from_secs(1)).await;
        if capture.as_ref().is_some_and(|gate| gate.is_paused()) {
            continue;
        }
        let request = if active_only { sender.try_active_fill_history_owners(cursor.clone()) }
            else { sender.try_fill_history_owners(cursor.clone()) };
        let page = match request {
            Ok(reply) => reply.wait().await,
            Err(error) if error.is_analytics_backpressure() => continue,
            Err(_) => return,
        };
        let page = match page {
            Ok(page) => page,
            Err(BrokerCommandError::Closed) => return,
            Err(error) => {
                log::warn!("Fill history owner page unavailable: {error}");
                tokio::time::sleep(Duration::from_secs(60)).await;
                continue;
            }
        };
        let mut retry = false;
        for owner in page.owners {
            if active_only {
                // A single slow/broken history must not pin this independent
                // lane at the same first owner on every retry.
                cursor=owner.insight_id.clone();
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            if capture.as_ref().is_some_and(|gate| gate.is_paused()) {
                continue 'sweep;
            }
            let result = tokio::time::timeout(
                Duration::from_secs(6),
                reader.read(&account, &owner.entry_order_id, &owner.symbol),
            )
            .await;
            match result {
                Ok(Ok(Some(history))) => {
                    if capture.as_ref().is_some_and(|gate| gate.is_paused()) {
                        continue 'sweep;
                    }
                    let result = match ExecutionFillBatch::from_history(owner, history) {
                        Ok(batch) => {
                            sender
                                .record_execution_fills_for_capture(batch, capture.clone())
                                .await
                        }
                        Err(error) => Err(error),
                    };
                    if let Err(error) = result {
                        if matches!(error, BrokerCommandError::Closed) {
                            return;
                        }
                        if matches!(error, BrokerCommandError::CapturePaused) {
                            continue 'sweep;
                        }
                        log::warn!("Fill history admission deferred: {error}");
                        // A single ambiguous/changed owner must not starve all
                        // subsequent owners. It is revisited next sweep.
                    }
                }
                Ok(Ok(None)) => {} // Unsupported/ambiguous is not complete.
                _ => {
                    retry = true;
                    break;
                } // Old EA/offline: back off once per page.
            }
        }
        if retry {
            tokio::time::sleep(Duration::from_secs(if active_only { 5 } else { 60 })).await;
        } else if let Some(next) = page.next_cursor {
            cursor = next;
        } else {
            cursor.clear();
            tokio::time::sleep(Duration::from_secs(if active_only { 5 } else { 60 })).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        broker::{
            command_journal::{BrokerCommandJournal, ExecutionCheckpointBatch},
            command_worker::BrokerCommandWorker,
            fill_history::ExecutionFillHistory,
            paper_broker::PaperBroker,
            types::{AccountType, BrokerError},
        },
        insight::{Insight, execution_checkpoint::InsightExecutionCheckpoint},
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct Reader {
        calls: AtomicUsize,
        entered: tokio::sync::Notify,
        dropped: Arc<AtomicBool>,
        response: Option<ExecutionFillHistory>,
        release: tokio::sync::Notify,
    }
    struct ReadGuard(Arc<AtomicBool>);
    impl Drop for ReadGuard {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    #[async_trait::async_trait]
    impl ExecutionFillHistoryReader for Reader {
        async fn read(
            &self,
            account: &str,
            order: &str,
            symbol: &str,
        ) -> Result<Option<ExecutionFillHistory>, BrokerError> {
            assert_eq!(
                (account, order, symbol),
                ("account", "saved-order", "GENERIC")
            );
            self.calls.fetch_add(1, Ordering::SeqCst);
            let _guard = ReadGuard(self.dropped.clone());
            self.entered.notify_one();
            if let Some(response) = self.response.as_ref() {
                self.release.notified().await;
                Ok(Some(response.clone()))
            } else {
                std::future::pending().await
            }
        }
    }

    async fn worker_fixture() -> (std::path::PathBuf, BrokerCommandWorker) {
        let dir = std::env::temp_dir().join(format!("aqe-history-reader-{}", uuid::Uuid::new_v4()));
        let mut journal =
            BrokerCommandJournal::open(dir.join("commands.db"), "strategy", "account")
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
        let worker = BrokerCommandWorker::start(
            Arc::new(PaperBroker::new(AccountType::Paper, 100_000., 1)),
            journal,
            Default::default(),
        )
        .unwrap();
        (dir, worker)
    }

    #[tokio::test]
    async fn history_background_read_is_paced_owned_and_cancelled_without_trading() {
        let (dir, worker) = worker_fixture().await;
        let reader = Arc::new(Reader {
            calls: AtomicUsize::new(0),
            entered: tokio::sync::Notify::new(),
            dropped: Arc::new(AtomicBool::new(false)),
            response: None,
            release: Default::default(),
        });
        let capture = crate::core::live_sync::capture_test_fixture::CaptureFixture::active().await;
        capture.pause().await;
        let paused_task = tokio::spawn(recover(
            reader.clone(),
            worker.sender.clone(),
            Some(capture.publisher.cloud_capture_gate()),
        ));
        tokio::time::sleep(Duration::from_millis(2200)).await;
        assert_eq!(
            reader.calls.load(Ordering::SeqCst),
            0,
            "a durable cloud pause must not start optional history reads"
        );
        paused_task.abort();
        assert!(paused_task.await.unwrap_err().is_cancelled());
        capture.shutdown().await;
        // Standalone local analytics has no cloud policy or new pause.
        let task = tokio::spawn(recover(reader.clone(), worker.sender.clone(), None));
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(reader.calls.load(Ordering::SeqCst), 0);
        tokio::time::timeout(Duration::from_secs(5), reader.entered.notified())
            .await
            .unwrap();
        assert_eq!(reader.calls.load(Ordering::SeqCst), 1);
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
                .execution_fill_page(0, None, 1)
                .await
                .unwrap()
                .records
                .is_empty()
        );
        drop(journal);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn source_capture_pause_during_fill_history_read_cannot_append_late_evidence() {
        let (dir, worker) = worker_fixture().await;
        let history:ExecutionFillHistory=serde_json::from_value(serde_json::json!({
            "account_id":"account","entry_order_id":"saved-order","position_id":"physical-position","symbol":"GENERIC","as_of_us":200,"clock":{"basis":"utc"},
            "fills":[{"version":1,"provider":"fixture","fill_id":"late-fill","order_id":"saved-order","position_id":"physical-position","symbol":"GENERIC","side":"buy","effect":"entry","quantity":2.0,"price":100.0,"occurred_at_us":100,"as_of_us":200,"cash":null,"account_notional":null}],
            "position_snapshot":{"position_id":"physical-position","symbol":"GENERIC","quantity":2.0,"side":"buy","as_of_us":200}
        })).unwrap();
        let reader = Arc::new(Reader {
            calls: AtomicUsize::new(0),
            entered: Default::default(),
            dropped: Arc::new(AtomicBool::new(false)),
            response: Some(history),
            release: Default::default(),
        });
        let capture = crate::core::live_sync::capture_test_fixture::CaptureFixture::active().await;
        let task = tokio::spawn(recover(
            reader.clone(),
            worker.sender.clone(),
            Some(capture.publisher.cloud_capture_gate()),
        ));
        tokio::time::timeout(Duration::from_secs(5), reader.entered.notified())
            .await
            .unwrap();
        capture.pause().await;
        reader.release.notify_one();
        tokio::time::sleep(Duration::from_millis(100)).await;
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
        let page = journal.execution_fill_page(0, None, 128).await.unwrap();
        assert!(page.records.is_empty());
        assert!(page.position_checks.is_empty());
        assert_eq!(
            journal
                .execution_checkpoint_page("", 32)
                .await
                .unwrap()
                .rows
                .len(),
            1,
            "mandatory execution checkpoint must be retained"
        );
        drop(journal);
        capture.shutdown().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn active_fill_lane_survives_stalled_archive_and_advances_past_failed_owner() {
        use crate::core::insight::types::InsightState;
        struct PriorityReader { active:tokio::sync::Notify, archive_calls:AtomicUsize, failed_calls:AtomicUsize }
        #[async_trait::async_trait]
        impl ExecutionFillHistoryReader for PriorityReader {
            async fn read(&self,account:&str,order:&str,symbol:&str)->Result<Option<ExecutionFillHistory>,BrokerError> {
                assert_eq!((account,symbol),("account","GENERIC"));
                match order {
                    "archive"=>{self.archive_calls.fetch_add(1,Ordering::SeqCst);std::future::pending().await},
                    "failed-active"=>{self.failed_calls.fetch_add(1,Ordering::SeqCst);Err(BrokerError::TradeError("isolated read failure".into()))},
                    "new-active"=>{self.active.notify_one();Ok(None)},
                    _=>panic!("unowned broker read"),
                }
            }
        }
        let dir=std::env::temp_dir().join(format!("aqe-active-history-{}",uuid::Uuid::new_v4()));
        let mut journal=BrokerCommandJournal::open(dir.join("commands.db"),"strategy","account").await.unwrap();
        for (n,order,state) in [(1,"archive",InsightState::Closed),(2,"failed-active",InsightState::Filled),(3,"new-active",InsightState::Filled)] {
            let mut row=Insight::default();row.insight_id=uuid::Uuid::from_u128(n);
            row.symbol="GENERIC".into();row.order_id=Some(order.into());row.state=state;
            journal.persist_execution_batch(&ExecutionCheckpointBatch::new(vec![InsightExecutionCheckpoint::capture(&row,1,true,None)],vec![]).unwrap()).await.unwrap();
        }
        let worker=BrokerCommandWorker::start(Arc::new(PaperBroker::new(AccountType::Paper,100_000.,1)),journal,Default::default()).unwrap();
        let reader=Arc::new(PriorityReader { active:Default::default(),archive_calls:AtomicUsize::new(0),failed_calls:AtomicUsize::new(0) });
        let archived=tokio::spawn(recover(reader.clone(),worker.sender.clone(),None));
        let active=tokio::spawn(recover_active(reader.clone(),worker.sender.clone(),None));
        tokio::time::timeout(Duration::from_secs(15),reader.active.notified()).await.unwrap();
        assert!(reader.archive_calls.load(Ordering::SeqCst)>0);
        assert_eq!(reader.failed_calls.load(Ordering::SeqCst),1,"the failed owner must not pin the active cursor");
        for task in [archived,active] {task.abort();assert!(task.await.unwrap_err().is_cancelled());}
        drop(worker.sender);
        drop(worker.events);
        drop(worker.ready);
        tokio::time::timeout(Duration::from_secs(2),worker.task).await.unwrap().unwrap().unwrap();
        let _=std::fs::remove_dir_all(dir);
    }
}
