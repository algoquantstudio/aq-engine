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
) {
    let account = sender.scope().1.to_owned();
    let mut cursor = String::new();
    loop {
        // Pace even unavailable histories. This is never on the trading path.
        tokio::time::sleep(Duration::from_secs(1)).await;
        let page = match sender.try_fill_history_owners(cursor.clone()) {
            Ok(reply) => reply.wait().await,
            Err(BrokerCommandError::QueueFull) => continue,
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
            tokio::time::sleep(Duration::from_secs(1)).await;
            let result = tokio::time::timeout(
                Duration::from_secs(6),
                reader.read(&account, &owner.entry_order_id, &owner.symbol),
            )
            .await;
            match result {
                Ok(Ok(Some(history))) => {
                    let result = match ExecutionFillBatch::from_history(owner, history) {
                        Ok(batch) => sender.record_execution_fills(batch).await,
                        Err(error) => Err(error),
                    };
                    if let Err(error) = result {
                        if matches!(error, BrokerCommandError::Closed) {
                            return;
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
            tokio::time::sleep(Duration::from_secs(60)).await;
        } else if let Some(next) = page.next_cursor {
            cursor = next;
        } else {
            cursor.clear();
            tokio::time::sleep(Duration::from_secs(60)).await;
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
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn history_background_read_is_paced_owned_and_cancelled_without_trading() {
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
        let reader = Arc::new(Reader {
            calls: AtomicUsize::new(0),
            entered: tokio::sync::Notify::new(),
            dropped: Arc::new(AtomicBool::new(false)),
        });
        let task = tokio::spawn(recover(reader.clone(), worker.sender.clone()));
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
}
