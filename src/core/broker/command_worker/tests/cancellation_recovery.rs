use super::*;
use crate::core::broker::types::{OrderLifecycleSnapshot, OrderLifecycleStatus};

struct CancelRecoveryProbe {
    reads: AtomicUsize,
    retries: AtomicUsize,
    delayed: bool,
    status: OrderLifecycleStatus,
}
impl CommandExecutor for CancelRecoveryProbe {
    async fn execute(&self, _: BrokerCommandAction) -> BrokerCommandOutcome {
        panic!("recovery must not replay normal execution or place an entry")
    }
    async fn pending_cancellation_recovery(
        &self,
        account: &str,
        order: &str,
    ) -> Result<Option<OrderLifecycleSnapshot>, super::super::super::types::BrokerError> {
        assert_eq!(account, "account");
        let count = self.reads.fetch_add(1, Ordering::SeqCst);
        if self.delayed && count == 0 {
            return Ok(None);
        }
        Ok(Some(OrderLifecycleSnapshot {
            order_id: order.into(),
            status: self.status,
            position_id: None,
            position_qty: None,
            symbol: Some("BTCUSD".into()),
            detail: None,
            close_details: None,
        }))
    }
    async fn retry_cancellation(&self, account: &str, _order: &str) -> BrokerCommandOutcome {
        assert_eq!(account, "account");
        self.retries.fetch_add(1, Ordering::SeqCst);
        BrokerCommandOutcome::Acknowledged
    }
}

async fn saved_cancel(fixture: &Fixture) -> (BrokerCommandJournal, BrokerCommand) {
    let mut journal = fixture.journal().await;
    let command = cancel(&uuid::Uuid::new_v4().to_string());
    journal.prepare(&command, 10).await.unwrap();
    journal.mark_dispatched(&command.id, 11).await.unwrap();
    journal
        .complete(&BrokerCommandCompletion {
            receipt: command.receipt(),
            queued_at_us: 10,
            dispatched_at_us: Some(11),
            completed_at_us: 12,
            outcome: BrokerCommandOutcome::Ambiguous {
                reason: "old MT5 CancelOrder timeout".into(),
            },
        })
        .await
        .unwrap();
    journal.request_execution_stop().await.unwrap();
    (journal, command)
}

#[tokio::test]
async fn recovered_cancel_retries_same_intent_and_reduces_only_after_proof() {
    for delayed in [false, true] {
        let fixture = Fixture::new();
        let (journal, command) = saved_cancel(&fixture).await;
        let probe = Arc::new(CancelRecoveryProbe {
            reads: AtomicUsize::new(0),
            retries: AtomicUsize::new(0),
            delayed,
            status: OrderLifecycleStatus::Pending,
        });
        let mut worker =
            BrokerCommandWorker::start_executor(probe.clone(), journal, Default::default())
                .unwrap();
        worker.sender.try_enqueue(command.clone()).unwrap();
        if delayed {
            assert!(matches!(
                next(&mut worker).await.outcome,
                BrokerCommandOutcome::Ambiguous { .. }
            ));
        }
        let confirmed = next(&mut worker).await;
        assert!(matches!(
            confirmed.outcome,
            BrokerCommandOutcome::Acknowledged
        ));
        assert_eq!(confirmed.receipt, command.receipt());
        assert_eq!(confirmed.dispatched_at_us, Some(11));
        assert_eq!(probe.retries.load(Ordering::SeqCst), 1);
        let mut journal = finish(worker).await;
        assert_eq!(
            journal.get(&command.id).await.unwrap().unwrap().state,
            JournalCommandState::Completed
        );
        journal
            .persist_execution_batch(&checkpoint_batch(&confirmed.receipt))
            .await
            .unwrap();
        assert_eq!(
            journal.get(&command.id).await.unwrap().unwrap().state,
            JournalCommandState::Applied
        );
        drop(journal);
    }
}

#[tokio::test]
async fn recovered_cancel_without_pending_proof_preserves_original_uncertainty() {
    for status in [
        OrderLifecycleStatus::Unknown,
        OrderLifecycleStatus::Filled,
        OrderLifecycleStatus::PositionOpen,
    ] {
        let fixture = Fixture::new();
        let (journal, command) = saved_cancel(&fixture).await;
        let probe = Arc::new(CancelRecoveryProbe {
            reads: AtomicUsize::new(0),
            retries: AtomicUsize::new(0),
            delayed: false,
            status,
        });
        let mut worker =
            BrokerCommandWorker::start_executor(probe.clone(), journal, Default::default())
                .unwrap();
        worker.sender.try_enqueue(command.clone()).unwrap();
        assert!(matches!(
            next(&mut worker).await.outcome,
            BrokerCommandOutcome::Ambiguous { .. }
        ));
        assert_eq!(probe.retries.load(Ordering::SeqCst), 0);
        let journal = finish(worker).await;
        assert!(
            matches!(journal.get(&command.id).await.unwrap().unwrap().completion.unwrap().outcome,
            BrokerCommandOutcome::Ambiguous {ref reason} if reason=="old MT5 CancelOrder timeout")
        );
        drop(journal);
    }
}
