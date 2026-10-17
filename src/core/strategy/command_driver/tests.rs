use super::*;
use crate::core::{
    broker::{command_journal::JournalCommandState, paper_broker::PaperBroker, types::AccountType},
    insight::Insight,
};

struct Fixture(std::path::PathBuf);

async fn commit_rows(
    driver: &mut StrategyCommandDriver,
    rows: Vec<crate::core::insight::execution_checkpoint::InsightExecutionCheckpoint>,
) {
    driver.persist_execution_checkpoints(rows, vec![]).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while driver.checkpoint_in_flight() {
            driver.poll_acknowledgements(256).unwrap();
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn entry_deadline_is_rebuilt_from_durable_checkpoint_on_recovery() {
    let fixture = Fixture::new();
    let broker = Arc::new(PaperBroker::new(AccountType::Paper, 100_000.0, 1));
    let now = chrono::DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z")
        .unwrap()
        .to_utc();
    broker.set_time(now);
    let mut driver = fixture.driver(broker.clone(), Default::default()).await;
    let receipt = driver.enqueue_command(entry()).unwrap();
    let mut row = checkpoint(&driver, &receipt);
    row.state = crate::core::insight::types::InsightState::New;
    row.created_at = now;
    row.timeframe = crate::core::utils::timeframe::TimeFrame::new(
        1,
        crate::core::utils::timeframe::TimeFrameUnit::Minute,
    );
    row.period_unfilled = Some(1);
    commit_rows(&mut driver, vec![row]).await;
    drop(driver.stop().await.unwrap());
    broker.set_time(now + chrono::TimeDelta::minutes(1));
    let (mut driver, _) = StrategyCommandDriver::start_recovering(
        broker.clone(),
        fixture.path(),
        "strategy",
        "account",
        Default::default(),
    )
    .await
    .unwrap();
    driver.finish_recovery();
    complete(&mut driver, &receipt).await;
    let Some(BrokerCommandProgress::Completed(completion)) = driver.progress(&receipt) else {
        panic!("expiry evidence")
    };
    assert!(matches!(
        completion.outcome,
        BrokerCommandOutcome::CancelledBeforeDispatch
    ));
    assert!(completion.dispatched_at_us.is_none());
    assert!(broker.get_orders().await.unwrap().is_empty());
    acknowledge(&mut driver, &receipt).await;
    drop(driver.stop().await.unwrap());
}

#[tokio::test]
async fn confirmed_ttl_configuration_can_add_or_remove_a_queued_entry_deadline() {
    for remove in [false, true] {
        let fixture = Fixture::new();
        let broker = Arc::new(PaperBroker::new(AccountType::Paper, 100_000.0, 1));
        let now = chrono::DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z")
            .unwrap()
            .to_utc();
        broker.set_time(now + chrono::TimeDelta::minutes(2));
        let mut driver = fixture.driver(broker.clone(), Default::default()).await;
        let receipt = driver.enqueue_command(entry()).unwrap();
        let mut row = checkpoint(&driver, &receipt);
        row.state = crate::core::insight::types::InsightState::New;
        row.created_at = now;
        row.timeframe = crate::core::utils::timeframe::TimeFrame::new(
            1,
            crate::core::utils::timeframe::TimeFrameUnit::Minute,
        );
        row.period_unfilled = remove.then_some(1);
        commit_rows(&mut driver, vec![row.clone()]).await;
        row.revision += 1;
        row.period_unfilled = (!remove).then_some(1);
        commit_rows(&mut driver, vec![row]).await;
        complete(&mut driver, &receipt).await;
        let Some(BrokerCommandProgress::Completed(completion)) = driver.progress(&receipt) else {
            panic!("entry result")
        };
        assert_eq!(
            matches!(
                completion.outcome,
                BrokerCommandOutcome::CancelledBeforeDispatch
            ),
            !remove
        );
        assert_eq!(
            broker.get_orders().await.unwrap().len(),
            usize::from(remove)
        );
        drop(driver.stop().await.unwrap());
    }
}

#[tokio::test]
async fn durable_stop_after_worker_exit_is_persisted_when_ownership_returns() {
    use crate::core::broker::command_journal::lifecycle::ExecutionPhase;
    let fixture = Fixture::new();
    let broker = Arc::new(PaperBroker::new(AccountType::Paper, 100_000.0, 1));
    let mut driver = fixture.driver(broker, Default::default()).await;
    driver.worker.events.close();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !driver.is_worker_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    driver.request_execution_stop();
    assert!(driver.worker.sender.execution_stop_pending());
    let journal = driver.stop().await.unwrap();
    assert_eq!(
        journal.execution_lifecycle().phase,
        ExecutionPhase::Stopping
    );
}

#[tokio::test]
async fn staged_intent_cannot_dispatch_until_checkpoint_acknowledgement() {
    let fixture = Fixture::new();
    let broker = Arc::new(PaperBroker::new(AccountType::Paper, 100_000.0, 1));
    let mut driver = fixture.driver(broker.clone(), Default::default()).await;
    let receipt = driver.enqueue_command(entry()).unwrap();
    assert_eq!(driver.dispatch_prepared(256).unwrap(), 0);
    // A journal request is a worker barrier, not an arbitrary timing sleep.
    assert!(driver.recovery_page(0, 32).await.unwrap().is_empty());
    assert!(broker.get_orders().await.unwrap().is_empty());
    let row = checkpoint(&driver, &receipt);
    driver
        .persist_execution_checkpoints(vec![row], vec![])
        .unwrap();
    let saved = driver
        .execution_checkpoint_page(String::new(), 32)
        .await
        .unwrap();
    assert_eq!(saved.rows.len(), 1);
    assert!(saved.rows[0].owner_receipts.contains(&receipt));
    assert_eq!(
        driver.dispatch_prepared(256).unwrap(),
        0,
        "DB commit alone is not owner admission"
    );
    assert!(broker.get_orders().await.unwrap().is_empty());
    assert_eq!(driver.poll_acknowledgements(256).unwrap(), 0);
    assert!(!driver.checkpoint_in_flight());
    assert_eq!(driver.dispatch_prepared(256).unwrap(), 1);
    complete(&mut driver, &receipt).await;
    assert_eq!(broker.get_orders().await.unwrap().len(), 1);
    drop(driver.stop().await.unwrap());
}

#[tokio::test]
async fn staged_admission_order_survives_out_of_order_checkpoint_commits() {
    let fixture = Fixture::new();
    let broker = Arc::new(PaperBroker::new(AccountType::Paper, 100_000.0, 1));
    let mut driver = fixture.driver(broker.clone(), Default::default()).await;
    let first = driver.enqueue_command(entry()).unwrap();
    let second = driver.enqueue_command(entry()).unwrap();
    let row = checkpoint(&driver, &second);
    commit_rows(&mut driver, vec![row]).await;
    assert_eq!(driver.dispatch_prepared(256).unwrap(), 0);
    assert!(broker.get_orders().await.unwrap().is_empty());
    let row = checkpoint(&driver, &first);
    commit_rows(&mut driver, vec![row]).await;
    assert_eq!(driver.dispatch_prepared(1).unwrap(), 1);
    assert!(driver.pending[&first.id].dispatch_queued);
    assert!(!driver.pending[&second.id].dispatch_queued);
    assert_eq!(driver.dispatch_prepared(1).unwrap(), 1);
    complete(&mut driver, &first).await;
    complete(&mut driver, &second).await;
    let records = driver.recovery_page(0, 32).await.unwrap();
    // Journal sequence is commit order; broker admission remains original order.
    assert_eq!(records.len(), 2);
    assert_eq!(broker.get_orders().await.unwrap().len(), 2);
    drop(driver.stop().await.unwrap());
}

#[tokio::test]
async fn staged_child_waits_for_durable_ancestor_chain() {
    let fixture = Fixture::new();
    let broker = Arc::new(PaperBroker::new(AccountType::Paper, 100_000.0, 1));
    let mut driver = fixture.driver(broker.clone(), Default::default()).await;
    let receipt = driver.enqueue_command(entry()).unwrap();
    let mut parent = Insight::default();
    parent.symbol = "ETHUSD".into();
    let mut child = checkpoint(&driver, &receipt);
    child.parent_id = Some(parent.insight_id);
    let child_id = child.insight_id;
    commit_rows(&mut driver, vec![child]).await;
    assert_eq!(driver.dispatch_prepared(256).unwrap(), 0);
    assert!(broker.get_orders().await.unwrap().is_empty());
    let mut parent_row =
        crate::core::insight::execution_checkpoint::InsightExecutionCheckpoint::capture(
            &parent, 1, true, None,
        );
    commit_rows(&mut driver, vec![parent_row.clone()]).await;
    assert_eq!(
        driver.dispatch_prepared(256).unwrap(),
        0,
        "an older parent checkpoint does not yet own this child"
    );
    parent_row.revision += 1;
    parent_row.child_ids.push(child_id);
    commit_rows(&mut driver, vec![parent_row]).await;
    assert_eq!(driver.dispatch_prepared(256).unwrap(), 1);
    complete(&mut driver, &receipt).await;
    assert_eq!(broker.get_orders().await.unwrap().len(), 1);
    drop(driver.stop().await.unwrap());
}

#[tokio::test]
async fn stop_before_checkpoint_preserves_evidence_without_broker_submission() {
    for cancel_one in [false, true] {
        let fixture = Fixture::new();
        let broker = Arc::new(PaperBroker::new(AccountType::Paper, 100_000.0, 1));
        let mut driver = fixture.driver(broker.clone(), Default::default()).await;
        let receipt = driver.enqueue_command(entry()).unwrap();
        if cancel_one {
            driver.cancel_pending_entry(&receipt).unwrap();
        } else {
            driver.stop_entries();
        }
        complete(&mut driver, &receipt).await;
        let Some(BrokerCommandProgress::Completed(completion)) = driver.progress(&receipt) else {
            panic!("missing cancellation evidence")
        };
        assert!(matches!(
            completion.outcome,
            BrokerCommandOutcome::CancelledBeforeDispatch
        ));
        assert!(broker.get_orders().await.unwrap().is_empty());
        assert_eq!(
            driver.recovery_page(0, 32).await.unwrap()[0].state,
            JournalCommandState::Completed
        );
        drop(driver.stop().await.unwrap());
    }
}

#[tokio::test]
async fn checkpoint_failure_retains_receipts_and_stops_new_entry_dispatch() {
    let fixture = Fixture::new();
    let broker = Arc::new(PaperBroker::new(AccountType::Paper, 100_000.0, 1));
    let mut driver = fixture.driver(broker, Default::default()).await;
    let receipt = driver.enqueue_command(entry()).unwrap();
    complete(&mut driver, &receipt).await;
    driver.mark_reduced(&receipt).unwrap();
    let mut row = checkpoint(&driver, &receipt);
    row.revision += 1; // Skip the next revision: this is an invalid state commit.
    driver
        .persist_execution_checkpoints(vec![row], vec![receipt.clone()])
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if driver.poll_acknowledgements(256).is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(driver.pending_count(), 1);
    assert!(driver.progress(&receipt).is_some());
    assert!(driver.sender().entries_stopped());
    let saved = driver
        .execution_checkpoint_page(String::new(), 32)
        .await
        .unwrap();
    assert_eq!(saved.rows.len(), 1);
    assert_eq!(
        saved.rows[0].revision, 1,
        "failed update must preserve the pre-dispatch checkpoint"
    );
    assert!(saved.rows[0].owner_receipts.contains(&receipt));
    assert_eq!(driver.recovery_page(0, 32).await.unwrap().len(), 1);
    drop(driver.stop().await.unwrap());
}
impl Fixture {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("aqe-driver-test-{}", uuid::Uuid::new_v4())))
    }
    fn path(&self) -> std::path::PathBuf {
        self.0.join("commands.db")
    }
    async fn driver(
        &self,
        broker: Arc<PaperBroker>,
        config: BrokerCommandWorkerConfig,
    ) -> StrategyCommandDriver {
        StrategyCommandDriver::start(broker, self.path(), "strategy", "account", config)
            .await
            .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn entry() -> BrokerCommand {
    let mut insight = Insight::default();
    insight.symbol = "ETHUSD".into();
    insight.quantity = Some(1.0);
    insight.limit_price = Some(100.0);
    BrokerCommand::new(
        "strategy",
        "account",
        &insight.insight_id.to_string(),
        BrokerCommandAction::Submit {
            request: (&insight).into(),
        },
        None,
    )
}

#[tokio::test]
async fn recovered_driver_holds_admission_until_owner_install_and_keeps_original_order() {
    let fixture = Fixture::new();
    let broker = Arc::new(PaperBroker::new(AccountType::Paper, 100_000.0, 1));
    let mut driver = fixture.driver(broker.clone(), Default::default()).await;
    let a = driver.enqueue_command(entry()).unwrap();
    let b = driver.enqueue_command(entry()).unwrap();
    for receipt in [&b, &a] {
        let mut row = checkpoint(&driver, receipt);
        row.state = crate::core::insight::types::InsightState::New;
        commit_rows(&mut driver, vec![row]).await;
    }
    drop(driver.stop().await.unwrap());
    let (mut recovered, graph) = StrategyCommandDriver::start_recovering(
        broker.clone(),
        fixture.path(),
        "strategy",
        "account",
        Default::default(),
    )
    .await
    .unwrap();
    assert_eq!(graph.rows.len(), 2);
    assert_eq!(recovered.pending_count(), 2);
    assert_eq!(recovered.dispatch_prepared(256).unwrap(), 0);
    assert!(recovered.enqueue_command(entry()).is_err());
    assert!(broker.get_orders().await.unwrap().is_empty());
    recovered.finish_recovery();
    assert_eq!(recovered.dispatch_prepared(1).unwrap(), 1);
    assert!(recovered.pending[&a.id].dispatch_queued);
    assert!(!recovered.pending[&b.id].dispatch_queued);
    complete(&mut recovered, &a).await;
    complete(&mut recovered, &b).await;
    assert_eq!(broker.get_orders().await.unwrap().len(), 2);
    let next = recovered.enqueue_command(entry()).unwrap();
    assert_eq!(recovered.pending[&next.id].admission_order, 3);
    drop(recovered.stop().await.unwrap());
}

#[tokio::test]
async fn recovered_dispatching_and_completed_commands_replay_evidence_without_resubmitting() {
    for dispatched_only in [false, true] {
        let fixture = Fixture::new();
        let broker = Arc::new(PaperBroker::new(AccountType::Paper, 100_000.0, 1));
        let mut driver = fixture.driver(broker.clone(), Default::default()).await;
        let receipt = driver.enqueue_command(entry()).unwrap();
        if dispatched_only {
            let mut row = checkpoint(&driver, &receipt);
            row.state = crate::core::insight::types::InsightState::New;
            commit_rows(&mut driver, vec![row]).await;
            let mut journal = driver.stop().await.unwrap();
            journal.mark_dispatched(&receipt.id, 10).await.unwrap();
            drop(journal);
        } else {
            complete(&mut driver, &receipt).await;
            assert_eq!(broker.get_orders().await.unwrap().len(), 1);
            drop(driver.stop().await.unwrap());
        }
        let before = broker.get_orders().await.unwrap().len();
        let (mut recovered, _) = StrategyCommandDriver::start_recovering(
            broker.clone(),
            fixture.path(),
            "strategy",
            "account",
            Default::default(),
        )
        .await
        .unwrap();
        recovered.finish_recovery();
        complete(&mut recovered, &receipt).await;
        assert_eq!(broker.get_orders().await.unwrap().len(), before);
        let Some(BrokerCommandProgress::Completed(completion)) = recovered.progress(&receipt)
        else {
            panic!("missing replayed evidence")
        };
        assert_eq!(
            matches!(completion.outcome, BrokerCommandOutcome::Ambiguous { .. }),
            dispatched_only
        );
        drop(recovered.stop().await.unwrap());
    }
}

#[tokio::test]
async fn recovered_queued_child_of_terminal_or_missing_parent_cancels_without_dispatch() {
    for missing_parent in [false, true] {
        let fixture = Fixture::new();
        let broker = Arc::new(PaperBroker::new(AccountType::Paper, 100_000.0, 1));
        let mut driver = fixture.driver(broker.clone(), Default::default()).await;
        let receipt = driver.enqueue_command(entry()).unwrap();
        let mut row = checkpoint(&driver, &receipt);
        row.state = crate::core::insight::types::InsightState::New;
        let mut parent = Insight::default();
        parent.symbol = "ETHUSD".into();
        parent.state = crate::core::insight::types::InsightState::Cancelled;
        row.parent_id = Some(parent.insight_id);
        let child_id = row.insight_id;
        let mut rows = vec![row];
        if !missing_parent {
            let mut parent =
                crate::core::insight::execution_checkpoint::InsightExecutionCheckpoint::capture(
                    &parent, 1, true, None,
                );
            parent.child_ids.push(child_id);
            rows.push(parent);
        }
        commit_rows(&mut driver, rows).await;
        drop(driver.stop().await.unwrap());
        let (mut recovered, _) = StrategyCommandDriver::start_recovering(
            broker.clone(),
            fixture.path(),
            "strategy",
            "account",
            Default::default(),
        )
        .await
        .unwrap();
        recovered.finish_recovery();
        complete(&mut recovered, &receipt).await;
        let Some(BrokerCommandProgress::Completed(completion)) = recovered.progress(&receipt)
        else {
            panic!("missing cancellation evidence")
        };
        assert!(matches!(
            completion.outcome,
            BrokerCommandOutcome::CancelledBeforeDispatch
        ));
        assert!(broker.get_orders().await.unwrap().is_empty());
        drop(recovered.stop().await.unwrap());
    }
}

async fn prepare(driver: &mut StrategyCommandDriver, receipt: &BrokerCommandReceipt) {
    if driver.pending.get(&receipt.id).unwrap().durable {
        return;
    }
    let mut row = checkpoint(driver, receipt);
    row.state = crate::core::insight::types::InsightState::New;
    driver
        .persist_execution_checkpoints(vec![row], vec![])
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while driver.checkpoint_in_flight() {
            driver.poll_acknowledgements(256).unwrap();
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    driver.dispatch_prepared(256).unwrap();
}

async fn complete(driver: &mut StrategyCommandDriver, receipt: &BrokerCommandReceipt) {
    prepare(driver, receipt).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while matches!(
            driver.progress(receipt),
            Some(BrokerCommandProgress::Queued)
        ) {
            driver.drain_ready(256, Duration::from_millis(5)).unwrap();
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        driver.progress(receipt),
        Some(BrokerCommandProgress::Completed(_))
    ));
}

fn checkpoint(
    driver: &StrategyCommandDriver,
    receipt: &BrokerCommandReceipt,
) -> crate::core::insight::execution_checkpoint::InsightExecutionCheckpoint {
    let mut insight = Insight::default();
    insight.insight_id = uuid::Uuid::parse_str(&receipt.insight_id).unwrap();
    insight.symbol = "ETHUSD".into();
    insight.state = crate::core::insight::types::InsightState::Executed;
    crate::core::insight::execution_checkpoint::InsightExecutionCheckpoint::capture(
        &insight,
        driver.checkpoint_revision(&insight.insight_id) + 1,
        true,
        None,
    )
}

async fn acknowledge(driver: &mut StrategyCommandDriver, receipt: &BrokerCommandReceipt) {
    driver.mark_reduced(receipt).unwrap();
    driver
        .persist_execution_checkpoints(vec![checkpoint(driver, receipt)], vec![receipt.clone()])
        .unwrap();
    // Receipt ownership cannot disappear merely because the ack was enqueued.
    assert!(driver.progress(receipt).is_some());
    tokio::time::timeout(Duration::from_secs(10), async {
        while driver.progress(receipt).is_some() {
            driver.poll_acknowledgements(256).unwrap();
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn bounded_receipts_reserve_cleanup_capacity_and_release_only_after_durable_ack() {
    let fixture = Fixture::new();
    let broker = Arc::new(PaperBroker::new(AccountType::Paper, 100_000.0, 1));
    let mut driver = fixture
        .driver(
            broker.clone(),
            BrokerCommandWorkerConfig {
                entry_capacity: 1,
                control_capacity: 1,
                completion_capacity: 1,
                ..Default::default()
            },
        )
        .await;
    let first = driver.enqueue_command(entry()).unwrap();
    complete(&mut driver, &first).await;
    let second = driver.enqueue_command(entry()).unwrap();
    complete(&mut driver, &second).await;
    assert_eq!(driver.pending_count(), 2);
    assert_eq!(
        driver.enqueue_command(entry()).unwrap_err(),
        BrokerCommandError::QueueFull
    );

    let cleanup = driver
        .enqueue(
            &first.insight_id,
            BrokerCommandAction::Cancel {
                order_id: first.insight_id.clone(),
            },
            None,
        )
        .unwrap();
    complete(&mut driver, &cleanup).await;
    assert_eq!(driver.pending_count(), 3);
    assert_eq!(
        driver
            .enqueue(
                &second.insight_id,
                BrokerCommandAction::Cancel {
                    order_id: second.insight_id.clone(),
                },
                None
            )
            .unwrap_err(),
        BrokerCommandError::QueueFull
    );

    acknowledge(&mut driver, &first).await;
    assert_eq!(driver.pending_count(), 2);
    let third = driver.enqueue_command(entry()).unwrap();
    complete(&mut driver, &third).await;
    let journal = driver.stop().await.unwrap();
    assert_eq!(
        journal.get(&first.id).await.unwrap().unwrap().state,
        JournalCommandState::Applied
    );
    assert_eq!(journal.recovery_page(0, 1000).await.unwrap().len(), 3);
}

#[tokio::test]
async fn receipt_identity_is_exact_and_duplicate_admission_never_redispatches() {
    let fixture = Fixture::new();
    let broker = Arc::new(PaperBroker::new(AccountType::Paper, 100_000.0, 1));
    let mut driver = fixture.driver(broker.clone(), Default::default()).await;
    let command = entry();
    let receipt = driver.enqueue_command(command.clone()).unwrap();
    assert_eq!(driver.enqueue_command(command.clone()).unwrap(), receipt);
    let mut conflict = command.clone();
    if let BrokerCommandAction::Submit { request } = &mut conflict.action {
        request.quantity = Some(2.0);
    }
    assert_eq!(
        driver.enqueue_command(conflict).unwrap_err(),
        BrokerCommandError::IdentityConflict
    );
    complete(&mut driver, &receipt).await;
    assert_eq!(driver.enqueue_command(command).unwrap(), receipt);
    assert_eq!(broker.get_orders().await.unwrap().len(), 1);
    let mut forged = receipt.clone();
    forged.insight_id = "another-insight".into();
    assert!(driver.progress(&forged).is_none());
    assert_eq!(
        driver.mark_reduced(&forged).unwrap_err(),
        BrokerCommandError::IdentityConflict
    );
    assert!(
        driver
            .persist_execution_checkpoints(
                vec![checkpoint(&driver, &receipt)],
                vec![receipt.clone()]
            )
            .is_err()
    );
    acknowledge(&mut driver, &receipt).await;
    assert!(driver.recovery_page(0, 256).await.unwrap().is_empty());
    drop(driver.stop().await.unwrap());
}

#[tokio::test]
async fn recovery_does_not_redispatch_ambiguous_intents_or_allow_them_to_be_forgotten() {
    let fixture = Fixture::new();
    let command = entry();
    let now = chrono::Utc::now().timestamp_micros();
    let mut journal = BrokerCommandJournal::open(fixture.path(), "strategy", "account")
        .await
        .unwrap();
    journal.prepare(&command, now).await.unwrap();
    journal.mark_dispatched(&command.id, now).await.unwrap();
    drop(journal);
    let broker = Arc::new(PaperBroker::new(AccountType::Paper, 100_000.0, 1));
    let mut driver = fixture.driver(broker.clone(), Default::default()).await;
    let recovery = driver.recovery_page(0, 256).await.unwrap();
    assert_eq!(recovery.len(), 1);
    assert_eq!(recovery[0].state, JournalCommandState::Dispatching);
    assert_eq!(driver.pending_count(), 0);
    assert!(broker.get_orders().await.unwrap().is_empty());
    let receipt = driver.enqueue_command(command).unwrap();
    complete(&mut driver, &receipt).await;
    let Some(BrokerCommandProgress::Completed(completion)) = driver.progress(&receipt) else {
        panic!("missing completion");
    };
    assert!(matches!(
        completion.outcome,
        BrokerCommandOutcome::Ambiguous { .. }
    ));
    driver.mark_reduced(&receipt).unwrap();
    assert!(
        driver
            .persist_execution_checkpoints(
                vec![checkpoint(&driver, &receipt)],
                vec![receipt.clone()]
            )
            .is_err()
    );
    assert_eq!(driver.pending_count(), 1);
    assert!(broker.get_orders().await.unwrap().is_empty());
    assert_eq!(driver.recovery_page(0, 256).await.unwrap().len(), 1);
    drop(driver.stop().await.unwrap());
}

#[tokio::test]
async fn ambiguous_cleanup_requires_terminal_checkpoint_for_exact_order() {
    let fixture = Fixture::new();
    let mut command = entry();
    command.action = BrokerCommandAction::Close {
        order_id: "target-order".into(),
        quantity: 1.0,
        price: None,
    };
    let now = chrono::Utc::now().timestamp_micros();
    let mut journal = BrokerCommandJournal::open(fixture.path(), "strategy", "account")
        .await
        .unwrap();
    journal.prepare(&command, now).await.unwrap();
    journal.mark_dispatched(&command.id, now).await.unwrap();
    drop(journal);
    let broker = Arc::new(PaperBroker::new(AccountType::Paper, 100_000.0, 1));
    let mut driver = fixture.driver(broker, Default::default()).await;
    let receipt = driver.enqueue_command(command).unwrap();
    complete(&mut driver, &receipt).await;
    driver.mark_reduced(&receipt).unwrap();
    let mut row = checkpoint(&driver, &receipt);
    row.order_id = Some("target-order".into());
    assert!(
        driver
            .persist_execution_checkpoints(vec![row.clone()], vec![receipt.clone()])
            .is_err()
    );
    row.state = crate::core::insight::types::InsightState::Closed;
    row.order_id = Some("wrong-order".into());
    assert!(
        driver
            .persist_execution_checkpoints(vec![row.clone()], vec![receipt.clone()])
            .is_err()
    );
    row.order_id = Some("target-order".into());
    driver
        .persist_execution_checkpoints(vec![row], vec![receipt.clone()])
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while driver.progress(&receipt).is_some() {
            driver.poll_acknowledgements(256).unwrap();
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(driver.recovery_page(0, 256).await.unwrap().is_empty());
    drop(driver.stop().await.unwrap());
}

#[tokio::test]
async fn zero_completion_budget_does_not_consume_and_stop_blocks_admission() {
    let fixture = Fixture::new();
    let broker = Arc::new(PaperBroker::new(AccountType::Paper, 100_000.0, 1));
    let mut driver = fixture.driver(broker, Default::default()).await;
    let receipt = driver.enqueue_command(entry()).unwrap();
    prepare(&mut driver, &receipt).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while !driver.has_ready_events() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(driver.drain_ready(0, Duration::ZERO).unwrap().is_empty());
    assert!(matches!(
        driver.progress(&receipt),
        Some(BrokerCommandProgress::Queued)
    ));
    assert_eq!(driver.drain_ready(1, Duration::ZERO).unwrap().len(), 1);
    driver.stop_entries();
    assert_eq!(
        driver.enqueue_command(entry()).unwrap_err(),
        BrokerCommandError::Stopping
    );
    let journal = driver.stop().await.unwrap();
    assert_eq!(journal.recovery_page(0, 256).await.unwrap().len(), 1);
}

#[tokio::test]
async fn atomic_checkpoint_batches_preserve_receipts_until_ack_and_obey_release_budget() {
    let fixture = Fixture::new();
    let broker = Arc::new(PaperBroker::new(AccountType::Paper, 100_000.0, 1));
    let mut driver = fixture.driver(broker, Default::default()).await;
    let first = driver.enqueue_command(entry()).unwrap();
    let second = driver.enqueue_command(entry()).unwrap();
    complete(&mut driver, &first).await;
    complete(&mut driver, &second).await;
    driver.mark_reduced(&first).unwrap();
    driver.mark_reduced(&second).unwrap();
    driver
        .persist_execution_checkpoints(
            vec![checkpoint(&driver, &first), checkpoint(&driver, &second)],
            vec![first.clone(), second.clone()],
        )
        .unwrap();
    assert_eq!(driver.pending_count(), 2);
    assert_eq!(
        driver
            .persist_execution_checkpoints(vec![checkpoint(&driver, &first)], vec![])
            .unwrap_err(),
        BrokerCommandError::QueueFull
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if driver.poll_acknowledgements(1).unwrap() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(driver.pending_count(), 1);
    assert_eq!(driver.poll_acknowledgements(0).unwrap(), 0);
    assert_eq!(driver.poll_acknowledgements(1).unwrap(), 1);
    assert_eq!(driver.pending_count(), 0);
    let page = driver
        .execution_checkpoint_page(String::new(), 32)
        .await
        .unwrap();
    assert_eq!(page.rows.len(), 2);
    assert!(driver.recovery_page(0, 256).await.unwrap().is_empty());
    drop(driver.stop().await.unwrap());
}

#[tokio::test]
async fn recovery_requests_remain_live_behind_a_full_completion_queue() {
    let fixture = Fixture::new();
    let broker = Arc::new(PaperBroker::new(AccountType::Paper, 100_000.0, 1));
    let mut driver = fixture
        .driver(
            broker.clone(),
            BrokerCommandWorkerConfig {
                entry_capacity: 4,
                completion_capacity: 1,
                ..Default::default()
            },
        )
        .await;
    let first = driver.enqueue_command(entry()).unwrap();
    let second = driver.enqueue_command(entry()).unwrap();
    prepare(&mut driver, &first).await;
    prepare(&mut driver, &second).await;
    // Do not drain the one-slot completion channel. The worker's second result
    // will block on delivery after executing and journaling the command.
    tokio::time::timeout(Duration::from_secs(10), async {
        while broker.get_orders().await.unwrap().len() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let recovery = tokio::time::timeout(Duration::from_secs(10), driver.recovery_page(0, 256))
        .await
        .expect("journal control must not wait for completion consumption")
        .unwrap();
    assert_eq!(recovery.len(), 2);
    assert!(
        recovery
            .iter()
            .all(|record| record.state == JournalCommandState::Completed)
    );
    complete(&mut driver, &first).await;
    complete(&mut driver, &second).await;
    acknowledge(&mut driver, &first).await;
    acknowledge(&mut driver, &second).await;
    assert!(driver.recovery_page(0, 256).await.unwrap().is_empty());
    drop(driver.stop().await.unwrap());
}
