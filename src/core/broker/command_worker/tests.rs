use super::*;
use crate::core::broker::{
    commands::{
        BrokerCommandId, BrokerDispatchConstraints, BrokerOperationGroup, BrokerOrderRequest,
    },
    paper_broker::PaperBroker,
    types::{AccountType, OrderClass, OrderSide, OrderType},
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

fn checkpoint_batch(receipt: &BrokerCommandReceipt) -> ExecutionCheckpointBatch {
    let mut insight = crate::core::insight::Insight::default();
    insight.insight_id = uuid::Uuid::parse_str(&receipt.insight_id).unwrap();
    insight.symbol = "ETHUSD".into();
    let checkpoint =
        crate::core::insight::execution_checkpoint::InsightExecutionCheckpoint::capture(
            &insight, 1, true, None,
        );
    ExecutionCheckpointBatch::new(vec![checkpoint], vec![receipt.clone()]).unwrap()
}

struct Fixture(std::path::PathBuf);
impl Fixture {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("aqe-command-test-{}", uuid::Uuid::new_v4())))
    }
    async fn journal(&self) -> BrokerCommandJournal {
        BrokerCommandJournal::open(self.0.join("commands.db"), "strategy", "account")
            .await
            .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn request(id: &str) -> BrokerOrderRequest {
    BrokerOrderRequest {
        client_order_id: id.into(),
        insight_id: id.into(),
        strategy_type: "Test".into(),
        symbol: "ETHUSD".into(),
        quantity: Some(1.0),
        side: OrderSide::Buy,
        order_type: OrderType::Limit,
        order_class: OrderClass::Bracket,
        limit_price: Some(100.0),
        stop_price: None,
        take_profit_levels: Some(vec![110.0]),
        stop_loss_levels: Some(vec![90.0]),
        trailing_stop_price: None,
    }
}

fn entry(id: &str) -> BrokerCommand {
    BrokerCommand::new(
        "strategy",
        "account",
        id,
        BrokerCommandAction::Submit {
            request: request(id),
        },
        None,
    )
}

fn quantity_limited_entry(id: &str, quantity: f64, limit: f64) -> BrokerCommand {
    let mut request = request(id);
    request.quantity = Some(quantity);
    BrokerCommand::new_with_constraints(
        "strategy",
        "account",
        id,
        BrokerCommandAction::Submit { request },
        None,
        BrokerDispatchConstraints {
            max_total_symbol_quantity: Some(limit),
        },
    )
}

#[test]
fn submission_reconciliation_schedule_is_identity_deduplicated_and_bounded() {
    let mut pending = VecDeque::new();
    let duplicate = entry("duplicate");
    schedule_submission_reconciliation(&mut pending, &duplicate);
    schedule_submission_reconciliation(&mut pending, &duplicate);
    assert_eq!(pending.len(), 1);
    for index in 1..MAX_SUBMISSION_RECONCILIATIONS {
        schedule_submission_reconciliation(&mut pending, &entry(&format!("command-{index}")));
    }
    assert_eq!(pending.len(), MAX_SUBMISSION_RECONCILIATIONS);
    schedule_submission_reconciliation(&mut pending, &entry("overflow"));
    assert_eq!(pending.len(), MAX_SUBMISSION_RECONCILIATIONS);
}

fn cancel(id: &str) -> BrokerCommand {
    BrokerCommand::new(
        "strategy",
        "account",
        id,
        BrokerCommandAction::Cancel {
            order_id: id.into(),
        },
        None,
    )
}

struct Probe {
    calls: parking_lot::Mutex<Vec<String>>,
    first: AtomicBool,
    started: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
    active: AtomicUsize,
    maximum_active: AtomicUsize,
}

struct ReceiptProbe {
    submissions: AtomicUsize,
    lookups: AtomicUsize,
}

struct DelayedReceiptProbe {
    submissions: AtomicUsize,
    lookups: AtomicUsize,
}

struct MarginProbe {
    probe: Probe,
    required: f64,
}

impl CommandExecutor for MarginProbe {
    async fn execute(&self, action: BrokerCommandAction) -> BrokerCommandOutcome {
        self.probe.execute(action).await
    }

    async fn estimate_order_margin(
        &self,
        _request: &BrokerOrderRequest,
    ) -> Result<Option<f64>, super::super::types::BrokerError> {
        Ok(Some(self.required))
    }
}

impl CommandExecutor for DelayedReceiptProbe {
    async fn execute(&self, _action: BrokerCommandAction) -> BrokerCommandOutcome {
        self.submissions.fetch_add(1, Ordering::SeqCst);
        BrokerCommandOutcome::Ambiguous {
            reason: "response lost after broker acceptance".into(),
        }
    }

    async fn reconcile_submission(
        &self,
        _account_id: &str,
        request: &BrokerOrderRequest,
    ) -> Result<SubmissionResolution, super::super::types::BrokerError> {
        use crate::core::broker::traits::OrderManagementProvider;
        if self.lookups.fetch_add(1, Ordering::SeqCst) == 0 {
            return Ok(SubmissionResolution::Unknown);
        }
        let paper = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        Ok(SubmissionResolution::Confirmed(
            paper.submit_order(request.clone()).await?,
        ))
    }
}

impl CommandExecutor for ReceiptProbe {
    async fn execute(&self, _action: BrokerCommandAction) -> BrokerCommandOutcome {
        self.submissions.fetch_add(1, Ordering::SeqCst);
        BrokerCommandOutcome::Ambiguous {
            reason: "response lost after broker acceptance".into(),
        }
    }

    async fn reconcile_submission(
        &self,
        _account_id: &str,
        request: &BrokerOrderRequest,
    ) -> Result<SubmissionResolution, super::super::types::BrokerError> {
        use crate::core::broker::traits::OrderManagementProvider;
        self.lookups.fetch_add(1, Ordering::SeqCst);
        let paper = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        Ok(SubmissionResolution::Confirmed(
            paper.submit_order(request.clone()).await?,
        ))
    }
}

#[tokio::test]
async fn ambiguous_submit_is_confirmed_by_exact_receipt_before_delivery() {
    let fixture = Fixture::new();
    let executor = Arc::new(ReceiptProbe {
        submissions: AtomicUsize::new(0),
        lookups: AtomicUsize::new(0),
    });
    let mut worker = BrokerCommandWorker::start_executor(
        executor.clone(),
        fixture.journal().await,
        Default::default(),
    )
    .unwrap();
    let command = entry("receipt-confirmed");
    worker.sender.try_enqueue(command.clone()).unwrap();
    let completion = next(&mut worker).await;
    assert!(matches!(
        completion.outcome,
        BrokerCommandOutcome::OrderResponse { .. }
    ));
    assert_eq!(executor.submissions.load(Ordering::SeqCst), 1);
    assert_eq!(executor.lookups.load(Ordering::SeqCst), 1);
    let journal = finish(worker).await;
    assert!(matches!(
        journal
            .get(&command.id)
            .await
            .unwrap()
            .unwrap()
            .completion
            .unwrap()
            .outcome,
        BrokerCommandOutcome::OrderResponse { .. }
    ));
}

#[tokio::test]
async fn recovered_ambiguous_submit_is_looked_up_without_resubmission() {
    let fixture = Fixture::new();
    let command = entry("recovered-receipt");
    let mut journal = fixture.journal().await;
    journal.prepare(&command, 10).await.unwrap();
    journal.mark_dispatched(&command.id, 11).await.unwrap();
    journal
        .complete(&BrokerCommandCompletion {
            receipt: command.receipt(),
            queued_at_us: 10,
            dispatched_at_us: Some(11),
            completed_at_us: 12,
            outcome: BrokerCommandOutcome::Ambiguous {
                reason: "process lost the submit response".into(),
            },
        })
        .await
        .unwrap();
    let executor = Arc::new(ReceiptProbe {
        submissions: AtomicUsize::new(0),
        lookups: AtomicUsize::new(0),
    });
    let mut worker =
        BrokerCommandWorker::start_executor(executor.clone(), journal, Default::default()).unwrap();
    worker.sender.try_enqueue(command).unwrap();
    let completion = next(&mut worker).await;
    assert!(matches!(
        completion.outcome,
        BrokerCommandOutcome::OrderResponse { .. }
    ));
    assert_eq!(executor.submissions.load(Ordering::SeqCst), 0);
    assert_eq!(executor.lookups.load(Ordering::SeqCst), 1);
    drop(finish(worker).await);
}

#[tokio::test]
async fn ambiguous_submit_gets_bounded_same_run_proof_retry_without_resubmission() {
    let fixture = Fixture::new();
    let executor = Arc::new(DelayedReceiptProbe {
        submissions: AtomicUsize::new(0),
        lookups: AtomicUsize::new(0),
    });
    let mut worker = BrokerCommandWorker::start_executor(
        executor.clone(),
        fixture.journal().await,
        Default::default(),
    )
    .unwrap();
    let command = entry("delayed-receipt");
    worker.sender.try_enqueue(command.clone()).unwrap();
    let ambiguous = next(&mut worker).await;
    assert!(matches!(
        ambiguous.outcome,
        BrokerCommandOutcome::Ambiguous { .. }
    ));
    let confirmed = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let BrokerCommandWorkerEvent::Completion(completion) =
                worker.events.recv().await.unwrap()
            {
                break completion;
            }
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        confirmed.outcome,
        BrokerCommandOutcome::OrderResponse { .. }
    ));
    assert_eq!(executor.submissions.load(Ordering::SeqCst), 1);
    assert_eq!(executor.lookups.load(Ordering::SeqCst), 2);
    let journal = finish(worker).await;
    assert!(matches!(
        journal
            .get(&command.id)
            .await
            .unwrap()
            .unwrap()
            .completion
            .unwrap()
            .outcome,
        BrokerCommandOutcome::OrderResponse { .. }
    ));
}

fn account_cache() -> Arc<BrokerSnapshotCache> {
    let cache = Arc::new(BrokerSnapshotCache::new("account", Default::default()).unwrap());
    refresh_account(&cache, "account");
    cache
}

fn refresh_account(cache: &BrokerSnapshotCache, id: &str) {
    cache.seed_account_for_test(
        crate::core::broker::types::Account {
            account_id: id.into(),
            account_type: AccountType::Paper,
            cash: 100_000.0,
            equity: 100_000.0,
            buying_power: 100_000.0,
            currency: "USD".into(),
            accrued_commission: 0.0,
            shorting_enabled: true,
            leverage: 1,
            max_total_orders: None,
        },
        std::time::Instant::now(),
    );
}

async fn wait_queued(worker: &BrokerCommandWorker, count: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let rows = worker
                .sender
                .try_recovery_page(0, 32)
                .unwrap()
                .wait()
                .await
                .unwrap();
            if rows.len() == count
                && rows
                    .iter()
                    .all(|row| row.state == JournalCommandState::Queued)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

struct ClockProbe {
    probe: Probe,
    now: parking_lot::Mutex<chrono::DateTime<chrono::Utc>>,
}
impl CommandExecutor for ClockProbe {
    fn current_time(&self) -> chrono::DateTime<chrono::Utc> {
        *self.now.lock()
    }
    async fn execute(&self, action: BrokerCommandAction) -> BrokerCommandOutcome {
        self.probe.execute(action).await
    }
}

#[tokio::test]
async fn expired_queued_entry_is_cancelled_at_dispatch_using_broker_clock() {
    let fixture = Fixture::new();
    let now = chrono::DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z")
        .unwrap()
        .to_utc();
    let executor = Arc::new(ClockProbe {
        probe: Probe::new(false),
        now: parking_lot::Mutex::new(now),
    });
    let lane =
        account_lane::AccountLane::shared("expiry fixture", &uuid::Uuid::new_v4().to_string())
            .unwrap();
    let held = lane.clone().acquire(false).await.unwrap();
    let mut worker = BrokerCommandWorker::start_executor_with_lane(
        executor.clone(),
        fixture.journal().await,
        Default::default(),
        Some(lane.clone()),
        None,
    )
    .unwrap();
    let command = entry("expires-while-waiting");
    let id = command.id.clone();
    let deadline = now + chrono::TimeDelta::seconds(1);
    worker.sender.set_entry_deadline(id.clone(), Some(deadline));
    worker.sender.try_enqueue(command).unwrap();
    wait_queued(&worker, 1).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while lane.waiter_counts() != [0, 1] {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    *executor.now.lock() = deadline;
    drop(held);
    let completion = next(&mut worker).await;
    assert!(matches!(
        completion.outcome,
        BrokerCommandOutcome::CancelledBeforeDispatch
    ));
    assert!(completion.dispatched_at_us.is_none());
    assert!(executor.probe.calls.lock().is_empty());
    worker.sender.release_entry_identity(&id);
    assert!(worker.sender.gate.lock().deadlines.is_empty());
    drop(finish(worker).await);
}

#[tokio::test]
async fn historical_clock_entry_is_valid_and_expiry_cannot_cancel_an_inflight_call() {
    let fixture = Fixture::new();
    let now = chrono::DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z")
        .unwrap()
        .to_utc();
    let executor = Arc::new(ClockProbe {
        probe: Probe::new(true),
        now: parking_lot::Mutex::new(now),
    });
    let mut worker = BrokerCommandWorker::start_executor(
        executor.clone(),
        fixture.journal().await,
        Default::default(),
    )
    .unwrap();
    let command = entry("historical-valid-entry");
    let deadline = now + chrono::TimeDelta::seconds(1);
    worker
        .sender
        .set_entry_deadline(command.id.clone(), Some(deadline));
    worker.sender.try_enqueue(command).unwrap();
    tokio::time::timeout(Duration::from_secs(2), executor.probe.started.notified())
        .await
        .unwrap();
    *executor.now.lock() = deadline;
    executor.probe.release.add_permits(1);
    let completion = next(&mut worker).await;
    assert!(matches!(
        completion.outcome,
        BrokerCommandOutcome::Acknowledged
    ));
    assert!(completion.dispatched_at_us.is_some());
    assert_eq!(executor.probe.calls.lock().len(), 1);
    drop(finish(worker).await);
}

#[tokio::test]
async fn expired_entry_does_not_wait_for_missing_account_snapshot() {
    let fixture = Fixture::new();
    let executor = Arc::new(Probe::new(false));
    let mut worker = BrokerCommandWorker::start_executor(
        executor.clone(),
        fixture.journal().await,
        Default::default(),
    )
    .unwrap();
    let cache = account_cache();
    worker.sender.install_snapshot_guard(cache.clone()).unwrap();
    cache.invalidate();
    let command = entry("expired-stale-account");
    worker.sender.set_entry_deadline(
        command.id.clone(),
        Some(chrono::Utc::now() - chrono::TimeDelta::seconds(1)),
    );
    worker.sender.try_enqueue(command).unwrap();
    assert!(matches!(
        next(&mut worker).await.outcome,
        BrokerCommandOutcome::CancelledBeforeDispatch
    ));
    assert!(executor.calls.lock().is_empty());
    drop(finish(worker).await);
}

#[tokio::test]
async fn stale_account_defers_both_lanes_and_refresh_preserves_control_priority() {
    let fixture = Fixture::new();
    let probe = Arc::new(Probe::new(false));
    let cache = account_cache();
    let mut worker = BrokerCommandWorker::start_executor(
        probe.clone(),
        fixture.journal().await,
        Default::default(),
    )
    .unwrap();
    worker.sender.install_snapshot_guard(cache.clone()).unwrap();
    cache.invalidate();
    worker.sender.try_enqueue(entry("deferred-entry")).unwrap();
    worker
        .sender
        .try_enqueue(cancel("deferred-cancel"))
        .unwrap();
    wait_queued(&worker, 2).await;
    assert!(probe.calls.lock().is_empty());
    assert!(
        worker.events.is_empty(),
        "unavailable data must not become rejection evidence"
    );
    refresh_account(&cache, "account");
    assert_eq!(
        next(&mut worker).await.receipt.insight_id,
        "deferred-cancel"
    );
    assert_eq!(next(&mut worker).await.receipt.insight_id, "deferred-entry");
    assert_eq!(probe.maximum_active.load(Ordering::SeqCst), 1);
    drop(finish(worker).await);
}

#[tokio::test]
async fn stop_cancels_parked_entry_without_waiting_for_account_refresh() {
    let fixture = Fixture::new();
    let probe = Arc::new(Probe::new(false));
    let cache = account_cache();
    let mut worker = BrokerCommandWorker::start_executor(
        probe.clone(),
        fixture.journal().await,
        Default::default(),
    )
    .unwrap();
    worker.sender.install_snapshot_guard(cache.clone()).unwrap();
    cache.invalidate();
    worker.sender.try_enqueue(entry("parked-entry")).unwrap();
    wait_queued(&worker, 1).await;
    let stop_started = std::time::Instant::now();
    worker.sender.request_execution_stop();
    assert!(worker.sender.entry_is_gated(&entry("parked-entry").id));
    assert!(stop_started.elapsed() < Duration::from_millis(100));
    // Durable completion latency includes storage, unlike immediate dispatch
    // gating. Do not turn the stop-dispatch target into an fsync deadline.
    let completion = tokio::time::timeout(Duration::from_secs(2), next(&mut worker))
        .await
        .unwrap();
    assert!(matches!(
        completion.outcome,
        BrokerCommandOutcome::CancelledBeforeDispatch
    ));
    assert_eq!(completion.dispatched_at_us, None);
    assert!(probe.calls.lock().is_empty());
    drop(finish(worker).await);
}

#[tokio::test]
async fn account_change_refuses_even_teardown_mutations() {
    let fixture = Fixture::new();
    let probe = Arc::new(Probe::new(false));
    let cache = account_cache();
    let worker = BrokerCommandWorker::start_executor(
        probe.clone(),
        fixture.journal().await,
        Default::default(),
    )
    .unwrap();
    worker.sender.install_snapshot_guard(cache.clone()).unwrap();
    refresh_account(&cache, "different-account");
    worker
        .sender
        .try_enqueue(cancel("old-account-order"))
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(2), worker.task)
        .await
        .unwrap()
        .unwrap()
        .err()
        .expect("changed account must stop dispatch");
    assert!(error.to_string().contains("account changed"));
    assert!(probe.calls.lock().is_empty());
    drop(worker.events);
    drop(worker.sender);
    let journal = fixture.journal().await;
    assert_eq!(
        journal.execution_lifecycle().phase,
        crate::core::broker::command_journal::lifecycle::ExecutionPhase::Stopping
    );
    let rows = journal.recovery_page(0, 32).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].state, JournalCommandState::Queued);
    assert!(rows[0].completion.is_none());
}

#[tokio::test]
async fn entry_risk_rechecks_account_after_waiting_without_blocking_cleanup() {
    let fixture = Fixture::new();
    let probe = Arc::new(Probe::new(true));
    let cache = account_cache();
    let mut worker = BrokerCommandWorker::start_executor(
        probe.clone(),
        fixture.journal().await,
        Default::default(),
    )
    .unwrap();
    worker.sender.install_snapshot_guard(cache.clone()).unwrap();
    worker.sender.try_enqueue(entry("first")).unwrap();
    probe.started.notified().await;
    worker
        .sender
        .try_enqueue(entry("no-longer-affordable"))
        .unwrap();
    let mut account = (*cache.account().unwrap()).clone();
    account.buying_power = 0.0;
    cache.seed_account_for_test(account, std::time::Instant::now());
    probe.release.add_permits(1);
    assert_eq!(next(&mut worker).await.receipt.insight_id, "first");
    let declined = next(&mut worker).await;
    assert!(
        matches!(declined.outcome, BrokerCommandOutcome::Declined { ref reason } if reason.contains("buying power"))
    );
    assert_eq!(declined.dispatched_at_us, None);
    assert_eq!(probe.calls.lock().len(), 1);
    worker
        .sender
        .try_enqueue(cancel("cleanup-with-no-buying-power"))
        .unwrap();
    assert_eq!(
        next(&mut worker).await.receipt.insight_id,
        "cleanup-with-no-buying-power"
    );
    assert_eq!(probe.calls.lock().len(), 2);
    drop(finish(worker).await);
}

#[test]
fn entry_risk_refuses_insolvent_accounts_but_never_blocks_close() {
    let mut account = (*account_cache().account().unwrap()).clone();
    let close = BrokerCommandAction::Close {
        order_id: "position".into(),
        quantity: 1.0,
        price: None,
    };
    for equity in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        account.equity = equity;
        assert!(entry_account_rejection(&entry("entry").action, &account).is_some());
        assert!(entry_account_rejection(&close, &account).is_none());
    }
}

#[tokio::test]
async fn entry_risk_checks_shorting_permission_at_dispatch() {
    let fixture = Fixture::new();
    let probe = Arc::new(Probe::new(false));
    let cache = account_cache();
    let mut account = (*cache.account().unwrap()).clone();
    account.shorting_enabled = false;
    account.cash = -10.0; // Leveraged cash is not the entry eligibility metric.
    cache.seed_account_for_test(account, std::time::Instant::now());
    let mut worker = BrokerCommandWorker::start_executor(
        probe.clone(),
        fixture.journal().await,
        Default::default(),
    )
    .unwrap();
    worker.sender.install_snapshot_guard(cache).unwrap();
    let mut sell = request("short-disabled");
    sell.side = OrderSide::Sell;
    worker
        .sender
        .try_enqueue(BrokerCommand::new(
            "strategy",
            "account",
            "short-disabled",
            BrokerCommandAction::Submit { request: sell },
            None,
        ))
        .unwrap();
    let declined = next(&mut worker).await;
    assert!(
        matches!(declined.outcome, BrokerCommandOutcome::Declined { ref reason } if reason.contains("shorting"))
    );
    assert_eq!(declined.dispatched_at_us, None);
    assert!(probe.calls.lock().is_empty());
    worker.sender.try_enqueue(entry("buy-allowed")).unwrap();
    assert!(!matches!(
        next(&mut worker).await.outcome,
        BrokerCommandOutcome::Declined { .. }
    ));
    assert_eq!(probe.calls.lock().len(), 1);
    drop(finish(worker).await);
}

#[tokio::test]
async fn entry_capacity_waits_for_snapshot_then_refuses_full_broker_without_blocking_cancel() {
    let fixture = Fixture::new();
    let probe = Arc::new(Probe::new(false));
    let cache = account_cache();
    let mut account = (*cache.account().unwrap()).clone();
    account.max_total_orders = Some(0);
    cache.seed_account_for_test(account, std::time::Instant::now());
    let mut worker = BrokerCommandWorker::start_executor(
        probe.clone(),
        fixture.journal().await,
        Default::default(),
    )
    .unwrap();
    worker.sender.install_snapshot_guard(cache.clone()).unwrap();
    worker.sender.try_enqueue(entry("limited")).unwrap();
    wait_queued(&worker, 1).await;
    assert!(probe.calls.lock().is_empty());
    cache.seed_orders_for_test(vec![]);
    let result = next(&mut worker).await;
    assert!(
        matches!(result.outcome, BrokerCommandOutcome::Declined { ref reason } if reason.contains("pending-order limit"))
    );
    assert!(result.dispatched_at_us.is_none());
    worker.sender.try_enqueue(cancel("cleanup")).unwrap();
    assert_eq!(next(&mut worker).await.receipt.insight_id, "cleanup");
    assert_eq!(probe.calls.lock().len(), 1);
    drop(finish(worker).await);
}

#[tokio::test]
async fn shared_capacity_does_not_reuse_slots_before_post_completion_snapshot() {
    let fixture = Fixture::new();
    let other_fixture = Fixture::new();
    let probe = Arc::new(Probe::new(false));
    let cache = account_cache();
    let mut account = (*cache.account().unwrap()).clone();
    account.max_total_orders = Some(1);
    cache.seed_account_for_test(account, std::time::Instant::now());
    cache.seed_orders_for_test(vec![]);
    let lane =
        account_lane::AccountLane::shared("capacity fixture", &uuid::Uuid::new_v4().to_string())
            .unwrap();
    let mut worker = BrokerCommandWorker::start_executor_with_lane(
        probe.clone(),
        fixture.journal().await,
        Default::default(),
        Some(lane.clone()),
        None,
    )
    .unwrap();
    let mut other = BrokerCommandWorker::start_executor_with_lane(
        probe.clone(),
        other_fixture.journal().await,
        Default::default(),
        Some(lane),
        None,
    )
    .unwrap();
    worker.sender.install_snapshot_guard(cache.clone()).unwrap();
    other.sender.install_snapshot_guard(cache.clone()).unwrap();
    worker.sender.try_enqueue(entry("first-slot")).unwrap();
    assert!(matches!(
        next(&mut worker).await.outcome,
        BrokerCommandOutcome::Acknowledged
    ));
    other.sender.try_enqueue(entry("same-slot")).unwrap();
    let refused = next(&mut other).await;
    assert!(matches!(
        refused.outcome,
        BrokerCommandOutcome::Declined { .. }
    ));
    assert!(refused.dispatched_at_us.is_none());
    worker
        .sender
        .try_enqueue(cancel("cleanup-reserved"))
        .unwrap();
    assert!(matches!(
        next(&mut worker).await.outcome,
        BrokerCommandOutcome::Acknowledged
    ));
    cache.seed_orders_for_test(vec![]);
    worker.sender.try_enqueue(entry("fresh-slot")).unwrap();
    assert!(matches!(
        next(&mut worker).await.outcome,
        BrokerCommandOutcome::Acknowledged
    ));
    assert_eq!(probe.calls.lock().len(), 3);
    drop(finish(other).await);
    drop(finish(worker).await);
}

#[tokio::test]
async fn shared_account_lane_reserves_authoritative_margin_between_workers() {
    let first_fixture = Fixture::new();
    let second_fixture = Fixture::new();
    let executor = Arc::new(MarginProbe {
        probe: Probe::new(false),
        required: 60_000.0,
    });
    let cache = account_cache();
    let lane =
        account_lane::AccountLane::shared("margin capacity", &uuid::Uuid::new_v4().to_string())
            .unwrap();
    let mut first = BrokerCommandWorker::start_executor_with_lane(
        executor.clone(),
        first_fixture.journal().await,
        Default::default(),
        Some(lane.clone()),
        None,
    )
    .unwrap();
    let mut second = BrokerCommandWorker::start_executor_with_lane(
        executor.clone(),
        second_fixture.journal().await,
        Default::default(),
        Some(lane),
        None,
    )
    .unwrap();
    first.sender.install_snapshot_guard(cache.clone()).unwrap();
    second.sender.install_snapshot_guard(cache.clone()).unwrap();

    first.sender.try_enqueue(entry("margin-first")).unwrap();
    assert!(matches!(
        next(&mut first).await.outcome,
        BrokerCommandOutcome::Acknowledged
    ));
    second.sender.try_enqueue(entry("margin-second")).unwrap();
    let refused = next(&mut second).await;
    assert!(
        matches!(refused.outcome, BrokerCommandOutcome::Declined { ref reason } if reason.contains("margin requirement"))
    );
    assert!(refused.dispatched_at_us.is_none());

    refresh_account(&cache, "account");
    second.sender.try_enqueue(entry("margin-fresh")).unwrap();
    assert!(matches!(
        next(&mut second).await.outcome,
        BrokerCommandOutcome::Acknowledged
    ));
    assert_eq!(executor.probe.calls.lock().len(), 2);
    drop(finish(first).await);
    drop(finish(second).await);
}

#[tokio::test]
async fn shared_symbol_capacity_counts_inflight_entries_until_broker_snapshots_advance() {
    let fixture = Fixture::new();
    let other_fixture = Fixture::new();
    let probe = Arc::new(Probe::new(false));
    let cache = account_cache();
    cache.seed_positions_for_test(vec![]);
    cache.seed_orders_for_test(vec![]);
    let lane =
        account_lane::AccountLane::shared("symbol capacity", &uuid::Uuid::new_v4().to_string())
            .unwrap();
    let mut first = BrokerCommandWorker::start_executor_with_lane(
        probe.clone(),
        fixture.journal().await,
        Default::default(),
        Some(lane.clone()),
        None,
    )
    .unwrap();
    let mut second = BrokerCommandWorker::start_executor_with_lane(
        probe.clone(),
        other_fixture.journal().await,
        Default::default(),
        Some(lane),
        None,
    )
    .unwrap();
    first.sender.install_snapshot_guard(cache.clone()).unwrap();
    second.sender.install_snapshot_guard(cache.clone()).unwrap();

    first
        .sender
        .try_enqueue(quantity_limited_entry("quantity-first", 60.0, 100.0))
        .unwrap();
    assert!(matches!(
        next(&mut first).await.outcome,
        BrokerCommandOutcome::Acknowledged
    ));
    second
        .sender
        .try_enqueue(quantity_limited_entry("quantity-second", 50.0, 100.0))
        .unwrap();
    let refused = next(&mut second).await;
    assert!(
        matches!(refused.outcome, BrokerCommandOutcome::Declined { ref reason } if reason.contains("aggregate symbol quantity"))
    );
    assert!(refused.dispatched_at_us.is_none());

    cache.seed_positions_for_test(vec![]);
    cache.seed_orders_for_test(vec![]);
    second
        .sender
        .try_enqueue(quantity_limited_entry("quantity-fresh", 100.0, 100.0))
        .unwrap();
    assert!(matches!(
        next(&mut second).await.outcome,
        BrokerCommandOutcome::Acknowledged
    ));
    assert_eq!(probe.calls.lock().len(), 2);
    drop(finish(second).await);
    drop(finish(first).await);
}

#[tokio::test]
async fn account_is_revalidated_after_slow_previous_mutation() {
    let fixture = Fixture::new();
    let probe = Arc::new(Probe::new(true));
    let cache = account_cache();
    let mut worker = BrokerCommandWorker::start_executor(
        probe.clone(),
        fixture.journal().await,
        Default::default(),
    )
    .unwrap();
    worker.sender.install_snapshot_guard(cache.clone()).unwrap();
    worker.sender.try_enqueue(entry("first")).unwrap();
    probe.started.notified().await;
    worker.sender.try_enqueue(entry("second")).unwrap();
    cache.invalidate();
    probe.release.add_permits(1);
    assert_eq!(next(&mut worker).await.receipt.insight_id, "first");
    // A journal barrier proves the worker got a scheduling opportunity; it
    // must not use the account observation from the earlier successful entry.
    let _ = worker
        .sender
        .try_recovery_page(0, 32)
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(probe.calls.lock().len(), 1);
    refresh_account(&cache, "account");
    assert_eq!(next(&mut worker).await.receipt.insight_id, "second");
    assert_eq!(probe.calls.lock().len(), 2);
    drop(finish(worker).await);
}

// Invalidates between mark_dispatched's fsync and the first future poll. Future
// construction has no broker side effect; only polling increments this probe.
struct InvalidateBeforePoll {
    cache: Arc<BrokerSnapshotCache>,
    invalidated: AtomicBool,
    polls: AtomicUsize,
}

struct OfflineProbe {
    connected: AtomicBool,
    polls: AtomicUsize,
}
impl CommandExecutor for OfflineProbe {
    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }
    async fn execute(&self, _: BrokerCommandAction) -> BrokerCommandOutcome {
        self.polls.fetch_add(1, Ordering::SeqCst);
        BrokerCommandOutcome::Acknowledged
    }
}

#[tokio::test]
async fn fresh_account_does_not_admit_mutation_while_execution_transport_is_disconnected() {
    let fixture = Fixture::new();
    let cache = account_cache();
    let probe = Arc::new(OfflineProbe {
        connected: AtomicBool::new(false),
        polls: AtomicUsize::new(0),
    });
    let mut worker = BrokerCommandWorker::start_executor(
        probe.clone(),
        fixture.journal().await,
        Default::default(),
    )
    .unwrap();
    worker.sender.install_snapshot_guard(cache.clone()).unwrap();
    worker.sender.try_enqueue(entry("offline-entry")).unwrap();
    wait_queued(&worker, 1).await;
    assert_eq!(probe.polls.load(Ordering::SeqCst), 0);
    assert!(worker.events.is_empty());
    probe.connected.store(true, Ordering::SeqCst);
    refresh_account(&cache, "account");
    assert!(matches!(
        next(&mut worker).await.outcome,
        BrokerCommandOutcome::Acknowledged
    ));
    assert_eq!(probe.polls.load(Ordering::SeqCst), 1);
    drop(finish(worker).await);
}
impl CommandExecutor for InvalidateBeforePoll {
    fn execute(&self, _: BrokerCommandAction) -> impl Future<Output = BrokerCommandOutcome> + Send {
        if !self.invalidated.swap(true, Ordering::SeqCst) {
            self.cache.invalidate();
        }
        async move {
            self.polls.fetch_add(1, Ordering::SeqCst);
            BrokerCommandOutcome::Acknowledged
        }
    }
}

#[tokio::test]
async fn invalidation_during_dispatch_fsync_defers_unpolled_intent_and_survives_restart() {
    let fixture = Fixture::new();
    let cache = account_cache();
    let probe = Arc::new(InvalidateBeforePoll {
        cache: cache.clone(),
        invalidated: AtomicBool::new(false),
        polls: AtomicUsize::new(0),
    });
    let worker = BrokerCommandWorker::start_executor(
        probe.clone(),
        fixture.journal().await,
        Default::default(),
    )
    .unwrap();
    worker.sender.install_snapshot_guard(cache.clone()).unwrap();
    let command = entry("fsync-race");
    worker.sender.try_enqueue(command.clone()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !probe.invalidated.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    wait_queued(&worker, 1).await;
    assert_eq!(probe.polls.load(Ordering::SeqCst), 0);
    let journal = finish(worker).await;
    let before = journal.get(&command.id).await.unwrap().unwrap();
    assert_eq!(before.state, JournalCommandState::Queued);
    assert_eq!(before.dispatched_at_us, None);
    assert!(before.completion.is_none());
    drop(journal);

    refresh_account(&cache, "account");
    let mut resumed = BrokerCommandWorker::start_executor(
        probe.clone(),
        fixture.journal().await,
        Default::default(),
    )
    .unwrap();
    resumed.sender.install_snapshot_guard(cache).unwrap();
    resumed.sender.try_enqueue(command.clone()).unwrap();
    let result = next(&mut resumed).await;
    assert!(matches!(result.outcome, BrokerCommandOutcome::Acknowledged));
    assert_eq!(result.receipt, command.receipt());
    assert_eq!(result.queued_at_us, before.queued_at_us);
    assert_eq!(probe.polls.load(Ordering::SeqCst), 1);
    drop(finish(resumed).await);
}

#[tokio::test]
async fn analytics_page_admission_is_bounded_across_sender_clones_and_read_only() {
    let fixture = Fixture::new();
    let probe = Arc::new(Probe::new(false));
    let worker = BrokerCommandWorker::start_executor(
        probe.clone(), fixture.journal().await, Default::default(),
    ).unwrap();
    let sender = worker.sender.clone();
    let permit = worker.sender.evidence_capacity.clone().try_acquire_owned().unwrap();
    assert!(matches!(sender.try_execution_evidence_page(1, 0, None, 128), Err(BrokerCommandError::QueueFull)));
    assert!(matches!(sender.try_execution_evidence_feed_page(0, None, 128), Err(BrokerCommandError::QueueFull)));
    assert!(matches!(sender.try_execution_fill_page(0, None, 128), Err(BrokerCommandError::QueueFull)));
    assert!(matches!(sender.try_prepare_execution_evidence_export(), Err(BrokerCommandError::QueueFull)));
    drop(permit);
    let page = sender.try_execution_evidence_page(1, 0, None, 128).unwrap().wait().await.unwrap();
    assert!(page.records.is_empty());
    assert_eq!(page.through_sequence, 0);
    let feed = sender.try_execution_evidence_feed_page(0, None, 128).unwrap().wait().await.unwrap();
    assert!(feed.observations.is_empty());
    assert_eq!(feed.source_id, page.source_id);
    let fills=sender.try_execution_fill_page(0,None,128).unwrap().wait().await.unwrap();
    assert!(fills.records.is_empty());assert_eq!(fills.source_id,page.source_id);
    assert!(sender.try_prepare_execution_evidence_export().unwrap().wait().await.unwrap().is_none());
    assert!(probe.calls.lock().is_empty(), "analytics must not invoke a broker mutation");
    drop(sender);
    drop(finish(worker).await);
}

#[tokio::test]
async fn checkpoint_and_recovery_remain_available_during_a_slow_broker_call() {
    let fixture = Fixture::new();
    let probe = Arc::new(Probe::new(true));
    let journal = BrokerCommandJournal::open_with_state_history(
        fixture.0.join("commands.db"), "strategy", "account", true,
    ).await.unwrap();
    let mut worker = BrokerCommandWorker::start_executor(
        probe.clone(),
        journal,
        Default::default(),
    )
    .unwrap();
    worker.sender.try_enqueue(entry("slow-broker")).unwrap();
    probe.started.notified().await;
    let mut insight = crate::core::insight::Insight::default();
    insight.symbol = "ETHUSD".into();
    let checkpoint =
        crate::core::insight::execution_checkpoint::InsightExecutionCheckpoint::capture(
            &insight, 1, true, None,
        );
    let batch = ExecutionCheckpointBatch::new(vec![checkpoint], vec![]).unwrap();
    let reply = worker.sender.try_persist_execution(batch).unwrap();
    tokio::time::timeout(Duration::from_secs(1), reply.wait())
        .await
        .unwrap()
        .unwrap();
    let page = tokio::time::timeout(
        Duration::from_secs(1),
        worker
            .sender
            .try_execution_checkpoint_page(String::new(), 32)
            .unwrap()
            .wait(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(page.rows.len(), 1);
    let evidence = tokio::time::timeout(
        Duration::from_secs(1),
        worker.sender.try_execution_evidence_page(1, 0, None, 128).unwrap().wait(),
    ).await.unwrap().unwrap();
    assert_eq!(evidence.records.len(), 1);
    assert_eq!(evidence.records[0].state.insight_id, insight.insight_id.to_string());
    assert!(evidence.records[0].state.entry_price.is_none());
    assert_eq!(probe.active.load(Ordering::SeqCst), 1);
    assert_eq!(probe.calls.lock().len(), 1);
    let recovery = worker
        .sender
        .try_recovery_page(0, 32)
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(recovery[0].state, JournalCommandState::Dispatching);
    probe.release.add_permits(1);
    assert!(matches!(
        next(&mut worker).await.outcome,
        BrokerCommandOutcome::Acknowledged
    ));
    drop(finish(worker).await);
}

#[tokio::test]
async fn fill_journal_waits_for_bounded_admission_without_broker_mutations() {
    let fixture=Fixture::new();let probe=Arc::new(Probe::new(false));
    let worker=BrokerCommandWorker::start_executor(probe.clone(),fixture.journal().await,Default::default()).unwrap();
    let sender=worker.sender.clone();
    let permit=sender.evidence_capacity.clone().try_acquire_owned().unwrap();
    let fill=serde_json::from_value(serde_json::json!({"insight_id":null,"fill":{
        "version":1,"provider":"generic-fill-v1","fill_id":"fill","order_id":"order","position_id":null,
        "symbol":"CUSTOM","side":"sell","effect":"entry","quantity":2.,"price":10.,
        "occurred_at_us":100,"as_of_us":200,"cash":null,"account_notional":null
    }})).unwrap();
    let writer=tokio::spawn(async move {sender.record_execution_fills(ExecutionFillBatch::new(vec![fill]).unwrap()).await});
    tokio::task::yield_now().await;assert!(!writer.is_finished());
    drop(permit);
    tokio::time::timeout(Duration::from_secs(2),writer).await.unwrap().unwrap().unwrap();
    let page=worker.sender.try_execution_fill_page(0,None,128).unwrap().wait().await.unwrap();
    assert_eq!(page.records.len(),1);assert!(page.records[0].observation.insight_id.is_none());
    assert!(probe.calls.lock().is_empty());
    let journal=finish(worker).await;
    assert_eq!(journal.execution_fill_page(0,None,128).await.unwrap().records,page.records);
}

impl Probe {
    fn new(block_first: bool) -> Self {
        Self {
            calls: Default::default(),
            first: AtomicBool::new(block_first),
            started: Default::default(),
            release: tokio::sync::Semaphore::new(0),
            active: AtomicUsize::new(0),
            maximum_active: AtomicUsize::new(0),
        }
    }
}

#[tokio::test]
async fn shared_account_wait_preserves_journal_stop_and_other_account_progress() {
    let first_fixture = Fixture::new();
    let second_fixture = Fixture::new();
    let third_fixture = Fixture::new();
    let lane =
        account_lane::AccountLane::shared("fixture", &uuid::Uuid::new_v4().to_string()).unwrap();
    let other_lane =
        account_lane::AccountLane::shared("fixture", &uuid::Uuid::new_v4().to_string()).unwrap();
    let probe = Arc::new(Probe::new(true));
    let mut first = BrokerCommandWorker::start_executor_with_lane(
        probe.clone(),
        first_fixture.journal().await,
        Default::default(),
        Some(lane.clone()),
        None,
    )
    .unwrap();
    let mut second = BrokerCommandWorker::start_executor_with_lane(
        probe.clone(),
        BrokerCommandJournal::open(
            second_fixture.0.join("commands.db"),
            "other-strategy",
            "account",
        )
        .await
        .unwrap(),
        Default::default(),
        Some(lane),
        None,
    )
    .unwrap();
    let other_probe = Arc::new(Probe::new(false));
    let mut third = BrokerCommandWorker::start_executor_with_lane(
        other_probe.clone(),
        third_fixture.journal().await,
        Default::default(),
        Some(other_lane),
        None,
    )
    .unwrap();
    first.sender.try_enqueue(entry("first")).unwrap();
    probe.started.notified().await;
    let waiting = BrokerCommand::new(
        "other-strategy",
        "account",
        "waiting",
        entry("waiting").action,
        None,
    );
    second.sender.try_enqueue(waiting).unwrap();
    wait_queued(&second, 1).await;
    assert_eq!(probe.calls.lock().as_slice(), &["submit:first"]);

    // A blocked account cannot block another account or journal requests.
    third.sender.try_enqueue(entry("independent")).unwrap();
    assert!(matches!(
        next(&mut third).await.outcome,
        BrokerCommandOutcome::Acknowledged
    ));
    let queued = second
        .sender
        .try_recovery_page(0, 32)
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(queued[0].state, JournalCommandState::Queued);
    assert!(queued[0].dispatched_at_us.is_none());

    second.sender.request_execution_stop();
    wait_stop_persisted(&second).await;
    assert!(matches!(
        next(&mut second).await.outcome,
        BrokerCommandOutcome::CancelledBeforeDispatch
    ));
    // Cleanup still waits for the current account mutation to finish.
    let mut cleanup = cancel("cleanup");
    cleanup.strategy_id = "other-strategy".into();
    second.sender.try_enqueue(cleanup).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let rows = second
                .sender
                .try_recovery_page(0, 32)
                .unwrap()
                .wait()
                .await
                .unwrap();
            if rows.iter().any(|row| {
                row.command.insight_id == "cleanup" && row.state == JournalCommandState::Queued
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(probe.calls.lock().len(), 1);
    probe.release.add_permits(1);
    assert!(matches!(
        next(&mut first).await.outcome,
        BrokerCommandOutcome::Acknowledged
    ));
    assert!(matches!(
        next(&mut second).await.outcome,
        BrokerCommandOutcome::Acknowledged
    ));
    assert_eq!(probe.maximum_active.load(Ordering::SeqCst), 1);
    assert_eq!(
        probe.calls.lock().as_slice(),
        &["submit:first", "cancel:cleanup"]
    );
    drop(finish(first).await);
    drop(finish(second).await);
    drop(finish(third).await);
}

#[tokio::test]
async fn cleanup_preempts_same_workers_waiting_entry_without_losing_it() {
    let fixture = Fixture::new();
    let lane =
        account_lane::AccountLane::shared("fixture", &uuid::Uuid::new_v4().to_string()).unwrap();
    let held = lane.clone().acquire(false).await.unwrap();
    let probe = Arc::new(Probe::new(false));
    let mut worker = BrokerCommandWorker::start_executor_with_lane(
        probe.clone(),
        fixture.journal().await,
        Default::default(),
        Some(lane),
        None,
    )
    .unwrap();
    worker.sender.try_enqueue(entry("entry")).unwrap();
    wait_queued(&worker, 1).await;
    worker.sender.try_enqueue(cancel("cleanup")).unwrap();
    wait_queued(&worker, 2).await;
    drop(held);
    let first = next(&mut worker).await;
    let second = next(&mut worker).await;
    assert!(matches!(first.outcome, BrokerCommandOutcome::Acknowledged));
    assert!(matches!(second.outcome, BrokerCommandOutcome::Acknowledged));
    assert_eq!(
        probe.calls.lock().as_slice(),
        &["cancel:cleanup", "submit:entry"]
    );
    drop(finish(worker).await);
}

#[tokio::test]
async fn another_strategys_cleanup_preempts_queued_entries_on_shared_account() {
    let first_fixture = Fixture::new();
    let second_fixture = Fixture::new();
    let lane =
        account_lane::AccountLane::shared("fixture", &uuid::Uuid::new_v4().to_string()).unwrap();
    let held = lane.clone().acquire(false).await.unwrap();
    let probe = Arc::new(Probe::new(false));
    let mut first = BrokerCommandWorker::start_executor_with_lane(
        probe.clone(),
        first_fixture.journal().await,
        Default::default(),
        Some(lane.clone()),
        None,
    )
    .unwrap();
    let mut second = BrokerCommandWorker::start_executor_with_lane(
        probe.clone(),
        BrokerCommandJournal::open(
            second_fixture.0.join("commands.db"),
            "other-strategy",
            "account",
        )
        .await
        .unwrap(),
        Default::default(),
        Some(lane.clone()),
        None,
    )
    .unwrap();
    first.sender.try_enqueue(entry("waiting-entry")).unwrap();
    wait_queued(&first, 1).await;
    let mut cleanup = cancel("other-cleanup");
    cleanup.strategy_id = "other-strategy".into();
    second.sender.try_enqueue(cleanup).unwrap();
    wait_queued(&second, 1).await;
    // Journal readiness alone does not prove the futures joined admission.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if lane.waiter_counts() == [1, 1] {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(held);
    assert!(matches!(
        next(&mut second).await.outcome,
        BrokerCommandOutcome::Acknowledged
    ));
    assert!(matches!(
        next(&mut first).await.outcome,
        BrokerCommandOutcome::Acknowledged
    ));
    assert_eq!(
        probe.calls.lock().as_slice(),
        &["cancel:other-cleanup", "submit:waiting-entry"]
    );
    assert_eq!(probe.maximum_active.load(Ordering::SeqCst), 1);
    drop(finish(first).await);
    drop(finish(second).await);
    assert_eq!(lane.waiter_counts(), [0, 0]);
}

#[tokio::test]
async fn cleanup_preempts_waiting_tp_update_without_losing_a_parked_entry() {
    let fixture = Fixture::new();
    let lane =
        account_lane::AccountLane::shared("fixture", &uuid::Uuid::new_v4().to_string()).unwrap();
    let held = lane.clone().acquire(false).await.unwrap();
    let probe = Arc::new(Probe::new(false));
    let cache = account_cache();
    let mut worker = BrokerCommandWorker::start_executor_with_lane(
        probe.clone(),
        fixture.journal().await,
        Default::default(),
        Some(lane.clone()),
        None,
    )
    .unwrap();
    worker.sender.install_snapshot_guard(cache.clone()).unwrap();
    cache.invalidate();
    let update = BrokerCommand::new(
        "strategy",
        "account",
        "tp",
        BrokerCommandAction::TakeProfit {
            order_id: "existing".into(),
            price: 110.0,
            quantity: 1.0,
        },
        None,
    );
    worker.sender.try_enqueue(update).unwrap();
    wait_queued(&worker, 1).await;
    worker.sender.try_enqueue(entry("after-update")).unwrap();
    refresh_account(&cache, "account");
    tokio::time::timeout(Duration::from_secs(2), async {
        while lane.waiter_counts() != [0, 1] {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    worker.sender.try_enqueue(cancel("cleanup")).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while lane.waiter_counts() != [1, 0] {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(held);
    for insight in ["cleanup", "tp", "after-update"] {
        let event = next(&mut worker).await;
        assert_eq!(event.receipt.insight_id, insight);
        assert!(matches!(event.outcome, BrokerCommandOutcome::Acknowledged));
    }
    assert_eq!(
        probe.calls.lock().as_slice(),
        &["cancel:cleanup", "mutation", "submit:after-update"]
    );
    assert_eq!(probe.maximum_active.load(Ordering::SeqCst), 1);
    drop(finish(worker).await);
}

impl CommandExecutor for Probe {
    async fn execute(&self, action: BrokerCommandAction) -> BrokerCommandOutcome {
        let label = match action {
            BrokerCommandAction::Submit { request } => {
                format!("submit:{}", request.client_order_id)
            }
            BrokerCommandAction::Cancel { order_id } => format!("cancel:{order_id}"),
            _ => "mutation".into(),
        };
        self.calls.lock().push(label);
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum_active.fetch_max(active, Ordering::SeqCst);
        struct Active<'a>(&'a AtomicUsize);
        impl Drop for Active<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        let _active = Active(&self.active);
        if self.first.swap(false, Ordering::SeqCst) {
            self.started.notify_one();
            self.release.acquire().await.unwrap().forget();
        }
        BrokerCommandOutcome::Acknowledged
    }
}

async fn next(worker: &mut BrokerCommandWorker) -> BrokerCommandCompletion {
    match tokio::time::timeout(Duration::from_secs(10), worker.events.recv())
        .await
        .unwrap()
        .unwrap()
    {
        BrokerCommandWorkerEvent::Completion(value) => value,
        other => panic!("unexpected worker event: {other:?}"),
    }
}

async fn finish(worker: BrokerCommandWorker) -> BrokerCommandJournal {
    let BrokerCommandWorker {
        sender,
        events,
        task,
        ..
    } = worker;
    drop(sender);
    drop(events);
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

async fn wait_stop_persisted(worker: &BrokerCommandWorker) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let wake = worker.ready.notified();
            assert!(worker.sender.execution_stop_error().is_none());
            if !worker.sender.execution_stop_pending() {
                break;
            }
            wake.await;
        }
    })
    .await
    .expect("stop persistence must not wait for broker responses or completion capacity");
}

#[tokio::test]
async fn durable_stop_is_saved_during_an_inflight_call_without_aborting_it() {
    use crate::core::broker::command_journal::lifecycle::ExecutionPhase;
    let fixture = Fixture::new();
    let probe = Arc::new(Probe::new(true));
    let mut worker = BrokerCommandWorker::start_executor(
        probe.clone(),
        fixture.journal().await,
        Default::default(),
    )
    .unwrap();
    worker.sender.try_enqueue(entry("inflight-stop")).unwrap();
    probe.started.notified().await;
    for _ in 0..1_000 {
        worker.sender.request_execution_stop();
    }
    wait_stop_persisted(&worker).await;
    assert_eq!(probe.active.load(Ordering::SeqCst), 1);
    assert!(matches!(
        worker.sender.try_enqueue(entry("after-stop")),
        Err(BrokerCommandError::Stopping)
    ));
    probe.release.add_permits(1);
    assert!(matches!(
        next(&mut worker).await.outcome,
        BrokerCommandOutcome::Acknowledged
    ));
    let journal = finish(worker).await;
    let lifecycle = journal.execution_lifecycle();
    assert_eq!(lifecycle.phase, ExecutionPhase::Stopping);
    drop(journal);
    let journal = fixture.journal().await;
    assert_eq!(journal.execution_lifecycle(), lifecycle);
    let worker = BrokerCommandWorker::start_executor(probe, journal, Default::default()).unwrap();
    assert!(worker.sender.entries_stopped());
    assert!(matches!(
        worker.sender.try_enqueue(entry("after-restart")),
        Err(BrokerCommandError::Stopping)
    ));
    drop(finish(worker).await);
}

#[tokio::test]
async fn durable_stop_is_saved_behind_a_full_completion_channel() {
    use crate::core::broker::command_journal::lifecycle::ExecutionPhase;
    let fixture = Fixture::new();
    let probe = Arc::new(Probe::new(false));
    let mut worker = BrokerCommandWorker::start_executor(
        probe.clone(),
        fixture.journal().await,
        BrokerCommandWorkerConfig {
            completion_capacity: 1,
            ..Default::default()
        },
    )
    .unwrap();
    worker.sender.try_enqueue(entry("first-full")).unwrap();
    worker.sender.try_enqueue(entry("second-full")).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while probe.calls.lock().len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(worker.events.len(), 1);
    worker.sender.request_execution_stop();
    wait_stop_persisted(&worker).await;
    assert_eq!(worker.events.len(), 1);
    assert!(matches!(
        next(&mut worker).await.outcome,
        BrokerCommandOutcome::Acknowledged
    ));
    assert!(matches!(
        next(&mut worker).await.outcome,
        BrokerCommandOutcome::Acknowledged
    ));
    assert_eq!(
        finish(worker).await.execution_lifecycle().phase,
        ExecutionPhase::Stopping
    );
}

#[tokio::test]
async fn durable_stop_survives_immediate_owner_receiver_loss() {
    use crate::core::broker::command_journal::lifecycle::ExecutionPhase;
    let fixture = Fixture::new();
    let worker = BrokerCommandWorker::start_executor(
        Arc::new(Probe::new(false)),
        fixture.journal().await,
        Default::default(),
    )
    .unwrap();
    worker.sender.request_execution_stop();
    let mut journal = finish(worker).await;
    assert_eq!(
        journal.execution_lifecycle().phase,
        ExecutionPhase::Stopping
    );
    assert!(journal.complete_execution_stop().await.unwrap());
    assert_eq!(journal.begin_execution_epoch().await.unwrap().epoch, 2);
}

#[tokio::test]
async fn durable_stop_failure_is_exposed_without_aborting_an_inflight_mutation() {
    let fixture = Fixture::new();
    let probe = Arc::new(Probe::new(true));
    let mut worker = BrokerCommandWorker::start_executor(
        probe.clone(),
        fixture.journal().await,
        Default::default(),
    )
    .unwrap();
    let command = entry("stop-failure-inflight");
    worker.sender.try_enqueue(command.clone()).unwrap();
    probe.started.notified().await;
    // Simulate a lifecycle fencing conflict through a separate test connection;
    // no production fault-injection hook or broker adapter is involved.
    let database = turso::Builder::new_local(fixture.0.join("commands.db").to_str().unwrap())
        .build()
        .await
        .unwrap();
    let connection = database.connect().unwrap();
    connection
        .execute("UPDATE broker_execution_lifecycle SET version=99", ())
        .await
        .unwrap();
    worker.sender.request_execution_stop();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let wake = worker.ready.notified();
            if worker.sender.execution_stop_error().is_some() {
                break;
            }
            wake.await;
        }
    })
    .await
    .unwrap();
    assert!(
        worker.sender.execution_stop_pending(),
        "a failed stop must never be acknowledged as durable"
    );
    assert!(worker.sender.entries_stopped());
    assert_eq!(probe.active.load(Ordering::SeqCst), 1);
    probe.release.add_permits(1);
    assert!(matches!(
        next(&mut worker).await.outcome,
        BrokerCommandOutcome::Acknowledged
    ));
    let BrokerCommandWorker {
        sender,
        events,
        task,
        ..
    } = worker;
    drop(sender);
    drop(events);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap(),
        Err(BrokerCommandError::Journal(_))
    ));
    // The command result survived the stop error. Restore only the injected
    // test conflict so the normal journal reader can verify its durable row.
    connection
        .execute("UPDATE broker_execution_lifecycle SET version=1", ())
        .await
        .unwrap();
    drop(connection);
    drop(database);
    let journal = fixture.journal().await;
    let rows = journal.recovery_page(0, 32).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].command.id, command.id);
    assert!(matches!(
        rows[0].completion.as_ref().unwrap().outcome,
        BrokerCommandOutcome::Acknowledged
    ));
}

#[test]
fn command_identity_is_stable_scoped_and_roundtrips() {
    let command = entry("first");
    assert_eq!(command.id, entry("first").id);
    assert_ne!(command.id, entry("split").id);
    let changed_account =
        BrokerCommand::new("strategy", "other", "first", command.action.clone(), None);
    assert_ne!(command.id, changed_account.id);
    let trimmed = BrokerCommand::new(
        " strategy ",
        " account ",
        "first",
        command.action.clone(),
        None,
    );
    assert_eq!(command.id, trimmed.id);
    let encoded = command.encode().unwrap();
    assert_eq!(
        serde_json::from_str::<BrokerCommand>(&encoded).unwrap(),
        command
    );
    assert!(encoded.len() < 1024);
    let mut invalid = command.clone();
    let BrokerCommandAction::Submit { request } = &mut invalid.action else {
        unreachable!()
    };
    request.quantity = Some(f64::NAN);
    assert!(invalid.encode().is_err());
    let mut forged = command;
    forged.id = BrokerCommandId(uuid::Uuid::new_v4().to_string());
    assert!(forged.validate().is_err());

    let limited = quantity_limited_entry("limited-roundtrip", 25.0, 100.0);
    let encoded = limited.encode().unwrap();
    assert_eq!(
        serde_json::from_str::<BrokerCommand>(&encoded).unwrap(),
        limited
    );
    let mut invalid_cleanup = cancel("invalid-constraint");
    invalid_cleanup
        .dispatch_constraints
        .max_total_symbol_quantity = Some(100.0);
    assert!(invalid_cleanup.validate().is_err());
}

#[test]
fn request_size_is_independent_of_nested_insights_and_state_history() {
    use crate::core::{
        insight::Insight,
        insight::types::StrategyType,
        utils::timeframe::{TimeFrame, TimeFrameUnit},
    };
    let mut parent = Insight::new(
        OrderSide::Buy,
        "ETHUSD".into(),
        StrategyType::Testing,
        TimeFrame::new(1, TimeFrameUnit::Minute),
        90,
        None,
    );
    parent.set_quantity(Some(10.0));
    parent.set_limit_price(Some(100.0));
    let original = BrokerOrderRequest::from(&parent);
    for _ in 0..1000 {
        let mut child = Insight::new(
            OrderSide::Buy,
            "ETHUSD".into(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            90,
            Some(parent.insight_id),
        );
        child.set_quantity(Some(1.0));
        child.children.push(Insight::default());
        parent.children.push(child);
        parent.state_log(Some("configuration/lifecycle history".repeat(20)));
    }
    assert_eq!(BrokerOrderRequest::from(&parent), original);
    assert!(serde_json::to_vec(&original).unwrap().len() < 1024);
    assert_ne!(
        BrokerOrderRequest::from(&parent.children[0]).client_order_id,
        original.client_order_id
    );
}

#[tokio::test]
async fn journal_is_single_owner_scoped_and_never_resets_dispatched_intents() {
    let fixture = Fixture::new();
    let mut journal = fixture.journal().await;
    assert!(
        BrokerCommandJournal::open(fixture.0.join("commands.db"), "strategy", "account")
            .await
            .is_err()
    );
    let command = entry("first");
    let record = journal.prepare(&command, 11).await.unwrap();
    assert_eq!(record.state, JournalCommandState::Queued);
    journal.mark_dispatched(&command.id, 12).await.unwrap();
    assert!(journal.mark_dispatched(&command.id, 13).await.is_err());
    drop(journal);
    assert!(
        BrokerCommandJournal::open(fixture.0.join("commands.db"), "strategy", "another-account")
            .await
            .is_err()
    );
    let mut journal = fixture.journal().await;
    let replay = journal.prepare(&command, 999).await.unwrap();
    assert_eq!(replay.state, JournalCommandState::Dispatching);
    assert_eq!(replay.queued_at_us, 11);
    assert_eq!(replay.dispatched_at_us, Some(12));
    let mut conflict = command.clone();
    if let BrokerCommandAction::Submit { request } = &mut conflict.action {
        request.quantity = Some(2.0);
    }
    assert_eq!(
        journal.prepare(&conflict, 15).await.unwrap_err(),
        BrokerCommandError::IdentityConflict
    );
    assert_eq!(
        journal.get(&command.id).await.unwrap().unwrap().command,
        command
    );
}

#[tokio::test]
async fn recovered_ambiguous_submit_is_not_sent_again() {
    let fixture = Fixture::new();
    let mut journal = fixture.journal().await;
    let command = entry("crashed");
    journal.prepare(&command, 10).await.unwrap();
    journal.mark_dispatched(&command.id, 11).await.unwrap();
    drop(journal);
    let probe = Arc::new(Probe::new(false));
    let mut worker = BrokerCommandWorker::start_executor(
        probe.clone(),
        fixture.journal().await,
        Default::default(),
    )
    .unwrap();
    // Expiry is not proof of non-execution for a journaled dispatched intent.
    worker.sender.set_entry_deadline(
        command.id.clone(),
        Some(chrono::Utc::now() - chrono::TimeDelta::seconds(1)),
    );
    worker.sender.try_enqueue(command.clone()).unwrap();
    let completion = next(&mut worker).await;
    assert!(matches!(
        completion.outcome,
        BrokerCommandOutcome::Ambiguous { .. }
    ));
    assert_eq!(completion.dispatched_at_us, Some(11));
    worker.sender.try_enqueue(command).unwrap();
    let repeated = next(&mut worker).await;
    assert_eq!(
        serde_json::to_value(&completion).unwrap(),
        serde_json::to_value(&repeated).unwrap()
    );
    assert!(probe.calls.lock().is_empty());
    drop(finish(worker).await);
}

#[tokio::test]
async fn recovered_queued_intent_can_be_admitted_once_and_conflicts_do_not_stop_other_work() {
    let fixture = Fixture::new();
    let mut journal = fixture.journal().await;
    let command = entry("queued-before-crash");
    journal.prepare(&command, 10).await.unwrap();
    drop(journal);
    let probe = Arc::new(Probe::new(false));
    let mut worker = BrokerCommandWorker::start_executor(
        probe.clone(),
        fixture.journal().await,
        Default::default(),
    )
    .unwrap();
    worker.sender.try_enqueue(command.clone()).unwrap();
    let completion = next(&mut worker).await;
    assert_eq!(completion.queued_at_us, 10);
    assert!(matches!(
        completion.outcome,
        BrokerCommandOutcome::Acknowledged
    ));
    let mut conflict = command;
    if let BrokerCommandAction::Submit { request } = &mut conflict.action {
        request.quantity = Some(2.0);
    }
    worker.sender.try_enqueue(conflict).unwrap();
    worker.sender.try_enqueue(entry("healthy")).unwrap();
    let event = tokio::time::timeout(Duration::from_secs(5), worker.events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        event,
        BrokerCommandWorkerEvent::Failed {
            error: BrokerCommandError::IdentityConflict,
            ..
        }
    ));
    assert_eq!(next(&mut worker).await.receipt.insight_id, "healthy");
    assert_eq!(
        *probe.calls.lock(),
        vec!["submit:queued-before-crash", "submit:healthy"]
    );
    drop(finish(worker).await);
}

#[tokio::test]
async fn owner_loss_preserves_inflight_completion_without_dispatching_queued_entries() {
    let fixture = Fixture::new();
    let probe = Arc::new(Probe::new(true));
    let worker = BrokerCommandWorker::start_executor(
        probe.clone(),
        fixture.journal().await,
        Default::default(),
    )
    .unwrap();
    let command = entry("in-flight");
    worker.sender.try_enqueue(command.clone()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), probe.started.notified())
        .await
        .unwrap();
    worker.sender.try_enqueue(entry("unsent")).unwrap();
    let BrokerCommandWorker {
        sender,
        events,
        task,
        ..
    } = worker;
    drop(events);
    probe.release.add_permits(1);
    let journal = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(sender.is_closed());
    let recovered = journal.get(&command.id).await.unwrap().unwrap();
    assert_eq!(recovered.state, JournalCommandState::Completed);
    assert!(recovered.completion.is_some());
    assert_eq!(*probe.calls.lock(), vec!["submit:in-flight"]);
}

#[tokio::test]
async fn cancelling_one_pending_entry_preserves_other_entries_and_inflight_evidence() {
    let fixture = Fixture::new();
    let probe = Arc::new(Probe::new(true));
    let mut worker = BrokerCommandWorker::start_executor(
        probe.clone(),
        fixture.journal().await,
        Default::default(),
    )
    .unwrap();
    let inflight = entry("already-dispatched");
    worker.sender.try_enqueue(inflight.clone()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), probe.started.notified())
        .await
        .unwrap();
    let pending = entry("cancel-only-this");
    worker.sender.try_enqueue(pending.clone()).unwrap();
    worker.sender.try_enqueue(entry("healthy-next")).unwrap();
    worker.sender.cancel_pending_entry(pending.id.clone());
    worker.sender.cancel_pending_entry(inflight.id.clone());
    probe.release.add_permits(2);
    let first = next(&mut worker).await;
    assert!(matches!(first.outcome, BrokerCommandOutcome::Acknowledged));
    assert!(
        first.dispatched_at_us.is_some(),
        "already-started execution cannot be relabelled unsent"
    );
    let cancelled = next(&mut worker).await;
    assert_eq!(cancelled.receipt.id, pending.id);
    assert!(matches!(
        cancelled.outcome,
        BrokerCommandOutcome::CancelledBeforeDispatch
    ));
    assert!(cancelled.dispatched_at_us.is_none());
    assert_eq!(next(&mut worker).await.receipt.insight_id, "healthy-next");
    assert_eq!(
        *probe.calls.lock(),
        vec!["submit:already-dispatched", "submit:healthy-next"]
    );
    drop(finish(worker).await);
}

#[tokio::test]
async fn completion_notification_is_retained_until_owner_registers_its_wait() {
    let fixture = Fixture::new();
    let probe = Arc::new(Probe::new(false));
    let mut worker =
        BrokerCommandWorker::start_executor(probe, fixture.journal().await, Default::default())
            .unwrap();
    worker.sender.try_enqueue(entry("late-waiter")).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while worker.events.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Notification happened before this future existed: a Notify permit must
    // retain readiness without copying an event into a second queue.
    tokio::time::timeout(Duration::from_millis(100), worker.ready.notified())
        .await
        .unwrap();
    assert_eq!(next(&mut worker).await.receipt.insight_id, "late-waiter");
    drop(finish(worker).await);
}

#[tokio::test]
async fn paper_command_completion_replays_without_duplicate_order_or_event() {
    let insight_id = uuid::Uuid::new_v4().to_string();
    let fixture = Fixture::new();
    let broker = Arc::new(PaperBroker::new(AccountType::Paper, 100_000.0, 1));
    let mut worker =
        BrokerCommandWorker::start(broker.clone(), fixture.journal().await, Default::default())
            .unwrap();
    let command = entry(&insight_id);
    worker.sender.try_enqueue(command.clone()).unwrap();
    let completion = next(&mut worker).await;
    assert!(matches!(
        completion.outcome,
        BrokerCommandOutcome::OrderResponse { .. }
    ));
    worker.sender.try_enqueue(command).unwrap();
    let duplicate = next(&mut worker).await;
    assert_eq!(
        serde_json::to_value(&completion).unwrap(),
        serde_json::to_value(&duplicate).unwrap()
    );
    assert_eq!(broker.get_orders().await.unwrap().len(), 1);
    assert_eq!(broker.drain_trade_events().len(), 1);
    assert!(broker.submit_order(request(&insight_id)).await.is_err());
    assert_eq!(broker.get_orders().await.unwrap().len(), 1);
    let mut journal = finish(worker).await;
    journal
        .persist_execution_batch(&checkpoint_batch(&completion.receipt))
        .await
        .unwrap();
    assert!(journal.recovery_page(0, 50).await.unwrap().is_empty());
    // Even an applied command retains its identity: replay cannot create a new order.
    assert_eq!(
        journal
            .get(&completion.receipt.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        JournalCommandState::Applied
    );
}

#[tokio::test]
async fn stop_during_thousand_command_burst_disables_dispatch_and_prioritizes_cleanup() {
    let fixture = Fixture::new();
    let probe = Arc::new(Probe::new(true));
    let mut worker = BrokerCommandWorker::start_executor(
        probe.clone(),
        fixture.journal().await,
        Default::default(),
    )
    .unwrap();
    worker.sender.try_enqueue(entry("in-flight")).unwrap();
    tokio::time::timeout(Duration::from_secs(5), probe.started.notified())
        .await
        .unwrap();
    for index in 0..1000 {
        worker
            .sender
            .try_enqueue(entry(&format!("queued-{index}")))
            .unwrap();
    }
    let started = std::time::Instant::now();
    worker.sender.stop_entries();
    assert!(started.elapsed() < Duration::from_millis(100));
    assert_eq!(
        worker.sender.try_enqueue(entry("too-late")).unwrap_err(),
        BrokerCommandError::Stopping
    );
    worker.sender.try_enqueue(cancel("late-fill")).unwrap();
    probe.release.add_permits(1);
    assert!(matches!(
        next(&mut worker).await.outcome,
        BrokerCommandOutcome::Acknowledged
    ));
    assert_eq!(next(&mut worker).await.receipt.insight_id, "late-fill");
    for _ in 0..1000 {
        let completion = next(&mut worker).await;
        assert!(matches!(
            completion.outcome,
            BrokerCommandOutcome::CancelledBeforeDispatch
        ));
        assert!(completion.dispatched_at_us.is_none());
    }
    assert_eq!(
        *probe.calls.lock(),
        vec!["submit:in-flight", "cancel:late-fill"]
    );
    assert_eq!(probe.maximum_active.load(Ordering::SeqCst), 1);
    drop(finish(worker).await);
}

#[tokio::test]
async fn queue_saturation_does_not_consume_teardown_capacity() {
    let fixture = Fixture::new();
    let probe = Arc::new(Probe::new(true));
    let config = BrokerCommandWorkerConfig {
        entry_capacity: 2,
        control_capacity: 1,
        ..Default::default()
    };
    let mut worker =
        BrokerCommandWorker::start_executor(probe.clone(), fixture.journal().await, config)
            .unwrap();
    worker.sender.try_enqueue(entry("first")).unwrap();
    tokio::time::timeout(Duration::from_secs(5), probe.started.notified())
        .await
        .unwrap();
    worker.sender.try_enqueue(entry("second")).unwrap();
    worker.sender.try_enqueue(entry("third")).unwrap();
    assert_eq!(
        worker.sender.try_enqueue(entry("full")).unwrap_err(),
        BrokerCommandError::QueueFull
    );
    worker.sender.try_enqueue(cancel("cleanup")).unwrap();
    assert_eq!(
        worker
            .sender
            .try_enqueue(cancel("control-full"))
            .unwrap_err(),
        BrokerCommandError::QueueFull
    );
    probe.release.add_permits(1);
    assert_eq!(next(&mut worker).await.receipt.insight_id, "first");
    assert_eq!(next(&mut worker).await.receipt.insight_id, "cleanup");
    assert_eq!(next(&mut worker).await.receipt.insight_id, "second");
    assert_eq!(next(&mut worker).await.receipt.insight_id, "third");
    assert_eq!(probe.maximum_active.load(Ordering::SeqCst), 1);
    drop(finish(worker).await);
}

#[tokio::test]
async fn timeout_is_ambiguous_and_group_version_survives_replay() {
    let fixture = Fixture::new();
    let probe = Arc::new(Probe::new(true));
    let config = BrokerCommandWorkerConfig {
        mutation_timeout: Duration::from_millis(20),
        ..Default::default()
    };
    let mut worker =
        BrokerCommandWorker::start_executor(probe.clone(), fixture.journal().await, config)
            .unwrap();
    let mut command = entry(&uuid::Uuid::new_v4().to_string());
    command.group = Some(BrokerOperationGroup {
        id: "stack".into(),
        version: 17,
    });
    worker.sender.try_enqueue(command.clone()).unwrap();
    let completion = next(&mut worker).await;
    assert!(matches!(
        completion.outcome,
        BrokerCommandOutcome::Ambiguous { .. }
    ));
    assert_eq!(completion.receipt.group.as_ref().unwrap().version, 17);
    worker.sender.try_enqueue(command).unwrap();
    assert!(matches!(
        next(&mut worker).await.outcome,
        BrokerCommandOutcome::Ambiguous { .. }
    ));
    assert_eq!(probe.calls.lock().len(), 1);
    let mut journal = finish(worker).await;
    assert!(
        journal
            .persist_execution_batch(&checkpoint_batch(&completion.receipt))
            .await
            .is_err()
    );
    assert_eq!(journal.recovery_page(0, 50).await.unwrap().len(), 1);
}

#[tokio::test]
async fn recovery_pages_are_bounded_deterministic_and_keep_unapplied_results() {
    let fixture = Fixture::new();
    let mut journal = fixture.journal().await;
    for index in 0..300 {
        journal
            .prepare(&entry(&format!("entry-{index}")), index)
            .await
            .unwrap();
    }
    let first = journal.recovery_page(0, 1000).await.unwrap();
    assert_eq!(first.len(), 256);
    let second = journal
        .recovery_page(first.last().unwrap().sequence, 1000)
        .await
        .unwrap();
    assert_eq!(second.len(), 44);
    assert_eq!(second.last().unwrap().command.insight_id, "entry-299");
    assert!(
        first
            .windows(2)
            .all(|rows| rows[0].sequence < rows[1].sequence)
    );
    assert!(
        journal
            .get(&BrokerCommandId("missing".into()))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn entry_amendments_preserve_pending_paper_order_fields() {
    let broker = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
    broker.submit_order(request("amend-limit")).await.unwrap();
    let before = broker.get_order("amend-limit").await.unwrap();
    broker
        .amend_entry_prices("amend-limit", Some(99.0), None)
        .await
        .unwrap();
    let after = broker.get_order("amend-limit").await.unwrap();
    assert_eq!(after.limit_price, Some(99.0));
    assert_eq!(after.qty, before.qty);
    assert_eq!(
        after.legs.unwrap().take_profit.unwrap().limit_price,
        before.legs.unwrap().take_profit.unwrap().limit_price
    );
    assert!(
        broker
            .amend_entry_prices("amend-limit", None, Some(102.0))
            .await
            .is_err()
    );
    assert!(
        broker
            .amend_entry_prices("amend-limit", Some(f64::NAN), None)
            .await
            .is_err()
    );
    let mut stop = request("amend-stop");
    stop.order_type = OrderType::StopLimit;
    stop.stop_price = Some(102.0);
    broker.submit_order(stop).await.unwrap();
    broker
        .amend_entry_prices("amend-stop", None, Some(103.0))
        .await
        .unwrap();
    let order = broker.get_order("amend-stop").await.unwrap();
    assert_eq!(order.stop_price, Some(103.0));
    assert_eq!(order.limit_price, Some(100.0));
    broker.cancel_order("amend-stop").await.unwrap();
    assert!(
        broker
            .amend_entry_prices("amend-stop", None, Some(104.0))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn take_profit_and_entry_replacement_are_distinct_for_pending_paper_orders() {
    let broker = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
    broker.submit_order(request("paper")).await.unwrap();
    broker
        .update_take_profit("paper", 115.0, 99.0)
        .await
        .unwrap();
    let order = broker.get_order("paper").await.unwrap();
    assert_eq!(order.limit_price, Some(100.0));
    assert_eq!(order.qty, 1.0);
    assert_eq!(
        order.legs.unwrap().take_profit.unwrap().limit_price,
        Some(115.0)
    );
    broker.update_stop_loss("paper", 85.0, 99.0).await.unwrap();
    let order = broker.get_order("paper").await.unwrap();
    assert_eq!(order.limit_price, Some(100.0));
    assert_eq!(order.qty, 1.0);
    assert_eq!(
        order.legs.unwrap().stop_loss.unwrap().limit_price,
        Some(85.0)
    );
    broker.replace_order("paper", 101.0, 2.0).await.unwrap();
    let order = broker.get_order("paper").await.unwrap();
    assert_eq!(order.limit_price, Some(101.0));
    assert_eq!(order.qty, 2.0);
    assert_eq!(
        order.legs.unwrap().take_profit.unwrap().limit_price,
        Some(115.0)
    );
}
