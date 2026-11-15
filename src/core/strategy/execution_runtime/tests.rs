use super::*;
use crate::core::{
    broker::{
        command_worker::BrokerCommandWorkerConfig,
        commands::{BrokerCommandAction, BrokerCommandId, BrokerCommandReceipt},
        data_feeds::yahoo::YahooFinanceDataFeed,
        types::AccountType,
    },
    pipeline::{InsightPipe, InsightPipeResult},
    utils::timeframe::TimeFrameUnit,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct Noop;
impl Strategy for Noop {
    fn on_start(&mut self, _: &mut dyn StrategyContext) {}
    fn init(&mut self, _: &mut dyn StrategyContext, _: &Asset) {}
    fn universe(&self, _: &mut dyn StrategyContext) -> HashSet<String> {
        HashSet::new()
    }
    fn on_bar(&mut self, _: &mut dyn StrategyContext, _: &str, _: &BarData) {}
    fn generate_insights(&mut self, _: &mut dyn StrategyContext, _: &str) {}
    fn insight_pipeline(&mut self, _: &mut dyn StrategyContext, _: &Insight) {}
    fn on_teardown(&mut self, _: &mut dyn StrategyContext) {}
}
type Owner = StrategyState<Noop, PaperBroker, YahooFinanceDataFeed>;

async fn owner() -> (Owner, std::path::PathBuf) {
    owner_with_config(Default::default()).await
}

async fn owner_with_config(config: BrokerCommandWorkerConfig) -> (Owner, std::path::PathBuf) {
    let directory = std::env::temp_dir().join(format!("aqe-continuation-index-{}", Uuid::new_v4()));
    let mut state = StrategyState::new(
        "index-test".into(),
        "1".into(),
        Noop,
        UnifiedBroker::new(
            PaperBroker::new(AccountType::Paper, 100_000.0, 1),
            YahooFinanceDataFeed::new(),
        ),
        StrategyMode::Backtest,
        TimeFrame::new(1, TimeFrameUnit::Minute),
    );
    state.status = StrategyStatus::Running;
    state
        .start_command_driver(directory.join("commands.db"), "strategy", "account", config)
        .await
        .unwrap();
    (state, directory)
}

async fn finish(mut state: Owner, directory: std::path::PathBuf) {
    drop(state.command_driver.take().unwrap().stop().await.unwrap());
    std::fs::remove_dir_all(directory).unwrap();
}

fn defer(state: &mut Owner, n: u128) -> Uuid {
    let id = Uuid::from_u128(n);
    let mut row = Insight::default();
    row.insight_id = id;
    row.symbol = "ETHUSD".into();
    row.period_unfilled = None;
    state.insights.add_insight(row);
    let dependency = InsightPipeDeferral::BrokerCommand(BrokerCommandReceipt {
        id: BrokerCommandId(format!("waiting-{n}")),
        insight_id: id.to_string(),
        group: None,
    });
    state
        .insight_pipeline
        .command_dependencies
        .update(id, Some(&dependency));
    state.insights.track_pipeline_continuation(id, true);
    state.insight_pipeline.deferred.insert(
        id,
        types::DeferredPipeResume {
            state: InsightState::New,
            index: 0,
            dependency,
            snapshot_attempted: None,
        },
    );
    id
}

#[tokio::test]
async fn completion_discovery_bounds_no_ready_and_state_change_work() {
    let (mut state, directory) = owner().await;
    for n in 1..=2048 {
        defer(&mut state, n);
    }
    let (examined, pending) =
        state.execution_continuation_turn(false, usize::MAX, Duration::from_secs(10));
    assert_eq!(examined, 256, "the cap applies when no receipt is ready");
    assert!(
        pending,
        "unfinished discovery must schedule another owner turn"
    );
    assert_eq!(state.insight_pipeline.deferred.len(), 2048);
    for _ in 0..7 {
        state.execution_continuation_turn(false, 256, Duration::from_secs(10));
    }
    assert_eq!(
        state.execution_continuation_turn(false, 256, Duration::from_secs(10)),
        (0, false)
    );
    // Both public mutable APIs must wake a changed state without a new broker revision.
    for n in 1..=2048 {
        if n % 2 == 0 {
            state.insights.get_mut(&Uuid::from_u128(n)).unwrap().state = InsightState::Executed;
        } else {
            state
                .insights
                .get_insight_mut(&Uuid::from_u128(n))
                .unwrap()
                .state = InsightState::Executed;
        }
    }
    assert_eq!(
        state.execution_continuation_turn(false, 256, Duration::ZERO),
        (0, true)
    );
    let (examined, pending) =
        state.execution_continuation_turn(false, 256, Duration::from_secs(10));
    assert_eq!(examined, 256);
    assert!(pending);
    assert_eq!(
        state.insight_pipeline.deferred.len(),
        2048 - 128,
        "state notices and candidates both consume work"
    );
    while state
        .execution_continuation_turn(false, 256, Duration::from_secs(10))
        .1
    {}
    assert!(state.insight_pipeline.deferred.is_empty());
    assert!(
        !state
            .insight_pipeline
            .command_dependencies
            .has_broker_waiters()
    );
    finish(state, directory).await;
}

struct CapacityPipe(Arc<AtomicUsize>);
impl InsightPipe for CapacityPipe {
    fn version(&self) -> &str {
        "1"
    }
    fn run(&mut self, _: &mut dyn StrategyContext, _: &mut Insight) -> InsightPipeResult {
        self.0.fetch_add(1, Ordering::SeqCst);
        InsightPipeResult::deferred(InsightPipeDeferral::BrokerQueueCapacity, self.name())
    }
}

#[tokio::test]
async fn completion_discovery_suppresses_repeated_deferral_but_wakes_changed_state() {
    let (mut state, directory) = owner().await;
    let calls = Arc::new(AtomicUsize::new(0));
    state.add_pipe(
        WrappedInsightPipe::builder(Box::new(CapacityPipe(calls.clone())))
            .target_state(InsightState::New)
            .build(),
    );
    let mut row = Insight::default();
    row.symbol = "ETHUSD".into();
    row.period_unfilled = None;
    let id = state.insights.add_insight(row);
    state.run_insight_pipeline_turn(Some(vec![id]), Some(Duration::from_secs(1)), false);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        state
            .execution_continuation_turn(false, 256, Duration::from_secs(10))
            .1
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        state.execution_continuation_turn(false, 256, Duration::from_secs(10)),
        (0, false)
    );
    state.insights.get_mut(&id).unwrap().state = InsightState::Executed;
    state.execution_continuation_turn(false, 256, Duration::from_secs(10));
    assert!(state.insight_pipeline.deferred.is_empty());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    finish(state, directory).await;
}

#[tokio::test]
async fn completion_discovery_retired_insight_releases_all_waiters() {
    let (mut state, directory) = owner().await;
    let id = defer(&mut state, 1);
    state.execution_continuation_turn(false, 256, Duration::from_secs(10));
    state.insights.remove_insight(&id);
    assert!(
        state
            .execution_continuation_turn(false, 256, Duration::from_secs(10))
            .1
    );
    assert!(state.insight_pipeline.deferred.is_empty());
    assert!(!state.insight_pipeline.command_dependencies.has_work());
    assert!(
        !state
            .insight_pipeline
            .command_dependencies
            .has_broker_waiters()
    );
    assert!(!state.insights.has_pipeline_state_changes());
    finish(state, directory).await;
}

#[test]
fn completion_discovery_does_not_retain_unwatched_historical_state_changes() {
    let mut insights = InsightCollection::new();
    for _ in 0..1000 {
        let id = insights.add_insight(Insight::default());
        insights.get_mut(&id).unwrap().state = InsightState::Cancelled;
        insights.refresh_runtime_tracking(&id);
        insights.remove_insight(&id);
    }
    assert!(!insights.has_pipeline_state_changes());
}

#[tokio::test]
async fn snapshot_discovery_unavailable_topics_share_the_work_and_time_budget() {
    let (mut state, directory) = owner().await;
    for n in 1..=1000 {
        state.insight_pipeline.snapshot_dependencies.update(
            Uuid::from_u128(n),
            Some(&InsightPipeDeferral::QuoteSnapshot {
                symbol: format!("S{n:04}"),
            }),
        );
    }
    assert_eq!(
        state.snapshot_continuation_turn(256, Duration::ZERO),
        (0, true)
    );
    for _ in 0..3 {
        assert_eq!(
            state.snapshot_continuation_turn(256, Duration::from_secs(10)),
            (256, true)
        );
    }
    assert_eq!(
        state.snapshot_continuation_turn(256, Duration::from_secs(10)),
        (232, false)
    );
    // The absence of a cache/revision still counts; a new explicit wake may
    // retry the topics, but no pending page repeatedly re-notifies itself.
    assert!(
        !state
            .insight_pipeline
            .snapshot_dependencies
            .has_topic_work()
    );
    finish(state, directory).await;
}

#[tokio::test]
async fn completion_discovery_live_level_wake_needs_no_future_completion_notification() {
    let (mut state, directory) = owner().await;
    let calls = Arc::new(AtomicUsize::new(0));
    state.add_pipe(
        WrappedInsightPipe::builder(Box::new(CapacityPipe(calls.clone())))
            .target_state(InsightState::New)
            .build(),
    );
    let mut row = Insight::default();
    row.symbol = "ETHUSD".into();
    row.period_unfilled = None;
    let id = state.insights.add_insight(row);
    state.run_insight_pipeline_turn(Some(vec![id]), Some(Duration::from_secs(1)), false);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let driver = state.command_driver.take().unwrap();
    assert!(
        !state.has_pending_command_continuations(),
        "no worker owner must not spin"
    );
    assert!(
        !driver.has_completion_events(),
        "there is no command or future completion"
    );
    state.command_driver = Some(driver);
    assert!(state.has_pending_command_continuations());
    tokio::time::timeout(Duration::from_secs(1), async {
        while state.has_pending_command_continuations() {
            tokio::select! {
                _ = tokio::task::yield_now(), if state.has_pending_command_continuations() => {
                    state.resume_live_command_pipes();
                }
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(
        !state.has_pending_command_continuations(),
        "unchanged deferral stops waking"
    );
    state.insights.get_mut(&id).unwrap().state = InsightState::Executed;
    assert!(
        state.has_pending_command_continuations(),
        "a state change is independently ready"
    );
    // Stop is still observed between pages, while the outstanding continuation
    // can consume its state change and retire during teardown.
    state.status = StrategyStatus::Stopping;
    state.shutdown();
    state.resume_live_command_pipes();
    assert!(state.insight_pipeline.deferred.is_empty());
    assert!(!state.has_pending_command_continuations());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    finish(state, directory).await;
}

struct OrderedCapacityPipe(Arc<std::sync::Mutex<Vec<Uuid>>>);
impl InsightPipe for OrderedCapacityPipe {
    fn version(&self) -> &str {
        "1"
    }
    fn run(&mut self, _: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        self.0.lock().unwrap().push(insight.insight_id);
        InsightPipeResult::deferred(InsightPipeDeferral::BrokerQueueCapacity, self.name())
    }
}

fn defer_capacity(state: &mut Owner, n: u128) -> Uuid {
    let id = defer(state, n);
    let dependency = InsightPipeDeferral::BrokerQueueCapacity;
    state
        .insight_pipeline
        .deferred
        .get_mut(&id)
        .unwrap()
        .dependency = dependency.clone();
    state
        .insight_pipeline
        .command_dependencies
        .update(id, Some(&dependency));
    id
}

#[tokio::test]
async fn backtest_completion_pages_preserve_order_and_deduplicate_ready_wakes() {
    let (mut state, directory) = owner().await;
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    state.add_pipe(
        WrappedInsightPipe::builder(Box::new(OrderedCapacityPipe(calls.clone())))
            .target_state(InsightState::New)
            .build(),
    );
    for n in (1..=600).rev() {
        defer_capacity(&mut state, n);
    }
    // Every owner initially has both a registration wake and a capacity cursor.
    // Discovery must count both, while a ready batch executes each owner once.
    let (examined, pending) =
        state.execution_continuation_turn(true, usize::MAX, Duration::from_secs(10));
    assert_eq!(examined, 256);
    assert!(pending);
    while state
        .execution_continuation_turn(true, 256, Duration::from_secs(10))
        .1
    {}
    assert_eq!(
        *calls.lock().unwrap(),
        (1..=600).map(Uuid::from_u128).collect::<Vec<_>>()
    );
    assert_eq!(
        state.execution_continuation_turn(true, 256, Duration::from_secs(10)),
        (0, false),
        "unchanged deferrals must not create another immediate batch"
    );
    // A changed state remains independently eligible at the same driver revision.
    for n in 1..=600 {
        state.insights.get_mut(&Uuid::from_u128(n)).unwrap().state = InsightState::Executed;
    }
    while state
        .execution_continuation_turn(true, 256, Duration::from_secs(10))
        .1
    {}
    assert!(state.insight_pipeline.deferred.is_empty());
    assert!(
        !state
            .insight_pipeline
            .command_dependencies
            .has_broker_waiters()
    );
    assert_eq!(calls.lock().unwrap().len(), 600);
    finish(state, directory).await;
}

#[tokio::test]
async fn backtest_completion_zero_remaining_budget_requeues_every_unprocessed_owner() {
    let (mut state, directory) = owner().await;
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    state.add_pipe(
        WrappedInsightPipe::builder(Box::new(OrderedCapacityPipe(calls.clone())))
            .target_state(InsightState::New)
            .build(),
    );
    let revision = state.command_driver.as_ref().unwrap().progress_revision();
    let ready = (1..=32)
        .rev()
        .map(|n| defer_capacity(&mut state, n))
        .collect::<Vec<_>>();
    state
        .insight_pipeline
        .command_dependencies
        .begin_revision(revision, true);
    for id in &ready {
        state
            .insight_pipeline
            .command_dependencies
            .mark_attempted(*id, revision);
    }
    // Teardown must still consume evidence, including when discovery used the
    // entire deadline. One complete insight makes progress; the suffix is retried.
    state.status = StrategyStatus::Stopping;
    state.shutdown();
    assert_eq!(
        state.run_backtest_continuation_page(ready, Duration::ZERO),
        1
    );
    assert_eq!(*calls.lock().unwrap(), vec![Uuid::from_u128(1)]);
    assert!(
        state
            .insight_pipeline
            .command_dependencies
            .attempted(&Uuid::from_u128(1), revision)
    );
    for n in 2..=32 {
        assert!(
            !state
                .insight_pipeline
                .command_dependencies
                .attempted(&Uuid::from_u128(n), revision)
        );
    }
    while state
        .execution_continuation_turn(true, 256, Duration::from_secs(10))
        .1
    {}
    assert_eq!(
        *calls.lock().unwrap(),
        (1..=32).map(Uuid::from_u128).collect::<Vec<_>>()
    );
    finish(state, directory).await;
}

struct AdmittingCapacityPipe {
    calls: Arc<std::sync::Mutex<Vec<Uuid>>>,
    receipts: Arc<std::sync::Mutex<Vec<BrokerCommandReceipt>>>,
    pending: HashMap<Uuid, BrokerCommandReceipt>,
}

impl InsightPipe for AdmittingCapacityPipe {
    fn version(&self) -> &str {
        "1"
    }

    fn run(&mut self, ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        self.calls.lock().unwrap().push(insight.insight_id);
        if let Some(receipt) = self.pending.get(&insight.insight_id) {
            if matches!(
                ctx.broker_command_progress(receipt),
                Some(BrokerCommandProgress::Completed(_))
            ) {
                ctx.broker_command_reduced(receipt);
                return InsightPipeResult::new(true, true, None, self.name().to_string());
            }
            return InsightPipeResult::deferred(
                InsightPipeDeferral::BrokerCommand(receipt.clone()),
                self.name(),
            );
        }
        let receipt = ctx.enqueue_broker_command(
            insight.insight_id,
            BrokerCommandAction::Submit {
                request: (&*insight).into(),
            },
            None,
        );
        match receipt {
            Ok(receipt) => {
                self.receipts.lock().unwrap().push(receipt.clone());
                self.pending.insert(insight.insight_id, receipt.clone());
                InsightPipeResult::deferred(
                    InsightPipeDeferral::BrokerCommand(receipt),
                    self.name(),
                )
            }
            Err(BrokerCommandError::QueueFull) => {
                InsightPipeResult::deferred(InsightPipeDeferral::BrokerQueueCapacity, self.name())
            }
            Err(error) => panic!("unexpected admission failure: {error}"),
        }
    }
}

fn small_lanes() -> BrokerCommandWorkerConfig {
    BrokerCommandWorkerConfig {
        entry_capacity: 1,
        control_capacity: 1,
        completion_capacity: 1,
        ..Default::default()
    }
}

fn settle_backtest_continuations(state: &mut Owner) {
    for _ in 0..32 {
        if !state
            .execution_continuation_turn(true, 256, Duration::from_secs(10))
            .1
        {
            return;
        }
    }
    panic!("unchanged continuation work must settle without another worker event");
}

fn capacity_checkpoint(
    state: &Owner,
    id: Uuid,
) -> crate::core::insight::execution_checkpoint::InsightExecutionCheckpoint {
    crate::core::insight::execution_checkpoint::InsightExecutionCheckpoint::capture(
        state.insights.get(&id).unwrap(),
        state
            .command_driver
            .as_ref()
            .unwrap()
            .checkpoint_revision(&id)
            + 1,
        true,
        None,
    )
}

#[tokio::test]
async fn backtest_capacity_batch_rechecks_slots_and_resumes_after_durable_retirement() {
    let (mut state, directory) = owner_with_config(small_lanes()).await;
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let receipts = Arc::new(std::sync::Mutex::new(Vec::new()));
    state.add_pipe(
        WrappedInsightPipe::builder(Box::new(AdmittingCapacityPipe {
            calls: calls.clone(),
            receipts: receipts.clone(),
            pending: HashMap::new(),
        }))
        .target_state(InsightState::New)
        .build(),
    );
    for n in (1..=64).rev() {
        let id = defer_capacity(&mut state, n);
        let row = state.insights.get_mut(&id).unwrap();
        row.quantity = Some(1.0);
        row.limit_price = Some(100.0);
    }
    settle_backtest_continuations(&mut state);
    assert_eq!(
        *calls.lock().unwrap(),
        vec![Uuid::from_u128(1), Uuid::from_u128(2)]
    );
    assert_eq!(receipts.lock().unwrap().len(), 2);
    assert!(
        !state
            .command_driver
            .as_ref()
            .unwrap()
            .has_admission_capacity()
    );
    for n in 3..=64 {
        assert!(matches!(
            state.insight_pipeline.deferred[&Uuid::from_u128(n)].dependency,
            InsightPipeDeferral::BrokerQueueCapacity
        ));
    }

    // Produce actual worker completion and a durable owner acknowledgement.
    // Cancellation avoids requiring a market-data fixture to execute the order.
    let first = receipts.lock().unwrap()[0].clone();
    let row = capacity_checkpoint(&state, Uuid::from_u128(1));
    let driver = state.command_driver.as_mut().unwrap();
    driver.cancel_pending_entry(&first).unwrap();
    driver
        .persist_execution_checkpoints(vec![row], vec![])
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while driver.checkpoint_in_flight() {
            driver.poll_acknowledgements(256).unwrap();
            tokio::task::yield_now().await;
        }
        while matches!(driver.progress(&first), Some(BrokerCommandProgress::Queued)) {
            driver.drain_ready(256, Duration::from_millis(5)).unwrap();
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        driver.progress(&first),
        Some(BrokerCommandProgress::Completed(_))
    ));
    state
        .insight_pipeline
        .command_dependencies
        .completed(&first, false);
    settle_backtest_continuations(&mut state);
    assert_eq!(
        *calls.lock().unwrap(),
        vec![Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(1)]
    );
    assert!(
        !state
            .command_driver
            .as_ref()
            .unwrap()
            .has_admission_capacity(),
        "completion alone cannot release durable receipt ownership"
    );

    let row = capacity_checkpoint(&state, Uuid::from_u128(1));
    let driver = state.command_driver.as_mut().unwrap();
    let before_retirement = driver.progress_revision();
    driver
        .persist_execution_checkpoints(vec![row], vec![first.clone()])
        .unwrap();
    assert!(!driver.has_admission_capacity());
    tokio::time::timeout(Duration::from_secs(10), async {
        while driver.checkpoint_in_flight() {
            driver.poll_acknowledgements(256).unwrap();
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(driver.has_admission_capacity());
    assert_ne!(driver.progress_revision(), before_retirement);
    assert!(driver.progress(&first).is_none());
    settle_backtest_continuations(&mut state);
    assert_eq!(
        *calls.lock().unwrap(),
        vec![
            Uuid::from_u128(1),
            Uuid::from_u128(2),
            Uuid::from_u128(1),
            Uuid::from_u128(3)
        ]
    );
    assert_eq!(
        receipts.lock().unwrap().len(),
        3,
        "one freed slot resumes exactly one further admission"
    );
    assert!(
        !state
            .command_driver
            .as_ref()
            .unwrap()
            .has_admission_capacity()
    );
    finish(state, directory).await;
}

#[tokio::test]
async fn backtest_capacity_recheck_preserves_mixed_evidence_state_changes_and_cleanup() {
    let (mut state, directory) = owner_with_config(small_lanes()).await;
    for n in [1000, 1001] {
        let mut row = Insight::default();
        row.insight_id = Uuid::from_u128(n);
        row.symbol = "ETHUSD".into();
        row.quantity = Some(1.0);
        row.limit_price = Some(100.0);
        state
            .command_driver
            .as_mut()
            .unwrap()
            .enqueue(
                &row.insight_id.to_string(),
                BrokerCommandAction::Submit {
                    request: (&row).into(),
                },
                None,
            )
            .unwrap();
    }
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    for target in [InsightState::New, InsightState::Executed] {
        state.add_pipe(
            WrappedInsightPipe::builder(Box::new(OrderedCapacityPipe(calls.clone())))
                .target_state(target)
                .build(),
        );
    }
    let ordinary = defer_capacity(&mut state, 1);
    let receipt = defer(&mut state, 2);
    let changed = defer_capacity(&mut state, 3);
    state.insights.get_mut(&changed).unwrap().state = InsightState::Executed;
    let inactive = defer_capacity(&mut state, 4);
    state.insights.get_mut(&inactive).unwrap().state = InsightState::Cancelled;
    let closing = defer_capacity(&mut state, 5);
    state.insights.get_mut(&closing).unwrap().closing = true;
    let cancelling = defer_capacity(&mut state, 6);
    state.insights.get_mut(&cancelling).unwrap().cancelling = true;
    assert_eq!(
        state.run_backtest_continuation_page(
            vec![cancelling, closing, inactive, changed, receipt, ordinary],
            Duration::from_secs(10)
        ),
        6
    );
    assert_eq!(
        *calls.lock().unwrap(),
        vec![receipt, changed, closing, cancelling]
    );
    assert!(!state.insight_pipeline.deferred.contains_key(&inactive));
    assert!(state.insight_pipeline.deferred.contains_key(&ordinary));
    assert_eq!(
        state.insight_pipeline.deferred[&changed].state,
        InsightState::Executed
    );
    calls.lock().unwrap().clear();
    // This optimization is confined to backtest continuation batches. Ordinary
    // callers and shutdown cleanup retain their previous invocation behavior.
    state.run_insight_pipeline_turn(Some(vec![ordinary]), Some(Duration::from_secs(10)), false);
    assert_eq!(*calls.lock().unwrap(), vec![ordinary]);
    calls.lock().unwrap().clear();
    state.shutdown_tx.send_replace(true);
    assert_eq!(
        state.run_backtest_continuation_page(vec![ordinary], Duration::ZERO),
        1
    );
    assert_eq!(*calls.lock().unwrap(), vec![ordinary]);
    finish(state, directory).await;
}
