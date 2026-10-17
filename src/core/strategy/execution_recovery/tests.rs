use super::*;
use crate::core::{
    broker::{
        command_journal::{BrokerCommandJournal, ExecutionCheckpointBatch, ExecutionIntent},
        commands::{BrokerCommand, BrokerCommandAction},
        data_feeds::yahoo::YahooFinanceDataFeed,
        types::{AccountType, AssetExchange, AssetStatus, AssetType, OrderType},
    },
    insight::{
        execution_checkpoint::InsightExecutionCheckpoint,
        operations::{MutationEffect, MutationKind, PendingInsightMutation},
    },
    pipeline::InsightPipeDeferral,
    utils::timeframe::TimeFrameUnit,
};
use chrono::{TimeZone, Utc};

fn insight(quantity: f64, state: InsightState) -> Insight {
    let mut insight = Insight::default();
    insight.symbol = "ETHUSD".into();
    insight.quantity = Some(quantity);
    insight.state = state;
    insight.created_at = Utc.with_ymd_and_hms(2025, 1, 1, 7, 0, 0).unwrap();
    insight.updated_at = insight.created_at;
    insight.state_history.push((
        insight.created_at,
        insight.state.clone(),
        Some("saved history".into()),
    ));
    insight
}

fn recovery(rows: Vec<InsightExecutionCheckpoint>) -> ExecutionRecovery {
    ExecutionRecovery {
        rows: rows.into_iter().map(|row| (row.insight_id, row)).collect(),
        commands: Vec::new(),
        stopping: false,
    }
}

#[test]
fn reconstructed_dependencies_keep_closed_shared_owners_available_after_pruning() {
    let mut root = insight(1.0, InsightState::Closed);
    let mut source = insight(2.0, InsightState::Closed);
    let mut live = insight(3.0, InsightState::Filled);
    source.parent_id = Some(root.insight_id);
    live.parent_id = Some(root.insight_id);
    root.set_execution_data("blueprint", &serde_json::json!({"levels":4}))
        .unwrap();
    source
        .set_execution_data("target", &serde_json::json!({"version":7,"price":102.0}))
        .unwrap();
    live.set_recovery_dependencies(&[root.insight_id, source.insight_id])
        .unwrap();
    root.children = vec![source.clone(), live.clone()];
    let rows = vec![
        InsightExecutionCheckpoint::capture(&root, 1, true, None),
        InsightExecutionCheckpoint::capture(&source, 1, true, None),
        InsightExecutionCheckpoint::capture(&live, 1, true, None),
    ];
    let encoded = serde_json::to_string(&rows).unwrap();
    let decoded = serde_json::from_str(&encoded).unwrap();
    let now = Utc::now();
    let mut restored =
        rebuild_insights(recovery(decoded), InsightStrategyContext::new(move || now))
            .unwrap()
            .collection;
    restored.prune_terminal_insight(&root.insight_id);
    restored.prune_terminal_insight(&source.insight_id);
    assert!(restored.get(&root.insight_id).is_none());
    assert!(restored.get(&source.insight_id).is_none());
    assert_eq!(
        restored
            .recovery_view(&source.insight_id)
            .unwrap()
            .execution_data("target")
            .unwrap()
            .unwrap()["version"],
        7
    );
    assert_eq!(
        restored
            .recovery_view(&root.insight_id)
            .unwrap()
            .execution_data("blueprint")
            .unwrap()
            .unwrap()["levels"],
        4
    );
    live.state = InsightState::Closed;
    restored.add_insight(live);
    assert!(restored.recovery_view(&root.insight_id).is_none());
    assert!(restored.recovery_view(&source.insight_id).is_none());
}

#[test]
fn reconstruction_preserves_nested_quantity_timestamps_and_single_history_ownership() {
    let mut parent = insight(1.0, InsightState::Filled);
    let mut child = insight(2.0, InsightState::Filled);
    let mut dormant = insight(3.0, InsightState::New);
    let mut closed = insight(4.0, InsightState::Closed);
    child.parent_id = Some(parent.insight_id);
    child.is_split_insight = true;
    dormant.parent_id = Some(child.insight_id);
    closed.parent_id = Some(parent.insight_id);
    child.children.push(dormant.clone());
    parent.children.extend([child.clone(), closed.clone()]);
    let rows = vec![
        InsightExecutionCheckpoint::capture(&parent, 1, true, None),
        InsightExecutionCheckpoint::capture(&child, 1, true, None),
        InsightExecutionCheckpoint::capture(&dormant, 1, false, None),
        InsightExecutionCheckpoint::capture(&closed, 1, true, None),
    ];
    let now = parent.created_at + chrono::Duration::days(100);
    let restored = rebuild_insights(recovery(rows), InsightStrategyContext::new(move || now))
        .unwrap()
        .collection;
    assert_eq!(restored.len(), 3);
    assert!(!restored.was_materialized(&dormant.insight_id));
    assert!(restored.was_materialized(&closed.insight_id));
    let restored_parent = restored.get(&parent.insight_id).unwrap();
    assert_eq!(restored_parent.created_at, parent.created_at);
    assert_eq!(restored_parent.updated_at, parent.updated_at);
    assert_eq!(
        restored_parent.context.as_ref().unwrap().current_time(),
        now
    );
    assert_eq!(
        restored_parent.snapshot_fingerprint_hash(),
        parent.snapshot_fingerprint_hash()
    );
    assert_eq!(
        restored_parent
            .children
            .iter()
            .map(|row| row.insight_id)
            .collect::<Vec<_>>(),
        vec![child.insight_id, closed.insight_id]
    );
    assert!(restored_parent.children[0].is_split_insight);
    assert_eq!(restored_parent.total_insight_quantity_size(), 6.0);
    assert_eq!(
        restored
            .total_insight_quantity_size("ETHUSD")
            .total_quantity,
        6.0
    );
    assert!(
        restored_parent
            .children
            .iter()
            .all(|row| row.state_history.is_empty())
    );
    assert!(
        restored_parent.children[0].children[0]
            .state_history
            .is_empty()
    );
    let restored_child = restored.get(&child.insight_id).unwrap();
    assert_eq!(restored_child.state_history, child.state_history);
    assert_eq!(
        restored_child.children[0].state_history,
        dormant.state_history
    );
    assert_eq!(restored_child.children[0].created_at, dormant.created_at);
}

#[test]
fn reconstruction_moves_a_thousand_dormant_templates_without_materializing_them() {
    let mut root = insight(1.0, InsightState::Filled);
    let mut rows = Vec::new();
    for _ in 0..1_000 {
        let mut child = insight(1.0, InsightState::New);
        child.parent_id = Some(root.insight_id);
        rows.push(InsightExecutionCheckpoint::capture(&child, 1, false, None));
        root.children.push(child);
    }
    rows.push(InsightExecutionCheckpoint::capture(&root, 1, true, None));
    let restored = rebuild_insights(recovery(rows), InsightStrategyContext::new(Utc::now))
        .unwrap()
        .collection;
    assert_eq!(restored.len(), 1);
    let parent = restored.get(&root.insight_id).unwrap();
    assert_eq!(parent.children.len(), 1_000);
    assert_eq!(parent.total_insight_quantity_size(), 1_001.0);
    assert!(parent.children.iter().all(
        |child| child.state_history.len() == 1 && !restored.was_materialized(&child.insight_id)
    ));
}

#[test]
fn reconstruction_rejects_materialized_children_of_dormant_parents_and_cycles() {
    let mut parent = insight(1.0, InsightState::New);
    let mut child = insight(1.0, InsightState::Filled);
    child.parent_id = Some(parent.insight_id);
    parent.children.push(child.clone());
    let a = InsightExecutionCheckpoint::capture(&parent, 1, false, None);
    let b = InsightExecutionCheckpoint::capture(&child, 1, true, None);
    assert!(rebuild_insights(recovery(vec![a, b]), InsightStrategyContext::new(Utc::now)).is_err());
    let mut a = InsightExecutionCheckpoint::capture(&parent, 1, true, None);
    let mut b = InsightExecutionCheckpoint::capture(&child, 1, true, None);
    a.parent_id = Some(b.insight_id);
    b.child_ids.push(a.insight_id);
    assert!(rebuild_insights(recovery(vec![a, b]), InsightStrategyContext::new(Utc::now)).is_err());
}

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
fn state() -> StrategyState<Noop, PaperBroker, YahooFinanceDataFeed> {
    StrategyState::new(
        "recovery".into(),
        "1".into(),
        Noop,
        UnifiedBroker::new(
            PaperBroker::new(AccountType::Paper, 100_000.0, 1),
            YahooFinanceDataFeed::new(),
        ),
        StrategyMode::Live,
        TimeFrame::new(1, TimeFrameUnit::Minute),
    )
}

#[tokio::test]
async fn owner_install_restores_identity_and_tombstones_without_broker_operations() {
    let directory = std::env::temp_dir().join(format!("aqe-owner-recovery-{}", Uuid::new_v4()));
    let path = directory.join("commands.db");
    let mut root = insight(1.0, InsightState::Filled);
    let mut child = insight(2.0, InsightState::Cancelled);
    child.parent_id = Some(root.insight_id);
    root.children.push(child.clone());
    let mut journal = BrokerCommandJournal::open(&path, "strategy", "account")
        .await
        .unwrap();
    journal
        .persist_execution_batch(
            &ExecutionCheckpointBatch::new(
                vec![
                    InsightExecutionCheckpoint::capture(&root, 1, true, None),
                    InsightExecutionCheckpoint::capture(&child, 1, true, None),
                ],
                vec![],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    drop(journal);
    let mut owner = state();
    owner
        .start_command_driver(&path, "strategy", "account", Default::default())
        .await
        .unwrap();
    assert_eq!(owner.insights.len(), 2);
    assert!(owner.insights.was_materialized(&child.insight_id));
    assert_eq!(
        owner.insights.get(&root.insight_id).unwrap().state_history,
        root.state_history
    );
    assert!(
        owner
            .broker
            .execution
            .get_orders()
            .await
            .unwrap()
            .is_empty()
    );
    drop(owner.command_driver.take().unwrap().stop().await.unwrap());
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn changed_pipeline_fails_closed_and_releases_the_journal_for_retry() {
    let directory =
        std::env::temp_dir().join(format!("aqe-owner-recovery-refused-{}", Uuid::new_v4()));
    let path = directory.join("commands.db");
    let row = insight(1.0, InsightState::Filled);
    let mut journal = BrokerCommandJournal::open(&path, "strategy", "account")
        .await
        .unwrap();
    journal
        .persist_execution_batch(
            &ExecutionCheckpointBatch::new(
                vec![InsightExecutionCheckpoint::capture(
                    &row,
                    1,
                    true,
                    Some(InsightPipelineCheckpoint {
                        state: InsightState::Filled,
                        index: 0,
                        dependency: InsightPipeDeferral::AccountSnapshot,
                        layout: vec!["removed pipe".into()],
                        nested: None,
                    }),
                )],
                vec![],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    drop(journal);
    let mut owner = state();
    assert!(
        owner
            .start_command_driver(&path, "strategy", "account", Default::default())
            .await
            .is_err()
    );
    assert!(owner.command_driver.is_none());
    assert!(owner.insights.is_empty());
    assert!(owner.insight_pipeline.deferred.is_empty());
    assert!(
        owner
            .broker
            .execution
            .get_orders()
            .await
            .unwrap()
            .is_empty()
    );
    let journal = BrokerCommandJournal::open(&path, "strategy", "account")
        .await
        .unwrap();
    assert_eq!(
        journal
            .execution_checkpoint_page("", 32)
            .await
            .unwrap()
            .rows
            .len(),
        1
    );
    drop(journal);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn owner_install_restores_first_fill_deferral_and_refuses_existing_ownership() {
    use crate::core::pipeline::{InsightPipe, InsightPipeResult};
    struct Waiting;
    impl InsightPipe for Waiting {
        fn version(&self) -> &str {
            "1"
        }
        fn run(&mut self, _: &mut dyn StrategyContext, _: &mut Insight) -> InsightPipeResult {
            InsightPipeResult::deferred(InsightPipeDeferral::AccountSnapshot, self.name())
        }
    }
    let mut owner = state();
    let pipe = crate::core::pipeline::InsightPipeBuilder::new(Box::new(Waiting))
        .target_state(InsightState::Filled)
        .build();
    let layout = vec![pipe.recovery_layout()];
    owner.insight_pipeline.add_pipe(pipe);
    let mut row = insight(1.0, InsightState::Filled);
    row.first_on_fill = true;
    let checkpoint = InsightExecutionCheckpoint::capture(
        &row,
        1,
        true,
        Some(InsightPipelineCheckpoint {
            state: InsightState::Filled,
            index: 0,
            dependency: InsightPipeDeferral::AccountSnapshot,
            layout,
            nested: None,
        }),
    );
    owner
        .install_execution_recovery(recovery(vec![checkpoint.clone()]))
        .unwrap();
    assert!(owner.insights.get(&row.insight_id).unwrap().first_on_fill);
    assert_eq!(
        owner.insight_pipeline.execution_checkpoint(&row.insight_id),
        checkpoint.pipeline
    );
    assert!(
        owner
            .install_execution_recovery(recovery(vec![checkpoint]))
            .is_err()
    );
    assert_eq!(owner.insights.len(), 1);
}

type Owner = StrategyState<Noop, PaperBroker, YahooFinanceDataFeed>;

fn execution_owner() -> Owner {
    let mut owner = state();
    let asset = Asset {
        id: "ETHUSD".into(),
        symbol: "ETHUSD".into(),
        name: "ETHUSD".into(),
        asset_type: AssetType::Stock,
        exchange: AssetExchange::NASDAQ,
        status: AssetStatus::Active,
        tradable: true,
        marginable: true,
        shortable: true,
        fractional: true,
        min_order_size: None,
        quantity_base: None,
        max_order_size: None,
        max_total_size: None,
        min_price_increment: None,
        price_base: None,
        contract_size: None,
        trading_calendar: None,
        fees: Default::default(),
    };
    owner.broker.execution.register_asset_metadata(&asset);
    owner.universe.insert(asset.symbol.clone(), asset);
    owner
}

fn limit(state: InsightState) -> Insight {
    let mut row = insight(1.0, state);
    row.order_type = OrderType::Limit;
    row.limit_price = Some(45.0);
    row
}

async fn saved_graph(rows: Vec<InsightExecutionCheckpoint>) -> std::path::PathBuf {
    let directory = std::env::temp_dir().join(format!("aqe-recovery-work-{}", Uuid::new_v4()));
    let mut journal =
        BrokerCommandJournal::open(directory.join("commands.db"), "strategy", "account")
            .await
            .unwrap();
    for rows in rows.chunks(32) {
        journal
            .persist_execution_batch(&ExecutionCheckpointBatch::new(rows.to_vec(), vec![]).unwrap())
            .await
            .unwrap();
    }
    drop(journal);
    directory
}

async fn start_saved(owner: &mut Owner, directory: &std::path::Path) {
    owner
        .start_command_driver(
            directory.join("commands.db"),
            "strategy",
            "account",
            crate::core::broker::command_worker::BrokerCommandWorkerConfig {
                entry_capacity: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
}

async fn drain(owner: &mut Owner) {
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        owner.drain_backtest_execution(),
    )
    .await
    .expect("recovered work must not stall")
    .unwrap();
}

#[tokio::test]
async fn durable_stop_restart_cancels_saved_entries_and_only_settlement_opens_a_new_epoch() {
    use crate::core::broker::command_journal::lifecycle::ExecutionPhase;
    let mut root = limit(InsightState::New);
    let mut child = limit(InsightState::New);
    child.parent_id = Some(root.insight_id);
    child.is_split_insight = true;
    root.children.push(child.clone());
    let command = BrokerCommand::new(
        "strategy",
        "account",
        &root.insight_id.to_string(),
        BrokerCommandAction::Submit {
            request: (&root).into(),
        },
        None,
    );
    root.pending_mutations.push(PendingInsightMutation {
        kind: MutationKind::Submit,
        receipt: command.receipt(),
        effect: MutationEffect::Submit,
    });
    let directory = saved_graph(vec![InsightExecutionCheckpoint::capture(
        &child, 1, true, None,
    )])
    .await;
    let mut journal =
        BrokerCommandJournal::open(directory.join("commands.db"), "strategy", "account")
            .await
            .unwrap();
    journal
        .persist_execution_batch(
            &ExecutionCheckpointBatch::with_intents(
                vec![InsightExecutionCheckpoint::capture(&root, 1, true, None)],
                vec![],
                vec![ExecutionIntent {
                    command,
                    admission_order: 1,
                    queued_at_us: 1,
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    journal.request_execution_stop().await.unwrap();
    let stopped = journal.execution_lifecycle();
    drop(journal);

    // Merely reopening the process must not clear the original stop request.
    for _ in 0..2 {
        let mut owner = execution_owner();
        start_saved(&mut owner, &directory).await;
        assert_eq!(owner.status, StrategyStatus::Stopping);
        assert!(*owner.shutdown_tx.borrow());
        assert!(
            owner
                .command_driver
                .as_ref()
                .unwrap()
                .sender()
                .entries_stopped()
        );
        assert!(
            !owner
                .command_driver
                .as_ref()
                .unwrap()
                .complete_execution_stop()
                .await
                .unwrap()
        );
        let journal = owner.command_driver.take().unwrap().stop().await.unwrap();
        assert_eq!(journal.execution_lifecycle(), stopped);
        drop(journal);
    }

    let mut owner = execution_owner();
    start_saved(&mut owner, &directory).await;
    drain(&mut owner).await;
    assert!(
        owner
            .broker
            .execution
            .get_orders()
            .await
            .unwrap()
            .is_empty()
    );
    for id in [root.insight_id, child.insight_id] {
        let row = owner.insights.get(&id).unwrap();
        assert_eq!(row.state, InsightState::Cancelled);
        assert!(row.pending_mutations.is_empty());
    }
    assert!(
        owner
            .command_driver
            .as_ref()
            .unwrap()
            .complete_execution_stop()
            .await
            .unwrap()
    );
    let journal = owner.command_driver.take().unwrap().stop().await.unwrap();
    assert_eq!(journal.execution_lifecycle().phase, ExecutionPhase::Stopped);
    assert!(journal.recovery_page(0, 32).await.unwrap().is_empty());
    drop(journal);

    // Production recovery reuses this owner, whose status still says Stopping.
    owner.shutdown_tx.send_replace(false);
    owner.status = StrategyStatus::Stopping;
    owner.prepare_recovered_live_restart().unwrap();
    let mut next = owner;
    start_saved(&mut next, &directory).await;
    assert!(!*next.shutdown_tx.borrow());
    assert!(
        !next
            .command_driver
            .as_ref()
            .unwrap()
            .sender()
            .entries_stopped()
    );
    next.status = StrategyStatus::Running;
    let mut fresh = limit(InsightState::New);
    next.bind_insight_context(&mut fresh);
    assert!(fresh.submit(&mut next).is_ok());
    next.insights.add_insight(fresh);
    drain(&mut next).await;
    assert_eq!(next.broker.execution.get_orders().await.unwrap().len(), 1);
    let journal = next.command_driver.take().unwrap().stop().await.unwrap();
    assert_eq!(journal.execution_lifecycle().phase, ExecutionPhase::Running);
    assert_eq!(journal.execution_lifecycle().epoch, stopped.epoch + 1);
    drop(journal);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn recovered_restart_preserves_new_stop_request() {
    let mut owner = execution_owner();
    owner.status = StrategyStatus::Stopping;
    owner.shutdown_tx.send_replace(true);
    assert!(owner.prepare_recovered_live_restart().is_err());
    assert_eq!(owner.status, StrategyStatus::Stopping);
    assert!(*owner.shutdown_tx.borrow());
}

#[test]
fn recovery_work_waits_for_start_and_preserves_root_configuration_ownership() {
    let mut root = limit(InsightState::New);
    let mut child = limit(InsightState::New);
    child.parent_id = Some(root.insight_id);
    child.is_split_insight = true;
    root.children.push(child.clone());
    let mut owner = execution_owner();
    owner
        .install_execution_recovery(recovery(vec![
            InsightExecutionCheckpoint::capture(&root, 1, true, None),
            InsightExecutionCheckpoint::capture(&child, 1, false, None),
        ]))
        .unwrap();
    for status in [StrategyStatus::Initialised, StrategyStatus::Paused] {
        owner.status = status;
        assert_eq!(
            owner.process_insight_work(256, std::time::Duration::from_secs(1)),
            0
        );
        assert!(
            !owner.has_pending_broker_events(),
            "held recovery must not create a busy loop"
        );
        assert!(owner.insights.get(&child.insight_id).is_none());
    }
    owner.status = StrategyStatus::Running;
    assert_eq!(
        owner.process_insight_work(256, std::time::Duration::from_secs(1)),
        1
    );
    let restored = owner.insights.get(&root.insight_id).unwrap();
    assert_eq!(restored.state_history, root.state_history);
    assert_eq!(restored.created_at, root.created_at);
    assert!(!restored.has_pending_submit());
    assert!(owner.insights.get(&child.insight_id).is_none());
    assert!(!owner.has_pending_broker_events());
}

#[tokio::test]
async fn recovery_work_resumes_materialized_children_and_dormant_splits_once_across_restarts() {
    let mut root = limit(InsightState::Filled);
    root.order_id = Some("retained-parent-position".into());
    let mut child = limit(InsightState::New);
    child.parent_id = Some(root.insight_id);
    let mut fragment = limit(InsightState::New);
    fragment.parent_id = Some(child.insight_id);
    fragment.is_split_insight = true;
    child.children.push(fragment.clone());
    let mut split = limit(InsightState::New);
    split.parent_id = Some(root.insight_id);
    split.is_split_insight = true;
    let mut cancelled = limit(InsightState::Cancelled);
    cancelled.parent_id = Some(root.insight_id);
    root.children
        .extend([child.clone(), split.clone(), cancelled.clone()]);
    let directory = saved_graph(vec![
        InsightExecutionCheckpoint::capture(&root, 1, true, None),
        InsightExecutionCheckpoint::capture(&child, 1, true, None),
        InsightExecutionCheckpoint::capture(&fragment, 1, false, None),
        InsightExecutionCheckpoint::capture(&split, 1, false, None),
        InsightExecutionCheckpoint::capture(&cancelled, 1, true, None),
    ])
    .await;
    let mut owner = execution_owner();
    owner.status = StrategyStatus::Running;
    start_saved(&mut owner, &directory).await;
    drain(&mut owner).await;
    assert_eq!(owner.broker.execution.get_orders().await.unwrap().len(), 3);
    let ids = [child.insight_id, fragment.insight_id, split.insight_id];
    let expected_orders: Vec<_> = ids
        .iter()
        .map(|id| {
            let row = owner.insights.get(id).unwrap();
            assert_eq!(row.state, InsightState::Executed);
            assert!(!row.has_pending_submit());
            row.order_id.clone().unwrap()
        })
        .collect();
    assert_eq!(
        owner.insights.get(&child.insight_id).unwrap().created_at,
        child.created_at
    );
    assert_eq!(
        owner.insights.get(&root.insight_id).unwrap().state_history,
        root.state_history
    );
    assert_eq!(
        owner
            .insights
            .get(&cancelled.insight_id)
            .unwrap()
            .state_history,
        cancelled.state_history
    );
    let broker = owner.broker.execution.as_ref().clone();
    drop(owner.command_driver.take().unwrap().stop().await.unwrap());
    drop(owner);

    let mut restarted = execution_owner();
    restarted.broker.execution = Arc::new(broker);
    restarted.status = StrategyStatus::Running;
    start_saved(&mut restarted, &directory).await;
    for _ in 0..3 {
        restarted.schedule_insight_children(root.insight_id);
        restarted.schedule_insight_children(child.insight_id);
        drain(&mut restarted).await;
    }
    assert_eq!(
        restarted.broker.execution.get_orders().await.unwrap().len(),
        3
    );
    assert_eq!(
        ids.iter()
            .map(|id| restarted
                .insights
                .get(id)
                .unwrap()
                .order_id
                .clone()
                .unwrap())
            .collect::<Vec<_>>(),
        expected_orders
    );
    assert_eq!(
        restarted.insights.get(&cancelled.insight_id).unwrap().state,
        InsightState::Cancelled
    );
    drop(
        restarted
            .command_driver
            .take()
            .unwrap()
            .stop()
            .await
            .unwrap(),
    );
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn recovery_work_keeps_ordinary_children_waiting_for_direct_parent_fill() {
    let mut root = limit(InsightState::Executed);
    root.order_id = Some("retained-limit".into());
    let mut child = limit(InsightState::New);
    child.parent_id = Some(root.insight_id);
    let mut fragment = limit(InsightState::New);
    fragment.parent_id = Some(child.insight_id);
    fragment.is_split_insight = true;
    child.children.push(fragment.clone());
    root.children.push(child.clone());
    let directory = saved_graph(vec![
        InsightExecutionCheckpoint::capture(&root, 1, true, None),
        InsightExecutionCheckpoint::capture(&child, 1, true, None),
        InsightExecutionCheckpoint::capture(&fragment, 1, false, None),
    ])
    .await;
    let mut owner = execution_owner();
    owner.status = StrategyStatus::Running;
    start_saved(&mut owner, &directory).await;
    drain(&mut owner).await;
    assert!(
        owner
            .broker
            .execution
            .get_orders()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        owner.insights.get(&child.insight_id).unwrap().state,
        InsightState::New
    );
    assert!(!owner.insights.was_materialized(&fragment.insight_id));
    assert!(!owner.has_pending_broker_events());

    owner.insights.get_mut(&root.insight_id).unwrap().state = InsightState::Filled;
    owner.insights.refresh_runtime_tracking(&root.insight_id);
    owner.schedule_insight_children(root.insight_id);
    drain(&mut owner).await;
    assert_eq!(owner.broker.execution.get_orders().await.unwrap().len(), 2);
    assert_eq!(
        owner.insights.get(&fragment.insight_id).unwrap().state,
        InsightState::Executed
    );
    drop(owner.command_driver.take().unwrap().stop().await.unwrap());
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn recovery_work_cleans_blocked_ancestors_without_materializing_dormant_descendants() {
    for missing in [false, true] {
        let mut root = limit(InsightState::Cancelled);
        let mut child = limit(InsightState::New);
        child.parent_id = Some(root.insight_id);
        child.is_split_insight = true;
        let mut nested = limit(InsightState::New);
        nested.parent_id = Some(child.insight_id);
        nested.is_split_insight = true;
        let mut dormant = limit(InsightState::New);
        dormant.parent_id = Some(nested.insight_id);
        dormant.is_split_insight = true;
        nested.children.push(dormant.clone());
        child.children.push(nested.clone());
        root.children.push(child.clone());
        let mut rows = vec![
            InsightExecutionCheckpoint::capture(&child, 1, true, None),
            InsightExecutionCheckpoint::capture(&nested, 1, true, None),
            InsightExecutionCheckpoint::capture(&dormant, 1, false, None),
        ];
        if !missing {
            rows.push(InsightExecutionCheckpoint::capture(&root, 1, true, None));
        }
        let mut owner = execution_owner();
        owner.status = StrategyStatus::Running;
        owner.install_execution_recovery(recovery(rows)).unwrap();
        while owner.has_pending_broker_events() {
            assert!(owner.process_insight_work(1, std::time::Duration::from_secs(1)) <= 1);
        }
        for id in [child.insight_id, nested.insight_id] {
            assert_eq!(
                owner.insights.get(&id).unwrap().state,
                InsightState::Cancelled
            );
            assert!(
                owner
                    .insights
                    .get(&id)
                    .unwrap()
                    .pending_mutations
                    .is_empty()
            );
        }
        assert!(!owner.insights.was_materialized(&dormant.insight_id));
    }
}

#[test]
fn recovery_work_stop_preempts_a_thousand_saved_child_activations() {
    let mut root = limit(InsightState::Filled);
    root.order_id = Some("retained-position".into());
    let mut rows = Vec::new();
    for _ in 0..1_000 {
        let mut child = limit(InsightState::New);
        child.parent_id = Some(root.insight_id);
        rows.push(InsightExecutionCheckpoint::capture(&child, 1, true, None));
        root.children.push(child);
    }
    rows.push(InsightExecutionCheckpoint::capture(&root, 1, true, None));
    let mut owner = execution_owner();
    owner.install_execution_recovery(recovery(rows)).unwrap();
    owner.status = StrategyStatus::Running;
    // Classify a saved child, then stop before its queued submit is executed.
    assert_eq!(
        owner.process_insight_work(1, std::time::Duration::from_secs(1)),
        1
    );
    owner.shutdown();
    let first = root.children[0].insight_id;
    owner.schedule_insight_cleanup(first);
    assert_eq!(
        owner.process_insight_work(1, std::time::Duration::from_secs(1)),
        1
    );
    assert_eq!(
        owner.insights.get(&first).unwrap().state,
        InsightState::Cancelled
    );
    let mut turns = 0;
    while owner.insight_work.is_ready() {
        let work = owner.process_insight_work(10_000, std::time::Duration::from_secs(1));
        assert!(work <= 256);
        turns += 1;
        assert!(
            turns < 30,
            "recovery should be linear, not repeatedly scan all children"
        );
    }
    assert!(turns > 1);
    for child in &root.children {
        let row = owner.insights.get(&child.insight_id).unwrap();
        assert_eq!(row.state, InsightState::Cancelled);
        assert!(!row.has_pending_submit());
    }
    // Without a driver/broker close confirmation the parent is not falsely closed.
    assert_eq!(
        owner.insights.get(&root.insight_id).unwrap().state,
        InsightState::Filled
    );
}

#[tokio::test]
async fn recovery_work_polls_an_existing_entry_intent_without_duplicating_it() {
    let mut root = limit(InsightState::Executed);
    root.order_id = Some("retained-entry".into());
    let mut child = limit(InsightState::New);
    child.parent_id = Some(root.insight_id);
    child.is_split_insight = true;
    let command = BrokerCommand::new(
        "strategy",
        "account",
        &child.insight_id.to_string(),
        BrokerCommandAction::Submit {
            request: (&child).into(),
        },
        None,
    );
    child.pending_mutations.push(PendingInsightMutation {
        kind: MutationKind::Submit,
        receipt: command.receipt(),
        effect: MutationEffect::Submit,
    });
    root.children.push(child.clone());
    let directory = saved_graph(vec![InsightExecutionCheckpoint::capture(
        &root, 1, true, None,
    )])
    .await;
    let mut journal =
        BrokerCommandJournal::open(directory.join("commands.db"), "strategy", "account")
            .await
            .unwrap();
    journal
        .persist_execution_batch(
            &ExecutionCheckpointBatch::with_intents(
                vec![InsightExecutionCheckpoint::capture(&child, 1, true, None)],
                vec![],
                vec![ExecutionIntent {
                    command,
                    admission_order: 1,
                    queued_at_us: 1,
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    drop(journal);
    let mut owner = execution_owner();
    start_saved(&mut owner, &directory).await;
    for status in [StrategyStatus::Initialised, StrategyStatus::Paused] {
        owner.status = status;
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            owner.drain_ready_broker_events(),
        )
        .await
        .expect("held entry intent must not spin the owner loop");
        assert!(
            owner
                .broker
                .execution
                .get_orders()
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(owner.command_driver.as_ref().unwrap().pending_count(), 1);
        assert!(
            owner
                .insights
                .get(&child.insight_id)
                .unwrap()
                .has_pending_submit()
        );
    }
    owner.status = StrategyStatus::Running;
    drain(&mut owner).await;
    assert_eq!(owner.broker.execution.get_orders().await.unwrap().len(), 1);
    let row = owner.insights.get(&child.insight_id).unwrap();
    assert_eq!(row.state, InsightState::Executed);
    assert!(!row.has_pending_submit());
    let journal = owner.command_driver.take().unwrap().stop().await.unwrap();
    assert_eq!(journal.last_admission_order().await.unwrap(), 1);
    assert!(journal.recovery_page(0, 256).await.unwrap().is_empty());
    drop(journal);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn historical_close_settles_recovered_ambiguous_cancel_without_redispatch() {
    use crate::core::broker::commands::{BrokerCommandCompletion, BrokerCommandOutcome};
    use crate::core::broker::types::{OrderLifecycleSnapshot, OrderLifecycleStatus, OrderLifecycleCloseDetails};
    let mut row = insight(1.0, InsightState::Executed);
    row.order_id = Some("filled-and-closed".into());
    row.cancelling = true;
    let command = BrokerCommand::new("strategy", "account", &row.insight_id.to_string(),
        BrokerCommandAction::Cancel { order_id: "filled-and-closed".into() }, None);
    row.pending_mutations.push(PendingInsightMutation {
        kind: MutationKind::Cancel, receipt: command.receipt(), effect: MutationEffect::Cancel,
    });
    let directory = saved_graph(vec![]).await;
    let mut journal = BrokerCommandJournal::open(directory.join("commands.db"), "strategy", "account").await.unwrap();
    journal.persist_execution_batch(&ExecutionCheckpointBatch::with_intents(
        vec![InsightExecutionCheckpoint::capture(&row, 1, true, None)], vec![],
        vec![ExecutionIntent { command: command.clone(), admission_order: 1, queued_at_us: 1 }],
    ).unwrap()).await.unwrap();
    journal.mark_dispatched(&command.id, 2).await.unwrap();
    journal.complete(&BrokerCommandCompletion {
        receipt: command.receipt(), queued_at_us: 1, dispatched_at_us: Some(2), completed_at_us: 3,
        outcome: BrokerCommandOutcome::Ambiguous { reason: "cancel timed out".into() },
    }).await.unwrap();
    journal.request_execution_stop().await.unwrap();
    drop(journal);
    let mut owner = execution_owner();
    start_saved(&mut owner, &directory).await;
    owner.apply_lifecycle_snapshot(OrderLifecycleSnapshot {
        order_id: "filled-and-closed".into(), status: OrderLifecycleStatus::Closed,
        position_id: None, position_qty: None, symbol: Some(row.symbol.clone()), detail: None,
        close_details: Some(OrderLifecycleCloseDetails {
            close_price: 101.0, closed_at: chrono::Utc::now(), realized_pnl: 1.0, commission: 0.0, swap: 0.0,
            position_accounting: None,
        }),
    });
    assert_eq!(owner.insights.get(&row.insight_id).unwrap().state, InsightState::Closed);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while owner.command_driver.as_ref().unwrap().pending_count() != 0 {
            owner.on_trade_update();
            tokio::task::yield_now().await;
        }
    }).await.expect("historical closure must settle the old cancellation");
    let journal = owner.command_driver.take().unwrap().stop().await.unwrap();
    assert!(journal.recovery_page(0, 256).await.unwrap().is_empty());
    drop(journal);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn recovered_terminal_cleanup_owner_survives_pruning_until_receipt_is_applied() {
    use crate::core::broker::commands::{BrokerCommandCompletion, BrokerCommandOutcome};
    for state in [InsightState::Cancelled, InsightState::Closed] {
      for protection in [None, Some(MutationKind::TakeProfit), Some(MutationKind::StopLoss)] {
        let mut row = insight(1.0, state.clone());
        row.order_id = Some("already-terminal".into());
        let (action, kind, effect) = if let Some(kind) = protection {
            let action = if kind == MutationKind::StopLoss {
                BrokerCommandAction::StopLoss { order_id: "already-terminal".into(), price: 90.0, quantity: 1.0 }
            } else {
                BrokerCommandAction::TakeProfit { order_id: "already-terminal".into(), price: 110.0, quantity: 1.0 }
            };
            let effect = if kind == MutationKind::StopLoss {
                MutationEffect::StopLoss(Some(vec![90.0]))
            } else { MutationEffect::TakeProfit(Some(vec![110.0])) };
            (action, kind, effect)
        } else if state == InsightState::Closed {
            (
                BrokerCommandAction::Close {
                    order_id: "already-terminal".into(),
                    quantity: 1.0,
                    price: None,
                },
                MutationKind::Close,
                MutationEffect::Close { price: None },
            )
        } else {
            (
                BrokerCommandAction::Cancel {
                    order_id: "already-terminal".into(),
                },
                MutationKind::Cancel,
                MutationEffect::Cancel,
            )
        };
        let command = BrokerCommand::new(
            "strategy",
            "account",
            &row.insight_id.to_string(),
            action,
            None,
        );
        row.pending_mutations.push(PendingInsightMutation {
            kind,
            receipt: command.receipt(),
            effect,
        });
        let directory = saved_graph(vec![]).await;
        let mut journal =
            BrokerCommandJournal::open(directory.join("commands.db"), "strategy", "account")
                .await
                .unwrap();
        journal
            .persist_execution_batch(
                &ExecutionCheckpointBatch::with_intents(
                    vec![InsightExecutionCheckpoint::capture(&row, 1, true, None)],
                    vec![],
                    vec![ExecutionIntent {
                        command: command.clone(),
                        admission_order: 1,
                        queued_at_us: 1,
                    }],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        journal.mark_dispatched(&command.id, 2).await.unwrap();
        journal
            .complete(&BrokerCommandCompletion {
                receipt: command.receipt(),
                queued_at_us: 1,
                dispatched_at_us: Some(2),
                completed_at_us: 3,
                outcome: BrokerCommandOutcome::Ambiguous {
                    reason: "ticket not found".into(),
                },
            })
            .await
            .unwrap();
        drop(journal);
        let mut owner = execution_owner();
        start_saved(&mut owner, &directory).await;
        // Simulate publication/pruning winning the race with completion replay.
        owner.insights.execution_saved(&row.insight_id);
        owner.insights.prune_terminal_insight(&row.insight_id);
        assert!(
            owner.insights.get(&row.insight_id).is_some(),
            "recovered receipt owner was pruned"
        );
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while owner.command_driver.as_ref().unwrap().pending_count() != 0 {
                owner.on_trade_update();
                owner.insights.prune_terminal_insight(&row.insight_id);
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("terminal recovery must settle without broker redispatch");
        assert!(owner.broker.execution.get_orders().await.unwrap().is_empty());
        if let Some(restored) = owner.insights.get(&row.insight_id) {
            assert!(restored.pending_mutations.is_empty());
            assert_eq!(restored.stop_loss_levels, row.stop_loss_levels);
            assert_eq!(restored.take_profit_levels, row.take_profit_levels);
        }
        let journal = owner.command_driver.take().unwrap().stop().await.unwrap();
        assert!(journal.recovery_page(0, 256).await.unwrap().is_empty());
        drop(journal);
        std::fs::remove_dir_all(directory).unwrap();
      }
    }
}

#[tokio::test]
async fn recovered_rejected_submissions_without_tickets_settle_only_matching_evidence() {
    use crate::core::broker::{
        commands::{BrokerCommandCompletion, BrokerCommandOutcome},
        types::{Order, TimeInForce, TradeUpdateEvent},
    };
    for mismatch in 0..4 {
        let mut owner = execution_owner();
        let directory = saved_graph(vec![]).await;
        let mut journal =
            BrokerCommandJournal::open(directory.join("commands.db"), "strategy", "account")
                .await
                .unwrap();
        // Reproduce the eight retained rejected submits, not a newly issued RPC.
        for index in 0..8 {
            let mut row = insight(1.0, InsightState::Rejected);
            let command = BrokerCommand::new(
                "strategy",
                "account",
                &row.insight_id.to_string(),
                BrokerCommandAction::Submit {
                    request: (&row).into(),
                },
                None,
            );
            row.pending_mutations.push(PendingInsightMutation {
                kind: MutationKind::Submit,
                receipt: command.receipt(),
                effect: MutationEffect::Submit,
            });
            let mut order = Order {
                order_id: row.insight_id.to_string(),
                insight_id: Some(row.insight_id.to_string()),
                strategy_type: None,
                asset: owner.universe["ETHUSD"].clone(),
                qty: 1.0,
                filled_qty: 0.0,
                limit_price: None,
                filled_price: None,
                stop_price: None,
                side: row.side.clone(),
                order_type: row.order_type.clone(),
                time_in_force: TimeInForce::GTC,
                status: TradeUpdateEvent::Rejected,
                order_class: row.order_class.clone(),
                created_at: 1,
                updated_at: 1,
                submitted_at: 1,
                filled_at: None,
                realized_pnl: None,
                position_accounting: None,
                commission: None,
                swap: None,
                rejection_reason: Some("10015".into()),
                legs: None,
            };
            match mismatch {
                1 => order.asset.symbol = "WRONG".into(),
                2 => order.insight_id = Some(Uuid::new_v4().to_string()),
                3 => order.status = TradeUpdateEvent::New,
                _ => {}
            }
            journal
                .persist_execution_batch(
                    &ExecutionCheckpointBatch::with_intents(
                        vec![InsightExecutionCheckpoint::capture(&row, 1, true, None)],
                        vec![],
                        vec![ExecutionIntent {
                            command: command.clone(),
                            admission_order: index + 1,
                            queued_at_us: 1,
                        }],
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
            journal.mark_dispatched(&command.id, 2).await.unwrap();
            journal
                .complete(&BrokerCommandCompletion {
                    receipt: command.receipt(),
                    queued_at_us: 1,
                    dispatched_at_us: Some(2),
                    completed_at_us: 3,
                    outcome: BrokerCommandOutcome::OrderResponse { order },
                })
                .await
                .unwrap();
        }
        drop(journal);
        start_saved(&mut owner, &directory).await;
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                owner.on_trade_update();
                let driver = owner.command_driver.as_ref().unwrap();
                if (mismatch == 0 && driver.pending_count() == 0)
                    || (mismatch != 0 && !driver.has_in_flight_work())
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            owner
                .broker
                .execution
                .get_orders()
                .await
                .unwrap()
                .is_empty(),
            "replayed submissions must not be reissued"
        );
        let journal = owner.command_driver.take().unwrap().stop().await.unwrap();
        assert_eq!(
            journal.recovery_page(0, 256).await.unwrap().len(),
            if mismatch == 0 { 0 } else { 8 }
        );
        drop(journal);
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[test]
fn recovery_entry_gate_checks_direct_fill_dependencies_and_all_ancestors() {
    let mut root = limit(InsightState::Executed);
    root.order_id = Some("entry".into());
    let mut child = limit(InsightState::New);
    child.parent_id = Some(root.insight_id);
    root.children.push(child.clone());
    let mut saved = recovery(vec![
        InsightExecutionCheckpoint::capture(&root, 1, true, None),
        InsightExecutionCheckpoint::capture(&child, 1, true, None),
    ]);
    assert!(!saved.can_dispatch_entry(child.insight_id));
    saved
        .rows
        .get_mut(&child.insight_id)
        .unwrap()
        .is_split_insight = true;
    assert!(saved.can_dispatch_entry(child.insight_id));
    saved
        .rows
        .get_mut(&root.insight_id)
        .unwrap()
        .suppress_child_entries = true;
    assert!(!saved.can_dispatch_entry(child.insight_id));
    saved
        .rows
        .get_mut(&root.insight_id)
        .unwrap()
        .suppress_child_entries = false;
    saved
        .rows
        .get_mut(&child.insight_id)
        .unwrap()
        .is_split_insight = false;
    saved.rows.get_mut(&root.insight_id).unwrap().state = InsightState::Filled;
    assert!(saved.can_dispatch_entry(child.insight_id));
    saved.rows.get_mut(&root.insight_id).unwrap().parent_id = Some(Uuid::new_v4());
    assert!(!saved.can_dispatch_entry(child.insight_id));
}

#[tokio::test]
async fn recovery_work_invalid_queued_children_get_durable_cancellation_without_dispatch() {
    for parent_state in [InsightState::Executed, InsightState::Cancelled] {
        let mut root = limit(parent_state);
        root.order_id = Some("retained-entry".into());
        let mut child = limit(InsightState::New);
        child.parent_id = Some(root.insight_id);
        let command = BrokerCommand::new(
            "strategy",
            "account",
            &child.insight_id.to_string(),
            BrokerCommandAction::Submit {
                request: (&child).into(),
            },
            None,
        );
        child.pending_mutations.push(PendingInsightMutation {
            kind: MutationKind::Submit,
            receipt: command.receipt(),
            effect: MutationEffect::Submit,
        });
        root.children.push(child.clone());
        let directory = saved_graph(vec![InsightExecutionCheckpoint::capture(
            &root, 1, true, None,
        )])
        .await;
        let mut journal =
            BrokerCommandJournal::open(directory.join("commands.db"), "strategy", "account")
                .await
                .unwrap();
        journal
            .persist_execution_batch(
                &ExecutionCheckpointBatch::with_intents(
                    vec![InsightExecutionCheckpoint::capture(&child, 1, true, None)],
                    vec![],
                    vec![ExecutionIntent {
                        command,
                        admission_order: 1,
                        queued_at_us: 1,
                    }],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        drop(journal);
        let mut owner = execution_owner();
        owner.status = StrategyStatus::Running;
        start_saved(&mut owner, &directory).await;
        drain(&mut owner).await;
        assert!(
            owner
                .broker
                .execution
                .get_orders()
                .await
                .unwrap()
                .is_empty()
        );
        let row = owner.insights.get(&child.insight_id).unwrap();
        assert_eq!(row.state, InsightState::Cancelled);
        assert!(!row.has_pending_submit());
        let journal = owner.command_driver.take().unwrap().stop().await.unwrap();
        assert!(journal.recovery_page(0, 256).await.unwrap().is_empty());
        let saved = journal.execution_checkpoint_page("", 32).await.unwrap();
        let child_row = saved
            .rows
            .iter()
            .find(|row| row.insight_id == child.insight_id)
            .unwrap();
        assert_eq!(child_row.state, InsightState::Cancelled);
        assert!(child_row.pending_mutations.is_empty());
        drop(journal);
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[tokio::test]
async fn recovery_work_expired_materialized_child_is_not_retimed_or_retained() {
    let mut root = limit(InsightState::Filled);
    root.order_id = Some("retained-position".into());
    let mut child = limit(InsightState::New);
    child.parent_id = Some(root.insight_id);
    child.period_unfilled = Some(5);
    child.timeframe = TimeFrame::new(1, TimeFrameUnit::Minute);
    let mut nested = limit(InsightState::New);
    nested.parent_id = Some(child.insight_id);
    nested.is_split_insight = true;
    child.children.push(nested.clone());
    root.children.push(child.clone());
    let directory = saved_graph(vec![
        InsightExecutionCheckpoint::capture(&root, 1, true, None),
        InsightExecutionCheckpoint::capture(&child, 1, true, None),
        InsightExecutionCheckpoint::capture(&nested, 1, false, None),
    ])
    .await;
    let mut owner = execution_owner();
    owner.status = StrategyStatus::Running;
    start_saved(&mut owner, &directory).await;
    // Set a deterministic recovery clock beyond the saved (not rebased) TTL.
    let now = child.created_at + chrono::Duration::hours(1);
    owner.insights.get_mut(&child.insight_id).unwrap().context =
        Some(InsightStrategyContext::new(move || now));
    drain(&mut owner).await;
    let row = owner.insights.get(&child.insight_id).unwrap();
    assert_eq!(row.created_at, child.created_at);
    assert_eq!(row.state, InsightState::Rejected);
    assert!(!owner.insights.was_materialized(&nested.insight_id));
    assert!(
        owner
            .broker
            .execution
            .get_orders()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(owner.insight_work.retained_request_count(), 0);
    drop(owner.command_driver.take().unwrap().stop().await.unwrap());
    std::fs::remove_dir_all(directory).unwrap();
}
