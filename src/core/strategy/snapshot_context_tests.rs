//! In-process only: no live broker, database, or network data-feed calls.
use super::*;
use crate::core::{
    broker::{
        data_feeds::yahoo::YahooFinanceDataFeed,
        snapshots::{SnapshotKind, SnapshotUnavailable},
        types::{AccountType, Bar, OrderSide},
    },
    pipeline::{
        InsightPipe, InsightPipeDeferral, InsightPipeResult,
        dynamic_quantity_to_risk::DynamicQuantityToRiskPipe,
        full_account_quantity_to_risk::FullAccountQuantityToRiskPipe,
        minimum_risk_to_reward::MinimumRiskToRewardPipe,
        percentage_dca_levels::PercentageDcaLevelsPipe, scale_out::ScaleOutPipe,
    },
    utils::timeframe::TimeFrameUnit,
};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
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
fn owner(mode: StrategyMode) -> Owner {
    StrategyState::new(
        "snapshot-test".into(),
        "1".into(),
        Noop,
        UnifiedBroker::new(
            PaperBroker::new(AccountType::Paper, 100_000.0, 1),
            YahooFinanceDataFeed::new(),
        ),
        mode,
        TimeFrame::new(1, TimeFrameUnit::Minute),
    )
}
fn quote_missing(error: BrokerError) {
    assert!(matches!(
        error,
        BrokerError::SnapshotUnavailable {
            kind: SnapshotKind::Quote { .. },
            ..
        }
    ));
}
fn row() -> Insight {
    Insight {
        symbol: "ETHUSD".into(),
        quantity: Some(1.0),
        ..Default::default()
    }
}

#[test]
fn builtin_cancel_opposite_queues_only_pending_same_symbol_entries() {
    let mut state = owner(StrategyMode::Backtest);
    state.status = StrategyStatus::Running;
    let mut pending = row();
    pending.side = OrderSide::Sell;
    pending.period_unfilled = None;
    let pending_id = state.insights.add_insight(pending);
    let mut filled = row();
    filled.side = OrderSide::Sell;
    filled.state = InsightState::Filled;
    let filled_id = state.insights.add_insight(filled);
    let mut unrelated = row();
    unrelated.side = OrderSide::Sell;
    unrelated.symbol = "BTCUSD".into();
    let unrelated_id = state.insights.add_insight(unrelated);
    let mut trigger = row();
    trigger.side = OrderSide::Buy;
    trigger.state = InsightState::Filled;
    let mut pipe = crate::core::pipeline::cancel_opposite::CancelOppositePipe::new();
    assert!(pipe.run(&mut state, &mut trigger).passed);
    let count = state.insight_work.retained_request_count();
    assert!(pipe.run(&mut state, &mut trigger).passed);
    assert_eq!(state.insight_work.retained_request_count(), count);
    assert!(state.insights.get(&pending_id).unwrap().suppress_child_entries);
    assert!(!state.insights.get(&filled_id).unwrap().suppress_child_entries);
    state.process_insight_work(16, Duration::from_secs(1));
    assert_eq!(state.insights.get(&pending_id).unwrap().state, InsightState::Cancelled);
    assert_eq!(state.insights.get(&filled_id).unwrap().state, InsightState::Filled);
    assert_eq!(state.insights.get(&unrelated_id).unwrap().state, InsightState::New);
}

#[test]
fn snapshot_context_missing_live_data_never_falls_back_to_transport() {
    let state = owner(StrategyMode::Live);
    assert!(matches!(
        state.account(),
        Err(BrokerError::SnapshotUnavailable {
            kind: SnapshotKind::Account,
            ..
        })
    ));
    assert!(state.cached_account().is_none());
    quote_missing(state.latest_quote("ETHUSD").unwrap_err());
    assert!(matches!(
        state.tools().get_all_unrealized_pnl(),
        Err(BrokerError::SnapshotUnavailable {
            kind: SnapshotKind::Positions,
            ..
        })
    ));
    assert!(matches!(
        state.broker.get_positions_sync(),
        Err(BrokerError::SnapshotUnavailable { .. })
    ));
}

#[test]
fn snapshot_context_builtin_quote_dependencies_defer_without_mutating_insights() {
    let mut state = owner(StrategyMode::Live);
    let mut pipes: Vec<Box<dyn InsightPipe>> = vec![
        Box::new(FullAccountQuantityToRiskPipe::new()),
        Box::new(DynamicQuantityToRiskPipe::new(100_000.0, 0.0)),
        Box::new(MinimumRiskToRewardPipe::new(Some(2.0))),
        Box::new(PercentageDcaLevelsPipe::new(0.5, 3)),
    ];
    for pipe in &mut pipes {
        let mut insight = row();
        let before = insight.snapshot_fingerprint_hash();
        let result = pipe.run(&mut state, &mut insight);
        assert_eq!(
            result.deferred,
            Some(InsightPipeDeferral::QuoteSnapshot {
                symbol: "ETHUSD".into()
            }),
            "{}",
            pipe.name()
        );
        assert!(result.success && !result.passed);
        assert_eq!(insight.snapshot_fingerprint_hash(), before);
    }
    let mut insight = row();
    insight.state = InsightState::Filled;
    insight.filled_price = Some(100.0);
    let result = ScaleOutPipe::new(0.5).run(&mut state, &mut insight);
    assert_eq!(
        result.deferred,
        Some(InsightPipeDeferral::QuoteSnapshot {
            symbol: "ETHUSD".into()
        })
    );
    assert!(!insight.closing);
}

#[test]
fn snapshot_context_backtest_has_account_before_start_and_never_uses_wall_clock_quotes() {
    let state = owner(StrategyMode::Backtest);
    assert_eq!(state.account().unwrap().cash, 100_000.0);
    assert_eq!(state.tools().get_all_unrealized_pnl().unwrap(), 0.0);
    quote_missing(state.latest_quote("ETHUSD").unwrap_err());
    let timestamp = chrono::DateTime::from_timestamp(1_500_000_000, 0).unwrap();
    {
        let mut history = state.broker.backtest_state.as_ref().unwrap().write();
        history.historical_bars.insert(
            "ETHUSD".into(),
            vec![Bar {
                symbol: "ETHUSD".into(),
                open: 99.0,
                high: 101.0,
                low: 98.0,
                close: 100.0,
                volume: 2.0,
                timestamp,
            }],
        );
        history.bar_indices.insert("ETHUSD".into(), 0);
        history.current_time = timestamp;
    }
    let quote = state.latest_quote("ETHUSD").unwrap();
    assert_eq!(quote.timestamp, timestamp);
    assert_eq!(quote.ask, 100.0);
    assert_eq!(state.current_time(), timestamp);
    quote_missing(state.latest_quote("SOLUSD").unwrap_err());
}

async fn start_reads(state: &Owner) {
    let observed = Instant::now();
    let account = state.broker.get_account().await.unwrap();
    state
        .broker
        .start_snapshot_reads(
            account,
            observed,
            &[],
            Default::default(),
            Default::default(),
        )
        .unwrap();
}

#[tokio::test]
async fn snapshot_context_startup_rollback_releases_refresh_before_returning_owner() {
    use crate::core::broker::command_journal::{BrokerCommandJournal, lifecycle::ExecutionPhase};
    let directory = std::env::temp_dir().join(format!("aqe-startup-rollback-{}", Uuid::new_v4()));
    let mut state = owner(StrategyMode::Live).with_artifact_root(&directory);
    start_reads(&state).await;
    let account = state.broker.account_snapshot().unwrap();
    let strategy_id = state.strategy_id.to_string();
    let path = state.live_execution_journal_path(&strategy_id, &account.account_id);
    state.start_live_execution(None).await.unwrap();
    assert!(
        state
            .broker
            .start_connection_worker(Default::default())
            .is_err()
    );
    let strategy = state.strategy.take().unwrap();
    state
        .rollback_failed_live_startup(strategy, "snapshot regression fixture")
        .await;
    assert!(state.strategy.is_some());
    assert!(matches!(state.status, StrategyStatus::Stopped));
    assert!(state.broker.snapshot_refresh_health().is_none());
    assert!(state.command_driver.is_none());
    assert!(state.broker.connection_worker_health().is_none());
    assert!(state.account().is_err());
    assert_eq!(Arc::strong_count(&state.broker.execution), 1);
    assert_eq!(Arc::strong_count(&state.broker.data), 1);
    let journal = BrokerCommandJournal::open(&path, &strategy_id, &account.account_id)
        .await
        .unwrap();
    assert_eq!(
        journal.execution_lifecycle().phase,
        ExecutionPhase::Stopping
    );
    drop(journal);
    drop(state);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn snapshot_context_live_start_invalidation_and_stop_own_refresh_lifetime() {
    let state = owner(StrategyMode::Live);
    start_reads(&state).await;
    assert_eq!(state.account().unwrap().cash, 100_000.0);
    let first = state.broker.account_snapshot().unwrap();
    assert!(Arc::ptr_eq(
        &first,
        &state.broker.account_snapshot().unwrap()
    ));
    let account = state.broker.get_account().await.unwrap();
    assert!(
        state
            .broker
            .start_snapshot_reads(
                account,
                Instant::now(),
                &[],
                Default::default(),
                Default::default()
            )
            .is_err()
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.broker.positions_snapshot().is_err() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(state.tools().get_all_unrealized_pnl().unwrap(), 0.0);
    let revision = state.broker.snapshot_revision();
    state.broker.invalidate_snapshot_reads();
    assert!(state.broker.snapshot_revision() > revision);
    assert!(state.account().is_err());
    state.broker.stop_snapshot_reads().await;
    assert!(state.account().is_err());
    assert!(state.broker.snapshot_refresh_health().is_none());
    assert_eq!(Arc::strong_count(&state.broker.execution), 1);
    assert_eq!(Arc::strong_count(&state.broker.data), 1);
}

struct AccountPipe {
    attempts: Arc<AtomicUsize>,
    keep_waiting: bool,
}

struct DeferredOnceStage(Arc<AtomicUsize>);
impl InsightPipe for DeferredOnceStage {
    fn version(&self) -> &str {
        "test"
    }
    fn run(&mut self, _: &mut dyn StrategyContext, _: &mut Insight) -> InsightPipeResult {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            InsightPipeResult::deferred(InsightPipeDeferral::AccountSnapshot, self.name())
        } else {
            InsightPipeResult::new(true, true, None, self.name().into())
        }
    }
}

#[tokio::test]
async fn snapshot_context_next_nested_stage_can_use_the_same_fresh_revision() {
    let mut state = owner(StrategyMode::Live);
    let first = Arc::new(AtomicUsize::new(0));
    let second = Arc::new(AtomicUsize::new(0));
    state.add_pipe(
        WrappedInsightPipe::builder(Box::new(crate::core::pipeline::and_pipe::AndPipe::new(
            vec![
                WrappedInsightPipe::builder(Box::new(DeferredOnceStage(first.clone()))).build(),
                WrappedInsightPipe::builder(Box::new(DeferredOnceStage(second.clone()))).build(),
            ],
        )))
        .build(),
    );
    state.insights.add_insight(row());
    state.run_insight_pipeline();
    assert_eq!(first.load(Ordering::SeqCst), 1);
    assert_eq!(second.load(Ordering::SeqCst), 0);
    start_reads(&state).await;
    for _ in 0..100 {
        if !state.resume_snapshot_pipes() {
            break;
        }
    }
    assert_eq!(first.load(Ordering::SeqCst), 2); // completed stage never replayed
    assert_eq!(second.load(Ordering::SeqCst), 2);
    assert!(state.insight_pipeline.deferred.is_empty());
    assert!(
        state
            .insight_pipeline
            .snapshot_dependencies
            .topics()
            .is_empty()
    );
    state.broker.stop_snapshot_reads().await;
}

struct PerDependencyProbe {
    attempts: Arc<std::sync::Mutex<HashMap<String, usize>>>,
}
impl InsightPipe for PerDependencyProbe {
    fn version(&self) -> &str {
        "test"
    }
    fn run(&mut self, _: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        *self
            .attempts
            .lock()
            .unwrap()
            .entry(insight.symbol.clone())
            .or_default() += 1;
        let dependency = match insight.symbol.as_str() {
            "account" => InsightPipeDeferral::AccountSnapshot,
            "positions" => InsightPipeDeferral::PositionSnapshot,
            symbol => InsightPipeDeferral::QuoteSnapshot {
                symbol: symbol.into(),
            },
        };
        InsightPipeResult::deferred(dependency, self.name())
    }
}

#[tokio::test]
async fn snapshot_context_unrelated_refresh_does_not_retry_other_dependency_owners() {
    // Current-thread runtime: after installing the empty-universe workers, all
    // operations below are synchronous until stop aborts them. The fixture feed
    // is never polled; only explicitly seeded in-memory quotes are used.
    let mut state = owner(StrategyMode::Live);
    let attempts = Arc::new(std::sync::Mutex::new(HashMap::new()));
    state.add_pipe(
        WrappedInsightPipe::builder(Box::new(PerDependencyProbe {
            attempts: attempts.clone(),
        }))
        .build(),
    );
    for symbol in ["account", "positions", "ETHUSD", "SOLUSD"] {
        state.insights.add_insight(Insight {
            symbol: symbol.into(),
            ..Default::default()
        });
    }
    state.run_insight_pipeline();
    start_reads(&state).await;
    let cache = state.broker.live_snapshot_cache().unwrap();
    cache
        .set_symbols(&["ETHUSD".into(), "SOLUSD".into()])
        .unwrap();
    let quote = |symbol: &str| crate::core::broker::types::Quote {
        symbol: symbol.into(),
        bid: 100.0,
        ask: 101.0,
        bid_size: 1.0,
        ask_size: 1.0,
        last: Some(100.5),
        last_size: Some(1.0),
        timestamp: chrono::Utc::now(),
    };
    cache.seed_quote_for_test(quote("ETHUSD"));
    cache.seed_quote_for_test(quote("SOLUSD"));
    cache.seed_positions_for_test(vec![]);
    let drain = |state: &mut Owner| {
        for _ in 0..100 {
            if !state.resume_snapshot_pipes() {
                return;
            }
        }
        panic!("unchanged snapshot work did not settle");
    };
    drain(&mut state);
    let counts = || {
        let attempts = attempts.lock().unwrap();
        ["account", "positions", "ETHUSD", "SOLUSD"].map(|symbol| attempts[symbol])
    };
    assert_eq!(counts(), [2, 2, 2, 2]);
    drain(&mut state);
    assert_eq!(counts(), [2, 2, 2, 2]);
    cache.seed_quote_for_test(quote("ETHUSD"));
    drain(&mut state);
    assert_eq!(counts(), [2, 2, 3, 2]);
    cache.seed_positions_for_test(vec![]);
    drain(&mut state);
    assert_eq!(counts(), [2, 3, 3, 2]);
    cache.seed_account_for_test((*cache.account().unwrap()).clone(), Instant::now());
    drain(&mut state);
    assert_eq!(counts(), [3, 4, 3, 2]);
    state.broker.stop_snapshot_reads().await;
}

#[tokio::test]
async fn snapshot_context_thousand_waiters_resume_in_bounded_pages_without_replaying_finished_rows()
{
    let mut state = owner(StrategyMode::Live);
    let attempts = Arc::new(AtomicUsize::new(0));
    state.add_pipe(
        WrappedInsightPipe::builder(Box::new(AccountPipe {
            attempts: attempts.clone(),
            keep_waiting: false,
        }))
        .build(),
    );
    for _ in 0..1_000 {
        state.insights.add_insight(row());
    }
    state.run_insight_pipeline();
    assert_eq!(attempts.load(Ordering::SeqCst), 1_000);
    assert_eq!(state.insight_pipeline.deferred.len(), 1_000);
    start_reads(&state).await;
    for _ in 0..1_001 {
        let before = attempts.load(Ordering::SeqCst);
        let more = state.resume_snapshot_pipes();
        let processed = attempts.load(Ordering::SeqCst) - before;
        assert!(processed <= 256);
        if !more {
            break;
        }
        assert!(processed > 0);
    }
    assert!(state.insight_pipeline.deferred.is_empty());
    assert_eq!(attempts.load(Ordering::SeqCst), 2_000);
    assert!(!state.resume_snapshot_pipes());
    assert_eq!(attempts.load(Ordering::SeqCst), 2_000);
    assert!(
        state
            .insight_pipeline
            .snapshot_dependencies
            .topics()
            .is_empty()
    );
    state.broker.stop_snapshot_reads().await;
}

#[test]
fn snapshot_context_sizing_account_dependencies_do_not_reject_or_allocate_children() {
    use crate::core::{
        broker::types::{AssetExchange, AssetStatus, AssetType},
        pipeline::percentage_risk_to_quantity::PercentageRiskToQuantityPipe,
    };
    let mut state = owner(StrategyMode::Live);
    state.universe.insert(
        "ETHUSD".into(),
        Asset {
            id: "ETHUSD".into(),
            symbol: "ETHUSD".into(),
            name: "ETHUSD".into(),
            asset_type: AssetType::Stock,
            status: AssetStatus::Active,
            exchange: AssetExchange::NASDAQ,
            tradable: true,
            marginable: true,
            shortable: true,
            fractional: true,
            min_order_size: Some(0.01),
            max_order_size: Some(10.0),
            max_total_size: None,
            quantity_base: Some(2),
            min_price_increment: Some(0.01),
            price_base: Some(2),
            contract_size: Some(1),
            trading_calendar: None,
            fees: Default::default(),
        },
    );
    let mut pipes: Vec<Box<dyn InsightPipe>> = vec![
        Box::new(PercentageRiskToQuantityPipe::new(2)),
        Box::new(FullAccountQuantityToRiskPipe::new()),
        Box::new(DynamicQuantityToRiskPipe::new(100_000.0, 0.0)),
    ];
    for pipe in &mut pipes {
        let mut insight = row();
        insight.quantity = None;
        insight.limit_price = Some(100.0);
        insight.set_stop_loss(Some(90.0));
        let before = insight.snapshot_fingerprint_hash();
        let result = pipe.run(&mut state, &mut insight);
        assert_eq!(
            result.deferred,
            Some(InsightPipeDeferral::AccountSnapshot),
            "{}",
            pipe.name()
        );
        assert_eq!(before, insight.snapshot_fingerprint_hash());
        assert!(insight.children.is_empty());
    }
}

#[tokio::test]
async fn snapshot_context_backtest_missing_quote_defers_to_market_clock_instead_of_failing_drain() {
    let mut state = owner(StrategyMode::Backtest);
    state.add_pipe(
        WrappedInsightPipe::builder(Box::new(FullAccountQuantityToRiskPipe::new())).build(),
    );
    let id = state.insights.add_insight(row());
    state.run_insight_pipeline();
    state.drain_backtest_execution().await.unwrap();
    assert_eq!(
        state.insight_pipeline.deferred[&id].dependency,
        InsightPipeDeferral::QuoteSnapshot {
            symbol: "ETHUSD".into()
        }
    );
    assert_eq!(state.insights.get(&id).unwrap().state, InsightState::New);
}
impl InsightPipe for AccountPipe {
    fn version(&self) -> &str {
        "1"
    }
    fn run(&mut self, ctx: &mut dyn StrategyContext, _: &mut Insight) -> InsightPipeResult {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        match ctx.account() {
            Ok(_) if !self.keep_waiting => {
                InsightPipeResult::new(true, true, None, self.name().into())
            }
            Ok(_) => InsightPipeResult::deferred(InsightPipeDeferral::AccountSnapshot, self.name()),
            Err(error) => InsightPipeResult::from_read_error(error, self.name()),
        }
    }
}

#[tokio::test]
async fn snapshot_context_notification_resumes_deferred_pipe_only_once_per_revision() {
    let mut state = owner(StrategyMode::Live);
    let attempts = Arc::new(AtomicUsize::new(0));
    state.add_pipe(
        WrappedInsightPipe::builder(Box::new(AccountPipe {
            attempts: attempts.clone(),
            keep_waiting: true,
        }))
        .build(),
    );
    let id = state.insights.add_insight(row());
    state.run_insight_pipeline();
    assert_eq!(
        state.insight_pipeline.deferred[&id].dependency,
        InsightPipeDeferral::AccountSnapshot
    );
    assert!(!state.resume_snapshot_pipes());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    start_reads(&state).await;
    assert!(!state.resume_snapshot_pipes());
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert!(!state.resume_snapshot_pipes());
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    state.broker.stop_snapshot_reads().await;
}

#[test]
fn snapshot_context_read_error_preserves_dependency_and_real_errors_stay_errors() {
    for (kind, dependency) in [
        (SnapshotKind::Account, InsightPipeDeferral::AccountSnapshot),
        (
            SnapshotKind::Positions,
            InsightPipeDeferral::PositionSnapshot,
        ),
        (
            SnapshotKind::Quote {
                symbol: "ETHUSD".into(),
            },
            InsightPipeDeferral::QuoteSnapshot {
                symbol: "ETHUSD".into(),
            },
        ),
    ] {
        let error = BrokerError::SnapshotUnavailable {
            kind,
            reason: SnapshotUnavailable::Stale,
        };
        let wire = serde_json::to_string(&error).unwrap();
        let decoded = serde_json::from_str(&wire).unwrap();
        assert_eq!(
            InsightPipeResult::from_read_error(decoded, "test").deferred,
            Some(dependency)
        );
    }
    let invalid = InsightPipeResult::from_read_error(
        BrokerError::DataFeedError("invalid input".into()),
        "test",
    );
    assert!(invalid.deferred.is_none() && !invalid.success);
}
