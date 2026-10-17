use super::*;
use crate::core::broker::data_feeds::yahoo::YahooFinanceDataFeed;
use crate::core::broker::types::AccountType;
use crate::core::pipeline::{InsightPipe, InsightPipeDeferral, InsightPipeResult};
use crate::core::utils::timeframe::TimeFrameUnit;
use std::sync::{Arc, Mutex};

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
type Seen = Arc<Mutex<Vec<Uuid>>>;

struct Probe {
    seen: Seen,
    forgotten: Seen,
    stop: bool,
    defer: bool,
}
impl InsightPipe for Probe {
    fn version(&self) -> &str {
        "test"
    }
    fn run(&mut self, ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        self.seen.lock().unwrap().push(insight.insight_id);
        if self.stop {
            ctx.shutdown();
        }
        if self.defer {
            InsightPipeResult::deferred(InsightPipeDeferral::AccountSnapshot, self.name())
        } else {
            InsightPipeResult::new(true, true, None, self.name().into())
        }
    }
    fn forget_insight_continuation(&mut self, id: &Uuid) {
        self.forgotten.lock().unwrap().push(*id);
    }
}

fn row(id: u128) -> Insight {
    Insight {
        insight_id: Uuid::from_u128(id),
        symbol: "ETHUSD".into(),
        ..Default::default()
    }
}

fn owner(count: u128, stop: bool, defer: bool) -> (Owner, Seen, Seen) {
    let mut owner = StrategyState::new(
        "sweep".into(),
        "test".into(),
        Noop,
        UnifiedBroker::new(
            PaperBroker::new(AccountType::Paper, 100_000.0, 1),
            YahooFinanceDataFeed::new(),
        ),
        StrategyMode::Live,
        TimeFrame::new(1, TimeFrameUnit::Minute),
    );
    owner.status = StrategyStatus::Running;
    let seen = Seen::default();
    let forgotten = Seen::default();
    owner.add_pipe(
        WrappedInsightPipe::builder(Box::new(Probe {
            seen: seen.clone(),
            forgotten: forgotten.clone(),
            stop,
            defer,
        }))
        .build(),
    );
    for id in 1..=count {
        owner.insights.add_insight(row(id));
    }
    (owner, seen, forgotten)
}

fn drain(owner: &mut Owner, sweep: &mut LivePipelineSweep) {
    for _ in 0..10_000 {
        if !sweep.is_pending() {
            return;
        }
        assert!(owner.advance_live_pipeline_sweep(sweep) <= 256);
    }
    panic!("sweep did not converge");
}

#[test]
fn pipeline_sweep_thousand_rows_are_bounded_and_coalesced_without_replay() {
    let (mut owner, seen, _) = owner(1_000, false, false);
    let mut sweep = LivePipelineSweep::default();
    sweep.request(&owner.insights, &owner.insight_pipeline);
    let first = owner.advance_live_pipeline_sweep(&mut sweep);
    assert!(first > 0 && first <= 256);
    assert_eq!(seen.lock().unwrap().len(), first);
    assert!(sweep.is_pending());
    // A timer during a long pass cannot reset its cursor and starve the tail.
    sweep.request(&owner.insights, &owner.insight_pipeline);
    drain(&mut owner, &mut sweep);
    assert_eq!(
        *seen.lock().unwrap(),
        (1..=1_000).map(Uuid::from_u128).collect::<Vec<_>>()
    );
}

#[test]
fn pipeline_sweep_time_budget_yields_after_one_atomic_insight_and_resumes() {
    let (mut owner, seen, _) = owner(10, false, false);
    let mut sweep = LivePipelineSweep::default();
    sweep.request(&owner.insights, &owner.insight_pipeline);
    for expected in 1..=10 {
        assert_eq!(
            owner.advance_live_pipeline_sweep_with_budget(&mut sweep, Duration::ZERO),
            1
        );
        assert_eq!(seen.lock().unwrap().len(), expected);
    }
    assert!(!sweep.is_pending());
}

#[test]
fn pipeline_sweep_stop_inside_pipe_prevents_the_next_insight() {
    let (mut owner, seen, _) = owner(1_000, true, false);
    let mut sweep = LivePipelineSweep::default();
    sweep.request(&owner.insights, &owner.insight_pipeline);
    assert_eq!(owner.advance_live_pipeline_sweep(&mut sweep), 1);
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert!(!sweep.is_pending());
    sweep.request(&owner.insights, &owner.insight_pipeline);
    assert_eq!(owner.advance_live_pipeline_sweep(&mut sweep), 0);
}

#[test]
fn pipeline_sweep_mutations_between_pages_are_safe_and_new_rows_are_eventually_seen() {
    let (mut owner, seen, _) = owner(1_000, false, false);
    let mut sweep = LivePipelineSweep::default();
    sweep.request(&owner.insights, &owner.insight_pipeline);
    owner.advance_live_pipeline_sweep_with_budget(&mut sweep, Duration::ZERO);
    owner.insights.remove_insight(&Uuid::from_u128(300));
    owner.insights.get_mut(&Uuid::from_u128(400)).unwrap().state = InsightState::Cancelled;
    owner
        .insights
        .refresh_runtime_tracking(&Uuid::from_u128(400));
    owner.insights.add_insight(row(0));
    owner.insights.add_insight(row(1_001));
    drain(&mut owner, &mut sweep);
    let expected: Vec<_> = (1..=1_000)
        .filter(|id| ![300, 400].contains(id))
        .map(Uuid::from_u128)
        .collect();
    assert_eq!(*seen.lock().unwrap(), expected);
    seen.lock().unwrap().clear();
    sweep.request(&owner.insights, &owner.insight_pipeline);
    drain(&mut owner, &mut sweep);
    assert_eq!(
        *seen.lock().unwrap(),
        (0..=1_001)
            .filter(|id| ![300, 400].contains(id))
            .map(Uuid::from_u128)
            .collect::<Vec<_>>()
    );
}

#[test]
fn pipeline_sweep_stale_continuations_are_released_in_bounded_pages() {
    let (mut owner, seen, forgotten) = owner(700, false, true);
    owner.run_insight_pipeline();
    assert_eq!(owner.insight_pipeline.deferred.len(), 700);
    for id in 1..=700 {
        owner.insights.remove_insight(&Uuid::from_u128(id));
    }
    let mut sweep = LivePipelineSweep::default();
    sweep.request(&owner.insights, &owner.insight_pipeline);
    let first = owner.advance_live_pipeline_sweep(&mut sweep);
    assert!(first > 0 && first <= 256);
    assert_eq!(forgotten.lock().unwrap().len(), first);
    drain(&mut owner, &mut sweep);
    assert_eq!(forgotten.lock().unwrap().len(), 700);
    assert_eq!(seen.lock().unwrap().len(), 700); // no replay to release state
    assert!(owner.insight_pipeline.deferred.is_empty());
}

#[test]
fn pipeline_sweep_state_change_releases_nested_continuation_before_next_state() {
    let (mut owner, seen, forgotten) = owner(1, false, true);
    let inner = owner
        .insight_pipeline
        .pipeline
        .get_mut(&InsightState::New)
        .unwrap()
        .pop_front()
        .unwrap();
    owner.add_pipe(
        WrappedInsightPipe::builder(Box::new(crate::core::pipeline::and_pipe::AndPipe::new(
            vec![inner],
        )))
        .build(),
    );
    owner.run_insight_pipeline();
    assert_eq!(seen.lock().unwrap().len(), 1);
    owner.insights.get_mut(&Uuid::from_u128(1)).unwrap().state = InsightState::Filled;
    owner.insights.refresh_runtime_tracking(&Uuid::from_u128(1));
    let mut sweep = LivePipelineSweep::default();
    sweep.request(&owner.insights, &owner.insight_pipeline);
    drain(&mut owner, &mut sweep);
    assert_eq!(*forgotten.lock().unwrap(), vec![Uuid::from_u128(1)]);
    assert!(owner.insight_pipeline.deferred.is_empty());
}

#[test]
fn pipeline_sweep_empty_and_removed_upper_bound_finish_without_spinning() {
    let (mut owner, seen, _) = owner(0, false, false);
    let mut sweep = LivePipelineSweep::default();
    sweep.request(&owner.insights, &owner.insight_pipeline);
    assert!(!sweep.is_pending());
    owner.insights.add_insight(row(1));
    sweep.request(&owner.insights, &owner.insight_pipeline);
    owner.insights.remove_insight(&Uuid::from_u128(1));
    assert_eq!(owner.advance_live_pipeline_sweep(&mut sweep), 0);
    assert!(!sweep.is_pending());
    assert!(seen.lock().unwrap().is_empty());
}
