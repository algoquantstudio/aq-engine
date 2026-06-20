use crate::core::insight::types::InsightState;
use crate::core::pipeline::WrappedInsightPipe;
use std::{
    collections::{HashMap, VecDeque},
    fmt::Display,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StrategyMode {
    Backtest,
    Live,
}

impl StrategyMode {
    pub fn as_str(self) -> &'static str {
        match self {
            StrategyMode::Backtest => "Backtest",
            StrategyMode::Live => "Live",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrategyStatus {
    Initialised,
    Running,
    Stopping,
    Paused,
    Stopped,
    Completed,
}
impl StrategyStatus {
    pub fn is_initialised(&self) -> bool {
        matches!(self, StrategyStatus::Initialised)
    }

    pub fn is_running(&self) -> bool {
        matches!(self, StrategyStatus::Running)
    }
    pub fn is_paused(&self) -> bool {
        matches!(self, StrategyStatus::Paused)
    }

    pub fn is_finished(&self) -> bool {
        matches!(self, StrategyStatus::Completed | StrategyStatus::Stopped)
    }
}

impl Default for StrategyStatus {
    fn default() -> Self {
        StrategyStatus::Initialised
    }
}

impl Display for StrategyStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StrategyStatus::Initialised => write!(f, "Initialised"),
            StrategyStatus::Running => write!(f, "Running"),
            StrategyStatus::Stopping => write!(f, "Stopping"),
            StrategyStatus::Paused => write!(f, "Paused"),
            StrategyStatus::Stopped => write!(f, "Stopped"),
            StrategyStatus::Completed => write!(f, "Completed"),
        }
    }
}

// ─────────────────────── Insight Pipeline ───────────────────────

/// State-keyed pipeline of `InsightPipe` stages.
///
/// Each `InsightState` maps to an ordered deque of pipes.
/// When an insight is in a given state, its pipes are run in order.
/// Mirrors Python's `INSIGHT_EXECUTORS: dict[InsightState, deque[BaseExecutor]]`.
pub struct InsightPipeline {
    pub pipeline: HashMap<InsightState, VecDeque<WrappedInsightPipe>>,
}

impl InsightPipeline {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a pipe into the bucket for `pipe.target_state()`.
    pub fn add_pipe(&mut self, pipe: WrappedInsightPipe) {
        let state = pipe.target_state.clone();
        self.pipeline
            .entry(state)
            .or_insert_with(VecDeque::new)
            .push_back(pipe);
    }

    /// Register multiple pipes at once.
    pub fn add_pipes(&mut self, pipes: Vec<WrappedInsightPipe>) {
        for pipe in pipes {
            self.add_pipe(pipe);
        }
    }

    /// Get the pipes registered for a given state.
    pub fn get_pipes_for_state(
        &mut self,
        state: &InsightState,
    ) -> Option<&mut VecDeque<WrappedInsightPipe>> {
        self.pipeline.get_mut(state)
    }
}

impl Default for InsightPipeline {
    fn default() -> Self {
        let mut pipeline = HashMap::new();
        pipeline.insert(InsightState::New, VecDeque::new());
        pipeline.insert(InsightState::Executed, VecDeque::new());
        pipeline.insert(InsightState::Closed, VecDeque::new());
        pipeline.insert(InsightState::Filled, VecDeque::new());
        pipeline.insert(InsightState::Cancelled, VecDeque::new());
        pipeline.insert(InsightState::Rejected, VecDeque::new());
        Self { pipeline }
    }
}
