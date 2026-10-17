use crate::core::insight::types::InsightState;
use crate::core::pipeline::WrappedInsightPipe;
use serde_json::Value;
use std::{
    collections::{HashMap, VecDeque},
    fmt::Display,
};

pub(crate) mod live_metrics;

#[derive(Clone, Debug)]
pub(super) struct HyperparameterSelection {
    pub(super) seed: String,
    pub(super) values: Value,
    pub(super) sweep_id: String,
    pub(super) strategy_fingerprint: String,
    pub(super) source_targets: Value,
    pub(super) is_sweep_run: bool,
}

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
    pub(crate) deferred: std::collections::BTreeMap<uuid::Uuid, DeferredPipeResume>,
    pub(super) snapshot_dependencies: super::snapshot_dependencies::SnapshotDependencies,
}

pub(crate) struct DeferredPipeResume {
    pub state: InsightState,
    pub index: usize,
    pub dependency: crate::core::pipeline::InsightPipeDeferral,
    pub snapshot_attempted: Option<crate::core::broker::snapshots::SnapshotReadRevision>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct InsightPipelineCheckpoint {
    pub state: InsightState,
    pub index: usize,
    pub dependency: crate::core::pipeline::InsightPipeDeferral,
    pub layout: Vec<String>,
    pub nested: Option<crate::core::pipeline::PipeContinuationCheckpoint>,
}

impl InsightPipeline {
    pub(crate) fn forget_continuation(&mut self, id: &uuid::Uuid) -> bool {
        self.snapshot_dependencies.remove(id);
        let Some(resume) = self.deferred.remove(id) else {
            return false;
        };
        if let Some(pipe) = self
            .pipeline
            .get_mut(&resume.state)
            .and_then(|pipes| pipes.get_mut(resume.index))
        {
            pipe.inner.forget_insight_continuation(id);
        }
        true
    }

    pub(crate) fn execution_checkpoint(
        &self,
        id: &uuid::Uuid,
    ) -> Option<InsightPipelineCheckpoint> {
        let resume = self.deferred.get(id)?;
        let pipes = self.pipeline.get(&resume.state)?;
        let pipe = pipes.get(resume.index)?;
        Some(InsightPipelineCheckpoint {
            state: resume.state.clone(),
            index: resume.index,
            dependency: resume.dependency.clone(),
            layout: pipes
                .iter()
                .map(WrappedInsightPipe::recovery_layout)
                .collect(),
            nested: pipe.inner.execution_checkpoint(id),
        })
    }

    pub(crate) fn restore_execution_checkpoint(
        &mut self,
        id: uuid::Uuid,
        checkpoint: InsightPipelineCheckpoint,
    ) -> Result<(), String> {
        let pipes = self
            .pipeline
            .get_mut(&checkpoint.state)
            .ok_or("saved pipeline state no longer exists")?;
        let layout = pipes
            .iter()
            .map(WrappedInsightPipe::recovery_layout)
            .collect::<Vec<_>>();
        if layout != checkpoint.layout {
            return Err("pipeline layout changed since execution checkpoint".into());
        }
        let pipe = pipes
            .get_mut(checkpoint.index)
            .ok_or("saved pipeline index is out of range")?;
        pipe.inner
            .restore_execution_checkpoint(id, checkpoint.nested)?;
        self.deferred.insert(
            id,
            DeferredPipeResume {
                state: checkpoint.state,
                index: checkpoint.index,
                dependency: checkpoint.dependency,
                snapshot_attempted: None,
            },
        );
        self.snapshot_dependencies
            .update(id, self.deferred.get(&id).map(|resume| &resume.dependency));
        Ok(())
    }

    pub fn new() -> Self {
        Self::default()
    }

    pub fn pending_deferral(
        &self,
        insight_id: &uuid::Uuid,
    ) -> Option<&crate::core::pipeline::InsightPipeDeferral> {
        self.deferred
            .get(insight_id)
            .map(|resume| &resume.dependency)
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
        Self {
            pipeline,
            deferred: Default::default(),
            snapshot_dependencies: Default::default(),
        }
    }
}
