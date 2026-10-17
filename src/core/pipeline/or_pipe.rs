use crate::core::insight::Insight;
use crate::core::pipeline::InsightPipeResult;
use crate::core::pipeline::WrappedInsightPipe;
use crate::core::strategy::StrategyContext;

use super::InsightPipe;

/// Runs a sequence of child pipes and passes when any executed child pipe passes.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - `pipes`: Non-empty list of wrapped insight pipes to evaluate in order.
///
/// Behaviour:
/// Skips child pipes whose wrapper says they should not run for the current insight. Each
/// remaining child pipe can mutate the insight before the next child pipe executes. The first
/// successful passing child returns `passed=true`; failed children contribute messages, and the
/// pipe returns `passed=false` when no child pipe passes.
pub struct OrPipe {
    pipes: Vec<WrappedInsightPipe>,
    resumed: rustc_hash::FxHashMap<uuid::Uuid, (crate::core::insight::types::InsightState, usize)>,
}

impl OrPipe {
    pub fn new(pipes: Vec<WrappedInsightPipe>) -> Self {
        assert!(!pipes.is_empty(), "OrPipe requires at least one sub-pipe");
        Self {
            pipes,
            resumed: Default::default(),
        }
    }
}

impl InsightPipe for OrPipe {
    fn version(&self) -> &str {
        "1.0"
    }

    fn run(&mut self, ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        let mut errors = Vec::new();
        let id = insight.insight_id;
        let state = insight.state.clone();
        let start = self
            .resumed
            .remove(&id)
            .filter(|(previous, _)| *previous == state)
            .map_or(0, |(_, index)| index);

        for (index, pipe) in self.pipes.iter_mut().enumerate().skip(start) {
            if !pipe.should_run(insight) {
                continue;
            }
            let result = pipe.run(ctx, insight);
            if result.deferred.is_some() {
                // Another alternative may issue a conflicting mutation. Wait for
                // this child's outcome rather than treating pending as false.
                self.resumed.insert(id, (state, index));
                return result;
            }
            if insight.state.is_inactive() || insight.closing || insight.cancelling {
                // A mutating rejection/exit is not a predicate that another branch
                // may override. Stop before an alternative can submit or exit again.
                return result;
            }
            if !result.success {
                if let Some(msg) = &result.message {
                    errors.push(msg.clone());
                }
                continue;
            }
            if result.passed {
                return InsightPipeResult::new(
                    true,
                    true,
                    Some(format!("Pipe {} passed: {:?}", pipe.name(), result.message)),
                    self.name().to_string(),
                );
            }
        }

        InsightPipeResult::new(
            false,
            true,
            Some(format!("No pipe passed. Errors: {:?}", errors)),
            self.name().to_string(),
        )
    }

    fn retain_active_insights(&mut self, active: &rustc_hash::FxHashSet<uuid::Uuid>) {
        self.resumed.retain(|id, _| active.contains(id));
        for pipe in &mut self.pipes {
            pipe.retain_active_insights(active);
        }
    }

    fn execution_checkpoint(&self, id: &uuid::Uuid) -> Option<super::PipeContinuationCheckpoint> {
        let (state, index) = self.resumed.get(id)?;
        Some(super::PipeContinuationCheckpoint {
            state: state.clone(),
            index: *index,
            nested: self
                .pipes
                .get(*index)?
                .inner
                .execution_checkpoint(id)
                .map(Box::new),
        })
    }

    fn forget_insight_continuation(&mut self, id: &uuid::Uuid) {
        if let Some((_, index)) = self.resumed.remove(id) {
            if let Some(pipe) = self.pipes.get_mut(index) {
                pipe.inner.forget_insight_continuation(id);
            }
        }
    }

    fn restore_execution_checkpoint(
        &mut self,
        id: uuid::Uuid,
        checkpoint: Option<super::PipeContinuationCheckpoint>,
    ) -> Result<(), String> {
        let Some(checkpoint) = checkpoint else {
            self.resumed.remove(&id);
            return Ok(());
        };
        let pipe = self
            .pipes
            .get_mut(checkpoint.index)
            .ok_or("saved composite pipe index is out of range")?;
        pipe.inner
            .restore_execution_checkpoint(id, checkpoint.nested.map(|nested| *nested))?;
        self.resumed
            .insert(id, (checkpoint.state, checkpoint.index));
        Ok(())
    }

    fn recovery_layout(&self) -> String {
        serde_json::to_string(&(
            self.name(),
            self.version(),
            self.pipes
                .iter()
                .map(WrappedInsightPipe::recovery_layout)
                .collect::<Vec<_>>(),
        ))
        .expect("composite layout contains only strings")
    }
}
