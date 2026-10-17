//! Startup-only reconstruction of the durable execution graph. Broker dispatch
//! stays gated until this graph and all suspended pipeline cursors are installed.
use super::*;
use crate::core::broker::{
    command_journal::recovery::ExecutionRecovery, commands::BrokerCommandError,
};
use std::collections::{BTreeSet, VecDeque};

struct RecoveredInsights {
    collection: InsightCollection,
    pipeline: Vec<(Uuid, InsightPipelineCheckpoint)>,
    work: VecDeque<Uuid>,
}

fn rebuild_insights(
    recovery: ExecutionRecovery,
    context: InsightStrategyContext,
) -> Result<RecoveredInsights, BrokerCommandError> {
    let invalid = |reason: &str| BrokerCommandError::Journal(reason.into());
    let mut remaining = recovery.rows;
    let mut parents = HashMap::with_capacity(remaining.len());
    let mut counts = HashMap::with_capacity(remaining.len());
    let mut ready = BTreeSet::new();
    for row in remaining.values() {
        row.validate()?;
        if row.state.is_active()
            && row
                .execution_data
                .recovery_dependencies()?
                .iter()
                .any(|owner| !remaining.contains_key(owner))
        {
            return Err(invalid("missing explicit execution recovery owner"));
        }
        if !row.materialized && row.pipeline.is_some() {
            return Err(invalid(
                "dormant insight cannot own a pipeline continuation",
            ));
        }
        if row.materialized
            && row
                .parent_id
                .and_then(|id| remaining.get(&id))
                .is_some_and(|parent| !parent.materialized)
        {
            return Err(invalid("materialized insight has a dormant parent"));
        }
        for child_id in &row.child_ids {
            let child = remaining
                .get(child_id)
                .ok_or_else(|| invalid("missing child during graph reconstruction"))?;
            if child.parent_id != Some(row.insight_id)
                || parents.insert(*child_id, row.insight_id).is_some()
            {
                return Err(invalid(
                    "conflicting child ownership during graph reconstruction",
                ));
            }
        }
        counts.insert(row.insight_id, row.child_ids.len());
        if row.child_ids.is_empty() {
            ready.insert(row.insight_id);
        }
    }
    let mut collection = InsightCollection::new();
    let mut assembled = HashMap::with_capacity(remaining.len());
    let mut pipeline = Vec::new();
    let mut work = VecDeque::new();
    // Leaf-first reconstruction moves each full row/history exactly once. Only
    // the bounded-depth structural view of materialized rows is duplicated.
    while let Some(id) = ready.pop_first() {
        let row = remaining
            .remove(&id)
            .ok_or_else(|| invalid("duplicate execution graph node"))?;
        let (mut insight, children, cursor, materialized) = row.into_parts();
        insight.context = Some(context.clone()); // Do NOT realign historical TTL timestamps.
        insight.children = children
            .into_iter()
            .map(|child| {
                assembled
                    .remove(&child)
                    .ok_or_else(|| invalid("child not assembled before parent"))
            })
            .collect::<Result<_, _>>()?;
        if let Some(cursor) = cursor {
            pipeline.push((id, cursor));
        }
        if materialized {
            work.push_back(id);
            if parents.contains_key(&id) {
                assembled.insert(id, insight.recovery_structure_reference());
            }
            collection.add_insight(insight);
        } else {
            if !parents.contains_key(&id) {
                return Err(invalid("dormant insight has no materialized ancestor"));
            }
            assembled.insert(id, insight);
        }
        if let Some(parent) = parents.get(&id) {
            let count = counts.get_mut(parent).expect("validated parent");
            *count -= 1;
            if *count == 0 {
                ready.insert(*parent);
            }
        }
    }
    if !remaining.is_empty() || !assembled.is_empty() {
        return Err(invalid("cyclic or disconnected execution graph"));
    }
    Ok(RecoveredInsights {
        collection,
        pipeline,
        work,
    })
}

impl<S, E, D> StrategyState<S, E, D>
where
    S: Strategy,
    E: Broker + OrderManagementProvider,
    D: DataFeed + DataProvider,
{
    pub(super) fn install_execution_recovery(
        &mut self,
        recovery: ExecutionRecovery,
    ) -> Result<(), BrokerCommandError> {
        if recovery.rows.is_empty() {
            return Ok(());
        }
        if !self.insights.is_empty() || !self.insight_pipeline.deferred.is_empty() {
            return Err(BrokerCommandError::Invalid(
                "cannot replace initialized insight ownership during execution recovery".into(),
            ));
        }
        let context = if let Some(state) = self.broker.backtest_state.clone() {
            InsightStrategyContext::new(move || state.read().current_time)
        } else {
            InsightStrategyContext::new(chrono::Utc::now)
        };
        let restored = rebuild_insights(recovery, context)?;
        for (id, cursor) in restored.pipeline {
            if let Err(error) = self
                .insight_pipeline
                .restore_execution_checkpoint(id, cursor)
            {
                self.insight_pipeline.deferred.clear();
                self.insight_pipeline.snapshot_dependencies = Default::default();
                let empty = FxHashSet::default();
                for pipes in self.insight_pipeline.pipeline.values_mut() {
                    for pipe in pipes {
                        pipe.retain_active_insights(&empty);
                    }
                }
                return Err(BrokerCommandError::Invalid(format!(
                    "execution pipeline recovery refused: {error}"
                )));
            }
        }
        // All canonical IDs (including terminal tombstones) exist before any
        // child cursor can be scheduled. Embedded templates cannot revive them.
        self.insights = restored.collection;
        self.insight_work.restore(restored.work);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
