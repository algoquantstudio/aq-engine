//! Bounded owner-side snapshot scheduling. No I/O and no cloned child trees.
//! Descendants are checkpointed before a parent can release a new entry intent.
use super::*;
use crate::core::{
    broker::commands::BrokerCommandError, insight::execution_checkpoint::InsightExecutionCheckpoint,
};
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

#[derive(Default)]
pub(super) struct ExecutionPersistence {
    roots: VecDeque<Uuid>,
    queued: FxHashSet<Uuid>,
    walk: Option<CheckpointWalk>,
}

struct CheckpointWalk {
    root: Uuid,
    stack: Vec<WalkFrame>,
}
struct WalkFrame {
    id: Uuid,
    path: Vec<usize>,
    next_child: usize,
}

enum WalkStep<'a> {
    Visited,
    Row(&'a Insight, bool, WalkFrame),
    Finished,
}

fn resolve<'a>(
    collection: &'a InsightCollection,
    root: &Uuid,
    path: &[usize],
) -> Option<&'a Insight> {
    let mut insight = collection.get(root)?;
    for index in path {
        let child = insight.children.get(*index)?;
        insight = collection.get(&child.insight_id).unwrap_or(child);
    }
    Some(insight)
}

impl CheckpointWalk {
    fn new(root: Uuid) -> Self {
        Self {
            root,
            stack: vec![WalkFrame {
                id: root,
                path: Vec::new(),
                next_child: 0,
            }],
        }
    }

    fn step<'a>(
        &mut self,
        collection: &'a InsightCollection,
    ) -> Result<WalkStep<'a>, BrokerCommandError> {
        let Some(frame) = self.stack.last_mut() else {
            return Ok(WalkStep::Finished);
        };
        let insight = resolve(collection, &self.root, &frame.path)
            .filter(|insight| insight.insight_id == frame.id)
            .ok_or_else(|| {
                BrokerCommandError::Invalid("insight hierarchy changed while checkpointing".into())
            })?;
        if let Some(child) = insight.children.get(frame.next_child) {
            let index = frame.next_child;
            frame.next_child += 1;
            // Never resurrect a canonical child from an old embedded template.
            if collection.was_materialized(&child.insight_id)
                && collection.get(&child.insight_id).is_none()
            {
                return Ok(WalkStep::Visited);
            }
            let id = child.insight_id;
            let mut path = frame.path.clone();
            path.push(index);
            if self.stack.iter().any(|ancestor| ancestor.id == id) || self.stack.len() >= 64 {
                return Err(BrokerCommandError::Invalid(
                    "cyclic or excessive-depth insight checkpoint hierarchy".into(),
                ));
            }
            self.stack.push(WalkFrame {
                id,
                path,
                next_child: 0,
            });
            Ok(WalkStep::Visited)
        } else {
            let frame = self.stack.pop().expect("current frame exists");
            Ok(WalkStep::Row(
                insight,
                collection.get(&insight.insight_id).is_some(),
                frame,
            ))
        }
    }
}

impl ExecutionPersistence {
    pub(super) fn is_ready(&self) -> bool {
        self.walk.is_some() || !self.roots.is_empty()
    }
    fn enqueue(&mut self, id: Uuid) {
        if self.queued.insert(id) {
            self.roots.push_back(id);
        }
    }
}

impl<S, E, D> StrategyState<S, E, D>
where
    S: Strategy,
    E: Broker + OrderManagementProvider,
    D: DataFeed + DataProvider,
{
    pub(super) fn advance_execution_persistence(&mut self) -> Result<(), BrokerCommandError> {
        self.advance_execution_persistence_with_budget(256, Duration::from_millis(5))
            .map(|_| ())
    }

    pub(super) fn advance_execution_persistence_with_budget(
        &mut self,
        max_work: usize,
        budget: Duration,
    ) -> Result<usize, BrokerCommandError> {
        let started = Instant::now();
        let limit = max_work.min(256);
        if limit == 0 || budget.is_zero() {
            return Ok(0);
        }
        let allow_entries = self.status.is_running() && !*self.shutdown_tx.borrow();
        let Some(driver) = self.command_driver.as_mut() else {
            return Ok(0);
        };
        let mut processed = driver.poll_acknowledgements(limit)?;
        if started.elapsed() < budget {
            processed +=
                driver.dispatch_prepared_with_admission(limit - processed, allow_entries)?;
        }
        if driver.checkpoint_in_flight() || processed == limit || started.elapsed() >= budget {
            return Ok(processed);
        }

        let mut roots = self.insights.execution_dirty_ids(32);
        roots.extend(driver.pending_owner_ids());
        roots.sort_unstable();
        roots.dedup();
        for id in roots {
            if self.execution_persistence.queued.contains(&id) {
                continue;
            }
            if processed == limit || started.elapsed() >= budget {
                break;
            }
            processed += 1;
            let Some(insight) = self.insights.get(&id) else {
                continue;
            };
            let pipeline = self.insight_pipeline.execution_checkpoint(&id);
            let unchanged = driver.checkpoint_execution_matches(
                &id,
                insight.snapshot_fingerprint_hash(),
                true,
                pipeline.as_ref(),
            );
            if unchanged && !driver.has_unpersisted_intents(&id) {
                if !driver.has_consumable_receipts(insight) {
                    self.insights.execution_saved(&id);
                    if !driver.has_pending_owner(&id) {
                        self.insights.execution_unpin(&id);
                    }
                    continue;
                }
            }
            self.execution_persistence.enqueue(id);
        }

        let mut rows = Vec::new();
        let mut ids = FxHashSet::default();
        let mut receipts = Vec::new();
        let mut bytes = 64usize;
        while processed < limit {
            if rows.len() >= 32 || started.elapsed() >= budget {
                break;
            }
            processed += 1;
            if self.execution_persistence.walk.is_none() {
                let Some(root) = self.execution_persistence.roots.pop_front() else {
                    break;
                };
                if self.insights.get(&root).is_none() {
                    self.execution_persistence.queued.remove(&root);
                    continue;
                }
                self.execution_persistence.walk = Some(CheckpointWalk::new(root));
            }
            let walk = self
                .execution_persistence
                .walk
                .as_mut()
                .expect("walk installed");
            match walk.step(&self.insights)? {
                WalkStep::Visited => {}
                WalkStep::Finished => {
                    let root = walk.root;
                    self.execution_persistence.queued.remove(&root);
                    self.execution_persistence.walk = None;
                }
                WalkStep::Row(insight, materialized, frame) => {
                    let id = insight.insight_id;
                    if !ids.insert(id) {
                        continue;
                    }
                    let fingerprint = insight.snapshot_fingerprint_hash();
                    // Canonical progress wins over embedded templates, including
                    // terminal rows whose tombstone must survive a later restart.
                    let pipeline = if materialized {
                        self.insight_pipeline.execution_checkpoint(&id)
                    } else {
                        None
                    };
                    let row = InsightExecutionCheckpoint::capture(
                        insight,
                        driver.checkpoint_revision(&id) + 1,
                        materialized,
                        pipeline,
                    );
                    let applied = driver.consumable_receipts(&row);
                    if driver.checkpoint_execution_matches(
                        &id,
                        fingerprint,
                        materialized,
                        row.pipeline.as_ref(),
                    ) && !driver.has_unpersisted_intents(&id)
                        && applied.is_empty()
                    {
                        if materialized {
                            self.insights.execution_saved(&id);
                            if !driver.has_pending_owner(&id) {
                                self.insights.execution_unpin(&id);
                            }
                        }
                        continue;
                    }
                    let size = driver.checkpoint_byte_budget(&row)?;
                    if bytes.saturating_add(size) > 1024 * 1024 {
                        walk.stack.push(frame); // Re-read the current row on the next bounded turn.
                        if rows.is_empty() {
                            return Err(BrokerCommandError::Invalid(
                                "one insight execution checkpoint exceeds the 1 MiB limit".into(),
                            ));
                        }
                        break;
                    }
                    bytes += size;
                    receipts.extend(applied);
                    rows.push(row);
                }
            }
        }
        if !rows.is_empty() {
            driver.persist_execution_checkpoints(rows, receipts)?;
        }
        Ok(processed)
    }
}
