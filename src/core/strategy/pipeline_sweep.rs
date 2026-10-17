//! A coalesced periodic live pass. Retains cursors, not an all-insight ID queue.
//! Broker/stop branches run between pages; custom pipe calls remain atomic.
use super::*;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::time::{Duration, Instant};

#[derive(Default)]
pub(super) struct LivePipelineSweep {
    pending: bool,
    pruning: bool,
    after: Option<Uuid>,
    deferred_upper: Option<Uuid>,
    active_upper: Option<Uuid>,
}

impl LivePipelineSweep {
    pub fn is_pending(&self) -> bool {
        self.pending
    }

    pub fn request(&mut self, insights: &InsightCollection, pipeline: &InsightPipeline) {
        if self.pending {
            return;
        }
        self.deferred_upper = pipeline.deferred.last_key_value().map(|(id, _)| *id);
        self.active_upper = insights.active_insight_upper_bound();
        self.pending = self.deferred_upper.is_some() || self.active_upper.is_some();
        self.pruning = self.deferred_upper.is_some();
        self.after = None;
    }

    fn cancel(&mut self) {
        *self = Self::default();
    }
}

impl<S, E, D> StrategyState<S, E, D>
where
    S: Strategy,
    E: Broker + OrderManagementProvider,
    D: DataFeed + DataProvider,
{
    pub(super) fn advance_live_pipeline_sweep(&mut self, sweep: &mut LivePipelineSweep) -> usize {
        self.advance_live_pipeline_sweep_with_budget(sweep, Duration::from_millis(5))
    }

    fn advance_live_pipeline_sweep_with_budget(
        &mut self,
        sweep: &mut LivePipelineSweep,
        budget: Duration,
    ) -> usize {
        let start = Instant::now();
        let mut processed = 0;
        while sweep.pending && processed < 256 {
            if !self.status.is_running()
                || *self.shutdown_tx.borrow()
                || self.runtime_telemetry.shutdown_requested()
            {
                sweep.cancel();
                break;
            }
            if processed > 0 && start.elapsed() >= budget {
                break;
            }
            if sweep.pruning {
                let upper = sweep.deferred_upper.expect("pruning has upper bound");
                let ids: Vec<_> = if sweep.after.is_some_and(|id| id >= upper) {
                    Vec::new()
                } else {
                    self.insight_pipeline
                        .deferred
                        .range((sweep.after.map_or(Unbounded, Excluded), Included(upper)))
                        .take(256 - processed)
                        .map(|(id, _)| *id)
                        .collect()
                };
                if ids.is_empty() {
                    sweep.pruning = false;
                    sweep.after = None;
                    continue;
                }
                for id in ids {
                    if processed > 0 && start.elapsed() >= budget {
                        break;
                    }
                    let stale = self
                        .insight_pipeline
                        .deferred
                        .get(&id)
                        .is_some_and(|resume| {
                            !self.insights.get(&id).is_some_and(|row| {
                                !row.state.is_inactive() && row.state == resume.state
                            })
                        });
                    if stale && self.insight_pipeline.forget_continuation(&id) {
                        self.insights.mark_execution_dirty(id);
                    }
                    sweep.after = Some(id);
                    processed += 1;
                }
            } else {
                let Some(upper) = sweep.active_upper else {
                    sweep.cancel();
                    break;
                };
                let ids = self
                    .insights
                    .active_insight_page(sweep.after, upper, 256 - processed);
                if ids.is_empty() {
                    sweep.cancel();
                    break;
                }
                let (last, count) = self.run_insight_pipeline_budgeted(
                    Some(ids),
                    Some(budget.saturating_sub(start.elapsed())),
                );
                if let Some(last) = last {
                    sweep.after = Some(last);
                }
                processed += count;
                if sweep.after == Some(upper) {
                    sweep.cancel();
                }
                if count == 0 {
                    sweep.cancel();
                    break;
                }
            }
        }
        processed
    }
}

#[cfg(test)]
mod tests;
