//! Execution barriers for the deterministic simulated clock. A ready queue is
//! not a completion barrier: the worker may be waiting on broker or journal I/O.
use super::*;
use crate::core::{broker::commands::BrokerCommandError, pipeline::InsightPipeDeferral};
use command_driver::BrokerCommandProgress;

impl<S, E, D> StrategyState<S, E, D>
where
    S: Strategy,
    E: Broker + OrderManagementProvider,
    D: DataFeed + DataProvider,
{
    /// Resume snapshot-dependent stages once per relevant observation revision.
    /// The caller yields/rechecks shutdown between pages; custom pipes that remain
    /// deferred on unchanged data cannot create an immediate notification loop.
    pub(super) fn resume_snapshot_pipes(&mut self) -> bool {
        self.snapshot_continuation_turn(256, std::time::Duration::from_millis(5))
            .1
    }

    fn snapshot_continuation_turn(
        &mut self,
        limit: usize,
        budget: std::time::Duration,
    ) -> (usize, bool) {
        use crate::core::broker::snapshots::SnapshotKind;
        let turn = std::time::Instant::now();
        let mut examined = 0;
        self.insight_pipeline.snapshot_dependencies.begin_topics(
            self.broker.snapshot_revision(),
            self.broker
                .snapshot_dependency_revision(&SnapshotKind::Account),
        );
        while examined < limit.min(256) && turn.elapsed() < budget {
            if self.runtime_telemetry.shutdown_requested() || *self.shutdown_tx.borrow() {
                self.shutdown();
                return (examined, false);
            }
            let Some(kind) = self.insight_pipeline.snapshot_dependencies.next_topic() else {
                break;
            };
            examined += 1;
            let Some(revision) = self.broker.snapshot_dependency_revision(&kind) else {
                continue;
            };
            let available = match &kind {
                SnapshotKind::Account => self.broker.account_snapshot().is_ok(),
                SnapshotKind::Positions => self.broker.positions_snapshot().is_ok(),
                SnapshotKind::Quote { symbol } => self.broker.quote_snapshot(symbol).is_ok(),
            };
            if !available {
                continue;
            }
            loop {
                if examined == limit.min(256) || turn.elapsed() >= budget {
                    self.insight_pipeline
                        .snapshot_dependencies
                        .requeue_topic(kind);
                    break;
                }
                let Some(id) = self
                    .insight_pipeline
                    .snapshot_dependencies
                    .next(&kind, revision)
                else {
                    break;
                };
                examined += 1;
                if self.runtime_telemetry.shutdown_requested() || *self.shutdown_tx.borrow() {
                    self.shutdown();
                    return (examined, false);
                }
                if !self
                    .insights
                    .get(&id)
                    .is_some_and(|row| !row.state.is_inactive())
                {
                    if self.insight_pipeline.forget_continuation(&id) {
                        self.insights.track_pipeline_continuation(id, false);
                        self.insights.mark_execution_dirty(id);
                    }
                } else if let Some(resume) = self.insight_pipeline.deferred.get_mut(&id) {
                    if resume.snapshot_attempted != Some(revision)
                        || self
                            .insights
                            .get(&id)
                            .is_some_and(|row| row.state != resume.state)
                    {
                        resume.snapshot_attempted = Some(revision);
                        self.run_insight_pipeline_budgeted(
                            Some(vec![id]),
                            Some(budget.saturating_sub(turn.elapsed())),
                        );
                    }
                }
            }
        }
        (
            examined,
            self.insight_pipeline.snapshot_dependencies.has_topic_work(),
        )
    }

    /// Discovery and execution share one bounded turn. Each cursor step counts,
    /// including an unchanged deferral or a state change with no continuation.
    /// No deferred-map or attempted-map sweep sits outside the time budget.
    fn resume_execution_continuations(&mut self, include_snapshots: bool) -> bool {
        self.execution_continuation_turn(
            include_snapshots,
            256,
            std::time::Duration::from_millis(5),
        )
        .1
    }

    fn execution_continuation_turn(
        &mut self,
        include_snapshots: bool,
        limit: usize,
        budget: std::time::Duration,
    ) -> (usize, bool) {
        let turn = std::time::Instant::now();
        let Some(driver) = &self.command_driver else {
            return (0, false);
        };
        let revision = driver.progress_revision();
        self.insight_pipeline
            .command_dependencies
            .begin_revision(revision, include_snapshots);
        let mut examined = 0;
        let mut resumed = false;
        // Only the deterministic backtest barrier includes snapshot waiters.
        // Its ready page can share one pipeline setup, as it did before indexed
        // discovery. Both modes retain a shared discovery/execution deadline;
        // live turns still interleave them one complete insight at a time.
        let mut backtest_ready = Vec::new();
        let mut backtest_selected = FxHashSet::default();
        while examined < limit.min(256) && turn.elapsed() < budget {
            // Merge state changes with dependency cursor heads by UUID. Discovery
            // pages preserve deterministic ordering even when no candidate is ready.
            let candidate = self.insight_pipeline.command_dependencies.peek();
            if self
                .insights
                .first_pipeline_state_change()
                .is_some_and(|id| candidate.is_none_or(|candidate| id <= candidate))
            {
                let id = self
                    .insights
                    .pop_pipeline_state_change()
                    .expect("observed change");
                examined += 1;
                self.insight_pipeline.command_dependencies.wake(id);
                continue;
            }
            let Some(id) = self.insight_pipeline.command_dependencies.next() else {
                if self.insights.has_pipeline_state_changes() {
                    continue;
                }
                break;
            };
            examined += 1;
            if self.runtime_telemetry.shutdown_requested() || *self.shutdown_tx.borrow() {
                self.shutdown();
            }
            let Some(resume) = self.insight_pipeline.deferred.get(&id) else {
                continue;
            };
            let state_changed = self
                .insights
                .get(&id)
                .is_none_or(|row| row.state != resume.state);
            if !state_changed
                && self
                    .insight_pipeline
                    .command_dependencies
                    .attempted(&id, revision)
            {
                continue;
            }
            let driver = self
                .command_driver
                .as_ref()
                .expect("execution owner retained");
            let completed = match &resume.dependency {
                InsightPipeDeferral::BrokerCommand(receipt) => matches!(
                    driver.progress(receipt),
                    Some(BrokerCommandProgress::Completed(_) | BrokerCommandProgress::Failed(_))
                ),
                InsightPipeDeferral::BrokerQueueCapacity => driver.has_admission_capacity(),
                InsightPipeDeferral::BrokerOperationGroup(group) => {
                    driver.operation_group_ready(group)
                }
                InsightPipeDeferral::AccountSnapshot => {
                    include_snapshots && self.broker.account_snapshot().is_ok()
                }
                InsightPipeDeferral::PositionSnapshot => {
                    include_snapshots && self.broker.positions_snapshot().is_ok()
                }
                InsightPipeDeferral::QuoteSnapshot { symbol } => {
                    include_snapshots && self.broker.quote_snapshot(symbol).is_ok()
                }
            };
            if state_changed || completed {
                // State-change wakes, receipt cursors and a cursor's pending
                // rerun can select the same owner before the batch executes.
                // Duplicate selections consume discovery work but cannot replay
                // an insight in this batch.
                if include_snapshots && !backtest_selected.insert(id) {
                    continue;
                }
                resumed = true;
                self.insight_pipeline
                    .command_dependencies
                    .mark_attempted(id, revision);
                if include_snapshots {
                    backtest_ready.push(id);
                } else {
                    // Teardown must still consume broker evidence after cancellation.
                    // A supplied budget also prevents the pipeline from hiding a
                    // second reducer drain at the end of each selected insight.
                    self.run_insight_pipeline_turn(
                        Some(vec![id]),
                        Some(budget.saturating_sub(turn.elapsed())),
                        true,
                    );
                }
            }
        }
        if !backtest_ready.is_empty() {
            self.run_backtest_continuation_page(
                backtest_ready,
                budget.saturating_sub(turn.elapsed()),
            );
        }
        (
            examined,
            resumed
                || self.insight_pipeline.command_dependencies.has_work()
                || self.insights.has_pipeline_state_changes(),
        )
    }

    fn run_backtest_continuation_page(
        &mut self,
        mut ready: Vec<Uuid>,
        budget: std::time::Duration,
    ) -> usize {
        let started = std::time::Instant::now();
        // A cursor rerun may wrap to a smaller UUID during discovery. Match the
        // established ready-batch ordering before processing or requeuing a suffix.
        ready.sort_unstable();
        // One shared setup avoids constructing/dropping the default six-state
        // pipeline map for every receipt. The owner still yields at the same
        // deadline without a hidden reducer drain. A selected suffix that did
        // not run must remain immediately eligible at this driver revision.
        let selected = ready.clone();
        let (_, processed) = self.run_insight_pipeline_turn_with_capacity_recheck(
            Some(selected),
            Some(budget.saturating_sub(started.elapsed())),
            true,
            true,
        );
        for id in ready.into_iter().skip(processed) {
            self.insight_pipeline
                .command_dependencies
                .requeue_unattempted(id);
        }
        processed
    }

    pub(super) fn has_pending_command_continuations(&self) -> bool {
        self.command_driver.as_ref().is_some_and(|driver| {
            self.insight_pipeline
                .command_dependencies
                .has_work_at_revision(driver.progress_revision())
                || self.insights.has_pipeline_state_changes()
        })
    }

    pub(super) fn resume_live_command_pipes(&mut self) -> bool {
        self.resume_execution_continuations(false)
    }

    pub(super) async fn drain_backtest_execution(&mut self) -> Result<(), BrokerError> {
        self.insight_pipeline.command_dependencies.begin_barrier();
        let mut shutdown = self.shutdown_tx.subscribe();
        loop {
            if self.runtime_telemetry.shutdown_requested() || *shutdown.borrow() {
                self.shutdown();
            }
            // There is one notification consumer. Notify retains a permit when
            // completion arrives between checking state and awaiting the wake.
            let notifier = self
                .command_driver
                .as_ref()
                .map(|driver| driver.completion_notifier());
            let notified = async {
                match notifier.as_ref() {
                    Some(notifier) => notifier.notified().await,
                    None => std::future::pending().await,
                }
            };
            tokio::pin!(notified);
            // Notify stores one permit even before this future is polled. Only
            // this strategy owner consumes the worker's completion notification.
            self.on_trade_update();
            if let Some(error) = self.execution_error.as_ref() {
                return Err(BrokerError::OrderError(format!(
                    "execution drain failed: {error}"
                )));
            }
            if let Some(error) = self
                .command_driver
                .as_ref()
                .and_then(|driver| driver.unresolved_execution_error())
            {
                self.shutdown();
                return Err(BrokerError::OrderError(format!(
                    "execution drain failed: {error}"
                )));
            }

            if self.resume_execution_continuations(true) {
                tokio::task::yield_now().await;
                continue;
            }
            if self.has_pending_broker_events() {
                tokio::task::yield_now().await;
                continue;
            }
            let waiting = self
                .command_driver
                .as_ref()
                .is_some_and(|driver| driver.has_in_flight_work());
            if !waiting {
                // Remaining broker deferrals without in-flight work cannot be
                // satisfied by advancing market time. Expose a broken custom
                // continuation instead of silently changing its execution bar.
                if self
                    .insight_pipeline
                    .command_dependencies
                    .has_broker_waiters()
                {
                    return Err(BrokerError::OrderError(
                        "execution continuation made no progress after broker completion".into(),
                    ));
                }
                return Ok(());
            }
            if self
                .command_driver
                .as_ref()
                .is_some_and(|driver| !driver.has_external_execution_work())
            {
                return Err(BrokerError::OrderError(
                    "queued execution has no dispatchable checkpoint or in-flight worker".into(),
                ));
            }
            // Normal progress is event-driven. The fallback detects a worker
            // exit and services TUI cancellation even if no completion is sent.
            tokio::select! {
                _ = &mut notified => {},
                _ = shutdown.changed(), if !*shutdown.borrow() => {},
                _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {},
            }
            if self.runtime_telemetry.shutdown_requested() || *self.shutdown_tx.borrow() {
                self.shutdown();
            }
            if self
                .command_driver
                .as_ref()
                .is_some_and(|driver| driver.is_worker_finished())
            {
                self.execution_error
                    .get_or_insert(BrokerCommandError::Closed);
            }
        }
    }
}

#[cfg(test)]
mod tests;
