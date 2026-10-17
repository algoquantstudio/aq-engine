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
        use crate::core::broker::snapshots::SnapshotKind;
        let turn = std::time::Instant::now();
        let budget = std::time::Duration::from_millis(5);
        let mut examined = 0;
        // Enumerate dependency topics, not every deferred broker command/insight.
        // Cache availability is checked once per topic and again by each pipe.
        for kind in self.insight_pipeline.snapshot_dependencies.topics() {
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
            while let Some(id) = self
                .insight_pipeline
                .snapshot_dependencies
                .next(&kind, revision)
            {
                if self.runtime_telemetry.shutdown_requested() || *self.shutdown_tx.borrow() {
                    self.shutdown();
                    return false;
                }
                examined += 1;
                if !self
                    .insights
                    .get(&id)
                    .is_some_and(|row| !row.state.is_inactive())
                {
                    if self.insight_pipeline.forget_continuation(&id) {
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
                if examined >= 256 || turn.elapsed() >= budget {
                    return true;
                }
            }
        }
        false
    }

    /// Never replay an entire active pipeline to poll a single broker receipt.
    /// Snapshot dependencies belong to their cache-refresh path, not to broker
    /// completion wakes. A repeated unchanged deferral is tried once per driver
    /// progress revision, preventing custom pipes from creating a busy loop.
    fn ready_execution_continuations(
        &self,
        attempted: &HashMap<Uuid, (InsightPipeDeferral, u64)>,
        include_snapshots: bool,
    ) -> Vec<Uuid> {
        let Some(driver) = &self.command_driver else {
            return Vec::new();
        };
        let revision = driver.progress_revision();
        let mut ready = Vec::new();
        for (id, resume) in &self.insight_pipeline.deferred {
            if attempted.get(id).is_some_and(|(dependency, previous)| {
                *previous == revision && *dependency == resume.dependency
            }) {
                continue;
            }
            let state_changed = self
                .insights
                .get(id)
                .is_some_and(|row| row.state != resume.state);
            let completed = match &resume.dependency {
                InsightPipeDeferral::BrokerCommand(receipt) => {
                    matches!(
                        driver.progress(receipt),
                        Some(
                            BrokerCommandProgress::Completed(_) | BrokerCommandProgress::Failed(_)
                        )
                    )
                }
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
                ready.push(*id);
                if ready.len() == 256 {
                    break;
                }
            }
        }
        ready.sort_unstable();
        ready
    }

    pub(super) fn resume_live_command_pipes(
        &mut self,
        attempted: &mut HashMap<Uuid, (InsightPipeDeferral, u64)>,
    ) -> bool {
        attempted.retain(|id, _| self.insight_pipeline.deferred.contains_key(id));
        let ready = self.ready_execution_continuations(attempted, false);
        let count = ready.len();
        let revision = self
            .command_driver
            .as_ref()
            .map_or(0, |driver| driver.progress_revision());
        let turn = std::time::Instant::now();
        for (index, id) in ready.into_iter().enumerate() {
            if let Some(resume) = self.insight_pipeline.deferred.get(&id) {
                attempted.insert(id, (resume.dependency.clone(), revision));
            }
            self.run_insight_pipeline_selected(Some(vec![id]));
            if turn.elapsed() >= std::time::Duration::from_millis(5) {
                return index + 1 < count || count == 256;
            }
        }
        count == 256
    }

    pub(super) async fn drain_backtest_execution(&mut self) -> Result<(), BrokerError> {
        let mut attempted = HashMap::new();
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

            let ready = self.ready_execution_continuations(&attempted, true);
            if !ready.is_empty() {
                let revision = self
                    .command_driver
                    .as_ref()
                    .map_or(0, |driver| driver.progress_revision());
                for id in &ready {
                    if let Some(resume) = self.insight_pipeline.deferred.get(id) {
                        attempted.insert(*id, (resume.dependency.clone(), revision));
                    }
                }
                self.run_insight_pipeline_selected(Some(ready));
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
                if self.insight_pipeline.deferred.values().any(|resume| {
                    matches!(
                        resume.dependency,
                        InsightPipeDeferral::BrokerCommand(_)
                            | InsightPipeDeferral::BrokerQueueCapacity
                            | InsightPipeDeferral::BrokerOperationGroup(_)
                    )
                }) {
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
