//! Single-owner scheduling for nested execution. Queue entries contain IDs and
//! cursors, never cloned Insight trees. Each tracked insight has at most one
//! entry/cleanup request and one child cursor of each kind. Broker admission is
//! separately bounded; saturation retains the cursor instead of losing a leg.
use super::*;
use crate::core::broker::commands::BrokerCommandError;
use crate::core::insight::InsightMutationState;
use std::collections::VecDeque;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum WorkKind {
    Recover,
    Submit,
    Cleanup,
    SplitChildren,
    FilledChildren,
}

#[derive(Clone, Copy, Debug)]
struct Work {
    id: Uuid,
    kind: WorkKind,
    next_child: usize,
}

#[derive(Default)]
pub(super) struct InsightWorkQueue {
    ready: VecDeque<Work>,
    // Rebuilt from canonical checkpoint identities, not another Insight graph.
    // Kept separate so startup does not execute work before Running/Stopping.
    recovery: VecDeque<Uuid>,
    owned: FxHashSet<(Uuid, WorkKind)>,
    waiting: HashMap<(Uuid, WorkKind), Work>,
    child_cursors: HashMap<(Uuid, WorkKind), usize>,
    admission_blocked: bool,
}

impl InsightWorkQueue {
    fn push(&mut self, id: Uuid, kind: WorkKind) {
        if self.owned.insert((id, kind)) {
            let work = Work {
                id,
                kind,
                next_child: self.child_cursors.get(&(id, kind)).copied().unwrap_or(0),
            };
            if kind == WorkKind::Cleanup {
                self.admission_blocked = false;
                self.ready.push_front(work);
            } else {
                self.ready.push_back(work);
            }
        }
    }

    fn children(&mut self, id: Uuid, kind: WorkKind, length: usize) {
        if self.child_cursors.get(&(id, kind)).copied().unwrap_or(0) < length {
            self.push(id, kind);
        }
    }

    pub(super) fn wake(&mut self, id: Uuid) {
        self.admission_blocked = false;
        for kind in [WorkKind::Cleanup, WorkKind::Submit] {
            if let Some(work) = self.waiting.remove(&(id, kind)) {
                if kind == WorkKind::Cleanup {
                    self.ready.push_front(work);
                } else {
                    self.ready.push_back(work);
                }
            }
        }
    }

    pub(super) fn is_ready(&self) -> bool {
        !self.admission_blocked && (!self.ready.is_empty() || !self.recovery.is_empty())
    }

    pub(super) fn is_ready_for_execution(&self, recovery_enabled: bool) -> bool {
        // An explicit cleanup (e.g. a late broker fill during initialization)
        // must not wait for startup to finish. Cleanup always occupies the front.
        self.is_ready()
            && (recovery_enabled
                || self
                    .ready
                    .front()
                    .is_some_and(|work| work.kind == WorkKind::Cleanup))
    }

    pub(super) fn restore(&mut self, recovery: VecDeque<Uuid>) {
        self.recovery = recovery;
    }

    #[cfg(test)]
    pub(super) fn retained_request_count(&self) -> usize {
        self.owned.len() + self.recovery.len()
    }

    pub(super) fn retire_children(&mut self, id: Uuid) {
        self.child_cursors.remove(&(id, WorkKind::SplitChildren));
        self.child_cursors.remove(&(id, WorkKind::FilledChildren));
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum EntryPermission {
    Ready,
    Waiting,
    Blocked,
}

impl<S, E, D> StrategyState<S, E, D>
where
    S: Strategy,
    E: Broker + OrderManagementProvider,
    D: DataFeed + DataProvider,
{
    pub(super) fn insight_execution_enabled(&self) -> bool {
        self.status.is_running()
            || self.status == StrategyStatus::Stopping
            || *self.shutdown_tx.borrow()
    }

    /// O(depth) canonical lookups, with no descendant scans or graph clones.
    /// Graph reconstruction validates both sides of every ownership edge; live
    /// activation below validates the template's direct parent before insertion.
    fn insight_entry_permission(&self, id: Uuid) -> EntryPermission {
        let mut current = id;
        let mut waiting = false;
        for _ in 0..64 {
            let Some(row) = self.insights.get(&current) else {
                return EntryPermission::Blocked;
            };
            if row.state.is_inactive()
                || row.closing
                || row.cancelling
                || row.suppress_child_entries
            {
                return EntryPermission::Blocked;
            }
            let Some(parent_id) = row.parent_id else {
                return if waiting {
                    EntryPermission::Waiting
                } else {
                    EntryPermission::Ready
                };
            };
            let Some(parent) = self.insights.get(&parent_id) else {
                return EntryPermission::Blocked;
            };
            waiting |= if row.is_split_insight() {
                !(parent.has_pending_submit() || parent.submitted || parent.order_id.is_some())
            } else {
                parent.state != InsightState::Filled
            };
            current = parent_id;
        }
        EntryPermission::Blocked // Includes cycles and invalid excessive depth.
    }

    fn recover_insight_work(&mut self, id: Uuid) {
        let Some(row) = self.insights.get(&id) else {
            return;
        };
        if row.state.is_inactive() {
            self.reduce_terminal_command_receipts(id);
            return;
        }
        if !self.status.is_running()
            || *self.shutdown_tx.borrow()
            || self.insight_entry_permission(id) == EntryPermission::Blocked
        {
            self.schedule_insight_cleanup(id);
            return;
        }
        // Root configuration belongs to the New-state pipeline. Only child
        // activation (or an existing receipt) is automatically resumed here.
        if row.has_pending_submit()
            || (row.parent_id.is_some()
                && row.state == InsightState::New
                && self.insight_entry_permission(id) == EntryPermission::Ready)
        {
            self.insight_work.push(id, WorkKind::Submit);
        }
        self.schedule_insight_children(id);
    }

    /// Called after a borrowed row has returned to the collection. Repeated
    /// observations cannot duplicate active cursors or revive terminal templates.
    pub(super) fn schedule_insight_children(&mut self, id: Uuid) {
        if !self.status.is_running() || *self.shutdown_tx.borrow() {
            self.insight_work.retire_children(id);
            return;
        }
        let Some(insight) = self.insights.get(&id) else {
            return;
        };
        if insight.state.is_inactive()
            || insight.cancelling
            || insight.closing
            || insight.suppress_child_entries
        {
            self.insight_work.retire_children(id);
            return;
        }
        if insight.children.is_empty() {
            return;
        }
        if self.insight_entry_permission(id) != EntryPermission::Ready {
            self.insight_work.retire_children(id);
            return;
        }
        if insight.has_pending_submit() || insight.submitted || insight.order_id.is_some() {
            self.insight_work
                .children(id, WorkKind::SplitChildren, insight.children.len());
        }
        if insight.state == InsightState::Filled {
            self.insight_work
                .children(id, WorkKind::FilledChildren, insight.children.len());
        }
    }

    pub(super) fn schedule_insight_cleanup(&mut self, id: Uuid) {
        // A pending submit may already have crossed the broker boundary. Keep its
        // identity alive, gate further dispatch, then cancel/close its evidence.
        self.insight_work.push(id, WorkKind::Cleanup);
        self.insight_work.wake(id);
    }

    pub(super) fn schedule_descendant_cleanup(&mut self, parent_id: Uuid) {
        let parents = std::iter::once(parent_id).collect();
        for id in self.insights.descendant_ids_for_parents(&parents) {
            self.schedule_insight_cleanup(id);
        }
    }

    /// A bounded CPU-only turn; pending broker operations sleep until a command
    /// completion or trade event wakes their insight. No broker future is polled
    /// here and no cancellation acknowledgement is fabricated.
    pub(super) fn process_insight_work(
        &mut self,
        max_work: usize,
        budget: std::time::Duration,
    ) -> usize {
        let started = std::time::Instant::now();
        let mut processed = 0;
        if !self
            .insight_work
            .is_ready_for_execution(self.insight_execution_enabled())
        {
            return 0;
        }
        while processed < max_work.min(256) {
            if processed > 0 && started.elapsed() >= budget {
                break;
            }
            if !self
                .insight_work
                .is_ready_for_execution(self.insight_execution_enabled())
            {
                break;
            }
            let cleanup_ready = self
                .insight_work
                .ready
                .front()
                .is_some_and(|work| work.kind == WorkKind::Cleanup);
            // Classify recovered cleanup before servicing a large entry cursor.
            // Explicit cleanup has priority over both queues, including on stop.
            let next = if cleanup_ready {
                self.insight_work.ready.pop_front()
            } else {
                self.insight_work
                    .recovery
                    .pop_front()
                    .map(|id| Work {
                        id,
                        kind: WorkKind::Recover,
                        next_child: 0,
                    })
                    .or_else(|| self.insight_work.ready.pop_front())
            };
            let Some(mut work) = next else {
                break;
            };
            processed += 1;
            if work.kind == WorkKind::Recover {
                self.recover_insight_work(work.id);
                continue;
            }
            if matches!(
                work.kind,
                WorkKind::SplitChildren | WorkKind::FilledChildren
            ) {
                if !self.status.is_running()
                    || *self.shutdown_tx.borrow()
                    || self.insight_entry_permission(work.id) != EntryPermission::Ready
                {
                    self.insight_work.owned.remove(&(work.id, work.kind));
                    self.insight_work.retire_children(work.id);
                    continue;
                }
                let Some(parent) = self.insights.get(&work.id) else {
                    self.insight_work.owned.remove(&(work.id, work.kind));
                    continue;
                };
                if parent.state.is_inactive()
                    || parent.cancelling
                    || parent.closing
                    || parent.suppress_child_entries
                {
                    self.insight_work.owned.remove(&(work.id, work.kind));
                    continue;
                }
                let Some(template) = parent.children.get(work.next_child) else {
                    self.insight_work.owned.remove(&(work.id, work.kind));
                    continue;
                };
                let should_activate = template.parent_id == Some(work.id)
                    && template.is_split_insight() == (work.kind == WorkKind::SplitChildren)
                    && if work.kind == WorkKind::SplitChildren {
                        parent.has_pending_submit() || parent.submitted || parent.order_id.is_some()
                    } else {
                        parent.state == InsightState::Filled
                    };
                let existing = should_activate.then_some(template.insight_id).filter(|id| {
                    self.insights.get(id).is_some_and(|row| {
                        row.state.is_active()
                            && row.parent_id == Some(work.id)
                            && row.is_split_insight() == (work.kind == WorkKind::SplitChildren)
                    })
                });
                // Check canonical/tombstone identity before cloning a template.
                let child = (should_activate
                    && !self.insights.was_materialized(&template.insight_id))
                .then(|| template.clone());
                work.next_child += 1;
                self.insight_work
                    .child_cursors
                    .insert((work.id, work.kind), work.next_child);
                self.insight_work.ready.push_back(work);
                if let Some(mut child) = child {
                    self.bind_insight_context(&mut child);
                    let id = child.insight_id;
                    self.insights.add_insight(child);
                    self.insight_work.push(id, WorkKind::Submit);
                } else if let Some(id) = existing {
                    // A crash may persist materialization before its submit was
                    // queued. Resume the canonical row; never clone its stale
                    // embedded template or overwrite its current broker state.
                    if self.insights.get(&id).is_some_and(|row| {
                        row.state == InsightState::New || row.has_pending_submit()
                    }) {
                        self.insight_work.push(id, WorkKind::Submit);
                    }
                    self.schedule_insight_children(id);
                }
                continue;
            }

            if work.kind == WorkKind::Submit
                && self.status.is_running()
                && !*self.shutdown_tx.borrow()
            {
                match self.insight_entry_permission(work.id) {
                    EntryPermission::Blocked => {
                        self.insight_work.owned.remove(&(work.id, work.kind));
                        self.schedule_insight_cleanup(work.id);
                        continue;
                    }
                    EntryPermission::Waiting
                        if self
                            .insights
                            .get(&work.id)
                            .is_none_or(|row| !row.has_pending_submit()) =>
                    {
                        // The direct parent's next submit/fill cursor wakes it.
                        self.insight_work.owned.remove(&(work.id, work.kind));
                        continue;
                    }
                    _ => {}
                }
            }

            let Some(mut insight) = self.insights.take_for_update(&work.id) else {
                self.insight_work.owned.remove(&(work.id, work.kind));
                continue;
            };
            let before = insight.snapshot_fingerprint_hash();
            let result = if work.kind == WorkKind::Cleanup
                || !self.status.is_running()
                || *self.shutdown_tx.borrow()
            {
                match insight.state {
                    InsightState::New
                        if !insight.has_pending_submit() && insight.order_id.is_none() =>
                    {
                        insight.order_cancelled(
                            "Entry cancelled before broker submission during cleanup",
                        );
                        Ok(InsightMutationState::Applied)
                    }
                    InsightState::Filled => insight.close(self),
                    InsightState::New | InsightState::Executed => insight.cancel(self),
                    _ => insight.poll_pending_submission(self),
                }
            } else {
                insight.submit(self)
            };
            let changed = before != insight.snapshot_fingerprint_hash();
            let active = insight.state.is_active();
            let pending_mutation = !insight.pending_mutations.is_empty();
            self.insights.restore_after_update(insight, changed);
            match result {
                Ok(InsightMutationState::Pending(_)) => {
                    self.insight_work.waiting.insert((work.id, work.kind), work);
                }
                Err(BrokerCommandError::QueueFull) => {
                    self.insight_work.ready.push_front(work);
                    self.insight_work.admission_blocked = true;
                    break; // Backpressure: retain this exact leg for a later turn.
                }
                Err(error) => {
                    // Uncertain owner failures retain the request for recovery;
                    // no new command identity and no false terminal transition.
                    warn!(
                        "Insight execution request failed id={} operation={:?}: {error}",
                        work.id, work.kind
                    );
                    if !active
                        && !pending_mutation
                        && !self
                            .command_driver
                            .as_ref()
                            .is_some_and(|driver| driver.has_pending_owner(&work.id))
                    {
                        // A definite local validation rejection (e.g. expired
                        // restored TTL) has no broker work/event left to wake it.
                        self.insight_work.owned.remove(&(work.id, work.kind));
                    } else {
                        self.insight_work.waiting.insert((work.id, work.kind), work);
                    }
                }
                Ok(InsightMutationState::Applied) => {
                    self.insight_work.owned.remove(&(work.id, work.kind));
                    if work.kind == WorkKind::Cleanup && active {
                        // A cancel/close acknowledgement is not a terminal event.
                        // The next trade event wakes another cleanup check.
                        self.insight_work.owned.insert((work.id, work.kind));
                        self.insight_work.waiting.insert((work.id, work.kind), work);
                    }
                }
            }
            if work.kind == WorkKind::Submit {
                self.schedule_insight_children(work.id);
            }
        }
        processed
    }
}
