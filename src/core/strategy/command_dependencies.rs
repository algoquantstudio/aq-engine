//! Reverse routes for deferred execution. Waking a dependency only installs a
//! cursor; fanout, stale entries and repeated attempts consume the owner's budget.
use crate::core::{broker::commands::BrokerCommandReceipt, pipeline::InsightPipeDeferral};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound::{Excluded, Unbounded};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Route {
    Receipt(String),
    Group(String, u64),
    Capacity,
    Snapshots,
    Retry,
}

impl Route {
    fn dependency(dependency: &InsightPipeDeferral) -> Self {
        match dependency {
            InsightPipeDeferral::BrokerCommand(receipt) => Self::Receipt(receipt.id.0.clone()),
            InsightPipeDeferral::BrokerOperationGroup(group) => {
                Self::Group(group.id.clone(), group.version)
            }
            InsightPipeDeferral::BrokerQueueCapacity => Self::Capacity,
            _ => Self::Snapshots,
        }
    }
}

#[derive(Default)]
struct Group {
    ids: BTreeSet<Uuid>,
    head: Option<Uuid>,
    after: Option<Uuid>,
    rerun: bool,
}

struct Owner {
    dependency: InsightPipeDeferral,
    attempted: Option<(u64, u64)>,
}

#[derive(Default)]
pub(super) struct CommandDependencies {
    owners: HashMap<Uuid, Owner>,
    groups: BTreeMap<Route, Group>,
    heads: BTreeSet<(Uuid, Route)>,
    dirty: BTreeSet<Uuid>,
    revision: Option<u64>,
    attempt_epoch: u64,
    snapshot_revision: Option<u64>,
    broker_count: usize,
}

impl CommandDependencies {
    pub fn update(&mut self, id: Uuid, dependency: Option<&InsightPipeDeferral>) {
        if self.owners.get(&id).map(|owner| &owner.dependency) == dependency {
            return;
        }
        self.remove(&id);
        let Some(dependency) = dependency else { return };
        let route = Route::dependency(dependency);
        self.broker_count += usize::from(route != Route::Snapshots);
        self.groups.entry(route).or_default().ids.insert(id);
        self.owners.insert(
            id,
            Owner {
                dependency: dependency.clone(),
                attempted: None,
            },
        );
        // Registration may happen after the completion was reduced. Check once
        // even if no future worker notification exists for this receipt.
        self.dirty.insert(id);
    }

    pub fn remove(&mut self, id: &Uuid) {
        if let Some(owner) = self.owners.remove(id) {
            let route = Route::dependency(&owner.dependency);
            self.broker_count -= usize::from(route != Route::Snapshots);
            self.remove_from(&route, id);
            self.remove_from(&Route::Retry, id);
        }
        self.dirty.remove(id);
    }

    fn remove_from(&mut self, route: &Route, id: &Uuid) {
        let Some(group) = self.groups.get_mut(route) else {
            return;
        };
        group.ids.remove(id);
        if group.head == Some(*id) {
            self.heads.remove(&(*id, route.clone()));
            group.head = None;
            self.advance(route, Some(*id));
        }
        if self
            .groups
            .get(route)
            .is_some_and(|group| group.ids.is_empty())
        {
            self.groups.remove(route);
        }
    }

    fn wake_route(&mut self, route: Route) {
        let Some(group) = self.groups.get_mut(&route) else {
            return;
        };
        if group.head.is_some() {
            group.rerun |= group.after.is_some();
        } else {
            self.advance(&route, None);
        }
    }

    fn advance(&mut self, route: &Route, after: Option<Uuid>) {
        let group = self.groups.get_mut(route).expect("indexed route");
        group.after = after;
        let next = group
            .ids
            .range((after.map_or(Unbounded, Excluded), Unbounded))
            .next()
            .copied();
        let next = if next.is_none() && group.rerun {
            group.rerun = false;
            group.after = None;
            group.ids.first().copied()
        } else {
            next
        };
        group.head = next;
        if let Some(id) = next {
            self.heads.insert((id, route.clone()));
        }
    }

    pub fn wake(&mut self, id: Uuid) {
        if self.owners.contains_key(&id) {
            self.dirty.insert(id);
        }
    }

    pub fn completed(&mut self, receipt: &BrokerCommandReceipt, group_ready: bool) {
        self.wake_route(Route::Receipt(receipt.id.0.clone()));
        if let Some(group) = receipt.group.as_ref().filter(|_| group_ready) {
            self.wake_route(Route::Group(group.id.clone(), group.version));
        }
    }

    pub fn begin_barrier(&mut self) {
        self.attempt_epoch = self.attempt_epoch.wrapping_add(1);
        self.revision = None;
        self.snapshot_revision = None;
    }

    pub fn begin_revision(&mut self, revision: u64, include_snapshots: bool) {
        if self.revision != Some(revision) {
            self.revision = Some(revision);
            self.wake_route(Route::Capacity);
            self.wake_route(Route::Retry);
        }
        if include_snapshots && self.snapshot_revision != Some(revision) {
            self.snapshot_revision = Some(revision);
            self.wake_route(Route::Snapshots);
        }
    }

    /// One candidate, including stale/duplicate work, per call. Merge cursor
    /// heads by UUID so fixed input and completion order stay deterministic.
    pub fn peek(&self) -> Option<Uuid> {
        self.dirty
            .first()
            .copied()
            .into_iter()
            .chain(self.heads.first().map(|(id, _)| *id))
            .min()
    }

    pub fn next(&mut self) -> Option<Uuid> {
        let direct = self.dirty.first().copied();
        let routed = self.heads.first().cloned();
        if direct.is_some_and(|id| routed.as_ref().is_none_or(|(head, _)| id <= *head)) {
            return self.dirty.pop_first();
        }
        let (id, route) = routed?;
        self.heads.remove(&(id, route.clone()));
        self.advance(&route, Some(id));
        Some(id)
    }

    pub fn attempted(&self, id: &Uuid, revision: u64) -> bool {
        self.owners
            .get(id)
            .is_some_and(|owner| owner.attempted == Some((self.attempt_epoch, revision)))
    }

    pub fn mark_attempted(&mut self, id: Uuid, revision: u64) {
        if let Some(owner) = self.owners.get_mut(&id) {
            owner.attempted = Some((self.attempt_epoch, revision));
            self.groups.entry(Route::Retry).or_default().ids.insert(id);
        }
    }

    pub fn requeue_unattempted(&mut self, id: Uuid) {
        if let Some(owner) = self.owners.get_mut(&id) {
            owner.attempted = None;
            self.dirty.insert(id);
        }
    }

    pub fn has_work(&self) -> bool {
        !self.dirty.is_empty() || !self.heads.is_empty()
    }

    pub fn has_work_at_revision(&self, revision: u64) -> bool {
        self.has_work()
            || (self.revision != Some(revision)
                && (self.groups.contains_key(&Route::Capacity)
                    || self.groups.contains_key(&Route::Retry)))
    }

    pub fn has_broker_waiters(&self) -> bool {
        self.broker_count != 0
    }
}

#[cfg(test)]
mod tests;
