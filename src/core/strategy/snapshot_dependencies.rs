//! Runtime-only reverse index: snapshot dependency -> deferred insight IDs.
//! Group cursors survive page yields and new revisions without starving the tail.
use crate::core::broker::snapshots::{SnapshotKind, SnapshotReadRevision};
use crate::core::pipeline::InsightPipeDeferral;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Included, Unbounded};
use uuid::Uuid;

#[derive(Default)]
struct Group {
    ids: BTreeSet<Uuid>,
    revision: Option<SnapshotReadRevision>,
    after: Option<Uuid>,
    upper: Option<Uuid>,
    active: bool,
    rerun: bool,
}

#[derive(Default)]
pub(super) struct SnapshotDependencies {
    groups: BTreeMap<SnapshotKind, Group>,
    owners: rustc_hash::FxHashMap<Uuid, SnapshotKind>,
    last_topic: Option<SnapshotKind>,
}

fn kind(dependency: &InsightPipeDeferral) -> Option<SnapshotKind> {
    match dependency {
        InsightPipeDeferral::AccountSnapshot => Some(SnapshotKind::Account),
        InsightPipeDeferral::PositionSnapshot => Some(SnapshotKind::Positions),
        InsightPipeDeferral::QuoteSnapshot { symbol } => Some(SnapshotKind::Quote {
            symbol: symbol.clone(),
        }),
        _ => None,
    }
}

impl SnapshotDependencies {
    pub fn update(&mut self, id: Uuid, dependency: Option<&InsightPipeDeferral>) -> bool {
        let next = dependency.and_then(kind);
        if self.owners.get(&id) == next.as_ref() {
            return false;
        }
        self.remove(&id);
        let Some(next) = next else {
            return false;
        };
        self.owners.insert(id, next.clone());
        let group = self.groups.entry(next).or_default();
        group.ids.insert(id);
        if group.active {
            if group.after.is_some_and(|after| id <= after)
                || group.upper.is_some_and(|upper| id > upper)
            {
                group.rerun = true;
            }
        } else {
            group.active = true;
            group.after = None;
            group.upper = None;
        }
        true
    }

    pub fn remove(&mut self, id: &Uuid) {
        let Some(kind) = self.owners.remove(id) else {
            return;
        };
        if let Some(group) = self.groups.get_mut(&kind) {
            group.ids.remove(id);
            if group.ids.is_empty() {
                self.groups.remove(&kind);
            }
        }
    }

    /// The temporary list is per dependency topic, never per insight. Rotate
    /// between pages so a frequently refreshed large group cannot monopolize work.
    pub fn topics(&self) -> Vec<SnapshotKind> {
        let mut topics: Vec<_> = self.groups.keys().cloned().collect();
        if let Some(last) = &self.last_topic {
            let offset = topics.partition_point(|key| key <= last);
            topics.rotate_left(offset);
        }
        topics
    }

    pub fn next(&mut self, kind: &SnapshotKind, revision: SnapshotReadRevision) -> Option<Uuid> {
        self.last_topic = Some(kind.clone());
        let group = self.groups.get_mut(kind)?;
        if group.revision != Some(revision) {
            group.revision = Some(revision);
            if group.active && group.after.is_some() {
                group.rerun = true;
            } else {
                group.active = true;
                group.after = None;
                group.upper = None;
            }
        }
        while group.active {
            let upper = *group
                .upper
                .get_or_insert_with(|| *group.ids.last().expect("nonempty indexed group"));
            let next = if group.after.is_some_and(|id| id >= upper) {
                None
            } else {
                group
                    .ids
                    .range((group.after.map_or(Unbounded, Excluded), Included(upper)))
                    .next()
                    .copied()
            };
            if let Some(id) = next {
                group.after = Some(id);
                return Some(id);
            }
            if group.rerun {
                group.rerun = false;
                group.after = None;
                group.upper = None;
            } else {
                group.active = false;
            }
        }
        None
    }
}

#[cfg(test)]
mod tests;
