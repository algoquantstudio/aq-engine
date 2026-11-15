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
    topic_after: Option<SnapshotKind>,
    topic_upper: Option<SnapshotKind>,
    topic_scan_active: bool,
    queued_topics: BTreeSet<SnapshotKind>,
    prefer_queued: bool,
    observation_revision: Option<(u64, Option<SnapshotReadRevision>)>,
    rescan_topics: bool,
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
        self.queued_topics.insert(next.clone());
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
                self.queued_topics.remove(&kind);
            }
        }
    }

    /// Start a finite scan only when an earlier notification's work is finished.
    /// New membership is separately queued, including behind the scan cursor.
    pub fn begin_topics(&mut self, revision: u64, source: Option<SnapshotReadRevision>) {
        let revision = (revision, source);
        self.rescan_topics |= self
            .observation_revision
            .is_some_and(|previous| previous != revision);
        self.observation_revision = Some(revision);
        if !self.topic_scan_active && self.queued_topics.is_empty() {
            self.start_topic_scan();
        }
    }

    fn start_topic_scan(&mut self) {
        self.rescan_topics = false;
        self.topic_after = None;
        self.topic_upper = self.groups.last_key_value().map(|(kind, _)| kind.clone());
        self.topic_scan_active = self.topic_upper.is_some();
    }

    fn next_queued_topic(&mut self) -> Option<SnapshotKind> {
        let kind = self
            .queued_topics
            .range((
                self.last_topic.as_ref().map_or(Unbounded, Excluded),
                Unbounded,
            ))
            .next()
            .or_else(|| self.queued_topics.first())
            .cloned()?;
        self.queued_topics.remove(&kind);
        self.last_topic = Some(kind.clone());
        Some(kind)
    }

    /// One O(log topics) step. Missing/unavailable topics still consume a step.
    pub fn next_topic(&mut self) -> Option<SnapshotKind> {
        if self.prefer_queued {
            self.prefer_queued = false;
            if let Some(kind) = self.next_queued_topic() {
                return Some(kind);
            }
        }
        if self.topic_scan_active {
            let upper = self.topic_upper.as_ref().expect("active topic scan");
            let next = self
                .groups
                .range((
                    self.topic_after.as_ref().map_or(Unbounded, Excluded),
                    Included(upper),
                ))
                .next()
                .map(|(kind, _)| kind.clone());
            if let Some(kind) = next {
                self.topic_scan_active = &kind != upper;
                self.topic_after = Some(kind.clone());
                self.last_topic = Some(kind.clone());
                self.prefer_queued = true;
                self.queued_topics.remove(&kind);
                return Some(kind);
            }
            self.topic_scan_active = false;
        }
        if let Some(kind) = self.next_queued_topic() {
            return Some(kind);
        }
        if self.rescan_topics {
            self.start_topic_scan();
            return self.next_topic();
        }
        None
    }

    pub fn requeue_topic(&mut self, kind: SnapshotKind) {
        if self.groups.contains_key(&kind) {
            self.queued_topics.insert(kind);
        }
    }

    pub fn has_topic_work(&self) -> bool {
        self.topic_scan_active || !self.queued_topics.is_empty() || self.rescan_topics
    }

    #[cfg(test)]
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
