//! Reference-counted, history-free recovery owners. Memory is bounded by the
//! active dependency set (at most sixteen owners per active insight), not by
//! the number of historical trades. These are never executable collection rows.
use super::{
    FxHashMap, FxHashSet, Insight, InsightCollection, InsightState, execution_data::ExecutionData,
};
use crate::core::broker::commands::BrokerCommandError;
use serde_json::Value;
use uuid::Uuid;

/// Read-only execution recovery data. Absence is not proof that an order closed.
/// `materialized == false` denotes configuration from an unsubmitted template.
#[derive(Clone, Copy)]
pub struct InsightRecoveryView<'a> {
    pub insight_id: Uuid,
    pub parent_id: Option<Uuid>,
    pub state: &'a InsightState,
    pub symbol: &'a str,
    pub filled_price: Option<f64>,
    pub materialized: bool,
    data: &'a ExecutionData,
}

impl InsightRecoveryView<'_> {
    pub fn execution_data(&self, key: &str) -> Result<Option<Value>, BrokerCommandError> {
        self.data.read(key)
    }
}

impl Insight {
    pub fn recovery_view(&self) -> InsightRecoveryView<'_> {
        InsightRecoveryView {
            insight_id: self.insight_id,
            parent_id: self.parent_id,
            state: &self.state,
            symbol: &self.symbol,
            filled_price: self.filled_price,
            materialized: true,
            data: &self.execution_data,
        }
    }
}

struct Retained {
    id: Uuid,
    parent: Option<Uuid>,
    state: InsightState,
    symbol: String,
    filled_price: Option<f64>,
    materialized: bool,
    data: ExecutionData,
}

impl Retained {
    fn capture(row: &Insight, materialized: bool) -> Self {
        Self {
            id: row.insight_id,
            parent: row.parent_id,
            state: row.state.clone(),
            symbol: row.symbol.clone(),
            filled_price: row.filled_price,
            materialized,
            data: row.execution_data.clone(),
        }
    }
    fn view(&self) -> InsightRecoveryView<'_> {
        InsightRecoveryView {
            insight_id: self.id,
            parent_id: self.parent,
            state: &self.state,
            symbol: &self.symbol,
            filled_price: self.filled_price,
            materialized: self.materialized,
            data: &self.data,
        }
    }
}

#[derive(Default)]
pub(super) struct RecoveryViews {
    owners: FxHashMap<Uuid, Vec<Uuid>>,
    counts: FxHashMap<Uuid, usize>,
    retained: FxHashMap<Uuid, Retained>,
}

impl RecoveryViews {
    fn replace(&mut self, owner: Uuid, dependencies: Vec<Uuid>) {
        let previous = self
            .owners
            .get(&owner)
            .map(Vec::as_slice)
            .unwrap_or_default();
        if previous == dependencies {
            return;
        }
        for id in previous.iter().filter(|id| !dependencies.contains(id)) {
            let count = self.counts.get_mut(id).expect("registered dependency");
            *count -= 1;
            if *count == 0 {
                self.counts.remove(id);
                self.retained.remove(id);
            }
        }
        for id in dependencies.iter().filter(|id| !previous.contains(id)) {
            *self.counts.entry(*id).or_default() += 1;
        }
        if dependencies.is_empty() {
            self.owners.remove(&owner);
        } else {
            self.owners.insert(owner, dependencies);
        }
        if self.owners.is_empty() {
            debug_assert!(self.counts.is_empty() && self.retained.is_empty());
            // Do not retain peak hash-table allocations through an idle run.
            *self = Self::default();
        }
    }

    pub(super) fn refresh(&mut self, row: &Insight) {
        // The canonical row supersedes any dormant/pruned recovery view.
        self.retained.remove(&row.insight_id);
        let dependencies = if row.state.is_active() {
            row.execution_data
                .recovery_dependencies()
                .expect("validated execution dependencies")
        } else {
            Vec::new()
        };
        self.replace(row.insight_id, dependencies);
    }

    pub(super) fn remove_owner(&mut self, owner: &Uuid) {
        self.replace(*owner, Vec::new());
    }

    pub(super) fn before_prune(&mut self, row: &Insight, materialized: &FxHashSet<Uuid>) {
        if !row.state.is_inactive() {
            return;
        }
        if self.counts.contains_key(&row.insight_id) {
            self.retained
                .insert(row.insight_id, Retained::capture(row, true));
        }
        // Retain only requested dormant descendants. Never replace newer
        // canonical progress with an embedded, once-materialized template.
        let mut pending: Vec<_> = row.children.iter().collect();
        while let Some(child) = pending.pop() {
            if materialized.contains(&child.insight_id) {
                continue;
            }
            if self.counts.contains_key(&child.insight_id) {
                self.retained
                    .insert(child.insight_id, Retained::capture(child, false));
            }
            pending.extend(child.children.iter());
        }
    }
}

impl InsightCollection {
    /// Current canonical data first, otherwise a retained read-only dependency
    /// view. Retained views are absent from active iteration, order lookup, counts
    /// and broker execution. They are released with the last active dependent.
    pub fn recovery_view(&self, id: &Uuid) -> Option<InsightRecoveryView<'_>> {
        self.insights
            .get(id)
            .map(Insight::recovery_view)
            .or_else(|| self.recovery_views.retained.get(id).map(Retained::view))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::insight::execution_checkpoint::InsightExecutionCheckpoint;

    fn row() -> Insight {
        let mut row = Insight::default();
        row.symbol = "ETHUSD".into();
        row.state = InsightState::Filled;
        row.filled_price = Some(100.0);
        row
    }

    #[test]
    fn closed_owner_retains_only_recovery_data_until_last_dependent_finishes() {
        let mut owner = row();
        owner.order_id = Some("retired-order".into());
        owner
            .set_execution_data("shared:v1", &serde_json::json!({"target":105.0}))
            .unwrap();
        owner.state_log(Some("x".repeat(1024 * 1024)));
        let id = owner.insight_id;
        let mut a = row();
        let mut b = row();
        for consumer in [&mut a, &mut b] {
            consumer.set_recovery_dependencies(&[id]).unwrap();
        }
        let mut collection = InsightCollection::new();
        collection.add_insight(owner.clone());
        collection.add_insight(a.clone());
        collection.add_insight(b.clone());
        // Re-registering an unchanged active consumer must not increment counts.
        collection.add_insight(a.clone());
        owner.state = InsightState::Closed;
        collection.add_insight(owner);
        collection.prune_terminal_insight(&id);
        assert!(collection.get(&id).is_none());
        assert!(collection.insight_id_for_order("retired-order").is_none());
        assert_eq!(collection.iter_active_insights().count(), 2);
        let view = collection.recovery_view(&id).unwrap();
        assert_eq!(view.state, &InsightState::Closed);
        assert_eq!(view.filled_price, Some(100.0));
        assert_eq!(
            view.execution_data("shared:v1").unwrap().unwrap()["target"],
            105.0
        );
        assert!(view.materialized);
        assert_eq!(collection.recovery_views.retained.len(), 1);
        a.state = InsightState::Closed;
        collection.add_insight(a);
        assert!(collection.recovery_view(&id).is_some());
        b.state = InsightState::Cancelled;
        collection.add_insight(b);
        assert!(collection.recovery_view(&id).is_none());
        assert!(collection.recovery_views.retained.is_empty());
        assert!(collection.recovery_views.owners.is_empty());
        assert!(collection.recovery_views.counts.is_empty());
        assert_eq!(collection.recovery_views.owners.capacity(), 0);
        assert_eq!(collection.recovery_views.counts.capacity(), 0);
        assert_eq!(collection.recovery_views.retained.capacity(), 0);
    }

    #[test]
    fn changing_dependencies_keeps_overlap_and_releases_removed_owners() {
        let mut first = row();
        let mut second = row();
        let mut consumer = row();
        let mut collection = InsightCollection::new();
        consumer
            .set_recovery_dependencies(&[first.insight_id, second.insight_id])
            .unwrap();
        collection.add_insight(consumer.clone());
        for target in [&mut first, &mut second] {
            target.state = InsightState::Closed;
            collection.add_insight(target.clone());
            collection.prune_terminal_insight(&target.insight_id);
        }
        consumer
            .set_recovery_dependencies(&[first.insight_id, Uuid::new_v4()])
            .unwrap();
        collection.add_insight(consumer.clone());
        assert!(collection.recovery_view(&first.insight_id).is_some());
        assert!(collection.recovery_view(&second.insight_id).is_none());
        consumer.set_recovery_dependencies(&[]).unwrap();
        collection.add_insight(consumer);
        assert!(collection.recovery_views.counts.is_empty());
        assert!(collection.recovery_views.retained.is_empty());
    }

    #[test]
    fn dormant_recovery_views_never_overwrite_canonical_terminal_progress() {
        let mut parent = row();
        let mut child = row();
        child.parent_id = Some(parent.insight_id);
        child
            .set_execution_data("target", &serde_json::json!(1))
            .unwrap();
        let mut dormant = row();
        dormant.state = InsightState::New;
        dormant.filled_price = None;
        dormant.parent_id = Some(parent.insight_id);
        parent.children = vec![child.clone(), dormant.clone()];
        let mut consumer = row();
        consumer
            .set_recovery_dependencies(&[parent.insight_id, child.insight_id, dormant.insight_id])
            .unwrap();
        let mut collection = InsightCollection::new();
        collection.add_insight(consumer.clone());
        collection.add_insight(parent.clone());
        child.state = InsightState::Closed;
        child
            .set_execution_data("target", &serde_json::json!(2))
            .unwrap();
        collection.add_insight(child.clone());
        collection.prune_terminal_insight(&child.insight_id);
        parent.state = InsightState::Closed;
        collection.add_insight(parent.clone());
        collection.prune_terminal_insight(&parent.insight_id);
        assert_eq!(
            collection
                .recovery_view(&child.insight_id)
                .unwrap()
                .execution_data("target")
                .unwrap(),
            Some(serde_json::json!(2))
        );
        assert!(
            !collection
                .recovery_view(&dormant.insight_id)
                .unwrap()
                .materialized
        );
        dormant.state = InsightState::Filled;
        dormant.filled_price = Some(99.0);
        collection.add_insight(dormant.clone());
        assert!(
            collection
                .recovery_view(&dormant.insight_id)
                .unwrap()
                .materialized
        );
        assert_eq!(
            collection
                .recovery_view(&dormant.insight_id)
                .unwrap()
                .filled_price,
            Some(99.0)
        );
        assert!(
            !collection
                .recovery_views
                .retained
                .contains_key(&dormant.insight_id)
        );
        consumer.state = InsightState::Closed;
        collection.add_insight(consumer);
        assert!(collection.recovery_views.retained.is_empty());
    }

    #[test]
    fn recovery_dependencies_round_trip_current_checkpoint_and_reject_invalid_updates() {
        let mut consumer = row();
        let id = Uuid::new_v4();
        consumer.set_recovery_dependencies(&[id, id]).unwrap();
        let before = consumer.snapshot_fingerprint_hash();
        assert!(consumer.set_recovery_dependencies(&[id; 17]).is_err());
        assert!(
            consumer
                .set_execution_data(
                    super::super::execution_data::RECOVERY_DEPENDENCIES,
                    &serde_json::json!("bad")
                )
                .is_err()
        );
        assert_eq!(consumer.snapshot_fingerprint_hash(), before);
        let checkpoint = InsightExecutionCheckpoint::capture(&consumer, 1, true, None);
        let mut encoded = serde_json::to_value(checkpoint).unwrap();
        assert_eq!(encoded["version"], super::super::execution_checkpoint::EXECUTION_CHECKPOINT_VERSION);
        let decoded: InsightExecutionCheckpoint = serde_json::from_value(encoded.clone()).unwrap();
        decoded.validate().unwrap();
        let (restored, _, _, _) = decoded.into_parts();
        assert_eq!(
            restored.execution_data.recovery_dependencies().unwrap(),
            vec![id]
        );
        assert_eq!(restored.snapshot_fingerprint_hash(), before);
        for version in 0..super::super::execution_checkpoint::EXECUTION_CHECKPOINT_VERSION {
            encoded["version"] = serde_json::json!(version);
            assert!(
                serde_json::from_value::<InsightExecutionCheckpoint>(encoded.clone())
                    .unwrap()
                    .validate()
                    .is_err()
            );
        }
    }
}
