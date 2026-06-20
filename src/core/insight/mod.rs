#[cfg(not(target_arch = "wasm32"))]
mod insight;
#[cfg(not(target_arch = "wasm32"))]
pub mod snapshot;
pub mod types;
use crate::core::broker::types::Order;
#[cfg(not(target_arch = "wasm32"))]
pub use insight::Insight;
#[cfg(not(target_arch = "wasm32"))]
pub use insight::InsightStrategyContext;
#[cfg(all(not(target_arch = "wasm32"), feature = "runtime"))]
use rustc_hash::{FxHashMap, FxHashSet};
#[cfg(not(target_arch = "wasm32"))]
pub use snapshot::{InsightPartialCloseSnapshot, InsightSnapshot, InsightStateHistorySnapshot};
use std::collections::HashMap;
#[cfg(all(not(target_arch = "wasm32"), not(feature = "runtime")))]
use std::collections::HashSet;
#[cfg(all(not(target_arch = "wasm32"), not(feature = "runtime")))]
type FxHashMap<K, V> = HashMap<K, V>;
#[cfg(all(not(target_arch = "wasm32"), not(feature = "runtime")))]
type FxHashSet<T> = HashSet<T>;
use types::InsightState;
use uuid::Uuid;

#[cfg(not(target_arch = "wasm32"))]
pub struct InsightCollection {
    insights: FxHashMap<Uuid, Insight>,
    dirty_insight_ids: FxHashSet<Uuid>,
    active_insight_ids: FxHashSet<Uuid>,
    terminal_pending_prune: FxHashSet<Uuid>,
    lifetime_state_counts: HashMap<InsightState, usize>,
    last_known_insight_state: FxHashMap<Uuid, InsightState>,
    order_id_to_insight_id: HashMap<String, Uuid>,
    insight_id_to_order_ids: FxHashMap<Uuid, Vec<String>>,
    order_id_index_enabled: bool,
}

#[cfg(not(target_arch = "wasm32"))]
impl InsightCollection {
    pub fn new() -> Self {
        Self {
            insights: FxHashMap::default(),
            dirty_insight_ids: FxHashSet::default(),
            active_insight_ids: FxHashSet::default(),
            terminal_pending_prune: FxHashSet::default(),
            lifetime_state_counts: HashMap::new(),
            last_known_insight_state: FxHashMap::default(),
            order_id_to_insight_id: HashMap::new(),
            insight_id_to_order_ids: FxHashMap::default(),
            order_id_index_enabled: true,
        }
    }

    pub fn with_order_id_index_enabled(mut self, enabled: bool) -> Self {
        self.set_order_id_index_enabled(enabled);
        self
    }

    pub fn set_order_id_index_enabled(&mut self, enabled: bool) {
        self.order_id_index_enabled = enabled;
        if !enabled {
            self.order_id_to_insight_id.clear();
            self.insight_id_to_order_ids.clear();
        }
    }

    pub fn add_insight(&mut self, insight: Insight) -> Uuid {
        let id = *insight.insight_id();
        self.insights.insert(id, insight);
        self.refresh_runtime_tracking(&id);
        id
    }

    fn collect_order_ids(insight: &Insight) -> Vec<String> {
        let mut order_ids = Vec::with_capacity(5);
        if let Some(order_id) = &insight.order_id {
            order_ids.push(order_id.clone());
        }
        if let Some(close_order_id) = &insight.close_order_id {
            order_ids.push(close_order_id.clone());
        }
        let legs = &insight.legs;
        for leg_order_id in [
            legs.take_profit
                .as_ref()
                .and_then(|leg| leg.order_id.as_ref()),
            legs.stop_loss
                .as_ref()
                .and_then(|leg| leg.order_id.as_ref()),
            legs.trailing_stop
                .as_ref()
                .and_then(|leg| leg.order_id.as_ref()),
        ]
        .into_iter()
        .flatten()
        {
            order_ids.push(leg_order_id.clone());
        }
        order_ids
    }

    fn decrement_state_count(&mut self, state: &InsightState) {
        if let Some(count) = self.lifetime_state_counts.get_mut(state) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.lifetime_state_counts.remove(state);
            }
        }
    }

    fn update_state_tracking(&mut self, insight_id: &Uuid, current_state: InsightState) {
        match self
            .last_known_insight_state
            .insert(*insight_id, current_state.clone())
        {
            Some(previous_state) if previous_state != current_state => {
                self.decrement_state_count(&previous_state);
                *self.lifetime_state_counts.entry(current_state).or_insert(0) += 1;
            }
            Some(_) => {}
            None => {
                *self.lifetime_state_counts.entry(current_state).or_insert(0) += 1;
            }
        }
    }

    fn index_order_ids_for_insight(&mut self, insight_id: &Uuid, order_ids: Vec<String>) {
        if !self.order_id_index_enabled {
            return;
        }

        match self.insight_id_to_order_ids.entry(*insight_id) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if entry.get() == &order_ids {
                    return;
                }
                for order_id in entry.get() {
                    self.order_id_to_insight_id.remove(order_id);
                }
                for order_id in &order_ids {
                    self.order_id_to_insight_id
                        .insert(order_id.clone(), *insight_id);
                }
                if order_ids.is_empty() {
                    entry.remove();
                } else {
                    entry.insert(order_ids);
                }
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                if order_ids.is_empty() {
                    return;
                }
                for order_id in &order_ids {
                    self.order_id_to_insight_id
                        .insert(order_id.clone(), *insight_id);
                }
                entry.insert(order_ids);
            }
        }
    }

    pub fn refresh_runtime_tracking(&mut self, insight_id: &Uuid) {
        let Some((current_state, order_ids)) = self.insights.get(insight_id).map(|insight| {
            let order_ids = if self.order_id_index_enabled {
                Self::collect_order_ids(insight)
            } else {
                Vec::new()
            };
            (insight.state.clone(), order_ids)
        }) else {
            self.remove_tracking(insight_id);
            return;
        };

        self.update_state_tracking(insight_id, current_state.clone());
        self.index_order_ids_for_insight(insight_id, order_ids);

        self.dirty_insight_ids.insert(*insight_id);

        if current_state.is_inactive() {
            self.terminal_pending_prune.insert(*insight_id);
            self.active_insight_ids.remove(insight_id);
        } else {
            self.terminal_pending_prune.remove(insight_id);
            self.active_insight_ids.insert(*insight_id);
        }
    }

    fn remove_tracking(&mut self, insight_id: &Uuid) {
        self.dirty_insight_ids.remove(insight_id);
        self.active_insight_ids.remove(insight_id);
        self.terminal_pending_prune.remove(insight_id);
        if let Some(previous_state) = self.last_known_insight_state.remove(insight_id) {
            self.decrement_state_count(&previous_state);
        }
        if let Some(order_ids) = self.insight_id_to_order_ids.remove(insight_id) {
            for order_id in order_ids {
                self.order_id_to_insight_id.remove(&order_id);
            }
        }
    }

    pub fn remove_dirty(&mut self, insight_id: &Uuid) {
        self.dirty_insight_ids.remove(insight_id);
    }

    pub fn take_dirty_insight_ids(&mut self) -> Vec<Uuid> {
        self.dirty_insight_ids.drain().collect::<Vec<_>>()
    }

    pub fn has_dirty_insights(&self) -> bool {
        !self.dirty_insight_ids.is_empty()
    }

    pub fn prune_terminal_insight(&mut self, insight_id: &Uuid) {
        self.insights.remove(insight_id);
        self.remove_tracking(insight_id);
    }

    pub fn prune_terminal_insights_without_aqs(&mut self) -> Vec<Uuid> {
        let mut prune_ids: Vec<Uuid> = self.terminal_pending_prune.iter().copied().collect();
        prune_ids.sort_unstable();
        for insight_id in &prune_ids {
            self.prune_terminal_insight(insight_id);
        }
        prune_ids
    }

    pub fn insight_ids_for_live_sync(&self, include_full_reconcile: bool) -> Vec<Uuid> {
        let mut ids: Vec<Uuid> = if include_full_reconcile {
            self.ids()
        } else {
            let mut ids = Vec::with_capacity(
                self.dirty_insight_ids.len() + self.terminal_pending_prune.len(),
            );
            ids.extend(self.dirty_insight_ids.iter().copied());
            ids.extend(self.terminal_pending_prune.iter().copied());
            ids
        };
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    pub fn dirty_insight_ids(&self) -> Vec<Uuid> {
        let mut ids = self.dirty_insight_ids.iter().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    pub fn active_insight_ids(&self) -> Vec<Uuid> {
        let mut ids = self.active_insight_ids.iter().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    pub fn active_insight_ids_unsorted(&self) -> Vec<Uuid> {
        self.active_insight_ids.iter().copied().collect()
    }

    pub fn child_ids_for_parents(&self, parent_ids: &FxHashSet<Uuid>) -> Vec<Uuid> {
        let mut ids = self
            .insights
            .iter()
            .filter_map(|(id, insight)| {
                insight
                    .parent_id()
                    .is_some_and(|parent_id| parent_ids.contains(&parent_id))
                    .then_some(*id)
            })
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    pub fn candidate_insight_ids_for_trade_event(&self, order: &Order) -> Vec<Uuid> {
        let mut candidates = Vec::new();

        if let Some(insight_id) = order
            .insight_id
            .as_deref()
            .and_then(|value| Uuid::parse_str(value).ok())
        {
            candidates.push(insight_id);
        }

        if let Some(mapped_id) = self.order_id_to_insight_id.get(&order.order_id) {
            if !candidates.contains(mapped_id) {
                candidates.push(*mapped_id);
            }
        }

        candidates
    }

    pub fn lifetime_state_counts(&self) -> &HashMap<InsightState, usize> {
        &self.lifetime_state_counts
    }

    pub fn get_insight(&self, id: &Uuid) -> Option<&Insight> {
        self.insights.get(id)
    }

    pub fn get_insight_mut(&mut self, id: &Uuid) -> Option<&mut Insight> {
        self.insights.get_mut(id)
    }

    pub fn get(&self, id: &Uuid) -> Option<&Insight> {
        self.insights.get(id)
    }

    pub fn get_insights_by_symbol(&self, symbol: &str) -> Vec<Insight> {
        self.insights
            .values()
            .filter_map(|insight| (insight.symbol() == symbol).then(|| insight.clone()))
            .collect()
    }

    pub fn get_active_insights(&self) -> Vec<Insight> {
        self.active_insight_ids_unsorted()
            .into_iter()
            .filter_map(|id| self.insights.get(&id).map(|insight| insight.clone()))
            .collect()
    }

    pub fn get_inactive_insights(&self) -> Vec<Insight> {
        self.insights
            .values()
            .filter_map(|insight| insight.state().is_inactive().then(|| insight.clone()))
            .collect()
    }

    pub fn prune_inactive_insights(&mut self) {
        let inactive_ids = self
            .insights
            .iter()
            .filter_map(|(id, insight)| insight.state().is_inactive().then_some(*id))
            .collect::<Vec<_>>();
        for insight_id in inactive_ids {
            self.prune_terminal_insight(&insight_id);
        }
    }

    pub fn remove_insight(&mut self, id: &Uuid) -> Option<Insight> {
        let removed = self.insights.remove(id);
        if removed.is_some() {
            self.remove_tracking(id);
        }
        removed
    }

    pub fn len(&self) -> usize {
        self.insights.len()
    }

    pub fn is_empty(&self) -> bool {
        self.insights.is_empty()
    }

    pub fn ids(&self) -> Vec<Uuid> {
        let mut ids = self.insights.keys().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    pub fn values(&self) -> std::vec::IntoIter<Insight> {
        self.insights
            .values()
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
    }

    pub fn get_mut(&mut self, id: &Uuid) -> Option<&mut Insight> {
        self.insights.get_mut(id)
    }

    pub fn get_state_count(&self) -> HashMap<InsightState, usize> {
        self.lifetime_state_counts.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::broker::types::OrderSide;
    use crate::core::utils::timeframe::{TimeFrame, TimeFrameUnit};

    fn test_insight(symbol: &str) -> Insight {
        Insight::new(
            OrderSide::Buy,
            symbol.to_string(),
            types::StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            80,
            None,
        )
    }

    #[test]
    fn refresh_runtime_tracking_replaces_previous_order_id_index_entries() {
        let mut collection = InsightCollection::new();
        let mut insight = test_insight("AAPL");
        let insight_id = *insight.insight_id();
        insight.order_id = Some("order-a".to_string());
        collection.add_insight(insight);

        assert_eq!(
            collection.order_id_to_insight_id.get("order-a").copied(),
            Some(insight_id)
        );

        collection
            .insights
            .get_mut(&insight_id)
            .expect("insight should exist")
            .order_id = Some("order-b".to_string());
        collection.refresh_runtime_tracking(&insight_id);

        assert!(!collection.order_id_to_insight_id.contains_key("order-a"));
        assert_eq!(
            collection.order_id_to_insight_id.get("order-b").copied(),
            Some(insight_id)
        );
    }

    #[test]
    fn prune_terminal_insight_removes_order_id_index_entries() {
        let mut collection = InsightCollection::new();
        let mut insight = test_insight("AAPL");
        let insight_id = *insight.insight_id();
        insight.order_id = Some("order-a".to_string());
        collection.add_insight(insight);

        collection.prune_terminal_insight(&insight_id);

        assert!(!collection.order_id_to_insight_id.contains_key("order-a"));
        assert!(!collection.insight_id_to_order_ids.contains_key(&insight_id));
    }

    #[test]
    fn disabled_order_id_index_skips_order_tracking() {
        let mut collection = InsightCollection::new().with_order_id_index_enabled(false);
        let mut insight = test_insight("AAPL");
        insight.order_id = Some("order-a".to_string());
        let insight_id = collection.add_insight(insight);

        assert!(collection.order_id_to_insight_id.is_empty());
        assert!(collection.insight_id_to_order_ids.is_empty());

        collection.refresh_runtime_tracking(&insight_id);
        assert!(collection.order_id_to_insight_id.is_empty());
        assert!(collection.insight_id_to_order_ids.is_empty());
    }

    #[test]
    fn active_insight_ids_excludes_terminal_insights() {
        let mut collection = InsightCollection::new();
        let active_id = collection.add_insight(test_insight("AAPL"));
        let mut closed = test_insight("MSFT");
        closed.state = InsightState::Closed;
        let closed_id = collection.add_insight(closed);

        let active_ids = collection.active_insight_ids();

        assert_eq!(active_ids, vec![active_id]);
        assert!(!active_ids.contains(&closed_id));
    }

    #[test]
    fn get_state_count_uses_cached_state_tracking() {
        let mut collection = InsightCollection::new();
        let active_id = collection.add_insight(test_insight("AAPL"));
        let mut closed = test_insight("MSFT");
        closed.state = InsightState::Closed;
        let closed_id = collection.add_insight(closed);

        assert_eq!(
            collection.get_state_count().get(&InsightState::New),
            Some(&1)
        );
        assert_eq!(
            collection.get_state_count().get(&InsightState::Closed),
            Some(&1)
        );

        collection.prune_terminal_insight(&closed_id);
        assert_eq!(
            collection.get_state_count().get(&InsightState::New),
            Some(&1)
        );
        assert!(
            !collection
                .get_state_count()
                .contains_key(&InsightState::Closed)
        );
        assert_eq!(collection.active_insight_ids(), vec![active_id]);
    }

    #[test]
    fn child_ids_for_parents_uses_insight_parent_refs() {
        let mut collection = InsightCollection::new();
        let parent_id = collection.add_insight(test_insight("AAPL"));
        let mut child = test_insight("AAPL");
        let child_id = *child.insight_id();
        child.parent_id = Some(parent_id);
        collection.add_insight(child);

        let parent_ids = FxHashSet::from_iter([parent_id]);
        assert_eq!(
            collection.child_ids_for_parents(&parent_ids),
            vec![child_id]
        );

        collection.prune_terminal_insight(&child_id);
        assert!(collection.child_ids_for_parents(&parent_ids).is_empty());
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Default for InsightCollection {
    fn default() -> Self {
        Self::new()
    }
}
