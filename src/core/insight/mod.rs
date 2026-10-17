#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod execution_checkpoint;
#[cfg(not(target_arch = "wasm32"))]
mod execution_data;
#[cfg(not(target_arch = "wasm32"))]
mod insight;
#[cfg(not(target_arch = "wasm32"))]
mod position_accounting;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod operations;
#[cfg(not(target_arch = "wasm32"))]
mod recovery_views;
#[cfg(not(target_arch = "wasm32"))]
pub use operations::{InsightMutationResult, InsightMutationState};
#[cfg(not(target_arch = "wasm32"))]
pub use recovery_views::InsightRecoveryView;
#[cfg(not(target_arch = "wasm32"))]
mod quantity;
#[cfg(not(target_arch = "wasm32"))]
pub mod snapshot;
pub mod types;
use crate::core::broker::types::Order;
#[cfg(not(target_arch = "wasm32"))]
pub use insight::Insight;
#[cfg(not(target_arch = "wasm32"))]
pub use quantity::InsightQuantitySize;
#[cfg(all(not(target_arch = "wasm32"), feature = "runtime"))]
use rustc_hash::{FxHashMap, FxHashSet};
#[cfg(not(target_arch = "wasm32"))]
pub use snapshot::{InsightPartialCloseSnapshot, InsightSnapshot, InsightStateHistorySnapshot};
use std::collections::HashMap;
#[cfg(all(not(target_arch = "wasm32"), not(feature = "runtime")))]
use std::collections::HashSet;
pub use types::InsightHistoryKind;
#[cfg(not(target_arch = "wasm32"))]
pub use types::InsightStrategyContext;
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
    // Ordered IDs permit bounded live cursor reads without collecting/sorting
    // the entire active set. Keep existing iteration order for backtests.
    ordered_active_insight_ids: std::collections::BTreeSet<Uuid>,
    active_by_symbol: HashMap<String, std::collections::BTreeSet<Uuid>>,
    active_symbol_by_id: FxHashMap<Uuid, String>,
    terminal_pending_prune: FxHashSet<Uuid>,
    lifetime_state_counts: HashMap<InsightState, usize>,
    last_known_insight_state: FxHashMap<Uuid, InsightState>,
    order_id_to_insight_id: HashMap<String, Uuid>,
    insight_id_to_order_ids: FxHashMap<Uuid, Vec<String>>,
    order_id_index_enabled: bool,
    // IDs only, not historical Insight clones. Prevent embedded templates from reviving
    // a child after its canonical terminal entry has been pruned.
    materialized_insight_ids: FxHashSet<Uuid>,
    execution_persistence_enabled: bool,
    execution_dirty: std::collections::BTreeSet<Uuid>,
    execution_pins: FxHashSet<Uuid>,
    recovery_views: recovery_views::RecoveryViews,
}

#[cfg(not(target_arch = "wasm32"))]
impl InsightCollection {
    pub fn new() -> Self {
        Self {
            insights: FxHashMap::default(),
            dirty_insight_ids: FxHashSet::default(),
            active_insight_ids: FxHashSet::default(),
            ordered_active_insight_ids: Default::default(),
            active_by_symbol: HashMap::new(),
            active_symbol_by_id: FxHashMap::default(),
            terminal_pending_prune: FxHashSet::default(),
            lifetime_state_counts: HashMap::new(),
            last_known_insight_state: FxHashMap::default(),
            order_id_to_insight_id: HashMap::new(),
            insight_id_to_order_ids: FxHashMap::default(),
            order_id_index_enabled: true,
            materialized_insight_ids: FxHashSet::default(),
            execution_persistence_enabled: false,
            execution_dirty: std::collections::BTreeSet::new(),
            execution_pins: FxHashSet::default(),
            recovery_views: Default::default(),
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
        self.materialized_insight_ids.insert(id);
        self.insights.insert(id, insight);
        self.refresh_runtime_tracking(&id);
        id
    }

    pub(crate) fn was_materialized(&self, id: &Uuid) -> bool {
        self.materialized_insight_ids.contains(id)
    }

    /// Temporarily move one row to the single strategy owner while a pipe runs.
    /// Keep identity/state indexes intact until it is restored; other rows stay
    /// accessible through StrategyContext throughout the callback.
    pub(crate) fn take_for_update(&mut self, id: &Uuid) -> Option<Insight> {
        self.insights.remove(id)
    }

    pub(crate) fn restore_after_update(&mut self, insight: Insight, changed: bool) {
        let id = insight.insight_id;
        self.insights.insert(id, insight);
        if changed {
            self.refresh_runtime_tracking(&id);
        }
    }

    pub fn insight_id_for_order(&self, order_id: &str) -> Option<Uuid> {
        self.order_id_to_insight_id.get(order_id).copied()
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
        if let Some(insight) = self.insights.get(insight_id) {
            self.recovery_views.refresh(insight);
        }
        self.refresh_symbol_tracking(insight_id);
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
        if self.execution_persistence_enabled {
            self.execution_dirty.insert(*insight_id);
        }

        if current_state.is_inactive() {
            self.terminal_pending_prune.insert(*insight_id);
            self.active_insight_ids.remove(insight_id);
            self.ordered_active_insight_ids.remove(insight_id);
        } else {
            self.terminal_pending_prune.remove(insight_id);
            self.active_insight_ids.insert(*insight_id);
            self.ordered_active_insight_ids.insert(*insight_id);
        }
    }

    fn remove_tracking(&mut self, insight_id: &Uuid) {
        self.recovery_views.remove_owner(insight_id);
        self.remove_symbol_tracking(insight_id);
        self.dirty_insight_ids.remove(insight_id);
        self.active_insight_ids.remove(insight_id);
        self.ordered_active_insight_ids.remove(insight_id);
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
        if self.execution_persistence_enabled
            && (self.execution_dirty.contains(insight_id)
                || self.execution_pins.contains(insight_id))
        {
            return;
        }
        if let Some(row) = self.insights.remove(insight_id) {
            self.recovery_views
                .before_prune(&row, &self.materialized_insight_ids);
        }
        self.remove_tracking(insight_id);
    }

    pub fn prune_terminal_insights_without_aqs(&mut self) -> Vec<Uuid> {
        let mut prune_ids: Vec<Uuid> = self.terminal_pending_prune.iter().copied().collect();
        if self.execution_persistence_enabled {
            prune_ids.retain(|id| {
                !self.execution_dirty.contains(id) && !self.execution_pins.contains(id)
            });
        }
        prune_ids.sort_unstable();
        for insight_id in &prune_ids {
            self.prune_terminal_insight(insight_id);
        }
        prune_ids
    }

    pub fn insight_ids_for_live_sync(&self, include_full_reconcile: bool) -> Vec<Uuid> {
        self.insight_ids_for_live_sync_limited(include_full_reconcile, None)
    }

    /// Deterministic bounded selection without sorting the entire dirty backlog.
    pub fn insight_ids_for_live_sync_limited(
        &self,
        include_full_reconcile: bool,
        limit: Option<usize>,
    ) -> Vec<Uuid> {
        if limit == Some(0) {
            return Vec::new();
        }
        let mut ids: Vec<Uuid> = if include_full_reconcile {
            self.ids()
        } else {
            let mut ids = Vec::with_capacity(
                self.dirty_insight_ids.len() + self.terminal_pending_prune.len(),
            );
            ids.extend(self.dirty_insight_ids.iter().copied());
            ids.extend(
                self.terminal_pending_prune
                    .iter()
                    .filter(|id| {
                        !self.dirty_insight_ids.contains(id)
                            && !(self.execution_persistence_enabled
                                && (self.execution_dirty.contains(id)
                                    || self.execution_pins.contains(id)))
                    })
                    .copied(),
            );
            ids
        };
        if let Some(limit) = limit.filter(|limit| *limit < ids.len()) {
            ids.select_nth_unstable(limit);
            ids.truncate(limit);
        }
        ids.sort_unstable();
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

    pub fn active_insight_count(&self) -> usize {
        self.active_insight_ids.len()
    }

    pub(crate) fn active_insight_upper_bound(&self) -> Option<Uuid> {
        self.ordered_active_insight_ids.last().copied()
    }

    fn remove_symbol_tracking(&mut self, id: &Uuid) {
        if let Some(symbol) = self.active_symbol_by_id.remove(id) {
            if let Some(ids) = self.active_by_symbol.get_mut(&symbol) {
                ids.remove(id);
                if ids.is_empty() {
                    self.active_by_symbol.remove(&symbol);
                }
            }
        }
    }

    fn refresh_symbol_tracking(&mut self, id: &Uuid) {
        let symbol = self
            .insights
            .get(id)
            .filter(|row| !row.state.is_inactive())
            .map(|row| row.symbol.as_str());
        if symbol == self.active_symbol_by_id.get(id).map(String::as_str) {
            return;
        }
        let symbol = symbol.map(str::to_owned);
        self.remove_symbol_tracking(id);
        if let Some(symbol) = symbol {
            self.active_by_symbol
                .entry(symbol.clone())
                .or_default()
                .insert(*id);
            self.active_symbol_by_id.insert(*id, symbol);
        }
    }

    pub(crate) fn active_symbol_upper_bound(&self, symbol: &str) -> Option<Uuid> {
        self.active_by_symbol.get(symbol)?.last().copied()
    }

    /// Scope is this strategy's feed. No historical rows or other symbols are
    /// visited; callers must additionally fence the producing feed/cache epoch.
    pub(crate) fn active_symbol_page(
        &self,
        symbol: &str,
        after: Option<Uuid>,
        upper: Uuid,
        limit: usize,
    ) -> Vec<Uuid> {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        if after.is_some_and(|id| id >= upper) || limit == 0 {
            return Vec::new();
        }
        self.active_by_symbol
            .get(symbol)
            .map_or_else(Vec::new, |ids| {
                ids.range((after.map_or(Unbounded, Excluded), Included(upper)))
                    .take(limit.min(256))
                    .copied()
                    .collect()
            })
    }

    /// The cursor and captured upper bound belong to one owner sweep. Rows
    /// inserted behind the cursor are picked up by the next sweep; removed rows
    /// need no tombstone and cannot make a page scan historical records.
    pub(crate) fn active_insight_page(
        &self,
        after: Option<Uuid>,
        upper: Uuid,
        limit: usize,
    ) -> Vec<Uuid> {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        if after.is_some_and(|id| id >= upper) || limit == 0 {
            return Vec::new();
        }
        self.ordered_active_insight_ids
            .range((after.map_or(Unbounded, Excluded), Included(upper)))
            .take(limit.min(256))
            .copied()
            .collect()
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

    /// Return every tracked descendant of the supplied parents, deepest generation first.
    ///
    /// Deepest-first ordering lets lifecycle cleanup close/cancel nested execution fragments
    /// before their containing child insight. A visited set also makes malformed cyclic parent
    /// references safe to reconcile.
    pub fn descendant_ids_for_parents(&self, parent_ids: &FxHashSet<Uuid>) -> Vec<Uuid> {
        let mut visited = parent_ids.clone();
        let mut frontier = parent_ids.clone();
        let mut generations = Vec::<Vec<Uuid>>::new();

        while !frontier.is_empty() {
            let mut generation = self
                .insights
                .iter()
                .filter_map(|(id, insight)| {
                    insight
                        .parent_id()
                        .is_some_and(|parent_id| frontier.contains(&parent_id))
                        .then_some(*id)
                })
                .filter(|id| visited.insert(*id))
                .collect::<Vec<_>>();
            generation.sort_unstable();
            if generation.is_empty() {
                break;
            }
            frontier = generation.iter().copied().collect();
            generations.push(generation);
        }

        generations.into_iter().rev().flatten().collect()
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

        let mut push_mapped = |order_id: &str| {
            if let Some(mapped_id) = self.order_id_to_insight_id.get(order_id) {
                if !candidates.contains(mapped_id) {
                    candidates.push(*mapped_id);
                }
            }
        };

        push_mapped(&order.order_id);
        if let Some(legs) = order.legs.as_ref() {
            for leg_order_id in [
                legs.take_profit
                    .as_ref()
                    .and_then(|leg| leg.order_id.as_deref()),
                legs.stop_loss
                    .as_ref()
                    .and_then(|leg| leg.order_id.as_deref()),
                legs.trailing_stop
                    .as_ref()
                    .and_then(|leg| leg.order_id.as_deref()),
            ]
            .into_iter()
            .flatten()
            {
                push_mapped(leg_order_id);
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
        self.iter_active_insights().cloned().collect()
    }

    /// Borrow materialized active rows without allocating an ID/row vector.
    /// Includes canonical orphans; excludes dormant embedded child templates.
    /// Use `visit_active_insights` when accounting for queued descendants too.
    /// Iteration order is unspecified. Iterator consumers may short-circuit.
    pub fn iter_active_insights(&self) -> impl Iterator<Item = &Insight> {
        self.active_insight_ids
            .iter()
            .filter_map(|id| self.insights.get(id))
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
        if let Some(row) = &removed {
            self.recovery_views
                .before_prune(row, &self.materialized_insight_ids);
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
        if self.execution_persistence_enabled && self.insights.contains_key(id) {
            self.execution_dirty.insert(*id);
        }
        self.insights.get_mut(id)
    }

    pub(crate) fn enable_execution_persistence(&mut self) {
        self.execution_persistence_enabled = true;
        self.execution_dirty.extend(self.insights.keys().copied());
    }
    pub(crate) fn execution_dirty_ids(&self, limit: usize) -> Vec<Uuid> {
        // Select only the requested page; a burst must not allocate/sort a copy
        // of every dirty ID on each owner turn.
        self.execution_dirty.iter().take(limit).copied().collect()
    }
    pub(crate) fn execution_pin(&mut self, id: Uuid) {
        self.execution_pins.insert(id);
    }
    pub(crate) fn execution_unpin(&mut self, id: &Uuid) {
        self.execution_pins.remove(id);
    }
    pub(crate) fn execution_saved(&mut self, id: &Uuid) {
        self.execution_dirty.remove(id);
    }
    pub(crate) fn has_execution_dirty(&self) -> bool {
        !self.execution_dirty.is_empty()
    }
    pub(crate) fn mark_execution_dirty(&mut self, id: Uuid) {
        if self.execution_persistence_enabled {
            self.execution_dirty.insert(id);
        }
    }

    pub fn get_state_count(&self) -> HashMap<InsightState, usize> {
        self.lifetime_state_counts.clone()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn limited_live_sync_matches_full_sort_including_overlapping_dirty_sets() {
        let mut collection = super::InsightCollection::new();
        for value in 1..=2000 {
            let id = uuid::Uuid::from_u128(value);
            collection.dirty_insight_ids.insert(id);
            if value % 2 == 0 {
                collection.terminal_pending_prune.insert(id);
            }
        }
        let expected = collection.insight_ids_for_live_sync(false);
        assert_eq!(
            expected,
            (1..=2000).map(uuid::Uuid::from_u128).collect::<Vec<_>>()
        );
        for limit in [0, 1, 16, 64, 1999, 2000, 3000] {
            assert_eq!(
                collection.insight_ids_for_live_sync_limited(false, Some(limit)),
                expected.iter().copied().take(limit).collect::<Vec<_>>()
            );
        }
    }
    use super::*;
    use crate::core::broker::types::{
        Asset, AssetExchange, AssetStatus, AssetType, Order, OrderClass, OrderLeg, OrderLegs,
        OrderSide, OrderType, TimeInForce, TradeUpdateEvent,
    };
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
    fn clean_pinned_terminal_rows_do_not_starve_dirty_publications() {
        let mut collection = InsightCollection::new();
        collection.enable_execution_persistence();
        for value in 1..=256 {
            let id = uuid::Uuid::from_u128(value);
            collection.terminal_pending_prune.insert(id);
            collection.execution_pin(id);
        }
        let dirty = uuid::Uuid::from_u128(257);
        collection.dirty_insight_ids.insert(dirty);
        assert_eq!(
            collection.insight_ids_for_live_sync_limited(false, Some(128)),
            vec![dirty]
        );
        let unpinned = uuid::Uuid::from_u128(1);
        collection.execution_unpin(&unpinned);
        assert_eq!(
            collection.insight_ids_for_live_sync_limited(false, Some(128)),
            vec![unpinned, dirty]
        );
    }

    #[test]
    fn history_snapshot_window_preserves_source_indices_and_bounds() {
        let mut insight = test_insight("ETHUSD");
        for n in 0..1000 {
            insight.state_log(Some(format!("history {n}")));
        }
        let full = InsightSnapshot::from_insight(&insight, "strategy");
        for offset in [0usize, 1, 500, full.state_history.len(), usize::MAX] {
            for limit in [0usize, 1, 256, usize::MAX] {
                let window =
                    InsightSnapshot::from_insight_with_history_window(&insight, offset, limit);
                let start = offset.min(full.state_history.len());
                let end = start.saturating_add(limit).min(full.state_history.len());
                assert_eq!(
                    serde_json::to_value(&window.state_history).unwrap(),
                    serde_json::to_value(&full.state_history[start..end]).unwrap()
                );
            }
        }
    }

    #[test]
    fn incremental_history_snapshot_matches_full_suffix_without_changing_projection() {
        let mut insight = test_insight("ETHUSD");
        for n in 0..1000 {
            insight.state_log(Some(format!("history {n}")));
        }
        let full = InsightSnapshot::from_insight(&insight, "strategy");
        for offset in [
            0,
            1,
            500,
            full.state_history.len(),
            full.state_history.len() + 10,
        ] {
            let suffix = InsightSnapshot::from_insight_with_history_offset(&insight, offset);
            assert_eq!(
                serde_json::to_value(&suffix.state_history).unwrap(),
                serde_json::to_value(&full.state_history[offset.min(full.state_history.len())..])
                    .unwrap()
            );
            let mut full_projection = serde_json::to_value(&full).unwrap();
            let mut suffix_projection = serde_json::to_value(&suffix).unwrap();
            full_projection
                .as_object_mut()
                .unwrap()
                .remove("state_history");
            suffix_projection
                .as_object_mut()
                .unwrap()
                .remove("state_history");
            assert_eq!(full_projection, suffix_projection);
        }
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
    fn candidate_insight_ids_for_trade_event_uses_leg_order_ids() {
        let mut collection = InsightCollection::new();
        let mut insight = test_insight("AAPL");
        let insight_id = *insight.insight_id();
        insight.legs.take_profit = Some(OrderLeg {
            order_id: Some("tp-order".to_string()),
            limit_price: Some(105.0),
            trail_price: None,
            side: OrderSide::Sell,
            filled_price: None,
            order_type: OrderType::Limit,
            status: TradeUpdateEvent::Pending,
            order_class: OrderClass::Bracket,
            created_at: 0,
            updated_at: 0,
            submitted_at: 0,
            filled_at: None,
        });
        collection.add_insight(insight);

        let order = Order {
            order_id: "close-order".to_string(),
            insight_id: None,
            strategy_type: None,
            asset: Asset {
                id: "AAPL".to_string(),
                symbol: "AAPL".to_string(),
                name: "AAPL".to_string(),
                asset_type: AssetType::Stock,
                status: AssetStatus::Active,
                exchange: AssetExchange::UNKNOWN("TEST".to_string()),
                tradable: true,
                marginable: true,
                shortable: true,
                fractional: true,
                min_order_size: None,
                quantity_base: None,
                max_order_size: None,
                max_total_size: None,
                min_price_increment: None,
                price_base: None,
                contract_size: None,
                trading_calendar: None,
                fees: Default::default(),
            },
            qty: 1.0,
            filled_qty: 1.0,
            limit_price: None,
            filled_price: Some(105.0),
            stop_price: None,
            side: OrderSide::Sell,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::GTC,
            status: TradeUpdateEvent::Closed,
            order_class: OrderClass::Bracket,
            created_at: 0,
            updated_at: 0,
            submitted_at: 0,
            filled_at: Some(0),
            realized_pnl: None,
            position_accounting: None,
            commission: None,
            swap: None,
            rejection_reason: None,
            legs: Some(OrderLegs {
                take_profit: Some(OrderLeg {
                    order_id: Some("tp-order".to_string()),
                    limit_price: Some(105.0),
                    trail_price: None,
                    side: OrderSide::Sell,
                    filled_price: Some(105.0),
                    order_type: OrderType::Limit,
                    status: TradeUpdateEvent::Filled,
                    order_class: OrderClass::Bracket,
                    created_at: 0,
                    updated_at: 0,
                    submitted_at: 0,
                    filled_at: Some(0),
                }),
                stop_loss: None,
                trailing_stop: None,
            }),
        };

        assert_eq!(
            collection.candidate_insight_ids_for_trade_event(&order),
            vec![insight_id]
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

    #[test]
    fn descendant_ids_for_parents_returns_nested_children_deepest_first() {
        let mut collection = InsightCollection::new();
        let parent_id = collection.add_insight(test_insight("AAPL"));
        let mut child = test_insight("AAPL");
        child.parent_id = Some(parent_id);
        let child_id = collection.add_insight(child);
        let mut grandchild = test_insight("AAPL");
        grandchild.parent_id = Some(child_id);
        let grandchild_id = collection.add_insight(grandchild);

        let parent_ids = FxHashSet::from_iter([parent_id]);
        assert_eq!(
            collection.descendant_ids_for_parents(&parent_ids),
            vec![grandchild_id, child_id]
        );
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Default for InsightCollection {
    fn default() -> Self {
        Self::new()
    }
}
