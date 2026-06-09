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
#[cfg(not(target_arch = "wasm32"))]
pub use snapshot::{InsightPartialCloseSnapshot, InsightSnapshot, InsightStateHistorySnapshot};
use std::collections::{HashMap, HashSet};
use types::InsightState;
use uuid::Uuid;

#[cfg(not(target_arch = "wasm32"))]
pub struct InsightCollection {
    insights: HashMap<Uuid, Insight>,
    dirty_insight_ids: HashSet<Uuid>,
    terminal_pending_prune: HashSet<Uuid>,
    lifetime_state_counts: HashMap<InsightState, usize>,
    last_known_insight_state: HashMap<Uuid, InsightState>,
    order_id_to_insight_id: HashMap<String, Uuid>,
    insight_id_to_order_ids: HashMap<Uuid, Vec<String>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl InsightCollection {
    pub fn new() -> Self {
        Self {
            insights: HashMap::new(),
            dirty_insight_ids: HashSet::new(),
            terminal_pending_prune: HashSet::new(),
            lifetime_state_counts: HashMap::new(),
            last_known_insight_state: HashMap::new(),
            order_id_to_insight_id: HashMap::new(),
            insight_id_to_order_ids: HashMap::new(),
        }
    }
    pub fn add_insight(&mut self, insight: Insight) -> Uuid {
        let id = *insight.insight_id();
        self.insights.insert(id, insight);
        self.refresh_runtime_tracking(&id);
        id
    }

    fn index_order_ids_for_insight(&mut self, insight_id: &Uuid) {
        if let Some(previous_order_ids) = self.insight_id_to_order_ids.remove(insight_id) {
            for order_id in previous_order_ids {
                self.order_id_to_insight_id.remove(&order_id);
            }
        }

        let Some(insight) = self.insights.get(insight_id) else {
            return;
        };

        let mut order_ids: Vec<String> = Vec::new();
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

        for order_id in &order_ids {
            self.order_id_to_insight_id
                .insert(order_id.clone(), *insight_id);
        }
        if !order_ids.is_empty() {
            self.insight_id_to_order_ids.insert(*insight_id, order_ids);
        }
    }

    pub fn refresh_runtime_tracking(&mut self, insight_id: &Uuid) {
        let Some(insight) = self.insights.get(insight_id) else {
            return;
        };

        let current_state = insight.state.clone();
        match self
            .last_known_insight_state
            .insert(*insight_id, current_state.clone())
        {
            Some(previous_state) if previous_state != current_state => {
                if let Some(count) = self.lifetime_state_counts.get_mut(&previous_state) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        self.lifetime_state_counts.remove(&previous_state);
                    }
                }
                *self
                    .lifetime_state_counts
                    .entry(current_state.clone())
                    .or_insert(0) += 1;
            }
            Some(_) => {}
            None => {
                *self
                    .lifetime_state_counts
                    .entry(current_state.clone())
                    .or_insert(0) += 1;
            }
        }

        self.index_order_ids_for_insight(insight_id);
        self.dirty_insight_ids.insert(*insight_id);

        if current_state.is_inactive() {
            self.terminal_pending_prune.insert(*insight_id);
        } else {
            self.terminal_pending_prune.remove(insight_id);
        }
    }

    pub fn remove_dirty(&mut self, insight_id: &Uuid) {
        self.dirty_insight_ids.remove(insight_id);
    }

    pub fn prune_terminal_insight(&mut self, insight_id: &Uuid) {
        self.insights.remove(insight_id);
        self.dirty_insight_ids.remove(insight_id);
        self.terminal_pending_prune.remove(insight_id);
        self.last_known_insight_state.remove(insight_id);
        if let Some(order_ids) = self.insight_id_to_order_ids.remove(insight_id) {
            for order_id in order_ids {
                self.order_id_to_insight_id.remove(&order_id);
            }
        }
    }

    pub fn prune_terminal_insights_without_aqs(&mut self) -> Vec<Uuid> {
        let prune_ids: Vec<Uuid> = self.terminal_pending_prune.iter().copied().collect();
        for insight_id in &prune_ids {
            self.prune_terminal_insight(insight_id);
        }
        prune_ids
    }

    pub fn insight_ids_for_live_sync(&self, include_full_reconcile: bool) -> Vec<Uuid> {
        let mut ids: Vec<Uuid> = if include_full_reconcile {
            self.insights.keys().copied().collect()
        } else {
            self.dirty_insight_ids
                .iter()
                .chain(self.terminal_pending_prune.iter())
                .copied()
                .collect::<HashSet<_>>()
                .into_iter()
                .collect()
        };
        ids.sort_unstable();
        ids
    }

    pub fn dirty_insight_ids(&self) -> Vec<Uuid> {
        let mut ids = self.dirty_insight_ids.iter().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    pub fn active_insight_ids(&self) -> Vec<Uuid> {
        let mut ids = self
            .insights
            .iter()
            .filter_map(|(id, insight)| insight.state().is_active().then_some(*id))
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

    pub fn get_insights_by_symbol(&self, symbol: &str) -> Vec<&Insight> {
        self.insights
            .values()
            .filter(|i| i.symbol() == symbol)
            .collect()
    }

    pub fn get_active_insights(&self) -> Vec<&Insight> {
        self.insights
            .values()
            .filter(|insight| insight.state().is_active())
            .collect()
    }

    pub fn get_inactive_insights(&self) -> Vec<&Insight> {
        self.insights
            .values()
            .filter(|insight| insight.state().is_inactive())
            .collect()
    }

    pub fn prune_inactive_insights(&mut self) {
        self.insights
            .retain(|_, insight| !insight.state().is_inactive());
    }

    pub fn remove_insight(&mut self, id: &Uuid) -> Option<Insight> {
        self.insights.remove(id)
    }
    pub fn len(&self) -> usize {
        self.insights.len()
    }
    pub fn is_empty(&self) -> bool {
        self.insights.is_empty()
    }

    pub fn keys(&self) -> impl Iterator<Item = &Uuid> {
        self.insights.keys()
    }

    pub fn values(&self) -> impl Iterator<Item = &Insight> {
        self.insights.values()
    }

    pub fn get_mut(&mut self, id: &Uuid) -> Option<&mut Insight> {
        self.insights.get_mut(id)
    }
    pub fn get_state_count(&self) -> HashMap<InsightState, usize> {
        let mut counts = HashMap::<InsightState, usize>::new();
        for insight in self.insights.values() {
            *counts.entry(insight.state().clone()).or_insert(0) += 1;
        }
        counts
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
            collection.order_id_to_insight_id.get("order-a"),
            Some(&insight_id)
        );

        collection
            .insights
            .get_mut(&insight_id)
            .expect("insight should exist")
            .order_id = Some("order-b".to_string());
        collection.refresh_runtime_tracking(&insight_id);

        assert!(!collection.order_id_to_insight_id.contains_key("order-a"));
        assert_eq!(
            collection.order_id_to_insight_id.get("order-b"),
            Some(&insight_id)
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
}

#[cfg(not(target_arch = "wasm32"))]
impl Default for InsightCollection {
    fn default() -> Self {
        Self::new()
    }
}
