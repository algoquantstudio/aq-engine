use super::{FxHashMap, FxHashSet, Insight, InsightCollection, InsightState};
use uuid::Uuid;

/// Active exposure, including queued descendants. Quantities are remaining units;
/// terminal insights are excluded. References borrow the collection (no Insight clones).
#[derive(Debug, Default)]
pub struct InsightQuantitySize<'a> {
    pub total_count: usize,
    pub total_quantity: f64,
    pub new: usize,
    pub executed: usize,
    pub filled: usize,
    pub insights: Vec<&'a Insight>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::utils::quantity_sizing::tests::sample_insight;

    #[test]
    fn quantity_counts_nested_templates_once_and_prefers_canonical_state() {
        let mut parent = sample_insight();
        parent.quantity = Some(100.0);
        let mut child = sample_insight();
        child.quantity = Some(100.0);
        child.parent_id = Some(*parent.insight_id());
        let mut grandchild = sample_insight();
        grandchild.quantity = Some(50.0);
        grandchild.parent_id = Some(*child.insight_id());
        child.children.push(grandchild.clone());
        parent.children.push(child.clone());
        // Duplicate UUIDs in a malformed nested tree must not recurse/count twice.
        parent.children.push(parent.clone());
        assert_eq!(parent.total_insight_quantity_size(), 250.0);
        let mut collection = InsightCollection::new();
        collection.add_insight(parent.clone());
        assert_eq!(
            collection.iter_active_insights().count(),
            1,
            "Only materialized rows are iterated"
        );
        child.state = InsightState::Filled;
        child.partial_filled_quantity = Some(20.0);
        collection.add_insight(child.clone());
        grandchild.state = InsightState::Executed;
        collection.add_insight(grandchild.clone());
        let result = collection.total_insight_quantity_size("BTCUSD");
        assert_eq!(
            (
                result.total_count,
                result.new,
                result.executed,
                result.filled
            ),
            (3, 1, 1, 1)
        );
        assert_eq!(result.total_quantity, 230.0);
        assert!(
            result
                .insights
                .iter()
                .any(|r| std::ptr::eq(*r, collection.get(child.insight_id()).unwrap()))
        );
        assert_eq!(
            collection.allocated_quantity_excluding("BTCUSD", &parent),
            0.0
        );
        child.state = InsightState::Closed;
        collection.add_insight(child.clone());
        collection.prune_terminal_insight(child.insight_id());
        assert_eq!(
            collection
                .total_insight_quantity_size("BTCUSD")
                .total_quantity,
            150.0
        );
        collection.prune_terminal_insight(parent.insight_id());
        // A canonical orphan must still reserve capacity.
        assert_eq!(
            collection
                .total_insight_quantity_size("BTCUSD")
                .total_quantity,
            50.0
        );
    }

    #[test]
    fn quantity_excludes_terminal_trees_and_other_symbols() {
        let mut parent = sample_insight();
        parent.quantity = Some(10.0);
        let mut child = sample_insight();
        child.quantity = Some(20.0);
        parent.children.push(child);
        parent.state = InsightState::Cancelled;
        let mut collection = InsightCollection::new();
        collection.add_insight(parent);
        let mut other = sample_insight();
        other.symbol = "SOLUSD".into();
        other.quantity = Some(15.0);
        collection.add_insight(other);
        assert_eq!(
            collection
                .total_insight_quantity_size("BTCUSD")
                .total_quantity,
            0.0
        );
        assert_eq!(
            collection
                .total_insight_quantity_size("SOLUSD")
                .total_quantity,
            15.0
        );
    }

    #[test]
    fn active_visitor_borrows_canonical_rows_across_symbols_and_pruned_parents() {
        let mut parent = sample_insight();
        let mut child = sample_insight();
        child.parent_id = Some(*parent.insight_id());
        parent.children.push(child.clone());
        let mut other = sample_insight();
        other.symbol = "ETHUSD".into();
        let mut collection = InsightCollection::new();
        collection.add_insight(parent.clone());
        child.state = InsightState::Filled;
        collection.add_insight(child.clone());
        collection.add_insight(other.clone());
        let borrowed: Vec<_> = collection.iter_active_insights().collect();
        assert_eq!(borrowed.len(), 3);
        assert!(
            borrowed
                .iter()
                .all(|row| std::ptr::eq(*row, collection.get(row.insight_id()).unwrap()))
        );
        let mut ids = FxHashSet::default();
        collection.visit_active_insights(|row| {
            assert!(ids.insert(*row.insight_id()));
            assert!(std::ptr::eq(row, collection.get(row.insight_id()).unwrap()));
        });
        assert_eq!(ids.len(), 3);
        parent.state = InsightState::Closed;
        collection.add_insight(parent.clone());
        collection.prune_terminal_insight(parent.insight_id());
        ids.clear();
        collection.visit_active_insights(|row| {
            ids.insert(*row.insight_id());
        });
        assert_eq!(
            ids,
            [*child.insight_id(), *other.insight_id()]
                .into_iter()
                .collect()
        );
        child.state = InsightState::Closed;
        collection.add_insight(child.clone());
        collection.prune_terminal_insight(child.insight_id());
        ids.clear();
        assert_eq!(collection.iter_active_insights().count(), 1);
        collection.visit_active_insights(|row| {
            ids.insert(*row.insight_id());
        });
        assert_eq!(ids, [*other.insight_id()].into_iter().collect());
    }
}

impl Insight {
    /// Sum this owned tree once per UUID. For submitted children, use the collection
    /// helper instead: its canonical entries have newer state than embedded templates.
    pub fn total_insight_quantity_size(&self) -> f64 {
        let mut seen = FxHashSet::default();
        let mut stack = vec![self];
        let mut total = 0.0;
        while let Some(insight) = stack.pop() {
            if !seen.insert(*insight.insight_id()) || insight.state().is_inactive() {
                continue;
            }
            total += insight.remaining_quantity();
            stack.extend(insight.children.iter());
        }
        total
    }
}

impl InsightCollection {
    /// Borrow each active canonical insight or unmaterialized descendant once,
    /// across all symbols. Includes active orphans, excludes terminal/pruned
    /// templates, and protects against duplicate UUIDs. Does not clone rows.
    ///
    /// This is an O(n) reservation/recovery traversal, not a per-quote routing
    /// API. Keep callbacks inexpensive and do not perform broker I/O in them.
    pub fn visit_active_insights<'a>(&'a self, visit: impl FnMut(&'a Insight)) {
        self.visit_quantity_insights(None, visit);
    }

    /// Accepts a symbol, String, or Asset. Iterative O(n) traversal, O(1) UUID lookup,
    /// canonical-over-template precedence, and cycle/duplicate protection.
    pub fn total_insight_quantity_size(&self, symbol: impl AsRef<str>) -> InsightQuantitySize<'_> {
        let mut result = InsightQuantitySize::default();
        self.visit_quantity_insights(None, |insight| {
            if insight.symbol() != symbol.as_ref() {
                return;
            }
            result.total_count += 1;
            result.total_quantity += insight.remaining_quantity();
            match insight.state() {
                InsightState::New => result.new += 1,
                InsightState::Executed => result.executed += 1,
                InsightState::Filled => result.filled += 1,
                _ => {}
            }
            result.insights.push(insight);
        });
        result
    }

    /// Allocation hot path: sum without allocating a result/reference array, excluding
    /// a tree that is being resized. The tree may temporarily be outside the collection.
    pub fn allocated_quantity_excluding(&self, symbol: &str, tree: &Insight) -> f64 {
        let mut total = 0.0;
        self.visit_quantity_insights(Some(tree), |insight| {
            if insight.symbol() == symbol {
                total += insight.remaining_quantity();
            }
        });
        total
    }

    /// Reserve queued as well as broker-accepted pending entries. Filled positions do
    /// not consume ACCOUNT_LIMIT_ORDERS. Broker-native bracket exits aren't entries.
    pub fn pending_order_count_excluding(&self, tree: &Insight) -> usize {
        use crate::core::broker::types::OrderType;
        let mut count = 0;
        self.visit_quantity_insights(Some(tree), |insight| {
            if matches!(insight.state(), InsightState::New | InsightState::Executed)
                && !matches!(insight.order_type(), OrderType::Market)
            {
                count += 1;
            }
        });
        count
    }

    fn visit_quantity_insights<'a>(
        &'a self,
        exclude: Option<&Insight>,
        mut visit: impl FnMut(&'a Insight),
    ) {
        let mut seen = FxHashSet::<Uuid>::default();
        if let Some(tree) = exclude {
            let mut stack = vec![tree];
            while let Some(row) = stack.pop() {
                if seen.insert(*row.insight_id()) {
                    stack.extend(row.children.iter());
                }
            }
            // Canonical descendants need not still be embedded (orphan-safe).
            let mut children: FxHashMap<Uuid, Vec<Uuid>> = FxHashMap::default();
            for id in &self.active_insight_ids {
                if let Some(row) = self.insights.get(id) {
                    if let Some(parent) = row.parent_id() {
                        children.entry(*parent).or_default().push(*id);
                    }
                }
            }
            let mut ids: Vec<_> = seen.iter().copied().collect();
            while let Some(id) = ids.pop() {
                if let Some(children) = children.get(&id) {
                    for child in children {
                        if seen.insert(*child) {
                            ids.push(*child);
                        }
                    }
                }
            }
        }
        let mut stack: Vec<_> = self
            .active_insight_ids
            .iter()
            .filter_map(|id| self.insights.get(id))
            .collect();
        while let Some(row) = stack.pop() {
            if !seen.insert(*row.insight_id()) || row.state().is_inactive() {
                continue;
            }
            visit(row);
            for child in &row.children {
                // Canonical entries are already seeded above. A once-materialized child
                // absent from the map was pruned/temporarily removed, never a new reservation.
                if !self.materialized_insight_ids.contains(child.insight_id()) {
                    stack.push(child);
                }
            }
        }
    }
}
