//! Bounded runtime preview selection and caches with exact input invalidation.
use crate::core::broker::{BrokerFeature, DataFeedFeature};
use crate::core::insight::types::InsightState;
use crate::core::insight::{Insight, InsightCollection};
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};

struct Candidate<'a>(&'a Insight);

impl Candidate<'_> {
    fn key(&self) -> (Reverse<chrono::DateTime<chrono::Utc>>, uuid::Uuid) {
        (Reverse(self.0.updated_at), self.0.insight_id)
    }
}
impl PartialEq for Candidate<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}
impl Eq for Candidate<'_> {}
impl PartialOrd for Candidate<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Candidate<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key().cmp(&other.key())
    }
}

pub(super) fn top_active_insights(insights: &InsightCollection) -> Vec<&Insight> {
    let limit = crate::core::tui::INSIGHT_PREVIEW_LIMIT;
    let mut candidates = BinaryHeap::with_capacity(limit);
    for insight in insights.iter_active_insights() {
        let candidate = Candidate(insight);
        if candidates.len() < limit {
            candidates.push(candidate);
        } else if let Some(mut worst) = candidates.peek_mut() {
            if candidate < *worst {
                *worst = candidate;
            }
        }
    }
    candidates
        .into_sorted_vec()
        .into_iter()
        .map(|candidate| candidate.0)
        .collect()
}

#[derive(Default)]
pub(super) struct RuntimeSnapshotCache {
    // Compare the actual advertised capabilities, including their displayed order.
    // Connection/reconnect or a public provider replacement can change these.
    capabilities: Option<(Vec<BrokerFeature>, Vec<DataFeedFeature>)>,
    counts: Option<HashMap<InsightState, usize>>,
}

impl RuntimeSnapshotCache {
    pub(super) fn capabilities(
        &mut self,
        broker: &[BrokerFeature],
        feed: &[DataFeedFeature],
    ) -> Option<Vec<String>> {
        if self
            .capabilities
            .as_ref()
            .is_some_and(|previous| previous.0 == broker && previous.1 == feed)
        {
            return None;
        }
        let mut labels: Vec<_> = broker
            .iter()
            .map(|feature| format!("Broker: {feature:?}"))
            .collect();
        labels.extend(feed.iter().map(|feature| format!("Feed: {feature:?}")));
        self.capabilities = Some((broker.to_vec(), feed.to_vec()));
        Some(labels)
    }

    pub(super) fn counts(
        &mut self,
        counts: &HashMap<InsightState, usize>,
    ) -> Option<HashMap<String, usize>> {
        if self.counts.as_ref() == Some(counts) {
            return None;
        }
        self.counts = Some(counts.clone());
        Some(
            counts
                .iter()
                .map(|(state, count)| (format!("{state:?}"), *count))
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn runtime_top_preview_matches_full_sort_with_ties_and_public_mutation() {
        let mut insights = InsightCollection::new();
        for index in (1..=2048).rev() {
            let mut insight = Insight::default();
            insight.insight_id = uuid::Uuid::from_u128(index);
            insight.updated_at = chrono::Utc.timestamp_opt((index % 19) as i64, 0).unwrap();
            insights.add_insight(insight);
        }
        let check = |insights: &InsightCollection| {
            let mut expected: Vec<_> = insights.iter_active_insights().collect();
            expected.sort_by_key(|insight| (Reverse(insight.updated_at), insight.insight_id));
            expected.truncate(crate::core::tui::INSIGHT_PREVIEW_LIMIT);
            assert_eq!(
                top_active_insights(insights)
                    .iter()
                    .map(|i| i.insight_id)
                    .collect::<Vec<_>>(),
                expected.iter().map(|i| i.insight_id).collect::<Vec<_>>(),
            );
        };
        check(&insights);
        // Mutating public fields need not touch collection tracking or a revision.
        insights
            .get_insight_mut(&uuid::Uuid::from_u128(19))
            .unwrap()
            .updated_at = chrono::Utc.timestamp_opt(999, 0).unwrap();
        check(&insights);
        assert_eq!(
            top_active_insights(&insights)[0].insight_id,
            uuid::Uuid::from_u128(19)
        );
        insights.remove_insight(&uuid::Uuid::from_u128(19));
        check(&insights);
        assert!(top_active_insights(&InsightCollection::new()).is_empty());
    }

    #[test]
    fn runtime_capability_cache_tracks_membership_and_order() {
        let mut cache = RuntimeSnapshotCache::default();
        let broker = vec![BrokerFeature::MarketOrder, BrokerFeature::LimitOrder];
        let feed = vec![DataFeedFeature::CandleStream];
        assert!(cache.capabilities(&broker, &feed).is_some());
        assert!(cache.capabilities(&broker, &feed).is_none());
        assert!(
            cache
                .capabilities(&broker.into_iter().rev().collect::<Vec<_>>(), &feed)
                .is_some()
        );
        assert_eq!(cache.capabilities(&[], &[]), Some(vec![]));
        assert!(cache.capabilities(&[], &[]).is_none());
    }

    #[test]
    fn runtime_count_cache_tracks_actual_counts_and_collection_replacement() {
        let mut cache = RuntimeSnapshotCache::default();
        let mut counts = HashMap::from([(InsightState::New, 4)]);
        assert_eq!(cache.counts(&counts).unwrap()["New"], 4);
        assert!(cache.counts(&counts).is_none());
        counts.insert(InsightState::New, 3);
        counts.insert(InsightState::Filled, 1);
        let changed = cache.counts(&counts).unwrap();
        assert_eq!(changed["New"], 3);
        assert_eq!(changed["Filled"], 1);
        assert_eq!(cache.counts(&HashMap::new()), Some(HashMap::new()));
    }

    #[test]
    #[ignore = "manual runtime preview performance probe"]
    fn runtime_preview_performance_probe() {
        use std::hint::black_box;
        use std::time::Instant;
        let mut insights = InsightCollection::new();
        for index in 1..=20_000 {
            let mut insight = Insight::default();
            insight.insight_id = uuid::Uuid::from_u128(index);
            insight.updated_at = chrono::Utc
                .timestamp_opt((index * 7919 % 997) as i64, 0)
                .unwrap();
            insights.add_insight(insight);
        }
        let baseline = || {
            let mut rows: Vec<_> = insights
                .active_insight_ids_unsorted()
                .into_iter()
                .filter_map(|id| insights.get(&id))
                .collect();
            rows.sort_by_key(|insight| (Reverse(insight.updated_at), insight.insight_id));
            rows.truncate(crate::core::tui::INSIGHT_PREVIEW_LIMIT);
            rows
        };
        assert_eq!(
            baseline().iter().map(|i| i.insight_id).collect::<Vec<_>>(),
            top_active_insights(&insights)
                .iter()
                .map(|i| i.insight_id)
                .collect::<Vec<_>>()
        );
        let mut before = Vec::new();
        let mut after = Vec::new();
        for _ in 0..40 {
            let start = Instant::now();
            black_box(baseline());
            before.push(start.elapsed().as_nanos());
            let start = Instant::now();
            black_box(top_active_insights(black_box(&insights)));
            after.push(start.elapsed().as_nanos());
        }
        before.sort_unstable();
        after.sort_unstable();
        eprintln!(
            "runtime_preview rows=20000 selected=128 baseline_p50_ns={} baseline_p95_ns={} cached_p50_ns={} cached_p95_ns={}",
            before[20], before[38], after[20], after[38]
        );
    }
}
