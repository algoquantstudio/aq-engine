use super::*;
use crate::core::broker::snapshots::BrokerSnapshotCache;

fn cache() -> BrokerSnapshotCache {
    BrokerSnapshotCache::new("account", Default::default()).unwrap()
}
fn id(value: u128) -> Uuid {
    Uuid::from_u128(value)
}

#[test]
fn snapshot_dependencies_group_membership_changes_release_old_routes() {
    let mut index = SnapshotDependencies::default();
    let account = InsightPipeDeferral::AccountSnapshot;
    let eth = InsightPipeDeferral::QuoteSnapshot {
        symbol: "ETHUSD".into(),
    };
    let sol = InsightPipeDeferral::QuoteSnapshot {
        symbol: "SOLUSD".into(),
    };
    assert!(index.update(id(1), Some(&account)));
    assert!(!index.update(id(1), Some(&account)));
    assert!(index.update(id(1), Some(&eth)));
    assert!(index.update(id(2), Some(&sol)));
    assert_eq!(
        index.topics(),
        vec![kind(&eth).unwrap(), kind(&sol).unwrap()]
    );
    index.update(id(1), Some(&InsightPipeDeferral::BrokerQueueCapacity));
    assert!(!index.owners.contains_key(&id(1)));
    index.remove(&id(2));
    assert!(index.groups.is_empty());
    assert!(index.owners.is_empty());
}

#[test]
fn snapshot_dependencies_thousand_waiters_cursor_does_not_replay_prefix() {
    let cache = cache();
    let mut index = SnapshotDependencies::default();
    for value in 1..=1_000 {
        index.update(id(value), Some(&InsightPipeDeferral::AccountSnapshot));
    }
    let revision = cache.dependency_revision(&SnapshotKind::Account);
    for value in 1..=1_000 {
        assert_eq!(
            index.next(&SnapshotKind::Account, revision),
            Some(id(value))
        );
    }
    assert!(index.next(&SnapshotKind::Account, revision).is_none());
    assert!(index.next(&SnapshotKind::Account, revision).is_none());
    cache.invalidate();
    assert_eq!(
        index.next(
            &SnapshotKind::Account,
            cache.dependency_revision(&SnapshotKind::Account)
        ),
        Some(id(1))
    );
}

#[test]
fn snapshot_dependencies_refresh_during_page_does_not_starve_tail() {
    let cache = cache();
    let mut index = SnapshotDependencies::default();
    for value in 1..=500 {
        index.update(id(value), Some(&InsightPipeDeferral::AccountSnapshot));
    }
    let first = cache.dependency_revision(&SnapshotKind::Account);
    for value in 1..=256 {
        assert_eq!(index.next(&SnapshotKind::Account, first), Some(id(value)));
    }
    cache.invalidate();
    let second = cache.dependency_revision(&SnapshotKind::Account);
    for value in 257..=500 {
        assert_eq!(index.next(&SnapshotKind::Account, second), Some(id(value)));
    }
    // Caller compares each owner's attempt version; the new pass catches only
    // observations missed by the prefix rather than restarting the tail forever.
    assert_eq!(index.next(&SnapshotKind::Account, second), Some(id(1)));
}

#[test]
fn snapshot_dependencies_new_members_and_removed_upper_bound_do_not_get_lost() {
    let cache = cache();
    let mut index = SnapshotDependencies::default();
    for value in 1..=3 {
        index.update(id(value), Some(&InsightPipeDeferral::AccountSnapshot));
    }
    let revision = cache.dependency_revision(&SnapshotKind::Account);
    assert_eq!(index.next(&SnapshotKind::Account, revision), Some(id(1)));
    index.remove(&id(3));
    index.update(id(0), Some(&InsightPipeDeferral::AccountSnapshot));
    assert_eq!(index.next(&SnapshotKind::Account, revision), Some(id(2)));
    assert_eq!(index.next(&SnapshotKind::Account, revision), Some(id(0)));
    assert_eq!(index.next(&SnapshotKind::Account, revision), Some(id(1)));
    assert_eq!(index.next(&SnapshotKind::Account, revision), Some(id(2)));
    assert!(index.next(&SnapshotKind::Account, revision).is_none());
}

#[test]
fn snapshot_dependencies_rotate_topics_and_cache_lifetimes_are_separate() {
    let first_cache = cache();
    let mut index = SnapshotDependencies::default();
    index.update(id(1), Some(&InsightPipeDeferral::AccountSnapshot));
    index.update(id(2), Some(&InsightPipeDeferral::PositionSnapshot));
    let first = first_cache.dependency_revision(&SnapshotKind::Account);
    assert_eq!(index.next(&SnapshotKind::Account, first), Some(id(1)));
    assert_eq!(index.topics()[0], SnapshotKind::Positions);
    assert!(index.next(&SnapshotKind::Account, first).is_none());
    let replacement = cache();
    assert_eq!(
        index.next(
            &SnapshotKind::Account,
            replacement.dependency_revision(&SnapshotKind::Account)
        ),
        Some(id(1))
    );
}

#[test]
fn snapshot_dependencies_topic_cursor_bounds_unavailable_discovery_and_finishes() {
    let mut index = SnapshotDependencies::default();
    for n in 0..1000 {
        index.update(
            id(n + 1),
            Some(&InsightPipeDeferral::QuoteSnapshot {
                symbol: format!("S{n:04}"),
            }),
        );
    }
    index.begin_topics(1, None);
    let mut visited = Vec::new();
    for _ in 0..256 {
        visited.push(index.next_topic().unwrap());
    }
    assert!(index.has_topic_work());
    index.begin_topics(1, None);
    while let Some(topic) = index.next_topic() {
        visited.push(topic);
    }
    assert_eq!(visited.len(), 1000);
    assert_eq!(visited.iter().collect::<BTreeSet<_>>().len(), 1000);
    assert!(
        !index.has_topic_work(),
        "unavailable topics must not cause an immediate loop"
    );
    // A fresh observation scans existing topics without building a topic vector.
    index.begin_topics(2, None);
    assert_eq!(std::iter::from_fn(|| index.next_topic()).count(), 1000);
    assert!(!index.has_topic_work());
}

#[test]
fn snapshot_dependencies_topic_cursor_preserves_midpage_refresh_and_new_membership() {
    let mut index = SnapshotDependencies::default();
    for n in 1..=3 {
        index.update(
            id(n),
            Some(&InsightPipeDeferral::QuoteSnapshot {
                symbol: format!("S{n}"),
            }),
        );
    }
    index.begin_topics(1, None);
    while index.next_topic().is_some() {}
    index.begin_topics(2, None);
    let first = index.next_topic().unwrap();
    index.begin_topics(3, None); // The first topic changed after it was visited.
    index.update(
        id(4),
        Some(&InsightPipeDeferral::QuoteSnapshot {
            symbol: "S0".into(),
        }),
    );
    index.remove(&id(3)); // Captured upper bound disappears.
    let remaining: Vec<_> = std::iter::from_fn(|| index.next_topic()).collect();
    assert!(
        remaining.contains(&first),
        "an intervening refresh must revisit the prefix"
    );
    assert!(remaining.contains(&SnapshotKind::Quote {
        symbol: "S0".into()
    }));
    assert!(!remaining.contains(&SnapshotKind::Quote {
        symbol: "S3".into()
    }));
    assert!(!index.has_topic_work());
}

#[test]
fn snapshot_dependencies_requeued_large_topic_does_not_starve_other_topics() {
    let mut index = SnapshotDependencies::default();
    index.update(id(1), Some(&InsightPipeDeferral::AccountSnapshot));
    index.update(id(2), Some(&InsightPipeDeferral::PositionSnapshot));
    index.begin_topics(1, None);
    let first = index.next_topic().unwrap();
    index.requeue_topic(first.clone());
    let second = index.next_topic().unwrap();
    assert_ne!(first, second);
    assert_eq!(index.next_topic(), Some(first));
    assert_eq!(index.next_topic(), None);
}

#[test]
fn snapshot_dependencies_replacement_cache_revisits_already_scanned_topics() {
    let old = cache();
    let replacement = cache();
    let mut index = SnapshotDependencies::default();
    index.update(id(1), Some(&InsightPipeDeferral::AccountSnapshot));
    index.update(id(2), Some(&InsightPipeDeferral::PositionSnapshot));
    index.begin_topics(0, Some(old.dependency_revision(&SnapshotKind::Account)));
    let first = index.next_topic().unwrap();
    index.begin_topics(
        0,
        Some(replacement.dependency_revision(&SnapshotKind::Account)),
    );
    let remaining: Vec<_> = std::iter::from_fn(|| index.next_topic()).collect();
    assert!(remaining.contains(&first));
    assert!(!index.has_topic_work());
}
