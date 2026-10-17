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
