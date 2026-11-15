use super::*;
use crate::core::broker::commands::{BrokerCommandId, BrokerOperationGroup};

fn receipt(n: u128, group: Option<BrokerOperationGroup>) -> BrokerCommandReceipt {
    BrokerCommandReceipt {
        id: BrokerCommandId(format!("receipt-{n}")),
        insight_id: Uuid::from_u128(n).to_string(),
        group,
    }
}

fn settle_registration(index: &mut CommandDependencies) {
    while index.next().is_some() {}
}

#[test]
fn command_dependencies_sparse_completion_visits_one_of_100000_waiters() {
    let mut index = CommandDependencies::default();
    for n in 1..=100_000 {
        index.update(
            Uuid::from_u128(n),
            Some(&InsightPipeDeferral::BrokerCommand(receipt(n, None))),
        );
    }
    settle_registration(&mut index);
    index.begin_revision(1, false);
    index.completed(&receipt(90_000, None), false);
    let mut examined = 0;
    let mut ready = Vec::new();
    while let Some(id) = index.next() {
        examined += 1;
        ready.push(id);
    }
    assert_eq!(
        examined, 1,
        "unrelated pending receipts must not be discovered"
    );
    assert_eq!(ready, vec![Uuid::from_u128(90_000)]);
    assert!(!index.has_work());
}

#[test]
fn command_dependencies_group_fanout_survives_pages_removal_and_new_members() {
    let group = BrokerOperationGroup {
        id: "close-all".into(),
        version: 7,
    };
    let dependency = InsightPipeDeferral::BrokerOperationGroup(group.clone());
    let mut index = CommandDependencies::default();
    for n in 1..=1024 {
        index.update(Uuid::from_u128(n), Some(&dependency));
    }
    settle_registration(&mut index);
    index.completed(&receipt(5000, Some(group.clone())), false);
    assert!(!index.has_work(), "partially completed groups stay asleep");
    index.completed(&receipt(5000, Some(group.clone())), true);
    let first: Vec<_> = (0..256).map(|_| index.next().unwrap()).collect();
    assert_eq!(first, (1..=256).map(Uuid::from_u128).collect::<Vec<_>>());
    assert!(index.has_work());
    index.remove(&Uuid::from_u128(257)); // Remove the active cursor head.
    index.remove(&Uuid::from_u128(1024));
    index.update(Uuid::from_u128(2000), Some(&dependency));
    let rest: BTreeSet<_> = std::iter::from_fn(|| index.next()).collect();
    let expected = (258..1024).chain([2000]).map(Uuid::from_u128).collect();
    assert_eq!(rest, expected);
    assert!(!index.has_work());
}

#[test]
fn command_dependencies_order_matches_sorted_reference_for_mixed_routes() {
    let group = BrokerOperationGroup {
        id: "exit".into(),
        version: 1,
    };
    let mut index = CommandDependencies::default();
    for n in (1..=1000).rev() {
        let dependency = if n % 3 == 0 {
            InsightPipeDeferral::BrokerOperationGroup(group.clone())
        } else {
            InsightPipeDeferral::BrokerCommand(receipt(n, None))
        };
        index.update(Uuid::from_u128(n), Some(&dependency));
    }
    settle_registration(&mut index);
    for n in (1..=1000).rev().filter(|n| n % 7 == 0 && n % 3 != 0) {
        index.completed(&receipt(n, None), false);
    }
    index.completed(&receipt(2000, Some(group)), true);
    let actual: Vec<_> = std::iter::from_fn(|| index.next()).collect();
    let expected: Vec<_> = (1..=1000)
        .filter(|n| n % 3 == 0 || n % 7 == 0)
        .map(Uuid::from_u128)
        .collect();
    assert_eq!(actual, expected);
}

#[test]
fn command_dependencies_repeated_deferrals_retry_only_after_progress_and_release_routes() {
    let id = Uuid::from_u128(1);
    let dependency = InsightPipeDeferral::BrokerCommand(receipt(1, None));
    let mut index = CommandDependencies::default();
    index.begin_revision(10, false);
    index.update(id, Some(&dependency));
    assert_eq!(
        index.next(),
        Some(id),
        "registration after completion must get checked"
    );
    index.mark_attempted(id, 10);
    index.update(id, Some(&dependency));
    index.begin_revision(10, false);
    assert!(index.attempted(&id, 10));
    assert_eq!(index.next(), None);
    index.begin_revision(11, false);
    assert_eq!(index.next(), Some(id));
    assert!(!index.attempted(&id, 11));
    index.mark_attempted(id, 11);
    index.begin_barrier();
    index.begin_revision(11, true);
    assert!(
        !index.attempted(&id, 11),
        "a new backtest barrier has its own attempts"
    );
    assert_eq!(index.next(), Some(id));
    index.update(
        id,
        Some(&InsightPipeDeferral::QuoteSnapshot {
            symbol: "ETHUSD".into(),
        }),
    );
    settle_registration(&mut index);
    index.completed(&receipt(1, None), true);
    assert!(!index.has_work());
    assert!(!index.has_broker_waiters());
    index.remove(&id);
    assert!(index.owners.is_empty() && index.groups.is_empty() && index.heads.is_empty());
}

#[test]
fn command_dependencies_unprocessed_batch_owner_retries_without_new_progress() {
    let id = Uuid::from_u128(1);
    let dependency = InsightPipeDeferral::BrokerCommand(receipt(1, None));
    let mut index = CommandDependencies::default();
    index.update(id, Some(&dependency));
    index.begin_revision(7, false);
    assert_eq!(index.next(), Some(id));
    index.mark_attempted(id, 7);
    index.requeue_unattempted(id);
    assert!(!index.attempted(&id, 7));
    assert_eq!(index.next(), Some(id));
    assert_eq!(index.next(), None);
    index.mark_attempted(id, 7);
    assert!(index.attempted(&id, 7));
    index.remove(&id);
    index.requeue_unattempted(id);
    assert!(!index.has_work(), "retired owners must not be resurrected");
}

#[test]
fn command_dependencies_capacity_and_snapshot_routes_are_independent() {
    let mut index = CommandDependencies::default();
    index.update(
        Uuid::from_u128(1),
        Some(&InsightPipeDeferral::BrokerQueueCapacity),
    );
    index.update(
        Uuid::from_u128(2),
        Some(&InsightPipeDeferral::AccountSnapshot),
    );
    settle_registration(&mut index);
    index.begin_revision(1, false);
    assert_eq!(index.next(), Some(Uuid::from_u128(1)));
    assert_eq!(index.next(), None);
    index.begin_revision(1, true);
    assert_eq!(index.next(), Some(Uuid::from_u128(2)));
    assert_eq!(index.next(), None);
    assert!(!index.has_work_at_revision(1));
    assert!(
        index.has_work_at_revision(2),
        "retirement must wake capacity without another worker notice"
    );
    index.begin_revision(2, false);
    assert_eq!(index.next(), Some(Uuid::from_u128(1)));
    assert_eq!(index.next(), None);
    assert!(!index.has_work_at_revision(2));
}

#[test]
fn command_dependencies_midpage_wakes_finish_tail_before_rerunning_prefix() {
    let group = BrokerOperationGroup {
        id: "many".into(),
        version: 1,
    };
    let dependency = InsightPipeDeferral::BrokerOperationGroup(group.clone());
    let mut index = CommandDependencies::default();
    for n in 1..=1024 {
        index.update(Uuid::from_u128(n), Some(&dependency));
    }
    settle_registration(&mut index);
    index.completed(&receipt(9000, Some(group.clone())), true);
    for n in 1..=256 {
        assert_eq!(index.next(), Some(Uuid::from_u128(n)));
    }
    // Repeated completion notices cannot reset the cursor and starve its tail.
    for _ in 0..10 {
        index.completed(&receipt(9000, Some(group.clone())), true);
    }
    index.remove(&Uuid::from_u128(257));
    index.update(Uuid::nil(), Some(&dependency));
    assert_eq!(
        index.next(),
        Some(Uuid::nil()),
        "new member behind cursor is not lost"
    );
    for n in 258..=1024 {
        assert_eq!(index.next(), Some(Uuid::from_u128(n)));
    }
    let rerun: Vec<_> = std::iter::from_fn(|| index.next()).collect();
    assert_eq!(
        rerun,
        (0..=1024)
            .filter(|n| *n != 257)
            .map(Uuid::from_u128)
            .collect::<Vec<_>>()
    );
    assert!(!index.has_work());
}
