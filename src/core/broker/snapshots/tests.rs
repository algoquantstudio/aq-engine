use super::*;
use crate::core::broker::types::AccountType;

fn capacity_order(
    id: &str,
    symbol: &str,
    qty: f64,
    filled_qty: f64,
    status: TradeUpdateEvent,
) -> Order {
    use crate::core::broker::types::{
        Asset, AssetExchange, AssetStatus, AssetType, OrderClass, OrderSide, OrderType, TimeInForce,
    };
    Order {
        order_id: id.into(),
        insight_id: None,
        strategy_type: None,
        asset: Asset {
            id: symbol.into(),
            symbol: symbol.into(),
            name: symbol.into(),
            asset_type: AssetType::Crypto,
            status: AssetStatus::Active,
            exchange: AssetExchange::UNKNOWN("MT5".into()),
            tradable: true,
            marginable: true,
            shortable: true,
            fractional: true,
            min_order_size: None,
            quantity_base: None,
            max_order_size: None,
            max_total_size: Some(1000.0),
            min_price_increment: None,
            price_base: None,
            contract_size: None,
            trading_calendar: None,
            fees: Default::default(),
        },
        qty,
        filled_qty,
        limit_price: Some(100.0),
        filled_price: None,
        stop_price: None,
        side: OrderSide::Buy,
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::GTC,
        status,
        order_class: OrderClass::Simple,
        created_at: 0,
        updated_at: 0,
        submitted_at: 0,
        filled_at: None,
        realized_pnl: None,
        position_accounting: None,
        commission: None,
        swap: None,
        rejection_reason: None,
        legs: None,
    }
}

#[test]
fn pending_capacity_excludes_positions_history_and_duplicate_orders() {
    use TradeUpdateEvent::*;
    let rows = [
        ("pending", Pending),
        ("pending", Accepted),
        ("partial", PartialFilled),
        ("filled", Filled),
        ("closed", Closed),
        ("cancelled", Cancelled),
        ("rejected", Rejected),
        ("expired", Expired),
    ];
    assert_eq!(
        pending_order_count(rows.iter().map(|(id, state)| (*id, state))),
        Some(2)
    );
    assert_eq!(
        pending_order_count([("conflict", &Pending), ("conflict", &Filled)]),
        None
    );
    assert_eq!(pending_order_count([("", &Pending)]), None);
    assert_eq!(
        pending_order_count(std::iter::repeat(("same", &Pending)).take(MAX_SNAPSHOT_POSITIONS + 1)),
        None
    );
}

#[test]
fn order_capacity_conservatively_aggregates_active_quantity_by_symbol() {
    let orders = vec![
        capacity_order(
            "sol-a",
            "SOLUSD",
            100.0,
            20.0,
            TradeUpdateEvent::PartialFilled,
        ),
        capacity_order("sol-b", "SOLUSD", 50.0, 0.0, TradeUpdateEvent::Pending),
        capacity_order("eth", "ETHUSD", 10.0, 0.0, TradeUpdateEvent::Accepted),
        capacity_order("filled", "SOLUSD", 500.0, 500.0, TradeUpdateEvent::Filled),
    ];
    let capacity = order_capacity(&orders).unwrap();
    assert_eq!(capacity.pending_orders, 3);
    assert_eq!(capacity.pending_quantity_by_symbol["SOLUSD"], 150.0);
    assert_eq!(capacity.pending_quantity_by_symbol["ETHUSD"], 10.0);
}

#[test]
fn entry_capacity_combines_fresh_positions_and_active_orders() {
    let cache = cache();
    let now = Instant::now();
    cache.seed_account_for_test(account("account"), now);
    let order = capacity_order("sol", "SOLUSD", 80.0, 20.0, TradeUpdateEvent::PartialFilled);
    let asset = order.asset.clone();
    cache.seed_orders_for_test(vec![order]);
    cache.seed_positions_for_test(vec![Position {
        account_id: "account".into(),
        order_id: Some("sol".into()),
        asset,
        avg_entry_price: 100.0,
        qty: 20.0,
        side: crate::core::broker::types::OrderSide::Buy,
        market_value: 2_000.0,
        cost_basis: 2_000.0,
        current_price: 100.0,
        unrealized_pnl: 0.0,
        realized_pnl: 0.0,
        entry_commission: 0.0,
        margin_required: Some(20.0),
    }]);
    let observed = cache
        .with_fresh_entry_capacity("account", false, Some("SOLUSD"), |_, _, _, symbol| symbol)
        .unwrap()
        .unwrap();
    assert_eq!(observed.0, 100.0);
}

#[test]
fn exact_order_lookup_preserves_split_positions_and_rejects_stale_or_wrong_identity() {
    let cache=Arc::new(cache());
    let now=Instant::now();
    cache.seed_account_for_test(account("account"),now);
    let make=|id:&str,pnl| Position {
        account_id:"account".into(),order_id:Some(id.into()),asset:capacity_order("a","SOLUSD",1.,1.,TradeUpdateEvent::Filled).asset,
        avg_entry_price:100.,qty:1.,side:crate::core::broker::types::OrderSide::Sell,
        market_value:99.,cost_basis:100.,current_price:99.,unrealized_pnl:pnl,realized_pnl:0.,entry_commission:0.,margin_required:None,
    };
    cache.seed_positions_for_test(vec![make("first",7.5),make("second",11.2)]);
    assert_eq!(cache.position_by_order_id("first","SOLUSD").unwrap().unwrap().unrealized_pnl,7.5);
    assert_eq!(cache.position_by_order_id("second","SOLUSD").unwrap().unwrap().unrealized_pnl,11.2);
    assert!(cache.position_by_order_id("absent","SOLUSD").unwrap().is_none());
    assert!(matches!(cache.position_by_order_id("first","BTCUSD"),Err(SnapshotUnavailable::Invalid)));
    // A malformed duplicate cannot replace the last good observation.
    cache.seed_positions_for_test(vec![make("first",99.),make("first",100.)]);
    assert_eq!(cache.position_by_order_id("first","SOLUSD").unwrap().unwrap().unrealized_pnl,7.5);
    let mut insight=crate::core::insight::Insight::default();
    insight.state=crate::core::insight::types::InsightState::Filled;
    insight.symbol="SOLUSD".into();insight.order_id=Some("first".into());
    let reader=cache.clone();
    insight.bind_context(crate::core::insight::InsightStrategyContext::new(chrono::Utc::now).with_position_reader(Some(Arc::new(move |order,symbol|
        reader.position_by_order_id(order,symbol).map_err(|reason|super::super::types::BrokerError::SnapshotUnavailable {kind:SnapshotKind::Positions,reason})))));
    let fingerprint=insight.snapshot_fingerprint_hash();
    assert_eq!(insight.get_position().unwrap().unwrap().unrealized_pnl,7.5);
    cache.seed_positions_for_test(vec![make("first",9.5),make("second",11.2)]);
    assert_eq!(insight.get_position().unwrap().unwrap().unrealized_pnl,9.5);
    assert_eq!(insight.snapshot_fingerprint_hash(),fingerprint); // Snapshot changes, Insight does not.
    let stale=now-Duration::from_secs(60);
    cache.state.write().positions.as_mut().unwrap().observed=stale;
    assert!(matches!(cache.position_by_order_id("first","SOLUSD"),Err(SnapshotUnavailable::Stale)));
}

#[test]
fn order_capacity_requires_fresh_account_and_fences_invalidated_responses() {
    let cache = cache();
    let generation = cache.generation();
    let now = Instant::now();
    cache.publish_orders(generation, now, vec![]);
    assert!(matches!(
        cache.order_capacity(),
        Err(SnapshotUnavailable::Missing)
    ));
    cache.seed_account_for_test(account("account"), now);
    assert_eq!(cache.order_capacity().unwrap().pending_orders, 0);
    cache.invalidate();
    cache.seed_account_for_test(account("account"), Instant::now());
    cache.publish_orders(generation, Instant::now(), vec![]);
    assert!(matches!(
        cache.order_capacity(),
        Err(SnapshotUnavailable::Missing)
    ));
    cache.publish_orders(
        cache.generation(),
        Instant::now() - Duration::from_secs(30),
        vec![],
    );
    assert!(matches!(
        cache.order_capacity(),
        Err(SnapshotUnavailable::Stale)
    ));
    cache.publish_orders(cache.generation(), Instant::now(), vec![]);
    assert!(cache.order_capacity().is_ok());
    cache.seed_account_for_test(account("different"), Instant::now());
    assert!(matches!(
        cache.order_capacity(),
        Err(SnapshotUnavailable::AccountChanged)
    ));
}

pub(super) fn account(id: &str) -> Account {
    Account {
        account_id: id.into(),
        account_type: AccountType::Paper,
        equity: 100_000.0,
        cash: 100_000.0,
        currency: "USD".into(),
        buying_power: 200_000.0,
        accrued_commission: 0.0,
        shorting_enabled: true,
        leverage: 2,
        max_total_orders: None,
    }
}
pub(super) fn quote(symbol: &str) -> Quote {
    Quote {
        profit_model: None,
        symbol: symbol.into(),
        bid: 100.0,
        ask: 101.0,
        bid_size: 1.0,
        ask_size: 1.0,
        last: Some(100.5),
        last_size: Some(1.0),
        timestamp: chrono::Utc::now(),
    }
}
fn cache() -> BrokerSnapshotCache {
    BrokerSnapshotCache::new(" account ", Default::default()).unwrap()
}
fn membership(cache: &BrokerSnapshotCache, symbol: &str) -> u64 {
    cache.state.read().quotes[symbol].membership
}

fn native_source(cache: &BrokerSnapshotCache, symbol: &str) -> SnapshotReadRevision {
    cache.dependency_revision(&SnapshotKind::Quote {
        symbol: symbol.into(),
    })
}

#[test]
fn native_cache_poll_suppression_is_per_symbol_and_expires_at_grace_boundary() {
    let cache = cache();
    cache
        .set_symbols(&["ETHUSD".into(), "SOLUSD".into()])
        .unwrap();
    let worker = cache.claim_worker().unwrap();
    let active = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let now = Instant::now();
    let q = quote("ETHUSD");
    let wall = q.timestamp;
    let source = native_source(&cache, "ETHUSD");
    cache.publish_native_quote(source, now, q, &active);
    let requests = cache.quote_requests_at(worker, now, wall).unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].0, "SOLUSD");
    assert!(!cache.quote_request_current(worker, "ETHUSD", source.membership, source.generation));
    assert_eq!(
        cache
            .quote_requests_at(worker, now + NATIVE_QUOTE_POLL_GRACE, wall)
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn native_cache_stopped_or_dropped_lease_immediately_restores_poll_eligibility() {
    let cache = cache();
    cache.set_symbols(&["ETHUSD".into()]).unwrap();
    let worker = cache.claim_worker().unwrap();
    let source = native_source(&cache, "ETHUSD");
    let active = Arc::new(std::sync::atomic::AtomicBool::new(true));
    cache.publish_native_quote(source, Instant::now(), quote("ETHUSD"), &active);
    assert!(cache.quote_requests(worker).unwrap().is_empty());
    active.store(false, std::sync::atomic::Ordering::Release);
    assert_eq!(cache.quote_requests(worker).unwrap().len(), 1);
    active.store(true, std::sync::atomic::Ordering::Release);
    assert!(cache.quote_requests(worker).unwrap().is_empty());
    drop(active);
    assert_eq!(cache.quote_requests(worker).unwrap().len(), 1);
}

#[test]
fn native_cache_source_age_and_custom_freshness_override_poll_grace() {
    let cache = BrokerSnapshotCache::new(
        "account",
        SnapshotFreshness {
            quote: Duration::from_millis(100),
            ..Default::default()
        },
    )
    .unwrap();
    cache.set_symbols(&["ETHUSD".into()]).unwrap();
    let worker = cache.claim_worker().unwrap();
    let active = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let now = Instant::now();
    let q = quote("ETHUSD");
    let wall = q.timestamp;
    cache.publish_native_quote(native_source(&cache, "ETHUSD"), now, q, &active);
    assert!(
        cache
            .quote_requests_at(worker, now, wall)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        cache
            .quote_requests_at(worker, now + Duration::from_millis(100), wall)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        cache
            .quote_requests_at(worker, now, wall + chrono::Duration::milliseconds(101))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn native_cache_late_poll_cannot_replace_new_native_observation() {
    let cache = cache();
    cache.set_symbols(&["ETHUSD".into()]).unwrap();
    let worker = cache.claim_worker().unwrap();
    let source = native_source(&cache, "ETHUSD");
    let active = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let before = Instant::now() - Duration::from_millis(10);
    let mut native = quote("ETHUSD");
    native.bid = 99.0;
    let mut response = native.clone();
    response.bid = 98.0;
    cache.publish_native_quote(source, Instant::now(), native, &active);
    cache.publish_quote(
        "ETHUSD",
        source.membership,
        source.generation,
        before,
        response,
    );
    assert_eq!(cache.quote("ETHUSD").unwrap().bid, 99.0);
    assert!(cache.quote_requests(worker).unwrap().is_empty());
}

#[test]
fn native_cache_obsolete_membership_generation_or_cache_cannot_suppress_recovery() {
    let cache = cache();
    cache.set_symbols(&["ETHUSD".into()]).unwrap();
    let worker = cache.claim_worker().unwrap();
    let source = native_source(&cache, "ETHUSD");
    let active = Arc::new(std::sync::atomic::AtomicBool::new(true));
    cache.publish_native_quote(source, Instant::now(), quote("ETHUSD"), &active);
    cache.invalidate();
    cache.publish_native_quote(source, Instant::now(), quote("ETHUSD"), &active);
    assert_eq!(cache.quote_requests(worker).unwrap().len(), 1);
    let source = native_source(&cache, "ETHUSD");
    cache.set_symbols(&[]).unwrap();
    cache.set_symbols(&["ETHUSD".into()]).unwrap();
    cache.publish_native_quote(source, Instant::now(), quote("ETHUSD"), &active);
    assert_eq!(cache.quote_requests(worker).unwrap().len(), 1);
    let other = BrokerSnapshotCache::new("account", Default::default()).unwrap();
    other.set_symbols(&["ETHUSD".into()]).unwrap();
    cache.publish_native_quote(
        native_source(&other, "ETHUSD"),
        Instant::now(),
        quote("ETHUSD"),
        &active,
    );
    assert!(cache.quote("ETHUSD").is_err());
    assert_eq!(cache.quote_requests(worker).unwrap().len(), 1);
}

#[test]
fn native_cache_invalid_or_disabled_observation_cannot_suppress_polling() {
    let cache = cache();
    cache.set_symbols(&["ETHUSD".into()]).unwrap();
    let worker = cache.claim_worker().unwrap();
    let source = native_source(&cache, "ETHUSD");
    let active = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut invalid = quote("ETHUSD");
    invalid.bid = f64::NAN;
    cache.publish_native_quote(source, Instant::now(), invalid, &active);
    assert_eq!(cache.quote_requests(worker).unwrap().len(), 1);
    active.store(false, std::sync::atomic::Ordering::Release);
    cache.publish_native_quote(source, Instant::now(), quote("ETHUSD"), &active);
    assert!(cache.quote("ETHUSD").is_err());
}

#[test]
fn snapshot_dependency_revisions_are_scoped_to_account_positions_and_each_symbol() {
    let cache = cache();
    cache
        .set_symbols(&["ETHUSD".into(), "SOLUSD".into()])
        .unwrap();
    let kinds = [
        SnapshotKind::Account,
        SnapshotKind::Positions,
        SnapshotKind::Quote {
            symbol: "ETHUSD".into(),
        },
        SnapshotKind::Quote {
            symbol: "SOLUSD".into(),
        },
    ];
    let revisions = || kinds.each_ref().map(|kind| cache.dependency_revision(kind));
    let before = revisions();
    cache.publish_quote(
        "ETHUSD",
        membership(&cache, "ETHUSD"),
        0,
        Instant::now(),
        quote("ETHUSD"),
    );
    let after_quote = revisions();
    assert_eq!(before[0], after_quote[0]);
    assert_eq!(before[1], after_quote[1]);
    assert_ne!(before[2], after_quote[2]);
    assert_eq!(before[3], after_quote[3]);
    cache.publish_account(0, Instant::now(), account("account"));
    let after_account = revisions();
    assert_ne!(after_account[0], after_quote[0]);
    assert_ne!(after_account[1], after_quote[1]); // account validation gates positions
    assert_eq!(after_account[2..], after_quote[2..]);
    cache.publish_positions(0, Instant::now(), vec![]);
    let after_positions = revisions();
    assert_eq!(after_positions[0], after_account[0]);
    assert_ne!(after_positions[1], after_account[1]);
    assert_eq!(after_positions[2..], after_account[2..]);
    cache.invalidate();
    assert!(
        revisions()
            .iter()
            .zip(after_positions)
            .all(|(new, old)| *new != old)
    );
}

#[test]
fn snapshot_dependency_revisions_reject_invalid_old_data_and_fence_membership_replacement() {
    let cache = cache();
    cache.set_symbols(&["ETHUSD".into()]).unwrap();
    let kind = SnapshotKind::Quote {
        symbol: "ETHUSD".into(),
    };
    let observed = Instant::now();
    let member = membership(&cache, "ETHUSD");
    cache.publish_quote("ETHUSD", member, 0, observed, quote("ETHUSD"));
    let accepted = cache.dependency_revision(&kind);
    cache.publish_quote("ETHUSD", member, 0, observed, quote("SOLUSD"));
    cache.publish_quote(
        "ETHUSD",
        member,
        0,
        observed - Duration::from_secs(1),
        quote("ETHUSD"),
    );
    assert_eq!(accepted, cache.dependency_revision(&kind));
    cache.set_symbols(&[]).unwrap();
    cache.set_symbols(&["ETHUSD".into()]).unwrap();
    let replacement = cache.dependency_revision(&kind);
    assert_ne!(accepted, replacement);
    cache.publish_quote("ETHUSD", member, 0, Instant::now(), quote("ETHUSD"));
    assert_eq!(replacement, cache.dependency_revision(&kind));
    let other = BrokerSnapshotCache::new("account", Default::default()).unwrap();
    assert_ne!(
        cache.dependency_revision(&SnapshotKind::Account),
        other.dependency_revision(&SnapshotKind::Account)
    );
}

#[test]
fn snapshot_reads_are_shared_and_freshness_begins_at_dispatch() {
    let cache = cache();
    assert_eq!(cache.account().unwrap_err(), SnapshotUnavailable::Missing);
    let observed = Instant::now();
    cache.publish_account(0, observed, account("account"));
    let first = cache.account_at(observed).unwrap();
    assert!(Arc::ptr_eq(
        &first,
        &cache.account_at(observed + Duration::from_secs(5)).unwrap()
    ));
    assert_eq!(
        cache
            .account_at(observed + Duration::from_secs(6))
            .unwrap_err(),
        SnapshotUnavailable::Stale
    );
    assert_eq!(
        cache
            .account_at(observed - Duration::from_secs(1))
            .unwrap_err(),
        SnapshotUnavailable::Stale
    );
    cache.publish_positions(0, observed, vec![]);
    assert!(cache.positions().unwrap().is_empty());
    assert!(Arc::ptr_eq(
        &cache.positions().unwrap(),
        &cache.positions().unwrap()
    ));
}

#[test]
fn mutation_poll_guard_requires_fresh_exact_account_and_never_polls_on_failure() {
    let cache = cache();
    let polled = std::cell::Cell::new(false);
    let poll = || polled.set(true);
    assert_eq!(
        cache.with_fresh_account("account", poll),
        Err(SnapshotUnavailable::Missing)
    );
    cache.seed_account_for_test(account("account"), Instant::now() - Duration::from_secs(6));
    assert_eq!(
        cache.with_fresh_account("account", poll),
        Err(SnapshotUnavailable::Stale)
    );
    cache.seed_account_for_test(account("account"), Instant::now());
    assert_eq!(
        cache.with_fresh_account("ACCOUNT", poll),
        Err(SnapshotUnavailable::AccountChanged)
    );
    assert!(!polled.get());
    cache.with_fresh_account("account", poll).unwrap();
    assert!(polled.replace(false));
    cache.seed_account_for_test(account("other-account"), Instant::now());
    assert_eq!(
        cache.with_fresh_account("account", poll),
        Err(SnapshotUnavailable::AccountChanged)
    );
    assert!(!polled.get());
}

#[tokio::test]
async fn admission_notifications_are_independent_and_ignore_quote_only_updates() {
    use futures::FutureExt;
    let cache = cache();
    let mutation = cache.mutation_notifier();
    let owner = cache.notifier();
    cache.set_symbols(&["ETHUSD".into()]).unwrap();
    cache.publish_account(0, Instant::now(), account("account"));
    assert!(owner.notified().now_or_never().is_some());
    assert!(mutation.notified().now_or_never().is_some());
    cache.publish_positions(0, Instant::now(), vec![]);
    cache.publish_quote(
        "ETHUSD",
        membership(&cache, "ETHUSD"),
        0,
        Instant::now(),
        quote("ETHUSD"),
    );
    assert!(owner.notified().now_or_never().is_some());
    assert!(mutation.notified().now_or_never().is_none());
    cache.invalidate();
    assert!(owner.notified().now_or_never().is_some());
    assert!(mutation.notified().now_or_never().is_some());
}

#[test]
fn invalidation_fences_late_reads_and_positions_require_account_validation() {
    let cache = cache();
    let now = Instant::now();
    cache.publish_positions(0, now, vec![]);
    assert_eq!(cache.positions().unwrap_err(), SnapshotUnavailable::Missing);
    cache.publish_account(0, now, account("account"));
    assert!(cache.positions().is_ok());
    cache.invalidate();
    cache.publish_account(0, now, account("account"));
    cache.publish_positions(0, now, vec![]);
    assert_eq!(cache.account().unwrap_err(), SnapshotUnavailable::Missing);
    assert_eq!(cache.positions().unwrap_err(), SnapshotUnavailable::Missing);
    cache.publish_account(cache.generation(), now, account("account"));
    assert!(cache.account().is_ok());
}

#[test]
fn account_change_is_latched_and_cannot_be_cleared_by_old_success_or_invalidation() {
    let cache = cache();
    cache.publish_account(0, Instant::now(), account("different"));
    cache.publish_account(0, Instant::now(), account("account"));
    cache.invalidate();
    cache.publish_account(cache.generation(), Instant::now(), account("account"));
    assert_eq!(
        cache.account().unwrap_err(),
        SnapshotUnavailable::AccountChanged
    );
    assert_eq!(
        cache.positions().unwrap_err(),
        SnapshotUnavailable::AccountChanged
    );
    assert_eq!(
        cache.claim_worker().unwrap_err(),
        SnapshotUnavailable::AccountChanged
    );
}

#[test]
fn quote_membership_is_bounded_and_readd_cannot_accept_removed_requests() {
    let cache = cache();
    cache
        .set_symbols(&["ETHUSD".into(), "ETHUSD".into()])
        .unwrap();
    assert_eq!(cache.state.read().quotes.len(), 1);
    let token = membership(&cache, "ETHUSD");
    let now = Instant::now();
    cache.publish_quote("ETHUSD", token, 0, now, quote("ETHUSD"));
    assert!(cache.quote("ETHUSD").is_ok());
    cache.set_symbols(&[]).unwrap();
    assert_eq!(
        cache.quote("ETHUSD").unwrap_err(),
        SnapshotUnavailable::UntrackedSymbol
    );
    cache.set_symbols(&["ETHUSD".into()]).unwrap();
    cache.publish_quote("ETHUSD", token, 0, now, quote("ETHUSD"));
    assert_eq!(
        cache.quote("ETHUSD").unwrap_err(),
        SnapshotUnavailable::Missing
    );
    assert!(
        cache
            .set_symbols(&vec!["oversized".into(); MAX_SNAPSHOT_SYMBOLS + 1])
            .is_err()
    );
    assert_eq!(
        cache.state.read().quotes.len(),
        1,
        "invalid replacement must be atomic"
    );
}

#[test]
fn quote_age_uses_both_local_observation_and_source_time_without_price_regression() {
    let cache = cache();
    cache.set_symbols(&["ETHUSD".into()]).unwrap();
    let token = membership(&cache, "ETHUSD");
    let now = Instant::now();
    let current = quote("ETHUSD");
    let wall = current.timestamp;
    cache.publish_quote("ETHUSD", token, 0, now, current.clone());
    assert!(cache.quote_at("ETHUSD", now, wall).is_ok());
    let mut old = current.clone();
    old.timestamp -= chrono::Duration::seconds(1);
    old.bid = 90.0;
    cache.publish_quote("ETHUSD", token, 0, now + Duration::from_secs(1), old);
    assert_eq!(cache.quote_at("ETHUSD", now, wall).unwrap().bid, 100.0);
    assert_eq!(
        cache
            .quote_at("ETHUSD", now + Duration::from_secs(6), wall)
            .unwrap_err(),
        SnapshotUnavailable::Stale
    );
    assert_eq!(
        cache
            .quote_at("ETHUSD", now, wall + chrono::Duration::seconds(6))
            .unwrap_err(),
        SnapshotUnavailable::Stale
    );
    assert_eq!(
        cache
            .quote_at("ETHUSD", now, wall - chrono::Duration::seconds(2))
            .unwrap_err(),
        SnapshotUnavailable::Invalid
    );
    let mut invalid = current;
    invalid.bid = f64::NAN;
    cache.publish_quote("ETHUSD", token, 0, now, invalid);
    assert!(cache.quote_at("ETHUSD", now, wall).unwrap().bid.is_finite());
}

#[test]
fn worker_ownership_and_release_fence_current_and_future_requests() {
    let cache = cache();
    cache.set_symbols(&["ETHUSD".into()]).unwrap();
    let first = cache.claim_worker().unwrap();
    assert!(cache.claim_worker().is_err());
    let generation = cache.request_generation(first).unwrap();
    let token = membership(&cache, "ETHUSD");
    cache.release_worker(first);
    assert!(cache.quote_requests(first).is_none());
    assert!(cache.request_generation(first).is_none());
    cache.publish_account(generation, Instant::now(), account("account"));
    assert_eq!(cache.account().unwrap_err(), SnapshotUnavailable::Missing);
    let second = cache.claim_worker().unwrap();
    cache.release_worker(first);
    assert!(cache.request_generation(second).is_some());
    assert!(!cache.quote_request_current(first, "ETHUSD", token, cache.generation()));
    cache.publish_quote("ETHUSD", token, generation, Instant::now(), quote("ETHUSD"));
    assert_eq!(
        cache.quote("ETHUSD").unwrap_err(),
        SnapshotUnavailable::Missing
    );
}
