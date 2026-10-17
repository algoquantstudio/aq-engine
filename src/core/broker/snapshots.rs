//! Bounded, transport-free reads for synchronous strategy decisions. Refresh
//! workers own I/O; snapshots do not mutate Insights or consume trading events.
use super::types::{Account, Order, Position, Quote, TradeUpdateEvent};
use parking_lot::RwLock;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

mod worker;
pub use worker::{SnapshotRefreshConfig, SnapshotRefreshHealth, SnapshotRefreshWorker};

pub const MAX_SNAPSHOT_SYMBOLS: usize = 4_096;
pub const MAX_SNAPSHOT_POSITIONS: usize = 65_536;

/// Compact broker pending-order capacity, excluding positions and history.
#[derive(Clone, Debug)]
pub struct OrderCapacitySnapshot {
    pub pending_orders: usize,
    pub pending_quantity_by_symbol: HashMap<String, f64>,
}

fn pending_order_count<'a>(
    rows: impl IntoIterator<Item = (&'a str, &'a TradeUpdateEvent)>,
) -> Option<usize> {
    let mut seen = HashMap::new();
    let mut count = 0;
    for (index, (id, status)) in rows.into_iter().enumerate() {
        if index >= MAX_SNAPSHOT_POSITIONS || id.trim().is_empty() || id.len() > 512 {
            return None;
        }
        let pending = matches!(
            status,
            TradeUpdateEvent::Accepted
                | TradeUpdateEvent::New
                | TradeUpdateEvent::PendingNew
                | TradeUpdateEvent::Pending
                | TradeUpdateEvent::PartialFilled
                | TradeUpdateEvent::Replaced
        );
        match seen.insert(id, pending) {
            Some(previous) if previous != pending => return None,
            Some(_) => {}
            None => count += usize::from(pending),
        }
    }
    Some(count)
}

fn order_capacity(orders: &[Order]) -> Option<OrderCapacitySnapshot> {
    let pending_orders = pending_order_count(
        orders
            .iter()
            .map(|order| (order.order_id.as_str(), &order.status)),
    )?;
    let mut seen = HashSet::with_capacity(orders.len().min(MAX_SNAPSHOT_POSITIONS));
    let mut pending_quantity_by_symbol = HashMap::<String, f64>::new();
    for order in orders {
        if !seen.insert(order.order_id.as_str()) {
            continue;
        }
        if !matches!(
            order.status,
            TradeUpdateEvent::Accepted
                | TradeUpdateEvent::New
                | TradeUpdateEvent::PendingNew
                | TradeUpdateEvent::Pending
                | TradeUpdateEvent::PartialFilled
                | TradeUpdateEvent::Replaced
        ) {
            continue;
        }
        let symbol = order.asset.symbol.trim();
        // Order and position snapshots are independent. Count the complete
        // still-active request so a partial-fill race can over-reserve briefly
        // but can never undercount the broker's aggregate symbol exposure.
        let reserved = order.qty.abs();
        if symbol.is_empty()
            || symbol.len() > 128
            || !order.qty.is_finite()
            || !order.filled_qty.is_finite()
            || reserved <= 0.0
        {
            return None;
        }
        let total = pending_quantity_by_symbol
            .entry(symbol.to_owned())
            .or_default();
        *total += reserved;
        if !total.is_finite() || pending_quantity_by_symbol.len() > MAX_SNAPSHOT_SYMBOLS {
            return None;
        }
    }
    Some(OrderCapacitySnapshot {
        pending_orders,
        pending_quantity_by_symbol,
    })
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub enum SnapshotKind {
    Account,
    Positions,
    Quote { symbol: String },
}

/// A dependency-local version, scoped to one cache/feed lifetime. An equal
/// symbol in a replacement cache is not the same observation. Runtime-only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotReadRevision {
    cache_id: uuid::Uuid,
    generation: u64,
    membership: u64,
    version: u64,
}

impl SnapshotReadRevision {
    pub(crate) fn same_subscription(self, other: Self) -> bool {
        self.cache_id == other.cache_id && self.membership == other.membership
    }
    pub(crate) fn same_source_epoch(self, other: Self) -> bool {
        self.same_subscription(other) && self.generation == other.generation
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SnapshotUnavailable {
    Missing,
    Stale,
    Invalid,
    AccountChanged,
    UntrackedSymbol,
}

impl std::fmt::Display for SnapshotUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "snapshot unavailable: {self:?}")
    }
}
impl std::error::Error for SnapshotUnavailable {}

#[derive(Clone, Copy, Debug)]
pub struct SnapshotFreshness {
    pub account: Duration,
    pub positions: Duration,
    pub quote: Duration,
    pub future_quote_tolerance: Duration,
}
impl Default for SnapshotFreshness {
    fn default() -> Self {
        Self {
            account: Duration::from_secs(5),
            positions: Duration::from_secs(5),
            quote: Duration::from_secs(5),
            future_quote_tolerance: Duration::from_secs(1),
        }
    }
}

struct Timed<T: ?Sized> {
    value: Arc<T>,
    // Age begins at request dispatch, not completion: a slow response does not
    // become freshly observed merely because its await finally returned.
    observed: Instant,
}
impl<T: ?Sized> Timed<T> {
    fn read(&self, now: Instant, max_age: Duration) -> Result<Arc<T>, SnapshotUnavailable> {
        match now.checked_duration_since(self.observed) {
            Some(age) if age <= max_age => Ok(Arc::clone(&self.value)),
            _ => Err(SnapshotUnavailable::Stale),
        }
    }
}

struct QuoteSlot {
    membership: u64,
    revision: u64,
    value: Option<Timed<Quote>>,
    native: Option<NativeQuoteObservation>,
}

struct NativeQuoteObservation {
    observed: Instant,
    lease: std::sync::Weak<std::sync::atomic::AtomicBool>,
}

// Suppress redundant polling only while a live lease is actively delivering.
// This is not the heartbeat cadence or a substitute for source freshness.
const NATIVE_QUOTE_POLL_GRACE: Duration = Duration::from_secs(2);
struct State {
    generation: u64,
    next_revision: u64,
    account_revision: u64,
    positions_revision: u64,
    next_worker: u64,
    worker: Option<u64>,
    closed: bool,
    next_membership: u64,
    account_changed: bool,
    account: Option<Timed<Account>>,
    positions: Option<Timed<[Position]>>,
    orders: Option<Timed<OrderCapacitySnapshot>>,
    capacity_symbols: HashSet<String>,
    quotes: HashMap<String, QuoteSlot>,
}

/// All account/position entries belong to exactly one expected account. A
/// confirmed identity mismatch is latched until a new scoped cache is created.
/// Quote membership is bounded and updates in place, not once per insight/tick.
pub struct BrokerSnapshotCache {
    identity: uuid::Uuid,
    expected_account: String,
    freshness: SnapshotFreshness,
    state: RwLock<State>,
    ready: Arc<tokio::sync::Notify>,
    mutation_ready: Arc<tokio::sync::Notify>,
    revision: std::sync::atomic::AtomicU64,
}

impl BrokerSnapshotCache {
    pub fn new(
        account_id: &str,
        freshness: SnapshotFreshness,
    ) -> Result<Self, SnapshotUnavailable> {
        if account_id.trim().is_empty()
            || freshness.account.is_zero()
            || freshness.positions.is_zero()
            || freshness.quote.is_zero()
        {
            return Err(SnapshotUnavailable::Invalid);
        }
        Ok(Self {
            identity: uuid::Uuid::new_v4(),
            expected_account: account_id.trim().into(),
            freshness,
            state: RwLock::new(State {
                generation: 0,
                next_revision: 0,
                account_revision: 0,
                positions_revision: 0,
                next_worker: 0,
                worker: None,
                closed: false,
                next_membership: 0,
                account_changed: false,
                account: None,
                positions: None,
                orders: None,
                capacity_symbols: HashSet::new(),
                quotes: HashMap::new(),
            }),
            ready: Arc::new(tokio::sync::Notify::new()),
            mutation_ready: Arc::new(tokio::sync::Notify::new()),
            revision: std::sync::atomic::AtomicU64::new(0),
        })
    }

    pub fn notifier(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.ready)
    }

    pub fn revision(&self) -> u64 {
        self.revision.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn dependency_revision(&self, kind: &SnapshotKind) -> SnapshotReadRevision {
        let state = self.state.read();
        let version = match kind {
            SnapshotKind::Account => state.account_revision,
            SnapshotKind::Positions => state.positions_revision,
            SnapshotKind::Quote { symbol } => {
                state.quotes.get(symbol).map_or(0, |slot| slot.revision)
            }
        };
        let membership = match kind {
            SnapshotKind::Quote { symbol } => {
                state.quotes.get(symbol).map_or(0, |slot| slot.membership)
            }
            _ => 0,
        };
        SnapshotReadRevision {
            cache_id: self.identity,
            generation: state.generation,
            membership,
            version,
        }
    }

    pub(crate) fn mutation_notifier(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.mutation_ready)
    }

    fn changed(&self) {
        self.revision
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        self.ready.notify_one();
    }

    fn admission_changed(&self) {
        self.changed();
        // Separate retained permits: the strategy's deferred-pipe listener
        // must not consume the mutation worker's account refresh wakeup. Quote
        // and position-only updates must not wake account admission retries.
        self.mutation_ready.notify_one();
    }

    pub fn account(&self) -> Result<Arc<Account>, SnapshotUnavailable> {
        self.account_at(Instant::now())
    }

    /// Linearize the first (non-blocking) mutation poll with cache invalidation
    /// and account changes. Never hold this read guard across an await.
    pub(crate) fn with_fresh_account<T>(
        &self,
        expected_account: &str,
        poll: impl FnOnce() -> T,
    ) -> Result<T, SnapshotUnavailable> {
        self.with_fresh_account_value(expected_account, |_| poll())
    }

    /// Inspect risk and make the first broker poll under the same snapshot
    /// guard, so refresh/invalidation cannot slip between validation and use.
    pub(crate) fn with_fresh_account_value<T>(
        &self,
        expected_account: &str,
        poll: impl FnOnce(&Account) -> T,
    ) -> Result<T, SnapshotUnavailable> {
        self.with_fresh_account_capacity(expected_account, false, |account, _| poll(account))
    }

    pub(crate) fn with_fresh_account_capacity<T>(
        &self,
        expected_account: &str,
        needs_pending_slot: bool,
        poll: impl FnOnce(&Account, Option<(usize, Instant)>) -> T,
    ) -> Result<T, SnapshotUnavailable> {
        let state = self.state.read();
        if state.account_changed || self.expected_account != expected_account {
            return Err(SnapshotUnavailable::AccountChanged);
        }
        if state.closed {
            return Err(SnapshotUnavailable::Missing);
        }
        let account = state
            .account
            .as_ref()
            .ok_or(SnapshotUnavailable::Missing)?
            .read(Instant::now(), self.freshness.account)?;
        let count = if needs_pending_slot && account.max_total_orders.is_some() {
            let orders = state.orders.as_ref().ok_or(SnapshotUnavailable::Missing)?;
            Some((
                orders
                    .read(Instant::now(), self.freshness.positions)?
                    .pending_orders,
                orders.observed,
            ))
        } else {
            None
        };
        Ok(poll(&account, count))
    }

    pub(crate) fn with_fresh_entry_capacity<T>(
        &self,
        expected_account: &str,
        needs_pending_slot: bool,
        symbol: Option<&str>,
        poll: impl FnOnce(&Account, Instant, Option<(usize, Instant)>, Option<(f64, Instant)>) -> T,
    ) -> Result<T, SnapshotUnavailable> {
        let state = self.state.read();
        if state.account_changed || self.expected_account != expected_account {
            return Err(SnapshotUnavailable::AccountChanged);
        }
        if state.closed {
            return Err(SnapshotUnavailable::Missing);
        }
        let now = Instant::now();
        let account = state
            .account
            .as_ref()
            .ok_or(SnapshotUnavailable::Missing)?
            .read(now, self.freshness.account)?;
        let needs_orders =
            needs_pending_slot && account.max_total_orders.is_some() || symbol.is_some();
        let orders = if needs_orders {
            Some(state.orders.as_ref().ok_or(SnapshotUnavailable::Missing)?)
        } else {
            None
        };
        let pending = if needs_pending_slot && account.max_total_orders.is_some() {
            let orders = orders.expect("orders required for pending capacity");
            Some((
                orders.read(now, self.freshness.positions)?.pending_orders,
                orders.observed,
            ))
        } else {
            None
        };
        let symbol_quantity = if let Some(symbol) = symbol {
            let orders = orders.expect("orders required for symbol capacity");
            let orders_value = orders.read(now, self.freshness.positions)?;
            let positions = state
                .positions
                .as_ref()
                .ok_or(SnapshotUnavailable::Missing)?;
            let positions_value = positions.read(now, self.freshness.positions)?;
            let open = positions_value
                .iter()
                .filter(|position| position.asset.symbol == symbol)
                .map(|position| position.qty.abs())
                .sum::<f64>();
            let pending = orders_value
                .pending_quantity_by_symbol
                .get(symbol)
                .copied()
                .unwrap_or_default();
            let total = open + pending;
            if !total.is_finite() {
                return Err(SnapshotUnavailable::Invalid);
            }
            Some((total, orders.observed.min(positions.observed)))
        } else {
            None
        };
        let account_observed = state
            .account
            .as_ref()
            .expect("account was validated above")
            .observed;
        Ok(poll(&account, account_observed, pending, symbol_quantity))
    }
    fn account_at(&self, now: Instant) -> Result<Arc<Account>, SnapshotUnavailable> {
        let state = self.state.read();
        if state.account_changed {
            return Err(SnapshotUnavailable::AccountChanged);
        }
        state
            .account
            .as_ref()
            .ok_or(SnapshotUnavailable::Missing)?
            .read(now, self.freshness.account)
    }

    pub fn positions(&self) -> Result<Arc<[Position]>, SnapshotUnavailable> {
        let state = self.state.read();
        if state.account_changed {
            return Err(SnapshotUnavailable::AccountChanged);
        }
        let now = Instant::now();
        // Positions cannot be used before the expected account is validated, or
        // after its validation expires, even if the positions request succeeded.
        state
            .account
            .as_ref()
            .ok_or(SnapshotUnavailable::Missing)?
            .read(now, self.freshness.account)?;
        state
            .positions
            .as_ref()
            .ok_or(SnapshotUnavailable::Missing)?
            .read(now, self.freshness.positions)
    }

    pub fn order_capacity(&self) -> Result<Arc<OrderCapacitySnapshot>, SnapshotUnavailable> {
        let state = self.state.read();
        if state.account_changed {
            return Err(SnapshotUnavailable::AccountChanged);
        }
        if state.closed {
            return Err(SnapshotUnavailable::Missing);
        }
        let now = Instant::now();
        state
            .account
            .as_ref()
            .ok_or(SnapshotUnavailable::Missing)?
            .read(now, self.freshness.account)?;
        state
            .orders
            .as_ref()
            .ok_or(SnapshotUnavailable::Missing)?
            .read(now, self.freshness.positions)
    }

    fn publish_orders(&self, generation: u64, observed: Instant, orders: Vec<Order>) {
        if orders.len() > MAX_SNAPSHOT_POSITIONS {
            return;
        }
        let Some(value) = order_capacity(&orders) else {
            return;
        };
        let mut state = self.state.write();
        if state.generation != generation
            || state.closed
            || state.account_changed
            || state
                .orders
                .as_ref()
                .is_some_and(|old| old.observed > observed)
        {
            return;
        }
        state.orders = Some(Timed {
            value: Arc::new(value),
            observed,
        });
        drop(state);
        self.admission_changed();
    }

    pub fn quote(&self, symbol: &str) -> Result<Arc<Quote>, SnapshotUnavailable> {
        self.quote_at(symbol, Instant::now(), chrono::Utc::now())
    }
    pub(crate) fn execution_quote_fresh(&self, quote: &Quote, received: Instant) -> bool {
        received.elapsed() <= self.freshness.quote
            && self.quote_timestamp_fresh(quote, chrono::Utc::now())
    }

    fn quote_timestamp_fresh(&self, quote: &Quote, now: chrono::DateTime<chrono::Utc>) -> bool {
        let age = now.signed_duration_since(quote.timestamp);
        if age >= chrono::Duration::zero() {
            age.to_std().is_ok_and(|age| age <= self.freshness.quote)
        } else {
            (-age)
                .to_std()
                .is_ok_and(|ahead| ahead <= self.freshness.future_quote_tolerance)
        }
    }
    fn quote_at(
        &self,
        symbol: &str,
        now: Instant,
        wall_now: chrono::DateTime<chrono::Utc>,
    ) -> Result<Arc<Quote>, SnapshotUnavailable> {
        let state = self.state.read();
        if state.account_changed {
            return Err(SnapshotUnavailable::AccountChanged);
        }
        let slot = state
            .quotes
            .get(symbol)
            .ok_or(SnapshotUnavailable::UntrackedSymbol)?;
        let quote = slot
            .value
            .as_ref()
            .ok_or(SnapshotUnavailable::Missing)?
            .read(now, self.freshness.quote)?;
        let source_age = wall_now.signed_duration_since(quote.timestamp);
        if source_age >= chrono::Duration::zero() {
            if source_age
                .to_std()
                .map_or(true, |age| age > self.freshness.quote)
            {
                return Err(SnapshotUnavailable::Stale);
            }
        } else if (-source_age)
            .to_std()
            .map_or(true, |ahead| ahead > self.freshness.future_quote_tolerance)
        {
            return Err(SnapshotUnavailable::Invalid);
        }
        Ok(quote)
    }

    /// Whole-cache invalidation fences responses dispatched before disconnect
    /// or reconnect. It never clears an account mismatch.
    pub fn invalidate(&self) {
        let mut state = self.state.write();
        state.generation = state
            .generation
            .checked_add(1)
            .expect("snapshot generation exhausted");
        state.account = None;
        state.orders = None;
        let retired = state.positions.take();
        for slot in state.quotes.values_mut() {
            slot.value = None;
        }
        drop(state);
        drop(retired);
        self.admission_changed();
    }

    /// Replace the feed's universe atomically. Duplicate names do not allocate
    /// extra slots; removal releases values, and re-addition gets a new token.
    pub fn set_symbols(&self, symbols: &[String]) -> Result<(), SnapshotUnavailable> {
        if symbols.len() > MAX_SNAPSHOT_SYMBOLS
            || symbols.iter().any(|s| s.is_empty() || s.len() > 128)
        {
            return Err(SnapshotUnavailable::Invalid);
        }
        let unique: HashSet<&str> = symbols.iter().map(String::as_str).collect();
        let mut state = self.state.write();
        state
            .quotes
            .retain(|symbol, _| unique.contains(symbol.as_str()));
        for symbol in unique {
            if !state.quotes.contains_key(symbol) {
                state.next_membership = state
                    .next_membership
                    .checked_add(1)
                    .expect("snapshot membership exhausted");
                let membership = state.next_membership;
                let revision = state.next_revision();
                state.quotes.insert(
                    symbol.into(),
                    QuoteSlot {
                        membership,
                        revision,
                        value: None,
                        native: None,
                    },
                );
            }
        }
        drop(state);
        self.changed();
        Ok(())
    }

    pub fn set_capacity_symbols(&self, symbols: &[String]) -> Result<(), SnapshotUnavailable> {
        if symbols.len() > MAX_SNAPSHOT_SYMBOLS
            || symbols
                .iter()
                .any(|symbol| symbol.trim().is_empty() || symbol.len() > 128)
        {
            return Err(SnapshotUnavailable::Invalid);
        }
        let mut state = self.state.write();
        state.capacity_symbols = symbols.iter().cloned().collect();
        drop(state);
        self.changed();
        Ok(())
    }

    fn requires_order_capacity(&self) -> bool {
        let state = self.state.read();
        !state.capacity_symbols.is_empty()
            || state
                .account
                .as_ref()
                .is_some_and(|account| account.value.max_total_orders.is_some())
    }

    #[cfg(test)]
    fn generation(&self) -> u64 {
        self.state.read().generation
    }
    #[cfg(test)]
    pub(crate) fn seed_account_for_test(&self, account: Account, observed: Instant) {
        self.publish_account(self.generation(), observed, account);
    }

    #[cfg(test)]
    pub(crate) fn seed_orders_for_test(&self, orders: Vec<Order>) {
        self.publish_orders(self.generation(), Instant::now(), orders);
    }
    #[cfg(test)]
    pub(crate) fn seed_positions_for_test(&self, positions: Vec<Position>) {
        self.publish_positions(self.generation(), Instant::now(), positions);
    }
    #[cfg(test)]
    pub(crate) fn seed_quote_for_test(&self, quote: Quote) {
        let symbol = quote.symbol.clone();
        let membership = self.state.read().quotes[&symbol].membership;
        self.publish_quote(
            &symbol,
            membership,
            self.generation(),
            Instant::now(),
            quote,
        );
    }
    fn claim_worker(&self) -> Result<u64, SnapshotUnavailable> {
        let mut state = self.state.write();
        if state.worker.is_some() {
            return Err(SnapshotUnavailable::Invalid);
        }
        if state.account_changed {
            return Err(SnapshotUnavailable::AccountChanged);
        }
        state.next_worker = state
            .next_worker
            .checked_add(1)
            .expect("snapshot worker identity exhausted");
        state.worker = Some(state.next_worker);
        state.closed = false;
        state.generation = state
            .generation
            .checked_add(1)
            .expect("snapshot generation exhausted");
        state.account = None;
        state.orders = None;
        let retired = state.positions.take();
        for slot in state.quotes.values_mut() {
            slot.value = None;
        }
        let worker = state.next_worker;
        drop(state);
        drop(retired);
        self.admission_changed();
        Ok(worker)
    }
    fn release_worker(&self, worker: u64) {
        let mut state = self.state.write();
        if state.worker != Some(worker) {
            return;
        }
        state.worker = None;
        state.closed = true;
        state.generation = state
            .generation
            .checked_add(1)
            .expect("snapshot generation exhausted");
        state.account = None;
        state.orders = None;
        let retired = state.positions.take();
        for slot in state.quotes.values_mut() {
            slot.value = None;
        }
        drop(state);
        drop(retired);
        self.admission_changed();
    }
    fn request_generation(&self, worker: u64) -> Option<u64> {
        let state = self.state.read();
        (state.worker == Some(worker) && !state.closed && !state.account_changed)
            .then_some(state.generation)
    }
    fn quote_requests(&self, worker: u64) -> Option<Vec<(String, u64, u64)>> {
        self.quote_requests_at(worker, Instant::now(), chrono::Utc::now())
    }

    fn native_quote_fresh(
        &self,
        slot: &QuoteSlot,
        now: Instant,
        wall_now: chrono::DateTime<chrono::Utc>,
    ) -> bool {
        let Some(native) = &slot.native else {
            return false;
        };
        native
            .lease
            .upgrade()
            .is_some_and(|lease| lease.load(std::sync::atomic::Ordering::Acquire))
            && now
                .checked_duration_since(native.observed)
                .is_some_and(|age| age < NATIVE_QUOTE_POLL_GRACE.min(self.freshness.quote))
            && slot
                .value
                .as_ref()
                .is_some_and(|value| self.quote_timestamp_fresh(&value.value, wall_now))
    }

    fn quote_requests_at(
        &self,
        worker: u64,
        now: Instant,
        wall_now: chrono::DateTime<chrono::Utc>,
    ) -> Option<Vec<(String, u64, u64)>> {
        let state = self.state.read();
        if state.worker != Some(worker) || state.closed || state.account_changed {
            return None;
        }
        Some(
            state
                .quotes
                .iter()
                .filter(|(_, slot)| !self.native_quote_fresh(slot, now, wall_now))
                .map(|(symbol, slot)| (symbol.clone(), slot.membership, state.generation))
                .collect(),
        )
    }
    fn quote_request_current(
        &self,
        worker: u64,
        symbol: &str,
        membership: u64,
        generation: u64,
    ) -> bool {
        let state = self.state.read();
        state.worker == Some(worker)
            && !state.closed
            && !state.account_changed
            && state.generation == generation
            && state.quotes.get(symbol).is_some_and(|slot| {
                slot.membership == membership
                    && !self.native_quote_fresh(slot, Instant::now(), chrono::Utc::now())
            })
    }

    fn publish_account(&self, generation: u64, observed: Instant, account: Account) {
        let mut retired_positions = None;
        let mut state = self.state.write();
        if state.generation != generation || state.closed || state.account_changed {
            return;
        }
        if account.account_id.trim() != self.expected_account {
            state.account_changed = true;
            state.account = None;
            retired_positions = state.positions.take();
        } else if [
            account.equity,
            account.cash,
            account.buying_power,
            account.accrued_commission,
        ]
        .iter()
        .all(|v| v.is_finite())
        {
            if state
                .account
                .as_ref()
                .is_some_and(|old| old.observed > observed)
            {
                return;
            }
            state.account = Some(Timed {
                value: Arc::new(account),
                observed,
            });
        } else {
            return;
        }
        let revision = state.next_revision();
        state.account_revision = revision;
        // Positions require fresh account validation, so refreshing the account
        // can make an already-cached position snapshot usable again.
        state.positions_revision = revision;
        drop(state);
        drop(retired_positions);
        self.admission_changed();
    }

    fn publish_positions(&self, generation: u64, observed: Instant, positions: Vec<Position>) {
        if positions.len() > MAX_SNAPSHOT_POSITIONS {
            return;
        }
        let account_changed = positions
            .iter()
            .any(|p| p.account_id.trim() != self.expected_account);
        if !account_changed
            && positions.iter().any(|p| {
                [
                    p.qty,
                    p.avg_entry_price,
                    p.current_price,
                    p.market_value,
                    p.cost_basis,
                    p.unrealized_pnl,
                    p.realized_pnl,
                    p.entry_commission,
                ]
                .iter()
                .any(|v| !v.is_finite())
                    || p.margin_required.is_some_and(|v| !v.is_finite())
            })
        {
            return;
        }
        // Validate and allocate outside the lock read by the strategy task.
        let value = (!account_changed).then(|| Timed {
            value: Arc::<[Position]>::from(positions),
            observed,
        });
        let mut state = self.state.write();
        if state.generation != generation
            || state.closed
            || state.account_changed
            || state
                .positions
                .as_ref()
                .is_some_and(|old| old.observed > observed)
        {
            return;
        }
        if account_changed {
            state.account_changed = true;
            state.account = None;
        }
        let retired = std::mem::replace(&mut state.positions, value);
        let revision = state.next_revision();
        state.positions_revision = revision;
        if account_changed {
            state.account_revision = revision;
        }
        drop(state);
        drop(retired);
        if account_changed {
            self.admission_changed();
        } else {
            self.changed();
        }
    }

    fn publish_quote(
        &self,
        symbol: &str,
        membership: u64,
        generation: u64,
        observed: Instant,
        quote: Quote,
    ) {
        self.publish_quote_observation(symbol, membership, generation, observed, quote, None);
    }

    pub(crate) fn publish_native_quote(
        &self,
        source: SnapshotReadRevision,
        observed: Instant,
        quote: Quote,
        lease: &Arc<std::sync::atomic::AtomicBool>,
    ) {
        if source.cache_id != self.identity || !lease.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
        let symbol = quote.symbol.clone();
        self.publish_quote_observation(
            &symbol,
            source.membership,
            source.generation,
            observed,
            quote,
            Some(NativeQuoteObservation {
                observed,
                lease: Arc::downgrade(lease),
            }),
        );
    }

    fn publish_quote_observation(
        &self,
        symbol: &str,
        membership: u64,
        generation: u64,
        observed: Instant,
        quote: Quote,
        native: Option<NativeQuoteObservation>,
    ) {
        if quote.symbol != symbol || !quote.valid_market_values() {
            return;
        }
        let mut state = self.state.write();
        if state.generation != generation || state.closed || state.account_changed {
            return;
        }
        let Some(slot) = state.quotes.get_mut(symbol) else {
            return;
        };
        if slot.membership != membership
            || slot
                .value
                .as_ref()
                .is_some_and(|old| old.value.timestamp > quote.timestamp || old.observed > observed)
        {
            return;
        }
        slot.value = Some(Timed {
            value: Arc::new(quote),
            observed,
        });
        slot.native = native;
        let revision = state.next_revision();
        state
            .quotes
            .get_mut(symbol)
            .expect("validated quote slot")
            .revision = revision;
        drop(state);
        self.changed();
    }
}

impl State {
    fn next_revision(&mut self) -> u64 {
        self.next_revision = self
            .next_revision
            .checked_add(1)
            .expect("snapshot revision exhausted");
        self.next_revision
    }
}

#[cfg(test)]
mod tests;
