//! Process-wide account admission. Workers retain their own bounded queues and
//! journals; only the broker mutation slot is shared. This is not a host/cluster lock.
use super::{BrokerCommandError, BrokerCommandId};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{Arc, LazyLock, Weak},
    time::Instant,
};

type Key = (String, String);
const MAX_ACCOUNT_LANES: usize = 4096;
// Separate bounded budgets: a full normal queue cannot consume cleanup slots.
const MAX_WAITERS: usize = 4096;
static LANES: LazyLock<parking_lot::Mutex<HashMap<Key, Weak<AccountLane>>>> =
    LazyLock::new(Default::default);

pub(super) struct AccountLane {
    key: Key,
    state: parking_lot::Mutex<Admission>,
}

#[derive(Default)]
struct Admission {
    active: bool,
    sequence: u64,
    // Cleanup first, FIFO within each priority. Entries contain only a wakeup,
    // never a broker payload, journal, or strategy state.
    waiting: [BTreeMap<u64, Arc<tokio::sync::Notify>>; 2],
    capacity: PendingCapacity,
    symbol_capacity: SymbolCapacity,
    margin_capacity: MarginCapacity,
}

// Keep only completion watermarks, not commands or order payloads. Serial account
// admission means completion watermarks are ordered. Never expire uncertain sends.
const MAX_CAPACITY_RESERVATIONS: usize = 4096;
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReservationState {
    AwaitingProof,
    Confirmed,
}
#[derive(Default)]
struct PendingCapacity {
    observation: Option<(usize, Instant)>,
    completed: VecDeque<(BrokerCommandId, Instant)>,
    reservations: HashMap<BrokerCommandId, ReservationState>,
}
impl PendingCapacity {
    #[cfg(test)]
    fn reserve(
        &mut self,
        id: &BrokerCommandId,
        limit: usize,
        count: usize,
        observed: Instant,
    ) -> bool {
        self.observe(count, observed);
        if !self.available(id, limit, count) {
            return false;
        }
        self.insert(id);
        true
    }
    fn observe(&mut self, count: usize, observed: Instant) {
        if self.observation.is_none_or(|(_, old)| observed > old) {
            self.observation = Some((count, observed));
            while self
                .completed
                .front()
                .is_some_and(|(_, done)| *done < observed)
            {
                let (id, _) = self.completed.pop_front().expect("front was present");
                self.reservations.remove(&id);
            }
        }
    }
    fn available(&self, id: &BrokerCommandId, limit: usize, count: usize) -> bool {
        let count = self.observation.map_or(count, |(count, _)| count);
        let reservations = self.reservations.len();
        !id.0.is_empty()
            && id.0.len() <= 512
            && !self.reservations.contains_key(id)
            && reservations < MAX_CAPACITY_RESERVATIONS
            && count.saturating_add(reservations) < limit
    }
    fn insert(&mut self, id: &BrokerCommandId) {
        self.reservations
            .insert(id.clone(), ReservationState::AwaitingProof);
    }
    fn finish(&mut self, id: &BrokerCommandId, outcome: &super::BrokerCommandOutcome) {
        use super::BrokerCommandOutcome;
        // Replayed, unrelated, or already-confirmed completions cannot free a
        // different command's slot or append another completion watermark.
        if self.reservations.get(id) != Some(&ReservationState::AwaitingProof) {
            return;
        }
        match outcome {
            BrokerCommandOutcome::Ambiguous { .. } => {}
            BrokerCommandOutcome::Declined { .. }
            | BrokerCommandOutcome::CancelledBeforeDispatch => {
                self.reservations.remove(id);
            }
            // Even an immediately filled/terminal response conservatively waits
            // for a post-completion observation before reusing this slot.
            BrokerCommandOutcome::OrderResponse { .. } | BrokerCommandOutcome::Acknowledged => {
                *self
                    .reservations
                    .get_mut(id)
                    .expect("reservation was validated") = ReservationState::Confirmed;
                self.completed.push_back((id.clone(), Instant::now()));
            }
        }
    }
}

#[derive(Clone)]
struct SymbolReservation {
    symbol: String,
    quantity: f64,
    state: ReservationState,
}

#[derive(Default)]
struct SymbolCapacity {
    observations: HashMap<String, (f64, Instant)>,
    completed: VecDeque<(BrokerCommandId, Instant)>,
    reservations: HashMap<BrokerCommandId, SymbolReservation>,
}

#[derive(Clone)]
struct MarginReservation {
    amount: f64,
    state: ReservationState,
}

#[derive(Default)]
struct MarginCapacity {
    observation: Option<(f64, Instant)>,
    completed: VecDeque<(BrokerCommandId, Instant)>,
    reservations: HashMap<BrokerCommandId, MarginReservation>,
}

impl MarginCapacity {
    fn observe(&mut self, available: f64, observed: Instant) {
        if self.observation.is_none_or(|(_, old)| observed > old) {
            self.observation = Some((available, observed));
            while self
                .completed
                .front()
                .is_some_and(|(_, done)| *done < observed)
            {
                let (id, _) = self.completed.pop_front().expect("front was present");
                self.reservations.remove(&id);
            }
        }
    }

    fn available(&self, id: &BrokerCommandId, required: f64, available: f64) -> bool {
        let available = self.observation.map_or(available, |(value, _)| value);
        let reserved = self
            .reservations
            .values()
            .map(|entry| entry.amount)
            .sum::<f64>();
        !id.0.is_empty()
            && id.0.len() <= 512
            && required.is_finite()
            && required >= 0.0
            && available.is_finite()
            && available >= 0.0
            && reserved.is_finite()
            && !self.reservations.contains_key(id)
            && self.reservations.len() < MAX_CAPACITY_RESERVATIONS
            && reserved + required <= available
    }

    fn insert(&mut self, id: &BrokerCommandId, amount: f64) {
        self.reservations.insert(
            id.clone(),
            MarginReservation {
                amount,
                state: ReservationState::AwaitingProof,
            },
        );
    }

    fn finish(&mut self, id: &BrokerCommandId, outcome: &super::BrokerCommandOutcome) {
        use super::BrokerCommandOutcome;
        if self.reservations.get(id).map(|entry| entry.state)
            != Some(ReservationState::AwaitingProof)
        {
            return;
        }
        match outcome {
            BrokerCommandOutcome::Ambiguous { .. } => {}
            BrokerCommandOutcome::Declined { .. }
            | BrokerCommandOutcome::CancelledBeforeDispatch => {
                self.reservations.remove(id);
            }
            BrokerCommandOutcome::OrderResponse { .. } | BrokerCommandOutcome::Acknowledged => {
                self.reservations
                    .get_mut(id)
                    .expect("reservation was validated")
                    .state = ReservationState::Confirmed;
                self.completed.push_back((id.clone(), Instant::now()));
            }
        }
    }
}

impl SymbolCapacity {
    fn observe(&mut self, symbol: &str, quantity: f64, observed: Instant) {
        if self
            .observations
            .get(symbol)
            .is_none_or(|(_, old)| observed > *old)
        {
            self.observations
                .insert(symbol.to_owned(), (quantity, observed));
            let retired = self
                .completed
                .iter()
                .filter(|(id, done)| {
                    *done < observed
                        && self
                            .reservations
                            .get(id)
                            .is_some_and(|entry| entry.symbol == symbol)
                })
                .map(|(id, _)| id.clone())
                .collect::<std::collections::HashSet<_>>();
            for id in &retired {
                self.reservations.remove(id);
            }
            self.completed.retain(|(id, _)| !retired.contains(id));
        }
    }

    fn available(
        &self,
        id: &BrokerCommandId,
        symbol: &str,
        requested: f64,
        limit: f64,
        observed: f64,
    ) -> bool {
        let observed = self
            .observations
            .get(symbol)
            .map_or(observed, |(quantity, _)| *quantity);
        let reserved = self
            .reservations
            .values()
            .filter(|entry| entry.symbol == symbol)
            .map(|entry| entry.quantity)
            .sum::<f64>();
        !id.0.is_empty()
            && id.0.len() <= 512
            && !symbol.is_empty()
            && symbol.len() <= 128
            && requested.is_finite()
            && requested > 0.0
            && limit.is_finite()
            && limit > 0.0
            && observed.is_finite()
            && reserved.is_finite()
            && !self.reservations.contains_key(id)
            && self.reservations.len() < MAX_CAPACITY_RESERVATIONS
            && observed + reserved + requested <= limit
    }

    fn insert(&mut self, id: &BrokerCommandId, symbol: &str, quantity: f64) {
        self.reservations.insert(
            id.clone(),
            SymbolReservation {
                symbol: symbol.to_owned(),
                quantity,
                state: ReservationState::AwaitingProof,
            },
        );
    }

    fn finish(&mut self, id: &BrokerCommandId, outcome: &super::BrokerCommandOutcome) {
        use super::BrokerCommandOutcome;
        if self.reservations.get(id).map(|entry| entry.state)
            != Some(ReservationState::AwaitingProof)
        {
            return;
        }
        match outcome {
            BrokerCommandOutcome::Ambiguous { .. } => {}
            BrokerCommandOutcome::Declined { .. }
            | BrokerCommandOutcome::CancelledBeforeDispatch => {
                self.reservations.remove(id);
            }
            BrokerCommandOutcome::OrderResponse { .. } | BrokerCommandOutcome::Acknowledged => {
                self.reservations
                    .get_mut(id)
                    .expect("reservation was validated")
                    .state = ReservationState::Confirmed;
                self.completed.push_back((id.clone(), Instant::now()));
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct EntryReservation {
    pub pending: bool,
    pub symbol: bool,
    pub margin: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CapacityRejection {
    PendingOrders,
    SymbolQuantity,
    Margin,
}

impl Admission {
    fn next(&self) -> Option<(usize, u64, Arc<tokio::sync::Notify>)> {
        self.waiting
            .iter()
            .enumerate()
            .find_map(|(priority, queue)| {
                queue
                    .first_key_value()
                    .map(|(id, wake)| (priority, *id, wake.clone()))
            })
    }
    fn wake(&self) {
        if !self.active {
            if let Some((_, _, wake)) = self.next() {
                wake.notify_one();
            }
        }
    }
}

pub(super) struct AccountPermit(Arc<AccountLane>);
impl AccountPermit {
    pub fn reserve_entry(
        &self,
        id: &BrokerCommandId,
        pending: Option<(usize, usize, Instant)>,
        symbol: Option<(&str, f64, f64, f64, Instant)>,
        margin: Option<(f64, f64, Instant)>,
    ) -> Result<EntryReservation, CapacityRejection> {
        let mut state = self.0.state.lock();
        if let Some((_, count, observed)) = pending {
            state.capacity.observe(count, observed);
        }
        if let Some((symbol, _, _, observed_quantity, observed)) = symbol {
            state
                .symbol_capacity
                .observe(symbol, observed_quantity, observed);
        }
        if let Some((_, available, observed)) = margin {
            state.margin_capacity.observe(available, observed);
        }
        if let Some((limit, count, _)) = pending
            && !state.capacity.available(id, limit, count)
        {
            return Err(CapacityRejection::PendingOrders);
        }
        if let Some((symbol, requested, limit, observed_quantity, _)) = symbol
            && !state
                .symbol_capacity
                .available(id, symbol, requested, limit, observed_quantity)
        {
            return Err(CapacityRejection::SymbolQuantity);
        }
        if let Some((required, available, _)) = margin
            && !state.margin_capacity.available(id, required, available)
        {
            return Err(CapacityRejection::Margin);
        }
        if pending.is_some() {
            state.capacity.insert(id);
        }
        if let Some((symbol, requested, _, _, _)) = symbol {
            state.symbol_capacity.insert(id, symbol, requested);
        }
        if let Some((required, _, _)) = margin {
            state.margin_capacity.insert(id, required);
        }
        Ok(EntryReservation {
            pending: pending.is_some(),
            symbol: symbol.is_some(),
            margin: margin.is_some(),
        })
    }
    pub fn finish_entry(
        &self,
        id: &BrokerCommandId,
        reservation: EntryReservation,
        outcome: &super::BrokerCommandOutcome,
    ) {
        let mut state = self.0.state.lock();
        if reservation.pending {
            state.capacity.finish(id, outcome);
        }
        if reservation.symbol {
            state.symbol_capacity.finish(id, outcome);
        }
        if reservation.margin {
            state.margin_capacity.finish(id, outcome);
        }
    }
}
impl Drop for AccountPermit {
    fn drop(&mut self) {
        let mut state = self.0.state.lock();
        state.active = false;
        state.wake();
    }
}

struct Waiter {
    lane: Arc<AccountLane>,
    priority: usize,
    id: u64,
    armed: bool,
}
impl Drop for Waiter {
    fn drop(&mut self) {
        if self.armed {
            let mut state = self.lane.state.lock();
            state.waiting[self.priority].remove(&self.id);
            state.wake();
        }
    }
}

impl AccountLane {
    pub fn settle_entry(&self, id: &BrokerCommandId, outcome: &super::BrokerCommandOutcome) {
        let mut state = self.state.lock();
        state.capacity.finish(id, outcome);
        state.symbol_capacity.finish(id, outcome);
        state.margin_capacity.finish(id, outcome);
    }

    #[cfg(test)]
    pub fn waiter_counts(&self) -> [usize; 2] {
        let state = self.state.lock();
        [state.waiting[0].len(), state.waiting[1].len()]
    }

    pub fn shared(provider: &str, account: &str) -> Result<Arc<Self>, BrokerCommandError> {
        let provider = provider.trim();
        let account = account.trim();
        if provider.is_empty() || provider.len() > 512 || account.is_empty() || account.len() > 512
        {
            return Err(BrokerCommandError::Invalid(
                "invalid mutation provider/account identity".into(),
            ));
        }
        let key = (provider.to_owned(), account.to_owned());
        let mut lanes = LANES.lock();
        if let Some(lane) = lanes.get(&key).and_then(Weak::upgrade) {
            return Ok(lane);
        }
        // Weak entries normally disappear on last-owner drop. Removing expired
        // entries also handles a drop waiting on this lock during admission.
        lanes.retain(|_, lane| lane.strong_count() != 0);
        if lanes.len() >= MAX_ACCOUNT_LANES {
            return Err(BrokerCommandError::QueueFull);
        }
        let lane = Arc::new(Self {
            key: key.clone(),
            state: Default::default(),
        });
        lanes.insert(key, Arc::downgrade(&lane));
        Ok(lane)
    }

    pub async fn acquire(
        self: Arc<Self>,
        cleanup: bool,
    ) -> Result<AccountPermit, BrokerCommandError> {
        let priority = usize::from(!cleanup);
        let (id, wake) = {
            let mut state = self.state.lock();
            // The normal uncontended path allocates neither a queue node nor a
            // Notify. A new normal request cannot barge past existing waiters.
            if !state.active
                && state.waiting[0].is_empty()
                && (cleanup || state.waiting[1].is_empty())
            {
                state.active = true;
                return Ok(AccountPermit(self.clone()));
            }
            if state.waiting[priority].len() >= MAX_WAITERS {
                return Err(BrokerCommandError::QueueFull);
            }
            let id = state.sequence;
            state.sequence = id.checked_add(1).ok_or(BrokerCommandError::QueueFull)?;
            let wake = Arc::new(tokio::sync::Notify::new());
            state.waiting[priority].insert(id, wake.clone());
            state.wake();
            (id, wake)
        };
        let mut waiter = Waiter {
            lane: self.clone(),
            priority,
            id,
            armed: true,
        };
        loop {
            // notify_one retains a permit if release races with this await.
            wake.notified().await;
            let mut state = self.state.lock();
            if !state.active
                && state
                    .next()
                    .is_some_and(|(p, next, _)| p == priority && next == id)
            {
                state.waiting[priority].remove(&id);
                state.active = true;
                waiter.armed = false;
                return Ok(AccountPermit(self.clone()));
            }
        }
    }
}

impl Drop for AccountLane {
    fn drop(&mut self) {
        let mut lanes = LANES.lock();
        // A concurrent new owner may have replaced our expired Weak entry.
        if lanes
            .get(&self.key)
            .is_some_and(|lane| std::ptr::eq(lane.as_ptr(), self))
        {
            lanes.remove(&self.key);
        }
        if lanes.is_empty() {
            lanes.shrink_to_fit();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_reservations_fence_old_and_inflight_observations() {
        let mut capacity = PendingCapacity::default();
        let old = Instant::now();
        let id = BrokerCommandId("first".into());
        let next = BrokerCommandId("next".into());
        assert!(capacity.reserve(&id, 1, 0, old));
        let during_send = Instant::now();
        capacity.finish(&id, &super::super::BrokerCommandOutcome::Acknowledged);
        assert!(!capacity.reserve(&next, 1, 0, during_send));
        let fresh = Instant::now();
        assert!(!capacity.reserve(&next, 1, 1, fresh));
        // An independent worker must not overwrite the shared observation with
        // its older, emptier cache after the reservation has been retired.
        assert!(!capacity.reserve(&next, 1, 0, old));
        assert!(capacity.reserve(&next, 1, 0, Instant::now()));
    }

    #[test]
    fn uncertain_capacity_is_bounded_and_not_freed_by_empty_snapshots() {
        let mut capacity = PendingCapacity::default();
        for index in 0..MAX_CAPACITY_RESERVATIONS {
            let id = BrokerCommandId(index.to_string());
            assert!(capacity.reserve(&id, usize::MAX, 0, Instant::now()));
            capacity.finish(
                &id,
                &super::super::BrokerCommandOutcome::Ambiguous {
                    reason: "timeout".into(),
                },
            );
        }
        assert!(!capacity.reserve(
            &BrokerCommandId("overflow".into()),
            usize::MAX,
            0,
            Instant::now()
        ));
        assert_eq!(capacity.reservations.len(), MAX_CAPACITY_RESERVATIONS);
        assert!(capacity.completed.is_empty());
    }

    #[test]
    fn definite_refusal_releases_capacity_without_refresh() {
        let mut capacity = PendingCapacity::default();
        let now = Instant::now();
        let id = BrokerCommandId("first".into());
        assert!(capacity.reserve(&id, 1, 0, now));
        capacity.finish(
            &id,
            &super::super::BrokerCommandOutcome::Declined {
                reason: "broker refusal".into(),
            },
        );
        assert!(capacity.reserve(&BrokerCommandId("next".into()), 1, 0, now));
    }

    #[tokio::test]
    async fn symbol_quantity_reservations_are_atomic_with_pending_order_slots() {
        let lane = AccountLane::shared("capacity", &uuid::Uuid::new_v4().to_string()).unwrap();
        let permit = lane.clone().acquire(false).await.unwrap();
        let observed = Instant::now();
        let first = BrokerCommandId("first".into());
        let reservation = permit
            .reserve_entry(
                &first,
                Some((2, 0, observed)),
                Some(("SOLUSD", 60.0, 100.0, 20.0, observed)),
                None,
            )
            .unwrap();
        assert!(reservation.pending && reservation.symbol);
        permit.finish_entry(
            &first,
            reservation,
            &super::super::BrokerCommandOutcome::Acknowledged,
        );
        drop(permit);

        let permit = lane.clone().acquire(false).await.unwrap();
        let second = BrokerCommandId("second".into());
        assert_eq!(
            permit.reserve_entry(
                &second,
                Some((2, 0, observed)),
                Some(("SOLUSD", 30.0, 100.0, 20.0, observed)),
                None,
            ),
            Err(CapacityRejection::SymbolQuantity),
        );
        // The failed combined admission must not consume either capacity.
        assert!(
            !lane
                .state
                .lock()
                .capacity
                .reservations
                .contains_key(&second)
        );
        assert!(
            !lane
                .state
                .lock()
                .symbol_capacity
                .reservations
                .contains_key(&second)
        );
    }

    #[tokio::test]
    async fn symbol_reservation_retires_only_after_a_post_completion_observation() {
        let lane = AccountLane::shared("capacity", &uuid::Uuid::new_v4().to_string()).unwrap();
        let before = Instant::now();
        let first = BrokerCommandId("first".into());
        let permit = lane.clone().acquire(false).await.unwrap();
        let reservation = permit
            .reserve_entry(
                &first,
                None,
                Some(("SOLUSD", 80.0, 100.0, 0.0, before)),
                None,
            )
            .unwrap();
        permit.finish_entry(
            &first,
            reservation,
            &super::super::BrokerCommandOutcome::Acknowledged,
        );
        drop(permit);

        let permit = lane.clone().acquire(false).await.unwrap();
        assert_eq!(
            permit.reserve_entry(
                &BrokerCommandId("stale".into()),
                None,
                Some(("SOLUSD", 21.0, 100.0, 0.0, before)),
                None,
            ),
            Err(CapacityRejection::SymbolQuantity),
        );
        assert!(
            permit
                .reserve_entry(
                    &BrokerCommandId("fresh".into()),
                    None,
                    Some(("SOLUSD", 100.0, 100.0, 0.0, Instant::now())),
                    None,
                )
                .is_ok()
        );
    }

    #[tokio::test]
    async fn authoritative_margin_is_reserved_until_a_new_account_observation() {
        let lane = AccountLane::shared("margin", &uuid::Uuid::new_v4().to_string()).unwrap();
        let observed = Instant::now();
        let first = BrokerCommandId("first".into());
        let permit = lane.clone().acquire(false).await.unwrap();
        let reservation = permit
            .reserve_entry(&first, None, None, Some((70.0, 100.0, observed)))
            .unwrap();
        assert!(reservation.margin);
        permit.finish_entry(
            &first,
            reservation,
            &super::super::BrokerCommandOutcome::Acknowledged,
        );
        drop(permit);

        let permit = lane.clone().acquire(false).await.unwrap();
        assert_eq!(
            permit.reserve_entry(
                &BrokerCommandId("stale".into()),
                None,
                None,
                Some((31.0, 100.0, observed)),
            ),
            Err(CapacityRejection::Margin),
        );
        assert!(
            permit
                .reserve_entry(
                    &BrokerCommandId("fresh".into()),
                    None,
                    None,
                    Some((100.0, 100.0, Instant::now())),
                )
                .is_ok()
        );
    }

    #[test]
    fn completion_identity_and_replay_cannot_release_another_reservation() {
        use super::super::BrokerCommandOutcome;
        let mut capacity = PendingCapacity::default();
        let now = Instant::now();
        let a = BrokerCommandId("a".into());
        let b = BrokerCommandId("b".into());
        assert!(capacity.reserve(&a, 2, 0, now));
        assert!(!capacity.reserve(&a, 2, 0, now));
        assert!(capacity.reserve(&b, 2, 0, now));
        let refusal = BrokerCommandOutcome::Declined {
            reason: "rejected".into(),
        };
        capacity.finish(&BrokerCommandId("unrelated".into()), &refusal);
        assert_eq!(capacity.reservations.len(), 2);
        capacity.finish(&a, &BrokerCommandOutcome::Acknowledged);
        capacity.finish(&a, &BrokerCommandOutcome::Acknowledged);
        capacity.finish(&a, &refusal);
        assert_eq!(capacity.completed.len(), 1);
        assert_eq!(capacity.reservations.len(), 2);
        capacity.finish(&b, &refusal);
        capacity.finish(&b, &refusal);
        assert_eq!(capacity.reservations.len(), 1);
    }

    #[test]
    fn reservation_identity_size_is_bounded() {
        let mut capacity = PendingCapacity::default();
        for id in [String::new(), "x".repeat(513)] {
            assert!(!capacity.reserve(&BrokerCommandId(id), 10, 0, Instant::now()));
        }
        assert!(capacity.reservations.is_empty());
    }

    #[test]
    fn later_exact_completion_resolves_only_its_uncertain_slot() {
        use super::super::BrokerCommandOutcome;
        let mut capacity = PendingCapacity::default();
        let a = BrokerCommandId("uncertain-a".into());
        let b = BrokerCommandId("uncertain-b".into());
        let now = Instant::now();
        for id in [&a, &b] {
            assert!(capacity.reserve(id, 2, 0, now));
            capacity.finish(
                id,
                &BrokerCommandOutcome::Ambiguous {
                    reason: "timeout".into(),
                },
            );
        }
        capacity.finish(&a, &BrokerCommandOutcome::Acknowledged);
        assert_eq!(capacity.completed.len(), 1);
        let c = BrokerCommandId("next".into());
        assert!(capacity.reserve(&c, 2, 0, Instant::now()));
        assert!(capacity.reservations.contains_key(&b));
        assert!(!capacity.reservations.contains_key(&a));
        assert!(!capacity.reserve(&BrokerCommandId("overflow".into()), 2, 0, Instant::now()));
    }

    #[tokio::test]
    async fn cleanup_precedes_existing_waiters_and_preserves_fifo_without_preemption() {
        let lane = AccountLane::shared("fixture", &uuid::Uuid::new_v4().to_string()).unwrap();
        let held = lane.clone().acquire(false).await.unwrap();
        let mut a = Box::pin(lane.clone().acquire(false));
        let mut b = Box::pin(lane.clone().acquire(false));
        let mut cleanup_a = Box::pin(lane.clone().acquire(true));
        let mut cleanup_b = Box::pin(lane.clone().acquire(true));
        for pending in [&mut a, &mut b, &mut cleanup_a, &mut cleanup_b] {
            assert!(futures::poll!(pending.as_mut()).is_pending());
        }
        assert!(
            lane.state.lock().active,
            "cleanup cannot interrupt an active mutation"
        );
        drop(held);
        assert!(futures::poll!(a.as_mut()).is_pending());
        let cleanup = cleanup_a.await.unwrap();
        assert!(futures::poll!(cleanup_b.as_mut()).is_pending());
        drop(cleanup);
        drop(cleanup_b.await.unwrap());
        let first = a.await.unwrap();
        assert!(futures::poll!(b.as_mut()).is_pending());
        drop(first);
        drop(b.await.unwrap());
        let state = lane.state.lock();
        assert!(!state.active && state.waiting.iter().all(BTreeMap::is_empty));
    }

    #[tokio::test]
    async fn cancelled_waiters_release_reservations_and_wake_the_next_owner() {
        let lane = AccountLane::shared("fixture", &uuid::Uuid::new_v4().to_string()).unwrap();
        let held = lane.clone().acquire(false).await.unwrap();
        let mut normal = Box::pin(lane.clone().acquire(false));
        let mut cleanup = Box::pin(lane.clone().acquire(true));
        assert!(futures::poll!(normal.as_mut()).is_pending());
        assert!(futures::poll!(cleanup.as_mut()).is_pending());
        drop(held); // Cleanup now has the wakeup, but hasn't taken the slot.
        drop(cleanup);
        drop(
            tokio::time::timeout(std::time::Duration::from_secs(1), normal)
                .await
                .unwrap()
                .unwrap(),
        );
        assert!(lane.state.lock().waiting.iter().all(BTreeMap::is_empty));
    }

    #[tokio::test]
    async fn full_normal_budget_preserves_cleanup_capacity_and_reclaims_cancelled_waiters() {
        use futures::FutureExt;
        let lane = AccountLane::shared("fixture", &uuid::Uuid::new_v4().to_string()).unwrap();
        let held = lane.clone().acquire(false).await.unwrap();
        let mut waiting = Vec::new();
        for _ in 0..MAX_WAITERS {
            let mut future = Box::pin(lane.clone().acquire(false));
            assert!(futures::poll!(future.as_mut()).is_pending());
            waiting.push(future);
        }
        assert!(matches!(
            lane.clone().acquire(false).now_or_never(),
            Some(Err(BrokerCommandError::QueueFull))
        ));
        let mut cleanup = Box::pin(lane.clone().acquire(true));
        assert!(futures::poll!(cleanup.as_mut()).is_pending());
        drop(held);
        let permit = cleanup.await.unwrap();
        drop(waiting);
        assert!(lane.state.lock().waiting.iter().all(BTreeMap::is_empty));
        drop(permit);
        assert!(lane.clone().acquire(false).now_or_never().unwrap().is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_owners_keep_one_mutation_and_drain_cleanup_first() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let lane = AccountLane::shared("fixture", &uuid::Uuid::new_v4().to_string()).unwrap();
        let held = lane.clone().acquire(false).await.unwrap();
        let active = Arc::new(AtomicUsize::new(0));
        let order = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let mut tasks = Vec::new();
        for index in 0..128 {
            let lane = lane.clone();
            let active = active.clone();
            let order = order.clone();
            tasks.push(tokio::spawn(async move {
                let cleanup = index % 2 == 0;
                let _permit = lane.acquire(cleanup).await.unwrap();
                assert_eq!(active.fetch_add(1, Ordering::SeqCst), 0);
                order.lock().push(cleanup);
                tokio::task::yield_now().await;
                assert_eq!(active.fetch_sub(1, Ordering::SeqCst), 1);
            }));
        }
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while lane.waiter_counts() != [64, 64] {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(held);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            for task in tasks {
                task.await.unwrap();
            }
        })
        .await
        .unwrap();
        let order = order.lock();
        assert_eq!(order.len(), 128);
        assert!(order[..64].iter().all(|cleanup| *cleanup));
        assert!(order[64..].iter().all(|cleanup| !*cleanup));
        assert_eq!(lane.waiter_counts(), [0, 0]);
    }

    #[tokio::test]
    async fn exact_trimmed_account_shares_one_slot_but_other_accounts_progress() {
        let id = uuid::Uuid::new_v4().to_string();
        let a = AccountLane::shared("broker", &id).unwrap();
        let b = AccountLane::shared("broker", &format!(" {id} ")).unwrap();
        let other = AccountLane::shared("other broker", &id).unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        use futures::FutureExt;
        let held = a.clone().acquire(false).await.unwrap();
        assert!(b.clone().acquire(false).now_or_never().is_none());
        assert!(other.clone().acquire(false).now_or_never().unwrap().is_ok());
        drop(held);
        assert!(b.clone().acquire(false).now_or_never().unwrap().is_ok());
        let key = a.key.clone();
        drop(a);
        assert!(LANES.lock().contains_key(&key));
        drop(b);
        assert!(!LANES.lock().contains_key(&key));
    }
}
