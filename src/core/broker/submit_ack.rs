//! A durable submit may wait longer for its original reply, but never for
//! dispatch. Scoped to one worker future; not inherited by spawned reads.
use super::mt5_bridge::Mt5RpcRequest;
use parking_lot::Mutex;
use std::{collections::VecDeque, future::Future, sync::Arc, time::Duration};

pub(super) const GRACE: Duration = Duration::from_secs(180);
type Queue = Arc<Mutex<VecDeque<Mt5RpcRequest>>>;

#[derive(Clone, Default)]
pub(super) struct Window(Arc<Mutex<State>>);
#[derive(Default)]
enum State {
    #[default]
    Unarmed,
    Queued(String, Queue),
    Checked(bool),
    Waiting,
}
tokio::task_local! { static CURRENT: Window; }

impl Window {
    pub async fn scope<F: Future>(&self, operation: F) -> F::Output {
        CURRENT.scope(self.clone(), operation).await
    }

    pub fn handed_off(&self) -> bool {
        let mut state = self.0.lock();
        match &*state {
            State::Unarmed => false,
            State::Waiting => true,
            State::Checked(result) => *result,
            State::Queued(id, queue) => {
                // Race against the EA's queue drain under the very same lock.
                // If we win, an expired submit cannot subsequently go on the wire.
                let mut queue = queue.lock();
                let handed_off = if let Some(i) = queue.iter().position(|r| r.request_id == *id) {
                    queue.remove(i);
                    false
                } else {
                    true
                };
                drop(queue);
                *state = State::Checked(handed_off);
                handed_off
            }
        }
    }

    pub fn waiting(&self) -> bool {
        matches!(*self.0.lock(), State::Waiting)
    }

    pub async fn bounded<F: Future>(
        &self,
        operation: F,
        timeout: Duration,
        grace: Duration,
        may_wait: impl FnOnce() -> bool,
        closed: impl Future<Output = ()>,
    ) -> Option<F::Output> {
        let deadline = tokio::time::Instant::now() + timeout;
        let operation = self.scope(operation);
        tokio::pin!(operation);
        if let Ok(value) = tokio::time::timeout_at(deadline, &mut operation).await {
            return Some(value);
        }
        if !may_wait() || !self.handed_off() {
            return None;
        }
        *self.0.lock() = State::Waiting;
        log::warn!(
            "MT5 submit handed off without a timely reply; holding the mutation lane for the original acknowledgement (no resubmit)"
        );
        tokio::select! {
            biased;
            _ = closed => None,
            value = tokio::time::timeout_at(deadline + grace, operation) => value.ok(),
        }
    }
}

pub(super) fn register(id: &str, queue: &Queue) -> Option<Window> {
    CURRENT
        .try_with(|window| {
            let mut state = window.0.lock();
            if !matches!(*state, State::Unarmed) {
                return None;
            }
            *state = State::Queued(id.to_owned(), queue.clone());
            Some(window.clone())
        })
        .ok()
        .flatten()
}
