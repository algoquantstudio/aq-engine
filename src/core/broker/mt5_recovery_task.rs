//! One owned, coalesced automatic recovery attempt. No broker mutation runs here.
use parking_lot::Mutex;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::Poll;
use tokio::task::JoinHandle;

#[derive(Default)]
pub(super) struct RecoveryTask {
    state: Mutex<State>,
    drain: tokio::sync::Mutex<()>,
}

#[derive(Default)]
struct State {
    enabled: bool,
    task: Option<JoinHandle<()>>,
}

impl RecoveryTask {
    /// Do not overlap a restarted transport with an old task still cancelling.
    pub(super) fn enable(&self) -> bool {
        let Ok(_drain) = self.drain.try_lock() else {
            return false;
        };
        let mut state = self.state.lock();
        if state.task.as_ref().is_some_and(|task| !task.is_finished()) {
            return false;
        }
        state.task = None;
        state.enabled = true;
        true
    }

    pub(super) fn schedule(&self, work: impl Future<Output = ()> + Send + 'static) -> bool {
        let mut state = self.state.lock();
        if !state.enabled || state.task.as_ref().is_some_and(|task| !task.is_finished()) {
            return false;
        }
        // A completed/panicked attempt does not leave a sticky scheduled flag.
        state.task = Some(tokio::spawn(work));
        true
    }

    pub(super) fn close(&self) {
        let mut state = self.state.lock();
        state.enabled = false;
        if let Some(task) = &state.task {
            task.abort();
        }
    }

    pub(super) async fn shutdown(&self) {
        self.close();
        // A JoinHandle has a single waiting waker. Serialize concurrent broker
        // and datafeed shutdown callers rather than replacing each other's wake.
        let _drain = self.drain.lock().await;
        self.close();
        // Keep ownership in the slot while awaiting. Cancelling shutdown must
        // not detach the task; another shutdown can finish draining it.
        poll_fn(|cx| {
            let mut state = self.state.lock();
            let Some(task) = state.task.as_mut() else {
                return Poll::Ready(());
            };
            match Pin::new(task).poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => {
                    state.task = None;
                    if let Err(error) = result {
                        if !error.is_cancelled() {
                            log::warn!("MT5 automatic recovery task failed: {error}");
                        }
                    }
                    Poll::Ready(())
                }
            }
        })
        .await;
    }
}

impl Drop for RecoveryTask {
    fn drop(&mut self) {
        if let Some(task) = self.state.get_mut().task.take() {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn coalesces_and_shutdown_releases_owned_work() {
        let slot = RecoveryTask::default();
        let retained = Arc::new(());
        assert!(!slot.schedule(async {}));
        assert!(slot.enable());
        let held = retained.clone();
        assert!(slot.schedule(async move {
            let _held = held;
            std::future::pending::<()>().await;
        }));
        assert!(!slot.schedule(async {}));
        slot.shutdown().await;
        assert_eq!(Arc::strong_count(&retained), 1);
        assert!(!slot.schedule(async {}));
        assert!(slot.enable());
        assert!(slot.schedule(async {}));
        slot.shutdown().await;
    }

    #[tokio::test]
    async fn abort_pending_blocks_reopen_and_cancelled_shutdown_retains_handle() {
        let slot = RecoveryTask::default();
        assert!(slot.enable());
        assert!(slot.schedule(std::future::pending()));
        slot.close();
        // Current-thread runtime: cancellation has not yet polled the task.
        assert!(!slot.enable());
        {
            let shutdown = slot.shutdown();
            tokio::pin!(shutdown);
            assert!(futures::poll!(&mut shutdown).is_pending());
        }
        assert!(slot.state.lock().task.is_some());
        slot.shutdown().await;
        assert!(slot.state.lock().task.is_none());
        assert!(slot.enable());
    }

    #[tokio::test]
    async fn panic_does_not_block_next_attempt() {
        let slot = RecoveryTask::default();
        assert!(slot.enable());
        assert!(slot.schedule(async { panic!("fake recovery failure") }));
        tokio::task::yield_now().await;
        assert!(slot.schedule(async {}));
        slot.shutdown().await;
    }

    #[tokio::test]
    async fn drop_aborts_instead_of_detaching() {
        let retained = Arc::new(());
        let slot = RecoveryTask::default();
        assert!(slot.enable());
        let held = retained.clone();
        assert!(slot.schedule(async move {
            let _held = held;
            std::future::pending::<()>().await;
        }));
        drop(slot);
        tokio::task::yield_now().await;
        assert_eq!(Arc::strong_count(&retained), 1);
    }

    #[tokio::test]
    async fn concurrent_shutdown_callers_both_finish() {
        let slot = RecoveryTask::default();
        assert!(slot.enable());
        assert!(slot.schedule(std::future::pending()));
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            tokio::join!(slot.shutdown(), slot.shutdown());
        })
        .await
        .unwrap();
        assert!(slot.state.lock().task.is_none());
        assert!(slot.enable());
    }
}
