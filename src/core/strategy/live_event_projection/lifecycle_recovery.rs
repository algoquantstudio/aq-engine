//! One exact startup envelope, retained across optional cloud-capture pauses.
//! This lane only publishes evidence; it cannot submit or retry broker orders.
use super::*;
use crate::core::live_sync::{EventBusError, EventId};
use tokio::sync::oneshot::{Receiver, error::TryRecvError};

#[derive(Default)]
pub(super) struct DeferredStart {
    event: Option<CloudEvent>,
    receipt: Option<Receiver<Result<EventId, EventBusError>>>,
    retry_at: Option<Instant>,
}

impl LocalBatchSyncState {
    pub(in crate::core::strategy) async fn publish_start_lifecycle_durable(
        &mut self,
        event: CloudEvent,
    ) -> bool {
        debug_assert_eq!(event.kind, EventKind::StrategyLifecycle);
        debug_assert_eq!(event.payload["state"], "running");
        let accepted = self.publish_critical_durable(event.clone()).await;
        // Cloud backpressure must not stop local trading, but neither is a
        // failed admission a startup receipt. Keep exactly one original fact.
        self.deferred_start = if !accepted && self.publisher.cloud_capture_paused() {
            DeferredStart {
                event: Some(event),
                ..Default::default()
            }
        } else {
            DeferredStart::default()
        };
        accepted
    }

    /// Nonblocking owner poll: the publisher owns SQLite admission. No extra
    /// task, retry queue, network request or broker operation is created here.
    pub(in crate::core::strategy) fn flush_start_lifecycle(&mut self, running: bool) {
        if !running {
            // Never recover an obsolete Running report after teardown begins.
            // Already-admitted envelopes retain their normal revision fences.
            self.deferred_start = Default::default();
            return;
        }
        let pending = &mut self.deferred_start;
        if let Some(receipt) = pending.receipt.as_mut() {
            match receipt.try_recv() {
                Ok(Ok(_)) => {
                    *pending = Default::default();
                    return;
                }
                Err(TryRecvError::Empty) => return,
                Ok(Err(EventBusError::CloudCapturePaused)) => {
                    pending.receipt = None;
                    pending.retry_at = Some(Instant::now() + Duration::from_secs(1));
                }
                result => {
                    self.publisher.mark_admission_failed();
                    error!("Deferred startup lifecycle admission failed: {result:?}");
                    return;
                }
            }
        }
        if self.publisher.cloud_capture_paused()
            || self.publisher.admission_failed()
            || pending.retry_at.is_some_and(|at| at > Instant::now())
        {
            return;
        }
        let Some(event) = pending.event.as_ref() else {
            return;
        };
        let subject = EventSubject::for_priority(
            &self.runtime_id,
            &self.strategy_id,
            EventPriority::Critical,
        );
        match self.publisher.queue_durable(subject, event.clone()) {
            Ok(receipt) => pending.receipt = Some(receipt),
            Err(EventBusError::CloudCapturePaused | EventBusError::PublisherCapacity) => {
                pending.retry_at = Some(Instant::now() + Duration::from_secs(1));
            }
            Err(error) => {
                self.publisher.mark_admission_failed();
                error!("Deferred startup lifecycle could not be queued: {error}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::live_sync::{InProcessEventBus, LocalEventPublisherConfig, OutboxStore};
    use std::sync::Arc;

    async fn fixture() -> (std::path::PathBuf, OutboxStore, LocalBatchSyncState) {
        let path = std::env::temp_dir().join(format!("deferred-start-{}.db", uuid::Uuid::new_v4()));
        let outbox = OutboxStore::open(&path).await.unwrap();
        let publisher = LocalEventPublisher::spawn(
            Arc::new(InProcessEventBus::acknowledged()),
            Some(outbox.clone()),
            LocalEventPublisherConfig {
                capture_limit_bytes: Some(1_000_000),
                ..Default::default()
            },
        );
        let auth = AqsAuth {
            access_method: "test".into(),
            session_id: "session".into(),
            session_secret: "test".into(),
            strategy_id: "strategy".into(),
            user_id: "users:test".into(),
            node_id: None,
            live_session_id: None,
            url: None,
        };
        (path, outbox, LocalBatchSyncState::new(publisher, &auth))
    }

    #[tokio::test]
    async fn deferred_start_retains_exact_event_until_capture_and_durable_receipt() {
        let (path, outbox, mut sync) = fixture().await;
        let event = sync.event(
            "strategy-lifecycle",
            EventPriority::Critical,
            EventKind::StrategyLifecycle,
            serde_json::json!({"state":"running","status":"Running"}),
            None,
        );
        assert!(!sync.publish_start_lifecycle_durable(event.clone()).await);
        assert_eq!(sync.deferred_start.event.as_ref().unwrap(), &event);
        for _ in 0..20 {
            sync.flush_start_lifecycle(true);
        }
        assert_eq!(sync.deferred_start.event.as_ref().unwrap(), &event);
        assert!(
            sync.publisher
                .wait_for_capture_initialization(Duration::from_secs(2))
                .await
        );
        tokio::time::timeout(Duration::from_secs(3), async {
            while sync.deferred_start.event.is_some() {
                sync.flush_start_lifecycle(true);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        for _ in 0..20 {
            sync.flush_start_lifecycle(true);
        }
        let saved = outbox
            .pending_events_for_entity(
                "strategy",
                "session",
                "strategy-lifecycle",
                EventKind::StrategyLifecycle,
                10,
            )
            .await
            .unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].event_id, event.event_id);
        assert_eq!(saved[0].entity_revision, event.entity_revision);
        assert_eq!(saved[0].created_at, event.created_at);
        assert_eq!(saved[0].payload, event.payload);
        sync.publisher.shutdown(Duration::from_secs(2)).await;
        drop(sync);
        drop(outbox);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn teardown_discards_unadmitted_start_without_publishing_running() {
        let (path, outbox, mut sync) = fixture().await;
        let event = sync.event(
            "strategy-lifecycle",
            EventPriority::Critical,
            EventKind::StrategyLifecycle,
            serde_json::json!({"state":"running"}),
            None,
        );
        assert!(!sync.publish_start_lifecycle_durable(event).await);
        sync.flush_start_lifecycle(false);
        assert!(sync.deferred_start.event.is_none());
        assert!(
            sync.publisher
                .wait_for_capture_initialization(Duration::from_secs(2))
                .await
        );
        sync.flush_start_lifecycle(true);
        assert!(
            outbox
                .pending_events_for_entity(
                    "strategy",
                    "session",
                    "strategy-lifecycle",
                    EventKind::StrategyLifecycle,
                    10
                )
                .await
                .unwrap()
                .is_empty()
        );
        sync.publisher.shutdown(Duration::from_secs(2)).await;
        drop(sync);
        drop(outbox);
        let _ = std::fs::remove_file(path);
    }
}
