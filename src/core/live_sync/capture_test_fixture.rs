//! Owned, temporary capture state for cross-module producer regressions.
use super::*;
use std::{sync::Arc, time::Duration};

pub(crate) struct CaptureFixture {
    pub publisher: LocalEventPublisher,
    store: OutboxStore,
    directory: std::path::PathBuf,
}

impl CaptureFixture {
    pub async fn active() -> Self {
        let directory =
            std::env::temp_dir().join(format!("source-capture-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let store = OutboxStore::open(directory.join("capture.db"))
            .await
            .unwrap();
        let publisher = LocalEventPublisher::spawn(
            Arc::new(InProcessEventBus::default()),
            Some(store.clone()),
            LocalEventPublisherConfig {
                capture_limit_bytes: Some(1024),
                critical_batch_delay: Duration::from_millis(1),
                ..Default::default()
            },
        );
        assert!(
            publisher
                .wait_for_capture_initialization(Duration::from_secs(3))
                .await
        );
        assert!(!publisher.cloud_capture_paused());
        Self {
            publisher,
            store,
            directory,
        }
    }

    pub async fn pause(&self) {
        let event = EventEnvelope::new(
            "runtime",
            "strategy",
            "session",
            "source-cap",
            1,
            EventPriority::Critical,
            EventKind::InsightProjection,
            serde_json::json!({"data":"x".repeat(4096)}),
        );
        let receipt = self
            .publisher
            .queue_durable(
                EventSubject::for_priority("runtime", "strategy", EventPriority::Critical),
                event,
            )
            .unwrap();
        assert!(matches!(
            receipt.await.unwrap(),
            Err(EventBusError::CloudCapturePaused)
        ));
        assert!(
            self.store
                .capture_pause_for_scope("runtime", "strategy", "session")
                .await
                .unwrap()
                .is_some()
        );
        assert!(self.publisher.cloud_capture_paused());
        assert!(!self.publisher.admission_failed());
    }

    pub async fn shutdown(self) {
        self.publisher.shutdown(Duration::from_secs(2)).await;
        drop(self.publisher);
        drop(self.store);
        std::fs::remove_dir_all(self.directory).unwrap();
    }
}
