mod live_memory_regressions {
    use super::*;
    use crate::core::live_sync::{
        EventDelivery, EventSubject, EventSubscription, InProcessEventBus, LocalDeliveryHandle,
        LocalEventBus, LocalEventPublisher, OutboxStore, SyncAcknowledgement,
    };
    use crate::core::strategy::{aqs_types::AqsAuth, live_event_projection::LocalBatchSyncState};
    use async_trait::async_trait;
    use std::time::Duration;

    struct BlockingBus {
        inner: InProcessEventBus,
        release: tokio::sync::Semaphore,
    }

    impl Default for BlockingBus {
        fn default() -> Self {
            Self {
                inner: InProcessEventBus::default(),
                release: tokio::sync::Semaphore::new(0),
            }
        }
    }

    #[async_trait]
    impl LocalEventBus for BlockingBus {
        async fn publish_critical(
            &self,
            subject: EventSubject,
            event: crate::core::live_sync::CloudEvent,
        ) -> Result<LocalDeliveryHandle, crate::core::live_sync::EventBusError> {
            self.release.acquire().await.unwrap().forget();
            self.inner.publish_critical(subject, event).await
        }

        fn publish_telemetry(
            &self,
            subject: EventSubject,
            event: crate::core::live_sync::CloudEvent,
        ) -> Result<(), crate::core::live_sync::EventBusError> {
            self.inner.publish_telemetry(subject, event)
        }

        async fn subscribe(
            &self,
            subject: EventSubject,
        ) -> Result<EventSubscription, crate::core::live_sync::EventBusError> {
            self.inner.subscribe(subject).await
        }

        async fn acknowledge(
            &self,
            delivery: &EventDelivery,
            acknowledgement: SyncAcknowledgement,
        ) -> Result<(), crate::core::live_sync::EventBusError> {
            self.inner.acknowledge(delivery, acknowledgement).await
        }
    }

    fn fixture() -> (
        StrategyState<NoopStrategy, TestExecutionBroker, FixedScaleOutDataFeed>,
        AqsAuth, LocalBatchSyncState,
    ) {
        let state = StrategyState::new(
            "FinalDrain".into(), "1".into(), NoopStrategy,
            UnifiedBroker::new(TestExecutionBroker::with_features(&[]), FixedScaleOutDataFeed::new()),
            StrategyMode::Live, TimeFrame::new(1, TimeFrameUnit::Minute),
        );
        let auth = AqsAuth {
            access_method: "aqe_live".into(), session_id: "session".into(), session_secret: "fixture".into(),
            strategy_id: "strategy".into(), user_id: "users:fixture".into(),
            node_id: None, live_session_id: None, url: None,
        };
        let publisher = LocalEventPublisher::spawn(Arc::new(InProcessEventBus::default()), None, Default::default());
        let sync = LocalBatchSyncState::new(publisher, &auth);
        (state, auth, sync)
    }

    const RECONCILIATION_CRASH_FIXTURE_PATH: &str =
        "AQE_TEST_RECONCILIATION_CRASH_PATH";

    #[tokio::test]
    async fn startup_capture_boundary_requires_exact_durable_gap_and_preserves_real_failure() {
        use crate::core::strategy::live_projection::StartupCloudBoundary;
        let (mut state,auth,mut healthy)=fixture();
        assert_eq!(state.establish_startup_cloud_boundary(&mut healthy,&auth).await.unwrap(),
            StartupCloudBoundary::LocallyAccepted);
        healthy.shutdown(Duration::from_secs(1)).await;
        let missing_directory = std::env::temp_dir().join(format!("missing-startup-gap-{}", uuid::Uuid::new_v4()));
        let missing_store = OutboxStore::open(missing_directory.join("capture.db")).await.unwrap();
        let unavailable = LocalEventPublisher::spawn(
            Arc::new(InProcessEventBus::default()),
            Some(missing_store),
            crate::core::live_sync::LocalEventPublisherConfig {
                // A failed budget initialization must not manufacture a gap
                // receipt. The public hosted builder rejects this value.
                capture_limit_bytes: Some(0),
                ..Default::default()
            },
        );
        let mut missing_gap = LocalBatchSyncState::new(unavailable, &auth);
        assert!(missing_gap.publisher.cloud_capture_paused());
        assert!(
            state.establish_startup_cloud_boundary(&mut missing_gap, &auth).await.is_err(),
            "an unavailable capture budget without durable gap proof is not a successful boundary"
        );
        missing_gap.shutdown(Duration::from_secs(1)).await;
        drop(missing_gap);
        std::fs::remove_dir_all(missing_directory).unwrap();
        let capture=crate::core::live_sync::capture_test_fixture::CaptureFixture::active().await;
        capture.pause().await;
        let mut sync=LocalBatchSyncState::new(capture.publisher.clone(),&auth);
        sync.runtime_id="runtime".into();sync.run_id="run".into();
        let mut row=Insight::default();row.state_log(Some("not exported at startup".into()));
        state.insights.add_insight(row);
        assert_eq!(state.establish_startup_cloud_boundary(&mut sync,&auth).await.unwrap(),
            StartupCloudBoundary::DeferredCaptureGap);
        assert!(state.insights.has_dirty_insights());
        assert!(!sync.publisher.admission_failed());
        sync.runtime_id="another-runtime".into();
        assert!(state.establish_startup_cloud_boundary(&mut sync,&auth).await.is_err(),
            "a previous/foreign runtime gap is not this owner's startup receipt");
        sync.runtime_id="runtime".into();sync.publisher.mark_admission_failed();
        assert!(state.establish_startup_cloud_boundary(&mut sync,&auth).await.is_err(),
            "a durable gap cannot hide a real local admission failure");
        drop(sync);
        capture.shutdown().await;
    }

    #[tokio::test]
    async fn recorded_cloud_gap_defers_stop_analytics_without_claiming_a_full_history_drain() {
        let (mut state,auth,_)=fixture();
        let store=OutboxStore::open(std::env::temp_dir().join(format!("stop-gap-history-{}.db",uuid::Uuid::new_v4()))).await.unwrap();
        let publisher=LocalEventPublisher::spawn(Arc::new(InProcessEventBus::default()),Some(store),
            crate::core::live_sync::LocalEventPublisherConfig{capture_limit_bytes:Some(1024),..Default::default()});
        assert!(publisher.wait_for_capture_initialization(Duration::from_secs(2)).await);
        let mut sync=LocalBatchSyncState::new(publisher.clone(),&auth);
        let mut row=Insight::default();row.state_log(Some("not exported".into()));
        state.insights.add_insight(row);
        assert!(!sync.defer_final_cloud_capture_for_stop().await.unwrap());
        let refused=sync.event("oversized-capture",crate::core::live_sync::EventPriority::Critical,
            crate::core::live_sync::EventKind::InsightProjection,serde_json::json!({"data":"x".repeat(4096)}),None);
        assert!(!sync.publish_projection_batch_durable(vec![refused],vec![]).await);
        assert!(sync.defer_final_cloud_capture_for_stop().await.unwrap());
        assert!(state.insights.has_dirty_insights());
        assert!(state.drain_final_insights_to_local_bus(&mut sync,&auth,Duration::ZERO).await.is_err(),
            "deferred cloud capture is not a successful full-history export");
        assert!(state.insights.has_dirty_insights());
        publisher.mark_admission_failed();
        assert!(!sync.defer_final_cloud_capture_for_stop().await.unwrap(),
            "a real failed-admission guard must not be hidden by a cloud pause");
        publisher.shutdown(Duration::from_secs(1)).await;
    }

    #[test]
    #[ignore = "subprocess crash fixture"]
    fn reconciliation_crash_fixture() {
        let Ok(path) = std::env::var(RECONCILIATION_CRASH_FIXTURE_PATH) else {
            return;
        };
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let (state, auth, _) = fixture();
                let outbox = OutboxStore::open(path).await.unwrap();
                let bus = Arc::new(BlockingBus::default());
                let publisher = LocalEventPublisher::spawn(
                    bus,
                    Some(outbox),
                    Default::default(),
                );
                let mut sync = LocalBatchSyncState::new(publisher, &auth);
                assert!(
                    state
                        .publish_startup_reconciliation_to_local_bus(&mut sync, &auth)
                        .await
                );
                // No publisher shutdown or Rust destructors: the parent must
                // recover the complete generation from SQLite alone.
                std::process::exit(87);
            });
    }

    #[tokio::test]
    async fn final_drain_yields_and_exports_all_history_with_a_bounded_queue() {
        let (mut state, auth, mut sync) = fixture();
        let mut row = Insight::default();
        for n in 0..10_000 { row.state_log(Some(format!("history {n}"))); }
        row.order_cancelled("fixture");
        let total = row.state_history.len() as u64 + 1;
        state.insights.add_insight(row);
        let publisher = sync.publisher.clone();
        let mut turns = 0;
        let drain = state.drain_final_insights_to_local_bus(&mut sync, &auth, Duration::from_secs(10));
        tokio::pin!(drain);
        loop {
            tokio::select! {
                result = &mut drain => { result.unwrap(); break; }
                _ = tokio::time::sleep(Duration::from_millis(1)) => {
                    turns += 1;
                    assert!(publisher.pending_critical_count() <= 4096);
                    assert!(publisher.queued_critical_encoded_bytes() <= 16 * 1024 * 1024);
                }
            }
        }
        assert!(turns > 1, "drain must yield instead of enqueueing the whole burst");
        assert_eq!(publisher.health().locally_accepted_critical, total);
        assert_eq!(publisher.pending_critical_count(), 0);
        publisher.shutdown(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn history_planning_respects_a_partially_drained_queue_watermark() {
        let (mut state,auth,_)=fixture();
        let bus=Arc::new(BlockingBus::default());
        let publisher=LocalEventPublisher::spawn(bus.clone(),None,Default::default());
        let mut sync=LocalBatchSyncState::new(publisher.clone(),&auth);
        let subject=EventSubject::for_priority(&sync.runtime_id,&auth.strategy_id,crate::core::live_sync::EventPriority::Critical);
        for index in 0..4090 {
            let event=crate::core::live_sync::EventEnvelope::new(&sync.runtime_id,&auth.strategy_id,&sync.live_session_id,
                format!("filler:{index}"),1,crate::core::live_sync::EventPriority::Critical,
                crate::core::live_sync::EventKind::InsightProjection,json!({"index":index}));
            publisher.try_publish_critical(subject.clone(),event).unwrap();
        }
        let mut row=Insight::default();
        for index in 0..32 {row.state_log(Some(format!("history {index}")));}
        let id=row.insight_id;
        state.insights.add_insight(row);
        state.publish_dirty_insights_to_local_bus(&mut sync,&auth,Some(128),true).await;
        assert_eq!(publisher.pending_critical_count(),4096);
        assert_eq!(sync.history_offsets.get(&id.to_string()),Some(&5));
        assert!(state.insights.has_dirty_insights(),"unplanned history remains owned by its source");
        bus.release.add_permits(4096);
        publisher.shutdown(Duration::from_secs(3)).await;
        assert_eq!(publisher.pending_critical_count(),0);
    }

    #[tokio::test]
    async fn failed_enqueue_preserves_dirty_row_and_history_cursor() {
        let (mut state, auth, mut sync) = fixture();
        sync.publisher.shutdown(Duration::from_secs(1)).await;
        let mut row = Insight::default();
        row.state_log(Some("not yet exported".into()));
        let id = row.insight_id;
        state.insights.add_insight(row);
        state.publish_dirty_insights_to_local_bus(&mut sync, &auth, Some(128), true).await;
        assert!(state.insights.has_dirty_insights());
        assert!(state.insights.get(&id).is_some());
        assert!(sync.insight_projection_fingerprints.is_empty());
        assert!(sync.history_offsets.is_empty());
        assert!(state.drain_final_insights_to_local_bus(&mut sync, &auth, Duration::from_secs(1)).await.is_err());
    }

    #[tokio::test]
    async fn hosted_projection_clears_source_only_after_local_outbox_commit() {
        let (mut state, auth, _) = fixture();
        let path = std::env::temp_dir().join(format!(
            "aqe-insight-boundary-{}.db",
            uuid::Uuid::new_v4()
        ));
        let outbox = OutboxStore::open(&path).await.unwrap();
        let bus = Arc::new(BlockingBus::default());
        let publisher = LocalEventPublisher::spawn(
            bus.clone(),
            Some(outbox.clone()),
            Default::default(),
        );
        let mut sync = LocalBatchSyncState::new(publisher.clone(), &auth);
        let mut row = Insight::default();
        row.state_log(Some("durable history".into()));
        let id = row.insight_id;
        state.insights.add_insight(row);

        tokio::time::timeout(
            Duration::from_secs(1),
            state.publish_dirty_insights_to_local_bus(&mut sync, &auth, Some(128), true),
        )
        .await
        .expect("local durability must not wait for blocked transport");
        assert!(!state.insights.has_dirty_insights());
        assert_eq!(sync.history_offsets.get(&id.to_string()), Some(&1));
        assert_eq!(
            outbox
                .pending_events_for_entity(
                    "strategy",
                    "session",
                    &format!("insight:{id}"),
                    crate::core::live_sync::EventKind::InsightProjection,
                    10,
                )
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            outbox
                .pending_events_for_entity(
                    "strategy",
                    "session",
                    &format!("insight-history:{id}"),
                    crate::core::live_sync::EventKind::InsightStateHistory,
                    10,
                )
                .await
                .unwrap()
                .len(),
            1
        );

        bus.release.add_permits(2);
        publisher.shutdown(Duration::from_secs(2)).await;
        assert!(publisher.shutdown_complete());
        drop(sync);
        drop(publisher);
        drop(outbox);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn startup_reconciliation_is_durable_before_transport_delivery() {
        let (state, auth, _) = fixture();
        let path = std::env::temp_dir().join(format!(
            "aqe-reconciliation-boundary-{}.db",
            uuid::Uuid::new_v4()
        ));
        let outbox = OutboxStore::open(&path).await.unwrap();
        let bus = Arc::new(BlockingBus::default());
        let publisher = LocalEventPublisher::spawn(
            bus.clone(),
            Some(outbox.clone()),
            Default::default(),
        );
        let mut sync = LocalBatchSyncState::new(publisher.clone(), &auth);
        let generation = crate::core::live_sync::deterministic_event_id(&format!(
            "aqs-reconciliation-v4:{}:{}:{}",
            auth.strategy_id, sync.live_session_id, sync.run_id
        ))
        .to_string();

        tokio::time::timeout(
            Duration::from_secs(1),
            state.publish_startup_reconciliation_to_local_bus(&mut sync, &auth),
        )
        .await
        .expect("local durability must not wait for blocked transport")
        .then_some(())
        .expect("startup reconciliation should be admitted");

        assert_eq!(
            outbox
                .pending_events_for_entity(
                    "strategy",
                    "session",
                    &format!("reconciliation:{generation}:start"),
                    crate::core::live_sync::EventKind::ReconciliationStart,
                    10,
                )
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            outbox
                .pending_events_for_entity(
                    "strategy",
                    "session",
                    &format!("reconciliation:{generation}:complete"),
                    crate::core::live_sync::EventKind::ReconciliationComplete,
                    10,
                )
                .await
                .unwrap()
                .len(),
            1
        );

        bus.release.add_permits(2);
        publisher.shutdown(Duration::from_secs(2)).await;
        drop(sync);
        drop(publisher);
        drop(outbox);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn startup_reconciliation_replays_after_abrupt_process_exit() {
        let path = std::env::temp_dir().join(format!(
            "aqe-reconciliation-crash-{}.db",
            uuid::Uuid::new_v4()
        ));
        let executable = std::env::current_exe().unwrap();
        let child_path = path.clone();
        let status = tokio::task::spawn_blocking(move || {
            std::process::Command::new(executable)
                .arg("core::strategy::tests::tests::live_memory_regressions::reconciliation_crash_fixture")
                .arg("--exact")
                .arg("--ignored")
                .env(RECONCILIATION_CRASH_FIXTURE_PATH, child_path)
                .status()
                .unwrap()
        })
        .await
        .unwrap();
        assert_eq!(status.code(), Some(87));

        let outbox = OutboxStore::open(&path).await.unwrap();
        let pending = outbox
            .pending_batch(
                crate::core::live_sync::EventPriority::Reconciliation,
                10,
                1024 * 1024,
            )
            .await
            .unwrap();
        assert_eq!(pending.len(), 2);
        assert!(pending.iter().any(|row| {
            row.event.kind == crate::core::live_sync::EventKind::ReconciliationStart
        }));
        assert!(pending.iter().any(|row| {
            row.event.kind == crate::core::live_sync::EventKind::ReconciliationComplete
        }));
        let runtime_id = pending[0].event.runtime_id.clone();
        let expected_ids = pending
            .iter()
            .map(|row| row.event.event_id)
            .collect::<std::collections::HashSet<_>>();
        let bus = Arc::new(InProcessEventBus::default());
        let mut subscription = bus
            .subscribe(EventSubject::for_priority(
                &runtime_id,
                "strategy",
                crate::core::live_sync::EventPriority::Reconciliation,
            ))
            .await
            .unwrap();
        let publisher = LocalEventPublisher::spawn(bus, Some(outbox.clone()), Default::default());
        let mut replayed = std::collections::HashSet::new();
        for _ in 0..2 {
            replayed.insert(
                tokio::time::timeout(Duration::from_secs(2), subscription.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .event
                    .event_id,
            );
        }
        assert_eq!(replayed, expected_ids);
        publisher.shutdown(Duration::from_secs(2)).await;
        drop(publisher);
        drop(outbox);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn final_drain_deadline_does_not_discard_remaining_history() {
        let (mut state, auth, mut sync) = fixture();
        let mut row = Insight::default();
        for n in 0..1000 { row.state_log(Some(format!("history {n}"))); }
        let id = row.insight_id;
        state.insights.add_insight(row);
        assert!(state.drain_final_insights_to_local_bus(&mut sync, &auth, Duration::ZERO).await.is_err());
        assert!(state.insights.has_dirty_insights());
        assert_eq!(sync.history_offsets.get(&id.to_string()), Some(&255));
        state.drain_final_insights_to_local_bus(&mut sync, &auth, Duration::from_secs(5)).await.unwrap();
        assert!(!state.insights.has_dirty_insights());
        sync.publisher.shutdown(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn history_burst_pauses_at_high_water_and_resumes_without_skipping_rows() {
        let mut state = StrategyState::new(
            "Burst".into(),
            "1".into(),
            NoopStrategy,
            UnifiedBroker::new(
                TestExecutionBroker::with_features(&[]),
                FixedScaleOutDataFeed::new(),
            ),
            StrategyMode::Live,
            TimeFrame::new(1, TimeFrameUnit::Minute),
        );
        let auth = AqsAuth {
            access_method: "aqe_live".into(),
            session_id: "session".into(),
            session_secret: "fixture".into(),
            strategy_id: "strategy".into(),
            user_id: "users:fixture".into(),
            node_id: None,
            live_session_id: None,
            url: None,
        };
        let publisher = LocalEventPublisher::spawn(
            Arc::new(InProcessEventBus::default()),
            None,
            Default::default(),
        );
        let mut sync = LocalBatchSyncState::new(publisher.clone(), &auth);
        let mut row = Insight::default();
        row.symbol = "AAPL".into();
        for index in 0..5000 {
            row.state_log(Some(format!("history {index}")));
        }
        row.order_cancelled("test");
        let id = row.insight_id;
        let total = row.state_history.len() as u64 + 1;
        state.insights.add_insight(row);
        state.publish_dirty_insights_to_local_bus(&mut sync, &auth, Some(128), true).await;
        assert_eq!(publisher.pending_critical_count(), 256);
        for _ in 0..100 {
            state.publish_dirty_insights_to_local_bus(&mut sync, &auth, Some(128), true).await;
        }
        assert_eq!(publisher.pending_critical_count(), 4096);
        assert!(state.insights.get(&id).is_some());
        assert_eq!(sync.history_offsets.get(&id.to_string()), Some(&4095));
        tokio::time::timeout(Duration::from_secs(10), async {
            while state.insights.get(&id).is_some() {
                tokio::time::sleep(Duration::from_millis(10)).await;
                state.publish_dirty_insights_to_local_bus(&mut sync, &auth, Some(128), true).await;
            }
        })
        .await
        .unwrap();
        publisher.shutdown(Duration::from_secs(5)).await;
        assert_eq!(publisher.pending_critical_count(), 0);
        assert_eq!(publisher.queued_critical_encoded_bytes(), 0);
        assert_eq!(publisher.health().locally_accepted_critical, total);
        assert_eq!(publisher.health().publish_failures, 0);
    }

    #[tokio::test]
    async fn pinned_terminal_insight_does_not_republish_history_on_every_flush() {
        let mut state = StrategyState::new(
            "PinnedTerminal".into(),
            "1".into(),
            NoopStrategy,
            UnifiedBroker::new(
                TestExecutionBroker::with_features(&[]),
                FixedScaleOutDataFeed::new(),
            ),
            StrategyMode::Live,
            TimeFrame::new(1, TimeFrameUnit::Minute),
        );
        let auth = AqsAuth {
            access_method: "aqe_live".into(),
            session_id: "session".into(),
            session_secret: "fixture".into(),
            strategy_id: "strategy".into(),
            user_id: "users:fixture".into(),
            node_id: None,
            live_session_id: None,
            url: None,
        };
        let publisher = LocalEventPublisher::spawn(
            Arc::new(InProcessEventBus::default()),
            None,
            Default::default(),
        );
        let mut sync = LocalBatchSyncState::new(publisher.clone(), &auth);
        let mut row = Insight::default();
        row.symbol = "AAPL".into();
        for index in 0..100 {
            row.state_log(Some(format!("initial configuration {index}")));
        }
        row.order_cancelled("fixture cancellation");
        let id = row.insight_id;
        let history_len = row.state_history.len();
        state.insights.add_insight(row);
        state.insights.enable_execution_persistence();
        state.insights.execution_pin(id);
        state.insights.execution_saved(&id);
        state.publish_dirty_insights_to_local_bus(&mut sync, &auth, Some(128), true).await;
        let pending = publisher.health().pending_critical;
        assert_eq!(pending, history_len as u64 + 1);
        for _ in 0..1000 {
            state.publish_dirty_insights_to_local_bus(&mut sync, &auth, Some(128), true).await;
        }
        assert!(state.insights.get(&id).is_some());
        assert_eq!(publisher.health().pending_critical, pending);
        assert_eq!(
            sync.history_offsets.get(&id.to_string()),
            Some(&history_len)
        );
        assert_eq!(sync.insight_projection_fingerprints.len(), 1);
        state.insights.execution_unpin(&id);
        state.publish_dirty_insights_to_local_bus(&mut sync, &auth, Some(128), true).await;
        assert!(state.insights.get(&id).is_none());
        assert!(sync.history_offsets.is_empty());
        assert!(sync.insight_projection_fingerprints.is_empty());
        assert_eq!(publisher.health().pending_critical, pending);
        publisher.shutdown(Duration::from_secs(5)).await;
        assert_eq!(publisher.health().pending_critical, 0);
        assert_eq!(publisher.queued_critical_encoded_bytes(), 0);
    }
}
