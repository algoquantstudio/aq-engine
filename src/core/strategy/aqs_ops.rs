use super::aqs_actions::{update_strategy_action_status, update_strategy_action_statuses};
use super::aqs_types::{
    self, AqsAuth, StrategyActionKind, StrategyActionStatus, StrategyEventLevel,
    StrategyEventRecord, StrategyEventTitle, StrategyEventType, StrategyUniverseAssetRecord,
    action_id_from_value,
};
use super::live_event_projection::LocalBatchSyncState;
#[cfg(test)]
use super::live_event_projection::{advance_entity_revision, without_json_nulls};
use super::local_live_artifacts::{
    LocalLiveEventDetails, LocalLiveRunArtifacts, MAX_PENDING_LOCAL_LIVE_EVENTS,
    PendingLocalLiveEvent, enqueue_pending_local_live_event,
};
use super::traits::Strategy;
use super::{StrategyState, StrategyStatus};
use crate::core::broker::DataStreamMode;
use crate::core::broker::traits::{Broker, DataFeed, DataProvider, OrderManagementProvider};
use crate::core::broker::types::{
    Account, BrokerError, OrderLifecycleSnapshot, OrderLifecycleStatus,
};
use crate::core::events::{MarketDataEvent, ResolvedEventStream};
use crate::core::lifecycle::LifecycleTiming;
use crate::core::live_sync::{
    EventEnvelope, EventKind, EventPriority, EventSubject, LatencyTrace, LocalEventPublisher,
    LocalEventPublisherConfig, ReconciliationLifecycleRequest, ReconciliationLifecycleResult,
    ReconciliationRecord, deterministic_event_id, now_epoch_micros,
};
use futures::StreamExt;
use log::{debug, error, info, warn};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

const LIVE_SYNC_BACKGROUND_RETRY_INITIAL_SECS: u64 = 5;
const LIVE_TRADE_NOTIFICATION_BATCH_SIZE: usize = 256;
const LIVE_SYNC_BACKGROUND_RETRY_MAX_SECS: u64 = 60;
const LIVE_SYNC_BACKGROUND_RETRY_POLL_MS: u64 = 500;
const LIVE_SYNC_FLUSH_MS: u64 = 100;
const LOCAL_SHUTDOWN_SIGNAL_POLL_MS: u64 = 100;
const LIVE_HEARTBEAT_SECS: u64 = 1;
const LIVE_SYNC_MAX_INSIGHTS_PER_FLUSH: usize = 128;
const LIVE_METRICS_SAMPLE_SECS: u64 = 5;
const LOCAL_LIVE_METRICS_WRITE_SECS: u64 = 5;
// Broker callbacks cannot await the strategy loop. Keep their handoff queues bounded and stop the
// live run on overflow rather than silently dropping lifecycle events/bars or allowing malformed
// replay traffic to exhaust the process heap.
const LIVE_BROKER_EVENT_QUEUE_CAPACITY: usize = 8_192;
const LIVE_MARKET_DATA_QUEUE_CAPACITY: usize = 4_096;

// Repeated stop requests join the existing teardown; they never rerun hooks or
// extend its deadline. Failed publication remains retryable, not acknowledged.
async fn acknowledge_teardown_stop(
    value: &serde_json::Value,
    processed: &mut HashSet<String>,
    sync: Option<&mut LocalBatchSyncState>,
) -> Option<String> {
    acknowledge_teardown_stop_delivery(value, processed, sync, None).await
}

async fn acknowledge_teardown_stop_delivery(
    value: &serde_json::Value,
    processed: &mut HashSet<String>,
    sync: Option<&mut LocalBatchSyncState>,
    delivery: Option<crate::core::live_sync::EventDelivery>,
) -> Option<String> {
    let action: StrategyActionKind = serde_json::from_value(value.get("action")?.clone()).ok()?;
    let status: StrategyActionStatus = serde_json::from_value(value.get("status")?.clone()).ok()?;
    if action != StrategyActionKind::Stop || status != StrategyActionStatus::Pending {
        return None;
    }
    let id = value
        .get("action_id")
        .or_else(|| value.get("id"))
        .and_then(action_id_from_value)?;
    if processed.contains(&id) && delivery.is_none() {
        return Some(id);
    }
    // Bound malicious/repeated controls while retaining already accepted IDs.
    if processed.len() >= 256 {
        return None;
    }
    let sync = sync?;
    let event = sync.event(
        format!("strategy-action:{id}"),
        EventPriority::Critical,
        EventKind::StrategyActionAcknowledgement,
        serde_json::json!({
            "action_id": id, "action": StrategyActionKind::Stop,
            "status": StrategyActionStatus::Acknowledged,
            "message": "Stop received; graceful teardown is already in progress",
            "acknowledged_at": chrono::Utc::now(),
        }),
        None,
    );
    let accepted = match delivery {
        Some(delivery) => sync.publish_control_ack(event, delivery),
        None => sync.publish_critical_durable(event).await,
    };
    if !accepted {
        return None;
    }
    processed.insert(id.clone());
    Some(id)
}

#[cfg(test)]
mod teardown_control_tests {
    use super::*;
    use crate::core::live_sync::{
        EventDelivery, EventSubscription, InProcessEventBus, LocalDeliveryHandle, LocalEventBus,
        OutboxStore, SyncAcknowledgement,
    };

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

    #[async_trait::async_trait]
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

    fn auth() -> AqsAuth {
        AqsAuth {
            access_method: "test".into(),
            session_id: "session".into(),
            session_secret: "test".into(),
            strategy_id: "strategy".into(),
            user_id: "users:test".into(),
            node_id: None,
            live_session_id: None,
            url: None,
        }
    }

    const STOP_ACK_CRASH_FIXTURE_PATH: &str = "AQE_TEST_STOP_ACK_CRASH_PATH";

    #[test]
    #[ignore = "subprocess crash fixture"]
    fn stop_ack_crash_fixture() {
        let Ok(path) = std::env::var(STOP_ACK_CRASH_FIXTURE_PATH) else {
            return;
        };
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let outbox = OutboxStore::open(path).await.unwrap();
                let bus = Arc::new(BlockingBus::default());
                let publisher = LocalEventPublisher::spawn(bus, Some(outbox), Default::default());
                let mut sync = LocalBatchSyncState::new(publisher, &auth());
                let mut processed = HashSet::new();
                let action = serde_json::json!({
                    "action_id": "strategy_actions:crash",
                    "action": "stop",
                    "status": "pending"
                });
                assert_eq!(
                    acknowledge_teardown_stop(&action, &mut processed, Some(&mut sync))
                        .await
                        .as_deref(),
                    Some("strategy_actions:crash")
                );
                // Intentionally bypass destructors and publisher shutdown. The
                // parent test must recover solely from the committed SQLite WAL.
                std::process::exit(86);
            });
    }

    #[tokio::test]
    async fn teardown_controls_are_idempotent_bounded_and_retry_failed_publication() {
        let publisher = LocalEventPublisher::spawn(
            Arc::new(InProcessEventBus::default()),
            None,
            Default::default(),
        );
        let mut sync = LocalBatchSyncState::new(publisher, &auth());
        let mut processed = HashSet::new();
        let action = serde_json::json!({"action_id":"strategy_actions:first", "action":"stop", "status":"pending"});
        assert!(
            acknowledge_teardown_stop(&action, &mut processed, Some(&mut sync))
                .await
                .is_some()
        );
        let queued = sync.publisher.pending_critical_count();
        assert!(
            acknowledge_teardown_stop(&action, &mut processed, Some(&mut sync))
                .await
                .is_some()
        );
        assert_eq!(sync.publisher.pending_critical_count(), queued);
        assert_eq!(processed.len(), 1);
        let second = serde_json::json!({"id":"strategy_actions:second", "action":"stop", "status":"pending"});
        assert!(
            acknowledge_teardown_stop(&second, &mut processed, Some(&mut sync))
                .await
                .is_some()
        );
        assert_eq!(processed.len(), 2);
        let completed = serde_json::json!({"id":"strategy_actions:complete", "action":"stop", "status":"completed"});
        assert!(
            acknowledge_teardown_stop(&completed, &mut processed, Some(&mut sync))
                .await
                .is_none()
        );
        sync.publisher.shutdown(Duration::ZERO).await;
        let failed =
            serde_json::json!({"id":"strategy_actions:retry", "action":"stop", "status":"pending"});
        assert!(
            acknowledge_teardown_stop(&failed, &mut processed, Some(&mut sync))
                .await
                .is_none()
        );
        assert!(!processed.contains("strategy_actions:retry"));
        processed.extend((0..256).map(|i| format!("bounded-{i}")));
        assert!(
            acknowledge_teardown_stop(&failed, &mut processed, None)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn database_stop_acknowledgement_is_durable_before_transport_delivery() {
        let path =
            std::env::temp_dir().join(format!("aqe-stop-ack-boundary-{}.db", uuid::Uuid::new_v4()));
        let outbox = OutboxStore::open(&path).await.unwrap();
        let bus = Arc::new(BlockingBus::default());
        let publisher =
            LocalEventPublisher::spawn(bus.clone(), Some(outbox.clone()), Default::default());
        let mut sync = LocalBatchSyncState::new(publisher.clone(), &auth());
        let mut processed = HashSet::new();
        let action = serde_json::json!({
            "action_id": "strategy_actions:durable",
            "action": "stop",
            "status": "pending"
        });

        let accepted = tokio::time::timeout(
            Duration::from_secs(1),
            acknowledge_teardown_stop(&action, &mut processed, Some(&mut sync)),
        )
        .await
        .expect("local durability must not wait for blocked transport");
        assert_eq!(accepted.as_deref(), Some("strategy_actions:durable"));
        assert!(processed.contains("strategy_actions:durable"));
        assert_eq!(
            outbox
                .pending_events_for_entity(
                    "strategy",
                    "session",
                    "strategy-action:strategy_actions:durable",
                    EventKind::StrategyActionAcknowledgement,
                    10,
                )
                .await
                .unwrap()
                .len(),
            1
        );

        bus.release.add_permits(1);
        publisher.shutdown(Duration::from_secs(2)).await;
        drop(sync);
        drop(publisher);
        drop(outbox);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn database_stop_acknowledgement_replays_after_abrupt_process_exit() {
        let path =
            std::env::temp_dir().join(format!("aqe-stop-ack-crash-{}.db", uuid::Uuid::new_v4()));
        let executable = std::env::current_exe().unwrap();
        let child_path = path.clone();
        let status = tokio::task::spawn_blocking(move || {
            std::process::Command::new(executable)
                .arg("core::strategy::aqs_ops::teardown_control_tests::stop_ack_crash_fixture")
                .arg("--exact")
                .arg("--ignored")
                .env(STOP_ACK_CRASH_FIXTURE_PATH, child_path)
                .status()
                .unwrap()
        })
        .await
        .unwrap();
        assert_eq!(status.code(), Some(86));

        let outbox = OutboxStore::open(&path).await.unwrap();
        let pending = outbox
            .pending_events_for_entity(
                "strategy",
                "session",
                "strategy-action:strategy_actions:crash",
                EventKind::StrategyActionAcknowledgement,
                10,
            )
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        let expected_id = pending[0].event_id;
        let runtime_id = pending[0].runtime_id.clone();
        let bus = Arc::new(InProcessEventBus::default());
        let mut subscription = bus
            .subscribe(EventSubject::for_priority(
                &runtime_id,
                "strategy",
                EventPriority::Critical,
            ))
            .await
            .unwrap();
        let publisher = LocalEventPublisher::spawn(bus, Some(outbox.clone()), Default::default());
        let delivery = tokio::time::timeout(Duration::from_secs(2), subscription.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delivery.event.event_id, expected_id);
        assert_eq!(
            delivery.event.entity_id,
            "strategy-action:strategy_actions:crash"
        );
        publisher.shutdown(Duration::from_secs(2)).await;
        drop(publisher);
        drop(outbox);
        let _ = std::fs::remove_file(path);
    }
}

fn next_live_sync_retry_delay(current: Duration) -> Duration {
    Duration::from_secs(current.as_secs().saturating_mul(2).clamp(
        LIVE_SYNC_BACKGROUND_RETRY_INITIAL_SECS,
        LIVE_SYNC_BACKGROUND_RETRY_MAX_SECS,
    ))
}

// ─────────────────────── Live Runner ───────────────────────

impl<S, E, D> StrategyState<S, E, D>
where
    S: Strategy + Send + Sync + 'static,
    E: Broker + OrderManagementProvider + Send + Sync + 'static,
    D: DataFeed + DataProvider + Send + Sync + 'static,
{
    pub(super) async fn rollback_failed_live_startup(&mut self, strategy: S, phase: &str) {
        self.status = StrategyStatus::Stopping;

        // Wake any workers created during startup before disconnecting their transports.
        self.shutdown();

        self.stop_legacy_metrics_recovery().await;

        if let Err(error) = self.stop_live_execution_worker().await {
            warn!("Failed to release execution worker during startup rollback: {error}");
        }
        self.stop_metrics_checkpoint_writer().await;

        // This rollback deliberately disconnects each half separately, so it
        // must release the unified read worker before either transport closes.
        self.broker.stop_connection_worker().await;
        self.broker.stop_snapshot_reads().await;

        // Disconnect both halves even when one transport reports a shutdown error.
        if let Err(error) = self.broker.disconnect_datafeed().await {
            warn!(
                "Failed to disconnect data feed while rolling back live startup after {}: {}",
                phase, error
            );
        }
        if let Err(error) = self.broker.disconnect_broker().await {
            warn!(
                "Failed to disconnect execution broker while rolling back live startup after {}: {}",
                phase, error
            );
        }

        self.strategy = Some(strategy);
        self.history_seed_anchor = None;
        self.status = StrategyStatus::Stopped;
        self.publish_runtime_snapshot("stopped", None);
        self.runtime_telemetry.stop_tui();
    }

    /// Run the strategy in live execution mode.
    ///
    /// This will register callbacks for trade events & bar streams,
    /// then loop via `tokio::select!` block listening on channels to drive the pipeline.
    pub async fn run_live(&mut self, auth: Option<AqsAuth>) -> Result<(), BrokerError> {
        self.run_live_with_teardown_timeout(auth, Duration::from_secs(90))
            .await
    }

    pub(super) async fn run_live_with_teardown_timeout(
        &mut self,
        auth: Option<AqsAuth>,
        teardown_timeout: Duration,
    ) -> Result<(), BrokerError> {
        super::ensure_rustls_crypto_provider();
        // Resolve/validate once before broker connection. A bad selected launch
        // file must never silently fall back to unauthenticated standalone live.
        let launch = crate::launch_config::from_process().map_err(BrokerError::ConnectionError)?;
        let configured_auth = launch.and_then(|c| c.cloud.as_ref()).map(AqsAuth::from_launch);
        let configured_hosted = match launch.and_then(|c| c.delivery.as_ref()) {
            Some(crate::launch_config::DeliveryConfig::EmbeddedGateway { url, outbox_directory, .. }) =>
                Some(crate::core::live_sync::HostedSyncConfig::new(url, outbox_directory)
                    .and_then(|config| config.with_launch_run(&launch.expect("delivery came from a launch file").run_id))
                    .map_err(BrokerError::ConnectionError)?),
            _ => None,
        };
        self.apply_hyper_parameters_from_process_args()?;
        let auth = configured_auth.or(auth).or_else(|| self.default_live_auth.clone());
        let mut embedded_sync = if let Some(config) = configured_hosted.as_ref().or(self.hosted_sync.as_ref()) {
            let credentials = auth.as_ref().ok_or_else(|| {
                BrokerError::ConnectionError(
                    "Hosted sync requires AQS live-session credentials".into(),
                )
            })?;
            Some(
                crate::core::live_sync::EmbeddedHostedSync::start(config, credentials)
                    .await
                    .map_err(BrokerError::ConnectionError)?,
            )
        } else {
            None
        };
        self.runtime_telemetry
            .start_tui(crate::core::tui::TuiConfig::from_process_args());
        self.publish_runtime_snapshot(
            if auth.is_some() {
                "connecting"
            } else {
                "not configured"
            },
            None,
        );
        let mut local_batch_sync = if let Some(embedded) = embedded_sync.as_ref() {
            let mut sync = LocalBatchSyncState::new(
                embedded.publisher.clone(),
                auth.as_ref().expect("hosted credentials checked"),
            );
            sync.runtime_id = embedded.runtime_id.clone();
            sync.run_id = embedded.run_id.clone();
            Some(sync)
        } else if auth.is_some() {
            let emergency_path = crate::launch_config::setting("AQE_OUTBOX_PATH")
                .map_err(BrokerError::ConnectionError)?.map(PathBuf::from);
            let sequence_strategy = auth.as_ref().map(|auth| auth.strategy_id.clone());
            let sequence_session = auth.as_ref().map(Self::live_session_key_for_auth);
            let sequence_scope = sequence_strategy
                .as_deref()
                .zip(sequence_session.as_deref());
            match LocalEventPublisher::from_environment(
                emergency_path,
                sequence_scope,
                LocalEventPublisherConfig::default(),
            )
            .await
            {
                Ok(publisher) => auth
                    .as_ref()
                    .map(|auth| LocalBatchSyncState::new(publisher, auth)),
                Err(error) => {
                    return Err(BrokerError::ConnectionError(format!(
                        "AQS event publisher could not start; refusing to use the retired direct-write path: {error}"
                    )));
                }
            }
        } else {
            None
        };
        let event_gateway_primary = local_batch_sync.is_some();
        let runtime_transport = if embedded_sync.is_some() {
            "Embedded hosted gateway"
        } else if crate::launch_config::setting("AQE_EVENT_BUS")
            .is_ok_and(|value| value.is_some_and(|value| value.eq_ignore_ascii_case("nats")))
        {
            "Managed NATS publisher"
        } else {
            "In-process publisher"
        };
        // AQE retains a direct SurrealDB connection for authenticated reads, metrics
        // continuity and strategy actions. It is never used as a projection writer.
        let mut db = if let Some(ref a) = auth {
            match Self::connect_live_sync_db(a).await {
                Ok(db) => Some(db),
                Err(error) => {
                    warn!(
                        "AQS Cloud read/action connection is unavailable at startup for strategy {}; continuing broker execution and retrying in the background: {}",
                        a.strategy_id, error
                    );
                    self.publish_runtime_event(
                        "warn",
                        format!(
                            "AQS read/action connection unavailable at startup; event delivery will continue through the runtime host: {error}"
                        ),
                        "offline",
                    );
                    None
                }
            }
        } else {
            None
        };

        // Take strategy out to avoid split-borrow
        let mut strategy = self
            .strategy
            .take()
            .expect("strategy must be Some before run_live");

        if let Err(error) = self
            .broker
            .configure_live_session(&self.strategy_id.to_string())
        {
            error!(
                "Live execution failed while configuring broker session: {}",
                error
            );
            self.rollback_failed_live_startup(strategy, "broker session configuration")
                .await;
            return Err(error);
        }

        // 1. Connect broker
        if let Err(error) = self.broker.connect().await {
            error!("Live execution failed while connecting broker: {}", error);
            self.rollback_failed_live_startup(strategy, "broker connection")
                .await;
            return Err(error);
        }

        // Broker lifecycle resolution is isolated from the strategy select loop. Reconciliation
        // may involve a remote broker history request, so it must never hold up bars, fills, risk
        // controls, or teardown. Unsupported brokers return explicit Unknown evidence.
        // Read/subscription tasks belong to this run. A dropped JoinHandle would
        // detach them, retaining broker/publisher references through teardown.
        // This task set must never own a broker mutation (those are journaled).
        let mut live_background_reads = tokio::task::JoinSet::new();
        if let Some(sync) = local_batch_sync.as_ref() {
            let request_subject =
                EventSubject::reconciliation_lifecycle_request(&sync.runtime_id, &sync.strategy_id);
            match sync.publisher.subscribe(request_subject).await {
                Ok(mut requests) => {
                    let publisher = sync.publisher.clone();
                    let reconciler = self.broker.lifecycle_reconciler();
                    let runtime_id = sync.runtime_id.clone();
                    let strategy_id = sync.strategy_id.clone();
                    let live_session_id = sync.live_session_id.clone();
                    let run_id = sync.run_id.clone();
                    let mut worker_shutdown = self.shutdown_tx.subscribe();
                    live_background_reads.spawn(async move {
                        loop {
                            let delivery = tokio::select! {
                                changed = worker_shutdown.changed() => {
                                    if changed.is_err() || *worker_shutdown.borrow() {
                                        break;
                                    }
                                    continue;
                                }
                                delivery = requests.recv() => match delivery {
                                    Some(delivery) => delivery,
                                    None => break,
                                }
                            };
                            let Ok(request) =
                                serde_json::from_value::<ReconciliationLifecycleRequest>(
                                    delivery.event.payload.clone(),
                                )
                            else {
                                continue;
                            };
                            let resolved = match reconciler.as_ref() {
                                Some(reconciler) => reconciler.reconcile(&request.order_ids).await,
                                None => Ok(request
                                    .order_ids
                                    .iter()
                                    .map(|order_id| OrderLifecycleSnapshot {
                                        close_details: None,
                                        order_id: order_id.clone(),
                                        status: OrderLifecycleStatus::Unknown,
                                        position_id: None,
                                        position_qty: None,
                                        symbol: None,
                                        detail: Some(
                                            "broker does not support authoritative order lifecycle snapshots"
                                                .to_string(),
                                        ),
                                    })
                                    .collect()),
                            };
                            let (snapshots, resolver_error) = match resolved {
                                Ok(snapshots) => (snapshots, None),
                                Err(error) => (Vec::new(), Some(error.to_string())),
                            };
                            let by_order_id = snapshots
                                .into_iter()
                                .map(|snapshot| (snapshot.order_id.clone(), snapshot))
                                .collect::<HashMap<_, _>>();
                            let results = request
                                .order_ids
                                .iter()
                                .map(|order_id| {
                                    let snapshot = by_order_id.get(order_id).cloned().unwrap_or_else(
                                        || OrderLifecycleSnapshot {
                                            close_details: None,
                                            order_id: order_id.clone(),
                                            status: OrderLifecycleStatus::Unknown,
                                            position_id: None,
                                            position_qty: None,
                                            symbol: None,
                                            detail: resolver_error.clone().or_else(|| {
                                                Some(
                                                    "broker returned no lifecycle evidence for this order"
                                                        .to_string(),
                                                )
                                            }),
                                        },
                                    );
                                    ReconciliationRecord {
                                        record_type: "terminal_order".to_string(),
                                        entity_id: order_id.clone(),
                                        payload: serde_json::to_value(snapshot).unwrap_or_default(),
                                    }
                                })
                                .collect();
                            let result = ReconciliationLifecycleResult {
                                generation: request.generation.clone(),
                                request_id: request.request_id.clone(),
                                results,
                            };
                            let mut event = EventEnvelope::new(
                                runtime_id.clone(),
                                strategy_id.clone(),
                                live_session_id.clone(),
                                format!("reconciliation-lifecycle-result:{}", request.request_id),
                                now_epoch_micros().max(1) as u64,
                                EventPriority::Reconciliation,
                                EventKind::ReconciliationLifecycleResult,
                                serde_json::to_value(result).unwrap_or_default(),
                            );
                            event.run_id = Some(run_id.clone());
                            event.event_id = deterministic_event_id(&format!(
                                "reconciliation-lifecycle-result:{}:{}",
                                request.generation, request.request_id
                            ));
                            if let Err(error) = publisher.respond_event(&delivery, event).await {
                                warn!(
                                    "Failed to return broker lifecycle reconciliation result: {}",
                                    error
                                );
                            }
                        }
                    });
                }
                Err(error) => {
                    warn!(
                        "AQS broker lifecycle reconciliation subscription is unavailable: {}",
                        error
                    );
                }
            }
        }

        let mut action_stream = if let (Some(client), Some(a)) = (&db, &auth) {
            Self::create_strategy_action_stream(client, a).await
        } else {
            None
        };
        let mut local_action_stream = if let Some(sync) = local_batch_sync.as_ref() {
            match sync
                .publisher
                .subscribe(EventSubject::control(&sync.runtime_id, &sync.strategy_id))
                .await
            {
                Ok(stream) => Some(stream),
                Err(error) => {
                    warn!(
                        "AQS local control subscription is unavailable for strategy {}: {}",
                        sync.strategy_id, error
                    );
                    None
                }
            }
        } else {
            None
        };

        let mut stop_action_id: Option<String> = None;
        let mut processed_action_ids: HashSet<String> = HashSet::new();
        let (live_metrics_reset_event, initial_account, initial_account_at) =
            match self.start(&mut strategy, db.as_ref(), auth.as_ref()).await {
                Ok(event) => event,
                Err(error) => {
                    error!("Live execution failed during strategy startup: {}", error);
                    live_background_reads.shutdown().await;
                    self.rollback_failed_live_startup(strategy, "strategy initialization")
                        .await;
                    return Err(error);
                }
            };
        let mut local_live_artifacts = if auth.is_none() {
            let mut artifacts = match LocalLiveRunArtifacts::start(
                &self.name,
                self.strategy_id,
                self.mode.clone(),
                self.artifact_root(),
            )
            .await
            {
                Ok(artifacts) => artifacts,
                Err(error) => {
                    error!(
                        "Live execution failed while creating local live run database: {}",
                        error
                    );
                    live_background_reads.shutdown().await;
                    self.rollback_failed_live_startup(strategy, "local run database creation")
                        .await;
                    return Err(error);
                }
            };
            info!(
                "AQS Cloud auth not provided; writing local live run database to {}",
                artifacts.db_path.display()
            );
            self.persist_local_live_metrics_if_needed(&mut artifacts, auth.as_ref(), true);
            Some(artifacts)
        } else {
            None
        };

        // Channels for incoming events
        let (trade_tx, mut trade_rx) = tokio::sync::mpsc::channel(LIVE_BROKER_EVENT_QUEUE_CAPACITY);
        let (bar_tx, mut bar_rx) =
            tokio::sync::mpsc::channel::<MarketDataEvent>(LIVE_MARKET_DATA_QUEUE_CAPACITY);
        let (stream_overflow_tx, mut stream_overflow_rx) =
            tokio::sync::mpsc::channel::<&'static str>(1);

        // 5. Subscribe to trade event stream
        let trade_tx_clone = trade_tx.clone();
        let trade_overflow_tx = stream_overflow_tx.clone();
        let trade_callback = Arc::new(move |event| match trade_tx_clone.try_send(event) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                let _ = trade_overflow_tx.try_send("broker lifecycle");
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {}
        });
        if let Err(error) = self
            .broker
            .subscribe_to_broker_event_stream(trade_callback)
            .await
        {
            error!(
                "Live execution failed while subscribing to trade stream: {}",
                error
            );
            live_background_reads.shutdown().await;
            self.rollback_failed_live_startup(strategy, "broker event subscription")
                .await;
            return Err(error);
        }

        // 6. Subscribe to market data streams
        let event_streams = self.resolve_event_streams();
        let mut streams_by_timeframe: HashMap<
            crate::core::utils::timeframe::TimeFrame,
            Vec<ResolvedEventStream>,
        > = HashMap::new();
        for stream in event_streams {
            streams_by_timeframe
                .entry(stream.key.timeframe)
                .or_default()
                .push(stream);
        }

        for (time_frame, streams) in streams_by_timeframe {
            let symbols: Vec<String> = streams
                .iter()
                .map(|stream| stream.key.symbol.clone())
                .collect();
            let streams_by_symbol: Arc<HashMap<String, ResolvedEventStream>> = Arc::new(
                streams
                    .into_iter()
                    .map(|stream| (stream.key.symbol.clone(), stream))
                    .collect(),
            );
            info!(
                "Subscribing live market data symbols={:?} timeframe={} mode={:?}",
                symbols,
                time_frame,
                DataStreamMode::CompletedBar
            );

            let bar_tx_clone = bar_tx.clone();
            let bar_overflow_tx = stream_overflow_tx.clone();
            let streams_by_symbol = streams_by_symbol.clone();
            let bar_callback = Arc::new(move |bar: crate::core::broker::types::Bar| {
                let Some(stream) = streams_by_symbol.get(&bar.symbol) else {
                    return;
                };
                let event = MarketDataEvent {
                    context: stream.context_at(bar.timestamp),
                    bar,
                };
                match bar_tx_clone.try_send(event) {
                    Ok(()) => {}
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        let _ = bar_overflow_tx.try_send("completed market-data bar");
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {}
                }
            });
            if let Err(error) = self
                .broker
                .subscribe_to_data_stream(
                    symbols,
                    time_frame,
                    DataStreamMode::CompletedBar,
                    bar_callback,
                )
                .await
            {
                error!(
                    "Live execution failed while subscribing to market data stream: {}",
                    error
                );
                live_background_reads.shutdown().await;
                self.rollback_failed_live_startup(strategy, "market data subscription")
                    .await;
                return Err(error);
            }
        }

        // Callbacks are installed before restored work can be dispatched. Pipeline
        // configuration is also complete, so saved continuation layouts can be checked.
        if let Err(error) = self.start_live_execution(auth.as_ref()).await {
            live_background_reads.shutdown().await;
            self.rollback_failed_live_startup(strategy, "durable execution recovery")
                .await;
            return Err(error);
        }

        let quote_symbols: Vec<_> = self.universe.keys().cloned().collect();
        let mut live_quotes = match super::live_quotes::LiveQuotes::start(
            &self.broker,
            quote_symbols.clone(),
            stream_overflow_tx.clone(),
        )
        .await
        {
            Ok(quotes) => quotes,
            Err(error) => {
                live_background_reads.shutdown().await;
                self.rollback_failed_live_startup(strategy, "native execution quote subscription")
                    .await;
                return Err(error);
            }
        };
        self.execution_quotes
            .configure(if live_quotes.is_subscribed() {
                quote_symbols
            } else {
                Vec::new()
            });
        let mut quote_sweep = super::live_quotes::QuoteSweep::default();

        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());
        let shutdown_signal_file = aqs_types::shutdown_signal_file_from_process_args();
        let mut shutdown_signal_interval =
            tokio::time::interval(Duration::from_millis(LOCAL_SHUTDOWN_SIGNAL_POLL_MS));
        shutdown_signal_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut recovering_previous_stop =
            self.status == StrategyStatus::Stopping && !*shutdown_rx.borrow();
        let mut entered_live_execution = !recovering_previous_stop && !*shutdown_rx.borrow();
        let recovery_deadline = Instant::now() + Duration::from_secs(90);
        if entered_live_execution {
            self.status = StrategyStatus::Running;
            info!("Started live trading mode for strategy: {}", self.name);
        } else {
            info!(
                "Live connections ready for stop recovery: {}; new trading remains disabled",
                self.name
            );
        }
        let mut aqs_sync_status = if auth.is_none() {
            "not configured".to_string()
        } else if event_gateway_primary {
            "local_accepted".to_string()
        } else {
            "offline".to_string()
        };
        if let (Some(sync), Some(a)) = (local_batch_sync.as_mut(), auth.as_ref()) {
            let universe = self
                .universe
                .values()
                .map(StrategyUniverseAssetRecord::from)
                .collect::<Vec<_>>();
            let account = self.broker.get_cached_account();
            let event = sync.event(
                "strategy-lifecycle",
                EventPriority::Critical,
                EventKind::StrategyLifecycle,
                serde_json::json!({
                    "state": if self.status == StrategyStatus::Stopping { "stopping" } else { "running" },
                    "status": format!("{:?}", self.status),
                    "strategy_name": self.name,
                    "strategy_id": a.strategy_id,
                    "universe": universe,
                    "account": account,
                    // Preserve the very first broker observation in the
                    // existing durable startup event, before telemetry can
                    // coalesce or callbacks can change the account value.
                    "account_origin": initial_account,
                    "account_origin_at": initial_account_at,
                    "node_id": a.node_id,
                }),
                None,
            );
            sync.publish_critical_durable(event).await;
            if let Some(reset_event) = live_metrics_reset_event {
                let event = sync.event(
                    "live-metrics-reset",
                    EventPriority::Critical,
                    EventKind::StrategyEvent,
                    serde_json::to_value(reset_event).unwrap_or_default(),
                    None,
                );
                sync.publish_critical_durable(event).await;
            }
            self.publish_account_and_metrics_to_local_bus(sync, account.as_ref(), true);
            if !self
                .publish_startup_reconciliation_to_local_bus(sync, a)
                .await
            {
                let error = BrokerError::ConnectionError(
                    "Startup reconciliation could not be committed to the local event journal"
                        .into(),
                );
                error!("Live execution failed during startup reconciliation: {error}");
                live_background_reads.shutdown().await;
                self.rollback_failed_live_startup(strategy, "startup reconciliation")
                    .await;
                return Err(error);
            }
        }
        self.publish_runtime_event(
            "info",
            if recovering_previous_stop {
                "Recovering previous stop; new trading is disabled"
            } else {
                "Live strategy started"
            },
            aqs_sync_status.clone(),
        );

        // Start only after a recovered stop has finished replacing the command
        // driver. Reads never run teardown or change Insight lifecycle state.
        let mut fill_history_started = false;

        // 7. Main Event Loop
        // Pipeline loop interval
        let mut pipeline_interval = tokio::time::interval(std::time::Duration::from_secs(5));
        let mut live_sync_interval =
            tokio::time::interval(std::time::Duration::from_millis(LIVE_SYNC_FLUSH_MS));
        let mut heartbeat_interval =
            tokio::time::interval(std::time::Duration::from_secs(LIVE_HEARTBEAT_SECS));
        let mut live_sync_reconnect_interval = tokio::time::interval(
            std::time::Duration::from_millis(LIVE_SYNC_BACKGROUND_RETRY_POLL_MS),
        );
        let mut local_metrics_interval = tokio::time::interval(std::time::Duration::from_secs(
            LOCAL_LIVE_METRICS_WRITE_SECS,
        ));
        live_sync_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        pipeline_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        local_metrics_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        live_sync_reconnect_interval
            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let initial_live_sync_retry_delay =
            Duration::from_secs(LIVE_SYNC_BACKGROUND_RETRY_INITIAL_SECS);
        let mut live_sync_retry_delay = initial_live_sync_retry_delay;
        let mut next_live_sync_retry_at = Instant::now() + initial_live_sync_retry_delay;
        let mut live_sync_reconnect_in_flight = false;
        let mut live_sync_reconnect_task_id = None;
        let (live_sync_reconnect_tx, mut live_sync_reconnect_rx) = tokio::sync::mpsc::channel(1);
        let mut metrics_continuity_lookup =
            super::metrics_continuity_lookup::MetricsContinuityLookup::default();
        let mut last_runtime_publish = Instant::now();
        let last_live_metrics_sample = Instant::now();
        let mut pending_local_live_events = VecDeque::new();
        let local_event_ready = local_live_artifacts
            .as_ref()
            .map(|artifacts| artifacts.event_notifier());
        let mut command_ready = self
            .command_driver
            .as_ref()
            .map(|driver| driver.completion_notifier());
        let mut command_attempted = HashMap::new();
        let snapshot_ready = self.broker.snapshot_notifier();
        let mut live_pipeline_sweep = super::pipeline_sweep::LivePipelineSweep::default();
        let mut lifecycle_reads = tokio::task::JoinSet::new();
        let mut lifecycle_check = self.broker.lifecycle_reconciler().map(|reader| {
            super::lifecycle_reconciliation::LifecycleCheck::start(reader, &mut lifecycle_reads)
        });
        let mut lifecycle_tick = tokio::time::interval(Duration::from_secs(5));
        lifecycle_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        macro_rules! mark_live_sync_offline {
            ($auth:expr, $reason:expr) => {{
                let message = format!(
                    "AQS live sync unavailable for strategy {}; continuing local broker execution and retrying in the background: {}",
                    $auth.strategy_id, $reason
                );
                warn!("{}", message);
                db = None;
                action_stream = None;
                live_sync_retry_delay = initial_live_sync_retry_delay;
                next_live_sync_retry_at = Instant::now() + live_sync_retry_delay;
                aqs_sync_status = "offline".to_string();
                self.publish_runtime_event("warn", message, aqs_sync_status.clone());
            }};
        }

        loop {
            if let Some(sync) = local_batch_sync.as_ref() {
                sync.flush_pending_publications();
            }
            if local_batch_sync
                .as_ref()
                .is_some_and(|sync| sync.publisher.admission_failed())
            {
                error!(
                    "Local publisher admission failed; stopping new execution and beginning graceful teardown"
                );
                break;
            }
            if recovering_previous_stop && !*shutdown_rx.borrow() {
                // Engine-owned recovery, not a fresh invocation of user teardown.
                self.cleanup_active_insights_for_teardown_internal();
                self.on_trade_update();
                if self.insights.active_insight_count() == 0 && !self.live_execution_pending() {
                    match self.resume_after_recovered_stop(auth.as_ref()).await {
                        Ok(()) => {
                            recovering_previous_stop = false;
                            entered_live_execution = true;
                            command_ready = self
                                .command_driver
                                .as_ref()
                                .map(|driver| driver.completion_notifier());
                            info!(
                                "Previous stop durably recovered; continuing this launch into live trading"
                            );
                            self.publish_runtime_event(
                                "info",
                                "Live strategy started after recovery",
                                aqs_sync_status.clone(),
                            );
                            if let Some(sync) = local_batch_sync.as_mut() {
                                let event = sync.event(
                                    "strategy-lifecycle",
                                    EventPriority::Critical,
                                    EventKind::StrategyLifecycle,
                                    serde_json::json!({"state": "running"}),
                                    None,
                                );
                                sync.publish_critical_durable(event).await;
                            }
                        }
                        Err(error) => {
                            self.execution_error =
                                Some(crate::core::broker::commands::BrokerCommandError::Invalid(
                                    error.to_string(),
                                ));
                            self.shutdown();
                        }
                    }
                } else if Instant::now() >= recovery_deadline {
                    self.execution_error =
                        Some(crate::core::broker::commands::BrokerCommandError::Invalid(
                            "Startup recovery deadline reached; unresolved journal retained".into(),
                        ));
                    self.shutdown();
                }
            }
            if let Some(artifacts) = local_live_artifacts.as_mut() {
                if let Err(error) = artifacts.poll_event_writes(&mut pending_local_live_events) {
                    warn!("Local live event batch not committed; retaining events: {error}");
                }
                if let Err(error) = artifacts.poll_metrics_writes() {
                    warn!("Local live metrics not committed; retaining snapshot: {error}");
                }
            }
            if entered_live_execution && !fill_history_started && !*shutdown_rx.borrow() {
                fill_history_started = true;
                if let (Some(reader), Some(driver)) = (
                    self.broker.execution.execution_fill_history_reader(),
                    self.command_driver.as_ref(),
                ) {
                    live_background_reads
                        .spawn(super::live_fill_history::recover(reader, driver.sender()));
                }
                if super::live_position_marks::enabled() {
                    if let (Some(reader), Some(driver)) = (
                        self.broker.execution.position_valuation_reader(),
                        self.command_driver.as_ref(),
                    ) {
                        live_background_reads
                            .spawn(super::live_position_marks::sample(reader, driver.sender()));
                    }
                }
            }
            if let Some(error) = self
                .live_execution_failure()
                .filter(|_| !recovering_previous_stop)
            {
                error!("Live execution requires recovery; entering teardown: {error}");
                self.shutdown();
            }
            // Re-check the retained stop flag between every bounded owner turn.
            if *shutdown_rx.borrow() {
                break;
            }
            tokio::select! {
                biased;

                // Poll stop/control before execution work; a sustained fill
                // burst must not starve shutdown delivery.
                notification = async {
                    match &mut action_stream {
                        Some(stream) => stream.next().await,
                        None => None,
                    }
                }, if action_stream.is_some() => {
                    match notification {
                        Some(Ok(notification)) => {
                            let action = notification.data;
                            let action_json = action.clone().into_json_value();
                            let action_name = action_json
                                .get("action")
                                .cloned()
                                .and_then(|value| serde_json::from_value::<StrategyActionKind>(value).ok());
                            let action_status = action_json
                                .get("status")
                                .cloned()
                                .and_then(|value| serde_json::from_value::<StrategyActionStatus>(value).ok());
                            let action_id = action_json.get("id").and_then(action_id_from_value);

                            if action_status == Some(StrategyActionStatus::Pending)
                                && action_name == Some(StrategyActionKind::Stop)
                            {
                                if action_id
                                    .as_ref()
                                    .is_some_and(|action_id| processed_action_ids.contains(action_id))
                                {
                                    continue;
                                }

                                // Gate dispatch before any acknowledgement I/O.
                                // The broker worker can otherwise submit queued
                                // entries while this branch waits on the DB.
                                self.status = StrategyStatus::Stopping;
                                self.shutdown();

                                if let Some(action_id) = action_id.as_ref() {
                                    warn!("[action_id={}] AQE received stop action from the SurrealDB action stream", action_id);

                                    let acknowledgement_durable = if let Some(sync) = local_batch_sync.as_mut() {
                                        let acknowledgement = sync.event(
                                            format!("strategy-action:{action_id}"),
                                            EventPriority::Critical,
                                            EventKind::StrategyActionAcknowledgement,
                                            serde_json::json!({
                                                "action_id": action_id,
                                                "action": StrategyActionKind::Stop,
                                                "status": StrategyActionStatus::Acknowledged,
                                                "message": "Stop requested by AQS",
                                                "acknowledged_at": chrono::Utc::now(),
                                            }),
                                            None,
                                        );
                                        sync.publish_critical_durable(acknowledgement).await
                                    } else {
                                        true
                                    };

                                    if acknowledgement_durable {
                                        processed_action_ids.insert(action_id.clone());
                                        if let Some(client) = db.as_ref() {
                                            update_strategy_action_status(
                                                client,
                                                action_id,
                                                StrategyActionStatus::Acknowledged,
                                                Some("Stop requested by AQS".to_string()),
                                                None,
                                                "acknowledged_at",
                                            )
                                            .await;
                                        }
                                        stop_action_id = Some(action_id.clone());
                                    } else {
                                        error!("[action_id={}] Stop acknowledgement was not durably admitted; leaving the action pending for recovery", action_id);
                                    }
                                }

                                warn!("Received remote stop action from AQS");
                            }
                        }
                        Some(Err(error)) => {
                            error!("Strategy action live stream error: {}", error);
                            if let Some(a) = auth.as_ref() {
                                mark_live_sync_offline!(a, error);
                            }
                        }
                        None => {
                            // A terminated stream stays ready forever. Remove
                            // it so this high-priority control branch cannot
                            // spin and starve execution, and reconnect normally.
                            if let Some(a) = auth.as_ref() {
                                mark_live_sync_offline!(a, "strategy action stream ended");
                            } else {
                                action_stream = None;
                            }
                        }
                    }
                }

                delivery = async {
                    match &mut local_action_stream {
                        Some(stream) => stream.recv().await,
                        None => None,
                    }
                }, if local_action_stream.is_some() => {
                    let Some(delivery) = delivery else {
                        local_action_stream = None;
                        continue;
                    };
                    let action_json = delivery.event.payload.clone();
                    let action_name = action_json
                        .get("action")
                        .cloned()
                        .and_then(|value| serde_json::from_value::<StrategyActionKind>(value).ok());
                    let action_status = action_json
                        .get("status")
                        .cloned()
                        .and_then(|value| serde_json::from_value::<StrategyActionStatus>(value).ok());
                    let action_id = action_json
                        .get("action_id")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string);

                    if let Some(action_id) = action_id.as_deref() {
                        info!("[action_id={}] AQE received local NATS control delivery", action_id);
                    } else {
                        warn!("AQE received local NATS control delivery without an action_id");
                    }

                    if action_status == Some(StrategyActionStatus::Pending)
                        && action_name == Some(StrategyActionKind::Stop)
                        && !action_id.as_ref().is_some_and(|id| processed_action_ids.contains(id))
                    {
                        self.status = StrategyStatus::Stopping;
                        self.shutdown();
                    }

                    if let Some(sync) = local_batch_sync.as_mut() {
                        if action_id
                            .as_ref()
                            .is_some_and(|action_id| processed_action_ids.contains(action_id))
                        {
                            continue;
                        }

                        if action_status == Some(StrategyActionStatus::Pending)
                            && action_name == Some(StrategyActionKind::Stop)
                        {
                            if let Some(action_id) = action_id.as_ref() {
                                let acknowledgement = sync.event(
                                    format!("strategy-action:{action_id}"),
                                    EventPriority::Critical,
                                    EventKind::StrategyActionAcknowledgement,
                                    serde_json::json!({
                                        "action_id": action_id,
                                        "action": StrategyActionKind::Stop,
                                        "status": StrategyActionStatus::Acknowledged,
                                        "message": "Stop requested by AQS runtime host",
                                        "acknowledged_at": chrono::Utc::now(),
                                    }),
                                    None,
                                );
                                if sync.publish_control_ack(acknowledgement, delivery) {
                                    processed_action_ids.insert(action_id.clone());
                                    stop_action_id = Some(action_id.clone());
                                }
                            }
                            warn!(
                                "[action_id={}] Received stop action from the AQS runtime host",
                                action_id.as_deref().unwrap_or("unknown")
                            );
                        }
                    }
                }

                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Shutdown signal received, exiting live loop");
                        break;
                    }
                }


                _ = shutdown_signal_interval.tick(), if shutdown_signal_file.is_some() => {
                    let requested = shutdown_signal_file
                        .as_ref()
                        .is_some_and(|path| path.is_file());
                    if requested {
                        if let Some(path) = shutdown_signal_file.as_ref() {
                            let _ = fs::remove_file(path);
                        }
                        info!("Local graceful-shutdown signal received, exiting live loop");
                        self.status = StrategyStatus::Stopping;
                        self.shutdown();
                        break;
                    }
                }

                _ = &mut ctrl_c => {
                    warn!("Process interrupt received, stopping live strategy");
                    self.status = StrategyStatus::Stopping;
                    self.shutdown();
                    break;
                }


                _ = async {
                    match command_ready.as_ref() {
                        Some(ready) => ready.notified().await,
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    self.on_trade_update();
                    // Resume receipt-dependent stages promptly, not on the next
                    // market bar. This is owner work, never broker transport I/O.
                    if !recovering_previous_stop && self.resume_live_command_pipes(&mut command_attempted) {
                        if let Some(ready) = command_ready.as_ref() { ready.notify_one(); }
                    }
                    if let (Some(sync), Some(a)) = (local_batch_sync.as_mut(), auth.as_ref()) {
                        self.publish_dirty_insights_to_local_bus(
                            sync, a, Some(LIVE_SYNC_MAX_INSIGHTS_PER_FLUSH), event_gateway_primary,
                        ).await;
                    }
                }

                _ = async {
                    match local_event_ready.as_ref() {
                        Some(ready) => ready.notified().await,
                        None => std::future::pending::<()>().await,
                    }
                } => {}

                // Handle broker trade events
                Some(broker_envelope) = trade_rx.recv(), if local_live_artifacts.is_none()
                    || pending_local_live_events.len() < MAX_PENDING_LOCAL_LIVE_EVENTS => {
                    let mut received_events = vec![broker_envelope];
                    let notification_limit = if local_live_artifacts.is_some() {
                        LIVE_TRADE_NOTIFICATION_BATCH_SIZE.min(MAX_PENDING_LOCAL_LIVE_EVENTS - pending_local_live_events.len())
                    } else { LIVE_TRADE_NOTIFICATION_BATCH_SIZE };
                    // Bound work before checking shutdown again. Remaining
                    // notifications stay queued in order; no trade is discarded.
                    while received_events.len() < notification_limit {
                        let Ok(event) = trade_rx.try_recv() else { break; };
                        received_events.push(event);
                    }
                    debug!(
                        "Live loop received {} trade event notification(s)",
                        received_events.len()
                    );
                    // Broker callbacks are one-shot evidence. Build their exact
                    // envelopes first, then commit the bounded page to the local
                    // outbox before the reducer is allowed to consume the page.
                    // This waits for local SQLite only, never NATS/SurrealDB.
                    let mut broker_projection_events =
                        Vec::with_capacity(received_events.len());
                    let mut execution_fills = Vec::new();

                    for broker_envelope in received_events {
                        let source_sequence = broker_envelope.source_sequence;
                        let broker_metadata = broker_envelope.metadata;
                        let (order, event) = broker_envelope.event;
                        if let Some(fill) = broker_metadata.execution_fill {
                            // Exact broker fills are independent of the order
                            // reducer's cumulative quantity and callback result.
                            // Never manufacture a fill from a terminal snapshot.
                            execution_fills.push(aqe_live_protocol::ExecutionFillInput {
                                insight_id: order.insight_id.clone(),
                                fill: *fill,
                            });
                        }
                        if broker_metadata.analytics_only {
                            // A broker history revision updates the fill ledger,
                            // not the current order or Insight lifecycle audit.
                            continue;
                        }
                        let broker_event = format!("{:?}", event);
                        let order_id = order.order_id.clone();
                        let symbol = order.asset.symbol.clone();
                        let message = format!("Received {} for order {}", broker_event, order_id);
                        self.push_runtime_event(
                            "info",
                            format!("Trade event {} for order {}", broker_event, order_id),
                        );
                        let payload = serde_json::json!({
                            "order_id": order_id.clone(),
                            "symbol": symbol.clone(),
                            "event": broker_event.clone(),
                            "source_sequence": source_sequence,
                            "source_clock": broker_metadata.source_clock,
                        });

                        if let Some(sync) = local_batch_sync.as_mut() {
                            let source_revision = source_sequence.unwrap_or_else(|| {
                                crate::core::live_sync::now_epoch_micros().max(1) as u64
                            });
                            let entity_id = format!("broker-order:{}", order_id);
                            let revision =
                                sync.next_revision_with_floor(&entity_id, source_revision);
                            let latency = LatencyTrace {
                                source_occurred_at_us: broker_metadata.source_occurred_at_us,
                                adapter_queued_at_us: broker_metadata.adapter_queued_at_us,
                                runtime_received_at_us: Some(
                                    crate::core::live_sync::now_epoch_micros(),
                                ),
                                ..Default::default()
                            };
                            let event = sync.event_at_revision(
                                entity_id,
                                revision,
                                EventPriority::Critical,
                                EventKind::BrokerOrderLifecycle,
                                payload.clone(),
                                Some(latency),
                            );
                            broker_projection_events.push(event);
                        }

                        if local_live_artifacts.is_some() {
                            if let Err(event) = enqueue_pending_local_live_event(
                                &mut pending_local_live_events,
                                PendingLocalLiveEvent {
                                    event_id: uuid::Uuid::new_v4().to_string(),
                                    created_at: chrono::Utc::now(),
                                    event_type: StrategyEventType::TradeEvent,
                                    level: StrategyEventLevel::Info,
                                    title: StrategyEventTitle::BrokerTradeEvent.to_string(),
                                    message: message.clone(),
                                    details: LocalLiveEventDetails {
                                        order_id: Some(order_id.clone()),
                                        symbol: Some(symbol.clone()),
                                        broker_event: Some(broker_event.clone()),
                                        ..Default::default()
                                    },
                                },
                            ) {
                                // Admission above reserves this whole notification
                                // page. Preserve evidence if that invariant breaks.
                                pending_local_live_events.push_back(event);
                                error!("Local event admission exceeded its reserved capacity; stopping entries");
                                self.shutdown();
                            }
                        }

                    }

                    if let Err(error) = self.persist_live_fill_evidence(execution_fills).await {
                        // Analytics is observational. Never initiate teardown
                        // or change an order because its analytics write failed.
                        error!("Broker fill analytics incomplete; evidence not recorded: {error}");
                    }

                    if let Some(sync) = local_batch_sync.as_ref() {
                        if !broker_projection_events.is_empty()
                            && !sync
                                .publish_critical_batch_durable(broker_projection_events)
                                .await
                        {
                            // Execution state must still consume the broker page,
                            // but no new entries may start after its audit boundary
                            // failed. Final shutdown will report incomplete sync.
                            error!("Broker lifecycle page was not durably admitted; stopping new entries");
                            self.shutdown();
                        }
                    }

                    // One bounded reducer turn. Remaining events stay owned by
                    // the adapter/strategy page and get a continuation below.
                    self.on_trade_update();

                    if let (Some(sync), Some(a)) = (local_batch_sync.as_mut(), auth.as_ref()) {
                        self.publish_dirty_insights_to_local_bus(
                            sync,
                            a,
                            Some(LIVE_SYNC_MAX_INSIGHTS_PER_FLUSH),
                            event_gateway_primary,
                        ).await;
                        let account = self.broker.get_cached_account();
                        self.publish_account_and_metrics_to_local_bus(sync, account.as_ref(), false);
                    }

                    if last_runtime_publish.elapsed() >= Duration::from_millis(500) {
                        self.publish_runtime_snapshot(aqs_sync_status.clone(), None);
                        last_runtime_publish = Instant::now();
                    }
                }

                Some(stream) = stream_overflow_rx.recv() => {
                    error!("Live {stream} queue exceeded its bounded capacity; stopping strategy {} instead of dropping execution events", self.name);
                    self.shutdown();
                    break;
                }

                // Yield before market work so ready maintenance/publication timers
                // are serviced even with a continuously nonempty quote queue.
                Some(event) = async {
                    tokio::task::yield_now().await;
                    live_quotes.receiver.recv().await
                }, if !quote_sweep.is_pending() => {
                    if recovering_previous_stop { continue; }
                    quote_sweep.begin(event, &self.insights, &mut self.execution_quotes);
                    if let Err(error) = self.advance_quote_sweep(&mut quote_sweep) {
                        error!("Live quote execution failed: {error}");
                        self.shutdown();
                        break;
                    }
                    if let (Some(sync), Some(a)) = (local_batch_sync.as_mut(), auth.as_ref()) {
                        self.publish_dirty_insights_to_local_bus(sync, a, Some(LIVE_SYNC_MAX_INSIGHTS_PER_FLUSH), event_gateway_primary).await;
                    }
                }

                _ = tokio::task::yield_now(), if quote_sweep.is_pending() => {
                    if let Err(error) = self.advance_quote_sweep(&mut quote_sweep) {
                        error!("Live quote execution failed: {error}");
                        self.shutdown();
                        break;
                    }
                    if let (Some(sync), Some(a)) = (local_batch_sync.as_mut(), auth.as_ref()) {
                        self.publish_dirty_insights_to_local_bus(sync, a, Some(LIVE_SYNC_MAX_INSIGHTS_PER_FLUSH), event_gateway_primary).await;
                    }
                }

                // Broker completions and trade evidence take priority over cache work.
                _ = async {
                    match snapshot_ready.as_ref() {
                        Some(ready) => ready.notified().await,
                        None => std::future::pending::<()>().await,
                    }
                }, if !quote_sweep.is_pending() => {
                    if matches!(self.broker.account_snapshot(), Err(BrokerError::SnapshotUnavailable {
                        reason: crate::core::broker::snapshots::SnapshotUnavailable::AccountChanged, ..
                    })) {
                        error!("Broker account identity changed; stopping strategy {} before further execution", self.name);
                        self.shutdown();
                        break;
                    }
                    if self.resume_snapshot_pipes() {
                        if let Some(ready) = snapshot_ready.as_ref() { ready.notify_one(); }
                    }
                    if let (Some(sync), Some(a)) = (local_batch_sync.as_mut(), auth.as_ref()) {
                        self.publish_dirty_insights_to_local_bus(
                            sync, a, Some(LIVE_SYNC_MAX_INSIGHTS_PER_FLUSH), event_gateway_primary,
                        ).await;
                    }
                    tokio::task::yield_now().await;
                }

                // Handle incoming market data bars
                Some(event) = bar_rx.recv(), if !quote_sweep.is_pending() => {
                    if recovering_previous_stop { continue; }
                    let symbol = event.context.symbol.clone();
                    info!(
                        "Live market data bar symbol={} history_key={} timeframe={} feature={} allow_trading={} timestamp={}",
                        symbol,
                        event.context.history_key,
                        event.context.timeframe,
                        event.context.is_feature,
                        event.context.allow_trading,
                        event.bar.timestamp
                    );
                    debug!("Live loop processing completed bar for {}", symbol);
                    if event.context.allow_trading && !event.context.is_feature {
                        self.broker.process_live_bar(&event.bar);
                        debug!("Live loop processed execution broker for {}", symbol);
                        self.on_trade_update();
                        debug!("Live loop processed trade events after execution step for {}", symbol);
                    }
                    self._on_market_data_event(&mut strategy, event);
                    debug!("Live loop completed _on_bar for {}", symbol);
                    if auth.is_none() {
                        self.insights.prune_terminal_insights_without_aqs();
                    }
                    if let (Some(sync), Some(a)) = (local_batch_sync.as_mut(), auth.as_ref()) {
                        self.publish_dirty_insights_to_local_bus(
                            sync,
                            a,
                            Some(LIVE_SYNC_MAX_INSIGHTS_PER_FLUSH),
                            event_gateway_primary,
                        ).await;
                    }
                    if last_runtime_publish.elapsed() >= Duration::from_millis(500) {
                        self.publish_runtime_snapshot(aqs_sync_status.clone(), None);
                        last_runtime_publish = Instant::now();
                    }
                }

                // Periodically run insight pipeline
                _ = pipeline_interval.tick(), if !quote_sweep.is_pending() => {
                    if self.runtime_telemetry.shutdown_requested()
                        && !*shutdown_rx.borrow()
                    {
                        warn!("Live stop requested from AQE TUI");
                        self.publish_runtime_event("warn", "Live stop requested from TUI", aqs_sync_status.clone());
                        self.status = StrategyStatus::Stopping;
                        self.shutdown();
                    }
                    if self.broker.connection_worker_health().is_some_and(|health| health.exited_tasks != 0) {
                        self.execution_error = Some(crate::core::broker::commands::BrokerCommandError::Invalid(
                            "provider connection maintenance exited; recovery is required".into()));
                        self.shutdown();
                        continue;
                    }
                    debug!("Live loop calling run_insight_pipeline");
                    if !recovering_previous_stop {
                        live_pipeline_sweep.request(&self.insights, &self.insight_pipeline);
                        self.advance_live_pipeline_sweep(&mut live_pipeline_sweep);
                    }
                    debug!("Live loop completed run_insight_pipeline");
                    if auth.is_none() {
                        self.insights.prune_terminal_insights_without_aqs();
                    }
                    if let (Some(sync), Some(a)) = (local_batch_sync.as_mut(), auth.as_ref()) {
                        self.publish_dirty_insights_to_local_bus(
                            sync,
                            a,
                            Some(LIVE_SYNC_MAX_INSIGHTS_PER_FLUSH),
                            event_gateway_primary,
                        ).await;
                    }

                    self.publish_runtime_snapshot(aqs_sync_status.clone(), None);
                }

                    _ = heartbeat_interval.tick(), if auth.is_some() => {
                        if let Some(sync) = local_batch_sync.as_mut() {
                            // Cached counters only: the TUI never queries SQLite, the gateway,
                            // or the broker from the strategy execution loop.
                            self.runtime_telemetry.update_snapshot(|snapshot| {
                                snapshot.sync = Some(crate::core::tui::RuntimeSyncSnapshot {
                                    transport: runtime_transport.into(),
                                    runtime_id: sync.runtime_id.clone(),
                                    run_id: sync.run_id.clone(),
                                    publisher: sync.publisher.health(),
                                    sampled_at: chrono::Utc::now(),
                                });
                            });
                            let event = sync.event(
                                "heartbeat",
                                EventPriority::Telemetry,
                                EventKind::Heartbeat,
                                serde_json::json!({
                                    "status": format!("{:?}", self.status),
                                    "insights_count": self.insights_state_counts_json(),
                                    "publisher_health": sync.publisher.health(),
                                    "execution_evidence_export": sync.evidence_export.health,
                                    "execution_fill_export": sync.fill_export.health,
                                    "position_mark_export": sync.mark_export.health,
                                    "sent_at": chrono::Utc::now(),
                                }),
                                None,
                            );
                            sync.publish_telemetry(event);
                            let account = self.broker.get_cached_account();
                            self.publish_account_and_metrics_to_local_bus(
                                sync,
                                account.as_ref(),
                                true,
                            );
                        }
                    }

                    _ = live_sync_reconnect_interval.tick(), if auth.is_some() && (db.is_none() || !self.live_metrics_cloud_write_enabled) => {
                        if !live_sync_reconnect_in_flight
                            && !metrics_continuity_lookup.is_running()
                            && !self.legacy_metrics_recovery.is_running()
                            && Instant::now() >= next_live_sync_retry_at
                        {
                            let reconnect_auth = auth
                                .as_ref()
                                .expect("AQS auth is present when reconnect polling is enabled")
                                .clone();
                            let reconnect_tx = live_sync_reconnect_tx.clone();
                            live_sync_reconnect_in_flight = true;
                            aqs_sync_status = "reconnecting".to_string();
                            self.publish_runtime_snapshot(aqs_sync_status.clone(), None);
                            live_sync_reconnect_task_id = Some(live_background_reads.spawn(async move {
                                let result = Self::connect_live_sync_resources(&reconnect_auth).await;
                                let _ = reconnect_tx.send(result).await;
                            }).id());
                        }
                    }

                    Some(result) = live_background_reads.join_next_with_id(), if !live_background_reads.is_empty() => {
                        match result {
                            Ok((id, ())) => {
                                if live_sync_reconnect_task_id == Some(id) {
                                    live_sync_reconnect_task_id = None;
                                }
                            }
                            Err(error) => {
                                if live_sync_reconnect_task_id == Some(error.id()) {
                                    live_sync_reconnect_task_id = None;
                                    live_sync_reconnect_in_flight = false;
                                    next_live_sync_retry_at = Instant::now() + live_sync_retry_delay;
                                }
                                warn!("Live background read task exited unexpectedly: {error}");
                            }
                        }
                    }

                    Some(reconnect_result) = live_sync_reconnect_rx.recv(), if live_sync_reconnect_in_flight => {
                        live_sync_reconnect_in_flight = false;
                        match reconnect_result {
                            Ok((next_db, next_action_stream)) => {
                                db = Some(next_db);
                                action_stream = next_action_stream;
                                if !self.live_metrics_cloud_write_enabled {
                                    if let (Some(client), Some(a)) = (db.as_ref(), auth.as_ref()) {
                                        if self.legacy_metrics_recovery_needed {
                                            self.start_legacy_live_trade_metrics(client, a);
                                        } else if let Ok(account) = self.broker.account_snapshot() {
                                            let account_id = account.account_id.trim().to_string();
                                            if !account_id.is_empty() {
                                                metrics_continuity_lookup.start(client.clone(), a.clone(), account_id);
                                            }
                                        }
                                    }
                                }
                                // A pending lookup owns the retry outcome. Do not
                                // double the backoff at both connection and lookup completion.
                                if !metrics_continuity_lookup.is_running() && !self.legacy_metrics_recovery.is_running() {
                                    live_sync_retry_delay = if self.live_metrics_cloud_write_enabled {
                                        initial_live_sync_retry_delay
                                    } else {
                                        next_live_sync_retry_delay(live_sync_retry_delay)
                                    };
                                }
                                next_live_sync_retry_at = Instant::now() + live_sync_retry_delay;
                                aqs_sync_status = "local_accepted".to_string();
                                self.publish_runtime_event(
                                    "info",
                                    "AQS live sync reconnected; queued state will be reconciled",
                                    aqs_sync_status.clone(),
                                );
                            }
                            Err(error) => {
                                let retry_in = live_sync_retry_delay;
                                next_live_sync_retry_at = Instant::now() + retry_in;
                                live_sync_retry_delay =
                                    next_live_sync_retry_delay(live_sync_retry_delay);
                                aqs_sync_status = "offline".to_string();
                                warn!(
                                    "AQS live sync background reconnect failed for strategy {}; retrying in {}s while broker execution continues: {}",
                                    auth.as_ref().map(|value| value.strategy_id.as_str()).unwrap_or("unknown"),
                                    retry_in.as_secs(),
                                    error
                                );
                                self.publish_runtime_snapshot(aqs_sync_status.clone(), None);
                            }
                        }
                    }

                    Some(completed) = self.legacy_metrics_recovery.next(), if self.legacy_metrics_recovery.is_running() => {
                        self.apply_legacy_live_trade_metrics(completed, auth.as_ref());
                        live_sync_retry_delay = if self.live_metrics_cloud_write_enabled {
                            initial_live_sync_retry_delay
                        } else {
                            next_live_sync_retry_delay(live_sync_retry_delay)
                        };
                        next_live_sync_retry_at = Instant::now() + live_sync_retry_delay;
                    }

                    Some(result) = metrics_continuity_lookup.next(), if metrics_continuity_lookup.is_running() => {
                        match result {
                            Ok(result) => {
                                if let (Some(client), Some(a)) = (db.as_ref(), auth.as_ref()) {
                                    if result.strategy_id == a.strategy_id {
                                        match result.metrics {
                                            Ok(metrics) => {
                                                self.resolve_deferred_live_metrics_continuity(
                                                    client, a, &result.account_id, metrics, result.legacy, result.baseline,
                                                );
                                            }
                                            Err(error) => warn!("AQS live metrics continuity lookup failed for strategy {}; Cloud writes remain deferred: {error}", a.strategy_id),
                                        }
                                    }
                                }
                            }
                            Err(error) => warn!("AQS live metrics continuity lookup task exited unexpectedly; Cloud writes remain deferred: {error}"),
                        }
                        if !self.legacy_metrics_recovery.is_running() {
                            live_sync_retry_delay = if self.live_metrics_cloud_write_enabled {
                                initial_live_sync_retry_delay
                            } else {
                                next_live_sync_retry_delay(live_sync_retry_delay)
                            };
                        }
                        next_live_sync_retry_at = Instant::now() + live_sync_retry_delay;
                    }

                    _ = live_sync_interval.tick() => {
                        if let (Some(sync), Some(a)) = (local_batch_sync.as_mut(), auth.as_ref()) {
                            sync.progress_execution_evidence(self.command_driver.as_ref().map(|driver| driver.sender()));
                            self.publish_dirty_insights_to_local_bus(
                                sync,
                                a,
                                Some(LIVE_SYNC_MAX_INSIGHTS_PER_FLUSH),
                                event_gateway_primary,
                            ).await;
                            let account = self.broker.get_cached_account();
                            self.publish_account_and_metrics_to_local_bus(
                                sync,
                                account.as_ref(),
                                false,
                            );
                        }
                    }

                    _ = local_metrics_interval.tick() => {
                        if let Some(sync) = local_batch_sync.as_mut() {
                            let account = self.broker.get_cached_account();
                            self.publish_account_and_metrics_to_local_bus(
                                sync,
                                account.as_ref(),
                                true,
                            );
                        }
                        if let Some(artifacts) = local_live_artifacts.as_mut() {
                            self.persist_local_live_metrics_if_needed(artifacts, auth.as_ref(), false);
                            self.publish_runtime_snapshot(aqs_sync_status.clone(), None);
                        }
                    }

                _ = lifecycle_tick.tick(), if lifecycle_check.is_some() => {
                    if let Some(check) = lifecycle_check.as_mut() { check.request(&self.insights); }
                }

                rows = async {
                    match lifecycle_check.as_mut() {
                        Some(check) => check.results.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if let Some(rows) = rows {
                        if let Some(check) = lifecycle_check.as_mut() { check.busy = false; }
                        for row in rows { self.apply_lifecycle_snapshot(row); }
                    } else {
                        lifecycle_check = None;
                    }
                }

                // Notifications may have already been consumed while a reducer
                // turn hit its budget. Do not wait for another tick/trade to
                // finish that page, and do not advance/replay market events.
                _ = tokio::task::yield_now(), if self.has_pending_broker_events() => {
                    self.on_trade_update();
                    if let (Some(sync), Some(a)) = (local_batch_sync.as_mut(), auth.as_ref()) {
                        self.publish_dirty_insights_to_local_bus(
                            sync, a, Some(LIVE_SYNC_MAX_INSIGHTS_PER_FLUSH), event_gateway_primary,
                        ).await;
                    }
                }

                // A periodic pass retains only cursors and yields between pages.
                // Stop/control, broker reduction and incoming events have priority.
                _ = tokio::task::yield_now(), if live_pipeline_sweep.is_pending() && !quote_sweep.is_pending() => {
                    self.advance_live_pipeline_sweep(&mut live_pipeline_sweep);
                    if let (Some(sync), Some(a)) = (local_batch_sync.as_mut(), auth.as_ref()) {
                        self.publish_dirty_insights_to_local_bus(
                            sync, a, Some(LIVE_SYNC_MAX_INSIGHTS_PER_FLUSH), event_gateway_primary,
                        ).await;
                    }
                }

                else => {
                    break;
                }
            }
        }

        // Teardown
        live_quotes.fence();
        self.execution_quotes = Default::default();
        // Cancel cloud broker-history/reconnect work before teardown and
        // await its release while transports are still available. The durable
        // command worker and local terminal-history reader remain alive below
        // to settle mutations even when a manual close callback was missed.
        metrics_continuity_lookup.shutdown().await;
        self.stop_legacy_metrics_recovery().await;
        live_background_reads.shutdown().await;
        macro_rules! teardown_progress {
            ($phase:expr, $message:expr) => {
                if let Some(sync) = local_batch_sync.as_mut() {
                    let event = sync.event(
                        format!("teardown-progress:{}", $phase),
                        EventPriority::Critical,
                        EventKind::StrategyEvent,
                        serde_json::json!({
                            "event_type": "lifecycle", "level": "info",
                            "title": "Strategy teardown progress", "message": $message,
                            "payload": {"phase": $phase, "action_id": stop_action_id},
                            "created_at": chrono::Utc::now(),
                        }), None,
                    );
                    sync.publish_critical_durable(event).await;
                }
            };
        }
        info!("Starting strategy teardown; broker connections remain active");
        teardown_progress!(
            "starting",
            "Starting teardown; broker connections remain active"
        );
        self.begin_teardown();
        if entered_live_execution {
            self.run_on_teardown_logic(LifecycleTiming::BeforeGenerated);
            strategy.on_teardown(self);
            self.run_on_teardown_logic(LifecycleTiming::AfterGenerated);
        } else {
            info!("Startup recovery ended before live execution; skipping user teardown hooks");
        }

        // A close request is not a close confirmation. Keep the bridge and broker event
        // queue alive until the requested flattening has actually reached terminal states.
        // AQS must not kill this process merely because broker cleanup takes longer.
        let mut last_cleanup_retry = Instant::now();
        let reconciliation_deadline = Instant::now() + teardown_timeout;
        loop {
            if let Some(sync) = local_batch_sync.as_ref() {
                sync.flush_pending_publications();
            }
            if !entered_live_execution {
                break;
            }
            let mut execution_fills = Vec::new();
            for _ in 0..LIVE_TRADE_NOTIFICATION_BATCH_SIZE {
                let Ok(envelope) = trade_rx.try_recv() else {
                    break;
                };
                if let Some(fill) = envelope.metadata.execution_fill {
                    execution_fills.push(aqe_live_protocol::ExecutionFillInput {
                        insight_id: envelope.event.0.insight_id,
                        fill: *fill,
                    });
                }
            }
            if let Err(error) = self.persist_live_fill_evidence(execution_fills).await {
                error!("Teardown fill analytics incomplete; evidence not recorded: {error}");
            }
            self.on_trade_update();
            if self.resume_live_command_pipes(&mut command_attempted) {
                if let Some(ready) = command_ready.as_ref() {
                    ready.notify_one();
                }
            }
            if let (Some(sync), Some(a)) = (local_batch_sync.as_mut(), auth.as_ref()) {
                self.publish_dirty_insights_to_local_bus(
                    sync,
                    a,
                    Some(LIVE_SYNC_MAX_INSIGHTS_PER_FLUSH),
                    event_gateway_primary,
                )
                .await;
            }
            let fatal = self.execution_error.clone().or_else(|| {
                self.command_driver
                    .as_ref()
                    .and_then(|driver| driver.non_reconcilable_execution_error())
            });
            if let Some(error) = fatal {
                error!("Teardown cannot prove broker settlement: {error}");
                break;
            }
            if self.live_execution_failure().is_some() && lifecycle_check.is_none() {
                error!(
                    "Teardown broker reconciliation unavailable or deadline reached; retaining unresolved execution journal"
                );
                break;
            }
            let remaining = self.insights.active_insight_count();
            if (remaining == 0 || !self.teardown_cleanup_requested)
                && !self.live_execution_pending()
            {
                break;
            }
            if Instant::now() >= reconciliation_deadline {
                self.execution_error = Some(crate::core::broker::commands::BrokerCommandError::Invalid(
                    "Teardown settlement deadline reached; broker settlement is unconfirmed and the execution journal is retained".into()));
                error!(
                    "Teardown settlement deadline reached; retaining unresolved execution journal"
                );
                break;
            }
            if self.teardown_cleanup_requested
                && last_cleanup_retry.elapsed() >= Duration::from_secs(2)
            {
                let commands = self
                    .command_driver
                    .as_ref()
                    .map_or(0, |driver| driver.pending_count());
                warn!(
                    "Teardown waiting for {} active insight(s), {} unsettled broker command(s); broker connection remains open",
                    remaining, commands
                );
                let report = self.cleanup_active_insights_for_teardown_internal();
                if report.has_failures() {
                    warn!(
                        "Teardown cleanup requires attention: {}",
                        report.failures.join("; ")
                    );
                }
                last_cleanup_retry = Instant::now();
            }
            tokio::select! {
                notification = async {
                    match action_stream.as_mut() {
                        Some(stream) => stream.next().await,
                        None => std::future::pending().await,
                    }
                } => {
                    match notification {
                        Some(Ok(notification)) => {
                            if let Some(id) = acknowledge_teardown_stop(
                                &notification.data.into_json_value(), &mut processed_action_ids,
                                local_batch_sync.as_mut(),
                            ).await { stop_action_id.get_or_insert(id); }
                        }
                        _ => action_stream = None,
                    }
                }
                delivery = async {
                    match local_action_stream.as_mut() {
                        Some(stream) => stream.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if let Some(delivery) = delivery {
                        let accepted = acknowledge_teardown_stop_delivery(
                            &delivery.event.payload, &mut processed_action_ids,
                            local_batch_sync.as_mut(), Some(delivery.clone()),
                        ).await;
                        if let Some(id) = accepted.as_ref() { stop_action_id.get_or_insert(id.clone()); }
                    } else { local_action_stream = None; }
                }
                _ = shutdown_signal_interval.tick(), if shutdown_signal_file.is_some() => {
                    if let Some(path) = shutdown_signal_file.as_ref().filter(|path| path.is_file()) {
                        let _ = fs::remove_file(path);
                        info!("Additional local stop received; broker teardown is already in progress");
                    }
                }
                _ = tokio::task::yield_now(), if self.has_pending_broker_events() => {}
                _ = lifecycle_tick.tick(), if lifecycle_check.is_some() => {
                    if let Some(check) = lifecycle_check.as_mut() { check.request(&self.insights); }
                }
                rows = async {
                    match lifecycle_check.as_mut() {
                        Some(check) => check.results.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if let Some(rows) = rows {
                        if let Some(check) = lifecycle_check.as_mut() { check.busy = false; }
                        for row in rows { self.apply_lifecycle_snapshot(row); }
                    } else { lifecycle_check = None; }
                }
                _ = async {
                    match command_ready.as_ref() {
                        Some(ready) => ready.notified().await,
                        None => std::future::pending::<()>().await,
                    }
                } => {},
                Some(_) = trade_rx.recv() => {},
                _ = tokio::time::sleep(Duration::from_millis(100)) => {},
            }
        }
        lifecycle_reads.shutdown().await;
        let execution_stop = self.finish_live_execution(local_batch_sync.as_mut()).await;

        // Capture every broker-backed value while the MT5 EA can still poll the bridge. The
        // shutdown persistence below must only consume this in-memory snapshot; issuing account
        // or quote RPCs after disconnect makes an otherwise clean stop wait for request retries.
        let final_account = self
            .broker
            .account_snapshot()
            .ok()
            .map(|account| (*account).clone());
        if let Some(account) = final_account.as_ref() {
            let captured_at = chrono::Utc::now();
            if last_live_metrics_sample.elapsed() >= Duration::from_secs(LIVE_METRICS_SAMPLE_SECS) {
                self.live_metrics.update_equity(account.equity, captured_at);
            } else {
                self.live_metrics
                    .refresh_equity(account.equity, captured_at);
            }
        }
        self.refresh_live_open_position_metrics();
        self.live_metrics.finish(chrono::Utc::now());
        let strategy_key = self.live_metrics_strategy_key(auth.as_ref());
        self.persist_local_live_metrics_checkpoint(&strategy_key);

        // Clean up subscriptions
        live_quotes.stop(&self.broker).await;
        let _ = self.broker.unsubscribe_from_trade_stream().await;
        let _ = self
            .broker
            .unsubscribe_from_data_stream(self.universe.keys().cloned().collect())
            .await;
        if execution_stop.is_ok() {
            info!("Teardown cleanup durably confirmed; disconnecting broker");
            teardown_progress!(
                "cleanup_complete",
                "Teardown cleanup complete; disconnecting broker"
            );
        }
        let disconnected = self.broker.disconnect().await.map(|_| ());

        let final_sync = if execution_stop.is_ok() && disconnected.is_ok() {
            match (local_batch_sync.as_mut(), auth.as_ref()) {
                (Some(sync), Some(auth)) => {
                    self.drain_final_insights_to_local_bus(sync, auth, Duration::from_secs(30))
                        .await
                }
                _ => Ok(()),
            }
        } else {
            Ok(())
        };

        self.strategy = Some(strategy);
        self.history_seed_anchor = None;

        if let Err(error) = execution_stop.and(disconnected).and(final_sync) {
            self.status = StrategyStatus::Stopping;
            let message = format!(
                "Live teardown incomplete; retained execution journal requires recovery: {error}"
            );
            teardown_progress!("incomplete", message.clone());
            if let Some(sync) = local_batch_sync.as_mut() {
                let actions = processed_action_ids
                    .iter()
                    .map(|action_id| {
                        sync.event(
                            format!("strategy-action:{action_id}"),
                            EventPriority::Critical,
                            EventKind::StrategyActionAcknowledgement,
                            serde_json::json!({
                                "action_id": action_id, "action": StrategyActionKind::Stop,
                                "status": StrategyActionStatus::Failed, "message": message.clone(),
                            }),
                            None,
                        )
                    })
                    .collect();
                sync.publish_critical_batch_durable(actions).await;
                sync.shutdown(LocalEventPublisherConfig::default().graceful_shutdown_timeout)
                    .await;
            }
            if let Some(embedded) = embedded_sync.as_mut() {
                embedded.shutdown().await;
            }
            if let Some(client) = &db {
                update_strategy_action_statuses(
                    client,
                    &processed_action_ids,
                    StrategyActionStatus::Failed,
                    &message,
                )
                .await;
            }
            if let Some(artifacts) = local_live_artifacts.as_mut() {
                match artifacts
                    .drain_event_writes(&mut pending_local_live_events)
                    .await
                {
                    Ok(()) => {
                        let _ = artifacts
                            .finish("failed", &message, self.live_metrics.snapshot())
                            .await;
                    }
                    Err(error) => warn!(
                        "Local event persistence also incomplete during failed teardown: {error}"
                    ),
                }
            }
            self.stop_metrics_checkpoint_writer().await;
            self.publish_runtime_event("error", message, "recovery_required");
            self.runtime_telemetry.stop_tui();
            return Err(error);
        }
        self.status = StrategyStatus::Stopped;

        teardown_progress!(
            "complete",
            "Teardown complete; final synchronization and process exit pending"
        );

        if let (Some(sync), Some(a)) = (local_batch_sync.as_mut(), auth.as_ref()) {
            self.publish_account_and_metrics_to_local_bus(sync, final_account.as_ref(), true);
            let stopped = sync.event(
                "strategy-lifecycle",
                EventPriority::Critical,
                EventKind::StrategyLifecycle,
                serde_json::json!({
                    "state": "stopped",
                    "status": "Stopped",
                    "strategy_name": self.name,
                    "strategy_id": a.strategy_id,
                    "node_id": a.node_id,
                    "stopped_at": chrono::Utc::now(),
                }),
                None,
            );
            let actions = processed_action_ids
                .iter()
                .map(|action_id| {
                    sync.event(
                        format!("strategy-action:{action_id}"),
                        EventPriority::Critical,
                        EventKind::StrategyActionAcknowledgement,
                        serde_json::json!({
                            "action_id": action_id,
                            "action": StrategyActionKind::Stop,
                            "status": StrategyActionStatus::Completed,
                            "message": "Strategy stopped gracefully",
                            "completed_at": chrono::Utc::now(),
                        }),
                        None,
                    )
                })
                .collect();
            sync.publish_stop_completion_durable(stopped, actions).await;
            sync.shutdown(LocalEventPublisherConfig::default().graceful_shutdown_timeout)
                .await;
        }

        if let Some(embedded) = embedded_sync.as_mut() {
            embedded.shutdown().await;
        }

        // Final cloud projection above may refresh the last checkpoint too.
        self.stop_metrics_checkpoint_writer().await;

        // A shutdown deadline is only a waiting bound, never a persistence ACK.
        // The embedded owner gets its own final drain opportunity above; inspect
        // completion afterwards before reporting a successfully finished run.
        if local_batch_sync
            .as_ref()
            .is_some_and(|sync| !sync.publisher.shutdown_complete())
        {
            self.status = StrategyStatus::Stopping;
            let message = "Final lifecycle synchronization incomplete; publisher has unaccepted or pending work";
            if let Some(client) = &db {
                update_strategy_action_statuses(
                    client,
                    &processed_action_ids,
                    StrategyActionStatus::Failed,
                    message,
                )
                .await;
            }
            if let Some(artifacts) = local_live_artifacts.as_mut() {
                if artifacts
                    .drain_event_writes(&mut pending_local_live_events)
                    .await
                    .is_ok()
                {
                    let _ = artifacts
                        .finish("failed", message, self.live_metrics.snapshot())
                        .await;
                }
            }
            self.publish_runtime_event("error", message, "persistence_failed");
            self.runtime_telemetry.stop_tui();
            return Err(BrokerError::ConnectionError(message.into()));
        }

        if let Some(client) = &db {
            update_strategy_action_statuses(
                client,
                &processed_action_ids,
                StrategyActionStatus::Completed,
                "Strategy stopped gracefully",
            )
            .await;
        }

        if let Some(artifacts) = local_live_artifacts.as_mut() {
            if let Err(error) = artifacts
                .drain_event_writes(&mut pending_local_live_events)
                .await
            {
                let message = format!(
                    "Local event persistence incomplete ({} pending): {error}",
                    pending_local_live_events.len()
                );
                self.publish_runtime_event("error", message, "persistence_failed");
                self.runtime_telemetry.stop_tui();
                return Err(error);
            }
            if let Err(error) = artifacts
                .finish(
                    "completed",
                    "Strategy exited live mode",
                    self.live_metrics.snapshot(),
                )
                .await
            {
                warn!(
                    "Failed to finalise local live run database {}: {}",
                    artifacts.db_path.display(),
                    error
                );
                self.publish_runtime_event(
                    "error",
                    format!("Local artifact finalisation incomplete: {error}"),
                    "persistence_failed",
                );
                self.runtime_telemetry.stop_tui();
                return Err(error);
            }
        }

        aqs_sync_status = if auth.is_some() {
            "completed".to_string()
        } else {
            "not configured".to_string()
        };
        self.publish_runtime_event("info", "Live strategy stopped", aqs_sync_status);
        self.runtime_telemetry.wait_for_tui_close();
        if let Some(path) = shutdown_signal_file.as_ref() {
            let _ = fs::remove_file(path);
        }

        Ok(())
    }
}

#[cfg(test)]
#[path = "aqs_ops_tests.rs"]
mod account_sync_tests;

impl<S, E, D> StrategyState<S, E, D>
where
    S: Strategy + Send + Sync + 'static,
    E: Broker + OrderManagementProvider + Send + Sync + 'static,
    D: DataFeed + DataProvider + Send + Sync + 'static,
{
    async fn start<C: surrealdb::Connection>(
        &mut self,
        strategy: &mut S,
        client: Option<&surrealdb::Surreal<C>>,
        auth: Option<&AqsAuth>,
    ) -> Result<
        (
            Option<StrategyEventRecord>,
            Account,
            chrono::DateTime<chrono::Utc>,
        ),
        BrokerError,
    > {
        // Establish identity once, before user callbacks. All subsequent synchronous
        // context reads use the refresh cache; a slow RPC is never moved into a pipe.
        let account_observed = Instant::now();
        let initial_account =
            tokio::time::timeout(Duration::from_secs(5), self.broker.get_account())
                .await
                .map_err(|_| {
                    BrokerError::AccountError("Timed out obtaining startup account identity".into())
                })??;
        let account_captured_at = chrono::Utc::now();
        self.broker.start_snapshot_reads(
            initial_account.clone(),
            account_observed,
            &[],
            Default::default(),
            Default::default(),
        )?;
        debug!("Invoking strategy on_start");
        self.run_on_start_logic(LifecycleTiming::BeforeGenerated);
        strategy.on_start(self);
        self.run_on_start_logic(LifecycleTiming::AfterGenerated);
        self.load_universe(strategy).await;

        if self.universe.is_empty() {
            return Err(BrokerError::DataFeedError(
                "No tradable universe assets were loaded; check data-feed connectivity, symbol mapping, and asset metadata responses"
                    .to_string(),
            ));
        }

        self.broker
            .set_snapshot_symbols(&self.universe.keys().cloned().collect::<Vec<_>>())?;
        self.broker.set_snapshot_capacity_symbols(
            &self
                .universe
                .values()
                .filter(|asset| asset.max_total_size.is_some())
                .map(|asset| asset.symbol.clone())
                .collect::<Vec<_>>(),
        )?;

        self.history_seed_anchor = Some(self.broker.get_current_time());
        self.start_alpha_models();
        self.load_init(strategy);
        self.init_alpha_models();

        let mut initial_universe_symbols = self.universe.keys().cloned().collect::<Vec<_>>();
        initial_universe_symbols.sort();
        initial_universe_symbols.dedup();
        let executed_at = account_captured_at;
        let live_metrics_reset_reason = self
            .initialize_live_metrics_continuity(
                client,
                auth,
                Some(&initial_account),
                executed_at,
                initial_universe_symbols,
            )
            .await;

        let reset_event = live_metrics_reset_reason.map(|reason| StrategyEventRecord {
            event_type: StrategyEventType::Lifecycle,
            level: StrategyEventLevel::Warn,
            title: StrategyEventTitle::LiveMetricsReset,
            message: reason,
            payload: Some(serde_json::json!({
                "account_id": initial_account.account_id.trim(),
            })),
            created_at: Some(executed_at),
        });
        Ok((reset_event, initial_account, account_captured_at))
    }
}
