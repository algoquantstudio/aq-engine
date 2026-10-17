//! Optional hosted synchronization owned by a standalone strategy process.
use super::{
    HttpBatchSink, HttpBatchSinkConfig, InProcessEventBus, LocalEventPublisher,
    LocalEventPublisherConfig, OutboxStore, ReconciliationStart, SyncAgent, SyncAgentConfig,
    deterministic_event_id, gateway_endpoint,
};
use crate::core::strategy::AqsAuth;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle};

#[derive(Clone, Debug)]
pub struct HostedSyncConfig {
    gateway_url: String,
    outbox_directory: PathBuf,
    namespace: String,
    database: String,
    launch_run_id: Option<String>,
}

impl HostedSyncConfig {
    /// Explicit opt-in. No local NATS, runtime-host process, or synchronization
    /// environment flags are required. Keep this directory across process restarts.
    pub fn new(gateway_url: &str, outbox_directory: impl Into<PathBuf>) -> Result<Self, String> {
        Ok(Self {
            gateway_url: gateway_endpoint(gateway_url)?.to_string(),
            outbox_directory: outbox_directory.into(),
            namespace: "aqs".into(),
            database: "aqs".into(),
            launch_run_id: None,
        })
    }

    pub fn with_database(
        mut self,
        namespace: impl Into<String>,
        database: impl Into<String>,
    ) -> Self {
        self.namespace = namespace.into();
        self.database = database.into();
        self
    }

    /// AQS has already selected this execution fence in its protected launch
    /// record. Keep it exact; standalone API users still get a fresh run ID.
    pub(crate) fn with_launch_run(mut self, run_id: &str) -> Result<Self, String> {
        uuid::Uuid::parse_str(run_id).map_err(|_| "Invalid prepared launch run identity")?;
        self.launch_run_id = Some(run_id.into());
        Ok(self)
    }
}

/// Owns an optional hosted-sync publisher and its background delivery workers.
/// Call `shutdown` before dropping a session to drain retained local work.
pub struct EmbeddedHostedSync {
    pub publisher: LocalEventPublisher,
    pub runtime_id: String,
    pub run_id: String,
    retirement: (Arc<HttpBatchSink>, OutboxStore, OutboxStore, String, String),
    shutdown: watch::Sender<bool>,
    workers: Vec<JoinHandle<()>>,
    _lock: Arc<File>,
}

impl EmbeddedHostedSync {
    pub async fn start(config: &HostedSyncConfig, auth: &AqsAuth) -> Result<Self, String> {
        super::install_default_rustls_provider();
        if auth.session_secret.is_empty() {
            return Err("Hosted sync requires AQS live-session credentials".into());
        }
        let session = auth
            .live_session_id
            .as_deref()
            .unwrap_or(&auth.session_id)
            .strip_prefix("live_strategy_session:")
            .unwrap_or(auth.live_session_id.as_deref().unwrap_or(&auth.session_id));
        let scope = uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_URL,
            &serde_json::to_vec(&(&auth.user_id, &auth.strategy_id, session))
                .map_err(|e| e.to_string())?,
        )
        .simple()
        .to_string();
        let directory = config.outbox_directory.join(scope);
        let (lock, runtime_id) = open_runtime(&directory)?;
        let lock = Arc::new(lock);
        let run_id = config.launch_run_id.clone().unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        if config.launch_run_id.is_some() {
            // The DB makes terminal states sticky for an execution fence. A
            // copied launch is single-use, not an auto-restart service. Never
            // run the broker again under a stopped or already-admitted config.
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)] { use std::os::unix::fs::OpenOptionsExt; options.mode(0o600); }
            let marker = options.open(directory.join(format!("launch-{run_id}.used")))
                .map_err(|_| "This copied launch has already been attempted or cannot be claimed. Stop/recover it in AQS and copy a fresh live command.")?;
            marker.sync_all().map_err(|_| "Cannot persist copied launch ownership")?;
        }
        let outbox = OutboxStore::open(directory.join("sync.db"))
            .await
            .map_err(|e| e.to_string())?;
        let emergency = OutboxStore::open(directory.join("publisher.db"))
            .await
            .map_err(|e| e.to_string())?;
        let initial_sequence = emergency
            .critical_sequence_cursor_for_runtime_scope(&runtime_id, &auth.strategy_id, session)
            .await
            .map_err(|e| e.to_string())?;
        // Establish run ownership before either worker can replay an older broker
        // snapshot. This is local metadata only, not an authoritative empty snapshot.
        // The actual broker snapshot fills in this same generation after startup.
        let start = ReconciliationStart {
            generation: deterministic_event_id(&format!(
                "aqs-reconciliation-v4:{}:{}:{}",
                auth.strategy_id, session, run_id
            ))
            .to_string(),
            runtime_id: runtime_id.clone(),
            strategy_id: auth.strategy_id.clone(),
            live_session_id: session.into(),
            run_id: Some(run_id.clone()),
            snapshot_complete: false,
            broker_features: Vec::new(),
            critical_cutoff: Some(initial_sequence),
            total_chunks: 0,
            total_records: 0,
        };
        for store in [&outbox, &emergency] {
            store
                .begin_reconciliation_job(&start)
                .await
                .map_err(|e| e.to_string())?;
        }
        let sink = Arc::new(
            HttpBatchSink::new(sink_config(config, auth, &runtime_id, &run_id, session)?, &config.gateway_url)
                .map_err(|e| e.to_string())?,
        );
        if config.launch_run_id.is_some() {
            // Copied commands did not claim an execution route in AQS. Refuse
            // initial broker startup if credentials cannot be admitted. Cloud
            // outages after admission still use the existing background outbox.
            sink.hosted_gateway().admit_prepared_run().await
                .map_err(|error| format!("Copied live launch was not admitted: {error}"))?;
        }
        let bus = Arc::new(InProcessEventBus::acknowledged());
        let (shutdown, receiver) = watch::channel(false);
        let agent = SyncAgent::new(
            SyncAgentConfig::new(&runtime_id, &auth.strategy_id, session),
            bus.clone(),
            outbox.clone(),
            sink.clone(),
        );
        let agent_lock = lock.clone();
        let agent_receiver = receiver.clone();
        let sync_worker = tokio::spawn(async move {
            let _lock = agent_lock;
            let mut receiver = agent_receiver;
            loop {
                if *receiver.borrow() {
                    break;
                }
                match agent.clone().run(receiver.clone()).await {
                    Ok(()) => break,
                    Err(error) => log::warn!(
                        "Embedded hosted sync worker failed; retained outbox will retry: {error}"
                    ),
                }
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {},
                    _ = receiver.changed() => break,
                }
            }
        });
        let action_lock = lock.clone();
        let action_runtime = runtime_id.clone();
        let action_run = run_id.clone();
        let action_strategy = auth.strategy_id.clone();
        let action_session = session.to_string();
        let action_bus = bus.clone();
        let action_receiver = receiver.clone();
        let retirement = (
            sink.clone(),
            outbox.clone(),
            emergency.clone(),
            auth.strategy_id.clone(),
            session.to_string(),
        );
        let action_worker = tokio::spawn(async move {
            let _lock = action_lock;
            super::actions::run_action_forwarder(
                action_runtime,
                action_strategy,
                action_session,
                action_run,
                action_bus,
                sink,
                outbox,
                action_receiver,
            )
            .await;
        });
        let maintenance_lock = lock.clone();
        let maintenance_store = emergency.clone();
        let maintenance_worker = tokio::spawn(async move {
            let _lock = maintenance_lock;
            maintain_publisher_outbox(maintenance_store, receiver).await;
        });
        let publisher = LocalEventPublisher::spawn_owned_with_initial_sequence(
            bus,
            emergency,
            LocalEventPublisherConfig::default(),
            initial_sequence,
            lock.clone(),
        );
        Ok(Self {
            publisher,
            runtime_id,
            run_id,
            retirement,
            shutdown,
            workers: vec![sync_worker, action_worker, maintenance_worker],
            _lock: lock,
        })
    }

    pub async fn shutdown(&mut self) {
        // First commit publisher work locally while the consumer is still alive.
        self.publisher.shutdown(Duration::from_secs(3)).await;
        let _ = self.shutdown.send(true);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        for worker in &mut self.workers {
            if tokio::time::timeout_at(deadline, &mut *worker)
                .await
                .is_err()
            {
                worker.abort();
                let _ = worker.await;
            }
        }
        self.workers.clear();
        let (sink, outbox, publisher_outbox, strategy, session) = &self.retirement;
        // Never seal after a timed-out publisher flush, or while any lane/DLQ still
        // owns work. Retained local files remain available to a later restart.
        if self.publisher.shutdown_complete()
            && outbox
                .retirement_uncommitted_for_scope(strategy, session)
                .await
                .unwrap_or(1)
                == 0
            && publisher_outbox
                .retirement_uncommitted_for_scope(strategy, session)
                .await
                .unwrap_or(1)
                == 0
        {
            match tokio::time::timeout(Duration::from_secs(3), sink.seal_drained_run()).await {
                Ok(Ok(())) => {
                    log::info!("Hosted run sealed for identity compaction: {}", self.run_id)
                }
                _ => log::warn!(
                    "Hosted run retirement unconfirmed; retaining identity protection for {}",
                    self.run_id
                ),
            }
        }
    }
}

async fn maintain_publisher_outbox(store: OutboxStore, mut shutdown: watch::Receiver<bool>) {
    let defaults = SyncAgentConfig::new("", "", "");
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            _ = interval.tick() => {
                let result = async {
                    let pruned = store.prune_committed(
                        defaults.retention_grace.as_micros().min(i64::MAX as u128) as i64
                    ).await?;
                    store.enforce_storage_policy(defaults.outbox_soft_limit_bytes).await?;
                    store.checkpoint_wal(pruned > 0).await
                }.await;
                if let Err(error) = result {
                    log::warn!("Embedded publisher outbox maintenance failed; pending events retained: {error}");
                }
            }
            _ = shutdown.changed() => break,
        }
    }
}

impl Drop for EmbeddedHostedSync {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        for worker in &self.workers {
            worker.abort();
        }
    }
}

fn open_runtime(directory: &Path) -> Result<(File, String), String> {
    std::fs::create_dir_all(directory).map_err(|e| e.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))
            .map_err(|e| e.to_string())?;
    }
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut lock = options
        .open(directory.join("runtime.lock"))
        .map_err(|e| e.to_string())?;
    lock.try_lock()
        .map_err(|_| "This standalone strategy/session outbox is already in use".to_string())?;
    let mut contents = String::new();
    (&mut lock)
        .take(4096)
        .read_to_string(&mut contents)
        .map_err(|e| e.to_string())?;
    let runtime_id = if contents.is_empty() {
        let id = uuid::Uuid::new_v4().to_string();
        lock.rewind().map_err(|e| e.to_string())?;
        lock.write_all(id.as_bytes()).map_err(|e| e.to_string())?;
        lock.sync_all().map_err(|e| e.to_string())?;
        id
    } else {
        uuid::Uuid::parse_str(contents.trim())
            .map_err(|_| "Invalid hosted runtime identity; preserve the outbox for recovery")?
            .to_string()
    };
    Ok((lock, runtime_id))
}

fn sink_config(
    config: &HostedSyncConfig,
    auth: &AqsAuth,
    runtime: &str,
    run: &str,
    session: &str,
) -> Result<HttpBatchSinkConfig, String> {
    let database_url = auth
        .url()
        .replacen("wss://", "https://", 1)
        .replacen("ws://", "http://", 1);
    let mut endpoint =
        reqwest::Url::parse(&database_url).map_err(|_| "Invalid AQS database URL")?;
    if !matches!(endpoint.scheme(), "http" | "https")
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
    {
        return Err("Invalid AQS database origin".into());
    }
    endpoint.set_path("/");
    endpoint.set_query(None);
    endpoint.set_fragment(None);
    let signin = endpoint
        .join("signin")
        .map_err(|e| e.to_string())?
        .to_string();
    endpoint
        .path_segments_mut()
        .map_err(|_| "Invalid database origin")?
        .clear()
        .extend([
            "api",
            &config.namespace,
            &config.database,
            "aqs",
            "live",
            "v1",
        ]);
    let base = endpoint.as_str().trim_end_matches('/');
    Ok(HttpBatchSinkConfig {
        batch_url: format!("{base}/batch"),
        actions_url: Some(format!("{base}/actions/{runtime}")),
        signin_url: Some(signin),
        bearer_token: String::new(),
        session_secret: Some(auth.session_secret.clone()),
        namespace: config.namespace.clone(),
        database: config.database.clone(),
        access_method: auth.access_method.clone(),
        user_id: auth.user_id.clone(),
        strategy_id: auth.strategy_id.clone(),
        live_session_id: session.into(),
        runtime_id: runtime.into(),
        run_id: run.into(),
        server_node_id: auth.node_id.clone().unwrap_or_default(),
        request_timeout: Duration::from_secs(10),
    })
}

#[cfg(test)]
#[path = "embedded_launch_tests.rs"]
mod launch_delivery_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::live_sync::{EventEnvelope, EventKind, EventPriority, EventSubject};

    fn credentials() -> AqsAuth {
        AqsAuth {
            access_method: "aqe_live".into(),
            session_id: "session".into(),
            session_secret: "isolated-test-secret".into(),
            strategy_id: "strategy".into(),
            user_id: "users:test".into(),
            node_id: None,
            live_session_id: Some("live_strategy_session:session".into()),
            url: Some("http://127.0.0.1:9".into()),
        }
    }

    #[test]
    fn runtime_lock_is_exclusive_private_and_restart_stable() {
        let path = std::env::temp_dir().join(format!("aqe-owned-runtime-{}", uuid::Uuid::new_v4()));
        let (lock, identity) = open_runtime(&path).unwrap();
        assert!(open_runtime(&path).is_err());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(path.metadata().unwrap().permissions().mode() & 0o777, 0o700);
            assert_eq!(
                path.join("runtime.lock")
                    .metadata()
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        drop(lock);
        let (lock, restored) = open_runtime(&path).unwrap();
        assert_eq!(identity, restored);
        drop(lock);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn hosted_configuration_uses_explicit_endpoints_without_environment_flags() {
        let config = HostedSyncConfig::new("http://127.0.0.1:9080", "/unused")
            .unwrap()
            .with_database("test namespace", "test database");
        let sink = sink_config(&config, &credentials(), "runtime", "run", "session").unwrap();
        assert_eq!(
            sink.signin_url.as_deref(),
            Some("http://127.0.0.1:9/signin")
        );
        assert_eq!(
            sink.batch_url,
            "http://127.0.0.1:9/api/test%20namespace/test%20database/aqs/live/v1/batch"
        );
        assert_eq!(sink.runtime_id, "runtime");
        assert_eq!(sink.run_id, "run");
        assert!(sink.bearer_token.is_empty());
        assert!(HostedSyncConfig::new("http://untrusted.example", "/unused").is_err());
    }

    async fn wait_for_release(directory: &Path) {
        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                if let Ok((lock, _)) = open_runtime(directory) {
                    drop(lock);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("all owned workers must release their process lock");
    }

    async fn publish_and_wait(sync: &EmbeddedHostedSync, store: &OutboxStore, revision: u64) {
        let mut event = EventEnvelope::new(
            &sync.runtime_id,
            "strategy",
            "session",
            format!("insight:{revision}"),
            revision,
            EventPriority::Critical,
            EventKind::InsightProjection,
            serde_json::json!({"state":"New"}),
        );
        event.run_id = Some(sync.run_id.clone());
        sync.publisher
            .publish_critical(
                EventSubject::for_priority(&sync.runtime_id, "strategy", EventPriority::Critical),
                event,
            )
            .unwrap();
        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                if store.health().await.unwrap().pending_critical == revision {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("publisher must commit locally despite unavailable cloud");
        assert_eq!(sync.publisher.critical_sequence_cursor(), revision);
    }

    #[tokio::test]
    async fn standalone_restart_retains_critical_events_and_supersedes_old_reconciliation() {
        let root =
            std::env::temp_dir().join(format!("aqe-embedded-restart-{}", uuid::Uuid::new_v4()));
        let config = HostedSyncConfig::new("http://127.0.0.1:9", &root).unwrap();
        let auth = credentials();
        let mut first = EmbeddedHostedSync::start(&config, &auth).await.unwrap();
        let runtime_id = first.runtime_id.clone();
        let old_run = first.run_id.clone();
        let directory = std::fs::read_dir(&root)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let sync_store = OutboxStore::open(directory.join("sync.db")).await.unwrap();
        publish_and_wait(&first, &sync_store, 1).await;
        assert!(EmbeddedHostedSync::start(&config, &auth).await.is_err());
        first.shutdown().await;
        drop(first);
        wait_for_release(&directory).await;
        let emergency = OutboxStore::open(directory.join("publisher.db"))
            .await
            .unwrap();
        let old_generation =
            deterministic_event_id(&format!("aqs-reconciliation-v4:strategy:session:{old_run}"))
                .to_string();
        let old_snapshot = EventEnvelope::new(
            &runtime_id,
            "strategy",
            "session",
            "reconciliation:old",
            1,
            EventPriority::Reconciliation,
            EventKind::ReconciliationComplete,
            serde_json::json!({"generation":old_generation, "snapshotComplete":true, "totalChunks":0,"totalRecords":0}),
        );
        for store in [&sync_store, &emergency] {
            store
                .insert_events(std::slice::from_ref(&old_snapshot))
                .await
                .unwrap();
        }
        let mut second = EmbeddedHostedSync::start(&config, &auth).await.unwrap();
        assert_eq!(second.runtime_id, runtime_id);
        assert_ne!(second.run_id, old_run);
        assert_eq!(second.publisher.critical_sequence_cursor(), 1);
        for store in [&sync_store, &emergency] {
            let health = store.health().await.unwrap();
            assert_eq!(
                health.pending_reconciliation, 0,
                "old snapshots must not replay"
            );
            assert_eq!(
                health.reconciliation.unwrap().run_id.as_deref(),
                Some(second.run_id.as_str())
            );
        }
        publish_and_wait(&second, &sync_store, 2).await;
        second.shutdown().await;
        drop(second);
        wait_for_release(&directory).await;
        assert_eq!(sync_store.health().await.unwrap().pending_critical, 2);
        drop(sync_store);
        drop(emergency);
        std::fs::remove_dir_all(root).unwrap();
    }
}
