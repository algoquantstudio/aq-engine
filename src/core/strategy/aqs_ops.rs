use super::aqs_sync::{
    mark_strategy_started, persist_live_account_state, persist_live_metrics,
    persist_strategy_event, update_strategy_action_status,
};
use super::aqs_types::{
    self, AqsAuth, StrategyEventRecord, StrategyUniverseAssetRecord, action_id_from_value,
};
use super::live_metrics::LiveMetricsSnapshot;
use super::traits::{Strategy, StrategyContext};
use super::{StrategyMode, StrategyState, StrategyStatus};
use crate::core::broker::DataStreamMode;
use crate::core::broker::traits::{Broker, DataFeed, DataProvider, OrderManagementProvider};
use crate::core::broker::types::{Account, BrokerError};
use crate::core::events::{MarketDataEvent, ResolvedEventStream};
use crate::core::insight::InsightSnapshot;
use crate::core::insight::types::InsightState;
use crate::core::lifecycle::LifecycleTiming;
use futures::StreamExt;
use log::{debug, error, info, warn};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use surrealdb::engine::any;
use surrealdb::method::QueryStream;
use surrealdb::opt::auth::Record;
use surrealdb::{IndexedResults, Notification};
use turso::{Builder, Connection, params};

type StrategyActionStream = QueryStream<Notification<surrealdb::types::Value>>;

const MAX_PENDING_AQS_SYNC_OPS: usize = 512;
const LIVE_SYNC_CONNECT_MAX_ATTEMPTS: usize = 3;
const LIVE_SYNC_RETRY_BASE_MS: u64 = 500;
const LIVE_SYNC_CONNECT_TIMEOUT_SECS: u64 = 10;
const LIVE_SYNC_STREAM_TIMEOUT_SECS: u64 = 10;
const LIVE_SYNC_QUERY_TIMEOUT_SECS: u64 = 10;
const LIVE_SYNC_RECONCILE_SECS: u64 = 15 * 60;
const LIVE_SYNC_RECONNECT_TIMEOUT_SECS: u64 = 40;
const LOCAL_LIVE_METRICS_WRITE_SECS: u64 = 5;

static PENDING_AQS_SYNC_DROPPED_OPS: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
enum PendingAqsSyncOp {
    StrategyStarted {
        universe: Vec<StrategyUniverseAssetRecord>,
        account: Option<Account>,
    },
    AccountState {
        account: Account,
        captured_at: chrono::DateTime<chrono::Utc>,
    },
    StrategyEvent(StrategyEventRecord),
    LiveMetrics(aqs_types::StrategyLiveMetricsRecord),
}

struct LocalLiveRunArtifacts {
    run_id: String,
    db_path: PathBuf,
    conn: Connection,
    last_metrics: Option<LiveMetricsSnapshot>,
}

impl LocalLiveRunArtifacts {
    async fn start(
        strategy_name: &str,
        strategy_id: uuid::Uuid,
        mode: StrategyMode,
    ) -> Result<Self, BrokerError> {
        let started_at = chrono::Utc::now();
        let run_id = format!(
            "{}-{}",
            started_at.format("%Y%m%d-%H%M%S"),
            uuid::Uuid::new_v4()
        );
        let dir = std::env::current_dir()
            .map_err(|error| {
                BrokerError::ConnectionError(format!(
                    "Failed to resolve current project directory for local live run: {}",
                    error
                ))
            })?
            .join("live")
            .join(&run_id);
        fs::create_dir_all(&dir).map_err(|error| {
            BrokerError::ConnectionError(format!(
                "Failed to create local live run directory {}: {}",
                dir.display(),
                error
            ))
        })?;

        let db_path = dir.join("live.db");
        let db = Builder::new_local(db_path.to_string_lossy().as_ref())
            .build()
            .await
            .map_err(|error| {
                BrokerError::ConnectionError(format!(
                    "Failed to open local live run database {}: {}",
                    db_path.display(),
                    error
                ))
            })?;
        let conn = db.connect().map_err(|error| {
            BrokerError::ConnectionError(format!(
                "Failed to connect to local live run database {}: {}",
                db_path.display(),
                error
            ))
        })?;
        let artifacts = Self {
            run_id,
            db_path,
            conn,
            last_metrics: None,
        };
        artifacts.init_schema().await?;

        let manifest = serde_json::json!({
            "run_id": artifacts.run_id.clone(),
            "strategy_id": strategy_id.to_string(),
            "strategy_name": strategy_name,
            "mode": format!("{:?}", mode),
            "status": "running",
            "started_at": started_at,
            "database_file": "live.db",
        });
        artifacts.upsert_metadata("run", &manifest).await?;
        artifacts
            .append_event(
                "lifecycle",
                "info",
                "Local live run started",
                "AQS Cloud auth was not provided; AQE is writing local live run artifacts",
                Some(serde_json::json!({
                    "run_id": artifacts.run_id.clone(),
                    "strategy_id": strategy_id.to_string(),
                    "strategy_name": strategy_name,
                })),
            )
            .await?;

        Ok(artifacts)
    }

    async fn init_schema(&self) -> Result<(), BrokerError> {
        self.conn
            .execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS run_metadata (
                    key TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    level TEXT NOT NULL,
                    title TEXT NOT NULL,
                    message TEXT NOT NULL,
                    payload_json TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_live_events_created_at ON events(created_at);
                CREATE INDEX IF NOT EXISTS idx_live_events_type ON events(event_type);

                CREATE TABLE IF NOT EXISTS metrics_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    captured_at TEXT NOT NULL,
                    final_equity REAL NOT NULL,
                    total_return REAL NOT NULL,
                    total_return_pct REAL NOT NULL,
                    open_positions_count INTEGER NOT NULL,
                    open_insights_count INTEGER NOT NULL,
                    total_trades INTEGER NOT NULL,
                    payload_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_live_metrics_captured_at ON metrics_snapshots(captured_at);
                "#,
            )
            .await
            .map_err(|error| {
                BrokerError::ConnectionError(format!(
                    "Failed to initialise local live run database {}: {}",
                    self.db_path.display(),
                    error
                ))
            })
    }

    async fn write_metrics_if_changed(
        &mut self,
        snapshot: Option<LiveMetricsSnapshot>,
        force: bool,
    ) -> Result<(), BrokerError> {
        let Some(snapshot) = snapshot else {
            return Ok(());
        };
        if !force && self.last_metrics.as_ref() == Some(&snapshot) {
            return Ok(());
        }

        let saved_snapshot = snapshot.clone();
        let payload_json = serde_json::to_string(&serde_json::json!({
            "run_id": self.run_id,
            "updated_at": chrono::Utc::now(),
            "metrics": snapshot,
        }))
        .map_err(|error| {
            BrokerError::ConnectionError(format!(
                "Failed to serialise local live metrics for {}: {}",
                self.run_id, error
            ))
        })?;
        self.conn
            .execute(
                "INSERT INTO metrics_snapshots (
                    captured_at,
                    final_equity,
                    total_return,
                    total_return_pct,
                    open_positions_count,
                    open_insights_count,
                    total_trades,
                    payload_json
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    saved_snapshot.updated_at.to_rfc3339(),
                    saved_snapshot.final_equity,
                    saved_snapshot.total_return,
                    saved_snapshot.total_return_pct,
                    saved_snapshot.open_positions_count as i64,
                    saved_snapshot.open_insights_count as i64,
                    saved_snapshot.total_trades as i64,
                    payload_json
                ],
            )
            .await
            .map_err(|error| {
                BrokerError::ConnectionError(format!(
                    "Failed to write local live metrics into {}: {}",
                    self.db_path.display(),
                    error
                ))
            })?;
        self.last_metrics = Some(saved_snapshot);
        Ok(())
    }

    async fn finish(
        &mut self,
        status: &str,
        message: &str,
        snapshot: Option<LiveMetricsSnapshot>,
    ) -> Result<(), BrokerError> {
        self.write_metrics_if_changed(snapshot, true).await?;
        let finished_at = chrono::Utc::now();
        self.append_event("lifecycle", "info", status, message, None)
            .await?;
        let completed = serde_json::json!({
            "run_id": self.run_id,
            "status": status,
            "message": message,
            "finished_at": finished_at,
        });
        self.upsert_metadata("completion", &completed).await
    }

    async fn append_event(
        &self,
        event_type: &str,
        level: &str,
        title: &str,
        message: &str,
        payload: Option<serde_json::Value>,
    ) -> Result<(), BrokerError> {
        let created_at = chrono::Utc::now();
        let payload_json = payload
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| {
                BrokerError::ConnectionError(format!(
                    "Failed to serialise local live event for {}: {}",
                    self.run_id, error
                ))
            })?;
        self.conn
            .execute(
                "INSERT INTO events (event_type, level, title, message, payload_json, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    event_type.to_string(),
                    level.to_string(),
                    title.to_string(),
                    message.to_string(),
                    payload_json,
                    created_at.to_rfc3339()
                ],
            )
            .await
            .map_err(|error| {
                BrokerError::ConnectionError(format!(
                    "Failed to write local live event into {}: {}",
                    self.db_path.display(),
                    error
                ))
            })
            .map(|_| ())
    }

    async fn upsert_metadata(
        &self,
        key: &str,
        payload: &serde_json::Value,
    ) -> Result<(), BrokerError> {
        let payload_json = serde_json::to_string(payload).map_err(|error| {
            BrokerError::ConnectionError(format!(
                "Failed to serialise local live metadata {} for {}: {}",
                key, self.run_id, error
            ))
        })?;
        self.conn
            .execute(
                "INSERT INTO run_metadata (key, payload_json, updated_at)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(key) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    updated_at = excluded.updated_at",
                params![
                    key.to_string(),
                    payload_json,
                    chrono::Utc::now().to_rfc3339()
                ],
            )
            .await
            .map_err(|error| {
                BrokerError::ConnectionError(format!(
                    "Failed to write local live metadata into {}: {}",
                    self.db_path.display(),
                    error
                ))
            })
            .map(|_| ())
    }
}

impl<S, E, D> StrategyState<S, E, D>
where
    S: Strategy,
    E: Broker + OrderManagementProvider,
    D: DataFeed + DataProvider,
{
    fn live_session_key_for_auth(auth: &AqsAuth) -> String {
        auth.live_session_id
            .as_deref()
            .map(aqs_types::live_session_key)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| auth.session_id.clone())
    }

    fn live_insight_record_id(auth: &AqsAuth, insight_id: &str) -> String {
        format!(
            "{}::{}::{}",
            auth.strategy_id,
            Self::live_session_key_for_auth(auth),
            insight_id
        )
    }

    fn is_transient_surreal_error(error: &surrealdb::Error) -> bool {
        let message = error.to_string().to_lowercase();
        message.contains("connection reset")
            || message.contains("connection closed")
            || message.contains("broken pipe")
            || message.contains("timed out")
            || message.contains("timeout")
            || message.contains("transport")
            || message.contains("503")
            || message.contains("service unavailable")
            || message.contains("temporarily unavailable")
            || message.contains("websocket error")
            || message.contains("http error")
    }

    async fn connect_live_sync_db(
        auth: &AqsAuth,
    ) -> Result<surrealdb::Surreal<any::Any>, BrokerError> {
        let signin_session_id = auth.session_id.clone();
        info!(
            "Authenticating AQS Cloud live sync with session id: {}",
            signin_session_id
        );
        for attempt in 1..=LIVE_SYNC_CONNECT_MAX_ATTEMPTS {
            let connect_result = tokio::time::timeout(
                Duration::from_secs(LIVE_SYNC_CONNECT_TIMEOUT_SECS),
                any::connect(auth.url()),
            )
            .await;

            match connect_result {
                Err(_) => {
                    let message = format!(
                        "Timed out connecting to AQS Cloud after {}s",
                        LIVE_SYNC_CONNECT_TIMEOUT_SECS
                    );
                    if attempt < LIVE_SYNC_CONNECT_MAX_ATTEMPTS {
                        warn!(
                            "AQS live sync connection error for strategy {} on attempt {}/{}: {}",
                            auth.strategy_id, attempt, LIVE_SYNC_CONNECT_MAX_ATTEMPTS, message
                        );
                        tokio::time::sleep(Duration::from_millis(
                            LIVE_SYNC_RETRY_BASE_MS * attempt as u64,
                        ))
                        .await;
                        continue;
                    }
                    error!("{}", message);
                    return Err(BrokerError::ConnectionError(message));
                }
                Ok(Err(error)) => {
                    let message = format!("Failed to connect to AQS Cloud: {}", error);
                    if attempt < LIVE_SYNC_CONNECT_MAX_ATTEMPTS {
                        warn!(
                            "AQS live sync connection error for strategy {} on attempt {}/{}: {}",
                            auth.strategy_id, attempt, LIVE_SYNC_CONNECT_MAX_ATTEMPTS, message
                        );
                        tokio::time::sleep(Duration::from_millis(
                            LIVE_SYNC_RETRY_BASE_MS * attempt as u64,
                        ))
                        .await;
                        continue;
                    }
                    error!("{}", message);
                    return Err(BrokerError::ConnectionError(message));
                }
                Ok(Ok(client)) => {
                    debug!(
                        "Connected transport for AQS live sync on strategy {}; authenticating session",
                        auth.strategy_id
                    );
                    let signin_result = tokio::time::timeout(
                        Duration::from_secs(LIVE_SYNC_CONNECT_TIMEOUT_SECS),
                        async {
                            client
                                .signin(Record {
                                    namespace: "aqs".to_string(),
                                    database: "aqs".to_string(),
                                    access: auth.access_method.clone(),
                                    params: std::collections::BTreeMap::from([
                                        ("session_id".to_string(), signin_session_id.clone()),
                                        ("secret".to_string(), auth.session_secret.clone()),
                                    ]),
                                })
                                .await
                                .map_err(|e| {
                                    format!("Failed to authenticate AQS Cloud live sync: {}", e)
                                })?;
                            client.use_ns("aqs").use_db("aqs").await.map_err(|e| {
                                format!("Failed to select AQS database for live sync: {}", e)
                            })?;
                            Ok::<(), String>(())
                        },
                    )
                    .await;

                    match signin_result {
                        Ok(Ok(())) => {
                            debug!(
                                "AQS live sync authenticated for strategy {}; marking session active",
                                auth.strategy_id
                            );
                            let activate_session_result = tokio::time::timeout(
                                Duration::from_secs(LIVE_SYNC_QUERY_TIMEOUT_SECS),
                                client
                                    .query(
                                        "UPDATE type::record('live_strategy_session', <uuid>$live_session_key)
                                         SET status = 'active',
                                             last_used_at = time::now()",
                                    )
                                    .bind(("live_session_key", Self::live_session_key_for_auth(auth))),
                            )
                            .await;

                            match activate_session_result {
                                Ok(Ok(response)) => {
                                    if let Err(error) = response.check() {
                                        warn!(
                                            "Failed to mark AQS live session active for strategy {}: {}",
                                            auth.strategy_id, error
                                        );
                                    }
                                }
                                Ok(Err(error)) => {
                                    warn!(
                                        "Failed to update AQS live session status for strategy {}: {}",
                                        auth.strategy_id, error
                                    );
                                }
                                Err(_) => {
                                    warn!(
                                        "Timed out marking AQS live session active for strategy {} after {}s",
                                        auth.strategy_id, LIVE_SYNC_QUERY_TIMEOUT_SECS
                                    );
                                }
                            }

                            info!("Connected to AQS Cloud for live sync");
                            let lifecycle_event = StrategyEventRecord {
                                event_type: "lifecycle".into(),
                                level: "info".into(),
                                title: "AQS live sync connected".into(),
                                message: "AQE authenticated and connected to AQS Cloud".into(),
                                payload: None,
                                created_at: Some(chrono::Utc::now()),
                            };
                            match tokio::time::timeout(
                                Duration::from_secs(LIVE_SYNC_QUERY_TIMEOUT_SECS),
                                persist_strategy_event(&client, auth, lifecycle_event),
                            )
                            .await
                            {
                                Ok(Ok(())) => {}
                                Ok(Err(error)) => {
                                    warn!(
                                        "Failed to persist AQS live sync lifecycle event for strategy {}: {}",
                                        auth.strategy_id, error
                                    );
                                }
                                Err(_) => {
                                    warn!(
                                        "Timed out persisting AQS live sync lifecycle event for strategy {} after {}s",
                                        auth.strategy_id, LIVE_SYNC_QUERY_TIMEOUT_SECS
                                    );
                                }
                            }
                            return Ok(client);
                        }
                        Ok(Err(error)) => {
                            if attempt < LIVE_SYNC_CONNECT_MAX_ATTEMPTS {
                                warn!(
                                    "AQS live sync authentication error for strategy {} on attempt {}/{}: {}",
                                    auth.strategy_id,
                                    attempt,
                                    LIVE_SYNC_CONNECT_MAX_ATTEMPTS,
                                    error
                                );
                                tokio::time::sleep(Duration::from_millis(
                                    LIVE_SYNC_RETRY_BASE_MS * attempt as u64,
                                ))
                                .await;
                                continue;
                            }
                            error!("{}", error);
                            return Err(BrokerError::ConnectionError(error));
                        }
                        Err(_) => {
                            let message = format!(
                                "Timed out authenticating AQS Cloud live sync after {}s",
                                LIVE_SYNC_CONNECT_TIMEOUT_SECS
                            );
                            if attempt < LIVE_SYNC_CONNECT_MAX_ATTEMPTS {
                                warn!(
                                    "AQS live sync authentication error for strategy {} on attempt {}/{}: {}",
                                    auth.strategy_id,
                                    attempt,
                                    LIVE_SYNC_CONNECT_MAX_ATTEMPTS,
                                    message
                                );
                                tokio::time::sleep(Duration::from_millis(
                                    LIVE_SYNC_RETRY_BASE_MS * attempt as u64,
                                ))
                                .await;
                                continue;
                            }
                            error!("{}", message);
                            return Err(BrokerError::ConnectionError(message));
                        }
                    }
                }
            }
        }

        Err(BrokerError::ConnectionError(format!(
            "Failed to connect to AQS Cloud after {} attempt(s)",
            LIVE_SYNC_CONNECT_MAX_ATTEMPTS
        )))
    }

    async fn create_strategy_action_stream(
        client: &surrealdb::Surreal<any::Any>,
        auth: &AqsAuth,
    ) -> Option<StrategyActionStream> {
        debug!(
            "Creating AQS strategy action stream for strategy {}",
            auth.strategy_id
        );
        let live_query = tokio::time::timeout(
            Duration::from_secs(LIVE_SYNC_STREAM_TIMEOUT_SECS),
            client
                .query(
                    "LIVE SELECT * FROM strategy_actions
                     WHERE strategy_id = type::record('strategy', $strategy_id)
                       AND live_session_id = type::record('live_strategy_session', <uuid>$live_session_key)
                       AND status = 'pending'",
                )
                .bind(("strategy_id", auth.strategy_id.clone()))
                .bind(("live_session_key", Self::live_session_key_for_auth(auth))),
        )
        .await;

        match live_query {
            Ok(Ok(mut results)) => match results.stream::<Notification<surrealdb::types::Value>>(0)
            {
                Ok(stream) => {
                    debug!(
                        "AQS strategy action stream ready for strategy {}",
                        auth.strategy_id
                    );
                    Some(stream)
                }
                Err(error) => {
                    warn!(
                        "Failed to materialize AQS strategy action stream for strategy {}: {}",
                        auth.strategy_id, error
                    );
                    None
                }
            },
            Ok(Err(error)) => {
                warn!(
                    "Failed to create AQS strategy action stream for strategy {}: {}",
                    auth.strategy_id, error
                );
                None
            }
            Err(_) => {
                warn!(
                    "Timed out creating AQS strategy action stream for strategy {} after {}s",
                    auth.strategy_id, LIVE_SYNC_STREAM_TIMEOUT_SECS
                );
                None
            }
        }
    }

    async fn reconnect_live_sync(
        db: &mut Option<surrealdb::Surreal<any::Any>>,
        action_stream: &mut Option<StrategyActionStream>,
        auth: &AqsAuth,
    ) -> Result<(), BrokerError> {
        warn!(
            "Reconnecting AQE live sync to AQS for strategy {}",
            auth.strategy_id
        );
        let reconnect_result = tokio::time::timeout(
            Duration::from_secs(LIVE_SYNC_RECONNECT_TIMEOUT_SECS),
            async {
                let next_db = Self::connect_live_sync_db(auth).await?;
                let next_action_stream = Self::create_strategy_action_stream(&next_db, auth).await;
                Ok::<_, BrokerError>((next_db, next_action_stream))
            },
        )
        .await;

        match reconnect_result {
            Ok(Ok((next_db, next_action_stream))) => {
                *db = Some(next_db);
                *action_stream = next_action_stream;
                info!(
                    "AQE live sync reconnect complete for strategy {}",
                    auth.strategy_id
                );
                if action_stream.is_none() {
                    warn!(
                        "AQE live sync reconnected for strategy {} but the strategy action stream is unavailable",
                        auth.strategy_id
                    );
                }
                Ok(())
            }
            Ok(Err(error)) => {
                *db = None;
                *action_stream = None;
                Err(error)
            }
            Err(_) => {
                *db = None;
                *action_stream = None;
                Err(BrokerError::ConnectionError(format!(
                    "AQE live sync reconnect timed out for strategy {} after {}s",
                    auth.strategy_id, LIVE_SYNC_RECONNECT_TIMEOUT_SECS
                )))
            }
        }
    }

    fn enqueue_pending_aqs_sync_op(
        pending_ops: &mut VecDeque<PendingAqsSyncOp>,
        op: PendingAqsSyncOp,
    ) {
        if pending_ops.len() >= MAX_PENDING_AQS_SYNC_OPS {
            pending_ops.pop_front();
            let dropped_total = PENDING_AQS_SYNC_DROPPED_OPS.fetch_add(1, Ordering::Relaxed) + 1;
            warn!(
                "Pending AQS sync queue reached capacity ({}); dropping oldest item (dropped_total={})",
                MAX_PENDING_AQS_SYNC_OPS, dropped_total
            );
        }
        pending_ops.push_back(op);
    }

    async fn execute_pending_aqs_sync_op<C: surrealdb::Connection>(
        client: &surrealdb::Surreal<C>,
        auth: &AqsAuth,
        op: PendingAqsSyncOp,
    ) -> Result<(), surrealdb::Error> {
        match op {
            PendingAqsSyncOp::StrategyStarted { universe, account } => {
                mark_strategy_started(client, auth, &universe, account.as_ref()).await
            }
            PendingAqsSyncOp::AccountState {
                account,
                captured_at,
            } => persist_live_account_state(client, auth, &account, captured_at).await,
            PendingAqsSyncOp::StrategyEvent(event) => {
                persist_strategy_event(client, auth, event).await
            }
            PendingAqsSyncOp::LiveMetrics(metrics) => {
                persist_live_metrics(client, auth, metrics).await
            }
        }
    }

    async fn flush_pending_aqs_sync_ops<C: surrealdb::Connection>(
        client: &surrealdb::Surreal<C>,
        auth: &AqsAuth,
        pending_ops: &mut VecDeque<PendingAqsSyncOp>,
    ) -> Result<(), surrealdb::Error> {
        while let Some(op) = pending_ops.pop_front() {
            match Self::execute_pending_aqs_sync_op(client, auth, op.clone()).await {
                Ok(()) => {}
                Err(error) if Self::is_transient_surreal_error(&error) => {
                    Self::enqueue_pending_aqs_sync_op(pending_ops, op);
                    return Err(error);
                }
                Err(error) => {
                    error!(
                        "Dropping pending AQS sync operation for strategy {} after non-transient error: {}",
                        auth.strategy_id, error
                    );
                }
            }
        }

        Ok(())
    }

    async fn upsert_live_insight_with_retry<C: surrealdb::Connection>(
        client: &surrealdb::Surreal<C>,
        auth: &AqsAuth,
        snapshot: &InsightSnapshot,
    ) -> Result<(), surrealdb::Error> {
        let max_attempts = 3usize;
        let live_session_key = Self::live_session_key_for_auth(auth);
        let record_id = Self::live_insight_record_id(auth, &snapshot.insight_id);

        for attempt in 1..=max_attempts {
            let result = match tokio::time::timeout(
                Duration::from_secs(LIVE_SYNC_QUERY_TIMEOUT_SECS),
                client
                    .query(
                        "UPSERT type::record('insights', $record_id)
                         CONTENT object::extend(
                             $snapshot,
                             {
                                 strategy_id: type::record('strategy', $strategy_id),
                                 live_session_id: type::record('live_strategy_session', <uuid>$live_session_key)
                             }
                         )",
                    )
                    .bind(("record_id", record_id.clone()))
                    .bind(("snapshot", snapshot.clone()))
                    .bind(("strategy_id", auth.strategy_id.clone()))
                    .bind(("live_session_key", live_session_key.clone())),
            )
            .await
            {
                Ok(result) => result.and_then(|response| response.check()),
                Err(_) => {
                    warn!(
                        "Timed out upserting live insight for strategy {} insight {} on attempt {}/{} after {}s",
                        auth.strategy_id,
                        snapshot.insight_id,
                        attempt,
                        max_attempts,
                        LIVE_SYNC_QUERY_TIMEOUT_SECS
                    );
                    Err(surrealdb::Error::internal(
                        "live insight upsert timed out".to_string(),
                    ))
                }
            };

            match result {
                Ok(_) => return Ok(()),
                Err(error)
                    if attempt < max_attempts && Self::is_transient_surreal_error(&error) =>
                {
                    warn!(
                        "Transient live insight sync error for strategy {} insight {} on attempt {}/{}: {}",
                        auth.strategy_id, snapshot.insight_id, attempt, max_attempts, error
                    );
                    tokio::time::sleep(Duration::from_millis(150 * attempt as u64)).await;
                }
                Err(error) => return Err(error),
            }
        }

        unreachable!("live insight upsert retry loop must return before exhaustion");
    }

    async fn persist_live_metrics_if_needed<C: surrealdb::Connection>(
        &mut self,
        client: &surrealdb::Surreal<C>,
        auth: &AqsAuth,
        pending_ops: &mut VecDeque<PendingAqsSyncOp>,
    ) -> Result<(), surrealdb::Error> {
        self.refresh_live_open_position_metrics();

        if !self.live_metrics.should_persist() {
            return Ok(());
        }

        if let Some(snapshot) = self.live_metrics.snapshot() {
            let record: aqs_types::StrategyLiveMetricsRecord = snapshot.into();
            match persist_live_metrics(client, auth, record.clone()).await {
                Ok(()) => {
                    self.live_metrics.mark_persisted();
                }
                Err(error) if Self::is_transient_surreal_error(&error) => {
                    Self::enqueue_pending_aqs_sync_op(
                        pending_ops,
                        PendingAqsSyncOp::LiveMetrics(record),
                    );
                    return Err(error);
                }
                Err(error) => {
                    error!(
                        "Failed to persist live metrics for strategy {}: {}",
                        auth.strategy_id, error
                    );
                }
            }
        }

        Ok(())
    }

    async fn persist_local_live_metrics_if_needed(
        &mut self,
        artifacts: &mut LocalLiveRunArtifacts,
        force: bool,
    ) {
        if let Ok(account) = self.broker.get_account().await {
            self.live_metrics
                .update_equity(account.equity, chrono::Utc::now());
        }
        self.refresh_live_open_position_metrics();
        if let Err(error) = artifacts
            .write_metrics_if_changed(self.live_metrics.snapshot(), force)
            .await
        {
            warn!(
                "Failed to persist local live metrics into {}: {}",
                artifacts.db_path.display(),
                error
            );
        }
    }

    async fn ensure_live_provider_connections(&self) {
        if !self.broker.is_broker_connected() {
            warn!(
                "Execution broker disconnected for live strategy {}; attempting reconnect",
                self.name
            );
            match self.broker.connect_broker().await {
                Ok(true) => info!(
                    "Execution broker reconnected for live strategy {}",
                    self.name
                ),
                Ok(false) => warn!(
                    "Execution broker reconnect returned false for live strategy {}",
                    self.name
                ),
                Err(error) => warn!(
                    "Execution broker reconnect failed for live strategy {}: {}",
                    self.name, error
                ),
            }
        }

        if !self.broker.is_datafeed_connected() {
            warn!(
                "Data feed disconnected for live strategy {}; attempting reconnect",
                self.name
            );
            match self.broker.connect_datafeed().await {
                Ok(true) => info!("Data feed reconnected for live strategy {}", self.name),
                Ok(false) => warn!(
                    "Data feed reconnect returned false for live strategy {}",
                    self.name
                ),
                Err(error) => warn!(
                    "Data feed reconnect failed for live strategy {}: {}",
                    self.name, error
                ),
            }
        }
    }

    fn latest_price_for_symbol(&self, symbol: &str) -> Option<f64> {
        self.latest_quote(symbol)
            .ok()
            .and_then(|quote| quote.last.or(Some((quote.bid + quote.ask) / 2.0)))
            .filter(|price| price.is_finite())
    }

    fn insights_state_counts_json(&self) -> serde_json::Value {
        let mut counts = serde_json::Map::new();
        for (state, count) in self.insights.get_state_count() {
            counts.insert(
                format!("{:?}", state),
                serde_json::Value::from(count as u64),
            );
        }
        serde_json::Value::Object(counts)
    }

    async fn persist_live_strategy_summary<C: surrealdb::Connection>(
        &self,
        client: &surrealdb::Surreal<C>,
        auth: &AqsAuth,
    ) -> Result<(), surrealdb::Error> {
        client
            .query(
                "UPDATE type::record('strategy', $id)
                 SET status = $status,
                     is_live = true,
                     live_session_id = type::record('live_strategy_session', <uuid>$live_session_key),
                     insights_count = $insights_count,
                     last_heartbeat = time::now()",
            )
            .bind(("id", auth.strategy_id.clone()))
            .bind(("status", format!("{:?}", self.status)))
            .bind(("live_session_key", Self::live_session_key_for_auth(auth)))
            .bind(("insights_count", self.insights_state_counts_json()))
            .await
            .and_then(|response| response.check())
            .map(|_| ())
    }

    async fn reconcile_remote_active_insights<C: surrealdb::Connection>(
        &self,
        client: &surrealdb::Surreal<C>,
        auth: &AqsAuth,
        synced_insight_states: &mut HashMap<String, String>,
        pending_ops: &mut VecDeque<PendingAqsSyncOp>,
    ) -> Result<usize, surrealdb::Error> {
        let live_session_key = Self::live_session_key_for_auth(auth);
        let local_active_ids = self
            .insights
            .active_insight_ids_unsorted()
            .into_iter()
            .map(|insight_id| insight_id.to_string())
            .collect::<HashSet<_>>();

        let mut result: IndexedResults = client
            .query(
                "SELECT insight_id, symbol, state, side
                 FROM insights
                 WHERE strategy_id = type::record('strategy', $strategy_id)
                   AND live_session_id = type::record('live_strategy_session', <uuid>$live_session_key)
                   AND state IN ['New', 'Executed', 'Filled']",
            )
            .bind(("strategy_id", auth.strategy_id.clone()))
            .bind(("live_session_key", live_session_key.clone()))
            .await?;
        let rows: Vec<serde_json::Value> = result.take(0)?;
        let mut reconciled = 0usize;

        for row in rows {
            let Some(insight_id) = row
                .get("insight_id")
                .and_then(|value| value.as_str())
                .map(ToString::to_string)
            else {
                continue;
            };

            if local_active_ids.contains(&insight_id) {
                continue;
            }

            let symbol = row
                .get("symbol")
                .and_then(|value| value.as_str())
                .unwrap_or("-");
            let previous_state = row
                .get("state")
                .and_then(|value| value.as_str())
                .unwrap_or("-");
            let side = row
                .get("side")
                .and_then(|value| value.as_str())
                .unwrap_or("-");
            let now = chrono::Utc::now();
            let record_id = Self::live_insight_record_id(auth, &insight_id);
            let reason = format!(
                "AQS reconciliation marked stale active insight as Cancelled; AQE no longer has active insight {}",
                insight_id
            );

            let update_result = client
                .query(
                    "UPDATE type::record('insights', $record_id)
                     SET state = 'Cancelled',
                         cancelling = false,
                         closing = false,
                         first_on_fill = false,
                         updated_at = <datetime>$now,
                         closed_at = <datetime>$now",
                )
                .bind(("record_id", record_id))
                .bind(("now", now))
                .await
                .and_then(|response| response.check());

            if let Err(error) = update_result {
                if Self::is_transient_surreal_error(&error) {
                    return Err(error);
                }
                error!(
                    "Failed to reconcile stale AQS insight {} for strategy {}: {}",
                    insight_id, auth.strategy_id, error
                );
                continue;
            }

            synced_insight_states.remove(&insight_id);
            reconciled += 1;

            let event = StrategyEventRecord {
                event_type: "insight_reconcile".into(),
                level: "warn".into(),
                title: "Stale insight cancelled".into(),
                message: reason.clone(),
                payload: Some(serde_json::json!({
                    "insight_id": insight_id,
                    "symbol": symbol,
                    "side": side,
                    "previous_state": previous_state,
                    "state": "Cancelled",
                    "reason": reason,
                })),
                created_at: Some(now),
            };

            if let Err(error) = persist_strategy_event(client, auth, event.clone()).await {
                if Self::is_transient_surreal_error(&error) {
                    Self::enqueue_pending_aqs_sync_op(
                        pending_ops,
                        PendingAqsSyncOp::StrategyEvent(event),
                    );
                    return Err(error);
                }
                error!(
                    "Failed to persist stale insight reconciliation event for strategy {} insight {}: {}",
                    auth.strategy_id, insight_id, error
                );
            }
        }

        if reconciled > 0 {
            warn!(
                "AQS reconciliation cancelled {} stale active insight(s) for strategy {}",
                reconciled, auth.strategy_id
            );
        }

        Ok(reconciled)
    }

    async fn persist_unsynced_insight_transitions<C: surrealdb::Connection>(
        client: &surrealdb::Surreal<C>,
        auth: &AqsAuth,
        snapshot: &InsightSnapshot,
        synced_history_offsets: &mut HashMap<String, usize>,
        pending_ops: &mut VecDeque<PendingAqsSyncOp>,
    ) -> Result<(), surrealdb::Error> {
        let start_index = synced_history_offsets
            .get(&snapshot.insight_id)
            .copied()
            .unwrap_or(0);
        let mut next_offset = start_index;

        for (index, history) in snapshot.state_history.iter().enumerate().skip(start_index) {
            next_offset = index + 1;
            let is_state_transition = history
                .message
                .as_deref()
                .is_some_and(|message| message.starts_with("State changed from "));
            if !is_state_transition {
                continue;
            }

            let event = StrategyEventRecord {
                event_type: "insight_state".into(),
                level: "info".into(),
                title: format!("Insight {}", history.state),
                message: history
                    .message
                    .clone()
                    .unwrap_or_else(|| format!("{} changed to {}", snapshot.symbol, history.state)),
                payload: Some(serde_json::json!({
                    "insight_id": snapshot.insight_id,
                    "symbol": snapshot.symbol,
                    "state": history.state,
                    "side": snapshot.side,
                    "history_index": index,
                    "history": history,
                })),
                created_at: Some(history.at),
            };

            if let Err(error) = persist_strategy_event(client, auth, event.clone()).await {
                if Self::is_transient_surreal_error(&error) {
                    Self::enqueue_pending_aqs_sync_op(
                        pending_ops,
                        PendingAqsSyncOp::StrategyEvent(event),
                    );
                    return Err(error);
                }
                error!(
                    "Failed to persist live insight transition event for strategy {} insight {} history_index={}: {}",
                    auth.strategy_id, snapshot.insight_id, index, error
                );
            }
        }

        synced_history_offsets.insert(snapshot.insight_id.clone(), next_offset);
        Ok(())
    }

    fn refresh_live_open_position_metrics(&mut self) {
        let mut open_positions_count = 0usize;
        let mut open_insights_count = 0usize;
        let mut open_positions_unrealized_pnl = 0.0f64;
        let mut open_positions_profitable_count = 0usize;
        let mut open_positions_losing_count = 0usize;
        let mut symbols = self
            .universe
            .values()
            .map(|asset| asset.symbol.clone())
            .collect::<Vec<_>>();
        let mut latest_prices = HashMap::<String, f64>::new();

        for insight_id in self.insights.active_insight_ids_unsorted() {
            let Some(insight) = self.insights.get(&insight_id) else {
                continue;
            };
            if insight.state.is_inactive() {
                continue;
            }
            open_insights_count += 1;

            if insight.state != InsightState::Filled {
                continue;
            }

            let remaining_qty = insight.remaining_quantity();
            if remaining_qty <= f64::EPSILON {
                continue;
            }

            let Some(entry_price) = insight
                .filled_price
                .or(insight.limit_price)
                .or(insight.stop_price)
            else {
                continue;
            };
            let current_price = if let Some(price) = latest_prices.get(&insight.symbol).copied() {
                price
            } else if let Some(price) = self.latest_price_for_symbol(&insight.symbol) {
                latest_prices.insert(insight.symbol.clone(), price);
                price
            } else {
                continue;
            };

            let pnl = match insight.side {
                crate::core::broker::types::OrderSide::Buy => {
                    (current_price - entry_price) * remaining_qty
                }
                crate::core::broker::types::OrderSide::Sell => {
                    (entry_price - current_price) * remaining_qty
                }
            };

            open_positions_count += 1;
            open_positions_unrealized_pnl += pnl;
            if pnl > 0.0 {
                open_positions_profitable_count += 1;
            } else if pnl < 0.0 {
                open_positions_losing_count += 1;
            }
            if !symbols.iter().any(|value| value == &insight.symbol) {
                symbols.push(insight.symbol.clone());
            }
        }

        symbols.sort();
        symbols.dedup();
        self.live_metrics.update_open_position_metrics(
            open_positions_count,
            open_insights_count,
            open_positions_unrealized_pnl,
            open_positions_profitable_count,
            open_positions_losing_count,
            symbols,
            chrono::Utc::now(),
        );
    }

    async fn sync_live_insights_to_aqs<C: surrealdb::Connection>(
        &mut self,
        client: &surrealdb::Surreal<C>,
        auth: &AqsAuth,
        synced_insight_states: &mut HashMap<String, String>,
        synced_insight_history_offsets: &mut HashMap<String, usize>,
        pending_ops: &mut VecDeque<PendingAqsSyncOp>,
        include_full_reconcile: bool,
    ) -> Result<bool, surrealdb::Error> {
        debug!(
            "Syncing live insights to AQS for strategy {}: {} in-memory insights",
            auth.strategy_id,
            self.insights.len()
        );
        let mut persist_metrics_after_sync = false;
        let mut prune_after_sync = Vec::new();
        let insight_ids = self
            .insights
            .insight_ids_for_live_sync(include_full_reconcile);
        let has_sync_work =
            include_full_reconcile || !insight_ids.is_empty() || !pending_ops.is_empty();
        let metrics_changed = self.live_metrics.should_persist();

        if !has_sync_work && !metrics_changed {
            debug!(
                "Skipping AQS live sync for strategy {} because there are no dirty insights, queued ops, or changed metrics",
                auth.strategy_id
            );
            return Ok(false);
        }
        if !has_sync_work && metrics_changed {
            return Ok(true);
        }

        if has_sync_work {
            if let Err(error) = self.persist_live_strategy_summary(client, auth).await {
                if Self::is_transient_surreal_error(&error) {
                    return Err(error);
                }
                error!(
                    "Failed to update live strategy summary for {}: {}",
                    auth.strategy_id, error
                );
            }
        }

        for insight_id in insight_ids {
            let Some((snapshot, is_terminal, current_state, snapshot_value)) =
                self.insights.get(&insight_id).map(|insight| {
                    let snapshot = InsightSnapshot::from_insight(&insight, &auth.strategy_id);
                    let is_terminal = insight.state.is_inactive();
                    let current_state = snapshot.state.clone();
                    let snapshot_value =
                        serde_json::to_value(&snapshot).unwrap_or(serde_json::Value::Null);
                    (snapshot, is_terminal, current_state, snapshot_value)
                })
            else {
                continue;
            };
            let upsert_result = Self::upsert_live_insight_with_retry(client, auth, &snapshot).await;

            if let Err(error) = upsert_result {
                if Self::is_transient_surreal_error(&error) {
                    return Err(error);
                }
                error!(
                    "Failed to upsert live insight {} for strategy {}: {}\nPayload: {}",
                    snapshot.insight_id,
                    auth.strategy_id,
                    error,
                    serde_json::to_string_pretty(&snapshot_value)
                        .unwrap_or_else(|_| "<failed to serialize snapshot payload>".to_string())
                );
                continue;
            }

            let previous_state = synced_insight_states.get(&snapshot.insight_id).cloned();
            Self::persist_unsynced_insight_transitions(
                client,
                auth,
                &snapshot,
                synced_insight_history_offsets,
                pending_ops,
            )
            .await?;

            if previous_state.as_deref() != Some(current_state.as_str()) {
                persist_metrics_after_sync = true;
                info!(
                    "Live insight synced: strategy={} insight={} symbol={} state={}",
                    auth.strategy_id, snapshot.insight_id, snapshot.symbol, current_state
                );
                if current_state == "Closed" {
                    self.live_metrics.record_closed_insight(&snapshot);
                }
                synced_insight_states.insert(snapshot.insight_id.clone(), current_state.clone());
            } else {
                synced_insight_states.insert(snapshot.insight_id.clone(), current_state.clone());
            }

            self.insights.remove_dirty(&insight_id);
            if is_terminal {
                prune_after_sync.push(insight_id);
            }
        }

        for insight_id in prune_after_sync {
            self.insights.prune_terminal_insight(&insight_id);
            synced_insight_states.remove(&insight_id.to_string());
            synced_insight_history_offsets.remove(&insight_id.to_string());
        }

        if include_full_reconcile {
            let reconciled = self
                .reconcile_remote_active_insights(client, auth, synced_insight_states, pending_ops)
                .await?;
            if reconciled > 0 {
                persist_metrics_after_sync = true;
            }
        }

        if has_sync_work || persist_metrics_after_sync {
            if let Err(error) = self.persist_live_strategy_summary(client, auth).await {
                if Self::is_transient_surreal_error(&error) {
                    return Err(error);
                }
                error!(
                    "Failed to update live strategy summary after reconciliation for {}: {}",
                    auth.strategy_id, error
                );
            }
        }

        Ok(persist_metrics_after_sync)
    }
}

// ─────────────────────── Live Runner ───────────────────────

impl<S, E, D> StrategyState<S, E, D>
where
    S: Strategy + Send + Sync + 'static,
    E: Broker + OrderManagementProvider + Send + Sync + 'static,
    D: DataFeed + DataProvider + Send + Sync + 'static,
{
    /// Run the strategy in live execution mode.
    ///
    /// This will register callbacks for trade events & bar streams,
    /// then loop via `tokio::select!` block listening on channels to drive the pipeline.
    pub async fn run_live(&mut self, auth: Option<AqsAuth>) -> Result<(), BrokerError> {
        let auth = auth.or_else(|| self.default_live_auth.clone());
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
        let mut db = if let Some(ref a) = auth {
            match Self::connect_live_sync_db(a).await {
                Ok(db) => Some(db),
                Err(error) => {
                    error!(
                        "Live execution failed while connecting AQS Cloud sync: {}",
                        error
                    );
                    self.runtime_telemetry.stop_tui();
                    return Err(error);
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
            self.runtime_telemetry.stop_tui();
            return Err(error);
        }

        // 1. Connect broker
        if let Err(error) = self.broker.connect().await {
            error!("Live execution failed while connecting broker: {}", error);
            self.runtime_telemetry.stop_tui();
            return Err(error);
        }

        let mut action_stream = if let (Some(client), Some(a)) = (&db, &auth) {
            Self::create_strategy_action_stream(client, a).await
        } else {
            None
        };

        let mut stop_action_id: Option<String> = None;
        let mut synced_insight_states: HashMap<String, String> = HashMap::new();
        let mut synced_insight_history_offsets: HashMap<String, usize> = HashMap::new();

        let mut pending_sync_ops = VecDeque::new();

        if let Err(error) = self
            .start(
                &mut strategy,
                db.as_ref(),
                auth.as_ref(),
                &mut pending_sync_ops,
            )
            .await
        {
            error!("Live execution failed during strategy startup: {}", error);
            self.runtime_telemetry.stop_tui();
            return Err(error);
        }
        let mut local_live_artifacts = if auth.is_none() {
            let mut artifacts =
                match LocalLiveRunArtifacts::start(&self.name, self.strategy_id, self.mode.clone())
                    .await
                {
                    Ok(artifacts) => artifacts,
                    Err(error) => {
                        error!(
                            "Live execution failed while creating local live run database: {}",
                            error
                        );
                        self.runtime_telemetry.stop_tui();
                        return Err(error);
                    }
                };
            info!(
                "AQS Cloud auth not provided; writing local live run database to {}",
                artifacts.db_path.display()
            );
            self.persist_local_live_metrics_if_needed(&mut artifacts, true)
                .await;
            Some(artifacts)
        } else {
            None
        };

        // Channels for incoming events
        let (trade_tx, mut trade_rx) = tokio::sync::mpsc::channel(100);
        let (bar_tx, mut bar_rx) = tokio::sync::mpsc::channel::<MarketDataEvent>(100);

        // 5. Subscribe to trade event stream
        let trade_tx_clone = trade_tx.clone();
        let trade_callback = Arc::new(move |event| {
            let _ = trade_tx_clone.try_send(event);
        });
        if let Err(error) = self.broker.subscribe_to_trade_stream(trade_callback).await {
            error!(
                "Live execution failed while subscribing to trade stream: {}",
                error
            );
            self.runtime_telemetry.stop_tui();
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
            let streams_by_symbol = streams_by_symbol.clone();
            let bar_callback = Arc::new(move |bar: crate::core::broker::types::Bar| {
                let Some(stream) = streams_by_symbol.get(&bar.symbol) else {
                    return;
                };
                let event = MarketDataEvent {
                    context: stream.context_at(bar.timestamp),
                    bar,
                };
                let _ = bar_tx_clone.try_send(event);
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
                self.runtime_telemetry.stop_tui();
                return Err(error);
            }
        }

        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());

        info!("Started live trading mode for strategy: {}", self.name);

        self.status = StrategyStatus::Running;
        let mut aqs_sync_status = if auth.is_some() {
            "connected".to_string()
        } else {
            "not configured".to_string()
        };

        if let (Some(client), Some(a)) = (&db, &auth) {
            let _ = persist_strategy_event(
                client,
                a,
                StrategyEventRecord {
                    event_type: "lifecycle".into(),
                    level: "info".into(),
                    title: "Strategy started".into(),
                    message: format!("Strategy '{}' entered live mode", self.name),
                    payload: Some(serde_json::json!({
                        "strategy_name": self.name,
                        "node_id": a.node_id,
                    })),
                    created_at: Some(chrono::Utc::now()),
                },
            )
            .await;
        }
        self.publish_runtime_event("info", "Live strategy started", aqs_sync_status.clone());

        // 7. Main Event Loop
        // Pipeline loop interval
        let mut pipeline_interval = tokio::time::interval(std::time::Duration::from_secs(5));
        let mut reconcile_interval =
            tokio::time::interval(std::time::Duration::from_secs(LIVE_SYNC_RECONCILE_SECS));
        let mut local_metrics_interval = tokio::time::interval(std::time::Duration::from_secs(
            LOCAL_LIVE_METRICS_WRITE_SECS,
        ));
        let mut force_full_reconcile = true;
        let mut live_sync_failure: Option<BrokerError> = None;
        let mut last_runtime_publish = std::time::Instant::now();

        macro_rules! reconnect_live_sync_or_stop {
            ($auth:expr) => {{
                aqs_sync_status = "reconnecting".to_string();
                self.publish_runtime_snapshot(aqs_sync_status.clone(), None);
                match Self::reconnect_live_sync(&mut db, &mut action_stream, $auth).await {
                    Ok(()) => {
                        aqs_sync_status = "connected".to_string();
                        self.publish_runtime_event(
                            "info",
                            "AQS live sync reconnected",
                            aqs_sync_status.clone(),
                        );
                    }
                    Err(error) => {
                        let message = format!(
                            "AQS live sync unavailable for strategy {} after {} attempt(s); stopping live run: {}",
                            $auth.strategy_id,
                            LIVE_SYNC_CONNECT_MAX_ATTEMPTS,
                            error
                        );
                        error!("{}", message);
                        live_sync_failure = Some(BrokerError::ConnectionError(message.clone()));
                        aqs_sync_status = "offline".to_string();
                        self.publish_runtime_event("error", message, aqs_sync_status.clone());
                        self.status = StrategyStatus::Stopping;
                        self.shutdown();
                        continue;
                    }
                }
            }};
        }

        loop {
            tokio::select! {
                // Handle broker trade events
                Some((order, event)) = trade_rx.recv() => {
                    debug!(
                        "Live loop received trade event {:?} for order {}",
                        event,
                        order.order_id
                    );
                    // Process trade events (we let `on_trade_update()` drain all pending broker state)
                    self.on_trade_update();
                    self.publish_runtime_event(
                        "info",
                        format!("Trade event {:?} for order {}", event, order.order_id),
                        aqs_sync_status.clone(),
                    );
                    if let Some(artifacts) = local_live_artifacts.as_ref() {
                        if let Err(error) = artifacts
                            .append_event(
                                "trade_event",
                                "info",
                                "Broker trade event",
                                &format!("Received {:?} for order {}", event, order.order_id),
                                Some(serde_json::json!({
                                    "order_id": order.order_id.clone(),
                                    "symbol": order.asset.symbol.clone(),
                                    "event": format!("{:?}", event),
                                })),
                            )
                            .await
                        {
                            warn!(
                                "Failed to persist local live trade event into {}: {}",
                                artifacts.db_path.display(),
                                error
                            );
                        }
                    }
                    if let Some(a) = auth.as_ref() {
                        if db.is_none() {
                            reconnect_live_sync_or_stop!(a);
                            force_full_reconcile = true;
                        }
                        if let Some(client) = db.as_ref() {
                            if let Err(error) = Self::flush_pending_aqs_sync_ops(client, a, &mut pending_sync_ops).await {
                                error!(
                                    "Live sync could not flush pending AQS ops for strategy {} after trade event: {}",
                                    a.strategy_id, error
                                );
                                reconnect_live_sync_or_stop!(a);
                                continue;
                            }
                            let sync_result = self
                                .sync_live_insights_to_aqs(client, a, &mut synced_insight_states, &mut synced_insight_history_offsets, &mut pending_sync_ops, force_full_reconcile)
                                .await;
                            match sync_result {
                                Ok(persist_metrics_after_sync) => {
                                    force_full_reconcile = false;
                                    if persist_metrics_after_sync {
                                        if let Err(error) = self.persist_live_metrics_if_needed(client, a, &mut pending_sync_ops).await {
                                            error!(
                                                "Live sync lost AQS connection for strategy {} while persisting live metrics: {}",
                                                a.strategy_id, error
                                            );
                                            reconnect_live_sync_or_stop!(a);
                                            continue;
                                        }
                                    }
                                    if let Ok(account) = self.broker.get_account().await {
                                        let captured_at = chrono::Utc::now();
                                        self.live_metrics
                                            .update_equity(account.equity, captured_at);
                                        if let Err(error) = persist_live_account_state(client, a, &account, captured_at).await {
                                            if Self::is_transient_surreal_error(&error) {
                                                Self::enqueue_pending_aqs_sync_op(
                                                    &mut pending_sync_ops,
                                                    PendingAqsSyncOp::AccountState {
                                                        account: account.clone(),
                                                        captured_at,
                                                    },
                                                );
                                                error!(
                                                    "Live sync lost AQS connection for strategy {} while persisting account state: {}",
                                                    a.strategy_id, error
                                                );
                                                reconnect_live_sync_or_stop!(a);
                                                continue;
                                            }
                                            error!(
                                                "Failed to persist live account state for strategy {}: {}",
                                                a.strategy_id, error
                                            );
                                        }
                                        if let Err(error) = self.persist_live_metrics_if_needed(client, a, &mut pending_sync_ops).await {
                                            error!(
                                                "Live sync lost AQS connection for strategy {} while persisting live metrics: {}",
                                                a.strategy_id, error
                                            );
                                            reconnect_live_sync_or_stop!(a);
                                            continue;
                                        }
                                    }
                                    let trade_event_record = StrategyEventRecord {
                                        event_type: "trade_event".into(),
                                        level: "info".into(),
                                        title: "Broker trade event".into(),
                                        message: format!("Received {:?} for order {}", event, order.order_id),
                                        payload: Some(serde_json::json!({
                                            "order_id": order.order_id,
                                            "symbol": order.asset.symbol,
                                            "event": format!("{:?}", event),
                                        })),
                                        created_at: Some(chrono::Utc::now()),
                                    };
                                    if let Err(error) = persist_strategy_event(
                                        client,
                                        a,
                                        trade_event_record.clone(),
                                    ).await {
                                        if Self::is_transient_surreal_error(&error) {
                                            Self::enqueue_pending_aqs_sync_op(
                                                &mut pending_sync_ops,
                                                PendingAqsSyncOp::StrategyEvent(trade_event_record),
                                            );
                                            error!(
                                                "Live sync lost AQS connection for strategy {} while persisting trade event: {}",
                                                a.strategy_id, error
                                            );
                                            reconnect_live_sync_or_stop!(a);
                                            continue;
                                        }
                                        error!(
                                            "Failed to persist trade event for strategy {}: {}",
                                            a.strategy_id, error
                                        );
                                    }
                                }
                                Err(error) => {
                                    error!(
                                        "Live sync lost AQS connection for strategy {} after trade event: {}",
                                        a.strategy_id, error
                                    );
                                    reconnect_live_sync_or_stop!(a);
                                    force_full_reconcile = true;
                                }
                            }
                        }
                    }
                    if last_runtime_publish.elapsed() >= std::time::Duration::from_millis(500) {
                        self.publish_runtime_snapshot(aqs_sync_status.clone(), None);
                        last_runtime_publish = std::time::Instant::now();
                    }
                }

                // Handle incoming market data bars
                Some(event) = bar_rx.recv() => {
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
                        let pruned_ids = self.insights.prune_terminal_insights_without_aqs();
                        for insight_id in pruned_ids {
                            synced_insight_states.remove(&insight_id.to_string());
                            synced_insight_history_offsets.remove(&insight_id.to_string());
                        }
                    }
                    if let Some(a) = auth.as_ref() {
                        if db.is_none() {
                            reconnect_live_sync_or_stop!(a);
                            force_full_reconcile = true;
                        }
                        if let Some(client) = db.as_ref() {
                            if let Err(error) = Self::flush_pending_aqs_sync_ops(client, a, &mut pending_sync_ops).await {
                                error!(
                                    "Live sync could not flush pending AQS ops for strategy {} after bar processing: {}",
                                    a.strategy_id, error
                                );
                                reconnect_live_sync_or_stop!(a);
                                continue;
                            }
                            match self
                                .sync_live_insights_to_aqs(client, a, &mut synced_insight_states, &mut synced_insight_history_offsets, &mut pending_sync_ops, force_full_reconcile)
                                .await
                            {
                                Ok(persist_metrics_after_sync) => {
                                    force_full_reconcile = false;
                                    if persist_metrics_after_sync {
                                        if let Err(error) = self.persist_live_metrics_if_needed(client, a, &mut pending_sync_ops).await {
                                            error!(
                                                "Live sync lost AQS connection for strategy {} while persisting live metrics: {}",
                                                a.strategy_id, error
                                            );
                                            reconnect_live_sync_or_stop!(a);
                                            continue;
                                        }
                                    }
                                }
                                Err(error) => {
                                    error!(
                                        "Live sync lost AQS connection for strategy {} after bar processing: {}",
                                        a.strategy_id, error
                                    );
                                    reconnect_live_sync_or_stop!(a);
                                    force_full_reconcile = true;
                                }
                            }
                        }
                    }
                    if last_runtime_publish.elapsed() >= std::time::Duration::from_millis(500) {
                        self.publish_runtime_snapshot(aqs_sync_status.clone(), None);
                        last_runtime_publish = std::time::Instant::now();
                    }
                }

                // Periodically run insight pipeline
                _ = pipeline_interval.tick() => {
                    if self.runtime_telemetry.shutdown_requested()
                        && !matches!(self.status, StrategyStatus::Stopping)
                    {
                        warn!("Live stop requested from AQE TUI");
                        self.publish_runtime_event("warn", "Live stop requested from TUI", aqs_sync_status.clone());
                        self.status = StrategyStatus::Stopping;
                        self.shutdown();
                    }
                    self.ensure_live_provider_connections().await;
                    debug!("Live loop calling run_insight_pipeline");
                    self.run_insight_pipeline();
                    debug!("Live loop completed run_insight_pipeline");
                    if auth.is_none() {
                        let pruned_ids = self.insights.prune_terminal_insights_without_aqs();
                        for insight_id in pruned_ids {
                            synced_insight_states.remove(&insight_id.to_string());
                            synced_insight_history_offsets.remove(&insight_id.to_string());
                        }
                    }

                    // Optional Push to SurrealDB
                    if let Some(a) = auth.as_ref() {
                        if db.is_none() {
                            reconnect_live_sync_or_stop!(a);
                            force_full_reconcile = true;
                        }
                        if let Some(client) = db.as_ref() {
                            if let Err(error) = Self::flush_pending_aqs_sync_ops(client, a, &mut pending_sync_ops).await {
                                error!(
                                    "Live sync could not flush pending AQS ops for strategy {} during pipeline sync: {}",
                                    a.strategy_id, error
                                );
                                reconnect_live_sync_or_stop!(a);
                                continue;
                            }
                            match self
                                .sync_live_insights_to_aqs(client, a, &mut synced_insight_states, &mut synced_insight_history_offsets, &mut pending_sync_ops, force_full_reconcile)
                                .await
                            {
                                Ok(persist_metrics_after_sync) => {
                                    force_full_reconcile = false;
                                    if persist_metrics_after_sync {
                                        if let Err(error) = self.persist_live_metrics_if_needed(client, a, &mut pending_sync_ops).await {
                                            error!(
                                                "Live sync lost AQS connection for strategy {} while persisting live metrics: {}",
                                                a.strategy_id, error
                                            );
                                            reconnect_live_sync_or_stop!(a);
                                            continue;
                                        }
                                    }

                                    if let Ok(account) = self.broker.get_account().await {
                                        let captured_at = chrono::Utc::now();
                                        self.live_metrics
                                            .update_equity(account.equity, captured_at);
                                        if let Err(error) = persist_live_account_state(client, a, &account, captured_at).await {
                                            if Self::is_transient_surreal_error(&error) {
                                                Self::enqueue_pending_aqs_sync_op(
                                                    &mut pending_sync_ops,
                                                    PendingAqsSyncOp::AccountState {
                                                        account: account.clone(),
                                                        captured_at,
                                                    },
                                                );
                                                error!(
                                                    "Live sync lost AQS connection for strategy {} while persisting account state: {}",
                                                    a.strategy_id, error
                                                );
                                                reconnect_live_sync_or_stop!(a);
                                                continue;
                                            }
                                            error!(
                                                "Failed to persist live account state for strategy {}: {}",
                                                a.strategy_id, error
                                            );
                                        }
                                        if let Err(error) = self.persist_live_metrics_if_needed(client, a, &mut pending_sync_ops).await {
                                            error!(
                                                "Live sync lost AQS connection for strategy {} while persisting live metrics: {}",
                                                a.strategy_id, error
                                            );
                                            reconnect_live_sync_or_stop!(a);
                                            continue;
                                        }
                                    }
                                }
                                Err(error) => {
                                    error!(
                                        "Live sync lost AQS connection for strategy {} during pipeline sync: {}",
                                        a.strategy_id, error
                                    );
                                    reconnect_live_sync_or_stop!(a);
                                    force_full_reconcile = true;
                                }
                            }
                        }
                    }
                    self.publish_runtime_snapshot(aqs_sync_status.clone(), None);
                }

                _ = local_metrics_interval.tick(), if local_live_artifacts.is_some() => {
                    if let Some(artifacts) = local_live_artifacts.as_mut() {
                        self.persist_local_live_metrics_if_needed(artifacts, false).await;
                    }
                    self.publish_runtime_snapshot(aqs_sync_status.clone(), None);
                }

                _ = reconcile_interval.tick() => {
                    if let Some(a) = auth.as_ref() {
                        if db.is_none() {
                            reconnect_live_sync_or_stop!(a);
                            force_full_reconcile = true;
                        }
                        if let Some(client) = db.as_ref() {
                            let has_reconcile_work = force_full_reconcile
                                || !pending_sync_ops.is_empty()
                                || self.insights.has_dirty_insights();
                            if !has_reconcile_work {
                                debug!(
                                    "Skipping periodic AQS live reconcile for strategy {} because there is no local sync work",
                                    a.strategy_id
                                );
                                continue;
                            }
                            if let Err(error) = Self::flush_pending_aqs_sync_ops(client, a, &mut pending_sync_ops).await {
                                error!(
                                    "Live sync could not flush pending AQS ops for strategy {} during reconcile: {}",
                                    a.strategy_id, error
                                );
                                reconnect_live_sync_or_stop!(a);
                                force_full_reconcile = true;
                                continue;
                            }
                            match self
                                .sync_live_insights_to_aqs(client, a, &mut synced_insight_states, &mut synced_insight_history_offsets, &mut pending_sync_ops, force_full_reconcile)
                                .await
                            {
                                Ok(_) => {
                                    force_full_reconcile = false;
                                }
                                Err(error) => {
                                    error!(
                                        "Live sync lost AQS connection for strategy {} during periodic reconcile: {}",
                                        a.strategy_id, error
                                    );
                                    reconnect_live_sync_or_stop!(a);
                                    force_full_reconcile = true;
                                }
                            }
                        }
                    }
                }

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
                            let action_name = action_json.get("action").and_then(|value| value.as_str());
                            let action_status = action_json.get("status").and_then(|value| value.as_str());
                            let action_id = action_json.get("id").and_then(action_id_from_value);

                            if action_status == Some("pending") && action_name == Some("stop") {
                                if let (Some(client), Some(a), Some(action_id)) = (&db, &auth, action_id) {
                                    update_strategy_action_status(
                                        client,
                                        &action_id,
                                        "acknowledged",
                                        Some("Stop requested by AQS".to_string()),
                                        None,
                                        "acknowledged_at",
                                    )
                                    .await;
                                    let _ = persist_strategy_event(
                                        client,
                                        a,
                                        StrategyEventRecord {
                                            event_type: "action".into(),
                                            level: "warn".into(),
                                            title: "Stop requested".into(),
                                            message: "AQE received a remote stop action".into(),
                                            payload: Some(action_json.clone()),
                                            created_at: Some(chrono::Utc::now()),
                                        },
                                    )
                                    .await;
                                    stop_action_id = Some(action_id.clone());
                                }

                                self.status = StrategyStatus::Stopping;
                                warn!("Received remote stop action from AQS");
                                self.shutdown();
                            }
                        }
                        Some(Err(error)) => {
                            error!("Strategy action live stream error: {}", error);
                            if let Some(a) = auth.as_ref() {
                                reconnect_live_sync_or_stop!(a);
                            }
                        }
                        None => {}
                    }
                }

                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Shutdown signal received, exiting live loop");
                        break;
                    }
                }

                _ = &mut ctrl_c => {
                    warn!("Process interrupt received, stopping live strategy");
                    self.status = StrategyStatus::Stopping;
                    self.shutdown();
                    break;
                }

                else => {
                    break;
                }
            }
        }

        // Teardown
        debug!("Invoking strategy on_teardown");
        self.begin_teardown();
        self.run_on_teardown_logic(LifecycleTiming::BeforeGenerated);
        strategy.on_teardown(self);
        self.run_on_teardown_logic(LifecycleTiming::AfterGenerated);

        // Clean up subscriptions
        let _ = self.broker.unsubscribe_from_trade_stream().await;
        let _ = self
            .broker
            .unsubscribe_from_data_stream(self.universe.keys().cloned().collect())
            .await;
        let _ = self.broker.disconnect().await;

        self.strategy = Some(strategy);
        self.history_seed_anchor = None;
        self.status = StrategyStatus::Stopped;

        if let (Some(client), Some(a)) = (&db, &auth) {
            let _: Result<IndexedResults, surrealdb::Error> = client
                .query("UPDATE type::record('strategy', $id) SET status = 'Stopped', is_live = false, last_heartbeat = time::now()")
                .bind(("id", a.strategy_id.clone()))
                .await;
            let _ = client
                .query(
                    "UPDATE type::record('live_strategy_session', <uuid>$live_session_key)
                     SET status = 'completed',
                         last_used_at = time::now()",
                )
                .bind(("live_session_key", Self::live_session_key_for_auth(a)))
                .await
                .and_then(|response| response.check());

            if let Ok(account) = self.broker.get_account().await {
                let captured_at = chrono::Utc::now();
                self.live_metrics.update_equity(account.equity, captured_at);
                let _ = persist_live_account_state(client, a, &account, captured_at).await;
            }

            self.live_metrics.finish(chrono::Utc::now());
            let _ = self
                .persist_live_metrics_if_needed(client, a, &mut pending_sync_ops)
                .await;

            if let Some(action_id) = stop_action_id {
                update_strategy_action_status(
                    client,
                    &action_id,
                    "completed",
                    Some("Strategy stopped gracefully".to_string()),
                    None,
                    "completed_at",
                )
                .await;
            }

            let _ = persist_strategy_event(
                client,
                a,
                StrategyEventRecord {
                    event_type: "lifecycle".into(),
                    level: "info".into(),
                    title: "Strategy stopped".into(),
                    message: format!("Strategy '{}' exited live mode", self.name),
                    payload: None,
                    created_at: Some(chrono::Utc::now()),
                },
            )
            .await;
        }

        if let Some(artifacts) = local_live_artifacts.as_mut() {
            if let Ok(account) = self.broker.get_account().await {
                self.live_metrics
                    .update_equity(account.equity, chrono::Utc::now());
            }
            self.refresh_live_open_position_metrics();
            self.live_metrics.finish(chrono::Utc::now());
            let (status, message) = if live_sync_failure.is_some() {
                (
                    "failed",
                    "Strategy exited live mode after AQS live sync failure",
                )
            } else {
                ("completed", "Strategy exited live mode")
            };
            if let Err(error) = artifacts
                .finish(status, message, self.live_metrics.snapshot())
                .await
            {
                warn!(
                    "Failed to finalise local live run database {}: {}",
                    artifacts.db_path.display(),
                    error
                );
            }
        }

        if live_sync_failure.is_some() {
            aqs_sync_status = "offline".to_string();
            self.publish_runtime_event(
                "error",
                "Live strategy stopped after sync failure",
                aqs_sync_status,
            );
        } else {
            aqs_sync_status = if auth.is_some() {
                "completed".to_string()
            } else {
                "not configured".to_string()
            };
            self.publish_runtime_event("info", "Live strategy stopped", aqs_sync_status);
        }
        self.runtime_telemetry.wait_for_tui_close();

        if let Some(error) = live_sync_failure {
            return Err(error);
        }

        Ok(())
    }
}

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
        pending_ops: &mut VecDeque<PendingAqsSyncOp>,
    ) -> Result<(), BrokerError> {
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

        self.history_seed_anchor = Some(self.broker.get_current_time());
        self.start_alpha_models();
        self.load_init(strategy);
        self.init_alpha_models();

        // Initial sync to SurrealDB after the universe is loaded and strategy has started, so the
        // dashboard has the initial state to work with (universe, account, etc.)
        let mut initial_universe_assets: Vec<StrategyUniverseAssetRecord> = self
            .universe
            .values()
            .map(StrategyUniverseAssetRecord::from)
            .collect();
        initial_universe_assets.sort_by(|left, right| left.symbol.cmp(&right.symbol));
        initial_universe_assets.dedup_by(|left, right| left.symbol == right.symbol);
        let initial_universe_symbols: Vec<String> = initial_universe_assets
            .iter()
            .map(|asset| asset.symbol.clone())
            .collect();
        let initial_account = self.broker.get_account().await.ok();
        let executed_at = chrono::Utc::now();
        let starting_cash = initial_account
            .as_ref()
            .map(|account| account.cash)
            .unwrap_or_default();
        let current_equity = initial_account
            .as_ref()
            .map(|account| account.equity)
            .unwrap_or(starting_cash);
        self.live_metrics.initialize(
            starting_cash,
            current_equity,
            executed_at,
            initial_universe_symbols,
        );

        if let (Some(client), Some(auth)) = (client, auth) {
            if let Err(error) = mark_strategy_started(
                client,
                auth,
                &initial_universe_assets,
                initial_account.as_ref(),
            )
            .await
            {
                if Self::is_transient_surreal_error(&error) {
                    Self::enqueue_pending_aqs_sync_op(
                        pending_ops,
                        PendingAqsSyncOp::StrategyStarted {
                            universe: initial_universe_assets.clone(),
                            account: initial_account.clone(),
                        },
                    );
                } else {
                    error!(
                        "Failed to mark strategy {} as started in AQS: {}",
                        auth.strategy_id, error
                    );
                }
            }
            let _ = self
                .persist_live_metrics_if_needed(client, auth, pending_ops)
                .await;
        }

        Ok(())
    }
}
