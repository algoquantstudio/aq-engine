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
use crate::core::broker::types::{Account, BrokerError, Order, Position, TradeUpdateEvent};
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
use std::time::{Duration, Instant};
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
const LIVE_SYNC_FLUSH_MS: u64 = 500;
const LIVE_SYNC_MAX_INSIGHTS_PER_FLUSH: usize = 128;
const LIVE_STARTUP_CLEANUP_MAX_CONCURRENT_BROKER_OPS: usize = 8;
const LIVE_ACCOUNT_SYNC_SECS: u64 = 5;
const LOCAL_LIVE_METRICS_WRITE_SECS: u64 = 5;
const MAX_PENDING_LOCAL_LIVE_EVENTS: usize = 512;

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

struct PendingLocalLiveEvent {
    event_type: String,
    level: String,
    title: String,
    message: String,
    details: LocalLiveEventDetails,
}

#[derive(Clone, Debug, Default)]
struct LocalLiveEventDetails {
    run_id: Option<String>,
    strategy_id: Option<String>,
    strategy_name: Option<String>,
    node_id: Option<String>,
    order_id: Option<String>,
    symbol: Option<String>,
    broker_event: Option<String>,
}

#[derive(Clone, Debug)]
struct RemoteStartupInsight {
    insight_id: String,
    symbol: String,
    previous_state: String,
    side: String,
    order_id: Option<String>,
    live_session_id: Option<serde_json::Value>,
}

#[derive(Clone, Copy, Debug)]
enum StartupBrokerActionKind {
    CancelOrder,
    ClosePosition,
}

#[derive(Clone, Debug)]
struct StartupBrokerAction {
    remote: RemoteStartupInsight,
    order_id: String,
    broker_order_status: String,
    broker_position_qty: Option<f64>,
    action: StartupBrokerActionKind,
}

#[derive(Clone, Debug)]
struct StartupCleanupDecision {
    remote: RemoteStartupInsight,
    target_state: String,
    broker_order_status: Option<String>,
    broker_action: Option<String>,
    broker_error: Option<String>,
    broker_position_qty: Option<f64>,
}

fn enqueue_pending_local_live_event(
    pending_events: &mut VecDeque<PendingLocalLiveEvent>,
    event: PendingLocalLiveEvent,
) {
    if pending_events.len() >= MAX_PENDING_LOCAL_LIVE_EVENTS {
        pending_events.pop_front();
        warn!(
            "Pending local live event queue reached capacity ({}); dropping oldest item",
            MAX_PENDING_LOCAL_LIVE_EVENTS
        );
    }
    pending_events.push_back(event);
}

async fn flush_pending_local_live_events(
    artifacts: &LocalLiveRunArtifacts,
    pending_events: &mut VecDeque<PendingLocalLiveEvent>,
) {
    while let Some(event) = pending_events.pop_front() {
        if let Err(error) = artifacts
            .append_event(
                &event.event_type,
                &event.level,
                &event.title,
                &event.message,
                event.details,
            )
            .await
        {
            warn!(
                "Failed to persist local live event into {}: {}",
                artifacts.db_path.display(),
                error
            );
        }
    }
}

impl LocalLiveRunArtifacts {
    async fn start(
        strategy_name: &str,
        strategy_id: uuid::Uuid,
        mode: StrategyMode,
        artifact_root: PathBuf,
    ) -> Result<Self, BrokerError> {
        let started_at = chrono::Utc::now();
        let run_id = format!(
            "{}-{}",
            started_at.format("%Y%m%d-%H%M%S"),
            uuid::Uuid::new_v4()
        );
        let dir = artifact_root.join("live").join(&run_id);
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

        artifacts
            .upsert_run_metadata(strategy_id, strategy_name, mode, started_at)
            .await?;
        artifacts
            .append_event(
                "lifecycle",
                "info",
                "Local live run started",
                "AQS Cloud auth was not provided; AQE is writing local live run artifacts",
                LocalLiveEventDetails {
                    run_id: Some(artifacts.run_id.clone()),
                    strategy_id: Some(strategy_id.to_string()),
                    strategy_name: Some(strategy_name.to_string()),
                    ..Default::default()
                },
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
                    run_id TEXT NOT NULL,
                    strategy_id TEXT,
                    strategy_name TEXT,
                    mode TEXT,
                    status TEXT,
                    message TEXT,
                    started_at TEXT,
                    finished_at TEXT,
                    database_file TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    level TEXT NOT NULL,
                    title TEXT NOT NULL,
                    message TEXT NOT NULL,
                    run_id TEXT,
                    strategy_id TEXT,
                    strategy_name TEXT,
                    node_id TEXT,
                    order_id TEXT,
                    symbol TEXT,
                    broker_event TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_live_events_created_at ON events(created_at);
                CREATE INDEX IF NOT EXISTS idx_live_events_type ON events(event_type);
                CREATE INDEX IF NOT EXISTS idx_live_events_symbol ON events(symbol);
                CREATE INDEX IF NOT EXISTS idx_live_events_order_id ON events(order_id);

                CREATE TABLE IF NOT EXISTS metrics_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    captured_at TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    starting_cash REAL NOT NULL,
                    final_equity REAL NOT NULL,
                    total_return REAL NOT NULL,
                    total_return_pct REAL NOT NULL,
                    total_trades INTEGER NOT NULL,
                    winning_trades INTEGER NOT NULL,
                    losing_trades INTEGER NOT NULL,
                    win_rate REAL NOT NULL,
                    max_drawdown REAL NOT NULL,
                    cagr REAL NOT NULL,
                    annualized_volatility REAL NOT NULL,
                    sharpe_ratio REAL NOT NULL,
                    sortino_ratio REAL NOT NULL,
                    calmar_ratio REAL NOT NULL,
                    max_drawdown_duration_days REAL NOT NULL,
                    expectancy REAL NOT NULL,
                    profit_factor REAL NOT NULL,
                    payoff_ratio REAL NOT NULL,
                    avg_winner REAL NOT NULL,
                    avg_loser REAL NOT NULL,
                    avg_winner_pct REAL NOT NULL,
                    avg_loser_pct REAL NOT NULL,
                    best_trade REAL NOT NULL,
                    worst_trade REAL NOT NULL,
                    consistency_score REAL NOT NULL,
                    longest_winning_trade_held_secs INTEGER NOT NULL,
                    longest_losing_trade_held_secs INTEGER NOT NULL,
                    average_trade_held_secs INTEGER NOT NULL,
                    open_positions_count INTEGER NOT NULL,
                    open_insights_count INTEGER NOT NULL,
                    open_positions_unrealized_pnl REAL NOT NULL,
                    open_positions_profitable_count INTEGER NOT NULL,
                    open_positions_losing_count INTEGER NOT NULL,
                    executed_at TEXT NOT NULL,
                    finished_at TEXT,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_live_metrics_captured_at ON metrics_snapshots(captured_at);
                CREATE INDEX IF NOT EXISTS idx_live_metrics_run_id ON metrics_snapshots(run_id);

                CREATE TABLE IF NOT EXISTS metrics_snapshot_symbols (
                    snapshot_id INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    PRIMARY KEY (snapshot_id, symbol),
                    FOREIGN KEY(snapshot_id) REFERENCES metrics_snapshots(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_live_metric_symbols_symbol ON metrics_snapshot_symbols(symbol);
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
        self.conn
            .execute(
                "INSERT INTO metrics_snapshots (
                    captured_at,
                    run_id,
                    starting_cash,
                    final_equity,
                    total_return,
                    total_return_pct,
                    total_trades,
                    winning_trades,
                    losing_trades,
                    win_rate,
                    max_drawdown,
                    cagr,
                    annualized_volatility,
                    sharpe_ratio,
                    sortino_ratio,
                    calmar_ratio,
                    max_drawdown_duration_days,
                    expectancy,
                    profit_factor,
                    payoff_ratio,
                    avg_winner,
                    avg_loser,
                    avg_winner_pct,
                    avg_loser_pct,
                    best_trade,
                    worst_trade,
                    consistency_score,
                    longest_winning_trade_held_secs,
                    longest_losing_trade_held_secs,
                    average_trade_held_secs,
                    open_positions_count,
                    open_insights_count,
                    open_positions_unrealized_pnl,
                    open_positions_profitable_count,
                    open_positions_losing_count,
                    executed_at,
                    finished_at,
                    updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33, ?34, ?35, ?36, ?37, ?38)",
                params![
                    saved_snapshot.updated_at.to_rfc3339(),
                    self.run_id.clone(),
                    saved_snapshot.starting_cash,
                    saved_snapshot.final_equity,
                    saved_snapshot.total_return,
                    saved_snapshot.total_return_pct,
                    saved_snapshot.total_trades as i64,
                    saved_snapshot.winning_trades as i64,
                    saved_snapshot.losing_trades as i64,
                    saved_snapshot.win_rate,
                    saved_snapshot.max_drawdown,
                    saved_snapshot.cagr,
                    saved_snapshot.annualized_volatility,
                    saved_snapshot.sharpe_ratio,
                    saved_snapshot.sortino_ratio,
                    saved_snapshot.calmar_ratio,
                    saved_snapshot.max_drawdown_duration_days,
                    saved_snapshot.expectancy,
                    saved_snapshot.profit_factor,
                    saved_snapshot.payoff_ratio,
                    saved_snapshot.avg_winner,
                    saved_snapshot.avg_loser,
                    saved_snapshot.avg_winner_pct,
                    saved_snapshot.avg_loser_pct,
                    saved_snapshot.best_trade,
                    saved_snapshot.worst_trade,
                    saved_snapshot.consistency_score,
                    saved_snapshot.longest_winning_trade_held_secs,
                    saved_snapshot.longest_losing_trade_held_secs,
                    saved_snapshot.average_trade_held_secs,
                    saved_snapshot.open_positions_count as i64,
                    saved_snapshot.open_insights_count as i64,
                    saved_snapshot.open_positions_unrealized_pnl,
                    saved_snapshot.open_positions_profitable_count as i64,
                    saved_snapshot.open_positions_losing_count as i64,
                    saved_snapshot.executed_at.to_rfc3339(),
                    saved_snapshot.finished_at.map(|value| value.to_rfc3339()),
                    saved_snapshot.updated_at.to_rfc3339()
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
        let snapshot_id = self.conn.last_insert_rowid();
        for symbol in &saved_snapshot.symbols {
            self.conn
                .execute(
                    "INSERT OR IGNORE INTO metrics_snapshot_symbols (snapshot_id, symbol)
                     VALUES (?1, ?2)",
                    params![snapshot_id, symbol.clone()],
                )
                .await
                .map_err(|error| {
                    BrokerError::ConnectionError(format!(
                        "Failed to write local live metric symbol into {}: {}",
                        self.db_path.display(),
                        error
                    ))
                })?;
        }
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
        self.append_event(
            "lifecycle",
            "info",
            status,
            message,
            LocalLiveEventDetails::default(),
        )
        .await?;
        self.upsert_completion_metadata(status, message, finished_at)
            .await
    }

    async fn append_event(
        &self,
        event_type: &str,
        level: &str,
        title: &str,
        message: &str,
        details: LocalLiveEventDetails,
    ) -> Result<(), BrokerError> {
        let created_at = chrono::Utc::now();
        self.conn
            .execute(
                "INSERT INTO events (
                    event_type,
                    level,
                    title,
                    message,
                    run_id,
                    strategy_id,
                    strategy_name,
                    node_id,
                    order_id,
                    symbol,
                    broker_event,
                    created_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    event_type.to_string(),
                    level.to_string(),
                    title.to_string(),
                    message.to_string(),
                    details.run_id,
                    details.strategy_id,
                    details.strategy_name,
                    details.node_id,
                    details.order_id,
                    details.symbol,
                    details.broker_event,
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

    async fn upsert_run_metadata(
        &self,
        strategy_id: uuid::Uuid,
        strategy_name: &str,
        mode: StrategyMode,
        started_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), BrokerError> {
        self.conn
            .execute(
                "INSERT INTO run_metadata (
                    key,
                    run_id,
                    strategy_id,
                    strategy_name,
                    mode,
                    status,
                    started_at,
                    database_file,
                    updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(key) DO UPDATE SET
                    run_id = excluded.run_id,
                    strategy_id = excluded.strategy_id,
                    strategy_name = excluded.strategy_name,
                    mode = excluded.mode,
                    status = excluded.status,
                    started_at = excluded.started_at,
                    database_file = excluded.database_file,
                    updated_at = excluded.updated_at",
                params![
                    "run".to_string(),
                    self.run_id.clone(),
                    strategy_id.to_string(),
                    strategy_name.to_string(),
                    format!("{:?}", mode),
                    "running".to_string(),
                    started_at.to_rfc3339(),
                    "live.db".to_string(),
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

    async fn upsert_completion_metadata(
        &self,
        status: &str,
        message: &str,
        finished_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), BrokerError> {
        self.conn
            .execute(
                "INSERT INTO run_metadata (
                    key,
                    run_id,
                    status,
                    message,
                    finished_at,
                    updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(key) DO UPDATE SET
                    run_id = excluded.run_id,
                    status = excluded.status,
                    message = excluded.message,
                    finished_at = excluded.finished_at,
                    updated_at = excluded.updated_at",
                params![
                    "completion".to_string(),
                    self.run_id.clone(),
                    status.to_string(),
                    message.to_string(),
                    finished_at.to_rfc3339(),
                    chrono::Utc::now().to_rfc3339()
                ],
            )
            .await
            .map_err(|error| {
                BrokerError::ConnectionError(format!(
                    "Failed to write local live completion metadata into {}: {}",
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

    async fn cleanup_stale_remote_live_insights_on_start<C: surrealdb::Connection>(
        &self,
        client: &surrealdb::Surreal<C>,
        auth: &AqsAuth,
        pending_ops: &mut VecDeque<PendingAqsSyncOp>,
    ) -> Result<usize, surrealdb::Error> {
        let local_insight_ids = self
            .insights
            .ids()
            .into_iter()
            .map(|insight_id| insight_id.to_string())
            .collect::<HashSet<_>>();

        let mut result: IndexedResults = client
            .query(
                "SELECT insight_id, symbol, state, side, order_id, live_session_id
                 FROM insights
                 WHERE strategy_id = type::record('strategy', $strategy_id)
                   AND state IN ['New', 'Executed', 'Filled']",
            )
            .bind(("strategy_id", auth.strategy_id.clone()))
            .await?;
        let rows: Vec<serde_json::Value> = result.take(0)?;
        let mut remote_insights = Vec::new();

        for row in rows {
            let Some(insight_id) = row
                .get("insight_id")
                .and_then(|value| value.as_str())
                .map(ToString::to_string)
            else {
                continue;
            };

            let remote = RemoteStartupInsight {
                insight_id: insight_id.clone(),
                symbol: row
                    .get("symbol")
                    .and_then(|value| value.as_str())
                    .unwrap_or("-")
                    .to_string(),
                previous_state: row
                    .get("state")
                    .and_then(|value| value.as_str())
                    .unwrap_or("-")
                    .to_string(),
                side: row
                    .get("side")
                    .and_then(|value| value.as_str())
                    .unwrap_or("-")
                    .to_string(),
                order_id: row
                    .get("order_id")
                    .and_then(|value| value.as_str())
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToString::to_string),
                live_session_id: row.get("live_session_id").cloned(),
            };

            if local_insight_ids.contains(&remote.insight_id) {
                warn!(
                    "AQS startup cleanup found active remote insight {} for strategy {} that already exists locally; leaving it untouched for future state reload handling",
                    remote.insight_id, auth.strategy_id
                );
                continue;
            }

            remote_insights.push(remote);
        }

        if remote_insights.is_empty() {
            return Ok(0);
        }

        let (orders_result, positions_result) =
            tokio::join!(self.broker.get_orders(), self.broker.get_positions());

        let (broker_orders, broker_orders_error) = match orders_result {
            Ok(orders) => {
                info!(
                    "AQS startup cleanup loaded {} broker order(s) to reconcile {} stale remote insight(s) for strategy {}",
                    orders.len(),
                    remote_insights.len(),
                    auth.strategy_id
                );
                (
                    orders
                        .into_iter()
                        .map(|order| (order.order_id.clone(), order))
                        .collect::<HashMap<String, Order>>(),
                    None,
                )
            }
            Err(error) => {
                let error = format!("{:?}", error);
                warn!(
                    "AQS startup cleanup failed to load broker orders for strategy {}; stale remote insights will be reconciled without broker order confirmation: {}",
                    auth.strategy_id, error
                );
                (HashMap::new(), Some(error))
            }
        };

        let broker_positions = match positions_result {
            Ok(positions) => positions
                .into_iter()
                .map(|position| (position.asset.symbol.clone(), position))
                .collect::<HashMap<String, Position>>(),
            Err(error) => {
                warn!(
                    "AQS startup cleanup failed to load broker positions for strategy {}; position context will be omitted from cleanup events: {:?}",
                    auth.strategy_id, error
                );
                HashMap::new()
            }
        };

        let mut decisions = Vec::new();
        let mut broker_actions = Vec::new();
        let mut missing_order_count = 0usize;
        let mut missing_order_samples = Vec::new();

        for remote in remote_insights {
            let remote_order_id = remote.order_id.clone();
            if let Some(order_id) = remote_order_id.as_deref() {
                if let Some(order) = broker_orders.get(order_id) {
                    let broker_order_status = format!("{:?}", order.status);
                    match order.status {
                        TradeUpdateEvent::Filled | TradeUpdateEvent::PartialFilled => {
                            let broker_position_qty = broker_positions
                                .get(&remote.symbol)
                                .map(|position| position.qty);
                            if broker_position_qty.is_none_or(|qty| qty.abs() <= f64::EPSILON) {
                                warn!(
                                    "AQS startup cleanup found stale remote insight {} state={} with filled broker order {} but no matching open broker position for {}; marking remote insight as Closed",
                                    remote.insight_id,
                                    remote.previous_state,
                                    order_id,
                                    remote.symbol
                                );
                                decisions.push(StartupCleanupDecision {
                                    remote,
                                    target_state: "Closed".to_string(),
                                    broker_order_status: Some(broker_order_status),
                                    broker_action: Some("position_not_found".to_string()),
                                    broker_error: None,
                                    broker_position_qty,
                                });
                                continue;
                            }

                            warn!(
                                "AQS startup cleanup found stale remote insight {} state={} with filled broker order {}; requesting broker position close",
                                remote.insight_id, remote.previous_state, order_id
                            );
                            broker_actions.push(StartupBrokerAction {
                                remote,
                                order_id: order_id.to_string(),
                                broker_order_status,
                                broker_position_qty,
                                action: StartupBrokerActionKind::ClosePosition,
                            });
                        }
                        TradeUpdateEvent::Closed => {
                            decisions.push(StartupCleanupDecision {
                                remote,
                                target_state: "Closed".to_string(),
                                broker_order_status: Some(broker_order_status),
                                broker_action: Some("already_closed".to_string()),
                                broker_error: None,
                                broker_position_qty: None,
                            });
                        }
                        TradeUpdateEvent::Cancelled
                        | TradeUpdateEvent::Rejected
                        | TradeUpdateEvent::Expired => {
                            decisions.push(StartupCleanupDecision {
                                remote,
                                target_state: "Cancelled".to_string(),
                                broker_order_status: Some(broker_order_status),
                                broker_action: Some("already_terminal".to_string()),
                                broker_error: None,
                                broker_position_qty: None,
                            });
                        }
                        _ => {
                            broker_actions.push(StartupBrokerAction {
                                remote,
                                order_id: order_id.to_string(),
                                broker_order_status,
                                broker_position_qty: None,
                                action: StartupBrokerActionKind::CancelOrder,
                            });
                        }
                    }
                    continue;
                }

                missing_order_count += 1;
                if missing_order_samples.len() < 5 {
                    missing_order_samples.push(format!("{}:{}", remote.insight_id, order_id));
                }
            }

            let broker_position_qty = broker_positions
                .get(&remote.symbol)
                .map(|position| position.qty);
            decisions.push(StartupCleanupDecision {
                remote,
                target_state: "Cancelled".to_string(),
                broker_order_status: None,
                broker_action: Some("order_not_found".to_string()),
                broker_error: broker_orders_error.clone().or_else(|| {
                    Some("broker order not found in startup order snapshot".to_string())
                }),
                broker_position_qty,
            });
        }

        if missing_order_count > 0 {
            warn!(
                "AQS startup cleanup did not find {} broker order(s) for stale remote insight(s) on strategy {}; marking matching remote insights as Cancelled. samples=[{}]",
                missing_order_count,
                auth.strategy_id,
                missing_order_samples.join(", ")
            );
        }

        if !broker_actions.is_empty() {
            let action_results = futures::stream::iter(broker_actions)
                .map(|action| async move {
                    match action.action {
                        StartupBrokerActionKind::ClosePosition => {
                            match self.broker.close_position(&action.order_id, 0.0, None).await {
                                Ok(true) => Some(StartupCleanupDecision {
                                    remote: action.remote,
                                    target_state: "Closed".to_string(),
                                    broker_order_status: Some(action.broker_order_status),
                                    broker_action: Some("close_position".to_string()),
                                    broker_error: None,
                                    broker_position_qty: action.broker_position_qty,
                                }),
                                Ok(false) => {
                                    warn!(
                                        "AQS startup cleanup could not close broker position for stale insight {} order {}: close_position returned false",
                                        action.remote.insight_id, action.order_id
                                    );
                                    None
                                }
                                Err(error) => {
                                    warn!(
                                        "AQS startup cleanup could not close broker position for stale insight {} order {}: {:?}",
                                        action.remote.insight_id, action.order_id, error
                                    );
                                    None
                                }
                            }
                        }
                        StartupBrokerActionKind::CancelOrder => {
                            match self.broker.cancel_order(&action.order_id).await {
                                Ok(true) => Some(StartupCleanupDecision {
                                    remote: action.remote,
                                    target_state: "Cancelled".to_string(),
                                    broker_order_status: Some(action.broker_order_status),
                                    broker_action: Some("cancel_order".to_string()),
                                    broker_error: None,
                                    broker_position_qty: None,
                                }),
                                Ok(false) => {
                                    warn!(
                                        "AQS startup cleanup could not cancel stale remote insight {} order {}: cancel_order returned false",
                                        action.remote.insight_id, action.order_id
                                    );
                                    None
                                }
                                Err(error) => {
                                    warn!(
                                        "AQS startup cleanup could not cancel stale remote insight {} order {}: {:?}",
                                        action.remote.insight_id, action.order_id, error
                                    );
                                    None
                                }
                            }
                        }
                    }
                })
                .buffer_unordered(LIVE_STARTUP_CLEANUP_MAX_CONCURRENT_BROKER_OPS)
                .collect::<Vec<_>>()
                .await;
            decisions.extend(action_results.into_iter().flatten());
        }

        let mut cleaned = 0usize;
        for decision in decisions {
            let insight_id = decision.remote.insight_id.clone();

            let now = chrono::Utc::now();
            let update_result = client
                .query(
                    "UPDATE insights
                     SET state = $target_state,
                         cancelling = false,
                         closing = false,
                         first_on_fill = false,
                         updated_at = <datetime>$now,
                         closed_at = <datetime>$now
                     WHERE strategy_id = type::record('strategy', $strategy_id)
                       AND insight_id = $insight_id
                       AND state IN ['New', 'Executed', 'Filled']",
                )
                .bind(("target_state", decision.target_state.clone()))
                .bind(("now", now))
                .bind(("strategy_id", auth.strategy_id.clone()))
                .bind(("insight_id", insight_id.clone()))
                .await
                .and_then(|response| response.check());

            if let Err(error) = update_result {
                if Self::is_transient_surreal_error(&error) {
                    return Err(error);
                }
                error!(
                    "Failed to clean stale remote insight {} for strategy {} on live startup: {}",
                    insight_id, auth.strategy_id, error
                );
                continue;
            }

            cleaned += 1;
            let reason = format!(
                "AQS live startup cleaned stale remote insight {}; AQE did not have this insight in local state",
                insight_id
            );
            let event = StrategyEventRecord {
                event_type: "insight_startup_cleanup".into(),
                level: "warn".into(),
                title: "Stale remote insight cleaned up".into(),
                message: reason.clone(),
                payload: Some(serde_json::json!({
                    "insight_id": insight_id,
                    "symbol": decision.remote.symbol,
                    "side": decision.remote.side,
                    "order_id": decision.remote.order_id,
                    "previous_state": decision.remote.previous_state,
                    "state": decision.target_state,
                    "live_session_id": decision.remote.live_session_id,
                    "broker_order_status": decision.broker_order_status,
                    "broker_action": decision.broker_action,
                    "broker_error": decision.broker_error,
                    "broker_position_qty": decision.broker_position_qty,
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
                    "Failed to persist startup cleanup event for strategy {} insight {}: {}",
                    auth.strategy_id, insight_id, error
                );
            }
        }

        if cleaned > 0 {
            warn!(
                "AQS live startup cleaned {} stale remote active insight(s) for strategy {}",
                cleaned, auth.strategy_id
            );
        }

        Ok(cleaned)
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

            let gross_pnl = match insight.side {
                crate::core::broker::types::OrderSide::Buy => {
                    (current_price - entry_price) * remaining_qty
                }
                crate::core::broker::types::OrderSide::Sell => {
                    (entry_price - current_price) * remaining_qty
                }
            };
            let pnl = gross_pnl + insight.swap.unwrap_or(0.0) - insight.commission.unwrap_or(0.0);

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
        max_insights: Option<usize>,
    ) -> Result<bool, surrealdb::Error> {
        debug!(
            "Syncing live insights to AQS for strategy {}: {} in-memory insights",
            auth.strategy_id,
            self.insights.len()
        );
        let mut persist_metrics_after_sync = false;
        let mut prune_after_sync = Vec::new();
        let mut insight_ids = self
            .insights
            .insight_ids_for_live_sync(include_full_reconcile);
        if !include_full_reconcile {
            if let Some(max_insights) = max_insights {
                if insight_ids.len() > max_insights {
                    insight_ids.truncate(max_insights);
                }
            }
        }
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
        let (trade_tx, mut trade_rx) = tokio::sync::mpsc::unbounded_channel();
        let (bar_tx, mut bar_rx) = tokio::sync::mpsc::unbounded_channel::<MarketDataEvent>();

        // 5. Subscribe to trade event stream
        let trade_tx_clone = trade_tx.clone();
        let trade_callback = Arc::new(move |event| {
            let _ = trade_tx_clone.send(event);
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
                let _ = bar_tx_clone.send(event);
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
        let mut live_sync_interval =
            tokio::time::interval(std::time::Duration::from_millis(LIVE_SYNC_FLUSH_MS));
        let mut reconcile_interval =
            tokio::time::interval(std::time::Duration::from_secs(LIVE_SYNC_RECONCILE_SECS));
        let mut local_metrics_interval = tokio::time::interval(std::time::Duration::from_secs(
            LOCAL_LIVE_METRICS_WRITE_SECS,
        ));
        live_sync_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut force_full_reconcile = true;
        let mut live_sync_failure: Option<BrokerError> = None;
        let mut last_runtime_publish = Instant::now();
        let mut last_account_sync = Instant::now()
            .checked_sub(Duration::from_secs(LIVE_ACCOUNT_SYNC_SECS))
            .unwrap_or_else(Instant::now);
        let mut pending_local_live_events = VecDeque::new();

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
                biased;

                // Handle broker trade events
                Some((order, event)) = trade_rx.recv() => {
                    let mut received_events = vec![(order, event)];
                    while let Ok(event) = trade_rx.try_recv() {
                        received_events.push(event);
                    }
                    debug!(
                        "Live loop received {} trade event notification(s)",
                        received_events.len()
                    );
                    // Process trade events (we let `on_trade_update()` drain all pending broker state)
                    self.on_trade_update();

                    for (order, event) in received_events {
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
                        });

                        if local_live_artifacts.is_some() {
                            enqueue_pending_local_live_event(
                                &mut pending_local_live_events,
                                PendingLocalLiveEvent {
                                    event_type: "trade_event".into(),
                                    level: "info".into(),
                                    title: "Broker trade event".into(),
                                    message: message.clone(),
                                    details: LocalLiveEventDetails {
                                        order_id: Some(order_id.clone()),
                                        symbol: Some(symbol.clone()),
                                        broker_event: Some(broker_event.clone()),
                                        ..Default::default()
                                    },
                                },
                            );
                        }

                        if auth.is_some() {
                            Self::enqueue_pending_aqs_sync_op(
                                &mut pending_sync_ops,
                                PendingAqsSyncOp::StrategyEvent(StrategyEventRecord {
                                    event_type: "trade_event".into(),
                                    level: "info".into(),
                                    title: "Broker trade event".into(),
                                    message,
                                    payload: Some(payload),
                                    created_at: Some(chrono::Utc::now()),
                                }),
                            );
                        }
                    }

                    if last_runtime_publish.elapsed() >= Duration::from_millis(500) {
                        self.publish_runtime_snapshot(aqs_sync_status.clone(), None);
                        last_runtime_publish = Instant::now();
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
                    if last_runtime_publish.elapsed() >= Duration::from_millis(500) {
                        self.publish_runtime_snapshot(aqs_sync_status.clone(), None);
                        last_runtime_publish = Instant::now();
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

                    self.publish_runtime_snapshot(aqs_sync_status.clone(), None);
                }

                    _ = live_sync_interval.tick() => {
                        if let Some(artifacts) = local_live_artifacts.as_ref() {
                            flush_pending_local_live_events(artifacts, &mut pending_local_live_events).await;
                        }

                        if let Some(a) = auth.as_ref() {
                            if db.is_none() {
                                reconnect_live_sync_or_stop!(a);
                                force_full_reconcile = true;
                            }
                            if let Some(client) = db.as_ref() {
                                let account_sync_due =
                                    last_account_sync.elapsed() >= Duration::from_secs(LIVE_ACCOUNT_SYNC_SECS);
                                let has_sync_work = !pending_sync_ops.is_empty()
                                    || self.insights.has_dirty_insights()
                                    || self.live_metrics.should_persist()
                                    || account_sync_due;

                                if !has_sync_work {
                                    continue;
                                }

                                if let Err(error) = Self::flush_pending_aqs_sync_ops(client, a, &mut pending_sync_ops).await {
                                    error!(
                                        "Live sync could not flush pending AQS ops for strategy {} during sync flush: {}",
                                        a.strategy_id, error
                                    );
                                    reconnect_live_sync_or_stop!(a);
                                    force_full_reconcile = true;
                                    continue;
                                }

                                let mut persist_metrics_after_sync = match self
                                    .sync_live_insights_to_aqs(
                                        client,
                                        a,
                                        &mut synced_insight_states,
                                        &mut synced_insight_history_offsets,
                                        &mut pending_sync_ops,
                                        false,
                                        Some(LIVE_SYNC_MAX_INSIGHTS_PER_FLUSH),
                                    )
                                    .await
                                {
                                    Ok(persist_metrics_after_sync) => {
                                        persist_metrics_after_sync
                                    }
                                    Err(error) => {
                                        error!(
                                            "Live sync lost AQS connection for strategy {} during sync flush: {}",
                                            a.strategy_id, error
                                        );
                                        reconnect_live_sync_or_stop!(a);
                                        force_full_reconcile = true;
                                        continue;
                                    }
                                };

                                if account_sync_due {
                                    last_account_sync = Instant::now();
                                    if let Ok(account) = self.broker.get_account().await {
                                        let captured_at = chrono::Utc::now();
                                        self.live_metrics.update_equity(account.equity, captured_at);
                                        persist_metrics_after_sync = true;
                                        if let Err(error) =
                                            persist_live_account_state(client, a, &account, captured_at).await
                                        {
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
                                    }
                                }

                                if persist_metrics_after_sync {
                                    if let Err(error) =
                                        self.persist_live_metrics_if_needed(client, a, &mut pending_sync_ops).await
                                    {
                                        error!(
                                            "Live sync lost AQS connection for strategy {} while persisting live metrics: {}",
                                            a.strategy_id, error
                                        );
                                        reconnect_live_sync_or_stop!(a);
                                        force_full_reconcile = true;
                                        continue;
                                    }
                                }
                            }
                        }
                    }

                    _ = local_metrics_interval.tick(), if local_live_artifacts.is_some() => {
                        if let Some(artifacts) = local_live_artifacts.as_mut() {
                            flush_pending_local_live_events(artifacts, &mut pending_local_live_events).await;
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
                                .sync_live_insights_to_aqs(client, a, &mut synced_insight_states, &mut synced_insight_history_offsets, &mut pending_sync_ops, force_full_reconcile, None)
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
            let _ = Self::flush_pending_aqs_sync_ops(client, a, &mut pending_sync_ops).await;
            let _ = self
                .sync_live_insights_to_aqs(
                    client,
                    a,
                    &mut synced_insight_states,
                    &mut synced_insight_history_offsets,
                    &mut pending_sync_ops,
                    true,
                    None,
                )
                .await;
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
            flush_pending_local_live_events(artifacts, &mut pending_local_live_events).await;
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
            if let Err(error) = self
                .cleanup_stale_remote_live_insights_on_start(client, auth, pending_ops)
                .await
            {
                if Self::is_transient_surreal_error(&error) {
                    warn!(
                        "AQS live startup could not clean stale remote active insights for strategy {} because the database connection was transiently unavailable: {}",
                        auth.strategy_id, error
                    );
                } else {
                    error!(
                        "AQS live startup could not clean stale remote active insights for strategy {}: {}",
                        auth.strategy_id, error
                    );
                }
            }

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
