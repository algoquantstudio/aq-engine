use super::StrategyMode;
use super::aqs_types::{StrategyEventLevel, StrategyEventType};
use super::types::live_metrics::{LiveMetricsCheckpoint, LiveMetricsSnapshot};
use crate::core::broker::types::BrokerError;
use std::collections::VecDeque;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use turso::{Builder, Connection, Value, params};

pub(super) const MAX_PENDING_LOCAL_LIVE_EVENTS: usize = 512;
const MAX_LOCAL_EVENT_BATCH: usize = 64;
const MAX_LOCAL_EVENT_BATCH_BYTES: usize = 256 * 1024;

mod metrics;
#[cfg(test)]
mod tests;

#[derive(Clone)]
struct MetricsWrite {
    key: String,
    snapshot: Arc<LiveMetricsSnapshot>,
}

pub(super) struct LocalLiveRunArtifacts {
    pub(super) run_id: String,
    pub(super) db_path: PathBuf,
    pub(super) conn: Connection,
    event_conn: Connection,
    event_writes: tokio::task::JoinSet<(Vec<String>, Result<(), BrokerError>)>,
    event_ready: Arc<tokio::sync::Notify>,
    event_retry_at: Instant,
    metrics_conn: Option<Connection>,
    metrics_writes: tokio::task::JoinSet<(Connection, Result<(), BrokerError>)>,
    metrics_inflight: Option<MetricsWrite>,
    metrics_pending: Option<MetricsWrite>,
    metrics_retry_at: Instant,
    pub(super) last_metrics: Option<Arc<LiveMetricsSnapshot>>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(super) struct LocalLiveMetricsCheckpoint {
    pub(super) strategy_key: String,
    pub(super) checkpoint: LiveMetricsCheckpoint,
}

#[derive(Clone, Debug)]
pub(super) struct PendingLocalLiveEvent {
    pub(super) event_id: String,
    pub(super) created_at: chrono::DateTime<chrono::Utc>,
    pub(super) event_type: StrategyEventType,
    pub(super) level: StrategyEventLevel,
    pub(super) title: String,
    pub(super) message: String,
    pub(super) details: LocalLiveEventDetails,
}

impl PendingLocalLiveEvent {
    fn byte_size(&self) -> usize {
        self.event_id.len()
            + self.title.len()
            + self.message.len()
            + 128
            + [
                &self.details.run_id,
                &self.details.strategy_id,
                &self.details.strategy_name,
                &self.details.node_id,
                &self.details.order_id,
                &self.details.symbol,
                &self.details.broker_event,
            ]
            .iter()
            .map(|value| value.as_ref().map_or(0, |s| s.len()))
            .sum::<usize>()
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct LocalLiveEventDetails {
    pub(super) run_id: Option<String>,
    pub(super) strategy_id: Option<String>,
    pub(super) strategy_name: Option<String>,
    pub(super) node_id: Option<String>,
    pub(super) order_id: Option<String>,
    pub(super) symbol: Option<String>,
    pub(super) broker_event: Option<String>,
}

pub(super) fn enqueue_pending_local_live_event(
    pending_events: &mut VecDeque<PendingLocalLiveEvent>,
    event: PendingLocalLiveEvent,
) -> Result<(), PendingLocalLiveEvent> {
    if pending_events.len() >= MAX_PENDING_LOCAL_LIVE_EVENTS {
        return Err(event);
    }
    pending_events.push_back(event);
    Ok(())
}

#[cfg(test)]
async fn flush_pending_local_live_events(
    artifacts: &LocalLiveRunArtifacts,
    pending_events: &mut VecDeque<PendingLocalLiveEvent>,
) -> Result<usize, BrokerError> {
    if !artifacts.event_writes.is_empty() {
        return Err(BrokerError::ConnectionError(
            "Cannot synchronously drain an in-flight local event batch".into(),
        ));
    }
    let batch = pending_event_batch(pending_events)?;
    artifacts.append_event_batch(&batch).await?;
    let committed = batch.len();
    drop(batch);
    pending_events.drain(..committed);
    Ok(committed)
}

fn pending_event_batch(
    pending_events: &VecDeque<PendingLocalLiveEvent>,
) -> Result<Vec<&PendingLocalLiveEvent>, BrokerError> {
    let mut bytes = 0usize;
    let batch = pending_events
        .iter()
        .take(MAX_LOCAL_EVENT_BATCH)
        .take_while(|event| {
            let size = event.byte_size();
            if bytes.saturating_add(size) > MAX_LOCAL_EVENT_BATCH_BYTES {
                return false;
            }
            bytes += size;
            true
        })
        .collect::<Vec<_>>();
    if batch.is_empty() {
        return if pending_events.is_empty() {
            Ok(batch)
        } else {
            Err(BrokerError::ConnectionError(
                "Local live event exceeds the 256 KiB batch limit; event retained".into(),
            ))
        };
    }
    Ok(batch)
}

impl LocalLiveRunArtifacts {
    pub(super) fn event_notifier(&self) -> Arc<tokio::sync::Notify> {
        self.event_ready.clone()
    }

    fn apply_event_commit(
        &mut self,
        pending: &mut VecDeque<PendingLocalLiveEvent>,
        result: Result<(Vec<String>, Result<(), BrokerError>), tokio::task::JoinError>,
    ) -> Result<(), BrokerError> {
        self.event_retry_at = Instant::now() + Duration::from_secs(1);
        let (ids, result) = result.map_err(|error| {
            BrokerError::ConnectionError(format!("Local event writer failed: {error}"))
        })?;
        result?;
        if pending.len() < ids.len()
            || !pending
                .iter()
                .zip(&ids)
                .all(|(event, id)| &event.event_id == id)
        {
            return Err(BrokerError::ConnectionError(
                "Local event queue prefix changed before commit acknowledgement".into(),
            ));
        }
        pending.drain(..ids.len());
        self.event_retry_at = Instant::now();
        Ok(())
    }

    /// Bounded owner work only: reap one completion and submit at most one batch.
    pub(super) fn poll_event_writes(
        &mut self,
        pending: &mut VecDeque<PendingLocalLiveEvent>,
    ) -> Result<(), BrokerError> {
        if let Some(result) = self.event_writes.try_join_next() {
            self.apply_event_commit(pending, result)?;
        }
        if !self.event_writes.is_empty()
            || pending.is_empty()
            || Instant::now() < self.event_retry_at
        {
            return Ok(());
        }
        let batch = match pending_event_batch(pending) {
            Ok(batch) => batch.into_iter().cloned().collect::<Vec<_>>(),
            Err(error) => {
                self.event_retry_at = Instant::now() + Duration::from_secs(1);
                return Err(error);
            }
        };
        let ids = batch.iter().map(|event| event.event_id.clone()).collect();
        let conn = self.event_conn.clone();
        let path = self.db_path.clone();
        let ready = self.event_ready.clone();
        self.event_writes.spawn(async move {
            let started = Instant::now();
            let refs = batch.iter().collect::<Vec<_>>();
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                Self::write_event_batch(&conn, &path, &refs),
            )
            .await
            .unwrap_or_else(|_| {
                Err(BrokerError::ConnectionError(
                    "Local event batch timed out; retained for idempotent retry".into(),
                ))
            });
            log::debug!(
                "Local live event batch rows={} elapsed_ms={} committed={}",
                batch.len(),
                started.elapsed().as_millis(),
                result.is_ok()
            );
            ready.notify_one();
            (ids, result)
        });
        Ok(())
    }

    pub(super) async fn drain_event_writes(
        &mut self,
        pending: &mut VecDeque<PendingLocalLiveEvent>,
    ) -> Result<(), BrokerError> {
        loop {
            self.poll_event_writes(pending)?;
            if let Some(result) = self.event_writes.join_next().await {
                self.apply_event_commit(pending, result)?;
            } else if pending.is_empty() {
                return Ok(());
            } else {
                tokio::time::sleep(
                    self.event_retry_at
                        .saturating_duration_since(Instant::now()),
                )
                .await;
            }
        }
    }

    pub(super) async fn start(
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
        let event_conn = db
            .connect()
            .map_err(|error| BrokerError::ConnectionError(error.to_string()))?;
        let artifacts = Self {
            run_id,
            db_path,
            conn,
            event_conn,
            event_writes: tokio::task::JoinSet::new(),
            event_ready: Arc::new(tokio::sync::Notify::new()),
            event_retry_at: Instant::now(),
            metrics_conn: Some(
                db.connect()
                    .map_err(|error| BrokerError::ConnectionError(error.to_string()))?,
            ),
            metrics_writes: tokio::task::JoinSet::new(),
            metrics_inflight: None,
            metrics_pending: None,
            metrics_retry_at: Instant::now(),
            last_metrics: None,
        };
        artifacts.init_schema().await?;

        artifacts
            .upsert_run_metadata(strategy_id, strategy_name, mode, started_at)
            .await?;
        artifacts
            .append_event(
                StrategyEventType::Lifecycle,
                StrategyEventLevel::Info,
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

    pub(super) async fn init_schema(&self) -> Result<(), BrokerError> {
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
                    event_key TEXT NOT NULL UNIQUE,
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
                    snapshot_key TEXT NOT NULL UNIQUE,
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

    pub(super) async fn finish(
        &mut self,
        status: &str,
        message: &str,
        snapshot: Option<LiveMetricsSnapshot>,
    ) -> Result<(), BrokerError> {
        if !self.event_writes.is_empty() {
            return Err(BrokerError::ConnectionError(
                "Cannot finalise local artifacts before draining event writes".into(),
            ));
        }
        self.queue_metrics_if_changed(snapshot, true)?;
        self.drain_metrics_writes().await?;
        let finished_at = chrono::Utc::now();
        self.append_event(
            StrategyEventType::Lifecycle,
            StrategyEventLevel::Info,
            status,
            message,
            LocalLiveEventDetails::default(),
        )
        .await?;
        self.upsert_completion_metadata(status, message, finished_at)
            .await
    }

    pub(super) async fn append_event(
        &self,
        event_type: StrategyEventType,
        level: StrategyEventLevel,
        title: &str,
        message: &str,
        details: LocalLiveEventDetails,
    ) -> Result<(), BrokerError> {
        let event = PendingLocalLiveEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            created_at: chrono::Utc::now(),
            event_type,
            level,
            title: title.to_string(),
            message: message.to_string(),
            details,
        };
        self.append_event_batch(&[&event]).await
    }

    async fn append_event_batch(
        &self,
        events: &[&PendingLocalLiveEvent],
    ) -> Result<(), BrokerError> {
        Self::write_event_batch(&self.event_conn, &self.db_path, events).await
    }

    async fn write_event_batch(
        conn: &Connection,
        db_path: &std::path::Path,
        events: &[&PendingLocalLiveEvent],
    ) -> Result<(), BrokerError> {
        if events.is_empty() {
            return Ok(());
        }
        if events.len() > MAX_LOCAL_EVENT_BATCH
            || events.iter().map(|event| event.byte_size()).sum::<usize>()
                > MAX_LOCAL_EVENT_BATCH_BYTES
        {
            return Err(BrokerError::ConnectionError(
                "Local event batch exceeds its bounded write budget".into(),
            ));
        }
        const FIELDS: &str = "event_key,event_type,level,title,message,run_id,strategy_id,strategy_name,node_id,order_id,symbol,broker_event,created_at";
        // 64 * 13 = 832 parameters, below SQLite's older 999-variable limit too.
        let rows = std::iter::repeat_n("(?,?,?,?,?,?,?,?,?,?,?,?,?)", events.len())
            .collect::<Vec<_>>()
            .join(",");
        let mut sql = format!(
            "INSERT INTO events ({FIELDS}) VALUES {rows} ON CONFLICT(event_key) DO UPDATE SET event_key=CASE WHEN "
        );
        let mut comparisons = FIELDS
            .split(',')
            .filter(|column| *column != "event_key")
            .map(|column| format!("events.{column} IS excluded.{column}"))
            .collect::<Vec<_>>();
        // Turso's scalar-expression compiler recurses per binary operator.
        // A balanced tree bounds that depth instead of a left-deep AND chain.
        while comparisons.len() > 1 {
            comparisons = comparisons
                .chunks(2)
                .map(|pair| {
                    if pair.len() == 2 {
                        format!("({} AND {})", pair[0], pair[1])
                    } else {
                        pair[0].clone()
                    }
                })
                .collect();
        }
        sql.push_str(&comparisons[0]);
        // A conflicting immutable retry fails the entire statement via NOT NULL,
        // while an identical retry keeps its original row ID and timestamp.
        sql.push_str(" THEN events.event_key ELSE NULL END");
        let mut values = Vec::<Value>::with_capacity(events.len() * 13);
        for event in events {
            values.extend([
                Value::Text(event.event_id.clone()),
                Value::Text(event.event_type.to_string()),
                Value::Text(event.level.to_string()),
                Value::Text(event.title.clone()),
                Value::Text(event.message.clone()),
            ]);
            for value in [
                &event.details.run_id,
                &event.details.strategy_id,
                &event.details.strategy_name,
                &event.details.node_id,
                &event.details.order_id,
                &event.details.symbol,
                &event.details.broker_event,
            ] {
                values.push(
                    value
                        .as_ref()
                        .map_or(Value::Null, |value| Value::Text(value.clone())),
                );
            }
            values.push(Value::Text(event.created_at.to_rfc3339()));
        }
        conn.execute(&sql, values)
            .await
            .map(|_| ())
            .map_err(|error| {
                BrokerError::ConnectionError(format!(
                    "Failed to commit local live event batch into {}: {}",
                    db_path.display(),
                    error
                ))
            })
    }

    pub(super) async fn upsert_run_metadata(
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

    pub(super) async fn upsert_completion_metadata(
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
