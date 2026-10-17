use super::{
    CloudEvent, DEFAULT_OUTBOX_SOFT_LIMIT_BYTES, EventBusError, EventEnvelope, EventId, EventKind,
    EventPriority, EventSubject, LocalEventBus, OutboxEventRecord, OutboxStore, OutboxStoreError,
    ReconciliationChunk, ReconciliationComplete, ReconciliationJobProgress,
    ReconciliationLifecycleRequest, ReconciliationLifecycleResult, ReconciliationSnapshot,
    ReconciliationStart, SyncAcknowledgement, deterministic_event_id, now_epoch_micros,
};
use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;
use surrealdb::method::QueryStream;
use surrealdb::{Notification, Surreal, engine::any};
use tokio::sync::{mpsc, watch};
mod periodic;

pub type RemoteActionStream = QueryStream<Notification<surrealdb::types::Value>>;

pub struct RemoteActionLiveSubscription {
    // The stream is backed by this authenticated connection. Retaining the handle also makes
    // reconnect ownership explicit instead of depending on an implementation detail of the SDK.
    _client: Surreal<any::Any>,
    pub stream: RemoteActionStream,
}

pub use aqe_live_protocol::{LiveSyncBatch, LiveSyncBatchResult};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoteSyncErrorKind {
    Transient,
    Authentication,
    InvalidBatch,
    SequenceConflict,
    Terminal,
}

#[derive(Clone, Debug)]
pub struct RemoteSyncError {
    pub kind: RemoteSyncErrorKind,
    pub message: String,
}

impl fmt::Display for RemoteSyncError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for RemoteSyncError {}

#[async_trait]
pub trait RemoteBatchSink: Send + Sync {
    async fn persist_batch(
        &self,
        batch: &LiveSyncBatch,
    ) -> Result<LiveSyncBatchResult, RemoteSyncError>;

    /// Refreshes finite-lived credentials once after a 401/403. Returning `false`
    /// tells the agent that synchronization is blocked, while local execution continues.
    async fn refresh_authentication(&self) -> Result<bool, RemoteSyncError> {
        Ok(false)
    }

    /// Health reporting is best-effort and intentionally independent from the durable event
    /// cursor. A failed health report must never stop execution or hold up critical replay.
    async fn report_health(&self, _health: &RemoteSyncHealth) -> Result<(), RemoteSyncError> {
        Ok(())
    }

    /// Upload and process one resumable broker-neutral reconciliation generation.
    async fn reconcile_snapshot(
        &self,
        _snapshot: &ReconciliationSnapshot,
    ) -> Result<(), RemoteSyncError> {
        Ok(())
    }

    async fn reconcile_start(&self, _start: &ReconciliationStart) -> Result<(), RemoteSyncError> {
        Ok(())
    }

    async fn reconcile_chunk(&self, _chunk: &ReconciliationChunk) -> Result<(), RemoteSyncError> {
        Ok(())
    }

    async fn reconcile_complete(
        &self,
        _complete: &ReconciliationComplete,
    ) -> Result<ReconciliationProcessResult, RemoteSyncError> {
        Ok(ReconciliationProcessResult::default())
    }

    async fn reconcile_lifecycle_result(
        &self,
        _result: &ReconciliationLifecycleResult,
    ) -> Result<(), RemoteSyncError> {
        Ok(())
    }

    /// Returns the durable remote lifecycle cursor after a sequence conflict. A missing cursor
    /// means the remote endpoint could not provide an authoritative answer; callers must retain
    /// the local events and retry instead of guessing that they were persisted.
    async fn fetch_remote_cursor(&self) -> Result<Option<u64>, RemoteSyncError> {
        Ok(None)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteSyncHealth {
    pub runtime_id: String,
    pub strategy_id: String,
    pub live_session_id: String,
    pub pending_local_events: u64,
    pub pending_telemetry: u64,
    pub pending_audit: u64,
    pub pending_reconciliation: u64,
    pub dlq_count: u64,
    pub oldest_pending_age_ms: Option<u64>,
    pub last_local_acknowledgement_us: Option<i64>,
    pub last_successful_commit_us: Option<i64>,
    pub sync_state: String,
    pub reason: Option<String>,
    pub retry_delay_ms: u64,
    pub database_bytes: u64,
    #[serde(default)]
    pub reconciliation: Option<ReconciliationJobProgress>,
    pub updated_at_us: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReconciliationProcessResult {
    /// Stable hosted request identity; retries must return the same broker evidence.
    #[serde(default)]
    pub lifecycle_request_id: Option<String>,
    #[serde(default)]
    pub lifecycle_order_ids: Vec<String>,
    #[serde(default)]
    pub candidate_count: u64,
    #[serde(default)]
    pub classified_count: u64,
    #[serde(default)]
    pub unresolved_count: u64,
    #[serde(default)]
    pub current_cursor: Option<String>,
    #[serde(default)]
    pub remaining: u64,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub superseded: bool,
}

#[derive(Clone, Debug)]
pub struct HttpBatchSinkConfig {
    /// Legacy-shaped URL used only to derive operational health/cursor/action
    /// endpoints. No execution batch is posted here; the gateway is mandatory.
    pub batch_url: String,
    pub actions_url: Option<String>,
    pub bearer_token: String,
    pub session_secret: Option<String>,
    pub signin_url: Option<String>,
    pub namespace: String,
    pub database: String,
    pub access_method: String,
    pub user_id: String,
    pub strategy_id: String,
    pub live_session_id: String,
    pub runtime_id: String,
    pub run_id: String,
    pub server_node_id: String,
    pub request_timeout: Duration,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemoteStrategyAction {
    pub id: serde_json::Value,
    pub action: serde_json::Value,
    pub status: serde_json::Value,
    #[serde(default)]
    pub strategy_id: Option<serde_json::Value>,
    #[serde(default)]
    pub runtime_id: Option<String>,
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(default)]
    pub requested_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl RemoteStrategyAction {
    pub fn record_id(&self) -> Option<String> {
        json_record_id(&self.id)
    }

    pub fn strategy_record_id(&self) -> Option<String> {
        self.strategy_id.as_ref().and_then(json_record_id)
    }
}

fn json_record_id(value: &serde_json::Value) -> Option<String> {
    if let Some(value) = value.as_str() {
        return Some(value.to_string());
    }
    let table = value
        .get("tb")
        .and_then(serde_json::Value::as_str)
        .or_else(|| value.get("table").and_then(serde_json::Value::as_str));
    let key = value.get("id").or_else(|| value.get("key"));
    match (table, key) {
        (Some(table), Some(key)) => Some(format!(
            "{table}:{}",
            key.as_str()
                .map(str::to_string)
                .unwrap_or_else(|| key.to_string())
        )),
        _ => None,
    }
}

fn jwt_expiry_us(token: &str) -> Option<i64> {
    let payload = token.split('.').nth(1)?;
    let decoded = decode_base64_url(payload)?;
    let claims = serde_json::from_slice::<serde_json::Value>(&decoded).ok()?;
    claims
        .get("exp")
        .and_then(serde_json::Value::as_i64)
        .map(|seconds| seconds.saturating_mul(1_000_000))
}

fn decode_base64_url(value: &str) -> Option<Vec<u8>> {
    fn sextet(byte: u8) -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'-' => Some(62),
            b'_' => Some(63),
            _ => None,
        }
    }
    let mut output = Vec::with_capacity(value.len().saturating_mul(3) / 4);
    let mut accumulator = 0u32;
    let mut bits = 0u8;
    for byte in value.bytes().take_while(|byte| *byte != b'=') {
        accumulator = (accumulator << 6) | u32::from(sextet(byte)?);
        bits = bits.saturating_add(6);
        if bits >= 8 {
            bits -= 8;
            output.push(((accumulator >> bits) & 0xff) as u8);
        }
    }
    Some(output)
}

#[derive(Clone)]
pub struct HttpBatchSink {
    client: reqwest::Client,
    config: HttpBatchSinkConfig,
    bearer_token: Arc<tokio::sync::RwLock<String>>,
    /// Operational health/action writes share a gate per remote scope. Execution
    /// batches and reconciliation are serialized by the gateway, not this gate.
    remote_write_gate: Arc<tokio::sync::Mutex<()>>,
    gateway: super::gateway::GatewayConnection,
}

type RemoteWriteGate = tokio::sync::Mutex<()>;

fn remote_write_gate(config: &HttpBatchSinkConfig) -> Arc<RemoteWriteGate> {
    static GATES: OnceLock<Mutex<HashMap<String, Weak<RemoteWriteGate>>>> = OnceLock::new();
    let scope = format!(
        "{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}",
        config.batch_url,
        config.namespace,
        config.database,
        config.strategy_id,
        config.live_session_id,
    );
    let mut gates = GATES.get_or_init(|| Mutex::new(HashMap::new())).lock();
    gates.retain(|_, gate| gate.strong_count() > 0);
    if let Some(gate) = gates.get(&scope).and_then(Weak::upgrade) {
        return gate;
    }
    let gate = Arc::new(RemoteWriteGate::new(()));
    gates.insert(scope, Arc::downgrade(&gate));
    gate
}

impl HttpBatchSink {
    /// Managed cloud sync requires the event gateway. There is no direct
    /// database batch fallback, including when the gateway is unavailable.
    pub fn new(config: HttpBatchSinkConfig, gateway_url: &str) -> Result<Self, RemoteSyncError> {
        let gateway = super::gateway::GatewayConnection::new(gateway_url, config.clone())?;
        let client = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|error| RemoteSyncError {
                kind: RemoteSyncErrorKind::Terminal,
                message: format!("failed to build live-sync HTTP client: {error}"),
            })?;
        let bearer_token = Arc::new(tokio::sync::RwLock::new(config.bearer_token.clone()));
        let remote_write_gate = remote_write_gate(&config);
        Ok(Self {
            client,
            config,
            bearer_token,
            remote_write_gate,
            gateway,
        })
    }

    pub(crate) fn hosted_gateway(&self) -> super::gateway::GatewayConnection {
        self.gateway.clone()
    }

    /// The caller must prove process exit and drain of both local outboxes first.
    pub async fn seal_drained_run(&self) -> Result<(), RemoteSyncError> {
        self.gateway.seal_drained_run().await
    }

    async fn refresh_if_expiring(&self) -> Result<(), RemoteSyncError> {
        let token = self.bearer_token.read().await.clone();
        if token.is_empty() {
            if <Self as RemoteBatchSink>::refresh_authentication(self).await? {
                return Ok(());
            }
            return Err(RemoteSyncError {
                kind: RemoteSyncErrorKind::Authentication,
                message: "AQS live-sync authentication is not configured".into(),
            });
        }
        let Some(expiry_us) = jwt_expiry_us(&token) else {
            return Ok(());
        };
        if expiry_us > now_epoch_micros().saturating_add(60_000_000) {
            return Ok(());
        }
        if !<Self as RemoteBatchSink>::refresh_authentication(self).await? {
            return Err(RemoteSyncError {
                kind: RemoteSyncErrorKind::Authentication,
                message: "AQS live-sync token is expiring and no refresh credential is available"
                    .to_string(),
            });
        }
        Ok(())
    }

    pub async fn fetch_pending_actions(
        &self,
    ) -> Result<Vec<RemoteStrategyAction>, RemoteSyncError> {
        self.refresh_if_expiring().await?;
        let Some(actions_url) = self.config.actions_url.as_ref() else {
            return Ok(Vec::new());
        };
        let bearer_token = self.bearer_token.read().await.clone();
        let response = self
            .client
            .get(actions_url)
            .bearer_auth(bearer_token)
            .send()
            .await
            .map_err(|error| RemoteSyncError {
                kind: RemoteSyncErrorKind::Transient,
                message: format!("failed to poll AQS actions: {error}"),
            })?;
        let status = response.status();
        if status.is_success() {
            return response
                .json::<Vec<RemoteStrategyAction>>()
                .await
                .map_err(|error| RemoteSyncError {
                    kind: RemoteSyncErrorKind::InvalidBatch,
                    message: format!("invalid AQS action response: {error}"),
                });
        }
        let body = response.text().await.unwrap_or_default();
        Err(RemoteSyncError {
            kind: match status.as_u16() {
                401 | 403 => RemoteSyncErrorKind::Authentication,
                429 | 500..=599 => RemoteSyncErrorKind::Transient,
                _ => RemoteSyncErrorKind::Terminal,
            },
            message: format!("AQS action endpoint returned {status}: {body}"),
        })
    }

    pub async fn subscribe_pending_actions(
        &self,
    ) -> Result<RemoteActionLiveSubscription, RemoteSyncError> {
        self.refresh_if_expiring().await?;
        let database_url = database_websocket_url(&self.config.batch_url)?;
        let client =
            any::connect(database_url.as_str())
                .await
                .map_err(|error| RemoteSyncError {
                    kind: RemoteSyncErrorKind::Transient,
                    message: format!("failed to connect AQS action live stream: {error}"),
                })?;
        let bearer_token = self.bearer_token.read().await.clone();
        client
            .authenticate(bearer_token)
            .await
            .map_err(|error| RemoteSyncError {
                kind: RemoteSyncErrorKind::Authentication,
                message: format!("failed to authenticate AQS action live stream: {error}"),
            })?;
        client
            .use_ns(&self.config.namespace)
            .use_db(&self.config.database)
            .await
            .map_err(|error| RemoteSyncError {
                kind: RemoteSyncErrorKind::Transient,
                message: format!("failed to select AQS action database: {error}"),
            })?;
        let strategy_key = self
            .config
            .strategy_id
            .split_once(':')
            .map(|(_, key)| key)
            .unwrap_or(self.config.strategy_id.as_str())
            .to_string();
        let live_session_key = self
            .config
            .live_session_id
            .split_once(':')
            .map(|(_, key)| key)
            .unwrap_or(self.config.live_session_id.as_str())
            .to_string();
        let mut response = client
            .query(
                "LIVE SELECT id, action, status, strategy_id, runtime_id, run_id, requested_at FROM strategy_actions
                 WHERE strategy_id = type::record('strategy', $strategy_id)
                   AND live_session_id = type::record('live_strategy_session', <uuid>$live_session_id)
                   AND runtime_id = $runtime_id
                   AND run_id = $run_id
                   AND (source = NONE OR source != 'hosted_gateway')
                   AND status = 'pending'",
            )
            .bind(("strategy_id", strategy_key))
            .bind(("live_session_id", live_session_key))
            .bind(("runtime_id", self.config.runtime_id.clone()))
            .bind(("run_id", self.config.run_id.clone()))
            .await
            .map_err(|error| RemoteSyncError {
                kind: RemoteSyncErrorKind::Transient,
                message: format!("failed to create AQS action live query: {error}"),
            })?;
        let stream = response
            .stream::<Notification<surrealdb::types::Value>>(0)
            .map_err(|error| RemoteSyncError {
                kind: RemoteSyncErrorKind::Transient,
                message: format!("failed to materialize AQS action live stream: {error}"),
            })?;
        Ok(RemoteActionLiveSubscription {
            _client: client,
            stream,
        })
    }

    /// Best-effort terminal update for an action which cannot target this runtime generation.
    /// This uses the same scoped live-session credential as action polling and only mutates a
    /// still-pending row addressed to this exact runtime. It prevents obsolete requests from
    /// being returned forever without putting database work in AQE's execution loop.
    pub async fn cancel_pending_action(
        &self,
        action_id: &str,
        message: &str,
    ) -> Result<(), RemoteSyncError> {
        self.refresh_if_expiring().await?;
        let _write_guard = self.remote_write_gate.lock().await;
        let database_url = database_websocket_url(&self.config.batch_url)?;
        let client =
            any::connect(database_url.as_str())
                .await
                .map_err(|error| RemoteSyncError {
                    kind: RemoteSyncErrorKind::Transient,
                    message: format!("failed to connect for AQS action cancellation: {error}"),
                })?;
        client
            .authenticate(self.bearer_token.read().await.clone())
            .await
            .map_err(|error| RemoteSyncError {
                kind: RemoteSyncErrorKind::Authentication,
                message: format!("failed to authenticate AQS action cancellation: {error}"),
            })?;
        client
            .use_ns(&self.config.namespace)
            .use_db(&self.config.database)
            .await
            .map_err(|error| RemoteSyncError {
                kind: RemoteSyncErrorKind::Transient,
                message: format!("failed to select AQS action database: {error}"),
            })?;
        client
            .query(
                "UPDATE <record<strategy_actions>> $action_id
                 SET status = 'cancelled',
                     message = $message,
                     processed_at = time::now(),
                     completed_at = time::now()
                 WHERE status = 'pending'
                   AND strategy_id = type::record('strategy', $strategy_id)
                   AND live_session_id = type::record('live_strategy_session', <uuid>$live_session_id)",
            )
            .bind(("action_id", action_id.to_string()))
            .bind(("message", message.to_string()))
            .bind((
                "strategy_id",
                self.config
                    .strategy_id
                    .split_once(':')
                    .map(|(_, key)| key)
                    .unwrap_or(self.config.strategy_id.as_str())
                    .to_string(),
            ))
            .bind((
                "live_session_id",
                self.config
                    .live_session_id
                    .split_once(':')
                    .map(|(_, key)| key)
                    .unwrap_or(self.config.live_session_id.as_str())
                    .to_string(),
            ))
            .await
            .map_err(|error| RemoteSyncError {
                kind: RemoteSyncErrorKind::Transient,
                message: format!("failed to cancel AQS action {action_id}: {error}"),
            })?
            .check()
            .map_err(|error| RemoteSyncError {
                kind: RemoteSyncErrorKind::Transient,
                message: format!("failed to commit AQS action cancellation {action_id}: {error}"),
            })?;
        Ok(())
    }

    // These endpoints remain for operational health and cursor/action reads;
    // execution batches and reconciliation are gateway-only.
    fn api_base_url(&self) -> &str {
        self.config
            .batch_url
            .trim_end_matches('/')
            .strip_suffix("/batch")
            .unwrap_or(self.config.batch_url.trim_end_matches('/'))
    }

    fn health_url(&self) -> String {
        format!("{}/health", self.api_base_url())
    }

    fn cursor_url(&self) -> String {
        format!(
            "{}/cursor/{}",
            self.api_base_url(),
            self.config.live_session_id
        )
    }
}

fn database_websocket_url(batch_url: &str) -> Result<String, RemoteSyncError> {
    let scheme_end = batch_url.find("://").ok_or_else(|| RemoteSyncError {
        kind: RemoteSyncErrorKind::Terminal,
        message: format!("invalid AQS batch URL: {batch_url}"),
    })?;
    let scheme = &batch_url[..scheme_end];
    let remainder = &batch_url[scheme_end + 3..];
    let authority = remainder.split('/').next().unwrap_or_default();
    if authority.is_empty() {
        return Err(RemoteSyncError {
            kind: RemoteSyncErrorKind::Terminal,
            message: format!("invalid AQS batch URL authority: {batch_url}"),
        });
    }
    let websocket_scheme = match scheme {
        "https" | "wss" => "wss",
        "http" | "ws" => "ws",
        _ => {
            return Err(RemoteSyncError {
                kind: RemoteSyncErrorKind::Terminal,
                message: format!("unsupported AQS batch URL scheme: {scheme}"),
            });
        }
    };
    Ok(format!("{websocket_scheme}://{authority}"))
}

#[async_trait]
impl RemoteBatchSink for HttpBatchSink {
    async fn persist_batch(
        &self,
        batch: &LiveSyncBatch,
    ) -> Result<LiveSyncBatchResult, RemoteSyncError> {
        self.gateway.publish(batch).await
    }

    async fn refresh_authentication(&self) -> Result<bool, RemoteSyncError> {
        let Some(secret) = self.config.session_secret.as_ref() else {
            return Ok(false);
        };
        let Some(signin_url) = self.config.signin_url.as_ref() else {
            return Ok(false);
        };
        let response = self
            .client
            .post(signin_url)
            .json(&serde_json::json!({
                "ns": self.config.namespace,
                "db": self.config.database,
                "ac": self.config.access_method,
                "session_id": self.config.live_session_id,
                "secret": secret,
            }))
            .send()
            .await
            .map_err(|error| RemoteSyncError {
                kind: RemoteSyncErrorKind::Transient,
                message: format!("failed to refresh AQS live-sync authentication: {error}"),
            })?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(RemoteSyncError {
                kind: if status.is_server_error() || status.as_u16() == 429 {
                    RemoteSyncErrorKind::Transient
                } else {
                    RemoteSyncErrorKind::Authentication
                },
                message: format!("AQS live-sync sign-in returned {status}: {body}"),
            });
        }
        let value: serde_json::Value =
            serde_json::from_str(&body).map_err(|error| RemoteSyncError {
                kind: RemoteSyncErrorKind::Authentication,
                message: format!("invalid AQS live-sync sign-in response: {error}"),
            })?;
        let token = value
            .as_str()
            .or_else(|| value.get("token").and_then(serde_json::Value::as_str))
            .filter(|value| !value.is_empty())
            .ok_or_else(|| RemoteSyncError {
                kind: RemoteSyncErrorKind::Authentication,
                message: "AQS live-sync sign-in returned no token".to_string(),
            })?;
        *self.bearer_token.write().await = token.to_string();
        Ok(true)
    }

    async fn report_health(&self, health: &RemoteSyncHealth) -> Result<(), RemoteSyncError> {
        let _write_guard = self.remote_write_gate.lock().await;
        let bearer_token = self.bearer_token.read().await.clone();
        let mut body = serde_json::to_value(health).map_err(|error| RemoteSyncError {
            kind: RemoteSyncErrorKind::Terminal,
            message: format!("failed to encode live-sync health: {error}"),
        })?;
        body["runId"] = serde_json::Value::String(self.config.run_id.clone());
        let response = self
            .client
            .post(self.health_url())
            .bearer_auth(bearer_token)
            .header("x-aqs-user-id", &self.config.user_id)
            .header("x-aqs-strategy-id", &self.config.strategy_id)
            .header("x-aqs-live-session-id", &self.config.live_session_id)
            .header("x-aqs-runtime-id", &self.config.runtime_id)
            .header("x-aqs-server-node-id", &self.config.server_node_id)
            .json(&body)
            .send()
            .await
            .map_err(|error| RemoteSyncError {
                kind: RemoteSyncErrorKind::Transient,
                message: format!("failed to report live-sync health: {error}"),
            })?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let body = response.text().await.unwrap_or_default();
        Err(RemoteSyncError {
            kind: match status.as_u16() {
                401 | 403 => RemoteSyncErrorKind::Authentication,
                429 | 500..=599 => RemoteSyncErrorKind::Transient,
                _ => RemoteSyncErrorKind::Terminal,
            },
            message: format!("live-sync health endpoint returned {status}: {body}"),
        })
    }

    async fn reconcile_snapshot(
        &self,
        _snapshot: &ReconciliationSnapshot,
    ) -> Result<(), RemoteSyncError> {
        Err(RemoteSyncError {
            kind: RemoteSyncErrorKind::InvalidBatch,
            message: "hosted reconciliation requires protocol-v2 chunks".into(),
        })
    }

    async fn reconcile_start(&self, start: &ReconciliationStart) -> Result<(), RemoteSyncError> {
        self.gateway.reconcile("start", start).await.map(|_| ())
    }

    async fn reconcile_chunk(&self, chunk: &ReconciliationChunk) -> Result<(), RemoteSyncError> {
        self.gateway.reconcile("chunk", chunk).await.map(|_| ())
    }

    async fn reconcile_complete(
        &self,
        complete: &ReconciliationComplete,
    ) -> Result<ReconciliationProcessResult, RemoteSyncError> {
        serde_json::from_value(self.gateway.reconcile("complete", complete).await?).map_err(|_| {
            RemoteSyncError {
                kind: RemoteSyncErrorKind::Transient,
                message: "invalid hosted reconciliation progress".into(),
            }
        })
    }

    async fn reconcile_lifecycle_result(
        &self,
        result: &ReconciliationLifecycleResult,
    ) -> Result<(), RemoteSyncError> {
        self.gateway
            .reconcile("lifecycle", result)
            .await
            .map(|_| ())
    }

    async fn fetch_remote_cursor(&self) -> Result<Option<u64>, RemoteSyncError> {
        let bearer_token = self.bearer_token.read().await.clone();
        let response = self
            .client
            .get(self.cursor_url())
            .bearer_auth(bearer_token)
            .header("x-aqs-user-id", &self.config.user_id)
            .header("x-aqs-strategy-id", &self.config.strategy_id)
            .header("x-aqs-live-session-id", &self.config.live_session_id)
            .header("x-aqs-runtime-id", &self.config.runtime_id)
            .header("x-aqs-server-node-id", &self.config.server_node_id)
            .send()
            .await
            .map_err(|error| RemoteSyncError {
                kind: RemoteSyncErrorKind::Transient,
                message: format!("failed to read the AQS live-sync cursor: {error}"),
            })?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(RemoteSyncError {
                kind: match status.as_u16() {
                    401 | 403 => RemoteSyncErrorKind::Authentication,
                    429 | 500..=599 => RemoteSyncErrorKind::Transient,
                    _ => RemoteSyncErrorKind::Terminal,
                },
                message: format!("AQS cursor endpoint returned {status}: {body}"),
            });
        }
        let rows: serde_json::Value =
            serde_json::from_str(&body).map_err(|error| RemoteSyncError {
                kind: RemoteSyncErrorKind::InvalidBatch,
                message: format!("invalid AQS cursor response: {error}"),
            })?;
        let rows = rows.as_array().cloned().unwrap_or_else(|| vec![rows]);
        Ok(rows
            .iter()
            .filter(|row| {
                row.get("runtime_id")
                    .or_else(|| row.get("runtimeId"))
                    .and_then(serde_json::Value::as_str)
                    .is_none_or(|runtime| runtime == self.config.runtime_id)
            })
            .filter_map(|row| {
                row.get("persisted_critical_sequence")
                    .or_else(|| row.get("persistedCriticalSequence"))
                    .and_then(serde_json::Value::as_u64)
            })
            .max())
    }
}

#[derive(Clone, Debug)]
pub struct SyncAgentConfig {
    pub runtime_id: String,
    pub strategy_id: String,
    pub live_session_id: String,
    pub critical_batch_events: usize,
    pub critical_batch_bytes: usize,
    pub telemetry_batch_events: usize,
    pub telemetry_batch_bytes: usize,
    pub flush_interval: Duration,
    pub telemetry_flush_interval: Duration,
    pub reconciliation_flush_interval: Duration,
    pub reconciliation_pause_critical_backlog: u64,
    pub retention_grace: Duration,
    pub health_interval: Duration,
    pub outbox_soft_limit_bytes: u64,
}

impl SyncAgentConfig {
    pub fn new(
        runtime_id: impl Into<String>,
        strategy_id: impl Into<String>,
        live_session_id: impl Into<String>,
    ) -> Self {
        Self {
            runtime_id: runtime_id.into(),
            strategy_id: strategy_id.into(),
            live_session_id: live_session_id.into(),
            critical_batch_events: 32,
            critical_batch_bytes: 64 * 1024,
            telemetry_batch_events: 100,
            telemetry_batch_bytes: 128 * 1024,
            flush_interval: Duration::from_millis(10),
            telemetry_flush_interval: Duration::from_millis(100),
            reconciliation_flush_interval: Duration::from_millis(100),
            reconciliation_pause_critical_backlog: 100,
            // Once SurrealDB acknowledges a batch, SQLite is no longer the source of truth.
            // Retain a short diagnostic window, then reuse the pages instead of carrying days
            // of committed rows into every subsequent run.
            retention_grace: Duration::from_secs(5 * 60),
            health_interval: Duration::from_secs(1),
            outbox_soft_limit_bytes: DEFAULT_OUTBOX_SOFT_LIMIT_BYTES,
        }
    }
}

#[derive(Clone, Debug)]
struct AgentHealthState {
    sync_state: String,
    reason: Option<String>,
}

impl Default for AgentHealthState {
    fn default() -> Self {
        Self {
            sync_state: "healthy".to_string(),
            reason: None,
        }
    }
}

#[derive(Clone)]
pub struct SyncAgent {
    config: SyncAgentConfig,
    bus: Arc<dyn LocalEventBus>,
    outbox: OutboxStore,
    sink: Arc<dyn RemoteBatchSink>,
    health_state: Arc<Mutex<AgentHealthState>>,
}

impl SyncAgent {
    pub fn new(
        config: SyncAgentConfig,
        bus: Arc<dyn LocalEventBus>,
        outbox: OutboxStore,
        sink: Arc<dyn RemoteBatchSink>,
    ) -> Self {
        Self {
            config,
            bus,
            outbox,
            sink,
            health_state: Arc::new(Mutex::new(AgentHealthState::default())),
        }
    }

    pub async fn run(self, mut shutdown: watch::Receiver<bool>) -> Result<(), SyncAgentError> {
        if *shutdown.borrow() {
            return Ok(());
        }
        let critical_subject = EventSubject::for_priority(
            &self.config.runtime_id,
            &self.config.strategy_id,
            EventPriority::Critical,
        );
        let telemetry_subject = EventSubject::for_priority(
            &self.config.runtime_id,
            &self.config.strategy_id,
            EventPriority::Telemetry,
        );
        let reconcile_subject = EventSubject::for_priority(
            &self.config.runtime_id,
            &self.config.strategy_id,
            EventPriority::Reconciliation,
        );
        let audit_subject = EventSubject::for_priority(
            &self.config.runtime_id,
            &self.config.strategy_id,
            EventPriority::Audit,
        );
        let critical = self.bus.subscribe(critical_subject).await?;
        let telemetry = self.bus.subscribe(telemetry_subject).await?;
        let reconciliation = self.bus.subscribe(reconcile_subject).await?;
        let audit = self.bus.subscribe(audit_subject).await?;
        // Ten fixed workers can each report at most one terminal error.
        let (ingestion_error_tx, mut ingestion_error_rx) = mpsc::channel(16);
        // JoinSet aborts owned tasks on every exit path, including cancellation of
        // run() and `?` errors. Detached ingestion/HTTP workers can otherwise keep
        // an embedded strategy runtime alive after startup fails.
        let mut workers = tokio::task::JoinSet::new();
        for mut subscription in [critical, audit, reconciliation] {
            let bus = Arc::clone(&self.bus);
            let outbox = self.outbox.clone();
            let shutdown = shutdown.clone();
            let errors = ingestion_error_tx.clone();
            workers.spawn(async move {
                if let Err(error) =
                    Self::ingest_critical_stream(bus, outbox, &mut subscription, shutdown).await
                {
                    let _ = errors.send(error).await;
                }
            });
        }
        for mut subscription in [telemetry] {
            let outbox = self.outbox.clone();
            let shutdown = shutdown.clone();
            let errors = ingestion_error_tx.clone();
            workers.spawn(async move {
                if let Err(error) =
                    Self::ingest_latest_stream(outbox, &mut subscription, shutdown).await
                {
                    let _ = errors.send(error).await;
                }
            });
        }
        // Critical persistence has a dedicated worker. Slow telemetry, history, health, and
        // reconciliation requests must never prevent lifecycle batches from draining.
        let critical_agent = self.clone();
        let mut critical_shutdown = shutdown.clone();
        let critical_errors = ingestion_error_tx.clone();
        workers.spawn(async move {
            let notifier = critical_agent.outbox.change_notifier();
            loop {
                if *critical_shutdown.borrow() {
                    break;
                }
                // Register before reading/flushing: notify_waiters does not retain a
                // permit for future waiters, and each scope must receive the wakeup.
                let notified = notifier.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if let Err(error) = critical_agent
                    .flush_priority(
                        EventPriority::Critical,
                        critical_agent.config.critical_batch_events,
                        critical_agent.config.critical_batch_bytes,
                    )
                    .await
                {
                    let _ = critical_errors.send(error).await;
                    break;
                }

                let deadline = match critical_agent
                    .outbox
                    .next_pending_deadline_for_runtime_scope(
                        EventPriority::Critical,
                        &critical_agent.config.runtime_id,
                        &critical_agent.config.strategy_id,
                        &critical_agent.config.live_session_id,
                    )
                    .await
                {
                    Ok(deadline) => deadline,
                    Err(error) => {
                        let _ = critical_errors.send(error.into()).await;
                        break;
                    }
                };

                if deadline.is_some_and(|deadline| deadline <= now_epoch_micros()) {
                    // More than one batch is ready. Drain cooperatively without waiting for a
                    // timer, while still yielding to broker and ingestion tasks between batches.
                    tokio::task::yield_now().await;
                    continue;
                }

                let wait = deadline.map(|deadline| {
                    let remaining_us = deadline.saturating_sub(now_epoch_micros()).max(1) as u64;
                    Duration::from_micros(remaining_us)
                });
                tokio::select! {
                    _ = &mut notified => {}
                    _ = async {
                        match wait {
                            Some(wait) => tokio::time::sleep(wait).await,
                            None => std::future::pending().await,
                        }
                    } => {}
                    changed = critical_shutdown.changed() => {
                        if changed.is_err() || *critical_shutdown.borrow() {
                            break;
                        }
                    }
                }
            }
        });
        for job in periodic::Job::ALL {
            let agent = self.clone();
            let receiver = shutdown.clone();
            let errors = ingestion_error_tx.clone();
            workers.spawn(async move {
                if let Err(error) = agent.run_periodic(job, receiver).await {
                    let _ = errors.send(error).await;
                }
            });
        }
        drop(ingestion_error_tx);
        let outcome = loop {
            if *shutdown.borrow() {
                break Ok(());
            }
            tokio::select! {
                biased;
                Some(error) = ingestion_error_rx.recv() => break Err(error),
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { break Ok(()); }
                }
                Some(_) = workers.join_next() => break Err(SyncAgentError::WorkerStopped),
            }
        };
        // Stop/join all periodic and ingestion work before the final flush. No
        // background publisher races that flush or retains a subscription.
        workers.shutdown().await;
        outcome?;
        match tokio::time::timeout(Duration::from_secs(2), self.flush_due()).await {
            Ok(result) => result?,
            Err(_) => self.set_health_state(
                "degraded",
                Some("shutdown flush timed out; durable events retained".into()),
            ),
        }
        let _ = self.publish_health().await;
        let _ = self.outbox.checkpoint_wal(true).await;
        Ok(())
    }

    async fn ingest_critical_stream(
        bus: Arc<dyn LocalEventBus>,
        outbox: OutboxStore,
        subscription: &mut super::EventSubscription,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), SyncAgentError> {
        loop {
            tokio::select! {
                delivery = subscription.recv() => {
                    let Some(delivery) = delivery else { return Ok(()) };
                    Self::accept_critical_batch(
                        bus.as_ref(),
                        &outbox,
                        delivery,
                        subscription,
                    ).await?;
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn ingest_latest_stream(
        outbox: OutboxStore,
        subscription: &mut super::EventSubscription,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), SyncAgentError> {
        loop {
            tokio::select! {
                delivery = subscription.recv() => {
                    let Some(delivery) = delivery else { return Ok(()) };
                    outbox.insert_telemetry_latest(&delivery.event).await?;
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn accept_critical_batch(
        bus: &dyn LocalEventBus,
        outbox: &OutboxStore,
        first: super::EventDelivery,
        subscription: &mut super::EventSubscription,
    ) -> Result<(), SyncAgentError> {
        // The publisher sends a micro-batch concurrently. Yield once so those publications can
        // reach the local subscription, then commit up to 100 messages in one WAL transaction.
        // Every request/reply is acknowledged only after that transaction commits.
        tokio::task::yield_now().await;
        let mut deliveries = vec![first];
        while deliveries.len() < 100 {
            match subscription.try_recv() {
                Ok(delivery) => deliveries.push(delivery),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
            }
        }
        let events = deliveries
            .iter()
            .map(|delivery| delivery.event.clone())
            .collect::<Vec<_>>();
        for event in &events {
            if event.kind == EventKind::ReconciliationStart {
                let start = serde_json::from_value::<ReconciliationStart>(event.payload.clone())
                    .map_err(|error| {
                        SyncAgentError::InvalidEvent(format!(
                            "invalid reconciliation start: {error}"
                        ))
                    })?;
                outbox.begin_reconciliation_job(&start).await?;
            }
        }
        outbox.insert_events(&events).await?;
        let event_ids = events
            .iter()
            .map(|event| event.event_id)
            .collect::<Vec<_>>();
        for delivery in &deliveries {
            bus.acknowledge(
                delivery,
                SyncAcknowledgement::LocalAccepted {
                    event_ids: event_ids.clone(),
                },
            )
            .await?;
        }
        Ok(())
    }

    async fn flush_due(&self) -> Result<(), SyncAgentError> {
        self.flush_priority(
            EventPriority::Critical,
            self.config.critical_batch_events,
            self.config.critical_batch_bytes,
        )
        .await?;
        self.flush_priority(
            EventPriority::Telemetry,
            self.config.telemetry_batch_events,
            self.config.telemetry_batch_bytes,
        )
        .await?;
        self.flush_priority(EventPriority::Audit, 100, 128 * 1024)
            .await?;
        // Reconciliation is deliberately lowest priority. A database slowdown must not let a
        // stale-run scan compete with current order and insight lifecycle persistence.
        if self.critical_backlog_exceeds(0).await? {
            return Ok(());
        }
        self.flush_priority(EventPriority::Reconciliation, 100, 256 * 1024)
            .await
    }

    async fn critical_backlog_exceeds(&self, limit: u64) -> Result<bool, OutboxStoreError> {
        self.outbox
            .critical_backlog_exceeds_for_runtime_scope(
                &self.config.runtime_id,
                &self.config.strategy_id,
                &self.config.live_session_id,
                limit,
            )
            .await
    }

    async fn publish_health(&self) -> Result<(), SyncAgentError> {
        let outbox = self.outbox.cached_health(Duration::from_secs(1)).await?;
        let state = self.health_state.lock().clone();
        let sync_state = if outbox.dead_letters > 0 && state.sync_state == "healthy" {
            "degraded".to_string()
        } else {
            state.sync_state
        };
        let reason = state.reason.or_else(|| {
            (outbox.dead_letters > 0).then(|| {
                format!(
                    "{} event(s) require dead-letter review",
                    outbox.dead_letters
                )
            })
        });
        let health = RemoteSyncHealth {
            runtime_id: self.config.runtime_id.clone(),
            strategy_id: self.config.strategy_id.clone(),
            live_session_id: self.config.live_session_id.clone(),
            pending_local_events: outbox.pending_critical,
            pending_telemetry: outbox.pending_telemetry,
            pending_audit: outbox.pending_audit,
            pending_reconciliation: outbox.pending_reconciliation,
            dlq_count: outbox.dead_letters,
            oldest_pending_age_ms: outbox.oldest_pending_age_ms,
            last_local_acknowledgement_us: outbox.last_local_ack_at_us,
            last_successful_commit_us: outbox.last_remote_commit_at_us,
            sync_state,
            reason,
            retry_delay_ms: outbox.current_retry_delay_ms,
            database_bytes: outbox.database_bytes,
            reconciliation: outbox.reconciliation,
            updated_at_us: now_epoch_micros(),
        };
        let event = EventEnvelope::new(
            self.config.runtime_id.clone(),
            self.config.strategy_id.clone(),
            self.config.live_session_id.clone(),
            "sync-health",
            health.updated_at_us.max(1) as u64,
            EventPriority::Telemetry,
            EventKind::SyncHealth,
            serde_json::to_value(&health).unwrap_or_default(),
        );
        self.bus.publish_telemetry(
            EventSubject::sync_status(&self.config.runtime_id, &self.config.strategy_id),
            event,
        )?;
        // Remote health is intentionally best-effort. The durable cursor remains the source of
        // truth and a failed health endpoint cannot recursively create more outbox work.
        let _ =
            tokio::time::timeout(Duration::from_secs(1), self.sink.report_health(&health)).await;
        Ok(())
    }

    fn set_health_state(&self, sync_state: &str, reason: Option<String>) {
        let mut state = self.health_state.lock();
        state.sync_state = sync_state.to_string();
        state.reason = reason;
    }

    async fn flush_priority(
        &self,
        priority: EventPriority,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<(), SyncAgentError> {
        let mut records = self
            .outbox
            .pending_batch_for_runtime_scope(
                priority,
                &self.config.runtime_id,
                &self.config.strategy_id,
                &self.config.live_session_id,
                max_events,
                max_bytes,
            )
            .await?;
        if records.is_empty() {
            return Ok(());
        }
        // An embedded runtime keeps its durable identity/cursor across process
        // restarts. Its oldest pending events may belong to an earlier run; the
        // hosted gateway authenticates each batch for exactly one run.
        let first_run = records[0].event.run_id.as_deref();
        let same_run = records
            .iter()
            .take_while(|record| record.event.run_id.as_deref() == first_run)
            .count();
        records.truncate(same_run);
        if priority == EventPriority::Reconciliation {
            return self.persist_reconciliation(records).await;
        }
        self.persist_with_bisection(records).await
    }

    async fn persist_reconciliation(
        &self,
        records: Vec<OutboxEventRecord>,
    ) -> Result<(), SyncAgentError> {
        for record in records {
            let reconciliation_generation = record
                .event
                .payload
                .get("generation")
                .and_then(serde_json::Value::as_str)
                .map(ToString::to_string);
            match self.send_reconciliation_event(&record.event).await {
                Ok(()) => {
                    self.outbox
                        .mark_remote_committed(&[record.event.event_id], None)
                        .await?;
                    let _ = self
                        .outbox
                        .record_persistence_acknowledgements(
                            std::slice::from_ref(&record.event),
                            now_epoch_micros(),
                        )
                        .await;
                    self.set_health_state("healthy", None);
                }
                Err(error) if error.kind == RemoteSyncErrorKind::Authentication => {
                    match self.sink.refresh_authentication().await {
                        Ok(true) => match self.send_reconciliation_event(&record.event).await {
                            Ok(()) => {
                                self.outbox
                                    .mark_remote_committed(&[record.event.event_id], None)
                                    .await?;
                                let _ = self
                                    .outbox
                                    .record_persistence_acknowledgements(
                                        std::slice::from_ref(&record.event),
                                        now_epoch_micros(),
                                    )
                                    .await;
                                self.set_health_state("healthy", None);
                            }
                            Err(retry_error) => {
                                self.set_health_state("blocked", Some(retry_error.message.clone()));
                                if let Some(generation) = reconciliation_generation.as_deref() {
                                    self.outbox
                                        .record_reconciliation_retry(
                                            generation,
                                            &retry_error.message,
                                        )
                                        .await?;
                                }
                                self.schedule_batch_retry(
                                    std::slice::from_ref(&record),
                                    &retry_error,
                                )
                                .await?;
                            }
                        },
                        Ok(false) => {
                            self.set_health_state("blocked", Some(error.message.clone()));
                            if let Some(generation) = reconciliation_generation.as_deref() {
                                self.outbox
                                    .record_reconciliation_retry(generation, &error.message)
                                    .await?;
                            }
                            self.schedule_batch_retry(std::slice::from_ref(&record), &error)
                                .await?;
                        }
                        Err(refresh_error) => {
                            // Preserve the actual credential-refresh failure; retaining only the
                            // original "token expired" message made recovery diagnosis opaque.
                            self.set_health_state("blocked", Some(refresh_error.message.clone()));
                            if let Some(generation) = reconciliation_generation.as_deref() {
                                self.outbox
                                    .record_reconciliation_retry(generation, &refresh_error.message)
                                    .await?;
                            }
                            self.schedule_batch_retry(
                                std::slice::from_ref(&record),
                                &refresh_error,
                            )
                            .await?;
                        }
                    }
                }
                Err(error)
                    if matches!(
                        error.kind,
                        RemoteSyncErrorKind::InvalidBatch | RemoteSyncErrorKind::Terminal
                    ) =>
                {
                    self.set_health_state("degraded", Some(error.message.clone()));
                    self.outbox
                        .move_to_dead_letter(record.event.event_id, &error.message)
                        .await?;
                }
                Err(error) => {
                    self.set_health_state("degraded", Some(error.message.clone()));
                    if let Some(generation) = reconciliation_generation.as_deref() {
                        self.outbox
                            .record_reconciliation_retry(generation, &error.message)
                            .await?;
                    }
                    self.schedule_batch_retry(std::slice::from_ref(&record), &error)
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn send_reconciliation_event(&self, event: &CloudEvent) -> Result<(), RemoteSyncError> {
        let invalid = |label: &str, error: serde_json::Error| RemoteSyncError {
            kind: RemoteSyncErrorKind::InvalidBatch,
            message: format!("invalid {label}: {error}"),
        };
        match event.kind {
            EventKind::ReconciliationStart => {
                let payload = serde_json::from_value::<ReconciliationStart>(event.payload.clone())
                    .map_err(|error| invalid("reconciliation start", error))?;
                self.sink.reconcile_start(&payload).await
            }
            EventKind::ReconciliationChunk => {
                let payload = serde_json::from_value::<ReconciliationChunk>(event.payload.clone())
                    .map_err(|error| invalid("reconciliation chunk", error))?;
                self.sink.reconcile_chunk(&payload).await
            }
            EventKind::ReconciliationComplete => {
                let payload =
                    serde_json::from_value::<ReconciliationComplete>(event.payload.clone())
                        .map_err(|error| invalid("reconciliation completion", error))?;
                loop {
                    let progress = self.sink.reconcile_complete(&payload).await?;
                    self.outbox
                        .update_reconciliation_progress(&ReconciliationJobProgress {
                            generation: payload.generation.clone(),
                            run_id: event.run_id.clone(),
                            status: progress.status.clone(),
                            candidate_count: progress.candidate_count,
                            classified_count: progress.classified_count,
                            unresolved_count: progress.unresolved_count,
                            current_cursor: progress.current_cursor.clone(),
                            retry_count: 0,
                            last_error: None,
                        })
                        .await
                        .map_err(|error| RemoteSyncError {
                            kind: RemoteSyncErrorKind::Transient,
                            message: error.to_string(),
                        })?;
                    if progress.superseded
                        || (progress.lifecycle_order_ids.is_empty() && progress.remaining == 0)
                    {
                        break Ok(());
                    }
                    if progress.lifecycle_order_ids.is_empty() {
                        // Let the dedicated critical flusher drain current lifecycle work before
                        // asking SurrealDB to classify another 25 stale rows.
                        while self
                            .critical_backlog_exceeds(
                                self.config.reconciliation_pause_critical_backlog,
                            )
                            .await
                            .map_err(|error| RemoteSyncError {
                                kind: RemoteSyncErrorKind::Transient,
                                message: error.to_string(),
                            })?
                        {
                            tokio::time::sleep(Duration::from_millis(25)).await;
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                    let request_id = progress
                        .lifecycle_request_id
                        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                    let request = ReconciliationLifecycleRequest {
                        generation: payload.generation.clone(),
                        request_id: request_id.clone(),
                        order_ids: progress.lifecycle_order_ids,
                    };
                    let mut request_event = EventEnvelope::new(
                        event.runtime_id.clone(),
                        event.strategy_id.clone(),
                        event.live_session_id.clone(),
                        format!("reconciliation-lifecycle:{request_id}"),
                        now_epoch_micros().max(1) as u64,
                        EventPriority::Reconciliation,
                        EventKind::ReconciliationLifecycleRequest,
                        serde_json::to_value(request)
                            .map_err(|error| invalid("reconciliation lifecycle request", error))?,
                    );
                    request_event.run_id = event.run_id.clone();
                    request_event.event_id = deterministic_event_id(&format!(
                        "reconciliation-lifecycle-request:{}:{}",
                        payload.generation, request_id
                    ));
                    let response = self
                        .bus
                        .request_event(
                            EventSubject::reconciliation_lifecycle_request(
                                &event.runtime_id,
                                &event.strategy_id,
                            ),
                            request_event,
                            Duration::from_secs(15),
                        )
                        .await
                        .map_err(|error| RemoteSyncError {
                            kind: RemoteSyncErrorKind::Transient,
                            message: format!(
                                "broker lifecycle reconciliation request failed: {error}"
                            ),
                        })?;
                    if response.kind != EventKind::ReconciliationLifecycleResult {
                        return Err(RemoteSyncError {
                            kind: RemoteSyncErrorKind::InvalidBatch,
                            message: format!(
                                "broker lifecycle reconciliation returned unexpected event kind {}",
                                response.kind
                            ),
                        });
                    }
                    let result =
                        serde_json::from_value::<ReconciliationLifecycleResult>(response.payload)
                            .map_err(|error| invalid("reconciliation lifecycle result", error))?;
                    if result.generation != payload.generation || result.request_id != request_id {
                        return Err(RemoteSyncError {
                            kind: RemoteSyncErrorKind::InvalidBatch,
                            message:
                                "broker lifecycle reconciliation response did not match its request"
                                    .to_string(),
                        });
                    }
                    self.sink.reconcile_lifecycle_result(&result).await?;
                    while self
                        .critical_backlog_exceeds(self.config.reconciliation_pause_critical_backlog)
                        .await
                        .map_err(|error| RemoteSyncError {
                            kind: RemoteSyncErrorKind::Transient,
                            message: error.to_string(),
                        })?
                    {
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            EventKind::ReconciliationLifecycleRequest
            | EventKind::ReconciliationLifecycleResult => Err(RemoteSyncError {
                kind: RemoteSyncErrorKind::InvalidBatch,
                message: format!(
                    "unexpected durable reconciliation control event kind: {}",
                    event.kind
                ),
            }),
            EventKind::ReconciliationSnapshot => {
                let payload =
                    serde_json::from_value::<ReconciliationSnapshot>(event.payload.clone())
                        .map_err(|error| invalid("legacy reconciliation snapshot", error))?;
                self.sink.reconcile_snapshot(&payload).await
            }
            _ => Err(RemoteSyncError {
                kind: RemoteSyncErrorKind::InvalidBatch,
                message: format!("invalid reconciliation event kind: {}", event.kind),
            }),
        }
    }

    async fn persist_with_bisection(
        &self,
        records: Vec<OutboxEventRecord>,
    ) -> Result<(), SyncAgentError> {
        if let Some((expected, actual)) = first_internal_critical_gap(&records) {
            let detail = format!(
                "local critical batch is non-contiguous: expected sequence {expected}, found {actual}; remote persistence paused for authoritative reconciliation"
            );
            self.set_health_state("degraded", Some(detail.clone()));
            let event_ids = records
                .iter()
                .map(|record| record.event.event_id)
                .collect::<Vec<_>>();
            self.outbox.schedule_retry(&event_ids, 300, &detail).await?;
            return Ok(());
        }
        let mut pending = VecDeque::from([records]);
        let mut authentication_refreshed = false;
        while let Some(records) = pending.pop_front() {
            let mut events: Vec<CloudEvent> =
                records.iter().map(|record| record.event.clone()).collect();
            let sent_at = now_epoch_micros();
            for event in &mut events {
                event.latency.persistence_request_sent_at_us = Some(sent_at);
            }
            let batch = LiveSyncBatch::from_events(events);
            match self.sink.persist_batch(&batch).await {
                Ok(result) => {
                    let ids: Vec<EventId> = batch.events().map(|event| event.event_id).collect();
                    self.outbox
                        .mark_remote_committed(
                            &ids,
                            (!batch.critical_events.is_empty()).then_some(
                                result
                                    .accepted_critical_through
                                    .unwrap_or(result.persisted_critical_through),
                            ),
                        )
                        .await?;
                    let acknowledged_at_us = now_epoch_micros();
                    if result.durability.as_deref() != Some("cloud_stream") {
                        let acknowledged_events = batch.events().cloned().collect::<Vec<_>>();
                        let _ = self
                            .outbox
                            .record_persistence_acknowledgements(
                                &acknowledged_events,
                                acknowledged_at_us,
                            )
                            .await;
                    }
                    self.set_health_state(
                        if result.durability.as_deref() == Some("cloud_stream") {
                            "cloud_accepted"
                        } else {
                            "healthy"
                        },
                        None,
                    );
                }
                Err(error)
                    if error.kind == RemoteSyncErrorKind::Authentication
                        && !authentication_refreshed =>
                {
                    authentication_refreshed = true;
                    if self.sink.refresh_authentication().await.unwrap_or(false) {
                        pending.push_front(records);
                    } else {
                        self.set_health_state("blocked", Some(error.message.clone()));
                        self.schedule_batch_retry(&records, &error).await?;
                    }
                }
                Err(error)
                    if error.kind == RemoteSyncErrorKind::InvalidBatch && records.len() > 1 =>
                {
                    let mut records = records;
                    let midpoint = records.len() / 2;
                    let right = records.split_off(midpoint);
                    pending.push_front(right);
                    pending.push_front(records);
                }
                Err(error) if error.kind == RemoteSyncErrorKind::SequenceConflict => {
                    match self.sink.fetch_remote_cursor().await {
                        Ok(Some(remote_cursor)) => {
                            let (already_persisted, still_pending): (Vec<_>, Vec<_>) =
                                records.into_iter().partition(|record| {
                                    record
                                        .event
                                        .critical_sequence
                                        .is_some_and(|sequence| sequence <= remote_cursor)
                                });
                            if !already_persisted.is_empty() {
                                let ids = already_persisted
                                    .iter()
                                    .map(|record| record.event.event_id)
                                    .collect::<Vec<_>>();
                                self.outbox
                                    .mark_remote_committed(&ids, Some(remote_cursor))
                                    .await?;
                                let events = already_persisted
                                    .iter()
                                    .map(|record| record.event.clone())
                                    .collect::<Vec<_>>();
                                let _ = self
                                    .outbox
                                    .record_persistence_acknowledgements(
                                        &events,
                                        now_epoch_micros(),
                                    )
                                    .await;
                            }
                            if still_pending.is_empty() {
                                self.set_health_state("healthy", None);
                            } else {
                                let detail = format!(
                                    "remote critical cursor is {remote_cursor}; local lifecycle sequence has a gap and is waiting for authoritative reconciliation"
                                );
                                self.set_health_state("degraded", Some(detail.clone()));
                                self.schedule_batch_retry(
                                    &still_pending,
                                    &RemoteSyncError {
                                        kind: RemoteSyncErrorKind::SequenceConflict,
                                        message: detail,
                                    },
                                )
                                .await?;
                            }
                        }
                        Ok(None) | Err(_) => {
                            self.set_health_state("degraded", Some(error.message.clone()));
                            self.schedule_batch_retry(&records, &error).await?;
                        }
                    }
                }
                Err(error)
                    if matches!(
                        error.kind,
                        RemoteSyncErrorKind::InvalidBatch | RemoteSyncErrorKind::Terminal
                    ) =>
                {
                    self.set_health_state("degraded", Some(error.message.clone()));
                    for record in records {
                        self.outbox
                            .move_to_dead_letter(record.event.event_id, &error.message)
                            .await?;
                    }
                }
                Err(error) => {
                    self.set_health_state("degraded", Some(error.message.clone()));
                    self.schedule_batch_retry(&records, &error).await?;
                }
            }
        }
        Ok(())
    }

    async fn schedule_batch_retry(
        &self,
        records: &[OutboxEventRecord],
        error: &RemoteSyncError,
    ) -> Result<(), SyncAgentError> {
        let attempt = records
            .iter()
            .map(|record| record.attempt_count)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        let reconciliation_only = records
            .iter()
            .all(|record| record.event.priority == EventPriority::Reconciliation);
        let delay = if reconciliation_only {
            reconciliation_retry_delay_seconds(attempt)
        } else {
            retry_delay_seconds(attempt)
        };
        let ids: Vec<EventId> = records.iter().map(|record| record.event.event_id).collect();
        self.outbox
            .schedule_retry(&ids, delay, &error.message)
            .await?;
        Ok(())
    }
}

fn first_internal_critical_gap(records: &[OutboxEventRecord]) -> Option<(u64, u64)> {
    let mut previous: Option<u64> = None;
    for sequence in records
        .iter()
        .filter(|record| record.event.priority == EventPriority::Critical)
        .filter_map(|record| record.event.critical_sequence)
    {
        if let Some(previous) = previous {
            let expected = previous.saturating_add(1);
            if sequence != expected {
                return Some((expected, sequence));
            }
        }
        previous = Some(sequence);
    }
    None
}

fn retry_delay_seconds(attempt: u32) -> u64 {
    const RETRIES: [u64; 7] = [1, 2, 5, 15, 30, 60, 300];
    RETRIES[(attempt.saturating_sub(1) as usize).min(RETRIES.len() - 1)]
}

fn reconciliation_retry_delay_seconds(attempt: u32) -> u64 {
    // Reconciliation is bounded and lower priority, so retry it promptly after the cloud
    // recovers. Capping at 30 seconds avoids the generic five-minute stall while the existing
    // critical-backlog gate still prevents cleanup from competing with live lifecycle writes.
    const RETRIES: [u64; 6] = [1, 2, 5, 10, 15, 30];
    RETRIES[(attempt.saturating_sub(1) as usize).min(RETRIES.len() - 1)]
}

#[derive(Debug)]
pub enum SyncAgentError {
    WorkerStopped,
    Bus(EventBusError),
    Outbox(OutboxStoreError),
    InvalidEvent(String),
}

impl fmt::Display for SyncAgentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WorkerStopped => formatter.write_str("live-sync worker stopped unexpectedly"),
            Self::Bus(error) => error.fmt(formatter),
            Self::Outbox(error) => error.fmt(formatter),
            Self::InvalidEvent(message) => write!(formatter, "invalid live-sync event: {message}"),
        }
    }
}

impl std::error::Error for SyncAgentError {}

impl From<EventBusError> for SyncAgentError {
    fn from(value: EventBusError) -> Self {
        Self::Bus(value)
    }
}

impl From<OutboxStoreError> for SyncAgentError {
    fn from(value: OutboxStoreError) -> Self {
        Self::Outbox(value)
    }
}

#[cfg(test)]
#[path = "sync_agent_tests.rs"]
mod tests;
