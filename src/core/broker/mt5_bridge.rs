use super::types::{
    Account, Asset, AssetExchange, AssetStatus, AssetType, Bar, BrokerError, Order, Position,
    Quote, TradeUpdateEvent,
};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use log::{debug, error, info, warn};
use parking_lot::{Mutex, RwLock};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::core::broker::DataStreamMode;
use crate::core::utils::timeframe::{TimeFrame, TimeFrameUnit};

const PROTOCOL_VERSION: u16 = 1;
const DEFAULT_BIND_ADDR: &str = "127.0.0.1:18080";
const DEFAULT_TIMEOUT_MS: u64 = 15_000;
const DEFAULT_HISTORY_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_ORDER_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 60_000;
const DEFAULT_POLL_INTERVAL_MS: u64 = 100;
const MAX_QUEUE_LEN: usize = 10_000;
const MAX_TRADE_EVENT_QUEUE_LEN: usize = 20_000;

static SHARED_MT5_BRIDGE: OnceLock<Arc<Mt5Bridge>> = OnceLock::new();

#[derive(Clone, Debug)]
pub struct Mt5BridgeConfig {
    pub bind_addr: SocketAddr,
    pub token: String,
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    pub poll_interval: Duration,
    pub symbol_map: HashMap<String, String>,
}

impl Mt5BridgeConfig {
    pub fn from_env() -> Result<Self, BrokerError> {
        let bind_addr = std::env::var("AQE_MT5_BRIDGE_BIND_ADDR")
            .unwrap_or_else(|_| DEFAULT_BIND_ADDR.to_string())
            .parse::<SocketAddr>()
            .map_err(|error| {
                BrokerError::ConnectionError(format!("invalid AQE_MT5_BRIDGE_BIND_ADDR: {}", error))
            })?;

        let token = std::env::var("AQE_MT5_BRIDGE_TOKEN").map_err(|_| {
            BrokerError::ConnectionError("AQE_MT5_BRIDGE_TOKEN is required".to_string())
        })?;

        let request_timeout = Duration::from_millis(read_env_u64(
            "AQE_MT5_REQUEST_TIMEOUT_MS",
            DEFAULT_TIMEOUT_MS,
        ));
        let connect_timeout = Duration::from_millis(read_env_u64(
            "AQE_MT5_CONNECT_TIMEOUT_MS",
            DEFAULT_CONNECT_TIMEOUT_MS,
        ));
        let poll_interval = Duration::from_millis(read_env_u64(
            "AQE_MT5_POLL_INTERVAL_MS",
            DEFAULT_POLL_INTERVAL_MS,
        ));
        let symbol_map = parse_symbol_map(std::env::var("AQE_MT5_SYMBOL_MAP").ok());

        Ok(Self {
            bind_addr,
            token,
            request_timeout,
            connect_timeout,
            poll_interval,
            symbol_map,
        })
    }

    pub fn mt5_symbol<'a>(&'a self, aqe_symbol: &'a str) -> &'a str {
        self.symbol_map
            .get(aqe_symbol)
            .map(String::as_str)
            .unwrap_or(aqe_symbol)
    }

    pub fn aqe_symbol(&self, mt5_symbol: &str) -> String {
        self.symbol_map
            .iter()
            .find_map(|(aqe, mt5)| (mt5 == mt5_symbol).then(|| aqe.clone()))
            .unwrap_or_else(|| mt5_symbol.to_string())
    }
}

fn read_env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn parse_symbol_map(value: Option<String>) -> HashMap<String, String> {
    value
        .unwrap_or_default()
        .split(',')
        .filter_map(|pair| {
            let (left, right) = pair.split_once('=')?;
            let left = left.trim();
            let right = right.trim();
            (!left.is_empty() && !right.is_empty()).then(|| (left.to_string(), right.to_string()))
        })
        .collect()
}

#[derive(Clone)]
pub struct Mt5Bridge {
    config: Mt5BridgeConfig,
    session_id: Arc<RwLock<String>>,
    state: Arc<RwLock<Mt5BridgeState>>,
    rpc_queue: Arc<Mutex<VecDeque<Mt5RpcRequest>>>,
    rpc_queue_notify: Arc<Notify>,
    pending_rpc: Arc<Mutex<HashMap<String, oneshot::Sender<Mt5RpcResponsePayload>>>>,
    event_queue: Arc<Mutex<VecDeque<QueuedTradeEvent>>>,
    local_trade_event_seq: Arc<AtomicU64>,
    trade_subscribers: Arc<Mutex<Vec<Arc<dyn Fn((Order, TradeUpdateEvent)) + Send + Sync>>>>,
    bar_subscribers: Arc<Mutex<Vec<Mt5BarSubscription>>>,
    server_started: Arc<AtomicBool>,
    server_shutdown: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    server_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

#[derive(Default)]
struct Mt5BridgeState {
    account: Option<Account>,
    assets: HashMap<String, Asset>,
    orders: HashMap<String, Order>,
    positions: HashMap<String, Position>,
    latest_quotes: HashMap<String, Quote>,
    latest_bars: HashMap<String, Bar>,
    latest_bars_by_stream: HashMap<(String, String), Bar>,
    latest_intrabar_bars: HashMap<(String, String), Bar>,
    history: HashMap<String, Vec<Bar>>,
    seen_event_ids: HashSet<String>,
    active_sessions: HashMap<String, DateTime<Utc>>,
    last_rpc_poll_attempt: Option<DateTime<Utc>>,
    last_rpc_poll: Option<DateTime<Utc>>,
    last_rpc_poll_session_id: Option<String>,
    last_rpc_poll_auth_error: Option<String>,
    last_rpc_poll_request_count: Option<usize>,
    last_rpc_delivery: Option<DateTime<Utc>>,
    last_rpc_delivery_request_id: Option<String>,
    last_rpc_delivery_action: Option<String>,
    last_rpc_delivery_count: Option<usize>,
    last_rpc_response: Option<DateTime<Utc>>,
    last_rpc_response_request_id: Option<String>,
    last_unknown_rpc_response_request_id: Option<String>,
    last_heartbeat: Option<DateTime<Utc>>,
    terminal_name: Option<String>,
    dropped_trade_event_count: u64,
    last_trade_event_sequence: Option<u64>,
}

#[derive(Clone, Debug)]
struct QueuedTradeEvent {
    sequence: u64,
    order: Order,
    event: TradeUpdateEvent,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mt5BridgeDiagnostics {
    pub bind_addr: String,
    pub session_id: String,
    pub last_poll_attempt_at: Option<DateTime<Utc>>,
    pub last_authorized_poll_at: Option<DateTime<Utc>>,
    pub last_poll_session_id: Option<String>,
    pub last_poll_auth_error: Option<String>,
    pub last_poll_request_count: Option<usize>,
    pub last_delivery_at: Option<DateTime<Utc>>,
    pub last_delivery_request_id: Option<String>,
    pub last_delivery_action: Option<String>,
    pub last_delivery_count: Option<usize>,
    pub pending_rpc_count: usize,
    pub queued_rpc_count: usize,
    pub last_response_at: Option<DateTime<Utc>>,
    pub last_response_request_id: Option<String>,
    pub last_unknown_response_request_id: Option<String>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub queued_trade_event_count: usize,
    pub dropped_trade_event_count: u64,
    pub last_trade_event_sequence: Option<u64>,
}

#[derive(Clone)]
struct Mt5BarSubscription {
    session_id: String,
    symbol: String,
    timeframe: TimeFrame,
    timeframe_code: String,
    mode: DataStreamMode,
    callback: Arc<dyn Fn(Bar) + Send + Sync>,
}

struct PendingMt5RpcRequest {
    request_id: String,
    rpc_queue: Arc<Mutex<VecDeque<Mt5RpcRequest>>>,
    pending_rpc: Arc<Mutex<HashMap<String, oneshot::Sender<Mt5RpcResponsePayload>>>>,
    cleanup_enabled: bool,
}

impl PendingMt5RpcRequest {
    fn new(
        request_id: String,
        rpc_queue: Arc<Mutex<VecDeque<Mt5RpcRequest>>>,
        pending_rpc: Arc<Mutex<HashMap<String, oneshot::Sender<Mt5RpcResponsePayload>>>>,
    ) -> Self {
        Self {
            request_id,
            rpc_queue,
            pending_rpc,
            cleanup_enabled: true,
        }
    }

    fn dismiss(&mut self) {
        self.cleanup_enabled = false;
    }

    fn cleanup(&self) {
        self.pending_rpc.lock().remove(&self.request_id);
        self.rpc_queue
            .lock()
            .retain(|request| request.request_id != self.request_id);
    }
}

impl Drop for PendingMt5RpcRequest {
    fn drop(&mut self) {
        if self.cleanup_enabled {
            self.cleanup();
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Mt5SubscriptionSpec {
    symbol: String,
    timeframe_code: String,
}

impl Mt5Bridge {
    pub fn shared_from_env() -> Result<Arc<Self>, BrokerError> {
        let config = Mt5BridgeConfig::from_env()?;
        Ok(SHARED_MT5_BRIDGE
            .get_or_init(|| Arc::new(Self::new(config)))
            .clone())
    }

    pub fn new(config: Mt5BridgeConfig) -> Self {
        Self {
            config,
            session_id: Arc::new(RwLock::new(Uuid::new_v4().to_string())),
            state: Arc::new(RwLock::new(Mt5BridgeState::default())),
            rpc_queue: Arc::new(Mutex::new(VecDeque::new())),
            rpc_queue_notify: Arc::new(Notify::new()),
            pending_rpc: Arc::new(Mutex::new(HashMap::new())),
            event_queue: Arc::new(Mutex::new(VecDeque::new())),
            local_trade_event_seq: Arc::new(AtomicU64::new(0)),
            trade_subscribers: Arc::new(Mutex::new(Vec::new())),
            bar_subscribers: Arc::new(Mutex::new(Vec::new())),
            server_started: Arc::new(AtomicBool::new(false)),
            server_shutdown: Arc::new(Mutex::new(None)),
            server_handle: Arc::new(Mutex::new(None)),
        }
    }

    pub fn config(&self) -> &Mt5BridgeConfig {
        &self.config
    }

    pub fn set_session_id(&self, session_id: impl Into<String>) {
        let session_id = session_id.into();
        let changed = {
            let mut current = self.session_id.write();
            if *current == session_id {
                false
            } else {
                *current = session_id.clone();
                true
            }
        };
        if changed {
            self.clear_session_runtime_state();
        }
        self.register_session(&session_id);
    }

    pub fn session_id(&self) -> String {
        self.session_id.read().clone()
    }

    fn register_session(&self, session_id: &str) {
        if session_id.is_empty() {
            return;
        }
        self.state
            .write()
            .active_sessions
            .insert(session_id.to_string(), Utc::now());
    }

    fn clear_session_runtime_state(&self) {
        self.rpc_queue.lock().clear();
        self.pending_rpc.lock().clear();
        self.event_queue.lock().clear();
        self.trade_subscribers.lock().clear();
        self.bar_subscribers.lock().clear();
        let mut state = self.state.write();
        state.account = None;
        state.assets.clear();
        state.orders.clear();
        state.positions.clear();
        state.latest_quotes.clear();
        state.latest_bars.clear();
        state.latest_bars_by_stream.clear();
        state.latest_intrabar_bars.clear();
        state.history.clear();
        state.seen_event_ids.clear();
        state.active_sessions.clear();
        state.last_rpc_poll_attempt = None;
        state.last_rpc_poll = None;
        state.last_rpc_poll_session_id = None;
        state.last_rpc_poll_auth_error = None;
        state.last_rpc_poll_request_count = None;
        state.last_rpc_delivery = None;
        state.last_rpc_delivery_request_id = None;
        state.last_rpc_delivery_action = None;
        state.last_rpc_delivery_count = None;
        state.last_rpc_response = None;
        state.last_rpc_response_request_id = None;
        state.last_unknown_rpc_response_request_id = None;
        state.last_heartbeat = None;
        state.terminal_name = None;
        state.dropped_trade_event_count = 0;
        state.last_trade_event_sequence = None;
        self.local_trade_event_seq.store(0, Ordering::Relaxed);
    }

    fn reset_connection_markers(&self) {
        let mut state = self.state.write();
        state.last_rpc_poll_attempt = None;
        state.last_rpc_poll = None;
        state.last_rpc_poll_session_id = None;
        state.last_rpc_poll_auth_error = None;
        state.last_rpc_poll_request_count = None;
        state.last_rpc_delivery = None;
        state.last_rpc_delivery_request_id = None;
        state.last_rpc_delivery_action = None;
        state.last_rpc_delivery_count = None;
        state.last_rpc_response = None;
        state.last_rpc_response_request_id = None;
        state.last_unknown_rpc_response_request_id = None;
        state.last_heartbeat = None;
    }

    fn record_rpc_poll_attempt(&self, session_id: &str) {
        let mut state = self.state.write();
        state.last_rpc_poll_attempt = Some(Utc::now());
        state.last_rpc_poll_session_id = (!session_id.is_empty()).then(|| session_id.to_string());
    }

    fn record_rpc_poll_auth_failure(&self, message: impl Into<String>) {
        self.state.write().last_rpc_poll_auth_error = Some(message.into());
    }

    fn record_rpc_poll(&self, session_id: &str) {
        let mut state = self.state.write();
        let now = Utc::now();
        state.last_rpc_poll_attempt = Some(now);
        state.last_rpc_poll = Some(now);
        state.last_rpc_poll_session_id = (!session_id.is_empty()).then(|| session_id.to_string());
        state.last_rpc_poll_auth_error = None;
        if !session_id.is_empty() {
            state.active_sessions.insert(session_id.to_string(), now);
        }
    }

    fn record_rpc_poll_delivery(&self, requests: &[Mt5RpcRequest]) {
        let mut state = self.state.write();
        state.last_rpc_poll_request_count = Some(requests.len());
        if let Some(request) = requests.last() {
            state.last_rpc_delivery = Some(Utc::now());
            state.last_rpc_delivery_request_id = Some(request.request_id.clone());
            state.last_rpc_delivery_action = Some(format!("{:?}", request.action));
            state.last_rpc_delivery_count = Some(requests.len());
        }
    }

    fn drain_rpc_requests(&self, max_requests: usize) -> (Vec<Mt5RpcRequest>, usize) {
        let mut queue = self.rpc_queue.lock();
        let mut requests = Vec::new();
        while requests.len() < max_requests {
            let Some(request) = queue.pop_front() else {
                break;
            };
            requests.push(request);
        }
        let queued_remaining = queue.len();
        (requests, queued_remaining)
    }

    fn is_rpc_polling(&self) -> bool {
        self.state
            .read()
            .last_rpc_poll
            .map(|poll| Utc::now() - poll < chrono::Duration::seconds(15))
            .unwrap_or(false)
    }

    pub fn diagnostics(&self) -> Mt5BridgeDiagnostics {
        let (
            last_poll_attempt_at,
            last_authorized_poll_at,
            last_poll_session_id,
            last_poll_auth_error,
            last_poll_request_count,
            last_delivery_at,
            last_delivery_request_id,
            last_delivery_action,
            last_delivery_count,
            last_response_at,
            last_response_request_id,
            last_unknown_response_request_id,
            last_heartbeat_at,
            dropped_trade_event_count,
            last_trade_event_sequence,
        ) = {
            let state = self.state.read();
            (
                state.last_rpc_poll_attempt,
                state.last_rpc_poll,
                state.last_rpc_poll_session_id.clone(),
                state.last_rpc_poll_auth_error.clone(),
                state.last_rpc_poll_request_count,
                state.last_rpc_delivery,
                state.last_rpc_delivery_request_id.clone(),
                state.last_rpc_delivery_action.clone(),
                state.last_rpc_delivery_count,
                state.last_rpc_response,
                state.last_rpc_response_request_id.clone(),
                state.last_unknown_rpc_response_request_id.clone(),
                state.last_heartbeat,
                state.dropped_trade_event_count,
                state.last_trade_event_sequence,
            )
        };
        Mt5BridgeDiagnostics {
            bind_addr: self.config.bind_addr.to_string(),
            session_id: self.session_id(),
            last_poll_attempt_at,
            last_authorized_poll_at,
            last_poll_session_id,
            last_poll_auth_error,
            last_poll_request_count,
            last_delivery_at,
            last_delivery_request_id,
            last_delivery_action,
            last_delivery_count,
            pending_rpc_count: self.pending_rpc.lock().len(),
            queued_rpc_count: self.rpc_queue.lock().len(),
            last_response_at,
            last_response_request_id,
            last_unknown_response_request_id,
            last_heartbeat_at,
            queued_trade_event_count: self.event_queue.lock().len(),
            dropped_trade_event_count,
            last_trade_event_sequence,
        }
    }

    pub async fn start(self: &Arc<Self>) -> Result<bool, BrokerError> {
        if self.server_started.swap(true, Ordering::SeqCst) {
            return Ok(true);
        }

        self.reset_connection_markers();

        let bind_addr = self.config.bind_addr;
        let app = Router::new()
            .route("/health", get(health))
            .route("/v1/heartbeat", post(heartbeat))
            .route("/v1/snapshot", post(snapshot))
            .route("/v1/market-data", post(market_data))
            .route("/v1/trade-event", post(trade_event))
            .route("/v1/trade-events", post(trade_events))
            .route("/v1/rpc/poll", post(poll_rpc))
            .route("/v1/rpc/response", post(rpc_response))
            .with_state(self.clone());

        let listener = tokio::net::TcpListener::bind(bind_addr)
            .await
            .map_err(|error| {
                self.server_started.store(false, Ordering::SeqCst);
                BrokerError::ConnectionError(format!(
                    "failed to bind MT5 bridge at {}: {}",
                    bind_addr, error
                ))
            })?;

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        info!("AQE MT5 bridge listening on http://{}", bind_addr);
        let handle = tokio::spawn(async move {
            let server = axum::serve(listener, app).with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            });
            if let Err(error) = server.await {
                error!("AQE MT5 bridge server stopped: {}", error);
            }
        });
        *self.server_shutdown.lock() = Some(shutdown_tx);
        *self.server_handle.lock() = Some(handle);
        Ok(true)
    }

    pub fn stop(&self) {
        if let Some(shutdown) = self.server_shutdown.lock().take() {
            let _ = shutdown.send(());
        }
        if let Some(handle) = self.server_handle.lock().take() {
            handle.abort();
        }
        self.server_started.store(false, Ordering::SeqCst);
        self.clear_session_runtime_state();
    }

    pub async fn shutdown(&self) {
        if let Some(shutdown) = self.server_shutdown.lock().take() {
            let _ = shutdown.send(());
        }

        if let Some(mut handle) = self.server_handle.lock().take() {
            match tokio::time::timeout(Duration::from_secs(2), &mut handle).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) if error.is_cancelled() => {}
                Ok(Err(error)) => {
                    warn!("MT5 bridge server task failed during shutdown: {}", error);
                }
                Err(_) => {
                    warn!("MT5 bridge server did not stop gracefully; aborting task");
                    handle.abort();
                    let _ = handle.await;
                }
            }
        }

        self.server_started.store(false, Ordering::SeqCst);
        self.clear_session_runtime_state();
    }

    pub async fn wait_for_rpc_poll(&self) -> Result<bool, BrokerError> {
        if self.is_rpc_polling() {
            return Ok(true);
        }

        let timeout = self.config.connect_timeout.max(Duration::from_millis(100));
        let deadline = tokio::time::Instant::now() + timeout;
        let mut sleep_for = self.config.poll_interval;
        if sleep_for < Duration::from_millis(50) {
            sleep_for = Duration::from_millis(50);
        }
        if sleep_for > Duration::from_millis(250) {
            sleep_for = Duration::from_millis(250);
        }

        info!(
            "Waiting for MT5 EA to poll bridge at {} for session {}",
            self.config.bind_addr,
            self.session_id()
        );

        loop {
            if self.is_rpc_polling() {
                return Ok(true);
            }
            if tokio::time::Instant::now() >= deadline {
                let diagnostics = self.diagnostics();
                let detail = if diagnostics.last_poll_attempt_at.is_none() {
                    if diagnostics.last_heartbeat_at.is_some() {
                        "MT5 heartbeat traffic reached this bridge, but no /v1/rpc/poll request reached it."
                            .to_string()
                    } else {
                        "No /v1/rpc/poll request reached this bridge.".to_string()
                    }
                } else if let Some(error) = diagnostics.last_poll_auth_error {
                    format!("The EA reached this bridge but poll authorization failed: {error}.")
                } else if let Some(last_poll) = diagnostics.last_authorized_poll_at {
                    let age_ms = (Utc::now() - last_poll).num_milliseconds();
                    format!(
                        "The EA last completed an authorized poll {age_ms}ms ago, but no fresh poll arrived for this check."
                    )
                } else {
                    "The EA reached this bridge but did not complete an authorized poll."
                        .to_string()
                };
                return Err(BrokerError::ConnectionError(format!(
                    "MT5 EA did not complete an authorized poll at {} within {:?}. {} Check the EA InpBridgeUrl, token, and MT5 WebRequest allow-list.",
                    self.config.bind_addr, timeout, detail
                )));
            }
            tokio::time::sleep(sleep_for).await;
        }
    }

    pub fn is_connected(&self) -> bool {
        let state = self.state.read();
        let heartbeat_connected = state
            .last_heartbeat
            .map(|heartbeat| Utc::now() - heartbeat < chrono::Duration::seconds(15))
            .unwrap_or(false);
        let rpc_polling = state
            .last_rpc_poll
            .map(|poll| Utc::now() - poll < chrono::Duration::seconds(15))
            .unwrap_or(false);
        heartbeat_connected || rpc_polling
    }

    pub fn account(&self) -> Result<Account, BrokerError> {
        self.state
            .read()
            .account
            .clone()
            .ok_or_else(|| BrokerError::AccountError("MT5 account snapshot not available".into()))
    }

    pub async fn request_account(&self) -> Result<Account, BrokerError> {
        let payload = self
            .request_rpc(Mt5RpcAction::GetAccount, serde_json::json!({}))
            .await?;
        let account = deserialize_payload::<Account>(payload, "MT5 account")?;
        self.state.write().account = Some(account.clone());
        Ok(account)
    }

    pub fn orders(&self) -> Vec<Order> {
        self.state.read().orders.values().cloned().collect()
    }

    pub async fn request_orders(&self) -> Result<Vec<Order>, BrokerError> {
        let payload = self
            .request_rpc(Mt5RpcAction::GetOrders, serde_json::json!({}))
            .await?;
        let mut orders = deserialize_payload::<Vec<Order>>(payload, "MT5 orders")?;
        for order in &mut orders {
            self.map_order_to_aqe(order);
        }
        let mut state = self.state.write();
        state.orders.clear();
        for order in &orders {
            state.orders.insert(order.order_id.clone(), order.clone());
        }
        Ok(orders)
    }

    pub fn order(&self, order_id: &str) -> Result<Order, BrokerError> {
        self.state
            .read()
            .orders
            .get(order_id)
            .cloned()
            .ok_or_else(|| BrokerError::OrderError(format!("MT5 order {} not found", order_id)))
    }

    pub fn positions(&self) -> Vec<Position> {
        self.state.read().positions.values().cloned().collect()
    }

    pub async fn request_positions(&self) -> Result<Vec<Position>, BrokerError> {
        let payload = self
            .request_rpc(Mt5RpcAction::GetPositions, serde_json::json!({}))
            .await?;
        let mut positions = deserialize_payload::<Vec<Position>>(payload, "MT5 positions")?;
        for position in &mut positions {
            self.map_asset_to_aqe(&mut position.asset);
        }
        let mut state = self.state.write();
        state.positions.clear();
        for position in &positions {
            state
                .positions
                .insert(position.asset.symbol.clone(), position.clone());
        }
        Ok(positions)
    }

    pub fn position(&self, symbol: &str) -> Result<Position, BrokerError> {
        self.state
            .read()
            .positions
            .get(symbol)
            .cloned()
            .ok_or_else(|| BrokerError::PositionError(format!("MT5 position {} not found", symbol)))
    }

    pub fn asset(&self, symbol: &str) -> Asset {
        self.state
            .read()
            .assets
            .get(symbol)
            .cloned()
            .unwrap_or_else(|| default_mt5_asset(symbol))
    }

    pub async fn request_asset(&self, symbol: &str) -> Result<Asset, BrokerError> {
        let mt5_symbol = self.config.mt5_symbol(symbol).to_string();
        let payload = self
            .request_rpc(
                Mt5RpcAction::GetTickerInfo,
                serde_json::json!({ "symbol": mt5_symbol }),
            )
            .await?;
        let mut asset = deserialize_payload::<Asset>(payload, "MT5 asset")?;
        self.map_asset_to_aqe(&mut asset);
        self.state
            .write()
            .assets
            .insert(asset.symbol.clone(), asset.clone());
        Ok(asset)
    }

    pub fn latest_quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
        self.state
            .read()
            .latest_quotes
            .get(symbol)
            .cloned()
            .ok_or_else(|| {
                BrokerError::DataFeedError(format!("MT5 quote {} not available", symbol))
            })
    }

    pub async fn request_latest_quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
        let mt5_symbol = self.config.mt5_symbol(symbol).to_string();
        let payload = self
            .request_rpc(
                Mt5RpcAction::GetLatestQuote,
                serde_json::json!({ "symbol": mt5_symbol }),
            )
            .await?;
        let mut quote = deserialize_payload::<Quote>(payload, "MT5 quote")?;
        quote.symbol = self.config.aqe_symbol(&quote.symbol);
        self.state
            .write()
            .latest_quotes
            .insert(quote.symbol.clone(), quote.clone());
        Ok(quote)
    }

    pub fn latest_bar(&self, symbol: &str) -> Result<Bar, BrokerError> {
        self.state
            .read()
            .latest_bars
            .get(symbol)
            .cloned()
            .ok_or_else(|| BrokerError::DataFeedError(format!("MT5 bar {} not available", symbol)))
    }

    pub async fn request_latest_bar(&self, symbol: &str) -> Result<Bar, BrokerError> {
        let mt5_symbol = self.config.mt5_symbol(symbol).to_string();
        let payload = self
            .request_rpc(
                Mt5RpcAction::GetLatestBar,
                serde_json::json!({ "symbol": mt5_symbol }),
            )
            .await?;
        let mut bar = deserialize_payload::<Bar>(payload, "MT5 bar")?;
        bar.symbol = self.config.aqe_symbol(&bar.symbol);
        self.state
            .write()
            .latest_bars
            .insert(bar.symbol.clone(), bar.clone());
        Ok(bar)
    }

    pub fn history(&self, symbol: &str, start: DateTime<Utc>, end: DateTime<Utc>) -> Vec<Bar> {
        self.state
            .read()
            .history
            .get(symbol)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|bar| bar.timestamp >= start && bar.timestamp <= end)
            .collect()
    }

    pub async fn request_history(
        &self,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        time_frame: TimeFrame,
    ) -> Result<DataFrame, BrokerError> {
        let mt5_symbol = self.config.mt5_symbol(symbol).to_string();
        let timeframe = mt5_timeframe_code(time_frame)?;
        let payload = self
            .request_rpc_with_timeout(
                Mt5RpcAction::GetHistory,
                serde_json::json!({
                    "symbol": mt5_symbol,
                    "start": start,
                    "end": end,
                    "start_ts": start.timestamp(),
                    "end_ts": end.timestamp(),
                    "timeframe": timeframe,
                }),
                self.config
                    .request_timeout
                    .max(Duration::from_millis(DEFAULT_HISTORY_TIMEOUT_MS)),
            )
            .await?;
        let mut bars = deserialize_payload::<Vec<Bar>>(payload, "MT5 history")?;
        for bar in &mut bars {
            bar.symbol = self.config.aqe_symbol(&bar.symbol);
        }
        self.state
            .write()
            .history
            .insert(symbol.to_string(), bars.clone());
        bars_to_frame(bars)
    }

    pub async fn request_order_action(
        &self,
        action: Mt5RpcAction,
        order: Option<Order>,
        payload: Value,
    ) -> Result<Order, BrokerError> {
        let response = self
            .request_rpc_with_timeout(
                action,
                payload,
                self.config
                    .request_timeout
                    .max(Duration::from_millis(DEFAULT_ORDER_TIMEOUT_MS)),
            )
            .await?;
        let mut broker_order = deserialize_payload::<Order>(response, "MT5 order")?;
        self.map_order_to_aqe(&mut broker_order);
        if let Some(mut local_order) = order {
            local_order.order_id = broker_order.order_id.clone();
            local_order.filled_qty = broker_order.filled_qty;
            local_order.filled_price = broker_order.filled_price;
            local_order.status = broker_order.status.clone();
            local_order.filled_at = broker_order.filled_at;
            local_order.rejection_reason = broker_order.rejection_reason.clone();
            self.upsert_order(local_order.clone());
            self.emit_order_event(local_order.clone(), local_order.status.clone());
            Ok(local_order)
        } else {
            self.upsert_order(broker_order.clone());
            self.emit_order_event(broker_order.clone(), broker_order.status.clone());
            Ok(broker_order)
        }
    }

    pub async fn request_rpc(
        &self,
        action: Mt5RpcAction,
        payload: Value,
    ) -> Result<Value, BrokerError> {
        self.request_rpc_with_timeout(action, payload, self.config.request_timeout)
            .await
    }

    pub async fn request_rpc_with_timeout(
        &self,
        action: Mt5RpcAction,
        payload: Value,
        request_timeout: Duration,
    ) -> Result<Value, BrokerError> {
        let mut remaining_attempts = 2;

        loop {
            let request_id = Uuid::new_v4().to_string();
            let (tx, rx) = oneshot::channel();
            self.pending_rpc.lock().insert(request_id.clone(), tx);

            let queued_len = {
                let mut queue = self.rpc_queue.lock();
                if queue.len() >= MAX_QUEUE_LEN {
                    self.pending_rpc.lock().remove(&request_id);
                    return Err(BrokerError::TradeError(
                        "MT5 RPC queue is full; bridge is not keeping up".into(),
                    ));
                }
                queue.push_back(Mt5RpcRequest {
                    session_id: String::new(),
                    request_id: request_id.clone(),
                    action,
                    payload: payload.clone(),
                });
                queue.len()
            };
            self.rpc_queue_notify.notify_one();
            let mut pending_guard = PendingMt5RpcRequest::new(
                request_id.clone(),
                self.rpc_queue.clone(),
                self.pending_rpc.clone(),
            );
            debug!(
                "Queued MT5 RPC request {:?} id={} pending={} queued={}",
                action,
                request_id,
                self.pending_rpc.lock().len(),
                queued_len
            );

            match tokio::time::timeout(request_timeout, rx).await {
                Ok(Ok(response)) if response.ok => {
                    pending_guard.dismiss();
                    return Ok(response.payload.unwrap_or(Value::Null));
                }
                Ok(Ok(response)) => {
                    pending_guard.dismiss();
                    return Err(BrokerError::TradeError(
                        response
                            .message
                            .unwrap_or_else(|| "MT5 RPC request failed".to_string()),
                    ));
                }
                Ok(Err(_)) => {
                    pending_guard.cleanup();
                    pending_guard.dismiss();
                    return Err(BrokerError::ConnectionError(format!(
                        "MT5 RPC response channel closed for request {}",
                        request_id
                    )));
                }
                Err(_) => {
                    let diagnostics = self.diagnostics();
                    let last_poll_age_ms = diagnostics
                        .last_authorized_poll_at
                        .map(|poll| (Utc::now() - poll).num_milliseconds());
                    let request_was_queued = self
                        .rpc_queue
                        .lock()
                        .iter()
                        .any(|request| request.request_id == request_id);
                    let request_was_pending = self.pending_rpc.lock().contains_key(&request_id);
                    pending_guard.cleanup();
                    pending_guard.dismiss();
                    warn!(
                        "MT5 RPC request {:?} id={} timed out after {:?}. queued_before_cleanup={} pending_before_cleanup={} last_poll_age_ms={:?} last_poll_requests={:?} last_delivery={:?} last_delivery_action={:?} last_response={:?}. Check that the EA can reach the bridge URL and is polling /v1/rpc/poll.",
                        action,
                        request_id,
                        request_timeout,
                        request_was_queued,
                        request_was_pending,
                        last_poll_age_ms,
                        diagnostics.last_poll_request_count,
                        diagnostics.last_delivery_request_id,
                        diagnostics.last_delivery_action,
                        diagnostics.last_response_request_id
                    );

                    if remaining_attempts > 1 && request_was_queued && request_was_pending {
                        remaining_attempts -= 1;
                        warn!(
                            "MT5 RPC request {:?} id={} timed out before delivery; waiting for the EA poll loop and retrying once",
                            action, request_id
                        );
                        self.wait_for_rpc_poll().await?;
                        continue;
                    }

                    return Err(BrokerError::ConnectionError(format!(
                        "MT5 RPC request {:?} timed out after {:?} request_id={} queued_before_cleanup={} pending_before_cleanup={} last_poll_age_ms={:?} last_poll_requests={:?} last_delivery={:?} last_delivery_action={:?} last_response={:?}",
                        action,
                        request_timeout,
                        request_id,
                        request_was_queued,
                        request_was_pending,
                        last_poll_age_ms,
                        diagnostics.last_poll_request_count,
                        diagnostics.last_delivery_request_id,
                        diagnostics.last_delivery_action,
                        diagnostics.last_response_request_id
                    )));
                }
            }
        }
    }

    pub fn enqueue_rpc(&self, request: Mt5RpcRequest) -> Result<(), BrokerError> {
        let mut request = request;
        if request.session_id.is_empty() {
            request.session_id = self.session_id();
        }
        let mut queue = self.rpc_queue.lock();
        if queue.len() >= MAX_QUEUE_LEN {
            return Err(BrokerError::TradeError(
                "MT5 RPC queue is full; bridge is not keeping up".into(),
            ));
        }
        queue.push_back(request);
        Ok(())
    }

    pub fn upsert_order(&self, order: Order) -> Order {
        let mut state = self.state.write();
        let mut order = order;
        if let Some(existing) = state.orders.get(&order.order_id) {
            if order.insight_id.is_none() {
                order.insight_id = existing.insight_id.clone();
            }
            if order.strategy_type.is_none() {
                order.strategy_type = existing.strategy_type.clone();
            }
            if order.legs.is_none() {
                order.legs = existing.legs.clone();
            }
        }
        state.orders.insert(order.order_id.clone(), order.clone());
        order
    }

    pub fn emit_order_event(&self, order: Order, event: TradeUpdateEvent) {
        let sequence = self.local_trade_event_seq.fetch_add(1, Ordering::Relaxed) + 1;
        self.enqueue_trade_event(sequence, order, event);
    }

    pub fn drain_trade_events(&self) -> Vec<(Order, TradeUpdateEvent)> {
        let mut events = self.event_queue.lock();
        let mut events = events.drain(..).collect::<Vec<_>>();
        events.sort_by_key(|event| event.sequence);
        events
            .into_iter()
            .map(|event| (event.order, event.event))
            .collect()
    }

    pub fn subscribe_trade_stream(
        &self,
        on_trade: Arc<dyn Fn((Order, TradeUpdateEvent)) + Send + Sync>,
    ) {
        self.trade_subscribers.lock().push(on_trade);
    }

    pub fn clear_trade_subscribers(&self) {
        self.trade_subscribers.lock().clear();
    }

    fn current_subscription_specs(&self) -> Vec<Mt5SubscriptionSpec> {
        let session_id = self.session_id();
        let mut specs = HashSet::new();
        for subscriber in self.bar_subscribers.lock().iter() {
            if subscriber.session_id != session_id {
                continue;
            }
            specs.insert(Mt5SubscriptionSpec {
                symbol: subscriber.symbol.clone(),
                timeframe_code: subscriber.timeframe_code.clone(),
            });
        }
        let mut specs: Vec<_> = specs.into_iter().collect();
        specs.sort_by(|left, right| {
            left.symbol
                .cmp(&right.symbol)
                .then(left.timeframe_code.cmp(&right.timeframe_code))
        });
        specs
    }

    async fn sync_bar_subscriptions(&self) -> Result<(), BrokerError> {
        let specs = self.current_subscription_specs();
        if specs.is_empty() {
            info!("MT5 bridge clearing bar subscriptions");
            self.request_rpc(Mt5RpcAction::UnsubscribeBars, serde_json::json!({}))
                .await?;
            return Ok(());
        }

        let subscriptions: Vec<_> = specs
            .into_iter()
            .map(|spec| {
                format!(
                    "{}|{}",
                    self.config.mt5_symbol(&spec.symbol),
                    spec.timeframe_code
                )
            })
            .collect();

        info!(
            "MT5 bridge syncing {} bar subscription(s): {}",
            subscriptions.len(),
            subscriptions.join(", ")
        );

        self.request_rpc(
            Mt5RpcAction::SubscribeBars,
            serde_json::json!({ "symbols": subscriptions }),
        )
        .await?;
        Ok(())
    }

    pub fn subscribe_bars(
        &self,
        symbols: Vec<String>,
        timeframe: TimeFrame,
        mode: DataStreamMode,
        on_bar: Arc<dyn Fn(Bar) + Send + Sync>,
    ) -> Result<(), BrokerError> {
        let timeframe_code = mt5_timeframe_code(timeframe)?;
        let session_id = self.session_id();
        let mut subscribers = self.bar_subscribers.lock();
        for symbol in symbols {
            subscribers.push(Mt5BarSubscription {
                session_id: session_id.clone(),
                symbol,
                timeframe,
                timeframe_code: timeframe_code.clone(),
                mode,
                callback: on_bar.clone(),
            });
        }
        Ok(())
    }

    pub async fn subscribe_bars_rpc(
        &self,
        symbols: Vec<String>,
        timeframe: TimeFrame,
        mode: DataStreamMode,
        on_bar: Arc<dyn Fn(Bar) + Send + Sync>,
    ) -> Result<(), BrokerError> {
        self.subscribe_bars(symbols, timeframe, mode, on_bar)?;
        self.sync_bar_subscriptions().await
    }

    pub fn unsubscribe_bars(
        &self,
        symbols: Vec<String>,
        timeframe: TimeFrame,
        mode: DataStreamMode,
    ) -> Result<(), BrokerError> {
        let timeframe_code = mt5_timeframe_code(timeframe)?;
        let symbol_set: HashSet<_> = symbols.into_iter().collect();
        let session_id = self.session_id();
        self.bar_subscribers.lock().retain(|subscriber| {
            !(subscriber.session_id == session_id
                && symbol_set.contains(&subscriber.symbol)
                && subscriber.timeframe_code == timeframe_code
                && subscriber.mode == mode)
        });
        Ok(())
    }

    pub async fn unsubscribe_bars_rpc(
        &self,
        symbols: Vec<String>,
        timeframe: TimeFrame,
        mode: DataStreamMode,
    ) -> Result<(), BrokerError> {
        self.unsubscribe_bars(symbols, timeframe, mode)?;
        self.sync_bar_subscriptions().await
    }

    pub fn unsubscribe_symbol_bars(&self, symbols: Vec<String>) {
        let symbol_set: HashSet<_> = symbols.into_iter().collect();
        let session_id = self.session_id();
        self.bar_subscribers.lock().retain(|subscriber| {
            !(subscriber.session_id == session_id && symbol_set.contains(&subscriber.symbol))
        });
    }

    pub async fn unsubscribe_symbol_bars_rpc(
        &self,
        symbols: Vec<String>,
    ) -> Result<(), BrokerError> {
        self.unsubscribe_symbol_bars(symbols);
        self.sync_bar_subscriptions().await
    }

    fn apply_snapshot(&self, _session_id: &str, payload: Mt5SnapshotPayload) {
        let mut state = self.state.write();
        if let Some(account) = payload.account {
            state.account = Some(account);
        }
        for asset in payload.assets {
            state.assets.insert(asset.symbol.clone(), asset);
        }
        for order in payload.orders {
            state.orders.insert(order.order_id.clone(), order);
        }
        for position in payload.positions {
            state
                .positions
                .insert(position.asset.symbol.clone(), position);
        }
    }

    fn apply_market_data(&self, session_id: &str, payload: Mt5MarketDataPayload) {
        let mut quotes = payload.quotes;
        let mut stream_bars = payload.bars;

        {
            let mut state = self.state.write();
            for quote in &mut quotes {
                quote.symbol = self.config.aqe_symbol(&quote.symbol);
                state
                    .latest_quotes
                    .insert(quote.symbol.clone(), quote.clone());
            }
            for bar in &payload.history {
                state
                    .history
                    .entry(bar.symbol.clone())
                    .or_insert_with(Vec::new)
                    .push(bar.clone());
            }
            for stream_bar in &mut stream_bars {
                stream_bar.bar.symbol = self.config.aqe_symbol(&stream_bar.bar.symbol);
                let timeframe = stream_bar
                    .timeframe
                    .clone()
                    .unwrap_or_else(|| "PERIOD_M1".to_string());
                state
                    .latest_bars
                    .insert(stream_bar.bar.symbol.clone(), stream_bar.bar.clone());
                state.latest_bars_by_stream.insert(
                    (stream_bar.bar.symbol.clone(), timeframe),
                    stream_bar.bar.clone(),
                );
                if let Some(timeframe) = stream_bar.timeframe.as_ref() {
                    state
                        .latest_intrabar_bars
                        .remove(&(stream_bar.bar.symbol.clone(), timeframe.clone()));
                }
            }
        }

        let subscribers = self.bar_subscribers.lock();

        for stream_bar in stream_bars {
            let timeframe_code = stream_bar.timeframe.as_deref().unwrap_or("PERIOD_M1");
            for subscriber in subscribers.iter() {
                if subscriber.session_id == session_id
                    && subscriber.symbol == stream_bar.bar.symbol
                    && subscriber.timeframe_code == timeframe_code
                    && subscriber.mode == DataStreamMode::CompletedBar
                {
                    (subscriber.callback)(stream_bar.bar.clone());
                }
            }
        }

        for quote in quotes {
            for subscriber in subscribers.iter() {
                if subscriber.session_id != session_id
                    || subscriber.symbol != quote.symbol
                    || subscriber.mode != DataStreamMode::Intrabar
                {
                    continue;
                }
                if let Some(bar) = self.synthesize_intrabar(&quote, subscriber) {
                    (subscriber.callback)(bar);
                }
            }
        }
    }

    fn synthesize_intrabar(&self, quote: &Quote, subscriber: &Mt5BarSubscription) -> Option<Bar> {
        let price = quote.last.unwrap_or((quote.bid + quote.ask) / 2.0);
        if !price.is_finite() || price <= 0.0 {
            return None;
        }

        let bucket = subscriber
            .timeframe
            .get_current_time_increment(quote.timestamp);
        let key = (subscriber.symbol.clone(), subscriber.timeframe_code.clone());
        let mut state = self.state.write();
        let entry = state
            .latest_intrabar_bars
            .entry(key)
            .or_insert_with(|| Bar {
                symbol: subscriber.symbol.clone(),
                open: price,
                high: price,
                low: price,
                close: price,
                volume: 0.0,
                timestamp: bucket,
            });

        if entry.timestamp != bucket {
            *entry = Bar {
                symbol: subscriber.symbol.clone(),
                open: price,
                high: price,
                low: price,
                close: price,
                volume: 0.0,
                timestamp: bucket,
            };
        } else {
            entry.high = entry.high.max(price);
            entry.low = entry.low.min(price);
            entry.close = price;
        }

        Some(entry.clone())
    }

    fn enqueue_trade_event(&self, sequence: u64, order: Order, event: TradeUpdateEvent) {
        let order = self.upsert_order(order);
        let subscriber_event = (order.clone(), event.clone());
        {
            let mut queue = self.event_queue.lock();
            if queue.len() >= MAX_TRADE_EVENT_QUEUE_LEN {
                queue.pop_front();
                self.state.write().dropped_trade_event_count += 1;
            }
            queue.push_back(QueuedTradeEvent {
                sequence,
                order,
                event,
            });
        }
        self.state.write().last_trade_event_sequence = Some(sequence);
        for subscriber in self.trade_subscribers.lock().iter() {
            subscriber(subscriber_event.clone());
        }
    }

    fn apply_trade_event(
        &self,
        session_id: &str,
        envelope_sequence: Option<u64>,
        payload: Mt5TradeEventPayload,
    ) {
        let sequence = payload
            .event_seq
            .or(envelope_sequence)
            .unwrap_or_else(|| self.local_trade_event_seq.fetch_add(1, Ordering::Relaxed) + 1);
        let event_key = payload
            .native_event_id
            .clone()
            .map(|native_id| format!("{}:{}", session_id, native_id))
            .unwrap_or_else(|| format!("{}:seq:{}", session_id, sequence));

        {
            let mut state = self.state.write();
            if !state.seen_event_ids.insert(event_key) {
                return;
            }
        }

        let mut order = payload.order;
        self.map_order_to_aqe(&mut order);
        let order = self.upsert_order(order);
        self.enqueue_trade_event(sequence, order, payload.event);
    }

    fn apply_rpc_response(&self, payload: Mt5RpcResponsePayload) {
        let request_id = payload.request_id.clone();
        let tx = self.pending_rpc.lock().remove(&request_id);
        if let Some(tx) = tx {
            let mut state = self.state.write();
            state.last_rpc_response = Some(Utc::now());
            state.last_rpc_response_request_id = Some(request_id.clone());
            state.last_unknown_rpc_response_request_id = None;
            drop(state);
            let _ = tx.send(payload);
        } else {
            self.state.write().last_unknown_rpc_response_request_id = Some(request_id.clone());
            warn!(
                "Received MT5 RPC response for unknown request {}",
                request_id
            );
        }
    }

    fn map_asset_to_aqe(&self, asset: &mut Asset) {
        asset.symbol = self.config.aqe_symbol(&asset.symbol);
        asset.id = asset.symbol.clone();
    }

    fn map_order_to_aqe(&self, order: &mut Order) {
        self.map_asset_to_aqe(&mut order.asset);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mt5RpcRequest {
    #[serde(default)]
    pub session_id: String,
    pub request_id: String,
    pub action: Mt5RpcAction,
    pub payload: Value,
}

#[derive(Clone, Debug, Copy, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Mt5RpcAction {
    GetTickerInfo,
    GetHistory,
    GetLatestQuote,
    GetLatestBar,
    GetOrders,
    GetPositions,
    GetAccount,
    SubmitOrder,
    CancelOrder,
    UpdateOrder,
    ClosePosition,
    CloseAllPositions,
    SubscribeBars,
    UnsubscribeBars,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mt5RpcPollPayload {
    pub max_requests: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mt5RpcPollResponse {
    pub requests: Vec<Mt5RpcRequest>,
    pub poll_interval_ms: u64,
    pub session_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mt5RpcResponsePayload {
    pub request_id: String,
    pub ok: bool,
    pub message: Option<String>,
    pub payload: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mt5BridgeEnvelope<T> {
    pub protocol_version: u16,
    pub session_id: String,
    pub request_id: String,
    pub event_seq: Option<u64>,
    pub server_time: Option<DateTime<Utc>>,
    pub payload: T,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mt5BridgeResponse<T> {
    pub ok: bool,
    pub protocol_version: u16,
    pub session_id: String,
    pub request_id: String,
    pub message: Option<String>,
    pub payload: Option<T>,
}

impl<T> Mt5BridgeResponse<T> {
    fn ok(session_id: &str, request_id: String, payload: Option<T>) -> Self {
        Self {
            ok: true,
            protocol_version: PROTOCOL_VERSION,
            session_id: session_id.to_string(),
            request_id,
            message: None,
            payload,
        }
    }

    fn error(session_id: &str, request_id: String, message: String) -> Self {
        Self {
            ok: false,
            protocol_version: PROTOCOL_VERSION,
            session_id: session_id.to_string(),
            request_id,
            message: Some(message),
            payload: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mt5HeartbeatPayload {
    pub terminal_name: Option<String>,
    pub account_id: Option<String>,
    pub server_time: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Mt5SnapshotPayload {
    pub account: Option<Account>,
    #[serde(default)]
    pub assets: Vec<Asset>,
    #[serde(default)]
    pub positions: Vec<Position>,
    #[serde(default)]
    pub orders: Vec<Order>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Mt5MarketDataPayload {
    #[serde(default)]
    pub quotes: Vec<Quote>,
    #[serde(default)]
    pub bars: Vec<Mt5StreamBar>,
    #[serde(default)]
    pub history: Vec<Bar>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mt5StreamBar {
    pub timeframe: Option<String>,
    #[serde(flatten)]
    pub bar: Bar,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mt5TradeEventPayload {
    pub native_event_id: Option<String>,
    pub event_seq: Option<u64>,
    pub event: TradeUpdateEvent,
    pub order: Order,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Mt5TradeEventsPayload {
    #[serde(default)]
    pub events: Vec<Mt5TradeEventPayload>,
    #[serde(default)]
    pub dropped_count: Option<u64>,
}

async fn health(
    State(bridge): State<Arc<Mt5Bridge>>,
) -> Json<Mt5BridgeResponse<serde_json::Value>> {
    let diagnostics = bridge.diagnostics();
    Json(Mt5BridgeResponse::ok(
        &bridge.session_id(),
        Uuid::new_v4().to_string(),
        Some(serde_json::json!({
            "connected": bridge.is_connected(),
            "protocolVersion": PROTOCOL_VERSION,
            "diagnostics": diagnostics,
        })),
    ))
}

async fn heartbeat(
    State(bridge): State<Arc<Mt5Bridge>>,
    headers: HeaderMap,
    Json(envelope): Json<Mt5BridgeEnvelope<Mt5HeartbeatPayload>>,
) -> (StatusCode, Json<Mt5BridgeResponse<serde_json::Value>>) {
    if let Err(message) = authorize_session_token(&bridge, &headers, &envelope.session_id) {
        return bridge_error(
            &bridge,
            envelope.request_id,
            StatusCode::UNAUTHORIZED,
            message,
        );
    }
    let mut state = bridge.state.write();
    state.last_heartbeat = Some(Utc::now());
    state
        .active_sessions
        .insert(envelope.session_id.clone(), Utc::now());
    state.terminal_name = envelope.payload.terminal_name;
    let session_id = bridge.session_id();
    (
        StatusCode::OK,
        Json(Mt5BridgeResponse::ok(
            &session_id,
            envelope.request_id,
            Some(serde_json::json!({ "serverTime": Utc::now() })),
        )),
    )
}

async fn snapshot(
    State(bridge): State<Arc<Mt5Bridge>>,
    headers: HeaderMap,
    Json(envelope): Json<Mt5BridgeEnvelope<Mt5SnapshotPayload>>,
) -> (StatusCode, Json<Mt5BridgeResponse<serde_json::Value>>) {
    if let Err(message) = authorize_session_token(&bridge, &headers, &envelope.session_id) {
        return bridge_error(
            &bridge,
            envelope.request_id,
            StatusCode::UNAUTHORIZED,
            message,
        );
    }
    bridge.register_session(&envelope.session_id);
    bridge.apply_snapshot(&envelope.session_id, envelope.payload);
    let session_id = bridge.session_id();
    (
        StatusCode::OK,
        Json(Mt5BridgeResponse::ok(
            &session_id,
            envelope.request_id,
            Some(serde_json::json!({ "accepted": true })),
        )),
    )
}

async fn market_data(
    State(bridge): State<Arc<Mt5Bridge>>,
    headers: HeaderMap,
    Json(envelope): Json<Mt5BridgeEnvelope<Mt5MarketDataPayload>>,
) -> (StatusCode, Json<Mt5BridgeResponse<serde_json::Value>>) {
    if let Err(message) = authorize_session_token(&bridge, &headers, &envelope.session_id) {
        return bridge_error(
            &bridge,
            envelope.request_id,
            StatusCode::UNAUTHORIZED,
            message,
        );
    }
    bridge.register_session(&envelope.session_id);
    bridge.apply_market_data(&envelope.session_id, envelope.payload);
    let session_id = bridge.session_id();
    (
        StatusCode::OK,
        Json(Mt5BridgeResponse::ok(
            &session_id,
            envelope.request_id,
            Some(serde_json::json!({ "accepted": true })),
        )),
    )
}

async fn trade_event(
    State(bridge): State<Arc<Mt5Bridge>>,
    headers: HeaderMap,
    Json(envelope): Json<Mt5BridgeEnvelope<Mt5TradeEventPayload>>,
) -> (StatusCode, Json<Mt5BridgeResponse<serde_json::Value>>) {
    if let Err(message) = authorize_session_token(&bridge, &headers, &envelope.session_id) {
        return bridge_error(
            &bridge,
            envelope.request_id,
            StatusCode::UNAUTHORIZED,
            message,
        );
    }
    bridge.register_session(&envelope.session_id);
    bridge.apply_trade_event(&envelope.session_id, envelope.event_seq, envelope.payload);
    let session_id = bridge.session_id();
    (
        StatusCode::OK,
        Json(Mt5BridgeResponse::ok(
            &session_id,
            envelope.request_id,
            Some(serde_json::json!({ "accepted": true })),
        )),
    )
}

async fn trade_events(
    State(bridge): State<Arc<Mt5Bridge>>,
    headers: HeaderMap,
    Json(envelope): Json<Mt5BridgeEnvelope<Mt5TradeEventsPayload>>,
) -> (StatusCode, Json<Mt5BridgeResponse<serde_json::Value>>) {
    if let Err(message) = authorize_session_token(&bridge, &headers, &envelope.session_id) {
        return bridge_error(
            &bridge,
            envelope.request_id,
            StatusCode::UNAUTHORIZED,
            message,
        );
    }
    bridge.register_session(&envelope.session_id);
    if let Some(dropped_count) = envelope.payload.dropped_count {
        bridge.state.write().dropped_trade_event_count += dropped_count;
    }
    let accepted = envelope.payload.events.len();
    for payload in envelope.payload.events {
        bridge.apply_trade_event(&envelope.session_id, envelope.event_seq, payload);
    }
    let session_id = bridge.session_id();
    (
        StatusCode::OK,
        Json(Mt5BridgeResponse::ok(
            &session_id,
            envelope.request_id,
            Some(serde_json::json!({ "accepted": accepted })),
        )),
    )
}

async fn poll_rpc(
    State(bridge): State<Arc<Mt5Bridge>>,
    headers: HeaderMap,
    Json(envelope): Json<Mt5BridgeEnvelope<Mt5RpcPollPayload>>,
) -> (StatusCode, Json<Mt5BridgeResponse<Mt5RpcPollResponse>>) {
    bridge.record_rpc_poll_attempt(&envelope.session_id);
    if let Err(message) = authorize_token(&bridge, &headers) {
        bridge.record_rpc_poll_auth_failure(message.clone());
        let session_id = bridge.session_id();
        return (
            StatusCode::UNAUTHORIZED,
            Json(Mt5BridgeResponse::error(
                &session_id,
                envelope.request_id,
                message,
            )),
        );
    }
    let session_id = bridge.session_id();
    if !envelope.session_id.is_empty() && envelope.session_id != session_id {
        warn!(
            "MT5 EA polled with stale session {}; resetting to {}",
            envelope.session_id, session_id
        );
    }
    bridge.record_rpc_poll(&session_id);

    let max_requests = envelope.payload.max_requests.unwrap_or(32).clamp(1, 256);
    let notified = bridge.rpc_queue_notify.notified();
    let (mut requests, mut queued_remaining) = bridge.drain_rpc_requests(max_requests);
    if requests.is_empty() {
        let hold_timeout = bridge
            .config
            .poll_interval
            .clamp(Duration::from_millis(50), Duration::from_millis(200));
        if tokio::time::timeout(hold_timeout, notified).await.is_ok() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        (requests, queued_remaining) = bridge.drain_rpc_requests(max_requests);
    }
    bridge.record_rpc_poll_delivery(&requests);
    if !requests.is_empty() {
        debug!(
            "MT5 EA polled {} RPC request(s) for session {}; queued_remaining={}",
            requests.len(),
            session_id,
            queued_remaining
        );
    }

    let response_session_id = session_id.clone();
    (
        StatusCode::OK,
        Json(Mt5BridgeResponse::ok(
            &session_id,
            envelope.request_id,
            Some(Mt5RpcPollResponse {
                requests,
                poll_interval_ms: bridge.config.poll_interval.as_millis() as u64,
                session_id: response_session_id,
            }),
        )),
    )
}

async fn rpc_response(
    State(bridge): State<Arc<Mt5Bridge>>,
    headers: HeaderMap,
    Json(envelope): Json<Mt5BridgeEnvelope<Mt5RpcResponsePayload>>,
) -> (StatusCode, Json<Mt5BridgeResponse<serde_json::Value>>) {
    if let Err(message) = authorize_session_token(&bridge, &headers, &envelope.session_id) {
        return bridge_error(
            &bridge,
            envelope.request_id,
            StatusCode::UNAUTHORIZED,
            message,
        );
    }
    bridge.register_session(&envelope.session_id);
    bridge.apply_rpc_response(envelope.payload);
    let session_id = bridge.session_id();
    (
        StatusCode::OK,
        Json(Mt5BridgeResponse::ok(
            &session_id,
            envelope.request_id,
            Some(serde_json::json!({ "accepted": true })),
        )),
    )
}

fn authorize_session_token(
    bridge: &Mt5Bridge,
    headers: &HeaderMap,
    session_id: &str,
) -> Result<(), String> {
    let header_session = header_value(headers, "x-aqe-mt5-session");
    let header_token = header_value(headers, "x-aqe-mt5-token");
    if session_id.is_empty() || header_session.as_deref() != Some(session_id) {
        return Err("invalid MT5 bridge session".to_string());
    }
    if header_token.as_deref() != Some(bridge.config.token.as_str()) {
        return Err("invalid MT5 bridge token".to_string());
    }
    Ok(())
}

fn authorize_token(bridge: &Mt5Bridge, headers: &HeaderMap) -> Result<(), String> {
    let header_token = header_value(headers, "x-aqe-mt5-token");
    if header_token.is_none() {
        return Err("missing MT5 bridge token".to_string());
    }
    if header_token.as_deref() != Some(bridge.config.token.as_str()) {
        return Err("invalid MT5 bridge token".to_string());
    }
    Ok(())
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string)
}

fn bridge_error(
    bridge: &Mt5Bridge,
    request_id: String,
    status: StatusCode,
    message: String,
) -> (StatusCode, Json<Mt5BridgeResponse<serde_json::Value>>) {
    (
        status,
        Json(Mt5BridgeResponse::error(
            &bridge.session_id(),
            request_id,
            message,
        )),
    )
}

pub fn default_mt5_asset(symbol: &str) -> Asset {
    Asset {
        id: symbol.to_string(),
        symbol: symbol.to_string(),
        name: symbol.to_string(),
        asset_type: AssetType::Forex,
        status: AssetStatus::Active,
        exchange: AssetExchange::UNKNOWN("MT5".to_string()),
        tradable: true,
        marginable: true,
        shortable: true,
        fractional: true,
        min_order_size: None,
        quantity_base: None,
        max_order_size: None,
        min_price_increment: None,
        price_base: None,
        contract_size: None,
        fees: Default::default(),
    }
}

fn deserialize_payload<T: serde::de::DeserializeOwned>(
    payload: Value,
    label: &str,
) -> Result<T, BrokerError> {
    serde_json::from_value(payload).map_err(|error| {
        BrokerError::DataFeedError(format!("failed to decode {}: {}", label, error))
    })
}

fn mt5_timeframe_code(time_frame: TimeFrame) -> Result<String, BrokerError> {
    let amount = time_frame.get_amount();
    let code = match (amount, time_frame.get_unit()) {
        (1, TimeFrameUnit::Minute) => "PERIOD_M1",
        (2, TimeFrameUnit::Minute) => "PERIOD_M2",
        (3, TimeFrameUnit::Minute) => "PERIOD_M3",
        (4, TimeFrameUnit::Minute) => "PERIOD_M4",
        (5, TimeFrameUnit::Minute) => "PERIOD_M5",
        (6, TimeFrameUnit::Minute) => "PERIOD_M6",
        (10, TimeFrameUnit::Minute) => "PERIOD_M10",
        (12, TimeFrameUnit::Minute) => "PERIOD_M12",
        (15, TimeFrameUnit::Minute) => "PERIOD_M15",
        (20, TimeFrameUnit::Minute) => "PERIOD_M20",
        (30, TimeFrameUnit::Minute) => "PERIOD_M30",
        (1, TimeFrameUnit::Hour) => "PERIOD_H1",
        (2, TimeFrameUnit::Hour) => "PERIOD_H2",
        (3, TimeFrameUnit::Hour) => "PERIOD_H3",
        (4, TimeFrameUnit::Hour) => "PERIOD_H4",
        (6, TimeFrameUnit::Hour) => "PERIOD_H6",
        (8, TimeFrameUnit::Hour) => "PERIOD_H8",
        (12, TimeFrameUnit::Hour) => "PERIOD_H12",
        (1, TimeFrameUnit::Day) => "PERIOD_D1",
        (1, TimeFrameUnit::Month) => "PERIOD_MN1",
        _ => {
            return Err(BrokerError::DataFeedError(format!(
                "MT5 does not support AQE timeframe {} {:?}",
                amount,
                time_frame.get_unit()
            )));
        }
    };
    Ok(code.to_string())
}

fn bars_to_frame(bars: Vec<Bar>) -> Result<DataFrame, BrokerError> {
    DataFrame::new(vec![
        Column::new(
            "symbol".into(),
            bars.iter()
                .map(|bar| bar.symbol.clone())
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "open".into(),
            bars.iter().map(|bar| bar.open).collect::<Vec<_>>(),
        ),
        Column::new(
            "high".into(),
            bars.iter().map(|bar| bar.high).collect::<Vec<_>>(),
        ),
        Column::new(
            "low".into(),
            bars.iter().map(|bar| bar.low).collect::<Vec<_>>(),
        ),
        Column::new(
            "close".into(),
            bars.iter().map(|bar| bar.close).collect::<Vec<_>>(),
        ),
        Column::new(
            "volume".into(),
            bars.iter().map(|bar| bar.volume).collect::<Vec<_>>(),
        ),
        Column::new(
            "timestamp".into(),
            bars.iter()
                .map(|bar| bar.timestamp.timestamp_millis())
                .collect::<Vec<_>>(),
        ),
    ])
    .map_err(|error| BrokerError::DataFeedError(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::broker::types::{
        OrderClass, OrderLeg, OrderLegs, OrderSide, OrderType, TimeInForce,
    };

    fn test_bridge(token: &str) -> Mt5Bridge {
        Mt5Bridge::new(Mt5BridgeConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            token: token.to_string(),
            request_timeout: Duration::from_millis(5_000),
            connect_timeout: Duration::from_millis(5_000),
            poll_interval: Duration::from_millis(250),
            symbol_map: HashMap::new(),
        })
    }

    fn headers(session_id: &str, token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-aqe-mt5-session", session_id.parse().unwrap());
        headers.insert("x-aqe-mt5-token", token.parse().unwrap());
        headers
    }

    fn trade_event_order(order_id: &str, insight_id: &str, status: TradeUpdateEvent) -> Order {
        Order {
            order_id: order_id.to_string(),
            insight_id: Some(insight_id.to_string()),
            strategy_type: Some("Testing".to_string()),
            asset: default_mt5_asset("AAPL"),
            qty: 1.0,
            filled_qty: 1.0,
            limit_price: None,
            filled_price: Some(100.0),
            stop_price: None,
            side: OrderSide::Buy,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::GTC,
            status,
            order_class: OrderClass::Simple,
            created_at: 0,
            updated_at: 0,
            submitted_at: 0,
            filled_at: Some(0),
            realized_pnl: None,
            commission: None,
            swap: None,
            rejection_reason: None,
            legs: None,
        }
    }

    #[test]
    fn session_token_auth_accepts_stale_session_with_valid_token() {
        let bridge = test_bridge("test-token");
        bridge.set_session_id("current-session");
        let headers = headers("old-session", "test-token");

        assert!(authorize_session_token(&bridge, &headers, "old-session").is_ok());
    }

    #[test]
    fn session_token_auth_rejects_invalid_token() {
        let bridge = test_bridge("test-token");
        let headers = headers("old-session", "wrong-token");

        assert_eq!(
            authorize_session_token(&bridge, &headers, "old-session"),
            Err("invalid MT5 bridge token".to_string())
        );
    }

    #[test]
    fn session_token_auth_rejects_mismatched_session_header() {
        let bridge = test_bridge("test-token");
        let headers = headers("other-session", "test-token");

        assert_eq!(
            authorize_session_token(&bridge, &headers, "old-session"),
            Err("invalid MT5 bridge session".to_string())
        );
    }

    #[test]
    fn pending_rpc_guard_cleans_abandoned_request() {
        let bridge = test_bridge("test-token");
        let request_id = "request-1".to_string();
        let (tx, _rx) = oneshot::channel();
        bridge.pending_rpc.lock().insert(request_id.clone(), tx);
        bridge.rpc_queue.lock().push_back(Mt5RpcRequest {
            session_id: String::new(),
            request_id: request_id.clone(),
            action: Mt5RpcAction::GetAccount,
            payload: serde_json::json!({}),
        });

        {
            let _guard = PendingMt5RpcRequest::new(
                request_id.clone(),
                bridge.rpc_queue.clone(),
                bridge.pending_rpc.clone(),
            );
        }

        assert!(!bridge.pending_rpc.lock().contains_key(&request_id));
        assert!(
            !bridge
                .rpc_queue
                .lock()
                .iter()
                .any(|request| request.request_id == request_id)
        );
    }

    #[test]
    fn trade_events_drain_in_sequence_order() {
        let bridge = test_bridge("test-token");
        bridge.apply_trade_event(
            "session-1",
            None,
            Mt5TradeEventPayload {
                native_event_id: Some("event-2".to_string()),
                event_seq: Some(2),
                event: TradeUpdateEvent::Closed,
                order: trade_event_order("order-2", "insight-1", TradeUpdateEvent::Closed),
            },
        );
        bridge.apply_trade_event(
            "session-1",
            None,
            Mt5TradeEventPayload {
                native_event_id: Some("event-1".to_string()),
                event_seq: Some(1),
                event: TradeUpdateEvent::Filled,
                order: trade_event_order("order-1", "insight-1", TradeUpdateEvent::Filled),
            },
        );

        let events = bridge.drain_trade_events();

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].0.order_id, "order-1");
        assert_eq!(events[0].1, TradeUpdateEvent::Filled);
        assert_eq!(events[1].0.order_id, "order-2");
        assert_eq!(events[1].1, TradeUpdateEvent::Closed);
    }

    #[test]
    fn trade_events_dedupe_by_native_event_id_not_order_status() {
        let bridge = test_bridge("test-token");
        for (native_id, sequence) in [("fill-1", 1), ("fill-2", 2), ("fill-1", 3)] {
            bridge.apply_trade_event(
                "session-1",
                None,
                Mt5TradeEventPayload {
                    native_event_id: Some(native_id.to_string()),
                    event_seq: Some(sequence),
                    event: TradeUpdateEvent::Filled,
                    order: trade_event_order("same-order", "insight-1", TradeUpdateEvent::Filled),
                },
            );
        }

        let events = bridge.drain_trade_events();

        assert_eq!(events.len(), 2);
        assert!(
            events
                .iter()
                .all(|(_, event)| *event == TradeUpdateEvent::Filled)
        );
    }

    #[test]
    fn trade_events_preserve_existing_order_route_and_legs_for_thin_mt5_events() {
        let bridge = test_bridge("test-token");
        let mut existing = trade_event_order("order-1", "insight-1", TradeUpdateEvent::Filled);
        existing.legs = Some(OrderLegs {
            take_profit: Some(OrderLeg {
                order_id: Some("tp-1".to_string()),
                limit_price: Some(110.0),
                trail_price: None,
                side: OrderSide::Sell,
                filled_price: None,
                order_type: OrderType::Limit,
                status: TradeUpdateEvent::Pending,
                order_class: OrderClass::Bracket,
                created_at: 0,
                updated_at: 0,
                submitted_at: 0,
                filled_at: None,
            }),
            stop_loss: Some(OrderLeg {
                order_id: Some("sl-1".to_string()),
                limit_price: Some(95.0),
                trail_price: None,
                side: OrderSide::Sell,
                filled_price: None,
                order_type: OrderType::Stop,
                status: TradeUpdateEvent::Pending,
                order_class: OrderClass::Bracket,
                created_at: 0,
                updated_at: 0,
                submitted_at: 0,
                filled_at: None,
            }),
            trailing_stop: None,
        });
        bridge.upsert_order(existing);

        let mut thin_close = trade_event_order("order-1", "", TradeUpdateEvent::Closed);
        thin_close.insight_id = None;
        thin_close.strategy_type = None;
        thin_close.legs = None;
        bridge.apply_trade_event(
            "session-1",
            None,
            Mt5TradeEventPayload {
                native_event_id: Some("close-1".to_string()),
                event_seq: Some(1),
                event: TradeUpdateEvent::Closed,
                order: thin_close,
            },
        );

        let events = bridge.drain_trade_events();

        assert_eq!(events.len(), 1);
        let order = &events[0].0;
        assert_eq!(order.insight_id.as_deref(), Some("insight-1"));
        assert_eq!(order.strategy_type.as_deref(), Some("Testing"));
        assert_eq!(
            order
                .legs
                .as_ref()
                .and_then(|legs| legs.take_profit.as_ref())
                .and_then(|leg| leg.order_id.as_deref()),
            Some("tp-1")
        );
    }
}
