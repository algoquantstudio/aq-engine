use super::types::{
    Account, Asset, AssetExchange, AssetStatus, AssetType, Bar, BrokerError, Order, Position,
    Quote, TradeUpdateEvent,
};
use actix_web::http::{StatusCode, header::HeaderMap};
use actix_web::{App, HttpRequest, HttpResponse, HttpServer, web};
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
use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc, oneshot};
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
const MAX_SEEN_EVENT_IDS: usize = 50_000;
// Trade traffic has its own generously-sized ordered ingress queue. Market
// data deliberately has a very small queue: when it is full the EA retries its
// newest snapshot instead of making AQE process stale quote batches.
const TRADE_INGRESS_CAPACITY: usize = 1_024;
const MARKET_DATA_INGRESS_CAPACITY: usize = 2;
const MT5_TRANSPORT_FRESH_SECS: i64 = 15;
const MT5_MARKET_DATA_FRESH_SECS: i64 = 15;
const MT5_RECOVERY_COOLDOWN_SECS: i64 = 5;

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
    reverse_symbol_map: Arc<HashMap<String, String>>,
    session_id: Arc<RwLock<String>>,
    state: Arc<RwLock<Mt5BridgeState>>,
    rpc_queue: Arc<Mutex<VecDeque<Mt5RpcRequest>>>,
    rpc_queue_notify: Arc<Notify>,
    pending_rpc: Arc<Mutex<HashMap<String, oneshot::Sender<Mt5RpcResponsePayload>>>>,
    event_queue: Arc<Mutex<VecDeque<QueuedTradeEvent>>>,
    local_trade_event_seq: Arc<AtomicU64>,
    trade_subscribers: Arc<Mutex<Vec<Arc<dyn Fn((Order, TradeUpdateEvent)) + Send + Sync>>>>,
    bar_subscribers: Arc<Mutex<Vec<Mt5BarSubscription>>>,
    trade_ingress_tx: Arc<Mutex<Option<mpsc::Sender<TradeIngress>>>>,
    market_data_ingress_tx: Arc<Mutex<Option<mpsc::Sender<MarketDataIngress>>>>,
    trade_ingress_depth: Arc<AtomicU64>,
    market_data_ingress_depth: Arc<AtomicU64>,
    trade_ingress_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    market_data_ingress_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    recovery_lock: Arc<AsyncMutex<()>>,
    recovery_scheduled: Arc<AtomicBool>,
    server_started: Arc<AtomicBool>,
    server_control: Arc<Mutex<Option<actix_web::dev::ServerHandle>>>,
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
    seen_event_order: VecDeque<String>,
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
    last_market_data: Option<DateTime<Utc>>,
    last_subscription_sync: Option<DateTime<Utc>>,
    last_recovery: Option<DateTime<Utc>>,
    terminal_name: Option<String>,
    heartbeat_account_id: Option<String>,
    terminal_connected: Option<bool>,
    terminal_trade_allowed: Option<bool>,
    dropped_trade_event_count: u64,
    last_trade_event_sequence: Option<u64>,
}

#[derive(Clone, Debug)]
struct QueuedTradeEvent {
    sequence: u64,
    order: Order,
    event: TradeUpdateEvent,
}

#[derive(Debug)]
struct TradeIngress {
    session_id: String,
    envelope_sequence: Option<u64>,
    events: Vec<Mt5TradeEventPayload>,
    dropped_count: u64,
}

#[derive(Debug)]
struct MarketDataIngress {
    session_id: String,
    payload: Mt5MarketDataPayload,
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
    pub last_market_data_at: Option<DateTime<Utc>>,
    pub last_subscription_sync_at: Option<DateTime<Utc>>,
    pub last_recovery_at: Option<DateTime<Utc>>,
    pub terminal_name: Option<String>,
    pub heartbeat_account_id: Option<String>,
    pub terminal_connected: Option<bool>,
    pub terminal_trade_allowed: Option<bool>,
    pub transport_connected: bool,
    pub broker_connected: bool,
    pub datafeed_connected: bool,
    pub queued_trade_event_count: usize,
    pub queued_trade_ingress_count: u64,
    pub queued_market_data_ingress_count: u64,
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
        let reverse_symbol_map = Arc::new(
            config
                .symbol_map
                .iter()
                .map(|(aqe, mt5)| (mt5.clone(), aqe.clone()))
                .collect(),
        );
        Self {
            config,
            reverse_symbol_map,
            session_id: Arc::new(RwLock::new(Uuid::new_v4().to_string())),
            state: Arc::new(RwLock::new(Mt5BridgeState::default())),
            rpc_queue: Arc::new(Mutex::new(VecDeque::new())),
            rpc_queue_notify: Arc::new(Notify::new()),
            pending_rpc: Arc::new(Mutex::new(HashMap::new())),
            event_queue: Arc::new(Mutex::new(VecDeque::new())),
            local_trade_event_seq: Arc::new(AtomicU64::new(0)),
            trade_subscribers: Arc::new(Mutex::new(Vec::new())),
            bar_subscribers: Arc::new(Mutex::new(Vec::new())),
            trade_ingress_tx: Arc::new(Mutex::new(None)),
            market_data_ingress_tx: Arc::new(Mutex::new(None)),
            trade_ingress_depth: Arc::new(AtomicU64::new(0)),
            market_data_ingress_depth: Arc::new(AtomicU64::new(0)),
            trade_ingress_handle: Arc::new(Mutex::new(None)),
            market_data_ingress_handle: Arc::new(Mutex::new(None)),
            recovery_lock: Arc::new(AsyncMutex::new(())),
            recovery_scheduled: Arc::new(AtomicBool::new(false)),
            server_started: Arc::new(AtomicBool::new(false)),
            server_control: Arc::new(Mutex::new(None)),
            server_handle: Arc::new(Mutex::new(None)),
        }
    }

    pub fn config(&self) -> &Mt5BridgeConfig {
        &self.config
    }

    pub fn aqe_symbol(&self, mt5_symbol: &str) -> String {
        self.reverse_symbol_map
            .get(mt5_symbol)
            .cloned()
            .unwrap_or_else(|| mt5_symbol.to_string())
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
        state.seen_event_order.clear();
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
        state.last_market_data = None;
        state.last_subscription_sync = None;
        state.last_recovery = None;
        state.terminal_name = None;
        state.heartbeat_account_id = None;
        state.terminal_connected = None;
        state.terminal_trade_allowed = None;
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
        state.last_market_data = None;
        state.last_subscription_sync = None;
        state.last_recovery = None;
        state.terminal_connected = None;
        state.terminal_trade_allowed = None;
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
            .map(|poll| Utc::now() - poll < chrono::Duration::seconds(MT5_TRANSPORT_FRESH_SECS))
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
            last_market_data_at,
            last_subscription_sync_at,
            last_recovery_at,
            terminal_name,
            heartbeat_account_id,
            terminal_connected,
            terminal_trade_allowed,
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
                state.last_market_data,
                state.last_subscription_sync,
                state.last_recovery,
                state.terminal_name.clone(),
                state.heartbeat_account_id.clone(),
                state.terminal_connected,
                state.terminal_trade_allowed,
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
            last_market_data_at,
            last_subscription_sync_at,
            last_recovery_at,
            terminal_name,
            heartbeat_account_id,
            terminal_connected,
            terminal_trade_allowed,
            transport_connected: self.is_transport_connected(),
            broker_connected: self.is_broker_connected(),
            datafeed_connected: self.is_datafeed_connected(),
            queued_trade_event_count: self.event_queue.lock().len(),
            queued_trade_ingress_count: self.trade_ingress_depth.load(Ordering::Relaxed),
            queued_market_data_ingress_count: self
                .market_data_ingress_depth
                .load(Ordering::Relaxed),
            dropped_trade_event_count,
            last_trade_event_sequence,
        }
    }

    fn start_ingress_workers(self: &Arc<Self>) {
        let (trade_tx, mut trade_rx) = mpsc::channel::<TradeIngress>(TRADE_INGRESS_CAPACITY);
        let trade_bridge = Arc::clone(self);
        let trade_depth = Arc::clone(&self.trade_ingress_depth);
        let trade_handle = tokio::spawn(async move {
            while let Some(ingress) = trade_rx.recv().await {
                trade_depth.fetch_sub(1, Ordering::Relaxed);
                let bridge = Arc::clone(&trade_bridge);
                if let Err(error) = tokio::task::spawn_blocking(move || {
                    if ingress.dropped_count > 0 {
                        bridge.state.write().dropped_trade_event_count += ingress.dropped_count;
                    }
                    bridge.apply_trade_event_batch(
                        &ingress.session_id,
                        ingress.envelope_sequence,
                        ingress.events,
                    );
                })
                .await
                {
                    error!("MT5 ordered trade ingress worker failed: {error}");
                }
            }
        });

        let (market_tx, mut market_rx) =
            mpsc::channel::<MarketDataIngress>(MARKET_DATA_INGRESS_CAPACITY);
        let market_bridge = Arc::clone(self);
        let market_depth = Arc::clone(&self.market_data_ingress_depth);
        let market_handle = tokio::spawn(async move {
            while let Some(ingress) = market_rx.recv().await {
                market_depth.fetch_sub(1, Ordering::Relaxed);
                let bridge = Arc::clone(&market_bridge);
                if let Err(error) = tokio::task::spawn_blocking(move || {
                    bridge.apply_market_data(&ingress.session_id, ingress.payload);
                })
                .await
                {
                    error!("MT5 market-data ingress worker failed: {error}");
                }
            }
        });

        *self.trade_ingress_tx.lock() = Some(trade_tx);
        *self.market_data_ingress_tx.lock() = Some(market_tx);
        *self.trade_ingress_handle.lock() = Some(trade_handle);
        *self.market_data_ingress_handle.lock() = Some(market_handle);
    }

    fn stop_ingress_workers(&self) {
        self.trade_ingress_tx.lock().take();
        self.market_data_ingress_tx.lock().take();
        if let Some(handle) = self.trade_ingress_handle.lock().take() {
            handle.abort();
        }
        if let Some(handle) = self.market_data_ingress_handle.lock().take() {
            handle.abort();
        }
        self.trade_ingress_depth.store(0, Ordering::Relaxed);
        self.market_data_ingress_depth.store(0, Ordering::Relaxed);
    }

    async fn shutdown_ingress_workers(&self) {
        self.trade_ingress_tx.lock().take();
        self.market_data_ingress_tx.lock().take();
        let handles = [
            self.trade_ingress_handle.lock().take(),
            self.market_data_ingress_handle.lock().take(),
        ];
        for handle in handles.into_iter().flatten() {
            let mut handle = handle;
            if tokio::time::timeout(Duration::from_secs(1), &mut handle)
                .await
                .is_err()
            {
                handle.abort();
                let _ = handle.await;
            }
        }
        self.trade_ingress_depth.store(0, Ordering::Relaxed);
        self.market_data_ingress_depth.store(0, Ordering::Relaxed);
    }

    fn enqueue_trade_ingress(&self, ingress: TradeIngress) -> Result<usize, &'static str> {
        let accepted = ingress.events.len();
        let sender = self
            .trade_ingress_tx
            .lock()
            .clone()
            .ok_or("trade ingress worker is not running")?;
        self.trade_ingress_depth.fetch_add(1, Ordering::Relaxed);
        if sender.try_send(ingress).is_err() {
            self.trade_ingress_depth.fetch_sub(1, Ordering::Relaxed);
            return Err("ordered trade ingress queue is full");
        }
        Ok(accepted)
    }

    fn enqueue_market_data_ingress(&self, ingress: MarketDataIngress) -> Result<(), &'static str> {
        let sender = self
            .market_data_ingress_tx
            .lock()
            .clone()
            .ok_or("market-data ingress worker is not running")?;
        self.market_data_ingress_depth
            .fetch_add(1, Ordering::Relaxed);
        if sender.try_send(ingress).is_err() {
            self.market_data_ingress_depth
                .fetch_sub(1, Ordering::Relaxed);
            return Err("market-data ingress queue is full; retry the latest snapshot");
        }
        Ok(())
    }

    pub async fn start(self: &Arc<Self>) -> Result<bool, BrokerError> {
        if self.server_started.swap(true, Ordering::SeqCst) {
            return Ok(true);
        }

        self.reset_connection_markers();
        self.start_ingress_workers();

        let bind_addr = self.config.bind_addr;
        let bridge_data: web::Data<Mt5Bridge> = web::Data::from(self.clone());
        let server = HttpServer::new(move || {
            App::new()
                .app_data(bridge_data.clone())
                .app_data(web::JsonConfig::default().limit(16 * 1024 * 1024))
                .route("/health", web::get().to(health))
                .route("/v1/heartbeat", web::post().to(heartbeat))
                .route("/v1/snapshot", web::post().to(snapshot))
                .route("/v1/market-data", web::post().to(market_data))
                .route("/v1/trade-event", web::post().to(trade_event))
                .route("/v1/trade-events", web::post().to(trade_events))
                .route("/v1/rpc/poll", web::post().to(poll_rpc))
                .route("/v1/rpc/response", web::post().to(rpc_response))
        })
        .disable_signals()
        .backlog(1_024)
        .keep_alive(Duration::from_secs(75))
        .bind(bind_addr)
        .map_err(|error| {
            self.server_started.store(false, Ordering::SeqCst);
            self.stop_ingress_workers();
            BrokerError::ConnectionError(format!(
                "failed to bind MT5 bridge at {}: {}",
                bind_addr, error
            ))
        })?
        .run();
        let server_control = server.handle();

        info!("AQE MT5 Actix bridge listening on http://{}", bind_addr);
        let handle = tokio::spawn(async move {
            if let Err(error) = server.await {
                error!("AQE MT5 bridge server stopped: {}", error);
            }
        });
        *self.server_control.lock() = Some(server_control);
        *self.server_handle.lock() = Some(handle);
        Ok(true)
    }

    pub fn stop(&self) {
        if let Some(control) = self.server_control.lock().take() {
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(control.stop(false));
            }
        }
        if let Some(handle) = self.server_handle.lock().take() {
            handle.abort();
        }
        self.stop_ingress_workers();
        self.server_started.store(false, Ordering::SeqCst);
        self.clear_session_runtime_state();
    }

    pub async fn shutdown(&self) {
        let server_control = { self.server_control.lock().take() };
        if let Some(control) = server_control {
            control.stop(true).await;
        }

        let server_handle = { self.server_handle.lock().take() };
        if let Some(mut handle) = server_handle {
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
        self.shutdown_ingress_workers().await;
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

    pub fn is_transport_connected(&self) -> bool {
        let state = self.state.read();
        let heartbeat_connected = state
            .last_heartbeat
            .map(|heartbeat| {
                Utc::now() - heartbeat < chrono::Duration::seconds(MT5_TRANSPORT_FRESH_SECS)
            })
            .unwrap_or(false);
        let rpc_polling = state
            .last_rpc_poll
            .map(|poll| Utc::now() - poll < chrono::Duration::seconds(MT5_TRANSPORT_FRESH_SECS))
            .unwrap_or(false);
        heartbeat_connected || rpc_polling
    }

    pub fn is_broker_connected(&self) -> bool {
        if !self.is_transport_connected() {
            return false;
        }
        self.state.read().terminal_connected.unwrap_or(true)
    }

    pub fn is_market_data_fresh(&self) -> bool {
        if self.bar_subscribers.lock().is_empty() {
            return true;
        }

        let state = self.state.read();
        let freshness_anchor = state.last_market_data.or(state.last_subscription_sync);
        freshness_anchor
            .map(|timestamp| {
                Utc::now() - timestamp < chrono::Duration::seconds(MT5_MARKET_DATA_FRESH_SECS)
            })
            .unwrap_or(false)
    }

    pub fn is_datafeed_connected(&self) -> bool {
        self.is_broker_connected() && self.is_market_data_fresh()
    }

    /// Backwards-compatible aggregate connection state. Broker and datafeed
    /// implementations use their more specific checks below.
    pub fn is_connected(&self) -> bool {
        self.is_broker_connected()
    }

    fn terminal_connection_error(&self) -> Option<BrokerError> {
        let state = self.state.read();
        (state.terminal_connected == Some(false)).then(|| {
            BrokerError::ConnectionError(format!(
                "MT5 terminal {} is reachable but disconnected from its broker account{}",
                state.terminal_name.as_deref().unwrap_or("EA"),
                state
                    .heartbeat_account_id
                    .as_deref()
                    .map(|account| format!(" {account}"))
                    .unwrap_or_default()
            ))
        })
    }

    async fn restore_runtime_state(&self, force: bool) -> Result<(), BrokerError> {
        let _recovery_guard = self.recovery_lock.lock().await;
        self.wait_for_rpc_poll().await?;
        if let Some(error) = self.terminal_connection_error() {
            return Err(error);
        }

        let recently_recovered = self
            .state
            .read()
            .last_recovery
            .map(|timestamp| {
                Utc::now() - timestamp < chrono::Duration::seconds(MT5_RECOVERY_COOLDOWN_SECS)
            })
            .unwrap_or(false);
        if recently_recovered && !force {
            return Ok(());
        }

        let subscription_count = self.current_subscription_specs().len();
        if subscription_count > 0 {
            info!(
                "MT5 bridge restoring {} bar subscription(s) after reconnect",
                subscription_count
            );
            self.sync_bar_subscriptions().await?;
        }

        let (account, orders, positions) = tokio::join!(
            self.request_account(),
            self.request_orders(),
            self.request_positions()
        );
        let mut refresh_errors = Vec::new();
        if let Err(error) = account {
            refresh_errors.push(format!("account: {error}"));
        }
        if let Err(error) = orders {
            refresh_errors.push(format!("orders: {error}"));
        }
        if let Err(error) = positions {
            refresh_errors.push(format!("positions: {error}"));
        }
        if !refresh_errors.is_empty() {
            return Err(BrokerError::ConnectionError(format!(
                "MT5 bridge reconnected but broker state refresh failed ({})",
                refresh_errors.join("; ")
            )));
        }

        self.state.write().last_recovery = Some(Utc::now());
        info!(
            "MT5 bridge recovery complete: subscriptions={} account/orders/positions refreshed",
            subscription_count
        );
        Ok(())
    }

    pub async fn recover_runtime_state(&self) -> Result<(), BrokerError> {
        self.restore_runtime_state(false).await
    }

    pub async fn recover_datafeed_state(&self) -> Result<(), BrokerError> {
        let _recovery_guard = self.recovery_lock.lock().await;
        self.wait_for_rpc_poll().await?;
        if let Some(error) = self.terminal_connection_error() {
            return Err(error);
        }
        if !self.current_subscription_specs().is_empty() {
            self.sync_bar_subscriptions().await?;
        }
        Ok(())
    }

    fn schedule_runtime_recovery(self: &Arc<Self>, reason: &'static str) {
        if self.recovery_scheduled.swap(true, Ordering::SeqCst) {
            return;
        }
        let bridge = Arc::clone(self);
        tokio::spawn(async move {
            warn!("MT5 bridge transport recovered ({reason}); restoring runtime state");
            if let Err(error) = bridge.restore_runtime_state(true).await {
                warn!("MT5 bridge automatic recovery failed: {error}");
            }
            bridge.recovery_scheduled.store(false, Ordering::SeqCst);
        });
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
        quote.symbol = self.aqe_symbol(&quote.symbol);
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
        bar.symbol = self.aqe_symbol(&bar.symbol);
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
            bar.symbol = self.aqe_symbol(&bar.symbol);
        }
        let frame = bars_to_frame(&bars)?;
        self.state.write().history.insert(symbol.to_string(), bars);
        Ok(frame)
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
                Ok(Ok(response))
                    if Self::is_idempotent_update_order_response(
                        action,
                        response.message.as_deref(),
                    ) =>
                {
                    pending_guard.dismiss();
                    debug!(
                        "Treating MT5 UPDATE_ORDER retcode 10025 as success because the requested protection levels were already applied"
                    );
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

    fn is_idempotent_update_order_response(action: Mt5RpcAction, message: Option<&str>) -> bool {
        matches!(action, Mt5RpcAction::UpdateOrder)
            && message.is_some_and(|message| message.trim() == "10025")
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
        let subscribers = self.bar_subscribers.lock().clone();
        let mut specs = HashSet::new();
        for subscriber in &subscribers {
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
            self.state.write().last_subscription_sync = Some(Utc::now());
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
        self.state.write().last_subscription_sync = Some(Utc::now());
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
        let quotes = payload.quotes;
        let stream_bars = payload.bars;
        let history = payload.history;
        let mut accepted_quotes = Vec::with_capacity(quotes.len());
        let mut accepted_stream_bars = Vec::with_capacity(stream_bars.len());

        {
            let mut state = self.state.write();
            state.last_market_data = Some(Utc::now());
            for mut quote in quotes {
                quote.symbol = self.aqe_symbol(&quote.symbol);
                let is_new_tick = state
                    .latest_quotes
                    .get(&quote.symbol)
                    .map(|previous| {
                        quote.timestamp > previous.timestamp
                            || (quote.timestamp == previous.timestamp
                                && (quote.bid != previous.bid
                                    || quote.ask != previous.ask
                                    || quote.last != previous.last))
                    })
                    .unwrap_or(true);
                if !is_new_tick {
                    continue;
                }
                state
                    .latest_quotes
                    .insert(quote.symbol.clone(), quote.clone());
                accepted_quotes.push(quote);
            }
            for mut bar in history {
                bar.symbol = self.aqe_symbol(&bar.symbol);
                state
                    .history
                    .entry(bar.symbol.clone())
                    .or_insert_with(Vec::new)
                    .push(bar);
            }
            for mut stream_bar in stream_bars {
                stream_bar.bar.symbol = self.aqe_symbol(&stream_bar.bar.symbol);
                let timeframe = stream_bar
                    .timeframe
                    .clone()
                    .unwrap_or_else(|| "PERIOD_M1".to_string());
                let stream_key = (stream_bar.bar.symbol.clone(), timeframe.clone());
                let is_new_completed_bar = state
                    .latest_bars_by_stream
                    .get(&stream_key)
                    .map(|previous| stream_bar.bar.timestamp > previous.timestamp)
                    .unwrap_or(true);
                if !is_new_completed_bar {
                    continue;
                }
                state
                    .latest_bars
                    .insert(stream_bar.bar.symbol.clone(), stream_bar.bar.clone());
                state
                    .latest_bars_by_stream
                    .insert(stream_key, stream_bar.bar.clone());
                if let Some(timeframe) = stream_bar.timeframe.as_ref() {
                    state
                        .latest_intrabar_bars
                        .remove(&(stream_bar.bar.symbol.clone(), timeframe.clone()));
                }
                accepted_stream_bars.push(stream_bar);
            }
        }

        let subscribers = self.bar_subscribers.lock().clone();

        for stream_bar in accepted_stream_bars {
            let timeframe_code = stream_bar.timeframe.as_deref().unwrap_or("PERIOD_M1");
            for subscriber in &subscribers {
                if subscriber.session_id == session_id
                    && subscriber.symbol == stream_bar.bar.symbol
                    && subscriber.timeframe_code == timeframe_code
                    && subscriber.mode == DataStreamMode::CompletedBar
                {
                    (subscriber.callback)(stream_bar.bar.clone());
                }
            }
        }

        for quote in accepted_quotes {
            for subscriber in &subscribers {
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
        let dropped_event = {
            let mut queue = self.event_queue.lock();
            let dropped = if queue.len() >= MAX_TRADE_EVENT_QUEUE_LEN {
                queue.pop_front();
                true
            } else {
                false
            };
            queue.push_back(QueuedTradeEvent {
                sequence,
                order,
                event,
            });
            dropped
        };
        let mut state = self.state.write();
        if dropped_event {
            state.dropped_trade_event_count += 1;
        }
        state.last_trade_event_sequence = Some(sequence);
        drop(state);

        let subscribers = self.trade_subscribers.lock().clone();
        for subscriber in subscribers {
            subscriber(subscriber_event.clone());
        }
    }

    fn mark_event_seen(&self, event_key: String) -> bool {
        let mut state = self.state.write();
        if state.seen_event_ids.contains(&event_key) {
            return false;
        }
        if state.seen_event_order.len() >= MAX_SEEN_EVENT_IDS {
            if let Some(expired) = state.seen_event_order.pop_front() {
                state.seen_event_ids.remove(&expired);
            }
        }
        state.seen_event_ids.insert(event_key.clone());
        state.seen_event_order.push_back(event_key);
        true
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

        if !self.mark_event_seen(event_key) {
            return;
        }

        let mut order = payload.order;
        self.map_order_to_aqe(&mut order);
        self.enqueue_trade_event(sequence, order, payload.event);
    }

    fn apply_trade_event_batch(
        &self,
        session_id: &str,
        envelope_sequence: Option<u64>,
        mut events: Vec<Mt5TradeEventPayload>,
    ) -> usize {
        let accepted = events.len();
        // Batches are FIFO on the EA, but sorting by the immutable event
        // sequence also keeps callbacks deterministic for retried or manually
        // constructed clients. Rust's stable sort preserves arrival order for
        // legacy events which do not carry an event sequence.
        events.sort_by_key(|event| event.event_seq.unwrap_or(u64::MAX));
        for payload in events {
            self.apply_trade_event(session_id, envelope_sequence, payload);
        }
        accepted
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
        asset.symbol = self.aqe_symbol(&asset.symbol);
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
    ReconcileOrders,
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
    #[serde(default)]
    pub terminal_connected: Option<bool>,
    #[serde(default)]
    pub terminal_trade_allowed: Option<bool>,
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

async fn health(bridge: web::Data<Mt5Bridge>) -> HttpResponse {
    let diagnostics = bridge.diagnostics();
    HttpResponse::Ok().json(Mt5BridgeResponse::ok(
        &bridge.session_id(),
        Uuid::new_v4().to_string(),
        Some(serde_json::json!({
            "connected": bridge.is_connected(),
            "transportConnected": bridge.is_transport_connected(),
            "brokerConnected": bridge.is_broker_connected(),
            "datafeedConnected": bridge.is_datafeed_connected(),
            "protocolVersion": PROTOCOL_VERSION,
            "diagnostics": diagnostics,
        })),
    ))
}

async fn heartbeat(
    bridge: web::Data<Mt5Bridge>,
    request: HttpRequest,
    envelope: web::Json<Mt5BridgeEnvelope<Mt5HeartbeatPayload>>,
) -> HttpResponse {
    let envelope = envelope.into_inner();
    if let Err(message) =
        authorize_session_token(bridge.get_ref(), request.headers(), &envelope.session_id)
    {
        return bridge_error(
            bridge.get_ref(),
            envelope.request_id,
            StatusCode::UNAUTHORIZED,
            message,
        );
    }
    let mut state = bridge.state.write();
    let broker_reconnected = state.terminal_connected == Some(false)
        && envelope.payload.terminal_connected == Some(true);
    state.last_heartbeat = Some(Utc::now());
    state
        .active_sessions
        .insert(envelope.session_id.clone(), Utc::now());
    state.terminal_name = envelope.payload.terminal_name;
    state.heartbeat_account_id = envelope.payload.account_id;
    state.terminal_connected = envelope.payload.terminal_connected;
    state.terminal_trade_allowed = envelope.payload.terminal_trade_allowed;
    drop(state);
    if broker_reconnected {
        bridge.schedule_runtime_recovery("MT5 terminal reconnected to its broker");
    }
    let session_id = bridge.session_id();
    HttpResponse::Ok().json(Mt5BridgeResponse::ok(
        &session_id,
        envelope.request_id,
        Some(serde_json::json!({ "serverTime": Utc::now() })),
    ))
}

async fn snapshot(
    bridge: web::Data<Mt5Bridge>,
    request: HttpRequest,
    envelope: web::Json<Mt5BridgeEnvelope<Mt5SnapshotPayload>>,
) -> HttpResponse {
    let envelope = envelope.into_inner();
    if let Err(message) =
        authorize_session_token(bridge.get_ref(), request.headers(), &envelope.session_id)
    {
        return bridge_error(
            bridge.get_ref(),
            envelope.request_id,
            StatusCode::UNAUTHORIZED,
            message,
        );
    }
    bridge.register_session(&envelope.session_id);
    bridge.apply_snapshot(&envelope.session_id, envelope.payload);
    let session_id = bridge.session_id();
    HttpResponse::Ok().json(Mt5BridgeResponse::ok(
        &session_id,
        envelope.request_id,
        Some(serde_json::json!({ "accepted": true })),
    ))
}

async fn market_data(
    bridge: web::Data<Mt5Bridge>,
    request: HttpRequest,
    envelope: web::Json<Mt5BridgeEnvelope<Mt5MarketDataPayload>>,
) -> HttpResponse {
    let envelope = envelope.into_inner();
    if let Err(message) =
        authorize_session_token(bridge.get_ref(), request.headers(), &envelope.session_id)
    {
        return bridge_error(
            bridge.get_ref(),
            envelope.request_id,
            StatusCode::UNAUTHORIZED,
            message,
        );
    }
    let request_id = envelope.request_id;
    let session_id = envelope.session_id;
    bridge.register_session(&session_id);
    if let Err(message) = bridge.enqueue_market_data_ingress(MarketDataIngress {
        session_id,
        payload: envelope.payload,
    }) {
        return bridge_error(
            bridge.get_ref(),
            request_id,
            StatusCode::SERVICE_UNAVAILABLE,
            message.to_string(),
        );
    }
    let session_id = bridge.session_id();
    HttpResponse::Ok().json(Mt5BridgeResponse::ok(
        &session_id,
        request_id,
        Some(serde_json::json!({
            "accepted": true,
            "queued": bridge.market_data_ingress_depth.load(Ordering::Relaxed),
        })),
    ))
}

async fn trade_event(
    bridge: web::Data<Mt5Bridge>,
    request: HttpRequest,
    envelope: web::Json<Mt5BridgeEnvelope<Mt5TradeEventPayload>>,
) -> HttpResponse {
    let envelope = envelope.into_inner();
    if let Err(message) =
        authorize_session_token(bridge.get_ref(), request.headers(), &envelope.session_id)
    {
        return bridge_error(
            bridge.get_ref(),
            envelope.request_id,
            StatusCode::UNAUTHORIZED,
            message,
        );
    }
    let request_id = envelope.request_id;
    let session_id = envelope.session_id;
    bridge.register_session(&session_id);
    let accepted = match bridge.enqueue_trade_ingress(TradeIngress {
        session_id,
        envelope_sequence: envelope.event_seq,
        events: vec![envelope.payload],
        dropped_count: 0,
    }) {
        Ok(accepted) => accepted,
        Err(message) => {
            return bridge_error(
                bridge.get_ref(),
                request_id,
                StatusCode::SERVICE_UNAVAILABLE,
                message.to_string(),
            );
        }
    };
    let session_id = bridge.session_id();
    HttpResponse::Ok().json(Mt5BridgeResponse::ok(
        &session_id,
        request_id,
        Some(serde_json::json!({
            "accepted": accepted,
            "queued": bridge.trade_ingress_depth.load(Ordering::Relaxed),
        })),
    ))
}

async fn trade_events(
    bridge: web::Data<Mt5Bridge>,
    request: HttpRequest,
    envelope: web::Json<Mt5BridgeEnvelope<Mt5TradeEventsPayload>>,
) -> HttpResponse {
    let envelope = envelope.into_inner();
    if let Err(message) =
        authorize_session_token(bridge.get_ref(), request.headers(), &envelope.session_id)
    {
        return bridge_error(
            bridge.get_ref(),
            envelope.request_id,
            StatusCode::UNAUTHORIZED,
            message,
        );
    }
    let request_id = envelope.request_id;
    let session_id = envelope.session_id;
    bridge.register_session(&session_id);
    let accepted = match bridge.enqueue_trade_ingress(TradeIngress {
        session_id,
        envelope_sequence: envelope.event_seq,
        events: envelope.payload.events,
        dropped_count: envelope.payload.dropped_count.unwrap_or(0),
    }) {
        Ok(accepted) => accepted,
        Err(message) => {
            return bridge_error(
                bridge.get_ref(),
                request_id,
                StatusCode::SERVICE_UNAVAILABLE,
                message.to_string(),
            );
        }
    };
    let session_id = bridge.session_id();
    HttpResponse::Ok().json(Mt5BridgeResponse::ok(
        &session_id,
        request_id,
        Some(serde_json::json!({
            "accepted": accepted,
            "queued": bridge.trade_ingress_depth.load(Ordering::Relaxed),
        })),
    ))
}

async fn poll_rpc(
    bridge: web::Data<Mt5Bridge>,
    request: HttpRequest,
    envelope: web::Json<Mt5BridgeEnvelope<Mt5RpcPollPayload>>,
) -> HttpResponse {
    let envelope = envelope.into_inner();
    bridge.record_rpc_poll_attempt(&envelope.session_id);
    if let Err(message) = authorize_token(bridge.get_ref(), request.headers()) {
        bridge.record_rpc_poll_auth_failure(message.clone());
        let session_id = bridge.session_id();
        return HttpResponse::Unauthorized().json(Mt5BridgeResponse::<Mt5RpcPollResponse>::error(
            &session_id,
            envelope.request_id,
            message,
        ));
    }
    let session_id = bridge.session_id();
    let had_stale_transport = bridge
        .state
        .read()
        .last_rpc_poll
        .map(|poll| Utc::now() - poll >= chrono::Duration::seconds(MT5_TRANSPORT_FRESH_SECS))
        .unwrap_or(false);
    let ea_session_reset = !envelope.session_id.is_empty() && envelope.session_id != session_id;
    if !envelope.session_id.is_empty() && envelope.session_id != session_id {
        warn!(
            "MT5 EA polled with stale session {}; resetting to {}",
            envelope.session_id, session_id
        );
    }
    bridge.record_rpc_poll(&session_id);
    if !bridge.bar_subscribers.lock().is_empty() && (had_stale_transport || ea_session_reset) {
        bridge.schedule_runtime_recovery(if ea_session_reset {
            "EA session reset"
        } else {
            "authorized polling resumed after a stale interval"
        });
    }

    let max_requests = envelope.payload.max_requests.unwrap_or(32).clamp(1, 256);
    // MT5 WebRequest under Wine can block indefinitely on an empty long-poll.
    // The EA already polls on a short timer, so acknowledge empty polls immediately.
    let (requests, queued_remaining) = bridge.drain_rpc_requests(max_requests);
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
    HttpResponse::Ok().json(Mt5BridgeResponse::ok(
        &session_id,
        envelope.request_id,
        Some(Mt5RpcPollResponse {
            requests,
            poll_interval_ms: bridge.config.poll_interval.as_millis() as u64,
            session_id: response_session_id,
        }),
    ))
}

async fn rpc_response(
    bridge: web::Data<Mt5Bridge>,
    request: HttpRequest,
    envelope: web::Json<Mt5BridgeEnvelope<Mt5RpcResponsePayload>>,
) -> HttpResponse {
    let envelope = envelope.into_inner();
    if let Err(message) =
        authorize_session_token(bridge.get_ref(), request.headers(), &envelope.session_id)
    {
        return bridge_error(
            bridge.get_ref(),
            envelope.request_id,
            StatusCode::UNAUTHORIZED,
            message,
        );
    }
    bridge.register_session(&envelope.session_id);
    bridge.apply_rpc_response(envelope.payload);
    let session_id = bridge.session_id();
    HttpResponse::Ok().json(Mt5BridgeResponse::ok(
        &session_id,
        envelope.request_id,
        Some(serde_json::json!({ "accepted": true })),
    ))
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
) -> HttpResponse {
    HttpResponse::build(status).json(Mt5BridgeResponse::<serde_json::Value>::error(
        &bridge.session_id(),
        request_id,
        message,
    ))
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

fn bars_to_frame(bars: &[Bar]) -> Result<DataFrame, BrokerError> {
    DataFrame::new(
        bars.len(),
        vec![
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
        ],
    )
    .map_err(|error| BrokerError::DataFeedError(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::broker::types::{
        OrderClass, OrderLeg, OrderLegs, OrderSide, OrderType, TimeInForce,
    };
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

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
        headers.insert(
            "x-aqe-mt5-session".parse().unwrap(),
            session_id.parse().unwrap(),
        );
        headers.insert("x-aqe-mt5-token".parse().unwrap(), token.parse().unwrap());
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
    fn reconcile_orders_action_uses_the_expected_ea_wire_name() {
        assert_eq!(
            serde_json::to_value(Mt5RpcAction::ReconcileOrders).unwrap(),
            serde_json::json!("RECONCILE_ORDERS")
        );
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
    fn trade_event_batches_notify_subscribers_in_sequence_order() {
        let bridge = test_bridge("test-token");
        let callback_order = Arc::new(Mutex::new(Vec::new()));
        let callback_order_clone = callback_order.clone();
        bridge.subscribe_trade_stream(Arc::new(move |(order, _event)| {
            callback_order_clone.lock().push(order.order_id);
        }));

        bridge.apply_trade_event_batch(
            "session-1",
            None,
            vec![
                Mt5TradeEventPayload {
                    native_event_id: Some("event-2".to_string()),
                    event_seq: Some(2),
                    event: TradeUpdateEvent::Closed,
                    order: trade_event_order("order-2", "insight-1", TradeUpdateEvent::Closed),
                },
                Mt5TradeEventPayload {
                    native_event_id: Some("event-1".to_string()),
                    event_seq: Some(1),
                    event: TradeUpdateEvent::Filled,
                    order: trade_event_order("order-1", "insight-1", TradeUpdateEvent::Filled),
                },
            ],
        );

        assert_eq!(
            callback_order.lock().as_slice(),
            ["order-1".to_string(), "order-2".to_string()]
        );
    }

    #[tokio::test]
    async fn ordered_trade_ingress_acknowledges_before_dispatch_and_preserves_sequence() {
        let bridge = Arc::new(test_bridge("test-token"));
        let callback_order = Arc::new(Mutex::new(Vec::new()));
        let callback_order_clone = Arc::clone(&callback_order);
        bridge.subscribe_trade_stream(Arc::new(move |(order, _event)| {
            callback_order_clone.lock().push(order.order_id);
        }));
        bridge.start_ingress_workers();

        let accepted = bridge
            .enqueue_trade_ingress(TradeIngress {
                session_id: "session-1".to_string(),
                envelope_sequence: None,
                events: vec![
                    Mt5TradeEventPayload {
                        native_event_id: Some("event-2".to_string()),
                        event_seq: Some(2),
                        event: TradeUpdateEvent::Closed,
                        order: trade_event_order("order-2", "insight-1", TradeUpdateEvent::Closed),
                    },
                    Mt5TradeEventPayload {
                        native_event_id: Some("event-1".to_string()),
                        event_seq: Some(1),
                        event: TradeUpdateEvent::Filled,
                        order: trade_event_order("order-1", "insight-1", TradeUpdateEvent::Filled),
                    },
                ],
                dropped_count: 0,
            })
            .unwrap();

        assert_eq!(accepted, 2);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if callback_order.lock().len() == 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("ordered ingress worker should dispatch promptly");
        assert_eq!(
            callback_order.lock().as_slice(),
            ["order-1".to_string(), "order-2".to_string()]
        );
        bridge.stop_ingress_workers();
    }

    #[test]
    fn market_data_ingress_applies_bounded_backpressure() {
        let bridge = test_bridge("test-token");
        let (sender, _receiver) = mpsc::channel::<MarketDataIngress>(2);
        *bridge.market_data_ingress_tx.lock() = Some(sender);

        for _ in 0..2 {
            bridge
                .enqueue_market_data_ingress(MarketDataIngress {
                    session_id: "session-1".to_string(),
                    payload: Mt5MarketDataPayload::default(),
                })
                .unwrap();
        }
        let error = bridge
            .enqueue_market_data_ingress(MarketDataIngress {
                session_id: "session-1".to_string(),
                payload: Mt5MarketDataPayload::default(),
            })
            .unwrap_err();

        assert!(error.contains("queue is full"));
        assert_eq!(bridge.market_data_ingress_depth.load(Ordering::Relaxed), 2);
    }

    #[actix_web::test]
    async fn actix_health_endpoint_preserves_bridge_response_shape() {
        let bridge = Arc::new(test_bridge("test-token"));
        let bridge_data: web::Data<Mt5Bridge> = web::Data::from(bridge);
        let app = actix_web::test::init_service(
            App::new()
                .app_data(bridge_data)
                .route("/health", web::get().to(health)),
        )
        .await;

        let request = actix_web::test::TestRequest::get()
            .uri("/health")
            .to_request();
        let response: Mt5BridgeResponse<Value> =
            actix_web::test::call_and_read_body_json(&app, request).await;

        assert!(response.ok);
        assert_eq!(response.protocol_version, PROTOCOL_VERSION);
        assert!(response.payload.is_some());
    }

    #[tokio::test]
    async fn actix_bridge_server_starts_and_stops_on_tokio_runtime() {
        let bridge = Arc::new(test_bridge("test-token"));

        assert!(bridge.start().await.unwrap());
        assert!(bridge.server_started.load(Ordering::SeqCst));
        assert!(bridge.server_control.lock().is_some());

        bridge.shutdown().await;
        assert!(!bridge.server_started.load(Ordering::SeqCst));
        assert!(bridge.server_control.lock().is_none());
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
    fn trade_event_dedupe_evicts_oldest_ids_at_capacity() {
        let bridge = test_bridge("test-token");
        for index in 0..=MAX_SEEN_EVENT_IDS {
            assert!(bridge.mark_event_seen(format!("event-{index}")));
        }

        let state = bridge.state.read();
        assert_eq!(state.seen_event_ids.len(), MAX_SEEN_EVENT_IDS);
        assert_eq!(state.seen_event_order.len(), MAX_SEEN_EVENT_IDS);
        assert!(!state.seen_event_ids.contains("event-0"));
        assert!(
            state
                .seen_event_ids
                .contains(&format!("event-{MAX_SEEN_EVENT_IDS}"))
        );
    }

    #[test]
    fn reverse_symbol_mapping_uses_precomputed_index() {
        let mut config = test_bridge("test-token").config().clone();
        config
            .symbol_map
            .insert("XAUUSD".to_string(), "GOLD.cash".to_string());
        let bridge = Mt5Bridge::new(config);

        assert_eq!(bridge.aqe_symbol("GOLD.cash"), "XAUUSD");
        assert_eq!(bridge.aqe_symbol("EURUSD"), "EURUSD");
    }

    #[test]
    fn terminal_disconnect_is_distinct_from_bridge_transport() {
        let bridge = test_bridge("test-token");
        {
            let mut state = bridge.state.write();
            state.last_rpc_poll = Some(Utc::now());
            state.terminal_connected = Some(false);
        }

        assert!(bridge.is_transport_connected());
        assert!(!bridge.is_broker_connected());
        assert!(!bridge.is_datafeed_connected());

        bridge.state.write().terminal_connected = Some(true);
        assert!(bridge.is_broker_connected());
    }

    #[test]
    fn update_order_no_changes_response_is_idempotent_success() {
        assert!(Mt5Bridge::is_idempotent_update_order_response(
            Mt5RpcAction::UpdateOrder,
            Some("10025")
        ));
        assert!(!Mt5Bridge::is_idempotent_update_order_response(
            Mt5RpcAction::SubmitOrder,
            Some("10025")
        ));
        assert!(!Mt5Bridge::is_idempotent_update_order_response(
            Mt5RpcAction::UpdateOrder,
            Some("10016")
        ));
    }

    #[test]
    fn datafeed_connection_requires_fresh_market_data_for_active_subscriptions() {
        let bridge = test_bridge("test-token");
        bridge
            .subscribe_bars(
                vec!["AAPL".to_string()],
                TimeFrame::new(1, TimeFrameUnit::Minute),
                DataStreamMode::CompletedBar,
                Arc::new(|_bar| {}),
            )
            .unwrap();
        {
            let mut state = bridge.state.write();
            state.last_rpc_poll = Some(Utc::now());
            state.terminal_connected = Some(true);
            state.last_subscription_sync = Some(Utc::now());
        }

        assert!(bridge.is_datafeed_connected());

        bridge.state.write().last_subscription_sync =
            Some(Utc::now() - chrono::Duration::seconds(MT5_MARKET_DATA_FRESH_SECS + 1));
        assert!(!bridge.is_datafeed_connected());

        bridge.state.write().last_market_data = Some(Utc::now());
        assert!(bridge.is_datafeed_connected());
    }

    #[tokio::test]
    async fn datafeed_recovery_replays_active_bar_subscriptions() {
        let bridge = Arc::new(test_bridge("test-token"));
        bridge
            .subscribe_bars(
                vec!["AAPL".to_string()],
                TimeFrame::new(1, TimeFrameUnit::Minute),
                DataStreamMode::CompletedBar,
                Arc::new(|_bar| {}),
            )
            .unwrap();
        {
            let mut state = bridge.state.write();
            state.last_rpc_poll = Some(Utc::now());
            state.terminal_connected = Some(true);
        }

        let recovering_bridge = Arc::clone(&bridge);
        let recovery =
            tokio::spawn(async move { recovering_bridge.recover_datafeed_state().await });
        let request = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(request) = bridge.rpc_queue.lock().pop_front() {
                    return request;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("subscription replay should queue an RPC request");

        assert!(matches!(request.action, Mt5RpcAction::SubscribeBars));
        bridge.apply_rpc_response(Mt5RpcResponsePayload {
            request_id: request.request_id,
            ok: true,
            message: None,
            payload: Some(serde_json::json!({})),
        });

        recovery.await.unwrap().unwrap();
        assert!(bridge.state.read().last_subscription_sync.is_some());
        assert!(bridge.is_datafeed_connected());
    }

    #[test]
    fn older_ea_heartbeat_payloads_remain_compatible() {
        let payload: Mt5HeartbeatPayload = serde_json::from_value(serde_json::json!({
            "terminalName": "MetaTrader 5",
            "accountId": "12345",
            "serverTime": "2026-08-27T10:00:00Z"
        }))
        .unwrap();

        assert_eq!(payload.terminal_connected, None);
        assert_eq!(payload.terminal_trade_allowed, None);
    }

    #[test]
    fn trade_callbacks_can_clear_subscribers_reentrantly() {
        let bridge = Arc::new(test_bridge("test-token"));
        let callback_bridge = bridge.clone();
        let callback_count = Arc::new(AtomicUsize::new(0));
        let callback_count_clone = callback_count.clone();
        bridge.subscribe_trade_stream(Arc::new(move |_event| {
            callback_count_clone.fetch_add(1, AtomicOrdering::Relaxed);
            callback_bridge.clear_trade_subscribers();
        }));

        bridge.emit_order_event(
            trade_event_order("order-1", "insight-1", TradeUpdateEvent::Filled),
            TradeUpdateEvent::Filled,
        );

        assert_eq!(callback_count.load(AtomicOrdering::Relaxed), 1);
        assert!(bridge.trade_subscribers.lock().is_empty());
    }

    #[test]
    fn bar_callbacks_can_unsubscribe_reentrantly() {
        let bridge = Arc::new(test_bridge("test-token"));
        let session_id = bridge.session_id();
        let timeframe = TimeFrame::new(1, TimeFrameUnit::Minute);
        let callback_bridge = bridge.clone();
        let callback_count = Arc::new(AtomicUsize::new(0));
        let callback_count_clone = callback_count.clone();
        bridge
            .subscribe_bars(
                vec!["AAPL".to_string()],
                timeframe,
                DataStreamMode::CompletedBar,
                Arc::new(move |_bar| {
                    callback_count_clone.fetch_add(1, AtomicOrdering::Relaxed);
                    callback_bridge
                        .unsubscribe_bars(
                            vec!["AAPL".to_string()],
                            timeframe,
                            DataStreamMode::CompletedBar,
                        )
                        .unwrap();
                }),
            )
            .unwrap();

        bridge.apply_market_data(
            &session_id,
            Mt5MarketDataPayload {
                bars: vec![Mt5StreamBar {
                    timeframe: Some("PERIOD_M1".to_string()),
                    bar: Bar {
                        symbol: "AAPL".to_string(),
                        open: 100.0,
                        high: 101.0,
                        low: 99.0,
                        close: 100.5,
                        volume: 10.0,
                        timestamp: Utc::now(),
                    },
                }],
                ..Default::default()
            },
        );

        assert_eq!(callback_count.load(AtomicOrdering::Relaxed), 1);
        assert!(bridge.bar_subscribers.lock().is_empty());
    }

    #[test]
    fn retried_completed_bars_are_emitted_once_per_stream() {
        let bridge = test_bridge("test-token");
        let session_id = bridge.session_id();
        let timeframe = TimeFrame::new(1, TimeFrameUnit::Minute);
        let callback_count = Arc::new(AtomicUsize::new(0));
        let callback_count_clone = callback_count.clone();
        bridge
            .subscribe_bars(
                vec!["AAPL".to_string()],
                timeframe,
                DataStreamMode::CompletedBar,
                Arc::new(move |_bar| {
                    callback_count_clone.fetch_add(1, AtomicOrdering::Relaxed);
                }),
            )
            .unwrap();

        let completed_bar = Mt5StreamBar {
            timeframe: Some("PERIOD_M1".to_string()),
            bar: Bar {
                symbol: "AAPL".to_string(),
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
                volume: 10.0,
                timestamp: Utc::now(),
            },
        };
        for _ in 0..2 {
            bridge.apply_market_data(
                &session_id,
                Mt5MarketDataPayload {
                    bars: vec![completed_bar.clone()],
                    ..Default::default()
                },
            );
        }

        assert_eq!(callback_count.load(AtomicOrdering::Relaxed), 1);
    }

    #[test]
    fn unchanged_intrabar_ticks_are_not_emitted_twice() {
        let bridge = test_bridge("test-token");
        let session_id = bridge.session_id();
        let timeframe = TimeFrame::new(1, TimeFrameUnit::Minute);
        let callback_count = Arc::new(AtomicUsize::new(0));
        let callback_count_clone = callback_count.clone();
        bridge
            .subscribe_bars(
                vec!["AAPL".to_string()],
                timeframe,
                DataStreamMode::Intrabar,
                Arc::new(move |_bar| {
                    callback_count_clone.fetch_add(1, AtomicOrdering::Relaxed);
                }),
            )
            .unwrap();

        let quote = Quote {
            symbol: "AAPL".to_string(),
            bid: 100.0,
            ask: 100.2,
            bid_size: 0.0,
            ask_size: 0.0,
            last: Some(100.1),
            last_size: None,
            timestamp: Utc::now(),
        };
        for _ in 0..2 {
            bridge.apply_market_data(
                &session_id,
                Mt5MarketDataPayload {
                    quotes: vec![quote.clone()],
                    ..Default::default()
                },
            );
        }

        assert_eq!(callback_count.load(AtomicOrdering::Relaxed), 1);
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
