use super::types::{
    Account, Asset, AssetExchange, AssetStatus, AssetType, Bar, BrokerError, Order, OrderSide,
    OrderType, Position, Quote, TradeUpdateEvent,
};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use log::{error, info, warn};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::task::JoinHandle;
use uuid::Uuid;

const PROTOCOL_VERSION: u16 = 1;
const DEFAULT_BIND_ADDR: &str = "127.0.0.1:18080";
const DEFAULT_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_POLL_INTERVAL_MS: u64 = 250;
const MAX_QUEUE_LEN: usize = 10_000;

static SHARED_MT5_BRIDGE: OnceLock<Arc<Mt5Bridge>> = OnceLock::new();

#[derive(Clone, Debug)]
pub struct Mt5BridgeConfig {
    pub bind_addr: SocketAddr,
    pub token: String,
    pub session_id: String,
    pub request_timeout: Duration,
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

        let session_id =
            std::env::var("AQE_MT5_SESSION_ID").unwrap_or_else(|_| Uuid::new_v4().to_string());
        let request_timeout = Duration::from_millis(read_env_u64(
            "AQE_MT5_REQUEST_TIMEOUT_MS",
            DEFAULT_TIMEOUT_MS,
        ));
        let poll_interval = Duration::from_millis(read_env_u64(
            "AQE_MT5_POLL_INTERVAL_MS",
            DEFAULT_POLL_INTERVAL_MS,
        ));
        let symbol_map = parse_symbol_map(std::env::var("AQE_MT5_SYMBOL_MAP").ok());

        Ok(Self {
            bind_addr,
            token,
            session_id,
            request_timeout,
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
    state: Arc<RwLock<Mt5BridgeState>>,
    command_queue: Arc<Mutex<VecDeque<Mt5Command>>>,
    event_queue: Arc<Mutex<VecDeque<(Order, TradeUpdateEvent)>>>,
    trade_subscribers: Arc<Mutex<Vec<Arc<dyn Fn((Order, TradeUpdateEvent)) + Send + Sync>>>>,
    bar_subscribers: Arc<Mutex<HashMap<String, Vec<Arc<dyn Fn(Bar) + Send + Sync>>>>>,
    server_started: Arc<AtomicBool>,
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
    history: HashMap<String, Vec<Bar>>,
    seen_event_ids: HashSet<String>,
    last_heartbeat: Option<DateTime<Utc>>,
    terminal_name: Option<String>,
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
            state: Arc::new(RwLock::new(Mt5BridgeState::default())),
            command_queue: Arc::new(Mutex::new(VecDeque::new())),
            event_queue: Arc::new(Mutex::new(VecDeque::new())),
            trade_subscribers: Arc::new(Mutex::new(Vec::new())),
            bar_subscribers: Arc::new(Mutex::new(HashMap::new())),
            server_started: Arc::new(AtomicBool::new(false)),
            server_handle: Arc::new(Mutex::new(None)),
        }
    }

    pub fn config(&self) -> &Mt5BridgeConfig {
        &self.config
    }

    pub async fn start(self: &Arc<Self>) -> Result<bool, BrokerError> {
        if self.server_started.swap(true, Ordering::SeqCst) {
            return Ok(true);
        }

        let bind_addr = self.config.bind_addr;
        let app = Router::new()
            .route("/health", get(health))
            .route("/v1/heartbeat", post(heartbeat))
            .route("/v1/snapshot", post(snapshot))
            .route("/v1/market-data", post(market_data))
            .route("/v1/trade-event", post(trade_event))
            .route("/v1/commands/poll", post(poll_commands))
            .route("/v1/commands/ack", post(command_ack))
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

        info!("AQE MT5 bridge listening on http://{}", bind_addr);
        let handle = tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, app).await {
                error!("AQE MT5 bridge server stopped: {}", error);
            }
        });
        *self.server_handle.lock() = Some(handle);
        Ok(true)
    }

    pub fn stop(&self) {
        if let Some(handle) = self.server_handle.lock().take() {
            handle.abort();
        }
        self.server_started.store(false, Ordering::SeqCst);
    }

    pub fn is_connected(&self) -> bool {
        let state = self.state.read();
        state
            .last_heartbeat
            .map(|heartbeat| Utc::now() - heartbeat < chrono::Duration::seconds(15))
            .unwrap_or(false)
    }

    pub fn account(&self) -> Result<Account, BrokerError> {
        self.state
            .read()
            .account
            .clone()
            .ok_or_else(|| BrokerError::AccountError("MT5 account snapshot not available".into()))
    }

    pub fn orders(&self) -> Vec<Order> {
        self.state.read().orders.values().cloned().collect()
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

    pub fn latest_bar(&self, symbol: &str) -> Result<Bar, BrokerError> {
        self.state
            .read()
            .latest_bars
            .get(symbol)
            .cloned()
            .ok_or_else(|| BrokerError::DataFeedError(format!("MT5 bar {} not available", symbol)))
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

    pub fn enqueue_command(&self, command: Mt5Command) -> Result<(), BrokerError> {
        let mut queue = self.command_queue.lock();
        if queue.len() >= MAX_QUEUE_LEN {
            return Err(BrokerError::TradeError(
                "MT5 command queue is full; bridge is not keeping up".into(),
            ));
        }
        queue.push_back(command);
        Ok(())
    }

    pub fn upsert_order(&self, order: Order) {
        self.state
            .write()
            .orders
            .insert(order.order_id.clone(), order);
    }

    pub fn drain_trade_events(&self) -> Vec<(Order, TradeUpdateEvent)> {
        let mut events = self.event_queue.lock();
        events.drain(..).collect()
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

    pub fn subscribe_bars(&self, symbols: Vec<String>, on_bar: Arc<dyn Fn(Bar) + Send + Sync>) {
        let mut subscribers = self.bar_subscribers.lock();
        for symbol in symbols {
            subscribers
                .entry(symbol)
                .or_insert_with(Vec::new)
                .push(on_bar.clone());
        }
    }

    pub fn unsubscribe_bars(&self, symbols: Vec<String>) {
        let mut subscribers = self.bar_subscribers.lock();
        for symbol in symbols {
            subscribers.remove(&symbol);
        }
    }

    fn apply_snapshot(&self, payload: Mt5SnapshotPayload) {
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

    fn apply_market_data(&self, payload: Mt5MarketDataPayload) {
        {
            let mut state = self.state.write();
            for quote in payload.quotes {
                state.latest_quotes.insert(quote.symbol.clone(), quote);
            }
            for bar in &payload.history {
                state
                    .history
                    .entry(bar.symbol.clone())
                    .or_insert_with(Vec::new)
                    .push(bar.clone());
            }
            for bar in &payload.bars {
                state.latest_bars.insert(bar.symbol.clone(), bar.clone());
            }
        }

        let subscribers = self.bar_subscribers.lock();
        for bar in payload.bars {
            if let Some(callbacks) = subscribers.get(&bar.symbol) {
                for callback in callbacks {
                    callback(bar.clone());
                }
            }
        }
    }

    fn apply_trade_event(&self, payload: Mt5TradeEventPayload) {
        let event_key = payload
            .native_event_id
            .clone()
            .unwrap_or_else(|| format!("{}:{:?}", payload.order.order_id, payload.event));

        {
            let mut state = self.state.write();
            if !state.seen_event_ids.insert(event_key) {
                return;
            }
            state
                .orders
                .insert(payload.order.order_id.clone(), payload.order.clone());
        }

        let event = (payload.order, payload.event);
        self.event_queue.lock().push_back(event.clone());
        for subscriber in self.trade_subscribers.lock().iter() {
            subscriber(event.clone());
        }
    }

    fn apply_command_ack(&self, payload: Mt5CommandAckPayload) {
        if let Some(order) = payload.order {
            self.upsert_order(order);
        }
        if !payload.ok {
            warn!(
                "MT5 command {} failed: {}",
                payload.command_id,
                payload
                    .message
                    .unwrap_or_else(|| "unknown error".to_string())
            );
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mt5Command {
    pub command_id: String,
    pub action: Mt5CommandAction,
    pub order: Option<Order>,
    pub order_id: Option<String>,
    pub symbol: Option<String>,
    pub qty: Option<f64>,
    pub price: Option<f64>,
    pub side: Option<OrderSide>,
    pub order_type: Option<OrderType>,
    pub take_profit: Option<f64>,
    pub stop_loss: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Mt5CommandAction {
    SubmitOrder,
    CancelOrder,
    ClosePosition,
    CloseAllPositions,
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
    pub bars: Vec<Bar>,
    #[serde(default)]
    pub history: Vec<Bar>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mt5TradeEventPayload {
    pub native_event_id: Option<String>,
    pub event: TradeUpdateEvent,
    pub order: Order,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mt5CommandPollPayload {
    pub max_commands: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mt5CommandPollResponse {
    pub commands: Vec<Mt5Command>,
    pub poll_interval_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mt5CommandAckPayload {
    pub command_id: String,
    pub ok: bool,
    pub message: Option<String>,
    pub order: Option<Order>,
}

async fn health(
    State(bridge): State<Arc<Mt5Bridge>>,
) -> Json<Mt5BridgeResponse<serde_json::Value>> {
    Json(Mt5BridgeResponse::ok(
        &bridge.config.session_id,
        Uuid::new_v4().to_string(),
        Some(serde_json::json!({
            "connected": bridge.is_connected(),
            "protocolVersion": PROTOCOL_VERSION,
        })),
    ))
}

async fn heartbeat(
    State(bridge): State<Arc<Mt5Bridge>>,
    headers: HeaderMap,
    Json(envelope): Json<Mt5BridgeEnvelope<Mt5HeartbeatPayload>>,
) -> (StatusCode, Json<Mt5BridgeResponse<serde_json::Value>>) {
    if let Err(message) = authorize(&bridge, &headers, &envelope.session_id) {
        return bridge_error(
            &bridge,
            envelope.request_id,
            StatusCode::UNAUTHORIZED,
            message,
        );
    }
    let mut state = bridge.state.write();
    state.last_heartbeat = Some(Utc::now());
    state.terminal_name = envelope.payload.terminal_name;
    (
        StatusCode::OK,
        Json(Mt5BridgeResponse::ok(
            &bridge.config.session_id,
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
    if let Err(message) = authorize(&bridge, &headers, &envelope.session_id) {
        return bridge_error(
            &bridge,
            envelope.request_id,
            StatusCode::UNAUTHORIZED,
            message,
        );
    }
    bridge.apply_snapshot(envelope.payload);
    (
        StatusCode::OK,
        Json(Mt5BridgeResponse::ok(
            &bridge.config.session_id,
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
    if let Err(message) = authorize(&bridge, &headers, &envelope.session_id) {
        return bridge_error(
            &bridge,
            envelope.request_id,
            StatusCode::UNAUTHORIZED,
            message,
        );
    }
    bridge.apply_market_data(envelope.payload);
    (
        StatusCode::OK,
        Json(Mt5BridgeResponse::ok(
            &bridge.config.session_id,
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
    if let Err(message) = authorize(&bridge, &headers, &envelope.session_id) {
        return bridge_error(
            &bridge,
            envelope.request_id,
            StatusCode::UNAUTHORIZED,
            message,
        );
    }
    bridge.apply_trade_event(envelope.payload);
    (
        StatusCode::OK,
        Json(Mt5BridgeResponse::ok(
            &bridge.config.session_id,
            envelope.request_id,
            Some(serde_json::json!({ "accepted": true })),
        )),
    )
}

async fn poll_commands(
    State(bridge): State<Arc<Mt5Bridge>>,
    headers: HeaderMap,
    Json(envelope): Json<Mt5BridgeEnvelope<Mt5CommandPollPayload>>,
) -> (StatusCode, Json<Mt5BridgeResponse<Mt5CommandPollResponse>>) {
    if let Err(message) = authorize(&bridge, &headers, &envelope.session_id) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(Mt5BridgeResponse::error(
                &bridge.config.session_id,
                envelope.request_id,
                message,
            )),
        );
    }

    let max_commands = envelope.payload.max_commands.unwrap_or(32).clamp(1, 256);
    let mut queue = bridge.command_queue.lock();
    let mut commands = Vec::new();
    for _ in 0..max_commands {
        let Some(command) = queue.pop_front() else {
            break;
        };
        commands.push(command);
    }

    (
        StatusCode::OK,
        Json(Mt5BridgeResponse::ok(
            &bridge.config.session_id,
            envelope.request_id,
            Some(Mt5CommandPollResponse {
                commands,
                poll_interval_ms: bridge.config.poll_interval.as_millis() as u64,
            }),
        )),
    )
}

async fn command_ack(
    State(bridge): State<Arc<Mt5Bridge>>,
    headers: HeaderMap,
    Json(envelope): Json<Mt5BridgeEnvelope<Mt5CommandAckPayload>>,
) -> (StatusCode, Json<Mt5BridgeResponse<serde_json::Value>>) {
    if let Err(message) = authorize(&bridge, &headers, &envelope.session_id) {
        return bridge_error(
            &bridge,
            envelope.request_id,
            StatusCode::UNAUTHORIZED,
            message,
        );
    }
    bridge.apply_command_ack(envelope.payload);
    (
        StatusCode::OK,
        Json(Mt5BridgeResponse::ok(
            &bridge.config.session_id,
            envelope.request_id,
            Some(serde_json::json!({ "accepted": true })),
        )),
    )
}

fn authorize(bridge: &Mt5Bridge, headers: &HeaderMap, session_id: &str) -> Result<(), String> {
    let header_session = header_value(headers, "x-aqe-mt5-session");
    let header_token = header_value(headers, "x-aqe-mt5-token");
    if session_id != bridge.config.session_id || header_session.as_deref() != Some(session_id) {
        return Err("invalid MT5 bridge session".to_string());
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
            &bridge.config.session_id,
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
    }
}
