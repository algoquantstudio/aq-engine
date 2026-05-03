use super::types::{
    Account, Asset, AssetExchange, AssetStatus, AssetType, Bar, BrokerError, Order, Position,
    Quote, TradeUpdateEvent,
};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use log::{error, info, warn};
use parking_lot::{Mutex, RwLock};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::core::utils::timeframe::{TimeFrame, TimeFrameUnit};

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
    session_id: Arc<RwLock<String>>,
    state: Arc<RwLock<Mt5BridgeState>>,
    rpc_queue: Arc<Mutex<VecDeque<Mt5RpcRequest>>>,
    pending_rpc: Arc<Mutex<HashMap<String, oneshot::Sender<Mt5RpcResponsePayload>>>>,
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
            session_id: Arc::new(RwLock::new(Uuid::new_v4().to_string())),
            state: Arc::new(RwLock::new(Mt5BridgeState::default())),
            rpc_queue: Arc::new(Mutex::new(VecDeque::new())),
            pending_rpc: Arc::new(Mutex::new(HashMap::new())),
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

    pub fn set_session_id(&self, session_id: impl Into<String>) {
        *self.session_id.write() = session_id.into();
    }

    pub fn session_id(&self) -> String {
        self.session_id.read().clone()
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
            .request_rpc(
                Mt5RpcAction::GetHistory,
                serde_json::json!({
                    "symbol": mt5_symbol,
                    "start": start,
                    "end": end,
                    "timeframe": timeframe,
                }),
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
        let response = self.request_rpc(action, payload).await?;
        let mut broker_order = deserialize_payload::<Order>(response, "MT5 order")?;
        self.map_order_to_aqe(&mut broker_order);
        if let Some(mut local_order) = order {
            local_order.order_id = broker_order.order_id.clone();
            local_order.filled_qty = broker_order.filled_qty;
            local_order.filled_price = broker_order.filled_price;
            local_order.status = broker_order.status.clone();
            local_order.filled_at = broker_order.filled_at;
            local_order.rejection_reason = broker_order.rejection_reason.clone();
            self.emit_order_event(local_order.clone(), local_order.status.clone());
            Ok(local_order)
        } else {
            self.emit_order_event(broker_order.clone(), broker_order.status.clone());
            Ok(broker_order)
        }
    }

    pub async fn request_rpc(
        &self,
        action: Mt5RpcAction,
        payload: Value,
    ) -> Result<Value, BrokerError> {
        let request_id = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        self.pending_rpc.lock().insert(request_id.clone(), tx);

        {
            let mut queue = self.rpc_queue.lock();
            if queue.len() >= MAX_QUEUE_LEN {
                self.pending_rpc.lock().remove(&request_id);
                return Err(BrokerError::TradeError(
                    "MT5 RPC queue is full; bridge is not keeping up".into(),
                ));
            }
            queue.push_back(Mt5RpcRequest {
                request_id: request_id.clone(),
                action,
                payload,
            });
            info!(
                "Queued MT5 RPC request {:?} id={} pending={} queued={}",
                action,
                request_id,
                self.pending_rpc.lock().len(),
                queue.len()
            );
        }

        match tokio::time::timeout(self.config.request_timeout, rx).await {
            Ok(Ok(response)) if response.ok => Ok(response.payload.unwrap_or(Value::Null)),
            Ok(Ok(response)) => Err(BrokerError::TradeError(
                response
                    .message
                    .unwrap_or_else(|| "MT5 RPC request failed".to_string()),
            )),
            Ok(Err(_)) => Err(BrokerError::ConnectionError(format!(
                "MT5 RPC response channel closed for request {}",
                request_id
            ))),
            Err(_) => {
                self.pending_rpc.lock().remove(&request_id);
                warn!(
                    "MT5 RPC request {:?} id={} timed out after {:?}. Check that the EA can reach the bridge URL and is polling /v1/rpc/poll.",
                    action, request_id, self.config.request_timeout
                );
                Err(BrokerError::ConnectionError(format!(
                    "MT5 RPC request {:?} timed out after {:?}",
                    action, self.config.request_timeout
                )))
            }
        }
    }

    pub fn enqueue_rpc(&self, request: Mt5RpcRequest) -> Result<(), BrokerError> {
        let mut queue = self.rpc_queue.lock();
        if queue.len() >= MAX_QUEUE_LEN {
            return Err(BrokerError::TradeError(
                "MT5 RPC queue is full; bridge is not keeping up".into(),
            ));
        }
        queue.push_back(request);
        Ok(())
    }

    pub fn upsert_order(&self, order: Order) {
        self.state
            .write()
            .orders
            .insert(order.order_id.clone(), order);
    }

    pub fn emit_order_event(&self, order: Order, event: TradeUpdateEvent) {
        self.upsert_order(order.clone());
        let event = (order, event);
        self.event_queue.lock().push_back(event.clone());
        for subscriber in self.trade_subscribers.lock().iter() {
            subscriber(event.clone());
        }
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

    pub async fn subscribe_bars_rpc(
        &self,
        symbols: Vec<String>,
        timeframe: TimeFrame,
        on_bar: Arc<dyn Fn(Bar) + Send + Sync>,
    ) -> Result<(), BrokerError> {
        let mt5_symbols: Vec<String> = symbols
            .iter()
            .map(|symbol| self.config.mt5_symbol(symbol).to_string())
            .collect();
        let timeframe = mt5_timeframe_code(timeframe)?;
        self.request_rpc(
            Mt5RpcAction::SubscribeBars,
            serde_json::json!({
                "symbols": mt5_symbols,
                "timeframe": timeframe,
            }),
        )
        .await?;
        self.subscribe_bars(symbols, on_bar);
        Ok(())
    }

    pub fn unsubscribe_bars(&self, symbols: Vec<String>) {
        let mut subscribers = self.bar_subscribers.lock();
        for symbol in symbols {
            subscribers.remove(&symbol);
        }
    }

    pub async fn unsubscribe_bars_rpc(&self, symbols: Vec<String>) -> Result<(), BrokerError> {
        let mt5_symbols: Vec<String> = symbols
            .iter()
            .map(|symbol| self.config.mt5_symbol(symbol).to_string())
            .collect();
        self.request_rpc(
            Mt5RpcAction::UnsubscribeBars,
            serde_json::json!({ "symbols": mt5_symbols }),
        )
        .await?;
        self.unsubscribe_bars(symbols);
        Ok(())
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

    fn apply_rpc_response(&self, payload: Mt5RpcResponsePayload) {
        let request_id = payload.request_id.clone();
        if let Some(tx) = self.pending_rpc.lock().remove(&request_id) {
            let _ = tx.send(payload);
        } else {
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

async fn health(
    State(bridge): State<Arc<Mt5Bridge>>,
) -> Json<Mt5BridgeResponse<serde_json::Value>> {
    Json(Mt5BridgeResponse::ok(
        &bridge.session_id(),
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
    if let Err(message) = authorize(&bridge, &headers, &envelope.session_id) {
        return bridge_error(
            &bridge,
            envelope.request_id,
            StatusCode::UNAUTHORIZED,
            message,
        );
    }
    bridge.apply_snapshot(envelope.payload);
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
    if let Err(message) = authorize(&bridge, &headers, &envelope.session_id) {
        return bridge_error(
            &bridge,
            envelope.request_id,
            StatusCode::UNAUTHORIZED,
            message,
        );
    }
    bridge.apply_market_data(envelope.payload);
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
    if let Err(message) = authorize(&bridge, &headers, &envelope.session_id) {
        return bridge_error(
            &bridge,
            envelope.request_id,
            StatusCode::UNAUTHORIZED,
            message,
        );
    }
    bridge.apply_trade_event(envelope.payload);
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

async fn poll_rpc(
    State(bridge): State<Arc<Mt5Bridge>>,
    headers: HeaderMap,
    Json(envelope): Json<Mt5BridgeEnvelope<Mt5RpcPollPayload>>,
) -> (StatusCode, Json<Mt5BridgeResponse<Mt5RpcPollResponse>>) {
    if let Err(message) = authorize_token(&bridge, &headers) {
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

    let max_requests = envelope.payload.max_requests.unwrap_or(32).clamp(1, 256);
    let mut queue = bridge.rpc_queue.lock();
    let mut requests = Vec::new();
    for _ in 0..max_requests {
        let Some(request) = queue.pop_front() else {
            break;
        };
        requests.push(request);
    }
    if !requests.is_empty() {
        info!(
            "MT5 EA polled {} RPC request(s); queued_remaining={}",
            requests.len(),
            queue.len()
        );
    }

    let session_id = bridge.session_id();
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
    if let Err(message) = authorize(&bridge, &headers, &envelope.session_id) {
        return bridge_error(
            &bridge,
            envelope.request_id,
            StatusCode::UNAUTHORIZED,
            message,
        );
    }
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

fn authorize(bridge: &Mt5Bridge, headers: &HeaderMap, session_id: &str) -> Result<(), String> {
    let expected_session_id = bridge.session_id();
    let header_session = header_value(headers, "x-aqe-mt5-session");
    let header_token = header_value(headers, "x-aqe-mt5-token");
    if session_id != expected_session_id || header_session.as_deref() != Some(session_id) {
        return Err("invalid MT5 bridge session".to_string());
    }
    if header_token.as_deref() != Some(bridge.config.token.as_str()) {
        return Err("invalid MT5 bridge token".to_string());
    }
    Ok(())
}

fn authorize_token(bridge: &Mt5Bridge, headers: &HeaderMap) -> Result<(), String> {
    let header_token = header_value(headers, "x-aqe-mt5-token");
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
