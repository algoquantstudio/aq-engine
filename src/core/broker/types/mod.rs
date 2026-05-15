use chrono::{DateTime, Utc};
use std::fmt;

// ─────────────────────── Broker Feature Flags ───────────────────────

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SupportedBrokerFeatures {
    pub bar_data_streaming: bool,
    pub trade_event_streaming: bool,
    pub featured_bar_data_streaming: bool,
    pub submit_order: bool,
    pub max_order_value: Option<f64>,
    pub cancel_order: bool,
    pub close_position: bool,
    pub get_account: bool,
    pub get_position: bool,
    pub get_positions: bool,
    pub get_history: bool,
    pub get_quote: bool,
    pub get_ticker_info: bool,
    pub leverage: bool,
    pub shorting: bool,
    pub margin: bool,
    pub bracket_orders: bool,
    pub trailing_stop: bool,
}

// ─────────────────────── Account ───────────────────────

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Account {
    pub account_id: String,
    pub account_type: AccountType,
    pub equity: f64,
    pub cash: f64,
    pub currency: String,
    pub buying_power: f64,
    pub shorting_enabled: bool,
    pub leverage: u8,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum AccountType {
    Paper,
    Live,
}

// ─────────────────────── Asset ───────────────────────

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Asset {
    pub id: String,
    pub symbol: String,
    pub name: String,
    pub asset_type: AssetType,
    pub status: AssetStatus,
    pub exchange: AssetExchange,
    pub tradable: bool,
    pub marginable: bool,
    pub shortable: bool,
    pub fractional: bool,
    pub min_order_size: Option<f64>,
    pub quantity_base: Option<i64>,
    pub max_order_size: Option<f64>,
    pub min_price_increment: Option<f64>,
    pub price_base: Option<i64>,
    pub contract_size: Option<i64>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum AssetType {
    Stock,
    Crypto,
    Forex,
    Commodity,
    Index,
    ETF,
    MutualFund,
    UNKNOWN(String),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum AssetStatus {
    Active,
    Inactive,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum AssetExchange {
    NYSE,
    NASDAQ,
    AMEX,
    CME,
    CBOE,
    ICE,
    LSE,
    SSE,
    BSE,
    TSE,
    UNKNOWN(String),
}

// ─────────────────────── Position ───────────────────────

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Position {
    pub account_id: String,
    pub asset: Asset,
    pub avg_entry_price: f64,
    pub qty: f64,
    pub side: OrderSide,
    pub market_value: f64,
    pub cost_basis: f64,
    pub current_price: f64,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,
    pub margin_required: Option<f64>,
}

// ─────────────────────── Order ───────────────────────

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct OrderRequest {
    pub asset: Asset,
    pub qty: f64,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub limit_price: Option<f64>,
    pub stop_price: Option<f64>,
    pub order_class: OrderClass,
    pub take_profit: Option<f64>,
    pub stop_loss: Option<f64>,
    pub trail_price: Option<f64>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Order {
    pub order_id: String,
    pub insight_id: Option<String>,
    pub strategy_type: Option<String>,
    pub asset: Asset,
    pub qty: f64,
    pub filled_qty: f64,
    pub limit_price: Option<f64>,
    pub filled_price: Option<f64>,
    pub stop_price: Option<f64>,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub status: TradeUpdateEvent,
    pub order_class: OrderClass,
    pub created_at: u64,
    pub updated_at: u64,
    pub submitted_at: u64,
    pub filled_at: Option<u64>,
    #[serde(default)]
    pub realized_pnl: Option<f64>,
    pub rejection_reason: Option<String>,
    pub legs: Option<OrderLegs>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct OrderLegs {
    pub take_profit: Option<OrderLeg>,
    pub stop_loss: Option<OrderLeg>,
    pub trailing_stop: Option<OrderLeg>,
}

impl Default for OrderLegs {
    fn default() -> Self {
        Self {
            take_profit: None,
            stop_loss: None,
            trailing_stop: None,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct OrderLeg {
    pub order_id: Option<String>,
    pub limit_price: Option<f64>,
    pub trail_price: Option<f64>,
    pub side: OrderSide,
    pub filled_price: Option<f64>,
    pub order_type: OrderType,
    pub status: TradeUpdateEvent,
    pub order_class: OrderClass,
    pub created_at: u64,
    pub updated_at: u64,
    pub submitted_at: u64,
    pub filled_at: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum OrderType {
    Market,
    Limit,
    Stop,
    StopLimit,
    TrailingStop,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum TimeInForce {
    Day,
    GTC,
    OPG,
    IOC,
    FOK,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum OrderClass {
    /// Simple Order - with no legs
    Simple,
    /// Bracket Order - (otooco) One Triggers One Cancels Other (TP + SL)
    Bracket,
    /// One Cancels Other (TP and SL) - on active order
    OCO,
    /// One Triggers Other (TP or SL) - on active order
    OTO,
    /// Trailing Stop Order
    TRO,
}

// ─────────────────────── Trade Events ───────────────────────

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub enum TradeUpdateEvent {
    Accepted,
    New,
    PendingNew,
    Pending,
    PartialFilled,
    Filled,
    Canceled,
    Rejected,
    Expired,
    Closed,
    Replaced,
}

// ─────────────────────── Trade Records (for logging) ───────────────────────

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TradeRecord {
    pub date: DateTime<Utc>,
    pub symbol: String,
    pub side: OrderSide,
    pub qty: f64,
    pub price: f64,
    pub order_id: String,
    #[serde(default)]
    pub insight_id: Option<String>,
    #[serde(default)]
    pub strategy_type: Option<String>,
    pub trade_type: TradeRecordType,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum TradeRecordType {
    Entry,
    Exit,
}

// ─────────────────────── Errors ───────────────────────

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum BrokerError {
    ConnectionError(String),
    DisconnectionError(String),
    OrderError(String),
    PositionError(String),
    AccountError(String),
    AssetError(String),
    TradeError(String),
    DataFeedError(String),
    DataFeedConnectionError(String),
    DataFeedDisconnectionError(String),
    InvalidTicker(String),
    OrderCancellationError(String),
}

impl fmt::Display for BrokerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BrokerError::ConnectionError(msg) => write!(f, "Connection error: {}", msg),
            BrokerError::DisconnectionError(msg) => write!(f, "Disconnection error: {}", msg),
            BrokerError::OrderError(msg) => write!(f, "Order error: {}", msg),
            BrokerError::PositionError(msg) => write!(f, "Position error: {}", msg),
            BrokerError::AccountError(msg) => write!(f, "Account error: {}", msg),
            BrokerError::AssetError(msg) => write!(f, "Asset error: {}", msg),
            BrokerError::TradeError(msg) => write!(f, "Trade error: {}", msg),
            BrokerError::DataFeedError(msg) => write!(f, "Data feed error: {}", msg),
            BrokerError::DataFeedConnectionError(msg) => {
                write!(f, "Data feed connection error: {}", msg)
            }
            BrokerError::DataFeedDisconnectionError(msg) => {
                write!(f, "Data feed disconnection error: {}", msg)
            }
            BrokerError::InvalidTicker(msg) => write!(f, "Invalid ticker: {}", msg),
            BrokerError::OrderCancellationError(msg) => {
                write!(f, "Order cancellation error: {}", msg)
            }
        }
    }
}

impl std::error::Error for BrokerError {}

impl From<reqwest::Error> for BrokerError {
    fn from(err: reqwest::Error) -> Self {
        BrokerError::ConnectionError(err.to_string())
    }
}

impl From<serde_json::Error> for BrokerError {
    fn from(err: serde_json::Error) -> Self {
        BrokerError::DataFeedError(err.to_string())
    }
}

// ─────────────────────── Market Data ───────────────────────

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Bar {
    pub symbol: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub timestamp: DateTime<Utc>,
}

/// Required columns for a valid bar DataFrame.
pub const BAR_DATAFRAME_COLUMNS: &[&str] = &[
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "timestamp",
];

/// Generic bar data input — either raw `Vec<Bar>` or a pre-formatted `DataFrame`.
/// Data feeds that already produce DataFrames (e.g. from Polars sources) pass `Frame`,
/// while feeds that parse JSON/binary (e.g. Yahoo) pass `Bars` for default conversion.
#[derive(Clone, Debug)]
pub enum BarData {
    /// Raw bar structs that need conversion to DataFrame.
    Bars(Vec<Bar>),
    /// A DataFrame that may already have the correct bar schema.
    Frame(polars::prelude::DataFrame),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Quote {
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    pub bid_size: f64,
    pub ask_size: f64,
    pub last: Option<f64>,
    pub last_size: Option<f64>,
    pub timestamp: DateTime<Utc>,
}
