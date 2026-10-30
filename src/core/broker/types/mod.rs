use chrono::{DateTime, Utc};
use std::fmt;
use uuid::Uuid;

mod mt5_bridge;
pub(crate) use mt5_bridge::{MT5_PROTOCOL_VERSION, Mt5BridgeV2Inbound};
pub use mt5_bridge::{
    Mt5BridgeEnvelope, Mt5BridgeResponse, Mt5HeartbeatPayload, Mt5MarketDataPayload, Mt5RpcAction,
    Mt5RpcPollPayload, Mt5RpcPollResponse, Mt5RpcRequest, Mt5RpcResponsePayload,
    Mt5SnapshotPayload, Mt5StreamBar, Mt5TradeEventPayload, Mt5TradeEventsPayload,
};

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
    #[serde(default)]
    pub accrued_commission: f64,
    pub shorting_enabled: bool,
    pub leverage: u8,
    /// Broker's pending-order ceiling. `None` means no reported limit.
    #[serde(default)]
    pub max_total_orders: Option<u32>,
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
    /// Aggregate symbol volume ceiling (not the per-order maximum), in AQE quantity units.
    /// MT5 reports this per direction; strategy sizing conservatively combines both sides.
    #[serde(default)]
    pub max_total_size: Option<f64>,
    pub min_price_increment: Option<f64>,
    pub price_base: Option<i64>,
    pub contract_size: Option<i64>,
    /// Producer-recorded trading calendar JSON; absent for unknown schedules.
    /// Generic instrument metadata, independent of AQS and strategy graphs.
    #[serde(default)]
    pub trading_calendar: Option<String>,
    #[serde(default)]
    pub fees: AssetFees,
}

impl AsRef<str> for Asset {
    fn as_ref(&self) -> &str {
        &self.symbol
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AssetFee {
    None,
    Points(f64),
    PercentageFee(f64),
    FixedFee(f64),
    PerContractFee(f64),
    PercentagePerContractFee(f64),
}

impl Default for AssetFee {
    fn default() -> Self {
        Self::None
    }
}

impl AssetFee {
    pub fn calculate(&self, price: f64, quantity: f64) -> f64 {
        self.calculate_with_contract(price, quantity, None)
    }

    pub fn calculate_with_contract(
        &self,
        price: f64,
        quantity: f64,
        contract_size: Option<i64>,
    ) -> f64 {
        if !price.is_finite() || !quantity.is_finite() {
            return 0.0;
        }

        let quantity = quantity.abs();
        let contract_size = contract_size.unwrap_or(1).max(1) as f64;
        match self {
            Self::None => 0.0,
            Self::Points(_) => 0.0,
            Self::PercentageFee(rate) if rate.is_finite() && *rate > 0.0 && *rate < 1.0 => {
                price.abs() * quantity * rate
            }
            Self::FixedFee(value) if value.is_finite() && *value > 0.0 => *value,
            Self::PerContractFee(value) if value.is_finite() && *value > 0.0 => value * quantity,
            Self::PercentagePerContractFee(rate)
                if rate.is_finite() && *rate > 0.0 && *rate < 1.0 =>
            {
                price.abs() * contract_size * quantity * rate
            }
            _ => 0.0,
        }
    }

    pub fn calculate_swap(
        &self,
        price: f64,
        quantity: f64,
        contract_size: Option<i64>,
        point: Option<f64>,
        days: f64,
    ) -> f64 {
        if !price.is_finite() || !quantity.is_finite() || !days.is_finite() || days <= 0.0 {
            return 0.0;
        }

        let quantity = quantity.abs();
        let contract_size = contract_size.unwrap_or(1).max(1) as f64;
        match self {
            Self::None => 0.0,
            Self::Points(points) if points.is_finite() => {
                let point = point.filter(|value| value.is_finite() && *value > 0.0);
                points * point.unwrap_or(0.0) * contract_size * quantity * days
            }
            Self::PercentageFee(rate) if rate.is_finite() => price.abs() * quantity * rate * days,
            Self::FixedFee(value) if value.is_finite() => value * days,
            Self::PerContractFee(value) if value.is_finite() => value * quantity * days,
            Self::PercentagePerContractFee(rate) if rate.is_finite() => {
                price.abs() * contract_size * quantity * rate * days
            }
            _ => 0.0,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssetSideFees {
    #[serde(default)]
    pub long: AssetFee,
    #[serde(default)]
    pub short: AssetFee,
}

impl AssetSideFees {
    pub fn for_side(&self, side: &OrderSide) -> &AssetFee {
        match side {
            OrderSide::Buy => &self.long,
            OrderSide::Sell => &self.short,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssetCommissionFees {
    #[serde(default)]
    pub entry: AssetSideFees,
    #[serde(default)]
    pub exit: AssetSideFees,
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssetSwapFees {
    #[serde(default)]
    pub long: AssetFee,
    #[serde(default)]
    pub short: AssetFee,
}

impl AssetSwapFees {
    pub fn for_side(&self, side: &OrderSide) -> &AssetFee {
        match side {
            OrderSide::Buy => &self.long,
            OrderSide::Sell => &self.short,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssetFees {
    #[serde(default)]
    pub commission: AssetCommissionFees,
    #[serde(default)]
    pub swap: AssetSwapFees,
    // Legacy backtest config fields. Kept for compatibility with existing strategy metadata and UI.
    #[serde(default)]
    pub entry: AssetFee,
    #[serde(default)]
    pub exit: AssetFee,
}

impl AssetFees {
    pub fn entry_commission(&self, price: f64, quantity: f64, contract_size: Option<i64>) -> f64 {
        self.entry
            .calculate_with_contract(price, quantity, contract_size)
    }

    pub fn exit_commission(&self, price: f64, quantity: f64, contract_size: Option<i64>) -> f64 {
        self.exit
            .calculate_with_contract(price, quantity, contract_size)
    }

    pub fn entry_commission_for_side(
        &self,
        side: &OrderSide,
        price: f64,
        quantity: f64,
        contract_size: Option<i64>,
    ) -> f64 {
        let side_fee = self.commission.entry.for_side(side);
        if matches!(side_fee, AssetFee::None)
            && self.commission.entry.long == AssetFee::None
            && self.commission.entry.short == AssetFee::None
        {
            self.entry_commission(price, quantity, contract_size)
        } else {
            side_fee.calculate_with_contract(price, quantity, contract_size)
        }
    }

    pub fn exit_commission_for_side(
        &self,
        side: &OrderSide,
        price: f64,
        quantity: f64,
        contract_size: Option<i64>,
    ) -> f64 {
        let side_fee = self.commission.exit.for_side(side);
        if matches!(side_fee, AssetFee::None)
            && self.commission.exit.long == AssetFee::None
            && self.commission.exit.short == AssetFee::None
        {
            self.exit_commission(price, quantity, contract_size)
        } else {
            side_fee.calculate_with_contract(price, quantity, contract_size)
        }
    }

    pub fn swap_for_side(
        &self,
        side: &OrderSide,
        price: f64,
        quantity: f64,
        contract_size: Option<i64>,
        point: Option<f64>,
        days: f64,
    ) -> f64 {
        self.swap
            .for_side(side)
            .calculate_swap(price, quantity, contract_size, point, days)
    }
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
    /// Exact execution/position ticket associated with the entry Insight.
    /// None for legacy payloads or symbol aggregates with multiple owners.
    #[serde(default)]
    pub order_id: Option<String>,
    pub asset: Asset,
    pub avg_entry_price: f64,
    pub qty: f64,
    pub side: OrderSide,
    pub market_value: f64,
    pub cost_basis: f64,
    pub current_price: f64,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,
    #[serde(default)]
    pub entry_commission: f64,
    pub margin_required: Option<f64>,
}

/// Transport-free position snapshot lookup. Implementations must distinguish
/// unavailable data from an observed absence and must never submit orders.
pub type PositionSnapshotReader = std::sync::Arc<dyn Fn(&str, &str) -> Result<Option<Position>, BrokerError> + Send + Sync>;

/// Broker-neutral lifecycle state used when reconciling persisted orders after a restart.
///
/// `Filled` means the broker confirms a fill but cannot prove whether its position remains open.
/// `PositionOpen` is stronger: the broker resolved the order to a currently open position.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum OrderLifecycleStatus {
    Pending,
    Filled,
    PositionOpen,
    Closed,
    Cancelled,
    Rejected,
    Expired,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderLifecycleSnapshot {
    pub order_id: String,
    pub status: OrderLifecycleStatus,
    #[serde(default)]
    pub position_id: Option<String>,
    #[serde(default)]
    pub position_qty: Option<f64>,
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default)]
    pub detail: Option<String>,
    /// Broker-confirmed final execution values; absent on legacy/unsupported adapters.
    #[serde(default)]
    pub close_details: Option<OrderLifecycleCloseDetails>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderLifecycleCloseDetails {
    pub close_price: f64,
    pub closed_at: DateTime<Utc>,
    pub realized_pnl: f64,
    pub commission: f64,
    pub swap: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position_accounting: Option<aqe_live_protocol::PositionCloseAccounting>,
}

impl OrderLifecycleCloseDetails {
    /// Optional analytics evidence must agree with its enclosing broker history.
    /// Invalid/foreign metadata cannot replace otherwise valid execution values.
    pub(crate) fn validated_position_accounting(
        &self,
        order_id: &str,
    ) -> Option<aqe_live_protocol::PositionCloseAccounting> {
        self.position_accounting.as_ref().filter(|value| {
            value.validate().is_ok()
                && value.owns_order(order_id)
                && value.closed_at_us == self.closed_at.timestamp_micros()
                && value.net_pnl == self.realized_pnl
                && value.commission == self.commission
                && value.swap == self.swap
        }).cloned()
    }
}

/// Latest broker state already resident in the adapter. Reading this snapshot must never
/// perform network or broker I/O, which makes it safe to use from the live strategy loop.
/// `snapshot_complete` is deliberately explicit: an empty snapshot is authoritative only when
/// the broker confirms that all active orders and positions have been enumerated.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerLifecycleSnapshot {
    #[serde(default)]
    pub orders: Vec<Order>,
    #[serde(default)]
    pub positions: Vec<Position>,
    #[serde(default)]
    pub terminal_history: Vec<OrderLifecycleSnapshot>,
    pub snapshot_complete: bool,
    pub captured_at: DateTime<Utc>,
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
    /// Cumulative executed quantity for an entry order snapshot. For an exit
    /// trade event, the quantity executed by that exit (never the entry total).
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position_accounting: Option<aqe_live_protocol::PositionCloseAccounting>,
    #[serde(default)]
    pub commission: Option<f64>,
    #[serde(default)]
    pub swap: Option<f64>,
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
    Cancelled,
    Rejected,
    Expired,
    Closed,
    Replaced,
}

/// Clock domain used by a broker event's source timestamp.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrokerClockDomain {
    BrokerServer,
    AdapterHost,
    RuntimeHost,
    Unknown,
}

/// Broker-neutral timing and optional exact execution evidence captured before
/// an event enters AQE. Legacy/cumulative order snapshots carry no fill fact.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerEventMetadata {
    /// Corrections must never enter the order/Insight lifecycle reducer.
    #[serde(default)]
    pub analytics_only: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_fill: Option<Box<aqe_live_protocol::ExecutionFill>>,
    #[serde(default)]
    pub source_occurred_at_us: Option<i64>,
    #[serde(default)]
    pub adapter_queued_at_us: Option<i64>,
    #[serde(default)]
    pub source_clock: BrokerClockDomain,
}

impl Default for BrokerClockDomain {
    fn default() -> Self {
        Self::Unknown
    }
}

/// Idempotent broker event envelope used by native streaming brokers.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerEventEnvelope<T> {
    pub event_id: Uuid,
    #[serde(default)]
    pub source_sequence: Option<u64>,
    pub event: T,
    #[serde(default)]
    pub metadata: BrokerEventMetadata,
}

impl<T> BrokerEventEnvelope<T> {
    pub fn new(event: T) -> Self {
        Self {
            event_id: Uuid::new_v4(),
            source_sequence: None,
            event,
            metadata: BrokerEventMetadata::default(),
        }
    }

    pub fn with_source_sequence(mut self, sequence: u64) -> Self {
        self.source_sequence = Some(sequence);
        self
    }

    pub fn with_metadata(mut self, metadata: BrokerEventMetadata) -> Self {
        self.metadata = metadata;
        self
    }
}

/// Native order lifecycle event shape shared by every broker implementation.
pub type BrokerOrderEvent = BrokerEventEnvelope<(Order, TradeUpdateEvent)>;

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
    #[serde(default)]
    pub commission: f64,
    #[serde(default)]
    pub swap: f64,
    /// Cash posted by this execution only, not the position's lifetime result.
    /// Absent in older artifacts; consumers must not manufacture it from P&L.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_cash: Option<aqe_live_protocol::ExecutionFillCash>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_notional: Option<f64>,
    /// Declared execution valuation, e.g. the paper broker's limited price-unit model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valuation_basis: Option<String>,
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
    /// A recoverable read dependency, never evidence that an insight is invalid.
    SnapshotUnavailable {
        kind: super::snapshots::SnapshotKind,
        reason: super::snapshots::SnapshotUnavailable,
    },
    /// Definitively unsupported before any transport mutation is attempted.
    UnsupportedOperation(String),
    /// The adapter proved that submission was refused before the broker mutation
    /// call. Unlike transport errors this is terminal evidence, not ambiguity.
    SubmissionRejected(String),
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
            BrokerError::SnapshotUnavailable { kind, reason } => write!(f, "{kind:?}: {reason}"),
            BrokerError::UnsupportedOperation(msg) => {
                write!(f, "Unsupported broker operation: {}", msg)
            }
            BrokerError::SubmissionRejected(msg) => write!(f, "Submission rejected: {}", msg),
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
    /// Optional native account-currency valuation model for supported contracts.
    /// Absent on old adapters, unsupported instruments and synthetic bar quotes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profit_model: Option<aqe_live_protocol::BrokerQuoteProfit>,
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    pub bid_size: f64,
    pub ask_size: f64,
    pub last: Option<f64>,
    pub last_size: Option<f64>,
    pub timestamp: DateTime<Utc>,
}

impl Quote {
    pub(crate) fn set_mapped_symbol(&mut self, symbol: String) {
        if let Some(model) = self.profit_model.as_mut() {
            if model.symbol.eq_ignore_ascii_case(&self.symbol) {
                model.symbol = symbol.clone();
            } else {
                self.profit_model = None;
            }
        }
        self.symbol = symbol;
    }
    pub(crate) fn valid_market_values(&self) -> bool {
        self.bid.is_finite()
            && self.ask.is_finite()
            && self.bid > 0.0
            && self.ask >= self.bid
            && self.bid_size.is_finite()
            && self.ask_size.is_finite()
            && self.bid_size >= 0.0
            && self.ask_size >= 0.0
            && self
                .last
                .is_none_or(|value| value.is_finite() && value > 0.0)
            && self
                .last_size
                .is_none_or(|value| value.is_finite() && value >= 0.0)
    }

    /// Zero-spread approximation for bar-only feeds/replay. No invented order-book sizes
    /// or last-trade size: cumulative candle volume is not the size of a trade.
    pub fn from_bar_close(bar: &Bar) -> Self {
        Self {
            profit_model: None,
            symbol: bar.symbol.clone(),
            bid: bar.close,
            ask: bar.close,
            bid_size: 0.0,
            ask_size: 0.0,
            last: Some(bar.close),
            last_size: None,
            timestamp: bar.timestamp,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::utils::tools::dynamic_round_for_asset;

    fn fee_rounding_asset() -> Asset {
        Asset {
            id: "rounding-asset".to_string(),
            symbol: "TEST".to_string(),
            name: "TEST".to_string(),
            asset_type: AssetType::Stock,
            status: AssetStatus::Active,
            exchange: AssetExchange::NASDAQ,
            tradable: true,
            marginable: true,
            shortable: true,
            fractional: true,
            min_order_size: None,
            quantity_base: None,
            max_order_size: None,
            max_total_size: None,
            min_price_increment: Some(0.01),
            price_base: None,
            contract_size: Some(10),
            trading_calendar: None,
            fees: AssetFees::default(),
        }
    }

    fn assert_dynamic_round_eq(left: f64, right: f64) {
        let asset = fee_rounding_asset();
        assert_eq!(
            dynamic_round_for_asset(left, &asset),
            dynamic_round_for_asset(right, &asset)
        );
    }

    #[test]
    fn percentage_per_contract_fee_uses_contract_notional() {
        let fee = AssetFee::PercentagePerContractFee(0.0002);

        let calculated = fee.calculate_with_contract(100.0, 2.0, Some(10));
        assert!((calculated - 0.4).abs() < f64::EPSILON);
    }

    #[test]
    fn side_specific_none_does_not_fallback_when_other_side_is_set() {
        let fees = AssetFees {
            commission: AssetCommissionFees {
                entry: AssetSideFees {
                    long: AssetFee::PerContractFee(2.0),
                    short: AssetFee::None,
                },
                exit: AssetSideFees::default(),
            },
            entry: AssetFee::FixedFee(10.0),
            ..AssetFees::default()
        };

        assert_eq!(
            fees.entry_commission_for_side(&OrderSide::Buy, 100.0, 3.0, Some(1)),
            6.0
        );
        assert_eq!(
            fees.entry_commission_for_side(&OrderSide::Sell, 100.0, 3.0, Some(1)),
            0.0
        );
    }

    #[test]
    fn swap_percentage_per_contract_fee_uses_daily_decimal_rate() {
        let fee = AssetFee::PercentagePerContractFee(0.0001);

        let calculated = fee.calculate_swap(100.0, 2.0, Some(10), Some(0.01), 3.0);
        assert!((calculated - 0.6).abs() < f64::EPSILON);
    }

    #[test]
    fn asset_fee_variants_calculate_expected_values() {
        assert_dynamic_round_eq(
            AssetFee::PercentageFee(0.01).calculate_with_contract(100.0, 2.0, Some(10)),
            2.0,
        );
        assert_dynamic_round_eq(
            AssetFee::FixedFee(5.0).calculate_with_contract(100.0, 2.0, Some(10)),
            5.0,
        );
        assert_dynamic_round_eq(
            AssetFee::PerContractFee(3.0).calculate_with_contract(100.0, 2.0, Some(10)),
            6.0,
        );
        assert_dynamic_round_eq(
            AssetFee::Points(2.0).calculate_with_contract(100.0, 2.0, Some(10)),
            0.0,
        );

        assert_dynamic_round_eq(
            AssetFee::Points(1.0).calculate_swap(100.0, 2.0, Some(10), Some(0.01), 3.0),
            0.6,
        );
        assert_dynamic_round_eq(
            AssetFee::PercentageFee(0.01).calculate_swap(100.0, 2.0, Some(10), Some(0.01), 3.0),
            6.0,
        );
        assert_dynamic_round_eq(
            AssetFee::FixedFee(5.0).calculate_swap(100.0, 2.0, Some(10), Some(0.01), 3.0),
            15.0,
        );
        assert_dynamic_round_eq(
            AssetFee::PerContractFee(3.0).calculate_swap(100.0, 2.0, Some(10), Some(0.01), 3.0),
            18.0,
        );
    }
}
