use super::{Account, Asset, Bar, Order, Position, Quote, TradeUpdateEvent};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub(crate) const MT5_PROTOCOL_VERSION: u16 = 1;

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
    GetExecutionFillHistory,
    GetPositionValuations,
    GetLatestQuote,
    GetLatestBar,
    GetOrders,
    ReconcileOrders,
    GetSubmission,
    EstimateMargin,
    GetPositions,
    GetAccount,
    SubmitOrder,
    CancelOrder,
    UpdateOrder,
    AmendEntry,
    ClosePosition,
    CloseAllPositions,
    SubscribeBars,
    UnsubscribeBars,
}

impl Mt5RpcAction {
    pub(crate) fn can_retry_after_transport_failure(self) -> bool {
        matches!(
            self,
            Self::GetTickerInfo
                | Self::GetHistory
                | Self::GetExecutionFillHistory
                | Self::GetPositionValuations
                | Self::GetLatestQuote
                | Self::GetLatestBar
                | Self::GetOrders
                | Self::ReconcileOrders
                | Self::GetSubmission
                | Self::EstimateMargin
                | Self::GetPositions
                | Self::GetAccount
                | Self::SubscribeBars
                | Self::UnsubscribeBars
        )
    }
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
    /// An EA must not send analytics-only order envelopes to older reducers.
    #[serde(default)]
    pub execution_fill_revision_version: u16,
    pub session_id: String,
    pub request_id: String,
    pub message: Option<String>,
    pub payload: Option<T>,
}

impl<T> Mt5BridgeResponse<T> {
    pub(crate) fn ok(session_id: &str, request_id: String, payload: Option<T>) -> Self {
        Self {
            ok: true,
            protocol_version: MT5_PROTOCOL_VERSION,
            execution_fill_revision_version: 1,
            session_id: session_id.to_string(),
            request_id,
            message: None,
            payload,
        }
    }

    pub(crate) fn error(session_id: &str, request_id: String, message: String) -> Self {
        Self {
            ok: false,
            protocol_version: MT5_PROTOCOL_VERSION,
            execution_fill_revision_version: 1,
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

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mt5MarketDataPayload {
    #[serde(default)]
    pub quotes: Vec<Quote>,
    #[serde(default)]
    pub bars: Vec<Mt5StreamBar>,
    /// Current broker candle (OHLC + cumulative tick volume). Optional for older EAs.
    #[serde(default)]
    pub intrabar_bars: Vec<Mt5StreamBar>,
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
    /// A historical deal correction is evidence, not another order transition.
    #[serde(default)]
    pub analytics_only: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_fill: Option<Box<aqe_live_protocol::ExecutionFill>>,
    pub native_event_id: Option<String>,
    pub event_seq: Option<u64>,
    #[serde(default)]
    pub source_occurred_at_us: Option<i64>,
    #[serde(default)]
    pub adapter_queued_at_us: Option<i64>,
    pub event: TradeUpdateEvent,
    pub order: Order,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mt5TradeEventsPayload {
    #[serde(default)]
    pub events: Vec<Mt5TradeEventPayload>,
    #[serde(default)]
    pub dropped_count: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "PascalCase",
    rename_all_fields = "camelCase"
)]
pub(crate) enum Mt5BridgeV2Inbound {
    Authenticate {
        nonce: String,
        token: String,
        session_id: String,
        #[serde(default)]
        last_acknowledged_sequence: Option<u64>,
    },
    Heartbeat {
        payload: Mt5HeartbeatPayload,
    },
    TradeEventBatch {
        #[serde(default)]
        sequence: Option<u64>,
        #[serde(default)]
        events: Vec<Mt5TradeEventPayload>,
        #[serde(default)]
        dropped_count: Option<u64>,
    },
    ExecutionFillRevisionBatch {
        #[serde(default)]
        sequence: Option<u64>,
        #[serde(default)]
        events: Vec<Mt5TradeEventPayload>,
        #[serde(default)]
        dropped_count: Option<u64>,
    },
    MarketDataBatch {
        #[serde(default)]
        sequence: Option<u64>,
        payload: Mt5MarketDataPayload,
    },
    AccountSnapshot {
        payload: Mt5SnapshotPayload,
    },
    BrokerCommandResult {
        payload: Mt5RpcResponsePayload,
    },
    BatchAcknowledgement {
        #[serde(default)]
        broker_event_sequence: Option<u64>,
        #[serde(default)]
        correlation_id: Option<String>,
    },
    Resume {
        #[serde(default)]
        last_acknowledged_sequence: Option<u64>,
    },
    GracefulShutdown,
}
