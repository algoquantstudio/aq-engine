use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use surrealdb::types::SurrealValue;

use super::live_metrics::{LiveMetricsCheckpoint, LiveMetricsSnapshot};
use crate::core::broker::types::{
    Asset, AssetExchange, AssetFee, AssetFees, AssetStatus, AssetType,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SurrealValue)]
#[serde(rename_all = "snake_case")]
#[surreal(rename_all = "snake_case")]
#[surreal(untagged)]
pub enum StrategyActionKind {
    Stop,
}

impl StrategyActionKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stop => "stop",
        }
    }
}

impl std::fmt::Display for StrategyActionKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SurrealValue)]
#[serde(rename_all = "snake_case")]
#[surreal(rename_all = "snake_case")]
#[surreal(untagged)]
pub enum StrategyActionStatus {
    Pending,
    Acknowledged,
    Processing,
    Completed,
    Failed,
    Cancelled,
}

impl StrategyActionStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Acknowledged => "acknowledged",
            Self::Processing => "processing",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl std::fmt::Display for StrategyActionStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SurrealValue)]
#[serde(rename_all = "snake_case")]
#[surreal(rename_all = "snake_case")]
#[surreal(untagged)]
pub enum StrategyEventType {
    Lifecycle,
    AccountSnapshot,
    InsightReconcile,
    InsightStartupCleanup,
    InsightState,
    TradeEvent,
    Action,
}

impl StrategyEventType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Lifecycle => "lifecycle",
            Self::AccountSnapshot => "account_snapshot",
            Self::InsightReconcile => "insight_reconcile",
            Self::InsightStartupCleanup => "insight_startup_cleanup",
            Self::InsightState => "insight_state",
            Self::TradeEvent => "trade_event",
            Self::Action => "action",
        }
    }
}

impl std::fmt::Display for StrategyEventType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SurrealValue)]
#[serde(rename_all = "snake_case")]
#[surreal(rename_all = "snake_case")]
#[surreal(untagged)]
pub enum StrategyEventLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl StrategyEventLevel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

impl std::fmt::Display for StrategyEventLevel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SurrealValue)]
#[surreal(untagged)]
pub enum StrategyEventTitle {
    #[serde(rename = "AQS live sync connected")]
    #[surreal(rename = "AQS live sync connected")]
    AqsLiveSyncConnected,
    #[serde(rename = "Account snapshot recorded")]
    #[surreal(rename = "Account snapshot recorded")]
    AccountSnapshotRecorded,
    #[serde(rename = "Live metrics reset")]
    #[surreal(rename = "Live metrics reset")]
    LiveMetricsReset,
    #[serde(rename = "Stale insight cancelled")]
    #[surreal(rename = "Stale insight cancelled")]
    StaleInsightCancelled,
    #[serde(rename = "Stale remote insight cleaned up")]
    #[surreal(rename = "Stale remote insight cleaned up")]
    StaleRemoteInsightCleanedUp,
    #[serde(rename = "Strategy started")]
    #[surreal(rename = "Strategy started")]
    StrategyStarted,
    #[serde(rename = "Broker trade event")]
    #[surreal(rename = "Broker trade event")]
    BrokerTradeEvent,
    #[serde(rename = "Stop requested")]
    #[surreal(rename = "Stop requested")]
    StopRequested,
    #[serde(rename = "Strategy stopped")]
    #[surreal(rename = "Strategy stopped")]
    StrategyStopped,
    #[serde(rename = "Insight New")]
    #[surreal(rename = "Insight New")]
    InsightNew,
    #[serde(rename = "Insight Executed")]
    #[surreal(rename = "Insight Executed")]
    InsightExecuted,
    #[serde(rename = "Insight Filled")]
    #[surreal(rename = "Insight Filled")]
    InsightFilled,
    #[serde(rename = "Insight Closed")]
    #[surreal(rename = "Insight Closed")]
    InsightClosed,
    #[serde(rename = "Insight Cancelled")]
    #[surreal(rename = "Insight Cancelled")]
    InsightCancelled,
    #[serde(rename = "Insight Rejected")]
    #[surreal(rename = "Insight Rejected")]
    InsightRejected,
}

impl StrategyEventTitle {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AqsLiveSyncConnected => "AQS live sync connected",
            Self::AccountSnapshotRecorded => "Account snapshot recorded",
            Self::LiveMetricsReset => "Live metrics reset",
            Self::StaleInsightCancelled => "Stale insight cancelled",
            Self::StaleRemoteInsightCleanedUp => "Stale remote insight cleaned up",
            Self::StrategyStarted => "Strategy started",
            Self::BrokerTradeEvent => "Broker trade event",
            Self::StopRequested => "Stop requested",
            Self::StrategyStopped => "Strategy stopped",
            Self::InsightNew => "Insight New",
            Self::InsightExecuted => "Insight Executed",
            Self::InsightFilled => "Insight Filled",
            Self::InsightClosed => "Insight Closed",
            Self::InsightCancelled => "Insight Cancelled",
            Self::InsightRejected => "Insight Rejected",
        }
    }

    pub fn for_insight_state(state: &str) -> Option<Self> {
        match state {
            "New" => Some(Self::InsightNew),
            "Executed" => Some(Self::InsightExecuted),
            "Filled" => Some(Self::InsightFilled),
            "Closed" => Some(Self::InsightClosed),
            "Cancelled" => Some(Self::InsightCancelled),
            "Rejected" => Some(Self::InsightRejected),
            _ => None,
        }
    }
}

impl std::fmt::Display for StrategyEventTitle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug)]
pub struct AqsAuth {
    pub access_method: String,
    pub session_id: String,
    pub session_secret: String,
    pub strategy_id: String,
    pub user_id: String,
    pub node_id: Option<String>,
    pub live_session_id: Option<String>,
    pub url: Option<String>,
}

impl AqsAuth {
    pub const DEFAULT_URL: &'static str =
        "wss://certain-squirre-06echdjfsdvq70hkk9g8bb2s0k.aws-euw1.surreal.cloud";

    pub fn from_args<I, S>(args: I) -> Option<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let args = args.into_iter().map(Into::into).collect::<Vec<_>>();
        let arg_value = |name: &str| {
            args.iter()
                .position(|arg| arg == name)
                .and_then(|index| args.get(index + 1))
                .cloned()
        };

        let session_secret = arg_value("--session-secret").unwrap_or_default();
        let strategy_id = arg_value("--strategy-id").unwrap_or_default();
        let session_id = arg_value("--session-id").unwrap_or_default();

        if session_secret.is_empty() || strategy_id.is_empty() || session_id.is_empty() {
            return None;
        }

        Some(Self {
            access_method: arg_value("--access-method").unwrap_or_else(|| "aqe_live".to_string()),
            session_id,
            session_secret,
            strategy_id,
            user_id: arg_value("--user-id").unwrap_or_default(),
            node_id: arg_value("--node-id"),
            live_session_id: arg_value("--live-session-id"),
            url: arg_value("--aqs-url"),
        })
    }

    pub fn from_process_args() -> Option<Self> {
        #[cfg(target_arch = "wasm32")]
        {
            None
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            Self::from_args(std::env::args())
        }
    }

    pub fn url(&self) -> &str {
        self.url.as_deref().unwrap_or(Self::DEFAULT_URL)
    }

    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }
}

pub fn shutdown_signal_file_from_args<I, S>(args: I) -> Option<PathBuf>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let args = args.into_iter().map(Into::into).collect::<Vec<_>>();
    args.iter()
        .position(|argument| argument == "--shutdown-signal-file")
        .and_then(|index| args.get(index + 1))
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
}

pub fn shutdown_signal_file_from_process_args() -> Option<PathBuf> {
    #[cfg(target_arch = "wasm32")]
    {
        None
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        shutdown_signal_file_from_args(std::env::args())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyAccountSnapshotRecord {
    pub account_id: String,
    pub account_type: String,
    pub equity: f64,
    pub cash: f64,
    pub currency: String,
    pub buying_power: f64,
    pub accrued_commission: f64,
    pub shorting_enabled: bool,
    pub leverage: i64,
}

#[derive(Debug, Clone, Serialize, SurrealValue)]
pub struct StrategyUniverseAssetRecord {
    pub id: String,
    pub symbol: String,
    pub name: String,
    pub asset_type: String,
    pub status: String,
    pub exchange: String,
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
    pub fees: StrategyUniverseAssetFeesRecord,
}

#[derive(Debug, Clone, Serialize, SurrealValue)]
pub struct StrategyUniverseAssetFeesRecord {
    pub commission: StrategyUniverseAssetCommissionFeesRecord,
    pub swap: StrategyUniverseAssetSwapFeesRecord,
    pub entry: StrategyUniverseAssetFeeRecord,
    pub exit: StrategyUniverseAssetFeeRecord,
}

#[derive(Debug, Clone, Serialize, SurrealValue)]
pub struct StrategyUniverseAssetCommissionFeesRecord {
    pub entry: StrategyUniverseAssetSideFeesRecord,
    pub exit: StrategyUniverseAssetSideFeesRecord,
}

#[derive(Debug, Clone, Serialize, SurrealValue)]
pub struct StrategyUniverseAssetSideFeesRecord {
    pub long: StrategyUniverseAssetFeeRecord,
    pub short: StrategyUniverseAssetFeeRecord,
}

#[derive(Debug, Clone, Serialize, SurrealValue)]
pub struct StrategyUniverseAssetSwapFeesRecord {
    pub long: StrategyUniverseAssetFeeRecord,
    pub short: StrategyUniverseAssetFeeRecord,
}

#[derive(Debug, Clone, Serialize, SurrealValue)]
pub struct StrategyUniverseAssetFeeRecord {
    pub kind: String,
    pub value: Option<f64>,
}

impl From<&Asset> for StrategyUniverseAssetRecord {
    fn from(value: &Asset) -> Self {
        Self {
            id: value.id.clone(),
            symbol: value.symbol.clone(),
            name: value.name.clone(),
            asset_type: asset_type_to_string(&value.asset_type),
            status: asset_status_to_string(&value.status),
            exchange: asset_exchange_to_string(&value.exchange),
            tradable: value.tradable,
            marginable: value.marginable,
            shortable: value.shortable,
            fractional: value.fractional,
            min_order_size: value.min_order_size,
            quantity_base: value.quantity_base,
            max_order_size: value.max_order_size,
            min_price_increment: value.min_price_increment,
            price_base: value.price_base,
            contract_size: value.contract_size,
            fees: StrategyUniverseAssetFeesRecord::from(&value.fees),
        }
    }
}

impl From<&AssetFees> for StrategyUniverseAssetFeesRecord {
    fn from(value: &AssetFees) -> Self {
        Self {
            commission: StrategyUniverseAssetCommissionFeesRecord {
                entry: StrategyUniverseAssetSideFeesRecord {
                    long: StrategyUniverseAssetFeeRecord::from(&value.commission.entry.long),
                    short: StrategyUniverseAssetFeeRecord::from(&value.commission.entry.short),
                },
                exit: StrategyUniverseAssetSideFeesRecord {
                    long: StrategyUniverseAssetFeeRecord::from(&value.commission.exit.long),
                    short: StrategyUniverseAssetFeeRecord::from(&value.commission.exit.short),
                },
            },
            swap: StrategyUniverseAssetSwapFeesRecord {
                long: StrategyUniverseAssetFeeRecord::from(&value.swap.long),
                short: StrategyUniverseAssetFeeRecord::from(&value.swap.short),
            },
            entry: StrategyUniverseAssetFeeRecord::from(&value.entry),
            exit: StrategyUniverseAssetFeeRecord::from(&value.exit),
        }
    }
}

impl From<&AssetFee> for StrategyUniverseAssetFeeRecord {
    fn from(value: &AssetFee) -> Self {
        match value {
            AssetFee::None => Self {
                kind: "none".to_string(),
                value: None,
            },
            AssetFee::Points(points) => Self {
                kind: "points".to_string(),
                value: Some(*points),
            },
            AssetFee::PercentageFee(rate) => Self {
                kind: "percentageFee".to_string(),
                value: Some(*rate),
            },
            AssetFee::FixedFee(amount) => Self {
                kind: "fixedFee".to_string(),
                value: Some(*amount),
            },
            AssetFee::PerContractFee(amount) => Self {
                kind: "perContractFee".to_string(),
                value: Some(*amount),
            },
            AssetFee::PercentagePerContractFee(rate) => Self {
                kind: "percentagePerContractFee".to_string(),
                value: Some(*rate),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyEquityPointRecord {
    pub account_id: String,
    pub currency: String,
    pub equity: f64,
    pub cash: f64,
    pub buying_power: f64,
    pub accrued_commission: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyEventRecord {
    pub event_type: StrategyEventType,
    pub level: StrategyEventLevel,
    pub title: StrategyEventTitle,
    pub message: String,
    pub payload: Option<serde_json::Value>,
    pub created_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
pub struct StrategyLiveMetricsRecord {
    pub starting_cash: f64,
    pub final_equity: f64,
    pub total_return: f64,
    pub total_return_pct: f64,
    pub total_trades: usize,
    pub winning_trades: usize,
    pub losing_trades: usize,
    pub win_rate: f64,
    pub max_drawdown: f64,
    pub cagr: f64,
    pub annualized_volatility: f64,
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub calmar_ratio: f64,
    pub max_drawdown_duration_days: f64,
    pub expectancy: f64,
    pub profit_factor: f64,
    pub payoff_ratio: f64,
    pub avg_winner: f64,
    pub avg_loser: f64,
    pub avg_winner_pct: f64,
    pub avg_loser_pct: f64,
    pub best_trade: f64,
    pub worst_trade: f64,
    pub consistency_score: f64,
    pub longest_winning_trade_held_secs: i64,
    pub longest_losing_trade_held_secs: i64,
    pub average_trade_held_secs: i64,
    pub open_positions_count: usize,
    pub open_insights_count: usize,
    pub open_positions_unrealized_pnl: f64,
    pub open_positions_profitable_count: usize,
    pub open_positions_losing_count: usize,
    pub symbols: Vec<String>,
    pub executed_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub tracker_checkpoint: Option<LiveMetricsCheckpoint>,
}

#[cfg(test)]
mod enum_tests {
    use super::*;

    #[test]
    fn parses_complete_live_auth_args() {
        let auth = AqsAuth::from_args([
            "strategy-bin",
            "--live",
            "--session-secret",
            "secret",
            "--access-method",
            "aqe_live",
            "--strategy-id",
            "strategy_123",
            "--user-id",
            "users:isaac",
            "--session-id",
            "session-key",
            "--node-id",
            "local",
            "--live-session-id",
            "live_strategy_session:abc",
        ])
        .expect("complete args should parse");

        assert_eq!(auth.session_secret, "secret");
        assert_eq!(auth.strategy_id, "strategy_123");
        assert_eq!(auth.user_id, "users:isaac");
        assert_eq!(auth.session_id, "session-key");
        assert_eq!(auth.node_id.as_deref(), Some("local"));
        assert_eq!(
            auth.live_session_id.as_deref(),
            Some("live_strategy_session:abc")
        );
    }

    #[test]
    fn missing_required_live_auth_args_returns_none() {
        assert!(AqsAuth::from_args(["strategy-bin", "--live"]).is_none());
        assert!(
            AqsAuth::from_args([
                "strategy-bin",
                "--session-secret",
                "secret",
                "--strategy-id",
                "strategy_123",
            ])
            .is_none()
        );
    }

    #[test]
    fn parses_local_shutdown_signal_file() {
        assert_eq!(
            shutdown_signal_file_from_args([
                "strategy-bin",
                "--live",
                "--shutdown-signal-file",
                "/tmp/strategy.stop",
            ]),
            Some(PathBuf::from("/tmp/strategy.stop"))
        );
        assert!(shutdown_signal_file_from_args(["strategy-bin", "--live"]).is_none());
    }
}

impl From<LiveMetricsSnapshot> for StrategyLiveMetricsRecord {
    fn from(value: LiveMetricsSnapshot) -> Self {
        Self::from_snapshot(value, None)
    }
}

impl StrategyLiveMetricsRecord {
    pub fn from_snapshot(
        value: LiveMetricsSnapshot,
        tracker_checkpoint: Option<LiveMetricsCheckpoint>,
    ) -> Self {
        Self {
            starting_cash: value.starting_cash,
            final_equity: value.final_equity,
            total_return: value.total_return,
            total_return_pct: value.total_return_pct,
            total_trades: value.total_trades,
            winning_trades: value.winning_trades,
            losing_trades: value.losing_trades,
            win_rate: value.win_rate,
            max_drawdown: value.max_drawdown,
            cagr: value.cagr,
            annualized_volatility: value.annualized_volatility,
            sharpe_ratio: value.sharpe_ratio,
            sortino_ratio: value.sortino_ratio,
            calmar_ratio: value.calmar_ratio,
            max_drawdown_duration_days: value.max_drawdown_duration_days,
            expectancy: value.expectancy,
            profit_factor: value.profit_factor,
            payoff_ratio: value.payoff_ratio,
            avg_winner: value.avg_winner,
            avg_loser: value.avg_loser,
            avg_winner_pct: value.avg_winner_pct,
            avg_loser_pct: value.avg_loser_pct,
            best_trade: value.best_trade,
            worst_trade: value.worst_trade,
            consistency_score: value.consistency_score,
            longest_winning_trade_held_secs: value.longest_winning_trade_held_secs,
            longest_losing_trade_held_secs: value.longest_losing_trade_held_secs,
            average_trade_held_secs: value.average_trade_held_secs,
            open_positions_count: value.open_positions_count,
            open_insights_count: value.open_insights_count,
            open_positions_unrealized_pnl: value.open_positions_unrealized_pnl,
            open_positions_profitable_count: value.open_positions_profitable_count,
            open_positions_losing_count: value.open_positions_losing_count,
            symbols: value.symbols,
            executed_at: value.executed_at,
            finished_at: value.finished_at,
            updated_at: value.updated_at,
            tracker_checkpoint,
        }
    }

    pub fn snapshot(&self) -> LiveMetricsSnapshot {
        LiveMetricsSnapshot {
            starting_cash: self.starting_cash,
            final_equity: self.final_equity,
            total_return: self.total_return,
            total_return_pct: self.total_return_pct,
            total_trades: self.total_trades,
            winning_trades: self.winning_trades,
            losing_trades: self.losing_trades,
            win_rate: self.win_rate,
            max_drawdown: self.max_drawdown,
            cagr: self.cagr,
            annualized_volatility: self.annualized_volatility,
            sharpe_ratio: self.sharpe_ratio,
            sortino_ratio: self.sortino_ratio,
            calmar_ratio: self.calmar_ratio,
            max_drawdown_duration_days: self.max_drawdown_duration_days,
            expectancy: self.expectancy,
            profit_factor: self.profit_factor,
            payoff_ratio: self.payoff_ratio,
            avg_winner: self.avg_winner,
            avg_loser: self.avg_loser,
            avg_winner_pct: self.avg_winner_pct,
            avg_loser_pct: self.avg_loser_pct,
            best_trade: self.best_trade,
            worst_trade: self.worst_trade,
            consistency_score: self.consistency_score,
            longest_winning_trade_held_secs: self.longest_winning_trade_held_secs,
            longest_losing_trade_held_secs: self.longest_losing_trade_held_secs,
            average_trade_held_secs: self.average_trade_held_secs,
            open_positions_count: self.open_positions_count,
            open_insights_count: self.open_insights_count,
            open_positions_unrealized_pnl: self.open_positions_unrealized_pnl,
            open_positions_profitable_count: self.open_positions_profitable_count,
            open_positions_losing_count: self.open_positions_losing_count,
            symbols: self.symbols.clone(),
            executed_at: self.executed_at,
            finished_at: self.finished_at,
            updated_at: self.updated_at,
        }
    }
}

pub fn live_session_key(value: &str) -> String {
    value
        .split_once(':')
        .map(|(_, key)| key.to_string())
        .unwrap_or_else(|| value.to_string())
}

fn asset_type_to_string(value: &AssetType) -> String {
    match value {
        AssetType::Stock => "Stock".to_string(),
        AssetType::Crypto => "Crypto".to_string(),
        AssetType::Forex => "Forex".to_string(),
        AssetType::Commodity => "Commodity".to_string(),
        AssetType::Index => "Index".to_string(),
        AssetType::ETF => "ETF".to_string(),
        AssetType::MutualFund => "MutualFund".to_string(),
        AssetType::UNKNOWN(other) => other.clone(),
    }
}

fn asset_status_to_string(value: &AssetStatus) -> String {
    match value {
        AssetStatus::Active => "Active".to_string(),
        AssetStatus::Inactive => "Inactive".to_string(),
    }
}

fn asset_exchange_to_string(value: &AssetExchange) -> String {
    match value {
        AssetExchange::NYSE => "NYSE".to_string(),
        AssetExchange::NASDAQ => "NASDAQ".to_string(),
        AssetExchange::AMEX => "AMEX".to_string(),
        AssetExchange::CME => "CME".to_string(),
        AssetExchange::CBOE => "CBOE".to_string(),
        AssetExchange::ICE => "ICE".to_string(),
        AssetExchange::LSE => "LSE".to_string(),
        AssetExchange::SSE => "SSE".to_string(),
        AssetExchange::BSE => "BSE".to_string(),
        AssetExchange::TSE => "TSE".to_string(),
        AssetExchange::UNKNOWN(other) => other.clone(),
    }
}

pub fn action_id_from_value(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(id) => Some(id.clone()),
        serde_json::Value::Object(map) => {
            let table = map.get("table").and_then(|value| value.as_str())?;
            let key_value = map.get("key")?;
            let key = match key_value {
                serde_json::Value::String(value) => value.clone(),
                serde_json::Value::Object(key_map) => key_map
                    .get("String")
                    .and_then(|value| value.as_str())
                    .map(str::to_string)
                    .or_else(|| {
                        key_map
                            .get("Uuid")
                            .and_then(|value| value.as_str())
                            .map(str::to_string)
                    })
                    .or_else(|| {
                        key_map
                            .get("Number")
                            .and_then(|value| value.as_i64())
                            .map(|value| value.to_string())
                    })?,
                other => other.as_str().map(str::to_string)?,
            };
            Some(format!("{}:{}", table, key))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strategy_action_enums_use_database_values() {
        assert_eq!(
            serde_json::to_value(StrategyActionKind::Stop).unwrap(),
            serde_json::json!("stop")
        );
        assert_eq!(
            serde_json::from_value::<StrategyActionStatus>(serde_json::json!("acknowledged"))
                .unwrap(),
            StrategyActionStatus::Acknowledged
        );

        assert_eq!(
            StrategyActionKind::from_value(surrealdb::types::Value::String("stop".into())).unwrap(),
            StrategyActionKind::Stop
        );
        assert_eq!(
            StrategyActionStatus::from_value(surrealdb::types::Value::String(
                "acknowledged".into(),
            ))
            .unwrap(),
            StrategyActionStatus::Acknowledged
        );
    }

    #[test]
    fn strategy_event_titles_cover_every_insight_state() {
        for (state, expected) in [
            ("New", StrategyEventTitle::InsightNew),
            ("Executed", StrategyEventTitle::InsightExecuted),
            ("Filled", StrategyEventTitle::InsightFilled),
            ("Closed", StrategyEventTitle::InsightClosed),
            ("Cancelled", StrategyEventTitle::InsightCancelled),
            ("Rejected", StrategyEventTitle::InsightRejected),
        ] {
            let title = StrategyEventTitle::for_insight_state(state).unwrap();
            assert_eq!(title, expected);
            assert_eq!(
                serde_json::to_value(title).unwrap(),
                serde_json::json!(format!("Insight {state}"))
            );
        }
    }

    #[test]
    fn strategy_event_title_decodes_persisted_database_value() {
        let title = StrategyEventTitle::from_value(surrealdb::types::Value::String(
            "Broker trade event".into(),
        ))
        .unwrap();

        assert_eq!(title, StrategyEventTitle::BrokerTradeEvent);
        assert_eq!(
            title.into_value(),
            surrealdb::types::Value::String("Broker trade event".into())
        );
        assert_eq!(
            StrategyEventTitle::from_value(surrealdb::types::Value::String(
                "Live metrics reset".into()
            ))
            .unwrap(),
            StrategyEventTitle::LiveMetricsReset
        );
    }

    #[test]
    fn strategy_event_metadata_decodes_persisted_database_values() {
        let event_type =
            StrategyEventType::from_value(surrealdb::types::Value::String("trade_event".into()))
                .unwrap();
        let level =
            StrategyEventLevel::from_value(surrealdb::types::Value::String("info".into())).unwrap();

        assert_eq!(event_type, StrategyEventType::TradeEvent);
        assert_eq!(level, StrategyEventLevel::Info);
        assert_eq!(
            event_type.into_value(),
            surrealdb::types::Value::String("trade_event".into())
        );
        assert_eq!(
            level.into_value(),
            surrealdb::types::Value::String("info".into())
        );
    }

    #[test]
    fn equity_history_payload_keeps_account_identity_and_currency() {
        let payload = serde_json::to_value(StrategyEquityPointRecord {
            account_id: " broker-42 ".trim().to_string(),
            currency: "gbp".to_uppercase(),
            equity: 101_000.0,
            cash: 100_000.0,
            buying_power: 200_000.0,
            accrued_commission: 12.5,
        })
        .unwrap();
        assert_eq!(payload["account_id"], "broker-42");
        assert_eq!(payload["currency"], "GBP");
    }
}
