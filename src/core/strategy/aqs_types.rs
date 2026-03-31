use chrono::{DateTime, Utc};
use serde::Serialize;
use surrealdb::types::SurrealValue;

use super::live_metrics::LiveMetricsSnapshot;
use crate::core::broker::types::{Asset, AssetExchange, AssetStatus, AssetType};

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

    pub fn url(&self) -> &str {
        self.url.as_deref().unwrap_or(Self::DEFAULT_URL)
    }

    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
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
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyEquityPointRecord {
    pub equity: f64,
    pub cash: f64,
    pub buying_power: f64,
}

#[derive(Debug, Clone)]
pub struct LatestPersistedAccountState {
    pub equity: f64,
    pub cash: f64,
    pub buying_power: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyEventRecord {
    pub event_type: String,
    pub level: String,
    pub title: String,
    pub message: String,
    pub payload: Option<serde_json::Value>,
    pub created_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
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
    pub sharpe_ratio: f64,
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
}

impl From<LiveMetricsSnapshot> for StrategyLiveMetricsRecord {
    fn from(value: LiveMetricsSnapshot) -> Self {
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
            sharpe_ratio: value.sharpe_ratio,
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
