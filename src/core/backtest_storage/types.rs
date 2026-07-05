use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BacktestTradeLogRow {
    pub id: i32,
    pub symbol: String,
    pub side: String,
    #[serde(alias = "strategy_type")]
    pub strategy_type: Option<String>,
    #[serde(default, alias = "parent_id")]
    pub parent_id: Option<String>,
    #[serde(default, alias = "is_child")]
    pub is_child: bool,
    #[serde(default, alias = "base_strategy_type")]
    pub base_strategy_type: Option<String>,
    #[serde(alias = "insight_id")]
    pub insight_id: Option<String>,
    #[serde(alias = "entry_time")]
    pub entry_time: String,
    #[serde(alias = "exit_time")]
    pub exit_time: Option<String>,
    pub qty: f64,
    #[serde(alias = "entry_price")]
    pub entry_price: f64,
    #[serde(alias = "exit_price")]
    pub exit_price: Option<f64>,
    #[serde(alias = "return_pct")]
    pub return_pct: Option<f64>,
    pub pnl: Option<f64>,
    #[serde(default)]
    pub commission: Option<f64>,
    #[serde(default)]
    pub swap: Option<f64>,
    pub status: String,
}
