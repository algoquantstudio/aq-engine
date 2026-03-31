use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StrategyBacktestConfig {
    #[serde(default = "default_timeframe_amount")]
    pub timeframe_amount: u8,
    #[serde(default = "default_timeframe_unit")]
    pub timeframe_unit: String,
    #[serde(default = "default_execution_risk")]
    pub execution_risk: f64,
    #[serde(default = "default_min_reward_risk_ratio")]
    pub min_reward_risk_ratio: f64,
    #[serde(default = "default_base_confidence")]
    pub base_confidence: f64,
    #[serde(default = "default_starting_cash")]
    pub starting_cash: f64,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_time: Option<String>,
}

impl Default for StrategyBacktestConfig {
    fn default() -> Self {
        Self {
            timeframe_amount: default_timeframe_amount(),
            timeframe_unit: default_timeframe_unit(),
            execution_risk: default_execution_risk(),
            min_reward_risk_ratio: default_min_reward_risk_ratio(),
            base_confidence: default_base_confidence(),
            starting_cash: default_starting_cash(),
            log_level: default_log_level(),
            start_time: None,
            end_time: None,
        }
    }
}

fn default_timeframe_amount() -> u8 {
    1
}
fn default_timeframe_unit() -> String {
    "Minute".to_string()
}
fn default_execution_risk() -> f64 {
    0.02
}
fn default_min_reward_risk_ratio() -> f64 {
    2.0
}
fn default_base_confidence() -> f64 {
    0.1
}
fn default_starting_cash() -> f64 {
    100_000.0
}
fn default_log_level() -> String {
    "info".to_string()
}
