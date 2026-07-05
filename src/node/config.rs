use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AssetSideFees {
    #[serde(default)]
    pub long: AssetFee,
    #[serde(default)]
    pub short: AssetFee,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AssetCommissionFees {
    #[serde(default)]
    pub entry: AssetSideFees,
    #[serde(default)]
    pub exit: AssetSideFees,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AssetSwapFees {
    #[serde(default)]
    pub long: AssetFee,
    #[serde(default)]
    pub short: AssetFee,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AssetFees {
    #[serde(default)]
    pub commission: AssetCommissionFees,
    #[serde(default)]
    pub swap: AssetSwapFees,
    // Legacy fields kept for existing editor/backtest metadata.
    #[serde(default)]
    pub entry: AssetFee,
    #[serde(default)]
    pub exit: AssetFee,
}

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
    #[serde(default = "default_broker_leverage")]
    pub broker_leverage: u8,
    #[serde(default)]
    pub broker_fees: AssetFees,
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
            broker_leverage: default_broker_leverage(),
            broker_fees: AssetFees::default(),
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
fn default_broker_leverage() -> u8 {
    1
}
fn default_log_level() -> String {
    "info".to_string()
}
