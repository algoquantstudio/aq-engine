use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TimeFrameUnit {
    Second,
    Minute,
    Hour,
    Day,
    Month,
}

impl std::fmt::Display for TimeFrameUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Second => write!(f, "Second"),
            Self::Minute => write!(f, "Minute"),
            Self::Hour => write!(f, "Hour"),
            Self::Day => write!(f, "Day"),
            Self::Month => write!(f, "Month"),
        }
    }
}

impl TimeFrameUnit {
    pub fn variants() -> &'static [TimeFrameUnit] {
        &[
            TimeFrameUnit::Second,
            TimeFrameUnit::Minute,
            TimeFrameUnit::Hour,
            TimeFrameUnit::Day,
            TimeFrameUnit::Month,
        ]
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum InsightState {
    New,
    Executed,
    Filled,
    Closed,
    Cancelled,
    Rejected,
}

/// Supported data feed providers from the AlgoQuant Engine.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DataFeedType {
    YahooFinance,
    Mt5,
}

impl Default for DataFeedType {
    fn default() -> Self {
        Self::YahooFinance
    }
}

impl std::fmt::Display for DataFeedType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::YahooFinance => write!(f, "Yahoo Finance"),
            Self::Mt5 => write!(f, "MT5"),
        }
    }
}

impl DataFeedType {
    pub fn variants() -> &'static [DataFeedType] {
        &[DataFeedType::YahooFinance, DataFeedType::Mt5]
    }
}

/// Supported execution broker types from the AlgoQuant Engine.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionBrokerType {
    Paper,
    Mt5,
}

impl Default for ExecutionBrokerType {
    fn default() -> Self {
        Self::Paper
    }
}

impl std::fmt::Display for ExecutionBrokerType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Paper => write!(f, "Paper Broker"),
            Self::Mt5 => write!(f, "MT5 Broker"),
        }
    }
}

impl ExecutionBrokerType {
    pub fn variants() -> &'static [ExecutionBrokerType] {
        &[ExecutionBrokerType::Paper, ExecutionBrokerType::Mt5]
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Mt5BridgeConfig {
    pub bind_addr: String,
    pub token_env: String,
    pub request_timeout_ms: u64,
    pub poll_interval_ms: u64,
    pub symbol_map: Option<String>,
}

impl Default for Mt5BridgeConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:18080".to_string(),
            token_env: "AQE_MT5_BRIDGE_TOKEN".to_string(),
            request_timeout_ms: 5_000,
            poll_interval_ms: 250,
            symbol_map: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Copy)]
#[serde(rename_all = "camelCase")]
pub enum DataFeedEnvironment {
    Live,
    Paper,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Copy)]
#[serde(rename_all = "camelCase")]
pub enum DataFeedServiceType {
    Data,
    Execution,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Copy, Default)]
#[serde(rename_all = "camelCase")]
pub enum DataFeedStatus {
    #[default]
    Disconnected,
    Connected,
    Configuring,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DataFeedConfig {
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub environment: Option<DataFeedEnvironment>,
    pub symbols: Option<Vec<String>>,
    #[serde(default)]
    pub mt5_account_id: Option<String>,
    #[serde(default)]
    pub mt5_server: Option<String>,
    #[serde(default)]
    pub mt5_password: Option<String>,
    #[serde(default)]
    pub mt5_bridge_token: Option<String>,
    #[serde(default)]
    pub mt5_bind_addr: Option<String>,
    #[serde(default)]
    pub mt5_test_symbol: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DataFeed {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub display_name: Option<String>,
    pub service_type: DataFeedServiceType,
    pub status: DataFeedStatus,
    pub config: DataFeedConfig,
    /// Built-in feeds (Yahoo Finance, Paper Broker) cannot be deleted.
    #[serde(default)]
    pub is_default: bool,
}
