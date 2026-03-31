use crate::node::config::StrategyBacktestConfig;
use crate::node::types::{DataFeedType, ExecutionBrokerType, InsightState};
use serde::{Deserialize, Serialize};

#[cfg(feature = "runtime")]
pub use crate::core::backtest_storage::BacktestTradeLogRow;

#[cfg(not(feature = "runtime"))]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BacktestTradeLogRow {
    pub id: i32,
    pub symbol: String,
    pub side: String,
    #[serde(alias = "strategy_type")]
    pub strategy_type: Option<String>,
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
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeInput {
    pub name: String,
    pub input_type: InputType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
    #[serde(default = "default_true")]
    pub is_public: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub insight_state: Option<InsightState>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum InputType {
    Str,
    Int,
    Float,
    Bool,
    Array,
    Insights,
    Trigger,
    OnBar,
    AlphaResult,
    InsightPipeResult,
    Universe,
    AlphaInstance,
    InsightPipeInstance,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeOutput {
    pub name: String,
    pub output_type: OutputType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub insight_state: Option<InsightState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OutputType {
    Insights,
    ExecutionResults,
    BarData,
    QuoteData,
    EventData,
    // Strategy lifecycle outputs
    OnStart,
    Init,
    Universe,
    OnBar,
    GenerateInsights,
    InsightPipeline,
    OnTeardown,
    // Component results
    AlphaResult,
    InsightPipeResult,
    // Component instances
    AlphaInstance,
    InsightPipeInstance,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum NodeType {
    Alpha,
    Pipe,
    Trigger,
    Strategy,
    Universe,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Node {
    pub id: String,
    #[serde(rename = "type")]
    pub node_type: NodeType,
    pub label: String,
    pub x: f64,
    pub y: f64,
    pub inputs: Vec<NodeInput>,
    pub outputs: Vec<NodeOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_file: Option<String>,
    #[serde(default)]
    pub undeletable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionEndpoint {
    pub node_id: String,
    #[serde(alias = "output", alias = "input")]
    pub port: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Connection {
    pub from: ConnectionEndpoint,
    pub to: ConnectionEndpoint,
}

/// Represents the top-level saved Strategy Meta structure. (.aqmeta)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StrategyMeta {
    pub id: String,
    #[serde(default)]
    pub strategy_cloud_id: Option<String>,
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub data_feed: DataFeedType,
    #[serde(default)]
    pub broker: ExecutionBrokerType,
    pub nodes: Vec<Node>,
    pub connections: Vec<Connection>,
    pub created_at: String,
    pub updated_at: String,
    // Strategy configuration
    #[serde(default)]
    pub config: StrategyBacktestConfig,
}

impl StrategyMeta {
    pub fn new(name: &str) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            strategy_cloud_id: None,
            name: name.to_string(),
            version: "1.0.0".to_string(),
            data_feed: DataFeedType::default(),
            broker: ExecutionBrokerType::default(),
            nodes: Vec::new(),
            connections: Vec::new(),
            created_at: chrono::Utc::now().to_rfc3339(),
            updated_at: chrono::Utc::now().to_rfc3339(),
            config: StrategyBacktestConfig::default(),
        }
    }

    /// Create the default Strategy node that every project starts with.
    pub fn create_strategy_node(name: &str) -> Node {
        Node {
            id: "strategy_root".to_string(),
            node_type: NodeType::Strategy,
            label: name.to_string(),
            x: 200.0,
            y: 200.0,
            inputs: vec![],
            outputs: vec![
                NodeOutput {
                    name: "on_start".to_string(),
                    output_type: OutputType::OnStart,
                    insight_state: None,
                },
                NodeOutput {
                    name: "init".to_string(),
                    output_type: OutputType::Init,
                    insight_state: None,
                },
                NodeOutput {
                    name: "universe".to_string(),
                    output_type: OutputType::Universe,
                    insight_state: None,
                },
                NodeOutput {
                    name: "on_bar".to_string(),
                    output_type: OutputType::OnBar,
                    insight_state: None,
                },
                NodeOutput {
                    name: "generate_insights".to_string(),
                    output_type: OutputType::GenerateInsights,
                    insight_state: None,
                },
                NodeOutput {
                    name: "insight_pipeline".to_string(),
                    output_type: OutputType::InsightPipeline,
                    insight_state: None,
                },
                NodeOutput {
                    name: "on_teardown".to_string(),
                    output_type: OutputType::OnTeardown,
                    insight_state: None,
                },
            ],
            source_file: None,
            undeletable: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FileNode {
    pub name: String,
    pub is_dir: bool,
    pub path: String,
    pub children: Option<Vec<FileNode>>,
}
