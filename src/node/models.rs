use crate::node::config::StrategyBacktestConfig;
use crate::node::types::{DataFeedType, ExecutionBrokerType, InsightState};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};

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
    /// Optional key in `StrategyMeta::hyper_parameters` used at runtime instead of `value`.
    /// The normal value remains in the document as the safe fallback for ordinary/live runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hyper_reference: Option<String>,
}

/// Scalar types supported by an exhaustive hyperparameter sweep.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum HyperParameterType {
    #[serde(alias = "int", alias = "integer")]
    Int,
    #[serde(alias = "float", alias = "number")]
    Float,
    Bool,
    String,
    #[serde(alias = "enum")]
    Enum,
}

impl HyperParameterType {
    fn accepts(self, value: &Value) -> bool {
        match self {
            Self::Int => value.as_i64().is_some(),
            Self::Float => value.as_f64().is_some(),
            Self::Bool => value.is_boolean(),
            Self::String | Self::Enum => value.is_string(),
        }
    }

    fn matches_input(self, input: &InputType) -> bool {
        matches!(
            (self, input),
            (Self::Int, InputType::Int)
                | (Self::Float, InputType::Float)
                | (Self::Bool, InputType::Bool)
                | (Self::String | Self::Enum, InputType::Str)
        )
    }
}

/// A named, scalar domain. Use `values` for discrete values, or `min`/`max`/`increment`
/// for an inclusive numeric range.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HyperParameterDefinition {
    pub name: String,
    #[serde(rename = "type")]
    pub value_type: HyperParameterType,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub increment: Option<f64>,
}

impl HyperParameterDefinition {
    pub fn expanded_values(&self) -> Result<Vec<Value>, String> {
        if !self.values.is_empty() {
            if self
                .values
                .iter()
                .all(|value| self.value_type.accepts(value))
            {
                return Ok(self.values.clone());
            }
            return Err(format!(
                "hyperparameter '{}' has a value that does not match its type",
                self.name
            ));
        }
        let (Some(min), Some(max), Some(increment)) = (self.min, self.max, self.increment) else {
            return Err(format!(
                "hyperparameter '{}' needs values or min, max, and increment",
                self.name
            ));
        };
        if !matches!(
            self.value_type,
            HyperParameterType::Int | HyperParameterType::Float
        ) {
            return Err(format!(
                "hyperparameter '{}' ranges are only supported for int and float values",
                self.name
            ));
        }
        if !min.is_finite()
            || !max.is_finite()
            || !increment.is_finite()
            || increment <= 0.0
            || max < min
        {
            return Err(format!(
                "hyperparameter '{}' has an invalid numeric range",
                self.name
            ));
        }
        let mut output = Vec::new();
        let epsilon = increment.abs() * 1e-9;
        let mut value = min;
        while value <= max + epsilon {
            let json_value = if self.value_type == HyperParameterType::Int {
                if value.fract().abs() > epsilon {
                    return Err(format!(
                        "hyperparameter '{}' int range does not produce integers",
                        self.name
                    ));
                }
                Value::from(value.round() as i64)
            } else {
                serde_json::Number::from_f64(value)
                    .map(Value::Number)
                    .ok_or_else(|| {
                        format!(
                            "hyperparameter '{}' has a non-finite range value",
                            self.name
                        )
                    })?
            };
            output.push(json_value);
            value += increment;
            if output.len() > 1_000_000 {
                return Err(format!(
                    "hyperparameter '{}' expands to more than 1,000,000 values",
                    self.name
                ));
            }
        }
        Ok(output)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HyperParameterRun {
    pub seed: String,
    pub values: BTreeMap<String, Value>,
}

/// Stable identifier for one concrete parameter map, shared by AQS and AQE.
pub fn canonical_hyperparameter_seed(values: &BTreeMap<String, Value>) -> Result<String, String> {
    let canonical = serde_json::to_vec(values).map_err(|error| error.to_string())?;
    Ok(format!("{:x}", Sha256::digest(canonical)))
}

/// Expand parameter domains in a canonical order. The seed is a SHA-256 fingerprint of the
/// canonical JSON map, so it remains stable across machines and UI ordering.
pub fn canonical_hyperparameter_runs(
    definitions: &[HyperParameterDefinition],
) -> Result<Vec<HyperParameterRun>, String> {
    let mut definitions = definitions.to_vec();
    definitions.sort_by(|a, b| a.name.cmp(&b.name));
    let mut names = HashSet::new();
    for definition in &definitions {
        if definition.name.trim().is_empty() || !names.insert(definition.name.clone()) {
            return Err(format!(
                "hyperparameter names must be unique and non-empty (got '{}')",
                definition.name
            ));
        }
    }
    let domains: Result<Vec<_>, _> = definitions
        .iter()
        .map(HyperParameterDefinition::expanded_values)
        .collect();
    let domains = domains?;
    let mut combinations = vec![BTreeMap::new()];
    for (definition, domain) in definitions.iter().zip(domains) {
        let mut next = Vec::with_capacity(combinations.len().saturating_mul(domain.len()));
        for current in combinations {
            for value in &domain {
                let mut map = current.clone();
                map.insert(definition.name.clone(), value.clone());
                next.push(map);
            }
        }
        combinations = next;
    }
    combinations
        .into_iter()
        .map(|values| {
            let seed = canonical_hyperparameter_seed(&values)?;
            Ok(HyperParameterRun { seed, values })
        })
        .collect()
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
    OnStart,
    Init,
    OnTeardown,
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
    // Kept for compatibility with generic node-port constructors. Outputs never consume
    // this value; scalar references are validated only on `NodeInput`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hyper_reference: Option<String>,
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
    LogicBlock,
    Trigger,
    Strategy,
    Universe,
    UniverseModel,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum LifecyclePhase {
    OnStart,
    OnInit,
    OnTeardown,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum LifecycleTiming {
    BeforeGenerated,
    AfterGenerated,
}

impl Default for LifecycleTiming {
    fn default() -> Self {
        Self::BeforeGenerated
    }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle_phase: Option<LifecyclePhase>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle_timing: Option<LifecycleTiming>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub can_fail: Option<bool>,
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
    pub data_feed_id: Option<String>,
    #[serde(default)]
    pub broker: ExecutionBrokerType,
    #[serde(default)]
    pub broker_id: Option<String>,
    pub nodes: Vec<Node>,
    pub connections: Vec<Connection>,
    pub created_at: String,
    pub updated_at: String,
    // Strategy configuration
    #[serde(default)]
    pub config: StrategyBacktestConfig,
    /// Named domains available to generated backtests and custom strategy code as
    /// `ctx.variables()["__HYPER__"]`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hyper_parameters: Vec<HyperParameterDefinition>,
    /// The seed used by ordinary backtests and live deployment when a default is selected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hyper_default_seed: Option<String>,
    /// Exact values for the selected seed. This avoids expanding a whole sweep simply to
    /// launch one normal backtest or a live deployment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hyper_default_values: Option<BTreeMap<String, Value>>,
    /// Explicit acknowledgement required by AQS before launching a very large local sweep.
    #[serde(default, skip_serializing_if = "is_false")]
    pub hyper_sweep_confirmed: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl StrategyMeta {
    pub fn new(name: &str) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            strategy_cloud_id: None,
            name: name.to_string(),
            version: "1.0.0".to_string(),
            data_feed: DataFeedType::default(),
            data_feed_id: None,
            broker: ExecutionBrokerType::default(),
            broker_id: None,
            nodes: Vec::new(),
            connections: Vec::new(),
            created_at: chrono::Utc::now().to_rfc3339(),
            updated_at: chrono::Utc::now().to_rfc3339(),
            config: StrategyBacktestConfig::default(),
            hyper_parameters: Vec::new(),
            hyper_default_seed: None,
            hyper_default_values: None,
            hyper_sweep_confirmed: false,
        }
    }

    /// Validate domains and ensure every reference targets a compatible scalar input.
    pub fn validate_hyper_parameters(&self) -> Result<(), String> {
        // Validate each domain independently. Expanding the Cartesian product is deferred
        // until AQE receives an explicit `--hyper-sweep` request.
        let mut names = HashSet::new();
        for definition in &self.hyper_parameters {
            if definition.name.trim().is_empty() || !names.insert(definition.name.clone()) {
                return Err(format!(
                    "hyperparameter names must be unique and non-empty (got '{}')",
                    definition.name
                ));
            }
            definition.expanded_values()?;
        }
        let definitions: BTreeMap<_, _> = self
            .hyper_parameters
            .iter()
            .map(|definition| (definition.name.as_str(), definition))
            .collect();
        for node in &self.nodes {
            for input in &node.inputs {
                let Some(reference) = input.hyper_reference.as_deref() else {
                    continue;
                };
                let definition = definitions.get(reference).ok_or_else(|| {
                    format!(
                        "input '{}.{}' references unknown hyperparameter '{}'",
                        node.label, input.name, reference
                    )
                })?;
                if !definition.value_type.matches_input(&input.input_type) {
                    return Err(format!(
                        "input '{}.{}' is incompatible with hyperparameter '{}'",
                        node.label, input.name, reference
                    ));
                }
            }
        }
        if let Some(seed) = self.hyper_default_seed.as_deref() {
            let Some(values) = self.hyper_default_values.as_ref() else {
                return Err(format!(
                    "default hyperparameter seed '{}' is missing its saved values; select it again",
                    seed
                ));
            };
            if canonical_hyperparameter_seed(values)? != seed {
                return Err("default hyperparameter seed does not match its saved values".to_string());
            }
        }
        Ok(())
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
                    hyper_reference: None,
                },
                NodeOutput {
                    name: "init".to_string(),
                    output_type: OutputType::Init,
                    insight_state: None,
                    hyper_reference: None,
                },
                NodeOutput {
                    name: "universe".to_string(),
                    output_type: OutputType::Universe,
                    insight_state: None,
                    hyper_reference: None,
                },
                NodeOutput {
                    name: "on_bar".to_string(),
                    output_type: OutputType::OnBar,
                    insight_state: None,
                    hyper_reference: None,
                },
                NodeOutput {
                    name: "insight_pipeline".to_string(),
                    output_type: OutputType::InsightPipeline,
                    insight_state: None,
                    hyper_reference: None,
                },
                NodeOutput {
                    name: "on_teardown".to_string(),
                    output_type: OutputType::OnTeardown,
                    insight_state: None,
                    hyper_reference: None,
                },
            ],
            source_file: None,
            lifecycle_phase: None,
            lifecycle_timing: None,
            can_fail: None,
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

#[cfg(test)]
mod hyperparameter_tests {
    use super::*;

    #[test]
    fn expands_ranges_in_a_stable_seed_order() {
        let runs = canonical_hyperparameter_runs(&[
            HyperParameterDefinition {
                name: "z".to_string(),
                value_type: HyperParameterType::Int,
                values: vec![Value::from(1), Value::from(2)],
                min: None,
                max: None,
                increment: None,
            },
            HyperParameterDefinition {
                name: "a".to_string(),
                value_type: HyperParameterType::Float,
                values: vec![],
                min: Some(0.5),
                max: Some(1.0),
                increment: Some(0.5),
            },
        ])
        .unwrap();
        assert_eq!(runs.len(), 4);
        assert_eq!(runs[0].values.get("a"), Some(&serde_json::json!(0.5)));
        assert_eq!(runs[0].values.get("z"), Some(&serde_json::json!(1)));
        assert_eq!(
            runs,
            canonical_hyperparameter_runs(&[
                HyperParameterDefinition {
                    name: "z".to_string(),
                    value_type: HyperParameterType::Int,
                    values: vec![Value::from(1), Value::from(2)],
                    min: None,
                    max: None,
                    increment: None
                },
                HyperParameterDefinition {
                    name: "a".to_string(),
                    value_type: HyperParameterType::Float,
                    values: vec![],
                    min: Some(0.5),
                    max: Some(1.0),
                    increment: Some(0.5)
                },
            ])
            .unwrap()
        );
    }
}
