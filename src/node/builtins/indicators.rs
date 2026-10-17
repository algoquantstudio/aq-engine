use crate::node::{InputType, Node, NodeInput, NodeOutput, NodeType, OutputType};
use serde_json::{Value, json};

fn setting(name: &str, input_type: InputType, value: Value) -> NodeInput {
    NodeInput {
        name: name.into(),
        input_type,
        value: Some(value),
        is_public: true,
        insight_state: None,
        hyper_reference: None,
    }
}
/// Indicators are on-start registration blocks, usable without writing custom Rust.
pub(super) fn nodes() -> Vec<Node> {
    let period = |p| setting("period", InputType::Int, json!(p));
    let field = |name, value| setting(name, InputType::Str, json!(value));
    let float = |name, value| setting(name, InputType::Float, json!(value));
    let mut specs = vec![
        (
            "sma",
            "SMA",
            vec![period(14), field("target_column", "close")],
        ),
        (
            "ema",
            "EMA",
            vec![period(14), field("target_column", "close")],
        ),
        (
            "rsi",
            "RSI",
            vec![period(14), field("target_column", "close")],
        ),
        (
            "atr",
            "ATR",
            vec![
                period(14),
                field("high_column", "high"),
                field("low_column", "low"),
                field("close_column", "close"),
                field("smoothing", "Simple"),
            ],
        ),
        (
            "qstick",
            "Q-stick",
            vec![
                period(14),
                field("open_column", "open"),
                field("close_column", "close"),
                field("smoothing", "Simple"),
            ],
        ),
        (
            "macd",
            "MACD",
            vec![
                setting("fast_period", InputType::Int, json!(12)),
                setting("slow_period", InputType::Int, json!(26)),
                setting("signal_period", InputType::Int, json!(9)),
                field("target_column", "close"),
            ],
        ),
        (
            "bollinger",
            "Bollinger Bands",
            vec![
                period(20),
                float("deviations", 2.0),
                field("target_column", "close"),
            ],
        ),
        (
            "vwma",
            "Volume-weighted MA",
            vec![
                period(20),
                field("target_column", "close"),
                field("volume_column", "volume"),
            ],
        ),
        (
            "roc",
            "Rate of Change",
            vec![period(12), field("target_column", "close")],
        ),
        (
            "volume_profile",
            "Volume Profile",
            vec![
                period(100),
                float("bin_size", 1.0),
                float("value_area", 0.7),
                field("target_column", "close"),
                field("volume_column", "volume"),
            ],
        ),
    ];
    specs
        .drain(..)
        .map(|(key, label, mut inputs)| {
            inputs.insert(
                0,
                NodeInput {
                    name: "on_start".into(),
                    input_type: InputType::OnStart,
                    value: None,
                    is_public: true,
                    insight_state: None,
                    hyper_reference: None,
                },
            );
            inputs.push(field("output_prefix", ""));
            Node {
                id: format!("builtin_indicator_{key}"),
                node_type: NodeType::LogicBlock,
                label: format!("{label} Indicator"),
                x: 0.0,
                y: 0.0,
                inputs,
                outputs: vec![NodeOutput {
                    name: "on_start".into(),
                    output_type: OutputType::OnStart,
                    insight_state: None,
                    hyper_reference: None,
                }],
                source_file: Some(format!("builtin_indicator_{key}")),
                lifecycle_phase: Some(crate::node::LifecyclePhase::OnStart),
                lifecycle_timing: None,
                can_fail: None,
                undeletable: false,
            }
        })
        .collect()
}
