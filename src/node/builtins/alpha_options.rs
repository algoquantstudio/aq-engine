use crate::node::{InputType, Node, NodeInput, NodeType};

pub(crate) fn model_name(source: &str) -> Option<&'static str> {
    match source {
        "built_in_test_entry" => Some("TestEntry"),
        "built_in_ema_price_crossover" => Some("EmaPriceCrossover"),
        "built_in_rsi_divergance_alpha" => Some("RsiDiverganceAlpha"),
        _ => None,
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) const COMMON_OPTIONS: &[&str] = &[
    "allow_long",
    "allow_short",
    "stop_loss_atr",
    "take_profit_atr",
    "max_active_insights",
    "cooldown_bars",
    "period_unfilled",
    "period_till_tp",
];

fn input(name: &str, input_type: InputType, value: serde_json::Value) -> NodeInput {
    NodeInput {
        name: name.into(),
        input_type,
        value: Some(value),
        is_public: true,
        insight_state: None,
        hyper_reference: None,
    }
}

pub(super) fn append(node: &mut Node) {
    let Some(model) = node.source_file.as_deref().and_then(model_name) else {
        return;
    };
    if node.node_type != NodeType::Alpha {
        return;
    }
    use serde_json::json;
    node.inputs.extend([
        input("allow_long", InputType::Bool, json!(true)),
        input("allow_short", InputType::Bool, json!(true)),
        input("stop_loss_atr", InputType::Float, json!(1.5)),
        input(
            "take_profit_atr",
            InputType::Float,
            json!(if model == "TestEntry" { 2.0 } else { 3.5 }),
        ),
        input("max_active_insights", InputType::Int, json!(0)),
        input("cooldown_bars", InputType::Int, json!(0)),
        input("period_unfilled", InputType::Int, json!(0)),
        input("period_till_tp", InputType::Int, json!(0)),
    ]);
    // Test Entry already exposes its original per-symbol cap as max_spawn.
    if model == "TestEntry" {
        node.inputs
            .retain(|input| input.name != "max_active_insights");
    }
    match model {
        "EmaPriceCrossover" => node.inputs.extend([
            input("max_entry_distance_atr", InputType::Float, json!(1.0)),
            input("swing_stop_atr", InputType::Float, json!(1.0)),
        ]),
        "RsiDiverganceAlpha" => node.inputs.extend([
            input("min_rsi_change", InputType::Float, json!(0.0)),
            input("require_low_relative_volume", InputType::Bool, json!(true)),
        ]),
        _ => {}
    }
}

/// Add newly introduced controls to saved alpha nodes without replacing values, wiring,
/// hyperparameter references, labels, or IDs. Repeated upgrades are a no-op.
pub fn upgrade_builtin_alpha_inputs(nodes: &mut [Node]) -> bool {
    let templates = super::get_builtin_nodes();
    let mut changed = false;
    for node in nodes.iter_mut().filter(|n| n.node_type == NodeType::Alpha) {
        let Some(template) = templates
            .iter()
            .find(|t| t.node_type == NodeType::Alpha && t.source_file == node.source_file)
        else {
            continue;
        };
        for input in &template.inputs {
            if !node
                .inputs
                .iter()
                .any(|existing| existing.name == input.name)
            {
                node.inputs.push(input.clone());
                changed = true;
            }
        }
    }
    changed
}
