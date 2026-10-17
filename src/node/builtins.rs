use crate::node::{InputType, Node, NodeInput};

#[path = "builtins/core.rs"]
mod core;
#[path = "builtins/flow.rs"]
mod flow;
#[path = "builtins/position_and_lifecycle.rs"]
mod position_and_lifecycle;
#[path = "builtins/signals_and_risk.rs"]
mod signals_and_risk;
#[path = "builtins/indicators.rs"]
mod indicators;
#[path = "builtins/alpha_options.rs"]
pub(crate) mod alpha_options;
pub use alpha_options::upgrade_builtin_alpha_inputs;

/// Retrieve all supported built-in engine components for visual node editors.
pub fn get_builtin_nodes() -> Vec<Node> {
    let mut nodes = Vec::new();
    nodes.extend(core::nodes());
    nodes.extend(flow::nodes());
    nodes.extend(signals_and_risk::nodes());
    nodes.extend(position_and_lifecycle::nodes());
    nodes.extend(indicators::nodes());
    for node in &mut nodes { alpha_options::append(node); }

    for node in nodes
        .iter_mut()
        .filter(|node| node.node_type == crate::node::NodeType::Pipe)
    {
        node.inputs.push(NodeInput {
            name: "insight_scope".to_string(),
            input_type: InputType::Str,
            value: Some(serde_json::json!("ParentOnly")),
            is_public: true,
            insight_state: None,
            hyper_reference: None,
        });
    }

    nodes
}

#[cfg(test)]
#[path = "builtins/tests.rs"]
mod tests;
