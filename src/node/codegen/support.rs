use crate::node::{LifecyclePhase, LifecycleTiming, Node};

#[derive(Debug, Clone)]
pub(super) struct EventRegistration {
    pub(super) timeframe_amount: u8,
    pub(super) timeframe_unit: String,
    pub(super) allow_trading: bool,
}

pub(super) fn format_number_literal(
    input_type: &crate::node::InputType,
    n: &serde_json::Number,
) -> String {
    match input_type {
        crate::node::InputType::Float => {
            if let Some(value) = n.as_f64() {
                if value.fract() == 0.0 {
                    format!("{value:.1}")
                } else {
                    value.to_string()
                }
            } else {
                n.to_string()
            }
        }
        _ => n.to_string(),
    }
}

pub(super) fn asset_fee_source(fee: &crate::node::AssetFee) -> String {
    match fee {
        crate::node::AssetFee::None => "AssetFee::None".to_string(),
        crate::node::AssetFee::Points(value) => {
            format!("AssetFee::Points({value:?})")
        }
        crate::node::AssetFee::PercentageFee(value) => {
            format!("AssetFee::PercentageFee({value:?})")
        }
        crate::node::AssetFee::FixedFee(value) => {
            format!("AssetFee::FixedFee({value:?})")
        }
        crate::node::AssetFee::PerContractFee(value) => {
            format!("AssetFee::PerContractFee({value:?})")
        }
        crate::node::AssetFee::PercentagePerContractFee(value) => {
            format!("AssetFee::PercentagePerContractFee({value:?})")
        }
    }
}

pub(super) fn asset_side_fees_source(fees: &crate::node::AssetSideFees) -> String {
    format!(
        "AssetSideFees {{ long: {}, short: {} }}",
        asset_fee_source(&fees.long),
        asset_fee_source(&fees.short)
    )
}

pub(super) fn asset_commission_fees_source(fees: &crate::node::AssetCommissionFees) -> String {
    format!(
        "AssetCommissionFees {{ entry: {}, exit: {} }}",
        asset_side_fees_source(&fees.entry),
        asset_side_fees_source(&fees.exit)
    )
}

pub(super) fn asset_swap_fees_source(fees: &crate::node::AssetSwapFees) -> String {
    format!(
        "AssetSwapFees {{ long: {}, short: {} }}",
        asset_fee_source(&fees.long),
        asset_fee_source(&fees.short)
    )
}

pub(super) fn asset_fees_source(fees: &crate::node::AssetFees) -> String {
    format!(
        "AssetFees {{ commission: {}, swap: {}, entry: {}, exit: {} }}",
        asset_commission_fees_source(&fees.commission),
        asset_swap_fees_source(&fees.swap),
        asset_fee_source(&fees.entry),
        asset_fee_source(&fees.exit)
    )
}

pub(super) fn node_input_value<'a>(node: &'a Node, name: &str) -> Option<&'a serde_json::Value> {
    node.inputs
        .iter()
        .find(|input| input.name == name)
        .and_then(|input| input.value.as_ref())
}

pub(super) fn node_input_string(node: &Node, name: &str, default: &str) -> String {
    node_input_value(node, name)
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(default)
        .to_string()
}

pub(super) fn node_input_u8(node: &Node, name: &str, default: u8) -> u8 {
    node_input_value(node, name)
        .and_then(|value| value.as_i64())
        .and_then(|value| u8::try_from(value.max(1)).ok())
        .unwrap_or(default)
}

pub(super) fn node_input_bool(node: &Node, name: &str, default: bool) -> bool {
    node_input_value(node, name)
        .and_then(|value| value.as_bool())
        .unwrap_or(default)
}

pub(super) fn node_input_literal(input: &crate::node::NodeInput) -> String {
    match input.value.as_ref() {
        Some(serde_json::Value::String(value)) => format!(
            "\"{}\".to_string()",
            value.replace('\\', "\\\\").replace('"', "\\\"")
        ),
        Some(serde_json::Value::Number(value)) => format_number_literal(&input.input_type, value),
        Some(serde_json::Value::Bool(value)) => value.to_string(),
        Some(serde_json::Value::Array(values)) => {
            let values: Vec<String> = values
                .iter()
                .filter_map(|value| value.as_str())
                .map(|value| {
                    format!(
                        "\"{}\".to_string()",
                        value.replace('\\', "\\\\").replace('"', "\\\"")
                    )
                })
                .collect();
            format!("vec![{}]", values.join(", "))
        }
        _ => "Default::default()".to_string(),
    }
}

pub(super) fn hyper_argument_source(input: &crate::node::NodeInput) -> String {
    let fallback = node_input_literal(input);
    match input.hyper_reference.as_deref() {
        Some(reference) => {
            let resolver = match input.input_type {
                crate::node::InputType::Int => "hyper_int",
                crate::node::InputType::Float => "hyper_float",
                crate::node::InputType::Bool => "hyper_bool",
                crate::node::InputType::Str => "hyper_string",
                _ => return fallback,
            };
            format!(
                "{}(ctx, \"{}\", {})",
                resolver,
                reference.replace('"', "\\\""),
                fallback
            )
        }
        None => fallback,
    }
}

pub(super) fn insight_pipe_scope_variant(node: &Node) -> &'static str {
    let Some(scope) = node
        .inputs
        .iter()
        .find(|input| input.name == "insight_scope")
        .and_then(|input| input.value.as_ref())
        .and_then(serde_json::Value::as_str)
    else {
        return "ParentOnly";
    };

    match scope
        .trim()
        .to_ascii_lowercase()
        .replace(['_', '-', ' '], "")
        .as_str()
    {
        "childonly" | "child" | "children" => "ChildOnly",
        "both" | "parentandchild" | "all" => "Both",
        _ => "ParentOnly",
    }
}

pub(super) fn timeframe_unit_variant(unit: &str) -> &'static str {
    match unit.trim().to_ascii_lowercase().as_str() {
        "second" | "seconds" | "s" => "Second",
        "hour" | "hours" | "h" => "Hour",
        "day" | "days" | "d" => "Day",
        "month" | "months" | "mo" => "Month",
        _ => "Minute",
    }
}

pub(super) fn event_registration_from_node(node: &Node) -> EventRegistration {
    // `EventStreamType` currently supports Bar only. Keep the `event_type` input on
    // the node so the saved graph can evolve without changing the user-facing shape.
    let _event_type = node_input_string(node, "event_type", "Bar");
    EventRegistration {
        timeframe_amount: node_input_u8(node, "timeframe_amount", 15),
        timeframe_unit: timeframe_unit_variant(&node_input_string(
            node,
            "timeframe_unit",
            "Minute",
        ))
        .to_string(),
        allow_trading: node_input_bool(node, "allow_trading", false),
    }
}

pub(super) fn lifecycle_timing_expr(timing: LifecycleTiming) -> &'static str {
    match timing {
        LifecycleTiming::BeforeGenerated => "LifecycleTiming::BeforeGenerated",
        LifecycleTiming::AfterGenerated => "LifecycleTiming::AfterGenerated",
    }
}

pub(super) fn lifecycle_builder_name(phase: LifecyclePhase) -> &'static str {
    match phase {
        LifecyclePhase::OnStart => "OnStartLogicBuilder",
        LifecyclePhase::OnInit => "OnInitLogicBuilder",
        LifecyclePhase::OnTeardown => "OnTeardownLogicBuilder",
    }
}

pub(super) fn lifecycle_add_method(phase: LifecyclePhase) -> &'static str {
    match phase {
        LifecyclePhase::OnStart => "add_on_start_logic",
        LifecyclePhase::OnInit => "add_on_init_logic",
        LifecyclePhase::OnTeardown => "add_on_teardown_logic",
    }
}

pub(super) fn push_state_component_registrations(
    src: &mut String,
    custom_lifecycle_logic: &[(
        LifecyclePhase,
        LifecycleTiming,
        bool,
        String,
        String,
        String,
        String,
    )],
    built_in_lifecycle_logic: &[(
        LifecyclePhase,
        LifecycleTiming,
        bool,
        String,
        String,
        String,
    )],
) {
    for (phase, timing, can_fail, mod_name, struct_name, args, _) in custom_lifecycle_logic {
        src.push_str(&format!(
            "        state.{}({}::new(Box::new({}::{}::new({}))).timing({}).can_fail({}).build());\n",
            lifecycle_add_method(*phase),
            lifecycle_builder_name(*phase),
            mod_name,
            struct_name,
            args,
            lifecycle_timing_expr(*timing),
            can_fail
        ));
    }
    for (phase, timing, can_fail, _mod_name, struct_name, args) in built_in_lifecycle_logic {
        src.push_str(&format!(
            "        state.{}({}::new(Box::new({}::new({}))).timing({}).can_fail({}).build());\n",
            lifecycle_add_method(*phase),
            lifecycle_builder_name(*phase),
            struct_name,
            args,
            lifecycle_timing_expr(*timing),
            can_fail
        ));
    }
    if !custom_lifecycle_logic.is_empty() || !built_in_lifecycle_logic.is_empty() {
        src.push('\n');
    }
}

pub(super) fn push_on_start_universe_registrations(
    src: &mut String,
    custom_universe_models: &[(bool, String, String, String, String)],
) {
    for (can_fail, mod_name, struct_name, args, _) in custom_universe_models {
        src.push_str(&format!(
            "        ctx.add_universe_model(UniverseModelBuilder::new(Box::new({}::{}::new({}))).can_fail({}).build());\n",
            mod_name, struct_name, args, can_fail
        ));
    }
    if !custom_universe_models.is_empty() {
        src.push('\n');
    }
}
