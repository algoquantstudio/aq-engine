use super::support::hyper_argument_source;
use crate::node::{InputType, Node, builtins};

/// Resolve built-in constructor fields by name, not saved input order. Additional controls are
/// builder options so older generated Rust and legacy .aqmeta files keep compiling.
pub(super) fn expression(node: &Node) -> Result<Option<(String, String)>, String> {
    let Some(model) = node
        .source_file
        .as_deref()
        .and_then(builtins::alpha_options::model_name)
    else {
        return Ok(None);
    };
    let template = builtins::get_builtin_nodes()
        .into_iter()
        .find(|n| n.source_file == node.source_file)
        .expect("known built-in alpha");
    let mut resolved = std::collections::HashMap::new();
    for default in template
        .inputs
        .iter()
        .filter(|i| i.input_type != InputType::OnBar)
    {
        let mut input = default.clone();
        let matching: Vec<_> = node
            .inputs
            .iter()
            .filter(|i| i.name == default.name)
            .collect();
        if matching.len() > 1 {
            return Err(format!("{}: duplicate input {}", node.label, default.name));
        }
        if let Some(saved) = matching.first() {
            if saved.input_type != default.input_type {
                return Err(format!(
                    "{}: invalid input type for {}",
                    node.label, default.name
                ));
            }
            input.value = saved.value.clone().or_else(|| default.value.clone());
            input.hyper_reference = saved.hyper_reference.clone();
        }
        let value = input.value.as_ref().expect("alpha setting default");
        let valid = match input.input_type {
            InputType::Bool => value.is_boolean(),
            InputType::Str => value.is_string(),
            InputType::Int => value
                .as_u64()
                .is_some_and(|number| match input.name.as_str() {
                    "atr_period" | "ema_period" | "rsi_period" | "local_window"
                    | "divergance_window" => (1..=100_000).contains(&number),
                    "period_unfilled" | "period_till_tp" => number <= u32::MAX as u64,
                    _ => number <= usize::MAX as u64,
                }),
            InputType::Float => value.as_f64().is_some_and(|number| {
                number.is_finite()
                    && if input.name == "min_rsi_change" {
                        (0.0..=100.0).contains(&number)
                    } else {
                        number > 0.0
                    }
            }),
            _ => false,
        };
        if !valid {
            return Err(format!("{}: invalid value for {}", node.label, input.name));
        }
        resolved.insert(input.name.clone(), hyper_argument_source(&input));
    }
    let names: &[&str] = match model {
        "TestEntry" => &[
            "atr_period",
            "limit_entries",
            "max_spawn",
            "base_confidence_modifier_field",
        ],
        "EmaPriceCrossover" => &["atr_period", "ema_period", "base_confidence_modifier_field"],
        _ => &[
            "local_window",
            "divergance_window",
            "atr_period",
            "rsi_period",
            "base_confidence_modifier_field",
        ],
    };
    let args = names
        .iter()
        .map(|name| resolved[*name].as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let options = builtins::alpha_options::COMMON_OPTIONS
        .iter()
        .map(|name| {
            format!(
                "{name}: {}",
                resolved.get(*name).map(String::as_str).unwrap_or("0")
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let mut expression = format!(
        "{model}::new({args}).with_options(aq_engine::core::alpha::BuiltinAlphaOptions {{ {options} }})"
    );
    match model {
        "EmaPriceCrossover" => expression.push_str(&format!(
            ".with_signal_options({}, {})",
            resolved["max_entry_distance_atr"], resolved["swing_stop_atr"]
        )),
        "RsiDiverganceAlpha" => expression.push_str(&format!(
            ".with_divergence_options({}, {})",
            resolved["min_rsi_change"], resolved["require_low_relative_volume"]
        )),
        _ => {}
    }
    Ok(Some((model.into(), expression)))
}
