use super::support::hyper_argument_source;
use crate::node::{InputType, Node, builtins};

pub(super) fn registration(node: &Node) -> Result<Option<String>, String> {
    let Some(source) = node
        .source_file
        .as_deref()
        .filter(|s| s.starts_with("builtin_indicator_"))
    else {
        return Ok(None);
    };
    if node.node_type != crate::node::NodeType::LogicBlock
        || node
            .lifecycle_phase
            .is_some_and(|p| p != crate::node::LifecyclePhase::OnStart)
    {
        return Err(format!(
            "{}: indicators must be on-start registration blocks",
            node.label
        ));
    }
    let template = builtins::get_builtin_nodes()
        .into_iter()
        .find(|n| n.source_file.as_deref() == Some(source))
        .ok_or_else(|| format!("Unknown built-in indicator: {source}"))?;
    if source == "builtin_indicator_macd" {
        let period = |name: &str, fallback| {
            node.inputs
                .iter()
                .find(|i| i.name == name)
                .map_or(Some(fallback), |i| {
                    if i.hyper_reference.is_some() {
                        None
                    } else {
                        i.value.as_ref().and_then(|v| v.as_u64()).or(Some(fallback))
                    }
                })
        };
        if let (Some(fast), Some(slow)) = (period("fast_period", 12), period("slow_period", 26)) {
            if fast >= slow {
                return Err(format!(
                    "{}: fast_period must be less than slow_period",
                    node.label
                ));
            }
        }
    }
    let mut args = std::collections::HashMap::new();
    for default in template
        .inputs
        .iter()
        .filter(|i| i.input_type != InputType::OnStart)
    {
        let matches: Vec<_> = node
            .inputs
            .iter()
            .filter(|i| i.name == default.name)
            .collect();
        if matches.len() > 1 {
            return Err(format!("{}: duplicate {}", node.label, default.name));
        }
        let mut input = default.clone();
        if let Some(saved) = matches.first() {
            if saved.input_type != default.input_type {
                return Err(format!("{}: invalid type for {}", node.label, default.name));
            }
            input.value = saved.value.clone().or(default.value.clone());
            input.hyper_reference = saved.hyper_reference.clone();
        }
        let value = input.value.as_ref().unwrap();
        let valid = match input.input_type {
            InputType::Int => value.as_u64().is_some_and(|n| (1..=1_000_000).contains(&n)),
            InputType::Float => value.as_f64().is_some_and(|n| {
                n.is_finite() && n > 0.0 && (input.name != "value_area" || n <= 1.0)
            }),
            InputType::Str => value
                .as_str()
                .is_some_and(|s| input.name == "output_prefix" || !s.trim().is_empty()),
            _ => false,
        };
        if !valid {
            return Err(format!("{}: invalid {}", node.label, input.name));
        }
        let expression = if input.name == "smoothing" {
            if input.hyper_reference.is_some() {
                return Err(
                    "Indicator smoothing is a fixed choice; sweep numeric periods instead".into(),
                );
            }
            match value.as_str().unwrap().trim().to_ascii_lowercase().as_str() {
                "simple" | "sma" => "Smoothing::Simple".into(),
                "exponential" | "ema" => "Smoothing::Exponential".into(),
                "wilder" | "rma" => "Smoothing::Wilder".into(),
                _ => return Err("Indicator smoothing must be Simple, Exponential or Wilder".into()),
            }
        } else {
            hyper_argument_source(&input)
        };
        args.insert(input.name.clone(), expression);
    }
    let p = args.get("period").map(String::as_str).unwrap_or("0");
    let target = args
        .get("target_column")
        .map(String::as_str)
        .unwrap_or("\"close\".to_string()");
    let constructor = match source.trim_start_matches("builtin_indicator_") {
        "sma" => format!("SimpleMovingAverage::new({p}, &{target})"),
        "ema" => format!("ExponentialMovingAverage::new({p}, &{target})"),
        "rsi" => format!("RelativeStrengthIndex::new({p}, &{target})"),
        "atr" => format!(
            "AverageTrueRange::new({p}).with_columns(&{}, &{}, &{}).with_smoothing({})",
            args["high_column"], args["low_column"], args["close_column"], args["smoothing"]
        ),
        "qstick" => format!(
            "QStick::new({p}).with_columns(&{}, &{}).with_smoothing({})",
            args["open_column"], args["close_column"], args["smoothing"]
        ),
        "macd" => format!(
            "Macd::new({}, {}, {}, &{target})",
            args["fast_period"], args["slow_period"], args["signal_period"]
        ),
        "bollinger" => format!(
            "BollingerBands::new({p}, {}, &{target})",
            args["deviations"]
        ),
        "vwma" => format!(
            "VolumeWeightedMovingAverage::new({p}, &{target}).with_volume_column(&{})",
            args["volume_column"]
        ),
        "roc" => format!("RateOfChange::new({p}, &{target})"),
        "volume_profile" => format!(
            "VolumeProfile::new({p}, {}).with_value_area({}).with_columns(&{target}, &{})",
            args["bin_size"], args["value_area"], args["volume_column"]
        ),
        _ => unreachable!(),
    };
    Ok(Some(format!(
        "        {{\n            use aq_engine::core::indicators::{{Indicator, I::*}};\n            let indicator = {constructor};\n            let prefix = {};\n            let indicator = if prefix.trim().is_empty() {{ indicator }} else {{ indicator.with_output_prefix(&prefix) }};\n            if let Err(error) = indicator.validate() {{\n                error!(\"Invalid indicator configuration: {{}}\", error);\n                ctx.shutdown();\n                return;\n            }} else {{\n                ctx.set_warm_up_bars(ctx.warm_up_bars().max(indicator.window_size() as i32));\n                ctx.register_indicator(Box::new(indicator));\n            }}\n        }}\n",
        args["output_prefix"]
    )))
}
