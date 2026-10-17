use super::*;
use crate::node::{ConnectionEndpoint, InputType, StrategyMeta};

fn indicator_meta() -> StrategyMeta {
    let mut meta = StrategyMeta::new("Indicator fixtures");
    meta.nodes
        .push(StrategyMeta::create_strategy_node(&meta.name));
    for mut node in crate::node::builtins::get_builtin_nodes()
        .into_iter()
        .filter(|n| {
            n.source_file
                .as_deref()
                .is_some_and(|s| s.starts_with("builtin_indicator_"))
        })
    {
        node.label = "My renamed indicator".into();
        node.inputs.reverse();
        meta.connections.push(Connection {
            from: ConnectionEndpoint {
                node_id: "strategy_root".into(),
                port: "on_start".into(),
            },
            to: ConnectionEndpoint {
                node_id: node.id.clone(),
                port: "on_start".into(),
            },
        });
        meta.nodes.push(node);
    }
    meta
}
#[test]
fn builtin_indicator_codegen_registers_all_ten_before_alphas() {
    let source = generate_main_rs(&indicator_meta()).unwrap();
    assert_eq!(
        source
            .matches("ctx.register_indicator(Box::new(indicator))")
            .count(),
        10
    );
    for constructor in [
        "SimpleMovingAverage::new(14",
        "ExponentialMovingAverage::new(14",
        "RelativeStrengthIndex::new(14",
        "AverageTrueRange::new(14",
        "QStick::new(14",
        "Macd::new(12, 26, 9",
        "BollingerBands::new(20, 2.0",
        "VolumeWeightedMovingAverage::new(20",
        "RateOfChange::new(12",
        "VolumeProfile::new(100, 1.0",
    ] {
        assert!(source.contains(constructor), "{constructor}");
    }
    assert!(!source.contains("MyRenamedIndicator::new"));
    assert_eq!(source.matches("ctx.shutdown();\n                return;").count(),10);
    assert_eq!(
        source
            .matches("ctx.warm_up_bars().max(indicator.window_size() as i32)")
            .count(),
        10
    );
}
#[test]
fn builtin_indicator_codegen_handles_custom_sources_prefixes_and_hyperparameters() {
    let mut meta = indicator_meta();
    meta.hyper_parameters
        .push(crate::node::HyperParameterDefinition {
            name: "length".into(),
            value_type: crate::node::HyperParameterType::Int,
            values: vec![serde_json::json!(5), serde_json::json!(20)],
            min: None,
            max: None,
            increment: None,
        });
    let node = meta
        .nodes
        .iter_mut()
        .find(|n| n.id == "builtin_indicator_sma")
        .unwrap();
    for input in &mut node.inputs {
        match input.name.as_str() {
            "period" => input.hyper_reference = Some("length".into()),
            "target_column" => input.value = Some(serde_json::json!("open")),
            "output_prefix" => input.value = Some(serde_json::json!("my_sma")),
            _ => (),
        }
    }
    let source = generate_main_rs(&meta).unwrap();
    assert!(source.contains("SimpleMovingAverage::new(hyper_int(ctx, \"length\", 14)"));
    assert!(source.contains("&\"open\".to_string()"));
    assert!(source.contains("let prefix = \"my_sma\".to_string()"));
}
#[test]
fn builtin_indicator_codegen_rejects_invalid_settings() {
    for (node_id, field, value) in [
        ("sma", "period", serde_json::json!(0)),
        ("sma", "target_column", serde_json::json!("")),
        ("macd", "fast_period", serde_json::json!(27)),
        ("atr", "smoothing", serde_json::json!("surprise")),
        ("volume_profile", "bin_size", serde_json::json!(-1)),
        ("volume_profile", "value_area", serde_json::json!(70)),
    ] {
        let mut meta = indicator_meta();
        let node = meta
            .nodes
            .iter_mut()
            .find(|n| n.id == format!("builtin_indicator_{node_id}"))
            .unwrap();
        node.inputs
            .iter_mut()
            .find(|i| i.name == field)
            .unwrap()
            .value = Some(value);
        assert!(generate_main_rs(&meta).is_err(), "{field}");
    }
    let mut meta = indicator_meta();
    let node = meta
        .nodes
        .iter_mut()
        .find(|n| n.id == "builtin_indicator_sma")
        .unwrap();
    node.inputs
        .iter_mut()
        .find(|i| i.name == "period")
        .unwrap()
        .input_type = InputType::Str;
    assert!(generate_main_rs(&meta).is_err());
}
