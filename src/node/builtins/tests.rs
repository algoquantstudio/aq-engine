use super::*;

#[test]
fn builtin_alpha_controls_upgrade_without_replacing_user_configuration() {
    let nodes = get_builtin_nodes();
    for mut node in nodes.into_iter().filter(|n| n.node_type == crate::node::NodeType::Alpha) {
        let atr = node.inputs.iter_mut().find(|i| i.name == "atr_period").unwrap();
        atr.value = Some(serde_json::json!(21));
        atr.hyper_reference = Some("atr_sweep".into());
        node.inputs.retain(|i| ["on_bar", "atr_period", "ema_period", "rsi_period", "local_window", "divergance_window", "base_confidence_modifier_field", "max_spawn", "limit_entries"].contains(&i.name.as_str()));
        let old = node.clone();
        let mut upgrade = vec![node];
        assert!(upgrade_builtin_alpha_inputs(&mut upgrade));
        assert!(!upgrade_builtin_alpha_inputs(&mut upgrade));
        for input in old.inputs { assert!(upgrade[0].inputs.contains(&input)); }
        assert_eq!(upgrade[0].id, old.id);
        assert_eq!(upgrade[0].outputs, old.outputs);
        assert!(upgrade[0].inputs.iter().any(|i| i.name == "cooldown_bars"));
        assert_eq!(upgrade[0].inputs.iter().any(|i| i.name == "max_active_insights"), upgrade[0].label != "test_entry");
    }
}

#[test]
fn allow_trading_window_exposes_compact_string_inputs() {
    let nodes = get_builtin_nodes();
    let node = nodes
        .iter()
        .find(|node| node.id == "builtin_allow_trading_window")
        .unwrap();
    let configurable_inputs = node
        .inputs
        .iter()
        .filter(|input| input.name != "insights" && input.name != "insight_scope")
        .collect::<Vec<_>>();

    assert_eq!(
        configurable_inputs
            .iter()
            .map(|input| input.name.as_str())
            .collect::<Vec<_>>(),
        ["start_utc", "end_utc", "allowed_days"]
    );
    assert!(
        configurable_inputs
            .iter()
            .all(|input| matches!(input.input_type, InputType::Str))
    );
}

#[test]
fn every_builtin_pipe_defaults_to_parent_only_scope() {
    for node in get_builtin_nodes()
        .into_iter()
        .filter(|node| node.node_type == crate::node::NodeType::Pipe)
    {
        let scope = node
            .inputs
            .iter()
            .find(|input| input.name == "insight_scope")
            .unwrap_or_else(|| panic!("{} is missing insight_scope", node.label));
        assert_eq!(scope.value, Some(serde_json::json!("ParentOnly")));
    }
}
