use crate::node::{Connection, LifecyclePhase, Node, NodeType, StrategyMeta};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

#[path = "codegen/graph.rs"]
mod graph;
#[path = "codegen/source.rs"]
mod source;
#[path = "codegen/support.rs"]
mod support;
#[path = "codegen/alpha.rs"]
mod alpha;
#[path = "codegen/indicators.rs"]
mod indicators;
#[cfg(test)]
#[path = "codegen/indicator_tests.rs"]
mod indicator_tests;

use graph::{get_reachable_nodes, toposort};
use source::{
    get_builtin_lifecycle_info, get_builtin_pipe_info, push_custom_module_decl,
    source_file_to_mod_name, to_pascal_case,
};
use support::{
    asset_fees_source, event_registration_from_node, hyper_argument_source,
    insight_pipe_scope_variant, push_on_start_universe_registrations,
    push_state_component_registrations,
};

/// Generate the complete `main.rs` source code for a strategy project.
///
/// Walks the node graph (toposort), resolves alpha/pipe modules,
/// and emits Rust source that creates a `StrategyState`, registers
/// components, and calls `run_backtest()`.
///
/// This is a pure function — no filesystem or Tauri dependency.
pub fn generate_main_rs(meta: &StrategyMeta) -> Result<String, String> {
    meta.validate_hyper_parameters()?;
    // ── 0. Filter reachable nodes ──
    let reachable_ids = get_reachable_nodes(&meta.nodes, &meta.connections);
    let reachable_nodes: Vec<Node> = meta
        .nodes
        .iter()
        .filter(|n| reachable_ids.contains(&n.id))
        .cloned()
        .collect();
    let reachable_connections: Vec<Connection> = meta
        .connections
        .iter()
        .filter(|c| {
            reachable_ids.contains(&c.from.node_id) && reachable_ids.contains(&c.to.node_id)
        })
        .cloned()
        .collect();
    // ── 1. Toposort the reachable node graph ──
    let sorted = toposort(&reachable_nodes, &reachable_connections)?;

    // ── 2. Collect alpha and pipe nodes ──
    let mut custom_alphas = Vec::new(); // (mod_name, struct_name, args_string, source_file)
    let mut built_in_alphas = Vec::new(); // (struct_name, constructor_expression)

    let mut custom_pipes = Vec::new(); // (mod_name, struct_name, args_string, allowed_alphas, target_state, source_file)
    let mut built_in_pipes = Vec::new(); // (mod_name, struct_name, args_string, allowed_alphas, target_state)
    let mut ordered_pipes = Vec::new(); // ordered pipe registration strings
    let mut custom_lifecycle_logic = Vec::new(); // (phase, timing, can_fail, mod_name, struct_name, args_string, source_file)
    let mut built_in_lifecycle_logic = Vec::new(); // (phase, timing, can_fail, mod_name, struct_name, args_string)
    let mut custom_universe_models = Vec::new(); // (can_fail, mod_name, struct_name, args_string, source_file)
    let mut event_registrations = Vec::new();
    let mut indicator_registrations = Vec::new();

    let mut universe_symbols: Vec<String> = Vec::new();

    // Mapping: Insight Pipe Node ID -> Vec<Alpha Name>
    let mut pipe_allow_alphas: HashMap<String, Vec<String>> = HashMap::new();
    // Mapping: Parent Pipe Node ID -> Vec<Child Variable Name>
    let mut pipe_children: HashMap<String, Vec<String>> = HashMap::new();
    let mut pipe_child_ids: HashMap<String, Vec<String>> = HashMap::new();
    let mut is_child_pipe: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut downstream_pipe_ids: HashMap<String, Vec<String>> = HashMap::new();

    // Pre-calculate mappings from Alpha -> Insight Pipe
    for conn in &reachable_connections {
        let from_node = reachable_nodes.iter().find(|n| n.id == conn.from.node_id);
        let to_node = reachable_nodes.iter().find(|n| n.id == conn.to.node_id);

        if let (Some(from), Some(to)) = (from_node, to_node) {
            if from.node_type == NodeType::Alpha && to.node_type == NodeType::Pipe {
                let alpha_struct = from.source_file.as_deref()
                    .and_then(crate::node::builtins::alpha_options::model_name)
                    .map(str::to_string).unwrap_or_else(|| to_pascal_case(&from.label));
                pipe_allow_alphas
                    .entry(to.id.clone())
                    .or_default()
                    .push(alpha_struct);
            }
            if from.node_type == NodeType::Pipe
                && to.node_type == NodeType::Pipe
                && conn.to.port != "insights_pipes"
            {
                downstream_pipe_ids
                    .entry(from.id.clone())
                    .or_default()
                    .push(to.id.clone());
            }
            if from.node_type == NodeType::Pipe
                && to.node_type == NodeType::Pipe
                && conn.to.port == "insights_pipes"
            {
                let child_var_name = format!("pipe_{}", from.id.replace("-", "_"));
                pipe_children
                    .entry(to.id.clone())
                    .or_default()
                    .push(child_var_name);
                pipe_child_ids
                    .entry(to.id.clone())
                    .or_default()
                    .push(from.id.clone());
                is_child_pipe.insert(from.id.clone());
            }
        }
    }

    // Pipe allow-lists should flow across the full downstream pipe chain and through nested
    // `insights_pipes` compositions so every descendant pipe keeps the originating alpha scope.
    let mut inherited_allow_alphas = pipe_allow_alphas.clone();
    let mut changed = true;
    while changed {
        changed = false;
        let pipe_ids: std::collections::HashSet<String> = inherited_allow_alphas
            .keys()
            .cloned()
            .chain(downstream_pipe_ids.keys().cloned())
            .chain(pipe_child_ids.keys().cloned())
            .collect();

        for pipe_id in pipe_ids {
            let Some(parent_allowed) = inherited_allow_alphas.get(&pipe_id).cloned() else {
                continue;
            };

            for child_id in pipe_child_ids.get(&pipe_id).into_iter().flatten() {
                let entry = inherited_allow_alphas.entry(child_id.clone()).or_default();
                let initial_len = entry.len();
                for alpha in &parent_allowed {
                    if !entry.contains(alpha) {
                        entry.push(alpha.clone());
                    }
                }
                if entry.len() != initial_len {
                    changed = true;
                }
            }

            for downstream_id in downstream_pipe_ids.get(&pipe_id).into_iter().flatten() {
                let entry = inherited_allow_alphas
                    .entry(downstream_id.clone())
                    .or_default();
                let initial_len = entry.len();
                for alpha in &parent_allowed {
                    if !entry.contains(alpha) {
                        entry.push(alpha.clone());
                    }
                }
                if entry.len() != initial_len {
                    changed = true;
                }
            }
        }
    }

    for node_id in &sorted {
        let node = reachable_nodes.iter().find(|n| n.id == *node_id).unwrap();

        if let Some(registration) = indicators::registration(node)? {
            indicator_registrations.push(registration);
            continue;
        }

        // Build the arguments string (e.g., `"param1".to_string(), 100, true`)
        let mut args = Vec::new();
        // Skip trigger inputs, data inputs, and non-public state fields
        for input in node.inputs.iter().filter(|i| {
            i.is_public
                && !(node.node_type == NodeType::Pipe && i.name == "insight_scope")
                && !matches!(
                    i.input_type,
                    crate::node::InputType::Trigger
                        | crate::node::InputType::OnStart
                        | crate::node::InputType::Init
                        | crate::node::InputType::OnTeardown
                        | crate::node::InputType::Insights
                        | crate::node::InputType::OnBar
                        | crate::node::InputType::AlphaResult
                        | crate::node::InputType::InsightPipeResult
                        | crate::node::InputType::Universe
                        | crate::node::InputType::AlphaInstance
                        | crate::node::InputType::InsightPipeInstance
                )
        }) {
            args.push(hyper_argument_source(input));
        }
        let args_str = args.join(", ");

        match node.node_type {
            NodeType::Alpha => {
                let struct_name = to_pascal_case(&node.label);
                if let Some(ref src) = node.source_file {
                    if src.starts_with("built_in") {
                        built_in_alphas.push(alpha::expression(node)?.unwrap_or_else(|| {
                            let expression = format!("{struct_name}::new({args_str})");
                            (struct_name, expression)
                        }));
                    } else {
                        let mod_name = source_file_to_mod_name(src);
                        custom_alphas.push((mod_name, struct_name, args_str, src.clone()));
                    }
                }
            }
            NodeType::Pipe => {
                let struct_name = to_pascal_case(&node.label);
                // Attach the allowed_alphas constraint if this pipe was mapped
                let allowed_alphas_constraint =
                    if let Some(alphas) = inherited_allow_alphas.get(&node.id) {
                        let mut alpha_strings = Vec::new();
                        for a in alphas {
                            alpha_strings.push(format!("\"{}\".to_string()", a));
                        }
                        format!(
                            ".allowed_alphas(vec![{}].into_iter().collect())",
                            alpha_strings.join(", ")
                        )
                    } else {
                        "".to_string()
                    };

                let mut target_state_modifier = String::new();
                if let Some(in_port) = node.inputs.iter().find(|i| i.insight_state.is_some()) {
                    if let Some(state) = &in_port.insight_state {
                        target_state_modifier = format!(".target_state(InsightState::{:?})", state);
                    }
                }
                let insight_scope_modifier = format!(
                    ".insight_scope(InsightPipeScope::{})",
                    insight_pipe_scope_variant(node)
                );

                if let Some(ref src) = node.source_file {
                    let child_var_name = format!("pipe_{}", node.id.replace("-", "_"));
                    let mut final_args = args_str.clone();
                    if let Some(children) = pipe_children.get(&node.id) {
                        if final_args.is_empty() {
                            final_args = format!("vec![{}]", children.join(", "));
                        } else {
                            final_args = format!("{}, vec![{}]", final_args, children.join(", "));
                        }
                    }

                    let builder_expr = if let Some((mod_name, builtin_struct)) =
                        get_builtin_pipe_info(src, &struct_name)
                    {
                        built_in_pipes.push((
                            mod_name.clone(),
                            builtin_struct.clone(),
                            args_str.clone(),
                            allowed_alphas_constraint.clone(),
                            target_state_modifier.clone(),
                        ));
                        let builtin_instance = format!("{}::new({})", builtin_struct, final_args);
                        format!(
                            "InsightPipeBuilder::new(Box::new({})){}{}{}.build()",
                            builtin_instance,
                            allowed_alphas_constraint,
                            target_state_modifier,
                            insight_scope_modifier
                        )
                    } else {
                        let mod_name = source_file_to_mod_name(src);
                        custom_pipes.push((
                            mod_name.clone(),
                            struct_name.clone(),
                            args_str.clone(),
                            allowed_alphas_constraint.clone(),
                            target_state_modifier.clone(),
                            src.clone(),
                        ));
                        format!(
                            "InsightPipeBuilder::new(Box::new({}::{}::new({}))){}{}{}.build()",
                            mod_name,
                            struct_name,
                            final_args,
                            allowed_alphas_constraint,
                            target_state_modifier,
                            insight_scope_modifier
                        )
                    };

                    ordered_pipes.push(format!(
                        "        let {} = {};\n",
                        child_var_name, builder_expr
                    ));
                    if !is_child_pipe.contains(&node.id) {
                        ordered_pipes.push(format!("        ctx.add_pipe({});\n", child_var_name));
                    }
                }
            }
            NodeType::LogicBlock => {
                let struct_name = to_pascal_case(&node.label);
                if let (Some(src), Some(phase)) = (&node.source_file, node.lifecycle_phase) {
                    let timing = node.lifecycle_timing.unwrap_or_default();
                    let default_can_fail = phase == LifecyclePhase::OnTeardown;
                    if let Some((mod_name, builtin_struct)) =
                        get_builtin_lifecycle_info(src, &struct_name)
                    {
                        built_in_lifecycle_logic.push((
                            phase,
                            timing,
                            node.can_fail.unwrap_or(default_can_fail),
                            mod_name,
                            builtin_struct,
                            args_str,
                        ));
                    } else {
                        let mod_name = source_file_to_mod_name(src);
                        custom_lifecycle_logic.push((
                            phase,
                            timing,
                            node.can_fail.unwrap_or(default_can_fail),
                            mod_name,
                            struct_name,
                            args_str,
                            src.clone(),
                        ));
                    }
                }
            }
            NodeType::Universe => {
                // Extract symbols from the node's "symbols" input
                if let Some(input) = node.inputs.iter().find(|i| i.name == "symbols") {
                    if let Some(ref val) = input.value {
                        if let Ok(syms) = serde_json::from_value::<Vec<String>>(val.clone()) {
                            universe_symbols = syms;
                        }
                    }
                }
            }
            NodeType::UniverseModel => {
                let struct_name = to_pascal_case(&node.label);
                if let Some(ref src) = node.source_file {
                    let mod_name = source_file_to_mod_name(src);
                    custom_universe_models.push((
                        node.can_fail.unwrap_or(false),
                        mod_name,
                        struct_name,
                        args_str,
                        src.clone(),
                    ));
                }
            }
            NodeType::Trigger => {
                let is_on_start_event = reachable_connections
                    .iter()
                    .any(|conn| conn.to.node_id == node.id && conn.to.port == "on_start");
                if is_on_start_event {
                    event_registrations.push(event_registration_from_node(node));
                }
            }
            _ => {}
        }
    }

    // ── 3. Generate source ──
    let mut src = String::new();

    // Imports
    src.push_str("// Auto-generated by AlgoQuant Studio — DO NOT EDIT MANUALLY\n");
    src.push_str("use aq_engine::prelude::*;\n");
    let mut hyper_helper_imports = std::collections::BTreeSet::new();
    for node in &meta.nodes {
        for input in &node.inputs {
            if input.hyper_reference.is_none() {
                continue;
            }
            let helper = match input.input_type {
                crate::node::InputType::Int => Some("hyper_int"),
                crate::node::InputType::Float => Some("hyper_float"),
                crate::node::InputType::Bool => Some("hyper_bool"),
                crate::node::InputType::Str => Some("hyper_string"),
                _ => None,
            };
            if let Some(helper) = helper {
                hyper_helper_imports.insert(helper);
            }
        }
    }
    if !hyper_helper_imports.is_empty() {
        src.push_str(&format!(
            "use aq_engine::core::strategy::hyperparameters::{{{}}};\n",
            hyper_helper_imports
                .into_iter()
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    src.push('\n');

    // Module declarations for user alphas/pipes
    let mut emitted_mods = std::collections::HashSet::new();
    for (mod_name, _, _, source_file) in &custom_alphas {
        if emitted_mods.insert(mod_name.clone()) {
            push_custom_module_decl(&mut src, mod_name, source_file);
        }
    }
    for (mod_name, _, _, _, _, source_file) in &custom_pipes {
        if emitted_mods.insert(mod_name.clone()) {
            push_custom_module_decl(&mut src, mod_name, source_file);
        }
    }
    for (_, _, _, mod_name, _, _, source_file) in &custom_lifecycle_logic {
        if emitted_mods.insert(mod_name.clone()) {
            push_custom_module_decl(&mut src, mod_name, source_file);
        }
    }
    for (_, mod_name, _, _, source_file) in &custom_universe_models {
        if emitted_mods.insert(mod_name.clone()) {
            push_custom_module_decl(&mut src, mod_name, source_file);
        }
    }

    // Explicit use imports for built-ins
    let mut emitted_uses = std::collections::HashSet::new();
    for (struct_name, _) in &built_in_alphas {
        let import = format!("use aq_engine::core::alpha::{};\n", struct_name);
        if emitted_uses.insert(import.clone()) {
            src.push_str(&import);
        }
    }
    for (mod_name, struct_name, _, _, _) in &built_in_pipes {
        let import = format!(
            "use aq_engine::core::pipeline::{}::{};\n",
            mod_name, struct_name
        );
        if emitted_uses.insert(import.clone()) {
            src.push_str(&import);
        }
    }
    for (_, _, _, mod_name, struct_name, _) in &built_in_lifecycle_logic {
        let import = format!(
            "use aq_engine::core::lifecycle::{}::{};\n",
            mod_name, struct_name
        );
        if emitted_uses.insert(import.clone()) {
            src.push_str(&import);
        }
    }

    if !custom_alphas.is_empty()
        || !custom_pipes.is_empty()
        || !custom_lifecycle_logic.is_empty()
        || !custom_universe_models.is_empty()
        || !built_in_pipes.is_empty()
        || !built_in_alphas.is_empty()
        || !built_in_lifecycle_logic.is_empty()
    {
        src.push('\n');
    }

    // Strategy struct
    let strategy_name = to_pascal_case(&meta.name);
    src.push_str(&format!("pub struct {} {{}}\n\n", strategy_name));

    // Strategy impl
    src.push_str(&format!("impl Strategy for {} {{\n", strategy_name));
    src.push_str(&format!(
        "    fn name(&self) -> &str {{ \"{}\" }}\n\n",
        meta.name
    ));

    // on_start
    src.push_str("    fn on_start(&mut self, ctx: &mut dyn StrategyContext) {\n");
    src.push_str(&format!(
        "        info!(\"[{{}}] Strategy '{}' started\", Utc::now());\n\n",
        meta.name
    ));

    // Risk params
    src.push_str(&format!(
        "        ctx.set_execution_risk({:.2});\n",
        meta.config.execution_risk
    ));
    src.push_str(&format!(
        "        ctx.set_min_reward_risk_ratio({:.2});\n",
        meta.config.min_reward_risk_ratio
    ));
    src.push_str(&format!(
        "        ctx.set_base_confidence({:.2});\n\n",
        meta.config.base_confidence
    ));

    // Register additional market-data event streams.
    for event in &event_registrations {
        src.push_str(&format!(
            "        ctx.add_events_with_options(EventStreamRequest::new(EventStreamType::Bar, Some(TimeFrame::new({}, TimeFrameUnit::{}))).allow_trading({}));\n",
            event.timeframe_amount,
            event.timeframe_unit,
            event.allow_trading
        ));
    }
    if !event_registrations.is_empty() {
        src.push('\n');
    }

    for registration in &indicator_registrations { src.push_str(registration); }

    // Register alpha models
    for (mod_name, struct_name, args, _) in &custom_alphas {
        src.push_str(&format!(
            "        ctx.add_alpha(AlphaModelBuilder::new(Box::new({}::{}::new({}))).build());\n",
            mod_name, struct_name, args
        ));
    }
    for (_, expression) in &built_in_alphas {
        src.push_str(&format!(
            "        ctx.add_alpha(AlphaModelBuilder::new(Box::new({})).build());\n",
            expression
        ));
    }
    if (!custom_alphas.is_empty() || !built_in_alphas.is_empty())
        && !custom_universe_models.is_empty()
    {
        src.push('\n');
    }

    // Register universe models before universe loading. The engine merges these symbols with
    // the symbols returned by Strategy::universe.
    push_on_start_universe_registrations(&mut src, &custom_universe_models);

    // Register insight pipes - preserving topological order
    for pipe_code in &ordered_pipes {
        src.push_str(pipe_code);
    }
    // Users must explicitly add the InsightSubmitPipe to submit insights
    // src.push_str("        ctx.add_pipe(InsightPipeBuilder::new(Box::new(InsightSubmitPipe::new())).target_state(InsightState::New).build());\n");

    src.push_str("    }\n\n");

    // init
    src.push_str("    fn init(&mut self, _ctx: &mut dyn StrategyContext, asset: &Asset) {\n");
    src.push_str("        debug!(\"Initialising asset: {}\", asset.symbol);\n");
    src.push_str("    }\n\n");

    // universe
    src.push_str("    fn universe(&self, _ctx: &mut dyn StrategyContext) -> HashSet<String> {\n");
    if universe_symbols.is_empty() {
        src.push_str("        HashSet::new() // No symbols configured\n");
    } else {
        src.push_str("        vec![\n");
        for sym in &universe_symbols {
            src.push_str(&format!("            \"{}\".to_string(),\n", sym));
        }
        src.push_str("        ].into_iter().collect()\n");
    }
    src.push_str("    }\n\n");

    // on_bar — delegates to alpha models (handled by StrategyState)
    src.push_str("    fn on_bar(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str, _bar: &BarData) {\n");
    src.push_str("        debug!(\"Strategy on_bar invoked for {}\", _symbol);\n");
    src.push_str("    }\n\n");

    // generate_insights — delegates to alpha models
    src.push_str(
        "    fn generate_insights(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str) {\n",
    );
    src.push_str("        debug!(\"Strategy generate_insights invoked for {}\", _symbol);\n");
    src.push_str("    }\n\n");

    // insight_pipeline — handled by registered pipes
    src.push_str("    fn insight_pipeline(&mut self, _ctx: &mut dyn StrategyContext, _insight: &Insight) {\n");
    src.push_str("        debug!(\"Strategy insight_pipeline invoked for insight {}\", _insight.insight_id());\n");
    src.push_str("    }\n\n");

    // on_teardown
    src.push_str("    fn on_teardown(&mut self, _ctx: &mut dyn StrategyContext) {\n");
    src.push_str(&format!(
        "        info!(\"[{{}}] Strategy '{}' teardown callback finished; broker settlement is still managed by AQE\", Utc::now());\n",
        meta.name
    ));
    src.push_str("    }\n");
    src.push_str("}\n\n");

    // main function
    src.push_str("#[tokio::main]\n");
    src.push_str("async fn main() {\n");
    src.push_str("    let args: Vec<String> = std::env::args().collect();\n");
    src.push_str("    let is_live = args.contains(&\"--live\".to_string());\n\n");
    if meta.data_feed == crate::node::types::DataFeedType::Csv {
        src.push_str("    if is_live { eprintln!(\"CSV is a historical-only data feed. Select a live-capable feed before deploying.\"); std::process::exit(2); }\n");
    }
    let has_hyperparameters = !meta.hyper_parameters.is_empty();
    let hyper_source_targets: Vec<serde_json::Value> = meta
        .nodes
        .iter()
        .flat_map(|node| {
            node.inputs.iter().filter_map(move |input| {
                input.hyper_reference.as_ref().map(|key| {
                    serde_json::json!({
                        "key": key,
                        "node_id": node.id,
                        "node_label": node.label,
                        "input": input.name,
                        "fallback": input.value,
                    })
                })
            })
        })
        .collect();
    if has_hyperparameters {
        let sweep_id = hex::encode(Sha256::digest(
            serde_json::to_vec(meta).map_err(|error| error.to_string())?,
        ));
        let source_targets =
            serde_json::to_string(&hyper_source_targets).map_err(|error| error.to_string())?;
        src.push_str("    let mut hyperparameters = HyperParameterConfig::new();\n");
        src.push_str(&format!(
            "    hyperparameters.set_sweep_id(\"{sweep_id}\").set_strategy_fingerprint(\"{sweep_id}\").set_source_targets(serde_json::from_str(r###\"{source_targets}\"###).expect(\"generated hyperparameter targets are valid\"));\n"
        ));
        for definition in &meta.hyper_parameters {
            let fallback = meta
                .nodes
                .iter()
                .flat_map(|node| node.inputs.iter())
                .find(|input| input.hyper_reference.as_deref() == Some(definition.name.as_str()))
                .and_then(|input| input.value.clone())
                .or_else(|| definition.values.first().cloned())
                .or_else(|| definition.min.map(serde_json::Value::from))
                .unwrap_or(serde_json::Value::Null);
            let parameter = serde_json::json!({
                "key": definition.name,
                "values": (!definition.values.is_empty()).then_some(&definition.values),
                "min": definition.min,
                "max": definition.max,
                "increment": definition.increment,
                "fallback": fallback,
            });
            let parameter_json =
                serde_json::to_string(&parameter).map_err(|error| error.to_string())?;
            src.push_str(&format!(
                "    hyperparameters.add_hyper_parameter(serde_json::from_str::<HyperParameter>(r###\"{parameter_json}\"###).expect(\"generated hyperparameter is valid\")).expect(\"generated hyperparameter registration is valid\");\n"
            ));
        }
        src.push_str("    let is_hyper_sweep = hyperparameters.is_sweep_requested(&args);\n\n");
    }
    src.push_str(&format!(
        "    let _ = set_logging_level(\"{}\");\n\n",
        meta.config.log_level.to_lowercase()
    ));

    // Timeframe
    src.push_str(&format!(
        "    let timeframe = TimeFrame::new({}, TimeFrameUnit::{});\n",
        meta.config.timeframe_amount, meta.config.timeframe_unit
    ));

    let broker_leverage = meta.config.broker_leverage.max(1);
    let broker_fees = asset_fees_source(&meta.config.broker_fees);

    let data_feed_init = match meta.data_feed {
        crate::node::types::DataFeedType::Mt5 => "        let data_feed = Mt5DataFeed::from_env().unwrap_or_else(|error| {\n            eprintln!(\"Failed to initialise MT5 data feed: {:?}\", error);\n            std::process::exit(1);\n        });\n".to_string(),
        crate::node::types::DataFeedType::Csv => "        let data_feed = CsvDataFeed::from_env().unwrap_or_else(|error| {\n            eprintln!(\"Failed to initialise CSV data feed: {:?}\", error);\n            std::process::exit(1);\n        });\n".to_string(),
        crate::node::types::DataFeedType::YahooFinance => "        let data_feed = YahooFinanceDataFeed::new();\n".to_string(),
    };

    let live_execution_init = if meta.broker == crate::node::types::ExecutionBrokerType::Mt5 {
        "        let execution = Mt5Broker::from_env().unwrap_or_else(|error| {\n            eprintln!(\"Failed to initialise MT5 broker: {:?}\", error);\n            std::process::exit(1);\n        });\n".to_string()
    } else {
        format!(
            "        let execution = PaperBroker::new(AccountType::Paper, {:.1}, {}).with_asset_fees({});\n",
            meta.config.starting_cash, broker_leverage, broker_fees
        )
    };

    let backtest_execution_init = format!(
        "        let execution = PaperBroker::new(AccountType::Paper, {:.1}, {}).with_asset_fees({});\n",
        meta.config.starting_cash, broker_leverage, broker_fees
    );

    let live_state_init = format!(
        "        let broker = UnifiedBroker::new(execution, data_feed);\n\n        let mut state = StrategyState::new(\n            \"{name}\".to_string(),\n            \"{version}\".to_string(),\n            {struct_name} {{}},\n            broker,\n            StrategyMode::Live,\n            timeframe,\n        );\n\n        state.strategy_id = Uuid::parse_str(\"{strategy_id}\").unwrap();\n        state.set_artifact_root(env!(\"CARGO_MANIFEST_DIR\"));\n\n",
        name = meta.name,
        version = meta.version,
        struct_name = strategy_name,
        strategy_id = meta.id,
    );

    let backtest_state_init = format!(
        "        let broker = UnifiedBroker::new(execution, data_feed);\n\n        let mut state = StrategyState::new(\n            \"{name}\".to_string(),\n            \"{version}\".to_string(),\n            {struct_name} {{}},\n            broker,\n            StrategyMode::Backtest,\n            timeframe,\n        );\n\n        state.strategy_id = Uuid::parse_str(\"{strategy_id}\").unwrap();\n        state.set_artifact_root(env!(\"CARGO_MANIFEST_DIR\"));\n\n",
        name = meta.name,
        version = meta.version,
        struct_name = strategy_name,
        strategy_id = meta.id,
    );

    // (Components and risk params are now registered inside on_start)
    src.push_str("    if is_live {\n");
    src.push_str(&data_feed_init);
    src.push_str(&live_execution_init);
    src.push_str(&live_state_init);
    if has_hyperparameters {
        src.push_str("        state.set_hyper_parameter_config(hyperparameters.clone());\n");
    }
    push_state_component_registrations(
        &mut src,
        &custom_lifecycle_logic,
        &built_in_lifecycle_logic,
    );
    src.push_str("        if let Err(e) = state.run_live(None).await {\n");
    src.push_str("            eprintln!(\"Live execution failed: {:?}\", e);\n");
    src.push_str("            std::process::exit(1);\n");
    src.push_str("        }\n");
    src.push_str("    } else {\n");
    if has_hyperparameters {
        src.push_str("        let hyper_selections_to_execute = hyperparameters.process_runs(&args).unwrap_or_else(|error| { eprintln!(\"{error}\"); std::process::exit(2); });\n        for selected_hyperparameters in hyper_selections_to_execute {\n");
    } else {
        src.push_str("        {\n");
    }
    src.push_str(&data_feed_init);
    src.push_str(&backtest_execution_init);
    src.push_str(&backtest_state_init);
    if has_hyperparameters {
        src.push_str("        state.set_hyper_parameter_config(hyperparameters.clone());\n        if let Some(selection) = selected_hyperparameters.as_ref() { state.set_hyper_parameter_run(selection, is_hyper_sweep).unwrap_or_else(|error| { eprintln!(\"{error}\"); std::process::exit(2); }); }\n");
    }
    push_state_component_registrations(
        &mut src,
        &custom_lifecycle_logic,
        &built_in_lifecycle_logic,
    );
    if meta.broker != crate::node::types::ExecutionBrokerType::Paper {
        src.push_str(&format!(
            "        info!(\"Backtest mode uses Paper Broker execution. Selected live broker '{}' is ignored for this run.\");\n",
            meta.broker
        ));
    }
    src.push_str("        // Run backtest with configured start and end timestamps\n");
    if let Some(ref s_time) = meta.config.start_time {
        src.push_str(&format!("        let start = \"{}:00Z\".parse::<chrono::DateTime<Utc>>().unwrap_or_else(|_| Utc::now() - chrono::Duration::days(30));\n", s_time));
    } else {
        src.push_str("        let start = Utc::now() - chrono::Duration::days(30);\n");
    }

    if let Some(ref e_time) = meta.config.end_time {
        src.push_str(&format!("        let end = \"{}:00Z\".parse::<chrono::DateTime<Utc>>().unwrap_or_else(|_| Utc::now());\n\n", e_time));
    } else {
        src.push_str("        let end = Utc::now();\n\n");
    }
    src.push_str("        if let Err(e) = state.run_backtest(start, end, timeframe).await {\n");
    src.push_str("            eprintln!(\"Backtest failed: {:?}\", e);\n");
    src.push_str("            std::process::exit(1);\n");
    src.push_str("        }\n");
    src.push_str("        }\n");
    src.push_str("    }\n");
    src.push_str("}\n");

    Ok(src)
}

#[cfg(test)]
#[path = "codegen/tests.rs"]
mod tests;
