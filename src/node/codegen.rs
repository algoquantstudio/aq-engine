use crate::node::{Connection, LifecyclePhase, LifecycleTiming, Node, NodeType, StrategyMeta};
use std::collections::{HashMap, VecDeque};

fn format_number_literal(input_type: &crate::node::InputType, n: &serde_json::Number) -> String {
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

fn lifecycle_timing_expr(timing: LifecycleTiming) -> &'static str {
    match timing {
        LifecycleTiming::BeforeGenerated => "LifecycleTiming::BeforeGenerated",
        LifecycleTiming::AfterGenerated => "LifecycleTiming::AfterGenerated",
    }
}

fn lifecycle_builder_name(phase: LifecyclePhase) -> &'static str {
    match phase {
        LifecyclePhase::OnStart => "OnStartLogicBuilder",
        LifecyclePhase::OnInit => "OnInitLogicBuilder",
        LifecyclePhase::OnTeardown => "OnTeardownLogicBuilder",
    }
}

fn lifecycle_add_method(phase: LifecyclePhase) -> &'static str {
    match phase {
        LifecyclePhase::OnStart => "add_on_start_logic",
        LifecyclePhase::OnInit => "add_on_init_logic",
        LifecyclePhase::OnTeardown => "add_on_teardown_logic",
    }
}

fn push_state_component_registrations(
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
    if !custom_lifecycle_logic.is_empty() {
        src.push('\n');
    }
}

fn push_on_start_universe_registrations(
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

/// Generate the complete `main.rs` source code for a strategy project.
///
/// Walks the node graph (toposort), resolves alpha/pipe modules,
/// and emits Rust source that creates a `StrategyState`, registers
/// components, and calls `run_backtest()`.
///
/// This is a pure function — no filesystem or Tauri dependency.
pub fn generate_main_rs(meta: &StrategyMeta) -> Result<String, String> {
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
    let mut built_in_alphas = Vec::new(); // (struct_name, args_string)

    let mut custom_pipes = Vec::new(); // (mod_name, struct_name, args_string, allowed_alphas, target_state, source_file)
    let mut built_in_pipes = Vec::new(); // (mod_name, struct_name, args_string, allowed_alphas, target_state)
    let mut ordered_pipes = Vec::new(); // ordered pipe registration strings
    let mut custom_lifecycle_logic = Vec::new(); // (phase, timing, can_fail, mod_name, struct_name, args_string, source_file)
    let mut custom_universe_models = Vec::new(); // (can_fail, mod_name, struct_name, args_string, source_file)

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
                let alpha_struct = to_pascal_case(&from.label);
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

        // Build the arguments string (e.g., `"param1".to_string(), 100, true`)
        let mut args = Vec::new();
        // Skip trigger inputs, data inputs, and non-public state fields
        for input in node.inputs.iter().filter(|i| {
            i.is_public
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
            if let Some(ref val) = input.value {
                match val {
                    serde_json::Value::String(s) => args.push(format!("\"{}\".to_string()", s)),
                    serde_json::Value::Number(n) => {
                        args.push(format_number_literal(&input.input_type, n))
                    }
                    serde_json::Value::Bool(b) => args.push(b.to_string()),
                    serde_json::Value::Array(arr) => {
                        let strings: Vec<String> = arr
                            .iter()
                            .filter_map(|v| match v {
                                serde_json::Value::String(s) => {
                                    Some(format!("\"{}\".to_string()", s))
                                }
                                _ => None,
                            })
                            .collect();
                        args.push(format!("vec![{}]", strings.join(", ")));
                    }
                    _ => args.push("Default::default()".to_string()),
                }
            } else {
                args.push("Default::default()".to_string());
            }
        }
        let args_str = args.join(", ");

        match node.node_type {
            NodeType::Alpha => {
                let struct_name = to_pascal_case(&node.label);
                if let Some(ref src) = node.source_file {
                    if src.starts_with("built_in") {
                        built_in_alphas.push((struct_name, args_str));
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
                        format!(
                            "InsightPipeBuilder::new(Box::new({}::new({}))){}{}.build()",
                            builtin_struct,
                            final_args,
                            allowed_alphas_constraint,
                            target_state_modifier
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
                            "InsightPipeBuilder::new(Box::new({}::{}::new({}))){}{}.build()",
                            mod_name,
                            struct_name,
                            final_args,
                            allowed_alphas_constraint,
                            target_state_modifier
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
                    let mod_name = source_file_to_mod_name(src);
                    let timing = node.lifecycle_timing.unwrap_or_default();
                    let default_can_fail = phase == LifecyclePhase::OnTeardown;
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
            _ => {}
        }
    }

    // ── 3. Generate source ──
    let mut src = String::new();

    // Imports
    src.push_str("// Auto-generated by AlgoQuant Studio — DO NOT EDIT MANUALLY\n");
    src.push_str("use aq_engine::core::strategy::{AqsAuth, StrategyState, Strategy, StrategyContext, StrategyMode};\n");
    src.push_str("use aq_engine::core::strategy::traits::BrokerAccess;\n");
    src.push_str("use aq_engine::core::broker::paper_broker::PaperBroker;\n");
    src.push_str("use aq_engine::core::broker::mt5_broker::Mt5Broker;\n");
    src.push_str("use aq_engine::core::broker::data_feeds::mt5::Mt5DataFeed;\n");
    src.push_str("use aq_engine::core::broker::data_feeds::yahoo::YahooFinanceDataFeed;\n");
    src.push_str("use aq_engine::core::broker::UnifiedBroker;\n");
    src.push_str("use aq_engine::core::broker::types::{Asset, BarData, AccountType};\n");
    src.push_str("use aq_engine::core::utils::timeframe::{TimeFrame, TimeFrameUnit};\n");
    src.push_str(
        "use aq_engine::core::insight::{Insight, InsightCollection, types::InsightState};\n",
    );
    src.push_str("use aq_engine::core::alpha::{AlphaModel, AlphaResult, AlphaModelBuilder};\n");
    src.push_str(
        "use aq_engine::core::pipeline::{InsightPipe, InsightPipeResult, InsightPipeBuilder};\n",
    );
    src.push_str("use aq_engine::core::lifecycle::{LifecycleTiming, OnStartLogicBuilder, OnInitLogicBuilder, OnTeardownLogicBuilder};\n");
    src.push_str("use aq_engine::core::universe::UniverseModelBuilder;\n");
    src.push_str("use std::collections::HashSet;\n");
    src.push_str("use chrono::Utc;\n");
    src.push_str("use log::{debug, info};\n");
    src.push_str("use uuid::Uuid;\n\n");

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

    if !custom_alphas.is_empty()
        || !custom_pipes.is_empty()
        || !custom_lifecycle_logic.is_empty()
        || !custom_universe_models.is_empty()
        || !built_in_pipes.is_empty()
        || !built_in_alphas.is_empty()
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

    // Register alpha models
    for (mod_name, struct_name, args, _) in &custom_alphas {
        src.push_str(&format!(
            "        ctx.add_alpha(AlphaModelBuilder::new(Box::new({}::{}::new({}))).build());\n",
            mod_name, struct_name, args
        ));
    }
    for (struct_name, args) in &built_in_alphas {
        src.push_str(&format!(
            "        ctx.add_alpha(AlphaModelBuilder::new(Box::new({}::new({}))).build());\n",
            struct_name, args
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
        "        info!(\"[{{}}] Strategy '{}' teardown complete\", Utc::now());\n",
        meta.name
    ));
    src.push_str("    }\n");
    src.push_str("}\n\n");

    // main function
    src.push_str("#[tokio::main]\n");
    src.push_str("async fn main() {\n");
    src.push_str("    let args: Vec<String> = std::env::args().collect();\n");
    src.push_str("    let is_live = args.contains(&\"--live\".to_string());\n\n");
    src.push_str(&format!(
        "    let log_level = \"{}\";\n",
        meta.config.log_level.to_lowercase()
    ));
    src.push_str("    let default_log_filter = format!(\"{},tracing::span=warn,turso=warn,libsql=warn\", log_level);\n");
    src.push_str(
        "    let env = env_logger::Env::default().default_filter_or(default_log_filter);\n",
    );
    src.push_str(
        "    let _ = env_logger::Builder::from_env(env).format_timestamp_millis().try_init();\n",
    );
    src.push_str("    info!(\"Logger initialised with level {}\", log_level);\n\n");

    // Timeframe
    src.push_str(&format!(
        "    let timeframe = TimeFrame::new({}, TimeFrameUnit::{});\n",
        meta.config.timeframe_amount, meta.config.timeframe_unit
    ));

    let broker_leverage = meta.config.broker_leverage.max(1);

    let data_feed_init = if meta.data_feed == crate::node::types::DataFeedType::Mt5 {
        "        let data_feed = Mt5DataFeed::from_env().unwrap_or_else(|error| {\n            eprintln!(\"Failed to initialise MT5 data feed: {:?}\", error);\n            std::process::exit(1);\n        });\n".to_string()
    } else {
        "        let data_feed = YahooFinanceDataFeed::new();\n".to_string()
    };

    let live_execution_init = if meta.broker == crate::node::types::ExecutionBrokerType::Mt5 {
        "        let execution = Mt5Broker::from_env().unwrap_or_else(|error| {\n            eprintln!(\"Failed to initialise MT5 broker: {:?}\", error);\n            std::process::exit(1);\n        });\n".to_string()
    } else {
        format!(
            "        let execution = PaperBroker::new(AccountType::Paper, {:.1}, {});\n",
            meta.config.starting_cash, broker_leverage
        )
    };

    let backtest_execution_init = format!(
        "        let execution = PaperBroker::new(AccountType::Paper, {:.1}, {});\n",
        meta.config.starting_cash, broker_leverage
    );

    let live_state_init = format!(
        "        let broker = UnifiedBroker::new(execution, data_feed);\n\n        let mut state = StrategyState::new(\n            \"{name}\".to_string(),\n            \"{version}\".to_string(),\n            {struct_name} {{}},\n            broker,\n            StrategyMode::Live,\n            timeframe,\n        );\n\n        state.strategy_id = Uuid::parse_str(\"{strategy_id}\").unwrap();\n\n",
        name = meta.name,
        version = meta.version,
        struct_name = strategy_name,
        strategy_id = meta.id,
    );

    let backtest_state_init = format!(
        "        let broker = UnifiedBroker::new(execution, data_feed);\n\n        let mut state = StrategyState::new(\n            \"{name}\".to_string(),\n            \"{version}\".to_string(),\n            {struct_name} {{}},\n            broker,\n            StrategyMode::Backtest,\n            timeframe,\n        );\n\n        state.strategy_id = Uuid::parse_str(\"{strategy_id}\").unwrap();\n\n",
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
    push_state_component_registrations(&mut src, &custom_lifecycle_logic);
    src.push_str("        let session_secret = args.iter().position(|a| a == \"--session-secret\").and_then(|i| args.get(i+1)).cloned().unwrap_or_default();\n");
    src.push_str("        let access_method = args.iter().position(|a| a == \"--access-method\").and_then(|i| args.get(i+1)).cloned().unwrap_or_else(|| \"aqe_live\".to_string());\n");
    src.push_str("        let strategy_id = args.iter().position(|a| a == \"--strategy-id\").and_then(|i| args.get(i+1)).cloned().unwrap_or_default();\n");
    src.push_str("        let user_id = args.iter().position(|a| a == \"--user-id\").and_then(|i| args.get(i+1)).cloned().unwrap_or_default();\n");
    src.push_str("        let session_id = args.iter().position(|a| a == \"--session-id\").and_then(|i| args.get(i+1)).cloned().unwrap_or_default();\n");
    src.push_str("        let node_id = args.iter().position(|a| a == \"--node-id\").and_then(|i| args.get(i+1)).cloned();\n");
    src.push_str("        let live_session_id = args.iter().position(|a| a == \"--live-session-id\").and_then(|i| args.get(i+1)).cloned();\n");
    src.push_str("        \n");
    src.push_str("        let auth = if !session_secret.is_empty() && !session_id.is_empty() && !strategy_id.is_empty() {\n");
    src.push_str("            Some(AqsAuth {\n");
    src.push_str("                access_method,\n");
    src.push_str("                session_id,\n");
    src.push_str("                session_secret,\n");
    src.push_str("                strategy_id,\n");
    src.push_str("                user_id,\n");
    src.push_str("                node_id,\n");
    src.push_str("                live_session_id,\n");
    src.push_str("                url: None,\n");
    src.push_str("            })\n");
    src.push_str("        } else {\n");
    src.push_str("            None\n");
    src.push_str("        };\n\n");
    src.push_str("        if let Err(e) = state.run_live(auth).await {\n");
    src.push_str("            eprintln!(\"Live execution failed: {:?}\", e);\n");
    src.push_str("            std::process::exit(1);\n");
    src.push_str("        }\n");
    src.push_str("    } else {\n");
    src.push_str(&data_feed_init);
    src.push_str(&backtest_execution_init);
    src.push_str(&backtest_state_init);
    push_state_component_registrations(&mut src, &custom_lifecycle_logic);
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
    src.push_str("        match state.run_backtest(start, end, timeframe).await {\n");
    src.push_str("            Ok(results) => {\n");
    src.push_str(
        "                info!(\"═══════════════════ Backtest Results ═══════════════════\");\n",
    );
    src.push_str("                results.print_metrics();\n");
    src.push_str("                info!(\"Insights generated: {}\", state.insights.len());\n");
    src.push_str("                info!(\"Insights: {:#?}\", state.insights.get_state_count());\n");
    src.push_str("                let run_id = Uuid::new_v4().to_string();\n");
    src.push_str("                let out_dir = std::env::current_dir().unwrap_or_default().join(\"backtests\").join(&run_id);\n");
    src.push_str("                if let Err(e) = results.save_to_disk_async(&out_dir, &*state.broker.backtest_state.as_ref().unwrap().read()).await {\n");
    src.push_str("                    eprintln!(\"Failed to save results to disk: {}\", e);\n");
    src.push_str("                } else {\n");
    src.push_str("                    if let Ok(abs_path) = std::fs::canonicalize(&out_dir) {\n");
    src.push_str(
        "                        println!(\"RESULTS_SAVED_TO: {}\", abs_path.display());\n",
    );
    src.push_str("                    } else {\n");
    src.push_str(
        "                        println!(\"RESULTS_SAVED_TO: {}\", out_dir.display());\n",
    );
    src.push_str("                    }\n");
    src.push_str("                }\n");
    src.push_str("            }\n");
    src.push_str("            Err(e) => {\n");
    src.push_str("                eprintln!(\"Backtest failed: {:?}\", e);\n");
    src.push_str("                std::process::exit(1);\n");
    src.push_str("            }\n");
    src.push_str("        }\n");
    src.push_str("    }\n");
    src.push_str("}\n");

    Ok(src)
}

/// Find all nodes reachable from the Strategy root node.
fn get_reachable_nodes(
    nodes: &[Node],
    connections: &[Connection],
) -> std::collections::HashSet<String> {
    let mut reachable = std::collections::HashSet::new();
    let mut queue = std::collections::VecDeque::new();

    // Find strategy root
    if let Some(root) = nodes.iter().find(|n| n.node_type == NodeType::Strategy) {
        reachable.insert(root.id.clone());
        queue.push_back(root.id.clone());
    }

    // Normal outward adjacency list.
    let mut adj: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();
    // Reverse adjacency for nested pipe composition (`child_pipe -> parent_pipe.insights_pipes`).
    let mut reverse_pipe_children: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for conn in connections {
        adj.entry(conn.from.node_id.clone())
            .or_default()
            .push(conn.to.node_id.clone());

        if conn.to.port == "insights_pipes" {
            reverse_pipe_children
                .entry(conn.to.node_id.clone())
                .or_default()
                .push(conn.from.node_id.clone());
        }
    }

    while let Some(curr) = queue.pop_front() {
        if let Some(neighbors) = adj.get(&curr) {
            for next in neighbors {
                if reachable.insert(next.clone()) {
                    queue.push_back(next.clone());
                }
            }
        }

        if let Some(children) = reverse_pipe_children.get(&curr) {
            for child in children {
                if reachable.insert(child.clone()) {
                    queue.push_back(child.clone());
                }
            }
        }
    }

    reachable
}

/// Topological sort of the node graph. Returns node IDs in execution order.
fn toposort(nodes: &[Node], connections: &[Connection]) -> Result<Vec<String>, String> {
    let mut in_degrees: HashMap<String, usize> = HashMap::new();
    let mut adj: HashMap<String, Vec<String>> = HashMap::new();

    for node in nodes {
        in_degrees.insert(node.id.clone(), 0);
        adj.insert(node.id.clone(), vec![]);
    }

    for conn in connections {
        if !in_degrees.contains_key(&conn.to.node_id) {
            continue;
        }
        *in_degrees.entry(conn.to.node_id.clone()).or_insert(0) += 1;
        adj.entry(conn.from.node_id.clone())
            .or_default()
            .push(conn.to.node_id.clone());
    }

    let mut queue = VecDeque::new();
    for node in nodes {
        if in_degrees.get(&node.id).copied().unwrap_or_default() == 0 {
            queue.push_back(node.id.clone());
        }
    }

    let mut sorted = Vec::new();
    while let Some(curr) = queue.pop_front() {
        sorted.push(curr.clone());
        if let Some(neighbors) = adj.get(&curr) {
            for next in neighbors {
                if let Some(deg) = in_degrees.get_mut(next) {
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(next.clone());
                    }
                }
            }
        }
    }

    if sorted.len() != nodes.len() {
        return Err("Cycle detected in strategy graph".to_string());
    }

    Ok(sorted)
}

/// Convert a source file path like `my_alpha.alpha.rs` to a Rust module name `my_alpha_alpha`.
fn source_file_to_mod_name(path: &str) -> String {
    let file_name = std::path::Path::new(path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown");
    // Strip .alpha or .pipe suffix if present
    let name = file_name
        .strip_suffix(".alpha")
        .or_else(|| file_name.strip_suffix(".pipe"))
        .or_else(|| file_name.strip_suffix(".logic"))
        .or_else(|| file_name.strip_suffix(".universe"))
        .unwrap_or(file_name);
    name.replace('-', "_").replace('.', "_")
}

fn push_custom_module_decl(src: &mut String, mod_name: &str, source_file: &str) {
    let module_path = source_file
        .strip_prefix("src/")
        .unwrap_or(source_file)
        .replace('\\', "/")
        .replace('"', "\\\"");
    src.push_str(&format!(
        "#[path = \"{}\"]\nmod {};\n",
        module_path, mod_name
    ));
}

/// Convert "my_alpha_name" or "My Alpha Name" to "MyAlphaName".
fn to_pascal_case(s: &str) -> String {
    s.split(|c: char| c == '_' || c == ' ' || c == '-')
        .filter(|w| !w.is_empty())
        .map(|w| {
            let mut chars = w.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().to_string() + &chars.collect::<String>(),
            }
        })
        .collect()
}

/// Identifies if a source file corresponds to a built-in pipe and returns `(mod_name, struct_name)`
fn get_builtin_pipe_info(src: &str, parsed_struct_name: &str) -> Option<(String, String)> {
    match src {
        "insight_submit" | "insight_submit.pipe.rs" => Some((
            "insight_submit".to_string(),
            "InsightSubmitPipe".to_string(),
        )),
        "allow_trading_window" => Some((
            "allow_trading_window".to_string(),
            "AllowTradingWindowPipe".to_string(),
        )),
        "and_pipe" => Some(("and_pipe".to_string(), "AndPipe".to_string())),
        "or_pipe" => Some(("or_pipe".to_string(), "OrPipe".to_string())),
        "cancel_opposite" => Some((
            "cancel_opposite".to_string(),
            "CancelOppositePipe".to_string(),
        )),
        "market_order_entry" => Some((
            "market_order_entry".to_string(),
            "MarketOrderEntryPipe".to_string(),
        )),
        "dynamic_quantity_to_risk" => Some((
            "dynamic_quantity_to_risk".to_string(),
            "DynamicQuantityToRiskPipe".to_string(),
        )),
        "full_account_quantity_to_risk" => Some((
            "full_account_quantity_to_risk".to_string(),
            "FullAccountQuantityToRiskPipe".to_string(),
        )),
        "minimum_risk_to_reward" => Some((
            "minimum_risk_to_reward".to_string(),
            "MinimumRiskToRewardPipe".to_string(),
        )),
        "reject_expired_insight" => Some((
            "reject_expired_insight".to_string(),
            "RejectExpiredInsightPipe".to_string(),
        )),
        "percentage_dca_levels" => Some((
            "percentage_dca_levels".to_string(),
            "PercentageDcaLevelsPipe".to_string(),
        )),
        "basic_stop_loss" => Some((
            "basic_stop_loss".to_string(),
            "BasicStopLossPipe".to_string(),
        )),
        "basic_take_profit" => Some((
            "basic_take_profit".to_string(),
            "BasicTakeProfitPipe".to_string(),
        )),
        "scale_out" => Some(("scale_out".to_string(), "ScaleOutPipe".to_string())),
        "close_market_changed" => Some((
            "close_market_changed".to_string(),
            "CloseMarketChangedPipe".to_string(),
        )),
        _ if src.starts_with("built_in") => {
            Some((source_file_to_mod_name(src), parsed_struct_name.to_string()))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{
        ConnectionEndpoint, InputType, NodeInput, NodeOutput, OutputType, StrategyMeta,
    };

    fn custom_node(
        id: &str,
        node_type: NodeType,
        label: &str,
        source_file: &str,
        inputs: Vec<NodeInput>,
        outputs: Vec<NodeOutput>,
    ) -> Node {
        Node {
            id: id.to_string(),
            node_type,
            label: label.to_string(),
            x: 0.0,
            y: 0.0,
            inputs,
            outputs,
            source_file: Some(source_file.to_string()),
            lifecycle_phase: None,
            lifecycle_timing: None,
            can_fail: None,
            undeletable: false,
        }
    }

    #[test]
    fn generated_backtest_save_does_not_inline_aqmeta_result_metadata() {
        let mut meta = StrategyMeta::new("Codegen Metadata Test");
        meta.nodes
            .push(StrategyMeta::create_strategy_node(&meta.name));

        let src = generate_main_rs(&meta).unwrap();

        assert!(src.contains("results.save_to_disk_async"));
        assert!(!src.contains("aqmeta_copied"));
        assert!(!src.contains("strategy.aqmeta"));
        assert!(!src.contains("\"run_config\""));
    }

    #[test]
    fn generated_lifecycle_logic_registers_and_runs_by_timing() {
        let mut meta = StrategyMeta::new("Lifecycle Test");
        meta.nodes
            .push(StrategyMeta::create_strategy_node(&meta.name));
        let mut before = custom_node(
            "before",
            NodeType::LogicBlock,
            "BeforeStart",
            "src/before_start.logic.rs",
            vec![NodeInput {
                name: "on_start".to_string(),
                input_type: InputType::OnStart,
                value: None,
                is_public: true,
                insight_state: None,
            }],
            vec![NodeOutput {
                name: "on_start".to_string(),
                output_type: OutputType::OnStart,
                insight_state: None,
            }],
        );
        before.lifecycle_phase = Some(LifecyclePhase::OnStart);
        before.lifecycle_timing = Some(LifecycleTiming::BeforeGenerated);
        before.can_fail = Some(false);

        let mut after = custom_node(
            "after",
            NodeType::LogicBlock,
            "AfterStart",
            "src/after_start.logic.rs",
            vec![NodeInput {
                name: "on_start".to_string(),
                input_type: InputType::OnStart,
                value: None,
                is_public: true,
                insight_state: None,
            }],
            vec![NodeOutput {
                name: "on_start".to_string(),
                output_type: OutputType::OnStart,
                insight_state: None,
            }],
        );
        after.lifecycle_phase = Some(LifecyclePhase::OnStart);
        after.lifecycle_timing = Some(LifecycleTiming::AfterGenerated);
        after.can_fail = Some(true);

        meta.nodes.push(before);
        meta.nodes.push(after);
        meta.connections.push(Connection {
            from: ConnectionEndpoint {
                node_id: "strategy_root".to_string(),
                port: "on_start".to_string(),
            },
            to: ConnectionEndpoint {
                node_id: "before".to_string(),
                port: "on_start".to_string(),
            },
        });
        meta.connections.push(Connection {
            from: ConnectionEndpoint {
                node_id: "before".to_string(),
                port: "on_start".to_string(),
            },
            to: ConnectionEndpoint {
                node_id: "after".to_string(),
                port: "on_start".to_string(),
            },
        });

        let src = generate_main_rs(&meta).unwrap();

        assert!(src.contains("state.add_on_start_logic"));
        assert!(src.contains("OnStartLogicBuilder::new"));
        assert!(src.contains("LifecycleTiming::BeforeGenerated"));
        assert!(src.contains("LifecycleTiming::AfterGenerated"));
        assert!(src.contains(".can_fail(false)"));
        assert!(src.contains(".can_fail(true)"));
        assert!(!src.contains("ctx.run_on_start_logic"));
        assert!(
            src.find("state.add_on_start_logic").unwrap() < src.find("state.run_backtest").unwrap()
        );
    }

    #[test]
    fn generated_universe_models_register_in_on_start_and_keep_static_universe() {
        let mut meta = StrategyMeta::new("Universe Test");
        meta.nodes
            .push(StrategyMeta::create_strategy_node(&meta.name));
        let mut universe_model = custom_node(
            "universe_model",
            NodeType::UniverseModel,
            "CustomUniverse",
            "src/custom_universe.universe.rs",
            vec![NodeInput {
                name: "strategy.universe".to_string(),
                input_type: InputType::Universe,
                value: None,
                is_public: true,
                insight_state: None,
            }],
            vec![NodeOutput {
                name: "universe".to_string(),
                output_type: OutputType::Universe,
                insight_state: None,
            }],
        );
        universe_model.can_fail = Some(false);
        meta.nodes.push(universe_model);
        meta.connections.push(Connection {
            from: ConnectionEndpoint {
                node_id: "strategy_root".to_string(),
                port: "universe".to_string(),
            },
            to: ConnectionEndpoint {
                node_id: "universe_model".to_string(),
                port: "strategy.universe".to_string(),
            },
        });

        let src = generate_main_rs(&meta).unwrap();

        assert!(src.contains("ctx.add_universe_model"));
        assert!(src.contains("UniverseModelBuilder::new"));
        assert!(!src.contains("ctx.run_universe_models"));
        assert!(src.contains("HashSet::new() // No symbols configured"));
        assert!(src.find("ctx.add_universe_model").unwrap() < src.find("fn universe").unwrap());
    }
}
