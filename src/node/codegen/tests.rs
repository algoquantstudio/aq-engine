use super::*;
use crate::node::{
    ConnectionEndpoint, InputType, InsightState, LifecycleTiming, NodeInput, NodeOutput,
    OutputType, StrategyMeta,
};

fn alpha_meta() -> StrategyMeta {
    let mut meta = StrategyMeta::new("Builtin Alpha Controls");
    meta.nodes.push(StrategyMeta::create_strategy_node(&meta.name));
    for node in crate::node::builtins::get_builtin_nodes().into_iter().filter(|n| n.node_type == NodeType::Alpha) {
        meta.connections.push(Connection {
            from: ConnectionEndpoint { node_id: "strategy_root".into(), port: "on_bar".into() },
            to: ConnectionEndpoint { node_id: node.id.clone(), port: "on_bar".into() },
        });
        meta.nodes.push(node);
    }
    meta
}

#[test]
fn builtin_alpha_codegen_preserves_legacy_constructors_and_ignores_saved_input_order() {
    let mut meta = alpha_meta();
    for node in meta.nodes.iter_mut().filter(|n| n.node_type == NodeType::Alpha) {
        node.inputs.retain(|i| ["on_bar", "atr_period", "ema_period", "rsi_period", "local_window", "divergance_window", "base_confidence_modifier_field", "max_spawn", "limit_entries"].contains(&i.name.as_str()));
        node.inputs.reverse();
    }
    let source = generate_main_rs(&meta).unwrap();
    for constructor in ["TestEntry::new(14, false, 1, \"\".to_string())", "EmaPriceCrossover::new(14, 14, \"\".to_string())", "RsiDiverganceAlpha::new(36, 50, 14, 14, \"\".to_string())"] {
        assert!(source.contains(constructor), "{constructor}");
    }
    assert_eq!(source.matches(".with_options(").count(), 3);
    assert!(source.contains("take_profit_atr: 2.0"));
    assert!(source.contains("take_profit_atr: 3.5"));
}

#[test]
fn builtin_alpha_codegen_accepts_custom_and_swept_controls() {
    let mut meta = alpha_meta();
    meta.hyper_parameters.push(crate::node::HyperParameterDefinition {
        name: "cooldown".into(), value_type: crate::node::HyperParameterType::Int,
        values: vec![serde_json::json!(2),serde_json::json!(5)], min: None, max: None, increment: None,
    });
    for node in meta.nodes.iter_mut().filter(|n| n.node_type == NodeType::Alpha) {
        node.label = "User-friendly display name".into();
        node.inputs.iter_mut().find(|i| i.name == "allow_short").unwrap().value = Some(serde_json::json!(false));
        node.inputs.iter_mut().find(|i| i.name == "cooldown_bars").unwrap().hyper_reference = Some("cooldown".into());
    }
    let source = generate_main_rs(&meta).unwrap();
    assert_eq!(source.matches("allow_short: false").count(),3);
    assert_eq!(source.matches("cooldown_bars: hyper_int(ctx, \"cooldown\", 0)").count(),3);
    assert!(!source.contains("UserFriendlyDisplayName::new"));
    assert!(source.contains(".with_signal_options(1.0, 1.0)"));
    assert!(source.contains(".with_divergence_options(0.0, true)"));
}

#[test]
fn builtin_alpha_codegen_rejects_invalid_values_before_compilation() {
    for (field,value) in [("atr_period", serde_json::json!(-1)), ("atr_period",serde_json::json!(0)),
        ("stop_loss_atr",serde_json::json!(0)), ("period_unfilled",serde_json::json!(4294967296u64)),
        ("allow_short",serde_json::json!("false"))] {
        let mut meta = alpha_meta();
        meta.nodes.iter_mut().find(|n| n.node_type == NodeType::Alpha).unwrap().inputs.iter_mut()
            .find(|i| i.name == field).unwrap().value = Some(value);
        assert!(generate_main_rs(&meta).unwrap_err().contains(field));
    }
}

#[test]
fn same_class_alpha_codegen_does_not_add_instance_metadata() {
    let mut meta = StrategyMeta::new("Generic Identity");
    meta.nodes.push(StrategyMeta::create_strategy_node(&meta.name));
    for (id, source) in [("custom-a", "src/shared_alpha.rs"), ("custom-b\"\\id", "src/shared_alpha.rs"), ("builtin-a", "built_in/test_entry")] {
        meta.nodes.push(custom_node(id, NodeType::Alpha, "Shared Alpha", source, vec![], vec![]));
        meta.connections.push(Connection {
            from: ConnectionEndpoint { node_id: "strategy_root".into(), port: "on_bar".into() },
            to: ConnectionEndpoint { node_id: id.into(), port: "on_bar".into() },
        });
    }
    let source = generate_main_rs(&meta).unwrap();
    assert!(!source.contains(".node_id("));
    assert_eq!(source.matches("ctx.add_alpha(").count(), 3);
    assert_eq!(source.matches("mod shared_alpha;").count(), 1);
}

#[test]
fn csv_codegen_uses_selected_provider_and_blocks_live_before_broker_initialization() {
    let mut meta = StrategyMeta::new("CSV Codegen Test");
    meta.data_feed = crate::node::DataFeedType::Csv;
    meta.nodes.push(StrategyMeta::create_strategy_node(&meta.name));
    let source = generate_main_rs(&meta).unwrap();
    assert!(source.contains("CsvDataFeed::from_env()"));
    assert!(!source.contains("YahooFinanceDataFeed::new()"));
    assert!(source.find("CSV is a historical-only data feed").unwrap() < source.find("CsvDataFeed::from_env()").unwrap());
    let json = serde_json::to_string(&meta).unwrap();
    let restored: StrategyMeta = serde_json::from_str(&json).unwrap();
    assert_eq!(restored.data_feed, crate::node::DataFeedType::Csv);
}

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
fn generated_backtest_save_is_engine_managed() {
    let mut meta = StrategyMeta::new("Codegen Metadata Test");
    meta.nodes
        .push(StrategyMeta::create_strategy_node(&meta.name));

    let src = generate_main_rs(&meta).unwrap();

    assert!(src.contains("use aq_engine::prelude::*;"));
    assert!(!src.contains("use aq_engine::core::alpha::{"));
    assert!(!src.contains("use aq_engine::core::pipeline::{"));
    assert!(src.contains("set_logging_level"));
    assert!(src.contains("teardown callback finished; broker settlement is still managed by AQE"));
    assert!(!src.contains("teardown complete"));
    assert!(src.contains("state.run_live(None).await"));
    assert!(src.contains("state.run_backtest(start, end, timeframe).await"));
    assert!(src.contains("state.set_artifact_root(env!(\"CARGO_MANIFEST_DIR\"));"));
    assert!(!src.contains("session_secret"));
    assert!(!src.contains("AqsAuth"));
    assert!(!src.contains("results.save_to_disk_async"));
    assert!(!src.contains("RESULTS_SAVED_TO"));
    assert!(!src.contains("aqmeta_copied"));
    assert!(!src.contains("strategy.aqmeta"));
    assert!(!src.contains("\"run_config\""));
    assert!(!src.contains("HyperParameterConfig"));
    assert!(!src.contains("--hyper-sweep"));
}

#[test]
fn generated_hyperparameter_runtime_is_engine_managed() {
    let mut meta = StrategyMeta::new("Hyperparameter Codegen Test");
    meta.nodes
        .push(StrategyMeta::create_strategy_node(&meta.name));
    meta.hyper_parameters
        .push(crate::node::HyperParameterDefinition {
            name: "period".to_string(),
            value_type: crate::node::HyperParameterType::Int,
            values: vec![serde_json::json!(7), serde_json::json!(14)],
            min: None,
            max: None,
            increment: None,
        });

    let src = generate_main_rs(&meta).unwrap();

    assert!(src.contains("HyperParameterConfig"));
    assert!(src.contains("state.set_hyper_parameter_config(hyperparameters.clone())"));
    assert!(src.contains("state.set_hyper_parameter_run(selection, is_hyper_sweep)"));
    assert!(!src.contains("\"runs\":"));
    assert!(!src.contains("hyperparameters::{"));
    assert!(!src.contains("fn hyper_values("));
    assert!(!src.contains("fn hyper_int<"));
    assert!(!src.contains("set_backtest_result_dir("));
    assert!(!src.contains("metrics.json"));
    assert!(!src.contains("__HYPER__\").to_string(), values"));
}

#[test]
fn generated_percentage_risk_to_quantity_uses_builtin_pipe() {
    let mut meta = StrategyMeta::new("Percentage Risk Pipe Test");
    meta.nodes
        .push(StrategyMeta::create_strategy_node(&meta.name));
    meta.nodes.push(custom_node(
        "percentage-risk",
        NodeType::Pipe,
        "percentage_risk_to_quantity",
        "percentage_risk_to_quantity",
        vec![
            NodeInput {
                name: "insights".to_string(),
                input_type: InputType::Insights,
                value: None,
                is_public: true,
                insight_state: Some(InsightState::New),
                hyper_reference: None,
            },
            NodeInput {
                name: "risk_percent".to_string(),
                input_type: InputType::Int,
                value: Some(serde_json::json!(2)),
                is_public: true,
                insight_state: None,
                hyper_reference: None,
            },
        ],
        vec![
            NodeOutput {
                name: "insights_out".to_string(),
                output_type: OutputType::Insights,
                insight_state: Some(InsightState::New),
                hyper_reference: None,
            },
            NodeOutput {
                name: "pipe_result".to_string(),
                output_type: OutputType::InsightPipeResult,
                insight_state: None,
                hyper_reference: None,
            },
            NodeOutput {
                name: "pipe_instance".to_string(),
                output_type: OutputType::InsightPipeInstance,
                insight_state: None,
                hyper_reference: None,
            },
        ],
    ));
    meta.connections.push(Connection {
        from: ConnectionEndpoint {
            node_id: "strategy_root".to_string(),
            port: "insight_pipeline".to_string(),
        },
        to: ConnectionEndpoint {
            node_id: "percentage-risk".to_string(),
            port: "insights".to_string(),
        },
    });

    let src = generate_main_rs(&meta).unwrap();

    assert!(src.contains(
        "use aq_engine::core::pipeline::percentage_risk_to_quantity::PercentageRiskToQuantityPipe;"
    ));
    assert!(src.contains("PercentageRiskToQuantityPipe::new(2)"));
    assert!(src.contains(".target_state(InsightState::New)"));
}

#[test]
fn generated_allow_trading_window_uses_compact_builtin_configuration() {
    let mut meta = StrategyMeta::new("Trading Window Pipe Test");
    meta.nodes
        .push(StrategyMeta::create_strategy_node(&meta.name));
    meta.nodes.push(custom_node(
        "trading-window",
        NodeType::Pipe,
        "allow_trading_window",
        "allow_trading_window",
        vec![
            NodeInput {
                name: "insights".to_string(),
                input_type: InputType::Insights,
                value: None,
                is_public: true,
                insight_state: Some(InsightState::New),
                hyper_reference: None,
            },
            NodeInput {
                name: "start_utc".to_string(),
                input_type: InputType::Str,
                value: Some(serde_json::json!("06:00")),
                is_public: true,
                insight_state: None,
                hyper_reference: None,
            },
            NodeInput {
                name: "end_utc".to_string(),
                input_type: InputType::Str,
                value: Some(serde_json::json!("20:00")),
                is_public: true,
                insight_state: None,
                hyper_reference: None,
            },
            NodeInput {
                name: "allowed_days".to_string(),
                input_type: InputType::Str,
                value: Some(serde_json::json!("0,1,2,3,4,5,6")),
                is_public: true,
                insight_state: None,
                hyper_reference: None,
            },
        ],
        Vec::new(),
    ));
    meta.connections.push(Connection {
        from: ConnectionEndpoint {
            node_id: "strategy_root".to_string(),
            port: "insight_pipeline".to_string(),
        },
        to: ConnectionEndpoint {
            node_id: "trading-window".to_string(),
            port: "insights".to_string(),
        },
    });

    let src = generate_main_rs(&meta).unwrap();

    assert!(
        src.contains(
            "use aq_engine::core::pipeline::allow_trading_window::AllowTradingWindowPipe;"
        )
    );
    assert!(src.contains(
            "AllowTradingWindowPipe::new(\"06:00\".to_string(), \"20:00\".to_string(), \"0,1,2,3,4,5,6\".to_string())"
        ));
    assert!(!src.contains("mod allow_trading_window;"));
}

#[test]
fn generated_end_of_day_close_uses_builtin_pipe() {
    let mut meta = StrategyMeta::new("End Of Day Close Pipe Test");
    meta.nodes
        .push(StrategyMeta::create_strategy_node(&meta.name));
    meta.nodes.push(custom_node(
        "eod-close",
        NodeType::Pipe,
        "end_of_day_close",
        "end_of_day_close",
        vec![
            NodeInput {
                name: "insights".to_string(),
                input_type: InputType::Insights,
                value: None,
                is_public: true,
                insight_state: Some(InsightState::Filled),
                hyper_reference: None,
            },
            NodeInput {
                name: "minutes_before_end_of_day".to_string(),
                input_type: InputType::Int,
                value: Some(serde_json::json!(20)),
                is_public: true,
                insight_state: None,
                hyper_reference: None,
            },
            NodeInput {
                name: "end_of_day_time_utc".to_string(),
                input_type: InputType::Str,
                value: Some(serde_json::json!("23:30")),
                is_public: true,
                insight_state: None,
                hyper_reference: None,
            },
        ],
        vec![
            NodeOutput {
                name: "insights_out".to_string(),
                output_type: OutputType::Insights,
                insight_state: Some(InsightState::Filled),
                hyper_reference: None,
            },
            NodeOutput {
                name: "pipe_result".to_string(),
                output_type: OutputType::InsightPipeResult,
                insight_state: None,
                hyper_reference: None,
            },
            NodeOutput {
                name: "pipe_instance".to_string(),
                output_type: OutputType::InsightPipeInstance,
                insight_state: None,
                hyper_reference: None,
            },
        ],
    ));
    meta.connections.push(Connection {
        from: ConnectionEndpoint {
            node_id: "strategy_root".to_string(),
            port: "insight_pipeline".to_string(),
        },
        to: ConnectionEndpoint {
            node_id: "eod-close".to_string(),
            port: "insights".to_string(),
        },
    });

    let src = generate_main_rs(&meta).unwrap();

    assert!(src.contains("use aq_engine::core::pipeline::end_of_day_close::EndOfDayClosePipe;"));
    assert!(src.contains("EndOfDayClosePipe::new(20, \"23:30\".to_string())"));
    assert!(src.contains(".target_state(InsightState::Filled)"));
}

#[test]
fn generated_insight_ttl_config_uses_builtin_pipe() {
    let mut meta = StrategyMeta::new("Insight TTL Config Pipe Test");
    meta.nodes
        .push(StrategyMeta::create_strategy_node(&meta.name));
    meta.nodes.push(custom_node(
        "ttl-config",
        NodeType::Pipe,
        "insight_ttl_config",
        "insight_ttl_config",
        vec![
            NodeInput {
                name: "insights".to_string(),
                input_type: InputType::Insights,
                value: None,
                is_public: true,
                insight_state: Some(InsightState::New),
                hyper_reference: None,
            },
            NodeInput {
                name: "period_unfilled".to_string(),
                input_type: InputType::Int,
                value: Some(serde_json::json!(5)),
                is_public: true,
                insight_state: None,
                hyper_reference: None,
            },
            NodeInput {
                name: "period_till_tp".to_string(),
                input_type: InputType::Int,
                value: Some(serde_json::json!(10)),
                is_public: true,
                insight_state: None,
                hyper_reference: None,
            },
            NodeInput {
                name: "insight_scope".to_string(),
                input_type: InputType::Str,
                value: Some(serde_json::json!("Both")),
                is_public: true,
                insight_state: None,
                hyper_reference: None,
            },
        ],
        vec![
            NodeOutput {
                name: "insights_out".to_string(),
                output_type: OutputType::Insights,
                insight_state: Some(InsightState::New),
                hyper_reference: None,
            },
            NodeOutput {
                name: "pipe_result".to_string(),
                output_type: OutputType::InsightPipeResult,
                insight_state: None,
                hyper_reference: None,
            },
            NodeOutput {
                name: "pipe_instance".to_string(),
                output_type: OutputType::InsightPipeInstance,
                insight_state: None,
                hyper_reference: None,
            },
        ],
    ));
    meta.connections.push(Connection {
        from: ConnectionEndpoint {
            node_id: "strategy_root".to_string(),
            port: "insight_pipeline".to_string(),
        },
        to: ConnectionEndpoint {
            node_id: "ttl-config".to_string(),
            port: "insights".to_string(),
        },
    });

    let src = generate_main_rs(&meta).unwrap();

    assert!(
        src.contains("use aq_engine::core::pipeline::insight_ttl_config::InsightTtlConfigPipe;")
    );
    assert!(src.contains("InsightTtlConfigPipe::new(5, 10)"));
    assert!(src.contains(".insight_scope(InsightPipeScope::Both)"));
    assert!(src.contains(".target_state(InsightState::New)"));
}

#[test]
fn generated_reject_expired_insight_defaults_to_parent_only_scope() {
    let mut meta = StrategyMeta::new("Reject Expired Insight Pipe Test");
    meta.nodes
        .push(StrategyMeta::create_strategy_node(&meta.name));
    meta.nodes.push(custom_node(
        "reject-expired",
        NodeType::Pipe,
        "reject_expired_insight",
        "reject_expired_insight",
        vec![NodeInput {
            name: "insights".to_string(),
            input_type: InputType::Insights,
            value: None,
            is_public: true,
            insight_state: Some(InsightState::Executed),
            hyper_reference: None,
        }],
        vec![
            NodeOutput {
                name: "insights_out".to_string(),
                output_type: OutputType::Insights,
                insight_state: Some(InsightState::Cancelled),
                hyper_reference: None,
            },
            NodeOutput {
                name: "pipe_result".to_string(),
                output_type: OutputType::InsightPipeResult,
                insight_state: None,
                hyper_reference: None,
            },
            NodeOutput {
                name: "pipe_instance".to_string(),
                output_type: OutputType::InsightPipeInstance,
                insight_state: None,
                hyper_reference: None,
            },
        ],
    ));
    meta.connections.push(Connection {
        from: ConnectionEndpoint {
            node_id: "strategy_root".to_string(),
            port: "insight_pipeline".to_string(),
        },
        to: ConnectionEndpoint {
            node_id: "reject-expired".to_string(),
            port: "insights".to_string(),
        },
    });

    let src = generate_main_rs(&meta).unwrap();

    assert!(src.contains(
        "use aq_engine::core::pipeline::reject_expired_insight::RejectExpiredInsightPipe;"
    ));
    assert!(src.contains("RejectExpiredInsightPipe::new()"));
    assert!(src.contains(".insight_scope(InsightPipeScope::ParentOnly)"));
    assert!(src.contains(".target_state(InsightState::Executed)"));
}

#[test]
fn generated_custom_pipe_uses_generic_child_only_scope() {
    let mut meta = StrategyMeta::new("Custom Scoped Pipe Test");
    meta.nodes
        .push(StrategyMeta::create_strategy_node(&meta.name));
    meta.nodes.push(custom_node(
        "custom-scope",
        NodeType::Pipe,
        "ScopeAware",
        "src/scope_aware.pipe.rs",
        vec![
            NodeInput {
                name: "insights".to_string(),
                input_type: InputType::Insights,
                value: None,
                is_public: true,
                insight_state: Some(InsightState::Filled),
                hyper_reference: None,
            },
            NodeInput {
                name: "insight_scope".to_string(),
                input_type: InputType::Str,
                value: Some(serde_json::json!("ChildOnly")),
                is_public: true,
                insight_state: None,
                hyper_reference: None,
            },
        ],
        vec![NodeOutput {
            name: "insights_out".to_string(),
            output_type: OutputType::Insights,
            insight_state: Some(InsightState::Filled),
            hyper_reference: None,
        }],
    ));
    meta.connections.push(Connection {
        from: ConnectionEndpoint {
            node_id: "strategy_root".to_string(),
            port: "insight_pipeline".to_string(),
        },
        to: ConnectionEndpoint {
            node_id: "custom-scope".to_string(),
            port: "insights".to_string(),
        },
    });

    let src = generate_main_rs(&meta).unwrap();

    assert!(src.contains("scope_aware::ScopeAware::new()"));
    assert!(src.contains(".insight_scope(InsightPipeScope::ChildOnly)"));
    assert!(!src.contains("ScopeAware::new(\"ChildOnly\".to_string())"));
}

#[test]
fn generated_builtin_lifecycle_logic_uses_engine_imports() {
    let mut meta = StrategyMeta::new("Built In Lifecycle Logic Test");
    meta.nodes
        .push(StrategyMeta::create_strategy_node(&meta.name));

    let mut preseed = custom_node(
        "preseed",
        NodeType::LogicBlock,
        "preseed_warmup_history",
        "preseed_warmup_history",
        vec![NodeInput {
            name: "init".to_string(),
            input_type: InputType::Init,
            value: None,
            is_public: true,
            insight_state: None,
            hyper_reference: None,
        }],
        vec![NodeOutput {
            name: "init".to_string(),
            output_type: OutputType::Init,
            insight_state: None,
            hyper_reference: None,
        }],
    );
    preseed.lifecycle_phase = Some(LifecyclePhase::OnInit);
    preseed.lifecycle_timing = Some(LifecycleTiming::BeforeGenerated);
    preseed.can_fail = Some(false);

    let mut cleanup = custom_node(
        "cleanup",
        NodeType::LogicBlock,
        "close_all_filled_positions",
        "close_all_filled_positions",
        vec![NodeInput {
            name: "on_teardown".to_string(),
            input_type: InputType::OnTeardown,
            value: None,
            is_public: true,
            insight_state: None,
            hyper_reference: None,
        }],
        vec![NodeOutput {
            name: "on_teardown".to_string(),
            output_type: OutputType::OnTeardown,
            insight_state: None,
            hyper_reference: None,
        }],
    );
    cleanup.lifecycle_phase = Some(LifecyclePhase::OnTeardown);
    cleanup.lifecycle_timing = Some(LifecycleTiming::BeforeGenerated);
    cleanup.can_fail = Some(true);

    meta.nodes.push(preseed);
    meta.nodes.push(cleanup);
    meta.connections.push(Connection {
        from: ConnectionEndpoint {
            node_id: "strategy_root".to_string(),
            port: "init".to_string(),
        },
        to: ConnectionEndpoint {
            node_id: "preseed".to_string(),
            port: "init".to_string(),
        },
    });
    meta.connections.push(Connection {
        from: ConnectionEndpoint {
            node_id: "strategy_root".to_string(),
            port: "on_teardown".to_string(),
        },
        to: ConnectionEndpoint {
            node_id: "cleanup".to_string(),
            port: "on_teardown".to_string(),
        },
    });

    let src = generate_main_rs(&meta).unwrap();

    assert!(
        src.contains(
            "use aq_engine::core::lifecycle::preseed_warmup_history::PreseedWarmupHistory;"
        )
    );
    assert!(src.contains(
        "use aq_engine::core::lifecycle::close_all_filled_positions::CloseAllFilledPositions;"
    ));
    assert!(src.contains(
            "state.add_on_init_logic(OnInitLogicBuilder::new(Box::new(PreseedWarmupHistory::new())).timing(LifecycleTiming::BeforeGenerated).can_fail(false).build());"
        ));
    assert!(src.contains(
            "state.add_on_teardown_logic(OnTeardownLogicBuilder::new(Box::new(CloseAllFilledPositions::new())).timing(LifecycleTiming::BeforeGenerated).can_fail(true).build());"
        ));
    assert!(!src.contains("mod preseed_warmup_history;"));
    assert!(!src.contains("mod close_all_filled_positions;"));
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
            hyper_reference: None,
        }],
        vec![NodeOutput {
            name: "on_start".to_string(),
            output_type: OutputType::OnStart,
            insight_state: None,
            hyper_reference: None,
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
            hyper_reference: None,
        }],
        vec![NodeOutput {
            name: "on_start".to_string(),
            output_type: OutputType::OnStart,
            insight_state: None,
            hyper_reference: None,
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
fn generated_add_event_node_registers_feature_timeframe() {
    let mut meta = StrategyMeta::new("Add Event Test");
    meta.nodes
        .push(StrategyMeta::create_strategy_node(&meta.name));
    meta.nodes.push(custom_node(
        "add-event",
        NodeType::Trigger,
        "Add Event",
        "add_event",
        vec![
            NodeInput {
                name: "on_start".to_string(),
                input_type: InputType::OnStart,
                value: None,
                is_public: true,
                insight_state: None,
                hyper_reference: None,
            },
            NodeInput {
                name: "event_type".to_string(),
                input_type: InputType::Str,
                value: Some(serde_json::json!("Bar")),
                is_public: true,
                insight_state: None,
                hyper_reference: None,
            },
            NodeInput {
                name: "timeframe_amount".to_string(),
                input_type: InputType::Int,
                value: Some(serde_json::json!(15)),
                is_public: true,
                insight_state: None,
                hyper_reference: None,
            },
            NodeInput {
                name: "timeframe_unit".to_string(),
                input_type: InputType::Str,
                value: Some(serde_json::json!("Minute")),
                is_public: true,
                insight_state: None,
                hyper_reference: None,
            },
            NodeInput {
                name: "allow_trading".to_string(),
                input_type: InputType::Bool,
                value: Some(serde_json::json!(true)),
                is_public: true,
                insight_state: None,
                hyper_reference: None,
            },
        ],
        vec![NodeOutput {
            name: "on_start".to_string(),
            output_type: OutputType::OnStart,
            insight_state: None,
            hyper_reference: None,
        }],
    ));
    meta.connections.push(Connection {
        from: ConnectionEndpoint {
            node_id: "strategy_root".to_string(),
            port: "on_start".to_string(),
        },
        to: ConnectionEndpoint {
            node_id: "add-event".to_string(),
            port: "on_start".to_string(),
        },
    });

    let src = generate_main_rs(&meta).unwrap();

    assert!(src.contains("EventStreamRequest"));
    assert!(src.contains("EventStreamType::Bar"));
    assert!(src.contains(
            "ctx.add_events_with_options(EventStreamRequest::new(EventStreamType::Bar, Some(TimeFrame::new(15, TimeFrameUnit::Minute))).allow_trading(true));"
        ));
    assert!(
        src.find("ctx.add_events_with_options").unwrap()
            < src
                .find("ctx.add_alpha")
                .unwrap_or(src.find("fn init").unwrap())
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
            hyper_reference: None,
        }],
        vec![NodeOutput {
            name: "universe".to_string(),
            output_type: OutputType::Universe,
            insight_state: None,
            hyper_reference: None,
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
