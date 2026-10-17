//! Offline regressions for built-in routing, sizing, and entry safety.
use super::*;
use crate::core::{
    alpha::{AlphaModel, TestEntry},
    broker::types::OrderSide,
    insight::types::StrategyType,
    utils::quantity_sizing::{
        normalised_percent_rate, round_whole_or_fractional_quantity,
        tests::{MockContext, sample_asset, sample_insight},
    },
};
use polars::prelude::*;

fn fractional_context() -> MockContext {
    let mut asset = sample_asset();
    asset.min_order_size = Some(0.0001);
    asset.quantity_base = Some(4);
    MockContext::new(asset)
}

#[test]
fn builtin_testing_routes_only_to_its_configured_alpha_family() {
    let unrelated = InsightPipeBuilder::new(Box::new(
        percentage_risk_to_quantity::PercentageRiskToQuantityPipe::new(2),
    ))
    .allowed_alphas(HashSet::from([
        "EmaPriceCrossover".into(),
        "RsiDiverganceAlpha".into(),
    ]))
    .insight_scope(InsightPipeScope::Both)
    .build();
    let testing = InsightPipeBuilder::new(Box::new(
        dynamic_quantity_to_risk::DynamicQuantityToRiskPipe::new(200_000.0, 1_000.0),
    ))
    .allowed_alphas(HashSet::from(["TestEntry".into()]))
    .insight_scope(InsightPipeScope::Both)
    .build();
    let mut insight = sample_insight();
    assert!(!unrelated.should_run(&insight));
    assert!(testing.should_run(&insight));
    insight.strategy_type = StrategyType::Custom("Testing-CHILD-CHILD".into());
    assert!(testing.should_run(&insight));
    assert!(!unrelated.should_run(&insight));
    for kind in [StrategyType::Manual, StrategyType::Portfolio] {
        insight.strategy_type = kind;
        assert!(!unrelated.should_run(&insight));
        assert!(!testing.should_run(&insight));
    }
}

#[test]
fn builtin_one_percent_means_one_percent() {
    assert_eq!(normalised_percent_rate(1, 2), 0.01);
}

#[test]
fn builtin_fractional_sizing_preserves_precision_without_rounding_up() {
    let ctx = fractional_context();
    let quantity = round_whole_or_fractional_quantity(&ctx, "BTCUSD", 1.23456, true).unwrap();
    assert!((quantity - 1.2345).abs() < 1e-10);
    assert_eq!(
        round_whole_or_fractional_quantity(&ctx, "BTCUSD", 1.0, false).unwrap(),
        1.0
    );
}

#[test]
fn builtin_tight_stop_cannot_exceed_buying_power() {
    let mut ctx = fractional_context();
    ctx.account.equity = 100_361.633;
    ctx.account.buying_power = 1_003_616.33;
    let mut insight = sample_insight();
    insight
        .set_limit_price(Some(84_065.0823))
        .set_stop_loss(Some(84_060.32));
    let result = percentage_risk_to_quantity::PercentageRiskToQuantityPipe::new(2)
        .run(&mut ctx, &mut insight);
    assert!(result.passed, "{result:?}");
    assert!(insight.total_insight_quantity_size() * 84_065.0823 <= ctx.account.buying_power);
}

#[test]
fn builtin_dynamic_rejects_wrong_side_stop() {
    let mut ctx = fractional_context();
    let mut insight = sample_insight();
    insight.set_stop_loss(Some(110.0));
    let result = dynamic_quantity_to_risk::DynamicQuantityToRiskPipe::new(1_000.0, 0.0)
        .run(&mut ctx, &mut insight);
    assert!(!result.passed);
    assert!(insight.quantity.is_none());
}

#[test]
fn builtin_market_entry_does_not_replace_stop_entry() {
    let mut ctx = fractional_context();
    let mut insight = sample_insight();
    insight.set_limit_price(None).set_stop_price(Some(110.0));
    ctx.history
        .insert("BTCUSD".into(), df!("close" => [100.0]).unwrap());
    market_order_entry::MarketOrderEntryPipe::new().run(&mut ctx, &mut insight);
    assert_eq!(insight.limit_price, None);
    assert_eq!(insight.stop_price, Some(110.0));
}

#[test]
fn builtin_market_entry_rejects_non_finite_close() {
    let mut ctx = fractional_context();
    let mut insight = sample_insight();
    insight.set_limit_price(None);
    ctx.history
        .insert("BTCUSD".into(), df!("close" => [f64::NAN]).unwrap());
    let result = market_order_entry::MarketOrderEntryPipe::new().run(&mut ctx, &mut insight);
    assert!(!result.passed);
    assert!(insight.limit_price.is_none());
}

#[test]
fn builtin_test_entry_limits_active_roots_per_symbol() {
    let mut ctx = fractional_context();
    ctx.history.insert(
        "BTCUSD".into(),
        df!("open" => [99.0, 99.0], "close" => [100.0, 100.0], "ATRr_2" => [1.0, 1.0]).unwrap(),
    );
    let mut alpha = TestEntry::new(2, false, 2, String::new());
    let mut ids = Vec::new();
    for _ in 0..2 {
        let insight = alpha.generate_insights(&mut ctx, "BTCUSD").insight.unwrap();
        ids.push(insight.insight_id);
        ctx.add_insight(insight);
    }
    for _ in 0..20 {
        assert!(
            alpha
                .generate_insights(&mut ctx, "BTCUSD")
                .insight
                .is_none()
        );
    }
    let mut finished = ctx.insights.take_for_update(&ids[0]).unwrap();
    finished.order_rejected("Test: capacity released");
    ctx.insights.restore_after_update(finished, true);
    assert!(
        alpha
            .generate_insights(&mut ctx, "BTCUSD")
            .insight
            .is_some()
    );
}

#[test]
fn builtin_test_entry_skips_flat_or_invalid_atr() {
    for atr in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        let mut ctx = fractional_context();
        ctx.history.insert(
            "BTCUSD".into(),
            df!("open" => [99.0, 99.0], "close" => [100.0, 100.0], "ATRr_2" => [atr, atr]).unwrap(),
        );
        assert!(
            TestEntry::new(2, false, 2, String::new())
                .generate_insights(&mut ctx, "BTCUSD")
                .insight
                .is_none()
        );
    }
}

#[test]
fn builtin_reward_risk_rejects_wrong_side_target() {
    let mut ctx = fractional_context();
    let mut insight = sample_insight();
    insight.side = OrderSide::Buy;
    insight.set_take_profit_levels(Some(vec![70.0]));
    assert!(
        !minimum_risk_to_reward::MinimumRiskToRewardPipe::new(None)
            .run(&mut ctx, &mut insight)
            .passed
    );
}

#[test]
fn builtin_test1_pipeline_uses_dynamic_budget_not_other_alpha_risk() {
    let mut ctx = fractional_context();
    ctx.account.buying_power = 1_003_616.33;
    let mut insight = sample_insight();
    insight.confidence = 10;
    insight
        .set_limit_price(Some(84_065.0823))
        .set_stop_loss(Some(84_060.32));
    let mut pipes = [
        InsightPipeBuilder::new(Box::new(
            percentage_risk_to_quantity::PercentageRiskToQuantityPipe::new(2),
        ))
        .allowed_alphas(HashSet::from([
            "EmaPriceCrossover".into(),
            "RsiDiverganceAlpha".into(),
        ]))
        .build(),
        InsightPipeBuilder::new(Box::new(
            dynamic_quantity_to_risk::DynamicQuantityToRiskPipe::new(200_000.0, 1_000.0),
        ))
        .allowed_alphas(HashSet::from(["TestEntry".into()]))
        .build(),
    ];
    for pipe in &mut pipes {
        if pipe.should_run(&insight) {
            assert!(pipe.run(&mut ctx, &mut insight).passed);
        }
    }
    assert_eq!(pipes[0].runs_count, 0);
    assert_eq!(pipes[1].runs_count, 1);
    assert!((insight.quantity.unwrap() - 1.1938).abs() < 1e-8);
    assert!(insight.total_insight_quantity_size() * 84_065.0823 <= 100_361.633);
}

#[test]
fn builtin_sizers_observe_contract_and_fractional_lot_budgets() {
    let mut asset = sample_asset();
    asset.contract_size = Some(100);
    asset.quantity_base = Some(2);
    asset.min_order_size = Some(0.01);
    let mut ctx = MockContext::new(asset);
    let mut full = sample_insight();
    ctx.account.buying_power = 15_123.0;
    assert!(
        full_account_quantity_to_risk::FullAccountQuantityToRiskPipe::new()
            .run(&mut ctx, &mut full)
            .passed
    );
    assert!((full.quantity.unwrap() - 1.51).abs() < 1e-9);
    let mut dynamic = sample_insight();
    dynamic.set_stop_loss(Some(99.0));
    assert!(
        dynamic_quantity_to_risk::DynamicQuantityToRiskPipe::new(15_123.0, 0.0)
            .run(&mut ctx, &mut dynamic)
            .passed
    );
    assert!((dynamic.quantity.unwrap() - 1.20).abs() < 1e-9);
    let mut percent = sample_insight();
    percent.set_stop_loss(Some(99.0));
    assert!(
        percentage_risk_to_quantity::PercentageRiskToQuantityPipe::new(1)
            .run(&mut ctx, &mut percent)
            .passed
    );
    assert!((percent.quantity.unwrap() - 1.0).abs() < 1e-9);
}

#[test]
fn builtin_expiry_defers_queue_pressure_and_preserves_receipt() {
    use crate::core::broker::commands::BrokerCommandError;
    for state in [InsightState::Executed, InsightState::Filled] {
        let mut ctx = fractional_context();
        ctx.command_error = Some(BrokerCommandError::QueueFull);
        let mut insight = sample_insight();
        insight.state = state.clone();
        insight.order_id = Some("order".into());
        insight.quantity = Some(1.0);
        insight.created_at = chrono::Utc::now() - chrono::Duration::hours(1);
        insight.filled_at = Some(insight.created_at);
        insight.filled_price = Some(100.0);
        let mut pipe = reject_expired_insight::RejectExpiredInsightPipe::new();
        let result = pipe.run(&mut ctx, &mut insight);
        assert_eq!(
            result.deferred,
            Some(InsightPipeDeferral::BrokerQueueCapacity)
        );
        assert_eq!(insight.state, state);
        assert!(ctx.commands.is_empty());
        ctx.command_error = None;
        let first = pipe.run(&mut ctx, &mut insight);
        assert!(matches!(
            first.deferred,
            Some(InsightPipeDeferral::BrokerCommand(_))
        ));
        let second = pipe.run(&mut ctx, &mut insight);
        assert_eq!(first.deferred, second.deferred);
        assert_eq!(ctx.commands.len(), 1);
    }
}

#[test]
fn builtin_scale_out_uses_executable_bid_not_last_and_zero_never_closes_all() {
    let mut ctx = fractional_context();
    ctx.command_error = None;
    let mut insight = sample_insight();
    insight.state = InsightState::Filled;
    insight.filled_price = Some(90.0);
    insight.order_id = Some("order".into());
    insight.quantity = Some(10.0);
    insight.set_take_profit_levels(Some(vec![99.5, 120.0]));
    let result = scale_out::ScaleOutPipe::new(0.5).run(&mut ctx, &mut insight);
    assert!(result.passed, "{result:?}"); // mock last=100, but executable bid=99
    assert!(ctx.commands.is_empty());
    for fraction in [0.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let result = scale_out::ScaleOutPipe::new(fraction).run(&mut ctx, &mut insight);
        assert!(!result.passed);
        assert!(!insight.closing);
        assert!(ctx.commands.is_empty());
    }
}

#[test]
fn builtin_dca_validation_keeps_existing_splits_and_short_levels_match_prices() {
    let mut ctx = fractional_context();
    let mut insight = sample_insight();
    let split = quantity_sizing::apply_max_order_size_with_child_legs(
        &mut ctx,
        &mut insight,
        250.0,
        quantity_sizing::AssetQuantityLimits::from_asset(&sample_asset()),
    )
    .unwrap();
    insight.set_quantity(Some(split.parent_quantity));
    let before = insight.snapshot_fingerprint_hash();
    assert!(
        !percentage_dca_levels::PercentageDcaLevelsPipe::new(0.6, 2)
            .run(&mut ctx, &mut insight)
            .passed
    );
    assert_eq!(insight.snapshot_fingerprint_hash(), before);
    let mut short = sample_insight();
    short.side = OrderSide::Sell;
    short.set_quantity(Some(10.0)).set_stop_loss(Some(110.0));
    assert!(
        percentage_dca_levels::PercentageDcaLevelsPipe::new(0.1, 2)
            .run(&mut ctx, &mut short)
            .passed
    );
    let parent_notional = short.quantity.unwrap() * 100.0;
    for child in &short.children {
        assert!(
            (child.quantity.unwrap() * child.limit_price.unwrap() - parent_notional).abs() < 0.02
        );
    }
}

#[test]
fn builtin_or_stops_after_a_terminal_mutation() {
    struct Reject;
    impl InsightPipe for Reject {
        fn version(&self) -> &str {
            "test"
        }
        fn run(&mut self, _: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
            insight.order_rejected("invalid");
            InsightPipeResult::new(false, true, None, "Reject".into())
        }
    }
    struct MustNotRun;
    impl InsightPipe for MustNotRun {
        fn version(&self) -> &str {
            "test"
        }
        fn run(&mut self, _: &mut dyn StrategyContext, _: &mut Insight) -> InsightPipeResult {
            panic!("alternative ran after rejection")
        }
    }
    let mut pipe = or_pipe::OrPipe::new(vec![
        InsightPipeBuilder::new(Box::new(Reject)).build(),
        InsightPipeBuilder::new(Box::new(MustNotRun)).build(),
    ]);
    let mut ctx = fractional_context();
    assert!(!pipe.run(&mut ctx, &mut sample_insight()).passed);
}

#[test]
fn builtin_end_of_day_non_ascii_input_does_not_panic() {
    let pipe = end_of_day_close::EndOfDayClosePipe::new(15, "€0".into());
    assert_eq!(pipe.end_of_day_time_utc, "23:45");
}
