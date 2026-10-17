//! Pure context/reducer fixtures: no transport, process, or filesystem access.
use super::*;
use crate::core::{
    alpha::WrappedAlphaModel,
    broker::{
        BrokerFeature,
        commands::*,
        types::{Account, Asset, Quote},
    },
    indicators::Indicator,
    insight::{InsightCollection, types::InsightState},
    pipeline::{
        InsightPipe, InsightPipeDeferral, WrappedInsightPipe, basic_stop_loss::BasicStopLossPipe,
        basic_take_profit::BasicTakeProfitPipe,
    },
    strategy::command_driver::BrokerCommandProgress,
    universe::WrappedUniverseModel,
    utils::{
        timeframe::{TimeFrame, TimeFrameUnit},
        tools::TradingTools,
    },
};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use polars::prelude::*;
use serde_json::Value;
use std::{cell::Cell, collections::HashMap, sync::Arc};
use uuid::Uuid;

struct Context {
    mode: StrategyMode,
    native: bool,
    features: &'static [BrokerFeature],
    quote: Option<Quote>,
    quote_reads: Cell<usize>,
    history: HashMap<String, DataFrame>,
    universe: HashMap<String, Asset>,
    insights: InsightCollection,
    variables: DashMap<String, Value>,
    timeframe: TimeFrame,
    commands: Vec<BrokerCommand>,
    acknowledged: bool,
}
impl Context {
    fn new() -> Self {
        Self {
            mode: StrategyMode::Live,
            native: true,
            features: &[],
            quote: Some(quote(99.0, 101.0)),
            quote_reads: Cell::new(0),
            history: HashMap::new(),
            universe: HashMap::new(),
            insights: InsightCollection::new(),
            variables: DashMap::new(),
            timeframe: TimeFrame::new(1, TimeFrameUnit::Minute),
            commands: vec![],
            acknowledged: false,
        }
    }
    fn set_history(&mut self, low: f64, high: f64) {
        self.history.insert(
            "ETHUSD".into(),
            DataFrame::new(
                1,
                vec![
                    Column::new("low".into(), [low]),
                    Column::new("high".into(), [high]),
                ],
            )
            .unwrap(),
        );
    }
}
struct Tools;
impl TradingTools for Tools {
    fn dynamic_round(&self, value: f64, _: &str) -> f64 {
        value
    }
    fn quantity_round(&self, value: f64, _: &str) -> f64 {
        value
    }
    fn calculate_time_to_live(&self, _: f64, _: f64, _: f64, additional: i32) -> i32 {
        additional
    }
    fn get_unrealized_pnl(&self, _: &str) -> Result<f64, BrokerError> {
        panic!("unexpected position read")
    }
    fn get_all_unrealized_pnl(&self) -> Result<f64, BrokerError> {
        panic!("unexpected position read")
    }
    fn get_filled_insights(&self) -> Vec<Insight> {
        panic!("unexpected collection scan")
    }
}
impl StrategyContext for Context {
    fn enqueue_broker_command(
        &mut self,
        id: Uuid,
        action: BrokerCommandAction,
        group: Option<BrokerOperationGroup>,
    ) -> Result<BrokerCommandReceipt, BrokerCommandError> {
        let command = BrokerCommand::new("strategy", "account", &id.to_string(), action, group);
        let receipt = command.receipt();
        self.commands.push(command);
        Ok(receipt)
    }
    fn broker_command_progress(
        &self,
        receipt: &BrokerCommandReceipt,
    ) -> Option<BrokerCommandProgress> {
        Some(if self.acknowledged {
            BrokerCommandProgress::Completed(Arc::new(BrokerCommandCompletion {
                receipt: receipt.clone(),
                queued_at_us: 0,
                dispatched_at_us: Some(0),
                completed_at_us: 1,
                outcome: BrokerCommandOutcome::Acknowledged,
            }))
        } else {
            BrokerCommandProgress::Queued
        })
    }
    fn broker_features(&self) -> &'static [BrokerFeature] {
        self.features
    }
    fn datafeed_features(&self) -> &'static [DataFeedFeature] {
        if self.native {
            &[DataFeedFeature::NativeBidAsk]
        } else {
            &[]
        }
    }
    fn universe(&self) -> &HashMap<String, Asset> {
        &self.universe
    }
    fn history(&self) -> &HashMap<String, DataFrame> {
        &self.history
    }
    fn insights(&self) -> &InsightCollection {
        &self.insights
    }
    fn mode(&self) -> StrategyMode {
        self.mode
    }
    fn add_insight(&mut self, row: Insight) {
        self.insights.add_insight(row);
    }
    fn register_indicator(&mut self, _: Box<dyn Indicator>) {}
    fn add_alpha(&mut self, _: WrappedAlphaModel) {}
    fn add_pipe(&mut self, _: WrappedInsightPipe) {}
    fn add_universe_model(&mut self, _: WrappedUniverseModel) {}
    fn set_execution_risk(&mut self, _: f64) {}
    fn set_min_reward_risk_ratio(&mut self, _: f64) {}
    fn set_base_confidence(&mut self, _: f64) {}
    fn execution_risk(&self) -> f64 {
        0.0
    }
    fn min_reward_risk_ratio(&self) -> f64 {
        0.0
    }
    fn base_confidence(&self) -> f64 {
        0.0
    }
    fn variables(&self) -> &DashMap<String, Value> {
        &self.variables
    }
    fn tools(&self) -> Box<dyn TradingTools + '_> {
        Box::new(Tools)
    }
    fn max_history_rows(&self) -> usize {
        1
    }
    fn set_max_history_rows(&mut self, _: usize) {}
    fn warm_up_bars(&self) -> i32 {
        0
    }
    fn set_warm_up_bars(&mut self, _: i32) {}
    fn timeframe(&self) -> &TimeFrame {
        &self.timeframe
    }
    fn account(&self) -> Result<Account, BrokerError> {
        panic!("unexpected account read")
    }
    fn current_time(&self) -> DateTime<Utc> {
        Utc::now()
    }
    fn bind_insight_context(&self, _: &mut Insight) {}
    fn latest_quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
        self.quote_reads.set(self.quote_reads.get() + 1);
        self.quote
            .clone()
            .ok_or_else(|| BrokerError::SnapshotUnavailable {
                kind: SnapshotKind::Quote {
                    symbol: symbol.into(),
                },
                reason: SnapshotUnavailable::Stale,
            })
    }
    fn shutdown(&mut self) {}
}
fn quote(bid: f64, ask: f64) -> Quote {
    Quote {
        symbol: "ETHUSD".into(),
        bid,
        ask,
        bid_size: 1.0,
        ask_size: 1.0,
        last: Some(1000.0),
        last_size: None,
        timestamp: Utc::now(),
    }
}
fn insight(side: OrderSide, level: f64) -> Insight {
    let mut row = Insight {
        symbol: "ETHUSD".into(),
        side,
        state: InsightState::Filled,
        quantity: Some(10.0),
        partial_filled_quantity: Some(4.0),
        filled_price: Some(100.0),
        order_id: Some("position".into()),
        ..Default::default()
    };
    row.set_stop_loss_levels(Some(vec![level]))
        .set_take_profit_levels(Some(vec![level]));
    row
}
fn pipe(trigger: ExitTrigger) -> Box<dyn InsightPipe> {
    match trigger {
        ExitTrigger::StopLoss => Box::new(BasicStopLossPipe::new()),
        ExitTrigger::TakeProfit => Box::new(BasicTakeProfitPipe::new()),
    }
}

#[test]
fn exit_trigger_live_uses_executable_side_and_inclusive_threshold_not_last_or_mid() {
    for (trigger, side, level, hit) in [
        (ExitTrigger::StopLoss, OrderSide::Buy, 98.0, false),
        (ExitTrigger::StopLoss, OrderSide::Buy, 99.0, true),
        (ExitTrigger::StopLoss, OrderSide::Sell, 102.0, false),
        (ExitTrigger::StopLoss, OrderSide::Sell, 101.0, true),
        (ExitTrigger::TakeProfit, OrderSide::Buy, 100.0, false),
        (ExitTrigger::TakeProfit, OrderSide::Buy, 99.0, true),
        (ExitTrigger::TakeProfit, OrderSide::Sell, 100.0, false),
        (ExitTrigger::TakeProfit, OrderSide::Sell, 101.0, true),
    ] {
        let mut ctx = Context::new(); // No history: native quote is sufficient.
        let mut row = insight(side, level);
        let result = pipe(trigger).run(&mut ctx, &mut row);
        assert!(result.success);
        assert_eq!(result.deferred.is_some(), hit);
        assert_eq!(ctx.commands.len(), usize::from(hit));
        assert_eq!(row.closing, hit);
        assert_eq!(row.state, InsightState::Filled);
        if hit {
            assert!(matches!(
                ctx.commands[0].action,
                BrokerCommandAction::Close {
                    quantity: 6.0,
                    price: None,
                    ..
                }
            ));
        }
    }
}

#[test]
fn exit_trigger_native_does_not_use_pre_fill_candle_extrema() {
    for trigger in [ExitTrigger::StopLoss, ExitTrigger::TakeProfit] {
        let mut ctx = Context::new();
        ctx.set_history(1.0, 1000.0);
        let mut row = insight(
            OrderSide::Buy,
            if matches!(trigger, ExitTrigger::StopLoss) {
                98.0
            } else {
                102.0
            },
        );
        let result = pipe(trigger).run(&mut ctx, &mut row);
        assert!(result.success && result.passed);
        assert!(ctx.commands.is_empty());
    }
}

#[test]
fn exit_trigger_missing_invalid_and_wrong_symbol_quotes_defer_without_mutation() {
    let mut invalid = quote(99.0, 101.0);
    invalid.bid = f64::NAN;
    let mut wrong = quote(99.0, 101.0);
    wrong.symbol = "SOLUSD".into();
    for quote in [None, Some(invalid), Some(wrong), Some(quote(101.0, 99.0))] {
        for trigger in [ExitTrigger::StopLoss, ExitTrigger::TakeProfit] {
            let mut ctx = Context::new();
            ctx.quote = quote.clone();
            ctx.set_history(1.0, 1000.0);
            let mut row = insight(OrderSide::Buy, 100.0);
            let result = pipe(trigger).run(&mut ctx, &mut row);
            assert_eq!(
                result.deferred,
                Some(InsightPipeDeferral::QuoteSnapshot {
                    symbol: "ETHUSD".into()
                })
            );
            assert!(result.success && !result.passed);
            assert!(!row.closing);
            assert!(ctx.commands.is_empty());
        }
    }
}

#[test]
fn exit_trigger_backtest_and_bar_only_keep_ohlc_without_reading_quotes() {
    for mode in [StrategyMode::Backtest, StrategyMode::Live] {
        for (trigger, side) in [
            (ExitTrigger::StopLoss, OrderSide::Buy),
            (ExitTrigger::StopLoss, OrderSide::Sell),
            (ExitTrigger::TakeProfit, OrderSide::Buy),
            (ExitTrigger::TakeProfit, OrderSide::Sell),
        ] {
            let mut ctx = Context::new();
            ctx.mode = mode;
            ctx.native = mode == StrategyMode::Backtest;
            ctx.quote = None;
            ctx.set_history(90.0, 110.0);
            let mut row = insight(side, 100.0);
            assert!(pipe(trigger).run(&mut ctx, &mut row).deferred.is_some());
            assert_eq!(ctx.commands.len(), 1);
            assert_eq!(ctx.quote_reads.get(), 0);
        }
    }
}

#[test]
fn exit_trigger_native_oco_skips_market_reads_and_duplicate_close() {
    for trigger in [ExitTrigger::StopLoss, ExitTrigger::TakeProfit] {
        let mut ctx = Context::new();
        ctx.features = &[BrokerFeature::OcoOrder];
        ctx.quote = None;
        let mut row = insight(OrderSide::Buy, 100.0);
        let result = pipe(trigger).run(&mut ctx, &mut row);
        assert!(result.success && result.passed);
        assert_eq!(ctx.quote_reads.get(), 0);
        assert!(ctx.commands.is_empty());
    }
}

#[test]
fn exit_trigger_pending_close_resumes_without_new_price_or_duplicate_command() {
    for trigger in [ExitTrigger::StopLoss, ExitTrigger::TakeProfit] {
        let mut ctx = Context::new();
        let mut row = insight(
            OrderSide::Buy,
            if matches!(trigger, ExitTrigger::StopLoss) {
                100.0
            } else {
                98.0
            },
        );
        let mut pipe = pipe(trigger);
        assert!(pipe.run(&mut ctx, &mut row).deferred.is_some());
        let receipt = row.pending_broker_command().unwrap().clone();
        ctx.quote = None;
        for _ in 0..3 {
            assert_eq!(
                pipe.run(&mut ctx, &mut row).deferred,
                Some(InsightPipeDeferral::BrokerCommand(receipt.clone()))
            );
        }
        ctx.acknowledged = true;
        assert!(pipe.run(&mut ctx, &mut row).success);
        assert_eq!(ctx.commands.len(), 1);
        assert_eq!(ctx.quote_reads.get(), 1);
        assert!(row.closing); // Acknowledgement is not terminal trade evidence.
        assert_eq!(row.state, InsightState::Filled);
    }
}

#[test]
fn exit_trigger_partial_tp_advances_once_on_ack_and_uses_remaining_quantity() {
    let mut ctx = Context::new();
    ctx.features = &[BrokerFeature::PartialClose];
    let mut row = insight(OrderSide::Buy, 98.0);
    row.set_take_profit_levels(Some(vec![98.0, 110.0]));
    let mut pipe = BasicTakeProfitPipe::new();
    assert!(pipe.run(&mut ctx, &mut row).deferred.is_some());
    assert!(matches!(
        ctx.commands[0].action,
        BrokerCommandAction::Close { quantity: 3.0, .. }
    ));
    assert_eq!(row.take_profit_levels(), Some(vec![98.0, 110.0]));
    ctx.quote = None;
    ctx.acknowledged = true;
    assert!(pipe.run(&mut ctx, &mut row).success);
    assert_eq!(row.take_profit_levels(), Some(vec![110.0]));
    assert_eq!(ctx.commands.len(), 1);
    assert!(row.closing);
    assert_eq!(ctx.quote_reads.get(), 1);
}

#[test]
fn exit_trigger_bar_fallback_reads_only_last_scalar_without_prior_extrema() {
    let mut ctx = Context::new();
    ctx.native = false;
    ctx.history.insert(
        "ETHUSD".into(),
        DataFrame::new(
            2,
            vec![
                Column::new("low".into(), [1.0, 99.0]),
                Column::new("high".into(), [1000.0, 101.0]),
            ],
        )
        .unwrap(),
    );
    for (trigger, level) in [
        (ExitTrigger::StopLoss, 98.0),
        (ExitTrigger::TakeProfit, 102.0),
    ] {
        let mut row = insight(OrderSide::Buy, level);
        let result = pipe(trigger).run(&mut ctx, &mut row);
        assert!(result.success && result.passed);
    }
    assert!(ctx.commands.is_empty());
    assert_eq!(ctx.quote_reads.get(), 0);
}

#[test]
fn exit_trigger_native_oco_with_multiple_partial_targets_keeps_manual_scale_out() {
    let mut ctx = Context::new();
    ctx.features = &[BrokerFeature::OcoOrder, BrokerFeature::PartialClose];
    let mut row = insight(OrderSide::Sell, 102.0);
    row.set_take_profit_levels(Some(vec![102.0, 90.0]));
    assert!(
        BasicTakeProfitPipe::new()
            .run(&mut ctx, &mut row)
            .deferred
            .is_some()
    );
    assert!(matches!(
        ctx.commands[0].action,
        BrokerCommandAction::Close { quantity: 3.0, .. }
    ));
    assert_eq!(ctx.quote_reads.get(), 1);
}

#[test]
fn exit_trigger_cleared_tp_does_not_orphan_an_existing_close_receipt() {
    for targets in [None, Some(vec![])] {
        let mut ctx = Context::new();
        let mut row = insight(OrderSide::Buy, 98.0);
        let mut pipe = BasicTakeProfitPipe::new();
        assert!(pipe.run(&mut ctx, &mut row).deferred.is_some());
        row.set_take_profit_levels(targets);
        ctx.quote = None;
        assert!(pipe.run(&mut ctx, &mut row).deferred.is_some());
        ctx.acknowledged = true;
        assert!(pipe.run(&mut ctx, &mut row).success);
        assert!(!row.has_pending_close());
        assert_eq!(ctx.commands.len(), 1);
        assert_eq!(ctx.quote_reads.get(), 1);
    }
}
