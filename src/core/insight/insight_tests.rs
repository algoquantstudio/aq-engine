use super::*;
use crate::core::alpha::WrappedAlphaModel;
use crate::core::broker::types::{
    Account, AccountType, Asset, AssetExchange, AssetStatus, AssetType, BrokerError, OrderSide,
    Quote,
};
use crate::core::indicators::Indicator;
use crate::core::insight::InsightMutationState;
use crate::core::pipeline::WrappedInsightPipe;
use crate::core::strategy::{StrategyContext, StrategyMode};
use crate::core::utils::timeframe::{TimeFrame, TimeFrameUnit};
use crate::core::utils::tools::TradingTools;
use chrono::{Duration, TimeZone};
use dashmap::DashMap;
use polars::prelude::DataFrame;
use serde_json::Value;
use std::cell::RefCell;
use std::collections::HashMap;

struct MockTools;

impl TradingTools for MockTools {
    fn dynamic_round(&self, value: f64, _symbol: &str) -> f64 {
        value
    }

    fn quantity_round(&self, value: f64, _symbol: &str) -> f64 {
        value
    }

    fn calculate_time_to_live(&self, _price: f64, _entry: f64, _atr: f64, additional: i32) -> i32 {
        additional
    }

    fn get_unrealized_pnl(&self, _symbol: &str) -> Result<f64, BrokerError> {
        Ok(0.0)
    }

    fn get_all_unrealized_pnl(&self) -> Result<f64, BrokerError> {
        Ok(0.0)
    }

    fn get_filled_insights(&self) -> Vec<Insight> {
        Vec::new()
    }
}

struct MockStrategyContext {
    universe: HashMap<String, Asset>,
    history: HashMap<String, DataFrame>,
    insights: crate::core::insight::InsightCollection,
    variables: DashMap<String, Value>,
    current_time: DateTime<Utc>,
    cancelled_orders: RefCell<Vec<String>>,
    updated_orders: RefCell<Vec<(String, f64, f64)>>,
    updated_stop_losses: RefCell<Vec<(String, f64, f64)>>,
    closed_positions: RefCell<Vec<(String, f64, Option<f64>)>>,
    account: Account,
    account_available: bool,
    commands: HashMap<
        crate::core::broker::commands::BrokerCommandId,
        crate::core::broker::commands::BrokerCommand,
    >,
    acknowledged: bool,
    cancelled_entries: Vec<crate::core::broker::commands::BrokerCommandReceipt>,
}

impl MockStrategyContext {
    fn new(current_time: DateTime<Utc>) -> Self {
        let asset = Asset {
            id: "asset-1".to_string(),
            symbol: "EURUSD=X".to_string(),
            name: "EURUSD=X".to_string(),
            asset_type: AssetType::Forex,
            status: AssetStatus::Active,
            exchange: AssetExchange::UNKNOWN("CCY".to_string()),
            tradable: true,
            marginable: true,
            shortable: true,
            fractional: true,
            min_order_size: Some(0.01),
            quantity_base: Some(2),
            max_order_size: None,
            max_total_size: None,
            min_price_increment: Some(0.0001),
            price_base: Some(4),
            contract_size: None,
            trading_calendar: None,
            fees: Default::default(),
        };

        Self {
            universe: HashMap::from([(asset.symbol.clone(), asset)]),
            history: HashMap::new(),
            insights: crate::core::insight::InsightCollection::new(),
            variables: DashMap::new(),
            current_time,
            cancelled_orders: RefCell::new(Vec::new()),
            updated_orders: RefCell::new(Vec::new()),
            updated_stop_losses: RefCell::new(Vec::new()),
            closed_positions: RefCell::new(Vec::new()),
            account: Account {
                account_id: "paper".to_string(),
                max_total_orders: None,
                account_type: AccountType::Paper,
                equity: 100_000.0,
                cash: 100_000.0,
                currency: "USD".to_string(),
                buying_power: 100_000.0,
                accrued_commission: 0.0,
                shorting_enabled: true,
                leverage: 1,
            },
            commands: HashMap::new(),
            account_available: true,
            acknowledged: false,
            cancelled_entries: Vec::new(),
        }
    }
}

impl StrategyContext for MockStrategyContext {
    fn broker_features(&self) -> &'static [crate::core::broker::BrokerFeature] {
        &[
            crate::core::broker::BrokerFeature::MaxTotalOrders,
            crate::core::broker::BrokerFeature::AmendLimitPrice,
            crate::core::broker::BrokerFeature::AmendStopPrice,
        ]
    }

    fn cached_account(&self) -> Option<Account> {
        self.account_available.then(|| self.account.clone())
    }

    fn cancel_pending_entry(
        &mut self,
        receipt: &crate::core::broker::commands::BrokerCommandReceipt,
    ) -> Result<(), crate::core::broker::commands::BrokerCommandError> {
        if !self.cancelled_entries.contains(receipt) {
            self.cancelled_entries.push(receipt.clone());
        }
        Ok(())
    }
    fn enqueue_broker_command(
        &mut self,
        insight_id: Uuid,
        action: crate::core::broker::commands::BrokerCommandAction,
        group: Option<crate::core::broker::commands::BrokerOperationGroup>,
    ) -> Result<
        crate::core::broker::commands::BrokerCommandReceipt,
        crate::core::broker::commands::BrokerCommandError,
    > {
        use crate::core::broker::commands::{BrokerCommand, BrokerCommandAction};
        match &action {
            BrokerCommandAction::Cancel { order_id } => {
                self.cancelled_orders.borrow_mut().push(order_id.clone())
            }
            BrokerCommandAction::Close {
                order_id,
                quantity,
                price,
            } => self
                .closed_positions
                .borrow_mut()
                .push((order_id.clone(), *quantity, *price)),
            BrokerCommandAction::TakeProfit {
                order_id,
                price,
                quantity,
            } => self
                .updated_orders
                .borrow_mut()
                .push((order_id.clone(), *price, *quantity)),
            BrokerCommandAction::StopLoss {
                order_id,
                price,
                quantity,
            } => self
                .updated_stop_losses
                .borrow_mut()
                .push((order_id.clone(), *price, *quantity)),
            _ => {}
        }
        let command = BrokerCommand::new(
            "strategy",
            "account",
            &insight_id.to_string(),
            action,
            group,
        );
        command.validate()?;
        let receipt = command.receipt();
        self.commands.insert(command.id.clone(), command);
        Ok(receipt)
    }

    fn broker_command_progress(
        &self,
        receipt: &crate::core::broker::commands::BrokerCommandReceipt,
    ) -> Option<crate::core::strategy::command_driver::BrokerCommandProgress> {
        use crate::core::{
            broker::commands::{BrokerCommandCompletion, BrokerCommandOutcome},
            strategy::command_driver::BrokerCommandProgress,
        };
        self.commands
            .get(&receipt.id)
            .filter(|command| command.receipt() == *receipt)?;
        Some(if self.acknowledged {
            BrokerCommandProgress::Completed(std::sync::Arc::new(BrokerCommandCompletion {
                receipt: receipt.clone(),
                queued_at_us: 1,
                dispatched_at_us: Some(2),
                completed_at_us: 3,
                outcome: BrokerCommandOutcome::Acknowledged,
            }))
        } else {
            BrokerCommandProgress::Queued
        })
    }
    fn universe(&self) -> &HashMap<String, Asset> {
        &self.universe
    }

    fn history(&self) -> &HashMap<String, DataFrame> {
        &self.history
    }

    fn insights(&self) -> &crate::core::insight::InsightCollection {
        &self.insights
    }

    fn mode(&self) -> StrategyMode {
        StrategyMode::Live
    }

    fn add_insight(&mut self, insight: Insight) {
        self.insights.add_insight(insight);
    }

    fn register_indicator(&mut self, _indicator: Box<dyn Indicator>) {}

    fn add_alpha(&mut self, _alpha: WrappedAlphaModel) {}

    fn add_pipe(&mut self, _pipe: WrappedInsightPipe) {}

    fn add_universe_model(&mut self, _model: crate::core::universe::WrappedUniverseModel) {}

    fn set_execution_risk(&mut self, _risk: f64) {}

    fn set_min_reward_risk_ratio(&mut self, _ratio: f64) {}

    fn set_base_confidence(&mut self, _confidence: f64) {}

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
        Box::new(MockTools)
    }

    fn max_history_rows(&self) -> usize {
        0
    }

    fn set_max_history_rows(&mut self, _rows: usize) {}

    fn warm_up_bars(&self) -> i32 {
        0
    }

    fn set_warm_up_bars(&mut self, _bars: i32) {}

    fn timeframe(&self) -> &TimeFrame {
        static TIMEFRAME: std::sync::LazyLock<TimeFrame> =
            std::sync::LazyLock::new(|| TimeFrame::new(1, TimeFrameUnit::Minute));
        &TIMEFRAME
    }

    fn account(&self) -> Result<Account, BrokerError> {
        Ok(self.account.clone())
    }

    fn current_time(&self) -> DateTime<Utc> {
        self.current_time
    }

    fn bind_insight_context(&self, insight: &mut Insight) {
        let current_time = self.current_time;
        insight.bind_context(InsightStrategyContext::new(move || current_time));
    }

    fn latest_quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
        Ok(Quote {
            symbol: symbol.to_string(),
            bid: 1.15,
            ask: 1.15,
            bid_size: 1.0,
            ask_size: 1.0,
            last: Some(1.15),
            last_size: Some(1.0),
            timestamp: self.current_time,
        })
    }

    fn shutdown(&mut self) {}
}

fn sample_insight(now: DateTime<Utc>) -> Insight {
    let mut insight = Insight::new(
        OrderSide::Buy,
        "EURUSD=X".to_string(),
        StrategyType::Testing,
        TimeFrame::new(1, TimeFrameUnit::Minute),
        80,
        None,
    );
    insight.set_quantity(Some(1.0));
    insight.align_time_to(now);
    insight
}

#[test]
fn update_take_profit_on_filled_insight_updates_broker_and_local_leg() {
    let created_at = Utc.with_ymd_and_hms(2026, 3, 23, 10, 0, 0).unwrap();
    let update_time = created_at + Duration::minutes(5);
    let mut ctx = MockStrategyContext::new(update_time);
    let mut insight = sample_insight(created_at);
    let leg_ts = created_at.timestamp().max(0) as u64;
    insight.state = InsightState::Filled;
    insight.order_id = Some("entry-order-1".to_string());
    insight.set_quantity(Some(1.5));
    insight.set_take_profit_levels(Some(vec![110.0]));
    insight.legs.take_profit = Some(OrderLeg {
        order_id: None,
        limit_price: Some(110.0),
        trail_price: None,
        side: OrderSide::Sell,
        filled_price: None,
        order_type: OrderType::Limit,
        status: TradeUpdateEvent::Pending,
        order_class: OrderClass::OTO,
        created_at: leg_ts,
        updated_at: leg_ts,
        submitted_at: leg_ts,
        filled_at: None,
    });

    assert!(matches!(
        insight
            .update_take_profit(&mut ctx, Some(vec![112.5]))
            .unwrap(),
        crate::core::insight::InsightMutationState::Pending(_)
    ));
    assert_eq!(insight.take_profit_levels(), Some(vec![110.0]));
    ctx.acknowledged = true;
    assert_eq!(
        insight
            .update_take_profit(&mut ctx, Some(vec![112.5]))
            .unwrap(),
        crate::core::insight::InsightMutationState::Applied
    );

    assert_eq!(
        ctx.updated_orders.borrow().as_slice(),
        &[("entry-order-1".to_string(), 112.5, 1.5)]
    );
    assert_eq!(insight.take_profit_levels(), Some(vec![112.5]));
    let take_profit = insight
        .legs
        .take_profit
        .as_ref()
        .expect("active take-profit leg should remain attached");
    assert_eq!(take_profit.limit_price, Some(112.5));
    assert_eq!(
        take_profit.updated_at,
        update_time.timestamp().max(0) as u64
    );
}

#[test]
fn submit_pipe_waits_for_existing_receipt_when_account_snapshot_disappears() {
    use crate::core::pipeline::{
        InsightPipe, InsightPipeDeferral, insight_submit::InsightSubmitPipe,
    };

    let now = Utc.with_ymd_and_hms(2026, 9, 26, 23, 40, 0).unwrap();
    for broker_state in [InsightState::Executed, InsightState::Filled] {
        let mut ctx = MockStrategyContext::new(now);
        let mut insight = sample_insight(now);
        let mut pipe = InsightSubmitPipe::new();
        let first = pipe.run(&mut ctx, &mut insight);
        let Some(InsightPipeDeferral::BrokerCommand(receipt)) = first.deferred else {
            panic!("first submission must wait for its broker receipt");
        };

        // Reproduce the outage before the submit reply: this used to fail
        // capacity validation and schedule compensating cancellation.
        ctx.account_available = false;
        for _ in 0..3 {
            let result = pipe.run(&mut ctx, &mut insight);
            assert!(result.success);
            assert_eq!(
                result.deferred,
                Some(InsightPipeDeferral::BrokerCommand(receipt.clone()))
            );
            assert_eq!(insight.state, InsightState::New);
            assert!(!insight.cancelling && !insight.suppress_child_entries);
        }

        // Trade evidence can arrive before the RPC completes. Keep polling the
        // same receipt without requiring account data or resubmitting the order.
        insight.order_id = Some("1966551688".into());
        insight.state = broker_state.clone();
        if broker_state == InsightState::Filled {
            insight.filled_price = Some(1.15);
        }
        assert_eq!(
            pipe.run(&mut ctx, &mut insight).deferred,
            Some(InsightPipeDeferral::BrokerCommand(receipt))
        );
        ctx.acknowledged = true;
        let result = pipe.run(&mut ctx, &mut insight);
        assert!(result.success && result.passed && result.deferred.is_none());
        assert!(!insight.has_pending_submit());
        assert_eq!(insight.state, broker_state);
        assert_eq!(ctx.commands.len(), 1, "must never submit twice or cancel");
        assert!(ctx.cancelled_entries.is_empty());
        assert!(ctx.cancelled_orders.borrow().is_empty());
        assert!(!insight.cancelling && !insight.suppress_child_entries);
    }
}

#[test]
fn submit_pipe_defers_missing_account_and_submits_once_after_refresh() {
    use crate::core::pipeline::{
        InsightPipe, InsightPipeDeferral, insight_submit::InsightSubmitPipe,
    };

    let now = Utc.with_ymd_and_hms(2026, 9, 26, 23, 40, 0).unwrap();
    let mut ctx = MockStrategyContext::new(now);
    let mut insight = sample_insight(now);
    let before = insight.snapshot_fingerprint_hash();
    let mut pipe = InsightSubmitPipe::new();
    ctx.account_available = false;
    for _ in 0..3 {
        let result = pipe.run(&mut ctx, &mut insight);
        assert!(result.success && !result.passed);
        assert_eq!(result.deferred, Some(InsightPipeDeferral::AccountSnapshot));
        assert_eq!(insight.snapshot_fingerprint_hash(), before);
        assert!(ctx.commands.is_empty());
    }
    ctx.account_available = true;
    let result = pipe.run(&mut ctx, &mut insight);
    assert!(matches!(
        result.deferred,
        Some(InsightPipeDeferral::BrokerCommand(_))
    ));
    assert_eq!(ctx.commands.len(), 1);
    assert!(insight.has_pending_submit());
}

#[test]
fn submit_pipe_still_rejects_actual_pending_order_capacity_exhaustion() {
    use crate::core::pipeline::{InsightPipe, insight_submit::InsightSubmitPipe};

    let now = Utc.with_ymd_and_hms(2026, 9, 26, 23, 40, 0).unwrap();
    let mut ctx = MockStrategyContext::new(now);
    ctx.account.max_total_orders = Some(0);
    let mut insight = sample_insight(now);
    insight.order_type = OrderType::Limit;
    insight.set_limit_price(Some(1.15));
    let result = InsightSubmitPipe::new().run(&mut ctx, &mut insight);
    assert!(!result.passed && result.deferred.is_none());
    assert!(result.message.unwrap().contains("max total pending orders"));
    assert!(ctx.commands.is_empty());
}

#[test]
fn expiry_after_queue_admission_gates_original_identity_without_rejecting_unknown_exposure() {
    let created_at = Utc.with_ymd_and_hms(2026, 3, 23, 10, 0, 0).unwrap();
    let mut ctx = MockStrategyContext::new(created_at);
    let mut insight = sample_insight(created_at);
    insight.set_quantity(Some(1.0));
    insight.set_period_unfilled(Some(1));
    let crate::core::insight::InsightMutationState::Pending(receipt) =
        insight.submit(&mut ctx).unwrap()
    else {
        panic!("submission must be pending until broker evidence");
    };
    ctx.current_time = created_at + Duration::minutes(2);
    assert!(insight.has_expired(&mut ctx));
    assert!(insight.has_expired(&mut ctx));
    assert_eq!(insight.state, InsightState::New);
    assert!(insight.cancelling);
    assert!(insight.has_pending_submit());
    assert_eq!(ctx.cancelled_entries, vec![receipt]);
    assert_eq!(
        ctx.commands.len(),
        1,
        "expiry must not create another submit identity"
    );
}

#[test]
fn update_stop_loss_on_filled_insight_updates_broker_and_local_leg() {
    let created_at = Utc.with_ymd_and_hms(2026, 3, 23, 10, 0, 0).unwrap();
    let update_time = created_at + Duration::minutes(5);
    let mut ctx = MockStrategyContext::new(update_time);
    let mut insight = sample_insight(created_at);
    let leg_ts = created_at.timestamp().max(0) as u64;
    insight.state = InsightState::Filled;
    insight.order_id = Some("entry-order-1".to_string());
    insight.set_quantity(Some(1.5));
    insight.set_stop_loss_levels(Some(vec![95.0]));
    insight.legs.stop_loss = Some(OrderLeg {
        order_id: None,
        limit_price: Some(95.0),
        trail_price: None,
        side: OrderSide::Sell,
        filled_price: None,
        order_type: OrderType::Stop,
        status: TradeUpdateEvent::Pending,
        order_class: OrderClass::OTO,
        created_at: leg_ts,
        updated_at: leg_ts,
        submitted_at: leg_ts,
        filled_at: None,
    });

    assert!(matches!(
        insight
            .update_stop_loss(&mut ctx, Some(vec![92.5]))
            .unwrap(),
        crate::core::insight::InsightMutationState::Pending(_)
    ));
    assert_eq!(insight.stop_loss_levels(), Some(vec![95.0]));
    ctx.acknowledged = true;
    assert_eq!(
        insight
            .update_stop_loss(&mut ctx, Some(vec![92.5]))
            .unwrap(),
        crate::core::insight::InsightMutationState::Applied
    );

    assert_eq!(
        ctx.updated_stop_losses.borrow().as_slice(),
        &[("entry-order-1".to_string(), 92.5, 1.5)]
    );
    assert_eq!(insight.stop_loss_levels(), Some(vec![92.5]));
    let stop_loss = insight
        .legs
        .stop_loss
        .as_ref()
        .expect("active stop-loss leg should remain attached");
    assert_eq!(stop_loss.limit_price, Some(92.5));
    assert_eq!(stop_loss.updated_at, update_time.timestamp().max(0) as u64);
}

#[test]
fn entry_amendments_wait_for_ack_and_ignore_filled_orders() {
    let mut ctx = MockStrategyContext::new(Utc::now());
    let mut insight = sample_insight(Utc::now());
    insight.update_limit(&mut ctx, 100.0).unwrap();
    assert_eq!(insight.limit_price(), Some(100.0));
    assert!(ctx.commands.is_empty());
    insight.state = InsightState::Executed;
    insight.order_id = Some("pending-entry".into());
    assert!(matches!(
        insight.update_limit(&mut ctx, 101.0).unwrap(),
        InsightMutationState::Pending(_)
    ));
    assert_eq!(insight.limit_price(), Some(100.0));
    insight.update_limit(&mut ctx, 101.0).unwrap();
    assert_eq!(ctx.commands.len(), 1);
    ctx.acknowledged = true;
    assert_eq!(
        insight.update_limit(&mut ctx, 101.0).unwrap(),
        InsightMutationState::Applied
    );
    assert_eq!(insight.limit_price(), Some(101.0));
    insight.state = InsightState::Filled;
    insight.update_limit(&mut ctx, 102.0).unwrap();
    insight.update_stop_limit(&mut ctx, 99.0).unwrap();
    assert_eq!(insight.limit_price(), Some(101.0));
    assert_eq!(ctx.commands.len(), 1);
}

#[test]
fn entry_stop_amendment_preserves_limit_and_fill_race() {
    let mut ctx = MockStrategyContext::new(Utc::now());
    let mut insight = sample_insight(Utc::now());
    insight
        .set_limit_price(Some(100.0))
        .set_stop_price(Some(101.0));
    insight.state = InsightState::Executed;
    insight.order_id = Some("pending-stop-limit".into());
    insight.update_stop_limit(&mut ctx, 102.0).unwrap();
    assert_eq!(insight.stop_price(), Some(101.0));
    ctx.acknowledged = true;
    insight.update_stop_limit(&mut ctx, 102.0).unwrap();
    assert_eq!(insight.stop_price(), Some(102.0));
    assert_eq!(insight.limit_price(), Some(100.0));
    ctx.acknowledged = false;
    insight.update_limit(&mut ctx, 99.0).unwrap();
    insight.state = InsightState::Filled;
    ctx.acknowledged = true;
    insight.update_limit(&mut ctx, 99.0).unwrap();
    assert_eq!(insight.limit_price(), Some(100.0));
}

#[test]
fn update_take_profit_on_executed_insight_updates_entry_order_and_local_leg() {
    let created_at = Utc.with_ymd_and_hms(2026, 3, 23, 10, 0, 0).unwrap();
    let update_time = created_at + Duration::minutes(5);
    let mut ctx = MockStrategyContext::new(update_time);
    let mut insight = sample_insight(created_at);
    let leg_ts = created_at.timestamp().max(0) as u64;
    insight.state = InsightState::Executed;
    insight.order_id = Some("pending-entry-order-1".to_string());
    insight.set_quantity(Some(1.5));
    insight.set_take_profit_levels(Some(vec![110.0]));
    insight.legs.take_profit = Some(OrderLeg {
        order_id: None,
        limit_price: Some(110.0),
        trail_price: None,
        side: OrderSide::Sell,
        filled_price: None,
        order_type: OrderType::Limit,
        status: TradeUpdateEvent::Pending,
        order_class: OrderClass::OTO,
        created_at: leg_ts,
        updated_at: leg_ts,
        submitted_at: leg_ts,
        filled_at: None,
    });

    assert!(matches!(
        insight
            .update_take_profit(&mut ctx, Some(vec![112.5]))
            .unwrap(),
        crate::core::insight::InsightMutationState::Pending(_)
    ));
    ctx.acknowledged = true;
    assert_eq!(
        insight
            .update_take_profit(&mut ctx, Some(vec![112.5]))
            .unwrap(),
        crate::core::insight::InsightMutationState::Applied
    );

    assert_eq!(
        ctx.updated_orders.borrow().as_slice(),
        &[("pending-entry-order-1".to_string(), 112.5, 1.5)]
    );
    assert_eq!(insight.state(), &InsightState::Executed);
    assert_eq!(insight.take_profit_levels(), Some(vec![112.5]));
    assert_eq!(
        insight
            .legs
            .take_profit
            .as_ref()
            .and_then(|leg| leg.limit_price),
        Some(112.5)
    );
}

#[test]
fn update_stop_loss_on_executed_insight_updates_entry_order_and_local_leg() {
    let created_at = Utc.with_ymd_and_hms(2026, 3, 23, 10, 0, 0).unwrap();
    let update_time = created_at + Duration::minutes(5);
    let mut ctx = MockStrategyContext::new(update_time);
    let mut insight = sample_insight(created_at);
    let leg_ts = created_at.timestamp().max(0) as u64;
    insight.state = InsightState::Executed;
    insight.order_id = Some("pending-entry-order-1".to_string());
    insight.set_quantity(Some(1.5));
    insight.set_stop_loss_levels(Some(vec![95.0]));
    insight.legs.stop_loss = Some(OrderLeg {
        order_id: None,
        limit_price: Some(95.0),
        trail_price: None,
        side: OrderSide::Sell,
        filled_price: None,
        order_type: OrderType::Stop,
        status: TradeUpdateEvent::Pending,
        order_class: OrderClass::OTO,
        created_at: leg_ts,
        updated_at: leg_ts,
        submitted_at: leg_ts,
        filled_at: None,
    });

    assert!(matches!(
        insight
            .update_stop_loss(&mut ctx, Some(vec![92.5]))
            .unwrap(),
        crate::core::insight::InsightMutationState::Pending(_)
    ));
    ctx.acknowledged = true;
    assert_eq!(
        insight
            .update_stop_loss(&mut ctx, Some(vec![92.5]))
            .unwrap(),
        crate::core::insight::InsightMutationState::Applied
    );

    assert_eq!(
        ctx.updated_stop_losses.borrow().as_slice(),
        &[("pending-entry-order-1".to_string(), 92.5, 1.5)]
    );
    assert_eq!(insight.state(), &InsightState::Executed);
    assert_eq!(insight.stop_loss_levels(), Some(vec![92.5]));
    assert_eq!(
        insight
            .legs
            .stop_loss
            .as_ref()
            .and_then(|leg| leg.limit_price),
        Some(92.5)
    );
}

#[test]
fn state_log_records_message_without_changing_state() {
    let created_at = Utc.with_ymd_and_hms(2026, 3, 23, 10, 0, 0).unwrap();
    let log_time = created_at + Duration::minutes(1);
    let mut insight = sample_insight(created_at);
    insight.state = InsightState::Filled;
    insight.closing = true;
    insight.bind_context(InsightStrategyContext::new(move || log_time));
    let history_len = insight.state_history.len();

    insight.state_log(Some("Risk check passed".to_string()));

    assert_eq!(insight.state, InsightState::Filled);
    assert!(insight.closing);
    assert_eq!(insight.updated_at, log_time);
    assert_eq!(insight.state_history.len(), history_len + 1);
    let (at, state, message) = insight.state_history.last().unwrap();
    assert_eq!(*at, log_time);
    assert_eq!(*state, InsightState::Filled);
    assert_eq!(message.as_deref(), Some("Risk check passed"));
}

#[test]
fn add_child_insight_does_not_append_parent_history_entry() {
    let now = Utc.with_ymd_and_hms(2026, 3, 23, 10, 0, 0).unwrap();
    let mut ctx = MockStrategyContext::new(now);
    let mut parent = sample_insight(now);
    let parent_id = *parent.insight_id();
    let parent_history_len = parent.state_history.len();
    let child = Insight::new(
        OrderSide::Buy,
        "EURUSD=X".to_string(),
        StrategyType::Testing,
        TimeFrame::new(1, TimeFrameUnit::Minute),
        80,
        None,
    );

    parent.add_child_insight(child, None, &mut ctx);

    assert_eq!(parent.children.len(), 1);
    assert_eq!(parent.children[0].parent_id, Some(parent_id));
    assert!(!parent.children[0].is_split_insight());
    assert_eq!(parent.children[0].quantity, parent.quantity);
    assert!(matches!(
        parent.children[0].strategy_type,
        StrategyType::Custom(ref value) if value == "Testing-CHILD"
    ));
    assert_eq!(parent.state_history.len(), parent_history_len);
}

#[test]
fn add_child_insight_marks_only_the_attached_execution_fragment_as_split() {
    let now = Utc.with_ymd_and_hms(2026, 3, 23, 10, 0, 0).unwrap();
    let mut ctx = MockStrategyContext::new(now);
    let mut parent = sample_insight(now);
    let parent_id = *parent.insight_id();
    let mut main_child = sample_insight(now);
    let main_child_id = *main_child.insight_id();
    let split_fragment = sample_insight(now);

    main_child.add_child_insight(split_fragment, Some(true), &mut ctx);
    parent.add_child_insight(main_child, None, &mut ctx);

    let main_child = &parent.children[0];
    assert_eq!(main_child.parent_id, Some(parent_id));
    assert!(!main_child.is_split_insight());
    assert_eq!(main_child.children[0].parent_id, Some(main_child_id));
    assert!(main_child.children[0].is_split_insight());
}

#[test]
fn no_op_insight_setters_do_not_append_history_entries() {
    let now = Utc.with_ymd_and_hms(2026, 3, 23, 10, 0, 0).unwrap();
    let mut insight = sample_insight(now);
    let history_len = insight.state_history.len();

    insight
        .set_quantity(Some(1.0))
        .set_limit_price(None)
        .set_stop_price(None)
        .set_take_profit_levels(None)
        .set_stop_loss_levels(None)
        .set_trailing_stop_price(None)
        .set_period_unfilled(None)
        .set_period_till_tp(None)
        .set_execution_depends(Vec::new())
        .set_first_on_fill(false);

    assert_eq!(insight.state_history.len(), history_len);
}

#[test]
fn filled_insight_cannot_be_rejected_or_cancelled() {
    let now = Utc.with_ymd_and_hms(2026, 3, 23, 10, 0, 0).unwrap();
    let mut insight = sample_insight(now);
    insight.position_filled(1.15, 1.0, "filled-order", None);

    insight.order_rejected("duplicate protection update returned no changes");
    assert_eq!(insight.state, InsightState::Filled);

    insight.order_cancelled("missing from a transient local snapshot");
    assert_eq!(insight.state, InsightState::Filled);
    assert!(!insight.state_history.iter().any(|(_, state, _)| {
        matches!(state, InsightState::Rejected | InsightState::Cancelled)
    }));
}

#[test]
fn new_insight_expiry_rejects_immediately() {
    let created_at = Utc.with_ymd_and_hms(2026, 3, 23, 10, 0, 0).unwrap();
    let now = created_at + Duration::minutes(2);
    let mut ctx = MockStrategyContext::new(now);
    let mut insight = sample_insight(created_at);
    insight.set_period_unfilled(Some(1));

    let expired = insight.has_expired(&mut ctx);

    assert!(expired);
    assert_eq!(insight.state, InsightState::Rejected);
    assert!(ctx.cancelled_orders.borrow().is_empty());
    assert!(ctx.closed_positions.borrow().is_empty());
    assert!(insight.state_history.iter().any(|(_, state, message)| {
        *state == InsightState::Rejected
            && message.as_deref().is_some_and(|value| {
                value.contains("Expired before submission fill window elapsed")
            })
    }));
}

#[test]
fn executed_insight_expiry_requests_cancel_without_marking_cancelled() {
    let created_at = Utc.with_ymd_and_hms(2026, 3, 23, 10, 0, 0).unwrap();
    let now = created_at + Duration::minutes(3);
    let mut ctx = MockStrategyContext::new(now);
    let mut insight = sample_insight(created_at);
    insight.set_period_unfilled(Some(1));
    insight.order_accepted("order-123");

    let expired = insight.has_expired(&mut ctx);

    assert!(expired);
    assert_eq!(insight.state, InsightState::Executed);
    assert!(insight.cancelling);
    assert_eq!(ctx.cancelled_orders.borrow().as_slice(), ["order-123"]);
    assert!(ctx.closed_positions.borrow().is_empty());
    assert!(insight.state_history.iter().any(|(_, state, message)| {
        *state == InsightState::Executed
            && message
                .as_deref()
                .is_some_and(|value| value.contains("cancel requested"))
    }));
}

#[test]
fn filled_insight_expiry_requests_close_without_marking_closed() {
    let created_at = Utc.with_ymd_and_hms(2026, 3, 23, 10, 0, 0).unwrap();
    let fill_time = created_at + Duration::minutes(1);
    let now = fill_time + Duration::minutes(3);
    let mut ctx = MockStrategyContext::new(now);
    let mut insight = sample_insight(created_at);
    insight.set_period_till_tp(Some(1));
    insight.order_accepted("order-456");
    insight.filled_at = Some(fill_time);
    insight.filled_price = Some(1.1542);
    insight.state = InsightState::Filled;
    insight.order_id = Some("order-456".to_string());

    let expired = insight.has_expired(&mut ctx);

    assert!(expired);
    assert_eq!(insight.state, InsightState::Filled);
    assert!(insight.closing);
    assert_eq!(
        ctx.closed_positions.borrow().as_slice(),
        [("order-456".to_string(), 1.0, None)]
    );
    assert!(insight.state_history.iter().any(|(_, state, message)| {
        *state == InsightState::Filled
            && message
                .as_deref()
                .is_some_and(|value| value.contains("Take-profit time window expired"))
    }));
}

#[test]
fn filled_insight_expiry_does_not_duplicate_close_while_pending() {
    let created_at = Utc.with_ymd_and_hms(2026, 3, 23, 10, 0, 0).unwrap();
    let fill_time = created_at + Duration::minutes(1);
    let now = fill_time + Duration::minutes(3);
    let mut ctx = MockStrategyContext::new(now);
    let mut insight = sample_insight(created_at);
    insight.set_period_till_tp(Some(1));
    insight.order_accepted("order-456");
    insight.filled_at = Some(fill_time);
    insight.filled_price = Some(1.1542);
    insight.state = InsightState::Filled;
    insight.order_id = Some("order-456".to_string());

    assert!(insight.has_expired(&mut ctx));
    let history_len_after_first_expiry = insight.state_history.len();
    assert!(insight.has_expired(&mut ctx));

    assert_eq!(insight.state, InsightState::Filled);
    assert!(insight.closing);
    assert_eq!(
        ctx.closed_positions.borrow().as_slice(),
        [("order-456".to_string(), 1.0, None)]
    );
    assert_eq!(insight.state_history.len(), history_len_after_first_expiry);
    let expiry_entries = insight
        .state_history
        .iter()
        .filter(|(_, state, message)| {
            *state == InsightState::Filled
                && message
                    .as_deref()
                    .is_some_and(|value| value.contains("Take-profit time window expired"))
        })
        .count();
    assert_eq!(expiry_entries, 1);
}
