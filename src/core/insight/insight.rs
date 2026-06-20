use uuid::Uuid;

use crate::core::broker::types::{
    OrderClass, OrderLeg, OrderLegs, OrderSide, OrderType, TradeUpdateEvent,
};
use crate::core::utils::timeframe::TimeFrame;

use super::types::{
    InsightState, InsightValidation, PartialCloseResult, StrategyDependentConfirmation,
    StrategyType,
};
use chrono::{DateTime, Utc};
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::Arc,
};

use crate::core::strategy::StrategyContext;
use log::{debug, info, warn};

#[derive(Clone, Copy, PartialEq)]
pub struct InsightOrderLegFingerprint<'a> {
    order_id: Option<&'a str>,
    limit_price: Option<f64>,
    trail_price: Option<f64>,
    side: &'a OrderSide,
    filled_price: Option<f64>,
    order_type: &'a OrderType,
    status: &'a TradeUpdateEvent,
    order_class: &'a OrderClass,
    created_at: u64,
    updated_at: u64,
    submitted_at: u64,
    filled_at: Option<u64>,
}

impl<'a> InsightOrderLegFingerprint<'a> {
    fn new(leg: &'a OrderLeg) -> Self {
        Self {
            order_id: leg.order_id.as_deref(),
            limit_price: leg.limit_price,
            trail_price: leg.trail_price,
            side: &leg.side,
            filled_price: leg.filled_price,
            order_type: &leg.order_type,
            status: &leg.status,
            order_class: &leg.order_class,
            created_at: leg.created_at,
            updated_at: leg.updated_at,
            submitted_at: leg.submitted_at,
            filled_at: leg.filled_at,
        }
    }
}

fn hash_f64<H: Hasher>(state: &mut H, value: f64) {
    value.to_bits().hash(state);
}

fn hash_option_f64<H: Hasher>(state: &mut H, value: Option<f64>) {
    value.map(f64::to_bits).hash(state);
}

fn hash_datetime<H: Hasher>(state: &mut H, value: DateTime<Utc>) {
    value.timestamp().hash(state);
    value.timestamp_subsec_nanos().hash(state);
}

fn hash_option_datetime<H: Hasher>(state: &mut H, value: Option<DateTime<Utc>>) {
    value.is_some().hash(state);
    if let Some(value) = value {
        hash_datetime(state, value);
    }
}

fn hash_levels<H: Hasher>(state: &mut H, levels: Option<&[f64]>) {
    levels.is_some().hash(state);
    if let Some(levels) = levels {
        levels.len().hash(state);
        for level in levels {
            hash_f64(state, *level);
        }
    }
}

fn hash_order_leg<H: Hasher>(state: &mut H, leg: Option<InsightOrderLegFingerprint<'_>>) {
    leg.is_some().hash(state);
    if let Some(leg) = leg {
        leg.order_id.hash(state);
        hash_option_f64(state, leg.limit_price);
        hash_option_f64(state, leg.trail_price);
        std::mem::discriminant(leg.side).hash(state);
        hash_option_f64(state, leg.filled_price);
        std::mem::discriminant(leg.order_type).hash(state);
        std::mem::discriminant(leg.status).hash(state);
        std::mem::discriminant(leg.order_class).hash(state);
        leg.created_at.hash(state);
        leg.updated_at.hash(state);
        leg.submitted_at.hash(state);
        leg.filled_at.hash(state);
    }
}

#[derive(Clone, Copy, PartialEq)]
pub struct InsightSnapshotFingerprint<'a> {
    insight_id: Uuid,
    parent_id: Option<Uuid>,
    state: &'a InsightState,
    children_len: usize,
    order_id: Option<&'a str>,
    side: &'a OrderSide,
    symbol: &'a str,
    quantity: Option<f64>,
    contracts: Option<f64>,
    order_type: &'a OrderType,
    order_class: &'a OrderClass,
    limit_price: Option<f64>,
    stop_price: Option<f64>,
    take_profit_levels: Option<&'a [f64]>,
    stop_loss_levels: Option<&'a [f64]>,
    trailing_stop_price: Option<f64>,
    strategy_type: &'a StrategyType,
    confidence: u8,
    timeframe_amount: u8,
    timeframe_unit: crate::core::utils::timeframe::TimeFrameUnit,
    period_unfilled: Option<u32>,
    period_till_tp: Option<u32>,
    execution_depends: &'a [StrategyDependentConfirmation],
    filled_price: Option<f64>,
    close_order_id: Option<&'a str>,
    close_price: Option<f64>,
    broker_realized_pnl: Option<f64>,
    partial_closes_len: usize,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    filled_at: Option<DateTime<Utc>>,
    closed_at: Option<DateTime<Utc>>,
    take_profit_leg: Option<InsightOrderLegFingerprint<'a>>,
    stop_loss_leg: Option<InsightOrderLegFingerprint<'a>>,
    trailing_stop_leg: Option<InsightOrderLegFingerprint<'a>>,
    market_changed: bool,
    submitted: bool,
    cancelling: bool,
    closing: bool,
    first_on_fill: bool,
    partial_filled_quantity: Option<f64>,
    state_history_len: usize,
}

#[derive(Clone)]
pub struct InsightStrategyContext {
    current_time_fn: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
}

impl std::fmt::Debug for InsightStrategyContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("InsightStrategyContext")
    }
}

impl InsightStrategyContext {
    pub fn new<F>(current_time_fn: F) -> Self
    where
        F: Fn() -> DateTime<Utc> + Send + Sync + 'static,
    {
        Self {
            current_time_fn: Arc::new(current_time_fn),
        }
    }

    pub fn current_time(&self) -> DateTime<Utc> {
        (self.current_time_fn)()
    }
}

#[derive(Clone, Debug)]
pub struct Insight {
    pub insight_id: Uuid,
    pub parent_id: Option<Uuid>,
    // strategy: Strategy,
    pub state: InsightState,
    pub children: Vec<Insight>,

    // Order information
    pub order_id: Option<String>,
    pub side: OrderSide,
    pub symbol: String,
    pub quantity: Option<f64>,
    pub contracts: Option<f64>,

    // Order execution details
    pub order_type: OrderType,
    pub order_class: OrderClass,
    pub limit_price: Option<f64>,
    pub stop_price: Option<f64>,
    pub take_profit_levels: Option<Vec<f64>>,
    pub stop_loss_levels: Option<Vec<f64>>,
    pub trailing_stop_price: Option<f64>,

    // Strategy information
    pub strategy_type: StrategyType,
    pub confidence: u8,
    pub timeframe: TimeFrame,
    pub period_unfilled: Option<u32>,
    pub period_till_tp: Option<u32>,
    pub execution_depends: Vec<StrategyDependentConfirmation>,

    // Closing information
    pub filled_price: Option<f64>,
    pub close_order_id: Option<String>,
    pub close_price: Option<f64>,
    pub broker_realized_pnl: Option<f64>,
    pub partial_closes: Vec<PartialCloseResult>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub filled_at: Option<DateTime<Utc>>,
    pub closed_at: Option<DateTime<Utc>>,
    pub legs: OrderLegs,

    // Market conditions
    pub market_changed: bool,

    // Internal flags
    pub submitted: bool,
    pub cancelling: bool,
    pub closing: bool,
    pub first_on_fill: bool,
    pub partial_filled_quantity: Option<f64>,

    // State tracking
    pub state_history: Vec<(DateTime<Utc>, InsightState, Option<String>)>,
    pub context: Option<InsightStrategyContext>,
}

impl Insight {
    fn active_leg_order_ids(&self) -> Vec<String> {
        let mut order_ids = Vec::new();
        let push_leg = |order_ids: &mut Vec<String>, leg: &crate::core::broker::types::OrderLeg| {
            if leg.status != TradeUpdateEvent::Filled
                && leg.status != TradeUpdateEvent::Closed
                && leg.status != TradeUpdateEvent::Cancelled
                && leg.status != TradeUpdateEvent::Rejected
            {
                if let Some(order_id) = leg.order_id.as_ref() {
                    order_ids.push(order_id.clone());
                }
            }
        };

        if let Some(tp) = self.legs.take_profit.as_ref() {
            push_leg(&mut order_ids, tp);
        }
        if let Some(sl) = self.legs.stop_loss.as_ref() {
            push_leg(&mut order_ids, sl);
        }
        if let Some(trailing) = self.legs.trailing_stop.as_ref() {
            push_leg(&mut order_ids, trailing);
        }
        order_ids
    }

    fn cancel_active_legs(&mut self, ctx: &mut dyn StrategyContext) {
        for order_id in self.active_leg_order_ids() {
            if let Err(e) = ctx.cancel_order(&order_id) {
                warn!(
                    "Failed to cancel leg order {} for {}: {:?}",
                    order_id, self.symbol, e
                );
                continue;
            }
            if let Some(tp) = self.legs.take_profit.as_mut() {
                if tp.order_id.as_deref() == Some(order_id.as_str()) {
                    tp.status = TradeUpdateEvent::Cancelled;
                }
            }
            if let Some(sl) = self.legs.stop_loss.as_mut() {
                if sl.order_id.as_deref() == Some(order_id.as_str()) {
                    sl.status = TradeUpdateEvent::Cancelled;
                }
            }
            if let Some(trailing) = self.legs.trailing_stop.as_mut() {
                if trailing.order_id.as_deref() == Some(order_id.as_str()) {
                    trailing.status = TradeUpdateEvent::Cancelled;
                }
            }
        }
    }

    fn order_side_label(&self) -> &'static str {
        match self.side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        }
    }

    fn entry_summary(&self) -> String {
        let quantity = self
            .quantity
            .map(|qty| format!("{qty:.4}"))
            .unwrap_or_else(|| "?".to_string());

        if let Some(entry_price) = self.entry_price() {
            if self.limit_price.is_some() {
                format!("{quantity} @ {entry_price:.4} LIMIT")
            } else if self.stop_price.is_some() {
                format!("{quantity} @ {entry_price:.4} STOP")
            } else if self.filled_price.is_some() {
                format!("{quantity} @ {entry_price:.4}")
            } else {
                format!("{quantity} @ {entry_price:.4}")
            }
        } else {
            format!("{quantity} @ MARKET")
        }
    }

    fn entry_price(&self) -> Option<f64> {
        self.filled_price.or(self.limit_price).or(self.stop_price)
    }

    fn normalize_levels(levels: Option<Vec<f64>>) -> Option<Vec<f64>> {
        let mut unique = Vec::new();
        for level in levels.unwrap_or_default() {
            if !level.is_finite() {
                continue;
            }
            if !unique
                .iter()
                .any(|existing: &f64| (*existing - level).abs() <= f64::EPSILON)
            {
                unique.push(level);
            }
        }
        if unique.is_empty() {
            None
        } else {
            Some(unique)
        }
    }

    pub fn snapshot_fingerprint(&self) -> InsightSnapshotFingerprint<'_> {
        InsightSnapshotFingerprint {
            insight_id: self.insight_id,
            parent_id: self.parent_id,
            state: &self.state,
            children_len: self.children.len(),
            order_id: self.order_id.as_deref(),
            side: &self.side,
            symbol: &self.symbol,
            quantity: self.quantity,
            contracts: self.contracts,
            order_type: &self.order_type,
            order_class: &self.order_class,
            limit_price: self.limit_price,
            stop_price: self.stop_price,
            take_profit_levels: self.take_profit_levels.as_deref(),
            stop_loss_levels: self.stop_loss_levels.as_deref(),
            trailing_stop_price: self.trailing_stop_price,
            strategy_type: &self.strategy_type,
            confidence: self.confidence,
            timeframe_amount: self.timeframe.get_amount(),
            timeframe_unit: self.timeframe.get_unit(),
            period_unfilled: self.period_unfilled,
            period_till_tp: self.period_till_tp,
            execution_depends: self.execution_depends.as_slice(),
            filled_price: self.filled_price,
            close_order_id: self.close_order_id.as_deref(),
            close_price: self.close_price,
            broker_realized_pnl: self.broker_realized_pnl,
            partial_closes_len: self.partial_closes.len(),
            created_at: self.created_at,
            updated_at: self.updated_at,
            filled_at: self.filled_at,
            closed_at: self.closed_at,
            take_profit_leg: self
                .legs
                .take_profit
                .as_ref()
                .map(InsightOrderLegFingerprint::new),
            stop_loss_leg: self
                .legs
                .stop_loss
                .as_ref()
                .map(InsightOrderLegFingerprint::new),
            trailing_stop_leg: self
                .legs
                .trailing_stop
                .as_ref()
                .map(InsightOrderLegFingerprint::new),
            market_changed: self.market_changed,
            submitted: self.submitted,
            cancelling: self.cancelling,
            closing: self.closing,
            first_on_fill: self.first_on_fill,
            partial_filled_quantity: self.partial_filled_quantity,
            state_history_len: self.state_history.len(),
        }
    }

    pub fn snapshot_fingerprint_hash(&self) -> u64 {
        let fingerprint = self.snapshot_fingerprint();
        let mut state = DefaultHasher::new();

        fingerprint.insight_id.hash(&mut state);
        fingerprint.parent_id.hash(&mut state);
        fingerprint.state.hash(&mut state);
        fingerprint.children_len.hash(&mut state);
        fingerprint.order_id.hash(&mut state);
        std::mem::discriminant(fingerprint.side).hash(&mut state);
        fingerprint.symbol.hash(&mut state);
        hash_option_f64(&mut state, fingerprint.quantity);
        hash_option_f64(&mut state, fingerprint.contracts);
        std::mem::discriminant(fingerprint.order_type).hash(&mut state);
        std::mem::discriminant(fingerprint.order_class).hash(&mut state);
        hash_option_f64(&mut state, fingerprint.limit_price);
        hash_option_f64(&mut state, fingerprint.stop_price);
        hash_levels(&mut state, fingerprint.take_profit_levels);
        hash_levels(&mut state, fingerprint.stop_loss_levels);
        hash_option_f64(&mut state, fingerprint.trailing_stop_price);
        fingerprint.strategy_type.hash(&mut state);
        fingerprint.confidence.hash(&mut state);
        fingerprint.timeframe_amount.hash(&mut state);
        std::mem::discriminant(&fingerprint.timeframe_unit).hash(&mut state);
        fingerprint.period_unfilled.hash(&mut state);
        fingerprint.period_till_tp.hash(&mut state);
        fingerprint.execution_depends.hash(&mut state);
        hash_option_f64(&mut state, fingerprint.filled_price);
        fingerprint.close_order_id.hash(&mut state);
        hash_option_f64(&mut state, fingerprint.close_price);
        hash_option_f64(&mut state, fingerprint.broker_realized_pnl);
        fingerprint.partial_closes_len.hash(&mut state);
        for partial in &self.partial_closes {
            partial.order_id.hash(&mut state);
            std::mem::discriminant(&partial.side).hash(&mut state);
            hash_f64(&mut state, partial.quantity);
            hash_f64(&mut state, partial.entry_price);
            hash_option_f64(&mut state, partial.filled_price);
        }
        hash_datetime(&mut state, fingerprint.created_at);
        hash_datetime(&mut state, fingerprint.updated_at);
        hash_option_datetime(&mut state, fingerprint.filled_at);
        hash_option_datetime(&mut state, fingerprint.closed_at);
        hash_order_leg(&mut state, fingerprint.take_profit_leg);
        hash_order_leg(&mut state, fingerprint.stop_loss_leg);
        hash_order_leg(&mut state, fingerprint.trailing_stop_leg);
        fingerprint.market_changed.hash(&mut state);
        fingerprint.submitted.hash(&mut state);
        fingerprint.cancelling.hash(&mut state);
        fingerprint.closing.hash(&mut state);
        fingerprint.first_on_fill.hash(&mut state);
        hash_option_f64(&mut state, fingerprint.partial_filled_quantity);
        fingerprint.state_history_len.hash(&mut state);
        if let Some((at, state_value, message)) = self.state_history.last() {
            hash_datetime(&mut state, *at);
            state_value.hash(&mut state);
            message.hash(&mut state);
        }

        state.finish()
    }

    pub fn remaining_quantity(&self) -> f64 {
        (self.quantity.unwrap_or(0.0) - self.partial_filled_quantity.unwrap_or(0.0)).max(0.0)
    }

    fn take_profit_summary(&self) -> String {
        self.take_profit_levels
            .as_ref()
            .and_then(|levels| levels.first().copied())
            .map(|price| format!("{price:.4}"))
            .unwrap_or_else(|| "-".to_string())
    }

    fn stop_loss_summary(&self) -> String {
        self.stop_loss_levels
            .as_ref()
            .and_then(|levels| levels.first().copied())
            .map(|price| format!("{price:.4}"))
            .unwrap_or_else(|| "-".to_string())
    }

    pub fn log_summary(&self) -> String {
        format!(
            "id={} {} {} {} strat={} tp={} sl={}",
            self.insight_id,
            self.symbol,
            self.order_side_label(),
            self.entry_summary(),
            self.strategy_type,
            self.take_profit_summary(),
            self.stop_loss_summary()
        )
    }

    pub fn bind_context(&mut self, context: InsightStrategyContext) -> &mut Self {
        self.context = Some(context);
        let now = self.current_time();
        self.align_time_to(now)
    }

    pub fn new(
        side: OrderSide,
        symbol: String,
        strategy_type: StrategyType,
        timeframe: TimeFrame,
        // quantity: Option<f64>,
        // limit_price: Option<f64>,
        // stop_price: Option<f64>,
        // tp: Option<Vec<f64>>,
        // sl: Option<f64>,
        confidence: u8,
        // execution_depends: Vec<StrategyDependentConfirmation>,
        // period_unfilled: Option<i32>,
        // period_till_tp: Option<i32>,
        parent_id: Option<Uuid>,
    ) -> Self {
        Self {
            side,
            symbol,
            strategy_type,
            timeframe,
            // quantity,
            // limit_price,
            // stop_price,
            // take_profit_levels: tp,
            // stop_loss: sl,
            confidence,
            // execution_depends,
            // period_unfilled,
            // period_till_tp,
            parent_id,

            ..Default::default()
        }
    }

    pub fn submit(&mut self, ctx: &mut dyn StrategyContext) -> &mut Self {
        if self.submitted || self.order_id.is_some() || self.state != InsightState::New {
            debug!(
                "Skipping duplicate submit for insight: {} submitted={} order_id={:?} state={:?}",
                self.log_summary(),
                self.submitted,
                self.order_id,
                self.state
            );
            return self;
        }
        if let Err(validation) = self.validate(ctx) {
            let message = match validation {
                InsightValidation::Invalid(message) => message,
                InsightValidation::Valid => "Insight validation failed".to_string(),
            };
            warn!(
                "Insight rejected before submit: {} reason={}",
                self.log_summary(),
                message
            );
            self.order_rejected(&message);
            return self;
        }

        info!("Submitting insight: {}", self.log_summary());
        ctx.submit_insight(self);
        self.update_state(self.state.clone(), Some("Insight submitted".to_string()));
        self
    }

    // ─────────────── Trade Event Helpers ───────────────

    /// Order accepted by broker. Transitions NEW → EXECUTED.
    pub fn order_accepted(&mut self, order_id: &str) {
        info!(
            "Insight executed: {} order_id={}",
            self.log_summary(),
            order_id
        );
        self.order_id = Some(order_id.to_string());
        self.submitted = true;
        self.update_state(
            InsightState::Executed,
            Some(format!("Order accepted: {}", order_id)),
        );
    }

    /// Entry order filled. Transitions EXECUTED → FILLED.
    pub fn position_filled(&mut self, filled_price: f64, filled_qty: f64, order_id: &str) {
        self.order_id = Some(order_id.to_string());
        self.submitted = true;
        self.filled_price = Some(filled_price);
        info!(
            "Insight filled: id={} {} {} qty={:.4} entry={:.4} strat={} order_id={}",
            self.insight_id,
            self.symbol,
            self.order_side_label(),
            filled_qty,
            filled_price,
            self.strategy_type,
            order_id
        );
        self.filled_at = Some(self.current_time());
        self.update_state(
            InsightState::Filled,
            Some(format!(
                "Filled @ {:.4} qty {:.2} order {}",
                filled_price, filled_qty, order_id
            )),
        );
        self.set_first_on_fill(true);
    }

    /// Partial fill received. Tracks cumulative partial fill quantity and registers a partial close result.
    pub fn partial_filled(&mut self, filled_qty: f64, filled_price: f64, order_id: &str) {
        info!(
            "Insight partial fill: id={} {} {} qty={:.4} fill={:.4} strat={} order_id={}",
            self.insight_id,
            self.symbol,
            self.order_side_label(),
            filled_qty,
            filled_price,
            self.strategy_type,
            order_id
        );
        self.filled_price.get_or_insert(filled_price);
        self.partial_filled_quantity =
            Some(self.partial_filled_quantity.unwrap_or(0.0) + filled_qty);

        let mut partial = PartialCloseResult::new(
            order_id.to_string(),
            self.side.clone(),
            filled_qty,
            self.limit_price.unwrap_or(0.0),
        );
        partial.set_filled_price(filled_price);
        self.partial_closes.push(partial);

        self.update_state(
            InsightState::Executed,
            Some(format!(
                "Partial fill: {:.2} @ {:.4}",
                filled_qty, filled_price
            )),
        );
    }

    /// Partial close / scale-out received while the insight remains open.
    pub fn partial_closed(&mut self, close_qty: f64, close_price: f64, close_order_id: &str) {
        info!(
            "Insight partial close: id={} {} {} qty={:.4} exit={:.4} strat={} close_order_id={}",
            self.insight_id,
            self.symbol,
            self.order_side_label(),
            close_qty,
            close_price,
            self.strategy_type,
            close_order_id
        );

        self.partial_filled_quantity =
            Some(self.partial_filled_quantity.unwrap_or(0.0) + close_qty);

        let entry_price = self.entry_price().unwrap_or(0.0);
        let mut partial = PartialCloseResult::new(
            close_order_id.to_string(),
            self.side.clone(),
            close_qty,
            entry_price,
        );
        partial.set_filled_price(close_price);
        self.partial_closes.push(partial);

        self.update_state(
            InsightState::Filled,
            Some(format!(
                "Partial close: {:.2} @ {:.4}",
                close_qty, close_price
            )),
        );
    }

    /// Position closed (SL/TP/manual). Transitions FILLED → CLOSED.
    pub fn position_closed(
        &mut self,
        close_price: f64,
        close_order_id: &str,
        _close_qty: f64,
        broker_realized_pnl: Option<f64>,
    ) {
        self.close_price = Some(close_price);
        self.close_order_id = Some(close_order_id.to_string());
        self.broker_realized_pnl = broker_realized_pnl;
        self.closed_at = Some(self.current_time());
        let pnl = self.get_pl(None, true);
        let pnl_pct = self.get_pl_pct();
        info!(
            "Insight closed: id={} {} {} {} -> {:.4} pnl={:.2} ({:.2}%) strat={} close_order_id={}",
            self.insight_id,
            self.symbol,
            self.order_side_label(),
            self.entry_summary(),
            close_price,
            pnl,
            pnl_pct,
            self.strategy_type,
            close_order_id
        );
        self.update_state(
            InsightState::Closed,
            Some(format!(
                "Closed @ {:.4} order {}",
                close_price, close_order_id
            )),
        );
    }

    /// Order rejected by broker. Transitions → REJECTED.
    pub fn order_rejected(&mut self, reason: &str) {
        info!("Insight rejected: {} reason={}", self.log_summary(), reason);
        self.update_state(
            InsightState::Rejected,
            Some(format!("Rejected: {}", reason)),
        );
    }

    /// Order cancelled. Transitions → CANCELLED.
    pub fn order_cancelled(&mut self, reason: &str) {
        info!(
            "Insight cancelled: {} reason={}",
            self.log_summary(),
            reason
        );
        self.update_state(
            InsightState::Cancelled,
            Some(format!("Cancelled: {}", reason)),
        );
    }

    pub fn cancel(&mut self, ctx: &mut dyn StrategyContext) -> &mut Self {
        if self.cancelling {
            return self;
        }
        self.cancelling = true;

        if let Some(order_id) = &self.order_id {
            if let Err(e) = ctx.cancel_order(order_id) {
                warn!("Failed to cancel order {}: {:?}", order_id, e);
                self.cancelling = false;
                return self;
            }
        } else {
            self.cancelling = false;
            return self;
        }

        debug!("Cancel requested for insight: {}", self.log_summary());
        self.update_state(self.state.clone(), Some("Insight cancelled".to_string()));
        self
    }
    pub fn cancel_order_by_id(
        &mut self,
        order_id: String,
        ctx: &mut dyn StrategyContext,
    ) -> &mut Self {
        self.cancelling = true;
        if let Err(e) = ctx.cancel_order(&order_id) {
            warn!("Failed to cancel order {}: {:?}", order_id, e);
            self.cancelling = false;
            return self;
        }
        debug!(
            "Cancel requested by order id for insight: {} order_id={}",
            self.log_summary(),
            order_id
        );
        self.update_state(
            self.state.clone(),
            Some(format!("Insight cancelled by order id {}", order_id)),
        );
        self
    }
    pub fn close(&mut self, ctx: &mut dyn StrategyContext) -> &mut Self {
        if self.closing {
            return self;
        }
        self.closing = true;
        let qty = self.remaining_quantity();
        let Some(order_id) = self.order_id.clone() else {
            warn!(
                "Failed to close position for {}: no filled order id is associated with the insight",
                self.symbol
            );
            self.closing = false;
            return self;
        };
        self.cancel_active_legs(ctx);
        if let Err(e) = ctx.close_position(&order_id, qty, None) {
            warn!("Failed to close position for {}: {:?}", self.symbol, e);
            self.closing = false;
            return self;
        }
        self.update_state(self.state.clone(), Some("Insight closed".to_string()));
        self
    }

    pub fn close_partial(
        &mut self,
        ctx: &mut dyn StrategyContext,
        quantity: f64,
        price: Option<f64>,
    ) -> &mut Self {
        if self.state == InsightState::Filled {
            let remaining = self.remaining_quantity();
            if quantity <= 0.0 || quantity > remaining {
                warn!(
                    "Failed to close partial position for {}: invalid quantity {:.4} with remaining {:.4}",
                    self.symbol, quantity, remaining
                );
                return self;
            }
            if (quantity - remaining).abs() <= f64::EPSILON {
                return self.close(ctx);
            }
            let Some(order_id) = self.order_id.clone() else {
                warn!(
                    "Failed to close partial position for {}: no filled order id is associated with the insight",
                    self.symbol
                );
                return self;
            };
            if let Err(e) = ctx.close_position(&order_id, quantity, price) {
                warn!(
                    "Failed to close partial position for {}: {:?}",
                    self.symbol, e
                );
            }
        }
        self
    }

    pub fn get_pl(&self, current_price: Option<f64>, include_partial_closes: bool) -> f64 {
        let price = self.close_price.or(current_price).unwrap_or(0.0);
        let mut partial_pl = 0.0;

        if include_partial_closes {
            for partial in &self.partial_closes {
                partial_pl += partial.get_pl();
            }
        }

        let entry = self.entry_price().unwrap_or(0.0);
        let remaining_qty = self.remaining_quantity();

        if self.close_price.is_some() {
            if let Some(realized_pnl) = self.broker_realized_pnl {
                return ((realized_pnl + partial_pl) * 100.0).round() / 100.0;
            }
        }

        let remaining_pl = if self.side == OrderSide::Buy {
            (price - entry) * remaining_qty
        } else {
            (entry - price) * remaining_qty
        };

        // Round the combined P&L to 2 decimals.
        ((remaining_pl + partial_pl) * 100.0).round() / 100.0
    }

    pub fn get_pl_pct(&self) -> f64 {
        let Some(entry) = self.entry_price() else {
            return 0.0;
        };
        let qty = self.quantity.unwrap_or(0.0);
        if entry.abs() <= f64::EPSILON || qty <= f64::EPSILON {
            return 0.0;
        }

        if let Some(realized_pnl) = self.broker_realized_pnl {
            let notional = entry * qty;
            if notional.abs() <= f64::EPSILON {
                return 0.0;
            }
            let raw = (realized_pnl / notional) * 100.0;
            return (raw * 100.0).round() / 100.0;
        }

        let Some(close) = self.close_price else {
            return 0.0;
        };

        let raw = match self.side {
            OrderSide::Buy => ((close - entry) / entry) * 100.0,
            OrderSide::Sell => ((entry - close) / entry) * 100.0,
        };
        (raw * 100.0).round() / 100.0
    }

    pub fn get_pl_ratio(&self) -> f64 {
        if let (Some(tp_levels), Some(sl_levels), Some(limit)) = (
            &self.take_profit_levels,
            &self.stop_loss_levels,
            self.limit_price,
        ) {
            let Some(sl) = sl_levels.last().copied() else {
                return 0.0;
            };
            if let Some(max_tp) = tp_levels.last() {
                let risk = (limit - sl).abs();
                if risk > 0.0 {
                    return ((max_tp - limit).abs() / risk * 100.0).round() / 100.0;
                }
            }
        }
        0.0
    }
    pub fn add_child_insight(
        &mut self,
        mut child_insight: Insight,
        _ctx: &mut dyn StrategyContext,
    ) -> &mut Self {
        // self.children_ids.push(insight_id);
        child_insight.strategy_type =
            StrategyType::Custom(format!("{}-CHILD", self.strategy_type.to_string()));
        child_insight.parent_id = Some(self.insight_id);
        if child_insight.quantity.is_none() {
            child_insight.quantity = self.quantity;
        }

        self.children.push(child_insight);
        self
    }
    pub fn update_market_changed(
        &mut self,
        ctx: &mut dyn StrategyContext,
        market_changed: bool,
        should_close_or_cancel: bool,
    ) -> &mut Self {
        self.market_changed = market_changed;
        self.update_state(self.state.clone(), Some("Market changed".to_string()));
        if should_close_or_cancel {
            match self.state {
                InsightState::Filled => {
                    self.close(ctx);
                }
                _ => {
                    self.cancel(ctx);
                }
            };
        }

        self
    }

    pub fn validate(&mut self, ctx: &mut dyn StrategyContext) -> Result<bool, InsightValidation> {
        let asset = ctx.universe().get(&self.symbol).ok_or_else(|| {
            InsightValidation::Invalid(format!("Symbol {} not in universe", self.symbol))
        })?;

        if !asset.tradable {
            return Err(InsightValidation::Invalid(format!(
                "Asset {} is not tradable",
                self.symbol
            )));
        }

        if self.side == OrderSide::Sell && !asset.shortable {
            return Err(InsightValidation::Invalid(format!(
                "Asset {} does not allow shorting",
                self.symbol
            )));
        }

        if self.market_changed {
            return Err(InsightValidation::Invalid(format!(
                "Market changed for {}",
                self.symbol
            )));
        }

        let quantity = self.quantity.ok_or_else(|| {
            InsightValidation::Invalid(format!("Missing quantity for {}", self.symbol))
        })?;

        if !quantity.is_finite() || quantity <= 0.0 {
            return Err(InsightValidation::Invalid(format!(
                "Invalid quantity {} for {}",
                quantity, self.symbol
            )));
        }

        if let Some(min_order_size) = asset.min_order_size {
            if quantity < min_order_size {
                return Err(InsightValidation::Invalid(format!(
                    "Quantity {} is below minimum order size {} for {}",
                    quantity, min_order_size, self.symbol
                )));
            }
        }

        if let Some(max_order_size) = asset.max_order_size {
            if quantity > max_order_size {
                return Err(InsightValidation::Invalid(format!(
                    "Quantity {} exceeds maximum order size {} for {}",
                    quantity, max_order_size, self.symbol
                )));
            }
        }

        if self.has_expired(ctx) {
            return Err(InsightValidation::Invalid(format!(
                "Insight {} has expired",
                self.insight_id
            )));
        }

        if !self.has_valid_entry_conditions() {
            return Err(InsightValidation::Invalid(format!(
                "Invalid entry conditions for {}",
                self.symbol
            )));
        }

        if !ctx.universe().contains_key(&self.symbol) {
            return Err(InsightValidation::Invalid(format!(
                "Symbol {} not in universe",
                self.symbol
            )));
        }
        Ok(true)
    }

    pub fn has_expired(&mut self, ctx: &mut dyn StrategyContext) -> bool {
        let now = self.reference_time(ctx);

        match self.state {
            InsightState::New => {
                let Some(period_unfilled) = self.period_unfilled else {
                    return false;
                };
                let expiry = self
                    .timeframe
                    .add_time_increment(self.created_at, period_unfilled as i64)
                    .unwrap_or(self.created_at);
                if now >= expiry {
                    self.order_rejected("Expired before submission fill window elapsed");
                    return true;
                }
                false
            }
            InsightState::Executed => {
                let Some(period_unfilled) = self.period_unfilled else {
                    return false;
                };
                let expiry = self
                    .timeframe
                    .add_time_increment(self.created_at, period_unfilled as i64)
                    .unwrap_or(self.created_at);
                if now >= expiry {
                    let was_cancelling = self.cancelling;
                    self.cancel(ctx);
                    if !was_cancelling && self.cancelling {
                        self.update_state(
                            self.state.clone(),
                            Some(
                                "Expired before fill window elapsed; cancel requested".to_string(),
                            ),
                        );
                    }
                    return true;
                }
                false
            }
            InsightState::Filled => {
                let Some(period_till_tp) = self.period_till_tp else {
                    return false;
                };
                let anchor = self.filled_at.unwrap_or(self.created_at);
                let expiry = self
                    .timeframe
                    .add_time_increment(anchor, period_till_tp as i64)
                    .unwrap_or(anchor);
                if now >= expiry {
                    let was_closing = self.closing;
                    self.close(ctx);
                    if !was_closing && self.closing {
                        self.update_state(
                            self.state.clone(),
                            Some("Take-profit time window expired; closing position".to_string()),
                        );
                    }
                    return true;
                }
                false
            }
            _ => false,
        }
    }

    fn reference_time(&self, ctx: &dyn StrategyContext) -> DateTime<Utc> {
        ctx.current_time()
    }

    fn has_valid_entry_conditions(&self) -> bool {
        let Some(entry) = self.limit_price.or(self.stop_price) else {
            return true;
        };

        match self.side {
            OrderSide::Buy => {
                let tp_ok = self
                    .take_profit_levels
                    .as_ref()
                    .map(|levels| levels.iter().all(|tp| *tp > entry))
                    .unwrap_or(true);
                let sl_ok = self
                    .stop_loss_levels
                    .as_ref()
                    .map(|levels| levels.iter().all(|sl| *sl < entry))
                    .unwrap_or(true);
                tp_ok && sl_ok
            }
            OrderSide::Sell => {
                let tp_ok = self
                    .take_profit_levels
                    .as_ref()
                    .map(|levels| levels.iter().all(|tp| *tp < entry))
                    .unwrap_or(true);
                let sl_ok = self
                    .stop_loss_levels
                    .as_ref()
                    .map(|levels| levels.iter().all(|sl| *sl > entry))
                    .unwrap_or(true);
                tp_ok && sl_ok
            }
        }
    }

    // Order information
    pub fn quantity(&self) -> Option<f64> {
        self.quantity
    }
    pub fn set_quantity(&mut self, quantity: Option<f64>) -> &mut Self {
        if self.quantity == quantity {
            return self;
        }
        self.quantity = quantity;
        self.update_state(
            self.state.clone(),
            Some(format!("Quantity set to {}", quantity.unwrap_or(0.0))),
        );
        self
    }
    pub fn limit_price(&self) -> Option<f64> {
        self.limit_price
    }
    pub fn set_limit_price(&mut self, limit_price: Option<f64>) -> &mut Self {
        if self.limit_price == limit_price {
            return self;
        }
        self.limit_price = limit_price;
        self.update_order_type();
        self.update_state(
            self.state.clone(),
            Some(format!("Limit price set to {}", limit_price.unwrap_or(0.0))),
        );
        self
    }
    pub fn stop_price(&self) -> Option<f64> {
        self.stop_price
    }
    pub fn set_stop_price(&mut self, stop_price: Option<f64>) -> &mut Self {
        if self.stop_price == stop_price {
            return self;
        }
        self.stop_price = stop_price;
        self.update_order_type();
        self.update_state(
            self.state.clone(),
            Some(format!("Stop price set to {}", stop_price.unwrap_or(0.0))),
        );
        self
    }
    pub fn take_profit_levels(&self) -> Option<Vec<f64>> {
        self.take_profit_levels.clone()
    }
    pub fn set_take_profit_levels(&mut self, take_profit_levels: Option<Vec<f64>>) -> &mut Self {
        let normalized_levels = Self::normalize_levels(take_profit_levels);
        if self.take_profit_levels == normalized_levels {
            return self;
        }
        self.take_profit_levels = normalized_levels;
        self.update_order_class();
        self.update_state(
            self.state.clone(),
            Some(format!(
                "Take profit levels set to {:?}",
                self.take_profit_levels.as_deref().unwrap_or(&[])
            )),
        );
        self
    }
    pub fn update_take_profit(
        &mut self,
        ctx: &mut dyn StrategyContext,
        take_profit_levels: Option<Vec<f64>>,
    ) -> bool {
        let normalized_levels = Self::normalize_levels(take_profit_levels);
        if self.take_profit_levels == normalized_levels {
            return true;
        }

        let previous_levels = self.take_profit_levels.clone();
        let target_tp = normalized_levels
            .as_ref()
            .and_then(|levels| levels.first().copied());
        let Some(target_tp) = target_tp else {
            self.take_profit_levels = None;
            self.update_order_class();
            self.update_state(
                self.state.clone(),
                Some("Take profit levels cleared".to_string()),
            );
            return true;
        };

        let active_tp_leg_without_order_id = self
            .legs
            .take_profit
            .as_ref()
            .map(|leg| {
                !matches!(
                    leg.status,
                    TradeUpdateEvent::Filled
                        | TradeUpdateEvent::Closed
                        | TradeUpdateEvent::Cancelled
                        | TradeUpdateEvent::Rejected
                ) && leg.order_id.is_none()
            })
            .unwrap_or(false);
        let tp_leg_order_id = self
            .legs
            .take_profit
            .as_ref()
            .filter(|leg| {
                !matches!(
                    leg.status,
                    TradeUpdateEvent::Filled
                        | TradeUpdateEvent::Closed
                        | TradeUpdateEvent::Cancelled
                        | TradeUpdateEvent::Rejected
                )
            })
            .and_then(|leg| leg.order_id.clone());
        let broker_order_id = tp_leg_order_id.or_else(|| {
            if self.state == InsightState::Filled && active_tp_leg_without_order_id {
                self.order_id.clone()
            } else {
                None
            }
        });

        if self.state == InsightState::Filled
            && active_tp_leg_without_order_id
            && broker_order_id.is_none()
        {
            self.update_state(
                self.state.clone(),
                Some("Failed to update take profit: filled insight has no broker order id".into()),
            );
            return false;
        }

        if let Some(order_id) = broker_order_id.as_ref() {
            let qty = self.remaining_quantity();
            if !qty.is_finite() || qty <= 0.0 {
                self.update_state(
                    self.state.clone(),
                    Some(format!(
                        "Failed to update take profit for order {order_id}: invalid remaining quantity {qty}"
                    )),
                );
                return false;
            }
            match ctx.update_order(order_id, target_tp, qty) {
                Ok(true) => {}
                Ok(false) => {
                    self.update_state(
                        self.state.clone(),
                        Some(format!(
                            "Failed to update take profit for order {order_id}: broker returned false"
                        )),
                    );
                    return false;
                }
                Err(err) => {
                    self.update_state(
                        self.state.clone(),
                        Some(format!(
                            "Failed to update take profit for order {order_id}: {err}"
                        )),
                    );
                    return false;
                }
            }
        }

        self.take_profit_levels = normalized_levels;
        if let Some(tp) = self.legs.take_profit.as_mut() {
            if !matches!(
                tp.status,
                TradeUpdateEvent::Filled
                    | TradeUpdateEvent::Closed
                    | TradeUpdateEvent::Cancelled
                    | TradeUpdateEvent::Rejected
            ) {
                tp.limit_price = Some(target_tp);
                tp.updated_at = ctx.current_time().timestamp().max(0) as u64;
            }
        }
        self.update_order_class();
        self.update_state(
            self.state.clone(),
            Some(format!(
                "Updated Take Profit For Order: {}: {:?} -> {:?}",
                broker_order_id.as_deref().unwrap_or("local"),
                previous_levels.as_deref().unwrap_or(&[]),
                self.take_profit_levels.as_deref().unwrap_or(&[])
            )),
        );
        true
    }
    pub fn add_take_profit_levels(&mut self, levels: Vec<f64>) -> &mut Self {
        let mut combined = self.take_profit_levels.clone().unwrap_or_default();
        combined.extend(levels);
        self.set_take_profit_levels(Some(combined))
    }
    pub fn stop_loss_levels(&self) -> Option<Vec<f64>> {
        self.stop_loss_levels.clone()
    }
    pub fn set_stop_loss_levels(&mut self, stop_loss_levels: Option<Vec<f64>>) -> &mut Self {
        let normalized_levels = Self::normalize_levels(stop_loss_levels);
        if self.stop_loss_levels == normalized_levels {
            return self;
        }
        self.stop_loss_levels = normalized_levels;
        self.update_order_class();
        self.update_state(
            self.state.clone(),
            Some(format!(
                "Stop loss levels set to {:?}",
                self.stop_loss_levels.as_deref().unwrap_or(&[])
            )),
        );
        self
    }
    pub fn update_stop_loss(
        &mut self,
        ctx: &mut dyn StrategyContext,
        stop_loss_levels: Option<Vec<f64>>,
    ) -> bool {
        let normalized_levels = Self::normalize_levels(stop_loss_levels);
        if self.stop_loss_levels == normalized_levels {
            return true;
        }

        let previous_levels = self.stop_loss_levels.clone();
        let target_sl = normalized_levels
            .as_ref()
            .and_then(|levels| levels.first().copied());
        let Some(target_sl) = target_sl else {
            self.stop_loss_levels = None;
            self.update_order_class();
            self.update_state(
                self.state.clone(),
                Some("Stop loss levels cleared".to_string()),
            );
            return true;
        };

        let active_sl_leg_without_order_id = self
            .legs
            .stop_loss
            .as_ref()
            .map(|leg| {
                !matches!(
                    leg.status,
                    TradeUpdateEvent::Filled
                        | TradeUpdateEvent::Closed
                        | TradeUpdateEvent::Cancelled
                        | TradeUpdateEvent::Rejected
                ) && leg.order_id.is_none()
            })
            .unwrap_or(false);
        let sl_leg_order_id = self
            .legs
            .stop_loss
            .as_ref()
            .filter(|leg| {
                !matches!(
                    leg.status,
                    TradeUpdateEvent::Filled
                        | TradeUpdateEvent::Closed
                        | TradeUpdateEvent::Cancelled
                        | TradeUpdateEvent::Rejected
                )
            })
            .and_then(|leg| leg.order_id.clone());
        let broker_order_id = sl_leg_order_id.or_else(|| {
            if self.state == InsightState::Filled && active_sl_leg_without_order_id {
                self.order_id.clone()
            } else {
                None
            }
        });

        if self.state == InsightState::Filled
            && active_sl_leg_without_order_id
            && broker_order_id.is_none()
        {
            self.update_state(
                self.state.clone(),
                Some("Failed to update stop loss: filled insight has no broker order id".into()),
            );
            return false;
        }

        if let Some(order_id) = broker_order_id.as_ref() {
            let qty = self.remaining_quantity();
            if !qty.is_finite() || qty <= 0.0 {
                self.update_state(
                    self.state.clone(),
                    Some(format!(
                        "Failed to update stop loss for order {order_id}: invalid remaining quantity {qty}"
                    )),
                );
                return false;
            }
            match ctx.update_stop_loss_order(order_id, target_sl, qty) {
                Ok(true) => {}
                Ok(false) => {
                    self.update_state(
                        self.state.clone(),
                        Some(format!(
                            "Failed to update stop loss for order {order_id}: broker returned false"
                        )),
                    );
                    return false;
                }
                Err(err) => {
                    self.update_state(
                        self.state.clone(),
                        Some(format!(
                            "Failed to update stop loss for order {order_id}: {err}"
                        )),
                    );
                    return false;
                }
            }
        }

        self.stop_loss_levels = normalized_levels;
        if let Some(sl) = self.legs.stop_loss.as_mut() {
            if !matches!(
                sl.status,
                TradeUpdateEvent::Filled
                    | TradeUpdateEvent::Closed
                    | TradeUpdateEvent::Cancelled
                    | TradeUpdateEvent::Rejected
            ) {
                sl.limit_price = Some(target_sl);
                sl.updated_at = ctx.current_time().timestamp().max(0) as u64;
            }
        }
        self.update_order_class();
        self.update_state(
            self.state.clone(),
            Some(format!(
                "Updated Stop Loss For Order: {}: {:?} -> {:?}",
                broker_order_id.as_deref().unwrap_or("local"),
                previous_levels.as_deref().unwrap_or(&[]),
                self.stop_loss_levels.as_deref().unwrap_or(&[])
            )),
        );
        true
    }
    pub fn add_stop_loss_levels(&mut self, levels: Vec<f64>) -> &mut Self {
        let mut combined = self.stop_loss_levels.clone().unwrap_or_default();
        combined.extend(levels);
        self.set_stop_loss_levels(Some(combined))
    }
    pub fn stop_loss(&self) -> Option<f64> {
        self.stop_loss_levels
            .as_ref()
            .and_then(|levels| levels.first().copied())
    }
    pub fn set_stop_loss(&mut self, stop_loss: Option<f64>) -> &mut Self {
        self.set_stop_loss_levels(stop_loss.map(|value| vec![value]))
    }
    pub fn trailing_stop_price(&self) -> Option<f64> {
        self.trailing_stop_price
    }
    pub fn set_trailing_stop_price(&mut self, trailing_stop_price: Option<f64>) -> &mut Self {
        if self.trailing_stop_price == trailing_stop_price {
            return self;
        }
        self.trailing_stop_price = trailing_stop_price;
        self.update_order_class();
        self.update_state(
            self.state.clone(),
            Some(format!(
                "Trailing stop set to {}",
                trailing_stop_price
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string())
            )),
        );
        self
    }

    pub fn order_class(&self) -> &OrderClass {
        &self.order_class
    }
    fn update_order_class(&mut self) -> &mut Self {
        let order_class = match (
            self.take_profit_levels
                .as_ref()
                .map(|v| !v.is_empty())
                .unwrap_or(false),
            self.stop_loss_levels
                .as_ref()
                .map(|v| !v.is_empty())
                .unwrap_or(false),
            self.trailing_stop_price.is_some(),
        ) {
            (true, true, _) => OrderClass::Bracket,
            (true, false, _) => OrderClass::Bracket,
            (false, true, _) => OrderClass::Bracket,
            (false, false, true) => OrderClass::Bracket,
            (false, false, false) => OrderClass::Simple,
        };
        if self.order_class == order_class {
            return self;
        }
        self.order_class = order_class;
        self.update_state(
            self.state.clone(),
            Some(format!("Order class set to {:?}", self.order_class)),
        );
        self
    }
    pub fn order_type(&self) -> &OrderType {
        &self.order_type
    }
    fn update_order_type(&mut self) -> &mut Self {
        let order_type = match (self.limit_price, self.stop_price) {
            (Some(_), Some(_)) => OrderType::StopLimit,
            (Some(_), None) => OrderType::Limit,
            (None, Some(_)) => OrderType::Stop,
            (None, None) => OrderType::Market,
        };
        if self.order_type == order_type {
            return self;
        }
        self.order_type = order_type;
        self.update_state(
            self.state.clone(),
            Some(format!("Order type set to {:?}", self.order_type)),
        );
        self
    }

    pub fn insight_id(&self) -> &Uuid {
        &self.insight_id
    }
    pub fn parent_id(&self) -> Option<&Uuid> {
        self.parent_id.as_ref()
    }
    pub fn set_parent_id(&mut self, parent_id: Uuid) -> &mut Self {
        if self.parent_id == Some(parent_id) {
            return self;
        }
        self.parent_id = Some(parent_id);
        self.update_state(
            self.state.clone(),
            Some(format!("Parent ID set to {}", parent_id)),
        );
        self
    }
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    pub fn state(&self) -> &InsightState {
        &self.state
    }
    pub fn side(&self) -> &OrderSide {
        &self.side
    }
    pub fn set_side(&mut self, side: OrderSide) -> &mut Self {
        if (self.side == side && self.state != InsightState::New)
            || self.state == InsightState::Filled
            || self.state == InsightState::Executed
        {
            return self;
        }
        self.side = side;
        self.update_state(
            self.state.clone(),
            Some(format!("Order side set to {:?}", self.side)),
        );
        self
    }

    pub fn opposite_side(&self) -> OrderSide {
        match self.side {
            OrderSide::Buy => OrderSide::Sell,
            OrderSide::Sell => OrderSide::Buy,
        }
    }
    pub fn strategy_type(&self) -> &StrategyType {
        &self.strategy_type
    }
    pub fn confidence(&self) -> u8 {
        self.confidence
    }
    pub fn timeframe(&self) -> &TimeFrame {
        &self.timeframe
    }
    pub fn period_unfilled(&self) -> Option<u32> {
        self.period_unfilled
    }
    pub fn can_expire(&self) -> bool {
        match self.state {
            InsightState::New | InsightState::Executed => self.period_unfilled.is_some(),
            InsightState::Filled => self.period_till_tp.is_some(),
            _ => false,
        }
    }
    pub fn set_period_unfilled(&mut self, period_unfilled: Option<u32>) -> &mut Self {
        if self.period_unfilled == period_unfilled {
            return self;
        }
        self.period_unfilled = period_unfilled;
        self.update_state(
            self.state.clone(),
            Some(format!(
                "Unfilled TTL set to {}",
                period_unfilled
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string())
            )),
        );
        self
    }
    pub fn period_till_tp(&self) -> Option<u32> {
        self.period_till_tp
    }
    pub fn set_period_till_tp(&mut self, period_till_tp: Option<u32>) -> &mut Self {
        if self.period_till_tp == period_till_tp {
            return self;
        }
        self.period_till_tp = period_till_tp;
        self.update_state(
            self.state.clone(),
            Some(format!(
                "Take-profit TTL set to {}",
                period_till_tp
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string())
            )),
        );
        self
    }
    pub fn execution_depends(&self) -> &Vec<StrategyDependentConfirmation> {
        &self.execution_depends
    }
    pub fn set_execution_depends(
        &mut self,
        execution_depends: Vec<StrategyDependentConfirmation>,
    ) -> &mut Self {
        if self.execution_depends == execution_depends {
            return self;
        }
        self.execution_depends = execution_depends;
        self.update_state(
            self.state.clone(),
            Some(format!(
                "Execution depends set to {:?}",
                self.execution_depends
            )),
        );
        self
    }
    pub fn first_on_fill(&self) -> bool {
        self.first_on_fill
    }
    pub fn set_first_on_fill(&mut self, first_on_fill: bool) -> &mut Self {
        if self.first_on_fill == first_on_fill {
            return self;
        }
        self.first_on_fill = first_on_fill;
        self.update_state(
            self.state.clone(),
            Some(format!("first_on_fill set to {}", first_on_fill)),
        );
        self
    }

    // State management
    fn update_state(&mut self, new_state: InsightState, message: Option<String>) {
        let at = self.current_time();
        self.updated_at = at;
        if matches!(new_state, InsightState::Filled) {
            self.cancelling = false;
        }
        if new_state.is_inactive() {
            self.cancelling = false;
            self.closing = false;
        }
        if self.state != new_state {
            self.state_history.push((
                at,
                new_state.clone(),
                format!(
                    "State changed from {:?} to {:?}",
                    self.state,
                    new_state.clone()
                )
                .into(),
            ));
            self.state = new_state.clone();
        }
        self.state_history.push((at, new_state, message));
    }

    pub fn state_log(&mut self, message: Option<String>) -> &mut Self {
        self.update_state(
            self.state.clone(),
            Some(message.unwrap_or_else(|| "State log entry".to_string())),
        );
        self
    }

    pub fn align_time_to(&mut self, at: DateTime<Utc>) -> &mut Self {
        self.created_at = at;
        self.updated_at = at;
        for (timestamp, _, _) in &mut self.state_history {
            *timestamp = at;
        }
        self
    }

    fn current_time(&self) -> DateTime<Utc> {
        self.context
            .as_ref()
            .map(|context| context.current_time())
            .unwrap_or_else(Utc::now)
    }
}

impl Default for Insight {
    fn default() -> Self {
        Self {
            insight_id: Uuid::new_v4(),
            // strategy: None,
            parent_id: None,
            children: Vec::new(),
            state: InsightState::New,

            // Order information
            order_id: None,
            side: OrderSide::Buy,
            symbol: String::new(),
            quantity: None,
            contracts: None,

            // Order execution details
            order_type: OrderType::Market,
            order_class: OrderClass::Simple,
            limit_price: None,
            stop_price: None,
            take_profit_levels: None,
            stop_loss_levels: None,
            trailing_stop_price: None,

            // Strategy information
            strategy_type: StrategyType::Manual,
            confidence: 0,
            timeframe: TimeFrame::default(),
            period_unfilled: None,
            period_till_tp: None,
            execution_depends: Vec::new(),

            // Closing information
            filled_price: None,
            close_order_id: None,
            close_price: None,
            broker_realized_pnl: None,
            partial_closes: Vec::new(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            filled_at: None,
            closed_at: None,
            legs: OrderLegs::default(),

            // Market conditions
            market_changed: false,

            // Internal flags
            submitted: false,
            cancelling: false,
            closing: false,
            first_on_fill: false,
            partial_filled_quantity: None,

            // State tracking
            state_history: Vec::new(),
            context: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::alpha::WrappedAlphaModel;
    use crate::core::broker::types::{
        Account, AccountType, Asset, AssetExchange, AssetStatus, AssetType, BrokerError, OrderSide,
        Quote,
    };
    use crate::core::indicators::Indicator;
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

        fn calculate_time_to_live(
            &self,
            _price: f64,
            _entry: f64,
            _atr: f64,
            additional: i32,
        ) -> i32 {
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
        updated_stop_losses: RefCell<Vec<(String, f64, f64)>>,
        closed_positions: RefCell<Vec<(String, f64, Option<f64>)>>,
        account: Account,
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
                min_price_increment: Some(0.0001),
                price_base: Some(4),
                contract_size: None,
            };

            Self {
                universe: HashMap::from([(asset.symbol.clone(), asset)]),
                history: HashMap::new(),
                insights: crate::core::insight::InsightCollection::new(),
                variables: DashMap::new(),
                current_time,
                cancelled_orders: RefCell::new(Vec::new()),
                updated_stop_losses: RefCell::new(Vec::new()),
                closed_positions: RefCell::new(Vec::new()),
                account: Account {
                    account_id: "paper".to_string(),
                    account_type: AccountType::Paper,
                    equity: 100_000.0,
                    cash: 100_000.0,
                    currency: "USD".to_string(),
                    buying_power: 100_000.0,
                    shorting_enabled: true,
                    leverage: 1,
                },
            }
        }
    }

    impl StrategyContext for MockStrategyContext {
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

        fn submit_insight(&mut self, _insight: &mut Insight) {}

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

        fn cancel_order(&self, order_id: &str) -> Result<bool, BrokerError> {
            self.cancelled_orders
                .borrow_mut()
                .push(order_id.to_string());
            Ok(true)
        }

        fn update_order(
            &self,
            _order_id: &str,
            _price: f64,
            _qty: f64,
        ) -> Result<bool, BrokerError> {
            Ok(true)
        }

        fn update_stop_loss_order(
            &self,
            order_id: &str,
            price: f64,
            qty: f64,
        ) -> Result<bool, BrokerError> {
            self.updated_stop_losses
                .borrow_mut()
                .push((order_id.to_string(), price, qty));
            Ok(true)
        }

        fn close_position(
            &self,
            order_id: &str,
            qty: f64,
            price: Option<f64>,
        ) -> Result<bool, BrokerError> {
            self.closed_positions
                .borrow_mut()
                .push((order_id.to_string(), qty, price));
            Ok(true)
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

        assert!(insight.update_stop_loss(&mut ctx, Some(vec![92.5])));

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

        parent.add_child_insight(child, &mut ctx);

        assert_eq!(parent.children.len(), 1);
        assert_eq!(parent.children[0].parent_id, Some(parent_id));
        assert_eq!(parent.children[0].quantity, parent.quantity);
        assert!(matches!(
            parent.children[0].strategy_type,
            StrategyType::Custom(ref value) if value == "Testing-CHILD"
        ));
        assert_eq!(parent.state_history.len(), parent_history_len);
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
        assert!(
            insight
                .state_history
                .iter()
                .any(|(_, state, message)| *state == InsightState::Rejected
                    && message.as_deref().is_some_and(
                        |value| value.contains("Expired before submission fill window elapsed")
                    ))
        );
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
}
