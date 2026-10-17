use uuid::Uuid;

use crate::core::broker::types::{
    OrderClass, OrderLeg, OrderLegs, OrderSide, OrderType, TradeUpdateEvent,
};
use crate::core::utils::timeframe::TimeFrame;

use super::types::{
    InsightHistoryKind, InsightState, InsightStrategyContext, InsightValidation,
    PartialCloseResult, StrategyDependentConfirmation, StrategyType,
};
use chrono::{DateTime, Utc};
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use crate::core::strategy::StrategyContext;
use log::{info, warn};

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
    is_split_insight: bool,
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
    position_accounting: Option<&'a aqe_live_protocol::PositionCloseAccounting>,
    commission: Option<f64>,
    swap: Option<f64>,
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
    entry_filled_quantity: Option<f64>,
    partial_filled_quantity: Option<f64>,
    state_history_len: usize,
}

#[derive(Clone, Debug)]
pub struct Insight {
    pub insight_id: Uuid,
    pub parent_id: Option<Uuid>,
    /// True when this insight is a broker-size execution fragment of its direct parent.
    /// Split insights are submitted alongside their parent and do not represent a separate
    /// strategy/DCA decision.
    pub is_split_insight: bool,
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
    /// Read-only analytics evidence, independent from execution/P&L helpers.
    pub(crate) position_accounting: Option<aqe_live_protocol::PositionCloseAccounting>,
    pub commission: Option<f64>,
    pub swap: Option<f64>,
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
    pub(crate) pending_mutations: Vec<super::operations::PendingInsightMutation>,
    pub(crate) execution_data: super::execution_data::ExecutionData,
    /// Cancellation/late-fill exposure must never reactivate dormant DCA entries.
    pub(crate) suppress_child_entries: bool,
    pub cancelling: bool,
    pub closing: bool,
    pub first_on_fill: bool,
    /// Cumulative entry execution, separate from the legacy partial-close quantity.
    pub entry_filled_quantity: Option<f64>,
    pub partial_filled_quantity: Option<f64>,

    // State tracking
    pub state_history: Vec<(DateTime<Utc>, InsightState, Option<String>)>,
    /// Type metadata aligned by index with `state_history`. Keeping the legacy tuple preserves
    /// source compatibility while append-only cloud history migrates to typed records.
    pub state_history_kinds: Vec<InsightHistoryKind>,
    pub context: Option<InsightStrategyContext>,
}

impl Insight {
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

    pub(crate) fn normalize_levels(levels: Option<Vec<f64>>) -> Option<Vec<f64>> {
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
            is_split_insight: self.is_split_insight,
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
            position_accounting: self.position_accounting.as_ref(),
            commission: self.commission,
            swap: self.swap,
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
            entry_filled_quantity: self.entry_filled_quantity,
            partial_filled_quantity: self.partial_filled_quantity,
            state_history_len: self.state_history.len(),
        }
    }

    pub fn snapshot_fingerprint_hash(&self) -> u64 {
        let fingerprint = self.snapshot_fingerprint();
        let mut state = DefaultHasher::new();

        fingerprint.insight_id.hash(&mut state);
        fingerprint.parent_id.hash(&mut state);
        fingerprint.is_split_insight.hash(&mut state);
        fingerprint.state.hash(&mut state);
        fingerprint.children_len.hash(&mut state);
        // Equal-length edits must still persist a changed ownership graph.
        // Hash direct IDs only; recursively hashing child trees is quadratic.
        for child in &self.children {
            child.insight_id.hash(&mut state);
        }
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
        fingerprint.position_accounting.hash(&mut state);
        hash_option_f64(&mut state, fingerprint.commission);
        hash_option_f64(&mut state, fingerprint.swap);
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
        self.suppress_child_entries.hash(&mut state);
        if !self.execution_data.is_empty() {
            self.execution_data.hash(&mut state);
        }
        for pending in &self.pending_mutations {
            pending.receipt.id.hash(&mut state);
            pending
                .receipt
                .group
                .as_ref()
                .map(|group| (&group.id, group.version))
                .hash(&mut state);
        }
        hash_option_f64(&mut state, fingerprint.partial_filled_quantity);
        hash_option_f64(&mut state, fingerprint.entry_filled_quantity);
        fingerprint.state_history_len.hash(&mut state);
        if let Some((at, state_value, message)) = self.state_history.last() {
            hash_datetime(&mut state, *at);
            state_value.hash(&mut state);
            message.hash(&mut state);
        }

        state.finish()
    }

    pub fn remaining_quantity(&self) -> f64 {
        // Pending capacity still reserves the complete requested entry. Once
        // filled, broker execution quantity is authoritative (legacy snapshots
        // without it fall back to the original requested quantity).
        let quantity = if matches!(self.state, InsightState::Filled | InsightState::Closed) {
            self.entry_filled_quantity.or(self.quantity)
        } else {
            self.quantity
        };
        (quantity.unwrap_or(0.0) - self.partial_filled_quantity.unwrap_or(0.0)).max(0.0)
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
    pub fn position_filled(
        &mut self,
        filled_price: f64,
        filled_qty: f64,
        order_id: &str,
        commission: Option<f64>,
    ) {
        self.order_id = Some(order_id.to_string());
        self.submitted = true;
        self.filled_price = Some(filled_price);
        self.entry_filled_quantity = Some(filled_qty);
        self.commission = commission;
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

    /// Cumulative entry fill snapshot. Repeated/older observations are no-ops.
    /// Entry execution is not a scale-out and must never produce realized P&L.
    pub fn partial_filled(
        &mut self,
        filled_qty: f64,
        filled_price: f64,
        order_id: &str,
        commission: Option<f64>,
    ) {
        if !filled_qty.is_finite()
            || filled_qty <= self.entry_filled_quantity.unwrap_or(0.0)
            || !filled_price.is_finite()
            || filled_price <= 0.0
        {
            return;
        }
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
        self.filled_price = Some(filled_price);
        self.entry_filled_quantity = Some(filled_qty);
        if commission.is_some() {
            self.commission = commission;
        }
        self.update_state(
            InsightState::Executed,
            Some(format!(
                "Partial fill: {:.2} @ {:.4}",
                filled_qty, filled_price
            )),
        );
    }

    /// Partial close / scale-out received while the insight remains open.
    pub fn partial_closed(
        &mut self,
        close_qty: f64,
        close_price: f64,
        close_order_id: &str,
        commission: Option<f64>,
    ) {
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

        self.closing = false;
        self.partial_filled_quantity =
            Some(self.partial_filled_quantity.unwrap_or(0.0) + close_qty);
        if commission.is_some() {
            self.commission = commission;
        }

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
        commission: Option<f64>,
        swap: Option<f64>,
    ) {
        self.close_price = Some(close_price);
        self.close_order_id = Some(close_order_id.to_string());
        self.broker_realized_pnl = broker_realized_pnl;
        if commission.is_some() {
            self.commission = commission;
        }
        if swap.is_some() {
            self.swap = swap;
        }
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
        ((remaining_pl + partial_pl + self.swap.unwrap_or(0.0) - self.commission.unwrap_or(0.0))
            * 100.0)
            .round()
            / 100.0
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

        let gross_pnl = match self.side {
            OrderSide::Buy => (close - entry) * qty,
            OrderSide::Sell => (entry - close) * qty,
        };
        let raw = ((gross_pnl + self.swap.unwrap_or(0.0) - self.commission.unwrap_or(0.0))
            / (entry * qty))
            * 100.0;
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
        is_split_order: Option<bool>,
        _ctx: &mut dyn StrategyContext,
    ) -> &mut Self {
        // self.children_ids.push(insight_id);
        child_insight.strategy_type =
            StrategyType::Custom(format!("{}-CHILD", self.strategy_type.to_string()));
        child_insight.parent_id = Some(self.insight_id);
        child_insight.is_split_insight = is_split_order.unwrap_or(false);
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
                    if let Err(error) = self.close(ctx) {
                        self.state_log(Some(format!("Market-change close pending: {error}")));
                    }
                }
                _ => {
                    if let Err(error) = self.cancel(ctx) {
                        self.state_log(Some(format!(
                            "Market-change cancellation pending: {error}"
                        )));
                    }
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

    /// Evaluate TTL without discarding queue-pressure or pending-command evidence.
    /// None means not expired; Some retains the broker operation's actual outcome.
    pub fn expire_if_due(
        &mut self,
        ctx: &mut dyn StrategyContext,
    ) -> Option<crate::core::insight::InsightMutationResult> {
        let (anchor, periods) = match self.state {
            InsightState::New | InsightState::Executed => (self.created_at, self.period_unfilled?),
            InsightState::Filled => (self.filled_at.unwrap_or(self.created_at), self.period_till_tp?),
            _ => return None,
        };
        let expiry = self
            .timeframe
            .add_time_increment(anchor, periods as i64)
            .unwrap_or(anchor);
        if self.reference_time(ctx) < expiry {
            return None;
        }
        let was_cancelling = self.cancelling;
        let was_closing = self.closing;
        let result = match self.state {
            InsightState::New if !self.has_pending_submit() && self.order_id.is_none() => {
                self.order_rejected("Expired before submission fill window elapsed");
                Ok(crate::core::insight::InsightMutationState::Applied)
            }
            InsightState::New | InsightState::Executed => self.cancel(ctx),
            InsightState::Filled => self.close(ctx),
            _ => unreachable!(),
        };
        if !was_cancelling && self.cancelling {
            self.state_log(Some("Expired before fill window elapsed; cancel requested".into()));
        }
        if !was_closing && self.closing {
            self.state_log(Some("Take-profit time window expired; closing position".into()));
        }
        Some(result)
    }

    pub fn has_expired(&mut self, ctx: &mut dyn StrategyContext) -> bool {
        match self.expire_if_due(ctx) {
            None => false,
            Some(result) => {
                if let Err(error) = result {
                    warn!("Expired insight {} cleanup deferred: {error}", self.insight_id);
                }
                true
            }
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
        if self.state == InsightState::Filled || self.state.is_inactive() {
            self.state_log(Some("Cannot set entry limit price: order has already filled or is inactive".into()));
            return self;
        }
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
        if self.state == InsightState::Filled || self.state.is_inactive() {
            self.state_log(Some("Cannot set entry stop price: order has already filled or is inactive".into()));
            return self;
        }
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
    pub fn is_split_insight(&self) -> bool {
        self.is_split_insight
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
        self.update_state_typed(new_state, message, InsightHistoryKind::Diagnostic);
    }

    fn push_state_history(
        &mut self,
        at: DateTime<Utc>,
        state: InsightState,
        message: Option<String>,
        kind: InsightHistoryKind,
    ) {
        self.state_history.push((at, state, message));
        self.state_history_kinds.push(kind);
    }

    fn update_state_typed(
        &mut self,
        new_state: InsightState,
        message: Option<String>,
        message_kind: InsightHistoryKind,
    ) {
        let at = self.current_time();
        self.updated_at = at;
        if self.state == InsightState::Filled
            && matches!(new_state, InsightState::Cancelled | InsightState::Rejected)
        {
            let attempted_state = format!("{:?}", new_state);
            let detail = message
                .as_deref()
                .unwrap_or("No transition reason was provided");
            warn!(
                "Ignoring invalid Filled -> {} transition for insight {}: {}",
                attempted_state, self.insight_id, detail
            );
            self.push_state_history(
                at,
                InsightState::Filled,
                Some(format!(
                    "Ignored invalid Filled -> {attempted_state} transition: {detail}"
                )),
                InsightHistoryKind::Diagnostic,
            );
            return;
        }
        if matches!(new_state, InsightState::Filled) {
            self.cancelling = false;
        }
        if new_state.is_inactive() {
            self.cancelling = false;
            self.closing = false;
        }
        if self.state != new_state {
            self.push_state_history(
                at,
                new_state.clone(),
                format!(
                    "State changed from {:?} to {:?}",
                    self.state,
                    new_state.clone()
                )
                .into(),
                InsightHistoryKind::StateTransition,
            );
            self.state = new_state.clone();
        }
        self.push_state_history(at, new_state, message, message_kind);
    }

    pub fn state_log(&mut self, message: Option<String>) -> &mut Self {
        self.update_state_typed(
            self.state.clone(),
            Some(message.unwrap_or_else(|| "State log entry".to_string())),
            InsightHistoryKind::Diagnostic,
        );
        self
    }

    pub fn state_log_typed(
        &mut self,
        message: Option<String>,
        kind: InsightHistoryKind,
    ) -> &mut Self {
        self.update_state_typed(
            self.state.clone(),
            Some(message.unwrap_or_else(|| "State log entry".to_string())),
            kind,
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
            is_split_insight: false,
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
            position_accounting: None,
            commission: None,
            swap: None,
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
            pending_mutations: Vec::new(),
            execution_data: Default::default(),
            suppress_child_entries: false,
            cancelling: false,
            closing: false,
            first_on_fill: false,
            entry_filled_quantity: None,
            partial_filled_quantity: None,

            // State tracking
            state_history: Vec::new(),
            state_history_kinds: Vec::new(),
            context: None,
        }
    }
}

#[cfg(test)]
#[path = "insight_tests.rs"]
mod tests;
