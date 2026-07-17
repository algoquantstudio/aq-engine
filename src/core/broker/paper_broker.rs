use super::backtest_state::{BacktestResults, BacktestState};
use super::traits::{Broker, OrderManagementProvider};
use super::types::{
    Account, AccountType, Asset, AssetFees, Bar, BrokerError, Order, OrderClass, OrderLeg,
    OrderLegs, OrderSide, OrderType, Position, TradeRecord, TradeRecordType, TradeUpdateEvent,
};
use crate::core::insight::Insight;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use log::debug;
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use uuid::Uuid;

fn is_terminal_leg_status(status: &TradeUpdateEvent) -> bool {
    matches!(
        status,
        TradeUpdateEvent::Filled
            | TradeUpdateEvent::Closed
            | TradeUpdateEvent::Cancelled
            | TradeUpdateEvent::Rejected
    )
}

fn update_order_leg(leg: Option<&mut OrderLeg>, order_id: &str, price: f64, now_ts: u64) -> bool {
    let Some(leg) = leg else {
        return false;
    };
    if leg.order_id.as_deref() != Some(order_id) || is_terminal_leg_status(&leg.status) {
        return false;
    }
    leg.limit_price = Some(price);
    leg.updated_at = now_ts;
    true
}

#[derive(Clone, Copy, Debug, Default)]
struct ClosePositionResult {
    fully_closed: bool,
    qty: f64,
    net_pnl: f64,
    entry_commission: f64,
    exit_commission: f64,
    swap: f64,
}

// ─────────────────────── PaperBroker ───────────────────────

/// Paper Broker — full OMS for backtesting and paper trading.
///
/// Order lifecycle:  New → Pending → Filled → Active (bracket legs) → Closed
///
/// Position tracking uses `DashMap<symbol, DashMap<order_id, Position>>` to support
/// DCA and per-order position management (mirrors Python `Positions[symbol][orderId]`).
pub struct PaperBroker {
    name: String,
    account_type: AccountType,
    connected: Arc<Mutex<bool>>,

    // Account State
    account: Arc<RwLock<Account>>,
    starting_cash: f64,
    leverage: u8,
    asset_fees: AssetFees,
    asset_metadata: Arc<DashMap<String, Asset>>,

    // OMS State
    orders: Arc<DashMap<String, Order>>,
    /// Per-symbol, per-order positions: symbol → { order_id → Position }
    positions: Arc<DashMap<String, DashMap<String, Position>>>,

    pending_orders: Arc<Mutex<VecDeque<String>>>,
    active_orders: Arc<DashMap<String, String>>, // order_id → order_id (bracket legs being tracked)
    terminal_orders_pending_release: Arc<Mutex<VecDeque<String>>>,

    // Queues for deferred processing within a step
    update_orders_queue: Arc<Mutex<VecDeque<(String, TradeUpdateEvent)>>>,
    close_orders_queue: Arc<Mutex<VecDeque<(String, f64, Option<f64>)>>>,

    // Trade events queue — collected during process_step, drained by StrategyState
    trade_events: Arc<Mutex<VecDeque<(Order, TradeUpdateEvent)>>>,

    // Trade event subscribers (for live streaming)
    #[allow(clippy::type_complexity)]
    trade_stream_subscribers: Arc<Mutex<Vec<Arc<dyn Fn((Order, TradeUpdateEvent)) + Send + Sync>>>>,

    // Time management for backtest
    current_time: Arc<RwLock<DateTime<Utc>>>,

    // Optional shared backtest state (set by UnifiedBroker)
    backtest_state: Option<Arc<RwLock<BacktestState>>>,
}

impl PaperBroker {
    pub fn new(account_type: AccountType, cash: f64, leverage: u8) -> Self {
        Self {
            name: "PaperBroker".to_string(),
            account_type: account_type.clone(),
            connected: Arc::new(Mutex::new(false)),
            starting_cash: cash,
            leverage,
            asset_fees: AssetFees::default(),
            asset_metadata: Arc::new(DashMap::new()),
            account: Arc::new(RwLock::new(Account {
                account_id: Uuid::new_v4().to_string(),
                account_type: account_type.clone(),
                equity: cash,
                cash,
                currency: "USD".to_string(),
                buying_power: cash * leverage as f64,
                accrued_commission: 0.0,
                shorting_enabled: true,
                leverage,
            })),
            orders: Arc::new(DashMap::new()),
            positions: Arc::new(DashMap::new()),
            pending_orders: Arc::new(Mutex::new(VecDeque::new())),
            active_orders: Arc::new(DashMap::new()),
            terminal_orders_pending_release: Arc::new(Mutex::new(VecDeque::new())),
            update_orders_queue: Arc::new(Mutex::new(VecDeque::new())),
            close_orders_queue: Arc::new(Mutex::new(VecDeque::new())),
            trade_events: Arc::new(Mutex::new(VecDeque::new())),
            trade_stream_subscribers: Arc::new(Mutex::new(Vec::new())),
            current_time: Arc::new(RwLock::new(Utc::now())),
            backtest_state: None,
        }
    }

    pub fn with_asset_fees(mut self, asset_fees: AssetFees) -> Self {
        self.asset_fees = asset_fees;
        for mut asset in self.asset_metadata.iter_mut() {
            asset.fees = self.asset_fees.clone();
        }
        self
    }

    pub fn register_asset_metadata(&self, asset: &Asset) {
        let mut asset = asset.clone();
        asset.fees = self.asset_fees.clone();
        self.asset_metadata.insert(asset.symbol.clone(), asset);
    }

    /// Set the shared backtest state (called by UnifiedBroker during backtest setup).
    pub fn set_backtest_state(&mut self, state: Arc<RwLock<BacktestState>>) {
        self.backtest_state = Some(state);
    }

    pub fn set_time(&self, time: DateTime<Utc>) {
        *self.current_time.write() = time;
    }

    // ─────────────────────── Step Pipeline ───────────────────────

    /// Process one bar step: pending orders → active orders → close orders → update account.
    /// Called by `UnifiedBroker::step()` with the current bars for each symbol.
    pub fn process_step(&self, bars: &HashMap<String, Bar>, current_time: DateTime<Utc>) {
        debug!(
            "PaperBroker::process_step enter bars={} current_time={}",
            bars.len(),
            current_time
        );
        self.set_time(current_time);
        let now_ts = current_time.timestamp() as u64;

        // 1. Process pending orders (market/limit/stop fills)
        debug!("PaperBroker::process_step pending_orders phase");
        self.process_pending_orders(bars, current_time, now_ts);

        // 2. Process active orders (bracket legs: TP/SL checks)
        debug!("PaperBroker::process_step active_orders phase");
        self.process_active_orders(bars, current_time, now_ts);

        // 3. Process deferred close/update queues
        debug!("PaperBroker::process_step close_queue phase");
        self.process_close_queue(bars, current_time, now_ts);
        debug!("PaperBroker::process_step update_queue phase");
        self.process_update_queue(now_ts);

        // 4. Update account balance with current prices
        debug!("PaperBroker::process_step account_update phase");
        self.update_account_balance(bars);
        debug!("PaperBroker::process_step exit");
    }

    pub fn process_close_queue_at(
        &self,
        bars: &HashMap<String, Bar>,
        current_time: DateTime<Utc>,
    ) -> usize {
        let queued_requests = self.close_orders_queue.lock().len();
        if queued_requests == 0 {
            return 0;
        }

        debug!(
            "PaperBroker::process_close_queue_at enter bars={} current_time={} queued_requests={}",
            bars.len(),
            current_time,
            queued_requests
        );
        self.set_time(current_time);
        self.process_close_queue(bars, current_time, current_time.timestamp() as u64);
        self.update_account_balance(bars);
        debug!("PaperBroker::process_close_queue_at exit");
        queued_requests
    }

    // ─────────────────────── Pending Orders ───────────────────────

    /// Process pending orders against the current bar.
    /// Market orders fill at open, Limit/Stop fill if bar range crosses the trigger price.
    fn process_pending_orders(
        &self,
        bars: &HashMap<String, Bar>,
        current_time: DateTime<Utc>,
        now_ts: u64,
    ) {
        let mut pending = self.pending_orders.lock();
        let mut remaining = VecDeque::new();

        while let Some(order_id) = pending.pop_front() {
            if let Some(mut order) = self.orders.get_mut(&order_id) {
                // Skip cancelled orders
                if order.status == TradeUpdateEvent::Cancelled {
                    continue;
                }

                let Some(bar) = bars.get(&order.asset.symbol) else {
                    remaining.push_back(order_id);
                    continue;
                };

                let fill_result = self.try_fill_order(&order, bar);

                if let Some(fill_price) = fill_result {
                    if let Some(reason) = self.insufficient_funds_reason(
                        &order.side,
                        order.qty,
                        fill_price,
                        order.asset.contract_size,
                    ) {
                        order.status = TradeUpdateEvent::Rejected;
                        order.rejection_reason = Some(reason);
                        order.updated_at = now_ts;
                        self.emit_trade_event(&order, TradeUpdateEvent::Rejected);
                        self.mark_order_terminal_for_release(&order_id);
                        continue;
                    }

                    // Fill the order
                    order.status = TradeUpdateEvent::Filled;
                    order.filled_qty = order.qty;
                    order.filled_price = Some(fill_price);
                    order.filled_at = Some(now_ts);
                    order.updated_at = now_ts;
                    order.commission = Some(order.asset.fees.entry_commission_for_side(
                        &order.side,
                        fill_price,
                        order.filled_qty,
                        order.asset.contract_size,
                    ));

                    // Build bracket legs if order_class is Bracket
                    if order.order_class == OrderClass::Bracket {
                        self.create_bracket_legs(&mut order, now_ts);
                    }

                    // Open/update position
                    self.open_position(&order, fill_price);

                    // Record trade
                    self.record_entry_trade(&order, fill_price, current_time);

                    // Emit Filled trade event
                    self.emit_trade_event(&order, TradeUpdateEvent::Filled);

                    // Move to active (for bracket leg tracking) or closed
                    if order.legs.is_some() {
                        self.active_orders
                            .insert(order_id.clone(), order_id.clone());
                    }
                } else {
                    remaining.push_back(order_id);
                }
            }
        }

        *pending = remaining;
    }

    /// Try to fill an order against a bar. Returns the fill price if filled.
    #[inline]
    fn try_fill_order(&self, order: &Order, bar: &Bar) -> Option<f64> {
        match order.order_type {
            OrderType::Market => Some(bar.open),
            OrderType::Limit => {
                if let Some(limit) = order.limit_price {
                    match order.side {
                        OrderSide::Buy => {
                            if bar.low <= limit {
                                Some(limit.min(bar.open)) // Fill at limit or better (open if gapped down)
                            } else {
                                None
                            }
                        }
                        OrderSide::Sell => {
                            if bar.high >= limit {
                                Some(limit.max(bar.open)) // Fill at limit or better (open if gapped up)
                            } else {
                                None
                            }
                        }
                    }
                } else {
                    None
                }
            }
            OrderType::Stop => {
                if let Some(stop) = order.stop_price {
                    match order.side {
                        OrderSide::Buy => {
                            if bar.high >= stop {
                                Some(stop.max(bar.open))
                            } else {
                                None
                            }
                        }
                        OrderSide::Sell => {
                            if bar.low <= stop {
                                Some(stop.min(bar.open))
                            } else {
                                None
                            }
                        }
                    }
                } else {
                    None
                }
            }
            OrderType::StopLimit => {
                // Stop-limit: stop triggers the limit order
                if let (Some(stop), Some(limit)) = (order.stop_price, order.limit_price) {
                    let triggered = match order.side {
                        OrderSide::Buy => bar.high >= stop,
                        OrderSide::Sell => bar.low <= stop,
                    };
                    if triggered {
                        // Now check limit within same bar (simplified)
                        match order.side {
                            OrderSide::Buy => {
                                if bar.low <= limit {
                                    Some(limit)
                                } else {
                                    None
                                }
                            }
                            OrderSide::Sell => {
                                if bar.high >= limit {
                                    Some(limit)
                                } else {
                                    None
                                }
                            }
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            OrderType::TrailingStop => {
                // TODO: implement trailing stop logic
                None
            }
        }
    }

    fn insufficient_funds_reason(
        &self,
        side: &OrderSide,
        qty: f64,
        price: f64,
        contract_size: Option<i64>,
    ) -> Option<String> {
        if !qty.is_finite() || qty <= 0.0 || !price.is_finite() || price <= 0.0 {
            return None;
        }

        let notional = qty * price;
        let commission = self
            .asset_fees
            .entry_commission_for_side(side, price, qty, contract_size);
        let margin_cost = if self.leverage > 1 {
            notional / self.leverage as f64
        } else {
            notional
        };
        let account = self.account.read();
        if notional > account.buying_power {
            Some(format!(
                "Insufficient funds: required buying power {:.2}, available {:.2}",
                notional, account.buying_power
            ))
        } else if margin_cost + commission > account.cash {
            Some(format!(
                "Insufficient cash: required cash {:.2}, available {:.2}",
                margin_cost + commission,
                account.cash
            ))
        } else {
            None
        }
    }

    // ─────────────────────── Active Orders (Bracket Legs) ───────────────────────

    /// Check active orders' bracket legs (TP/SL) against the current bar.
    fn process_active_orders(
        &self,
        bars: &HashMap<String, Bar>,
        current_time: DateTime<Utc>,
        now_ts: u64,
    ) {
        let mut to_remove = Vec::new();

        for entry in self.active_orders.iter() {
            let order_id = entry.key();

            if let Some(mut order) = self.orders.get_mut(order_id) {
                let Some(bar) = bars.get(&order.asset.symbol) else {
                    continue;
                };

                // Read side before mutably borrowing legs
                let side = order.side.clone();
                let oid = order_id.clone();

                if let Some(ref mut legs) = order.legs {
                    let mut closed = false;
                    let mut close_price = 0.0;

                    if let Some(ref mut trailing) = legs.trailing_stop {
                        if trailing.status != TradeUpdateEvent::Filled {
                            let trailing_triggered =
                                self.update_trailing_stop_leg(&side, trailing, bar, now_ts);
                            if trailing_triggered {
                                close_price = trailing.limit_price.unwrap_or(bar.close);
                                trailing.status = TradeUpdateEvent::Filled;
                                trailing.filled_price = Some(close_price);
                                trailing.filled_at = Some(now_ts);
                                closed = true;
                            }
                        }
                    }

                    // Check Stop Loss first (priority over TP)
                    if !closed {
                        if let Some(ref mut sl) = legs.stop_loss {
                            if sl.status != TradeUpdateEvent::Filled {
                                let sl_triggered = match side {
                                    OrderSide::Buy => {
                                        sl.limit_price.map_or(false, |price| bar.low <= price)
                                    }
                                    OrderSide::Sell => {
                                        sl.limit_price.map_or(false, |price| bar.high >= price)
                                    }
                                };
                                if sl_triggered {
                                    close_price = sl.limit_price.unwrap();
                                    sl.status = TradeUpdateEvent::Filled;
                                    sl.filled_price = Some(close_price);
                                    sl.filled_at = Some(now_ts);
                                    closed = true;
                                }
                            }
                        }
                    }

                    // Check Take Profit (only if SL didn't trigger)
                    if !closed {
                        if let Some(ref mut tp) = legs.take_profit {
                            if tp.status != TradeUpdateEvent::Filled {
                                let tp_triggered = match side {
                                    OrderSide::Buy => {
                                        tp.limit_price.map_or(false, |price| bar.high >= price)
                                    }
                                    OrderSide::Sell => {
                                        tp.limit_price.map_or(false, |price| bar.low <= price)
                                    }
                                };
                                if tp_triggered {
                                    close_price = tp.limit_price.unwrap();
                                    tp.status = TradeUpdateEvent::Filled;
                                    tp.filled_price = Some(close_price);
                                    tp.filled_at = Some(now_ts);
                                    closed = true;
                                }
                            }
                        }
                    }

                    if closed {
                        let close_result = self.close_position_for_order(
                            &order,
                            close_price,
                            order.qty,
                            current_time,
                        );
                        order.status = TradeUpdateEvent::Closed;
                        order.updated_at = now_ts;
                        order.filled_price = Some(close_price);
                        order.filled_qty = order.qty;
                        if let Some(result) = close_result {
                            order.realized_pnl = Some(result.net_pnl);
                            order.commission =
                                Some(result.entry_commission + result.exit_commission);
                            order.swap = Some(result.swap);
                        }
                        order.qty = 0.0;
                        self.cancel_remaining_legs(&mut order);

                        // Emit Closed trade event
                        self.emit_trade_event(&order, TradeUpdateEvent::Closed);
                        self.mark_order_terminal_for_release(&oid);

                        to_remove.push(oid.clone());
                    }
                }
            }
        }

        for id in to_remove {
            if let Some(mut order) = self.orders.get_mut(&id) {
                order.status = TradeUpdateEvent::Closed;
                order.qty = 0.0;
                self.cancel_remaining_legs(&mut order);
            }
            self.active_orders.remove(&id);
        }
    }

    // ─────────────────────── Position Management ───────────────────────

    /// Open or add to a position when an order is filled.
    fn open_position(&self, order: &Order, fill_price: f64) {
        let symbol = &order.asset.symbol;
        let order_id = &order.order_id;

        // Get or create the per-symbol map
        let symbol_positions = self
            .positions
            .entry(symbol.clone())
            .or_insert_with(DashMap::new);

        let position = Position {
            account_id: self.account.read().account_id.clone(),
            asset: order.asset.clone(),
            avg_entry_price: fill_price,
            qty: order.filled_qty,
            side: order.side.clone(),
            market_value: fill_price * order.filled_qty,
            cost_basis: fill_price * order.filled_qty,
            current_price: fill_price,
            unrealized_pnl: 0.0,
            realized_pnl: 0.0,
            entry_commission: order.commission.unwrap_or(0.0),
            margin_required: if self.leverage > 1 {
                Some(fill_price * order.filled_qty / self.leverage as f64)
            } else {
                None
            },
        };

        symbol_positions.insert(order_id.clone(), position);

        // Deduct from buying power
        let cost = fill_price * order.filled_qty;
        let margin_cost = if self.leverage > 1 {
            cost / self.leverage as f64
        } else {
            cost
        };

        let mut account = self.account.write();
        let entry_commission = order.commission.unwrap_or(0.0);
        account.cash -= margin_cost + entry_commission;
        account.buying_power = (account.buying_power - cost - entry_commission).max(0.0);
        account.accrued_commission += entry_commission;
    }

    /// Close a position for a specific order (bracket leg fill or explicit close).
    fn close_position_for_order(
        &self,
        order: &Order,
        close_price: f64,
        close_qty: f64,
        current_time: DateTime<Utc>,
    ) -> Option<ClosePositionResult> {
        let symbol = &order.asset.symbol;
        let order_id = &order.order_id;

        if let Some(symbol_positions) = self.positions.get(symbol) {
            let fully_closed = if let Some(mut position) = symbol_positions.get_mut(order_id) {
                let requested_qty = if close_qty <= 0.0 {
                    position.qty
                } else {
                    close_qty.min(position.qty)
                };
                let position_qty_before = position.qty;
                let fully_closed = requested_qty >= position.qty;
                let entry_commission = if position_qty_before > f64::EPSILON {
                    position.entry_commission * (requested_qty / position_qty_before)
                } else {
                    0.0
                };
                let exit_commission = order.asset.fees.exit_commission_for_side(
                    &position.side,
                    close_price,
                    requested_qty,
                    order.asset.contract_size,
                );
                let swap = order.asset.fees.swap_for_side(
                    &position.side,
                    position.avg_entry_price,
                    requested_qty,
                    order.asset.contract_size,
                    order.asset.min_price_increment,
                    Self::swap_days_for_order(order, current_time.timestamp() as u64),
                );

                // Calculate realized P&L
                let gross_pnl = match position.side {
                    OrderSide::Buy => (close_price - position.avg_entry_price) * requested_qty,
                    OrderSide::Sell => (position.avg_entry_price - close_price) * requested_qty,
                };
                let net_pnl = gross_pnl + swap - entry_commission - exit_commission;

                // Return capital + profit to account
                let cost = position.avg_entry_price * requested_qty;
                let margin_cost = if self.leverage > 1 {
                    cost / self.leverage as f64
                } else {
                    cost
                };

                let mut account = self.account.write();
                account.cash += margin_cost + gross_pnl + swap - exit_commission;
                account.buying_power =
                    (account.buying_power + cost + gross_pnl + swap - exit_commission).max(0.0);
                account.accrued_commission += exit_commission;

                // Record exit trade
                if let Some(ref state) = self.backtest_state {
                    state.write().record_trade(TradeRecord {
                        date: current_time,
                        symbol: symbol.clone(),
                        side: order.side.clone(),
                        qty: requested_qty,
                        price: close_price,
                        order_id: order_id.clone(),
                        insight_id: order.insight_id.clone(),
                        strategy_type: order.strategy_type.clone(),
                        commission: exit_commission,
                        swap,
                        trade_type: TradeRecordType::Exit,
                    });
                }

                if !fully_closed {
                    position.qty -= requested_qty;
                    position.entry_commission =
                        (position.entry_commission - entry_commission).max(0.0);
                    position.cost_basis = position.avg_entry_price * position.qty;
                    position.market_value = position.current_price * position.qty;
                    position.margin_required = if self.leverage > 1 {
                        Some(position.cost_basis / self.leverage as f64)
                    } else {
                        None
                    };
                }

                ClosePositionResult {
                    fully_closed,
                    qty: requested_qty,
                    net_pnl,
                    entry_commission,
                    exit_commission,
                    swap,
                }
            } else {
                return None;
            };

            if fully_closed.fully_closed {
                symbol_positions.remove(order_id);
            }
            let empty = symbol_positions.is_empty();
            drop(symbol_positions);
            if empty {
                self.positions.remove(symbol);
            }
            return Some(fully_closed);
        }
        None
    }

    // ─────────────────────── Account Balance ───────────────────────

    /// Update account equity and unrealized P&L based on current prices.
    fn update_account_balance(&self, bars: &HashMap<String, Bar>) {
        let mut total_unrealized_pnl = 0.0;
        let mut total_margin_held = 0.0;
        let mut mae_updates = Vec::new();

        for entry in self.positions.iter() {
            let bar = bars.get(entry.key());
            for mut pos in entry.value().iter_mut() {
                if let Some(bar) = bar {
                    pos.current_price = bar.close;
                    pos.market_value = bar.close * pos.qty;
                    pos.unrealized_pnl = match pos.side {
                        OrderSide::Buy => (bar.close - pos.avg_entry_price) * pos.qty,
                        OrderSide::Sell => (pos.avg_entry_price - bar.close) * pos.qty,
                    };
                    if self.backtest_state.is_some() {
                        let mae_pct = if pos.avg_entry_price.abs() > f64::EPSILON {
                            match pos.side {
                                OrderSide::Buy => {
                                    ((bar.low - pos.avg_entry_price) / pos.avg_entry_price) * 100.0
                                }
                                OrderSide::Sell => {
                                    ((pos.avg_entry_price - bar.high) / pos.avg_entry_price) * 100.0
                                }
                            }
                        } else {
                            0.0
                        };
                        mae_updates.push((pos.key().clone(), mae_pct));
                    }
                    total_unrealized_pnl += pos.unrealized_pnl;
                }
                total_margin_held += pos.margin_required.unwrap_or(pos.cost_basis);
            }
        }

        if !mae_updates.is_empty()
            && let Some(ref state) = self.backtest_state
        {
            let mut state = state.write();
            for (order_id, mae_pct) in mae_updates {
                state.record_trade_mae(&order_id, mae_pct);
            }
        }

        let mut account = self.account.write();
        account.equity = account.cash + total_unrealized_pnl + total_margin_held;
    }

    // ─────────────────────── Deferred Queues ───────────────────────

    fn process_close_queue(
        &self,
        bars: &HashMap<String, Bar>,
        current_time: DateTime<Utc>,
        now_ts: u64,
    ) {
        let mut queue = self.close_orders_queue.lock();
        debug!(
            "PaperBroker::process_close_queue start queued_requests={}",
            queue.len()
        );
        while let Some((order_id, qty, requested_price)) = queue.pop_front() {
            debug!(
                "PaperBroker::process_close_queue popped order_id={} qty={:.4} requested_price={:?}",
                order_id, qty, requested_price
            );
            if let Some(order) = self.orders.get(&order_id).map(|order| order.clone()) {
                if let Some(bar) = bars.get(&order.asset.symbol) {
                    let fill_price = requested_price.unwrap_or(bar.close);
                    debug!(
                        "PaperBroker::process_close_queue closing order_id={} symbol={} fill_price={:.4}",
                        order_id, order.asset.symbol, fill_price
                    );
                    let close_result =
                        self.close_position_for_order(&order, fill_price, qty, current_time);
                    let fully_closed = close_result
                        .map(|result| result.fully_closed)
                        .unwrap_or(false);
                    let close_qty = close_result
                        .map(|result| result.qty)
                        .unwrap_or_else(|| if qty <= 0.0 { order.filled_qty } else { qty });
                    debug!(
                        "PaperBroker::process_close_queue close_position_for_order returned fully_closed={} close_qty={:.4}",
                        fully_closed, close_qty
                    );

                    let mut terminal_close = fully_closed;
                    if let Some(mut o) = self.orders.get_mut(&order_id) {
                        o.updated_at = now_ts;
                        o.filled_price = Some(fill_price);
                        o.filled_qty = close_qty;
                        let remaining_qty = (o.qty - close_qty).max(0.0);
                        terminal_close = fully_closed || remaining_qty <= f64::EPSILON;
                        if let Some(result) = close_result {
                            let previous_realized = o.realized_pnl.unwrap_or(0.0);
                            o.realized_pnl = Some(previous_realized + result.net_pnl);
                            let previous_commission = o.commission.unwrap_or(0.0);
                            o.commission = Some(previous_commission + result.exit_commission);
                            let previous_swap = o.swap.unwrap_or(0.0);
                            o.swap = Some(previous_swap + result.swap);
                        }

                        if terminal_close {
                            o.qty = 0.0;
                            o.status = TradeUpdateEvent::Closed;
                            self.cancel_remaining_legs(&mut o);
                            debug!(
                                "PaperBroker::process_close_queue emitting Closed for order_id={}",
                                order_id
                            );
                            self.emit_trade_event(&o, TradeUpdateEvent::Closed);
                            self.mark_order_terminal_for_release(&order_id);
                        } else {
                            o.qty = remaining_qty;
                            o.status = TradeUpdateEvent::PartialFilled;
                            debug!(
                                "PaperBroker::process_close_queue emitting PartialFilled for order_id={} remaining_qty={:.4}",
                                order_id, o.qty
                            );
                            self.emit_trade_event(&o, TradeUpdateEvent::PartialFilled);
                        }
                    }

                    if terminal_close {
                        self.active_orders.remove(&order_id);
                    }
                }
            }
        }
        debug!("PaperBroker::process_close_queue end");
    }

    fn process_update_queue(&self, now_ts: u64) {
        let mut queue = self.update_orders_queue.lock();
        while let Some((order_id, status)) = queue.pop_front() {
            if let Some(mut order) = self.orders.get_mut(&order_id) {
                order.status = status;
                order.updated_at = now_ts;
            }
        }
    }

    // ─────────────────────── Trade Recording ───────────────────────

    fn record_entry_trade(&self, order: &Order, fill_price: f64, current_time: DateTime<Utc>) {
        if let Some(ref state) = self.backtest_state {
            state.write().record_trade(TradeRecord {
                date: current_time,
                symbol: order.asset.symbol.clone(),
                side: order.side.clone(),
                qty: order.filled_qty,
                price: fill_price,
                order_id: order.order_id.clone(),
                insight_id: order.insight_id.clone(),
                strategy_type: order.strategy_type.clone(),
                commission: order.commission.unwrap_or(0.0),
                swap: 0.0,
                trade_type: TradeRecordType::Entry,
            });
        }
    }

    fn emit_trade_event(&self, order: &Order, event: TradeUpdateEvent) {
        // Collect for synchronous drain (Backtests)
        self.trade_events
            .lock()
            .push_back((order.clone(), event.clone()));

        // Emit to async live subscribers
        let subscribers = self.trade_stream_subscribers.lock();
        for sub in subscribers.iter() {
            sub((order.clone(), event.clone()));
        }
    }

    fn cancel_remaining_legs(&self, order: &mut Order) {
        let now_ts = self.current_time_ts();
        if let Some(ref mut legs) = order.legs {
            if let Some(ref mut tp) = legs.take_profit {
                if tp.status != TradeUpdateEvent::Filled {
                    tp.status = TradeUpdateEvent::Cancelled;
                    tp.updated_at = now_ts;
                }
            }
            if let Some(ref mut sl) = legs.stop_loss {
                if sl.status != TradeUpdateEvent::Filled {
                    sl.status = TradeUpdateEvent::Cancelled;
                    sl.updated_at = now_ts;
                }
            }
            if let Some(ref mut trailing) = legs.trailing_stop {
                if trailing.status != TradeUpdateEvent::Filled {
                    trailing.status = TradeUpdateEvent::Cancelled;
                    trailing.updated_at = now_ts;
                }
            }
        }
    }

    fn cancel_leg_on_parent(order: &mut Order, leg_order_id: &str, now_ts: u64) -> bool {
        let cancel_leg = |leg: &mut OrderLeg, leg_order_id: &str, now_ts: u64| -> bool {
            if leg.order_id.as_deref() != Some(leg_order_id) {
                return false;
            }
            if matches!(
                leg.status,
                TradeUpdateEvent::Filled
                    | TradeUpdateEvent::Closed
                    | TradeUpdateEvent::Cancelled
                    | TradeUpdateEvent::Rejected
            ) {
                return true;
            }
            leg.status = TradeUpdateEvent::Cancelled;
            leg.updated_at = now_ts;
            true
        };

        let Some(legs) = order.legs.as_mut() else {
            return false;
        };

        if let Some(tp) = legs.take_profit.as_mut() {
            if cancel_leg(tp, leg_order_id, now_ts) {
                return true;
            }
        }
        if let Some(sl) = legs.stop_loss.as_mut() {
            if cancel_leg(sl, leg_order_id, now_ts) {
                return true;
            }
        }
        if let Some(trailing) = legs.trailing_stop.as_mut() {
            if cancel_leg(trailing, leg_order_id, now_ts) {
                return true;
            }
        }
        false
    }

    // ─────────────────────── Bracket Leg Creation ───────────────────────

    fn create_bracket_legs(&self, order: &mut Order, _now_ts: u64) {
        let mut legs = OrderLegs::default();

        // Take Profit leg (from insight's take_profit_levels → first level → or from order's stored data)
        // We store TP/SL on the Order itself during submit
        if let Some(ref existing_legs) = order.legs {
            if existing_legs.take_profit.is_some() {
                legs.take_profit = existing_legs.take_profit.clone();
            }
            if existing_legs.stop_loss.is_some() {
                legs.stop_loss = existing_legs.stop_loss.clone();
            }
            if existing_legs.trailing_stop.is_some() {
                legs.trailing_stop = existing_legs.trailing_stop.clone();
            }
        }

        if let Some(trailing) = legs.trailing_stop.as_mut() {
            if trailing.limit_price.is_none() {
                if let (Some(entry), Some(gap)) = (order.filled_price, trailing.trail_price) {
                    trailing.limit_price = Some(match order.side {
                        OrderSide::Buy => entry - gap,
                        OrderSide::Sell => entry + gap,
                    });
                    trailing.updated_at = _now_ts;
                }
            }
        }

        order.legs = Some(legs);
    }

    fn update_trailing_stop_leg(
        &self,
        parent_side: &OrderSide,
        trailing: &mut OrderLeg,
        bar: &Bar,
        now_ts: u64,
    ) -> bool {
        let Some(gap) = trailing.trail_price else {
            return false;
        };

        let current_stop = trailing.limit_price.unwrap_or_else(|| match parent_side {
            OrderSide::Buy => bar.open - gap,
            OrderSide::Sell => bar.open + gap,
        });

        // First evaluate the stop that existed at the start of this bar. A
        // bar's high/low has no ordering information, so moving the trail
        // from its favourable extreme and then triggering against the same
        // bar's adverse extreme creates look-ahead fills.
        let triggered = match parent_side {
            OrderSide::Buy => bar.low <= current_stop,
            OrderSide::Sell => bar.high >= current_stop,
        };
        if triggered {
            return true;
        }

        // A favourable move becomes the active trailing level for the next
        // bar. This is conservative and deterministic with OHLC data.
        let next_stop = match parent_side {
            OrderSide::Buy => current_stop.max(bar.high - gap),
            OrderSide::Sell => current_stop.min(bar.low + gap),
        };
        if (next_stop - current_stop).abs() > f64::EPSILON {
            trailing.limit_price = Some(next_stop);
            trailing.updated_at = now_ts;
        } else if trailing.limit_price.is_none() {
            trailing.limit_price = Some(current_stop);
            trailing.updated_at = now_ts;
        }

        false
    }

    fn mark_order_terminal_for_release(&self, order_id: &str) {
        let mut pending_release = self.terminal_orders_pending_release.lock();
        if !pending_release.iter().any(|existing| existing == order_id) {
            pending_release.push_back(order_id.to_string());
        }
    }

    fn release_terminal_orders(&self) {
        let release_ids: Vec<String> = self
            .terminal_orders_pending_release
            .lock()
            .drain(..)
            .collect();

        if release_ids.is_empty() {
            return;
        }
        let release_set = release_ids.iter().cloned().collect::<HashSet<_>>();

        {
            let mut pending_orders = self.pending_orders.lock();
            pending_orders.retain(|order_id| !release_set.contains(order_id));
        }
        {
            let mut close_queue = self.close_orders_queue.lock();
            close_queue.retain(|(order_id, _, _)| !release_set.contains(order_id));
        }
        {
            let mut update_queue = self.update_orders_queue.lock();
            update_queue.retain(|(order_id, _)| !release_set.contains(order_id));
        }

        for order_id in release_ids {
            self.active_orders.remove(&order_id);
            self.orders.remove(&order_id);
        }
    }

    // ─────────────────────── Helpers ───────────────────────

    #[inline]
    fn current_time_ts(&self) -> u64 {
        self.current_time.read().timestamp() as u64
    }

    #[inline]
    fn swap_days_for_order(order: &Order, current: u64) -> f64 {
        let Some(opened_at) = order.filled_at else {
            return 0.0;
        };
        if current <= opened_at {
            return 0.0;
        }
        (current - opened_at) as f64 / 86_400.0
    }

    /// Synchronous account access (no async overhead).
    pub fn get_account_sync(&self) -> Result<Account, BrokerError> {
        let acc = self.account.read();
        Ok(acc.clone())
    }

    /// Aggregate positions per symbol (flatten the per-order map).
    fn get_aggregate_position(&self, symbol: &str) -> Option<Position> {
        let symbol_positions = self.positions.get(symbol)?;
        let mut agg: Option<Position> = None;

        for entry in symbol_positions.iter() {
            let pos = entry.value();
            match agg {
                None => agg = Some(pos.clone()),
                Some(ref mut a) => {
                    // Weighted average entry price
                    let total_qty = a.qty + pos.qty;
                    if total_qty > 0.0 {
                        a.avg_entry_price =
                            (a.avg_entry_price * a.qty + pos.avg_entry_price * pos.qty) / total_qty;
                    }
                    a.qty = total_qty;
                    a.market_value += pos.market_value;
                    a.cost_basis += pos.cost_basis;
                    a.current_price = pos.current_price;
                    a.unrealized_pnl += pos.unrealized_pnl;
                    a.realized_pnl += pos.realized_pnl;
                }
            }
        }

        agg
    }

    /// Compute final backtest results from trade log and account history.
    ///  TODO: Remove this function and use the BacktestState::compute_results method instead.
    /// # Arguments
    ///
    /// * `state_guard` - Optional guard for the backtest state.
    ///
    /// # Returns
    ///
    /// * `BacktestResults` - The computed backtest results.
    pub fn compute_results(
        &self,
        state_guard: Option<parking_lot::RwLockReadGuard<'_, BacktestState>>,
    ) -> BacktestResults {
        let account = self.account.read();

        let (
            trade_log,
            account_history,
            max_drawdown,
            executed_at,
            finished_at,
            backtest_start,
            backtest_end,
        ) = if let Some(ref state) = state_guard {
            // Calculate max drawdown from account history
            let mut peak = self.starting_cash;
            let mut max_dd = 0.0f64;
            for (_, acc) in &state.account_history {
                if acc.equity > peak {
                    peak = acc.equity;
                }
                let dd = (peak - acc.equity) / peak;
                if dd > max_dd {
                    max_dd = dd;
                }
            }
            (
                state.trade_log.clone(),
                state.account_history.clone(),
                max_dd,
                state.get_executed_at(),
                state.get_finished_at(),
                state.backtest_start,
                state.backtest_end,
            )
        } else {
            (
                Vec::new(),
                Vec::new(),
                0.0,
                Utc::now(),
                Some(Utc::now()),
                None,
                None,
            )
        };

        // Count winning/losing trades (pair entries with exits)
        let mut wins = 0usize;
        let mut losses = 0usize;
        let mut entries: HashMap<String, &TradeRecord> = HashMap::new();

        for record in &trade_log {
            match record.trade_type {
                TradeRecordType::Entry => {
                    entries.insert(record.order_id.clone(), record);
                }
                TradeRecordType::Exit => {
                    if let Some(entry) = entries.remove(&record.order_id) {
                        let pnl = match entry.side {
                            OrderSide::Buy => (record.price - entry.price) * entry.qty,
                            OrderSide::Sell => (entry.price - record.price) * entry.qty,
                        };
                        if pnl >= 0.0 {
                            wins += 1;
                        } else {
                            losses += 1;
                        }
                    }
                }
            }
        }

        let total_trades = wins + losses;
        let win_rate = if total_trades > 0 {
            wins as f64 / total_trades as f64
        } else {
            0.0
        };
        let total_return_pct = (account.equity - self.starting_cash) / self.starting_cash * 100.0;

        BacktestResults {
            starting_cash: self.starting_cash,
            final_equity: account.equity,
            total_return_pct,
            total_trades,
            winning_trades: wins,
            losing_trades: losses,
            win_rate,
            max_drawdown,
            trade_log,
            account_history,
            executed_at,
            finished_at: finished_at.unwrap_or_else(|| Utc::now()),
            backtest_start,
            backtest_end,
        }
    }
}

// ─────────────────────── Broker Trait ───────────────────────

impl Broker for PaperBroker {
    async fn connect(&self) -> Result<bool, BrokerError> {
        let mut connected = self.connected.lock();
        *connected = true;
        Ok(true)
    }

    async fn disconnect(&self) -> Result<bool, BrokerError> {
        let mut connected = self.connected.lock();
        *connected = false;
        Ok(true)
    }

    fn is_connected(&self) -> bool {
        *self.connected.lock()
    }

    fn get_current_time(&self) -> DateTime<Utc> {
        *self.current_time.read()
    }

    fn get_name(&self) -> String {
        self.name.clone()
    }

    fn get_account_type(&self) -> Result<AccountType, BrokerError> {
        Ok(self.account_type.clone())
    }

    fn configure_asset_metadata(&self, asset: &Asset) -> Result<(), BrokerError> {
        self.register_asset_metadata(asset);
        Ok(())
    }
}

// ─────────────────────── OrderManagementProvider Trait ───────────────────────

impl OrderManagementProvider for PaperBroker {
    fn process_live_bar(&self, bar: &Bar) {
        let mut bars = HashMap::new();
        bars.insert(bar.symbol.clone(), bar.clone());
        self.process_step(&bars, bar.timestamp);
    }

    async fn submit_order(&self, insight: Insight) -> Result<Order, BrokerError> {
        let qty = insight.quantity.ok_or_else(|| {
            BrokerError::OrderError(format!(
                "Cannot submit order for {} without a quantity",
                insight.symbol
            ))
        })?;
        if !qty.is_finite() || qty <= 0.0 {
            return Err(BrokerError::OrderError(format!(
                "Cannot submit order for {} with invalid quantity {}",
                insight.symbol, qty
            )));
        }

        let order_id = Uuid::new_v4().to_string();
        let now_ts = self.current_time_ts();

        let asset = self
            .asset_metadata
            .get(&insight.symbol)
            .map(|asset| {
                let mut asset = asset.clone();
                asset.fees = self.asset_fees.clone();
                asset
            })
            .unwrap_or_else(|| Asset {
                id: Uuid::new_v4().to_string(),
                symbol: insight.symbol.clone(),
                name: insight.symbol.clone(),
                asset_type: super::types::AssetType::Stock,
                status: super::types::AssetStatus::Active,
                exchange: super::types::AssetExchange::NYSE,
                tradable: true,
                marginable: true,
                shortable: true,
                fractional: true,
                min_order_size: None,
                quantity_base: None,
                max_order_size: None,
                min_price_increment: None,
                price_base: None,
                contract_size: None,
                fees: self.asset_fees.clone(),
            });

        // Build bracket legs from insight if applicable
        let legs = if insight.order_class == OrderClass::Bracket {
            let opposite_side = match insight.side {
                OrderSide::Buy => OrderSide::Sell,
                OrderSide::Sell => OrderSide::Buy,
            };

            let tp_leg = insight.take_profit_levels.as_ref().and_then(|levels| {
                levels.last().map(|&price| OrderLeg {
                    order_id: Some(Uuid::new_v4().to_string()),
                    limit_price: Some(price),
                    trail_price: None,
                    side: opposite_side.clone(),
                    filled_price: None,
                    order_type: OrderType::Limit,
                    status: TradeUpdateEvent::Pending,
                    order_class: OrderClass::Bracket,
                    created_at: now_ts,
                    updated_at: now_ts,
                    submitted_at: now_ts,
                    filled_at: None,
                })
            });

            let sl_leg = insight.stop_loss_levels.as_ref().and_then(|levels| {
                levels.last().copied().map(|price| OrderLeg {
                    order_id: Some(Uuid::new_v4().to_string()),
                    limit_price: Some(price),
                    trail_price: None,
                    side: opposite_side.clone(),
                    filled_price: None,
                    order_type: OrderType::Stop,
                    status: TradeUpdateEvent::Pending,
                    order_class: OrderClass::Bracket,
                    created_at: now_ts,
                    updated_at: now_ts,
                    submitted_at: now_ts,
                    filled_at: None,
                })
            });

            let trailing_leg = insight.trailing_stop_price.map(|price| OrderLeg {
                order_id: Some(Uuid::new_v4().to_string()),
                limit_price: None,
                trail_price: Some(price),
                side: opposite_side.clone(),
                filled_price: None,
                order_type: OrderType::TrailingStop,
                status: TradeUpdateEvent::Pending,
                order_class: OrderClass::Bracket,
                created_at: now_ts,
                updated_at: now_ts,
                submitted_at: now_ts,
                filled_at: None,
            });

            Some(OrderLegs {
                take_profit: tp_leg,
                stop_loss: sl_leg,
                trailing_stop: trailing_leg,
            })
        } else {
            None
        };

        let order = Order {
            order_id: order_id.clone(),
            insight_id: Some(insight.insight_id.to_string()),
            strategy_type: Some(insight.strategy_type.to_string()),
            asset,
            qty,
            filled_qty: 0.0,
            limit_price: insight.limit_price,
            filled_price: None,
            stop_price: insight.stop_price,
            side: insight.side,
            order_type: insight.order_type,
            time_in_force: super::types::TimeInForce::GTC,
            status: TradeUpdateEvent::Pending,
            order_class: insight.order_class,
            created_at: now_ts,
            updated_at: now_ts,
            submitted_at: now_ts,
            filled_at: None,
            realized_pnl: None,
            commission: None,
            swap: None,
            rejection_reason: None,
            legs,
        };

        // Validate affordability up front when the trigger price is already known.
        if let Some(estimated_price) = order.limit_price.or(order.stop_price) {
            if let Some(reason) = self.insufficient_funds_reason(
                &order.side,
                order.qty,
                estimated_price,
                order.asset.contract_size,
            ) {
                let mut rejected_order = order.clone();
                rejected_order.status = TradeUpdateEvent::Rejected;
                rejected_order.rejection_reason = Some(reason);
                self.orders.insert(order_id.clone(), rejected_order.clone());
                self.emit_trade_event(&rejected_order, TradeUpdateEvent::Rejected);
                self.mark_order_terminal_for_release(&order_id);
                return Ok(rejected_order);
            }
        }

        self.orders.insert(order_id.clone(), order);
        self.pending_orders.lock().push_back(order_id.clone());

        let created_order =
            self.orders
                .get(&order_id)
                .map(|o| o.clone())
                .ok_or(BrokerError::OrderError(
                    "Failed to retrieve created order".into(),
                ))?;

        // Emit PendingNew trade event
        self.emit_trade_event(&created_order, TradeUpdateEvent::PendingNew);

        Ok(created_order)
    }

    async fn cancel_order(&self, order_id: &str) -> Result<bool, BrokerError> {
        if let Some(mut order) = self.orders.get_mut(order_id) {
            if order.status == TradeUpdateEvent::Filled || order.status == TradeUpdateEvent::Closed
            {
                return Err(BrokerError::OrderCancellationError(
                    "Cannot cancel filled/closed order".into(),
                ));
            }
            order.status = TradeUpdateEvent::Cancelled;
            order.updated_at = self.current_time_ts();

            // Emit Cancelled trade event
            self.emit_trade_event(&order, TradeUpdateEvent::Cancelled);
            self.mark_order_terminal_for_release(order_id);
            Ok(true)
        } else {
            let now_ts = self.current_time_ts();
            let parent_order_id = self.orders.iter().find_map(|entry| {
                let order = entry.value();
                let Some(legs) = order.legs.as_ref() else {
                    return None;
                };

                let matches_leg = legs
                    .take_profit
                    .as_ref()
                    .and_then(|leg| leg.order_id.as_deref())
                    == Some(order_id)
                    || legs
                        .stop_loss
                        .as_ref()
                        .and_then(|leg| leg.order_id.as_deref())
                        == Some(order_id)
                    || legs
                        .trailing_stop
                        .as_ref()
                        .and_then(|leg| leg.order_id.as_deref())
                        == Some(order_id);

                if matches_leg {
                    Some(entry.key().clone())
                } else {
                    None
                }
            });

            if let Some(parent_order_id) = parent_order_id {
                if let Some(mut parent) = self.orders.get_mut(&parent_order_id) {
                    if Self::cancel_leg_on_parent(&mut parent, order_id, now_ts) {
                        parent.updated_at = now_ts;
                        self.emit_trade_event(&parent, TradeUpdateEvent::Cancelled);
                        return Ok(true);
                    }
                }
            }

            Err(BrokerError::OrderCancellationError(format!(
                "Order not found: {}",
                order_id
            )))
        }
    }

    async fn update_order(
        &self,
        order_id: &str,
        price: f64,
        qty: f64,
    ) -> Result<bool, BrokerError> {
        let now_ts = self.current_time_ts();
        if let Some(mut order) = self.orders.get_mut(order_id) {
            if matches!(
                order.status,
                TradeUpdateEvent::Filled | TradeUpdateEvent::PartialFilled
            ) {
                if let Some(legs) = order.legs.as_mut() {
                    if let Some(tp) = legs.take_profit.as_mut() {
                        if is_terminal_leg_status(&tp.status) {
                            return Err(BrokerError::OrderError(format!(
                                "Take profit leg is not active for filled order {}",
                                order_id
                            )));
                        }
                        tp.limit_price = Some(price);
                        tp.updated_at = now_ts;
                        order.qty = qty;
                        order.updated_at = now_ts;
                        return Ok(true);
                    }
                }
                return Err(BrokerError::OrderError(format!(
                    "Filled order {} has no active take profit leg to update",
                    order_id
                )));
            }
            order.limit_price = Some(price);
            order.qty = qty;
            order.updated_at = now_ts;
            return Ok(true);
        }

        for mut order in self.orders.iter_mut() {
            let Some(legs) = order.legs.as_mut() else {
                continue;
            };

            if update_order_leg(legs.take_profit.as_mut(), order_id, price, now_ts)
                || update_order_leg(legs.stop_loss.as_mut(), order_id, price, now_ts)
                || update_order_leg(legs.trailing_stop.as_mut(), order_id, price, now_ts)
            {
                order.qty = qty;
                order.updated_at = now_ts;
                return Ok(true);
            }
        }

        Err(BrokerError::OrderError("Order not found".into()))
    }

    async fn update_stop_loss(
        &self,
        order_id: &str,
        price: f64,
        qty: f64,
    ) -> Result<bool, BrokerError> {
        let now_ts = self.current_time_ts();
        if let Some(mut order) = self.orders.get_mut(order_id) {
            if matches!(
                order.status,
                TradeUpdateEvent::Filled | TradeUpdateEvent::PartialFilled
            ) {
                if let Some(legs) = order.legs.as_mut() {
                    if let Some(sl) = legs.stop_loss.as_mut() {
                        if is_terminal_leg_status(&sl.status) {
                            return Err(BrokerError::OrderError(format!(
                                "Stop loss leg is not active for filled order {}",
                                order_id
                            )));
                        }
                        sl.limit_price = Some(price);
                        sl.updated_at = now_ts;
                        order.qty = qty;
                        order.updated_at = now_ts;
                        return Ok(true);
                    }
                }
                return Err(BrokerError::OrderError(format!(
                    "Filled order {} has no active stop loss leg to update",
                    order_id
                )));
            }
            order.limit_price = Some(price);
            order.qty = qty;
            order.updated_at = now_ts;
            return Ok(true);
        }

        for mut order in self.orders.iter_mut() {
            let Some(legs) = order.legs.as_mut() else {
                continue;
            };

            if update_order_leg(legs.stop_loss.as_mut(), order_id, price, now_ts) {
                order.qty = qty;
                order.updated_at = now_ts;
                return Ok(true);
            }
        }

        Err(BrokerError::OrderError("Order not found".into()))
    }

    async fn close_position(
        &self,
        order_id: &str,
        qty: f64,
        price: Option<f64>,
    ) -> Result<bool, BrokerError> {
        debug!(
            "PaperBroker::close_position request order_id={} qty={:.4} price={:?}",
            order_id, qty, price
        );
        let Some(order) = self.orders.get(order_id).map(|order| order.clone()) else {
            return Err(BrokerError::OrderError(format!(
                "Order not found for close request: {}",
                order_id
            )));
        };
        let symbol = order.asset.symbol.clone();
        let Some(symbol_positions) = self.positions.get(&symbol) else {
            debug!(
                "PaperBroker::close_position no position found for order_id={} symbol={}",
                order_id, symbol
            );
            return Err(BrokerError::PositionError(format!(
                "No position found for order {} ({})",
                order_id, symbol
            )));
        };
        let Some(position) = symbol_positions.get(order_id) else {
            debug!(
                "PaperBroker::close_position no position bucket found for order_id={} symbol={}",
                order_id, symbol
            );
            return Err(BrokerError::PositionError(format!(
                "No open position found for order {} ({})",
                order_id, symbol
            )));
        };
        let position_qty = position.qty;
        drop(position);
        drop(symbol_positions);

        let close_qty = if qty <= 0.0 {
            position_qty
        } else {
            qty.min(position_qty)
        };
        if close_qty <= 0.0 {
            return Err(BrokerError::PositionError(format!(
                "No closable quantity found for order {} ({})",
                order_id, symbol
            )));
        }
        debug!(
            "PaperBroker::close_position queueing order_id={} qty={:.4} price={:?}",
            order_id, close_qty, price
        );
        self.close_orders_queue
            .lock()
            .push_back((order_id.to_string(), close_qty, price));
        debug!("PaperBroker::close_position queued successfully");
        Ok(true)
    }

    async fn close_all_positions(&self) -> Result<bool, BrokerError> {
        let order_ids: Vec<String> = self
            .positions
            .iter()
            .flat_map(|entry| {
                entry
                    .value()
                    .iter()
                    .map(|position| position.key().clone())
                    .collect::<Vec<_>>()
            })
            .collect();
        for order_id in order_ids {
            self.close_position(&order_id, 0.0, None).await?;
        }
        Ok(true)
    }

    async fn get_orders(&self) -> Result<Vec<Order>, BrokerError> {
        Ok(self
            .orders
            .iter()
            .map(|entry| entry.value().clone())
            .collect())
    }

    async fn get_order(&self, order_id: &str) -> Result<Order, BrokerError> {
        self.orders
            .get(order_id)
            .map(|o| o.clone())
            .ok_or(BrokerError::OrderError("Order not found".into()))
    }

    async fn get_positions(&self) -> Result<Vec<Position>, BrokerError> {
        // Return aggregated positions (one per symbol)
        let mut result = Vec::new();
        for entry in self.positions.iter() {
            let symbol = entry.key();
            if let Some(agg) = self.get_aggregate_position(symbol) {
                result.push(agg);
            }
        }
        Ok(result)
    }

    async fn get_position(&self, symbol: &str) -> Result<Position, BrokerError> {
        self.get_aggregate_position(symbol)
            .ok_or(BrokerError::PositionError("Position not found".into()))
    }

    async fn get_account(&self) -> Result<Account, BrokerError> {
        Ok(self.account.read().clone())
    }

    fn drain_trade_events(&self) -> Vec<(Order, TradeUpdateEvent)> {
        let events: Vec<(Order, TradeUpdateEvent)> = self.trade_events.lock().drain(..).collect();
        self.release_terminal_orders();
        events
    }

    async fn subscribe_to_trade_stream(
        &self,
        on_trade: Arc<dyn Fn((Order, TradeUpdateEvent)) + Send + Sync>,
    ) -> Result<(), BrokerError> {
        let mut subscribers = self.trade_stream_subscribers.lock();
        subscribers.push(on_trade);
        Ok(())
    }

    async fn unsubscribe_from_trade_stream(&self) -> Result<(), BrokerError> {
        let mut subscribers = self.trade_stream_subscribers.lock();
        subscribers.clear();
        Ok(())
    }
}
