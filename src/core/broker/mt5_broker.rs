use super::BrokerFeature;
use super::commands::{BrokerOrderRequest, SubmissionCorrelation, SubmissionResolution};
use super::mt5_bridge::{Mt5Bridge, Mt5RpcAction};
use super::traits::{Broker, OrderManagementProvider};
use super::types::{
    Account, AccountType, BrokerError, Order, OrderClass, OrderLeg, OrderLegs,
    OrderLifecycleSnapshot, OrderSide, OrderType, Position, TimeInForce, TradeUpdateEvent,
};
use crate::core::utils::tools::dynamic_round_for_asset;
use chrono::{DateTime, Utc};
use log::debug;
use std::sync::Arc;

#[derive(Clone)]
pub struct Mt5Broker {
    name: String,
    bridge: Arc<Mt5Bridge>,
}

impl Mt5Broker {
    async fn cancellation_status(&self, account: &str, order: &str)
        -> Result<Option<OrderLifecycleSnapshot>, BrokerError> {
        if account.trim().is_empty() || order.trim().is_empty()
            || self.bridge.account()?.account_id.trim() != account {
            return Err(BrokerError::AccountError("MT5 cancellation recovery account mismatch".into()));
        }
        let mut rows = self.reconcile_order_lifecycle(&[order.to_owned()]).await?;
        if self.bridge.account()?.account_id.trim() != account {
            return Err(BrokerError::AccountError("MT5 account changed during cancellation recovery".into()));
        }
        if rows.len() != 1 || rows[0].order_id != order {
            return Ok(None);
        }
        let row = rows.remove(0);
        if self.bridge.order(order).is_ok_and(|saved|
            row.symbol.as_deref().is_some_and(|symbol| symbol != saved.asset.symbol)) {
            return Ok(None);
        }
        Ok(Some(row))
    }

    pub(super) async fn read_account_cash_flows(
        &self,
        account: &str,
        source: Option<&str>,
        after: u64,
    ) -> Result<Option<aqe_live_protocol::AccountCashFlowPage>, BrokerError> {
        let payload = self.bridge.request_rpc_with_timeout(
            Mt5RpcAction::GetAccountCashFlows,
            serde_json::json!({"accountId":account,"sourceId":source,"afterSequence":after}),
            std::time::Duration::from_secs(3),
        ).await?;
        if payload.is_null() {
            return Ok(None);
        }
        let page: aqe_live_protocol::AccountCashFlowPage = serde_json::from_value(payload)
            .map_err(|_| BrokerError::AccountError("invalid MT5 account cash-flow response".into()))?;
        page.validate(account).map_err(|e| BrokerError::AccountError(e.into()))?;
        Ok(Some(page))
    }
    fn is_reconcilable_close_error(error: &BrokerError) -> bool {
        // FROZEN can race a broker-managed TP/SL just like POSITION_CLOSED.
        // Neither code alone proves the position closed; reconcile the ticket.
        matches!(error, BrokerError::TradeError(message) if matches!(message.trim(), "10029" | "10036"))
    }

    fn has_confirmed_cached_close(&self, order_id: &str) -> bool {
        self.bridge.order(order_id).is_ok_and(|order| {
            order.status == TradeUpdateEvent::Closed
                && order.filled_price.is_some_and(|price| price.is_finite() && price > 0.0)
                && order.realized_pnl.is_some_and(f64::is_finite)
                && order.commission.is_some_and(f64::is_finite)
                && order.swap.is_some_and(f64::is_finite)
        })
    }

    // Do not infer a close from absence in the active-position list. Require
    // ticket-scoped terminal history with actual execution values.
    fn reconciled_close_order(order: &Order, snapshot: &OrderLifecycleSnapshot) -> Option<Order> {
        if snapshot.order_id != order.order_id
            || snapshot.status != super::types::OrderLifecycleStatus::Closed
            || snapshot.symbol.as_deref().is_some_and(|symbol| symbol != order.asset.symbol)
        {
            return None;
        }
        let close = snapshot.close_details.as_ref()?;
        if !close.close_price.is_finite()
            || close.close_price <= 0.0
            || !close.realized_pnl.is_finite()
            || !close.commission.is_finite()
            || !close.swap.is_finite()
            || close.closed_at > Utc::now()
            || close.closed_at.timestamp() < order.filled_at.unwrap_or(order.created_at) as i64
        {
            return None;
        }
        let mut closed = order.clone();
        closed.status = TradeUpdateEvent::Closed;
        closed.filled_price = Some(close.close_price);
        closed.updated_at = close.closed_at.timestamp().max(0) as u64;
        closed.filled_at = Some(closed.updated_at);
        closed.realized_pnl = Some(close.realized_pnl);
        closed.position_accounting = close.validated_position_accounting(&order.order_id);
        closed.commission = Some(close.commission);
        closed.swap = Some(close.swap);
        Some(closed)
    }

    pub fn from_env() -> Result<Self, BrokerError> {
        Ok(Self::new(Mt5Bridge::shared_from_env()?))
    }

    pub fn new(bridge: Arc<Mt5Bridge>) -> Self {
        Self {
            name: "MT5 Broker".to_string(),
            bridge,
        }
    }

    fn now_ts() -> u64 {
        Utc::now().timestamp().max(0) as u64
    }

    fn order_comment(order: &Order) -> Option<String> {
        let strategy_type = order
            .strategy_type
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())?;
        let insight_id = order
            .insight_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())?;
        Some(format!("{}:{}", strategy_type, insight_id))
    }

    fn order_leg_limit_price(order: Option<&Order>, stop_loss: bool) -> Option<f64> {
        let legs = order?.legs.as_ref()?;
        let leg = if stop_loss {
            legs.stop_loss.as_ref()
        } else {
            legs.take_profit.as_ref()
        }?;
        leg.limit_price
            .filter(|price| price.is_finite() && *price > 0.0)
    }

    fn update_order_payload(
        order_id: &str,
        price: f64,
        qty: f64,
        existing_order: Option<&Order>,
        update_stop_loss: bool,
    ) -> serde_json::Value {
        let symbol = existing_order.map(|order| order.asset.symbol.clone());
        let comment = existing_order.and_then(Self::order_comment);
        let mut payload = serde_json::json!({
            "orderId": order_id,
            "symbol": symbol,
            "qty": qty,
            "price": price,
            "comment": comment,
            "insightId": existing_order.and_then(|order| order.insight_id.clone()),
            "strategyType": existing_order.and_then(|order| order.strategy_type.clone())
        });

        if update_stop_loss {
            payload["stopLoss"] = serde_json::json!(price);
            if let Some(take_profit) = Self::order_leg_limit_price(existing_order, false) {
                payload["takeProfit"] = serde_json::json!(take_profit);
            }
        } else {
            payload["takeProfit"] = serde_json::json!(price);
            if let Some(stop_loss) = Self::order_leg_limit_price(existing_order, true) {
                payload["stopLoss"] = serde_json::json!(stop_loss);
            }
        }

        payload
    }

    fn order_from_request(&self, insight: BrokerOrderRequest) -> Result<Order, BrokerError> {
        let qty = insight.quantity.ok_or_else(|| {
            BrokerError::OrderError(format!(
                "Cannot submit MT5 order for {} without a quantity",
                insight.symbol
            ))
        })?;
        if !qty.is_finite() || qty <= 0.0 {
            return Err(BrokerError::OrderError(format!(
                "Cannot submit MT5 order for {} with invalid quantity {}",
                insight.symbol, qty
            )));
        }

        let now_ts = Self::now_ts();
        let symbol = self.bridge.config().mt5_symbol(&insight.symbol).to_string();
        let mut asset = self.bridge.asset(&insight.symbol);
        asset.symbol = symbol;

        let legs = if insight.order_class == OrderClass::Bracket {
            let opposite_side = match insight.side.clone() {
                OrderSide::Buy => OrderSide::Sell,
                OrderSide::Sell => OrderSide::Buy,
            };
            Some(OrderLegs {
                take_profit: insight.take_profit_levels.as_ref().and_then(|levels| {
                    levels.last().copied().map(|price| OrderLeg {
                        order_id: None,
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
                }),
                stop_loss: insight.stop_loss_levels.as_ref().and_then(|levels| {
                    levels.last().copied().map(|price| OrderLeg {
                        order_id: None,
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
                }),
                trailing_stop: insight.trailing_stop_price.map(|gap| OrderLeg {
                    order_id: None,
                    limit_price: None,
                    trail_price: Some(gap),
                    side: opposite_side.clone(),
                    filled_price: None,
                    order_type: OrderType::TrailingStop,
                    status: TradeUpdateEvent::Pending,
                    order_class: OrderClass::Bracket,
                    created_at: now_ts,
                    updated_at: now_ts,
                    submitted_at: now_ts,
                    filled_at: None,
                }),
            })
        } else {
            None
        };

        Ok(Order {
            order_id: insight.client_order_id.clone(),
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
            time_in_force: TimeInForce::GTC,
            status: TradeUpdateEvent::PendingNew,
            order_class: insight.order_class,
            created_at: now_ts,
            updated_at: now_ts,
            submitted_at: now_ts,
            filled_at: None,
            realized_pnl: None,
            position_accounting: None,
            commission: None,
            swap: None,
            rejection_reason: None,
            legs,
        })
    }
}

#[async_trait::async_trait]
impl super::fill_history::ExecutionFillHistoryReader for Mt5Broker {
    async fn read(&self, account_id: &str, entry_order_id: &str, symbol: &str)
        -> Result<Option<super::fill_history::ExecutionFillHistory>, BrokerError> {
        let native_symbol = self.bridge.config().mt5_symbol(symbol).to_owned();
        let payload = self.bridge.request_rpc_with_timeout(
            Mt5RpcAction::GetExecutionFillHistory,
            serde_json::json!({"accountId":account_id,"entryOrderId":entry_order_id,"symbol":native_symbol,"invalidationVersion":1,"positionSnapshotVersion":1}),
            std::time::Duration::from_secs(5),
        ).await?;
        if payload.is_null() { return Ok(None); }
        let mut history: super::fill_history::ExecutionFillHistory = serde_json::from_value(payload)
            .map_err(|error| BrokerError::TradeError(format!("invalid fill history: {error}")))?;
        history.validate(account_id, entry_order_id, &native_symbol)?;
        history.symbol = symbol.to_owned();
        for fill in &mut history.fills { fill.symbol = symbol.to_owned(); }
        for invalidation in &mut history.invalidations { invalidation.symbol = symbol.to_owned(); }
        if let Some(position) = &mut history.position_snapshot { position.symbol = symbol.to_owned(); }
        Ok(Some(history))
    }
}

#[async_trait::async_trait]
impl super::traits::OrderLifecycleReconciler for Mt5Broker {
    async fn reconcile(
        &self,
        order_ids: &[String],
    ) -> Result<Vec<OrderLifecycleSnapshot>, BrokerError> {
        <Self as OrderManagementProvider>::reconcile_order_lifecycle(self, order_ids).await
    }
}

#[async_trait::async_trait]
impl super::position_valuation::PositionValuationReader for Mt5Broker {
    async fn read_positions(
        &self,
        account_id: &str,
        symbol: &str,
        position_ids: &[String],
    ) -> Result<Option<super::position_valuation::PositionValuations>, BrokerError> {
        use super::position_valuation::{PositionValuations, validate_position_valuation_request};
        validate_position_valuation_request(account_id, symbol, position_ids)
            .map_err(|e| BrokerError::TradeError(e.into()))?;
        // MT5 identifiers are canonical positive decimal IDs, not order tickets.
        if position_ids.iter().any(|id| id.parse::<i64>().ok().filter(|n| *n > 0)
            .is_none_or(|n| n.to_string() != *id)) {
            return Err(BrokerError::TradeError("invalid MT5 position identifier".into()));
        }
        let native_symbol = self.bridge.config().mt5_symbol(symbol).to_owned();
        validate_position_valuation_request(account_id, &native_symbol, position_ids)
            .map_err(|e| BrokerError::TradeError(e.into()))?;
        let payload = self.bridge.request_rpc_with_timeout(
            Mt5RpcAction::GetPositionValuations,
            serde_json::json!({"accountId":account_id,"symbol":native_symbol,"positionIds":position_ids,"valuationVersion":1}),
            std::time::Duration::from_secs(5),
        ).await?;
        if payload.is_null() { return Ok(None); }
        let mut values: PositionValuations = serde_json::from_value(payload)
            .map_err(|e| BrokerError::TradeError(format!("invalid position valuation: {e}")))?;
        values.validate(account_id, &native_symbol, position_ids)
            .map_err(|e| BrokerError::TradeError(e.into()))?;
        if values.provider != "mt5-position-v1" {
            return Err(BrokerError::TradeError("unexpected MT5 valuation provider".into()));
        }
        values.symbol = symbol.to_owned();
        Ok(Some(values))
    }
}

impl Broker for Mt5Broker {
    async fn connect(&self) -> Result<bool, BrokerError> {
        self.bridge.start().await?;
        self.bridge.wait_for_rpc_poll().await?;
        self.bridge.recover_runtime_state().await?;
        Ok(self.bridge.is_broker_connected())
    }

    async fn disconnect(&self) -> Result<bool, BrokerError> {
        self.bridge.shutdown().await;
        Ok(true)
    }

    fn is_connected(&self) -> bool {
        self.bridge.is_broker_connected()
    }

    fn get_current_time(&self) -> DateTime<Utc> {
        Utc::now()
    }

    fn get_name(&self) -> String {
        self.name.clone()
    }

    fn get_account_type(&self) -> Result<AccountType, BrokerError> {
        Ok(AccountType::Live)
    }

    fn get_features(&self) -> &'static [BrokerFeature] {
        &[
            BrokerFeature::MarketOrder,
            BrokerFeature::LimitOrder,
            BrokerFeature::AmendLimitPrice,
            BrokerFeature::AmendStopPrice,
            BrokerFeature::StopOrder,
            BrokerFeature::StopLimitOrder,
            BrokerFeature::OcoOrder,
            BrokerFeature::TrailingOrder,
            BrokerFeature::TradeEventStream,
            BrokerFeature::AccountStream,
            BrokerFeature::OrderLifecycleSnapshot,
            BrokerFeature::BrokerManagedOco,
            BrokerFeature::MaxTotalOrders,
            BrokerFeature::MaxTotalSize,
            BrokerFeature::AuthoritativeMarginEstimate,
        ]
    }

    fn configure_live_session(&self, session_id: &str) -> Result<(), BrokerError> {
        self.bridge.set_session_id(session_id);
        Ok(())
    }

    fn get_cached_lifecycle_snapshot(
        &self,
    ) -> Option<crate::core::broker::types::BrokerLifecycleSnapshot> {
        Some(crate::core::broker::types::BrokerLifecycleSnapshot {
            orders: self.bridge.orders(),
            positions: self.bridge.positions(),
            terminal_history: Vec::new(),
            snapshot_complete: self.bridge.has_complete_runtime_snapshot(),
            captured_at: Utc::now(),
        })
    }
}

impl OrderManagementProvider for Mt5Broker {
    fn account_cash_flow_reader(&self) -> Option<Arc<dyn super::account_cash_flow::AccountCashFlowReader>> {
        Some(Arc::new(self.clone()))
    }
    async fn estimate_order_margin(
        &self,
        request: &BrokerOrderRequest,
    ) -> Result<Option<f64>, BrokerError> {
        let quantity = request
            .quantity
            .filter(|value| value.is_finite() && *value > 0.0)
            .ok_or_else(|| {
                BrokerError::OrderError("margin estimate requires a positive quantity".into())
            })?;
        let symbol = self.bridge.config().mt5_symbol(&request.symbol);
        let response = self.bridge.request_rpc(Mt5RpcAction::EstimateMargin, serde_json::json!({
            "symbol": symbol,
            "qty": quantity,
            "side": match request.side { OrderSide::Buy => "Buy", OrderSide::Sell => "Sell" },
            "orderType": match request.order_type {
                OrderType::Market => "Market",
                OrderType::Limit => "Limit",
                OrderType::Stop => "Stop",
                OrderType::StopLimit => "StopLimit",
                OrderType::TrailingStop => "TrailingStop",
            },
            "price": request.limit_price.or(request.stop_price),
        })).await?;
        let margin = response
            .get("margin")
            .and_then(serde_json::Value::as_f64)
            .filter(|value| value.is_finite() && *value >= 0.0)
            .ok_or_else(|| {
                BrokerError::TradeError("MT5 returned an invalid margin estimate".into())
            })?;
        Ok(Some(margin))
    }

    async fn reconcile_submission(
        &self,
        account_id: &str,
        request: &BrokerOrderRequest,
    ) -> Result<SubmissionResolution, BrokerError> {
        let correlation = SubmissionCorrelation::new(account_id, request)?;
        if self.bridge.account()?.account_id.trim() != correlation.account_id {
            return Err(BrokerError::TradeError(
                "submission lookup account mismatch".into(),
            ));
        }
        let response = self.bridge.request_rpc(Mt5RpcAction::GetSubmission, serde_json::json!({
            "submissionKey": correlation.key, "submissionFingerprint": correlation.fingerprint,
            "submissionAccount": correlation.account_id,
        })).await?;
        let Some(value) = response.get("order").filter(|value| !value.is_null()) else {
            return Ok(SubmissionResolution::Unknown);
        };
        if response.get("fingerprint").and_then(|v| v.as_str())
            != Some(correlation.fingerprint.as_str())
        {
            return Err(BrokerError::TradeError(
                "submission lookup fingerprint mismatch".into(),
            ));
        }
        let mut order: Order = serde_json::from_value(value.clone())
            .map_err(|e| BrokerError::TradeError(format!("invalid submission receipt: {e}")))?;
        self.bridge.map_order_to_aqe(&mut order);
        if order.insight_id.as_deref() != Some(request.insight_id.as_str())
            || order.asset.symbol != request.symbol
            || order.side != request.side
            || order.order_id.is_empty()
        {
            return Err(BrokerError::TradeError(
                "submission receipt identity mismatch".into(),
            ));
        }
        Ok(SubmissionResolution::Confirmed(order))
    }
    fn lifecycle_reconciler(&self) -> Option<Arc<dyn super::traits::OrderLifecycleReconciler>> {
        Some(Arc::new(self.clone()))
    }

    async fn pending_cancellation_recovery(&self, account: &str, order: &str)
        -> Result<Option<OrderLifecycleSnapshot>, BrokerError> {
        if self.bridge.order(order).is_ok_and(|saved| matches!(saved.status,
            TradeUpdateEvent::Filled | TradeUpdateEvent::PartialFilled | TradeUpdateEvent::Closed
            | TradeUpdateEvent::Cancelled | TradeUpdateEvent::Expired | TradeUpdateEvent::Rejected)) {
            return Ok(None);
        }
        Ok(self.cancellation_status(account, order).await?.filter(|row|
            row.status == super::types::OrderLifecycleStatus::Pending
                && row.position_id.is_none() && row.position_qty.is_none()
                && row.symbol.as_ref().is_some_and(|symbol| !symbol.trim().is_empty())))
    }

    async fn retry_cancellation(&self, account: &str, order: &str) -> Result<bool, BrokerError> {
        use super::types::OrderLifecycleStatus;
        // Re-read under the account mutation lane: the earlier read can race a
        // fill. OrderDelete targets this exact ticket only; never place an entry
        // or close a position as a substitute for a failed cancellation.
        if self.pending_cancellation_recovery(account, order).await?.is_none() {
            return Ok(false);
        }
        let existing = match self.bridge.order(order) {
            Ok(saved) => Some(saved),
            Err(_) => self.bridge.request_orders().await?.into_iter().find(|saved| saved.order_id == order),
        };
        let Some(mut existing) = existing else { return Ok(false); };
        if matches!(existing.status, TradeUpdateEvent::Filled | TradeUpdateEvent::PartialFilled
            | TradeUpdateEvent::Closed | TradeUpdateEvent::Cancelled | TradeUpdateEvent::Expired | TradeUpdateEvent::Rejected)
            || self.bridge.account()?.account_id.trim() != account {
            return Ok(false);
        }
        let result = self.bridge.request_rpc(Mt5RpcAction::CancelOrder,
            serde_json::json!({"orderId":order,"insightId":existing.insight_id,"strategyType":existing.strategy_type})).await;
        // An ACK (or a timeout) alone cannot establish cancellation. Older EAs
        // also return ACK before terminal history is visible. Keep the original
        // ambiguous receipt unless the exact ticket is now terminal.
        let after = self.cancellation_status(account, order).await?;
        if after.is_some_and(|row| matches!(row.status,
            OrderLifecycleStatus::Cancelled | OrderLifecycleStatus::Expired)) {
            // Confirmation must reach the canonical Insight reducer, not just
            // consume its receipt: a Cancel ACK alone never changes its state.
            if self.bridge.order(order).is_ok_and(|saved| matches!(saved.status,
                TradeUpdateEvent::Filled | TradeUpdateEvent::PartialFilled | TradeUpdateEvent::Closed | TradeUpdateEvent::Rejected)) {
                return Ok(false);
            }
            if !self.bridge.order(order).is_ok_and(|saved| matches!(saved.status,
                TradeUpdateEvent::Cancelled | TradeUpdateEvent::Expired)) {
                existing.status = TradeUpdateEvent::Cancelled;
                existing.updated_at = Self::now_ts();
                self.bridge.emit_order_event(existing, TradeUpdateEvent::Cancelled);
            }
            return Ok(true);
        }
        result.map(|_| false)
    }

    fn execution_fill_history_reader(
        &self,
    ) -> Option<Arc<dyn super::fill_history::ExecutionFillHistoryReader>> {
        Some(Arc::new(self.clone()))
    }

    fn position_valuation_reader(
        &self,
    ) -> Option<Arc<dyn super::position_valuation::PositionValuationReader>> {
        Some(Arc::new(self.clone()))
    }

    async fn submit_order(&self, insight: BrokerOrderRequest) -> Result<Order, BrokerError> {
        let correlation = SubmissionCorrelation::new(&self.bridge.account()?.account_id, &insight)?;
        let order = self.order_from_request(insight)?;
        let take_profit = order
            .legs
            .as_ref()
            .and_then(|legs| legs.take_profit.as_ref())
            .and_then(|leg| leg.limit_price)
            .map(|price| dynamic_round_for_asset(price, &order.asset));
        let stop_loss = order
            .legs
            .as_ref()
            .and_then(|legs| legs.stop_loss.as_ref())
            .and_then(|leg| leg.limit_price)
            .map(|price| dynamic_round_for_asset(price, &order.asset));
        let side = match order.side {
            OrderSide::Buy => "Buy",
            OrderSide::Sell => "Sell",
        };
        let order_type = match order.order_type {
            OrderType::Market => "Market",
            OrderType::Limit => "Limit",
            OrderType::Stop => "Stop",
            OrderType::StopLimit => "StopLimit",
            OrderType::TrailingStop => "TrailingStop",
        };
        let mut payload = serde_json::json!({
            "submissionKey": correlation.key,
            "submissionFingerprint": correlation.fingerprint,
            "submissionAccount": correlation.account_id,
            "clientOrderId": order.order_id.clone(),
            "insightId": order.insight_id.clone(),
            "strategyType": order.strategy_type.clone(),
            "symbol": order.asset.symbol.clone(),
            "qty": order.qty,
            "price": order.limit_price.or(order.stop_price),
            "limitPrice": order.limit_price,
            "stopPrice": order.stop_price,
            "side": side,
            "orderType": order_type,
        });
        if let Some(comment) = Self::order_comment(&order) {
            payload["comment"] = serde_json::json!(comment);
        }
        if let Some(take_profit) = take_profit.filter(|price| *price > 0.0) {
            payload["takeProfit"] = serde_json::json!(take_profit);
        }
        if let Some(stop_loss) = stop_loss.filter(|price| *price > 0.0) {
            payload["stopLoss"] = serde_json::json!(stop_loss);
        }
        if let Some(trailing_gap) = order
            .legs
            .as_ref()
            .and_then(|legs| legs.trailing_stop.as_ref())
            .and_then(|leg| leg.trail_price)
            .filter(|gap| gap.is_finite() && *gap > 0.0)
        {
            payload["trailingStop"] =
                serde_json::json!(dynamic_round_for_asset(trailing_gap, &order.asset));
        }
        self.bridge
            .request_order_action(Mt5RpcAction::SubmitOrder, Some(order), payload)
            .await
    }

    async fn cancel_order(&self, order_id: &str) -> Result<bool, BrokerError> {
        let existing_order = match self.bridge.order(order_id) {
            Ok(order) => Some(order),
            Err(_) => self
                .bridge
                .request_orders()
                .await?
                .into_iter()
                .find(|order| order.order_id == order_id),
        };

        let Some(mut existing_order) = existing_order else {
            return Err(BrokerError::OrderCancellationError(format!(
                "MT5 order {} was not found for cancellation",
                order_id
            )));
        };

        if matches!(
            existing_order.status,
            TradeUpdateEvent::Filled
                | TradeUpdateEvent::Closed
                | TradeUpdateEvent::Cancelled
                | TradeUpdateEvent::Rejected
                | TradeUpdateEvent::Expired
        ) {
            return Err(BrokerError::OrderCancellationError(format!(
                "MT5 order {} is already terminal with status {:?}",
                order_id, existing_order.status
            )));
        }

        self.bridge
            .request_rpc(
                Mt5RpcAction::CancelOrder,
                serde_json::json!({
                    "orderId": order_id,
                    "insightId": existing_order.insight_id.clone(),
                    "strategyType": existing_order.strategy_type.clone()
                }),
            )
            .await?;

        existing_order.status = TradeUpdateEvent::Cancelled;
        existing_order.updated_at = Self::now_ts();
        self.bridge
            .emit_order_event(existing_order, TradeUpdateEvent::Cancelled);

        Ok(true)
    }

    async fn amend_entry_prices(
        &self,
        order_id: &str,
        limit_price: Option<f64>,
        stop_price: Option<f64>,
    ) -> Result<bool, BrokerError> {
        if (limit_price.is_none() && stop_price.is_none())
            || limit_price
                .into_iter()
                .chain(stop_price)
                .any(|p| !p.is_finite() || p <= 0.0)
        {
            return Err(BrokerError::UnsupportedOperation(
                "Invalid entry price amendment".into(),
            ));
        }
        // Dedicated RPC: older EAs must not acknowledge this as a TP/SL update.
        let result = self.bridge.request_rpc(Mt5RpcAction::AmendEntry,
            serde_json::json!({"orderId": order_id, "limitPrice": limit_price, "stopPrice": stop_price})).await?;
        if result.get("amended").and_then(serde_json::Value::as_bool) != Some(true) {
            return Err(BrokerError::UnsupportedOperation(
                result
                    .get("reason")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("Entry amendment not acknowledged")
                    .into(),
            ));
        }
        // Patch under the cache lock; never overwrite a concurrent fill snapshot.
        self.bridge
            .acknowledge_entry_prices(order_id, limit_price, stop_price);
        Ok(true)
    }

    async fn update_take_profit(
        &self,
        order_id: &str,
        price: f64,
        qty: f64,
    ) -> Result<bool, BrokerError> {
        let existing_order = self.bridge.order(order_id).ok();
        self.bridge
            .request_rpc(
                Mt5RpcAction::UpdateOrder,
                Self::update_order_payload(order_id, price, qty, existing_order.as_ref(), false),
            )
            .await?;
        if let Some(mut order) = existing_order {
            if let Some(take_profit) = order
                .legs
                .as_mut()
                .and_then(|legs| legs.take_profit.as_mut())
            {
                take_profit.limit_price = Some(price);
                take_profit.updated_at = Self::now_ts();
            }
            order.updated_at = Self::now_ts();
            self.bridge.upsert_order(order);
        }
        Ok(true)
    }

    async fn update_stop_loss(
        &self,
        order_id: &str,
        price: f64,
        qty: f64,
    ) -> Result<bool, BrokerError> {
        let existing_order = self.bridge.order(order_id).ok();
        self.bridge
            .request_rpc(
                Mt5RpcAction::UpdateOrder,
                Self::update_order_payload(order_id, price, qty, existing_order.as_ref(), true),
            )
            .await?;
        if let Some(mut order) = existing_order {
            if let Some(stop_loss) = order.legs.as_mut().and_then(|legs| legs.stop_loss.as_mut()) {
                stop_loss.limit_price = Some(price);
                stop_loss.updated_at = Self::now_ts();
            }
            order.updated_at = Self::now_ts();
            self.bridge.upsert_order(order);
        }
        Ok(true)
    }

    async fn close_position(
        &self,
        order_id: &str,
        qty: f64,
        price: Option<f64>,
    ) -> Result<bool, BrokerError> {
        let existing_order = self.bridge.order(order_id).ok();
        let symbol = existing_order
            .as_ref()
            .map(|order| order.asset.symbol.clone());
        let comment = existing_order.as_ref().and_then(Self::order_comment);
        let response = self
            .bridge
            .request_rpc(
                Mt5RpcAction::ClosePosition,
                serde_json::json!({
                    "orderId": order_id,
                    "symbol": symbol,
                    "qty": qty,
                    "price": price,
                    "comment": comment,
                    "insightId": existing_order.as_ref().and_then(|order| order.insight_id.clone()),
                    "strategyType": existing_order.as_ref().and_then(|order| order.strategy_type.clone())
                }),
            )
            .await;
        let response = match response {
            Ok(response) => response,
            Err(error) if Self::is_reconcilable_close_error(&error) => {
                // The broker can close a position while our close is in flight.
                // Reconcile read-only; never send a second close or fabricate PnL.
                if let Some(order) = existing_order.as_ref() {
                    for attempt in 0..3 {
                        if attempt > 0 {
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        }
                        // Native terminal events are also broker evidence. Their
                        // close event is already queued for the insight reducer.
                        if self.has_confirmed_cached_close(order_id) {
                            return Ok(true);
                        }
                        let snapshots = match tokio::time::timeout(
                            std::time::Duration::from_secs(1),
                            self.reconcile_order_lifecycle(&[order_id.to_string()]),
                        )
                        .await
                        {
                            Ok(Ok(snapshots)) => snapshots,
                            _ if self.has_confirmed_cached_close(order_id) => return Ok(true),
                            _ => return Err(error),
                        };
                        if let Some(closed) = snapshots
                            .iter()
                            .find_map(|snapshot| Self::reconciled_close_order(order, snapshot))
                        {
                            log::info!(
                                "MT5 close response reconciled: order_id={} error={} confirmed closed; continuing execution",
                                order_id, error
                            );
                            self.bridge.emit_order_event(closed, TradeUpdateEvent::Closed);
                            return Ok(true);
                        }
                    }
                }
                if self.has_confirmed_cached_close(order_id) {
                    return Ok(true);
                }
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        if let Ok(mut close_order) = serde_json::from_value::<Order>(response) {
            close_order.asset.symbol = self.bridge.aqe_symbol(&close_order.asset.symbol);
            let event = close_order.status.clone();
            debug!(
                "MT5 close_position RPC confirmed order_id={} status={:?} realized_pnl={:?} commission={:?}",
                close_order.order_id,
                close_order.status,
                close_order.realized_pnl,
                close_order.commission
            );
            self.bridge.emit_order_event(close_order, event);
        } else if let Some(mut close_order) = existing_order {
            close_order.status = TradeUpdateEvent::Closed;
            close_order.filled_qty = if qty > 0.0 { qty } else { close_order.qty };
            if price.is_some() {
                close_order.filled_price = price;
            }
            close_order.updated_at = Self::now_ts();
            debug!(
                "MT5 close_position RPC returned non-order payload for order_id={}; waiting for broker trade event for commission/PnL",
                close_order.order_id
            );
        }
        Ok(true)
    }

    async fn close_all_positions(&self) -> Result<bool, BrokerError> {
        self.bridge
            .request_rpc(Mt5RpcAction::CloseAllPositions, serde_json::json!({}))
            .await?;
        Ok(true)
    }

    async fn get_orders(&self) -> Result<Vec<Order>, BrokerError> {
        self.bridge.request_orders().await
    }

    async fn reconcile_order_lifecycle(
        &self,
        order_ids: &[String],
    ) -> Result<Vec<OrderLifecycleSnapshot>, BrokerError> {
        if order_ids.is_empty() {
            return Ok(Vec::new());
        }
        let payload = self
            .bridge
            .request_rpc(
                Mt5RpcAction::ReconcileOrders,
                serde_json::json!({ "orderIds": order_ids }),
            )
            .await?;
        let mut snapshots = serde_json::from_value::<Vec<OrderLifecycleSnapshot>>(payload)
            .map_err(|error| {
                BrokerError::OrderError(format!(
                    "failed to decode MT5 order lifecycle reconciliation: {error}"
                ))
            })?;
        for snapshot in &mut snapshots {
            if let Some(symbol) = snapshot.symbol.as_mut() {
                *symbol = self.bridge.aqe_symbol(symbol);
            }
        }
        Ok(snapshots)
    }

    async fn get_order(&self, order_id: &str) -> Result<Order, BrokerError> {
        self.bridge
            .request_orders()
            .await?
            .into_iter()
            .find(|order| order.order_id == order_id)
            .ok_or_else(|| BrokerError::OrderError(format!("MT5 order {} not found", order_id)))
    }

    async fn get_positions(&self) -> Result<Vec<Position>, BrokerError> {
        self.bridge.request_positions().await
    }

    async fn get_position(&self, symbol: &str) -> Result<Position, BrokerError> {
        self.bridge.request_positions().await?;
        self.bridge.position(symbol)
    }

    async fn get_account(&self) -> Result<Account, BrokerError> {
        self.bridge.request_account().await
    }

    fn get_cached_account(&self) -> Option<Account> {
        self.bridge.account().ok()
    }

    fn drain_trade_events_bounded(&self, max_events: usize) -> Vec<(Order, TradeUpdateEvent)> {
        self.bridge.drain_trade_events_bounded(max_events)
    }

    fn has_pending_trade_events(&self) -> bool {
        self.bridge.has_pending_trade_events()
    }

    fn drain_broker_events(&self) -> Vec<crate::core::broker::types::BrokerOrderEvent> {
        self.bridge.drain_broker_events()
    }

    async fn subscribe_to_trade_stream(
        &self,
        on_trade: Arc<dyn Fn((Order, TradeUpdateEvent)) + Send + Sync>,
    ) -> Result<(), BrokerError> {
        self.bridge.subscribe_trade_stream(on_trade);
        Ok(())
    }

    async fn subscribe_to_broker_event_stream(
        &self,
        on_trade: Arc<dyn Fn(crate::core::broker::types::BrokerOrderEvent) + Send + Sync>,
    ) -> Result<(), BrokerError> {
        self.bridge.subscribe_broker_event_stream(on_trade);
        Ok(())
    }

    async fn unsubscribe_from_trade_stream(&self) -> Result<(), BrokerError> {
        self.bridge.clear_trade_subscribers();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::alpha::WrappedAlphaModel;
    use crate::core::broker::mt5_bridge::Mt5BridgeConfig;
    use crate::core::broker::types::{
        Asset, AssetExchange, AssetStatus, AssetType, Position, Quote,
    };
    use crate::core::indicators::Indicator;
    use crate::core::insight::types::StrategyType;
    use crate::core::insight::{Insight, InsightCollection, InsightStrategyContext};
    use crate::core::pipeline::WrappedInsightPipe;
    use crate::core::strategy::{StrategyContext, StrategyMode, TeardownCleanupReport};
    use crate::core::universe::WrappedUniverseModel;
    use crate::core::utils::timeframe::{TimeFrame, TimeFrameUnit};
    use crate::core::utils::tools::TradingTools;
    use dashmap::DashMap;
    use polars::prelude::DataFrame;
    use serde_json::Value;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use uuid::Uuid;

    fn sample_asset() -> Asset {
        Asset {
            id: "EURUSD".to_string(),
            symbol: "EURUSD".to_string(),
            name: "EURUSD".to_string(),
            asset_type: AssetType::Forex,
            status: AssetStatus::Active,
            exchange: AssetExchange::UNKNOWN("MT5".to_string()),
            tradable: true,
            marginable: true,
            shortable: true,
            fractional: true,
            min_order_size: Some(0.01),
            quantity_base: Some(2),
            max_order_size: None,
            max_total_size: None,
            min_price_increment: Some(0.0001),
            price_base: Some(5),
            contract_size: None,
            trading_calendar: None,
            fees: Default::default(),
        }
    }

    fn sample_order() -> Order {
        Order {
            order_id: "order-1".to_string(),
            insight_id: Some("insight-1".to_string()),
            strategy_type: Some("Strategy".to_string()),
            asset: sample_asset(),
            qty: 1.0,
            filled_qty: 1.0,
            limit_price: None,
            filled_price: Some(100.0),
            stop_price: None,
            side: OrderSide::Buy,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::GTC,
            status: TradeUpdateEvent::Filled,
            order_class: OrderClass::Bracket,
            created_at: 0,
            updated_at: 0,
            submitted_at: 0,
            filled_at: Some(0),
            realized_pnl: None,
            position_accounting: None,
            commission: None,
            swap: None,
            rejection_reason: None,
            legs: Some(OrderLegs {
                take_profit: Some(OrderLeg {
                    order_id: None,
                    limit_price: Some(120.0),
                    trail_price: None,
                    side: OrderSide::Sell,
                    filled_price: None,
                    order_type: OrderType::Limit,
                    status: TradeUpdateEvent::Pending,
                    order_class: OrderClass::Bracket,
                    created_at: 0,
                    updated_at: 0,
                    submitted_at: 0,
                    filled_at: None,
                }),
                stop_loss: Some(OrderLeg {
                    order_id: None,
                    limit_price: Some(95.0),
                    trail_price: None,
                    side: OrderSide::Sell,
                    filled_price: None,
                    order_type: OrderType::Stop,
                    status: TradeUpdateEvent::Pending,
                    order_class: OrderClass::Bracket,
                    created_at: 0,
                    updated_at: 0,
                    submitted_at: 0,
                    filled_at: None,
                }),
                trailing_stop: None,
            }),
        }
    }

    #[test]
    fn already_closed_error_is_narrowly_classified() {
        for code in ["10029", "10036"] {
            assert!(Mt5Broker::is_reconcilable_close_error(&BrokerError::TradeError(code.into())));
        }
        for code in ["10039", "10030", "timeout", "110036"] {
            assert!(!Mt5Broker::is_reconcilable_close_error(&BrokerError::TradeError(code.into())));
        }
    }

    #[test]
    fn already_closed_requires_matching_terminal_history_and_preserves_actual_pnl() {
        use super::super::types::OrderLifecycleStatus;
        let order = sample_order();
        let mut snapshot: OrderLifecycleSnapshot = serde_json::from_value(serde_json::json!({
            "orderId": "order-1", "status": "Closed",
            "closeDetails": {"closePrice": 98.5, "closedAt": "2026-10-01T09:29:55Z",
                "realizedPnl": -1.5, "commission": 0.2, "swap": -0.1}
        })).unwrap();
        let closed = Mt5Broker::reconciled_close_order(&order, &snapshot).unwrap();
        assert_eq!(closed.status, TradeUpdateEvent::Closed);
        assert_eq!(closed.filled_price, Some(98.5));
        assert_eq!(closed.realized_pnl, Some(-1.5));
        assert_eq!(closed.commission, Some(0.2));
        assert_eq!(closed.swap, Some(-0.1));
        assert_eq!(closed.insight_id, order.insight_id);
        snapshot.order_id = "another-position".into();
        assert!(Mt5Broker::reconciled_close_order(&order, &snapshot).is_none());
        snapshot.order_id = order.order_id.clone();
        for state in [OrderLifecycleStatus::Unknown, OrderLifecycleStatus::PositionOpen,
            OrderLifecycleStatus::Filled, OrderLifecycleStatus::Cancelled] {
            snapshot.status = state;
            assert!(Mt5Broker::reconciled_close_order(&order, &snapshot).is_none());
        }
        snapshot.status = OrderLifecycleStatus::Closed;
        snapshot.close_details = None;
        assert!(Mt5Broker::reconciled_close_order(&order, &snapshot).is_none());
    }

    #[test]
    fn update_order_payload_sets_take_profit_and_preserves_stop_loss() {
        let order = sample_order();
        let payload = Mt5Broker::update_order_payload("order-1", 121.5, 1.0, Some(&order), false);

        assert_eq!(payload["orderId"], serde_json::json!("order-1"));
        assert_eq!(payload["symbol"], serde_json::json!("EURUSD"));
        assert_eq!(payload["insightId"], serde_json::json!("insight-1"));
        assert_eq!(payload["strategyType"], serde_json::json!("Strategy"));
        assert_eq!(payload["takeProfit"], serde_json::json!(121.5));
        assert_eq!(payload["stopLoss"], serde_json::json!(95.0));
    }

    #[test]
    fn stop_only_update_without_cached_order_never_invents_a_take_profit() {
        let payload = Mt5Broker::update_order_payload("position", 110.0, 1.0, None, true);
        assert_eq!(payload["stopLoss"], 110.0);
        assert!(payload.get("takeProfit").is_none());
        let ea = include_str!("../../../integrations/mt5/AqeMt5BridgeEA.mq5");
        assert!(ea.contains("double take_profit = ExtractNumber(json, \"takeProfit\", -1.0);"));
        assert!(!ea.contains("double take_profit = ExtractNumber(json, \"takeProfit\", price);"));
    }

    #[test]
    fn update_order_payload_sets_stop_loss_and_preserves_take_profit() {
        let order = sample_order();
        let payload = Mt5Broker::update_order_payload("order-1", 94.0, 1.0, Some(&order), true);

        assert_eq!(payload["orderId"], serde_json::json!("order-1"));
        assert_eq!(payload["symbol"], serde_json::json!("EURUSD"));
        assert_eq!(payload["stopLoss"], serde_json::json!(94.0));
        assert_eq!(payload["takeProfit"], serde_json::json!(120.0));
    }

    #[test]
    fn lifecycle_reconciliation_payload_decodes_broker_neutral_states() {
        let snapshots = serde_json::from_value::<Vec<OrderLifecycleSnapshot>>(serde_json::json!([
            {
                "orderId": "1891391279",
                "status": "PositionOpen",
                "positionId": "1891391279",
                "positionQty": 0.81,
                "symbol": "ETHUSD",
                "detail": "open_position"
            },
            {
                "orderId": "1891390354",
                "status": "Closed",
                "positionId": "1891390354",
                "positionQty": null,
                "symbol": "ETHUSD",
                "detail": "position_entry_and_exit_volume_confirmed",
                "closeDetails": {
                    "closePrice": 2450.5,
                    "closedAt": "2026-09-05T12:11:28.388Z",
                    "realizedPnl": -18.5,
                    "commission": 1.0,
                    "swap": -0.5
                }
            }
        ]))
        .expect("MT5 lifecycle snapshots should use the broker-neutral wire format");

        assert_eq!(snapshots.len(), 2);
        assert_eq!(
            snapshots[0].status,
            super::super::types::OrderLifecycleStatus::PositionOpen
        );
        assert_eq!(snapshots[0].position_qty, Some(0.81));
        assert!(snapshots[0].close_details.is_none());
        let details = snapshots[1].close_details.as_ref().unwrap();
        assert_eq!(details.close_price, 2450.5);
        assert_eq!(details.realized_pnl, -18.5);
        assert_eq!(
            serde_json::to_value(&snapshots[1]).unwrap()["closeDetails"]["commission"],
            1.0
        );
        assert_eq!(
            snapshots[1].status,
            super::super::types::OrderLifecycleStatus::Closed
        );
    }

    #[test]
    fn mt5_declares_native_oco_but_not_partial_close() {
        let bridge = Arc::new(Mt5Bridge::new(Mt5BridgeConfig {
            bind_addr: "127.0.0.1:18080".parse().unwrap(),
            token: "test".to_string(),
            request_timeout: Duration::from_secs(1),
            connect_timeout: Duration::from_secs(1),
            poll_interval: Duration::from_millis(100),
            symbol_map: HashMap::new(),
        }));
        let broker = Mt5Broker::new(bridge);

        assert!(broker.get_features().contains(&BrokerFeature::OcoOrder));
        assert!(
            broker
                .get_features()
                .contains(&BrokerFeature::TrailingOrder)
        );
        assert!(!broker.get_features().contains(&BrokerFeature::PartialClose));
        assert!(
            !broker
                .get_features()
                .contains(&BrokerFeature::BuyingPowerLeveraged)
        );
    }

    #[test]
    fn order_from_insight_preserves_trailing_stop_gap() {
        let bridge = Arc::new(Mt5Bridge::new(Mt5BridgeConfig {
            bind_addr: "127.0.0.1:18080".parse().unwrap(),
            token: "test".to_string(),
            request_timeout: Duration::from_secs(1),
            connect_timeout: Duration::from_secs(1),
            poll_interval: Duration::from_millis(100),
            symbol_map: HashMap::new(),
        }));
        let broker = Mt5Broker::new(bridge);
        let mut insight = Insight::new(
            OrderSide::Buy,
            "EURUSD".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            100,
            None,
        );
        insight
            .set_quantity(Some(1.0))
            .set_stop_loss(Some(1.05))
            .set_trailing_stop_price(Some(0.0025));

        let order = broker
            .order_from_request(insight.into())
            .expect("trailing-stop insight should form an MT5 order");
        let trailing = order
            .legs
            .as_ref()
            .and_then(|legs| legs.trailing_stop.as_ref())
            .expect("MT5 order should retain the trailing leg");
        assert_eq!(trailing.order_type, OrderType::TrailingStop);
        assert_eq!(trailing.trail_price, Some(0.0025));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "places a live MT5 test order; requires the EA polling 0.0.0.0:18070 with token test"]
    async fn live_mt5_market_order_updates_take_profit_and_stop_loss_after_fill() {
        let bind_addr =
            std::env::var("AQE_MT5_TEST_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:18070".to_string());
        let token = std::env::var("AQE_MT5_TEST_TOKEN").unwrap_or_else(|_| "test".to_string());
        let bridge = Arc::new(Mt5Bridge::new(Mt5BridgeConfig {
            bind_addr: bind_addr.parse().unwrap(),
            token,
            request_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(60),
            poll_interval: Duration::from_millis(100),
            symbol_map: HashMap::new(),
        }));
        bridge.set_session_id(format!("aqe-mt5-tp-sl-test-{}", Uuid::new_v4()));
        let broker = Mt5Broker::new(bridge.clone());

        broker
            .connect()
            .await
            .expect("MT5 EA should poll the AQE test bridge with the configured token");

        let existing_positions = broker
            .get_positions()
            .await
            .expect("MT5 positions should load before live test");
        let existing_orders = broker
            .get_orders()
            .await
            .expect("MT5 orders should load before live test");
        println!(
            "pre-test MT5 state positions={} pending_orders={}",
            existing_positions.len(),
            existing_orders.len()
        );

        let market = select_live_test_market(&bridge, &existing_positions, &existing_orders).await;
        println!(
            "selected MT5 live test market symbol={} qty={} mid={} tick={}",
            market.symbol, market.qty, market.mid_price, market.price_step
        );

        let initial_tp = round_to_step(
            market.mid_price + live_test_bracket_distance(market.mid_price, market.price_step),
            market.price_step,
        );
        let initial_sl = round_to_step(
            market.mid_price - live_test_bracket_distance(market.mid_price, market.price_step),
            market.price_step,
        );

        let mut insight = Insight::new(
            OrderSide::Buy,
            market.symbol.clone(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            100,
            None,
        );
        insight.quantity = Some(market.qty);
        insight.order_type = OrderType::Market;
        insight.order_class = OrderClass::Bracket;
        insight.take_profit_levels = Some(vec![initial_tp]);
        insight.stop_loss_levels = Some(vec![initial_sl]);
        let mut managed_insight = insight.clone();

        let submitted_order = broker
            .submit_order(insight.into())
            .await
            .expect("MT5 market order should submit and fill");
        println!(
            "submitted order_id={} status={:?} filled_qty={} filled_price={:?}",
            submitted_order.order_id,
            submitted_order.status,
            submitted_order.filled_qty,
            submitted_order.filled_price
        );

        let test_result = async {
            let filled_order = wait_for_trade_event(
                &broker,
                &submitted_order.order_id,
                TradeUpdateEvent::Filled,
                Duration::from_secs(20),
            )
            .await?;
            println!(
                "received fill event before updates order_id={} filled_price={:?}",
                filled_order.order_id, filled_order.filled_price
            );

            managed_insight.legs = submitted_order.legs.clone().unwrap_or_default();
            let mut ctx = LiveMt5InsightContext::new(&broker).await?;
            ctx.bind_insight_context(&mut managed_insight);
            managed_insight.order_accepted(&submitted_order.order_id);

            let refreshed_order =
                refresh_order(&broker, &bridge, &submitted_order.order_id).await?;
            let entry_price = refreshed_order
                .filled_price
                .or(filled_order.filled_price)
                .unwrap_or(market.mid_price)
                .max(market.price_step);
            managed_insight.position_filled(
                entry_price,
                filled_order.filled_qty,
                &submitted_order.order_id,
                filled_order.commission,
            );

            let tp_prices = [
                round_to_step(entry_price * 1.120, market.price_step),
                round_to_step(entry_price * 1.140, market.price_step),
                round_to_step(entry_price * 1.160, market.price_step),
            ];
            let sl_prices = [
                round_to_step(entry_price * 0.880, market.price_step),
                round_to_step(entry_price * 0.860, market.price_step),
                round_to_step(entry_price * 0.840, market.price_step),
            ];

            for (index, take_profit) in tp_prices.into_iter().enumerate() {
                drain_trade_events(&broker, &format!("before TP update {}", index + 1));
                loop {
                    match managed_insight
                        .update_take_profit(&mut ctx, Some(vec![take_profit]))
                        .map_err(|error| error.to_string())?
                    {
                        crate::core::insight::InsightMutationState::Applied => {
                            ctx.persist_insight(&managed_insight).await?;
                            break;
                        }
                        crate::core::insight::InsightMutationState::Pending(receipt) => {
                            ctx.wait_for_command(&receipt, &managed_insight).await?
                        }
                    }
                }
                expect_level(
                    "take_profit_levels",
                    managed_insight.take_profit_levels(),
                    take_profit,
                )?;
                expect_level(
                    "take_profit_leg.limit_price",
                    managed_insight
                        .legs
                        .take_profit
                        .as_ref()
                        .and_then(|leg| leg.limit_price)
                        .map(|price| vec![price]),
                    take_profit,
                )?;
                drain_trade_events(&broker, &format!("after TP update {}", index + 1));
                println!(
                    "TP update {} insight+RPC accepted order_id={} insight_tp={:?} leg_tp={:?}",
                    index + 1,
                    submitted_order.order_id,
                    managed_insight.take_profit_levels(),
                    managed_insight
                        .legs
                        .take_profit
                        .as_ref()
                        .and_then(|leg| leg.limit_price)
                );
            }

            for (index, stop_loss) in sl_prices.into_iter().enumerate() {
                drain_trade_events(&broker, &format!("before SL update {}", index + 1));
                loop {
                    match managed_insight
                        .update_stop_loss(&mut ctx, Some(vec![stop_loss]))
                        .map_err(|error| error.to_string())?
                    {
                        crate::core::insight::InsightMutationState::Applied => {
                            ctx.persist_insight(&managed_insight).await?;
                            break;
                        }
                        crate::core::insight::InsightMutationState::Pending(receipt) => {
                            ctx.wait_for_command(&receipt, &managed_insight).await?
                        }
                    }
                }
                expect_level(
                    "stop_loss_levels",
                    managed_insight.stop_loss_levels(),
                    stop_loss,
                )?;
                expect_level(
                    "stop_loss_leg.limit_price",
                    managed_insight
                        .legs
                        .stop_loss
                        .as_ref()
                        .and_then(|leg| leg.limit_price)
                        .map(|price| vec![price]),
                    stop_loss,
                )?;
                drain_trade_events(&broker, &format!("after SL update {}", index + 1));
                println!(
                    "SL update {} insight+RPC accepted order_id={} insight_sl={:?} leg_sl={:?}",
                    index + 1,
                    submitted_order.order_id,
                    managed_insight.stop_loss_levels(),
                    managed_insight
                        .legs
                        .stop_loss
                        .as_ref()
                        .and_then(|leg| leg.limit_price)
                );
            }

            Ok::<(), String>(())
        }
        .await;

        println!(
            "closing MT5 live test position {}",
            submitted_order.order_id
        );
        let cleanup_qty = if submitted_order.filled_qty > 0.0 {
            submitted_order.filled_qty
        } else {
            market.qty
        };
        let close_result = broker
            .close_position(&submitted_order.order_id, cleanup_qty, None)
            .await;
        match close_result {
            Ok(_) => {
                let closed_order = wait_for_trade_event(
                    &broker,
                    &submitted_order.order_id,
                    TradeUpdateEvent::Closed,
                    Duration::from_secs(20),
                )
                .await
                .expect("MT5 close event should be received");
                println!(
                    "closed test position order_id={} realized_pnl={:?} commission={:?}",
                    closed_order.order_id, closed_order.realized_pnl, closed_order.commission
                );
                assert!(
                    closed_order.commission.is_some(),
                    "MT5 closed trade event should include broker commission, even when the value is 0.0"
                );
            }
            Err(error) => {
                if let Err(test_error) = &test_result {
                    panic!(
                        "MT5 live test failed before cleanup close: {}; cleanup close failed: {:?}",
                        test_error, error
                    );
                }
                panic!("MT5 live test position should close: {:?}", error);
            }
        }

        broker
            .disconnect()
            .await
            .expect("MT5 bridge should disconnect after live test");

        test_result.expect("MT5 live TP/SL update flow should complete")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "read-only live MT5 ticker info inspection; requires the EA polling the configured bridge"]
    async fn live_mt5_print_ticker_info_fee_breakdowns() {
        let bind_addr =
            std::env::var("AQE_MT5_TEST_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:18070".to_string());
        let token = std::env::var("AQE_MT5_TEST_TOKEN").unwrap_or_else(|_| "test".to_string());
        let symbols = std::env::var("AQE_MT5_TEST_SYMBOLS")
            .ok()
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .filter(|symbols| !symbols.is_empty())
            .unwrap_or_else(|| {
                ["AAPL.NAS", "GBPUSD", "BTCUSD", "WTI_Q6"]
                    .into_iter()
                    .map(str::to_string)
                    .collect()
            });

        let bridge = Arc::new(Mt5Bridge::new(Mt5BridgeConfig {
            bind_addr: bind_addr.parse().unwrap(),
            token,
            request_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(60),
            poll_interval: Duration::from_millis(100),
            symbol_map: HashMap::new(),
        }));
        bridge.set_session_id(format!("aqe-mt5-ticker-info-test-{}", Uuid::new_v4()));
        let broker = Mt5Broker::new(bridge.clone());

        broker
            .connect()
            .await
            .expect("MT5 EA should poll the AQE ticker info test bridge with the configured token");

        for symbol in symbols {
            match bridge.request_asset(&symbol).await {
                Ok(asset) => {
                    let sample_price = 1.0;
                    let sample_qty = 1.0;
                    println!("ticker_info symbol={symbol}");
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&asset)
                            .expect("asset should serialize for diagnostics")
                    );
                    println!(
                        "fee_calculation symbol={} price={} qty={} entry_long={} entry_short={} exit_long={} exit_short={} swap_long_1d={} swap_short_1d={}",
                        asset.symbol,
                        sample_price,
                        sample_qty,
                        asset.fees.entry_commission_for_side(
                            &OrderSide::Buy,
                            sample_price,
                            sample_qty,
                            asset.contract_size,
                        ),
                        asset.fees.entry_commission_for_side(
                            &OrderSide::Sell,
                            sample_price,
                            sample_qty,
                            asset.contract_size,
                        ),
                        asset.fees.exit_commission_for_side(
                            &OrderSide::Buy,
                            sample_price,
                            sample_qty,
                            asset.contract_size,
                        ),
                        asset.fees.exit_commission_for_side(
                            &OrderSide::Sell,
                            sample_price,
                            sample_qty,
                            asset.contract_size,
                        ),
                        asset.fees.swap_for_side(
                            &OrderSide::Buy,
                            sample_price,
                            sample_qty,
                            asset.contract_size,
                            asset.min_price_increment,
                            1.0
                        ),
                        asset.fees.swap_for_side(
                            &OrderSide::Sell,
                            sample_price,
                            sample_qty,
                            asset.contract_size,
                            asset.min_price_increment,
                            1.0
                        )
                    );
                }
                Err(error) => {
                    println!("ticker_info symbol={symbol} error={error:?}");
                }
            }
        }

        broker
            .disconnect()
            .await
            .expect("MT5 bridge should disconnect after ticker info test");
    }

    #[tokio::test]
    async fn live_protection_harness_persists_admission_and_reduction() {
        use crate::core::broker::{
            commands::BrokerCommandAction, paper_broker::PaperBroker, types::AccountType,
        };
        let directory =
            std::env::temp_dir().join(format!("aqe-protection-harness-{}", Uuid::new_v4()));
        let paper = Arc::new(PaperBroker::new(AccountType::Paper, 100_000.0, 1));
        let mut insight = Insight::default();
        insight.symbol = "ETHUSD".into();
        insight.set_quantity(Some(1.0)).set_limit_price(Some(100.0));
        let order = paper.submit_order((&insight).into()).await.unwrap();
        let driver = crate::core::strategy::command_driver::StrategyCommandDriver::start(
            paper,
            directory.join("commands.db"),
            "test",
            "account",
            Default::default(),
        )
        .await
        .unwrap();
        // No connection is opened. Paper execution exercises the same durable
        // harness against a simulated order, without opening an MT5 connection.
        let broker = Mt5Broker::new(Arc::new(Mt5Bridge::new(Mt5BridgeConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            token: "test".into(),
            request_timeout: Duration::from_secs(1),
            connect_timeout: Duration::from_secs(1),
            poll_interval: Duration::from_millis(100),
            symbol_map: HashMap::new(),
        })));
        let mut ctx = LiveMt5InsightContext {
            broker: &broker,
            driver,
            universe: HashMap::new(),
            history: HashMap::new(),
            insights: InsightCollection::new(),
            variables: DashMap::new(),
            reduced: Vec::new(),
        };
        let receipt = ctx
            .driver
            .enqueue(
                &insight.insight_id().to_string(),
                BrokerCommandAction::Cancel {
                    order_id: order.order_id,
                },
                None,
            )
            .unwrap();
        ctx.wait_for_command(&receipt, &insight).await.unwrap();
        assert!(ctx.driver.checkpoint_revision(insight.insight_id()) > 0);
        ctx.broker_command_reduced(&receipt);
        ctx.persist_insight(&insight).await.unwrap();
        assert!(ctx.driver.progress(&receipt).is_none());
        drop(ctx.driver.stop().await.unwrap());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "places demo MT5 pending entries; requires updated EA on port 18071"]
    async fn live_mt5_pending_entry_limit_and_stop_amendments() {
        let bridge = Arc::new(Mt5Bridge::new(Mt5BridgeConfig {
            bind_addr: std::env::var("AQE_MT5_TEST_BIND_ADDR")
                .unwrap_or_else(|_| "0.0.0.0:18071".into())
                .parse()
                .unwrap(),
            token: std::env::var("AQE_MT5_TEST_TOKEN").unwrap_or_else(|_| "test".into()),
            request_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(60),
            poll_interval: Duration::from_millis(100),
            symbol_map: HashMap::new(),
        }));
        bridge.set_session_id(format!("aqe-entry-amend-test-{}", Uuid::new_v4()));
        let broker = Mt5Broker::new(bridge.clone());
        broker.connect().await.unwrap();
        let support = bridge
            .request_rpc(
                Mt5RpcAction::AmendEntry,
                serde_json::json!({"orderId":"0", "limitPrice":1.0}),
            )
            .await
            .expect("Install the updated EA before running entry-amendment tests");
        assert!(
            support.get("amended").is_some(),
            "EA lacks AMEND_ENTRY; no test orders were submitted"
        );
        let positions = broker.get_positions().await.unwrap();
        let orders = broker.get_orders().await.unwrap();
        let market = select_live_test_market(&bridge, &positions, &orders).await;
        for stop in [false, true] {
            let initial = round_to_step(
                market.mid_price * if stop { 1.10 } else { 0.90 },
                market.price_step,
            );
            let changed = round_to_step(
                market.mid_price * if stop { 1.11 } else { 0.89 },
                market.price_step,
            );
            let mut insight = Insight::new(
                OrderSide::Buy,
                market.symbol.clone(),
                StrategyType::Testing,
                TimeFrame::new(1, TimeFrameUnit::Minute),
                100,
                None,
            );
            insight.set_quantity(Some(market.qty));
            if stop {
                insight.set_stop_price(Some(initial));
            } else {
                insight.set_limit_price(Some(initial));
            }
            let submitted = broker.submit_order((&insight).into()).await.unwrap();
            println!("test entry ticket={} symbol={} stop={} initial={} target={}", submitted.order_id, market.symbol, stop, initial, changed);
            // Retain any assertion failure until this exact test ticket is cleaned up.
            let result = async {
                insight.order_accepted(&submitted.order_id);
                let mut ctx = LiveMt5InsightContext::new(&broker).await?;
                ctx.bind_insight_context(&mut insight);
                let mutation = if stop {
                    insight.update_stop_limit(&mut ctx, changed)
                } else {
                    insight.update_limit(&mut ctx, changed)
                }
                .map_err(|e| e.to_string())?;
                if let crate::core::insight::InsightMutationState::Pending(receipt) = mutation {
                    ctx.wait_for_command(&receipt, &insight).await?;
                }
                let applied = if stop {
                    insight.update_stop_limit(&mut ctx, changed)
                } else {
                    insight.update_limit(&mut ctx, changed)
                }
                .map_err(|e| e.to_string())?;
                if applied != crate::core::insight::InsightMutationState::Applied {
                    return Err("Amendment still pending".into());
                }
                ctx.persist_insight(&insight).await?;
                let rows = broker.get_orders().await.map_err(|e| e.to_string())?;
                let row = rows
                    .iter()
                    .find(|o| o.order_id == submitted.order_id)
                    .ok_or("Test entry unexpectedly filled or disappeared")?;
                let price = if stop {
                    row.stop_price
                } else {
                    row.limit_price
                };
                if price.is_none_or(|p| (p - changed).abs() > market.price_step) {
                    return Err(format!("Broker entry price mismatch: {price:?}"));
                }
                println!("broker verified amendment ticket={} price={price:?}", submitted.order_id);
                drop(ctx.driver.stop().await.map_err(|e| e.to_string())?);
                Ok::<(), String>(())
            }
            .await;
            let cleanup = broker.cancel_order(&submitted.order_id).await;
            if cleanup.is_err() {
                broker
                    .close_position(&submitted.order_id, market.qty, None)
                    .await
                    .expect("Test ticket cleanup failed; inspect MT5");
            }
            let remaining = broker.get_orders().await.expect("Verify test cleanup with MT5");
            assert!(!remaining.iter().any(|row| row.order_id == submitted.order_id), "Test ticket remains pending after cleanup");
            println!("test entry cleaned up ticket={}", submitted.order_id);
            assert!(
                result.is_ok(),
                "entry amendment failed after cleanup: {result:?}"
            );
        }
    }

    struct LiveTestMarket {
        symbol: String,
        qty: f64,
        mid_price: f64,
        price_step: f64,
    }

    struct LiveMt5TestTools;

    impl TradingTools for LiveMt5TestTools {
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

    struct LiveMt5InsightContext<'a> {
        broker: &'a Mt5Broker,
        driver: crate::core::strategy::command_driver::StrategyCommandDriver,
        universe: HashMap<String, Asset>,
        history: HashMap<String, DataFrame>,
        insights: InsightCollection,
        variables: DashMap<String, Value>,
        reduced: Vec<crate::core::broker::commands::BrokerCommandReceipt>,
    }

    impl<'a> LiveMt5InsightContext<'a> {
        async fn new(broker: &'a Mt5Broker) -> Result<Self, String> {
            let account = broker
                .get_account()
                .await
                .map_err(|error| error.to_string())?;
            let path = std::env::temp_dir()
                .join(format!("aqe-live-protection-{}", uuid::Uuid::new_v4()))
                .join("commands.db");
            let driver = crate::core::strategy::command_driver::StrategyCommandDriver::start(
                Arc::new(broker.clone()),
                path,
                "live-protection-test",
                account.account_id.trim(),
                Default::default(),
            )
            .await
            .map_err(|error| error.to_string())?;
            Ok(Self {
                broker,
                driver,
                universe: HashMap::new(),
                history: HashMap::new(),
                insights: InsightCollection::new(),
                variables: DashMap::new(),
                reduced: Vec::new(),
            })
        }

        async fn persist_insight(&mut self, insight: &Insight) -> Result<(), String> {
            let row =
                crate::core::insight::execution_checkpoint::InsightExecutionCheckpoint::capture(
                    insight,
                    self.driver.checkpoint_revision(insight.insight_id()) + 1,
                    true,
                    None,
                );
            // Admission alone cannot dispatch: commit the pending mutation and
            // intent together, then durably acknowledge reductions after apply.
            self.driver
                .persist_execution_checkpoints(vec![row], self.reduced.clone())
                .map_err(|error| error.to_string())?;
            self.reduced.clear();
            tokio::time::timeout(Duration::from_secs(10), async {
                while self.driver.checkpoint_in_flight() {
                    self.driver
                        .drain_ready(256, Duration::from_millis(5))
                        .map_err(|error| error.to_string())?;
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                Ok::<(), String>(())
            })
            .await
            .map_err(|_| "live protection checkpoint timed out".to_string())?
        }

        async fn wait_for_command(
            &mut self,
            receipt: &crate::core::broker::commands::BrokerCommandReceipt,
            insight: &Insight,
        ) -> Result<(), String> {
            use crate::core::strategy::command_driver::BrokerCommandProgress;
            self.persist_insight(insight).await?;
            tokio::time::timeout(Duration::from_secs(40), async {
                loop {
                    self.driver.drain_ready(256, Duration::from_millis(5)).map_err(|error| error.to_string())?;
                    match self.driver.progress(receipt) {
                        Some(BrokerCommandProgress::Completed(completion)) => {
                            if matches!(completion.outcome, crate::core::broker::commands::BrokerCommandOutcome::Ambiguous { .. }) {
                                return Err("live protection command requires reconciliation".into());
                            }
                            return Ok(());
                        }
                        Some(BrokerCommandProgress::Failed(error)) => return Err(error.to_string()),
                        _ => tokio::time::sleep(Duration::from_millis(1)).await,
                    }
                }
            }).await.map_err(|_| "live protection command timed out".to_string())?
        }

        fn block_on_broker<F, T>(&self, future: F) -> T
        where
            F: std::future::Future<Output = T>,
        {
            tokio::task::block_in_place(|| futures::executor::block_on(future))
        }
    }

    impl StrategyContext for LiveMt5InsightContext<'_> {
        fn broker_features(&self) -> &'static [BrokerFeature] {
            self.broker.get_features()
        }
        fn enqueue_broker_command(
            &mut self,
            insight_id: uuid::Uuid,
            action: crate::core::broker::commands::BrokerCommandAction,
            group: Option<crate::core::broker::commands::BrokerOperationGroup>,
        ) -> Result<
            crate::core::broker::commands::BrokerCommandReceipt,
            crate::core::broker::commands::BrokerCommandError,
        > {
            self.driver.enqueue(&insight_id.to_string(), action, group)
        }

        fn broker_command_progress(
            &self,
            receipt: &crate::core::broker::commands::BrokerCommandReceipt,
        ) -> Option<crate::core::strategy::command_driver::BrokerCommandProgress> {
            self.driver.progress(receipt)
        }

        fn broker_command_reduced(
            &mut self,
            receipt: &crate::core::broker::commands::BrokerCommandReceipt,
        ) {
            self.driver
                .mark_reduced(receipt)
                .expect("receipt belongs to this test context");
            if !self.reduced.contains(receipt) {
                self.reduced.push(receipt.clone());
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
            StrategyMode::Live
        }

        fn add_insight(&mut self, insight: Insight) {
            self.insights.add_insight(insight);
        }

        fn register_indicator(&mut self, _indicator: Box<dyn Indicator>) {}

        fn add_alpha(&mut self, _alpha: WrappedAlphaModel) {}

        fn add_pipe(&mut self, _pipe: WrappedInsightPipe) {}

        fn add_universe_model(&mut self, _model: WrappedUniverseModel) {}

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
            Box::new(LiveMt5TestTools)
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
            self.block_on_broker(self.broker.get_account())
        }

        fn current_time(&self) -> DateTime<Utc> {
            Utc::now()
        }

        fn bind_insight_context(&self, insight: &mut Insight) {
            insight.bind_context(InsightStrategyContext::new(Utc::now));
        }

        fn latest_quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
            Err(BrokerError::DataFeedError(format!(
                "latest_quote is not needed by the MT5 insight update test for {symbol}"
            )))
        }

        fn cleanup_active_insights_for_teardown(&mut self) -> TeardownCleanupReport {
            TeardownCleanupReport::default()
        }

        fn shutdown(&mut self) {}
    }

    async fn select_live_test_market(
        bridge: &Mt5Bridge,
        existing_positions: &[Position],
        existing_orders: &[Order],
    ) -> LiveTestMarket {
        let symbol = std::env::var("AQE_MT5_TEST_SYMBOL")
            .ok()
            .map(|symbol| symbol.trim().to_string())
            .filter(|symbol| !symbol.is_empty())
            .unwrap_or_else(|| "BNBUSD".to_string());
        let allow_existing_position = std::env::var("AQE_MT5_TEST_ALLOW_EXISTING_SYMBOL_EXPOSURE")
            .ok()
            .map(|value| {
                let value = value.trim();
                value == "1"
                    || value.eq_ignore_ascii_case("true")
                    || value.eq_ignore_ascii_case("yes")
            })
            .unwrap_or(false);

        let mut errors = Vec::new();
        for symbol in [symbol] {
            if !allow_existing_position
                && existing_positions
                    .iter()
                    .any(|position| position.asset.symbol == symbol)
            {
                errors.push(format!(
                    "{symbol}: skipped because an existing position is already open"
                ));
                continue;
            }
            if existing_orders
                .iter()
                .any(|order| order.asset.symbol == symbol)
            {
                errors.push(format!(
                    "{symbol}: skipped because an existing pending order is already open"
                ));
                continue;
            }

            let asset = match bridge.request_asset(&symbol).await {
                Ok(asset) => asset,
                Err(error) => {
                    errors.push(format!("{symbol} asset: {error:?}"));
                    continue;
                }
            };
            if !asset.tradable {
                errors.push(format!("{symbol}: asset is not tradable"));
                continue;
            }

            let quote = match bridge.request_latest_quote(&symbol).await {
                Ok(quote) => quote,
                Err(error) => {
                    errors.push(format!("{symbol} quote: {error:?}"));
                    continue;
                }
            };
            let mid_price = if quote.bid > 0.0 && quote.ask > 0.0 {
                (quote.bid + quote.ask) / 2.0
            } else {
                quote.ask.max(quote.bid)
            };
            if !mid_price.is_finite() || mid_price <= 0.0 {
                errors.push(format!(
                    "{symbol}: invalid quote bid={} ask={}",
                    quote.bid, quote.ask
                ));
                continue;
            }

            let price_step = asset
                .min_price_increment
                .or_else(|| asset.price_base.map(|base| 1.0 / base.max(1) as f64))
                .unwrap_or(0.01)
                .max(0.00001);
            let qty = std::env::var("AQE_MT5_TEST_QTY")
                .ok()
                .and_then(|value| value.trim().parse::<f64>().ok())
                .filter(|value| value.is_finite() && *value > 0.0)
                .unwrap_or_else(|| asset.min_order_size.unwrap_or(0.01).max(0.01));
            return LiveTestMarket {
                symbol,
                qty,
                mid_price,
                price_step,
            };
        }

        panic!(
            "No tradable MT5 live test symbol found. Tried: {}",
            errors.join(" | ")
        );
    }

    async fn refresh_order(
        broker: &Mt5Broker,
        bridge: &Mt5Bridge,
        order_id: &str,
    ) -> Result<Order, String> {
        if let Ok(order) = bridge.order(order_id) {
            return Ok(order);
        }

        let orders = broker
            .get_orders()
            .await
            .map_err(|error| format!("MT5 orders should refresh: {error:?}"))?;

        orders
            .into_iter()
            .find(|order| order.order_id == order_id)
            .or_else(|| bridge.order(order_id).ok())
            .ok_or_else(|| format!("MT5 order {order_id} should exist after refresh"))
    }

    async fn wait_for_trade_event(
        broker: &Mt5Broker,
        order_id: &str,
        expected: TradeUpdateEvent,
        timeout: Duration,
    ) -> Result<Order, String> {
        let deadline = Instant::now() + timeout;
        let mut seen = Vec::new();
        loop {
            for (order, event) in broker.drain_trade_events() {
                println!(
                    "trade event event={:?} order_id={} status={:?} filled_price={:?}",
                    event, order.order_id, order.status, order.filled_price
                );
                seen.push(format!("{event:?}:{}", order.order_id));
                if event == expected && order.order_id == order_id {
                    return Ok(order);
                }
            }

            if Instant::now() >= deadline {
                return Err(format!(
                    "Timed out waiting for {:?} event on order {}. Seen events: {}",
                    expected,
                    order_id,
                    seen.join(", ")
                ));
            }

            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    fn drain_trade_events(broker: &Mt5Broker, label: &str) {
        let events = broker.drain_trade_events();
        if events.is_empty() {
            println!("{label}: no queued trade events");
            return;
        }

        for (order, event) in events {
            println!(
                "{label}: drained event={:?} order_id={} status={:?}",
                event, order.order_id, order.status
            );
        }
    }

    fn expect_level(name: &str, actual: Option<Vec<f64>>, expected: f64) -> Result<(), String> {
        let actual = actual.ok_or_else(|| format!("{name} was None, expected {expected}"))?;
        if actual.len() != 1 || (actual[0] - expected).abs() > f64::EPSILON {
            return Err(format!(
                "{name} mismatch: expected [{expected}], got {:?}",
                actual
            ));
        }
        Ok(())
    }

    fn live_test_bracket_distance(price: f64, price_step: f64) -> f64 {
        (price.abs() * 0.10).max(price_step * 5000.0)
    }

    fn round_to_step(price: f64, price_step: f64) -> f64 {
        if !price.is_finite() || !price_step.is_finite() || price_step <= 0.0 {
            return price;
        }
        (price / price_step).round() * price_step
    }
}
