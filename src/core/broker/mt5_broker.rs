use super::mt5_bridge::{Mt5Bridge, Mt5RpcAction};
use super::traits::{Broker, OrderManagementProvider};
use super::types::{
    Account, AccountType, BrokerError, Order, OrderClass, OrderLeg, OrderLegs, OrderSide,
    OrderType, Position, TimeInForce, TradeUpdateEvent,
};
use crate::core::insight::Insight;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use uuid::Uuid;

pub struct Mt5Broker {
    name: String,
    bridge: Arc<Mt5Bridge>,
}

impl Mt5Broker {
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

    fn order_from_insight(&self, insight: Insight) -> Result<Order, BrokerError> {
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
                trailing_stop: None,
            })
        } else {
            None
        };

        Ok(Order {
            order_id: Uuid::new_v4().to_string(),
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
            rejection_reason: None,
            legs,
        })
    }
}

impl Broker for Mt5Broker {
    async fn connect(&self) -> Result<bool, BrokerError> {
        self.bridge.start().await
    }

    async fn disconnect(&self) -> Result<bool, BrokerError> {
        self.bridge.stop();
        Ok(true)
    }

    fn is_connected(&self) -> bool {
        self.bridge.is_connected()
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

    fn configure_live_session(&self, session_id: &str) -> Result<(), BrokerError> {
        self.bridge.set_session_id(session_id);
        Ok(())
    }
}

impl OrderManagementProvider for Mt5Broker {
    async fn submit_order(&self, insight: Insight) -> Result<Order, BrokerError> {
        let order = self.order_from_insight(insight)?;
        let take_profit = order
            .legs
            .as_ref()
            .and_then(|legs| legs.take_profit.as_ref())
            .and_then(|leg| leg.limit_price);
        let stop_loss = order
            .legs
            .as_ref()
            .and_then(|legs| legs.stop_loss.as_ref())
            .and_then(|leg| leg.limit_price);
        let payload = serde_json::json!({
            "clientOrderId": order.order_id.clone(),
            "symbol": order.asset.symbol.clone(),
            "qty": order.qty,
            "price": order.limit_price.or(order.stop_price),
            "side": order.side.clone(),
            "orderType": order.order_type.clone(),
            "takeProfit": take_profit,
            "stopLoss": stop_loss,
            "order": order.clone(),
        });
        self.bridge
            .request_order_action(Mt5RpcAction::SubmitOrder, Some(order), payload)
            .await
    }

    async fn cancel_order(&self, order_id: &str) -> Result<bool, BrokerError> {
        self.bridge
            .request_rpc(
                Mt5RpcAction::CancelOrder,
                serde_json::json!({ "orderId": order_id }),
            )
            .await?;
        Ok(true)
    }

    async fn update_order(
        &self,
        _order_id: &str,
        _price: f64,
        _qty: f64,
    ) -> Result<bool, BrokerError> {
        Err(BrokerError::OrderError(
            "MT5 order replacement is not supported in v1; cancel and resubmit".to_string(),
        ))
    }

    async fn close_position(
        &self,
        order_id: &str,
        qty: f64,
        price: Option<f64>,
    ) -> Result<bool, BrokerError> {
        self.bridge
            .request_rpc(
                Mt5RpcAction::ClosePosition,
                serde_json::json!({ "orderId": order_id, "qty": qty, "price": price }),
            )
            .await?;
        if let Ok(mut order) = self.bridge.order(order_id) {
            order.status = TradeUpdateEvent::Closed;
            order.filled_qty = qty;
            order.filled_price = price.or(order.filled_price);
            order.filled_at = Some(Self::now_ts());
            self.bridge
                .emit_order_event(order, TradeUpdateEvent::Closed);
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
        self.bridge
            .request_positions()
            .await?
            .into_iter()
            .find(|position| position.asset.symbol == symbol)
            .ok_or_else(|| BrokerError::PositionError(format!("MT5 position {} not found", symbol)))
    }

    async fn get_account(&self) -> Result<Account, BrokerError> {
        self.bridge.request_account().await
    }

    fn drain_trade_events(&self) -> Vec<(Order, TradeUpdateEvent)> {
        self.bridge.drain_trade_events()
    }

    async fn subscribe_to_trade_stream(
        &self,
        on_trade: Arc<dyn Fn((Order, TradeUpdateEvent)) + Send + Sync>,
    ) -> Result<(), BrokerError> {
        self.bridge.subscribe_trade_stream(on_trade);
        Ok(())
    }

    async fn unsubscribe_from_trade_stream(&self) -> Result<(), BrokerError> {
        self.bridge.clear_trade_subscribers();
        Ok(())
    }
}
