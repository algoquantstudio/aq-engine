use super::mt5_bridge::{Mt5Bridge, Mt5RpcAction};
use super::traits::{Broker, OrderManagementProvider};
use super::types::{
    Account, AccountType, BrokerError, Order, OrderClass, OrderLeg, OrderLegs, OrderSide,
    OrderType, Position, TimeInForce, TradeUpdateEvent,
};
use crate::core::insight::Insight;
use crate::core::utils::tools::dynamic_round_for_asset;
use chrono::{DateTime, Utc};
use log::debug;
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
            realized_pnl: None,
            commission: None,
            swap: None,
            rejection_reason: None,
            legs,
        })
    }
}

impl Broker for Mt5Broker {
    async fn connect(&self) -> Result<bool, BrokerError> {
        self.bridge.start().await?;
        self.bridge.wait_for_rpc_poll().await
    }

    async fn disconnect(&self) -> Result<bool, BrokerError> {
        self.bridge.shutdown().await;
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
            "clientOrderId": order.order_id.clone(),
            "insightId": order.insight_id.clone(),
            "strategyType": order.strategy_type.clone(),
            "symbol": order.asset.symbol.clone(),
            "qty": order.qty,
            "price": order.limit_price.or(order.stop_price),
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

        let cancelled = !self
            .bridge
            .request_orders()
            .await?
            .into_iter()
            .any(|order| order.order_id == order_id);

        if !cancelled {
            return Err(BrokerError::OrderCancellationError(format!(
                "MT5 did not confirm cancellation for order {}",
                order_id
            )));
        }

        existing_order.status = TradeUpdateEvent::Cancelled;
        existing_order.updated_at = Self::now_ts();
        self.bridge
            .emit_order_event(existing_order, TradeUpdateEvent::Cancelled);

        Ok(true)
    }

    async fn update_order(
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
            .await?;
        if let Ok(mut close_order) = serde_json::from_value::<Order>(response) {
            close_order.asset.symbol = self.bridge.config().aqe_symbol(&close_order.asset.symbol);
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
            min_price_increment: Some(0.0001),
            price_base: Some(5),
            contract_size: None,
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
    fn update_order_payload_sets_take_profit_and_preserves_stop_loss() {
        let order = sample_order();
        let payload = Mt5Broker::update_order_payload("order-1", 121.5, 1.0, Some(&order), false);

        assert_eq!(payload["orderId"], serde_json::json!("order-1"));
        assert_eq!(payload["symbol"], serde_json::json!("EURUSD"));
        assert_eq!(payload["takeProfit"], serde_json::json!(121.5));
        assert_eq!(payload["stopLoss"], serde_json::json!(95.0));
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
            .submit_order(insight)
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
            let mut ctx = LiveMt5InsightContext::new(&broker);
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
                if !managed_insight.update_take_profit(&mut ctx, Some(vec![take_profit])) {
                    return Err(format!(
                        "insight TP update {} returned false for order {}",
                        index + 1,
                        submitted_order.order_id
                    ));
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
                if !managed_insight.update_stop_loss(&mut ctx, Some(vec![stop_loss])) {
                    return Err(format!(
                        "insight SL update {} returned false for order {}",
                        index + 1,
                        submitted_order.order_id
                    ));
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
        universe: HashMap<String, Asset>,
        history: HashMap<String, DataFrame>,
        insights: InsightCollection,
        variables: DashMap<String, Value>,
    }

    impl<'a> LiveMt5InsightContext<'a> {
        fn new(broker: &'a Mt5Broker) -> Self {
            Self {
                broker,
                universe: HashMap::new(),
                history: HashMap::new(),
                insights: InsightCollection::new(),
                variables: DashMap::new(),
            }
        }

        fn block_on_broker<F, T>(&self, future: F) -> T
        where
            F: std::future::Future<Output = T>,
        {
            tokio::task::block_in_place(|| futures::executor::block_on(future))
        }
    }

    impl StrategyContext for LiveMt5InsightContext<'_> {
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

        fn submit_insight(&mut self, _insight: &mut Insight) {}

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

        fn cancel_order(&self, order_id: &str) -> Result<bool, BrokerError> {
            self.block_on_broker(self.broker.cancel_order(order_id))
        }

        fn update_order(&self, order_id: &str, price: f64, qty: f64) -> Result<bool, BrokerError> {
            self.block_on_broker(self.broker.update_order(order_id, price, qty))
        }

        fn update_stop_loss_order(
            &self,
            order_id: &str,
            price: f64,
            qty: f64,
        ) -> Result<bool, BrokerError> {
            self.block_on_broker(self.broker.update_stop_loss(order_id, price, qty))
        }

        fn close_position(
            &self,
            order_id: &str,
            qty: f64,
            price: Option<f64>,
        ) -> Result<bool, BrokerError> {
            self.block_on_broker(self.broker.close_position(order_id, qty, price))
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
