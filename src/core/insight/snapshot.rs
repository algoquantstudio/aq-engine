use super::Insight;
use super::types::{InsightState, StrategyDependentConfirmation, StrategyType};
use crate::core::broker::types::{OrderClass, OrderLeg, OrderSide, OrderType, TradeUpdateEvent};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
#[cfg(feature = "runtime")]
use surrealdb::types::SurrealValue;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "runtime", derive(SurrealValue))]
pub struct InsightStateHistorySnapshot {
    pub at: DateTime<Utc>,
    pub state: String,
    pub message: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "runtime", derive(SurrealValue))]
pub struct InsightPartialCloseSnapshot {
    pub order_id: String,
    pub side: String,
    pub quantity: f64,
    pub entry_price: f64,
    pub filled_price: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "runtime", derive(SurrealValue))]
pub struct InsightOrderLegSnapshot {
    pub order_id: Option<String>,
    pub limit_price: Option<f64>,
    pub trail_price: Option<f64>,
    pub side: String,
    pub filled_price: Option<f64>,
    pub order_type: String,
    pub status: String,
    pub order_class: String,
    pub created_at: u64,
    pub updated_at: u64,
    pub submitted_at: u64,
    pub filled_at: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "runtime", derive(SurrealValue))]
pub struct InsightLegsSnapshot {
    pub take_profit: Option<InsightOrderLegSnapshot>,
    pub stop_loss: Option<InsightOrderLegSnapshot>,
    pub trailing_stop: Option<InsightOrderLegSnapshot>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "runtime", derive(SurrealValue))]
pub struct InsightSnapshot {
    pub insight_id: String,
    pub parent_id: Option<String>,
    pub state: String,
    pub children: Vec<serde_json::Value>,
    pub order_id: Option<String>,
    pub side: String,
    pub symbol: String,
    pub quantity: Option<f64>,
    pub contracts: Option<f64>,
    pub order_type: String,
    pub order_class: String,
    pub limit_price: Option<f64>,
    pub stop_price: Option<f64>,
    pub take_profit_levels: Option<Vec<f64>>,
    pub stop_loss_levels: Option<Vec<f64>>,
    pub trailing_stop_price: Option<f64>,
    pub strategy_type: String,
    pub confidence: u8,
    pub timeframe: serde_json::Value,
    pub period_unfilled: Option<u32>,
    pub period_till_tp: Option<u32>,
    pub execution_depends: Vec<String>,
    pub filled_price: Option<f64>,
    pub close_order_id: Option<String>,
    pub close_price: Option<f64>,
    pub broker_realized_pnl: Option<f64>,
    pub partial_closes: Vec<InsightPartialCloseSnapshot>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub filled_at: Option<DateTime<Utc>>,
    pub closed_at: Option<DateTime<Utc>>,
    pub legs: InsightLegsSnapshot,
    pub market_changed: bool,
    pub submitted: bool,
    pub cancelling: bool,
    pub closing: bool,
    pub first_on_fill: bool,
    pub partial_filled_quantity: Option<f64>,
    pub state_history: Vec<InsightStateHistorySnapshot>,
}

impl InsightSnapshot {
    fn insight_state_label(state: &InsightState) -> &'static str {
        match state {
            InsightState::New => "New",
            InsightState::Executed => "Executed",
            InsightState::Filled => "Filled",
            InsightState::Closed => "Closed",
            InsightState::Cancelled => "Cancelled",
            InsightState::Rejected => "Rejected",
        }
    }

    fn order_side_label(side: &OrderSide) -> &'static str {
        match side {
            OrderSide::Buy => "Buy",
            OrderSide::Sell => "Sell",
        }
    }

    fn order_type_label(order_type: &OrderType) -> &'static str {
        match order_type {
            OrderType::Market => "Market",
            OrderType::Limit => "Limit",
            OrderType::Stop => "Stop",
            OrderType::StopLimit => "StopLimit",
            OrderType::TrailingStop => "TrailingStop",
        }
    }

    fn order_class_label(order_class: &OrderClass) -> &'static str {
        match order_class {
            OrderClass::Simple => "Simple",
            OrderClass::Bracket => "Bracket",
            OrderClass::OCO => "OCO",
            OrderClass::OTO => "OTO",
            OrderClass::TRO => "TRO",
        }
    }

    fn trade_update_event_label(status: &TradeUpdateEvent) -> &'static str {
        match status {
            TradeUpdateEvent::Accepted => "Accepted",
            TradeUpdateEvent::New => "New",
            TradeUpdateEvent::PendingNew => "PendingNew",
            TradeUpdateEvent::Pending => "Pending",
            TradeUpdateEvent::PartialFilled => "PartialFilled",
            TradeUpdateEvent::Filled => "Filled",
            TradeUpdateEvent::Canceled => "Canceled",
            TradeUpdateEvent::Rejected => "Rejected",
            TradeUpdateEvent::Expired => "Expired",
            TradeUpdateEvent::Closed => "Closed",
            TradeUpdateEvent::Replaced => "Replaced",
        }
    }

    fn strategy_type_label(strategy_type: &StrategyType) -> Cow<'_, str> {
        match strategy_type {
            StrategyType::Manual => Cow::Borrowed("Manual"),
            StrategyType::Testing => Cow::Borrowed("Testing"),
            StrategyType::Portfolio => Cow::Borrowed("Portfolio"),
            StrategyType::Custom(value) => Cow::Borrowed(value.as_str()),
        }
    }

    fn strategy_dependency_label(dep: &StrategyDependentConfirmation) -> Cow<'_, str> {
        match dep {
            StrategyDependentConfirmation::None => Cow::Borrowed("None"),
            StrategyDependentConfirmation::HighRelativeVolumeConfirmationModel => {
                Cow::Borrowed("HighRelativeVolumeConfirmationModel")
            }
            StrategyDependentConfirmation::LowRelativeVolumeConfirmationModel => {
                Cow::Borrowed("LowRelativeVolumeConfirmationModel")
            }
            StrategyDependentConfirmation::HighTimeFrameConfirmationModel => {
                Cow::Borrowed("HighTimeFrameConfirmationModel")
            }
            StrategyDependentConfirmation::LowTimeFrameConfirmationModel => {
                Cow::Borrowed("LowTimeFrameConfirmationModel")
            }
            StrategyDependentConfirmation::HighConfidenceConfirmationModel => {
                Cow::Borrowed("HighConfidenceConfirmationModel")
            }
            StrategyDependentConfirmation::LowConfidenceConfirmationModel => {
                Cow::Borrowed("LowConfidenceConfirmationModel")
            }
            StrategyDependentConfirmation::UpStateConfirmationModel => {
                Cow::Borrowed("UpStateConfirmationModel")
            }
            StrategyDependentConfirmation::DownStateConfirmationModel => {
                Cow::Borrowed("DownStateConfirmationModel")
            }
            StrategyDependentConfirmation::FlatStateConfirmationModel => {
                Cow::Borrowed("FlatStateConfirmationModel")
            }
            StrategyDependentConfirmation::Custom(value) => Cow::Borrowed(value.as_str()),
        }
    }

    fn order_leg_snapshot(leg: &OrderLeg) -> InsightOrderLegSnapshot {
        InsightOrderLegSnapshot {
            order_id: leg.order_id.clone(),
            limit_price: leg.limit_price,
            trail_price: leg.trail_price,
            side: Self::order_side_label(&leg.side).to_string(),
            filled_price: leg.filled_price,
            order_type: Self::order_type_label(&leg.order_type).to_string(),
            status: Self::trade_update_event_label(&leg.status).to_string(),
            order_class: Self::order_class_label(&leg.order_class).to_string(),
            created_at: leg.created_at,
            updated_at: leg.updated_at,
            submitted_at: leg.submitted_at,
            filled_at: leg.filled_at,
        }
    }

    pub fn from_insight(insight: &Insight, _strategy_id: &str) -> Self {
        Self {
            insight_id: insight.insight_id().to_string(),
            parent_id: insight.parent_id().map(|id| id.to_string()),
            state: Self::insight_state_label(insight.state()).to_string(),
            children: Vec::new(),
            order_id: insight.order_id.clone(),
            side: Self::order_side_label(insight.side()).to_string(),
            symbol: insight.symbol().to_string(),
            quantity: insight.quantity(),
            contracts: insight.contracts,
            order_type: Self::order_type_label(insight.order_type()).to_string(),
            order_class: Self::order_class_label(insight.order_class()).to_string(),
            limit_price: insight.limit_price(),
            stop_price: insight.stop_price(),
            take_profit_levels: insight.take_profit_levels(),
            stop_loss_levels: insight.stop_loss_levels(),
            trailing_stop_price: insight.trailing_stop_price(),
            strategy_type: Self::strategy_type_label(insight.strategy_type()).into_owned(),
            confidence: insight.confidence(),
            timeframe: serde_json::json!({
                "amount": insight.timeframe().get_amount(),
                "unit": insight.timeframe().get_unit().to_string(),
            }),
            period_unfilled: insight.period_unfilled(),
            period_till_tp: insight.period_till_tp(),
            execution_depends: insight
                .execution_depends()
                .iter()
                .map(|dep| Self::strategy_dependency_label(dep).into_owned())
                .collect(),
            filled_price: insight.filled_price,
            close_order_id: insight.close_order_id.clone(),
            close_price: insight.close_price,
            broker_realized_pnl: insight.broker_realized_pnl,
            partial_closes: insight
                .partial_closes
                .iter()
                .map(|partial| InsightPartialCloseSnapshot {
                    order_id: partial.order_id.clone(),
                    side: Self::order_side_label(&partial.side).to_string(),
                    quantity: partial.quantity,
                    entry_price: partial.entry_price,
                    filled_price: partial.filled_price,
                })
                .collect(),
            created_at: insight.created_at,
            updated_at: insight.updated_at,
            filled_at: insight.filled_at,
            closed_at: insight.closed_at,
            legs: InsightLegsSnapshot {
                take_profit: insight
                    .legs
                    .take_profit
                    .as_ref()
                    .map(Self::order_leg_snapshot),
                stop_loss: insight
                    .legs
                    .stop_loss
                    .as_ref()
                    .map(Self::order_leg_snapshot),
                trailing_stop: insight
                    .legs
                    .trailing_stop
                    .as_ref()
                    .map(Self::order_leg_snapshot),
            },
            market_changed: insight.market_changed,
            submitted: insight.submitted,
            cancelling: insight.cancelling,
            closing: insight.closing,
            first_on_fill: insight.first_on_fill,
            partial_filled_quantity: insight.partial_filled_quantity,
            state_history: insight
                .state_history
                .iter()
                .map(|(at, state, message)| InsightStateHistorySnapshot {
                    at: *at,
                    state: Self::insight_state_label(state).to_string(),
                    message: message.clone(),
                })
                .collect(),
        }
    }
}
