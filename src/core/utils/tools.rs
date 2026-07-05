use crate::core::broker::UnifiedBroker;
use crate::core::broker::traits::{Broker, DataFeed, DataProvider, OrderManagementProvider};
use crate::core::broker::types::{Asset, BrokerError, OrderSide, Position};
use crate::core::insight::types::InsightState;
use crate::core::insight::{Insight, InsightCollection};
use std::collections::HashMap;

pub trait TradingTools {
    fn dynamic_round(&self, value: f64, symbol: &str) -> f64;
    fn quantity_round(&self, value: f64, symbol: &str) -> f64;
    fn calculate_time_to_live(&self, price: f64, entry: f64, atr: f64, additional: i32) -> i32;
    fn get_unrealized_pnl(&self, symbol: &str) -> Result<f64, BrokerError>;
    fn get_all_unrealized_pnl(&self) -> Result<f64, BrokerError>;
    fn get_filled_insights(&self) -> Vec<Insight>;
}

pub struct StrategyTools<'a, E, D>
where
    E: Broker + OrderManagementProvider,
    D: DataFeed + DataProvider,
{
    universe: &'a HashMap<String, Asset>,
    insights: &'a InsightCollection,
    broker: &'a UnifiedBroker<E, D>,
}

impl<'a, E, D> StrategyTools<'a, E, D>
where
    E: Broker + OrderManagementProvider,
    D: DataFeed + DataProvider,
{
    pub fn new(
        universe: &'a HashMap<String, Asset>,
        insights: &'a InsightCollection,
        broker: &'a UnifiedBroker<E, D>,
    ) -> Self {
        Self {
            universe,
            insights,
            broker,
        }
    }
}

pub fn dynamic_round_for_asset(value: f64, asset: &Asset) -> f64 {
    if let Some(base) = asset.price_base {
        return round_to_digits(value, base);
    }

    let increment = asset.min_price_increment.unwrap_or(0.01);
    round_to_increment(value, increment)
}

pub fn quantity_round_for_asset(value: f64, asset: &Asset) -> f64 {
    if let Some(base) = asset.quantity_base {
        return round_to_digits(value, base);
    }

    let increment = asset.min_order_size.unwrap_or(1.0);
    round_to_increment(value, increment)
}

pub fn calculate_time_to_live(price: f64, entry: f64, atr: f64, additional: i32) -> i32 {
    if atr <= 0.0 {
        return additional.max(0);
    }
    (((price - entry).abs() / atr).ceil() as i32) + additional
}

pub fn unrealized_pnl_for_position(position: &Position) -> f64 {
    match position.side {
        OrderSide::Buy => (position.current_price - position.avg_entry_price) * position.qty,
        OrderSide::Sell => (position.avg_entry_price - position.current_price) * position.qty,
    }
}

pub fn aggregate_unrealized_pnl(positions: &[Position]) -> f64 {
    positions.iter().map(unrealized_pnl_for_position).sum()
}

pub fn filled_insights_from_collection(insights: &InsightCollection) -> Vec<Insight> {
    insights
        .get_active_insights()
        .into_iter()
        .filter(|insight| *insight.state() == InsightState::Filled)
        .collect()
}

fn round_to_increment(value: f64, increment: f64) -> f64 {
    if increment <= 0.0 {
        return value;
    }
    (value / increment).round() * increment
}

fn round_to_digits(value: f64, digits: i64) -> f64 {
    let factor = 10f64.powi(digits as i32);
    (value * factor).round() / factor
}

impl<'a, E, D> TradingTools for StrategyTools<'a, E, D>
where
    E: Broker + OrderManagementProvider,
    D: DataFeed + DataProvider,
{
    fn dynamic_round(&self, value: f64, symbol: &str) -> f64 {
        self.universe
            .get(symbol)
            .map(|asset| dynamic_round_for_asset(value, asset))
            .unwrap_or(value)
    }

    fn quantity_round(&self, value: f64, symbol: &str) -> f64 {
        self.universe
            .get(symbol)
            .map(|asset| quantity_round_for_asset(value, asset))
            .unwrap_or(value)
    }

    fn calculate_time_to_live(&self, price: f64, entry: f64, atr: f64, additional: i32) -> i32 {
        calculate_time_to_live(price, entry, atr, additional)
    }

    fn get_unrealized_pnl(&self, symbol: &str) -> Result<f64, BrokerError> {
        let position = self.broker.get_position_sync(symbol)?;
        Ok(unrealized_pnl_for_position(&position))
    }

    fn get_all_unrealized_pnl(&self) -> Result<f64, BrokerError> {
        let positions = self.broker.get_positions_sync()?;
        Ok(aggregate_unrealized_pnl(&positions))
    }

    fn get_filled_insights(&self) -> Vec<Insight> {
        filled_insights_from_collection(self.insights)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::broker::types::{AssetExchange, AssetStatus, AssetType};

    fn sample_asset() -> Asset {
        Asset {
            id: "1".to_string(),
            symbol: "TSLA".to_string(),
            name: "Tesla".to_string(),
            asset_type: AssetType::Stock,
            status: AssetStatus::Active,
            exchange: AssetExchange::NASDAQ,
            tradable: true,
            marginable: true,
            shortable: true,
            fractional: true,
            min_order_size: Some(0.001),
            quantity_base: Some(3),
            max_order_size: None,
            min_price_increment: Some(0.01),
            price_base: Some(2),
            contract_size: None,
            fees: Default::default(),
        }
    }

    #[test]
    fn dynamic_round_uses_asset_price_precision() {
        assert_eq!(dynamic_round_for_asset(12.3456, &sample_asset()), 12.35);
    }

    #[test]
    fn quantity_round_uses_asset_quantity_precision() {
        assert_eq!(quantity_round_for_asset(1.23456, &sample_asset()), 1.235);
    }

    #[test]
    fn ttl_matches_python_formula() {
        assert_eq!(calculate_time_to_live(110.0, 100.0, 2.5, 2), 6);
    }

    #[test]
    fn unrealized_pnl_matches_side() {
        let mut position = Position {
            account_id: "acct".to_string(),
            asset: sample_asset(),
            avg_entry_price: 100.0,
            qty: 2.0,
            side: OrderSide::Buy,
            market_value: 210.0,
            cost_basis: 200.0,
            current_price: 105.0,
            unrealized_pnl: 0.0,
            realized_pnl: 0.0,
            entry_commission: 0.0,
            margin_required: None,
        };
        assert_eq!(unrealized_pnl_for_position(&position), 10.0);

        position.side = OrderSide::Sell;
        assert_eq!(unrealized_pnl_for_position(&position), -10.0);
    }

    #[test]
    fn filters_filled_insights() {
        use crate::core::insight::types::StrategyType;
        use crate::core::utils::timeframe::{TimeFrame, TimeFrameUnit};

        let mut insights = InsightCollection::new();
        let mut filled = Insight::new(
            OrderSide::Buy,
            "TSLA".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            50,
            None,
        );
        filled.state = InsightState::Filled;
        insights.add_insight(filled.clone());

        let new_insight = Insight::new(
            OrderSide::Sell,
            "AAPL".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            50,
            None,
        );
        insights.add_insight(new_insight);

        let result = filled_insights_from_collection(&insights);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].symbol(), "TSLA");
    }
}
