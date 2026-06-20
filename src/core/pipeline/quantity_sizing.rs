use crate::core::broker::types::{Account, Asset, OrderSide};
use crate::core::insight::Insight;
use crate::core::strategy::StrategyContext;

/// Shared quantity sizing helpers for AQE built-in insight pipes.
///
/// Author: @isaac-diaby
///
/// These helpers centralise broker/asset quantity constraints so sizing pipes do
/// not silently discard quantity above an asset's maximum single-entry size.
const EPSILON: f64 = f64::EPSILON;

pub const DEFAULT_RISK_PERCENT: i64 = 2;

#[derive(Clone, Copy, Debug)]
pub struct AssetQuantityLimits {
    pub max_order_size: Option<f64>,
    pub min_order_size: Option<f64>,
    pub fractional: bool,
}

impl AssetQuantityLimits {
    pub fn from_asset(asset: &Asset) -> Self {
        Self {
            max_order_size: asset.max_order_size,
            min_order_size: asset.min_order_size,
            fractional: asset.fractional,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct QuantitySplitResult {
    pub parent_quantity: f64,
    pub child_count: usize,
    pub child_quantity_total: f64,
    pub target_quantity: f64,
}

impl QuantitySplitResult {
    pub fn total_quantity(&self) -> f64 {
        self.parent_quantity + self.child_quantity_total
    }
}

pub fn normalised_percent_rate(value: i64, default_percent: i64) -> f64 {
    let percent = if value > 0 { value } else { default_percent };
    if percent > 1 {
        percent as f64 / 100.0
    } else {
        percent as f64
    }
}

pub fn account_size_from_equity_or_cash(account: &Account) -> f64 {
    if account.equity > 0.0 {
        account.equity
    } else {
        account.cash.max(account.buying_power)
    }
}

pub fn entry_price(ctx: &dyn StrategyContext, insight: &Insight) -> Result<f64, String> {
    if let Some(entry) = insight.limit_price.or(insight.stop_price) {
        return Ok(entry);
    }

    let quote = ctx
        .latest_quote(&insight.symbol)
        .map_err(|e| e.to_string())?;
    Ok(match insight.side {
        OrderSide::Buy => quote.ask,
        OrderSide::Sell => quote.bid,
    })
}

pub fn validate_stop_for_side(insight: &Insight, entry: f64, stop_loss: f64) -> Result<(), String> {
    let invalid_stop = match insight.side {
        OrderSide::Buy => stop_loss >= entry,
        OrderSide::Sell => stop_loss <= entry,
    };

    if invalid_stop {
        Err("Stop loss is invalid for the insight side".to_string())
    } else {
        Ok(())
    }
}

pub fn invalid_asset_quantity(asset_fractional: bool, quantity: f64) -> Option<&'static str> {
    if !asset_fractional && quantity < 1.0 {
        return Some("Asset is not fractionable");
    }

    if !asset_fractional && quantity.fract().abs() > EPSILON {
        return Some("Asset requires whole-unit quantity");
    }

    None
}

pub fn round_whole_or_fractional_quantity(
    ctx: &dyn StrategyContext,
    symbol: &str,
    quantity: f64,
    fractional: bool,
) -> Result<f64, String> {
    if quantity > 1.0 {
        Ok(quantity.floor())
    } else if fractional {
        Ok(ctx.tools().quantity_round(quantity, symbol))
    } else {
        Err(format!(
            "Asset {} is not fractionable for quantity {:.4}",
            symbol, quantity
        ))
    }
}

pub fn validate_sized_quantity(
    symbol: &str,
    quantity: f64,
    limits: AssetQuantityLimits,
) -> Result<(), String> {
    if quantity <= 0.0 {
        return Err("Calculated quantity is zero".to_string());
    }

    if let Some(reason) = invalid_asset_quantity(limits.fractional, quantity) {
        return Err(format!(
            "Asset {} invalid quantity {:.4}: {}",
            symbol, quantity, reason
        ));
    }

    if let Some(min_order_size) = limits.min_order_size {
        if quantity < min_order_size {
            return Err(format!(
                "Quantity {:.4} is below minimum order size {:.4}",
                quantity, min_order_size
            ));
        }
    }

    Ok(())
}

fn create_child_insight(
    ctx: &mut dyn StrategyContext,
    parent: &mut Insight,
    quantity: f64,
) -> Result<(), String> {
    let mut child = Insight::new(
        parent.side.clone(),
        parent.symbol.clone(),
        parent.strategy_type.clone(),
        *parent.timeframe(),
        parent.confidence(),
        Some(*parent.insight_id()),
    );

    child
        .set_quantity(Some(quantity))
        .set_limit_price(parent.limit_price())
        .set_stop_price(parent.stop_price())
        .set_stop_loss_levels(parent.stop_loss_levels())
        .set_take_profit_levels(parent.take_profit_levels())
        .set_trailing_stop_price(parent.trailing_stop_price())
        .set_period_unfilled(parent.period_unfilled())
        .set_period_till_tp(parent.period_till_tp())
        .set_execution_depends(parent.execution_depends().clone());

    parent.add_child_insight(child, ctx);
    Ok(())
}

fn add_child_insights_for_remaining_quantity(
    ctx: &mut dyn StrategyContext,
    insight: &mut Insight,
    remaining_quantity: f64,
    max_order_size: f64,
    limits: AssetQuantityLimits,
) -> Result<(usize, f64), String> {
    if !insight.children.is_empty() {
        let existing_child_quantity = insight
            .children
            .iter()
            .filter_map(|child| child.quantity)
            .sum();
        return Ok((insight.children.len(), existing_child_quantity));
    }

    if remaining_quantity <= 0.0 {
        return Ok((0, 0.0));
    }

    let mut remaining = remaining_quantity;
    let mut child_count = 0usize;
    let mut child_quantity_total = 0.0;

    while remaining > EPSILON {
        let raw_child_quantity = remaining.min(max_order_size);
        let child_quantity = ctx
            .tools()
            .quantity_round(raw_child_quantity, &insight.symbol);

        if child_quantity <= 0.0 {
            break;
        }

        if let Some(min_order_size) = limits.min_order_size {
            if child_quantity < min_order_size {
                break;
            }
        }

        if let Some(reason) = invalid_asset_quantity(limits.fractional, child_quantity) {
            return Err(format!(
                "{} for child quantity {:.4}",
                reason, child_quantity
            ));
        }

        create_child_insight(ctx, insight, child_quantity)?;
        child_count += 1;
        child_quantity_total += child_quantity;
        remaining = (remaining - child_quantity).max(0.0);
    }

    Ok((child_count, child_quantity_total))
}

pub fn apply_max_order_size_with_child_legs(
    ctx: &mut dyn StrategyContext,
    insight: &mut Insight,
    target_quantity: f64,
    limits: AssetQuantityLimits,
) -> Result<QuantitySplitResult, String> {
    let mut parent_quantity = target_quantity;
    let mut child_count = 0usize;
    let mut child_quantity_total = 0.0;

    if let Some(max_order_size) = limits.max_order_size {
        if max_order_size <= 0.0 {
            return Err(format!(
                "Asset {} has invalid maximum order size {:.4}",
                insight.symbol, max_order_size
            ));
        }

        if parent_quantity > max_order_size {
            let capped_quantity = ctx.tools().quantity_round(max_order_size, &insight.symbol);
            if capped_quantity <= 0.0 {
                return Err(format!(
                    "Asset {} maximum order size rounds to zero",
                    insight.symbol
                ));
            }

            let remaining_quantity = (parent_quantity - capped_quantity).max(0.0);
            parent_quantity = capped_quantity;
            let (count, total) = add_child_insights_for_remaining_quantity(
                ctx,
                insight,
                remaining_quantity,
                max_order_size,
                limits,
            )?;
            child_count = count;
            child_quantity_total = total;
        }
    }

    validate_sized_quantity(&insight.symbol, parent_quantity, limits)?;

    Ok(QuantitySplitResult {
        parent_quantity,
        child_count,
        child_quantity_total,
        target_quantity,
    })
}

pub fn split_summary(result: &QuantitySplitResult) -> String {
    if result.child_count > 0 {
        format!(
            ", parent_qty={:.4}, children={}, child_qty={:.4}, target_qty={:.4}",
            result.parent_quantity,
            result.child_count,
            result.child_quantity_total,
            result.target_quantity
        )
    } else if result.parent_quantity < result.target_quantity {
        format!(", capped_from={:.4}", result.target_quantity)
    } else {
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::alpha::WrappedAlphaModel;
    use crate::core::broker::types::{
        AccountType, AssetExchange, AssetStatus, AssetType, BrokerError, Quote,
    };
    use crate::core::indicators::Indicator;
    use crate::core::insight::InsightCollection;
    use crate::core::insight::types::{StrategyDependentConfirmation, StrategyType};
    use crate::core::pipeline::WrappedInsightPipe;
    use crate::core::strategy::{StrategyContext, StrategyMode};
    use crate::core::universe::WrappedUniverseModel;
    use crate::core::utils::timeframe::{TimeFrame, TimeFrameUnit};
    use crate::core::utils::tools::TradingTools;
    use chrono::Utc;
    use dashmap::DashMap;
    use polars::prelude::DataFrame;
    use serde_json::Value;
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

    struct MockContext {
        universe: HashMap<String, Asset>,
        history: HashMap<String, DataFrame>,
        insights: InsightCollection,
        variables: DashMap<String, Value>,
        account: Account,
        timeframe: TimeFrame,
    }

    impl MockContext {
        fn new(asset: Asset) -> Self {
            Self {
                universe: HashMap::from([(asset.symbol.clone(), asset)]),
                history: HashMap::new(),
                insights: InsightCollection::new(),
                variables: DashMap::new(),
                account: Account {
                    account_id: "paper".to_string(),
                    account_type: AccountType::Paper,
                    equity: 10_000.0,
                    cash: 10_000.0,
                    currency: "USD".to_string(),
                    buying_power: 10_000.0,
                    shorting_enabled: true,
                    leverage: 1,
                },
                timeframe: TimeFrame::new(1, TimeFrameUnit::Minute),
            }
        }
    }

    impl StrategyContext for MockContext {
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
            StrategyMode::Backtest
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
            0.02
        }

        fn min_reward_risk_ratio(&self) -> f64 {
            1.0
        }

        fn base_confidence(&self) -> f64 {
            0.7
        }

        fn variables(&self) -> &DashMap<String, Value> {
            &self.variables
        }

        fn tools(&self) -> Box<dyn TradingTools + '_> {
            Box::new(MockTools)
        }

        fn max_history_rows(&self) -> usize {
            2000
        }

        fn set_max_history_rows(&mut self, _rows: usize) {}

        fn warm_up_bars(&self) -> i32 {
            0
        }

        fn set_warm_up_bars(&mut self, _bars: i32) {}

        fn timeframe(&self) -> &TimeFrame {
            &self.timeframe
        }

        fn account(&self) -> Result<Account, BrokerError> {
            Ok(self.account.clone())
        }

        fn current_time(&self) -> chrono::DateTime<chrono::Utc> {
            Utc::now()
        }

        fn bind_insight_context(&self, _insight: &mut Insight) {}

        fn latest_quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
            Ok(Quote {
                symbol: symbol.to_string(),
                bid: 99.0,
                ask: 100.0,
                bid_size: 1.0,
                ask_size: 1.0,
                last: Some(100.0),
                last_size: Some(1.0),
                timestamp: Utc::now(),
            })
        }

        fn cancel_order(&self, _order_id: &str) -> Result<bool, BrokerError> {
            Ok(false)
        }

        fn update_order(
            &self,
            _order_id: &str,
            _price: f64,
            _qty: f64,
        ) -> Result<bool, BrokerError> {
            Ok(false)
        }

        fn update_stop_loss_order(
            &self,
            _order_id: &str,
            _price: f64,
            _qty: f64,
        ) -> Result<bool, BrokerError> {
            Ok(false)
        }

        fn close_position(
            &self,
            _order_id: &str,
            _qty: f64,
            _price: Option<f64>,
        ) -> Result<bool, BrokerError> {
            Ok(false)
        }

        fn shutdown(&mut self) {}
    }

    fn sample_asset() -> Asset {
        Asset {
            id: "asset-1".to_string(),
            symbol: "BTCUSD".to_string(),
            name: "BTCUSD".to_string(),
            asset_type: AssetType::Crypto,
            status: AssetStatus::Active,
            exchange: AssetExchange::UNKNOWN("TEST".to_string()),
            tradable: true,
            marginable: true,
            shortable: true,
            fractional: true,
            min_order_size: Some(1.0),
            quantity_base: Some(0),
            max_order_size: Some(100.0),
            min_price_increment: Some(0.01),
            price_base: Some(2),
            contract_size: None,
        }
    }

    fn sample_insight() -> Insight {
        let mut insight = Insight::new(
            OrderSide::Buy,
            "BTCUSD".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            80,
            None,
        );
        insight
            .set_limit_price(Some(100.0))
            .set_stop_loss(Some(90.0))
            .set_take_profit_levels(Some(vec![120.0]))
            .set_period_unfilled(Some(5))
            .set_period_till_tp(Some(10))
            .set_execution_depends(vec![
                StrategyDependentConfirmation::LowRelativeVolumeConfirmationModel,
            ]);
        insight
    }

    #[test]
    fn split_quantity_creates_child_legs_for_excess_max_order_size() {
        let asset = sample_asset();
        let mut ctx = MockContext::new(asset.clone());
        let mut insight = sample_insight();

        let result = apply_max_order_size_with_child_legs(
            &mut ctx,
            &mut insight,
            250.0,
            AssetQuantityLimits::from_asset(&asset),
        )
        .unwrap();

        assert_eq!(result.parent_quantity, 100.0);
        assert_eq!(result.child_count, 2);
        assert_eq!(result.child_quantity_total, 150.0);
        assert_eq!(result.total_quantity(), 250.0);
        assert_eq!(insight.children.len(), 2);
        assert_eq!(insight.children[0].quantity(), Some(100.0));
        assert_eq!(insight.children[1].quantity(), Some(50.0));
        assert_eq!(insight.children[0].limit_price(), Some(100.0));
        assert_eq!(insight.children[0].stop_loss(), Some(90.0));
        assert_eq!(insight.children[0].take_profit_levels(), Some(vec![120.0]));
        assert_eq!(insight.children[0].period_unfilled(), Some(5));
        assert_eq!(insight.children[0].period_till_tp(), Some(10));
        assert_eq!(
            insight.children[0].execution_depends().len(),
            insight.execution_depends().len()
        );
    }

    #[test]
    fn split_quantity_reuses_existing_child_legs() {
        let asset = sample_asset();
        let mut ctx = MockContext::new(asset.clone());
        let mut insight = sample_insight();
        let limits = AssetQuantityLimits::from_asset(&asset);

        apply_max_order_size_with_child_legs(&mut ctx, &mut insight, 250.0, limits).unwrap();
        let result =
            apply_max_order_size_with_child_legs(&mut ctx, &mut insight, 250.0, limits).unwrap();

        assert_eq!(insight.children.len(), 2);
        assert_eq!(result.child_count, 2);
        assert_eq!(result.child_quantity_total, 150.0);
    }

    #[test]
    fn normalised_percent_rate_accepts_percent_inputs() {
        assert_eq!(normalised_percent_rate(2, DEFAULT_RISK_PERCENT), 0.02);
        assert_eq!(normalised_percent_rate(0, DEFAULT_RISK_PERCENT), 0.02);
    }
}
