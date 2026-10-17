use crate::core::broker::types::{Account, Asset, BrokerError, OrderSide};
use crate::core::insight::Insight;
use crate::core::strategy::StrategyContext;

/// Shared quantity sizing helpers for built-in pipes and user strategies.
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
    pub max_total_size: Option<f64>,
    pub min_order_size: Option<f64>,
    pub quantity_base: Option<i64>,
    pub fractional: bool,
}

impl AssetQuantityLimits {
    pub fn from_asset(asset: &Asset) -> Self {
        Self {
            max_order_size: asset.max_order_size,
            max_total_size: asset.max_total_size,
            min_order_size: asset.min_order_size,
            quantity_base: asset.quantity_base,
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
    // The input is an integer percentage, never a fractional rate: 1 means 1%.
    percent as f64 / 100.0
}

pub fn account_size_from_equity_or_cash(account: &Account) -> f64 {
    if account.equity > 0.0 {
        account.equity
    } else {
        account.cash.max(account.buying_power)
    }
}

pub fn entry_price(ctx: &dyn StrategyContext, insight: &Insight) -> Result<f64, BrokerError> {
    if let Some(entry) = insight.limit_price.or(insight.stop_price) {
        return Ok(entry);
    }

    let quote = ctx.latest_quote(&insight.symbol)?;
    if quote.symbol != insight.symbol || !quote.valid_market_values() {
        return Err(BrokerError::SnapshotUnavailable {
            kind: crate::core::broker::snapshots::SnapshotKind::Quote {
                symbol: insight.symbol.clone(),
            },
            reason: crate::core::broker::snapshots::SnapshotUnavailable::Invalid,
        });
    }
    Ok(match insight.side {
        OrderSide::Buy => quote.ask,
        OrderSide::Sell => quote.bid,
    })
}

pub fn validate_stop_for_side(insight: &Insight, entry: f64, stop_loss: f64) -> Result<(), String> {
    if !entry.is_finite() || entry <= 0.0 || !stop_loss.is_finite() || stop_loss <= 0.0 {
        return Err("Entry and stop loss must be finite and positive".into());
    }
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
    let asset = ctx
        .universe()
        .get(symbol)
        .ok_or_else(|| format!("Asset {symbol} not found"))?;
    let mut limits = AssetQuantityLimits::from_asset(asset);
    limits.fractional = fractional;
    let rounded = floor_quantity(quantity, limits);
    validate_sized_quantity(symbol, rounded, limits)?;
    Ok(rounded)
}

/// Broker quantities are lots/contracts when a contract size is supplied.
/// Apply the multiplier before rounding, not by rounding whole lots afterwards.
pub fn contract_multiplier(asset: &Asset) -> f64 {
    asset.contract_size.unwrap_or(1).max(1) as f64
}

pub fn validate_sized_quantity(
    symbol: &str,
    quantity: f64,
    limits: AssetQuantityLimits,
) -> Result<(), String> {
    if !quantity.is_finite() || quantity <= 0.0 {
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

/// Round down, never up across an allocation/broker ceiling.
pub fn floor_quantity(quantity: f64, limits: AssetQuantityLimits) -> f64 {
    if !quantity.is_finite() || quantity <= 0.0 {
        return 0.0;
    }
    let step = if !limits.fractional {
        1.0
    } else if let Some(base) = limits.quantity_base {
        10_f64.powi(-(base.clamp(0, 12) as i32))
    } else {
        limits
            .min_order_size
            .filter(|v| v.is_finite() && *v > 0.0)
            .unwrap_or(1e-8)
    };
    let units = quantity / step;
    // Absorb only floating-point representation noise at an exact grid boundary.
    // `min` ensures even that correction cannot exceed the caller's hard budget.
    ((units + units.abs() * f64::EPSILON * 4.0).floor() * step).min(quantity)
}

/// Remaining conservative combined-symbol capacity, reserving queued descendants too.
pub fn available_asset_quantity(
    ctx: &dyn StrategyContext,
    insight: &Insight,
    limit: Option<f64>,
) -> Result<f64, String> {
    match limit {
        None => Ok(f64::INFINITY),
        Some(limit) if limit.is_finite() && limit > 0.0 => Ok((limit
            - ctx
                .insights()
                .allocated_quantity_excluding(insight.symbol(), insight))
        .max(0.0)),
        Some(_) => Err("Invalid broker Asset max total quantity".to_string()),
    }
}

pub fn cap_asset_total_quantity(
    ctx: &dyn StrategyContext,
    insight: &mut Insight,
    desired: f64,
    limits: AssetQuantityLimits,
) -> Result<f64, String> {
    if !desired.is_finite() || desired <= 0.0 {
        return Err("Calculated quantity must be finite and positive".to_string());
    }
    let available = available_asset_quantity(ctx, insight, limits.max_total_size)?;
    // This is a group budget, not an individual order. Rounding the aggregate before
    // rounding its parts can discard another full step due to sum representation noise.
    let capped = desired.min(available);
    if desired > available {
        insight.state_log(Some(format!("Reached broker Asset max total quantity in pending insights: requested={desired:.8} available={available:.8} capped={capped:.8}")));
    }
    Ok(capped)
}

pub fn available_pending_orders(
    ctx: &dyn StrategyContext,
    insight: &Insight,
) -> Result<Option<usize>, String> {
    if !ctx.has_broker_feature(crate::core::broker::BrokerFeature::MaxTotalOrders) {
        return Ok(None);
    }
    let account = ctx
        .cached_account()
        .ok_or("Broker account snapshot unavailable for order-limit validation")?;
    Ok(account.max_total_orders.map(|limit| {
        (limit as usize).saturating_sub(ctx.insights().pending_order_count_excluding(insight))
    }))
}

/// Cap a logical stack before allocating any child objects. One common scale factor
/// preserves the DCA shape; downward rounding leaves unused dust, never excess exposure.
pub fn cap_quantity_levels(
    ctx: &dyn StrategyContext,
    insight: &mut Insight,
    quantities: &mut [f64],
    limits: AssetQuantityLimits,
) -> Result<f64, String> {
    if quantities.is_empty() || quantities.iter().any(|q| !q.is_finite() || *q <= 0.0) {
        return Err("Invalid DCA quantity levels".into());
    }
    let desired: f64 = quantities.iter().sum();
    let capped = cap_asset_total_quantity(ctx, insight, desired, limits)?;
    let mut scale = (capped / desired).min(1.0);
    if let Some(slots) = available_pending_orders(ctx, insight)? {
        if slots < quantities.len() {
            return Err("Insufficient broker pending order slots for DCA levels".into());
        }
        if let Some(max) = limits.max_order_size {
            let max = floor_quantity(max, limits);
            if max <= 0.0 {
                return Err("Invalid maximum order size".into());
            }
            let count = |scale: f64| {
                quantities
                    .iter()
                    .map(|q| (floor_quantity(q * scale, limits) / max).ceil().max(1.0) as usize)
                    .sum::<usize>()
            };
            if count(scale) > slots {
                let (mut low, mut high) = (0.0, scale);
                for _ in 0..48 {
                    let mid = (low + high) / 2.0;
                    if count(mid) <= slots {
                        low = mid;
                    } else {
                        high = mid;
                    }
                }
                scale = low;
                insight.state_log(Some(format!("Reached broker max total pending orders: DCA allocation scaled to={scale:.8} available_slots={slots}")));
            }
        }
    }
    for quantity in quantities {
        *quantity = floor_quantity(*quantity * scale, limits);
        validate_sized_quantity(insight.symbol(), *quantity, limits)?;
    }
    Ok(scale)
}

/// Pure bounded split planning, shared by built-ins and custom stack builders.
pub fn plan_quantity_parts(
    quantity: f64,
    limits: AssetQuantityLimits,
    max_parts: Option<usize>,
) -> Result<Vec<f64>, String> {
    let quantity = floor_quantity(quantity, limits);
    validate_sized_quantity("entry", quantity, limits)?;
    let max = match limits.max_order_size {
        Some(max) if max.is_finite() && max > 0.0 => floor_quantity(max, limits),
        Some(_) => return Err("Invalid maximum order size".to_string()),
        None => quantity,
    };
    validate_sized_quantity("entry", max, limits)?;
    let count = (quantity / max).ceil() as usize;
    // Protect the allocator even with malformed custom metadata/unlimited paper accounts.
    if count > max_parts.unwrap_or(65_536).min(65_536) {
        return Err(
            "Reached broker max total pending orders or safe split planning limit".to_string(),
        );
    }
    let mut result = Vec::with_capacity(count);
    let mut remaining = quantity;
    for _ in 0..count {
        let part = floor_quantity(remaining.min(max), limits);
        if part <= 0.0 || limits.min_order_size.is_some_and(|min| part < min) {
            break;
        }
        validate_sized_quantity("entry", part, limits)?;
        result.push(part);
        remaining = (remaining - part).max(0.0);
    }
    Ok(result)
}

/// Final guard for a complete pending tree (including custom DCA builders).
/// It never mutates already-submitted orders: insufficient capacity fails closed.
pub fn validate_pending_tree_limits(
    ctx: &dyn StrategyContext,
    insight: &Insight,
) -> Result<(), String> {
    let Some(asset) = ctx.universe().get(insight.symbol()) else {
        return Err("Asset is not loaded".into());
    };
    let available = available_asset_quantity(ctx, insight, asset.max_total_size)?;
    if insight.total_insight_quantity_size() > available + 1e-8 {
        return Err("Reached broker Asset max total quantity in pending insights".into());
    }
    if let Some(available) = available_pending_orders(ctx, insight)? {
        use crate::core::broker::types::OrderType;
        let mut seen = rustc_hash::FxHashSet::default();
        let mut stack = vec![insight];
        let mut count = 0;
        while let Some(row) = stack.pop() {
            if !seen.insert(*row.insight_id()) || row.state().is_inactive() {
                continue;
            }
            if !matches!(row.order_type(), OrderType::Market) {
                count += 1;
            }
            stack.extend(row.children.iter());
        }
        if count > available {
            return Err(format!(
                "Reached broker max total pending orders: requested={count} available={available}"
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

    parent.add_child_insight(child, Some(true), ctx);
    Ok(())
}

pub fn apply_max_order_size_with_child_legs(
    ctx: &mut dyn StrategyContext,
    insight: &mut Insight,
    target_quantity: f64,
    limits: AssetQuantityLimits,
) -> Result<QuantitySplitResult, String> {
    if insight.children.iter().any(|child| {
        child.is_split_insight()
            && (child.submitted || *child.state() != crate::core::insight::types::InsightState::New)
    }) {
        return Err("Cannot resize already-active split children".into());
    }
    if insight
        .children
        .iter()
        .any(|child| child.is_split_insight() && !child.children.is_empty())
    {
        return Err("Cannot rebuild split children with attached descendants; size the complete stack first".into());
    }
    // Ordinary DCA descendants reserve their own budget; sizing this execution group
    // must never reuse them as if they were per-order split fragments.
    let reserved: f64 = insight
        .children
        .iter()
        .filter(|child| !child.is_split_insight())
        .map(Insight::total_insight_quantity_size)
        .sum();
    let capped_total = cap_asset_total_quantity(ctx, insight, target_quantity + reserved, limits)?;
    let mut target = floor_quantity((capped_total - reserved).max(0.0), limits);
    let mut slots = available_pending_orders(ctx, insight)?;
    if let Some(ref mut available) = slots {
        let mut stack: Vec<_> = insight
            .children
            .iter()
            .filter(|c| !c.is_split_insight())
            .collect();
        while let Some(child) = stack.pop() {
            if child.state().is_inactive() {
                continue;
            }
            if !matches!(
                child.order_type(),
                crate::core::broker::types::OrderType::Market
            ) {
                *available = available.saturating_sub(1);
            }
            stack.extend(child.children.iter());
        }
        if matches!(
            insight.order_type(),
            crate::core::broker::types::OrderType::Market
        ) {
            slots = None;
        } else if let Some(max) = limits.max_order_size {
            let capacity = floor_quantity(max, limits) * *available as f64;
            if target > capacity {
                insight.state_log(Some(format!("Reached broker max total pending orders: available={} quantity reduced from={target:.8} to={capacity:.8}", *available)));
                target = capacity;
            }
        }
    }
    let parts = plan_quantity_parts(target, limits, slots)?;
    let parent_quantity = *parts.first().ok_or("Calculated quantity is zero")?;
    // Planning/validation finished before touching the tree. Re-running sizing cannot
    // retain an oversized old set of execution fragments.
    let unchanged = insight
        .children
        .iter()
        .filter(|child| child.is_split_insight())
        .map(|child| child.quantity())
        .eq(parts.iter().skip(1).copied().map(Some));
    if !unchanged {
        insight.children.retain(|child| !child.is_split_insight());
        for quantity in parts.iter().skip(1) {
            create_child_insight(ctx, insight, *quantity)?;
        }
    }
    Ok(QuantitySplitResult {
        parent_quantity,
        child_count: parts.len() - 1,
        child_quantity_total: parts.iter().skip(1).sum(),
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
pub(crate) mod tests {
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

    struct MockTools<'a>(&'a MockContext);

    impl TradingTools for MockTools<'_> {
        fn sizing_context(&self) -> Result<&dyn StrategyContext, String> {
            Ok(self.0)
        }
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

    pub(crate) struct MockContext {
        universe: HashMap<String, Asset>,
        pub(crate) history: HashMap<String, DataFrame>,
        pub(crate) insights: InsightCollection,
        variables: DashMap<String, Value>,
        pub(crate) account: Account,
        timeframe: TimeFrame,
        pub(crate) command_error: Option<crate::core::broker::commands::BrokerCommandError>,
        pub(crate) commands: Vec<crate::core::broker::commands::BrokerCommandAction>,
    }

    impl MockContext {
        pub(crate) fn new(asset: Asset) -> Self {
            Self {
                universe: HashMap::from([(asset.symbol.clone(), asset)]),
                history: HashMap::new(),
                insights: InsightCollection::new(),
                variables: DashMap::new(),
                account: Account {
                    account_id: "paper".to_string(),
                    max_total_orders: None,
                    account_type: AccountType::Paper,
                    equity: 10_000.0,
                    cash: 10_000.0,
                    currency: "USD".to_string(),
                    buying_power: 10_000.0,
                    accrued_commission: 0.0,
                    shorting_enabled: true,
                    leverage: 1,
                },
                timeframe: TimeFrame::new(1, TimeFrameUnit::Minute),
                command_error: Some(crate::core::broker::commands::BrokerCommandError::Closed),
                commands: Vec::new(),
            }
        }
    }

    impl StrategyContext for MockContext {
        fn enqueue_broker_command(
            &mut self,
            insight_id: uuid::Uuid,
            action: crate::core::broker::commands::BrokerCommandAction,
            group: Option<crate::core::broker::commands::BrokerOperationGroup>,
        ) -> Result<crate::core::broker::commands::BrokerCommandReceipt, crate::core::broker::commands::BrokerCommandError> {
            if let Some(error) = &self.command_error { return Err(error.clone()); }
            self.commands.push(action);
            Ok(crate::core::broker::commands::BrokerCommandReceipt {
                id: crate::core::broker::commands::BrokerCommandId(format!("mock-{}", self.commands.len())),
                insight_id: insight_id.to_string(), group,
            })
        }
        fn broker_features(&self) -> &'static [crate::core::broker::BrokerFeature] {
            &[crate::core::broker::BrokerFeature::MaxTotalOrders]
        }
        fn cached_account(&self) -> Option<Account> {
            Some(self.account.clone())
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
            StrategyMode::Backtest
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
            Box::new(MockTools(self))
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

        fn shutdown(&mut self) {}
    }

    pub(crate) fn sample_asset() -> Asset {
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
            max_total_size: None,
            min_price_increment: Some(0.01),
            price_base: Some(2),
            contract_size: None,
            trading_calendar: None,
            fees: Default::default(),
        }
    }

    pub(crate) fn sample_insight() -> Insight {
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
        assert!(insight.children.iter().all(Insight::is_split_insight));
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
        let child_ids: Vec<_> = insight
            .children
            .iter()
            .map(|child| *child.insight_id())
            .collect();
        let result =
            apply_max_order_size_with_child_legs(&mut ctx, &mut insight, 250.0, limits).unwrap();

        assert_eq!(insight.children.len(), 2);
        assert_eq!(result.child_count, 2);
        assert_eq!(result.child_quantity_total, 150.0);
        assert_eq!(
            child_ids,
            insight
                .children
                .iter()
                .map(|child| *child.insight_id())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn normalised_percent_rate_accepts_percent_inputs() {
        assert_eq!(normalised_percent_rate(2, DEFAULT_RISK_PERCENT), 0.02);
        assert_eq!(normalised_percent_rate(0, DEFAULT_RISK_PERCENT), 0.02);
    }

    #[test]
    fn tools_facade_uses_context_capacity_and_shared_split_math() {
        let mut asset = sample_asset();
        asset.max_total_size = Some(1000.0);
        let mut ctx = MockContext::new(asset.clone());
        let mut other = sample_insight();
        other.quantity = Some(300.0);
        ctx.add_insight(other);
        let mut insight = sample_insight();
        let tools = ctx.tools();
        assert_eq!(
            tools
                .total_insight_quantity_size(&asset.symbol)
                .unwrap()
                .total_quantity,
            300.0
        );
        assert_eq!(tools.available_asset_quantity(&insight).unwrap(), 700.0);
        assert_eq!(tools.available_pending_orders(&insight).unwrap(), None);
        assert_eq!(
            tools.cap_asset_total_quantity(&mut insight, 900.0).unwrap(),
            700.0
        );
        let mut levels = [500.0, 500.0];
        tools
            .cap_quantity_levels(&mut insight, &mut levels)
            .unwrap();
        assert_eq!(levels, [350.0, 350.0]);
        let parts = tools.plan_quantity_parts(350.0, &asset, None).unwrap();
        assert_eq!(parts, vec![100.0, 100.0, 100.0, 50.0]);
        assert_eq!(
            tools.floor_quantity(1.23456, &asset),
            floor_quantity(1.23456, AssetQuantityLimits::from_asset(&asset))
        );
        insight.quantity = Some(700.0);
        assert!(tools.validate_pending_tree_limits(&insight).is_ok());
        insight.quantity = Some(701.0);
        assert!(tools.validate_pending_tree_limits(&insight).is_err());
    }

    #[test]
    fn total_quantity_cap_includes_other_pending_trees_and_rebuilds_splits() {
        let mut asset = sample_asset();
        asset.max_total_size = Some(1000.0);
        let limits = AssetQuantityLimits::from_asset(&asset);
        let mut ctx = MockContext::new(asset.clone());
        let mut other = sample_insight();
        other.quantity = Some(300.0);
        let mut child = sample_insight();
        child.quantity = Some(200.0);
        other.add_child_insight(child, None, &mut ctx);
        ctx.add_insight(other);
        let mut insight = sample_insight();
        let split =
            apply_max_order_size_with_child_legs(&mut ctx, &mut insight, 1500.0, limits).unwrap();
        insight.set_quantity(Some(split.parent_quantity));
        assert_eq!(insight.total_insight_quantity_size(), 500.0);
        assert_eq!(split.child_count, 4);
        assert!(insight.state_history.iter().any(|(_, _, message)| {
            message
                .as_deref()
                .is_some_and(|m| m.contains("Reached broker Asset max total quantity"))
        }));
        let smaller =
            apply_max_order_size_with_child_legs(&mut ctx, &mut insight, 150.0, limits).unwrap();
        insight.set_quantity(Some(smaller.parent_quantity));
        assert_eq!(insight.total_insight_quantity_size(), 150.0);
        assert_eq!(insight.children.len(), 1);
        assert_eq!(
            ctx.insights()
                .total_insight_quantity_size(&asset)
                .total_quantity,
            500.0
        );
    }

    #[test]
    fn quantity_slots_cap_splits_and_exclude_filled_positions() {
        use crate::core::insight::types::InsightState;
        let asset = sample_asset();
        let limits = AssetQuantityLimits::from_asset(&asset);
        let mut ctx = MockContext::new(asset);
        ctx.account.max_total_orders = Some(3);
        let mut pending = sample_insight();
        pending.quantity = Some(10.0);
        pending.state = InsightState::Executed;
        ctx.add_insight(pending);
        let mut filled = sample_insight();
        filled.quantity = Some(10.0);
        filled.state = InsightState::Filled;
        ctx.add_insight(filled);
        let mut insight = sample_insight();
        let result =
            apply_max_order_size_with_child_legs(&mut ctx, &mut insight, 900.0, limits).unwrap();
        assert_eq!(result.total_quantity(), 200.0);
        assert_eq!(result.child_count, 1);
    }

    #[test]
    fn quantity_stack_scaling_preserves_shape_and_broker_ceiling() {
        let mut asset = sample_asset();
        asset.max_total_size = Some(1000.0);
        asset.quantity_base = Some(2);
        asset.min_order_size = Some(0.01);
        let limits = AssetQuantityLimits::from_asset(&asset);
        let mut ctx = MockContext::new(asset);
        let mut existing = sample_insight();
        existing.quantity = Some(250.0);
        ctx.add_insight(existing);
        let mut insight = sample_insight();
        let mut quantities = [200.0, 200.0, 600.0, 1200.0];
        let scale = cap_quantity_levels(&ctx, &mut insight, &mut quantities, limits).unwrap();
        assert!((scale - 750.0 / 2200.0).abs() < 1e-9);
        assert!(quantities.iter().sum::<f64>() <= 750.0);
        assert!((quantities[3] / quantities[0] - 6.0).abs() < 0.001);
        ctx.account.max_total_orders = Some(6);
        let mut quantities = [200.0, 200.0, 600.0, 1200.0];
        cap_quantity_levels(&ctx, &mut insight, &mut quantities, limits).unwrap();
        let parts: usize = quantities
            .iter()
            .map(|q| plan_quantity_parts(*q, limits, None).unwrap().len())
            .sum();
        assert!(parts <= 5); // existing pending entry reserves one slot
    }

    #[test]
    fn quantity_invalid_or_exhausted_limits_fail_without_children() {
        let mut asset = sample_asset();
        asset.max_total_size = Some(100.0);
        let limits = AssetQuantityLimits::from_asset(&asset);
        let mut ctx = MockContext::new(asset);
        let mut existing = sample_insight();
        existing.quantity = Some(100.0);
        ctx.add_insight(existing);
        let mut insight = sample_insight();
        assert!(apply_max_order_size_with_child_legs(&mut ctx, &mut insight, 1.0, limits).is_err());
        assert!(insight.children.is_empty());
        assert!(plan_quantity_parts(f64::INFINITY, limits, None).is_err());
        assert!(plan_quantity_parts(1e30, limits, None).is_err());
    }
}
