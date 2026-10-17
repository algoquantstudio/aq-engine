use super::{
    BuiltinAlphaOptions,
    options::{self, EntryGate},
};
use crate::core::alpha::{AlphaModel, AlphaResult};
use crate::core::broker::types::{Asset, OrderSide};
use crate::core::indicators::{atr::AverageTrueRange, rsi::RelativeStrengthIndex};
use crate::core::insight::{Insight, types::StrategyDependentConfirmation, types::StrategyType};
use crate::core::strategy::StrategyContext;
use polars::prelude::Float64Chunked;

/// Generates ATR-managed insights from confirmed RSI price-divergence pivots.
///
/// The newest pivot must have `local_window` later bars to confirm it. Only the nearest
/// earlier pivot within `divergance_window` is compared; there is no look-ahead.
/// Price and RSI keep their original row indices, including indicator warm-up nulls.
/// Work is bounded by the configured pivot windows, not the total stored history.
///
/// Existing constructor spelling and defaults are retained for saved strategies.
/// Use `with_options` for direction, exits, expiry, active limits, and cooldowns;
/// `with_divergence_options` controls minimum RSI separation and volume confirmation.
pub struct RsiDiverganceAlpha {
    local_window: usize,
    divergance_window: usize,
    atr_period: usize,
    rsi_period: usize,
    atr_column: String,
    rsi_column: String,
    base_confidence_modifier_field: Option<String>,
    options: BuiltinAlphaOptions,
    min_rsi_change: f64,
    require_low_relative_volume: bool,
    gate: EntryGate,
}

impl RsiDiverganceAlpha {
    pub fn new(
        local_window: usize,
        divergance_window: usize,
        atr_period: usize,
        rsi_period: usize,
        base_confidence_modifier_field: String,
    ) -> Self {
        Self {
            local_window,
            divergance_window,
            atr_period,
            rsi_period,
            atr_column: format!("ATRr_{atr_period}"),
            rsi_column: format!("RSI_{rsi_period}"),
            base_confidence_modifier_field: (!base_confidence_modifier_field.trim().is_empty())
                .then(|| base_confidence_modifier_field.trim().to_string()),
            options: BuiltinAlphaOptions::default(),
            min_rsi_change: 0.0,
            require_low_relative_volume: true,
            gate: EntryGate::default(),
        }
    }

    pub fn with_options(mut self, options: BuiltinAlphaOptions) -> Self {
        self.options = options;
        self
    }

    /// Minimum RSI difference in points (0 keeps strict positive divergence).
    pub fn with_divergence_options(
        mut self,
        min_rsi_change: f64,
        require_low_relative_volume: bool,
    ) -> Self {
        self.min_rsi_change = min_rsi_change;
        self.require_low_relative_volume = require_low_relative_volume;
        self
    }

    fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("Local window", self.local_window),
            ("Divergence window", self.divergance_window),
            ("ATR period", self.atr_period),
            ("RSI period", self.rsi_period),
        ] {
            options::validate_period(name, value)?;
        }
        if self.divergance_window <= self.local_window {
            return Err("Divergence window must be greater than the local pivot window".into());
        }
        if !self.min_rsi_change.is_finite() || !(0.0..=100.0).contains(&self.min_rsi_change) {
            return Err("Minimum RSI change must be between 0 and 100 points".into());
        }
        self.options.validate()
    }

    fn minimum_history_bars(&self) -> usize {
        let confirmed_pivot_bars = self.local_window.saturating_mul(2).saturating_add(2);
        self.atr_period
            .max(self.rsi_period.saturating_add(confirmed_pivot_bars))
    }

    fn is_pivot(values: &Float64Chunked, center: usize, width: usize, low: bool) -> bool {
        let Some(value) = values.get(center).filter(|v| v.is_finite() && *v > 0.0) else {
            return false;
        };
        (center - width..=center + width).all(|index| {
            index == center
                || values.get(index).is_some_and(|neighbor| {
                    neighbor.is_finite()
                        && neighbor > 0.0
                        && if low {
                            value < neighbor
                        } else {
                            value > neighbor
                        }
                })
        })
    }

    fn diverges(
        prices: &Float64Chunked,
        rsi: &Float64Chunked,
        width: usize,
        distance: usize,
        minimum_change: f64,
        low: bool,
    ) -> bool {
        if width == 0 || width > prices.len().saturating_sub(2) / 2 || prices.len() != rsi.len() {
            return false;
        }
        let latest = prices.len() - width - 1;
        if !Self::is_pivot(prices, latest, width, low) {
            return false;
        }
        let Some(latest_rsi) = rsi
            .get(latest)
            .filter(|v| v.is_finite() && (0.0..=100.0).contains(v))
        else {
            return false;
        };
        // Strict local pivots cannot occur within 'width' bars of each other.
        let first = latest.saturating_sub(distance).max(width);
        for previous in (first..latest.saturating_sub(width)).rev() {
            if !Self::is_pivot(prices, previous, width, low) {
                continue;
            }
            let Some(previous_rsi) = rsi
                .get(previous)
                .filter(|v| v.is_finite() && (0.0..=100.0).contains(v))
            else {
                return false;
            };
            let change = if low {
                latest_rsi - previous_rsi
            } else {
                previous_rsi - latest_rsi
            };
            let price_diverged = if low {
                prices.get(latest).unwrap() < prices.get(previous).unwrap()
            } else {
                prices.get(latest).unwrap() > prices.get(previous).unwrap()
            };
            return price_diverged && change > 0.0 && change >= minimum_change;
        }
        false
    }
}

impl AlphaModel for RsiDiverganceAlpha {
    fn version(&self) -> &str {
        "0.3"
    }

    fn start(&mut self, ctx: &mut dyn StrategyContext) {
        self.gate.clear();
        if self.validate().is_err() {
            return;
        }
        ctx.register_indicator(Box::new(AverageTrueRange::new(self.atr_period)));
        ctx.register_indicator(Box::new(RelativeStrengthIndex::new(
            self.rsi_period,
            "close",
        )));
        ctx.set_warm_up_bars(ctx.warm_up_bars().max(self.minimum_history_bars() as i32));
    }

    fn init(&mut self, _ctx: &mut dyn StrategyContext, _asset: &Asset) {}

    fn generate_insights(&mut self, ctx: &mut dyn StrategyContext, symbol: &str) -> AlphaResult {
        if let Err(message) = self.validate() {
            return AlphaResult::new(None, false, Some(message), self.name().into());
        }
        if !self
            .gate
            .allows(ctx, symbol, "RsiDiverganceAlpha", &self.options)
        {
            return AlphaResult::new(None, true, None, self.name().into());
        }
        let Some(asset) = self.get_asset(ctx, symbol) else {
            return AlphaResult::new(
                None,
                false,
                Some(format!("Asset {symbol} not found")),
                self.name().into(),
            );
        };
        if !asset.tradable {
            return AlphaResult::new(None, true, None, self.name().into());
        }
        let minimum_history_bars = self.minimum_history_bars();
        let history = match self.get_history(ctx, symbol) {
            Some(history) if history.height() >= minimum_history_bars => history,
            _ => {
                return AlphaResult::new(
                    None,
                    true,
                    Some(format!(
                        "Not enough history for confirmed RSI divergence (need at least {minimum_history_bars} bars)"
                    )),
                    self.name().into(),
                );
            }
        };
        let build = || -> Result<Option<Insight>, String> {
            let confidence =
                options::confidence(ctx, history, self.base_confidence_modifier_field.as_deref())?;
            let highs = history
                .column("high")
                .map_err(|e| e.to_string())?
                .f64()
                .map_err(|e| e.to_string())?;
            let lows = history
                .column("low")
                .map_err(|e| e.to_string())?
                .f64()
                .map_err(|e| e.to_string())?;
            let rsi = history
                .column(&self.rsi_column)
                .map_err(|e| e.to_string())?
                .f64()
                .map_err(|e| e.to_string())?;
            let latest_atr = options::numeric(history, &self.atr_column, 0)?;
            let latest_close = options::numeric(history, "close", 0)?;
            let high = options::numeric(history, "high", 0)?;
            let low = options::numeric(history, "low", 0)?;
            if latest_atr <= 0.0 || low <= 0.0 || latest_close < low || latest_close > high {
                return Err("Waiting for valid prices and ATR".into());
            }
            let bullish = self.options.allow_long
                && Self::diverges(
                    lows,
                    rsi,
                    self.local_window,
                    self.divergance_window,
                    self.min_rsi_change,
                    true,
                );
            let bearish = self.options.allow_short
                && asset.shortable
                && Self::diverges(
                    highs,
                    rsi,
                    self.local_window,
                    self.divergance_window,
                    self.min_rsi_change,
                    false,
                );
            let (side, price, sign) = if bullish {
                (OrderSide::Buy, high, 1.0)
            } else if bearish {
                (OrderSide::Sell, low, -1.0)
            } else {
                return Ok(None);
            };
            let tools = ctx.tools();
            let entry = tools.dynamic_round(price, symbol);
            let tp = tools.dynamic_round(
                entry + sign * latest_atr * self.options.take_profit_atr,
                symbol,
            );
            let sl = tools.dynamic_round(
                entry - sign * latest_atr * self.options.stop_loss_atr,
                symbol,
            );
            if !options::valid_levels(&side, entry, sl, tp) {
                return Err("ATR levels are invalid after price rounding".into());
            }
            let ttluf = tools
                .calculate_time_to_live(latest_close, entry, latest_atr, 0)
                .max(1) as u32;
            let ttlf = tools
                .calculate_time_to_live(tp, entry, latest_atr, 0)
                .max(1) as u32;
            let mut insight = Insight::new(
                side,
                symbol.to_string(),
                StrategyType::Custom(self.name().to_string()),
                ctx.timeframe().clone(),
                confidence,
                None,
            );
            insight
                .set_limit_price(Some(entry))
                .set_take_profit_levels(Some(vec![tp]))
                .set_stop_loss(Some(sl))
                .set_period_unfilled(Some(self.options.unfilled_ttl(ttluf)))
                .set_period_till_tp(Some(self.options.filled_ttl(ttlf)))
                .set_execution_depends(vec![if self.require_low_relative_volume {
                    StrategyDependentConfirmation::LowRelativeVolumeConfirmationModel
                } else {
                    StrategyDependentConfirmation::None
                }]);
            Ok(Some(insight))
        };
        match build() {
            Ok(insight) => {
                if insight.is_some() {
                    self.gate.emitted(symbol);
                }
                AlphaResult::new(insight, true, None, self.name().into())
            }
            Err(message) => AlphaResult::new(None, true, Some(message), self.name().into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::broker::types::{
        Account, AssetExchange, AssetStatus, AssetType, BrokerError, Quote,
    };
    use crate::core::indicators::Indicator;
    use crate::core::insight::InsightCollection;
    use crate::core::pipeline::WrappedInsightPipe;
    use crate::core::strategy::StrategyMode;
    use crate::core::universe::WrappedUniverseModel;
    use crate::core::utils::timeframe::{TimeFrame, TimeFrameUnit};
    use crate::core::utils::tools::{TradingTools, calculate_time_to_live};
    use dashmap::DashMap;
    use polars::prelude::*;
    use serde_json::Value;
    use std::collections::HashMap;

    struct MockTools {
        round_prices: bool,
    }

    impl TradingTools for MockTools {
        fn dynamic_round(&self, value: f64, _symbol: &str) -> f64 {
            if self.round_prices {
                value.round()
            } else {
                value
            }
        }

        fn quantity_round(&self, value: f64, _symbol: &str) -> f64 {
            value
        }

        fn calculate_time_to_live(&self, price: f64, entry: f64, atr: f64, additional: i32) -> i32 {
            calculate_time_to_live(price, entry, atr, additional)
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
        base_confidence: f64,
        warm_up_bars: i32,
        timeframe: TimeFrame,
        registered: Vec<String>,
        round_prices: bool,
    }

    impl MockContext {
        fn new(symbol: &str, history: DataFrame) -> Self {
            let asset = Asset {
                id: symbol.to_string(),
                symbol: symbol.to_string(),
                name: symbol.to_string(),
                asset_type: AssetType::Crypto,
                status: AssetStatus::Active,
                exchange: AssetExchange::UNKNOWN("TEST".to_string()),
                tradable: true,
                marginable: true,
                shortable: true,
                fractional: true,
                min_order_size: Some(0.001),
                quantity_base: Some(3),
                max_order_size: None,
                max_total_size: None,
                min_price_increment: Some(0.01),
                price_base: Some(2),
                contract_size: None,
                trading_calendar: None,
                fees: Default::default(),
            };
            Self {
                universe: HashMap::from([(symbol.to_string(), asset)]),
                history: HashMap::from([(symbol.to_string(), history)]),
                insights: InsightCollection::new(),
                variables: DashMap::new(),
                base_confidence: 0.7,
                warm_up_bars: 0,
                timeframe: TimeFrame::new(1, TimeFrameUnit::Minute),
                registered: Vec::new(),
                round_prices: false,
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

        fn register_indicator(&mut self, indicator: Box<dyn Indicator>) {
            self.registered.push(indicator.name());
        }

        fn add_alpha(&mut self, _alpha: crate::core::alpha::WrappedAlphaModel) {}

        fn add_pipe(&mut self, _pipe: WrappedInsightPipe) {}

        fn add_universe_model(&mut self, _model: WrappedUniverseModel) {}

        fn set_execution_risk(&mut self, _risk: f64) {}

        fn set_min_reward_risk_ratio(&mut self, _ratio: f64) {}

        fn set_base_confidence(&mut self, confidence: f64) {
            self.base_confidence = confidence;
        }

        fn execution_risk(&self) -> f64 {
            0.01
        }

        fn min_reward_risk_ratio(&self) -> f64 {
            1.0
        }

        fn base_confidence(&self) -> f64 {
            self.base_confidence
        }

        fn variables(&self) -> &DashMap<String, Value> {
            &self.variables
        }

        fn tools(&self) -> Box<dyn TradingTools + '_> {
            Box::new(MockTools {
                round_prices: self.round_prices,
            })
        }

        fn max_history_rows(&self) -> usize {
            2000
        }

        fn set_max_history_rows(&mut self, _rows: usize) {}

        fn warm_up_bars(&self) -> i32 {
            self.warm_up_bars
        }

        fn set_warm_up_bars(&mut self, bars: i32) {
            self.warm_up_bars = bars;
        }

        fn timeframe(&self) -> &TimeFrame {
            &self.timeframe
        }

        fn account(&self) -> Result<Account, BrokerError> {
            unimplemented!("account is not used by RSI divergence alpha tests")
        }

        fn current_time(&self) -> chrono::DateTime<chrono::Utc> {
            chrono::Utc::now()
        }

        fn bind_insight_context(&self, _insight: &mut Insight) {}

        fn latest_quote(&self, _symbol: &str) -> Result<Quote, BrokerError> {
            unimplemented!("latest_quote is not used by RSI divergence alpha tests")
        }

        fn shutdown(&mut self) {}
    }

    include!("builtin_tests.rs");

    fn divergence_history(
        highs: Vec<f64>,
        lows: Vec<f64>,
        closes: Vec<f64>,
        rsi: Vec<Option<f64>>,
    ) -> DataFrame {
        let len = highs.len();
        DataFrame::new(
            len,
            vec![
                Column::new("high".into(), highs),
                Column::new("low".into(), lows),
                Column::new("close".into(), closes),
                Column::new("ATRr_2".into(), vec![Some(1.0); len]),
                Column::new("RSI_2".into(), rsi),
            ],
        )
        .unwrap()
    }

    #[test]
    fn warm_up_covers_rsi_and_confirmed_pivot_window() {
        let mut alpha = RsiDiverganceAlpha::new(36, 50, 14, 14, String::new());
        let mut ctx = MockContext::new(
            "BTCUSD",
            divergence_history(
                vec![1.0; 88],
                vec![1.0; 88],
                vec![1.0; 88],
                vec![Some(50.0); 88],
            ),
        );

        alpha.start(&mut ctx);

        assert_eq!(ctx.warm_up_bars(), 88);
    }

    #[test]
    fn creates_buy_insight_on_confirmed_bullish_divergence() {
        let mut alpha = RsiDiverganceAlpha::new(1, 4, 2, 2, String::new());
        let history = divergence_history(
            vec![11.0, 11.0, 6.0, 4.0, 5.0, 3.0, 4.0],
            vec![10.0, 10.0, 5.0, 3.0, 4.0, 2.0, 3.0],
            vec![10.5, 10.5, 5.5, 3.5, 4.5, 2.5, 3.5],
            vec![
                None,
                None,
                Some(20.0),
                Some(30.0),
                Some(25.0),
                Some(40.0),
                Some(35.0),
            ],
        );
        let mut ctx = MockContext::new("BTCUSD", history);

        let result = alpha.generate_insights(&mut ctx, "BTCUSD");

        let insight = result.insight.expect("expected bullish divergence insight");
        assert!(result.success);
        assert_eq!(insight.side(), &OrderSide::Buy);
    }

    #[test]
    fn creates_sell_insight_on_confirmed_bearish_divergence() {
        let mut alpha = RsiDiverganceAlpha::new(1, 4, 2, 2, String::new());
        let history = divergence_history(
            vec![8.0, 8.0, 3.0, 5.0, 4.0, 6.0, 5.0],
            vec![7.0, 7.0, 2.0, 4.0, 3.0, 5.0, 4.0],
            vec![7.5, 7.5, 2.5, 4.5, 3.5, 5.5, 4.5],
            vec![
                None,
                None,
                Some(80.0),
                Some(70.0),
                Some(75.0),
                Some(60.0),
                Some(65.0),
            ],
        );
        let mut ctx = MockContext::new("BTCUSD", history);

        let result = alpha.generate_insights(&mut ctx, "BTCUSD");

        let insight = result.insight.expect("expected bearish divergence insight");
        assert!(result.success);
        assert_eq!(insight.side(), &OrderSide::Sell);
    }

    #[test]
    fn reports_not_enough_history_until_confirmed_divergence_can_exist() {
        let mut alpha = RsiDiverganceAlpha::new(3, 10, 2, 2, String::new());
        let history = divergence_history(
            vec![10.0; 8],
            vec![9.0; 8],
            vec![9.5; 8],
            vec![
                None,
                None,
                Some(50.0),
                Some(50.0),
                Some(50.0),
                Some(50.0),
                Some(50.0),
                Some(50.0),
            ],
        );
        let mut ctx = MockContext::new("BTCUSD", history);

        let result = alpha.generate_insights(&mut ctx, "BTCUSD");

        assert!(result.success);
        assert!(result.insight.is_none());
        assert_eq!(
            result.message.as_deref(),
            Some("Not enough history for confirmed RSI divergence (need at least 10 bars)")
        );
    }
}
