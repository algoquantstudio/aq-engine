use crate::core::alpha::{AlphaModel, AlphaResult};
use crate::core::broker::types::{Asset, OrderSide};
use crate::core::indicators::{atr::AverageTrueRange, rsi::RelativeStrengthIndex};
use crate::core::insight::{Insight, types::StrategyDependentConfirmation, types::StrategyType};
use crate::core::strategy::StrategyContext;

/// Generates ATR-managed insights from confirmed RSI price divergence pivots.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - `local_window`: Number of bars on each side required to confirm a local high or low pivot.
/// - `divergance_window`: Maximum number of bars between the latest pivot and the prior pivot.
/// - `atr_period`: Number of bars used by the registered ATR indicator.
/// - `rsi_period`: Number of bars used by the registered RSI indicator on `close`.
/// - `base_confidence_modifier_field`: Optional history column whose latest absolute value
///   scales `ctx.base_confidence()` before it is converted to insight confidence.
///
/// Behaviour:
/// Registers ATR and RSI indicators during `start`, sets warm-up to the minimum history needed
/// for confirmed pivots and indicator readiness, then aligns the RSI series with price highs
/// and lows. A buy insight is emitted on a lower low with higher RSI; a sell insight is emitted
/// only for shortable assets on a higher high with lower RSI. The model uses ATR for entry,
/// stop-loss, take-profit, and time-to-live values, and attaches a low-relative-volume
/// confirmation requirement to the generated insight.
pub struct RsiDiverganceAlpha {
    local_window: usize,
    divergance_window: usize,
    atr_period: usize,
    rsi_period: usize,
    atr_column: String,
    rsi_column: String,
    base_confidence_modifier_field: Option<String>,
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
            atr_column: format!("ATRr_{}", atr_period),
            rsi_column: format!("RSI_{}", rsi_period),
            base_confidence_modifier_field: (!base_confidence_modifier_field.trim().is_empty())
                .then_some(base_confidence_modifier_field),
        }
    }

    fn minimum_history_bars(&self) -> usize {
        let confirmed_pivot_bars = self.local_window.saturating_mul(2).saturating_add(2);
        self.atr_period.max(self.rsi_period + confirmed_pivot_bars)
    }

    fn latest_value(df: &polars::prelude::DataFrame, column: &str) -> Result<f64, String> {
        let idx = df
            .height()
            .checked_sub(1)
            .ok_or_else(|| "Not enough rows".to_string())?;
        df.column(column)
            .map_err(|e| format!("Missing column '{}': {}", column, e))?
            .f64()
            .map_err(|e| format!("Column '{}' is not Float64: {}", column, e))?
            .get(idx)
            .ok_or_else(|| format!("Latest value for '{}' is null", column))
    }

    fn series_values(df: &polars::prelude::DataFrame, column: &str) -> Result<Vec<f64>, String> {
        Ok(df
            .column(column)
            .map_err(|e| format!("Missing column '{}': {}", column, e))?
            .f64()
            .map_err(|e| format!("Column '{}' is not Float64: {}", column, e))?
            .into_iter()
            .flatten()
            .collect())
    }

    fn aligned_indicator_window<'a>(
        base: &'a [f64],
        indicator_column: &polars::prelude::Float64Chunked,
        column_name: &str,
    ) -> Result<(&'a [f64], Vec<f64>), String> {
        let indicator_values: Vec<Option<f64>> = indicator_column.into_iter().collect();
        let first_valid_index = indicator_values
            .iter()
            .position(|value| value.is_some())
            .ok_or_else(|| format!("No values for {}", column_name))?;

        if base.len() < first_valid_index {
            return Err(format!(
                "Column '{}' is misaligned with base series",
                column_name
            ));
        }

        let aligned_base = &base[first_valid_index..];
        let aligned_indicator = indicator_values[first_valid_index..]
            .iter()
            .map(|value| {
                value.ok_or_else(|| {
                    format!(
                        "Unexpected null in aligned indicator series '{}'",
                        column_name
                    )
                })
            })
            .collect::<Result<Vec<f64>, String>>()?;

        Ok((aligned_base, aligned_indicator))
    }

    fn confidence_for_symbol(&self, ctx: &dyn StrategyContext, symbol: &str) -> Result<u8, String> {
        let mut confidence = ctx.base_confidence();
        if let Some(field) = &self.base_confidence_modifier_field {
            let history = ctx
                .history()
                .get(symbol)
                .ok_or_else(|| format!("No history for {}", symbol))?;
            let last = Self::latest_value(history, field)?;
            confidence *= last.abs();
        }
        if confidence <= 0.0 {
            return Err("Base Confidence is 0.".to_string());
        }
        Ok((confidence * 100.0).round().clamp(0.0, 100.0) as u8)
    }

    fn local_low_pivots(values: &[f64], local_window: usize) -> Vec<usize> {
        if values.len() < local_window.saturating_mul(2) + 1 {
            return Vec::new();
        }

        let mut pivots = Vec::new();
        let window_len = local_window.saturating_mul(2) + 1;
        for (offset, window) in values.windows(window_len).enumerate() {
            let current = window[local_window];
            let is_pivot = window
                .iter()
                .enumerate()
                .all(|(idx, value)| idx == local_window || current < *value);
            if is_pivot {
                pivots.push(offset + local_window);
            }
        }
        pivots
    }

    fn local_high_pivots(values: &[f64], local_window: usize) -> Vec<usize> {
        if values.len() < local_window.saturating_mul(2) + 1 {
            return Vec::new();
        }

        let mut pivots = Vec::new();
        let window_len = local_window.saturating_mul(2) + 1;
        for (offset, window) in values.windows(window_len).enumerate() {
            let current = window[local_window];
            let is_pivot = window
                .iter()
                .enumerate()
                .all(|(idx, value)| idx == local_window || current > *value);
            if is_pivot {
                pivots.push(offset + local_window);
            }
        }
        pivots
    }

    fn lower_low_with_higher_rsi(
        lows: &[f64],
        rsi: &[f64],
        local_window: usize,
        divergance_window: usize,
    ) -> bool {
        if lows.len() != rsi.len() || lows.len() < local_window.saturating_mul(2) + 2 {
            return false;
        }

        let pivots = Self::local_low_pivots(lows, local_window);
        if pivots.len() < 2 {
            return false;
        }

        let last = pivots[pivots.len() - 1];
        let last_confirmed_index = lows.len().saturating_sub(local_window + 1);
        let Some(prev) = pivots[..pivots.len() - 1]
            .iter()
            .rev()
            .find(|&&pivot| last.saturating_sub(pivot) <= divergance_window)
            .copied()
        else {
            return false;
        };

        last == last_confirmed_index && lows[last] < lows[prev] && rsi[last] > rsi[prev]
    }

    fn higher_high_with_lower_rsi(
        highs: &[f64],
        rsi: &[f64],
        local_window: usize,
        divergance_window: usize,
    ) -> bool {
        if highs.len() != rsi.len() || highs.len() < local_window.saturating_mul(2) + 2 {
            return false;
        }

        let pivots = Self::local_high_pivots(highs, local_window);
        if pivots.len() < 2 {
            return false;
        }

        let last = pivots[pivots.len() - 1];
        let last_confirmed_index = highs.len().saturating_sub(local_window + 1);
        let Some(prev) = pivots[..pivots.len() - 1]
            .iter()
            .rev()
            .find(|&&pivot| last.saturating_sub(pivot) <= divergance_window)
            .copied()
        else {
            return false;
        };

        last == last_confirmed_index && highs[last] > highs[prev] && rsi[last] < rsi[prev]
    }
}

impl AlphaModel for RsiDiverganceAlpha {
    fn version(&self) -> &str {
        "0.2"
    }

    fn start(&mut self, ctx: &mut dyn StrategyContext) {
        ctx.register_indicator(Box::new(AverageTrueRange::new(self.atr_period)));
        ctx.register_indicator(Box::new(RelativeStrengthIndex::new(
            self.rsi_period,
            "close",
        )));
        ctx.set_warm_up_bars(self.minimum_history_bars() as i32);
    }

    fn init(&mut self, _ctx: &mut dyn StrategyContext, _asset: &Asset) {}

    fn generate_insights(&mut self, ctx: &mut dyn StrategyContext, symbol: &str) -> AlphaResult {
        let Some(asset) = self.get_asset(ctx, symbol) else {
            return AlphaResult::new(
                None,
                false,
                Some(format!("Asset {} not found", symbol)),
                self.name().to_string(),
            );
        };
        let minimum_history_bars = self.minimum_history_bars();
        let history = match self.get_history(ctx, symbol) {
            Some(history) if history.height() >= minimum_history_bars => history,
            _ => {
                return AlphaResult::new(
                    None,
                    true,
                    Some(format!(
                        "Not enough history for confirmed RSI divergence (need at least {} bars)",
                        minimum_history_bars
                    )),
                    self.name().to_string(),
                );
            }
        };
        let confidence = match self.confidence_for_symbol(ctx, symbol) {
            Ok(value) => value,
            Err(message) => {
                return AlphaResult::new(None, true, Some(message), self.name().to_string());
            }
        };

        let highs = match Self::series_values(history, "high") {
            Ok(v) => v,
            Err(e) => return AlphaResult::new(None, false, Some(e), self.name().to_string()),
        };
        let lows = match Self::series_values(history, "low") {
            Ok(v) => v,
            Err(e) => return AlphaResult::new(None, false, Some(e), self.name().to_string()),
        };
        let closes = match Self::series_values(history, "close") {
            Ok(v) => v,
            Err(e) => return AlphaResult::new(None, false, Some(e), self.name().to_string()),
        };
        let latest_atr = match Self::latest_value(history, &self.atr_column) {
            Ok(v) => v,
            Err(e) => return AlphaResult::new(None, false, Some(e), self.name().to_string()),
        };
        let rsi_column = match history.column(&self.rsi_column) {
            Ok(column) => match column.f64() {
                Ok(values) => values,
                Err(e) => {
                    return AlphaResult::new(
                        None,
                        false,
                        Some(format!(
                            "Column '{}' is not Float64: {}",
                            self.rsi_column, e
                        )),
                        self.name().to_string(),
                    );
                }
            },
            Err(e) => {
                return AlphaResult::new(
                    None,
                    false,
                    Some(format!("Missing column '{}': {}", self.rsi_column, e)),
                    self.name().to_string(),
                );
            }
        };
        let (aligned_lows, rsi_values) =
            match Self::aligned_indicator_window(&lows, rsi_column, &self.rsi_column) {
                Ok(values) => values,
                Err(e) => return AlphaResult::new(None, true, Some(e), self.name().to_string()),
            };
        let (aligned_highs, aligned_rsi_for_highs) =
            match Self::aligned_indicator_window(&highs, rsi_column, &self.rsi_column) {
                Ok(values) => values,
                Err(e) => return AlphaResult::new(None, true, Some(e), self.name().to_string()),
            };

        let latest_close = *closes.last().unwrap_or(&0.0);
        if latest_atr <= 0.0 {
            return AlphaResult::new(
                None,
                true,
                Some("ATR is not ready".to_string()),
                self.name().to_string(),
            );
        }

        let bullish = Self::lower_low_with_higher_rsi(
            aligned_lows,
            &rsi_values,
            self.local_window,
            self.divergance_window,
        );
        let bearish = asset.shortable
            && Self::higher_high_with_lower_rsi(
                aligned_highs,
                &aligned_rsi_for_highs,
                self.local_window,
                self.divergance_window,
            );

        let (side, entry, tp, sl) = if bullish {
            let entry = ctx
                .tools()
                .dynamic_round(*highs.last().unwrap_or(&latest_close), symbol);
            (
                OrderSide::Buy,
                entry,
                ctx.tools().dynamic_round(entry + latest_atr * 3.5, symbol),
                ctx.tools().dynamic_round(entry - latest_atr * 1.5, symbol),
            )
        } else if bearish {
            let entry = ctx
                .tools()
                .dynamic_round(*lows.last().unwrap_or(&latest_close), symbol);
            (
                OrderSide::Sell,
                entry,
                ctx.tools().dynamic_round(entry - latest_atr * 3.5, symbol),
                ctx.tools().dynamic_round(entry + latest_atr * 1.5, symbol),
            )
        } else {
            return AlphaResult::new(None, true, None, self.name().to_string());
        };

        let ttluf = ctx
            .tools()
            .calculate_time_to_live(latest_close, entry, latest_atr, 0)
            .max(1);
        let ttlf = ctx
            .tools()
            .calculate_time_to_live(tp, entry, latest_atr, 0)
            .max(1);
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
            .set_period_unfilled(Some(ttluf as u32))
            .set_period_till_tp(Some(ttlf as u32))
            .set_execution_depends(vec![
                StrategyDependentConfirmation::LowRelativeVolumeConfirmationModel,
            ]);
        AlphaResult::new(Some(insight), true, None, self.name().to_string())
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

    struct MockTools;

    impl TradingTools for MockTools {
        fn dynamic_round(&self, value: f64, _symbol: &str) -> f64 {
            value
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
                min_price_increment: Some(0.01),
                price_base: Some(2),
                contract_size: None,
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
            Box::new(MockTools)
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

    fn divergence_history(
        highs: Vec<f64>,
        lows: Vec<f64>,
        closes: Vec<f64>,
        rsi: Vec<Option<f64>>,
    ) -> DataFrame {
        let len = highs.len();
        DataFrame::new(vec![
            Column::new("high".into(), highs),
            Column::new("low".into(), lows),
            Column::new("close".into(), closes),
            Column::new("ATRr_2".into(), vec![Some(1.0); len]),
            Column::new("RSI_2".into(), rsi),
        ])
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
