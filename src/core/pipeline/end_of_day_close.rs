use crate::core::insight::Insight;
use crate::core::insight::types::InsightState;
use crate::core::pipeline::InsightPipeResult;
use crate::core::strategy::StrategyContext;
use chrono::Timelike;

use super::InsightPipe;

const DEFAULT_MINUTES_BEFORE_END_OF_DAY: i64 = 15;
const DEFAULT_END_OF_DAY_TIME_UTC: &str = "23:45";
const MINUTES_PER_DAY: i64 = 24 * 60;

/// Closes filled insights during a configurable UTC end-of-day close window.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - `minutes_before_end_of_day`: Number of minutes before the configured UTC end-of-day time
///   when filled insights should start closing. Values below `1` use the built-in default.
/// - `end_of_day_time_utc`: UTC end-of-day time in `HH:MM` or `HHMM` format. Invalid or empty
///   values use the built-in default.
///
/// Behaviour:
/// Runs only on filled insights. The pipe normalises its configuration, compares
/// `ctx.current_time()` with the close window, and calls `insight.close(ctx)` once the current UTC
/// minute falls inside that window. The window supports midnight wraparound, so an end-of-day
/// time near midnight can still close positions before and after `00:00`.
pub struct EndOfDayClosePipe {
    pub minutes_before_end_of_day: i64,
    pub end_of_day_time_utc: String,
}

impl EndOfDayClosePipe {
    pub fn new(minutes_before_end_of_day: i64, end_of_day_time_utc: String) -> Self {
        Self {
            minutes_before_end_of_day: Self::normalise_positive_minutes(
                minutes_before_end_of_day,
                DEFAULT_MINUTES_BEFORE_END_OF_DAY,
            ),
            end_of_day_time_utc: Self::normalise_utc_time(&end_of_day_time_utc),
        }
    }

    fn normalise_positive_minutes(value: i64, default_value: i64) -> i64 {
        if value > 0 { value } else { default_value }
    }

    fn normalise_utc_time(value: &str) -> String {
        Self::parse_utc_time_minutes(value)
            .map(Self::minutes_to_time)
            .unwrap_or_else(|| DEFAULT_END_OF_DAY_TIME_UTC.to_string())
    }

    fn parse_utc_time_minutes(value: &str) -> Option<i64> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return None;
        }

        let (hour, minute) = if let Some((hour, minute)) = trimmed.split_once(':') {
            (hour.trim(), minute.trim())
        } else if trimmed.len() == 4 {
            (&trimmed[0..2], &trimmed[2..4])
        } else {
            return None;
        };

        let hour = hour.parse::<i64>().ok()?;
        let minute = minute.parse::<i64>().ok()?;
        if !(0..=23).contains(&hour) || !(0..=59).contains(&minute) {
            return None;
        }

        Some(hour * 60 + minute)
    }

    fn minutes_to_time(minutes: i64) -> String {
        let minutes = minutes.clamp(0, MINUTES_PER_DAY - 1);
        format!("{:02}:{:02}", minutes / 60, minutes % 60)
    }

    fn end_of_day_minutes(&self) -> i64 {
        Self::parse_utc_time_minutes(&self.end_of_day_time_utc).unwrap_or_else(|| {
            Self::parse_utc_time_minutes(DEFAULT_END_OF_DAY_TIME_UTC)
                .expect("default UTC end-of-day time is valid")
        })
    }

    fn close_window_start_minutes(&self) -> i64 {
        (self.end_of_day_minutes() - self.minutes_before_end_of_day).rem_euclid(MINUTES_PER_DAY)
    }

    fn is_close_window(&self, current_minutes: i64) -> bool {
        let start = self.close_window_start_minutes();
        let end = self.end_of_day_minutes();

        if start <= end {
            current_minutes >= start && current_minutes <= end
        } else {
            current_minutes >= start || current_minutes <= end
        }
    }
}

impl InsightPipe for EndOfDayClosePipe {
    fn version(&self) -> &str {
        "1.0"
    }

    fn run(&mut self, ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        if insight.state() != &InsightState::Filled {
            return InsightPipeResult::new(true, true, None, self.name().to_string());
        }

        if insight.closing {
            return InsightPipeResult::new(
                false,
                true,
                Some("Insight is already being closed".to_string()),
                self.name().to_string(),
            );
        }

        self.minutes_before_end_of_day = Self::normalise_positive_minutes(
            self.minutes_before_end_of_day,
            DEFAULT_MINUTES_BEFORE_END_OF_DAY,
        );
        self.end_of_day_time_utc = Self::normalise_utc_time(&self.end_of_day_time_utc);

        let now = ctx.current_time();
        let current_minutes = i64::from(now.hour() * 60 + now.minute());
        if !self.is_close_window(current_minutes) {
            return InsightPipeResult::new(true, true, None, self.name().to_string());
        }

        insight.close(ctx);
        InsightPipeResult::new(
            false,
            true,
            Some(format!(
                "Closing {} before configured end of day {} UTC",
                insight.symbol, self.end_of_day_time_utc
            )),
            self.name().to_string(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::alpha::WrappedAlphaModel;
    use crate::core::broker::types::{
        Account, AccountType, Asset, AssetExchange, AssetStatus, AssetType, BrokerError, OrderSide,
        Quote,
    };
    use crate::core::indicators::Indicator;
    use crate::core::insight::InsightCollection;
    use crate::core::insight::types::StrategyType;
    use crate::core::pipeline::WrappedInsightPipe;
    use crate::core::strategy::{StrategyContext, StrategyMode};
    use crate::core::universe::WrappedUniverseModel;
    use crate::core::utils::timeframe::{TimeFrame, TimeFrameUnit};
    use crate::core::utils::tools::TradingTools;
    use chrono::{TimeZone, Utc};
    use dashmap::DashMap;
    use polars::prelude::DataFrame;
    use serde_json::Value;
    use std::cell::RefCell;
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
        current_time: chrono::DateTime<Utc>,
        closed_positions: RefCell<Vec<(String, f64, Option<f64>)>>,
    }

    impl MockContext {
        fn new(current_time: chrono::DateTime<Utc>) -> Self {
            let asset = Asset {
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
                min_order_size: Some(0.001),
                quantity_base: Some(3),
                max_order_size: None,
                min_price_increment: Some(0.01),
                price_base: Some(2),
                contract_size: None,
            };

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
                current_time,
                closed_positions: RefCell::new(Vec::new()),
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

        fn current_time(&self) -> chrono::DateTime<Utc> {
            self.current_time
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
                timestamp: self.current_time,
            })
        }

        fn cancel_order(&self, _order_id: &str) -> Result<bool, BrokerError> {
            Ok(true)
        }

        fn update_order(
            &self,
            _order_id: &str,
            _price: f64,
            _qty: f64,
        ) -> Result<bool, BrokerError> {
            Ok(true)
        }

        fn update_stop_loss_order(
            &self,
            _order_id: &str,
            _price: f64,
            _qty: f64,
        ) -> Result<bool, BrokerError> {
            Ok(true)
        }

        fn close_position(
            &self,
            order_id: &str,
            qty: f64,
            price: Option<f64>,
        ) -> Result<bool, BrokerError> {
            self.closed_positions
                .borrow_mut()
                .push((order_id.to_string(), qty, price));
            Ok(true)
        }

        fn shutdown(&mut self) {}
    }

    fn filled_insight() -> Insight {
        let mut insight = Insight::new(
            OrderSide::Buy,
            "BTCUSD".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            80,
            None,
        );
        insight.state = InsightState::Filled;
        insight.order_id = Some("entry-order-1".to_string());
        insight.set_quantity(Some(2.5));
        insight
    }

    #[test]
    fn normalises_invalid_inputs_to_defaults() {
        let pipe = EndOfDayClosePipe::new(0, "bad-time".to_string());

        assert_eq!(
            pipe.minutes_before_end_of_day,
            DEFAULT_MINUTES_BEFORE_END_OF_DAY
        );
        assert_eq!(pipe.end_of_day_time_utc, DEFAULT_END_OF_DAY_TIME_UTC);
    }

    #[test]
    fn supports_hhmm_time_format() {
        let pipe = EndOfDayClosePipe::new(10, "2345".to_string());

        assert_eq!(pipe.end_of_day_time_utc, "23:45");
    }

    #[test]
    fn closes_filled_insight_inside_window() {
        let mut ctx = MockContext::new(Utc.with_ymd_and_hms(2026, 6, 8, 23, 40, 0).unwrap());
        let mut pipe = EndOfDayClosePipe::new(15, "23:45".to_string());
        let mut insight = filled_insight();

        let result = pipe.run(&mut ctx, &mut insight);

        assert!(!result.passed);
        assert!(result.success);
        assert!(insight.closing);
        assert_eq!(
            ctx.closed_positions.borrow().as_slice(),
            &[("entry-order-1".to_string(), 2.5, None)]
        );
    }

    #[test]
    fn passes_filled_insight_outside_window() {
        let mut ctx = MockContext::new(Utc.with_ymd_and_hms(2026, 6, 8, 23, 20, 0).unwrap());
        let mut pipe = EndOfDayClosePipe::new(15, "23:45".to_string());
        let mut insight = filled_insight();

        let result = pipe.run(&mut ctx, &mut insight);

        assert!(result.passed);
        assert!(result.success);
        assert!(!insight.closing);
        assert!(ctx.closed_positions.borrow().is_empty());
    }

    #[test]
    fn supports_midnight_wrapped_close_window() {
        let mut ctx = MockContext::new(Utc.with_ymd_and_hms(2026, 6, 9, 0, 3, 0).unwrap());
        let mut pipe = EndOfDayClosePipe::new(10, "00:05".to_string());
        let mut insight = filled_insight();

        let result = pipe.run(&mut ctx, &mut insight);

        assert!(!result.passed);
        assert!(result.success);
        assert_eq!(ctx.closed_positions.borrow().len(), 1);
    }
}
