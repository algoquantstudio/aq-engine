use crate::core::insight::Insight;
use crate::core::pipeline::InsightPipeResult;
use crate::core::strategy::StrategyContext;

use super::InsightPipe;

/// Configures insight time-to-live periods for unfilled and take-profit windows.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - `period_unfilled`: Number of strategy timeframe periods an unfilled insight can remain
///   active. Values below `1` clear the setting.
/// - `period_till_tp`: Number of strategy timeframe periods a filled insight can remain open
///   while waiting for take profit. Values below `1` clear the setting.
///
/// Behaviour:
/// Writes the configured periods with `Insight::set_period_unfilled` and
/// `Insight::set_period_till_tp`. Use `0` to unset either field and let the insight run without
/// that TTL constraint.
pub struct InsightTtlConfigPipe {
    period_unfilled: Option<u32>,
    period_till_tp: Option<u32>,
}

impl InsightTtlConfigPipe {
    pub fn new(period_unfilled: i64, period_till_tp: i64) -> Self {
        Self {
            period_unfilled: Self::normalise_period(period_unfilled),
            period_till_tp: Self::normalise_period(period_till_tp),
        }
    }

    fn normalise_period(value: i64) -> Option<u32> {
        u32::try_from(value).ok().filter(|period| *period > 0)
    }
}

impl InsightPipe for InsightTtlConfigPipe {
    fn version(&self) -> &str {
        "1.0"
    }

    fn run(&mut self, _ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        insight
            .set_period_unfilled(self.period_unfilled)
            .set_period_till_tp(self.period_till_tp);

        InsightPipeResult::new(
            true,
            true,
            Some(format!(
                "Insight TTL configured: period_unfilled={:?}, period_till_tp={:?}",
                self.period_unfilled, self.period_till_tp
            )),
            self.name().to_string(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::alpha::WrappedAlphaModel;
    use crate::core::broker::types::OrderSide;
    use crate::core::broker::types::{Account, AccountType, Asset, BrokerError, Quote};
    use crate::core::indicators::Indicator;
    use crate::core::insight::InsightCollection;
    use crate::core::insight::types::StrategyType;
    use crate::core::pipeline::WrappedInsightPipe;
    use crate::core::strategy::{StrategyContext, StrategyMode, TeardownCleanupReport};
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
        timeframe: TimeFrame,
    }

    impl MockContext {
        fn new() -> Self {
            Self {
                universe: HashMap::new(),
                history: HashMap::new(),
                insights: InsightCollection::new(),
                variables: DashMap::new(),
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
            Ok(Account {
                account_id: "paper".to_string(),
                account_type: AccountType::Paper,
                equity: 10_000.0,
                cash: 10_000.0,
                currency: "USD".to_string(),
                buying_power: 10_000.0,
                shorting_enabled: true,
                leverage: 1,
            })
        }

        fn current_time(&self) -> chrono::DateTime<Utc> {
            Utc::now()
        }

        fn bind_insight_context(&self, _insight: &mut Insight) {}

        fn latest_quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
            Err(BrokerError::DataFeedError(format!(
                "No quote available for {}",
                symbol
            )))
        }

        fn cleanup_active_insights_for_teardown(&mut self) -> TeardownCleanupReport {
            TeardownCleanupReport::default()
        }

        fn cancel_order(&self, _order_id: &str) -> Result<bool, BrokerError> {
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

    #[test]
    fn sets_period_unfilled_and_period_till_tp() {
        let mut pipe = InsightTtlConfigPipe::new(5, 10);
        let mut insight = Insight::new(
            OrderSide::Buy,
            "AAPL".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            80,
            None,
        );
        let mut ctx = MockContext::new();

        let result = pipe.run(&mut ctx, &mut insight);

        assert!(result.passed);
        assert_eq!(insight.period_unfilled(), Some(5));
        assert_eq!(insight.period_till_tp(), Some(10));
    }

    #[test]
    fn zero_or_negative_periods_clear_ttl_fields() {
        let mut pipe = InsightTtlConfigPipe::new(0, -1);
        let mut insight = Insight::new(
            OrderSide::Buy,
            "AAPL".to_string(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            80,
            None,
        );
        insight
            .set_period_unfilled(Some(5))
            .set_period_till_tp(Some(10));
        let mut ctx = MockContext::new();

        let result = pipe.run(&mut ctx, &mut insight);

        assert!(result.passed);
        assert_eq!(insight.period_unfilled(), None);
        assert_eq!(insight.period_till_tp(), None);
    }
}
