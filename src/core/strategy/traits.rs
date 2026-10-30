use super::command_driver::BrokerCommandProgress;
use crate::core::alpha::WrappedAlphaModel;
use crate::core::broker::commands::{
    BrokerCommandAction, BrokerCommandError, BrokerCommandReceipt, BrokerOperationGroup,
};
use crate::core::broker::traits::{Broker, DataFeed, DataProvider, OrderManagementProvider};
use crate::core::broker::types::BrokerError;
use crate::core::broker::types::{Account, Asset, BarData, Position, Quote};
use crate::core::broker::{
    BrokerFeature, DataFeedFeature, UnifiedBroker, effective_buying_power_from,
};
use crate::core::events::{EventStreamRequest, EventStreamType, StrategyEventContext};
use crate::core::indicators::Indicator;
use crate::core::insight::{Insight, InsightCollection};
use crate::core::pipeline::WrappedInsightPipe;
use crate::core::strategy::StrategyMode;
use crate::core::universe::WrappedUniverseModel;
use crate::core::utils::timeframe::TimeFrame;
use crate::core::utils::tools::TradingTools;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use polars::prelude::DataFrame;
use serde_json::Value;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Default)]
pub struct TeardownCleanupReport {
    /// Requested work counts; completion requires terminal broker evidence.
    pub rejected_new: usize,
    pub cancelled_executed: usize,
    pub closed_filled: usize,
    pub failures: Vec<String>,
}

impl TeardownCleanupReport {
    pub fn total_actions(&self) -> usize {
        self.rejected_new + self.cancelled_executed + self.closed_filled
    }

    pub fn has_failures(&self) -> bool {
        !self.failures.is_empty()
    }

    pub fn summary(&self) -> String {
        format!(
            "new_cleanup_requested={}, cancel_requested={}, close_requested={}",
            self.rejected_new, self.cancelled_executed, self.closed_filled
        )
    }
}
// ─────────────────────── Object-safe Strategy Context ───────────────────────

/// Core context available to **all** components (Strategy, AlphaModel, InsightPipe).
///
/// Object-safe — can be used as `&mut dyn StrategyContext`.
/// For typed broker access, see the `BrokerAccess` extension trait.
pub trait StrategyContext {
    /// Non-blocking broker mutation admission. A receipt is not execution proof.
    /// Retain it across `Deferred` pipe resumes and inspect `broker_command_progress`.
    fn enqueue_broker_command(
        &mut self,
        _insight_id: uuid::Uuid,
        _action: BrokerCommandAction,
        _group: Option<BrokerOperationGroup>,
    ) -> Result<BrokerCommandReceipt, BrokerCommandError> {
        Err(BrokerCommandError::Closed)
    }

    fn broker_command_progress(
        &self,
        _receipt: &BrokerCommandReceipt,
    ) -> Option<BrokerCommandProgress> {
        None
    }

    /// The operation's owner consumed its completion. This is not a durable
    /// checkpoint acknowledgement; the runtime persists the resulting state.
    fn broker_command_reduced(&mut self, _receipt: &BrokerCommandReceipt) {}

    fn cancel_pending_entry(
        &mut self,
        _receipt: &BrokerCommandReceipt,
    ) -> Result<(), BrokerCommandError> {
        Err(BrokerCommandError::Closed)
    }

    /// Low-level order helpers still return admission receipts, never `bool`.
    /// Insight methods are preferred because they retain/poll these receipts.
    fn cancel_order(&mut self, order_id: &str) -> Result<BrokerCommandReceipt, BrokerCommandError> {
        let id = self
            .insights()
            .insight_id_for_order(order_id)
            .ok_or_else(|| {
                BrokerCommandError::Invalid("order is not tracked by this strategy".into())
            })?;
        self.enqueue_broker_command(
            id,
            BrokerCommandAction::Cancel {
                order_id: order_id.into(),
            },
            None,
        )
    }

    fn close_position(
        &mut self,
        order_id: &str,
        quantity: f64,
        price: Option<f64>,
    ) -> Result<BrokerCommandReceipt, BrokerCommandError> {
        let id = self
            .insights()
            .insight_id_for_order(order_id)
            .ok_or_else(|| {
                BrokerCommandError::Invalid("position is not tracked by this strategy".into())
            })?;
        self.enqueue_broker_command(
            id,
            BrokerCommandAction::Close {
                order_id: order_id.into(),
                quantity,
                price,
            },
            None,
        )
    }

    fn universe(&self) -> &HashMap<String, Asset>;
    fn history(&self) -> &HashMap<String, DataFrame>;
    fn insights(&self) -> &InsightCollection;
    fn mode(&self) -> StrategyMode;
    fn add_insight(&mut self, insight: Insight);

    /// Request cancellation of a canonical pending entry (never an already-filled position).
    /// The runtime owns retries, broker receipts, and any fill/cancel race cleanup.
    fn request_insight_cancellation(&mut self, _id: uuid::Uuid) -> Result<bool, BrokerCommandError> {
        Err(BrokerCommandError::Invalid(
            "Insight cancellation is not supported by this context".into(),
        ))
    }
    fn register_indicator(&mut self, indicator: Box<dyn Indicator>);

    fn add_alpha(&mut self, alpha: WrappedAlphaModel);
    fn add_pipe(&mut self, pipe: WrappedInsightPipe);
    fn add_universe_model(&mut self, model: WrappedUniverseModel);
    fn add_events(&mut self, event_type: EventStreamType, timeframe: Option<TimeFrame>) {
        self.add_events_with_options(EventStreamRequest::new(event_type, timeframe));
    }
    fn add_events_with_options(&mut self, _request: EventStreamRequest) {}
    fn current_event(&self) -> Option<StrategyEventContext> {
        None
    }
    fn set_execution_risk(&mut self, risk: f64);
    fn set_min_reward_risk_ratio(&mut self, ratio: f64);
    fn set_base_confidence(&mut self, confidence: f64);
    fn execution_risk(&self) -> f64;
    fn min_reward_risk_ratio(&self) -> f64;
    fn base_confidence(&self) -> f64;
    fn variables(&self) -> &DashMap<String, Value>;
    fn tools(&self) -> Box<dyn TradingTools + '_>;
    fn max_history_rows(&self) -> usize;
    fn set_max_history_rows(&mut self, rows: usize);

    /// Native execution capabilities exposed by the configured broker.
    fn broker_features(&self) -> &'static [BrokerFeature] {
        &[]
    }

    /// Market-data capabilities exposed by the configured data feed.
    fn datafeed_features(&self) -> &'static [DataFeedFeature] {
        &[]
    }

    fn has_broker_feature(&self, feature: BrokerFeature) -> bool {
        self.broker_features().contains(&feature)
    }

    fn has_broker_features(&self, features: &[BrokerFeature]) -> bool {
        features
            .iter()
            .all(|feature| self.has_broker_feature(*feature))
    }

    /// Return sizing buying power with the configured broker's leverage representation applied.
    ///
    /// This is the preferred helper for strategy and pipe code that already loaded an account
    /// snapshot. It never applies leverage to a non-marginable asset and never double-applies it
    /// when the broker advertises [`BrokerFeature::BuyingPowerLeveraged`].
    fn effective_buying_power(&self, account: &Account, asset: &Asset) -> f64 {
        effective_buying_power_from(
            account,
            asset,
            self.has_broker_feature(BrokerFeature::BuyingPowerLeveraged),
        )
    }

    fn has_datafeed_feature(&self, feature: DataFeedFeature) -> bool {
        self.datafeed_features().contains(&feature)
    }

    fn has_datafeed_features(&self, features: &[DataFeedFeature]) -> bool {
        features
            .iter()
            .all(|feature| self.has_datafeed_feature(*feature))
    }

    /// Get the current warm-up period (number of bars to skip before generating signals).
    fn warm_up_bars(&self) -> i32;
    /// Set the warm-up period. Only raises the value — never lowers it.
    fn set_warm_up_bars(&mut self, bars: i32);
    fn timeframe(&self) -> &TimeFrame;
    /// Transport-free decision snapshot. Missing/stale live data returns a typed
    /// SnapshotUnavailable error; pipes must defer instead of rejecting the insight.
    fn account(&self) -> Result<Account, BrokerError>;
    /// Current individual broker positions, including entry IDs. Reads the
    /// account-scoped snapshot only; stale/missing data is an error, not flat.
    fn get_positions(&self) -> Result<Vec<Position>, BrokerError> {
        Err(BrokerError::SnapshotUnavailable {
            kind: crate::core::broker::snapshots::SnapshotKind::Positions,
            reason: crate::core::broker::snapshots::SnapshotUnavailable::Missing,
        })
    }
    /// Non-blocking broker snapshot for hot-path capacity checks. Never performs I/O.
    fn cached_account(&self) -> Option<Account> {
        None
    }
    fn current_time(&self) -> DateTime<Utc>;
    fn bind_insight_context(&self, insight: &mut Insight);
    /// Transport-free quote for this context's feed and clock. Never fetches a
    /// network quote as a fallback when a simulated/live snapshot is missing.
    fn latest_quote(&self, symbol: &str) -> Result<Quote, BrokerError>;
    fn preseed_warmup_history(
        &mut self,
        symbol: &str,
        warmup_bars: i32,
    ) -> Result<usize, BrokerError> {
        let _ = (symbol, warmup_bars);
        Err(BrokerError::DataFeedError(
            "preseed_warmup_history is not supported by this strategy context".to_string(),
        ))
    }
    fn cleanup_active_insights_for_teardown(&mut self) -> TeardownCleanupReport {
        TeardownCleanupReport::default()
    }

    fn shutdown(&mut self);
}

// ─────────────────────── Generic Broker Extension ───────────────────────

/// Extension trait for code that needs typed access to the broker.
/// Not object-safe (has associated types).
pub trait BrokerAccess<E, D>: StrategyContext
where
    E: Broker + OrderManagementProvider,
    D: DataFeed + DataProvider,
{
    fn broker(&self) -> &UnifiedBroker<E, D>;
    fn broker_mut(&mut self) -> &mut UnifiedBroker<E, D>;
}

// ─────────────────────── Strategy Trait ───────────────────────

pub trait Strategy {
    fn name(&self) -> &str {
        "Base Strategy"
    }

    /// Called once when the strategy starts (Python's `start()`).
    fn on_start(&mut self, ctx: &mut dyn StrategyContext);

    /// Called once per asset during universe loading (Python's `init(asset)`).
    fn init(&mut self, ctx: &mut dyn StrategyContext, asset: &Asset);

    /// Returns the set of symbols this strategy trades (Python's `universe()`).
    fn universe(&self, ctx: &mut dyn StrategyContext) -> HashSet<String>;

    /// Called each time a new bar arrives for a symbol (Python's `on_bar(symbol, bar)`).
    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, symbol: &str, bar: &BarData);

    /// Called after on_bar to generate trading insights (Python's `generateInsights(symbol)`).
    fn generate_insights(&mut self, ctx: &mut dyn StrategyContext, symbol: &str);

    /// Called for each active insight to manage its execution pipeline.
    fn insight_pipeline(&mut self, ctx: &mut dyn StrategyContext, insight: &Insight);

    /// Called once at the end of the strategy lifecycle.
    fn on_teardown(&mut self, ctx: &mut dyn StrategyContext);
}
