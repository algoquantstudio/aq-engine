//! Common imports for AQE strategy entrypoints.
//!
//! Use `use aq_engine::prelude::*;` in a strategy's generated `main.rs`. Built-in alpha models
//! and insight pipes are intentionally omitted: code generation imports those individually only
//! when the strategy graph uses them.

pub use chrono::Utc;
pub use log::{debug, info};
pub use std::collections::HashSet;
pub use uuid::Uuid;

pub use crate::core::alpha::{AlphaModel, AlphaModelBuilder, AlphaResult};
pub use crate::core::broker::{
    UnifiedBroker,
    data_feeds::{mt5::Mt5DataFeed, yahoo::YahooFinanceDataFeed},
    mt5_broker::Mt5Broker,
    paper_broker::PaperBroker,
    types::{
        AccountType, Asset, AssetCommissionFees, AssetFee, AssetFees, AssetSideFees, AssetSwapFees,
        BarData,
    },
};
pub use crate::core::insight::{Insight, InsightCollection, types::InsightState};
pub use crate::core::lifecycle::{
    LifecycleTiming, OnInitLogicBuilder, OnStartLogicBuilder, OnTeardownLogicBuilder,
};
pub use crate::core::pipeline::{InsightPipe, InsightPipeBuilder, InsightPipeResult};
pub use crate::core::strategy::{
    EventStreamRequest, EventStreamType, Strategy, StrategyContext, StrategyMode, StrategyState,
    set_logging_level, traits::BrokerAccess,
};
pub use crate::core::universe::UniverseModelBuilder;
pub use crate::core::utils::timeframe::{TimeFrame, TimeFrameUnit};
