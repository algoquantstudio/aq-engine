#[cfg(test)]
mod tests;
use crate::core::alpha::WrappedAlphaModel;
use crate::core::broker::backtest_state::BacktestResults;
use crate::core::broker::paper_broker::PaperBroker;
use crate::core::broker::types::{Account, Asset, BarData, BrokerError, Order, TradeUpdateEvent};
use crate::core::indicators::Indicator;
use crate::core::insight::types::InsightState;
use crate::core::insight::{Insight, InsightCollection, InsightSnapshot, InsightStrategyContext};
use crate::core::pipeline::WrappedInsightPipe;
use crate::core::utils::tools::{StrategyTools, TradingTools};
use dashmap::DashMap;
use polars::prelude::*;
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::vec;
use uuid::Uuid;
mod aqs_sync;
mod aqs_types;
mod live_metrics;
mod types;
pub use types::{InsightPipeline, StrategyMode, StrategyStatus};
pub mod traits;
use crate::core::broker::traits::{Broker, DataFeed, DataProvider, OrderManagementProvider};
use crate::core::broker::{DataStreamMode, UnifiedBroker};
use crate::core::utils::timeframe::TimeFrame;
pub use traits::{BrokerAccess, Strategy, StrategyContext};

use aqs_sync::{
    mark_strategy_started, persist_live_account_state, persist_live_metrics,
    persist_strategy_event, update_strategy_action_status,
};
pub use aqs_types::AqsAuth;
use aqs_types::{StrategyEventRecord, StrategyUniverseAssetRecord, action_id_from_value};
use futures::StreamExt;
use live_metrics::LivePerformanceTracker;
use log::{debug, error, info, warn};
use std::sync::Arc;
use std::time::Duration;
use surrealdb::IndexedResults;
use surrealdb::Notification;
use surrealdb::engine::any;
use surrealdb::method::QueryStream;
use surrealdb::opt::auth::Record;

type StrategyActionStream = QueryStream<Notification<surrealdb::types::Value>>;

const MAX_PENDING_AQS_SYNC_OPS: usize = 512;
const LIVE_SYNC_CONNECT_MAX_ATTEMPTS: usize = 5;
const LIVE_SYNC_RETRY_BASE_MS: u64 = 500;
const LIVE_SYNC_CONNECT_TIMEOUT_SECS: u64 = 10;
const LIVE_SYNC_STREAM_TIMEOUT_SECS: u64 = 10;
const LIVE_SYNC_QUERY_TIMEOUT_SECS: u64 = 10;
const LIVE_SYNC_RECONCILE_SECS: u64 = 60;
const LIVE_SYNC_RECONNECT_TIMEOUT_SECS: u64 = 30;

#[derive(Clone)]
enum PendingAqsSyncOp {
    StrategyStarted {
        universe: Vec<StrategyUniverseAssetRecord>,
        account: Option<Account>,
    },
    AccountState {
        account: Account,
        captured_at: chrono::DateTime<chrono::Utc>,
    },
    StrategyEvent(StrategyEventRecord),
    LiveMetrics(aqs_types::StrategyLiveMetricsRecord),
}

// ─────────────────────── StrategyState ───────────────────────

pub struct StrategyState<S, E, D>
where
    S: Strategy,
    E: Broker + OrderManagementProvider,
    D: DataFeed + DataProvider,
{
    pub strategy_id: Uuid,
    pub name: String,
    pub version: String,

    /// Wrapped in Option to enable split-borrow during backtest loop.
    /// Always `Some` outside of active backtest iteration.
    strategy: Option<S>,
    pub timeframe: TimeFrame,
    pub insights: InsightCollection,
    pub history: HashMap<String, DataFrame>,
    pub universe: HashMap<String, Asset>,
    pub broker: UnifiedBroker<E, D>,

    //
    pub status: StrategyStatus,
    pub mode: StrategyMode,

    // Alpha Models
    alpha_models: Vec<WrappedAlphaModel>,

    // Indicators
    pub indicators: HashMap<String, Box<dyn Indicator>>,

    // Risk Management Parameters
    warm_up_bars: i32,
    pub execution_risk: f64,
    pub min_reward_risk_ratio: f64,
    pub base_confidence: f64,
    pub variables: DashMap<String, Value>,
    pub max_history_rows: usize,
    live_metrics: LivePerformanceTracker,

    // Insight Pipeline
    pub insight_pipeline: InsightPipeline,

    // Internal Shutdown
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl<S, E, D> StrategyState<S, E, D>
where
    S: Strategy,
    E: Broker + OrderManagementProvider,
    D: DataFeed + DataProvider,
{
    fn live_session_key_for_auth(auth: &AqsAuth) -> String {
        auth.live_session_id
            .as_deref()
            .map(aqs_types::live_session_key)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| auth.session_id.clone())
    }

    fn live_insight_record_id(auth: &AqsAuth, insight_id: &str) -> String {
        format!(
            "{}::{}::{}",
            auth.strategy_id,
            Self::live_session_key_for_auth(auth),
            insight_id
        )
    }

    fn is_transient_surreal_error(error: &surrealdb::Error) -> bool {
        let message = error.to_string().to_lowercase();
        message.contains("connection reset")
            || message.contains("connection closed")
            || message.contains("broken pipe")
            || message.contains("timed out")
            || message.contains("timeout")
            || message.contains("transport")
            || message.contains("503")
            || message.contains("service unavailable")
            || message.contains("temporarily unavailable")
            || message.contains("websocket error")
            || message.contains("http error")
    }

    async fn connect_live_sync_db(auth: &AqsAuth) -> Option<surrealdb::Surreal<any::Any>> {
        let signin_session_id = auth.session_id.clone();
        info!(
            "Authenticating AQS Cloud live sync with session id: {}",
            signin_session_id
        );
        for attempt in 1..=LIVE_SYNC_CONNECT_MAX_ATTEMPTS {
            let connect_result = tokio::time::timeout(
                Duration::from_secs(LIVE_SYNC_CONNECT_TIMEOUT_SECS),
                any::connect(auth.url()),
            )
            .await;

            match connect_result {
                Err(_) => {
                    let message = format!(
                        "Timed out connecting to AQS Cloud after {}s",
                        LIVE_SYNC_CONNECT_TIMEOUT_SECS
                    );
                    if attempt < LIVE_SYNC_CONNECT_MAX_ATTEMPTS {
                        warn!(
                            "Transient AQS live sync connection error for strategy {} on attempt {}/{}: {}",
                            auth.strategy_id, attempt, LIVE_SYNC_CONNECT_MAX_ATTEMPTS, message
                        );
                        tokio::time::sleep(Duration::from_millis(
                            LIVE_SYNC_RETRY_BASE_MS * attempt as u64,
                        ))
                        .await;
                        continue;
                    }
                    error!("{}", message);
                    return None;
                }
                Ok(Err(error)) => {
                    if Self::is_transient_surreal_error(&error)
                        && attempt < LIVE_SYNC_CONNECT_MAX_ATTEMPTS
                    {
                        warn!(
                            "Transient AQS live sync connection error for strategy {} on attempt {}/{}: {}",
                            auth.strategy_id, attempt, LIVE_SYNC_CONNECT_MAX_ATTEMPTS, error
                        );
                        tokio::time::sleep(Duration::from_millis(
                            LIVE_SYNC_RETRY_BASE_MS * attempt as u64,
                        ))
                        .await;
                        continue;
                    }
                    error!("Failed to connect to AQS Cloud: {}", error);
                    return None;
                }
                Ok(Ok(client)) => {
                    debug!(
                        "Connected transport for AQS live sync on strategy {}; authenticating session",
                        auth.strategy_id
                    );
                    let signin_result = tokio::time::timeout(
                        Duration::from_secs(LIVE_SYNC_CONNECT_TIMEOUT_SECS),
                        async {
                            client
                                .signin(Record {
                                    namespace: "aqs".to_string(),
                                    database: "aqs".to_string(),
                                    access: auth.access_method.clone(),
                                    params: std::collections::BTreeMap::from([
                                        ("session_id".to_string(), signin_session_id.clone()),
                                        ("secret".to_string(), auth.session_secret.clone()),
                                    ]),
                                })
                                .await
                                .map_err(|e| {
                                    format!("Failed to authenticate AQS Cloud live sync: {}", e)
                                })?;
                            client.use_ns("aqs").use_db("aqs").await.map_err(|e| {
                                format!("Failed to select AQS database for live sync: {}", e)
                            })?;
                            Ok::<(), String>(())
                        },
                    )
                    .await;

                    match signin_result {
                        Ok(Ok(())) => {
                            debug!(
                                "AQS live sync authenticated for strategy {}; marking session active",
                                auth.strategy_id
                            );
                            let activate_session_result = tokio::time::timeout(
                                Duration::from_secs(LIVE_SYNC_QUERY_TIMEOUT_SECS),
                                client
                                    .query(
                                        "UPDATE type::record('live_strategy_session', <uuid>$live_session_key)
                                         SET status = 'active',
                                             last_used_at = time::now()",
                                    )
                                    .bind(("live_session_key", Self::live_session_key_for_auth(auth))),
                            )
                            .await;

                            match activate_session_result {
                                Ok(Ok(response)) => {
                                    if let Err(error) = response.check() {
                                        warn!(
                                            "Failed to mark AQS live session active for strategy {}: {}",
                                            auth.strategy_id, error
                                        );
                                    }
                                }
                                Ok(Err(error)) => {
                                    warn!(
                                        "Failed to update AQS live session status for strategy {}: {}",
                                        auth.strategy_id, error
                                    );
                                }
                                Err(_) => {
                                    warn!(
                                        "Timed out marking AQS live session active for strategy {} after {}s",
                                        auth.strategy_id, LIVE_SYNC_QUERY_TIMEOUT_SECS
                                    );
                                }
                            }

                            info!("Connected to AQS Cloud for live sync");
                            let lifecycle_event = StrategyEventRecord {
                                event_type: "lifecycle".into(),
                                level: "info".into(),
                                title: "AQS live sync connected".into(),
                                message: "AQE authenticated and connected to AQS Cloud".into(),
                                payload: None,
                                created_at: Some(chrono::Utc::now()),
                            };
                            match tokio::time::timeout(
                                Duration::from_secs(LIVE_SYNC_QUERY_TIMEOUT_SECS),
                                persist_strategy_event(&client, auth, lifecycle_event),
                            )
                            .await
                            {
                                Ok(Ok(())) => {}
                                Ok(Err(error)) => {
                                    warn!(
                                        "Failed to persist AQS live sync lifecycle event for strategy {}: {}",
                                        auth.strategy_id, error
                                    );
                                }
                                Err(_) => {
                                    warn!(
                                        "Timed out persisting AQS live sync lifecycle event for strategy {} after {}s",
                                        auth.strategy_id, LIVE_SYNC_QUERY_TIMEOUT_SECS
                                    );
                                }
                            }
                            return Some(client);
                        }
                        Ok(Err(error)) => {
                            let transient = error.contains("503")
                                || error.contains("service unavailable")
                                || error.contains("connection reset")
                                || error.contains("connection closed")
                                || error.contains("timeout")
                                || error.contains("timed out")
                                || error.contains("transport")
                                || error.contains("websocket error")
                                || error.contains("http error");
                            if transient && attempt < LIVE_SYNC_CONNECT_MAX_ATTEMPTS {
                                warn!(
                                    "Transient AQS live sync authentication error for strategy {} on attempt {}/{}: {}",
                                    auth.strategy_id,
                                    attempt,
                                    LIVE_SYNC_CONNECT_MAX_ATTEMPTS,
                                    error
                                );
                                tokio::time::sleep(Duration::from_millis(
                                    LIVE_SYNC_RETRY_BASE_MS * attempt as u64,
                                ))
                                .await;
                                continue;
                            }
                            error!("{}", error);
                            return None;
                        }
                        Err(_) => {
                            let message = format!(
                                "Timed out authenticating AQS Cloud live sync after {}s",
                                LIVE_SYNC_CONNECT_TIMEOUT_SECS
                            );
                            if attempt < LIVE_SYNC_CONNECT_MAX_ATTEMPTS {
                                warn!(
                                    "Transient AQS live sync authentication error for strategy {} on attempt {}/{}: {}",
                                    auth.strategy_id,
                                    attempt,
                                    LIVE_SYNC_CONNECT_MAX_ATTEMPTS,
                                    message
                                );
                                tokio::time::sleep(Duration::from_millis(
                                    LIVE_SYNC_RETRY_BASE_MS * attempt as u64,
                                ))
                                .await;
                                continue;
                            }
                            error!("{}", message);
                            return None;
                        }
                    }
                }
            }
        }

        None
    }

    async fn create_strategy_action_stream(
        client: &surrealdb::Surreal<any::Any>,
        auth: &AqsAuth,
    ) -> Option<StrategyActionStream> {
        debug!(
            "Creating AQS strategy action stream for strategy {}",
            auth.strategy_id
        );
        let live_query = tokio::time::timeout(
            Duration::from_secs(LIVE_SYNC_STREAM_TIMEOUT_SECS),
            client
                .query(
                    "LIVE SELECT * FROM strategy_actions
                     WHERE strategy_id = type::record('strategy', $strategy_id)
                       AND live_session_id = type::record('live_strategy_session', <uuid>$live_session_key)
                       AND status = 'pending'",
                )
                .bind(("strategy_id", auth.strategy_id.clone()))
                .bind(("live_session_key", Self::live_session_key_for_auth(auth))),
        )
        .await;

        match live_query {
            Ok(Ok(mut results)) => match results.stream::<Notification<surrealdb::types::Value>>(0)
            {
                Ok(stream) => {
                    debug!(
                        "AQS strategy action stream ready for strategy {}",
                        auth.strategy_id
                    );
                    Some(stream)
                }
                Err(error) => {
                    warn!(
                        "Failed to materialize AQS strategy action stream for strategy {}: {}",
                        auth.strategy_id, error
                    );
                    None
                }
            },
            Ok(Err(error)) => {
                warn!(
                    "Failed to create AQS strategy action stream for strategy {}: {}",
                    auth.strategy_id, error
                );
                None
            }
            Err(_) => {
                warn!(
                    "Timed out creating AQS strategy action stream for strategy {} after {}s",
                    auth.strategy_id, LIVE_SYNC_STREAM_TIMEOUT_SECS
                );
                None
            }
        }
    }

    async fn reconnect_live_sync(
        db: &mut Option<surrealdb::Surreal<any::Any>>,
        action_stream: &mut Option<StrategyActionStream>,
        auth: &AqsAuth,
    ) {
        warn!(
            "Reconnecting AQE live sync to AQS for strategy {}",
            auth.strategy_id
        );
        let reconnect_result = tokio::time::timeout(
            Duration::from_secs(LIVE_SYNC_RECONNECT_TIMEOUT_SECS),
            async {
                let next_db = Self::connect_live_sync_db(auth).await;
                let next_action_stream = match next_db.as_ref() {
                    Some(client) => Self::create_strategy_action_stream(client, auth).await,
                    None => None,
                };
                (next_db, next_action_stream)
            },
        )
        .await;

        match reconnect_result {
            Ok((next_db, next_action_stream)) => {
                *db = next_db;
                *action_stream = next_action_stream;
                if db.is_some() {
                    info!(
                        "AQE live sync reconnect complete for strategy {}",
                        auth.strategy_id
                    );
                    if action_stream.is_none() {
                        warn!(
                            "AQE live sync reconnected for strategy {} but the strategy action stream is unavailable",
                            auth.strategy_id
                        );
                    }
                } else {
                    warn!(
                        "AQE live sync reconnect failed for strategy {}; will retry on the next live loop tick",
                        auth.strategy_id
                    );
                }
            }
            Err(_) => {
                *db = None;
                *action_stream = None;
                warn!(
                    "AQE live sync reconnect timed out for strategy {} after {}s; will retry on the next live loop tick",
                    auth.strategy_id, LIVE_SYNC_RECONNECT_TIMEOUT_SECS
                );
            }
        }
    }

    fn enqueue_pending_aqs_sync_op(
        pending_ops: &mut VecDeque<PendingAqsSyncOp>,
        op: PendingAqsSyncOp,
    ) {
        if pending_ops.len() >= MAX_PENDING_AQS_SYNC_OPS {
            pending_ops.pop_front();
            warn!(
                "Pending AQS sync queue reached capacity ({}); dropping oldest item",
                MAX_PENDING_AQS_SYNC_OPS
            );
        }
        pending_ops.push_back(op);
    }

    async fn execute_pending_aqs_sync_op<C: surrealdb::Connection>(
        client: &surrealdb::Surreal<C>,
        auth: &AqsAuth,
        op: PendingAqsSyncOp,
    ) -> Result<(), surrealdb::Error> {
        match op {
            PendingAqsSyncOp::StrategyStarted { universe, account } => {
                mark_strategy_started(client, auth, &universe, account.as_ref()).await
            }
            PendingAqsSyncOp::AccountState {
                account,
                captured_at,
            } => persist_live_account_state(client, auth, &account, captured_at).await,
            PendingAqsSyncOp::StrategyEvent(event) => {
                persist_strategy_event(client, auth, event).await
            }
            PendingAqsSyncOp::LiveMetrics(metrics) => {
                persist_live_metrics(client, auth, metrics).await
            }
        }
    }

    async fn flush_pending_aqs_sync_ops<C: surrealdb::Connection>(
        client: &surrealdb::Surreal<C>,
        auth: &AqsAuth,
        pending_ops: &mut VecDeque<PendingAqsSyncOp>,
    ) -> Result<(), surrealdb::Error> {
        while let Some(op) = pending_ops.pop_front() {
            match Self::execute_pending_aqs_sync_op(client, auth, op.clone()).await {
                Ok(()) => {}
                Err(error) if Self::is_transient_surreal_error(&error) => {
                    Self::enqueue_pending_aqs_sync_op(pending_ops, op);
                    return Err(error);
                }
                Err(error) => {
                    error!(
                        "Dropping pending AQS sync operation for strategy {} after non-transient error: {}",
                        auth.strategy_id, error
                    );
                }
            }
        }

        Ok(())
    }

    async fn upsert_live_insight_with_retry<C: surrealdb::Connection>(
        client: &surrealdb::Surreal<C>,
        auth: &AqsAuth,
        snapshot: &InsightSnapshot,
    ) -> Result<(), surrealdb::Error> {
        let max_attempts = 3usize;
        let live_session_key = Self::live_session_key_for_auth(auth);
        let record_id = Self::live_insight_record_id(auth, &snapshot.insight_id);

        for attempt in 1..=max_attempts {
            let result = match tokio::time::timeout(
                Duration::from_secs(LIVE_SYNC_QUERY_TIMEOUT_SECS),
                client
                    .query(
                        "UPSERT type::record('insights', $record_id)
                         CONTENT object::extend(
                             $snapshot,
                             {
                                 strategy_id: type::record('strategy', $strategy_id),
                                 live_session_id: type::record('live_strategy_session', <uuid>$live_session_key)
                             }
                         )",
                    )
                    .bind(("record_id", record_id.clone()))
                    .bind(("snapshot", snapshot.clone()))
                    .bind(("strategy_id", auth.strategy_id.clone()))
                    .bind(("live_session_key", live_session_key.clone())),
            )
            .await
            {
                Ok(result) => result.and_then(|response| response.check()),
                Err(_) => {
                    warn!(
                        "Timed out upserting live insight for strategy {} insight {} on attempt {}/{} after {}s",
                        auth.strategy_id,
                        snapshot.insight_id,
                        attempt,
                        max_attempts,
                        LIVE_SYNC_QUERY_TIMEOUT_SECS
                    );
                    Err(surrealdb::Error::internal(
                        "live insight upsert timed out".to_string(),
                    ))
                }
            };

            match result {
                Ok(_) => return Ok(()),
                Err(error)
                    if attempt < max_attempts && Self::is_transient_surreal_error(&error) =>
                {
                    warn!(
                        "Transient live insight sync error for strategy {} insight {} on attempt {}/{}: {}",
                        auth.strategy_id, snapshot.insight_id, attempt, max_attempts, error
                    );
                    tokio::time::sleep(Duration::from_millis(150 * attempt as u64)).await;
                }
                Err(error) => return Err(error),
            }
        }

        unreachable!("live insight upsert retry loop must return before exhaustion");
    }

    fn trade_event_matches_insight(insight: &Insight, order: &Order) -> bool {
        order
            .insight_id
            .as_deref()
            .is_some_and(|value| value == insight.insight_id.to_string())
            || insight.order_id.as_deref() == Some(&order.order_id)
            || insight.close_order_id.as_deref() == Some(&order.order_id)
    }

    fn close_price_from_order(order: &Order) -> f64 {
        order
            .legs
            .as_ref()
            .and_then(|legs| {
                legs.stop_loss
                    .as_ref()
                    .filter(|leg| leg.status == TradeUpdateEvent::Filled)
                    .and_then(|leg| leg.filled_price.or(leg.limit_price))
                    .or_else(|| {
                        legs.take_profit
                            .as_ref()
                            .filter(|leg| leg.status == TradeUpdateEvent::Filled)
                            .and_then(|leg| leg.filled_price.or(leg.limit_price))
                    })
                    .or_else(|| {
                        legs.trailing_stop
                            .as_ref()
                            .filter(|leg| leg.status == TradeUpdateEvent::Filled)
                            .and_then(|leg| leg.filled_price.or(leg.limit_price))
                    })
            })
            .or(order.filled_price)
            .or(order.stop_price)
            .or(order.limit_price)
            .unwrap_or(0.0)
    }

    fn order_log_summary(order: &Order) -> String {
        let entry = order
            .filled_price
            .or(order.limit_price)
            .or(order.stop_price)
            .map(|price| format!("{price:.4}"))
            .unwrap_or_else(|| "MARKET".to_string());

        let tp = order
            .legs
            .as_ref()
            .and_then(|legs| legs.take_profit.as_ref())
            .and_then(|leg| leg.limit_price)
            .map(|price| format!("{price:.4}"))
            .unwrap_or_else(|| "-".to_string());

        let sl = order
            .legs
            .as_ref()
            .and_then(|legs| legs.stop_loss.as_ref())
            .and_then(|leg| leg.limit_price)
            .map(|price| format!("{price:.4}"))
            .unwrap_or_else(|| "-".to_string());

        format!(
            "{} {:?} {:.4} @ {} type={:?} status={:?} strat={} order_id={} tp={} sl={}",
            order.asset.symbol,
            order.side,
            order.qty,
            entry,
            order.order_type,
            order.status,
            order.strategy_type.as_deref().unwrap_or("-"),
            order.order_id,
            tp,
            sl
        )
    }

    fn sync_broker_managed_levels(insight: &mut Insight, order: &Order) {
        if let Some(legs) = &order.legs {
            if let Some(tp) = legs.take_profit.as_ref().and_then(|leg| leg.limit_price) {
                insight.add_take_profit_levels(vec![tp]);
            }
            if let Some(sl) = legs.stop_loss.as_ref().and_then(|leg| leg.limit_price) {
                insight.add_stop_loss_levels(vec![sl]);
            }
            if let Some(trailing_gap) = legs.trailing_stop.as_ref().and_then(|leg| leg.trail_price)
            {
                insight.set_trailing_stop_price(Some(trailing_gap));
            }
            insight.legs = legs.clone();
        }
    }

    pub fn new(
        name: String,
        version: String,
        strategy: S,
        broker: UnifiedBroker<E, D>,
        mode: StrategyMode,
        timeframe: TimeFrame,
    ) -> Self {
        Self {
            name,
            version,
            strategy: Some(strategy),
            broker,
            mode,
            status: StrategyStatus::Initialised,
            strategy_id: Uuid::new_v4(),
            insights: Default::default(),
            history: Default::default(),
            universe: Default::default(),
            alpha_models: Vec::new(),
            indicators: HashMap::new(),
            warm_up_bars: 0,
            execution_risk: 0.02,
            min_reward_risk_ratio: 2.0,
            base_confidence: 1.0,
            variables: DashMap::new(),
            max_history_rows: 2000,
            live_metrics: LivePerformanceTracker::default(),
            insight_pipeline: Default::default(),
            timeframe,
            shutdown_tx: tokio::sync::watch::channel(false).0,
        }
    }

    /// Access the strategy (panics if called during backtest iteration — internal only).
    pub fn strategy(&self) -> &S {
        self.strategy
            .as_ref()
            .expect("strategy is taken during iteration")
    }

    /// Mutably access the strategy.
    pub fn strategy_mut(&mut self) -> &mut S {
        self.strategy
            .as_mut()
            .expect("strategy is taken during iteration")
    }

    // ─────────────────── Registration Helpers ───────────────────

    /// Register an alpha model (Python's `registerAlpha`).
    pub fn add_alpha(&mut self, alpha: WrappedAlphaModel) {
        self.alpha_models.push(alpha);
    }

    /// Register an indicator securely with deduplication (Python's TA registration).
    pub fn register_indicator(&mut self, mut indicator: Box<dyn Indicator>) {
        let name = indicator.name();
        if !self.indicators.contains_key(&name) {
            // Apply `run` to all existing history DataFrames to backfill
            for (_, df) in self.history.iter_mut() {
                if df.height() > 0 {
                    if let Err(e) = indicator.run(df) {
                        eprintln!(
                            "Failed to run indicator {} on existing history: {}",
                            name, e
                        );
                    }
                }
            }
            self.indicators.insert(name, indicator);
        }
    }

    /// Register a pipe into the correct `InsightState` bucket (Python's `add_executor`).
    pub fn add_pipe(&mut self, pipe: WrappedInsightPipe) {
        self.insight_pipeline.add_pipe(pipe);
    }

    /// Register multiple pipes at once (Python's `add_executors`).
    pub fn add_pipes(&mut self, pipes: Vec<WrappedInsightPipe>) {
        self.insight_pipeline.add_pipes(pipes);
    }

    // ─────────────────── Warm-Up ───────────────────

    /// Get the current warm-up period.
    pub fn warm_up_bars(&self) -> i32 {
        self.warm_up_bars
    }

    /// Set the warm-up period. Only raises the value — never lowers it.
    /// This allows multiple alpha models / strategies to each declare
    /// their own minimum warm-up requirement without conflicting.
    pub fn set_warm_up_bars(&mut self, bars: i32) {
        if bars > self.warm_up_bars {
            self.warm_up_bars = bars;
        }
    }

    pub fn set_execution_risk(&mut self, risk: f64) {
        self.execution_risk = risk;
    }

    pub fn set_min_reward_risk_ratio(&mut self, ratio: f64) {
        self.min_reward_risk_ratio = ratio;
    }

    pub fn set_base_confidence(&mut self, confidence: f64) {
        self.base_confidence = Self::normalize_base_confidence(confidence);
    }

    pub fn base_confidence(&self) -> f64 {
        self.base_confidence
    }

    fn normalize_base_confidence(confidence: f64) -> f64 {
        if confidence <= 0.0 {
            0.0
        } else if confidence <= 1.0 {
            confidence
        } else {
            (confidence / 100.0).clamp(0.0, 1.0)
        }
    }

    pub fn max_history_rows(&self) -> usize {
        self.max_history_rows
    }

    pub fn update_max_history_rows(&mut self, rows: usize) {
        self.max_history_rows = rows.max(100);
    }

    /// Trigger strategy shutdown gracefully.
    pub fn shutdown(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Get the current timeframe.
    pub fn timeframe(&self) -> TimeFrame {
        self.timeframe
    }

    /// Set the timeframe.
    pub fn set_timeframe(&mut self, timeframe: TimeFrame) {
        self.timeframe = timeframe;
    }

    // ─────────────────── Universe Loading ───────────────────

    /// Load universe: strategy.universe() → broker.get_ticker_info() → strategy.init per asset.
    /// Mirrors Python's `_loadUniverse()`.
    async fn load_universe(&mut self, strategy: &mut S) {
        let symbol_set = strategy.universe(self);
        let mut universe_symbols: Vec<_> = symbol_set.iter().cloned().collect();
        universe_symbols.sort();
        info!("Loading strategy universe: {}", universe_symbols.join(", "));

        for symbol in &symbol_set {
            debug!("Loading asset metadata for {}", symbol);
            match self.broker.get_ticker_info(symbol).await {
                Ok(asset) => {
                    // Initialize empty history for this symbol
                    self.history
                        .insert(asset.symbol.clone(), DataFrame::default());
                    self.universe.insert(asset.symbol.clone(), asset);
                }
                Err(_e) => {
                    continue;
                }
            }
        }

        // Call strategy.init() per asset
        let assets: Vec<Asset> = self.universe.values().cloned().collect();
        for asset in &assets {
            debug!("Running strategy init for {}", asset.symbol);
            strategy.init(self, asset);
        }
    }

    // ─────────────────── Alpha Model Loading ───────────────────

    /// Start all alpha models, then init each one per asset in the universe.
    /// Called after `load_universe()`. Reusable for both backtesting and live runs.
    fn load_alpha_models(&mut self) {
        let mut alphas = std::mem::take(&mut self.alpha_models);

        // alpha.start() — one-time startup
        for alpha in alphas.iter_mut() {
            debug!("Starting alpha model {}", alpha.name());
            alpha.start(self);
        }

        // alpha.init(asset) — per-asset initialization
        let assets: Vec<Asset> = self.universe.values().cloned().collect();
        for asset in &assets {
            for alpha in alphas.iter_mut() {
                debug!(
                    "Initialising alpha model {} for {}",
                    alpha.name(),
                    asset.symbol
                );
                alpha.init(self, asset);
            }
        }

        self.alpha_models = alphas;
    }

    // ─────────────────── Bar Processing ───────────────────

    /// Process a bar callback: append to HISTORY, warm-up check, on_bar → generate insights.
    /// Mirrors Python's `_on_bar()`.
    fn _on_bar(&mut self, strategy: &mut S, symbol: &str, bar: &BarData) {
        match bar {
            BarData::Bars(bars) => {
                if let Some(last_bar) = bars.last() {
                    debug!(
                        "on_bar symbol={} o={:.4} h={:.4} l={:.4} c={:.4} v={:.0}",
                        symbol,
                        last_bar.open,
                        last_bar.high,
                        last_bar.low,
                        last_bar.close,
                        last_bar.volume
                    );
                } else {
                    debug!("on_bar symbol={} bars=empty", symbol);
                }
            }
            BarData::Frame(frame) => {
                debug!("on_bar symbol={} frame_shape={:?}", symbol, frame.shape());
            }
        }
        let max_history_rows = self.max_history_rows();
        // Append to history DataFrame
        if let Some(history_df) = self.history.get_mut(symbol) {
            let bar_df_res = self.broker.data.format_on_bar(bar.clone());

            if let Ok(mut bar_df) = bar_df_res {
                // Pre-fill bar_df with Null values for indicators to match schema
                for (name, _ind) in self.indicators.iter() {
                    let null_series =
                        Series::full_null(name.into(), bar_df.height(), &DataType::Float64);
                    let _ = bar_df.with_column(null_series);
                }

                if history_df.height() == 0 {
                    *history_df = bar_df.clone(); // Just taking the newly formed df
                } else if let Ok(combined) = history_df.vstack(&bar_df) {
                    *history_df = combined;
                }
                if history_df.height() > max_history_rows {
                    *history_df = history_df.tail(Some(max_history_rows));
                }

                let current_time = self.broker.get_current_time();
                for (_name, ind) in self.indicators.iter_mut() {
                    let ws = ind.window_size();
                    let h = history_df.height();
                    if h >= ws {
                        let tail = history_df.tail(Some(ws));
                        if let Ok(res) = ind.run_bar(&tail) {
                            for (col_name, val) in res {
                                if let Ok(s) = history_df.column(&col_name) {
                                    if let Ok(ca) = s.f64() {
                                        let mut vec: Vec<Option<f64>> = ca.into_iter().collect();
                                        if !vec.is_empty() {
                                            let last_idx = vec.len() - bar_df.height();
                                            for i in last_idx..vec.len() {
                                                vec[i] = Some(val);
                                            }
                                            let new_s = Series::new(col_name.into(), &vec);
                                            let _ = history_df.with_column(new_s);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    ind.set_last_run_time(current_time);
                }
            }
        }

        // Warm-up check: skip strategy callbacks until enough bars accumulated
        if self.warm_up_bars > 0 {
            if let Some(history_df) = self.history.get(symbol) {
                if (history_df.height() as i32) < self.warm_up_bars {
                    debug!(
                        "Warm-up active for {}: {}/{} bars loaded, skipping strategy callbacks",
                        symbol,
                        history_df.height(),
                        self.warm_up_bars
                    );
                    return;
                }
            }
        }

        // Call strategy callbacks
        debug!(
            "Warm-up complete for {}, invoking strategy callbacks",
            symbol
        );
        strategy.on_bar(self, symbol, bar);

        // Generate insights from alpha models then strategy
        self._generate_insights(strategy, symbol);
    }

    // ─────────────────── Insight Generation ───────────────────

    /// Loop alpha models → generate insights → submit successful ones.
    /// Then call strategy.generate_insights().
    /// Mirrors Python's `_generateInsights()`.
    fn _generate_insights(&mut self, strategy: &mut S, symbol: &str) {
        // Take alpha_models out to avoid borrow conflict
        let mut alphas = std::mem::take(&mut self.alpha_models);

        for alpha in alphas.iter_mut() {
            if !alpha.is_allowed_asset(symbol) {
                continue;
            }
            debug!("Running alpha model {} for {}", alpha.name(), symbol);
            let result = alpha.generate_insights(self, symbol);
            debug!(
                "Alpha model {} result for {}: success={} message={:?} created_insight={}",
                alpha.name(),
                symbol,
                result.success,
                result.message,
                result.insight.is_some()
            );
            if result.success {
                if let Some(insight) = result.insight {
                    info!("New insight created: {}", insight.log_summary());
                    self.add_insight(insight);
                }
            }
        }

        // Put alpha_models back
        self.alpha_models = alphas;

        // Strategy's own generate_insights
        debug!("Running strategy generate_insights for {}", symbol);
        strategy.generate_insights(self, symbol);
        debug!("Completed strategy generate_insights for {}", symbol);
    }

    // ─────────────────── Insight Pipeline ───────────────────

    /// Run insight pipeline: for each active insight, run pipes matching its state.
    /// Mirrors Python's `_insightListener` (the per-step portion).
    fn run_insight_pipeline(&mut self) {
        // Take pipeline and insights out to avoid borrow conflicts
        let mut pipeline = std::mem::take(&mut self.insight_pipeline);
        let mut insights = std::mem::take(&mut self.insights);
        let mut touched_insight_ids = HashSet::new();

        let insight_ids: Vec<Uuid> = insights.keys().cloned().collect();

        for id in insight_ids {
            let insight = match insights.get_mut(&id) {
                Some(ins) => ins,
                None => continue,
            };
            touched_insight_ids.insert(id);

            if insight.has_expired(self) {
                debug!(
                    "Skipping expired insight {} in state {:?}",
                    insight.insight_id(),
                    insight.state()
                );
                continue;
            }

            let state = insight.state.clone();
            let should_clear_first_on_fill =
                state == InsightState::Filled && insight.first_on_fill();

            if let Some(pipes) = pipeline.get_pipes_for_state(&state) {
                let mut passed = true;
                let mut rejection_reason: Option<String> = None;
                for pipe in pipes.iter_mut() {
                    if !pipe.should_run(insight) {
                        continue;
                    }
                    debug!(
                        "Running insight pipe {} for insight {} in state {:?}",
                        pipe.name(),
                        insight.insight_id(),
                        state
                    );
                    let result = pipe.run(self, insight);
                    debug!(
                        "Insight pipe {} result for insight {}: success={} passed={} message={:?}",
                        pipe.name(),
                        insight.insight_id(),
                        result.success,
                        result.passed,
                        result.message
                    );
                    if !result.success {
                        passed = false;
                        rejection_reason = Some(result.message.unwrap_or_else(|| {
                            format!("Insight pipe {} failed", result.pipe_name)
                        }));
                        break;
                    }
                    if !result.passed {
                        passed = false;
                        rejection_reason = Some(result.message.unwrap_or_else(|| {
                            format!("Insight pipe {} did not pass", result.pipe_name)
                        }));
                        break;
                    }
                }
                if !passed {
                    if !insight.state.is_inactive() {
                        insight.order_rejected(
                            rejection_reason
                                .as_deref()
                                .unwrap_or("Insight pipeline failed"),
                        );
                    }
                    continue;
                }
            }

            if should_clear_first_on_fill {
                debug!(
                    "Filled-state insight pipeline completed for insight {}; clearing first_on_fill",
                    insight.insight_id()
                );
                insight.set_first_on_fill(false);
            }
        }

        // Put them back
        self.insight_pipeline = pipeline;
        self.insights = insights;
        for insight_id in touched_insight_ids {
            self.insights.refresh_runtime_tracking(&insight_id);
        }
    }

    async fn persist_live_metrics_if_needed<C: surrealdb::Connection>(
        &mut self,
        client: &surrealdb::Surreal<C>,
        auth: &AqsAuth,
        pending_ops: &mut VecDeque<PendingAqsSyncOp>,
    ) -> Result<(), surrealdb::Error> {
        self.refresh_live_open_position_metrics();

        if !self.live_metrics.should_persist() {
            return Ok(());
        }

        if let Some(snapshot) = self.live_metrics.snapshot() {
            let record: aqs_types::StrategyLiveMetricsRecord = snapshot.into();
            match persist_live_metrics(client, auth, record.clone()).await {
                Ok(()) => {
                    self.live_metrics.mark_persisted();
                }
                Err(error) if Self::is_transient_surreal_error(&error) => {
                    Self::enqueue_pending_aqs_sync_op(
                        pending_ops,
                        PendingAqsSyncOp::LiveMetrics(record),
                    );
                    return Err(error);
                }
                Err(error) => {
                    error!(
                        "Failed to persist live metrics for strategy {}: {}",
                        auth.strategy_id, error
                    );
                }
            }
        }

        Ok(())
    }

    fn latest_price_for_symbol(&self, symbol: &str) -> Option<f64> {
        self.latest_quote(symbol)
            .ok()
            .and_then(|quote| quote.last.or(Some((quote.bid + quote.ask) / 2.0)))
            .filter(|price| price.is_finite())
    }

    fn insights_state_counts_json(&self) -> serde_json::Value {
        let mut counts = serde_json::Map::new();
        for (state, count) in self.insights.lifetime_state_counts() {
            counts.insert(
                format!("{:?}", state),
                serde_json::Value::from(*count as u64),
            );
        }
        serde_json::Value::Object(counts)
    }

    fn refresh_live_open_position_metrics(&mut self) {
        let mut open_positions_count = 0usize;
        let mut open_insights_count = 0usize;
        let mut open_positions_unrealized_pnl = 0.0f64;
        let mut open_positions_profitable_count = 0usize;
        let mut open_positions_losing_count = 0usize;
        let mut symbols = self
            .universe
            .values()
            .map(|asset| asset.symbol.clone())
            .collect::<Vec<_>>();
        let mut latest_prices = HashMap::<String, f64>::new();

        for insight in self.insights.values() {
            if insight.state.is_inactive() {
                continue;
            }
            open_insights_count += 1;

            if insight.state != InsightState::Filled {
                continue;
            }

            let remaining_qty = insight.remaining_quantity();
            if remaining_qty <= f64::EPSILON {
                continue;
            }

            let Some(entry_price) = insight
                .filled_price
                .or(insight.limit_price)
                .or(insight.stop_price)
            else {
                continue;
            };
            let current_price = if let Some(price) = latest_prices.get(&insight.symbol).copied() {
                price
            } else if let Some(price) = self.latest_price_for_symbol(&insight.symbol) {
                latest_prices.insert(insight.symbol.clone(), price);
                price
            } else {
                continue;
            };

            let pnl = match insight.side {
                crate::core::broker::types::OrderSide::Buy => {
                    (current_price - entry_price) * remaining_qty
                }
                crate::core::broker::types::OrderSide::Sell => {
                    (entry_price - current_price) * remaining_qty
                }
            };

            open_positions_count += 1;
            open_positions_unrealized_pnl += pnl;
            if pnl > 0.0 {
                open_positions_profitable_count += 1;
            } else if pnl < 0.0 {
                open_positions_losing_count += 1;
            }
            if !symbols.iter().any(|value| value == &insight.symbol) {
                symbols.push(insight.symbol.clone());
            }
        }

        symbols.sort();
        symbols.dedup();
        self.live_metrics.update_open_position_metrics(
            open_positions_count,
            open_insights_count,
            open_positions_unrealized_pnl,
            open_positions_profitable_count,
            open_positions_losing_count,
            symbols,
            chrono::Utc::now(),
        );
    }

    async fn sync_live_insights_to_aqs<C: surrealdb::Connection>(
        &mut self,
        client: &surrealdb::Surreal<C>,
        auth: &AqsAuth,
        synced_insight_states: &mut HashMap<String, String>,
        pending_ops: &mut VecDeque<PendingAqsSyncOp>,
        include_full_reconcile: bool,
    ) -> Result<bool, surrealdb::Error> {
        debug!(
            "Syncing live insights to AQS for strategy {}: {} in-memory insights",
            auth.strategy_id,
            self.insights.len()
        );
        if let Err(error) = client
            .query(
                "UPDATE type::record('strategy', $id)
                 SET status = $status,
                     is_live = true,
                     live_session_id = type::record('live_strategy_session', <uuid>$live_session_key),
                     insights_count = $insights_count,
                     last_heartbeat = time::now()",
            )
            .bind(("id", auth.strategy_id.clone()))
            .bind(("status", format!("{:?}", self.status)))
            .bind(("live_session_key", Self::live_session_key_for_auth(auth)))
            .bind(("insights_count", self.insights_state_counts_json()))
            .await
            .and_then(|response| response.check())
        {
            if Self::is_transient_surreal_error(&error) {
                return Err(error);
            }
            error!(
                "Failed to update live strategy summary for {}: {}",
                auth.strategy_id, error
            );
        }

        let mut persist_metrics_after_sync = false;
        let mut prune_after_sync = Vec::new();
        let insight_ids = self
            .insights
            .insight_ids_for_live_sync(include_full_reconcile);

        for insight_id in insight_ids {
            let Some(insight) = self.insights.get(&insight_id) else {
                continue;
            };
            let snapshot = InsightSnapshot::from_insight(insight, &auth.strategy_id);
            let is_terminal = insight.state.is_inactive();
            let current_state = snapshot.state.clone();
            let snapshot_value = serde_json::to_value(&snapshot).unwrap_or(serde_json::Value::Null);
            let _ = insight;
            let upsert_result = Self::upsert_live_insight_with_retry(client, auth, &snapshot).await;

            if let Err(error) = upsert_result {
                if Self::is_transient_surreal_error(&error) {
                    return Err(error);
                }
                error!(
                    "Failed to upsert live insight {} for strategy {}: {}\nPayload: {}",
                    snapshot.insight_id,
                    auth.strategy_id,
                    error,
                    serde_json::to_string_pretty(&snapshot_value)
                        .unwrap_or_else(|_| "<failed to serialize snapshot payload>".to_string())
                );
                continue;
            }

            let previous_state = synced_insight_states.get(&snapshot.insight_id).cloned();
            if previous_state.as_deref() != Some(current_state.as_str()) {
                persist_metrics_after_sync = true;
                info!(
                    "Live insight synced: strategy={} insight={} symbol={} state={}",
                    auth.strategy_id, snapshot.insight_id, snapshot.symbol, current_state
                );
                let latest_history = snapshot.state_history.last().cloned();
                if current_state == "Closed" {
                    self.live_metrics.record_closed_insight(&snapshot);
                }
                if let Err(error) = persist_strategy_event(
                    client,
                    auth,
                    StrategyEventRecord {
                        event_type: "insight_state".into(),
                        level: "info".into(),
                        title: format!("Insight {}", current_state),
                        message: latest_history
                            .as_ref()
                            .and_then(|entry| entry.message.clone())
                            .unwrap_or_else(|| {
                                format!("{} changed to {}", snapshot.symbol, current_state)
                            }),
                        payload: Some(serde_json::json!({
                            "insight_id": snapshot.insight_id,
                            "symbol": snapshot.symbol,
                            "state": current_state,
                            "side": snapshot.side,
                            "history": latest_history,
                        })),
                        created_at: Some(chrono::Utc::now()),
                    },
                )
                .await
                {
                    if Self::is_transient_surreal_error(&error) {
                        Self::enqueue_pending_aqs_sync_op(
                            pending_ops,
                            PendingAqsSyncOp::StrategyEvent(StrategyEventRecord {
                                event_type: "insight_state".into(),
                                level: "info".into(),
                                title: format!("Insight {}", current_state),
                                message: latest_history
                                    .as_ref()
                                    .and_then(|entry| entry.message.clone())
                                    .unwrap_or_else(|| {
                                        format!("{} changed to {}", snapshot.symbol, current_state)
                                    }),
                                payload: Some(serde_json::json!({
                                    "insight_id": snapshot.insight_id,
                                    "symbol": snapshot.symbol,
                                    "state": current_state,
                                    "side": snapshot.side,
                                    "history": latest_history,
                                })),
                                created_at: Some(chrono::Utc::now()),
                            }),
                        );
                        return Err(error);
                    }
                    error!(
                        "Failed to persist live insight state event for strategy {} insight {}: {}",
                        auth.strategy_id, snapshot.insight_id, error
                    );
                }
                synced_insight_states.insert(snapshot.insight_id.clone(), current_state.clone());
            } else {
                synced_insight_states.insert(snapshot.insight_id.clone(), current_state.clone());
            }

            self.insights.remove_dirty(&insight_id);
            if is_terminal {
                prune_after_sync.push(insight_id);
            }
        }

        for insight_id in prune_after_sync {
            self.insights.prune_terminal_insight(&insight_id);
            synced_insight_states.remove(&insight_id.to_string());
        }

        Ok(persist_metrics_after_sync)
    }

    fn sync_backtest_insight_snapshots(&self) {
        let Some(backtest_state) = self.broker.backtest_state.as_ref() else {
            return;
        };

        let mut state = backtest_state.write();
        let strategy_id = self.strategy_id.to_string();
        for insight in self.insights.values() {
            state.record_insight_snapshot(InsightSnapshot::from_insight(insight, &strategy_id));
        }
    }

    // ─────────────────── Trade Event Processing ───────────────────

    /// Process trade events from the broker. Mirrors Python's `_on_trade_update`.
    /// Handles all event types: New, PendingNew, Filled, PartialFilled,
    /// Canceled, Rejected, Closed, Replaced.
    /// Works with any broker implementing `OrderManagementProvider`.
    fn on_trade_update(&mut self) {
        let events = self.broker.drain_trade_events();
        let mut children_to_submit = Vec::new();
        let mut parents_to_close = Vec::new();
        let mut touched_insight_ids = HashSet::new();

        for (order, event) in events {
            debug!(
                "Trade event: event={:?} {}",
                event,
                Self::order_log_summary(&order)
            );
            if !self.universe.contains_key(&order.asset.symbol) {
                continue;
            }
            let mut insight_ids = self.insights.candidate_insight_ids_for_trade_event(&order);
            if insight_ids.is_empty() {
                insight_ids = self.insights.keys().cloned().collect();
            }
            for id in insight_ids {
                let insight = match self.insights.get_mut(&id) {
                    Some(i) => i,
                    None => continue,
                };
                if insight.symbol != order.asset.symbol {
                    continue;
                }
                let matched = Self::trade_event_matches_insight(insight, &order);

                match (&insight.state, &event) {
                    // NEW → EXECUTED on accept
                    (
                        InsightState::New,
                        TradeUpdateEvent::Accepted
                        | TradeUpdateEvent::PendingNew
                        | TradeUpdateEvent::New,
                    ) => {
                        if matched {
                            insight.order_accepted(&order.order_id);
                            touched_insight_ids.insert(id);
                            break;
                        }
                    }
                    // NEW can jump straight to fill in live brokers
                    (InsightState::New, TradeUpdateEvent::Filled) => {
                        if matched {
                            insight.order_accepted(&order.order_id);
                            let price = order.filled_price.or(order.limit_price).unwrap_or(0.0);
                            insight.position_filled(price, order.filled_qty, &order.order_id);
                            Self::sync_broker_managed_levels(insight, &order);
                            touched_insight_ids.insert(id);
                            if !insight.children.is_empty() {
                                children_to_submit.extend(insight.children.clone());
                            }
                            break;
                        }
                    }
                    // NEW can jump straight to partial fill in live brokers
                    (InsightState::New, TradeUpdateEvent::PartialFilled) => {
                        if matched {
                            insight.order_accepted(&order.order_id);
                            let price = order.filled_price.or(order.limit_price).unwrap_or(0.0);
                            insight.partial_filled(order.filled_qty, price, &order.order_id);
                            Self::sync_broker_managed_levels(insight, &order);
                            touched_insight_ids.insert(id);
                            break;
                        }
                    }
                    // EXECUTED → FILLED
                    (InsightState::Executed, TradeUpdateEvent::Filled) => {
                        if matched {
                            let price = order.filled_price.or(order.limit_price).unwrap_or(0.0);
                            insight.position_filled(price, order.filled_qty, &order.order_id);
                            Self::sync_broker_managed_levels(insight, &order);
                            touched_insight_ids.insert(id);

                            // If this insight has children, queue them for submission
                            if !insight.children.is_empty() {
                                children_to_submit.extend(insight.children.clone());
                            }
                            break;
                        }
                    }
                    // EXECUTED partial fill
                    (InsightState::Executed, TradeUpdateEvent::PartialFilled) => {
                        if matched {
                            let price = order.filled_price.or(order.limit_price).unwrap_or(0.0);
                            insight.partial_filled(order.filled_qty, price, &order.order_id);
                            Self::sync_broker_managed_levels(insight, &order);
                            touched_insight_ids.insert(id);
                            break;
                        }
                    }
                    // FILLED partial close / scale-out
                    (InsightState::Filled, TradeUpdateEvent::PartialFilled) => {
                        if matched {
                            let price = Self::close_price_from_order(&order);
                            insight.partial_closed(order.filled_qty, price, &order.order_id);
                            touched_insight_ids.insert(id);
                            // We dont need to reset this when closing the insight - its kind of a
                            // design choice
                            // Self::sync_broker_managed_levels(insight, &order);
                            break;
                        }
                    }
                    // EXECUTED → CANCELED
                    (InsightState::Executed | InsightState::New, TradeUpdateEvent::Canceled) => {
                        if matched {
                            Self::sync_broker_managed_levels(insight, &order);
                            insight.order_canceled("Order canceled");
                            touched_insight_ids.insert(id);
                            // Cancel any open children
                            parents_to_close.push(insight.insight_id);
                            break;
                        }
                    }
                    // EXECUTED → REJECTED
                    (InsightState::Executed | InsightState::New, TradeUpdateEvent::Rejected) => {
                        if matched {
                            Self::sync_broker_managed_levels(insight, &order);
                            insight.order_rejected(
                                order
                                    .rejection_reason
                                    .as_deref()
                                    .unwrap_or("Order rejected"),
                            );
                            touched_insight_ids.insert(id);
                            // Cancel any open children
                            parents_to_close.push(insight.insight_id);
                            break;
                        }
                    }
                    // EXECUTED → REPLACED (order modified)
                    (InsightState::Executed, TradeUpdateEvent::Replaced) => {
                        if matched {
                            if order.limit_price != insight.limit_price {
                                insight.limit_price = order.limit_price;
                            }
                            if insight.quantity != Some(order.qty) {
                                insight.quantity = Some(order.qty);
                            }
                            Self::sync_broker_managed_levels(insight, &order);
                            touched_insight_ids.insert(id);
                            break;
                        }
                    }
                    // FILLED → CLOSED (SL/TP/manual close)
                    (InsightState::Filled, TradeUpdateEvent::Filled | TradeUpdateEvent::Closed) => {
                        if matched {
                            let price = Self::close_price_from_order(&order);
                            // again we dont need to sync the TP/SL again on close because we will
                            // have the close price.
                            // Self::sync_broker_managed_levels(insight, &order);
                            insight.position_closed(price, &order.order_id, order.filled_qty);
                            touched_insight_ids.insert(id);

                            // Close/Cancel any open children
                            parents_to_close.push(insight.insight_id);
                            break;
                        }
                    }
                    _ => {}
                }
            }
        }

        // Process deferred child events
        for mut child in children_to_submit {
            child.submit(self);
            self.add_insight(child);
        }

        // Process orphaned children cleanup
        if !parents_to_close.is_empty() {
            let mut children_to_process = Vec::new();

            // First pass: identify which children need which action (needs immutable/short mutable borrow)
            let insight_ids: Vec<Uuid> = self.insights.keys().cloned().collect();
            for parent_id in parents_to_close {
                for id in &insight_ids {
                    if let Some(child) = self.insights.get(&id) {
                        if child.parent_id == Some(parent_id) {
                            children_to_process.push(*id);
                        }
                    }
                }
            }

            // Second pass: apply closures (requires mutable self)
            for id in children_to_process {
                if let Some(mut child) = self.insights.remove_insight(&id) {
                    match child.state {
                        InsightState::Filled => {
                            child.close(self);
                            self.add_insight(child);
                        }
                        InsightState::New | InsightState::Executed => {
                            child.cancel(self);
                            self.add_insight(child);
                        }
                        _ => {
                            self.add_insight(child);
                        }
                    }
                }
            }
        }

        for insight_id in touched_insight_ids {
            self.insights.refresh_runtime_tracking(&insight_id);
        }
    }
}

// ─────────────────────── StrategyContext impl ───────────────────────

impl<S, E, D> StrategyContext for StrategyState<S, E, D>
where
    S: Strategy,
    E: Broker + OrderManagementProvider,
    D: DataFeed + DataProvider,
{
    fn universe(&self) -> &HashMap<String, Asset> {
        &self.universe
    }

    fn history(&self) -> &HashMap<String, DataFrame> {
        &self.history
    }

    fn insights(&self) -> &InsightCollection {
        &self.insights
    }

    fn add_insight(&mut self, insight: Insight) {
        let mut insight = insight;
        self.bind_insight_context(&mut insight);
        self.insights.add_insight(insight);
    }

    fn submit_insight(&mut self, insight: &mut Insight) {
        info!("Submitting order to broker: {}", insight.log_summary());
        let result =
            futures::executor::block_on(self.broker.execution.submit_order(insight.clone()));
        if let Ok(order) = result {
            debug!(
                "Broker accepted: insight_id={} {}",
                insight.insight_id(),
                Self::order_log_summary(&order)
            );
            insight.order_id = Some(order.order_id.clone());
        }
    }

    fn register_indicator(&mut self, indicator: Box<dyn crate::core::indicators::Indicator>) {
        // self refers to StrategyState, which has the implementation
        self.register_indicator(indicator);
    }

    fn add_alpha(&mut self, alpha: crate::core::alpha::WrappedAlphaModel) {
        self.add_alpha(alpha);
    }

    fn add_pipe(&mut self, pipe: crate::core::pipeline::WrappedInsightPipe) {
        self.add_pipe(pipe);
    }

    fn set_execution_risk(&mut self, risk: f64) {
        self.set_execution_risk(risk);
    }

    fn set_min_reward_risk_ratio(&mut self, ratio: f64) {
        self.set_min_reward_risk_ratio(ratio);
    }

    fn set_base_confidence(&mut self, confidence: f64) {
        self.set_base_confidence(confidence);
    }

    fn execution_risk(&self) -> f64 {
        self.execution_risk
    }

    fn min_reward_risk_ratio(&self) -> f64 {
        self.min_reward_risk_ratio
    }

    fn base_confidence(&self) -> f64 {
        self.base_confidence
    }

    fn variables(&self) -> &DashMap<String, Value> {
        &self.variables
    }

    fn max_history_rows(&self) -> usize {
        self.max_history_rows
    }

    fn set_max_history_rows(&mut self, rows: usize) {
        self.update_max_history_rows(rows);
    }

    fn tools(&self) -> Box<dyn TradingTools + '_> {
        Box::new(StrategyTools::new(
            &self.universe,
            &self.insights,
            &self.broker,
        ))
    }

    fn mode(&self) -> StrategyMode {
        self.mode.clone()
    }

    fn warm_up_bars(&self) -> i32 {
        self.warm_up_bars
    }

    fn set_warm_up_bars(&mut self, bars: i32) {
        self.set_warm_up_bars(bars);
    }

    fn timeframe(&self) -> &TimeFrame {
        &self.timeframe
    }

    fn account(&self) -> Result<crate::core::broker::types::Account, BrokerError> {
        futures::executor::block_on(self.broker.get_account())
    }

    fn current_time(&self) -> chrono::DateTime<chrono::Utc> {
        self.broker.get_current_time()
    }

    fn bind_insight_context(&self, insight: &mut Insight) {
        if let Some(state) = self.broker.backtest_state.clone() {
            insight.bind_context(InsightStrategyContext::new(move || {
                state.read().current_time
            }));
        } else {
            insight.bind_context(InsightStrategyContext::new(chrono::Utc::now));
        }
    }

    fn latest_quote(&self, symbol: &str) -> Result<crate::core::broker::types::Quote, BrokerError> {
        if matches!(self.mode, StrategyMode::Backtest) {
            if let Some(state) = &self.broker.backtest_state {
                let state = state.read();
                if let Some(bar) = state.get_current_bar(symbol) {
                    return Ok(crate::core::broker::types::Quote {
                        symbol: symbol.to_string(),
                        bid: bar.close,
                        ask: bar.close,
                        bid_size: bar.volume,
                        ask_size: bar.volume,
                        last: Some(bar.close),
                        last_size: Some(bar.volume),
                        timestamp: bar.timestamp,
                    });
                }
            }
        }
        futures::executor::block_on(self.broker.get_latest_quote(symbol))
    }

    fn cancel_order(&self, order_id: &str) -> Result<bool, BrokerError> {
        self.broker.cancel_order_sync(order_id)
    }

    fn close_position(
        &self,
        order_id: &str,
        qty: f64,
        price: Option<f64>,
    ) -> Result<bool, BrokerError> {
        self.broker.close_position_sync(order_id, qty, price)
    }

    fn shutdown(&mut self) {
        self.shutdown_tx.send(true).ok();
    }
}

// ─────────────────────── BrokerAccess impl ───────────────────────

impl<S, E, D> BrokerAccess<E, D> for StrategyState<S, E, D>
where
    S: Strategy,
    E: Broker + OrderManagementProvider,
    D: DataFeed + DataProvider,
{
    fn broker(&self) -> &UnifiedBroker<E, D> {
        &self.broker
    }

    fn broker_mut(&mut self) -> &mut UnifiedBroker<E, D> {
        &mut self.broker
    }
}

// ─────────────────────── Backtest Runner (PaperBroker only) ───────────────────────

impl<S, D> StrategyState<S, PaperBroker, D>
where
    S: Strategy,
    D: DataFeed + DataProvider,
{
    /// Run a full backtest.
    ///
    /// Flow (mirrors Python's `_run` + `_run_backtest_loop`):
    /// 1. `strategy.on_start(ctx)`
    /// 2. `load_universe()` — strategy.universe() → broker.get_ticker_info() → strategy.init per asset
    /// 3. `alpha.start()` per alpha
    /// 4. `alpha.init(asset)` per alpha per asset
    /// 5. `broker.load_backtest_data()` — fills BacktestState
    /// 6. Loop: `broker.step()` → for each bar: `_on_bar()` → `run_insight_pipeline()`
    /// 7. `strategy.on_teardown(ctx)`
    /// 8. Return `BacktestResults`
    pub async fn run_backtest(
        &mut self,
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
        time_frame: TimeFrame,
    ) -> Result<BacktestResults, BrokerError> {
        // Take strategy out to avoid split-borrow (self.strategy vs self as ctx)
        let mut strategy = self
            .strategy
            .take()
            .expect("strategy must be Some before run_backtest");

        // ── 0. Connect broker (execution + data feed) ──
        info!(
            "Starting backtest strategy={} timeframe={:?} start={} end={}",
            self.name, time_frame, start, end
        );
        self.broker.connect().await?;

        // ── 1. Strategy lifecycle: on_start ──
        debug!("Invoking strategy on_start");
        strategy.on_start(self);

        // ── 2. Load universe (strategy.init per asset) ──
        self.load_universe(&mut strategy).await;

        assert!(
            !self.universe.is_empty(),
            "Universe is empty — strategy.universe() must return at least one symbol"
        );

        // ── 3. Load alpha models (start + init per asset) ──
        self.load_alpha_models();

        // ── 5. Load historical data into BacktestState ──
        let symbols: Vec<String> = self.universe.keys().cloned().collect();
        info!("Loading backtest history for symbols: {:?}", symbols);
        self.broker
            .load_backtest_data(&symbols, start, end, time_frame)
            .await?;
        info!("Backtest history loaded successfully");

        // ── 6. Main backtest loop ──
        loop {
            // Advance one step: process orders → get current bars
            let step_result = self.broker.step()?;

            match step_result {
                None => break, // Backtest complete
                Some(current_bars) => {
                    debug!(
                        "Backtest step bars received for {} symbols",
                        current_bars.len()
                    );
                    // Process trade events from the broker (fills, closes, cancels, etc.)
                    debug!("Backtest loop calling on_trade_update");
                    self.on_trade_update();
                    debug!("Backtest loop on_trade_update returned");
                    self.sync_backtest_insight_snapshots();

                    // Call _on_bar for each symbol's bar
                    for (symbol, bar) in &current_bars {
                        self._on_bar(&mut strategy, symbol, &BarData::Bars(vec![bar.clone()]));
                    }
                    // Run insight pipeline after processing all bars for this step
                    debug!("Backtest loop calling run_insight_pipeline");
                    self.run_insight_pipeline();
                    debug!("Backtest loop run_insight_pipeline returned");
                    self.sync_backtest_insight_snapshots();
                }
            }
        }

        // ── 7. Teardown ──
        debug!("Invoking strategy on_teardown");
        strategy.on_teardown(self);
        self.sync_backtest_insight_snapshots();

        // Disconnect data feed
        let _ = self.broker.disconnect().await;

        // Put strategy back
        self.strategy = Some(strategy);

        // ── 8. Results ──
        let results = self.broker.get_results();
        Ok(results)
    }
}

// ─────────────────────── Live Runner ───────────────────────

impl<S, E, D> StrategyState<S, E, D>
where
    S: Strategy + Send + Sync + 'static,
    E: Broker + OrderManagementProvider + Send + Sync + 'static,
    D: DataFeed + DataProvider + Send + Sync + 'static,
{
    /// Run the strategy in live execution mode.
    ///
    /// This will register callbacks for trade events & bar streams,
    /// then loop via `tokio::select!` block listening on channels to drive the pipeline.
    pub async fn run_live(&mut self, auth: Option<AqsAuth>) -> Result<(), BrokerError> {
        // Take strategy out to avoid split-borrow
        let mut strategy = self
            .strategy
            .take()
            .expect("strategy must be Some before run_live");

        self.broker
            .configure_live_session(&self.strategy_id.to_string())?;

        // 1. Connect broker
        self.broker.connect().await?;

        // ── SurrealDB Connection for Live Sync ──
        let mut db = if let Some(ref a) = auth {
            Self::connect_live_sync_db(a).await
        } else {
            None
        };

        let mut action_stream = if let (Some(client), Some(a)) = (&db, &auth) {
            Self::create_strategy_action_stream(client, a).await
        } else {
            None
        };

        let mut stop_action_id: Option<String> = None;
        let mut synced_insight_states: HashMap<String, String> = HashMap::new();

        let mut pending_sync_ops = VecDeque::new();

        self.start(
            &mut strategy,
            db.as_ref(),
            auth.as_ref(),
            &mut pending_sync_ops,
        )
        .await?;

        // Channels for incoming events
        let (trade_tx, mut trade_rx) = tokio::sync::mpsc::channel(100);
        let (bar_tx, mut bar_rx) = tokio::sync::mpsc::channel(100);

        // 5. Subscribe to trade event stream
        let trade_tx_clone = trade_tx.clone();
        let trade_callback = Arc::new(move |event| {
            let _ = trade_tx_clone.try_send(event);
        });
        self.broker
            .subscribe_to_trade_stream(trade_callback)
            .await?;

        // 6. Subscribe to market data stream
        let symbols: Vec<String> = self.universe.keys().cloned().collect();
        let time_frame = self.timeframe.clone();

        let bar_tx_clone = bar_tx.clone();
        let bar_callback = Arc::new(move |bar| {
            let _ = bar_tx_clone.try_send(bar);
        });
        self.broker
            .subscribe_to_data_stream(
                symbols.clone(),
                time_frame,
                DataStreamMode::CompletedBar,
                bar_callback,
            )
            .await?;

        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());

        info!("Started live trading mode for strategy: {}", self.name);

        self.status = StrategyStatus::Running;

        if let (Some(client), Some(a)) = (&db, &auth) {
            let _ = persist_strategy_event(
                client,
                a,
                StrategyEventRecord {
                    event_type: "lifecycle".into(),
                    level: "info".into(),
                    title: "Strategy started".into(),
                    message: format!("Strategy '{}' entered live mode", self.name),
                    payload: Some(serde_json::json!({
                        "strategy_name": self.name,
                        "node_id": a.node_id,
                    })),
                    created_at: Some(chrono::Utc::now()),
                },
            )
            .await;
        }

        // 7. Main Event Loop
        // Pipeline loop interval
        let mut pipeline_interval = tokio::time::interval(std::time::Duration::from_secs(5));
        let mut reconcile_interval =
            tokio::time::interval(std::time::Duration::from_secs(LIVE_SYNC_RECONCILE_SECS));
        let mut force_full_reconcile = true;

        loop {
            tokio::select! {
                // Handle broker trade events
                Some((order, event)) = trade_rx.recv() => {
                    debug!(
                        "Live loop received trade event {:?} for order {}",
                        event,
                        order.order_id
                    );
                    // Process trade events (we let `on_trade_update()` drain all pending broker state)
                    self.on_trade_update();
                    if auth.is_none() {
                        let pruned_ids = self.insights.prune_terminal_insights_without_aqs();
                        for insight_id in pruned_ids {
                            synced_insight_states.remove(&insight_id.to_string());
                        }
                    }
                    if let Some(a) = auth.as_ref() {
                        if db.is_none() {
                            Self::reconnect_live_sync(&mut db, &mut action_stream, a).await;
                            force_full_reconcile = true;
                        }
                        if let Some(client) = db.as_ref() {
                            if let Err(error) = Self::flush_pending_aqs_sync_ops(client, a, &mut pending_sync_ops).await {
                                error!(
                                    "Live sync could not flush pending AQS ops for strategy {} after trade event: {}",
                                    a.strategy_id, error
                                );
                                Self::reconnect_live_sync(&mut db, &mut action_stream, a).await;
                                continue;
                            }
                            let sync_result = self
                                .sync_live_insights_to_aqs(client, a, &mut synced_insight_states, &mut pending_sync_ops, force_full_reconcile)
                                .await;
                            match sync_result {
                                Ok(persist_metrics_after_sync) => {
                                    force_full_reconcile = false;
                                    if persist_metrics_after_sync {
                                        if let Err(error) = self.persist_live_metrics_if_needed(client, a, &mut pending_sync_ops).await {
                                            error!(
                                                "Live sync lost AQS connection for strategy {} while persisting live metrics: {}",
                                                a.strategy_id, error
                                            );
                                            Self::reconnect_live_sync(&mut db, &mut action_stream, a).await;
                                            continue;
                                        }
                                    }
                                    if let Ok(account) = self.broker.get_account().await {
                                        let captured_at = chrono::Utc::now();
                                        self.live_metrics
                                            .update_equity(account.equity, captured_at);
                                        if let Err(error) = persist_live_account_state(client, a, &account, captured_at).await {
                                            if Self::is_transient_surreal_error(&error) {
                                                Self::enqueue_pending_aqs_sync_op(
                                                    &mut pending_sync_ops,
                                                    PendingAqsSyncOp::AccountState {
                                                        account: account.clone(),
                                                        captured_at,
                                                    },
                                                );
                                                error!(
                                                    "Live sync lost AQS connection for strategy {} while persisting account state: {}",
                                                    a.strategy_id, error
                                                );
                                                Self::reconnect_live_sync(&mut db, &mut action_stream, a).await;
                                                continue;
                                            }
                                            error!(
                                                "Failed to persist live account state for strategy {}: {}",
                                                a.strategy_id, error
                                            );
                                        }
                                        if let Err(error) = self.persist_live_metrics_if_needed(client, a, &mut pending_sync_ops).await {
                                            error!(
                                                "Live sync lost AQS connection for strategy {} while persisting live metrics: {}",
                                                a.strategy_id, error
                                            );
                                            Self::reconnect_live_sync(&mut db, &mut action_stream, a).await;
                                            continue;
                                        }
                                    }
                                    let trade_event_record = StrategyEventRecord {
                                        event_type: "trade_event".into(),
                                        level: "info".into(),
                                        title: "Broker trade event".into(),
                                        message: format!("Received {:?} for order {}", event, order.order_id),
                                        payload: Some(serde_json::json!({
                                            "order_id": order.order_id,
                                            "symbol": order.asset.symbol,
                                            "event": format!("{:?}", event),
                                        })),
                                        created_at: Some(chrono::Utc::now()),
                                    };
                                    if let Err(error) = persist_strategy_event(
                                        client,
                                        a,
                                        trade_event_record.clone(),
                                    ).await {
                                        if Self::is_transient_surreal_error(&error) {
                                            Self::enqueue_pending_aqs_sync_op(
                                                &mut pending_sync_ops,
                                                PendingAqsSyncOp::StrategyEvent(trade_event_record),
                                            );
                                            error!(
                                                "Live sync lost AQS connection for strategy {} while persisting trade event: {}",
                                                a.strategy_id, error
                                            );
                                            Self::reconnect_live_sync(&mut db, &mut action_stream, a).await;
                                            continue;
                                        }
                                        error!(
                                            "Failed to persist trade event for strategy {}: {}",
                                            a.strategy_id, error
                                        );
                                    }
                                }
                                Err(error) => {
                                    error!(
                                        "Live sync lost AQS connection for strategy {} after trade event: {}",
                                        a.strategy_id, error
                                    );
                                    Self::reconnect_live_sync(&mut db, &mut action_stream, a).await;
                                    force_full_reconcile = true;
                                }
                            }
                        }
                    }
                }

                // Handle incoming market data bars
                Some(bar) = bar_rx.recv() => {
                    let symbol = bar.symbol.clone();
                    debug!("Live loop processing completed bar for {}", symbol);
                    self.broker.process_live_bar(&bar);
                    debug!("Live loop processed execution broker for {}", symbol);
                    self.on_trade_update();
                    if auth.is_none() {
                        let pruned_ids = self.insights.prune_terminal_insights_without_aqs();
                        for insight_id in pruned_ids {
                            synced_insight_states.remove(&insight_id.to_string());
                        }
                    }
                    debug!("Live loop processed trade events after execution step for {}", symbol);
                    self._on_bar(&mut strategy, &symbol, &BarData::Bars(vec![bar]));
                    debug!("Live loop completed _on_bar for {}", symbol);
                    if let Some(a) = auth.as_ref() {
                        if db.is_none() {
                            Self::reconnect_live_sync(&mut db, &mut action_stream, a).await;
                            force_full_reconcile = true;
                        }
                        if let Some(client) = db.as_ref() {
                            if let Err(error) = Self::flush_pending_aqs_sync_ops(client, a, &mut pending_sync_ops).await {
                                error!(
                                    "Live sync could not flush pending AQS ops for strategy {} after bar processing: {}",
                                    a.strategy_id, error
                                );
                                Self::reconnect_live_sync(&mut db, &mut action_stream, a).await;
                                continue;
                            }
                            match self
                                .sync_live_insights_to_aqs(client, a, &mut synced_insight_states, &mut pending_sync_ops, force_full_reconcile)
                                .await
                            {
                                Ok(persist_metrics_after_sync) => {
                                    force_full_reconcile = false;
                                    if persist_metrics_after_sync {
                                        if let Err(error) = self.persist_live_metrics_if_needed(client, a, &mut pending_sync_ops).await {
                                            error!(
                                                "Live sync lost AQS connection for strategy {} while persisting live metrics: {}",
                                                a.strategy_id, error
                                            );
                                            Self::reconnect_live_sync(&mut db, &mut action_stream, a).await;
                                            continue;
                                        }
                                    }
                                }
                                Err(error) => {
                                    error!(
                                        "Live sync lost AQS connection for strategy {} after bar processing: {}",
                                        a.strategy_id, error
                                    );
                                    Self::reconnect_live_sync(&mut db, &mut action_stream, a).await;
                                    force_full_reconcile = true;
                                }
                            }
                        }
                    }
                }

                // Periodically run insight pipeline
                _ = pipeline_interval.tick() => {
                    debug!("Live loop calling run_insight_pipeline");
                    self.run_insight_pipeline();
                    debug!("Live loop completed run_insight_pipeline");
                    if auth.is_none() {
                        let pruned_ids = self.insights.prune_terminal_insights_without_aqs();
                        for insight_id in pruned_ids {
                            synced_insight_states.remove(&insight_id.to_string());
                        }
                    }

                    // Optional Push to SurrealDB
                    if let Some(a) = auth.as_ref() {
                        if db.is_none() {
                            Self::reconnect_live_sync(&mut db, &mut action_stream, a).await;
                            force_full_reconcile = true;
                        }
                        if let Some(client) = db.as_ref() {
                            if let Err(error) = Self::flush_pending_aqs_sync_ops(client, a, &mut pending_sync_ops).await {
                                error!(
                                    "Live sync could not flush pending AQS ops for strategy {} during pipeline sync: {}",
                                    a.strategy_id, error
                                );
                                Self::reconnect_live_sync(&mut db, &mut action_stream, a).await;
                                continue;
                            }
                            match self
                                .sync_live_insights_to_aqs(client, a, &mut synced_insight_states, &mut pending_sync_ops, force_full_reconcile)
                                .await
                            {
                                Ok(persist_metrics_after_sync) => {
                                    force_full_reconcile = false;
                                    if persist_metrics_after_sync {
                                        if let Err(error) = self.persist_live_metrics_if_needed(client, a, &mut pending_sync_ops).await {
                                            error!(
                                                "Live sync lost AQS connection for strategy {} while persisting live metrics: {}",
                                                a.strategy_id, error
                                            );
                                            Self::reconnect_live_sync(&mut db, &mut action_stream, a).await;
                                            continue;
                                        }
                                    }

                                    if let Ok(account) = self.broker.get_account().await {
                                        let captured_at = chrono::Utc::now();
                                        self.live_metrics
                                            .update_equity(account.equity, captured_at);
                                        if let Err(error) = persist_live_account_state(client, a, &account, captured_at).await {
                                            if Self::is_transient_surreal_error(&error) {
                                                Self::enqueue_pending_aqs_sync_op(
                                                    &mut pending_sync_ops,
                                                    PendingAqsSyncOp::AccountState {
                                                        account: account.clone(),
                                                        captured_at,
                                                    },
                                                );
                                                error!(
                                                    "Live sync lost AQS connection for strategy {} while persisting account state: {}",
                                                    a.strategy_id, error
                                                );
                                                Self::reconnect_live_sync(&mut db, &mut action_stream, a).await;
                                                continue;
                                            }
                                            error!(
                                                "Failed to persist live account state for strategy {}: {}",
                                                a.strategy_id, error
                                            );
                                        }
                                        if let Err(error) = self.persist_live_metrics_if_needed(client, a, &mut pending_sync_ops).await {
                                            error!(
                                                "Live sync lost AQS connection for strategy {} while persisting live metrics: {}",
                                                a.strategy_id, error
                                            );
                                            Self::reconnect_live_sync(&mut db, &mut action_stream, a).await;
                                            continue;
                                        }
                                    }
                                }
                                Err(error) => {
                                    error!(
                                        "Live sync lost AQS connection for strategy {} during pipeline sync: {}",
                                        a.strategy_id, error
                                    );
                                    Self::reconnect_live_sync(&mut db, &mut action_stream, a).await;
                                    force_full_reconcile = true;
                                }
                            }
                        }
                    }
                }

                _ = reconcile_interval.tick() => {
                    if let Some(a) = auth.as_ref() {
                        if db.is_none() {
                            Self::reconnect_live_sync(&mut db, &mut action_stream, a).await;
                            force_full_reconcile = true;
                        }
                        if let Some(client) = db.as_ref() {
                            if let Err(error) = Self::flush_pending_aqs_sync_ops(client, a, &mut pending_sync_ops).await {
                                error!(
                                    "Live sync could not flush pending AQS ops for strategy {} during reconcile: {}",
                                    a.strategy_id, error
                                );
                                Self::reconnect_live_sync(&mut db, &mut action_stream, a).await;
                                force_full_reconcile = true;
                                continue;
                            }
                            match self
                                .sync_live_insights_to_aqs(client, a, &mut synced_insight_states, &mut pending_sync_ops, true)
                                .await
                            {
                                Ok(_) => {
                                    force_full_reconcile = false;
                                }
                                Err(error) => {
                                    error!(
                                        "Live sync lost AQS connection for strategy {} during periodic reconcile: {}",
                                        a.strategy_id, error
                                    );
                                    Self::reconnect_live_sync(&mut db, &mut action_stream, a).await;
                                    force_full_reconcile = true;
                                }
                            }
                        }
                    }
                }

                notification = async {
                    match &mut action_stream {
                        Some(stream) => stream.next().await,
                        None => None,
                    }
                }, if action_stream.is_some() => {
                    match notification {
                        Some(Ok(notification)) => {
                            let action = notification.data;
                            let action_json = action.clone().into_json_value();
                            let action_name = action_json.get("action").and_then(|value| value.as_str());
                            let action_status = action_json.get("status").and_then(|value| value.as_str());
                            let action_id = action_json.get("id").and_then(action_id_from_value);

                            if action_status == Some("pending") && action_name == Some("stop") {
                                if let (Some(client), Some(a), Some(action_id)) = (&db, &auth, action_id) {
                                    update_strategy_action_status(
                                        client,
                                        &action_id,
                                        "acknowledged",
                                        Some("Stop requested by AQS".to_string()),
                                        None,
                                        "acknowledged_at",
                                    )
                                    .await;
                                    let _ = persist_strategy_event(
                                        client,
                                        a,
                                        StrategyEventRecord {
                                            event_type: "action".into(),
                                            level: "warn".into(),
                                            title: "Stop requested".into(),
                                            message: "AQE received a remote stop action".into(),
                                            payload: Some(action_json.clone()),
                                            created_at: Some(chrono::Utc::now()),
                                        },
                                    )
                                    .await;
                                    stop_action_id = Some(action_id.clone());
                                }

                                self.status = StrategyStatus::Stopping;
                                warn!("Received remote stop action from AQS");
                                self.shutdown();
                            }
                        }
                        Some(Err(error)) => {
                            error!("Strategy action live stream error: {}", error);
                            if let Some(a) = auth.as_ref() {
                                Self::reconnect_live_sync(&mut db, &mut action_stream, a).await;
                            }
                        }
                        None => {}
                    }
                }

                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Shutdown signal received, exiting live loop");
                        break;
                    }
                }

                _ = &mut ctrl_c => {
                    warn!("Process interrupt received, stopping live strategy");
                    self.status = StrategyStatus::Stopping;
                    self.shutdown();
                    break;
                }

                else => {
                    break;
                }
            }
        }

        // Teardown
        debug!("Invoking strategy on_teardown");
        strategy.on_teardown(self);

        // Clean up subscriptions
        let _ = self.broker.unsubscribe_from_trade_stream().await;
        let _ = self.broker.unsubscribe_from_data_stream(symbols).await;
        let _ = self.broker.disconnect().await;

        self.strategy = Some(strategy);
        self.status = StrategyStatus::Stopped;

        if let (Some(client), Some(a)) = (&db, &auth) {
            let _: Result<IndexedResults, surrealdb::Error> = client
                .query("UPDATE type::record('strategy', $id) SET status = 'Stopped', is_live = false, last_heartbeat = time::now()")
                .bind(("id", a.strategy_id.clone()))
                .await;
            let _ = client
                .query(
                    "UPDATE type::record('live_strategy_session', <uuid>$live_session_key)
                     SET status = 'completed',
                         last_used_at = time::now()",
                )
                .bind(("live_session_key", Self::live_session_key_for_auth(a)))
                .await
                .and_then(|response| response.check());

            if let Ok(account) = self.broker.get_account().await {
                let captured_at = chrono::Utc::now();
                self.live_metrics.update_equity(account.equity, captured_at);
                let _ = persist_live_account_state(client, a, &account, captured_at).await;
            }

            self.live_metrics.finish(chrono::Utc::now());
            let _ = self
                .persist_live_metrics_if_needed(client, a, &mut pending_sync_ops)
                .await;

            if let Some(action_id) = stop_action_id {
                update_strategy_action_status(
                    client,
                    &action_id,
                    "completed",
                    Some("Strategy stopped gracefully".to_string()),
                    None,
                    "completed_at",
                )
                .await;
            }

            let _ = persist_strategy_event(
                client,
                a,
                StrategyEventRecord {
                    event_type: "lifecycle".into(),
                    level: "info".into(),
                    title: "Strategy stopped".into(),
                    message: format!("Strategy '{}' exited live mode", self.name),
                    payload: None,
                    created_at: Some(chrono::Utc::now()),
                },
            )
            .await;
        }

        Ok(())
    }
}

impl<S, E, D> StrategyState<S, E, D>
where
    S: Strategy + Send + Sync + 'static,
    E: Broker + OrderManagementProvider + Send + Sync + 'static,
    D: DataFeed + DataProvider + Send + Sync + 'static,
{
    async fn start<C: surrealdb::Connection>(
        &mut self,
        strategy: &mut S,
        client: Option<&surrealdb::Surreal<C>>,
        auth: Option<&AqsAuth>,
        pending_ops: &mut VecDeque<PendingAqsSyncOp>,
    ) -> Result<(), BrokerError> {
        debug!("Invoking strategy on_start");
        strategy.on_start(self);
        self.load_universe(strategy).await;

        assert!(
            !self.universe.is_empty(),
            "Universe is empty — strategy.universe() must return at least one symbol"
        );

        self.load_alpha_models();

        // Initial sync to SurrealDB after the universe is loaded and strategy has started, so the
        // dashboard has the initial state to work with (universe, account, etc.)
        let mut initial_universe_assets: Vec<StrategyUniverseAssetRecord> = self
            .universe
            .values()
            .map(StrategyUniverseAssetRecord::from)
            .collect();
        initial_universe_assets.sort_by(|left, right| left.symbol.cmp(&right.symbol));
        initial_universe_assets.dedup_by(|left, right| left.symbol == right.symbol);
        let initial_universe_symbols: Vec<String> = initial_universe_assets
            .iter()
            .map(|asset| asset.symbol.clone())
            .collect();
        let initial_account = self.broker.get_account().await.ok();
        let executed_at = chrono::Utc::now();
        let starting_cash = initial_account
            .as_ref()
            .map(|account| account.cash)
            .unwrap_or_default();
        let current_equity = initial_account
            .as_ref()
            .map(|account| account.equity)
            .unwrap_or(starting_cash);
        self.live_metrics.initialize(
            starting_cash,
            current_equity,
            executed_at,
            initial_universe_symbols,
        );

        if let (Some(client), Some(auth)) = (client, auth) {
            if let Err(error) = mark_strategy_started(
                client,
                auth,
                &initial_universe_assets,
                initial_account.as_ref(),
            )
            .await
            {
                if Self::is_transient_surreal_error(&error) {
                    Self::enqueue_pending_aqs_sync_op(
                        pending_ops,
                        PendingAqsSyncOp::StrategyStarted {
                            universe: initial_universe_assets.clone(),
                            account: initial_account.clone(),
                        },
                    );
                } else {
                    error!(
                        "Failed to mark strategy {} as started in AQS: {}",
                        auth.strategy_id, error
                    );
                }
            }
            let _ = self
                .persist_live_metrics_if_needed(client, auth, pending_ops)
                .await;
        }

        Ok(())
    }
}
