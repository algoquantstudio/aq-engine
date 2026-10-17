//! Read-only AQS Cloud queries used by AQE startup continuity.
//!
//! AQE projections are emitted through the local event bus. This module intentionally contains
//! no database writes; the runtime host and event gateway own projection persistence.

use super::aqs_types::{AqsAuth, StrategyLiveMetricsRecord};
use super::types::live_metrics::HistoricalClosedInsightMetric;
use chrono::{DateTime, Utc};
use surrealdb::IndexedResults;
use surrealdb::types::SurrealValue;

#[derive(Clone, Debug, surrealdb::types::SurrealValue)]
pub(crate) struct MetricAccountOrigin {
    pub account_id: String,
    pub currency: String,
    pub equity: f64,
    pub captured_at: DateTime<Utc>,
}

/// O(1) strategy-row lookup. The gateway captures this once from broker
/// evidence; editor startingCash and the mutable latest-account row are not
/// continuity proof. Shared by local and remote AQE deployment.
pub(crate) async fn load_strategy_account_baseline<C: surrealdb::Connection>(
    client: &surrealdb::Surreal<C>,
    auth: &AqsAuth,
) -> Result<Option<MetricAccountOrigin>, surrealdb::Error> {
    let mut result: IndexedResults = client
        .query(
            "SELECT VALUE live_account_baseline FROM ONLY type::record('strategy', $strategy_id)
             WHERE user_id = <record<users>> $user_id",
        )
        .bind(("user_id", auth.user_id.clone()))
        .bind(("strategy_id", auth.strategy_id.clone()))
        .await?
        .check()?;
    result.take(0)
}

pub(crate) async fn load_latest_live_metrics<C: surrealdb::Connection>(
    client: &surrealdb::Surreal<C>,
    auth: &AqsAuth,
) -> Result<Option<StrategyLiveMetricsRecord>, surrealdb::Error> {
    let mut result: IndexedResults = client
        .query(
            "SELECT * OMIT id, user_id, strategy_id, live_session_id
             FROM strategy_live_metrics
             WHERE user_id = <record<users>> $user_id
               AND strategy_id = type::record('strategy', $strategy_id)
             ORDER BY updated_at DESC
             LIMIT 1",
        )
        .bind(("user_id", auth.user_id.clone()))
        .bind(("strategy_id", auth.strategy_id.clone()))
        .await?
        .check()?;
    result.take(0)
}

pub(crate) async fn load_latest_metric_account_origin<C: surrealdb::Connection>(
    client: &surrealdb::Surreal<C>,
    auth: &AqsAuth,
) -> Result<Option<MetricAccountOrigin>, surrealdb::Error> {
    let mut result: IndexedResults = client
        .query(
            "SELECT account_id, currency, equity, captured_at
             FROM strategy_accounts
             WHERE user_id = <record<users>> $user_id
               AND strategy_id = type::record('strategy', $strategy_id)
             ORDER BY captured_at DESC
             LIMIT 1",
        )
        .bind(("user_id", auth.user_id.clone()))
        .bind(("strategy_id", auth.strategy_id.clone()))
        .await?
        .check()?;
    result.take(0)
}

pub(crate) async fn load_earliest_metric_account_origin<C: surrealdb::Connection>(
    client: &surrealdb::Surreal<C>,
    auth: &AqsAuth,
    account_id: &str,
) -> Result<Option<MetricAccountOrigin>, surrealdb::Error> {
    let mut result: IndexedResults = client
        .query(
            "SELECT account_id, currency, equity, timestamp AS captured_at
             FROM strategy_equity_points
             WHERE user_id = <record<users>> $user_id
               AND strategy_id = type::record('strategy', $strategy_id)
               AND string::trim(account_id) = $account_id
               AND equity > 0 AND equity < math::inf
             ORDER BY captured_at ASC
             LIMIT 1",
        )
        .bind(("user_id", auth.user_id.clone()))
        .bind(("strategy_id", auth.strategy_id.clone()))
        .bind(("account_id", account_id.trim().to_string()))
        .await?
        .check()?;
    result.take(0)
}

pub(crate) async fn load_closed_insight_metric_page<C: surrealdb::Connection>(
    client: &surrealdb::Surreal<C>,
    auth: &AqsAuth,
    start: usize,
    limit: usize,
) -> Result<Vec<HistoricalClosedInsightMetric>, surrealdb::Error> {
    let mut result: IndexedResults = client
        .query(
            "SELECT insight_id, side, quantity, limit_price, stop_price, filled_price, close_price,
                    broker_realized_pnl, commission, swap, created_at, updated_at,
                    filled_at, closed_at
             FROM insights
             WHERE strategy_id = type::record('strategy', $strategy_id)
               AND state = 'Closed'
             ORDER BY closed_at ASC, created_at ASC, insight_id ASC
             LIMIT $limit START $start",
        )
        .bind(("strategy_id", auth.strategy_id.clone()))
        .bind(("limit", limit))
        .bind(("start", start))
        .await?
        .check()?;
    result.take(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_baseline_reader_accepts_persisted_cash_and_provenance() {
        let at = Utc::now();
        let mut object = surrealdb::types::Object::new();
        object.insert("account_id", "account".to_string());
        object.insert("currency", "GBP".to_string());
        object.insert("equity", 900.0);
        object.insert("cash", 1000.0);
        object.insert("captured_at", at);
        object.insert("source", "startup_account".to_string());
        let origin =
            MetricAccountOrigin::from_value(surrealdb::types::Value::Object(object)).unwrap();
        assert_eq!(origin.account_id, "account");
        assert_eq!(origin.currency, "GBP");
        assert_eq!(origin.equity, 900.0);
        assert_eq!(origin.captured_at, at);
    }
}
