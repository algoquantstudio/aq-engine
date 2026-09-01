use super::aqs_types::{
    AqsAuth, StrategyAccountSnapshotRecord, StrategyActionStatus, StrategyEquityPointRecord,
    StrategyEventLevel, StrategyEventRecord, StrategyEventTitle, StrategyEventType,
    StrategyLiveMetricsRecord, StrategyUniverseAssetRecord, live_session_key,
};
use super::live_metrics::HistoricalClosedInsightMetric;
use crate::core::broker::types::Account;
use chrono::{DateTime, Utc};
use log::debug;
use serde::Deserialize;
use surrealdb::IndexedResults;
use surrealdb::types::SurrealValue;
use uuid::Uuid;

const STARTUP_INSIGHT_CLEANUP_QUERY: &str =
    "BEGIN TRANSACTION;
     FOR $item IN $items {
         UPDATE type::record($item.record_id)
         SET state = $item.target_state,
             cancelling = false,
             closing = false,
             first_on_fill = false,
             updated_at = <datetime>$item.created_at,
             closed_at = <datetime>$item.created_at
         WHERE (
               ($item.target_state = 'Closed' AND state IN ['New', 'Executed', 'Filled'])
               OR ($item.target_state = 'Cancelled' AND state IN ['New', 'Executed'])
           );

         UPSERT type::record('strategy_events', $item.event_record_id)
         CONTENT {
             user_id: <record<users>> $user_id,
             strategy_id: type::record('strategy', $strategy_id),
             live_session_id: IF $live_session_key = '' THEN NONE ELSE type::record('live_strategy_session', <uuid>$live_session_key) END,
             event_type: $item.event_type,
             level: $item.level,
             title: $item.title,
             message: $item.message,
             payload: IF $item.payload = NONE OR $item.payload = NULL THEN NONE ELSE $item.payload END,
             created_at: <datetime>$item.created_at
         };
     };
     COMMIT TRANSACTION;";

#[derive(Clone, Debug)]
pub(crate) struct StartupInsightCleanupWrite {
    pub record_id: String,
    pub target_state: String,
    pub event: StrategyEventRecord,
}

fn strategy_event_record_id(
    auth: &AqsAuth,
    live_session_key: &str,
    event: &StrategyEventRecord,
    created_at: DateTime<Utc>,
) -> String {
    let event_key = format!(
        "{}::{}::{}::{}::{}::{}",
        auth.strategy_id,
        live_session_key,
        created_at.timestamp_millis(),
        event.event_type,
        event.title,
        event.message
    );
    Uuid::new_v5(&Uuid::NAMESPACE_OID, event_key.as_bytes()).to_string()
}

pub async fn persist_strategy_event<C: surrealdb::Connection>(
    client: &surrealdb::Surreal<C>,
    auth: &AqsAuth,
    event: StrategyEventRecord,
) -> Result<(), surrealdb::Error> {
    let live_session_key = auth
        .live_session_id
        .as_deref()
        .map(live_session_key)
        .unwrap_or_default();
    let created_at = event.created_at.unwrap_or_else(Utc::now);
    let event_record_id = strategy_event_record_id(auth, &live_session_key, &event, created_at);

    client
        .query(
            "UPSERT type::record('strategy_events', $event_record_id)
             CONTENT {
                user_id: <record<users>> $user_id,
                strategy_id: type::record('strategy', $strategy_id),
                live_session_id: IF $live_session_key = '' THEN NONE ELSE type::record('live_strategy_session', <uuid>$live_session_key) END,
                event_type: $event_type,
                level: $level,
                title: $title,
                message: $message,
                payload: IF $payload = NONE OR $payload = NULL THEN NONE ELSE $payload END,
                created_at: IF $created_at = NONE THEN time::now() ELSE <datetime>$created_at END
            }",
        )
        .bind(("event_record_id", event_record_id))
        .bind(("user_id", auth.user_id.clone()))
        .bind(("strategy_id", auth.strategy_id.clone()))
        .bind(("live_session_key", live_session_key))
        .bind(("event_type", event.event_type.to_string()))
        .bind(("level", event.level.to_string()))
        .bind(("title", event.title.to_string()))
        .bind(("message", event.message))
        .bind(("payload", event.payload))
        .bind(("created_at", event.created_at))
        .await
        .and_then(|response| response.check())?;

    Ok(())
}

pub(crate) async fn persist_startup_insight_cleanup_batch<C: surrealdb::Connection>(
    client: &surrealdb::Surreal<C>,
    auth: &AqsAuth,
    writes: &[StartupInsightCleanupWrite],
) -> Result<(), surrealdb::Error> {
    if writes.is_empty() {
        return Ok(());
    }

    let live_session_key = auth
        .live_session_id
        .as_deref()
        .map(live_session_key)
        .unwrap_or_default();
    let items = writes
        .iter()
        .map(|write| {
            let created_at = write.event.created_at.unwrap_or_else(Utc::now);
            serde_json::json!({
                "record_id": write.record_id,
                "target_state": write.target_state,
                "event_record_id": strategy_event_record_id(
                    auth,
                    &live_session_key,
                    &write.event,
                    created_at,
                ),
                "event_type": write.event.event_type.to_string(),
                "level": write.event.level.to_string(),
                "title": write.event.title.to_string(),
                "message": write.event.message,
                "payload": write.event.payload,
                "created_at": created_at,
            })
        })
        .collect::<Vec<_>>();

    client
        .query(STARTUP_INSIGHT_CLEANUP_QUERY)
        .bind(("items", items))
        .bind(("user_id", auth.user_id.clone()))
        .bind(("strategy_id", auth.strategy_id.clone()))
        .bind(("live_session_key", live_session_key))
        .await
        .and_then(|response| response.check())?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::STARTUP_INSIGHT_CLEANUP_QUERY;

    #[test]
    fn startup_cleanup_updates_the_selected_record_directly() {
        assert!(STARTUP_INSIGHT_CLEANUP_QUERY.contains("UPDATE type::record($item.record_id)"));
        assert!(!STARTUP_INSIGHT_CLEANUP_QUERY.contains("UPDATE insights"));
        assert!(!STARTUP_INSIGHT_CLEANUP_QUERY.contains("AND insight_id ="));
    }
}

pub async fn mark_strategy_started<C: surrealdb::Connection>(
    client: &surrealdb::Surreal<C>,
    auth: &AqsAuth,
    universe: &[StrategyUniverseAssetRecord],
    account: Option<&Account>,
) -> Result<(), surrealdb::Error> {
    let live_session_key = auth
        .live_session_id
        .as_deref()
        .map(live_session_key)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| auth.session_id.clone());

    let starting_cash = account.map(|value| value.cash);
    client
        .query(
            "UPDATE type::record('strategy', $id)
             SET status = 'Running',
                 is_live = true,
                 last_heartbeat = time::now(),
                 universe = $universe,
                 live_session_id = type::record('live_strategy_session', <uuid>$live_session_key),
                 runtime_config.startTime = <string>time::now(),
                 runtime_config.endTime = NONE,
                 runtime_config.startingCash = $starting_cash",
        )
        .bind(("id", auth.strategy_id.clone()))
        .bind(("universe", universe.to_vec()))
        .bind(("live_session_key", live_session_key))
        .bind(("starting_cash", starting_cash))
        .await
        .and_then(|response| response.check())?;

    Ok(())
}

pub async fn update_strategy_action_status<C: surrealdb::Connection>(
    client: &surrealdb::Surreal<C>,
    action_id: &str,
    status: StrategyActionStatus,
    message: Option<String>,
    error: Option<String>,
    timestamp_field: &str,
) {
    let query = format!(
        "UPDATE <record<strategy_actions>> $id
         SET status = $status,
             message = $message,
             error = $error,
             processed_at = time::now(),
             {} = time::now()",
        timestamp_field
    );

    let _: Result<IndexedResults, surrealdb::Error> = client
        .query(query)
        .bind(("id", action_id.to_string()))
        .bind(("status", status.to_string()))
        .bind(("message", message))
        .bind(("error", error))
        .await;
}

pub async fn persist_live_account_state<C: surrealdb::Connection>(
    client: &surrealdb::Surreal<C>,
    auth: &AqsAuth,
    account: &Account,
    captured_at: DateTime<Utc>,
    write_latest: bool,
    write_history: bool,
) -> Result<(), surrealdb::Error> {
    if !write_latest && !write_history {
        return Ok(());
    }

    let account_snapshot = StrategyAccountSnapshotRecord {
        account_id: account.account_id.trim().to_string(),
        account_type: format!("{:?}", account.account_type),
        equity: account.equity,
        cash: account.cash,
        currency: account.currency.trim().to_uppercase(),
        buying_power: account.buying_power,
        accrued_commission: account.accrued_commission,
        shorting_enabled: account.shorting_enabled,
        leverage: account.leverage as i64,
    };
    let equity_point = StrategyEquityPointRecord {
        account_id: account.account_id.trim().to_string(),
        currency: account.currency.trim().to_uppercase(),
        equity: account.equity,
        cash: account.cash,
        buying_power: account.buying_power,
        accrued_commission: account.accrued_commission,
    };
    let live_session_key = auth
        .live_session_id
        .as_deref()
        .map(live_session_key)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| auth.session_id.clone());
    let account_record_id = format!(
        "{}::{}::{}",
        auth.strategy_id, live_session_key, account_snapshot.account_id
    );
    let equity_point_record_id = format!(
        "{}::{}::{}::{}",
        auth.strategy_id,
        live_session_key,
        equity_point.account_id,
        captured_at.timestamp_millis()
    );

    if write_latest {
        debug!(
            "AQS live sync: account state changed for strategy {} -> equity {:.2}, cash {:.2}, buying power {:.2}",
            auth.strategy_id, account.equity, account.cash, account.buying_power
        );

        client
            .query(
                "UPSERT type::record('strategy_accounts', $account_record_id)
                 MERGE {
                    user_id: <record<users>> $user_id,
                    strategy_id: type::record('strategy', $strategy_id),
                    live_session_id: type::record('live_strategy_session', <uuid>$live_session_key),
                    account_id: $snapshot.account_id,
                    account_type: $snapshot.account_type,
                    equity: $snapshot.equity,
                    cash: $snapshot.cash,
                    currency: $snapshot.currency,
                    buying_power: $snapshot.buying_power,
                    accrued_commission: $snapshot.accrued_commission,
                    shorting_enabled: $snapshot.shorting_enabled,
                    leverage: $snapshot.leverage,
                    created_at: <datetime>$captured_at,
                    captured_at: <datetime>$captured_at
                 }
                 RETURN AFTER",
            )
            .bind(("user_id", auth.user_id.clone()))
            .bind(("strategy_id", auth.strategy_id.clone()))
            .bind(("live_session_key", live_session_key.clone()))
            .bind(("account_record_id", account_record_id))
            .bind(("captured_at", captured_at))
            .bind((
                "snapshot",
                serde_json::to_value(&account_snapshot).unwrap_or_default(),
            ))
            .await
            .and_then(|response| response.check())?;

        debug!(
            "AQS live sync: wrote strategy_accounts snapshot for strategy {}",
            auth.strategy_id
        );
    }

    if !write_history {
        return Ok(());
    }

    let equity_result: Result<IndexedResults, surrealdb::Error> = client
        .query(
            "UPSERT type::record('strategy_equity_points', $equity_point_record_id)
             CONTENT {
                user_id: <record<users>> $user_id,
                strategy_id: type::record('strategy', $strategy_id),
                live_session_id: type::record('live_strategy_session', <uuid>$live_session_key),
                account_id: $point.account_id,
                currency: $point.currency,
                timestamp: <datetime>$captured_at,
                equity: $point.equity,
                cash: $point.cash,
                buying_power: $point.buying_power,
                accrued_commission: $point.accrued_commission
             }",
        )
        .bind(("user_id", auth.user_id.clone()))
        .bind(("strategy_id", auth.strategy_id.clone()))
        .bind(("live_session_key", live_session_key))
        .bind(("equity_point_record_id", equity_point_record_id))
        .bind(("captured_at", captured_at))
        .bind((
            "point",
            serde_json::to_value(&equity_point).unwrap_or_default(),
        ))
        .await
        .and_then(|response| response.check());

    if let Err(error) = equity_result {
        return Err(error);
    }

    persist_strategy_event(
        client,
        auth,
        StrategyEventRecord {
            event_type: StrategyEventType::AccountSnapshot,
            level: StrategyEventLevel::Debug,
            title: StrategyEventTitle::AccountSnapshotRecorded,
            message: format!(
                "Equity {:.2}, cash {:.2}, buying power {:.2}, accrued commission {:.2}",
                account.equity, account.cash, account.buying_power, account.accrued_commission
            ),
            payload: Some(serde_json::json!({
                "equity": account.equity,
                "cash": account.cash,
                "buying_power": account.buying_power,
                "accrued_commission": account.accrued_commission,
            })),
            created_at: Some(captured_at),
        },
    )
    .await?;

    Ok(())
}

pub async fn persist_live_metrics<C: surrealdb::Connection>(
    client: &surrealdb::Surreal<C>,
    auth: &AqsAuth,
    metrics: StrategyLiveMetricsRecord,
) -> Result<(), surrealdb::Error> {
    let live_session_key = auth
        .live_session_id
        .as_deref()
        .map(live_session_key)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| auth.session_id.clone());
    let record_id = format!("{}::{}", auth.strategy_id, live_session_key);

    client
        .query(
            "UPSERT type::record('strategy_live_metrics', $record_id)
             CONTENT {
                user_id: <record<users>> $user_id,
                strategy_id: type::record('strategy', $strategy_id),
                live_session_id: type::record('live_strategy_session', <uuid>$live_session_key),
                starting_cash: $metrics.starting_cash,
                final_equity: $metrics.final_equity,
                total_return: $metrics.total_return,
                total_return_pct: $metrics.total_return_pct,
                total_trades: $metrics.total_trades,
                winning_trades: $metrics.winning_trades,
                losing_trades: $metrics.losing_trades,
                win_rate: $metrics.win_rate,
                max_drawdown: $metrics.max_drawdown,
                cagr: $metrics.cagr,
                annualized_volatility: $metrics.annualized_volatility,
                sharpe_ratio: $metrics.sharpe_ratio,
                sortino_ratio: $metrics.sortino_ratio,
                calmar_ratio: $metrics.calmar_ratio,
                max_drawdown_duration_days: $metrics.max_drawdown_duration_days,
                expectancy: $metrics.expectancy,
                profit_factor: $metrics.profit_factor,
                payoff_ratio: $metrics.payoff_ratio,
                avg_winner: $metrics.avg_winner,
                avg_loser: $metrics.avg_loser,
                avg_winner_pct: $metrics.avg_winner_pct,
                avg_loser_pct: $metrics.avg_loser_pct,
                best_trade: $metrics.best_trade,
                worst_trade: $metrics.worst_trade,
                consistency_score: $metrics.consistency_score,
                longest_winning_trade_held_secs: $metrics.longest_winning_trade_held_secs,
                longest_losing_trade_held_secs: $metrics.longest_losing_trade_held_secs,
                average_trade_held_secs: $metrics.average_trade_held_secs,
                open_positions_count: $metrics.open_positions_count,
                open_insights_count: $metrics.open_insights_count,
                open_positions_unrealized_pnl: $metrics.open_positions_unrealized_pnl,
                open_positions_profitable_count: $metrics.open_positions_profitable_count,
                open_positions_losing_count: $metrics.open_positions_losing_count,
                symbols: $metrics.symbols,
                executed_at: <datetime>$metrics.executed_at,
                finished_at: IF $metrics.finished_at = NONE OR $metrics.finished_at = NULL THEN NONE ELSE <datetime>$metrics.finished_at END,
                updated_at: <datetime>$metrics.updated_at,
                tracker_checkpoint: IF $metrics.tracker_checkpoint = NONE OR $metrics.tracker_checkpoint = NULL THEN NONE ELSE $metrics.tracker_checkpoint END
             }",
        )
        .bind(("record_id", record_id))
        .bind(("user_id", auth.user_id.clone()))
        .bind(("strategy_id", auth.strategy_id.clone()))
        .bind(("live_session_key", live_session_key))
        .bind(("metrics", serde_json::to_value(metrics).unwrap_or_default()))
        .await
        .and_then(|response| response.check())?;

    Ok(())
}

#[derive(Clone, Debug, Deserialize, SurrealValue)]
pub(crate) struct MetricAccountOrigin {
    pub account_id: String,
    pub cash: f64,
    pub captured_at: DateTime<Utc>,
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
            "SELECT account_id, cash, captured_at
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
            "SELECT account_id, cash, captured_at
             FROM strategy_accounts
             WHERE user_id = <record<users>> $user_id
               AND strategy_id = type::record('strategy', $strategy_id)
               AND string::trim(account_id) = $account_id
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
