use super::aqs_types::{
    AqsAuth, LatestPersistedAccountState, StrategyAccountSnapshotRecord, StrategyEquityPointRecord,
    StrategyEventRecord, StrategyLiveMetricsRecord, StrategyUniverseAssetRecord, live_session_key,
};
use crate::core::broker::types::Account;
use chrono::{DateTime, Utc};
use log::{debug, info};
use surrealdb::IndexedResults;
use uuid::Uuid;

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
    let event_key = format!(
        "{}::{}::{}::{}::{}::{}",
        auth.strategy_id,
        live_session_key,
        created_at.timestamp_millis(),
        event.event_type,
        event.title,
        event.message
    );
    let event_record_id = Uuid::new_v5(&Uuid::NAMESPACE_OID, event_key.as_bytes()).to_string();

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
        .bind(("event_type", event.event_type))
        .bind(("level", event.level))
        .bind(("title", event.title))
        .bind(("message", event.message))
        .bind(("payload", event.payload))
        .bind(("created_at", event.created_at))
        .await
        .and_then(|response| response.check())?;

    Ok(())
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
    status: &str,
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
) -> Result<(), surrealdb::Error> {
    let account_snapshot = StrategyAccountSnapshotRecord {
        account_id: account.account_id.clone(),
        account_type: format!("{:?}", account.account_type),
        equity: account.equity,
        cash: account.cash,
        currency: account.currency.clone(),
        buying_power: account.buying_power,
        shorting_enabled: account.shorting_enabled,
        leverage: account.leverage as i64,
    };
    let equity_point = StrategyEquityPointRecord {
        equity: account.equity,
        cash: account.cash,
        buying_power: account.buying_power,
    };
    let live_session_key = auth
        .live_session_id
        .as_deref()
        .map(live_session_key)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| auth.session_id.clone());
    let account_record_id = format!(
        "{}::{}::{}",
        auth.strategy_id, live_session_key, account.account_id
    );
    let equity_point_record_id = format!(
        "{}::{}::{}::{}",
        auth.strategy_id,
        live_session_key,
        account.account_id,
        captured_at.timestamp_millis()
    );

    let latest_state: Option<LatestPersistedAccountState> = client
        .query(
            "SELECT equity, cash, buying_power
             FROM type::record('strategy_accounts', $account_record_id)
             LIMIT 1",
        )
        .bind(("account_record_id", account_record_id.clone()))
        .await
        .ok()
        .and_then(|mut result: IndexedResults| result.take::<Vec<serde_json::Value>>(0).ok())
        .and_then(|rows| rows.into_iter().next())
        .and_then(|row| {
            Some(LatestPersistedAccountState {
                equity: row.get("equity")?.as_f64()?,
                cash: row.get("cash")?.as_f64()?,
                buying_power: row.get("buying_power")?.as_f64()?,
            })
        });

    let account_changed = !latest_state.as_ref().is_some_and(|latest| {
        latest.equity == account.equity
            && latest.cash == account.cash
            && latest.buying_power == account.buying_power
    });

    if account_changed {
        info!(
            "AQS live sync: account state changed for strategy {} -> equity {:.2}, cash {:.2}, buying power {:.2}",
            auth.strategy_id, account.equity, account.cash, account.buying_power
        );
    } else {
        debug!(
            "AQS live sync: refreshing latest strategy_accounts row for unchanged account state on strategy {}",
            auth.strategy_id
        );
    }

    let account_result: Result<IndexedResults, surrealdb::Error> = client
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
        .and_then(|response| response.check());

    match account_result {
        Ok(_) => info!(
            "AQS live sync: wrote strategy_accounts snapshot for strategy {}",
            auth.strategy_id
        ),
        Err(error) => {
            return Err(error);
        }
    }

    if !account_changed {
        return Ok(());
    }

    let equity_result: Result<IndexedResults, surrealdb::Error> = client
        .query(
            "UPSERT type::record('strategy_equity_points', $equity_point_record_id)
             CONTENT {
                user_id: <record<users>> $user_id,
                strategy_id: type::record('strategy', $strategy_id),
                live_session_id: type::record('live_strategy_session', <uuid>$live_session_key),
                timestamp: <datetime>$captured_at,
                equity: $point.equity,
                cash: $point.cash,
                buying_power: $point.buying_power
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
            event_type: "account_snapshot".into(),
            level: "debug".into(),
            title: "Account snapshot recorded".into(),
            message: format!(
                "Equity {:.2}, cash {:.2}, buying power {:.2}",
                account.equity, account.cash, account.buying_power
            ),
            payload: Some(serde_json::json!({
                "equity": account.equity,
                "cash": account.cash,
                "buying_power": account.buying_power,
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
                sharpe_ratio: $metrics.sharpe_ratio,
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
                updated_at: <datetime>$metrics.updated_at
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
