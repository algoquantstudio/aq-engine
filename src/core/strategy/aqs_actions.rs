//! Direct SurrealDB action handling retained by AQE.
//!
//! Strategy, insight, account and metrics projections must not be added here; those flow through
//! the runtime host and event gateway.

use super::aqs_types::StrategyActionStatus;
use futures::StreamExt;

pub(crate) async fn update_strategy_action_statuses<C: surrealdb::Connection>(
    client: &surrealdb::Surreal<C>,
    ids: &std::collections::HashSet<String>,
    status: StrategyActionStatus,
    message: &str,
) {
    // A burst of repeated Stop actions must not multiply the shutdown budget.
    // Durable acknowledgements have already been queued for every accepted ID.
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        futures::stream::iter(ids).for_each_concurrent(8, |id| {
            update_strategy_action_status(
                client,
                id,
                status,
                Some(message.into()),
                None,
                "completed_at",
            )
        }),
    )
    .await;
}

pub(crate) async fn update_strategy_action_status<C: surrealdb::Connection>(
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

    // This direct write is a compatibility acknowledgement, not the durable
    // lifecycle path. An offline DB must not hold up broker teardown.
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        client
            .query(query)
            .bind(("id", action_id.to_string()))
            .bind(("status", status.to_string()))
            .bind(("message", message))
            .bind(("error", error))
            .await
            .and_then(|response| response.check())
    })
    .await;
    if !matches!(result, Ok(Ok(_))) {
        log::warn!(
            "Direct strategy action update unavailable; durable lifecycle publication remains authoritative"
        );
    }
}
