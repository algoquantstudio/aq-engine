//! Shared control forwarding for managed hosts and embedded standalone sync.
use super::{
    EventEnvelope, EventKind, EventPriority, EventSubject, HttpBatchSink, LocalEventBus,
    OutboxStore, RemoteBatchSink, RemoteStrategyAction, RemoteSyncErrorKind,
    deterministic_event_id,
};
use futures::StreamExt;
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::sync::watch;

pub async fn run_action_forwarder(
    runtime_id: String,
    strategy_id: String,
    live_session_id: String,
    run_id: String,
    bus: Arc<dyn LocalEventBus>,
    sink: Arc<HttpBatchSink>,
    outbox: OutboxStore,
    mut shutdown: watch::Receiver<bool>,
) {
    // Independently owned so a slow legacy database subscription cannot delay the
    // hosted control socket. Dropping this forwarder cancels its hosted worker too.
    let mut hosted_workers = tokio::task::JoinSet::new();
    {
        let gateway = sink.hosted_gateway();
        hosted_workers.spawn(super::hosted_controls::run(
            gateway,
            runtime_id.clone(),
            strategy_id.clone(),
            live_session_id.clone(),
            run_id.clone(),
            bus.clone(),
            outbox.clone(),
            shutdown.clone(),
        ));
    }
    let mut handled = HashSet::new();
    let mut recovery_backoff = Duration::from_secs(1);
    let mut recovery_sleep = Box::pin(tokio::time::sleep(Duration::ZERO));
    let subject = EventSubject::control(&runtime_id, &strategy_id);
    let mut live_subscription = match sink.subscribe_pending_actions().await {
        Ok(subscription) => Some(subscription),
        Err(error) => {
            eprintln!(
                "AQS action live stream unavailable for {strategy_id}: {error}; using recovery polling"
            );
            None
        }
    };
    loop {
        tokio::select! {
            notification = async {
                match live_subscription.as_mut() {
                    Some(subscription) => subscription.stream.next().await,
                    None => std::future::pending().await,
                }
            } => {
                match notification {
                    Some(Ok(notification)) => {
                        let value = notification.data.into_json_value();
                        match serde_json::from_value::<RemoteStrategyAction>(value.clone()).ok() {
                            Some(action) => {
                                let action_id = action.record_id().unwrap_or_else(|| "<missing-id>".to_string());
                                eprintln!("[action_id={action_id}] received and decoded from Surreal live stream for strategy {strategy_id}");
                                forward_action(
                                    &runtime_id,
                                    &strategy_id,
                                    &live_session_id,
                                    &run_id,
                                    &subject,
                                    bus.as_ref(),
                                    sink.as_ref(),
                                    &outbox,
                                    &mut handled,
                                    action,
                                ).await;
                            }
                            None => {
                                let action_id = value.get("id").map(ToString::to_string).unwrap_or_else(|| "<missing-id>".to_string());
                                eprintln!("[action_id={action_id}] AQS action live stream returned an invalid action for {strategy_id}");
                            }
                        }
                    }
                    Some(Err(error)) => {
                        eprintln!("AQS action live stream failed for {strategy_id}: {error}; recovery polling remains active");
                        live_subscription = None;
                        recovery_backoff = Duration::from_secs(1);
                        recovery_sleep.as_mut().reset(tokio::time::Instant::now());
                    }
                    None => {
                        eprintln!("AQS action live stream closed for {strategy_id}; recovery polling remains active");
                        live_subscription = None;
                        recovery_backoff = Duration::from_secs(1);
                        recovery_sleep.as_mut().reset(tokio::time::Instant::now());
                    }
                }
            }
            _ = &mut recovery_sleep => {
                let mut recovery_succeeded = true;
                let actions = match sink.fetch_pending_actions().await {
                    Ok(actions) => actions,
                    Err(error) if error.kind == RemoteSyncErrorKind::Authentication => {
                        recovery_succeeded = false;
                        if let Err(refresh_error) = sink.refresh_authentication().await {
                            eprintln!("AQS action authentication refresh failed for {strategy_id}: {refresh_error}");
                        }
                        live_subscription = None;
                        Vec::new()
                    }
                    Err(error) => {
                        recovery_succeeded = false;
                        eprintln!("AQS action recovery poll failed for {strategy_id}: {error}");
                        Vec::new()
                    }
                };
                for action in actions {
                    let action_id = action.record_id().unwrap_or_else(|| "<missing-id>".to_string());
                    eprintln!("[action_id={action_id}] received and decoded from recovery poll for strategy {strategy_id}");
                    forward_action(
                        &runtime_id,
                        &strategy_id,
                        &live_session_id,
                        &run_id,
                        &subject,
                        bus.as_ref(),
                        sink.as_ref(),
                        &outbox,
                        &mut handled,
                        action,
                    ).await;
                }
                if live_subscription.is_none() && !sink.hosted_safety_registered().await {
                    match sink.subscribe_pending_actions().await {
                        Ok(subscription) => live_subscription = Some(subscription),
                        Err(error) => {
                            recovery_succeeded = false;
                            eprintln!("AQS action live-stream reconnect failed for {strategy_id}: {error}");
                        }
                    }
                }
                let next_delay = if recovery_succeeded {
                    recovery_backoff = Duration::from_secs(1);
                    Duration::from_secs(5)
                } else {
                    let delay = recovery_backoff;
                    recovery_backoff = next_action_recovery_backoff(recovery_backoff);
                    delay
                };
                recovery_sleep
                    .as_mut()
                    .reset(tokio::time::Instant::now() + next_delay);
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
        }
    }
}

pub fn next_action_recovery_backoff(current: Duration) -> Duration {
    current.saturating_mul(2).min(Duration::from_secs(60))
}

async fn forward_action(
    runtime_id: &str,
    strategy_id: &str,
    live_session_id: &str,
    run_id: &str,
    subject: &EventSubject,
    bus: &dyn LocalEventBus,
    sink: &HttpBatchSink,
    outbox: &OutboxStore,
    handled: &mut HashSet<String>,
    action: RemoteStrategyAction,
) {
    if !action_belongs_to_strategy(&action, strategy_id)
        || action.status.as_str() != Some("pending")
    {
        return;
    }
    let Some(action_id) = action.record_id() else {
        return;
    };
    // Hosted rows are persistence mirrors, not a second command-delivery channel.
    if action_id.starts_with("strategy_actions:aqs_hosted_") {
        return;
    }
    if !action_targets_process(&action, runtime_id, run_id) {
        let reason =
            format!("Superseded action target (expected runtime_id={runtime_id}, run_id={run_id})");
        eprintln!(
            "[action_id={action_id}] cancelling stale/superseded action for {strategy_id}: {reason}"
        );
        // Never record a stale action as delivered. A failed cancellation must be
        // retried, and old-run notifications must not grow the in-memory cache.
        if let Err(error) = sink.cancel_pending_action(&action_id, &reason).await {
            eprintln!("[action_id={action_id}] failed to cancel stale action remotely: {error}");
        } else {
            eprintln!("[action_id={action_id}] stale action marked cancelled remotely");
        }
        return;
    }
    if handled.contains(&action_id)
        || outbox
            .action_was_accepted(&action_id, strategy_id, live_session_id)
            .await
            .unwrap_or(false)
    {
        return;
    }
    let mut event = EventEnvelope::new(
        runtime_id,
        strategy_id,
        live_session_id,
        format!("strategy-action:{action_id}"),
        1,
        EventPriority::Critical,
        EventKind::Control,
        serde_json::json!({
            "action_id": action_id,
            "action": action.action,
            "status": action.status,
        }),
    );
    event.event_id = deterministic_event_id(&format!("aqs-control:{live_session_id}:{action_id}"));
    event.run_id = Some(run_id.to_string());
    let expected_event_id = event.event_id;
    eprintln!(
        "[action_id={action_id}] publishing stop action to local event-bus control subject {subject}"
    );
    match bus.publish_critical(subject.clone(), event).await {
        Ok(delivery) => match delivery.wait().await {
            Ok(ref acknowledgement) if control_received(acknowledgement, expected_event_id) => {
                eprintln!(
                    "[action_id={action_id}] AQE acknowledged local event-bus control delivery"
                );
                if let Err(error) = outbox
                    .mark_action_accepted(&action_id, strategy_id, live_session_id)
                    .await
                {
                    eprintln!("[action_id={action_id}] failed to persist accepted action: {error}");
                } else {
                    handled.insert(action_id);
                    if handled.len() > 10_000 {
                        handled.clear();
                    }
                }
            }
            Ok(_) => eprintln!(
                "[action_id={action_id}] control receipt was rejected or did not identify this event"
            ),
            Err(error) => eprintln!(
                "[action_id={action_id}] AQE did not acknowledge event-bus control delivery: {error}"
            ),
        },
        Err(error) => {
            eprintln!(
                "[action_id={action_id}] failed to publish event-bus control delivery: {error}"
            )
        }
    }
}

pub(super) fn control_received(
    acknowledgement: &super::SyncAcknowledgement,
    event_id: uuid::Uuid,
) -> bool {
    matches!(acknowledgement, super::SyncAcknowledgement::LocalAccepted { event_ids } if event_ids.contains(&event_id))
}

pub fn action_targets_process(
    action: &RemoteStrategyAction,
    runtime_id: &str,
    run_id: &str,
) -> bool {
    action.runtime_id.as_deref() == Some(runtime_id) && action.run_id.as_deref() == Some(run_id)
}

fn action_belongs_to_strategy(action: &RemoteStrategyAction, strategy_id: &str) -> bool {
    action.strategy_record_id().is_some_and(|owner| {
        owner.strip_prefix("strategy:").unwrap_or(&owner)
            == strategy_id.strip_prefix("strategy:").unwrap_or(strategy_id)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn receipt_requires_acceptance_of_the_exact_control_event() {
        use aqe_live_protocol::{SyncAcknowledgement, SyncRejectionReason};
        let event_id = uuid::Uuid::new_v4();
        assert!(control_received(
            &SyncAcknowledgement::LocalAccepted {
                event_ids: vec![event_id]
            },
            event_id
        ));
        assert!(!control_received(
            &SyncAcknowledgement::LocalAccepted {
                event_ids: vec![uuid::Uuid::new_v4()]
            },
            event_id
        ));
        assert!(!control_received(
            &SyncAcknowledgement::Rejected {
                event_id,
                reason: SyncRejectionReason::Unauthorized
            },
            event_id
        ));
        assert!(!control_received(
            &SyncAcknowledgement::Persisted {
                critical_through: 1,
                telemetry_revisions: vec![]
            },
            event_id
        ));
    }

    #[test]
    fn control_owner_must_be_present_and_match() {
        let mut action: RemoteStrategyAction = serde_json::from_value(serde_json::json!({
            "id": "strategy_actions:stop", "action": "stop", "status": "pending",
            "runtime_id": "runtime", "run_id": "run"
        }))
        .unwrap();
        assert!(!action_belongs_to_strategy(&action, "one"));
        action.strategy_id = Some(serde_json::json!("strategy:other"));
        assert!(!action_belongs_to_strategy(&action, "one"));
        action.strategy_id = Some(serde_json::json!("strategy:one"));
        assert!(action_belongs_to_strategy(&action, "one"));
        assert!(action_belongs_to_strategy(&action, "strategy:one"));
        assert!(action_targets_process(&action, "runtime", "run"));
        assert!(!action_targets_process(&action, "runtime", "old-run"));
    }
}
