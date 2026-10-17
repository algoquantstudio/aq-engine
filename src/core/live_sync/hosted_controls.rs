//! Hosted stop receipt, independent from legacy Surreal action recovery.
use super::{gateway::GatewayConnection, *};
use aqe_live_protocol::{HostedControlStatus, HostedStopControl};
use futures::{SinkExt, StreamExt};
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;

pub(super) async fn run(
    gateway: GatewayConnection,
    runtime: String,
    strategy: String,
    session: String,
    run: String,
    bus: Arc<dyn LocalEventBus>,
    outbox: OutboxStore,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut backoff = 1u64;
    loop {
        if *shutdown.borrow() {
            return;
        }
        let connected = tokio::select! {
            _ = shutdown.changed() => return,
            connected = tokio::time::timeout(Duration::from_secs(15), gateway.connect_controls()) => connected,
        };
        if let Ok(Ok(mut socket)) = connected {
            let result = async {
                loop {
                    let frame = tokio::select! {
                        _ = shutdown.changed() => return Ok::<(), String>(()),
                        frame = tokio::time::timeout(Duration::from_secs(20), socket.next()) => frame.map_err(|_| "hosted control heartbeat timed out")?,
                    };
                    match frame {
                        Some(Ok(Message::Text(text))) => {
                            let control: HostedStopControl = serde_json::from_str(&text).map_err(|_| "invalid hosted control")?;
                            if control.target.strategy_id != strategy || control.target.live_session_id != session
                                || control.target.runtime_id != runtime || control.target.run_id != run {
                                return Err("hosted control target mismatch".into());
                            }
                            backoff = 1;
                            if control.status != HostedControlStatus::Pending { continue; }
                            if !outbox.action_was_accepted(&control.action_id, &strategy, &session).await.map_err(|e| e.to_string())? {
                                let mut event = EventEnvelope::new(&runtime, &strategy, &session,
                                    format!("strategy-action:{}", control.action_id), 1, EventPriority::Critical, EventKind::Control,
                                    serde_json::json!({"action_id":control.action_id,"action":"stop","status":"pending"}));
                                event.run_id = Some(run.clone());
                                event.event_id = deterministic_event_id(&format!("aqs-control:{session}:{}",control.action_id));
                                let expected_event_id = event.event_id;
                                let delivery = bus.publish_critical(EventSubject::control(&runtime, &strategy), event).await.map_err(|e| e.to_string())?;
                                let acknowledgement = delivery.wait().await.map_err(|e| e.to_string())?;
                                if !super::actions::control_received(&acknowledgement, expected_event_id) {
                                    return Err("hosted control was not acknowledged by AQE".into());
                                }
                                outbox.mark_action_accepted(&control.action_id, &strategy, &session).await.map_err(|e| e.to_string())?;
                                log::info!("[action_id={}] AQE acknowledged hosted control receipt for runtime={} run={}", control.action_id, runtime, run);
                            }
                            // A lost remote receipt reconnects and repeats the receipt, not the
                            // action. Receipt does not claim that broker teardown has completed.
                            gateway.acknowledge_control(&control.action_id).await.map_err(|e| e.to_string())?;
                        }
                        Some(Ok(Message::Ping(bytes))) => {
                            tokio::time::timeout(Duration::from_secs(5), socket.send(Message::Pong(bytes))).await
                                .map_err(|_| "hosted control pong timed out")?.map_err(|_| "hosted control socket closed")?;
                        }
                        Some(Ok(Message::Close(_))) | None => return Err("hosted control socket closed".into()),
                        Some(Err(_)) => return Err("hosted control stream failed".into()),
                        _ => {},
                    }
                }
            }.await;
            if *shutdown.borrow() {
                return;
            }
            if let Err(error) = result {
                log::warn!("Hosted control stream reconnecting: {error}");
            }
        }
        tokio::select! {
            _ = shutdown.changed() => return,
            _ = tokio::time::sleep(Duration::from_secs(backoff)) => {},
        }
        backoff = backoff.saturating_mul(2).min(30);
    }
}
