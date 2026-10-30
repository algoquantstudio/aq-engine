//! A separately owned local request/reply lane for compact operational facts.
//! Its receipts never acknowledge original trading-event IDs or sequences.
use super::*;
use aqe_live_protocol::SafetyObservation;
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;

const LOCAL_TIMEOUT: Duration = Duration::from_millis(250);

fn envelope(observation: &SafetyObservation) -> CloudEvent {
    let mut event = EventEnvelope::new(
        &observation.runtime_id,
        &observation.strategy_id,
        &observation.live_session_id,
        "operational-safety-v1",
        observation.revision,
        EventPriority::Critical,
        EventKind::Control,
        serde_json::to_value(observation).expect("validated operational fact"),
    );
    event.run_id = Some(observation.run_id.clone());
    event.event_id =
        deterministic_event_id(&format!("aqe-safety-handoff-v1:{}", observation.event_id));
    event.created_at = observation.created_at;
    event
}

fn decode(event: &CloudEvent) -> Option<SafetyObservation> {
    if event.kind != EventKind::Control
        || event.priority != EventPriority::Critical
        || event.critical_sequence.is_some()
        || event.entity_id != "operational-safety-v1"
    {
        return None;
    }
    let observation: SafetyObservation = serde_json::from_value(event.payload.clone()).ok()?;
    observation.validate().ok()?;
    if event.runtime_id != observation.runtime_id
        || event.strategy_id != observation.strategy_id
        || event.live_session_id != observation.live_session_id
        || event.run_id.as_deref() != Some(&observation.run_id)
    {
        return None;
    }
    Some(observation)
}

async fn flush(store: &OutboxStore, bus: &dyn LocalEventBus) -> Result<(), OutboxStoreError> {
    for observation in store.pending_safety_handoffs().await? {
        let request = envelope(&observation);
        let response = bus
            .request_event(
                EventSubject::operational_safety(&observation.runtime_id, &observation.strategy_id),
                request.clone(),
                LOCAL_TIMEOUT,
            )
            .await;
        let delivered = response
            .ok()
            .filter(|response| {
                response.event_id == request.event_id
                    && response.entity_revision == request.entity_revision
            })
            .and_then(|response| decode(&response))
            .is_some_and(|stored| stored.covers(&observation));
        store
            .settle_safety_observation(&observation, delivered)
            .await?;
    }
    Ok(())
}

pub(super) async fn run_publisher(
    store: OutboxStore,
    bus: Arc<dyn LocalEventBus>,
    mut shutdown: watch::Receiver<bool>,
) {
    let changed = store.safety_change_notifier();
    loop {
        if *shutdown.borrow() {
            return;
        }
        let notified = changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        tokio::select! {biased;
            _=shutdown.changed()=>return,
            result=flush(&store,bus.as_ref())=>if let Err(error)=result {log::warn!("Compact stop handoff retained for retry: {error}");},
        }
        tokio::select! {biased;
            _=shutdown.changed()=>return,
            _=&mut notified=>{},
            _=tokio::time::sleep(Duration::from_secs(5))=>{},
        }
    }
}

pub(super) async fn run_receiver(
    store: OutboxStore,
    bus: Arc<dyn LocalEventBus>,
    mut subscription: EventSubscription,
    runtime: String,
    strategy: String,
    session: String,
    run: String,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), OutboxStoreError> {
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let delivery = tokio::select! {biased;
            _=shutdown.changed()=>return Ok(()),
            delivery=subscription.recv()=>match delivery {Some(delivery)=>delivery,None=>return Ok(())},
        };
        let Some(observation) = decode(&delivery.event) else {
            continue;
        };
        if observation.runtime_id != runtime
            || observation.strategy_id != strategy
            || observation.live_session_id != session
            || observation.run_id != run
            || delivery.event.event_id != envelope(&observation).event_id
            || delivery.event.entity_revision != observation.revision
        {
            continue;
        }
        let stored = match store.record_safety_only(&[observation]).await {
            Ok(mut stored) => stored.remove(0),
            Err(OutboxStoreError::InvalidRecord(_)) => continue,
            Err(error) => return Err(error),
        };
        let mut response = delivery.event.clone();
        response.payload = serde_json::to_value(stored).expect("validated operational receipt");
        // Lost response is safe: the source retries the same fact and the
        // receiver returns the durable current fact, not a made-up full ACK.
        let _ = bus.respond_event(&delivery, response).await;
    }
}

#[cfg(test)]
mod tests;
