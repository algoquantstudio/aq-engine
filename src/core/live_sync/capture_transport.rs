//! Local immutable quality-notice handoff. Never shares stop request workers,
//! trading queues, or critical sequence acknowledgement with customer data.
use super::*;
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;

const ENTITY: &str = "capture-gap-v1";
const LOCAL_TIMEOUT: Duration = Duration::from_millis(250);

fn envelope(notice: &CaptureGapNotice) -> CloudEvent {
    let mut event = EventEnvelope::new(
        &notice.source_runtime_id,
        &notice.strategy_id,
        &notice.live_session_id,
        ENTITY,
        notice.source_generation,
        EventPriority::Audit,
        EventKind::Control,
        serde_json::to_value(notice).expect("validated scalar quality notice"),
    );
    event.run_id = notice.source_run_id.clone();
    event.event_id = deterministic_event_id(&format!("aqe-gap-handoff-v1:{}", notice.notice_id));
    event.created_at = chrono::DateTime::from_timestamp_micros(notice.last_blocked_at_us)
        .expect("validated quality timestamp");
    event
}

fn decode(event: &CloudEvent) -> Option<CaptureGapNotice> {
    if event.kind != EventKind::Control
        || event.priority != EventPriority::Audit
        || event.critical_sequence.is_some()
        || event.entity_id != ENTITY
    {
        return None;
    }
    let notice = CaptureGapNotice::from_bounded_value(&event.payload).ok()?;
    let expected = envelope(&notice);
    if event.event_id != expected.event_id
        || event.entity_revision != expected.entity_revision
        || event.runtime_id != expected.runtime_id
        || event.strategy_id != expected.strategy_id
        || event.live_session_id != expected.live_session_id
        || event.run_id != expected.run_id
        || event.created_at != expected.created_at
    {
        return None;
    }
    Some(notice)
}

fn receipt(
    response: &CloudEvent,
    request: &CloudEvent,
    notice: &CaptureGapNotice,
) -> Option<CaptureGapReceipt> {
    if response.event_id != request.event_id
        || response.entity_revision != request.entity_revision
        || response.entity_id != ENTITY
        || response.priority != EventPriority::Audit
        || response.kind != EventKind::Control
        || response.critical_sequence.is_some()
        || response.runtime_id != request.runtime_id
        || response.strategy_id != request.strategy_id
        || response.live_session_id != request.live_session_id
        || response.run_id != request.run_id
        || response.created_at != request.created_at
    {
        return None;
    }
    let receipt = CaptureGapReceipt::from_bounded_value(&response.payload).ok()?;
    receipt.covers(notice).then_some(receipt)
}

async fn flush(
    store: &OutboxStore,
    bus: &dyn LocalEventBus,
    owner: &CaptureOwnerLease,
) -> Result<(), OutboxStoreError> {
    if !owner.matches_path(store.path()) || owner.run.is_empty() {
        return Err(OutboxStoreError::InvalidRecord(
            "gap lease belongs to another source".into(),
        ));
    }
    for notice in store
        .local_capture_gap_notices(&owner.strategy, &owner.session)
        .await?
    {
        // A legacy gap must first be adopted by its actual leased producer.
        // Import leases and matching caller-supplied strings are not authority.
        if notice.source_runtime_id != owner.runtime
            || notice.source_run_id.as_deref() != Some(&owner.run)
        {
            continue;
        }
        let request = envelope(&notice);
        if let Ok(response) = bus
            .request_event(
                EventSubject::capture_gap_handoff(&owner.runtime, &owner.strategy),
                request.clone(),
                LOCAL_TIMEOUT,
            )
            .await
        {
            if let Some(receipt) = receipt(&response, &request, &notice) {
                store
                    .settle_local_capture_gap_notice(&notice, &receipt)
                    .await?;
            }
        }
    }
    Ok(())
}

pub(super) async fn run_publisher(
    store: OutboxStore,
    bus: Arc<dyn LocalEventBus>,
    owner: Arc<CaptureOwnerLease>,
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
            result=flush(&store,bus.as_ref(),&owner)=>if let Err(error)=result {log::debug!("Local cloud-history notice retained: {error}");},
        }
        tokio::select! {biased;
            _=shutdown.changed()=>return,
            _=&mut notified=>{},
            _=tokio::time::sleep(Duration::from_secs(5))=>{},
        }
    }
}

/// Optional diagnostics failures are retried here, not sent to the sync agent's
/// fatal-error lane. Trading and stop subscriptions remain alive independently.
pub(super) async fn run_receiver(
    store: OutboxStore,
    bus: Arc<dyn LocalEventBus>,
    runtime: String,
    strategy: String,
    session: String,
    run: String,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() {
            return;
        }
        let subscription = tokio::select! {biased;
            _=shutdown.changed()=>return,
            result=bus.subscribe(EventSubject::capture_gap_handoff(&runtime,&strategy))=>result,
        };
        if let Ok(mut subscription) = subscription {
            loop {
                let delivery = tokio::select! {biased;
                    _=shutdown.changed()=>return,
                    delivery=subscription.recv()=>match delivery {Some(delivery)=>delivery,None=>break},
                };
                let Some(notice) = decode(&delivery.event) else {
                    continue;
                };
                if notice.source_runtime_id != runtime
                    || notice.strategy_id != strategy
                    || notice.live_session_id != session
                    || notice.source_run_id.as_deref() != Some(&run)
                {
                    continue;
                }
                match store.accept_capture_gap_notice(&notice).await {
                    Ok(true) => {
                        let mut response = delivery.event.clone();
                        response.payload =
                            serde_json::to_value(CaptureGapReceipt::for_notice(&notice))
                                .expect("scalar gap receipt");
                        let _ = bus.respond_event(&delivery, response).await;
                    }
                    Ok(false) => {} // No ownership receipt when the mailbox is full.
                    Err(error) => {
                        log::debug!("Cloud-history receiver retained source ownership: {error}")
                    }
                }
            }
        }
        tokio::select! {biased;
            _=shutdown.changed()=>return,
            _=tokio::time::sleep(Duration::from_secs(1))=>{},
        }
    }
}

#[cfg(test)]
mod tests;
