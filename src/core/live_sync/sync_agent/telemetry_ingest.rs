//! Coalesce disposable heads before SQLite, where a slow writer otherwise
//! turns the subscription's FIFO into minutes of obsolete heartbeat writes.
use super::*;
use crate::core::live_sync::{EventDelivery, EventSubscription, FlatbufferEventCodec};

const MAX_DRAIN: usize = 256;
const MAX_HEADS: usize = 64;
const MAX_BYTES: usize = 2 * 1024 * 1024;

fn replaceable(delivery: &EventDelivery) -> bool {
    delivery.reply_to.is_none()
        && delivery.event.priority == EventPriority::Telemetry
        && delivery.event.critical_sequence.is_none()
        && matches!(
            delivery.event.kind,
            EventKind::Heartbeat
                | EventKind::AccountProjection
                | EventKind::LiveMetricsProjection
                | EventKind::EquityProjection
        )
}

fn same_head(left: &EventDelivery, right: &EventDelivery) -> bool {
    replaceable(left)
        && replaceable(right)
        && left.subject == right.subject
        && left.event.runtime_id == right.event.runtime_id
        && left.event.strategy_id == right.event.strategy_id
        && left.event.live_session_id == right.event.live_session_id
        && left.event.run_id == right.event.run_id
        && left.event.entity_id == right.event.entity_id
        && left.event.kind == right.event.kind
}

/// Drains only already available messages. No debounce, extra retry, larger
/// subscription, quota exemption or mutation of an accepted durable record.
/// One item beyond a byte/count boundary is carried into the next batch.
pub(super) fn collect(
    first: EventDelivery,
    subscription: &mut EventSubscription,
) -> Result<(Vec<EventDelivery>, Option<EventDelivery>), SyncAgentError> {
    let mut heads: Vec<(EventDelivery, usize)> = Vec::new();
    let mut bytes = 0usize;
    let mut next = Some(first);
    let mut carry = None;
    for _ in 0..MAX_DRAIN {
        let Some(delivery) = next.take().or_else(|| subscription.try_recv().ok()) else {
            break;
        };
        let size = FlatbufferEventCodec::encode(&delivery.event)
            .map_err(|error| SyncAgentError::InvalidEvent(error.to_string()))?
            .len();
        if size > super::super::LOCAL_EVENT_HARD_LIMIT_BYTES {
            return Err(SyncAgentError::InvalidEvent(
                "telemetry exceeds local wire limit".into(),
            ));
        }
        let index = heads
            .iter()
            .position(|(head, _)| same_head(head, &delivery));
        if let Some(index) = index {
            // Match outbox's immutable-revision ordering, including duplicate
            // revisions. Never rewrite timestamps to make an old head fresh.
            if heads[index].0.event.entity_revision >= delivery.event.entity_revision {
                continue;
            }
            let total = bytes - heads[index].1 + size;
            if total > MAX_BYTES {
                carry = Some(delivery);
                break;
            }
            bytes = total;
            heads[index] = (delivery, size);
        } else {
            if heads.len() >= MAX_HEADS || bytes.saturating_add(size) > MAX_BYTES {
                carry = Some(delivery);
                break;
            }
            bytes += size;
            heads.push((delivery, size));
        }
    }
    Ok((heads.into_iter().map(|(head, _)| head).collect(), carry))
}

pub(super) async fn run(
    outbox: OutboxStore,
    subscription: &mut EventSubscription,
    mut shutdown: watch::Receiver<bool>,
    capture_limit: Option<u64>,
) -> Result<(), SyncAgentError> {
    let mut carry = None;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let first = match carry.take() {
            Some(delivery) => delivery,
            None => tokio::select! { biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { return Ok(()); }
                    continue;
                }
                delivery = subscription.recv() => match delivery { Some(delivery) => delivery, None => return Ok(()) },
            },
        };
        let (heads, next) = collect(first, subscription)?;
        carry = next;
        for delivery in heads {
            if *shutdown.borrow() {
                return Ok(());
            }
            let result = match capture_limit {
                Some(limit) => {
                    outbox
                        .capture_telemetry_latest_with_capacity(&delivery.event, limit)
                        .await
                }
                None => outbox.insert_telemetry_latest(&delivery.event).await,
            };
            match result {
                Ok(()) | Err(OutboxStoreError::CapturePaused(_)) => {}
                Err(error) => return Err(error.into()),
            }
        }
        // A backlog must not monopolize the runtime's ingestion/control tasks.
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
mod tests;
