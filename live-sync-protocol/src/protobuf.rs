//! Versioned, lossless live-batch envelope; canonical identity remains JSON.
//! Payloads remain exact JSON; generic protobuf Value's double is not suitable
//! for our u64 revisions, broker tickets and event-identity inputs.
use crate::{CloudEvent, EventPriority, LatencyTrace, LiveSyncBatch};
use prost::Message;

#[derive(Clone, PartialEq, Message)]
struct Batch {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(string, tag = "2")]
    runtime: String,
    #[prost(string, tag = "3")]
    strategy: String,
    #[prost(string, tag = "4")]
    session: String,
    #[prost(uint64, optional, tag = "5")]
    first: Option<u64>,
    #[prost(uint64, optional, tag = "6")]
    last: Option<u64>,
    #[prost(message, repeated, tag = "7")]
    critical: Vec<Event>,
    #[prost(message, repeated, tag = "8")]
    telemetry: Vec<Event>,
}
#[derive(Clone, PartialEq, Message)]
struct Event {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(bytes = "vec", tag = "2")]
    id: Vec<u8>,
    #[prost(string, tag = "3")]
    runtime: String,
    #[prost(string, tag = "4")]
    strategy: String,
    #[prost(string, tag = "5")]
    session: String,
    #[prost(string, optional, tag = "6")]
    run: Option<String>,
    #[prost(string, tag = "7")]
    entity: String,
    #[prost(uint64, tag = "8")]
    revision: u64,
    #[prost(uint64, optional, tag = "9")]
    sequence: Option<u64>,
    #[prost(uint32, tag = "10")]
    priority: u32,
    #[prost(bytes = "bytes", tag = "11")]
    kind_json: prost::bytes::Bytes,
    #[prost(bytes = "bytes", tag = "12")]
    payload_json: prost::bytes::Bytes,
    #[prost(message, optional, tag = "13")]
    latency: Option<Trace>,
    #[prost(sint64, tag = "14")]
    seconds: i64,
    #[prost(uint32, tag = "15")]
    nanos: u32,
}
#[derive(Clone, PartialEq, Message)]
struct Trace {
    #[prost(sint64, optional, tag = "1")]
    source: Option<i64>,
    #[prost(sint64, optional, tag = "2")]
    adapter: Option<i64>,
    #[prost(sint64, optional, tag = "3")]
    runtime: Option<i64>,
    #[prost(sint64, optional, tag = "4")]
    bus: Option<i64>,
    #[prost(sint64, optional, tag = "5")]
    outbox: Option<i64>,
    #[prost(sint64, optional, tag = "6")]
    request: Option<i64>,
    #[prost(sint64, optional, tag = "7")]
    committed: Option<i64>,
    #[prost(sint64, optional, tag = "8")]
    ack: Option<i64>,
    #[prost(sint64, optional, tag = "9")]
    applied: Option<i64>,
    #[prost(sint64, optional, tag = "10")]
    offset: Option<i64>,
    #[prost(uint64, optional, tag = "11")]
    uncertainty: Option<u64>,
}
impl From<&LatencyTrace> for Trace {
    fn from(t: &LatencyTrace) -> Self {
        Self {
            source: t.source_occurred_at_us,
            adapter: t.adapter_queued_at_us,
            runtime: t.runtime_received_at_us,
            bus: t.local_bus_published_at_us,
            outbox: t.local_outbox_committed_at_us,
            request: t.persistence_request_sent_at_us,
            committed: t.persistence_committed_at_us,
            ack: t.persistence_ack_received_at_us,
            applied: t.client_applied_at_us,
            offset: t.clock_offset_us,
            uncertainty: t.clock_uncertainty_us,
        }
    }
}
impl From<Trace> for LatencyTrace {
    fn from(t: Trace) -> Self {
        Self {
            source_occurred_at_us: t.source,
            adapter_queued_at_us: t.adapter,
            runtime_received_at_us: t.runtime,
            local_bus_published_at_us: t.bus,
            local_outbox_committed_at_us: t.outbox,
            persistence_request_sent_at_us: t.request,
            persistence_committed_at_us: t.committed,
            persistence_ack_received_at_us: t.ack,
            client_applied_at_us: t.applied,
            clock_offset_us: t.offset,
            clock_uncertainty_us: t.uncertainty,
        }
    }
}
mod borrowed;
fn restore(e: Event) -> Result<CloudEvent, String> {
    Ok(CloudEvent {
        protocol_version: e.version.try_into().map_err(|_| "invalid version")?,
        event_id: uuid::Uuid::from_slice(&e.id).map_err(|_| "invalid UUID")?,
        runtime_id: e.runtime,
        strategy_id: e.strategy,
        live_session_id: e.session,
        run_id: e.run,
        entity_id: e.entity,
        entity_revision: e.revision,
        critical_sequence: e.sequence,
        priority: match e.priority {
            0 => EventPriority::Critical,
            1 => EventPriority::Telemetry,
            2 => EventPriority::Audit,
            3 => EventPriority::Reconciliation,
            _ => return Err("invalid priority".into()),
        },
        kind: serde_json::from_slice(&e.kind_json).map_err(|e| e.to_string())?,
        payload: serde_json::from_slice(&e.payload_json).map_err(|e| e.to_string())?,
        latency: e.latency.ok_or("missing latency")?.into(),
        created_at: chrono::DateTime::from_timestamp(e.seconds, e.nanos)
            .ok_or("invalid timestamp")?,
    })
}
pub fn encode(batch: &LiveSyncBatch) -> Result<Vec<u8>, String> {
    borrowed::encode(batch)
}
pub fn decode(bytes: &[u8]) -> Result<LiveSyncBatch, String> {
    if bytes.len() > 256 * 1024 {
        return Err("oversized protobuf batch".into());
    }
    decode_bytes(prost::bytes::Bytes::copy_from_slice(bytes))
}
pub fn decode_bytes(bytes: prost::bytes::Bytes) -> Result<LiveSyncBatch, String> {
    if bytes.len() > 256 * 1024 {
        return Err("oversized protobuf batch".into());
    }
    // Count before allocation, including duplicate occurrences of event fields.
    // Unknown fields remain forward compatible; recursion is bounded by prost.
    let mut cursor = &bytes[..];
    let mut events = 0;
    while !cursor.is_empty() {
        let (tag, wire) = prost::encoding::decode_key(&mut cursor).map_err(|e| e.to_string())?;
        if tag == 7 || tag == 8 {
            events += 1;
            if events > 100 {
                return Err("too many events".into());
            }
        }
        prost::encoding::skip_field(
            wire,
            tag,
            &mut cursor,
            prost::encoding::DecodeContext::default(),
        )
        .map_err(|e| e.to_string())?;
    }
    let b = Batch::decode(bytes).map_err(|e| e.to_string())?;
    Ok(LiveSyncBatch {
        protocol_version: b.version.try_into().map_err(|_| "invalid version")?,
        runtime_id: b.runtime,
        strategy_id: b.strategy,
        live_session_id: b.session,
        first_critical_sequence: b.first,
        last_critical_sequence: b.last,
        critical_events: b
            .critical
            .into_iter()
            .map(restore)
            .collect::<Result<_, _>>()?,
        telemetry: b
            .telemetry
            .into_iter()
            .map(restore)
            .collect::<Result<_, _>>()?,
    })
}

/// Preserve the existing gateway's decoded JSON batch budget as well as the
/// wire/event bounds. Binary framing must not bypass a logical payload limit.
pub fn decode_http(bytes: prost::bytes::Bytes) -> Result<LiveSyncBatch, String> {
    let batch = decode_bytes(bytes)?;
    if crate::json_encoded_len(&batch).map_err(|e| e.to_string())? > 256 * 1024 {
        return Err("decoded batch exceeds JSON budget".into());
    }
    Ok(batch)
}

pub const MIME: &str = "application/vnd.aqs.live.v1+protobuf";
/// Bulk transfers and opaque/custom payloads stay on their current JSON path.
/// The selected envelope-heavy shapes were compared through real gateways.
pub fn prefer(batch: &LiveSyncBatch) -> bool {
    let count = batch.critical_events.len() + batch.telemetry.len();
    count > 0
        && count <= 25
        && batch.events().all(|event| {
            use crate::EventKind as K;
            match event.kind {
                K::InsightProjection
                | K::InsightStateHistory
                | K::EquityProjection
                | K::LiveMetricsProjection
                | K::AccountProjection
                | K::StrategyLifecycle
                | K::Heartbeat
                | K::StrategyEvent => true,
                K::ExecutionFillPage | K::PositionMarkPage => {
                    match (
                        event.payload["after_sequence"].as_u64(),
                        event.payload["next_sequence"].as_u64(),
                    ) {
                        (Some(after), Some(next)) => {
                            next > after
                                && next - after <= 8
                                && event.payload["body"]
                                    .as_str()
                                    .is_some_and(|body| body.len() <= 8 * 1024)
                        }
                        _ => false,
                    }
                }
                _ => false,
            }
        })
}
