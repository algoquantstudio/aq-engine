use super::{CloudEvent, EventKind, EventPriority, LIVE_SYNC_PROTOCOL_VERSION, LatencyTrace};
use chrono::{TimeZone, Utc};
use flatbuffers::{
    FlatBufferBuilder, Follow, ForwardsUOffset, InvalidFlatbuffer, Table, TableFinishedWIPOffset,
    TableUnfinishedWIPOffset, VOffsetT, Vector, Verifiable, Verifier, WIPOffset,
};
use std::fmt;
use uuid::Uuid;

const FILE_IDENTIFIER: &str = "AQSL";

#[derive(Debug)]
pub enum FlatbufferEventCodecError {
    InvalidBuffer(InvalidFlatbuffer),
    InvalidUuid(uuid::Error),
    InvalidJson(serde_json::Error),
    InvalidPriority(u8),
    InvalidTimestamp(i64),
    UnsupportedProtocol(u16),
}

impl fmt::Display for FlatbufferEventCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBuffer(error) => write!(formatter, "invalid FlatBuffer: {error}"),
            Self::InvalidUuid(error) => write!(formatter, "invalid event id: {error}"),
            Self::InvalidJson(error) => write!(formatter, "invalid JSON payload: {error}"),
            Self::InvalidPriority(value) => write!(formatter, "invalid event priority: {value}"),
            Self::InvalidTimestamp(value) => write!(formatter, "invalid event timestamp: {value}"),
            Self::UnsupportedProtocol(value) => {
                write!(formatter, "unsupported live-sync protocol version: {value}")
            }
        }
    }
}

impl std::error::Error for FlatbufferEventCodecError {}

impl From<InvalidFlatbuffer> for FlatbufferEventCodecError {
    fn from(value: InvalidFlatbuffer) -> Self {
        Self::InvalidBuffer(value)
    }
}

impl From<uuid::Error> for FlatbufferEventCodecError {
    fn from(value: uuid::Error) -> Self {
        Self::InvalidUuid(value)
    }
}

impl From<serde_json::Error> for FlatbufferEventCodecError {
    fn from(value: serde_json::Error) -> Self {
        Self::InvalidJson(value)
    }
}

pub struct FlatbufferEventCodec;

impl FlatbufferEventCodec {
    pub fn encode(event: &CloudEvent) -> Result<Vec<u8>, FlatbufferEventCodecError> {
        Self::encode_with_latency(event, &event.latency)
    }

    pub(crate) fn encode_with_local_commit(
        event: &CloudEvent,
        at_us: i64,
    ) -> Result<Vec<u8>, FlatbufferEventCodecError> {
        let mut latency = event.latency.clone();
        latency.local_outbox_committed_at_us = Some(at_us);
        Self::encode_with_latency(event, &latency)
    }

    fn encode_with_latency(
        event: &CloudEvent,
        latency: &LatencyTrace,
    ) -> Result<Vec<u8>, FlatbufferEventCodecError> {
        let mut builder = FlatBufferBuilder::with_capacity(1024);
        let event_id = builder.create_string(&event.event_id.to_string());
        let runtime_id = builder.create_string(&event.runtime_id);
        let strategy_id = builder.create_string(&event.strategy_id);
        let live_session_id = builder.create_string(&event.live_session_id);
        let run_id = event
            .run_id
            .as_deref()
            .map(|value| builder.create_string(value));
        let entity_id = builder.create_string(&event.entity_id);
        let kind = builder.create_string(event.kind.as_str());
        let payload = serde_json::to_vec(&event.payload)?;
        let payload = builder.create_vector(&payload);
        let latency = serde_json::to_vec(latency)?;
        let latency = builder.create_vector(&latency);

        let start = builder.start_table();
        builder.push_slot::<u16>(WireEvent::VT_PROTOCOL_VERSION, event.protocol_version, 0);
        builder.push_slot_always(WireEvent::VT_EVENT_ID, event_id);
        builder.push_slot_always(WireEvent::VT_RUNTIME_ID, runtime_id);
        builder.push_slot_always(WireEvent::VT_STRATEGY_ID, strategy_id);
        builder.push_slot_always(WireEvent::VT_LIVE_SESSION_ID, live_session_id);
        builder.push_slot_always(WireEvent::VT_ENTITY_ID, entity_id);
        builder.push_slot::<u64>(WireEvent::VT_ENTITY_REVISION, event.entity_revision, 0);
        if let Some(sequence) = event.critical_sequence {
            builder.push_slot::<u64>(WireEvent::VT_CRITICAL_SEQUENCE, sequence, 0);
            builder.push_slot::<bool>(WireEvent::VT_HAS_CRITICAL_SEQUENCE, true, false);
        }
        builder.push_slot::<u8>(WireEvent::VT_PRIORITY, priority_to_wire(event.priority), 0);
        builder.push_slot_always(WireEvent::VT_KIND, kind);
        builder.push_slot_always(WireEvent::VT_PAYLOAD_JSON, payload);
        builder.push_slot_always(WireEvent::VT_LATENCY_JSON, latency);
        builder.push_slot::<i64>(
            WireEvent::VT_CREATED_AT_US,
            event.created_at.timestamp_micros(),
            0,
        );
        if let Some(run_id) = run_id {
            builder.push_slot_always(WireEvent::VT_RUN_ID, run_id);
        }
        let table = builder.end_table(start);
        let root = WIPOffset::<WireEvent<'_>>::new(table.value());
        builder.finish(root, Some(FILE_IDENTIFIER));
        Ok(builder.finished_data().to_vec())
    }

    pub fn decode(bytes: &[u8]) -> Result<CloudEvent, FlatbufferEventCodecError> {
        let wire = flatbuffers::root::<WireEvent<'_>>(bytes)?;
        let protocol_version = wire.protocol_version();
        if !(1..=LIVE_SYNC_PROTOCOL_VERSION).contains(&protocol_version) {
            return Err(FlatbufferEventCodecError::UnsupportedProtocol(
                protocol_version,
            ));
        }
        let created_at_us = wire.created_at_us();
        let created_at = Utc
            .timestamp_micros(created_at_us)
            .single()
            .ok_or(FlatbufferEventCodecError::InvalidTimestamp(created_at_us))?;

        Ok(CloudEvent {
            protocol_version,
            event_id: Uuid::parse_str(wire.event_id())?,
            runtime_id: wire.runtime_id().to_string(),
            strategy_id: wire.strategy_id().to_string(),
            live_session_id: wire.live_session_id().to_string(),
            run_id: wire.run_id().map(str::to_string),
            entity_id: wire.entity_id().to_string(),
            entity_revision: wire.entity_revision(),
            critical_sequence: wire
                .has_critical_sequence()
                .then(|| wire.critical_sequence()),
            priority: priority_from_wire(wire.priority())?,
            kind: kind_from_wire(wire.kind()),
            payload: serde_json::from_slice(wire.payload_json().bytes())?,
            latency: serde_json::from_slice::<LatencyTrace>(wire.latency_json().bytes())?,
            created_at,
        })
    }
}

fn priority_to_wire(priority: EventPriority) -> u8 {
    match priority {
        EventPriority::Critical => 1,
        EventPriority::Telemetry => 2,
        EventPriority::Reconciliation => 3,
        EventPriority::Audit => 4,
    }
}

fn priority_from_wire(priority: u8) -> Result<EventPriority, FlatbufferEventCodecError> {
    match priority {
        1 => Ok(EventPriority::Critical),
        2 => Ok(EventPriority::Telemetry),
        3 => Ok(EventPriority::Reconciliation),
        4 => Ok(EventPriority::Audit),
        other => Err(FlatbufferEventCodecError::InvalidPriority(other)),
    }
}

fn kind_from_wire(kind: &str) -> EventKind {
    match kind {
        "strategy_lifecycle" => EventKind::StrategyLifecycle,
        "insight_projection" => EventKind::InsightProjection,
        "insight_state_history" => EventKind::InsightStateHistory,
        "execution_evidence_chunk" => EventKind::ExecutionEvidenceChunk,
        "execution_fill_page" => EventKind::ExecutionFillPage,
        "position_mark_page" => EventKind::PositionMarkPage,
        "broker_order_lifecycle" => EventKind::BrokerOrderLifecycle,
        "strategy_action_acknowledgement" => EventKind::StrategyActionAcknowledgement,
        "account_projection" => EventKind::AccountProjection,
        "live_metrics_projection" => EventKind::LiveMetricsProjection,
        "equity_projection" => EventKind::EquityProjection,
        "heartbeat" => EventKind::Heartbeat,
        "strategy_event" => EventKind::StrategyEvent,
        "log" => EventKind::Log,
        "reconciliation_start" => EventKind::ReconciliationStart,
        "reconciliation_chunk" => EventKind::ReconciliationChunk,
        "reconciliation_complete" => EventKind::ReconciliationComplete,
        "reconciliation_lifecycle_request" => EventKind::ReconciliationLifecycleRequest,
        "reconciliation_lifecycle_result" => EventKind::ReconciliationLifecycleResult,
        "reconciliation_snapshot" => EventKind::ReconciliationSnapshot,
        "reconciliation_progress" => EventKind::ReconciliationProgress,
        "sync_health" => EventKind::SyncHealth,
        "control" => EventKind::Control,
        other => EventKind::Custom(other.to_string()),
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
struct WireEvent<'a> {
    table: Table<'a>,
}

impl<'a> Follow<'a> for WireEvent<'a> {
    type Inner = WireEvent<'a>;

    unsafe fn follow(buffer: &'a [u8], location: usize) -> Self::Inner {
        Self {
            // SAFETY: `root` verifies the table location before `Follow` is invoked.
            table: unsafe { Table::new(buffer, location) },
        }
    }
}

impl Verifiable for WireEvent<'_> {
    fn run_verifier(
        verifier: &mut Verifier<'_, '_>,
        position: usize,
    ) -> Result<(), InvalidFlatbuffer> {
        verifier
            .visit_table(position)?
            .visit_field::<u16>("protocol_version", Self::VT_PROTOCOL_VERSION, false)?
            .visit_field::<ForwardsUOffset<&str>>("event_id", Self::VT_EVENT_ID, true)?
            .visit_field::<ForwardsUOffset<&str>>("runtime_id", Self::VT_RUNTIME_ID, true)?
            .visit_field::<ForwardsUOffset<&str>>("strategy_id", Self::VT_STRATEGY_ID, true)?
            .visit_field::<ForwardsUOffset<&str>>(
                "live_session_id",
                Self::VT_LIVE_SESSION_ID,
                true,
            )?
            .visit_field::<ForwardsUOffset<&str>>("entity_id", Self::VT_ENTITY_ID, true)?
            .visit_field::<u64>("entity_revision", Self::VT_ENTITY_REVISION, false)?
            .visit_field::<u64>("critical_sequence", Self::VT_CRITICAL_SEQUENCE, false)?
            .visit_field::<bool>(
                "has_critical_sequence",
                Self::VT_HAS_CRITICAL_SEQUENCE,
                false,
            )?
            .visit_field::<u8>("priority", Self::VT_PRIORITY, false)?
            .visit_field::<ForwardsUOffset<&str>>("kind", Self::VT_KIND, true)?
            .visit_field::<ForwardsUOffset<Vector<'_, u8>>>(
                "payload_json",
                Self::VT_PAYLOAD_JSON,
                true,
            )?
            .visit_field::<ForwardsUOffset<Vector<'_, u8>>>(
                "latency_json",
                Self::VT_LATENCY_JSON,
                true,
            )?
            .visit_field::<i64>("created_at_us", Self::VT_CREATED_AT_US, false)?
            .visit_field::<ForwardsUOffset<&str>>("run_id", Self::VT_RUN_ID, false)?
            .finish();
        Ok(())
    }
}

impl<'a> WireEvent<'a> {
    const VT_PROTOCOL_VERSION: VOffsetT = 4;
    const VT_EVENT_ID: VOffsetT = 6;
    const VT_RUNTIME_ID: VOffsetT = 8;
    const VT_STRATEGY_ID: VOffsetT = 10;
    const VT_LIVE_SESSION_ID: VOffsetT = 12;
    const VT_ENTITY_ID: VOffsetT = 14;
    const VT_ENTITY_REVISION: VOffsetT = 16;
    const VT_CRITICAL_SEQUENCE: VOffsetT = 18;
    const VT_HAS_CRITICAL_SEQUENCE: VOffsetT = 20;
    const VT_PRIORITY: VOffsetT = 22;
    const VT_KIND: VOffsetT = 24;
    const VT_PAYLOAD_JSON: VOffsetT = 26;
    const VT_LATENCY_JSON: VOffsetT = 28;
    const VT_CREATED_AT_US: VOffsetT = 30;
    const VT_RUN_ID: VOffsetT = 32;

    fn protocol_version(&self) -> u16 {
        // SAFETY: verified by `Verifiable` before access.
        unsafe { self.table.get::<u16>(Self::VT_PROTOCOL_VERSION, Some(0)) }.unwrap_or(0)
    }

    fn event_id(&self) -> &'a str {
        unsafe {
            self.table
                .get::<ForwardsUOffset<&str>>(Self::VT_EVENT_ID, None)
        }
        .expect("required verified field")
    }

    fn runtime_id(&self) -> &'a str {
        unsafe {
            self.table
                .get::<ForwardsUOffset<&str>>(Self::VT_RUNTIME_ID, None)
        }
        .expect("required verified field")
    }

    fn strategy_id(&self) -> &'a str {
        unsafe {
            self.table
                .get::<ForwardsUOffset<&str>>(Self::VT_STRATEGY_ID, None)
        }
        .expect("required verified field")
    }

    fn live_session_id(&self) -> &'a str {
        unsafe {
            self.table
                .get::<ForwardsUOffset<&str>>(Self::VT_LIVE_SESSION_ID, None)
        }
        .expect("required verified field")
    }

    fn entity_id(&self) -> &'a str {
        unsafe {
            self.table
                .get::<ForwardsUOffset<&str>>(Self::VT_ENTITY_ID, None)
        }
        .expect("required verified field")
    }

    fn entity_revision(&self) -> u64 {
        unsafe { self.table.get::<u64>(Self::VT_ENTITY_REVISION, Some(0)) }.unwrap_or(0)
    }

    fn critical_sequence(&self) -> u64 {
        unsafe { self.table.get::<u64>(Self::VT_CRITICAL_SEQUENCE, Some(0)) }.unwrap_or(0)
    }

    fn has_critical_sequence(&self) -> bool {
        unsafe {
            self.table
                .get::<bool>(Self::VT_HAS_CRITICAL_SEQUENCE, Some(false))
        }
        .unwrap_or(false)
    }

    fn priority(&self) -> u8 {
        unsafe { self.table.get::<u8>(Self::VT_PRIORITY, Some(0)) }.unwrap_or(0)
    }

    fn kind(&self) -> &'a str {
        unsafe { self.table.get::<ForwardsUOffset<&str>>(Self::VT_KIND, None) }
            .expect("required verified field")
    }

    fn payload_json(&self) -> Vector<'a, u8> {
        unsafe {
            self.table
                .get::<ForwardsUOffset<Vector<'a, u8>>>(Self::VT_PAYLOAD_JSON, None)
        }
        .expect("required verified field")
    }

    fn latency_json(&self) -> Vector<'a, u8> {
        unsafe {
            self.table
                .get::<ForwardsUOffset<Vector<'a, u8>>>(Self::VT_LATENCY_JSON, None)
        }
        .expect("required verified field")
    }

    fn created_at_us(&self) -> i64 {
        unsafe { self.table.get::<i64>(Self::VT_CREATED_AT_US, Some(0)) }.unwrap_or(0)
    }

    fn run_id(&self) -> Option<&'a str> {
        unsafe {
            self.table
                .get::<ForwardsUOffset<&str>>(Self::VT_RUN_ID, None)
        }
    }
}

#[allow(dead_code)]
fn _table_marker_types(
    _: WIPOffset<TableUnfinishedWIPOffset>,
    _: WIPOffset<TableFinishedWIPOffset>,
) {
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::live_sync::{EventEnvelope, EventPriority};

    #[test]
    fn local_commit_encoding_borrows_payload_and_preserves_original_wire_fields() {
        let event = EventEnvelope::new(
            "runtime",
            "strategy",
            "session",
            "entity",
            1,
            EventPriority::Audit,
            EventKind::InsightStateHistory,
            serde_json::json!({"state":"New","nested":[{"price":0.00000012,"quantity":1.25}],"text":"x".repeat(16000)}),
        );
        let mut previous = event.clone();
        previous.latency.local_outbox_committed_at_us = Some(12345);
        assert_eq!(
            FlatbufferEventCodec::encode_with_local_commit(&event, 12345).unwrap(),
            FlatbufferEventCodec::encode(&previous).unwrap()
        );
        assert!(event.latency.local_outbox_committed_at_us.is_none());
    }

    #[test]
    #[ignore = "isolated CPU/allocation microbenchmark, run explicitly with --ignored --nocapture"]
    fn benchmark_borrowed_local_commit_encoding() {
        let event = EventEnvelope::new(
            "runtime",
            "strategy",
            "session",
            "entity",
            1,
            EventPriority::Audit,
            EventKind::InsightStateHistory,
            serde_json::json!({"rows":(0..400).map(|n|serde_json::json!({"id":n,"symbol":"fixture","price":100.25,"quantity":1.5,"metadata":{"state":"Filled","tags":["one","two"]}})).collect::<Vec<_>>()}),
        );
        let start = std::time::Instant::now();
        for _ in 0..500 {
            let mut copied = event.clone();
            copied.latency.local_outbox_committed_at_us = Some(12345);
            std::hint::black_box(FlatbufferEventCodec::encode(&copied).unwrap());
        }
        let old = start.elapsed();
        let start = std::time::Instant::now();
        for _ in 0..500 {
            std::hint::black_box(
                FlatbufferEventCodec::encode_with_local_commit(&event, 12345).unwrap(),
            );
        }
        eprintln!(
            "runtime-encoding iterations=500 copied_ms={} borrowed_ms={} payload_bytes={}",
            old.as_millis(),
            start.elapsed().as_millis(),
            serde_json::to_vec(&event.payload).unwrap().len()
        );
    }

    #[test]
    fn flatbuffer_round_trip_preserves_event_identity_and_payload() {
        let mut event = EventEnvelope::new(
            "runtime-1",
            "strategy-1",
            "session-1",
            "insight-1",
            7,
            EventPriority::Critical,
            EventKind::InsightProjection,
            serde_json::json!({"state": "Filled"}),
        );
        event.critical_sequence = Some(12);
        let encoded = FlatbufferEventCodec::encode(&event).unwrap();
        assert!(flatbuffers::buffer_has_identifier(
            &encoded,
            FILE_IDENTIFIER,
            false
        ));
        let decoded = FlatbufferEventCodec::decode(&encoded).unwrap();
        assert_eq!(decoded.event_id, event.event_id);
        assert_eq!(decoded.critical_sequence, Some(12));
        assert_eq!(decoded.payload["state"], "Filled");
    }
}
