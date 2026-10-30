//! Encode the same schema without cloning the domain envelope's strings.
use super::*;
use prost::{bytes::BufMut, encoding::*};

struct RefEvent<'a> {
    event: &'a CloudEvent,
    kind: Vec<u8>,
    payload: Vec<u8>,
    trace: Trace,
}
impl RefEvent<'_> {
    fn encode_raw(&self, out: &mut impl BufMut) {
        let e = self.event;
        uint32::encode(1, &u32::from(e.protocol_version), out);
        encode_key(2, WireType::LengthDelimited, out);
        encode_varint(16, out);
        out.put_slice(e.event_id.as_bytes());
        for (tag, value) in [
            (3, &e.runtime_id),
            (4, &e.strategy_id),
            (5, &e.live_session_id),
            (7, &e.entity_id),
        ] {
            if !value.is_empty() {
                string::encode(tag, value, out);
            }
        }
        if let Some(v) = &e.run_id {
            string::encode(6, v, out);
        }
        if e.entity_revision != 0 {
            uint64::encode(8, &e.entity_revision, out);
        }
        if let Some(v) = e.critical_sequence {
            uint64::encode(9, &v, out);
        }
        let priority = match e.priority {
            EventPriority::Critical => 0,
            EventPriority::Telemetry => 1,
            EventPriority::Audit => 2,
            EventPriority::Reconciliation => 3,
        };
        if priority != 0 {
            uint32::encode(10, &priority, out);
        }
        bytes::encode(11, &self.kind, out);
        bytes::encode(12, &self.payload, out);
        message::encode(13, &self.trace, out);
        if e.created_at.timestamp() != 0 {
            sint64::encode(14, &e.created_at.timestamp(), out);
        }
        if e.created_at.timestamp_subsec_nanos() != 0 {
            uint32::encode(15, &e.created_at.timestamp_subsec_nanos(), out);
        }
    }
    fn encoded_len(&self) -> usize {
        let e = self.event;
        let mut n = uint32::encoded_len(1, &u32::from(e.protocol_version)) + key_len(2) + 1 + 16;
        for (tag, value) in [
            (3, &e.runtime_id),
            (4, &e.strategy_id),
            (5, &e.live_session_id),
            (7, &e.entity_id),
        ] {
            if !value.is_empty() {
                n += string::encoded_len(tag, value);
            }
        }
        if let Some(v) = &e.run_id {
            n += string::encoded_len(6, v);
        }
        if e.entity_revision != 0 {
            n += uint64::encoded_len(8, &e.entity_revision);
        }
        if let Some(v) = e.critical_sequence {
            n += uint64::encoded_len(9, &v);
        }
        let priority = match e.priority {
            EventPriority::Critical => 0,
            EventPriority::Telemetry => 1,
            EventPriority::Audit => 2,
            EventPriority::Reconciliation => 3,
        };
        if priority != 0 {
            n += uint32::encoded_len(10, &priority);
        }
        n += bytes::encoded_len(11, &self.kind)
            + bytes::encoded_len(12, &self.payload)
            + message::encoded_len(13, &self.trace);
        if e.created_at.timestamp() != 0 {
            n += sint64::encoded_len(14, &e.created_at.timestamp());
        }
        if e.created_at.timestamp_subsec_nanos() != 0 {
            n += uint32::encoded_len(15, &e.created_at.timestamp_subsec_nanos());
        }
        n
    }
}
pub(super) fn encode(b: &LiveSyncBatch) -> Result<Vec<u8>, String> {
    let events = b
        .events()
        .map(|e| {
            Ok(RefEvent {
                event: e,
                kind: serde_json::to_vec(&e.kind).map_err(|e| e.to_string())?,
                payload: serde_json::to_vec(&e.payload).map_err(|e| e.to_string())?,
                trace: (&e.latency).into(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let version = u32::from(b.protocol_version);
    let mut len = uint32::encoded_len(1, &version);
    for (tag, v) in [
        (2, &b.runtime_id),
        (3, &b.strategy_id),
        (4, &b.live_session_id),
    ] {
        if !v.is_empty() {
            len += string::encoded_len(tag, v);
        }
    }
    if let Some(v) = b.first_critical_sequence {
        len += uint64::encoded_len(5, &v);
    }
    if let Some(v) = b.last_critical_sequence {
        len += uint64::encoded_len(6, &v);
    }
    for (i, e) in events.iter().enumerate() {
        len += key_len(if i < b.critical_events.len() { 7 } else { 8 })
            + encoded_len_varint(e.encoded_len() as u64)
            + e.encoded_len();
    }
    let mut out = Vec::with_capacity(len);
    uint32::encode(1, &version, &mut out);
    for (tag, v) in [
        (2, &b.runtime_id),
        (3, &b.strategy_id),
        (4, &b.live_session_id),
    ] {
        if !v.is_empty() {
            string::encode(tag, v, &mut out);
        }
    }
    if let Some(v) = b.first_critical_sequence {
        uint64::encode(5, &v, &mut out);
    }
    if let Some(v) = b.last_critical_sequence {
        uint64::encode(6, &v, &mut out);
    }
    for (i, e) in events.iter().enumerate() {
        encode_key(
            if i < b.critical_events.len() { 7 } else { 8 },
            WireType::LengthDelimited,
            &mut out,
        );
        encode_varint(e.encoded_len() as u64, &mut out);
        e.encode_raw(&mut out);
    }
    debug_assert_eq!(out.len(), len);
    Ok(out)
}
