use super::{CloudEvent, EventId};
use crate::core::live_sync::FlatbufferEventCodecError;
use std::fmt;

#[derive(Debug)]
pub enum OutboxStoreError {
    Database(String),
    Codec(FlatbufferEventCodecError),
    InvalidRecord(String),
}

impl fmt::Display for OutboxStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(message) => write!(formatter, "outbox database error: {message}"),
            Self::Codec(error) => write!(formatter, "outbox codec error: {error}"),
            Self::InvalidRecord(message) => write!(formatter, "invalid outbox record: {message}"),
        }
    }
}

impl std::error::Error for OutboxStoreError {}

impl From<FlatbufferEventCodecError> for OutboxStoreError {
    fn from(value: FlatbufferEventCodecError) -> Self {
        Self::Codec(value)
    }
}

#[derive(Clone, Debug)]
pub struct OutboxEventRecord {
    pub event: CloudEvent,
    pub attempt_count: u32,
    pub next_attempt_at_us: i64,
    pub local_committed_at_us: i64,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug)]
pub struct DeadLetterRecord {
    pub event_id: EventId,
    pub strategy_id: String,
    pub reason: String,
    pub failed_at_us: i64,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct OutboxHealth {
    pub pending_critical: u64,
    pub pending_telemetry: u64,
    pub pending_audit: u64,
    pub pending_reconciliation: u64,
    pub dead_letters: u64,
    pub oldest_pending_age_ms: Option<u64>,
    pub last_local_ack_at_us: Option<i64>,
    pub last_remote_commit_at_us: Option<i64>,
    pub current_retry_delay_ms: u64,
    pub last_error: Option<String>,
    pub database_bytes: u64,
    pub reconciliation: Option<ReconciliationJobProgress>,
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReconciliationJobProgress {
    pub generation: String,
    pub run_id: Option<String>,
    pub status: String,
    pub candidate_count: u64,
    pub classified_count: u64,
    pub unresolved_count: u64,
    pub current_cursor: Option<String>,
    pub retry_count: u64,
    pub last_error: Option<String>,
}
