use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use uuid::Uuid;

use crate::LIVE_SYNC_PROTOCOL_VERSION;

pub type RuntimeId = String;
pub type StrategyId = String;
pub type EventId = Uuid;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventPriority {
    Critical,
    Telemetry,
    /// Durable append-only audit data. Audit records are acknowledged locally and retained,
    /// but are always drained after current lifecycle and telemetry projections.
    Audit,
    Reconciliation,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    StrategyLifecycle,
    InsightProjection,
    InsightStateHistory,
    /// Sanitized, chunked execution journal evidence; audit lane only.
    ExecutionEvidenceChunk,
    /// Exact broker fill revisions, independent from cumulative insight state.
    ExecutionFillPage,
    /// Scheduled broker valuations; no Insight mutation or fill semantics.
    PositionMarkPage,
    BrokerOrderLifecycle,
    StrategyActionAcknowledgement,
    AccountProjection,
    LiveMetricsProjection,
    EquityProjection,
    Heartbeat,
    StrategyEvent,
    Log,
    ReconciliationStart,
    ReconciliationChunk,
    ReconciliationComplete,
    ReconciliationLifecycleRequest,
    ReconciliationLifecycleResult,
    /// Protocol-v1 compatibility only. New publishers must emit bounded v2 messages.
    ReconciliationSnapshot,
    ReconciliationProgress,
    SyncHealth,
    Control,
    Custom(String),
}

impl EventKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::StrategyLifecycle => "strategy_lifecycle",
            Self::InsightProjection => "insight_projection",
            Self::InsightStateHistory => "insight_state_history",
            Self::ExecutionEvidenceChunk => "execution_evidence_chunk",
            Self::ExecutionFillPage => "execution_fill_page",
            Self::PositionMarkPage => "position_mark_page",
            Self::BrokerOrderLifecycle => "broker_order_lifecycle",
            Self::StrategyActionAcknowledgement => "strategy_action_acknowledgement",
            Self::AccountProjection => "account_projection",
            Self::LiveMetricsProjection => "live_metrics_projection",
            Self::EquityProjection => "equity_projection",
            Self::Heartbeat => "heartbeat",
            Self::StrategyEvent => "strategy_event",
            Self::Log => "log",
            Self::ReconciliationStart => "reconciliation_start",
            Self::ReconciliationChunk => "reconciliation_chunk",
            Self::ReconciliationComplete => "reconciliation_complete",
            Self::ReconciliationLifecycleRequest => "reconciliation_lifecycle_request",
            Self::ReconciliationLifecycleResult => "reconciliation_lifecycle_result",
            Self::ReconciliationSnapshot => "reconciliation_snapshot",
            Self::ReconciliationProgress => "reconciliation_progress",
            Self::SyncHealth => "sync_health",
            Self::Control => "control",
            Self::Custom(value) => value,
        }
    }
}

impl fmt::Display for EventKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LatencyTrace {
    pub source_occurred_at_us: Option<i64>,
    pub adapter_queued_at_us: Option<i64>,
    pub runtime_received_at_us: Option<i64>,
    pub local_bus_published_at_us: Option<i64>,
    pub local_outbox_committed_at_us: Option<i64>,
    pub persistence_request_sent_at_us: Option<i64>,
    pub persistence_committed_at_us: Option<i64>,
    pub persistence_ack_received_at_us: Option<i64>,
    pub client_applied_at_us: Option<i64>,
    #[serde(default)]
    pub clock_offset_us: Option<i64>,
    #[serde(default)]
    pub clock_uncertainty_us: Option<u64>,
}

impl LatencyTrace {
    pub fn mark_runtime_received(mut self) -> Self {
        self.runtime_received_at_us = Some(now_epoch_micros());
        self
    }

    pub fn mark_local_bus_published(&mut self) {
        self.local_bus_published_at_us = Some(now_epoch_micros());
    }

    pub fn mark_local_outbox_committed(&mut self) {
        self.local_outbox_committed_at_us = Some(now_epoch_micros());
    }

    pub fn final_latency_us(&self) -> Option<i64> {
        let start = self.source_occurred_at_us.or(self.runtime_received_at_us)?;
        let end = self
            .client_applied_at_us
            .or(self.persistence_ack_received_at_us)
            .or(self.local_outbox_committed_at_us)
            .or(self.local_bus_published_at_us)?;
        Some(end.saturating_sub(start))
    }

    pub fn should_retain_trace(&self, is_critical: bool) -> bool {
        is_critical
            || self
                .final_latency_us()
                .is_some_and(|value| value >= 250_000)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityRevision {
    pub entity_id: String,
    pub revision: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventEnvelope<T> {
    pub protocol_version: u16,
    pub event_id: EventId,
    pub runtime_id: RuntimeId,
    pub strategy_id: StrategyId,
    pub live_session_id: String,
    /// Identifies one concrete strategy process. A live session may contain many sequential
    /// process runs, so the session ID alone cannot distinguish stale projections.
    #[serde(default)]
    pub run_id: Option<String>,
    pub entity_id: String,
    pub entity_revision: u64,
    pub critical_sequence: Option<u64>,
    pub priority: EventPriority,
    pub kind: EventKind,
    pub payload: T,
    pub latency: LatencyTrace,
    pub created_at: DateTime<Utc>,
}

pub type CloudEvent = EventEnvelope<Value>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingEntityEventsRequest {
    pub strategy_id: String,
    pub live_session_id: String,
    pub entity_id: String,
    pub event_kind: EventKind,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingEntityEventsResponse {
    #[serde(default)]
    pub events: Vec<CloudEvent>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReconciliationRecord {
    pub record_type: String,
    pub entity_id: String,
    pub payload: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReconciliationSnapshot {
    pub generation: String,
    pub runtime_id: String,
    pub strategy_id: String,
    pub live_session_id: String,
    pub snapshot_complete: bool,
    pub critical_cutoff: Option<u64>,
    #[serde(default)]
    pub records: Vec<ReconciliationRecord>,
}

pub const RECONCILIATION_TARGET_BYTES: usize = 64 * 1024;
pub const LOCAL_EVENT_HARD_LIMIT_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReconciliationStart {
    pub generation: String,
    pub runtime_id: String,
    pub strategy_id: String,
    pub live_session_id: String,
    #[serde(default)]
    pub run_id: Option<String>,
    pub snapshot_complete: bool,
    /// Broker-neutral lifecycle capabilities captured with this authoritative snapshot.
    /// Strings keep protocol compatibility with external broker implementations.
    #[serde(default)]
    pub broker_features: Vec<String>,
    pub critical_cutoff: Option<u64>,
    pub total_chunks: u32,
    pub total_records: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReconciliationChunk {
    pub generation: String,
    pub chunk_index: u32,
    #[serde(default)]
    pub records: Vec<ReconciliationRecord>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReconciliationComplete {
    pub generation: String,
    pub total_chunks: u32,
    pub total_records: u64,
    pub snapshot_complete: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReconciliationLifecycleRequest {
    pub generation: String,
    pub request_id: String,
    #[serde(default)]
    pub order_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReconciliationLifecycleResult {
    pub generation: String,
    pub request_id: String,
    #[serde(default)]
    pub results: Vec<ReconciliationRecord>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InsightHistoryKind {
    StateTransition,
    Configuration,
    Execution,
    Diagnostic,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InsightReconciliationStatus {
    Current,
    ConfirmedOpen,
    Reconciling,
    Unresolved,
}

impl<T> EventEnvelope<T> {
    pub fn new(
        runtime_id: impl Into<String>,
        strategy_id: impl Into<String>,
        live_session_id: impl Into<String>,
        entity_id: impl Into<String>,
        entity_revision: u64,
        priority: EventPriority,
        kind: EventKind,
        payload: T,
    ) -> Self {
        Self {
            protocol_version: LIVE_SYNC_PROTOCOL_VERSION,
            event_id: Uuid::new_v4(),
            runtime_id: runtime_id.into(),
            strategy_id: strategy_id.into(),
            live_session_id: live_session_id.into(),
            run_id: None,
            entity_id: entity_id.into(),
            entity_revision,
            critical_sequence: None,
            priority,
            kind,
            payload,
            latency: LatencyTrace::default().mark_runtime_received(),
            created_at: Utc::now(),
        }
    }

    pub fn map_payload<U>(self, map: impl FnOnce(T) -> U) -> EventEnvelope<U> {
        EventEnvelope {
            protocol_version: self.protocol_version,
            event_id: self.event_id,
            runtime_id: self.runtime_id,
            strategy_id: self.strategy_id,
            live_session_id: self.live_session_id,
            run_id: self.run_id,
            entity_id: self.entity_id,
            entity_revision: self.entity_revision,
            critical_sequence: self.critical_sequence,
            priority: self.priority,
            kind: self.kind,
            payload: map(self.payload),
            latency: self.latency,
            created_at: self.created_at,
        }
    }
}

impl<T: Serialize> EventEnvelope<T> {
    pub fn into_cloud_event(self) -> Result<CloudEvent, serde_json::Error> {
        let payload = serde_json::to_value(&self.payload)?;
        Ok(self.map_payload(|_| payload))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncRejectionReason {
    InvalidProtocol,
    InvalidPayload,
    Unauthorized,
    CrossScopeWrite,
    SequenceConflict,
    TerminalStateRegression,
    PayloadTooLarge,
    PermanentlyRejected,
    Other(String),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum SyncAcknowledgement {
    LocalAccepted {
        event_ids: Vec<EventId>,
    },
    Persisted {
        critical_through: u64,
        telemetry_revisions: Vec<EntityRevision>,
    },
    Rejected {
        event_id: EventId,
        reason: SyncRejectionReason,
    },
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct EventSubject(String);

impl EventSubject {
    pub fn new(value: impl Into<String>) -> Result<Self, &'static str> {
        let value = value.into();
        if value.is_empty()
            || value.starts_with('.')
            || value.ends_with('.')
            || value.chars().any(char::is_whitespace)
        {
            return Err("invalid event subject");
        }
        Ok(Self(value))
    }

    pub fn for_priority(runtime_id: &str, strategy_id: &str, priority: EventPriority) -> Self {
        let suffix = match priority {
            EventPriority::Critical => "critical",
            EventPriority::Telemetry => "telemetry",
            EventPriority::Audit => "audit",
            EventPriority::Reconciliation => "reconcile",
        };
        Self(format!(
            "aqs.local.v1.{}.{}.{}",
            sanitize_subject_token(runtime_id),
            sanitize_subject_token(strategy_id),
            suffix
        ))
    }

    /// Fresh local events are copied to this subject once. Durable outbox replay deliberately
    /// never targets it, preventing a historical backlog from flooding the local UI.
    pub fn provisional(runtime_id: &str, strategy_id: &str) -> Self {
        Self(format!(
            "aqs.local.v1.{}.{}.provisional",
            sanitize_subject_token(runtime_id),
            sanitize_subject_token(strategy_id)
        ))
    }

    pub fn logs(runtime_id: &str, strategy_id: &str) -> Self {
        Self(format!(
            "aqs.local.v1.{}.{}.logs",
            sanitize_subject_token(runtime_id),
            sanitize_subject_token(strategy_id)
        ))
    }

    pub fn control(runtime_id: &str, strategy_id: &str) -> Self {
        Self(format!(
            "aqs.local.v1.{}.{}.control",
            sanitize_subject_token(runtime_id),
            sanitize_subject_token(strategy_id)
        ))
    }

    pub fn sync_status(runtime_id: &str, strategy_id: &str) -> Self {
        Self(format!(
            "aqs.local.v1.{}.{}.sync-status",
            sanitize_subject_token(runtime_id),
            sanitize_subject_token(strategy_id)
        ))
    }

    pub fn reconciliation_lifecycle_request(runtime_id: &str, strategy_id: &str) -> Self {
        Self(format!(
            "aqs.local.v1.{}.{}.reconcile-lifecycle-request",
            sanitize_subject_token(runtime_id),
            sanitize_subject_token(strategy_id)
        ))
    }

    pub fn reconciliation_lifecycle_result(runtime_id: &str, strategy_id: &str) -> Self {
        Self(format!(
            "aqs.local.v1.{}.{}.reconcile-lifecycle-result",
            sanitize_subject_token(runtime_id),
            sanitize_subject_token(strategy_id)
        ))
    }

    pub fn pending_entity_lookup(runtime_id: &str) -> Self {
        Self(format!(
            "aqs.local.v1.{}.pending-entity-lookup",
            sanitize_subject_token(runtime_id)
        ))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EventSubject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

fn sanitize_subject_token(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

pub fn now_epoch_micros() -> i64 {
    Utc::now().timestamp_micros()
}

/// Builds an idempotency key that remains stable when a publisher retries the same entity
/// revision after an acknowledgement or process boundary is lost.
pub fn deterministic_event_id(key: &str) -> EventId {
    Uuid::new_v5(&Uuid::NAMESPACE_URL, key.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subjects_do_not_allow_user_values_to_create_wildcards() {
        let subject =
            EventSubject::for_priority("runtime.*", "strategy.>", EventPriority::Critical);
        assert_eq!(
            subject.as_str(),
            "aqs.local.v1.runtime__.strategy__.critical"
        );
    }

    #[test]
    fn critical_traces_are_always_retained() {
        assert!(LatencyTrace::default().should_retain_trace(true));
    }
}
