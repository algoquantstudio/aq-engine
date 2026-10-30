//! Low-latency, broker-neutral live synchronization primitives.
//!
//! The execution engine publishes immutable events to a local bus. Persistence, retry,
//! reconciliation, and remote database work are deliberately owned by a separate sync agent.

pub mod actions;
mod bus;
mod codec;
mod diagnostics;
mod capture_owner;
pub use capture_owner::{CAPTURE_OWNER_LEASE_VERSION, CaptureOwnerError, CaptureOwnerLease};
mod journal_recovery;
pub use journal_recovery::{JOURNAL_RECOVERY_LINK_VERSION, JournalRecoveryLink};
mod embedded;
mod gateway;
mod hosted_controls;
mod outbox;
mod protocol;
mod publisher;
mod runtime;
mod safety_transport;
mod capture_transport;
mod sync_agent;
#[cfg(test)]
pub(crate) mod capture_test_fixture;

pub use aqe_live_protocol::{HostedControlStatus, HostedControlTarget, HostedStopControl};
pub use aqe_live_protocol::{CAPTURE_GAP_NOTICE_VERSION, CAPTURE_GAP_LOCAL_HANDOFF_VERSION, CaptureGapNotice, CaptureGapReceipt};
pub use bus::{
    EventBusError, EventDelivery, EventSubscription, InProcessEventBus, LocalDeliveryHandle,
    LocalEventBus, NATS_SUBSCRIPTION_CAPACITY, NatsEventBus,
};
pub use codec::{FlatbufferEventCodec, FlatbufferEventCodecError};
pub(crate) use embedded::EmbeddedHostedSync;
/// Optional standalone hosted-sync session; no AQS server implementation is linked.
pub use embedded::EmbeddedHostedSync as HostedSyncSession;
pub use embedded::HostedSyncConfig;
pub use outbox::{
    PhysicalCaptureBudget,PhysicalCaptureLimit,PhysicalCaptureUsage,MIN_OPERATIONAL_HEADROOM_BYTES,
    CaptureCapacity, CapturePauseRecord, DEFAULT_OUTBOX_SOFT_LIMIT_BYTES, DeadLetterRecord,
    OutboxEventRecord, OutboxHealth, OutboxStore, OutboxStoreError, ReconciliationJobProgress,
};
pub(crate) use outbox::{install_physical_capture_spill_control,measure_physical_capture};
pub use protocol::{
    CloudEvent, EntityRevision, EventEnvelope, EventId, EventKind, EventPriority, EventSubject,
    InsightHistoryKind, InsightReconciliationStatus, LOCAL_EVENT_HARD_LIMIT_BYTES, LatencyTrace,
    PendingEntityEventsRequest, PendingEntityEventsResponse, RECONCILIATION_TARGET_BYTES,
    ReconciliationChunk, ReconciliationComplete, ReconciliationLifecycleRequest,
    ReconciliationLifecycleResult, ReconciliationPurpose, ReconciliationRecord,
    ReconciliationSnapshot, ReconciliationStart, RuntimeId, StrategyId, SyncAcknowledgement,
    SyncRejectionReason, deterministic_event_id, gateway_endpoint, now_epoch_micros,
};
pub use publisher::{
    CloudCaptureState, CloudCaptureStatus, LocalEventPublisher, LocalEventPublisherConfig,
    PublisherHealth,SourceJournalMonitor,
};
pub(crate) use publisher::CloudCaptureGate;
pub(crate) use publisher::FinalSourceJournal;
pub use runtime::{
    PUBLISHER_IMPORT_BOUND_VERSION,
    legacy_publisher_outbox_path, publisher_outbox_path,
    SOURCE_OBSERVATION_LIFECYCLE_VERSION,
    REGISTRATION_AUDIT_DIRECTORY, RegistrationAudit, RegistrationAuditDecision, RuntimeHostHealth,
    RuntimeHostMetadata, RuntimeLogSource, RuntimeStrategyRegistration,
};
pub use sync_agent::{
    HttpBatchSink, HttpBatchSinkConfig, LiveSyncBatch, LiveSyncBatchResult, ReceiverCaptureHealth,
    ReconciliationProcessResult, RemoteActionLiveSubscription, RemoteBatchSink,
    RemoteStrategyAction, RemoteSyncError, RemoteSyncErrorKind, RemoteSyncHealth, SyncAgent,
    SyncAgentConfig, SyncAgentError,
};

pub const LIVE_SYNC_PROTOCOL_VERSION: u16 = 2;

/// Installs AQE's process-wide Rustls provider before any HTTP, WebSocket, or NATS client is
/// constructed. AQE consistently selects AWS-LC so feature unification cannot leave Rustls with
/// two enabled providers and no deterministic default.
pub fn install_default_rustls_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    }
}
