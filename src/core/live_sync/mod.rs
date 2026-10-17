//! Low-latency, broker-neutral live synchronization primitives.
//!
//! The execution engine publishes immutable events to a local bus. Persistence, retry,
//! reconciliation, and remote database work are deliberately owned by a separate sync agent.

pub mod actions;
mod bus;
mod codec;
mod embedded;
mod gateway;
mod hosted_controls;
mod outbox;
mod protocol;
mod publisher;
mod runtime;
mod sync_agent;

pub use aqe_live_protocol::{HostedControlStatus, HostedControlTarget, HostedStopControl};
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
    DEFAULT_OUTBOX_SOFT_LIMIT_BYTES, DeadLetterRecord, OutboxEventRecord, OutboxHealth,
    OutboxStore, OutboxStoreError, ReconciliationJobProgress,
};
pub use protocol::{
    CloudEvent, EntityRevision, EventEnvelope, EventId, EventKind, EventPriority, EventSubject,
    InsightHistoryKind, InsightReconciliationStatus, LOCAL_EVENT_HARD_LIMIT_BYTES, LatencyTrace,
    PendingEntityEventsRequest, PendingEntityEventsResponse, RECONCILIATION_TARGET_BYTES,
    ReconciliationChunk, ReconciliationComplete, ReconciliationLifecycleRequest,
    ReconciliationLifecycleResult, ReconciliationRecord, ReconciliationSnapshot,
    ReconciliationStart, RuntimeId, StrategyId, SyncAcknowledgement, SyncRejectionReason,
    deterministic_event_id, gateway_endpoint, now_epoch_micros,
};
pub use publisher::{LocalEventPublisher, LocalEventPublisherConfig, PublisherHealth};
pub use runtime::{
    REGISTRATION_AUDIT_DIRECTORY, RegistrationAudit, RegistrationAuditDecision, RuntimeHostHealth,
    RuntimeHostMetadata, RuntimeLogSource, RuntimeStrategyRegistration,
};
pub use sync_agent::{
    HttpBatchSink, HttpBatchSinkConfig, LiveSyncBatch, LiveSyncBatchResult,
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
