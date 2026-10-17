use super::aqs_types::{self, AqsAuth};
use crate::core::live_sync::{
    CloudEvent, EventEnvelope, EventKind, EventPriority, EventSubject, InsightHistoryKind,
    LatencyTrace, LocalEventPublisher, RECONCILIATION_TARGET_BYTES, ReconciliationRecord,
    deterministic_event_id, now_epoch_micros,
};
use log::{error, warn};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

const MAX_RETRY_EVENTS: usize = 256;
const MAX_RETRY_BYTES: usize = 8 * 1024 * 1024;
const DURABLE_PUBLICATION_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Default)]
struct PendingPublications {
    events: VecDeque<(EventSubject, CloudEvent, usize)>,
    bytes: usize,
}

pub(super) fn advance_entity_revision(current: &mut u64, clock_revision: u64) -> u64 {
    *current = current.saturating_add(1).max(clock_revision.max(1));
    *current
}

pub(super) struct LocalBatchSyncState {
    pub(super) evidence_export: super::live_evidence_export::ExecutionEvidenceExporter,
    pub(super) fill_export: super::live_evidence_export::ExecutionEvidenceExporter,
    pub(super) mark_export: super::live_evidence_export::ExecutionEvidenceExporter,
    control_receipts: parking_lot::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    pending_publications: parking_lot::Mutex<PendingPublications>,
    pub(super) publisher: LocalEventPublisher,
    pub(super) runtime_id: String,
    pub(super) strategy_id: String,
    pub(super) live_session_id: String,
    pub(super) run_id: String,
    pub(super) entity_revisions: HashMap<String, u64>,
    // Only a fixed-size digest per retained insight, never a second snapshot/history.
    pub(super) insight_projection_fingerprints: HashMap<String, [u8; 32]>,
    pub(super) history_offsets: HashMap<String, usize>,
    pub(super) insight_states: HashMap<String, String>,
    pub(super) split_insight_history: bool,
    pub(super) last_account_projection_fingerprint: Option<[u8; 32]>,
    pub(super) last_account_projection_at: Option<Instant>,
    pub(super) last_metrics_projection_at: Option<Instant>,
    pub(super) last_equity_history_at: Option<Instant>,
    pub(super) last_equity_history_value: Option<f64>,
}

impl Drop for LocalBatchSyncState {
    fn drop(&mut self) {
        // Dropping a JoinHandle detaches it. Aborted owners must not leave
        // orphan tasks acknowledging controls after their execution is gone.
        for task in self.control_receipts.get_mut().drain(..) {
            task.abort();
        }
    }
}

impl LocalBatchSyncState {
    pub(super) fn new(publisher: LocalEventPublisher, auth: &AqsAuth) -> Self {
        Self {
            evidence_export: super::live_evidence_export::ExecutionEvidenceExporter::new(),
            fill_export: super::live_evidence_export::ExecutionEvidenceExporter::new_fills(),
            mark_export: super::live_evidence_export::ExecutionEvidenceExporter::new_marks(),
            control_receipts: Default::default(),
            pending_publications: Default::default(),
            publisher,
            runtime_id: crate::launch_config::setting("AQE_RUNTIME_ID").ok().flatten()
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            strategy_id: auth.strategy_id.clone(),
            live_session_id: auth
                .live_session_id
                .as_deref()
                .map(aqs_types::live_session_key)
                .unwrap_or_else(|| auth.session_id.clone()),
            run_id: crate::launch_config::setting("AQE_RUN_ID").ok().flatten()
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            entity_revisions: HashMap::new(),
            insight_projection_fingerprints: HashMap::new(),
            history_offsets: HashMap::new(),
            insight_states: HashMap::new(),
            split_insight_history: environment_flag_with_default("AQE_SPLIT_INSIGHT_HISTORY", true),
            last_account_projection_fingerprint: None,
            last_account_projection_at: None,
            last_metrics_projection_at: None,
            last_equity_history_at: None,
            last_equity_history_value: None,
        }
    }

    pub(super) fn next_revision(&mut self, entity_id: &str) -> u64 {
        // Revisions are projection cursors shared across process lifetimes, not counters local
        // to one AQE process. Starting every run at one caused lifecycle and heartbeat event IDs
        // to collide with an earlier run of the same live session. Epoch microseconds provide a
        // restart-stable floor, while the stored value preserves strict monotonicity when several
        // updates are created within the same clock tick.
        let clock_revision = now_epoch_micros().max(1) as u64;
        let revision = self
            .entity_revisions
            .entry(entity_id.to_string())
            .or_default();
        advance_entity_revision(revision, clock_revision)
    }

    pub(super) fn progress_execution_evidence(&mut self, sender: Option<crate::core::broker::command_worker::BrokerCommandSender>) {
        self.evidence_export.poll(sender.clone(), &self.publisher, super::live_evidence_export::ExportIdentity {
            runtime_id: self.runtime_id.clone(), strategy_id: self.strategy_id.clone(),
            session_id: self.live_session_id.clone(), run_id: self.run_id.clone(),
        });
        self.fill_export.poll(sender.clone(), &self.publisher, super::live_evidence_export::ExportIdentity {
            runtime_id: self.runtime_id.clone(), strategy_id: self.strategy_id.clone(),
            session_id: self.live_session_id.clone(), run_id: self.run_id.clone(),
        });
        self.mark_export.poll(sender, &self.publisher, super::live_evidence_export::ExportIdentity {
            runtime_id: self.runtime_id.clone(), strategy_id: self.strategy_id.clone(),
            session_id: self.live_session_id.clone(), run_id: self.run_id.clone(),
        });
    }

    pub(super) async fn finish_execution_evidence(
        &mut self,
        sender: &crate::core::broker::command_worker::BrokerCommandSender,
    ) -> Result<Option<u64>, String> {
        let started = Instant::now();
        let evidence = self.evidence_export
            .finish(
                sender,
                &self.publisher,
                &super::live_evidence_export::ExportIdentity {
                    runtime_id: self.runtime_id.clone(),
                    strategy_id: self.strategy_id.clone(),
                    session_id: self.live_session_id.clone(),
                    run_id: self.run_id.clone(),
                },
                Duration::from_secs(10),
            )
            .await;
        // One shared shutdown budget, and still attempt fills if state delivery
        // failed. Neither failure changes broker stop/recovery semantics.
        let fills = self.fill_export.finish(sender, &self.publisher,
            &super::live_evidence_export::ExportIdentity {
                runtime_id: self.runtime_id.clone(), strategy_id: self.strategy_id.clone(),
                session_id: self.live_session_id.clone(), run_id: self.run_id.clone(),
            }, Duration::from_secs(10).saturating_sub(started.elapsed())).await;
        let marks = self.mark_export.finish(sender, &self.publisher,
            &super::live_evidence_export::ExportIdentity {
                runtime_id:self.runtime_id.clone(),strategy_id:self.strategy_id.clone(),
                session_id:self.live_session_id.clone(),run_id:self.run_id.clone(),
            },Duration::from_secs(10).saturating_sub(started.elapsed())).await;
        let result=evidence.as_ref().ok().copied().flatten();
        let errors=[("state",evidence.err()),("fills",fills.err()),("marks",marks.err())]
            .into_iter().filter_map(|(kind,error)|error.map(|e|format!("{kind}: {e}"))).collect::<Vec<_>>();
        if errors.is_empty() {Ok(result)} else {Err(errors.join("; "))}
    }

    /// Advance an entity cursor while preserving a source-provided ordering floor.
    ///
    /// Broker adapters can expose more than one sequence domain (for example,
    /// command acknowledgements generated by the runtime and lifecycle events
    /// generated by the terminal). Consequently, a source sequence is useful
    /// ordering evidence but is not itself a unique projection revision. Keeping
    /// the cursor per entity prevents equal or regressing source sequences from
    /// assigning one deterministic event ID to different lifecycle payloads.
    pub(super) fn next_revision_with_floor(
        &mut self,
        entity_id: &str,
        source_revision: u64,
    ) -> u64 {
        let revision = self
            .entity_revisions
            .entry(entity_id.to_string())
            .or_default();
        advance_entity_revision(revision, source_revision.max(1))
    }

    pub(super) fn event(
        &mut self,
        entity_id: impl Into<String>,
        priority: EventPriority,
        kind: EventKind,
        payload: serde_json::Value,
        latency: Option<LatencyTrace>,
    ) -> CloudEvent {
        let entity_id = entity_id.into();
        let revision = self.next_revision(&entity_id);
        self.event_at_revision(entity_id, revision, priority, kind, payload, latency)
    }

    /// Domain time can stay fixed across several broker/pipeline mutations. It is
    /// not a sync revision. Unchanged dirty snapshots do not create another event;
    /// changed ones get a strictly increasing revision even at the same timestamp.
    pub(super) fn insight_projection_event(
        &mut self,
        insight_id: &str,
        source_revision: u64,
        payload: serde_json::Value,
    ) -> Option<CloudEvent> {
        let mut payload = without_json_nulls(payload);
        let object = payload
            .as_object_mut()
            .expect("insight projection is an object");
        object.remove("revision");
        payload.sort_all_objects();
        let digest: [u8; 32] =
            Sha256::digest(serde_json::to_vec(&payload).expect("JSON projection is serializable"))
                .into();
        if self.insight_projection_fingerprints.get(insight_id) == Some(&digest) {
            return None;
        }
        let entity_id = format!("insight:{insight_id}");
        let cursor = self.entity_revisions.entry(entity_id.clone()).or_default();
        let revision = advance_entity_revision(
            cursor,
            source_revision.max(now_epoch_micros().max(1) as u64),
        );
        payload["revision"] = revision.into();
        let event = self.event_at_revision(
            entity_id,
            revision,
            EventPriority::Critical,
            EventKind::InsightProjection,
            payload,
            None,
        );
        self.insight_projection_fingerprints
            .insert(insight_id.to_string(), digest);
        // The publisher retains this exact envelope for retries, including its
        // original creation timestamp. Never regenerate it under the same ID.
        Some(event)
    }

    pub(super) fn event_at_revision(
        &self,
        entity_id: impl Into<String>,
        revision: u64,
        priority: EventPriority,
        kind: EventKind,
        payload: serde_json::Value,
        latency: Option<LatencyTrace>,
    ) -> CloudEvent {
        let entity_id = entity_id.into();
        let payload = without_json_nulls(payload);
        let mut event = EventEnvelope::new(
            self.runtime_id.clone(),
            self.strategy_id.clone(),
            self.live_session_id.clone(),
            entity_id.clone(),
            revision,
            priority,
            kind.clone(),
            payload,
        );
        event.run_id = Some(self.run_id.clone());
        event.event_id = deterministic_event_id(&format!(
            "aqs-live-v2:{}:{}:{}:{}:{}",
            self.run_id, self.live_session_id, entity_id, revision, kind
        ));
        if let Some(latency) = latency {
            event.latency = latency;
            event
                .latency
                .runtime_received_at_us
                .get_or_insert_with(crate::core::live_sync::now_epoch_micros);
        }
        event
    }

    #[cfg(test)]
    pub(super) fn publish_critical(&self, event: CloudEvent) -> bool {
        let subject = EventSubject::for_priority(
            &self.runtime_id,
            &self.strategy_id,
            EventPriority::Critical,
        );
        self.publish_retained(subject, event)
    }

    /// Startup/teardown lifecycle boundaries must not advance until the exact
    /// envelope is committed to the local SQLite outbox. Ordinary projections
    /// remain nonblocking because their source state can be reproduced.
    pub(super) async fn publish_critical_durable(&self, event: CloudEvent) -> bool {
        self.publish_critical_batch_durable(vec![event]).await
    }

    /// Do not acknowledge a successful stop when its lifecycle projection could
    /// not enter durable storage. The failed-admission bit remains set so the
    /// shutdown caller reports incomplete synchronization.
    pub(super) async fn publish_stop_completion_durable(
        &self,
        stopped: CloudEvent,
        actions: Vec<CloudEvent>,
    ) -> bool {
        if !self.publish_critical_durable(stopped).await {
            error!("Stop action completion withheld: stopped lifecycle was not admitted");
            return false;
        }
        self.publish_critical_batch_durable(actions).await
    }

    pub(super) async fn publish_critical_batch_durable(&self, events: Vec<CloudEvent>) -> bool {
        let subject = EventSubject::for_priority(
            &self.runtime_id,
            &self.strategy_id,
            EventPriority::Critical,
        );
        self.publish_batch_durable(
            events
                .into_iter()
                .map(|event| (subject.clone(), event))
                .collect(),
        )
        .await
    }

    pub(super) async fn publish_projection_batch_durable(
        &self,
        projections: Vec<CloudEvent>,
        history: Vec<CloudEvent>,
    ) -> bool {
        let critical_subject = EventSubject::for_priority(
            &self.runtime_id,
            &self.strategy_id,
            EventPriority::Critical,
        );
        let audit_subject =
            EventSubject::for_priority(&self.runtime_id, &self.strategy_id, EventPriority::Audit);
        let mut events = Vec::with_capacity(projections.len() + history.len());
        events.extend(
            projections
                .into_iter()
                .map(|event| (critical_subject.clone(), event)),
        );
        events.extend(
            history
                .into_iter()
                .map(|event| (audit_subject.clone(), event)),
        );
        self.publish_batch_durable(events).await
    }

    pub(super) async fn publish_reconciliation_batch_durable(
        &self,
        events: Vec<CloudEvent>,
    ) -> bool {
        let subject = EventSubject::for_priority(
            &self.runtime_id,
            &self.strategy_id,
            EventPriority::Reconciliation,
        );
        self.publish_batch_durable(
            events
                .into_iter()
                .map(|event| (subject.clone(), event))
                .collect(),
        )
        .await
    }

    async fn publish_batch_durable(&self, events: Vec<(EventSubject, CloudEvent)>) -> bool {
        use crate::core::live_sync::EventBusError;
        if !self.publisher.supports_durable_admission() {
            // Standalone/in-process AQE remains usable without the hosted outbox.
            return events
                .into_iter()
                .all(|(subject, event)| self.publish_retained(subject, event));
        }
        let deadline = Instant::now() + DURABLE_PUBLICATION_TIMEOUT;
        let mut receipts = Vec::with_capacity(events.len());
        for (subject, event) in events {
            loop {
                match self.publisher.queue_durable(subject.clone(), event.clone()) {
                    Ok(receipt) => {
                        receipts.push(receipt);
                        break;
                    }
                    Err(EventBusError::PublisherCapacity) if Instant::now() < deadline => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(error) => {
                        self.publisher.mark_admission_failed();
                        error!("Durable local event admission failed: {error}");
                        return false;
                    }
                }
            }
        }
        for receipt in receipts {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if !matches!(
                tokio::time::timeout(remaining, receipt).await,
                Ok(Ok(Ok(_)))
            ) {
                self.publisher.mark_admission_failed();
                error!("Durable local event publication failed before its boundary completed");
                return false;
            }
        }
        true
    }

    fn publish_retained(&self, subject: EventSubject, event: CloudEvent) -> bool {
        use crate::core::live_sync::{EventBusError, FlatbufferEventCodec};
        let mut pending = self.pending_publications.lock();
        if pending.events.is_empty() {
            match self
                .publisher
                .try_publish_critical(subject.clone(), event.clone())
            {
                Ok(_) => return true,
                Err(EventBusError::PublisherCapacity) => {}
                Err(error) => {
                    self.publisher.mark_admission_failed();
                    error!("Local event publication failed: {error}");
                    return false;
                }
            }
        }
        if pending
            .events
            .iter()
            .any(|(_, queued, _)| queued.event_id == event.event_id)
        {
            return true;
        }
        let bytes = match FlatbufferEventCodec::encode(&event) {
            Ok(encoded) => encoded.len(),
            Err(_) => {
                self.publisher.mark_admission_failed();
                return false;
            }
        };
        if pending.events.len() >= MAX_RETRY_EVENTS
            || bytes > crate::core::live_sync::LOCAL_EVENT_HARD_LIMIT_BYTES
            || bytes > MAX_RETRY_BYTES.saturating_sub(pending.bytes)
        {
            self.publisher.mark_admission_failed();
            error!("Local event retry capacity exhausted; event was not accepted");
            return false;
        }
        pending.bytes += bytes;
        pending.events.push_back((subject, event, bytes));
        true
    }

    /// Bounded work per live-loop turn, preserving original IDs and FIFO order.
    pub(super) fn flush_pending_publications(&self) {
        use crate::core::live_sync::EventBusError;
        let mut pending = self.pending_publications.lock();
        for _ in 0..16 {
            let Some((subject, event, bytes)) = pending.events.front() else {
                break;
            };
            match self
                .publisher
                .try_publish_critical(subject.clone(), event.clone())
            {
                Ok(_) => {
                    pending.bytes -= *bytes;
                    pending.events.pop_front();
                }
                Err(EventBusError::PublisherCapacity) => break,
                Err(error) => {
                    self.publisher.mark_admission_failed();
                    error!("Retained local publication unavailable: {error}");
                    break;
                }
            }
        }
    }

    pub(super) async fn shutdown(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            self.flush_pending_publications();
            if self.pending_publications.lock().events.is_empty() {
                break;
            }
            if Instant::now() >= deadline {
                self.publisher.mark_admission_failed();
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        self.publisher
            .shutdown(deadline.saturating_duration_since(Instant::now()))
            .await;
        let receipts = std::mem::take(&mut *self.control_receipts.lock());
        for mut task in receipts {
            if tokio::time::timeout(
                deadline.saturating_duration_since(Instant::now()),
                &mut task,
            )
            .await
            .is_err()
            {
                task.abort(); // No transport ACK: the runtime still owns redelivery.
            }
        }
    }

    /// The runtime retains/redelivers control until SQLite has accepted the
    /// acknowledgement. Never wait for disk or transport in the strategy loop.
    pub(super) fn publish_control_ack(
        &self,
        event: CloudEvent,
        delivery: crate::core::live_sync::EventDelivery,
    ) -> bool {
        let mut tasks = self.control_receipts.lock();
        tasks.retain(|task| !task.is_finished());
        if tasks.len() >= MAX_RETRY_EVENTS {
            return false;
        }
        let subject = EventSubject::for_priority(
            &self.runtime_id,
            &self.strategy_id,
            EventPriority::Critical,
        );
        let Ok(receipt) = self.publisher.queue_durable(subject, event) else {
            return false;
        };
        let publisher = self.publisher.clone();
        tasks.push(tokio::spawn(async move {
            if matches!(
                tokio::time::timeout(Duration::from_secs(10), receipt).await,
                Ok(Ok(Ok(_)))
            ) {
                let _ = tokio::time::timeout(
                    Duration::from_millis(250),
                    publisher.acknowledge(
                        &delivery,
                        crate::core::live_sync::SyncAcknowledgement::LocalAccepted {
                            event_ids: vec![delivery.event.event_id],
                        },
                    ),
                )
                .await;
            }
        }));
        true
    }

    pub(super) fn publish_telemetry(&self, event: CloudEvent) {
        let subject = EventSubject::for_priority(
            &self.runtime_id,
            &self.strategy_id,
            EventPriority::Telemetry,
        );
        if let Err(error) = self.publisher.publish_telemetry(subject, event) {
            warn!("Failed to queue AQS telemetry update: {}", error);
        }
    }
}

pub(super) fn classify_insight_history(
    message: Option<&str>,
    state: &str,
) -> (InsightHistoryKind, Option<String>, Option<String>) {
    let Some(message) = message.map(str::trim).filter(|message| !message.is_empty()) else {
        return (InsightHistoryKind::Diagnostic, None, None);
    };
    if let Some(transition) = message.strip_prefix("State changed from ") {
        if let Some((from, to)) = transition.split_once(" to ") {
            return (
                InsightHistoryKind::StateTransition,
                Some(from.trim().to_string()),
                Some(to.trim().to_string()),
            );
        }
    }
    let normalized = message.to_ascii_lowercase();
    let configuration = [
        "quantity set",
        "take profit",
        "stop loss",
        "order type set",
        "order class set",
        "execution depends set",
        "limit price set",
        "stop price set",
        "period ",
        "parent id set",
    ]
    .iter()
    .any(|prefix| normalized.starts_with(prefix));
    if configuration {
        return (InsightHistoryKind::Configuration, None, None);
    }
    let execution = [
        "insight submitted",
        "insight filled",
        "insight closed",
        "filled at",
        "rejected:",
        "cancelled:",
        "order id set",
    ]
    .iter()
    .any(|prefix| normalized.starts_with(prefix));
    if execution {
        return (InsightHistoryKind::Execution, None, Some(state.to_string()));
    }
    (InsightHistoryKind::Diagnostic, None, None)
}

pub(super) fn chunk_reconciliation_records(
    records: Vec<ReconciliationRecord>,
) -> Vec<Vec<ReconciliationRecord>> {
    // Reserve headroom for the envelope and FlatBuffer table. The hard bus guard remains the
    // final authority, but normal reconciliation chunks should stay close to 64 KiB.
    let payload_target = RECONCILIATION_TARGET_BYTES.saturating_sub(2 * 1024);
    let mut chunks = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes = 0usize;
    for record in records {
        let record_bytes = serde_json::to_vec(&record)
            .map(|bytes| bytes.len())
            .unwrap_or(payload_target);
        let separator = usize::from(!current.is_empty());
        if !current.is_empty()
            && current_bytes
                .saturating_add(separator)
                .saturating_add(record_bytes)
                > payload_target
        {
            chunks.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes = current_bytes
            .saturating_add(usize::from(!current.is_empty()))
            .saturating_add(record_bytes);
        current.push(record);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

pub(super) fn without_json_nulls(mut value: serde_json::Value) -> serde_json::Value {
    match &mut value {
        serde_json::Value::Object(object) => {
            object.retain(|_, value| !value.is_null());
            for value in object.values_mut() {
                let nested = std::mem::take(value);
                *value = without_json_nulls(nested);
            }
        }
        serde_json::Value::Array(array) => {
            for value in array {
                let nested = std::mem::take(value);
                *value = without_json_nulls(nested);
            }
        }
        _ => {}
    }
    value
}

fn environment_flag_with_default(name: &str, default: bool) -> bool {
    match crate::launch_config::setting(name).ok().flatten() {
        Some(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        None => default,
    }
}

#[cfg(test)]
mod retained_publication_tests {
    use super::*;
    use crate::core::live_sync::{
        EventDelivery, EventSubscription, InProcessEventBus, LocalDeliveryHandle, LocalEventBus,
        LocalEventPublisherConfig, OutboxStore, SyncAcknowledgement,
    };
    use async_trait::async_trait;
    use std::sync::Arc;

    struct BlockingBus {
        inner: InProcessEventBus,
        release: tokio::sync::Semaphore,
    }

    impl Default for BlockingBus {
        fn default() -> Self {
            Self {
                inner: InProcessEventBus::default(),
                release: tokio::sync::Semaphore::new(0),
            }
        }
    }

    #[async_trait]
    impl LocalEventBus for BlockingBus {
        async fn publish_critical(
            &self,
            subject: EventSubject,
            event: CloudEvent,
        ) -> Result<LocalDeliveryHandle, crate::core::live_sync::EventBusError> {
            self.release.acquire().await.unwrap().forget();
            self.inner.publish_critical(subject, event).await
        }

        fn publish_telemetry(
            &self,
            subject: EventSubject,
            event: CloudEvent,
        ) -> Result<(), crate::core::live_sync::EventBusError> {
            self.inner.publish_telemetry(subject, event)
        }

        async fn subscribe(
            &self,
            subject: EventSubject,
        ) -> Result<EventSubscription, crate::core::live_sync::EventBusError> {
            self.inner.subscribe(subject).await
        }

        async fn acknowledge(
            &self,
            delivery: &EventDelivery,
            acknowledgement: SyncAcknowledgement,
        ) -> Result<(), crate::core::live_sync::EventBusError> {
            self.inner.acknowledge(delivery, acknowledgement).await
        }
    }

    fn fixture(bus: Arc<InProcessEventBus>) -> LocalBatchSyncState {
        LocalBatchSyncState::new(
            LocalEventPublisher::spawn(
                bus,
                None,
                LocalEventPublisherConfig {
                    critical_queue_events: 1,
                    ..Default::default()
                },
            ),
            &AqsAuth {
                access_method: "test".into(),
                session_id: "session".into(),
                session_secret: "test".into(),
                strategy_id: "strategy".into(),
                user_id: "users:test".into(),
                node_id: None,
                live_session_id: None,
                url: None,
            },
        )
    }

    #[tokio::test]
    async fn pressure_retains_exact_envelopes_in_order_and_shutdown_flushes() {
        let bus = Arc::new(InProcessEventBus::default());
        let mut sync = fixture(bus.clone());
        let subject = EventSubject::for_priority(
            &sync.runtime_id,
            &sync.strategy_id,
            EventPriority::Critical,
        );
        let mut subscription = bus.subscribe(subject).await.unwrap();
        let mut expected = Vec::new();
        for i in 0..4 {
            let event = sync.event(
                format!("one-shot-{i}"),
                EventPriority::Critical,
                EventKind::StrategyActionAcknowledgement,
                serde_json::json!({"index":i}),
                None,
            );
            expected.push((event.event_id, event.entity_revision));
            assert!(sync.publish_critical(event));
        }
        assert_eq!(sync.pending_publications.lock().events.len(), 3);
        assert!(!sync.publisher.admission_failed());
        sync.shutdown(Duration::from_secs(3)).await;
        assert!(sync.publisher.shutdown_complete());
        assert!(sync.pending_publications.lock().events.is_empty());
        assert_eq!(sync.pending_publications.lock().bytes, 0);
        for (index, (id, revision)) in expected.into_iter().enumerate() {
            let delivered = subscription.try_recv().unwrap().event;
            assert_eq!(
                (delivered.event_id, delivered.entity_revision),
                (id, revision)
            );
            assert_eq!(delivered.critical_sequence, Some(index as u64 + 1));
        }
        assert!(subscription.try_recv().is_err());
    }

    #[tokio::test]
    async fn retained_retry_budget_is_bounded_and_overflow_remains_visible() {
        let mut sync = fixture(Arc::new(InProcessEventBus::default()));
        for i in 0..=MAX_RETRY_EVENTS {
            let event = sync.event(
                format!("event-{i}"),
                EventPriority::Critical,
                EventKind::StrategyActionAcknowledgement,
                serde_json::json!({}),
                None,
            );
            assert!(sync.publish_critical(event));
        }
        let event = sync.event(
            "overflow",
            EventPriority::Critical,
            EventKind::StrategyActionAcknowledgement,
            serde_json::json!({}),
            None,
        );
        assert!(!sync.publish_critical(event));
        assert_eq!(
            sync.pending_publications.lock().events.len(),
            MAX_RETRY_EVENTS
        );
        assert!(sync.publisher.admission_failed());
        sync.shutdown(Duration::from_millis(20)).await;
        assert!(!sync.publisher.shutdown_complete());
    }

    #[tokio::test]
    async fn lifecycle_boundary_waits_for_sqlite_but_not_transport() {
        let path = std::env::temp_dir().join(format!(
            "aqe-lifecycle-boundary-{}.db",
            uuid::Uuid::new_v4()
        ));
        let outbox = OutboxStore::open(&path).await.unwrap();
        let bus = Arc::new(BlockingBus::default());
        let publisher = LocalEventPublisher::spawn(
            bus.clone(),
            Some(outbox.clone()),
            LocalEventPublisherConfig::default(),
        );
        let sync = LocalBatchSyncState::new(
            publisher.clone(),
            &AqsAuth {
                access_method: "test".into(),
                session_id: "session".into(),
                session_secret: "test".into(),
                strategy_id: "strategy".into(),
                user_id: "users:test".into(),
                node_id: None,
                live_session_id: None,
                url: None,
            },
        );
        let event = sync.event_at_revision(
            "strategy-lifecycle",
            1,
            EventPriority::Critical,
            EventKind::StrategyLifecycle,
            serde_json::json!({"state":"running"}),
            None,
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(1), sync.publish_critical_durable(event))
                .await
                .unwrap()
        );
        assert_eq!(
            outbox
                .pending_events_for_entity(
                    "strategy",
                    "session",
                    "strategy-lifecycle",
                    EventKind::StrategyLifecycle,
                    10,
                )
                .await
                .unwrap()
                .len(),
            1
        );

        bus.release.add_permits(1);
        publisher.shutdown(Duration::from_secs(2)).await;
        assert!(publisher.shutdown_complete());
        drop(sync);
        drop(publisher);
        drop(outbox);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn failed_stop_lifecycle_never_publishes_successful_action_completion() {
        let bus = Arc::new(InProcessEventBus::default());
        let mut sync = fixture(bus.clone());
        let subject = EventSubject::for_priority(
            &sync.runtime_id,
            &sync.strategy_id,
            EventPriority::Critical,
        );
        let mut subscription = bus.subscribe(subject).await.unwrap();
        let stopped = sync.event(
            "strategy-lifecycle",
            EventPriority::Critical,
            EventKind::StrategyLifecycle,
            serde_json::json!({"state":"stopped", "oversized": "x".repeat(
                crate::core::live_sync::LOCAL_EVENT_HARD_LIMIT_BYTES + 1
            )}),
            None,
        );
        let completion = sync.event(
            "strategy-action:stop",
            EventPriority::Critical,
            EventKind::StrategyActionAcknowledgement,
            serde_json::json!({"status":"completed"}),
            None,
        );
        assert!(
            !sync
                .publish_stop_completion_durable(stopped, vec![completion])
                .await
        );
        assert!(sync.publisher.admission_failed());
        sync.shutdown(Duration::from_secs(1)).await;
        assert!(subscription.try_recv().is_err());
        assert!(!sync.publisher.shutdown_complete());
    }

    #[tokio::test]
    async fn successful_stop_publishes_lifecycle_before_action_completion() {
        let bus = Arc::new(InProcessEventBus::default());
        let mut sync = fixture(bus.clone());
        let subject = EventSubject::for_priority(
            &sync.runtime_id,
            &sync.strategy_id,
            EventPriority::Critical,
        );
        let mut subscription = bus.subscribe(subject).await.unwrap();
        let stopped = sync.event(
            "strategy-lifecycle",
            EventPriority::Critical,
            EventKind::StrategyLifecycle,
            serde_json::json!({"state":"stopped"}),
            None,
        );
        let completion = sync.event(
            "strategy-action:stop",
            EventPriority::Critical,
            EventKind::StrategyActionAcknowledgement,
            serde_json::json!({"status":"completed"}),
            None,
        );
        assert!(
            sync.publish_stop_completion_durable(stopped, vec![completion])
                .await
        );
        sync.shutdown(Duration::from_secs(3)).await;
        assert!(sync.publisher.shutdown_complete());
        let lifecycle = subscription.try_recv().unwrap().event;
        let action = subscription.try_recv().unwrap().event;
        assert_eq!(lifecycle.kind, EventKind::StrategyLifecycle);
        assert_eq!(action.kind, EventKind::StrategyActionAcknowledgement);
        assert_eq!(lifecycle.critical_sequence, Some(1));
        assert_eq!(action.critical_sequence, Some(2));
        assert!(subscription.try_recv().is_err());
    }
}
