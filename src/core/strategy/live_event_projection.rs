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
mod source_capture;
mod capture_notices;
mod lifecycle_recovery;

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
    deferred_start: lifecycle_recovery::DeferredStart,
    capture_notices: parking_lot::Mutex<capture_notices::CaptureNoticeLimiter>,
    source_monitor:Option<crate::core::live_sync::SourceJournalMonitor>,
    final_source_journal:Option<crate::core::live_sync::FinalSourceJournal>,
    pub(super) capture_recovery: super::live_capture_recovery::CaptureRecovery,
    pub(super) fill_export: super::live_analytics_export::ExecutionAnalyticsExporter,
    pub(super) mark_export: super::live_analytics_export::ExecutionAnalyticsExporter,
    export_schedule: super::live_analytics_export::ExecutionExportScheduler,
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
    pub(super) account_flow_source: Option<String>,
    pub(super) account_flow_cursor: u64,
    pub(super) account_flow_gap_reported: bool,
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
            deferred_start: Default::default(),
            capture_notices: Default::default(),
            source_monitor:None,
            final_source_journal:None,
            capture_recovery: Default::default(),
            fill_export: super::live_analytics_export::ExecutionAnalyticsExporter::new_fills(),
            mark_export: super::live_analytics_export::ExecutionAnalyticsExporter::new_marks(),
            export_schedule: Default::default(),
            control_receipts: Default::default(),
            pending_publications: Default::default(),
            publisher,
            runtime_id: crate::launch_config::setting("AQE_RUNTIME_ID")
                .ok()
                .flatten()
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            strategy_id: auth.strategy_id.clone(),
            live_session_id: auth
                .live_session_id
                .as_deref()
                .map(aqs_types::live_session_key)
                .unwrap_or_else(|| auth.session_id.clone()),
            run_id: crate::launch_config::setting("AQE_RUN_ID")
                .ok()
                .flatten()
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            entity_revisions: HashMap::new(),
            insight_projection_fingerprints: HashMap::new(),
            history_offsets: HashMap::new(),
            insight_states: HashMap::new(),
            split_insight_history: environment_flag_with_default("AQE_SPLIT_INSIGHT_HISTORY", true),
            last_account_projection_fingerprint: None,
            last_account_projection_at: None,
            account_flow_source: None,
            account_flow_cursor: 0,
            account_flow_gap_reported: false,
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

    /// At-least-once account audit evidence on the existing gateway path. The
    /// broker source/event identity remains in the payload for economic dedup;
    /// neither event count nor balance deltas are strategy P&L.
    pub(super) fn publish_account_cash_flows(
        &mut self,
        flows: &crate::core::broker::account_cash_flow::AccountCashFlows,
    ) {
        if self.account_flow_source.as_deref() != Some(flows.source_id.as_str()) {
            self.account_flow_source = Some(flows.source_id.clone());
            self.account_flow_cursor = 0;
            self.account_flow_gap_reported = false;
        }
        if self.publisher.cloud_capture_paused() {
            return;
        }
        if (flows.missing_prefix || flows.capacity_exceeded) && !self.account_flow_gap_reported {
            let event = self.event_at_revision(
                format!("account-flow-gap:{}", uuid::Uuid::new_v5(
                    &uuid::Uuid::NAMESPACE_OID, flows.source_id.as_bytes(),
                )),
                flows.cursor.max(1),
                EventPriority::Audit,
                EventKind::StrategyEvent,
                serde_json::json!({
                    "event_type": "account_cash_flow_gap", "level": "warn",
                    "title": "Account money-operation history is partial",
                    "message": "Retained observations are not complete funding history; missing operations are not zero.",
                    "payload": {
                        "schema_version": 1, "source_id": flows.source_id,
                        "account_id": flows.account_id, "missing_prefix": flows.missing_prefix,
                        "capacity_exceeded": flows.capacity_exceeded,
                    },
                }),
                None,
            );
            let subject = EventSubject::for_priority(
                &self.runtime_id, &self.strategy_id, EventPriority::Audit,
            );
            if self.publisher.try_publish_critical(subject, event).is_err() {
                return;
            }
            self.account_flow_gap_reported = true;
        }
        let mut rows = flows.records.values()
            .filter(|r| r.sequence > self.account_flow_cursor)
            .collect::<Vec<_>>();
        rows.sort_by_key(|r| r.sequence);
        // Bounded work per trading/projection turn. Pending source facts stay
        // in the cache; failed outbox admission does not advance this cursor.
        for record in rows.into_iter().take(8) {
            let identity = serde_json::to_vec(&(
                &flows.provider, &flows.account_id, &flows.currency,
                &flows.source_id, &record.event_id,
            )).expect("primitive flow identity");
            let entity = format!("account-flow:{}", uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, &identity));
            let event = self.event_at_revision(
                entity, record.sequence, EventPriority::Audit, EventKind::StrategyEvent,
                serde_json::json!({
                    "event_type": "account_cash_flow", "level": "info",
                    "title": "Account money operation observed",
                    "message": "Broker account evidence; not an Insight or attributed strategy profit.",
                    "created_at": chrono::DateTime::from_timestamp_micros(record.observed_at_us),
                    "payload": {
                        "schema_version": 1, "provider": flows.provider,
                        "source_id": flows.source_id, "account_id": flows.account_id,
                        "currency": flows.currency, "coverage": flows.coverage,
                        "missing_prefix": flows.missing_prefix,
                        "capacity_exceeded": flows.capacity_exceeded, "record": record,
                    },
                }),
                None,
            );
            let subject = EventSubject::for_priority(
                &self.runtime_id, &self.strategy_id, EventPriority::Audit,
            );
            // Optional analytics pressure cannot fill the mandatory lifecycle
            // retry queue or turn a full preview queue into trading failure.
            if self.publisher.try_publish_critical(subject, event).is_err() {
                break;
            }
            self.account_flow_cursor = record.sequence;
        }
    }

    pub(super) fn progress_execution_analytics(
        &mut self,
        sender: Option<crate::core::broker::command_worker::BrokerCommandSender>,
    ) {
        self.export_schedule.poll(
            [&mut self.fill_export, &mut self.mark_export],
            sender,
            &self.publisher,
            super::live_analytics_export::ExportIdentity {
                runtime_id: self.runtime_id.clone(),
                strategy_id: self.strategy_id.clone(),
                session_id: self.live_session_id.clone(),
                run_id: self.run_id.clone(),
            },
        );
    }

    pub(super) async fn finish_execution_analytics(
        &mut self,
        sender: &crate::core::broker::command_worker::BrokerCommandSender,
    ) -> Result<(), String> {
        let started = Instant::now();
        let identity = super::live_analytics_export::ExportIdentity {
            runtime_id: self.runtime_id.clone(),
            strategy_id: self.strategy_id.clone(),
            session_id: self.live_session_id.clone(),
            run_id: self.run_id.clone(),
        };
        let mut streams = [
            ("fills", &mut self.fill_export),
            ("marks", &mut self.mark_export),
        ];
        // Settle the scheduler's sole in-flight page before another stream
        // prepares a page. Preserve the existing shared ten-second stop budget.
        streams.sort_by_key(|(_, exporter)| !exporter.has_inflight());
        let mut errors = Vec::new();
        for (kind, exporter) in streams {
            let delivered = exporter.finish(sender, &self.publisher, &identity,
                Duration::from_secs(10).saturating_sub(started.elapsed())).await;
            match delivered {
                Ok(_) => {},
                Err(error) => errors.push(format!("{kind}: {error}")),
            }
        }
        if errors.is_empty() { Ok(()) } else { Err(errors.join("; ")) }
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
        // Fingerprinting already removed object nulls. The only later payload
        // mutation inserted a non-null revision, so preserve that normalized
        // value rather than walking every field a second time.
        let event = self.event_from_normalized_payload(
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
        self.event_from_normalized_payload(
            entity_id, revision, priority, kind, without_json_nulls(payload), latency,
        )
    }

    /// Private envelope construction for a payload normalized by this module.
    /// Ordinary publication/recovery callers keep the normalizing entry point.
    fn event_from_normalized_payload(
        &self,
        entity_id: impl Into<String>,
        revision: u64,
        priority: EventPriority,
        kind: EventKind,
        payload: serde_json::Value,
        latency: Option<LatencyTrace>,
    ) -> CloudEvent {
        let entity_id = entity_id.into();
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

    /// Stop facts need durable local acceptance, but must not require another
    /// trading-data slot at an intentional cloud capture pause. Compact receipt
    /// is explicitly NOT full lifecycle/history acceptance.
    pub(super) async fn publish_stop_operational_durable(&self, event: CloudEvent) -> bool {
        if !self.publisher.cloud_capture_paused() {
            if self.publish_critical_durable(event.clone()).await {
                return true;
            }
            if !self.publisher.cloud_capture_paused() {
                return false;
            }
        }
        let Some(observation) = aqe_live_protocol::SafetyObservation::from_event(&event) else {
            return false;
        };
        match self.publisher.persist_stop_facts(&[observation]).await {
            Ok(()) => true,
            Err(error) => {
                error!("Compact stop fact was not durably admitted: {error}");
                false
            }
        }
    }

    pub(super) async fn publish_stop_facts_durable(&self, events: Vec<CloudEvent>) -> bool {
        if events.len() > MAX_RETRY_EVENTS {
            return false;
        }
        if !self.publisher.cloud_capture_paused() {
            // Retain normal micro-batching; many repeated stop requests must
            // not add one batch-delay/transaction round trip per action.
            if self.publish_critical_batch_durable(events.clone()).await {
                return true;
            }
            if !self.publisher.cloud_capture_paused() {
                return false;
            }
        }
        let Some(observations) = events
            .iter()
            .map(aqe_live_protocol::SafetyObservation::from_event)
            .collect::<Option<Vec<_>>>()
        else {
            return false;
        };
        self.publisher
            .persist_stop_facts(&observations)
            .await
            .is_ok()
    }

    /// The caller must first prove broker settlement. This only distinguishes
    /// an explicitly recorded cloud-history gap from a failed evidence write;
    /// it neither clears dirty source history nor claims a full export receipt.
    pub(super) async fn defer_final_cloud_capture_for_stop(
        &self,
    ) -> Result<bool, crate::core::broker::types::BrokerError> {
        let deferred = self
            .publisher
            .has_recorded_capture_gap(&self.runtime_id, &self.strategy_id, &self.live_session_id, &self.run_id)
            .await
            .map_err(|error| {
                crate::core::broker::types::BrokerError::ConnectionError(error.to_string())
            })?;
        if deferred {
            warn!(
                "Broker settlement confirmed; final cloud capture deferred by a durable history gap. Accepted evidence remains retained."
            );
        }
        Ok(deferred)
    }

    /// Never complete a stop before its lifecycle fact is durable. Full-event
    /// failure still withholds completion; intentional capture pause alone uses
    /// separate bounded operational headroom and never advances the data cursor.
    pub(super) async fn publish_stop_completion_durable(
        &self,
        stopped: CloudEvent,
        actions: Vec<CloudEvent>,
    ) -> bool {
        if !self.publish_stop_operational_durable(stopped).await {
            error!("Stop action completion withheld: stopped lifecycle was not admitted");
            return false;
        }
        self.publish_stop_facts_durable(actions).await
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
                    Err(EventBusError::CloudCapturePaused) => return false,
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
            match tokio::time::timeout(remaining, receipt).await {
                Ok(Ok(Ok(_))) => {}
                Ok(Ok(Err(EventBusError::CloudCapturePaused))) => return false,
                _ => {
                    self.publisher.mark_admission_failed();
                    error!("Durable local event publication failed before its boundary completed");
                    return false;
                }
            }
        }
        true
    }

    fn publish_retained(&self, subject: EventSubject, event: CloudEvent) -> bool {
        use crate::core::live_sync::{EventBusError, FlatbufferEventCodec};
        if self.publisher.cloud_capture_paused() {
            // Do not turn intentional capture backpressure into an additional
            // retry queue or a fatal engine admission failure. Local preview is
            // still allowed; no cloud/durable acceptance is claimed.
            let _ = self.publisher.try_publish_critical(subject, event);
            return false;
        }
        let mut pending = self.pending_publications.lock();
        if pending.events.is_empty() {
            match self
                .publisher
                .try_publish_critical(subject.clone(), event.clone())
            {
                Ok(_) => return true,
                Err(EventBusError::CloudCapturePaused) => return false,
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
        if self.publisher.cloud_capture_paused() {
            return;
        }
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
                Err(EventBusError::CloudCapturePaused) => break,
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
        let observation = aqe_live_protocol::SafetyObservation::from_event(&event);
        let receipt = match self.publisher.queue_durable(subject, event) {
            Ok(receipt) => Some(receipt),
            Err(crate::core::live_sync::EventBusError::CloudCapturePaused)
                if observation.is_some() =>
            {
                None
            }
            Err(_) => return false,
        };
        let publisher = self.publisher.clone();
        tasks.push(tokio::spawn(async move {
            let accepted = match receipt {
                Some(receipt) => match tokio::time::timeout(Duration::from_secs(10), receipt).await
                {
                    Ok(Ok(Ok(_))) => true,
                    Ok(Ok(Err(crate::core::live_sync::EventBusError::CloudCapturePaused))) => {
                        match observation {
                            Some(observation) => {
                                publisher.persist_stop_facts(&[observation]).await.is_ok()
                            }
                            None => false,
                        }
                    }
                    _ => false,
                },
                None => match observation {
                    Some(observation) => publisher.persist_stop_facts(&[observation]).await.is_ok(),
                    None => false,
                },
            };
            if accepted {
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
            if matches!(error, crate::core::live_sync::EventBusError::CloudCapturePaused) {
                let status = self.publisher.cloud_capture_status();
                if let Some(level) = self.capture_notices.lock().level(&status, Instant::now()) {
                    log::log!(level,
                        "AQS cloud telemetry deferred: capture_state={} reason={}; local_buffer_bytes={} limit_bytes={:?}; accepted evidence is retained, local broker execution continues",
                        status.diagnostic_phase(), status.diagnostic_reason(now_epoch_micros()),
                        status.retained_bytes, status.limit_bytes);
                }
            } else {
                warn!("Failed to queue AQS telemetry update: {}", error);
            }
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
    async fn normalized_insight_envelope_matches_generic_cleanup_without_changing_identity() {
        let mut sync = fixture(Arc::new(InProcessEventBus::default()));
        let raw = serde_json::json!({
            "insight_id":"one", "state":"Filled", "revision":7, "missing":null,
            "zero":0, "disabled":false, "empty":"", "symbol":"ETHUSD",
            "nested":{"missing":null,"label":"سعر · 값 · 値"},
            "array":[null,{"missing":null,"keep":false},[],{}]
        });
        let actual = sync.insight_projection_event("one", 7, raw.clone()).unwrap();
        let mut expected_payload = raw.clone();
        expected_payload["revision"] = actual.entity_revision.into();
        expected_payload.sort_all_objects();
        let mut expected = sync.event_at_revision(actual.entity_id.clone(), actual.entity_revision,
            EventPriority::Critical, EventKind::InsightProjection, expected_payload, None);
        // Only constructor observation clocks vary between these two calls.
        assert!(actual.latency.runtime_received_at_us.is_some());
        expected.latency = actual.latency.clone();
        expected.created_at = actual.created_at;
        assert_eq!(serde_json::to_vec(&actual).unwrap(), serde_json::to_vec(&expected).unwrap(),
            "payload ordering, envelope fields and immutable retry bytes must agree");
        assert_eq!(actual.payload.get("missing"), None);
        assert!(actual.payload["array"][0].is_null());
        assert!(sync.insight_projection_event("one", 999, raw.clone()).is_none());
        let mut changed = raw.clone();
        changed["zero"] = 1.into();
        let next = sync.insight_projection_event("one", 6, changed).unwrap();
        assert!(next.entity_revision > actual.entity_revision);
        assert_ne!(next.event_id, actual.event_id);
        // Generic event/recovery paths still normalize arbitrary raw payloads.
        let generic = sync.event_at_revision("generic", 1, EventPriority::Telemetry,
            EventKind::Heartbeat, raw.clone(), None);
        assert_eq!(generic.payload, without_json_nulls(raw));
        sync.publisher.shutdown(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    #[ignore = "in-process optimized envelope microbenchmark; no broker, database or network"]
    async fn normalized_envelope_component_profile() {
        let sync = fixture(Arc::new(InProcessEventBus::default()));
        let mut rows = Vec::new();
        for (phase, normalized) in [false, true, true, false].into_iter().enumerate() {
            for width in [32, 1024, 32768] {
                let payload = without_json_nulls(serde_json::json!({
                    "insight_id":"one", "state":"Filled", "revision":7, "missing":null,
                    "symbol":"ETHUSD", "side":"Sell", "quantity":10., "entry_price":2309.99,
                    "strategy_type":"x".repeat(width), "disabled":false, "zero":0, "empty":"",
                    "nested":{"missing":null,"label":"سعر · 값 · 値","legs":[null,{"price":1.,"missing":null}]},
                    "partial_closes":[],"take_profit_levels":[2200.,2100.],"stop_loss_levels":[2400.]
                }));
                let mut samples = Vec::new();
                for i in 0..21_000 {
                    // The caller owns this already-normalized value in both
                    // cases. Exclude fixture cloning from the envelope clock.
                    let value = std::hint::black_box(payload.clone());
                    let started = Instant::now();
                    let event = if normalized {
                        sync.event_from_normalized_payload("insight:one", i, EventPriority::Critical,
                            EventKind::InsightProjection, value, None)
                    } else {
                        sync.event_at_revision("insight:one", i, EventPriority::Critical,
                            EventKind::InsightProjection, value, None)
                    };
                    let ns = started.elapsed().as_nanos();
                    std::hint::black_box(event);
                    if i >= 1_000 { samples.push(ns); }
                }
                samples.sort_unstable();
                rows.push(serde_json::json!({"phase":phase,"normalized":normalized,"string_bytes":width,
                    "samples":samples.len(),"p50_ns":samples[samples.len()/2],
                    "p95_ns":samples[(samples.len()*95).div_ceil(100)-1]}));
            }
        }
        println!("PUBLISHER_NORMALIZED_REPORT {}", serde_json::json!({"rows":rows,
            "source_sha256":hex::encode(Sha256::digest(include_bytes!("live_event_projection.rs"))),
            "scope":"In-process envelope construction only, clone and first normalization/fingerprinting excluded; no full-publisher, bandwidth, RSS or end-to-end latency claim"}));
        sync.publisher.shutdown(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn account_cash_flow_audit_is_not_profit_and_does_not_repeat_on_ordinary_refresh() {
        use crate::core::broker::account_cash_flow::AccountCashFlows;
        let bus=Arc::new(InProcessEventBus::acknowledged());
        let mut sync=fixture(bus.clone());
        let subject=EventSubject::for_priority(&sync.runtime_id,&sync.strategy_id,EventPriority::Audit);
        let mut events=bus.subscribe(subject).await.unwrap();
        let page: aqe_live_protocol::AccountCashFlowPage=serde_json::from_value(serde_json::json!({
            "version":1,"provider":"mt5-account-deals-v1","source_id":"ea-instance","account_id":"account","currency":"GBP",
            "from_us":10,"through_us":100,"coverage":"observed_only","first_after_sequence":0,"after_sequence":0,"next_sequence":1,"through_sequence":1,
            "records":[{"sequence":1,"event_id":"balance-ticket","kind":"balance","amount":-50.,"occurred_at_us":null,"observed_at_us":90,"broker_time_ms":20,"deleted":false}]
        })).unwrap();
        let mut state=AccountCashFlows::from_page(&page).unwrap();
        sync.publish_account_cash_flows(&state);
        let delivery=tokio::time::timeout(Duration::from_secs(2),events.recv()).await.unwrap().unwrap();
        assert_eq!(delivery.event.kind,EventKind::StrategyEvent);
        assert_eq!(delivery.event.priority,EventPriority::Audit);
        assert_eq!(delivery.event.payload["event_type"],"account_cash_flow");
        assert_eq!(delivery.event.payload["payload"]["record"]["kind"],"balance");
        assert_eq!(delivery.event.payload["payload"]["record"]["amount"],-50.);
        assert_eq!(delivery.event.payload["payload"]["coverage"],"observed_only");
        bus.acknowledge(&delivery, SyncAcknowledgement::LocalAccepted {
            event_ids: vec![delivery.event.event_id],
        }).await.unwrap();
        sync.publish_account_cash_flows(&state);
        assert_eq!(sync.account_flow_cursor,1);
        assert!(events.try_recv().is_err());
        let mut correction=page;correction.after_sequence=1;correction.next_sequence=2;correction.through_sequence=2;
        correction.records[0].sequence=2;correction.records[0].amount=-45.;
        state.apply(&correction).unwrap();
        // Delivery to a subscriber precedes release of the publisher's single
        // fixture slot. The real projection retries retained facts on its next
        // turn; exercise that path instead of assuming immediate queue capacity.
        tokio::time::timeout(Duration::from_secs(2), async {
            while sync.account_flow_cursor < 2 {
                sync.publish_account_cash_flows(&state);
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        let revised=tokio::time::timeout(Duration::from_secs(2),events.recv()).await.unwrap().unwrap();
        assert_eq!(revised.event.payload["payload"]["record"]["event_id"],"balance-ticket");
        assert_eq!(revised.event.payload["payload"]["record"]["amount"],-45.);
        bus.acknowledge(&revised, SyncAcknowledgement::LocalAccepted {
            event_ids: vec![revised.event.event_id],
        }).await.unwrap();
        assert_eq!(sync.account_flow_cursor,2);
        assert!(sync.pending_publications.lock().events.is_empty());
        assert!(!sync.publisher.admission_failed());
        sync.publisher.shutdown(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn capture_pause_keeps_control_receipts_and_stop_completion_durable() {
        let path = std::env::temp_dir().join(format!(
            "strategy-stop-headroom-{}.db",
            uuid::Uuid::new_v4()
        ));
        let store = OutboxStore::open(&path).await.unwrap();
        let bus = Arc::new(InProcessEventBus::acknowledged());
        let publisher = LocalEventPublisher::spawn(
            bus.clone(),
            Some(store.clone()),
            LocalEventPublisherConfig {
                capture_limit_bytes: Some(1024),
                ..Default::default()
            },
        );
        assert!(
            publisher
                .wait_for_capture_initialization(Duration::from_secs(2))
                .await
        );
        let mut sync = LocalBatchSyncState::new(
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
        assert!(!sync.defer_final_cloud_capture_for_stop().await.unwrap());
        let large = sync.event(
            "insight:large",
            EventPriority::Critical,
            EventKind::InsightProjection,
            serde_json::json!({"data":"x".repeat(4096)}),
            None,
        );
        assert!(
            !sync
                .publish_projection_batch_durable(vec![large], vec![])
                .await
        );
        assert!(publisher.cloud_capture_paused());
        assert!(sync.defer_final_cloud_capture_for_stop().await.unwrap());
        let subject = EventSubject::control(&sync.runtime_id, &sync.strategy_id);
        let mut controls = bus.subscribe(subject.clone()).await.unwrap();
        let control=sync.event("control:stop",EventPriority::Critical,EventKind::Control,
            serde_json::json!({"action_id":"strategy_actions:headroom","action":"stop","status":"pending"}),None);
        let control_id = control.event_id;
        let receipt = bus.publish_critical(subject, control).await.unwrap();
        let delivery = controls.recv().await.unwrap();
        let ack=sync.event("strategy-action:strategy_actions:headroom",EventPriority::Critical,
            EventKind::StrategyActionAcknowledgement,
            serde_json::json!({"action_id":"strategy_actions:headroom","action":"stop","status":"acknowledged"}),None);
        assert!(sync.publish_control_ack(ack, delivery));
        assert!(
            matches!(receipt.wait().await.unwrap(),crate::core::live_sync::SyncAcknowledgement::LocalAccepted{event_ids} if event_ids==[control_id])
        );
        let stopped = sync.event(
            "strategy-lifecycle",
            EventPriority::Critical,
            EventKind::StrategyLifecycle,
            serde_json::json!({"state":"stopped"}),
            None,
        );
        let completed=sync.event("strategy-action:strategy_actions:headroom",EventPriority::Critical,
            EventKind::StrategyActionAcknowledgement,
            serde_json::json!({"action_id":"strategy_actions:headroom","action":"stop","status":"completed"}),None);
        assert!(
            sync.publish_stop_completion_durable(stopped, vec![completed])
                .await
        );
        assert!(!publisher.admission_failed());
        assert_eq!(publisher.critical_sequence_cursor(), 0);
        assert_eq!(store.capture_retained_bytes().await.unwrap(), 0);
        assert_eq!(store.pending_count().await.unwrap(), 0);
        assert_eq!(
            store
                .retirement_uncommitted_for_scope("strategy", "session")
                .await
                .unwrap(),
            2
        );
        let database = turso::Builder::new_local(path.to_string_lossy().as_ref())
            .build()
            .await
            .unwrap();
        let reader = database.connect().unwrap();
        let mut rows = reader
            .query("SELECT observation FROM safety_observations", ())
            .await
            .unwrap();
        let mut states = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            let fact: aqe_live_protocol::SafetyObservation =
                serde_json::from_str(&row.get::<String>(0).unwrap()).unwrap();
            states.push(fact.state);
        }
        states.sort();
        assert_eq!(states, ["completed", "stopped"]);
        sync.shutdown(Duration::from_secs(2)).await;
        assert!(publisher.shutdown_complete());
    }

    #[tokio::test]
    async fn intentional_cloud_capture_pause_does_not_poison_strategy_admission() {
        let outbox = OutboxStore::open(std::env::temp_dir().join(format!(
            "strategy-capture-pause-{}.db",
            uuid::Uuid::new_v4()
        )))
        .await
        .unwrap();
        let publisher = LocalEventPublisher::spawn(
            Arc::new(InProcessEventBus::default()),
            Some(outbox.clone()),
            LocalEventPublisherConfig {
                capture_limit_bytes: Some(1024),
                ..Default::default()
            },
        );
        assert!(
            publisher
                .wait_for_capture_initialization(Duration::from_secs(2))
                .await
        );
        let mut sync = LocalBatchSyncState::new(
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
        let event = sync.event(
            "insight:large",
            EventPriority::Critical,
            EventKind::InsightProjection,
            serde_json::json!({"data":"x".repeat(4096)}),
            None,
        );
        assert!(
            !sync
                .publish_projection_batch_durable(vec![event], vec![])
                .await
        );
        assert!(publisher.cloud_capture_paused());
        assert!(
            !publisher.admission_failed(),
            "the live-loop fatal-admission guard must remain false"
        );
        for revision in 1..100 {
            let event = sync.event(
                format!("insight:{revision}"),
                EventPriority::Critical,
                EventKind::InsightProjection,
                serde_json::json!({"state":"Filled"}),
                None,
            );
            assert!(!sync.publish_critical(event));
            sync.flush_pending_publications();
        }
        assert_eq!(sync.pending_publications.lock().events.len(), 0);
        assert_eq!(publisher.critical_sequence_cursor(), 0);
        assert_eq!(outbox.capture_retained_bytes().await.unwrap(), 0);
        assert!(
            outbox
                .capture_pause_for_scope(&sync.runtime_id, &sync.strategy_id, &sync.live_session_id)
                .await
                .unwrap()
                .is_some()
        );
        assert!(!publisher.admission_failed());
        publisher.shutdown(Duration::from_secs(1)).await;
        assert!(publisher.shutdown_complete());
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
