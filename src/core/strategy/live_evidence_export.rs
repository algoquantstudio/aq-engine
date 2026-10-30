//! Read-only analytics delivery runs outside the strategy decision/reducer turn.
//! The command journal owns frozen pages until ALL chunks have durable outbox
//! receipts. Backpressure/errors retain the page; no broker action is submitted.
use crate::core::{
    broker::command_worker::BrokerCommandSender,
    live_sync::{
        CloudEvent, EventKind, EventPriority, EventSubject, LocalEventPublisher,
        deterministic_event_id,
    },
};
use aqe_live_protocol::{
    ExecutionEvidenceExportReceipt, ExecutionEvidenceFeedPage, execution_evidence_chunks,
};
use std::time::{Duration, Instant};
use tokio::{sync::oneshot, task::JoinHandle};
mod health;
mod scheduler;
pub(super) use health::EvidenceExportHealth;
use health::{ExportError, SharedBacklog};
pub(super) use scheduler::ExecutionExportScheduler;

#[derive(Clone)]
pub(super) struct ExportIdentity {
    pub runtime_id: String,
    pub strategy_id: String,
    pub session_id: String,
    pub run_id: String,
}

type ExportTask = (
    JoinHandle<()>,
    oneshot::Receiver<Result<Option<u64>, ExportError>>,
);
pub(super) struct ExecutionEvidenceExporter {
    stream: EvidenceStream,
    active: Option<ExportTask>,
    next_attempt: Instant,
    last_warning_at: Option<Instant>,
    backlog: SharedBacklog,
    pub health: EvidenceExportHealth,
}
#[derive(Clone, Copy)]
enum EvidenceStream {
    State,
    Fills,
    Marks,
}
impl EvidenceStream {
    fn label(self) -> &'static str {
        match self {
            Self::State => "state",
            Self::Fills => "fills",
            Self::Marks => "marks",
        }
    }
}

fn export_enabled(stream: EvidenceStream, value: Option<&str>) -> bool {
    value.map_or(matches!(stream, EvidenceStream::Marks), |v| {
        matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes")
    })
}

impl ExecutionEvidenceExporter {
    pub(super) fn has_inflight(&self) -> bool {
        self.active.is_some()
    }

    pub fn new() -> Self {
        Self::for_stream(EvidenceStream::State)
    }

    pub fn new_fills() -> Self {
        Self::for_stream(EvidenceStream::Fills)
    }
    pub fn new_marks() -> Self {
        Self::for_stream(EvidenceStream::Marks)
    }

    fn for_stream(stream: EvidenceStream) -> Self {
        // Fill delivery stays opt-in outside AQS. Marks default on; the legacy
        // duplicate Insight-state archive remains opt-in for CLI-only users
        // and is always disabled by protected AQS launch configuration.
        let setting = crate::launch_config::setting(match stream {
            EvidenceStream::State => "AQE_EXECUTION_EVIDENCE_EXPORT",
            EvidenceStream::Fills => "AQE_EXECUTION_FILL_EXPORT",
            EvidenceStream::Marks => "AQE_POSITION_MARK_EXPORT",
        })
        .ok()
        .flatten();
        let enabled = export_enabled(stream, setting.as_deref());
        Self {
            stream,
            active: None,
            next_attempt: Instant::now(),
            last_warning_at: None,
            backlog: Default::default(),
            health: EvidenceExportHealth {
                enabled,
                stream: stream.label(),
                status: if enabled { "idle" } else { "disabled" },
                delivery_basis: "durable_local_outbox_only",
                ..Default::default()
            },
        }
    }

    #[cfg(test)]
    pub fn poll(
        &mut self,
        sender: Option<BrokerCommandSender>,
        publisher: &LocalEventPublisher,
        identity: ExportIdentity,
    ) {
        self.poll_completion();
        self.try_start(sender, publisher, &identity);
    }

    /// Constant-time completion polling. No I/O or historical scan on the owner.
    fn poll_completion(&mut self) {
        if let Some((_, reply)) = self.active.as_mut() {
            let result = match reply.try_recv() {
                Ok(result) => result,
                Err(oneshot::error::TryRecvError::Empty) => return,
                Err(oneshot::error::TryRecvError::Closed) => Err(ExportError::Failure(
                    "execution evidence export task ended".into(),
                )),
            };
            self.active.take();
            self.record_result(result, Instant::now());
        }
    }

    fn try_start(
        &mut self,
        sender: Option<BrokerCommandSender>,
        publisher: &LocalEventPublisher,
        identity: &ExportIdentity,
    ) -> bool {
        if !self.health.enabled
            || self.active.is_some()
            || publisher.cloud_capture_paused()
            || self.next_attempt > Instant::now()
            || !publisher.supports_durable_admission()
        {
            return false;
        }
        let Some(sender) = sender else {
            return false;
        };
        let publisher = publisher.clone();
        let identity = identity.clone();
        let (reply, receiver) = oneshot::channel();
        self.health.last_attempt_at_us = Some(chrono::Utc::now().timestamp_micros());
        self.health.status = "exporting";
        let stream = self.stream;
        let backlog = self.backlog.clone();
        let task = tokio::spawn(async move {
            let result = tokio::time::timeout(
                Duration::from_secs(30),
                export_stream(&sender, &publisher, &identity, stream, &backlog),
            )
            .await
            .unwrap_or_else(|_| {
                Err(ExportError::Failure(
                    "execution evidence durable admission timed out; page retained".into(),
                ))
            });
            let _ = reply.send(result);
        });
        self.active = Some((task, receiver));
        true
    }

    /// Drain the final checkpoint before journal ownership stops. This only
    /// admits analytics to the durable local outbox; it neither waits for cloud
    /// delivery nor changes broker settlement. A failed/bounded-out drain keeps
    /// the frozen source page for the next launch, and must not fail trading stop.
    pub async fn finish(
        &mut self,
        sender: &BrokerCommandSender,
        publisher: &LocalEventPublisher,
        identity: &ExportIdentity,
        budget: Duration,
    ) -> Result<Option<u64>, String> {
        if !self.health.enabled {
            return Ok(None);
        }
        self.health.last_attempt_at_us = Some(chrono::Utc::now().timestamp_micros());
        let result = tokio::time::timeout(budget, async {
            // Join any in-flight handoff first. Starting a concurrent prepare
            // would race the shared journal permit and its frozen-page receipt.
            if let Some((_, reply)) = self.active.as_mut() {
                let completed = reply
                    .await
                    .map_err(|_| "execution evidence export task ended".to_owned())?;
                self.active.take();
                if let Ok(Some(sequence)) = completed {
                    self.health.last_exported_source_sequence = Some(sequence);
                }
                // A failed prior attempt retained its page. Retry it once here
                // rather than letting a normal poll backoff skip the final state.
            }
            if !publisher.supports_durable_admission() {
                return Err(
                    "execution evidence requires a durable local outbox; page retained".into(),
                );
            }
            loop {
                match export_stream(sender, publisher, identity, self.stream, &self.backlog)
                    .await
                    .map_err(|e| e.to_string())?
                {
                    Some(sequence) => self.health.last_exported_source_sequence = Some(sequence),
                    None => return Ok(self.health.last_exported_source_sequence),
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            Err("execution evidence shutdown budget expired; remaining page retained".into())
        });
        // An elapsed budget may leave the polling task awaiting admission. Join
        // its cancellation now; no detached task may use a stopped journal owner.
        if let Some((task, _)) = self.active.take() {
            task.abort();
            let _ = task.await;
        }
        self.health.last_error = result.as_ref().err().cloned();
        self.health.status = if result.is_ok() {
            "idle"
        } else {
            "deferred_on_stop"
        };
        self.refresh_health();
        result
    }
}

impl Drop for ExecutionEvidenceExporter {
    fn drop(&mut self) {
        if let Some((task, _)) = self.active.take() {
            task.abort();
        }
    }
}

async fn export_one(
    sender: &BrokerCommandSender,
    publisher: &LocalEventPublisher,
    identity: &ExportIdentity,
    backlog: &SharedBacklog,
) -> Result<Option<u64>, ExportError> {
    let sampled = chrono::Utc::now().timestamp_micros();
    let Some(page) = sender
        .try_prepare_execution_evidence_export()?
        .wait()
        .await?
    else {
        backlog.lock().empty(sampled);
        return Ok(None);
    };
    backlog.lock().page(
        page.generated_at_us,
        page.after_sequence,
        page.through_sequence,
        page.observations.len(),
        page.observations
            .iter()
            .map(|o| o.record.recorded_at_us)
            .min(),
    );
    let receipt = publish_page(publisher, identity, &page).await?;
    let sequence = receipt.next_sequence;
    acknowledge_when_available(|| {
        sender
            .try_acknowledge_execution_evidence_export(receipt.clone())
            .map(|reply| reply.wait())
    })
    .await?;
    backlog.lock().acknowledged();
    Ok(Some(sequence))
}

async fn export_stream(
    sender: &BrokerCommandSender,
    publisher: &LocalEventPublisher,
    identity: &ExportIdentity,
    stream: EvidenceStream,
    backlog: &SharedBacklog,
) -> Result<Option<u64>, ExportError> {
    if matches!(stream, EvidenceStream::State) {
        return export_one(sender, publisher, identity, backlog).await;
    }
    let sampled = chrono::Utc::now().timestamp_micros();
    if matches!(stream, EvidenceStream::Marks) {
        let Some(transfer) = sender.try_prepare_position_mark_export()?.wait().await? else {
            backlog.lock().empty(sampled);
            return Ok(None);
        };
        let receipt = publish_mark_page_observed(publisher, identity, &transfer, backlog).await?;
        let sequence = receipt.next_sequence;
        acknowledge_when_available(|| {
            sender
                .try_acknowledge_position_mark_export(receipt.clone())
                .map(|reply| reply.wait())
        })
        .await?;
        backlog.lock().acknowledged();
        return Ok(Some(sequence));
    }
    let Some(transfer) = sender.try_prepare_execution_fill_export()?.wait().await? else {
        backlog.lock().empty(sampled);
        return Ok(None);
    };
    let receipt = publish_fill_page_observed(publisher, identity, &transfer, backlog).await?;
    let sequence = receipt.next_sequence;
    acknowledge_when_available(|| {
        sender
            .try_acknowledge_execution_fill_export(receipt.clone())
            .map(|reply| reply.wait())
    })
    .await?;
    backlog.lock().acknowledged();
    Ok(Some(sequence))
}

/// Durable outbox admission already succeeded. Brief journal contention must
/// retry only its acknowledgement, not republish the frozen page. The caller's
/// 30-second task/stop budget owns cancellation; no permit is held while waiting.
async fn acknowledge_when_available<T, R>(
    mut request: impl FnMut() -> Result<R, crate::core::broker::commands::BrokerCommandError>,
) -> Result<T, ExportError>
where
    R: std::future::Future<Output = Result<T, crate::core::broker::commands::BrokerCommandError>>,
{
    loop {
        match request() {
            Ok(reply) => return reply.await.map_err(Into::into),
            Err(error) if error.is_analytics_backpressure() => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

#[cfg(test)]
async fn publish_mark_page(
    publisher: &LocalEventPublisher,
    identity: &ExportIdentity,
    transfer: &aqe_live_protocol::PositionMarkTransfer,
) -> Result<aqe_live_protocol::PositionMarkExportReceipt, String> {
    publish_mark_page_observed(publisher, identity, transfer, &SharedBacklog::default()).await.map_err(|e| e.to_string())
}

async fn publish_mark_page_observed(
    publisher: &LocalEventPublisher,
    identity: &ExportIdentity,
    transfer: &aqe_live_protocol::PositionMarkTransfer,
    backlog: &SharedBacklog,
) -> Result<aqe_live_protocol::PositionMarkExportReceipt, ExportError> {
    let page = transfer.page()?;
    backlog.lock().page(
        page.generated_at_us,
        page.after_sequence,
        page.through_sequence,
        page.records.len(),
        page.records.iter().map(|r| r.recorded_at_us).min(),
    );
    if identity
        .strategy_id
        .strip_prefix("strategy:")
        .unwrap_or(&identity.strategy_id)
        != page
            .strategy_id
            .strip_prefix("strategy:")
            .unwrap_or(&page.strategy_id)
    {
        return Err("position mark publisher scope mismatch".into());
    }
    let entity = format!(
        "position-mark:{}:{}",
        transfer.source_id, transfer.transfer_id
    );
    let mut event = CloudEvent::new(
        &identity.runtime_id,
        &identity.strategy_id,
        &identity.session_id,
        entity.clone(),
        transfer.next_sequence,
        EventPriority::Audit,
        EventKind::PositionMarkPage,
        serde_json::to_value(transfer).map_err(|e| e.to_string())?,
    );
    event.run_id = Some(identity.run_id.clone());
    event.event_id = deterministic_event_id(&format!(
        "aqs-mark-v1:{}:{}:{}",
        identity.run_id, identity.session_id, entity
    ));
    event.created_at = chrono::DateTime::from_timestamp_micros(page.generated_at_us)
        .ok_or("invalid position mark snapshot time")?;
    publisher
        .queue_durable(
            EventSubject::for_priority(
                &identity.runtime_id,
                &identity.strategy_id,
                EventPriority::Audit,
            ),
            event,
        )
        .map_err(|e| ExportError::admission(publisher, e))?
        .await
        .map_err(|_| "position mark receipt channel closed")?
        .map_err(|e| ExportError::admission(publisher, e))?;
    Ok(transfer.receipt())
}

#[cfg(test)]
async fn publish_fill_page(
    publisher: &LocalEventPublisher,
    identity: &ExportIdentity,
    transfer: &aqe_live_protocol::ExecutionFillTransfer,
) -> Result<aqe_live_protocol::ExecutionFillExportReceipt, String> {
    publish_fill_page_observed(publisher, identity, transfer, &SharedBacklog::default()).await.map_err(|e| e.to_string())
}

async fn publish_fill_page_observed(
    publisher: &LocalEventPublisher,
    identity: &ExportIdentity,
    transfer: &aqe_live_protocol::ExecutionFillTransfer,
    backlog: &SharedBacklog,
) -> Result<aqe_live_protocol::ExecutionFillExportReceipt, ExportError> {
    let page = transfer.page()?;
    backlog.lock().page(
        page.generated_at_us,
        page.after_sequence,
        page.through_sequence,
        page.records.len() + page.position_checks.len(),
        page.records
            .iter()
            .map(|r| r.recorded_at_us)
            .chain(page.position_checks.iter().map(|r| r.recorded_at_us))
            .min(),
    );
    if identity.strategy_id.trim_start_matches("strategy:")
        != page.strategy_id.trim_start_matches("strategy:")
    {
        return Err("execution fill publisher scope mismatch".into());
    }
    let entity = format!(
        "execution-fill:{}:{}",
        transfer.source_id, transfer.transfer_id
    );
    let mut event = CloudEvent::new(
        &identity.runtime_id,
        &identity.strategy_id,
        &identity.session_id,
        entity.clone(),
        transfer.next_sequence,
        EventPriority::Audit,
        EventKind::ExecutionFillPage,
        serde_json::to_value(transfer).map_err(|e| e.to_string())?,
    );
    event.run_id = Some(identity.run_id.clone());
    event.event_id = deterministic_event_id(&format!(
        "aqs-fill-v1:{}:{}:{}",
        identity.run_id, identity.session_id, entity
    ));
    event.created_at = chrono::DateTime::from_timestamp_micros(page.generated_at_us)
        .ok_or("invalid fill snapshot time")?;
    publisher
        .queue_durable(
            EventSubject::for_priority(
                &identity.runtime_id,
                &identity.strategy_id,
                EventPriority::Audit,
            ),
            event,
        )
        .map_err(|e| ExportError::admission(publisher, e))?
        .await
        .map_err(|_| "execution fill receipt channel closed")?
        .map_err(|e| ExportError::admission(publisher, e))?;
    Ok(transfer.receipt())
}

async fn publish_page(
    publisher: &LocalEventPublisher,
    identity: &ExportIdentity,
    page: &ExecutionEvidenceFeedPage,
) -> Result<ExecutionEvidenceExportReceipt, ExportError> {
    if identity.strategy_id.trim_start_matches("strategy:")
        != page.strategy_id.trim_start_matches("strategy:")
    {
        return Err("execution evidence publisher scope mismatch".into());
    }
    let chunks = execution_evidence_chunks(page)?;
    let receipt = ExecutionEvidenceExportReceipt {
        source_id: page.source_id.clone(),
        transfer_id: chunks[0].transfer_id.clone(),
        after_sequence: page.after_sequence,
        next_sequence: page.next_sequence,
    };
    let subject = EventSubject::for_priority(
        &identity.runtime_id,
        &identity.strategy_id,
        EventPriority::Audit,
    );
    // A page has at most 37 chunks (~1 MiB). Queue this bounded group before
    // waiting, so the publisher can persist micro-batches instead of paying a
    // separate transport fallback delay for every chunk while offline.
    let mut receipts = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        let entity = format!(
            "execution-evidence:{}:{}:{}",
            chunk.source_id, chunk.transfer_id, chunk.index
        );
        let mut event = CloudEvent::new(
            &identity.runtime_id,
            &identity.strategy_id,
            &identity.session_id,
            entity.clone(),
            chunk.next_sequence,
            EventPriority::Audit,
            EventKind::ExecutionEvidenceChunk,
            serde_json::to_value(chunk).map_err(|e| e.to_string())?,
        );
        event.run_id = Some(identity.run_id.clone());
        event.event_id = deterministic_event_id(&format!(
            "aqs-evidence-v2:{}:{}:{}",
            identity.run_id, identity.session_id, entity
        ));
        // Retries keep the exact envelope identity, including source snapshot
        // time. Transport receipt clocks are carried separately in LatencyTrace.
        event.created_at = chrono::DateTime::from_timestamp_micros(page.generated_at_us)
            .ok_or("invalid evidence snapshot time")?;
        let durable = publisher
            .queue_durable(subject.clone(), event)
            .map_err(|e| ExportError::admission(publisher, e))?;
        receipts.push(durable);
    }
    for durable in receipts {
        durable
            .await
            .map_err(|_| "execution evidence receipt channel closed")?
            .map_err(|e| ExportError::admission(publisher, e))?;
    }
    Ok(receipt)
}

#[cfg(test)]
mod tests;
