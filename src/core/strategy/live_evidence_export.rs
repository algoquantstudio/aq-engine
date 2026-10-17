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

#[derive(Clone)]
pub(super) struct ExportIdentity {
    pub runtime_id: String,
    pub strategy_id: String,
    pub session_id: String,
    pub run_id: String,
}

#[derive(Default, serde::Serialize)]
pub(super) struct EvidenceExportHealth {
    pub enabled: bool,
    /// Source-local cursor acknowledged by the outbox, not its transport cursor.
    pub last_exported_source_sequence: Option<u64>,
    pub last_error: Option<String>,
    pub last_attempt_at_us: Option<i64>,
    /// A local handoff is not cloud persistence or broker reconciliation.
    pub delivery_basis: &'static str,
}

type ExportTask = (
    JoinHandle<()>,
    oneshot::Receiver<Result<Option<u64>, String>>,
);
pub(super) struct ExecutionEvidenceExporter {
    stream: EvidenceStream,
    active: Option<ExportTask>,
    next_attempt: Instant,
    pub health: EvidenceExportHealth,
}
#[derive(Clone, Copy)]
enum EvidenceStream {
    State,
    Fills,
    Marks,
}

fn export_enabled(stream: EvidenceStream, value: Option<&str>) -> bool {
    value.map_or(matches!(stream, EvidenceStream::Marks), |v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
}

impl ExecutionEvidenceExporter {
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
        .ok().flatten();
        let enabled = export_enabled(stream, setting.as_deref());
        Self {
            stream,
            active: None,
            next_attempt: Instant::now(),
            health: EvidenceExportHealth {
                enabled,
                delivery_basis: "durable_local_outbox_only",
                ..Default::default()
            },
        }
    }

    /// Constant-time polling and at most one bounded background task. No I/O,
    /// checkpoint serialization or historical scan on the strategy loop.
    pub fn poll(
        &mut self,
        sender: Option<BrokerCommandSender>,
        publisher: &LocalEventPublisher,
        identity: ExportIdentity,
    ) {
        if let Some((_, reply)) = self.active.as_mut() {
            let result = match reply.try_recv() {
                Ok(result) => result,
                Err(oneshot::error::TryRecvError::Empty) => return,
                Err(oneshot::error::TryRecvError::Closed) => {
                    Err("execution evidence export task ended".into())
                }
            };
            self.active.take();
            match result {
                Ok(Some(sequence)) => {
                    self.health.last_exported_source_sequence = Some(sequence);
                    self.health.last_error = None;
                    self.next_attempt = Instant::now() + Duration::from_millis(250);
                }
                Ok(None) => {
                    self.health.last_error = None;
                    self.next_attempt = Instant::now() + Duration::from_secs(2);
                }
                Err(error) => {
                    if self.health.last_error.as_ref() != Some(&error) {
                        log::warn!("Execution analytics delivery deferred: {error}");
                    }
                    self.health.last_error = Some(error);
                    self.next_attempt = Instant::now() + Duration::from_secs(5);
                }
            }
        }
        if !self.health.enabled
            || self.next_attempt > Instant::now()
            || !publisher.supports_durable_admission()
        {
            return;
        }
        let Some(sender) = sender else {
            return;
        };
        let publisher = publisher.clone();
        let (reply, receiver) = oneshot::channel();
        self.health.last_attempt_at_us = Some(chrono::Utc::now().timestamp_micros());
        let stream = self.stream;
        let task = tokio::spawn(async move {
            let result = tokio::time::timeout(
                Duration::from_secs(30),
                export_stream(&sender, &publisher, &identity, stream),
            )
            .await
            .unwrap_or_else(|_| {
                Err("execution evidence durable admission timed out; page retained".into())
            });
            let _ = reply.send(result);
        });
        self.active = Some((task, receiver));
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
                match export_stream(sender, publisher, identity, self.stream).await? {
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
) -> Result<Option<u64>, String> {
    let Some(page) = sender
        .try_prepare_execution_evidence_export()
        .map_err(|e| e.to_string())?
        .wait()
        .await
        .map_err(|e| e.to_string())?
    else {
        return Ok(None);
    };
    let receipt = publish_page(publisher, identity, &page).await?;
    let sequence = receipt.next_sequence;
    sender
        .try_acknowledge_execution_evidence_export(receipt)
        .map_err(|e| e.to_string())?
        .wait()
        .await
        .map_err(|e| e.to_string())?;
    Ok(Some(sequence))
}

async fn export_stream(
    sender: &BrokerCommandSender,
    publisher: &LocalEventPublisher,
    identity: &ExportIdentity,
    stream: EvidenceStream,
) -> Result<Option<u64>, String> {
    if matches!(stream, EvidenceStream::State) {
        return export_one(sender, publisher, identity).await;
    }
    if matches!(stream, EvidenceStream::Marks) {
        let Some(transfer) = sender
            .try_prepare_position_mark_export()
            .map_err(|e| e.to_string())?
            .wait()
            .await
            .map_err(|e| e.to_string())?
        else {
            return Ok(None);
        };
        let receipt = publish_mark_page(publisher, identity, &transfer).await?;
        let sequence = receipt.next_sequence;
        sender
            .try_acknowledge_position_mark_export(receipt)
            .map_err(|e| e.to_string())?
            .wait()
            .await
            .map_err(|e| e.to_string())?;
        return Ok(Some(sequence));
    }
    let Some(transfer) = sender
        .try_prepare_execution_fill_export()
        .map_err(|e| e.to_string())?
        .wait()
        .await
        .map_err(|e| e.to_string())?
    else {
        return Ok(None);
    };
    let receipt = publish_fill_page(publisher, identity, &transfer).await?;
    let sequence = receipt.next_sequence;
    sender
        .try_acknowledge_execution_fill_export(receipt)
        .map_err(|e| e.to_string())?
        .wait()
        .await
        .map_err(|e| e.to_string())?;
    Ok(Some(sequence))
}

async fn publish_mark_page(
    publisher: &LocalEventPublisher,
    identity: &ExportIdentity,
    transfer: &aqe_live_protocol::PositionMarkTransfer,
) -> Result<aqe_live_protocol::PositionMarkExportReceipt, String> {
    let page = transfer.page()?;
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
        .map_err(|e| e.to_string())?
        .await
        .map_err(|_| "position mark receipt channel closed")?
        .map_err(|e| e.to_string())?;
    Ok(transfer.receipt())
}

async fn publish_fill_page(
    publisher: &LocalEventPublisher,
    identity: &ExportIdentity,
    transfer: &aqe_live_protocol::ExecutionFillTransfer,
) -> Result<aqe_live_protocol::ExecutionFillExportReceipt, String> {
    let page = transfer.page()?;
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
        .map_err(|e| e.to_string())?
        .await
        .map_err(|_| "execution fill receipt channel closed")?
        .map_err(|e| e.to_string())?;
    Ok(transfer.receipt())
}

async fn publish_page(
    publisher: &LocalEventPublisher,
    identity: &ExportIdentity,
    page: &ExecutionEvidenceFeedPage,
) -> Result<ExecutionEvidenceExportReceipt, String> {
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
            .map_err(|e| e.to_string())?;
        receipts.push(durable);
    }
    for durable in receipts {
        durable
            .await
            .map_err(|_| "execution evidence receipt channel closed")?
            .map_err(|e| e.to_string())?;
    }
    Ok(receipt)
}

#[cfg(test)]
mod tests;
