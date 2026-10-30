//! Bounded owner-side current-state replay after an intentional cloud capture
//! pause. Broker execution never awaits this worker or remote persistence.
use super::{aqs_types::AqsAuth, live_event_projection::LocalBatchSyncState};
use crate::core::{
    insight::{InsightCollection, InsightSnapshot},
    live_sync::{
        CloudCaptureState, CloudEvent, EventKind, EventPriority, EventSubject,
        FlatbufferEventCodec, ReconciliationComplete, ReconciliationPurpose, ReconciliationStart,
        deterministic_event_id, now_epoch_micros,
    },
};
use std::time::{Duration, Instant};
use uuid::Uuid;

const PAGE_ROWS: usize = 32;
// Leave room for the JSON batch envelope and transport metadata below the
// gateway's 256-KiB request bound.
const PAGE_BYTES: usize = 128 * 1024;

#[derive(Default, serde::Serialize)]
pub(super) struct CaptureRecoveryHealth {
    pub generation: Option<String>,
    pub replayed_insights: u64,
    pub last_error: Option<String>,
}

#[derive(Clone, Copy)]
enum Advance {
    Page(Option<Uuid>, u64),
    Sealed,
    Resumed,
}

struct Pending {
    events: Vec<CloudEvent>,
    advance: Advance,
}

struct Plan {
    pause_generation: u64,
    generation: String,
    started_at_us: i64,
    origin_runtime_id: Option<String>,
    origin_run_id: Option<String>,
    last_blocked_run_id: Option<String>,
    handoff_count: u64,
    critical_cutoff: u64,
    upper: Option<Uuid>,
    after: Option<Uuid>,
    replay_done: bool,
    sealed: bool,
    pending: Option<Pending>,
}

pub(super) struct CaptureRecovery {
    plan: Option<Plan>,
    work: tokio::task::JoinSet<Result<Advance, String>>,
    retry_at: Instant,
    pub health: CaptureRecoveryHealth,
}

impl Default for CaptureRecovery {
    fn default() -> Self {
        Self {
            plan: None,
            work: Default::default(),
            retry_at: Instant::now(),
            health: Default::default(),
        }
    }
}

impl CaptureRecovery {
    /// Await all source/replay operations before replacing the journal owner.
    /// Keep exact pending envelopes/cutoffs: a later poll retries them rather
    /// than inventing a different barrier under the same generation.
    pub(super) async fn quiesce(&mut self) {
        self.work.shutdown().await;
    }
    pub fn needs_poll(&self, publisher: &crate::core::live_sync::LocalEventPublisher) -> bool {
        publisher.cloud_capture_paused() || self.plan.is_some() || !self.work.is_empty()
    }
    /// Called by the single strategy owner. Only page construction happens
    /// here; SQLite admission/receipts and resume CAS run on one owned worker.
    pub fn poll(
        &mut self,
        sync: &mut LocalBatchSyncState,
        insights: &InsightCollection,
        auth: &AqsAuth,
        broker_features: impl FnOnce() -> Vec<String>,
        source_journal:Option<crate::core::broker::command_worker::BrokerCommandSender>,
    ) {
        let status = sync.publisher.cloud_capture_status();
        if matches!(
            status.state,
            CloudCaptureState::Disabled | CloudCaptureState::Active
        ) {
            // abort_all alone retains completed JoinSet entries and would keep
            // the normal hot path polling forever after the resume CAS.
            self.work = Default::default();
            self.plan = None;
            self.health.last_error = None;
            return;
        }
        let Some(pause) = status.pause.as_ref() else {
            return;
        };
        // A source publisher is run-scoped. Import/recovery of a previous
        // process's pause must be performed by its authenticated host owner,
        // never silently cleared using this run's source credentials.
        if pause.runtime_id != sync.runtime_id
            || pause.strategy_id != sync.strategy_id
            || pause.live_session_id != sync.live_session_id
            || pause.owner_run_id.as_deref().is_some_and(|run| run != sync.run_id)
        {
            self.health.last_error =
                Some("Cloud capture recovery belongs to another source scope".into());
            return;
        }
        if self
            .plan
            .as_ref()
            .is_some_and(|p| p.pause_generation != pause.generation)
        {
            self.work.abort_all();
            self.plan = None;
        }
        while let Some(result) = self.work.try_join_next() {
            match result {
                Ok(Ok(advance)) => {
                    if let Some(plan) = self.plan.as_mut() {
                        plan.pending = None;
                        self.health.last_error = None;
                        match advance {
                            Advance::Page(after, count) => {
                                plan.after = after;
                                self.health.replayed_insights =
                                    self.health.replayed_insights.saturating_add(count);
                            }
                            Advance::Sealed => plan.sealed = true,
                            Advance::Resumed => {
                                self.plan = None;
                                return;
                            }
                        }
                    }
                }
                Ok(Err(error)) => {
                    self.health.last_error = Some(error);
                    self.retry_at = Instant::now() + Duration::from_secs(1);
                }
                Err(error) if error.is_cancelled() => {}
                Err(error) => {
                    self.health.last_error =
                        Some(format!("Cloud capture recovery worker failed: {error}"));
                    self.retry_at = Instant::now() + Duration::from_secs(1);
                }
            }
        }
        if status.state != CloudCaptureState::ReconciliationRequired
            || !self.work.is_empty()
            || self.retry_at > Instant::now()
            || sync.publisher.admission_failed()
        {
            return;
        }
        if self.plan.is_none() {
            // A new owner attempt may follow an already sealed partial replay.
            // Give it a fresh generation; never reuse an accepted generation
            // with a different cutoff/barrier after cancellation or restart.
            // Retries within this plan retain the exact same IDs and payloads.
            let generation = deterministic_event_id(&format!(
                "capture-recovery-v1:{}:{}:{}:{}:{}:{}",
                sync.runtime_id,
                sync.strategy_id,
                sync.live_session_id,
                sync.run_id,
                pause.generation,
                Uuid::new_v4()
            ))
            .to_string();
            self.health = CaptureRecoveryHealth {
                generation: Some(generation.clone()),
                ..Default::default()
            };
            self.plan = Some(Plan {
                pause_generation: pause.generation,
                generation,
                started_at_us: pause.started_at_us,
                origin_runtime_id: pause.origin_runtime_id.clone(),
                origin_run_id: pause.origin_run_id.clone(),
                last_blocked_run_id: pause.last_blocked_run_id.clone(),
                handoff_count: pause.handoff_count,
                critical_cutoff: sync.publisher.critical_sequence_cursor(),
                upper: insights.cloud_recovery_upper_bound(),
                after: None,
                replay_done: false,
                sealed: false,
                pending: None,
            });
        }
        let plan = self.plan.as_mut().unwrap();
        if plan.sealed {
            let publisher = sync.publisher.clone();
            let generation = plan.pause_generation;
            self.work.spawn(async move {
                let result=if publisher.source_journal_limit().is_some() {
                    let sender=source_journal.ok_or_else(||"Cloud capture recovery has no source journal owner".to_owned())?;
                    publisher.complete_capture_reconciliation_with_source(generation,&sender).await
                }else{publisher.complete_capture_reconciliation(generation).await};
                match result {
                    Ok(true) => Ok(Advance::Resumed),
                    Ok(false) => {
                        Err("Cloud capture resume was fenced by capacity or a newer pause".into())
                    }
                    Err(error) => Err(error.to_string()),
                }
            });
            return;
        }
        if plan.pending.is_none() && !plan.replay_done {
            let ids = plan
                .upper
                .map(|upper| insights.cloud_recovery_page(plan.after, upper, PAGE_ROWS))
                .unwrap_or_default();
            if ids.is_empty() {
                plan.replay_done = true;
            } else {
                let mut bytes = 0usize;
                let mut events = Vec::new();
                let mut after = plan.after;
                for id in ids {
                    let Some(row) = insights.get(&id) else {
                        after = Some(id);
                        continue;
                    };
                    // Current state only. The gap explicitly does not claim a
                    // replay of missing history, fills, or account marks.
                    let snapshot = InsightSnapshot::from_insight_with_history_window(row, 0, 0);
                    let mut payload = match serde_json::to_value(snapshot) {
                        Ok(value) => value,
                        Err(error) => {
                            self.health.last_error = Some(error.to_string());
                            return;
                        }
                    };
                    let object = payload
                        .as_object_mut()
                        .expect("Insight snapshot is an object");
                    object.remove("state_history");
                    object.insert("strategy_id".into(), auth.strategy_id.clone().into());
                    object.insert(
                        "live_session_id".into(),
                        sync.live_session_id.clone().into(),
                    );
                    object.insert("origin_run_id".into(), sync.run_id.clone().into());
                    object.insert("last_event_run_id".into(), sync.run_id.clone().into());
                    object.insert("reconciliation_status".into(), "current".into());
                    let entity = format!("insight:{id}");
                    let revision = sync.next_revision(&entity);
                    let event = sync.event_at_revision(
                        entity,
                        revision,
                        EventPriority::Critical,
                        EventKind::InsightProjection,
                        payload,
                        None,
                    );
                    let size = match FlatbufferEventCodec::encode(&event) {
                        Ok(value) => value.len(),
                        Err(error) => {
                            self.health.last_error = Some(error.to_string());
                            return;
                        }
                    };
                    if size > PAGE_BYTES {
                        self.health.last_error = Some("Current insight exceeds the bounded recovery page; capture remains paused".into());
                        return;
                    }
                    if !events.is_empty() && bytes.saturating_add(size) > PAGE_BYTES {
                        break;
                    }
                    bytes += size;
                    events.push(event);
                    after = Some(id);
                }
                plan.pending = Some(Pending {
                    advance: Advance::Page(after, events.len() as u64),
                    events,
                });
            }
        }
        if plan.pending.is_none() && plan.replay_done {
            let checkpoint_at = now_epoch_micros();
            let gap = sync.event_at_revision(format!("cloud-history-gap:{}",plan.generation), 1,
                EventPriority::Critical, EventKind::StrategyEvent, serde_json::json!({
                    "event_type":"lifecycle", "level":"warn", "title":"Cloud history gap",
                    "message":"Cloud capture was paused. Current insight state was replayed; missing history, fills and account marks are not reconstructed.",
                    "payload":{"kind":"cloud_capture_gap","version":2,"run_id":sync.run_id,
                        "runtime_id":sync.runtime_id,"pause_generation":plan.pause_generation,
                        "origin_runtime_id":plan.origin_runtime_id,"origin_run_id":plan.origin_run_id,
                        "last_blocked_run_id":plan.last_blocked_run_id,"ownership_handoffs":plan.handoff_count,
                        "reconciliation_generation":plan.generation,"started_at_us":plan.started_at_us,
                        "recovery_checkpoint_at_us":checkpoint_at,"accepted_evidence_retained":true,
                        "history_complete":false}
                }), None);
            let start = ReconciliationStart {
                purpose: ReconciliationPurpose::CaptureRecovery,
                critical_replay_through: Some(sync.publisher.critical_sequence_cursor()),
                generation: plan.generation.clone(),
                runtime_id: sync.runtime_id.clone(),
                strategy_id: sync.strategy_id.clone(),
                live_session_id: sync.live_session_id.clone(),
                run_id: Some(sync.run_id.clone()),
                // No unbounded broker-cache copy on the owner turn. Exact
                // lifecycle queries can resolve stale filled rows remotely.
                snapshot_complete: false,
                broker_features: broker_features(),
                critical_cutoff: Some(plan.critical_cutoff),
                total_chunks: 0,
                total_records: 0,
            };
            let start_event = sync.event_at_revision(
                format!("reconciliation:{}:start", plan.generation),
                1,
                EventPriority::Reconciliation,
                EventKind::ReconciliationStart,
                serde_json::to_value(start).unwrap(),
                None,
            );
            let complete_event = sync.event_at_revision(
                format!("reconciliation:{}:complete", plan.generation),
                1,
                EventPriority::Reconciliation,
                EventKind::ReconciliationComplete,
                serde_json::to_value(ReconciliationComplete {
                    generation: plan.generation.clone(),
                    total_chunks: 0,
                    total_records: 0,
                    snapshot_complete: false,
                })
                .unwrap(),
                None,
            );
            plan.pending = Some(Pending {
                events: vec![gap, start_event, complete_event],
                advance: Advance::Sealed,
            });
        }
        if let Some(pending) = &plan.pending {
            let events = pending.events.clone();
            let advance = pending.advance;
            let publisher = sync.publisher.clone();
            self.work.spawn(async move {
                for event in events {
                    let id = event.event_id;
                    let subject = EventSubject::for_priority(
                        &event.runtime_id,
                        &event.strategy_id,
                        event.priority,
                    );
                    let receipt = publisher
                        .queue_recovery_durable(subject, event)
                        .map_err(|error| error.to_string())?;
                    let accepted = tokio::time::timeout(Duration::from_secs(5), receipt)
                        .await
                        .map_err(|_| "Cloud recovery durable admission timed out".to_string())?
                        .map_err(|_| "Cloud recovery receipt closed".to_string())?
                        .map_err(|error| error.to_string())?;
                    if accepted != id {
                        return Err("Cloud recovery receipt identity mismatch".into());
                    }
                }
                Ok(advance)
            });
        }
    }
}

#[cfg(test)]
mod tests;
