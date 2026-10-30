//! Generic producer-side capture admission. No service/tier/broker concepts.
use super::*;
use crate::core::live_sync::{CaptureCapacity, CapturePauseRecord, OutboxStoreError};
use std::sync::atomic::AtomicU8;
mod source;
mod diagnostics;
use source::{Qualification,SourceGuard};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloudCaptureState {
    #[default]
    Disabled,
    Starting,
    Active,
    Paused,
    ReconciliationRequired,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudCaptureStatus {
    pub state: CloudCaptureState,
    pub limit_bytes: Option<u64>,
    /// Last capacity observation, not an unqualified real-time billing meter.
    pub retained_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at_us: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pause: Option<CapturePauseRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical: Option<crate::core::live_sync::PhysicalCaptureUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Last qualified local journal observation, not cloud/billing usage.
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub source_journal:Option<crate::core::broker::command_journal::AnalyticsCaptureStatus>,
}

impl CloudCaptureStatus {
    pub fn is_disabled(&self) -> bool {
        self.state == CloudCaptureState::Disabled
    }
}

pub(super) struct CaptureController {
    state: AtomicU8,
    revision: AtomicU64,
    status: Mutex<CloudCaptureStatus>,
    changed: Notify,
    limit: Option<u64>,
    owner: Option<Arc<crate::core::live_sync::CaptureOwnerLease>>,
    source:SourceGuard,
    pub(super) source_monitor_active:AtomicBool,
    source_observation_token:uuid::Uuid,
    source_observation_armed:AtomicBool,
}
impl Drop for CaptureController {
    fn drop(&mut self){
        // Releasing the in-process claim is not a clean close of the durable
        // witness. A replacement must still recover that unclosed interval.
        if let Some(owner)=self.owner.as_ref(){owner.release_source_observation(self.source_observation_token);}
    }
}

/// Read-only producer gate. Unlike a publisher clone, this does not retain any
/// delivery queue or worker. It has no billing, broker or transport concepts.
#[derive(Clone)]
pub(crate) struct CloudCaptureGate {
    controller: Arc<CaptureController>,
}

impl CloudCaptureGate {
    pub(super) fn new(controller: Arc<CaptureController>) -> Self {
        Self { controller }
    }

    /// New optional analytics must defer while capture is initializing, paused
    /// or awaiting current-state reconciliation. Previously accepted journal
    /// pages and their receipts are deliberately not gated by this view.
    pub(crate) fn is_paused(&self) -> bool {
        self.controller.blocked(false)
    }
    pub(crate) fn note_journal_pause(&self,pause:crate::core::broker::command_journal::AnalyticsCapturePause) {
        if let Err(error)=self.controller.note_source_pause(pause){self.controller.unavailable(&error);}
    }
    pub(crate) fn journal_required(&self)->bool {self.controller.source_limit().is_some()}
    pub(crate) fn journal_unavailable(&self) {
        self.controller.source_unavailable();
    }
}

impl CaptureController {
    #[cfg(test)]
    pub fn new(limit: Option<u64>) -> Self {
        Self::with_owner(limit, None)
    }

    #[cfg(test)]
    pub fn with_owner(limit: Option<u64>, owner: Option<Arc<crate::core::live_sync::CaptureOwnerLease>>) -> Self {
        Self::with_source_owner(limit,owner,None)
    }
    pub fn with_source_owner(limit:Option<u64>,owner:Option<Arc<crate::core::live_sync::CaptureOwnerLease>>,source:Option<crate::core::live_sync::PhysicalCaptureLimit>)->Self {
        let state = if limit.is_some() || source.is_some() {
            CloudCaptureState::Starting
        } else {
            CloudCaptureState::Disabled
        };
        Self {
            state: AtomicU8::new(state as u8),
            revision: AtomicU64::new(0),
            status: Mutex::new(CloudCaptureStatus {
                state,
                limit_bytes: limit,
                ..Default::default()
            }),
            changed: Notify::new(),
            limit,
            owner,
            source:SourceGuard::new(source),
            source_monitor_active:AtomicBool::new(false),
            source_observation_token:if source.is_some(){uuid::Uuid::new_v4()}else{uuid::Uuid::nil()},
            source_observation_armed:AtomicBool::new(false),
        }
    }

    pub fn limit(&self) -> Option<u64> {
        self.limit
    }
    pub fn source_limit(&self)->Option<crate::core::live_sync::PhysicalCaptureLimit>{self.source.limit()}
    pub(super) fn source_observation_instance_id(&self) -> uuid::Uuid {
        self.source_observation_token
    }
    pub(super) fn prepared_source_observation_id(
        &self, source: &str, strategy: &str, account: &str,
    ) -> Option<uuid::Uuid> {
        if !self.source_observation_armed.load(Ordering::Acquire) || self.source.is_sealed() {
            return None;
        }
        let report = self.source.snapshot()?;
        (report.journal_source_id == source && report.strategy_id == strategy
            && report.account_id == account).then_some(self.source_observation_token)
    }
    #[cfg(test)]
    pub fn source_suspension(&self)->Option<i64>{self.source.suspension()}
    pub fn qualify_source_liveness(&self){
        self.source.qualification(crate::core::live_sync::now_epoch_micros());
        if self.source.suspension().is_some(){self.close_source_interruption();}
    }
    fn close_source_interruption(&self){
        let mut status=self.status();
        if matches!(status.state,CloudCaptureState::Active|CloudCaptureState::Disabled){
            status.state=CloudCaptureState::Paused;
            status.last_error=Some("source journal observation interrupted".into());self.publish(status);
        }
    }
    pub async fn persist_source_suspension(&self,outbox:&OutboxStore)->Result<(),OutboxStoreError>{
        let Some(at)=self.source.pending_suspension() else{return Ok(());};
        // Close the cached controller state before granting any durable-write
        // acknowledgement. A subsequent sample cannot expose Active in the
        // interval before the destination's reconciliation status is refreshed.
        self.close_source_interruption();
        let owner=self.owner.as_ref().ok_or_else(||OutboxStoreError::InvalidRecord("source suspension owner unavailable".into()))?;
        owner.claim_source_observation(self.source_observation_token).map_err(|_|OutboxStoreError::InvalidRecord("source observation already has another owner".into()))?;
        let limit=self.limit.ok_or_else(||OutboxStoreError::InvalidRecord("source suspension policy unavailable".into()))?;
        outbox.record_source_observation_suspension(owner,at,limit).await?;
        // A different interruption cannot acquire this acknowledgement while
        // the write was awaiting I/O. Repeated failures keep one open identity.
        self.source.acknowledge_suspension(at);
        Ok(())
    }
    pub fn seal_source(&self)->Result<(),OutboxStoreError>{self.source.seal()}
    pub fn source_is_sealed(&self)->bool{self.source.is_sealed()}
    pub fn validate_source_policy(&self)->Result<(),OutboxStoreError>{self.source.validate_configuration(self.limit,self.owner.as_deref())}
    pub fn source_unavailable(&self) {
        self.source.unavailable();
        let mut status=self.status();status.state=CloudCaptureState::Paused;
        status.last_error=Some("source journal quality observation unavailable".into());self.publish(status);
    }
    pub(super) fn note_source_error(&self, reason: String) {
        let mut status = self.status();
        status.last_error = Some(reason);
        self.publish(status);
    }
    pub fn note_source_pause(&self,pause:crate::core::broker::command_journal::AnalyticsCapturePause)->Result<(),OutboxStoreError> {
        if self.source.limit().is_none(){return Ok(());}
        let owner=self.owner.as_ref().ok_or_else(||OutboxStoreError::InvalidRecord("source journal capture requires an owner lease".into()))?;
        if self.source.refused(pause,owner)? {
            let mut status=self.status();status.state=CloudCaptureState::Paused;self.publish(status);
        }
        Ok(())
    }
    pub async fn observe_source(&self,outbox:&OutboxStore,report:crate::core::broker::command_journal::AnalyticsCaptureStatus)->Result<(),OutboxStoreError> {
        let owner=self.owner.as_ref().ok_or_else(||OutboxStoreError::InvalidRecord("source journal capture requires an owner lease".into()))?;
        self.source.validate_report(&report,owner,crate::core::live_sync::now_epoch_micros())?;
        if self.source.is_sealed(){return Err(OutboxStoreError::InvalidRecord("source observation is sealed".into()));}
        if !self.source_observation_armed.load(Ordering::Acquire) {
            let limit=self.limit.ok_or_else(||OutboxStoreError::InvalidRecord("source observation limit unavailable".into()))?;
            outbox.begin_source_observation(owner,self.source_observation_token,&report,limit).await?;
            self.source_observation_armed.store(true,Ordering::Release);
        }
        self.source.observe(report,owner,crate::core::live_sync::now_epoch_micros())?;
        let mut status=self.status();
        status.source_journal=self.source.snapshot();
        if !matches!(self.source.qualification(crate::core::live_sync::now_epoch_micros()),Qualification::Clear|Qualification::Disabled){status.state=CloudCaptureState::Paused;}
        self.publish(status);Ok(())
    }
    pub async fn close_sealed_source_observation(&self,outbox:&OutboxStore)->Result<(),OutboxStoreError>{
        if !self.source.is_sealed() || !self.source_observation_armed.load(Ordering::Acquire) {
            return Err(OutboxStoreError::InvalidRecord("only an armed immutable final journal can close source observation".into()));
        }
        let owner=self.owner.as_ref().ok_or_else(||OutboxStoreError::InvalidRecord("source observation owner unavailable".into()))?;
        outbox.close_source_observation(owner,self.source_observation_token).await
    }
    pub fn owner(&self) -> Option<Arc<crate::core::live_sync::CaptureOwnerLease>> {
        self.owner.clone()
    }
    pub fn is_starting(&self) -> bool {
        self.state.load(Ordering::Acquire) == CloudCaptureState::Starting as u8
    }
    pub fn requires_reconciliation(&self) -> bool {
        self.state.load(Ordering::Acquire) == CloudCaptureState::ReconciliationRequired as u8
    }
    pub fn blocked(&self, recovery: bool) -> bool {
        if self.source.blocks_capture(){return true;}
        match self.state.load(Ordering::Acquire) {
            state
                if state == CloudCaptureState::Disabled as u8
                    || state == CloudCaptureState::Active as u8 =>
            {
                false
            }
            state if recovery && state == CloudCaptureState::ReconciliationRequired as u8 => false,
            _ => true,
        }
    }
    pub fn status(&self) -> CloudCaptureStatus {
        self.status.lock().clone()
    }

    fn publish(&self, status: CloudCaptureStatus) {
        self.publish_if_current(status, None);
    }

    fn publish_if_current(&self, status: CloudCaptureStatus, revision: Option<u64>) {
        let mut current = self.status.lock();
        if revision.is_some_and(|revision| self.revision.load(Ordering::Acquire) != revision) {
            return;
        }
        let state = status.state;
        *current = status;
        self.state.store(state as u8, Ordering::Release);
        self.revision.fetch_add(1, Ordering::AcqRel);
        drop(current);
        self.changed.notify_waiters();
    }

    pub fn refused(&self, capacity: CaptureCapacity) {
        let mut status = self.status();
        status.state = CloudCaptureState::Paused;
        status.retained_bytes = capacity.retained_bytes;
        status.physical = capacity.physical;
        status.observed_at_us = Some(crate::core::live_sync::now_epoch_micros());
        self.publish(status);
    }

    pub async fn refresh(&self, outbox: &OutboxStore) -> Result<(), OutboxStoreError> {
        self.validate_source_policy()?;
        let Some(limit) = self.limit else {
            return Ok(());
        };
        // Any synchronous expiry fence precedes the revision used for this
        // refresh, so its own first metadata commit is visible immediately.
        self.qualify_source_liveness();
        let revision = self.revision.load(Ordering::Acquire);
        if limit == 0 || limit > i64::MAX as u64 {
            return Err(OutboxStoreError::InvalidRecord(
                "invalid publisher capture byte limit".into(),
            ));
        }
        let retained_bytes = outbox.capture_retained_bytes().await?;
        let mut physical=outbox.physical_capture_usage().await?;
        if physical.is_some_and(|usage|!usage.below_resume_threshold()) {
            outbox.try_reclaim_physical_wal().await?;
            physical=outbox.physical_capture_usage().await?;
        }
        let physical_ready=physical.is_none_or(|usage|usage.below_resume_threshold());
        if let Some(owner) = &self.owner {
            outbox.adopt_latest_capture_pause(owner).await?;
        }
        let qualification=self.source.qualification(crate::core::live_sync::now_epoch_micros());
        self.persist_source_suspension(outbox).await?;
        let pause = outbox.latest_capture_pause().await?;
        let source_error = (qualification == Qualification::Blocked)
            .then(|| self.status().last_error)
            .flatten();
        let state = if qualification==Qualification::Starting {
            if pause.is_some(){CloudCaptureState::Paused}else{CloudCaptureState::Starting}
        }else if qualification==Qualification::Blocked || (qualification==Qualification::Ready && pause.is_none()) {
            CloudCaptureState::Paused
        }else if pause.is_some() {
            if retained_bytes <= (limit / 5) * 4 && physical_ready {
                CloudCaptureState::ReconciliationRequired
            } else {
                CloudCaptureState::Paused
            }
        } else {
            // No new history has been omitted yet. The first genuine capture
            // attempt records its exact durable pause after rollback. Blocking
            // it here would leave startup with no factual gap to defer against.
            CloudCaptureState::Active
        };
        self.publish_if_current(
            CloudCaptureStatus {
                state,
                limit_bytes: Some(limit),
                retained_bytes,
                observed_at_us: Some(crate::core::live_sync::now_epoch_micros()),
                pause,
                physical,
                last_error: source_error,
                source_journal:self.source.snapshot(),
            },
            Some(revision),
        );
        Ok(())
    }

    pub fn unavailable(&self, error: &OutboxStoreError) {
        let mut status = self.status();
        status.last_error = Some(error.to_string());
        // Never fall back to unlimited capture when the budget cannot be read.
        status.state = if status.pause.is_some() {
            CloudCaptureState::Paused
        } else {
            CloudCaptureState::Starting
        };
        self.publish(status);
    }

    pub fn permits_new_source(&self, event: &CloudEvent) -> bool {
        self.owner.as_ref().is_none_or(|owner| {
            self.limit.is_none() || (event.runtime_id == owner.runtime
                && event.strategy_id == owner.strategy && event.live_session_id == owner.session
                && event.run_id.as_deref() == Some(owner.run.as_str()))
        })
    }

    pub async fn initialized(&self, timeout: Duration) -> bool {
        tokio::time::timeout(timeout, async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if !self.is_starting() {
                    return;
                }
                changed.await;
            }
        })
        .await
        .is_ok()
    }

    /// The caller owns current-state resynchronization. Space becoming free is
    /// not proof that state has been reconciled, and never clears a pause alone.
    pub async fn complete(
        &self,
        outbox: &OutboxStore,
        generation: u64,
    ) -> Result<bool, OutboxStoreError> {
        let snapshot = self.status();
        let Some(pause) = snapshot.pause else {
            return Ok(false);
        };
        if snapshot.state != CloudCaptureState::ReconciliationRequired
            || pause.generation != generation
        {
            return Ok(false);
        }
        let Some(limit) = self.limit else {
            return Ok(false);
        };
        // Journal recovery is a separate durable transition. Clearing only the
        // outbox cannot expose Active while a source pause (or stale/unavailable
        // source observation) remains. The owning coordinator must settle it.
        if !matches!(self.source.qualification(crate::core::live_sync::now_epoch_micros()),Qualification::Disabled|Qualification::Clear){return Ok(false);}
        if outbox.capture_retained_bytes().await? > (limit / 5) * 4 {
            return Ok(false);
        }
        if outbox.physical_capture_usage().await?.is_some_and(|usage|!usage.below_resume_threshold()) {
            return Ok(false);
        }
        if !outbox
            .complete_capture_reconciliation(
                &pause.runtime_id,
                &pause.strategy_id,
                &pause.live_session_id,
                generation,
            )
            .await?
        {
            return Ok(false);
        }
        self.refresh(outbox).await?;
        Ok(!self.blocked(false))
    }
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod source_tests;
