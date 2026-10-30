//! One bounded owner observation and one race hint, never a per-fill queue.
use super::*;
use crate::core::broker::command_journal::{AnalyticsCapturePause, AnalyticsCaptureStatus};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

pub(super) const SOURCE_FRESHNESS_US: i64 = 10_000_000;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Qualification {
    Disabled,
    Starting,
    Blocked,
    Ready,
    Clear,
}
#[derive(Default)]
struct Observation {
    status: Option<AnalyticsCaptureStatus>,
    hint: Option<AnalyticsCapturePause>,
    unavailable_at_us: Option<i64>,
    latest_unavailable_at_us: Option<i64>,
    persisted_unavailable_at_us: Option<i64>,
    last_interruption_start_us: i64,
    // Sealing requires the exclusive journal returned by the exited worker.
    // That owner is retained until final publisher shutdown.
    sealed: bool,
}
pub(super) struct SourceGuard {
    limit: Option<crate::core::live_sync::PhysicalCaptureLimit>,
    observed: Mutex<Observation>,
    unavailable: AtomicBool,
    fresh_until_us: AtomicI64,
}
fn scope(value: &str) -> &str {
    value
        .trim()
        .strip_prefix("strategy:")
        .unwrap_or(value.trim())
}
fn invalid() -> OutboxStoreError {
    OutboxStoreError::InvalidRecord(
        "source journal observation does not match its capture owner/policy".into(),
    )
}

impl SourceGuard {
    pub fn new(limit: Option<crate::core::live_sync::PhysicalCaptureLimit>) -> Self {
        Self {
            limit,
            observed: Mutex::new(Default::default()),
            unavailable: AtomicBool::new(false),
            fresh_until_us: AtomicI64::new(0),
        }
    }
    pub fn limit(&self) -> Option<crate::core::live_sync::PhysicalCaptureLimit> {
        self.limit
    }
    pub fn validate_configuration(
        &self,
        payload: Option<u64>,
        owner: Option<&crate::core::live_sync::CaptureOwnerLease>,
    ) -> Result<(), OutboxStoreError> {
        if let Some(limit) = self.limit {
            limit.validate()?;
            if payload.is_none() || owner.is_none() {
                return Err(invalid());
            }
            let owner = owner.expect("checked owner");
            if scope(&owner.strategy).is_empty()
                || [&owner.runtime, &owner.strategy, &owner.session, &owner.run]
                    .iter()
                    .any(|id| id.trim().is_empty() || id.len() > 128)
            {
                return Err(invalid());
            }
        }
        Ok(())
    }
    pub fn snapshot(&self) -> Option<AnalyticsCaptureStatus> {
        self.observed.lock().status.clone()
    }
    pub fn qualification(&self, now: i64) -> Qualification {
        if self.limit.is_none() {
            return Qualification::Disabled;
        }
        let mut observed = self.observed.lock();
        if observed.unavailable_at_us.is_some() {
            return Qualification::Blocked;
        }
        let Some(status) = observed.status.as_ref() else {
            return if observed.hint.is_some() {
                Qualification::Blocked
            } else {
                Qualification::Starting
            };
        };
        if (!observed.sealed && now.saturating_sub(status.observed_at_us) > SOURCE_FRESHNESS_US)
            || status.observed_at_us > now.saturating_add(1_000_000)
        {
            self.interrupt(&mut observed, now);
            return Qualification::Blocked;
        }
        if observed.hint.is_some() {
            return Qualification::Blocked;
        }
        match status.pause.as_ref() {
            None => Qualification::Clear,
            Some(_)
                if status.handoff_receipt.is_some() && status.physical.below_resume_threshold() =>
            {
                Qualification::Ready
            }
            Some(_) => Qualification::Blocked,
        }
    }
    /// No journal/database work on producer admission. Healthy observations use
    /// atomic loads; only a newly expired deadline takes the fixed-state lock.
    pub fn blocks_capture(&self) -> bool {
        if self.limit.is_none() {
            return false;
        }
        if self.unavailable.load(Ordering::Acquire) {
            return true;
        }
        let deadline = self.fresh_until_us.load(Ordering::Acquire);
        let now = crate::core::live_sync::now_epoch_micros();
        if deadline >= now {
            return false;
        }
        !matches!(
            self.qualification(now),
            Qualification::Clear | Qualification::Ready | Qualification::Disabled
        )
    }
    fn interrupt(&self, observed: &mut Observation, now: i64) {
        if observed.unavailable_at_us.is_none() {
            let started = now.max(observed.last_interruption_start_us.saturating_add(1));
            observed.unavailable_at_us = Some(started);
            observed.last_interruption_start_us = started;
            observed.persisted_unavailable_at_us = None;
        }
        observed.latest_unavailable_at_us = Some(
            observed
                .latest_unavailable_at_us
                .unwrap_or(0)
                .max(now)
                .max(observed.unavailable_at_us.unwrap()),
        );
        self.unavailable.store(true, Ordering::Release);
    }
    pub fn validate_report(
        &self,
        status: &AnalyticsCaptureStatus,
        owner: &crate::core::live_sync::CaptureOwnerLease,
        now: i64,
    ) -> Result<(), OutboxStoreError> {
        status.validate().map_err(|_| invalid())?;
        if self.limit != Some(status.physical_limit())
            || scope(&status.strategy_id).is_empty()
            || scope(&status.strategy_id) != scope(&owner.strategy)
            || status.observed_at_us > now.saturating_add(1_000_000)
            || now.saturating_sub(status.observed_at_us) > SOURCE_FRESHNESS_US
        {
            return Err(invalid());
        }
        Ok(())
    }
    pub fn observe(
        &self,
        status: AnalyticsCaptureStatus,
        owner: &crate::core::live_sync::CaptureOwnerLease,
        now: i64,
    ) -> Result<(), OutboxStoreError> {
        self.validate_report(&status,owner,now)?;
        let mut observed = self.observed.lock();
        if observed.sealed {
            return Err(invalid());
        }
        // A caller may be the first observer after an expired interval. Detect
        // that before replacing the old sample, even if maintenance was busy.
        if observed.unavailable_at_us.is_none()
            && observed.status.as_ref().is_some_and(|previous| {
                now.saturating_sub(previous.observed_at_us) > SOURCE_FRESHNESS_US
                    || previous.observed_at_us > now.saturating_add(1_000_000)
            })
        {
            self.interrupt(&mut observed, now);
        }
        if observed.unavailable_at_us.is_some_and(|at| {
            observed.persisted_unavailable_at_us != Some(at)
                || status.observed_at_us < observed.latest_unavailable_at_us.unwrap_or(at)
        }) {
            return Err(invalid());
        }
        if let Some(previous) = &observed.status {
            if previous.journal_source_id != status.journal_source_id
                || previous.strategy_id != status.strategy_id
                || previous.account_id != status.account_id
                || previous.generation > status.generation
                || previous.observed_at_us > status.observed_at_us
                || (previous.observed_at_us == status.observed_at_us && previous != &status)
                || (previous.generation > 0
                    && previous.pause.is_none()
                    && status.pause.is_some()
                    && previous.generation == status.generation)
                || (previous.generation == status.generation
                    && previous.pause.is_some()
                    && status.pause.is_some()
                    && (previous.pause != status.pause
                        || (previous.handoff_receipt.is_some()
                            && previous.handoff_receipt != status.handoff_receipt)))
            {
                return Err(invalid());
            }
        }
        if let Some(hint) = &observed.hint {
            if hint.journal_source_id != status.journal_source_id
                || hint.strategy_id != status.strategy_id
                || hint.account_id != status.account_id
            {
                return Err(invalid());
            }
            if status.generation >= hint.generation
                && status.observed_at_us >= hint.last_blocked_at_us
            {
                if status.generation == hint.generation
                    && status.pause.as_ref().is_some_and(|pause| pause != hint)
                {
                    return Err(invalid());
                }
                observed.hint = None;
            }
        }
        let deadline = status.observed_at_us.saturating_add(SOURCE_FRESHNESS_US);
        observed.status = Some(status);
        observed.unavailable_at_us = None;
        observed.latest_unavailable_at_us = None;
        observed.persisted_unavailable_at_us = None;
        self.fresh_until_us.store(deadline, Ordering::Release);
        self.unavailable.store(false, Ordering::Release);
        Ok(())
    }
    pub fn seal(&self) -> Result<(), OutboxStoreError> {
        let mut observed = self.observed.lock();
        if self.limit.is_none()
            || observed.status.is_none()
            || observed.hint.is_some()
            || observed.unavailable_at_us.is_some()
            || observed.sealed
        {
            return Err(invalid());
        }
        observed.sealed = true;
        self.fresh_until_us.store(i64::MAX, Ordering::Release);
        Ok(())
    }
    pub fn is_sealed(&self) -> bool {
        self.observed.lock().sealed
    }
    pub fn unavailable(&self) {
        if self.limit.is_some() {
            self.interrupt(
                &mut self.observed.lock(),
                crate::core::live_sync::now_epoch_micros(),
            );
        }
    }
    pub fn suspension(&self) -> Option<i64> {
        self.observed.lock().unavailable_at_us
    }
    pub fn pending_suspension(&self) -> Option<i64> {
        let observed = self.observed.lock();
        observed
            .unavailable_at_us
            .filter(|at| observed.persisted_unavailable_at_us != Some(*at))
    }
    pub fn acknowledge_suspension(&self, at: i64) {
        let mut observed = self.observed.lock();
        if observed.unavailable_at_us == Some(at) {
            observed.persisted_unavailable_at_us = Some(at);
        }
    }
    /// A real durable refusal races a previously sampled healthy status. Latch
    /// it synchronously; only a newer matching owner observation can qualify it.
    pub fn refused(
        &self,
        pause: AnalyticsCapturePause,
        owner: &crate::core::live_sync::CaptureOwnerLease,
    ) -> Result<bool, OutboxStoreError> {
        if self.limit.is_none() {
            return Ok(false);
        }
        pause.validate().map_err(|_| invalid())?;
        if scope(&pause.strategy_id) != scope(&owner.strategy) {
            return Err(invalid());
        }
        let mut observed = self.observed.lock();
        if let Some(previous) = &observed.status {
            if previous.journal_source_id != pause.journal_source_id
                || previous.strategy_id != pause.strategy_id
                || previous.account_id != pause.account_id
            {
                return Err(invalid());
            }
            if previous.generation > pause.generation
                || (previous.generation == pause.generation
                    && previous.pause.is_none()
                    && previous.observed_at_us >= pause.last_blocked_at_us)
            {
                return Ok(false);
            }
        }
        if let Some(hint) = &observed.hint {
            if hint.journal_source_id != pause.journal_source_id
                || hint.account_id != pause.account_id
                || hint.strategy_id != pause.strategy_id
            {
                return Err(invalid());
            }
            if hint.generation > pause.generation {
                return Ok(false);
            }
            if hint.generation == pause.generation {
                if hint != &pause {
                    return Err(invalid());
                }
                return Ok(false);
            }
        }
        observed.hint = Some(pause);
        Ok(true)
    }
}
