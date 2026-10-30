//! Bounded cached telemetry; no history scan or database query on the owner turn.
use super::*;

#[derive(Clone, Copy, Default, serde::Serialize)]
pub(crate) struct ExportBacklogHealth {
    /// Source snapshot time, not a claim about evidence arriving afterwards.
    pub sampled_at_us: Option<i64>,
    pub pending_records_at_last_sample: Option<u64>,
    pub pending_page_records: usize,
    pub oldest_pending_page_recorded_at_us: Option<i64>,
    pub oldest_pending_page_age_ms: Option<u64>,
}

impl ExportBacklogHealth {
    pub(super) fn page(
        &mut self,
        sampled: i64,
        after: u64,
        through: u64,
        count: usize,
        oldest: Option<i64>,
    ) {
        self.sampled_at_us = Some(sampled);
        self.pending_records_at_last_sample = Some(through.saturating_sub(after));
        self.pending_page_records = count;
        self.oldest_pending_page_recorded_at_us = oldest.filter(|at| *at > 0);
    }

    pub(super) fn acknowledged(&mut self) {
        self.pending_records_at_last_sample = self
            .pending_records_at_last_sample
            .map(|n| n.saturating_sub(self.pending_page_records as u64));
        self.pending_page_records = 0;
        self.oldest_pending_page_recorded_at_us = None;
        self.oldest_pending_page_age_ms = None;
    }

    pub(super) fn empty(&mut self, sampled: i64) {
        *self = Self {
            sampled_at_us: Some(sampled),
            pending_records_at_last_sample: Some(0),
            ..Self::default()
        };
    }

    pub(super) fn with_age(mut self, now_us: i64) -> Self {
        self.oldest_pending_page_age_ms = self
            .oldest_pending_page_recorded_at_us
            .map(|at| now_us.saturating_sub(at).max(0) as u64 / 1000);
        self
    }
}

type Backlog = std::sync::Arc<parking_lot::Mutex<ExportBacklogHealth>>;
pub(super) type SharedBacklog = Backlog;

#[derive(Default, serde::Serialize)]
pub(crate) struct EvidenceExportHealth {
    pub enabled: bool,
    pub stream: &'static str,
    pub status: &'static str,
    pub last_exported_source_sequence: Option<u64>,
    pub last_error: Option<String>,
    pub last_attempt_at_us: Option<i64>,
    pub last_success_at_us: Option<i64>,
    pub busy_deferrals: u64,
    pub queue_deferrals: u64,
    pub failures: u64,
    pub capture_deferrals: u64,
    pub consecutive_deferrals: u32,
    /// Waiting for admission is not proof of pending source evidence.
    pub deferred_since_us: Option<i64>,
    pub deferred_for_ms: Option<u64>,
    pub backlog: ExportBacklogHealth,
    pub delivery_basis: &'static str,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum ExportError {
    Busy,
    QueueFull,
    CapturePaused(Box<crate::core::live_sync::CloudCaptureStatus>),
    Failure(String),
}
impl ExportError {
    pub(super) fn admission(publisher: &LocalEventPublisher, error: crate::core::live_sync::EventBusError) -> Self {
        if matches!(error, crate::core::live_sync::EventBusError::CloudCapturePaused) {
            Self::CapturePaused(Box::new(publisher.cloud_capture_status()))
        } else { Self::Failure(error.to_string()) }
    }
}
impl From<crate::core::broker::commands::BrokerCommandError> for ExportError {
    fn from(error: crate::core::broker::commands::BrokerCommandError) -> Self {
        use crate::core::broker::commands::BrokerCommandError;
        match error {
            BrokerCommandError::AnalyticsBusy => Self::Busy,
            BrokerCommandError::AnalyticsQueueFull => Self::QueueFull,
            other => Self::Failure(other.to_string()),
        }
    }
}
impl From<String> for ExportError {
    fn from(error: String) -> Self {
        Self::Failure(error)
    }
}
impl From<&str> for ExportError {
    fn from(error: &str) -> Self { Self::Failure(error.into()) }
}
impl std::fmt::Display for ExportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy => f.write_str("analytics reader busy"),
            Self::QueueFull => f.write_str("analytics journal request queue full"),
            Self::CapturePaused(status) => write!(f,
                "capture_state={} reason={}; local_buffer_bytes={} limit_bytes={:?}; prepared page retained, broker execution continues",
                status.diagnostic_phase(), status.diagnostic_reason(chrono::Utc::now().timestamp_micros()),
                status.retained_bytes, status.limit_bytes),
            Self::Failure(error) => f.write_str(error),
        }
    }
}

pub(super) fn retry_delay(error: &ExportError, consecutive: u32) -> Duration {
    let exponent = consecutive.saturating_sub(1).min(8);
    let (base, cap) = match error {
        ExportError::Busy => (100u64, 2_000),
        ExportError::QueueFull => (250, 5_000),
        ExportError::CapturePaused(_) => (250, 5_000),
        ExportError::Failure(_) => (1_000, 30_000),
    };
    Duration::from_millis(base.saturating_mul(1u64 << exponent).min(cap))
}

impl ExecutionEvidenceExporter {
    pub(super) fn refresh_health(&mut self) {
        let now = chrono::Utc::now().timestamp_micros();
        if let Some(backlog) = self.backlog.try_lock() {
            self.health.backlog = backlog.with_age(now);
        }
        self.health.deferred_for_ms = self
            .health
            .deferred_since_us
            .map(|at| now.saturating_sub(at).max(0) as u64 / 1000);
    }

    pub(super) fn record_result(&mut self, result: Result<Option<u64>, ExportError>, now: Instant) {
        self.refresh_health();
        match result {
            Ok(sequence) => {
                if let Some(sequence) = sequence {
                    self.health.last_exported_source_sequence = Some(sequence);
                }
                self.health.status = "idle";
                self.health.last_success_at_us = Some(chrono::Utc::now().timestamp_micros());
                self.health.last_error = None;
                self.health.consecutive_deferrals = 0;
                self.health.deferred_since_us = None;
                self.health.deferred_for_ms = None;
                self.next_attempt = now
                    + if sequence.is_some() {
                        Duration::from_millis(250)
                    } else {
                        Duration::from_secs(2)
                    };
            }
            Err(error) => {
                self.health.consecutive_deferrals =
                    self.health.consecutive_deferrals.saturating_add(1);
                self.health
                    .deferred_since_us
                    .get_or_insert_with(|| chrono::Utc::now().timestamp_micros());
                self.refresh_health();
                self.health.status = match error {
                    ExportError::Busy => {
                        self.health.busy_deferrals = self.health.busy_deferrals.saturating_add(1);
                        "busy"
                    }
                    ExportError::QueueFull => {
                        self.health.queue_deferrals = self.health.queue_deferrals.saturating_add(1);
                        "queue_full"
                    }
                    ExportError::CapturePaused(ref status) => {
                        self.health.capture_deferrals = self.health.capture_deferrals.saturating_add(1);
                        match status.state {
                            crate::core::live_sync::CloudCaptureState::Starting => "capture_initializing",
                            crate::core::live_sync::CloudCaptureState::ReconciliationRequired => "reconciling",
                            _ => "capture_paused",
                        }
                    }
                    ExportError::Failure(_) => {
                        self.health.failures = self.health.failures.saturating_add(1);
                        "retrying"
                    }
                };
                self.next_attempt = now + retry_delay(&error, self.health.consecutive_deferrals);
                self.health.last_error = Some(error.to_string());
                let sustained = self.health.deferred_for_ms.is_some_and(|ms| ms >= 30_000);
                let initializing = matches!(&error, ExportError::CapturePaused(status)
                    if status.state == crate::core::live_sync::CloudCaptureState::Starting && status.last_error.is_none());
                let may_warn = (!matches!(error, ExportError::Busy) && !initializing) || sustained;
                // Independent of last_error/success: rapid busy/success cycles
                // cannot repeatedly reset the warning limiter.
                if may_warn
                    && self.last_warning_at.is_none_or(|at| {
                        now.saturating_duration_since(at) >= Duration::from_secs(60)
                    })
                {
                    log::warn!(
                        "Execution analytics [{}] deferred: {}; deferred_for_ms={:?} source_pending_at_sample={:?} oldest_prepared_record_age_ms={:?}",
                        self.health.stream,
                        error,
                        self.health.deferred_for_ms,
                        self.health.backlog.pending_records_at_last_sample,
                        self.health.backlog.oldest_pending_page_age_ms,
                    );
                    self.last_warning_at = Some(now);
                } else {
                    log::debug!(
                        "Execution analytics [{}] deferred: {}",
                        self.health.stream,
                        error
                    );
                }
            }
        }
    }
}
