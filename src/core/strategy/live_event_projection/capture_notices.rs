use crate::core::live_sync::{CloudCaptureState, CloudCaptureStatus};
use std::time::{Duration, Instant};

/// Fixed-size limiter, independent of event identity and success/error cycles.
#[derive(Default)]
pub(super) struct CaptureNoticeLimiter {
    last: Option<(CloudCaptureState, bool, Instant)>,
}
impl CaptureNoticeLimiter {
    pub(super) fn level(
        &mut self,
        status: &CloudCaptureStatus,
        now: Instant,
    ) -> Option<log::Level> {
        if self.last.is_some_and(|(state, failed, at)| {
            state == status.state
                && failed == status.last_error.is_some()
                && now.saturating_duration_since(at) < Duration::from_secs(60)
        }) {
            return None;
        }
        self.last = Some((status.state, status.last_error.is_some(), now));
        Some(
            if status.state == CloudCaptureState::Starting && status.last_error.is_none() {
                log::Level::Info
            } else {
                log::Level::Warn
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn initialization_is_informational_and_repeated_pauses_are_bounded() {
        let mut limiter = CaptureNoticeLimiter::default();
        let now = Instant::now();
        let mut status = CloudCaptureStatus {
            state: CloudCaptureState::Starting,
            ..Default::default()
        };
        assert_eq!(limiter.level(&status, now), Some(log::Level::Info));
        assert_eq!(limiter.level(&status, now + Duration::from_secs(1)), None);
        status.state = CloudCaptureState::ReconciliationRequired;
        assert_eq!(
            limiter.level(&status, now + Duration::from_secs(2)),
            Some(log::Level::Warn)
        );
        assert_eq!(limiter.level(&status, now + Duration::from_secs(59)), None);
        assert_eq!(
            limiter.level(&status, now + Duration::from_secs(62)),
            Some(log::Level::Warn)
        );
        status.state = CloudCaptureState::Starting;
        let mut limiter = CaptureNoticeLimiter::default();
        assert_eq!(limiter.level(&status, now), Some(log::Level::Info));
        status.last_error = Some("storage unavailable".into());
        assert_eq!(
            limiter.level(&status, now + Duration::from_secs(1)),
            Some(log::Level::Warn)
        );
    }
}
