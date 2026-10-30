//! Cached, bounded diagnostics only; never query storage from a log statement.
use super::*;

impl CloudCaptureStatus {
    pub fn diagnostic_phase(&self) -> &'static str {
        match self.state {
            CloudCaptureState::Starting => "initializing",
            CloudCaptureState::ReconciliationRequired => "reconciling",
            CloudCaptureState::Paused => "paused",
            CloudCaptureState::Active => "source_observation_pending",
            CloudCaptureState::Disabled => "disabled",
        }
    }

    /// Reason for a refused admission, not a subscription/quota diagnosis.
    pub fn diagnostic_reason(&self, now_us: i64) -> &str {
        if let Some(error) = self.last_error.as_deref() {
            return error;
        }
        if self.state == CloudCaptureState::Starting {
            return "waiting for the first qualified local journal observation";
        }
        if self
            .limit_bytes
            .is_some_and(|limit| self.retained_bytes >= limit)
        {
            return "local cloud-history payload buffer reached its limit";
        }
        if self
            .physical
            .is_some_and(|usage| usage.exceeds_data_limit())
        {
            return "local cloud-history disk partition reached its limit";
        }
        if self.source_journal.as_ref().is_some_and(|source| {
            now_us.saturating_sub(source.observed_at_us) > source::SOURCE_FRESHNESS_US
        }) {
            return "local journal observation expired; fresh observation and reconciliation required";
        }
        if self
            .source_journal
            .as_ref()
            .is_some_and(|source| source.pause.is_some())
        {
            return "source journal has a retained history-gap receipt awaiting recovery";
        }
        if self.pause.is_some() || self.state == CloudCaptureState::ReconciliationRequired {
            return "current-state replay and durable history-gap acknowledgement are pending";
        }
        "local source observation is not currently qualified"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_diagnostics_distinguish_startup_capacity_and_recovery() {
        let mut status = CloudCaptureStatus {
            state: CloudCaptureState::Starting,
            ..Default::default()
        };
        assert_eq!(status.diagnostic_phase(), "initializing");
        assert!(status.diagnostic_reason(1).contains("first qualified"));
        status.state = CloudCaptureState::Paused;
        status.limit_bytes = Some(100);
        status.retained_bytes = 100;
        assert!(status.diagnostic_reason(1).contains("payload buffer"));
        status.retained_bytes = 0;
        status.state = CloudCaptureState::ReconciliationRequired;
        assert_eq!(status.diagnostic_phase(), "reconciling");
        assert!(status.diagnostic_reason(1).contains("replay"));
        status.last_error = Some("source journal storage unavailable".into());
        assert_eq!(
            status.diagnostic_reason(1),
            "source journal storage unavailable"
        );
    }
}
