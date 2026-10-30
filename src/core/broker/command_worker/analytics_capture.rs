//! Low-priority, bounded access to local analytics-quality facts. An owner-side
//! bridge can await outbox/gateway I/O after releasing the journal permit. The
//! broker worker itself never waits on a cloud service or another SQLite file.
use super::*;
use crate::core::broker::command_journal::{
    AnalyticsCaptureHandoffReceipt, AnalyticsCapturePause, AnalyticsCaptureStatus,
};

pub(super) async fn observe_refusal<T>(
    journal: &BrokerCommandJournal,
    capture: &Option<CloudCaptureGate>,
    result: &Result<T, BrokerCommandError>,
) {
    if !matches!(result, Err(BrokerCommandError::CapturePaused)) {
        return;
    }
    let Some(capture) = capture
        .as_ref()
        .filter(|capture| capture.journal_required())
    else {
        return;
    };
    match journal.analytics_capture_pause().await {
        Ok(Some(pause)) => capture.note_journal_pause(pause),
        _ => capture.journal_unavailable(),
    }
}

impl BrokerCommandSender {
    /// The owning recovery coordinator must have sealed replay first. This
    /// bounded request never contacts a broker or waits on another file.
    pub fn try_complete_analytics_capture(
        &self,
        receipt: AnalyticsCaptureHandoffReceipt,
    ) -> Result<BrokerJournalReply<bool>, BrokerCommandError> {
        receipt
            .validate()
            .map_err(|error| BrokerCommandError::Invalid(error.into()))?;
        let permit = self.try_analytics_permit()?;
        let (reply, receive) = oneshot::channel();
        self.try_analytics_request(JournalRequest::CompleteAnalyticsCapture {
            receipt,
            _permit: permit,
            reply,
        })?;
        Ok(BrokerJournalReply(receive))
    }
    pub fn try_analytics_capture_status(
        &self,
    ) -> Result<BrokerJournalReply<Option<AnalyticsCaptureStatus>>, BrokerCommandError> {
        let permit = self.try_analytics_permit()?;
        let (reply, receive) = oneshot::channel();
        self.try_analytics_request(JournalRequest::AnalyticsCaptureStatus {
            _permit: permit,
            reply,
        })?;
        Ok(BrokerJournalReply(receive))
    }
    pub fn try_analytics_capture_pause(
        &self,
    ) -> Result<BrokerJournalReply<Option<AnalyticsCapturePause>>, BrokerCommandError> {
        let permit = self.try_analytics_permit()?;
        let (reply, receive) = oneshot::channel();
        self.try_analytics_request(JournalRequest::AnalyticsCapturePause {
            _permit: permit,
            reply,
        })?;
        Ok(BrokerJournalReply(receive))
    }

    pub fn try_acknowledge_analytics_capture_handoff(
        &self,
        receipt: AnalyticsCaptureHandoffReceipt,
    ) -> Result<BrokerJournalReply<bool>, BrokerCommandError> {
        receipt
            .validate()
            .map_err(|error| BrokerCommandError::Invalid(error.into()))?;
        let permit = self.try_analytics_permit()?;
        let (reply, receive) = oneshot::channel();
        self.try_analytics_request(JournalRequest::AcknowledgeAnalyticsCaptureHandoff {
            receipt,
            _permit: permit,
            reply,
        })?;
        Ok(BrokerJournalReply(receive))
    }
}
