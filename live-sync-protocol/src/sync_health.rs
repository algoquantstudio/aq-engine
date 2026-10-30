//! Capture admission and delivery acknowledgement are separate observations.
use serde::{Deserialize, Serialize};

/// Only retain the status needed by readers, not the host's disk accounting.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaptureDeliveryStatus {
    pub state: String,
}

/// A successful upload of older work does not prove that new capture is active.
/// Preserve delivery errors, and never change admission or retry behavior here.
pub fn sync_delivery_state<'a>(delivery: &'a str, capture: Option<&str>) -> &'a str {
    if !matches!(delivery, "healthy" | "cloud_accepted") {
        return delivery;
    }
    match capture {
        None | Some("active" | "disabled") => delivery,
        Some("paused") => "capture_paused",
        Some("reconciliation_required") => "capture_reconciliation_required",
        Some(_) => "capture_unavailable",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_acknowledged_old_batch_cannot_hide_a_capture_pause() {
        for delivery in ["healthy", "cloud_accepted"] {
            assert_eq!(
                sync_delivery_state(delivery, Some("paused")),
                "capture_paused"
            );
            assert_eq!(
                sync_delivery_state(delivery, Some("reconciliation_required")),
                "capture_reconciliation_required"
            );
            for state in ["starting", "unknown", ""] {
                assert_eq!(
                    sync_delivery_state(delivery, Some(state)),
                    "capture_unavailable"
                );
            }
            for state in [None, Some("active"), Some("disabled")] {
                assert_eq!(sync_delivery_state(delivery, state), delivery);
            }
        }
        for delivery in ["blocked", "degraded", "cloud_paused", "recovering"] {
            assert_eq!(sync_delivery_state(delivery, Some("paused")), delivery);
        }
    }
}
