//! Portable, generic local-buffer policy. No subscription, price or broker
//! concepts. This optional extension is absent from all existing launch files.
use serde::{Deserialize, Serialize};

pub const CAPTURE_SUPPORT_MARKER: &str = "aqe-protected-capture-config-v1";
pub const SOURCE_OBSERVATION_SUPPORT_MARKER: &str = "aqe-source-observation-lifecycle-v1";
pub const JOURNAL_RECOVERY_SUPPORT_MARKER: &str = "aqe-exited-journal-recovery-v1";
pub const SEPARATE_HISTORY_SUPPORT_MARKER: &str = "aqe-separate-cloud-history-v1";
pub const BOUNDED_METADATA_SUPPORT_MARKER: &str = "aqe-bounded-outbox-metadata-v1";
pub const MIN_CAPTURE_OPERATIONAL_HEADROOM_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapturePartitionConfig {
    pub physical_limit_bytes: u64,
    pub operational_headroom_bytes: u64,
}
impl CapturePartitionConfig {
    pub fn validate(self) -> Result<(), String> {
        if self.physical_limit_bytes > i64::MAX as u64
            || self.operational_headroom_bytes < MIN_CAPTURE_OPERATIONAL_HEADROOM_BYTES
            || self.physical_limit_bytes <= self.operational_headroom_bytes
        {
            return Err(format!(
                "{CAPTURE_SUPPORT_MARKER}: invalid physical partition"
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureOutboxConfig {
    pub payload_limit_bytes: u64,
    pub physical: CapturePartitionConfig,
}
impl CaptureOutboxConfig {
    pub fn validate(self) -> Result<(), String> {
        self.physical.validate()?;
        if self.payload_limit_bytes == 0 || self.payload_limit_bytes > i64::MAX as u64 {
            return Err(format!(
                "{CAPTURE_SUPPORT_MARKER}: invalid payload partition"
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloudCaptureConfig {
    pub version: u16,
    pub source_journal: CapturePartitionConfig,
    pub publisher: CaptureOutboxConfig,
    /// Embedded delivery owns a second outbox. Managed delivery's shared host
    /// receiver is operator-budgeted separately, never per-strategy here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedded_receiver: Option<CaptureOutboxConfig>,
}
impl CloudCaptureConfig {
    pub fn validate(self) -> Result<(), String> {
        if self.version != 1 {
            return Err(format!(
                "{CAPTURE_SUPPORT_MARKER}/{SOURCE_OBSERVATION_SUPPORT_MARKER}/{JOURNAL_RECOVERY_SUPPORT_MARKER}/{SEPARATE_HISTORY_SUPPORT_MARKER}/{BOUNDED_METADATA_SUPPORT_MARKER}: unsupported capture version"
            ));
        }
        self.source_journal.validate()?;
        self.publisher.validate()?;
        if let Some(receiver) = self.embedded_receiver {
            receiver.validate()?;
        }
        Ok(())
    }
}
