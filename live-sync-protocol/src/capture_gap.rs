//! Bounded data-quality facts. Never an event/fill receipt or proof of broker
//! settlement; authentication belongs to the service handling the notice.
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const CAPTURE_GAP_NOTICE_VERSION: u16 = 1;
pub const CAPTURE_GAP_LOCAL_HANDOFF_VERSION: u16 = 1;
pub const MAX_CAPTURE_GAP_NOTICE_BYTES: usize = 2048;

fn bounded_scalar_object(value: &serde_json::Value, keys: &[&str]) -> bool {
    value.as_object().is_some_and(|object| {
        object.len() == keys.len()
            && object.iter().all(|(key, value)| {
                keys.contains(&key.as_str())
                    && (value.is_null()
                        || value.as_u64().is_some()
                        || value.as_i64().is_some()
                        || value.as_str().is_some_and(|s| s.len() <= 128))
            })
    })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CaptureGapNotice {
    pub version: u16,
    pub notice_id: Uuid,
    pub strategy_id: String,
    pub live_session_id: String,
    pub source_runtime_id: String,
    pub source_run_id: Option<String>,
    pub source_generation: u64,
    pub started_at_us: i64,
    pub last_blocked_at_us: i64,
    pub origin_runtime_id: Option<String>,
    pub origin_run_id: Option<String>,
    pub last_blocked_run_id: Option<String>,
    pub ownership_handoffs: u64,
}

impl CaptureGapNotice {
    /// Bound an already-decoded local bus value before cloning/deserializing it.
    /// This lane must not duplicate an arbitrary customer JSON payload in RAM.
    pub fn from_bounded_value(value: &serde_json::Value) -> Result<Self, &'static str> {
        if !bounded_scalar_object(
            value,
            &[
                "version",
                "noticeId",
                "strategyId",
                "liveSessionId",
                "sourceRuntimeId",
                "sourceRunId",
                "sourceGeneration",
                "startedAtUs",
                "lastBlockedAtUs",
                "originRuntimeId",
                "originRunId",
                "lastBlockedRunId",
                "ownershipHandoffs",
            ],
        ) {
            return Err("invalid capture gap notice shape");
        }
        let notice: Self = serde_json::from_value(value.clone())
            .map_err(|_| "invalid capture gap notice encoding")?;
        notice.validate()?;
        Ok(notice)
    }
    pub fn stable_id(&self) -> Uuid {
        let fields = (
            &self.strategy_id,
            &self.live_session_id,
            &self.source_runtime_id,
            &self.source_run_id,
            self.source_generation,
            self.started_at_us,
            self.last_blocked_at_us,
            &self.origin_runtime_id,
            &self.origin_run_id,
            &self.last_blocked_run_id,
            self.ownership_handoffs,
        );
        let bytes = serde_json::to_vec(&fields).expect("bounded scalar tuple");
        Uuid::new_v5(&Uuid::NAMESPACE_OID, &bytes)
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.version != CAPTURE_GAP_NOTICE_VERSION
            || self.notice_id != self.stable_id()
            || self.source_generation == 0
            || self.source_generation > i64::MAX as u64
            || self.ownership_handoffs > i64::MAX as u64
            || self.started_at_us <= 0
            || self.last_blocked_at_us < self.started_at_us
            || chrono::DateTime::from_timestamp_micros(self.last_blocked_at_us).is_none()
            || [
                &self.strategy_id,
                &self.live_session_id,
                &self.source_runtime_id,
            ]
            .iter()
            .any(|s| s.is_empty() || s.len() > 128)
            || [
                &self.source_run_id,
                &self.origin_runtime_id,
                &self.origin_run_id,
                &self.last_blocked_run_id,
            ]
            .iter()
            .any(|s| s.as_ref().is_some_and(|s| s.is_empty() || s.len() > 128))
            || crate::json_encoded_len(self)
                .map_err(|_| "invalid capture gap encoding")?
                > MAX_CAPTURE_GAP_NOTICE_BYTES
        {
            return Err("invalid capture gap notice");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CaptureGapReceipt {
    pub version: u16,
    pub notice_id: Uuid,
    pub strategy_id: String,
    pub live_session_id: String,
}
impl CaptureGapReceipt {
    pub fn from_bounded_value(value: &serde_json::Value) -> Result<Self, &'static str> {
        if !bounded_scalar_object(
            value,
            &["version", "noticeId", "strategyId", "liveSessionId"],
        ) {
            return Err("invalid capture gap receipt shape");
        }
        serde_json::from_value(value.clone()).map_err(|_| "invalid capture gap receipt encoding")
    }
    pub fn for_notice(n: &CaptureGapNotice) -> Self {
        Self {
            version: CAPTURE_GAP_NOTICE_VERSION,
            notice_id: n.notice_id,
            strategy_id: n.strategy_id.clone(),
            live_session_id: n.live_session_id.clone(),
        }
    }
    pub fn covers(&self, n: &CaptureGapNotice) -> bool {
        self == &Self::for_notice(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn notice_is_fixed_bounded_and_receipts_cannot_acknowledge_changed_facts() {
        let mut n = CaptureGapNotice {
            version: 1,
            notice_id: Uuid::nil(),
            strategy_id: "strategy".into(),
            live_session_id: "session".into(),
            source_runtime_id: "runtime".into(),
            source_run_id: None,
            source_generation: 2,
            started_at_us: 10,
            last_blocked_at_us: 20,
            origin_runtime_id: None,
            origin_run_id: None,
            last_blocked_run_id: None,
            ownership_handoffs: 0,
        };
        n.notice_id = n.stable_id();
        n.validate().unwrap();
        let receipt = CaptureGapReceipt::for_notice(&n);
        assert!(receipt.covers(&n));
        let canonical = serde_json::to_value(&n).unwrap();
        assert_eq!(CaptureGapNotice::from_bounded_value(&canonical).unwrap(), n);
        let mut oversized = canonical.clone();
        oversized["originRuntimeId"] = serde_json::json!("x".repeat(2 * 1024 * 1024));
        assert!(CaptureGapNotice::from_bounded_value(&oversized).is_err());
        let mut nested = canonical;
        nested["sourceRunId"] = serde_json::json!({"customer":"not metadata"});
        assert!(CaptureGapNotice::from_bounded_value(&nested).is_err());
        let canonical_receipt = serde_json::to_value(&receipt).unwrap();
        assert_eq!(
            CaptureGapReceipt::from_bounded_value(&canonical_receipt).unwrap(),
            receipt
        );
        let mut bad_receipt = canonical_receipt;
        bad_receipt["strategyId"] = serde_json::json!("x".repeat(1024 * 1024));
        assert!(CaptureGapReceipt::from_bounded_value(&bad_receipt).is_err());
        n.started_at_us = 9;
        assert!(n.validate().is_err());
        n.notice_id = n.stable_id();
        assert!(!receipt.covers(&n));
        let mut value = serde_json::to_value(&n).unwrap();
        value["account"] = serde_json::json!({"private":true});
        assert!(serde_json::from_value::<CaptureGapNotice>(value).is_err());
        n.last_blocked_at_us = i64::MAX;
        n.notice_id = n.stable_id();
        assert!(
            n.validate().is_err(),
            "a valid identity cannot admit an unrepresentable timestamp"
        );
    }
}
