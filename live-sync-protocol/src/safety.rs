//! Bounded operational facts, not an alternative trading-data channel.
//! Full-event acceptance and compact stop-fact acceptance are separate receipts.
use crate::{CloudEvent, EventKind, EventPriority};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const SAFETY_OBSERVATION_VERSION: u16 = 1;
pub const MAX_SAFETY_OBSERVATION_BYTES: usize = 8192;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SafetyKind {
    Lifecycle,
    Action,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SafetyObservation {
    pub version: u16,
    pub strategy_id: String,
    pub live_session_id: String,
    pub runtime_id: String,
    pub run_id: String,
    pub event_id: Uuid,
    pub revision: u64,
    pub created_at: DateTime<Utc>,
    pub kind: SafetyKind,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl SafetyObservation {
    /// Extracts a bounded operational fact. Extraction is NOT durable acceptance
    /// of the original event. Customer payload fields never cross this boundary.
    pub fn from_event(event: &CloudEvent) -> Option<Self> {
        if event.priority != EventPriority::Critical {
            return None;
        }
        let (kind, state, action_id) = match event.kind {
            EventKind::StrategyLifecycle => (
                SafetyKind::Lifecycle,
                event.payload.get("state")?.as_str()?.to_ascii_lowercase(),
                None,
            ),
            EventKind::StrategyActionAcknowledgement => {
                // Unknown/custom actions are not quota-independent capabilities.
                if event.payload.get("action").and_then(|v| v.as_str()) != Some("stop") {
                    return None;
                }
                (
                    SafetyKind::Action,
                    event.payload.get("status")?.as_str()?.to_owned(),
                    Some(event.payload.get("action_id")?.as_str()?.to_owned()),
                )
            }
            _ => return None,
        };
        let observation = Self {
            version: SAFETY_OBSERVATION_VERSION,
            strategy_id: event.strategy_id.clone(),
            live_session_id: event.live_session_id.clone(),
            runtime_id: event.runtime_id.clone(),
            run_id: event.run_id.clone()?,
            event_id: event.event_id,
            revision: event.entity_revision,
            created_at: event.created_at,
            kind,
            state,
            action_id,
            message: event
                .payload
                .get("message")
                .or_else(|| event.payload.get("reason"))
                .and_then(|v| v.as_str())
                .map(|v| v.chars().take(1024).collect()),
        };
        observation.validate().ok()?;
        Some(observation)
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.version != SAFETY_OBSERVATION_VERSION
            || self.revision == 0
            || self.revision > i64::MAX as u64
            || [
                &self.strategy_id,
                &self.live_session_id,
                &self.runtime_id,
                &self.run_id,
            ]
            .iter()
            .any(|id| id.is_empty() || id.len() > 128)
            || self
                .message
                .as_ref()
                .is_some_and(|m| m.chars().count() > 1024)
        {
            return Err("invalid safety observation");
        }
        match self.kind {
            SafetyKind::Lifecycle
                if self.action_id.is_none()
                    && matches!(self.state.as_str(), "stopping" | "stopped" | "failed") => {}
            SafetyKind::Action
                if matches!(
                    self.state.as_str(),
                    "acknowledged" | "processing" | "completed" | "failed"
                ) && self
                    .action_id
                    .as_ref()
                    .is_some_and(|id| id.starts_with("strategy_actions:") && id.len() <= 256) => {}
            _ => return Err("unsupported safety capability"),
        }
        if crate::json_encoded_len(self)
            .map_err(|_| "invalid safety encoding")?
            > MAX_SAFETY_OBSERVATION_BYTES
        {
            return Err("safety observation too large");
        }
        Ok(())
    }

    pub fn entity_key(&self) -> &str {
        self.action_id.as_deref().unwrap_or("lifecycle")
    }
    pub fn terminal(&self) -> bool {
        matches!(self.state.as_str(), "stopped" | "completed" | "failed")
    }

    pub fn state_rank(&self) -> i64 {
        if self.terminal() {
            3
        } else if self.state == "processing" {
            2
        } else {
            1
        }
    }

    pub fn same_run(&self, other: &Self) -> bool {
        self.runtime_id == other.runtime_id
            && self.strategy_id == other.strategy_id
            && self.live_session_id == other.live_session_id
            && self.run_id == other.run_id
    }

    /// Compact facts may replace older nonterminal facts, never immutable
    /// trading evidence. A terminal stop dominates a later retry of "stopping".
    pub fn covers(&self, other: &Self) -> bool {
        self.same_run(other)
            && self.entity_key() == other.entity_key()
            && (self == other
                || (self.event_id != other.event_id
                    && ((self.terminal() && !other.terminal())
                        || (self.revision > other.revision
                            && self.state_rank() >= other.state_rank()
                            && (!other.terminal() || self.state == other.state)))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EventEnvelope;
    fn event() -> CloudEvent {
        let mut event = EventEnvelope::new(
            "runtime",
            "strategy",
            "session",
            "lifecycle",
            1,
            EventPriority::Critical,
            EventKind::StrategyLifecycle,
            serde_json::json!({"state":"stopped","account":{"private":"not operational"},"universe":["BTC"]}),
        );
        event.run_id = Some("run".into());
        event
    }
    #[test]
    fn extracts_only_stop_safety_without_customer_payloads() {
        let mut e = event();
        let o = SafetyObservation::from_event(&e).unwrap();
        let encoded = serde_json::to_string(&o).unwrap();
        assert!(!encoded.contains("account"));
        assert!(!encoded.contains("universe"));
        e.payload["state"] = serde_json::json!("running");
        assert!(SafetyObservation::from_event(&e).is_none());
        e.kind = EventKind::StrategyActionAcknowledgement;
        e.payload = serde_json::json!({"action":"stop","status":"completed","action_id":"strategy_actions:a"});
        assert!(SafetyObservation::from_event(&e).is_some());
        e.payload["action"] = serde_json::json!("start");
        assert!(SafetyObservation::from_event(&e).is_none());
    }
    #[test]
    fn operational_receipts_cover_only_the_same_entity_and_a_nonregressing_fact() {
        let first = SafetyObservation::from_event(&event()).unwrap();
        assert!(first.covers(&first));
        let mut changed = first.clone();
        changed.state = "stopping".into();
        assert!(
            !first.covers(&changed),
            "same event ID cannot mean a different fact"
        );
        changed.event_id = Uuid::new_v4();
        changed.revision += 1;
        assert!(
            first.covers(&changed),
            "terminal stop dominates a later stopping retry"
        );
        changed.state = "failed".into();
        assert!(
            !first.covers(&changed),
            "conflicting terminal outcomes need explicit recovery"
        );
        changed.run_id = "other".into();
        assert!(!first.covers(&changed));
    }

    #[test]
    fn rejects_extra_fields_invalid_combinations_and_oversized_messages() {
        let mut o = SafetyObservation::from_event(&event()).unwrap();
        let mut v = serde_json::to_value(&o).unwrap();
        v["payload"] = serde_json::json!({"extra":1});
        assert!(serde_json::from_value::<SafetyObservation>(v).is_err());
        o.action_id = Some("strategy_actions:a".into());
        assert!(o.validate().is_err());
        o.action_id = None;
        o.message = Some("x".repeat(1025));
        assert!(o.validate().is_err());
    }
}
