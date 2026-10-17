//! Hosted stop controls are addressed to one immutable process, never to a mutable
//! "latest registration" timestamp. Repeated requests for that process are idempotent.
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HostedControlTarget {
    pub strategy_id: String,
    pub live_session_id: String,
    pub runtime_id: String,
    pub run_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostedControlStatus {
    Idle,
    Pending,
    Received,
    Completed,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostedStopControl {
    /// First durable registration order; credential refreshes never change it.
    #[serde(default)]
    pub registration_sequence: u64,
    pub target: HostedControlTarget,
    pub action_id: String,
    pub status: HostedControlStatus,
    pub requested_at_us: Option<i64>,
    #[serde(default)]
    pub acknowledged_at_us: Option<i64>,
    #[serde(default)]
    pub completed_at_us: Option<i64>,
    #[serde(default)]
    pub message: Option<String>,
    pub updated_at_us: i64,
}
