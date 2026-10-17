use crate::{CloudEvent, EntityRevision, EventPriority};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveSyncBatch {
    pub protocol_version: u16,
    pub runtime_id: String,
    pub strategy_id: String,
    pub live_session_id: String,
    pub first_critical_sequence: Option<u64>,
    pub last_critical_sequence: Option<u64>,
    pub critical_events: Vec<CloudEvent>,
    pub telemetry: Vec<CloudEvent>,
}

impl LiveSyncBatch {
    pub fn from_events(events: Vec<CloudEvent>) -> Self {
        let first = events.first().expect("a sync batch is never empty");
        let protocol_version = first.protocol_version;
        let runtime_id = first.runtime_id.clone();
        let strategy_id = first.strategy_id.clone();
        let live_session_id = first.live_session_id.clone();
        let mut critical_events = Vec::new();
        let mut telemetry = Vec::new();
        for event in events {
            match event.priority {
                EventPriority::Critical | EventPriority::Reconciliation => {
                    critical_events.push(event)
                }
                EventPriority::Telemetry | EventPriority::Audit => telemetry.push(event),
            }
        }
        let first_critical_sequence = critical_events
            .iter()
            .filter_map(|event| event.critical_sequence)
            .min();
        let last_critical_sequence = critical_events
            .iter()
            .filter_map(|event| event.critical_sequence)
            .max();
        Self {
            protocol_version,
            runtime_id,
            strategy_id,
            live_session_id,
            first_critical_sequence,
            last_critical_sequence,
            critical_events,
            telemetry,
        }
    }

    pub fn events(&self) -> impl Iterator<Item = &CloudEvent> {
        self.critical_events.iter().chain(self.telemetry.iter())
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveSyncBatchResult {
    pub persisted_critical_through: u64,
    #[serde(default)]
    pub telemetry_revisions: Vec<EntityRevision>,
    #[serde(default)]
    pub applied: usize,
    #[serde(default)]
    pub duplicates: usize,
    #[serde(default)]
    pub superseded: usize,
    /// Present only when the durable hosted stream accepted the batch ahead of database
    /// projection. This cursor must never be presented as a database-commit timestamp.
    #[serde(default)]
    pub accepted_critical_through: Option<u64>,
    #[serde(default)]
    pub durability: Option<String>,
}
