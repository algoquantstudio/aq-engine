//! Successful delivery is a recovery hint, never a substitute for a durable ACK.
//! Keep outage backoff, but do not leave proven-reachable work asleep for minutes.
use super::*;

const PRIORITIES: [EventPriority; 4] = [
    EventPriority::Critical,
    EventPriority::Telemetry,
    EventPriority::Audit,
    EventPriority::Reconciliation,
];
const LIMIT: usize = 256;
const PROBE_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct Deferred {
    priority: EventPriority,
    ids: Vec<EventId>,
    error: String,
    deadline_us: i64,
}

#[derive(Default)]
struct State {
    lanes: [Option<Deferred>; 4],
    last_probe: Option<std::time::Instant>,
}

#[derive(Default)]
pub(super) struct Recovery(Mutex<State>);

impl Recovery {
    pub(super) fn remember(
        &self,
        priority: EventPriority,
        ids: &[EventId],
        error: &str,
        deadline_us: i64,
    ) {
        if ids.is_empty() || error.len() > 4096 || error == "event gateway returned HTTP 429" {
            return;
        }
        let index = PRIORITIES.iter().position(|p| *p == priority).unwrap();
        let mut state = self.0.lock();
        let lane = state.lanes[index].get_or_insert_with(|| Deferred {
            priority,
            ids: Vec::new(),
            error: error.to_owned(),
            deadline_us,
        });
        // Preserve the first deferred group rather than allocating a map per
        // event/error. Everything beyond this hint remains durably scheduled.
        if lane.error != error {
            return;
        }
        lane.deadline_us = lane.deadline_us.max(deadline_us);
        for id in ids {
            if lane.ids.len() == LIMIT {
                break;
            }
            if !lane.ids.contains(id) {
                lane.ids.push(*id);
            }
        }
    }

    fn proven_reachable(&self) -> Vec<Deferred> {
        let mut state = self.0.lock();
        if state
            .last_probe
            .is_some_and(|at| at.elapsed() < PROBE_INTERVAL)
        {
            return Vec::new();
        }
        let now = now_epoch_micros();
        let ready = state
            .lanes
            .iter_mut()
            .filter_map(Option::take)
            .filter(|lane| lane.deadline_us > now)
            .collect::<Vec<_>>();
        if !ready.is_empty() {
            state.last_probe = Some(std::time::Instant::now());
        }
        ready
    }
}

// Old outboxes did not persist an error kind. Recover only exact static
// transport diagnostics on startup; never guess that arbitrary errors are safe.
fn known_transport_failure(error: &str) -> bool {
    matches!(
        error,
        "event gateway publication unavailable"
            | "event gateway registration unavailable"
            | "invalid event gateway acknowledgement"
    ) || error
        .strip_prefix("event gateway returned HTTP ")
        .and_then(|status| status.parse::<u16>().ok())
        .is_some_and(|status| (500..=599).contains(&status))
}

impl SyncAgent {
    pub(super) async fn restore_delivery_recovery(&self) -> Result<(), SyncAgentError> {
        for priority in PRIORITIES {
            for hint in self
                .outbox
                .deferred_retry_hints(
                    priority,
                    &self.config.runtime_id,
                    &self.config.strategy_id,
                    &self.config.live_session_id,
                )
                .await?
            {
                if known_transport_failure(&hint.error) {
                    self.delivery_recovery.remember(
                        priority,
                        &[hint.event_id],
                        &hint.error,
                        hint.deadline_us,
                    );
                }
            }
        }
        Ok(())
    }

    pub(super) async fn wake_delivery_retries(&self) {
        if self.cloud_pause_remaining().is_some() {
            return;
        }
        for deferred in self.delivery_recovery.proven_reachable() {
            if self
                .outbox
                .wake_retry_events(
                    deferred.priority,
                    &self.config.runtime_id,
                    &self.config.strategy_id,
                    &self.config.live_session_id,
                    &deferred.ids,
                    &deferred.error,
                )
                .await
                .is_err()
            {
                // This hint is best-effort. Losing it must neither stop the
                // delivery worker nor acknowledge/delete any retained event.
                self.delivery_recovery.remember(
                    deferred.priority,
                    &deferred.ids,
                    &deferred.error,
                    deferred.deadline_us,
                );
            }
        }
    }
}

#[cfg(test)]
mod tests;
