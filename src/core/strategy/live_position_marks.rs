//! Bounded low-priority broker reads. A missing read is never a flat position.
use crate::core::broker::{
    command_journal::marks::{MAX_MARK_ADMISSION, PositionMarkBatch, PositionMarkOwner},
    command_worker::BrokerCommandSender,
    commands::BrokerCommandError,
    position_valuation::PositionValuationReader,
};
use aqe_live_protocol::POSITION_MARK_MINUTE_US;
use std::{collections::BTreeMap, sync::Arc, time::Duration};

pub(super) fn enabled() -> bool {
    // Broker-neutral live valuations are enabled by default. Standalone users
    // may explicitly opt out; no calendar or AQS connection is required.
    capture_enabled(std::env::var("AQE_POSITION_MARK_CAPTURE").ok().as_deref())
}
fn capture_enabled(value: Option<&str>) -> bool {
    value.is_none_or(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
}
fn minute() -> i64 {
    let now = chrono::Utc::now().timestamp_micros();
    now.div_euclid(POSITION_MARK_MINUTE_US) * POSITION_MARK_MINUTE_US
}

pub(super) async fn sample(
    reader: Arc<dyn PositionValuationReader>,
    sender: BrokerCommandSender,
    capture: Option<crate::core::live_sync::CloudCaptureGate>,
) {
    let account = sender.scope().1.to_owned();
    let mut after = None;
    let mut next_minute = 0;
    'sampling: loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if capture.as_ref().is_some_and(|gate| gate.is_paused()) {
            // Resume with current valuations, never synthesize skipped minutes
            // or retain pages of optional reads behind a cloud pause.
            after = None;
            next_minute = 0;
            continue;
        }
        let slot = minute();
        if slot < next_minute {
            continue;
        }
        let page = match sender.try_position_mark_owners(slot, after.clone()) {
            Ok(reply) => reply.wait().await,
            Err(error) if error.is_analytics_backpressure() => continue,
            Err(_) => return,
        };
        let page = match page {
            Ok(p) => p,
            Err(BrokerCommandError::Closed) => return,
            Err(e) => {
                log::warn!("Position mark owners unavailable: {e}");
                next_minute = slot + POSITION_MARK_MINUTE_US;
                continue;
            }
        };
        let mut groups = BTreeMap::<(String, u64, String), Vec<PositionMarkOwner>>::new();
        for owner in page.owners {
            groups
                .entry((
                    owner.fill_provider.clone(),
                    owner.execution_epoch,
                    owner.symbol.clone(),
                ))
                .or_default()
                .push(owner);
        }
        for ((_, _, symbol), owners) in groups {
            if capture.as_ref().is_some_and(|gate| gate.is_paused()) {
                continue 'sampling;
            }
            let ids = owners
                .iter()
                .map(|o| o.position_id.clone())
                .collect::<Vec<_>>();
            let result = tokio::time::timeout(
                Duration::from_secs(6),
                reader.read_positions(&account, &symbol, &ids),
            )
            .await;
            if let Ok(Ok(Some(values))) = result {
                if capture.as_ref().is_some_and(|gate| gate.is_paused()) {
                    continue 'sampling;
                }
                // Validate the whole response before partitioning; extra or
                // repeated identities must not disappear during chunking.
                if let Err(e) = values.validate(&account, &symbol, &ids) {
                    log::warn!("Position mark read rejected: {e}");
                } else {
                    let sample_minute = minute();
                    for owners in owners.chunks(MAX_MARK_ADMISSION) {
                        let mut subset = values.clone();
                        subset
                            .positions
                            .retain(|p| owners.iter().any(|o| o.position_id == p.position_id));
                        let result =
                            match PositionMarkBatch::new(owners.to_vec(), subset, sample_minute) {
                                Ok(batch) => {
                                    sender
                                        .record_position_marks_for_capture(batch, capture.clone())
                                        .await
                                }
                                Err(e) => Err(e),
                            };
                        if let Err(e) = result {
                            if matches!(e, BrokerCommandError::Closed) {
                                return;
                            }
                            if matches!(e, BrokerCommandError::CapturePaused) {
                                continue 'sampling;
                            }
                            if !e.is_analytics_backpressure() {
                                log::warn!("Position mark admission deferred: {e}");
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
            }
            // All reads, including unsupported symbols, advance the owner page.
            // Never let a busy/old EA cause a tight retry loop or starve trading.
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        if let Some(next) = page.next_after {
            after = Some(next);
        } else {
            after = None;
            next_minute = slot + POSITION_MARK_MINUTE_US;
        }
    }
}

#[cfg(test)]
mod tests;
