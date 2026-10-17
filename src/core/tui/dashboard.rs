//! Presentation helpers use copied telemetry only; never perform broker/network/disk I/O.
use super::{RuntimeSnapshot, detail_line};
use chrono::Utc;
use ratatui::{
    style::{Color, Style},
    text::Line,
};

pub(super) fn active_count(snapshot: &RuntimeSnapshot) -> usize {
    ["New", "Executed", "Filled"]
        .iter()
        .map(|state| snapshot.insight_counts.get(*state).copied().unwrap_or(0))
        .sum()
}

fn counts(snapshot: &RuntimeSnapshot, states: &[&str]) -> String {
    states
        .iter()
        .map(|state| {
            format!(
                "{state}: {}",
                snapshot.insight_counts.get(*state).copied().unwrap_or(0)
            )
        })
        .collect::<Vec<_>>()
        .join("  ")
}

pub(super) fn live_summary(snapshot: &RuntimeSnapshot) -> Vec<Line<'static>> {
    let age = (Utc::now() - snapshot.updated_at).num_seconds().max(0);
    vec![
        Line::styled(
            format!(
                "Snapshot age: {age}s{} | market clock: {} UTC",
                if age > 10 { " — STALE" } else { "" },
                snapshot
                    .current_time
                    .map(|time| time.format("%H:%M:%S").to_string())
                    .unwrap_or_else(|| "unknown".into())
            ),
            Style::default().fg(if age > 10 { Color::Yellow } else { Color::Cyan }),
        ),
        Line::from(counts(snapshot, &["New", "Executed", "Filled"])),
        Line::from(counts(snapshot, &["Closed", "Cancelled", "Rejected"])),
    ]
}

pub(super) fn diagnostic_lines(snapshot: &RuntimeSnapshot) -> Vec<Line<'static>> {
    let mut lines = vec![
        detail_line(
            "Telemetry uptime",
            format!(
                "{}s",
                (Utc::now() - snapshot.started_at).num_seconds().max(0)
            ),
        ),
        detail_line(
            "Snapshot age",
            format!(
                "{}s",
                (Utc::now() - snapshot.updated_at).num_seconds().max(0)
            ),
        ),
        detail_line("Broker", &snapshot.broker_status),
        detail_line("Data feed", &snapshot.datafeed_status),
        detail_line("AQS runner status", &snapshot.aqs_sync_status),
        detail_line("Universe", snapshot.universe.join(", ")),
        detail_line(
            "Active preview",
            format!(
                "{} / {} (latest updated; bounded at {})",
                snapshot.active_insights.len(),
                active_count(snapshot),
                super::INSIGHT_PREVIEW_LIMIT
            ),
        ),
    ];
    if let Some(sync) = &snapshot.sync {
        let health = &sync.publisher;
        lines.extend([
            detail_line("Delivery path", &sync.transport),
            detail_line("Runtime ID", &sync.runtime_id),
            detail_line("Run ID", &sync.run_id),
            detail_line(
                "Publisher sample age",
                format!("{}s", (Utc::now() - sync.sampled_at).num_seconds().max(0)),
            ),
            detail_line("Queued critical", health.pending_critical.to_string()),
            detail_line(
                "Coalesced telemetry entities",
                health.pending_telemetry_entities.to_string(),
            ),
            detail_line(
                "Locally accepted critical (cumulative)",
                health.locally_accepted_critical.to_string(),
            ),
            detail_line(
                "Deduplicated (cumulative)",
                health.deduplicated_critical.to_string(),
            ),
            detail_line(
                "Emergency writes (cumulative)",
                health.emergency_outbox_writes.to_string(),
            ),
            detail_line(
                "Publish failures (cumulative)",
                health.publish_failures.to_string(),
            ),
            detail_line(
                "Last local ACK latency",
                health
                    .last_local_ack_latency_us
                    .map(|us| format!("{:.2} ms", us as f64 / 1000.0))
                    .unwrap_or_else(|| "not observed".into()),
            ),
            detail_line("Publisher stopping", health.shutting_down.to_string()),
            Line::styled(
                "Local ACK does not confirm cloud persistence or dashboard rendering.",
                Style::default().fg(Color::Yellow),
            ),
            Line::from("Remote outbox / DLQ / reconciliation: not observed by this process."),
        ]);
    } else {
        lines.push(Line::from(
            "Publisher: no sample (standalone, backtest, or startup).",
        ));
    }
    if let Some(metrics) = &snapshot.metrics {
        lines.push(detail_line(
            "Metrics age",
            metrics
                .updated_at
                .map(|time| format!("{}s", (Utc::now() - time).num_seconds().max(0)))
                .unwrap_or_else(|| "unknown".into()),
        ));
    }
    if let Some(path) = &snapshot.saved_result_path {
        lines.push(detail_line("Saved result", path));
    }
    lines.push(Line::from(
        "Provider capabilities (advertised support, not subscription activity):",
    ));
    lines.extend(
        snapshot
            .provider_capabilities
            .iter()
            .map(|feature| Line::from(feature.clone())),
    );
    lines
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn counts_exclude_terminal_states() {
        let mut snapshot = RuntimeSnapshot::default();
        snapshot.insight_counts = [
            ("New".into(), 2),
            ("Filled".into(), 3),
            ("Closed".into(), 100),
        ]
        .into();
        assert_eq!(active_count(&snapshot), 5);
    }
    #[test]
    fn disconnected_is_not_reported_as_caught_up() {
        let snapshot = RuntimeSnapshot::default();
        let text = format!("{:?}", diagnostic_lines(&snapshot));
        assert!(text.contains("no sample"));
        assert!(!text.contains("caught up"));
    }
}
