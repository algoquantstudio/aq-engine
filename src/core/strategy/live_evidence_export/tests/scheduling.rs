use super::*;
use crate::core::broker::commands::BrokerCommandError;

#[test]
fn capture_deferral_is_not_an_export_failure_and_keeps_specific_diagnostics() {
    use crate::core::live_sync::{CloudCaptureState, CloudCaptureStatus};
    let mut exporter = enabled_exporter();
    let now = Instant::now();
    let mut status = CloudCaptureStatus { state: CloudCaptureState::Starting, ..Default::default() };
    exporter.record_result(Err(ExportError::CapturePaused(Box::new(status.clone()))), now);
    assert_eq!(exporter.health.status, "capture_initializing");
    assert_eq!(exporter.health.failures, 0);
    assert_eq!(exporter.health.capture_deferrals, 1);
    assert!(exporter.last_warning_at.is_none());
    status.state = CloudCaptureState::ReconciliationRequired;
    let error = ExportError::CapturePaused(Box::new(status));
    assert!(error.to_string().contains("capture_state=reconciling"));
    assert!(error.to_string().contains("prepared page retained"));
    for n in 1..100 {
        assert!(health::retry_delay(&error, n) <= Duration::from_secs(5));
    }
    exporter.record_result(Err(error), now + Duration::from_secs(1));
    assert_eq!(exporter.health.status, "reconciling");
    assert_eq!(exporter.health.failures, 0);
    assert_eq!(exporter.last_warning_at, Some(now + Duration::from_secs(1)));
    exporter.record_result(Ok(Some(29)), now + Duration::from_secs(2));
    assert_eq!(exporter.health.last_exported_source_sequence, Some(29));
    assert!(exporter.health.last_error.is_none());
}

#[tokio::test]
async fn admitted_page_ack_waits_for_analytics_without_republishing_or_detaching() {
    let mut attempts = 0;
    let acknowledged = acknowledge_when_available(|| {
        attempts += 1;
        match attempts {
            1 => Err(BrokerCommandError::AnalyticsBusy),
            2 => Err(BrokerCommandError::AnalyticsQueueFull),
            _ => Ok(std::future::ready(Ok(42))),
        }
    })
    .await
    .unwrap();
    assert_eq!(acknowledged, 42);
    assert_eq!(attempts, 3);
    let mut refused = 0;
    let result = tokio::time::timeout(
        Duration::from_millis(5),
        acknowledge_when_available(|| {
            refused += 1;
            Err::<std::future::Ready<Result<(), BrokerCommandError>>, _>(
                BrokerCommandError::AnalyticsBusy,
            )
        }),
    )
    .await;
    assert!(result.is_err());
    let after_cancel = refused;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        refused, after_cancel,
        "cancellation owns the retry; no detached task remains"
    );
}

#[test]
fn analytics_busy_queue_pressure_and_failures_have_distinct_bounded_retries() {
    assert_eq!(
        ExportError::from(BrokerCommandError::AnalyticsBusy),
        ExportError::Busy
    );
    assert_eq!(
        ExportError::from(BrokerCommandError::AnalyticsQueueFull),
        ExportError::QueueFull
    );
    assert!(matches!(
        ExportError::from(BrokerCommandError::QueueFull),
        ExportError::Failure(_)
    ));
    for attempt in 1..1000 {
        assert!(health::retry_delay(&ExportError::Busy, attempt) <= Duration::from_secs(2));
        assert!(health::retry_delay(&ExportError::QueueFull, attempt) <= Duration::from_secs(5));
        assert!(
            health::retry_delay(&ExportError::Failure("failure".into()), attempt)
                <= Duration::from_secs(30)
        );
    }
    let mut exporter = enabled_exporter();
    let now = Instant::now();
    exporter.record_result(Err(ExportError::Busy), now);
    assert_eq!(exporter.health.status, "busy");
    assert_eq!(
        exporter.next_attempt.duration_since(now),
        Duration::from_millis(100)
    );
    assert_eq!(exporter.health.busy_deferrals, 1);
    assert_eq!(exporter.health.queue_deferrals, 0);
    assert!(
        exporter.last_warning_at.is_none(),
        "ordinary contention is not a warning"
    );
    assert!(
        exporter
            .health
            .backlog
            .pending_records_at_last_sample
            .is_none(),
        "a refused read does not prove a backlog"
    );
    exporter.record_result(Err(ExportError::QueueFull), now);
    assert_eq!(exporter.health.status, "queue_full");
    assert_eq!(exporter.health.queue_deferrals, 1);
    assert_eq!(exporter.last_warning_at, Some(now));
    exporter.record_result(Ok(None), now + Duration::from_secs(1));
    exporter.record_result(
        Err(ExportError::Failure("journal read failed".into())),
        now + Duration::from_secs(2),
    );
    assert_eq!(
        exporter.last_warning_at,
        Some(now),
        "success does not reset the warning limiter"
    );
    exporter.record_result(
        Err(ExportError::Failure("still failed".into())),
        now + Duration::from_secs(61),
    );
    assert_eq!(
        exporter.last_warning_at,
        Some(now + Duration::from_secs(61))
    );
    assert_eq!(exporter.health.failures, 2);
}

#[test]
fn pending_evidence_age_is_sampled_and_cleared_only_after_acknowledgement() {
    let mut backlog = health::ExportBacklogHealth::default();
    assert!(backlog.pending_records_at_last_sample.is_none());
    backlog.page(2_000_000, 10, 20, 3, Some(1_000_000));
    let health = backlog.with_age(3_000_000);
    assert_eq!(health.pending_records_at_last_sample, Some(10));
    assert_eq!(health.oldest_pending_page_age_ms, Some(2000));
    assert_eq!(health.sampled_at_us, Some(2_000_000));
    backlog.acknowledged();
    let health = backlog.with_age(4_000_000);
    assert_eq!(health.pending_records_at_last_sample, Some(7));
    assert_eq!(health.pending_page_records, 0);
    assert!(
        health.oldest_pending_page_age_ms.is_none(),
        "the acknowledged page is no longer pending; the next page's age is unknown"
    );
    backlog.empty(5_000_000);
    assert_eq!(backlog.pending_records_at_last_sample, Some(0));
}

#[tokio::test]
async fn fair_export_scheduler_runs_one_page_at_a_time_and_rotates_ready_streams() {
    let fixture = Fixture::new();
    let worker = worker(fixture.journal().await);
    let publisher = LocalEventPublisher::spawn(
        Arc::new(InProcessEventBus::default()),
        Some(
            OutboxStore::open(fixture.0.join("outbox.db"))
                .await
                .unwrap(),
        ),
        Default::default(),
    );
    let mut schedule = ExecutionExportScheduler::default();
    let mut state = enabled_exporter();
    let mut fills = ExecutionEvidenceExporter::new_fills();
    let mut marks = ExecutionEvidenceExporter::new_marks();
    fills.health.enabled = true;
    marks.health.enabled = true;
    for expected in ["state", "fills", "marks", "state", "fills", "marks"] {
        for stream in [&mut state, &mut fills, &mut marks] {
            stream.next_attempt = Instant::now();
        }
        schedule.poll(
            [&mut state, &mut fills, &mut marks],
            Some(worker.sender.clone()),
            &publisher,
            identity(),
        );
        let active = [&state, &fills, &marks]
            .into_iter()
            .filter(|s| s.has_inflight())
            .collect::<Vec<_>>();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].health.stream, expected);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                for stream in [&mut state, &mut fills, &mut marks] {
                    stream.poll_completion();
                }
                if [&state, &fills, &marks].iter().all(|s| !s.has_inflight()) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            [&state, &fills, &marks]
                .iter()
                .all(|s| s.health.last_error.is_none())
        );
    }
    state.health.enabled = false;
    for stream in [&mut state, &mut fills, &mut marks] {
        stream.next_attempt = Instant::now();
    }
    schedule.poll(
        [&mut state, &mut fills, &mut marks],
        Some(worker.sender.clone()),
        &publisher,
        identity(),
    );
    assert!(
        !state.has_inflight(),
        "the disabled duplicate-state archive stays off"
    );
    assert!(fills.has_inflight());
    fills
        .finish(
            &worker.sender,
            &publisher,
            &identity(),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert!(
        [&state, &fills, &marks]
            .iter()
            .all(|s| s.health.busy_deferrals == 0)
    );
    assert!(!publisher.admission_failed());
    drop(stop_worker(worker).await);
    publisher.shutdown(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn active_export_blocks_other_export_starts_and_drop_cancels_it() {
    let mut schedule = ExecutionExportScheduler::default();
    let mut state = enabled_exporter();
    let mut fills = ExecutionEvidenceExporter::new_fills();
    let mut marks = ExecutionEvidenceExporter::new_marks();
    fills.health.enabled = true;
    marks.health.enabled = true;
    let (reply, receiver) = oneshot::channel();
    let task = tokio::spawn(async move {
        std::future::pending::<()>().await;
        let _ = reply.send(Ok(None));
    });
    let abort = task.abort_handle();
    marks.active = Some((task, receiver));
    let publisher = LocalEventPublisher::spawn(
        Arc::new(InProcessEventBus::default()),
        None,
        Default::default(),
    );
    schedule.poll(
        [&mut state, &mut fills, &mut marks],
        None,
        &publisher,
        identity(),
    );
    assert!(!state.has_inflight() && !fills.has_inflight() && marks.has_inflight());
    drop(marks);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !abort.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    publisher.shutdown(Duration::from_secs(1)).await;
}
