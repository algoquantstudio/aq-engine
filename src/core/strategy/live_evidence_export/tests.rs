use super::*;

#[test]
fn marks_default_on_but_fill_and_duplicate_state_delivery_remain_opt_in() {
    assert!(export_enabled(EvidenceStream::Marks, None));
    assert!(!export_enabled(EvidenceStream::Marks, Some("false")));
    assert!(!export_enabled(EvidenceStream::Fills, None));
    assert!(export_enabled(EvidenceStream::Fills, Some("true")));
    assert!(!export_enabled(EvidenceStream::State, None));
}
use crate::core::broker::{
    command_worker::BrokerCommandWorker, paper_broker::PaperBroker, types::AccountType,
};
use crate::core::{
    broker::command_journal::{BrokerCommandJournal, ExecutionCheckpointBatch},
    insight::{Insight, execution_checkpoint::InsightExecutionCheckpoint},
    live_sync::{FlatbufferEventCodec, InProcessEventBus, OutboxStore},
};
use aqe_live_protocol::{ExecutionEvidenceChunk, assemble_execution_evidence};
use std::sync::Arc;

#[tokio::test]
async fn position_marks_handoff_retries_and_finish_preserve_separate_fill_state_cursors() {
    use crate::core::broker::{
        command_journal::{ExecutionFillBatch, marks::PositionMarkBatch},
        fill_history::ExecutionFillHistory,
    };
    use aqe_live_protocol::{POSITION_MARK_MINUTE_US, PositionMarkTransfer, PositionValuations};
    let fixture = Fixture::new();
    let mut journal = BrokerCommandJournal::open_with_state_history(
        fixture.0.join("commands.db"),
        "strategy",
        "account",
        false,
    )
    .await
    .unwrap();
    let mut insight = Insight::default();
    insight.symbol = "GENERIC".into();
    insight.order_id = Some("saved-order".into());
    journal
        .persist_execution_batch(
            &ExecutionCheckpointBatch::new(
                vec![InsightExecutionCheckpoint::capture(&insight, 1, true, None)],
                vec![],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let owner = journal
        .fill_history_owner_page("")
        .await
        .unwrap()
        .owners
        .remove(0);
    let history:ExecutionFillHistory=serde_json::from_value(serde_json::json!({
        "account_id":"account","entry_order_id":"saved-order","position_id":"position","symbol":"GENERIC","as_of_us":200,"clock":{"basis":"utc"},
        "fills":[{"version":1,"provider":"fills","fill_id":"actual","order_id":"saved-order","position_id":"position","symbol":"GENERIC","side":"buy","effect":"entry","quantity":2.,"price":100.,"occurred_at_us":100,"as_of_us":200,"cash":null,"account_notional":null}],
        "position_snapshot":{"position_id":"position","symbol":"GENERIC","quantity":2.,"side":"buy","as_of_us":200}
    })).unwrap();
    journal
        .record_execution_fills(&ExecutionFillBatch::from_history(owner, history).unwrap())
        .await
        .unwrap();
    let slot = chrono::Utc::now()
        .timestamp_micros()
        .div_euclid(POSITION_MARK_MINUTE_US)
        * POSITION_MARK_MINUTE_US;
    let owner = journal
        .position_mark_owners(slot, None)
        .await
        .unwrap()
        .owners
        .remove(0);
    let values:PositionValuations=serde_json::from_value(serde_json::json!({
        "version":1,"provider":"marks","account_id":"account","account_currency":"GBP","symbol":"GENERIC","as_of_us":slot,
        "positions":[{"position_id":"position","status":"open","quantity":2.,"side":"buy","average_entry_price":100.,"current_price":110.,"gross_unrealized_pnl":20.,"carry":-0.5,"quote_time":null,"notional":{"basis":"unavailable","reason":"currency_conversion_unavailable"}}]
    })).unwrap();
    journal
        .record_position_marks(PositionMarkBatch::new(vec![owner], values, slot).unwrap())
        .await
        .unwrap();
    let frozen = journal
        .prepare_position_mark_export()
        .await
        .unwrap()
        .unwrap();
    let outbox = OutboxStore::open(fixture.0.join("outbox.db"))
        .await
        .unwrap();
    let publisher = LocalEventPublisher::spawn(
        Arc::new(InProcessEventBus::acknowledged()),
        Some(outbox.clone()),
        crate::core::live_sync::LocalEventPublisherConfig {
            nats_unavailable_fallback_after: Duration::from_millis(1),
            critical_batch_delay: Duration::from_millis(1),
            ..Default::default()
        },
    );
    let mut wrong = identity();
    wrong.strategy_id = "other".into();
    assert!(
        publish_mark_page(&publisher, &wrong, &frozen)
            .await
            .is_err()
    );
    let receipt = publish_mark_page(&publisher, &identity(), &frozen)
        .await
        .unwrap();
    assert_eq!(
        publish_mark_page(&publisher, &identity(), &frozen)
            .await
            .unwrap(),
        receipt
    );
    // Crash after durable outbox admission but before acknowledging the source.
    drop(journal);
    let journal = BrokerCommandJournal::open_with_state_history(
        fixture.0.join("commands.db"),
        "strategy",
        "account",
        false,
    )
    .await
    .unwrap();
    assert_eq!(
        journal
            .prepare_position_mark_export()
            .await
            .unwrap()
            .unwrap(),
        frozen
    );
    let worker = worker(journal);
    let mut exporter = ExecutionEvidenceExporter::new_marks();
    exporter.health.enabled = true;
    exporter
        .finish(
            &worker.sender,
            &publisher,
            &identity(),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    let journal = stop_worker(worker).await;
    assert!(
        journal
            .prepare_position_mark_export()
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        journal
            .prepare_execution_evidence_export()
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        journal
            .prepare_execution_fill_export()
            .await
            .unwrap()
            .is_some()
    );
    assert!(journal.recovery_page(0, 100).await.unwrap().is_empty());
    let events = outbox
        .pending_batch(EventPriority::Audit, 100, 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    let decoded =
        FlatbufferEventCodec::decode(&FlatbufferEventCodec::encode(&events[0].event).unwrap())
            .unwrap();
    assert_eq!(decoded.kind, EventKind::PositionMarkPage);
    assert!(decoded.critical_sequence.is_none());
    let transfer: PositionMarkTransfer = serde_json::from_value(decoded.payload).unwrap();
    assert_eq!(transfer, frozen);
    publisher.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn exact_fill_export_drains_to_audit_outbox_without_changing_broker_state() {
    use crate::core::broker::command_journal::ExecutionFillBatch;
    use aqe_live_protocol::{
        ExecutionFill, ExecutionFillEffect, ExecutionFillInput, ExecutionFillSide,
        ExecutionFillTransfer,
    };
    let fixture = Fixture::new();
    let mut journal = fixture.journal().await;
    let rows = (0..70)
        .map(|i| ExecutionFillInput {
            insight_id: None,
            fill: ExecutionFill {
                version: 1,
                provider: "test-v1".into(),
                fill_id: i.to_string(),
                order_id: "order".into(),
                position_id: None,
                symbol: "契約-FUTURE".into(),
                side: ExecutionFillSide::Buy,
                effect: ExecutionFillEffect::Entry,
                quantity: 1.,
                price: -10.,
                occurred_at_us: 1,
                as_of_us: 2,
                cash: None,
                account_notional: None,
            },
        })
        .collect::<Vec<_>>();
    journal
        .record_execution_fills(&ExecutionFillBatch::new(rows).unwrap())
        .await
        .unwrap();
    let first = journal
        .prepare_execution_fill_export()
        .await
        .unwrap()
        .unwrap();
    let outbox = OutboxStore::open(fixture.0.join("outbox.db"))
        .await
        .unwrap();
    let publisher = LocalEventPublisher::spawn(
        Arc::new(InProcessEventBus::acknowledged()),
        Some(outbox.clone()),
        crate::core::live_sync::LocalEventPublisherConfig {
            nats_unavailable_fallback_after: Duration::from_millis(1),
            critical_batch_delay: Duration::from_millis(1),
            ..Default::default()
        },
    );
    let mut wrong = identity();
    wrong.strategy_id = "other".into();
    assert!(publish_fill_page(&publisher, &wrong, &first).await.is_err());
    let receipt = publish_fill_page(&publisher, &identity(), &first)
        .await
        .unwrap();
    assert_eq!(
        publish_fill_page(&publisher, &identity(), &first)
            .await
            .unwrap(),
        receipt
    );
    // Simulate a crash between durable admission and the source acknowledgement.
    drop(journal);
    let reopened = fixture.journal().await;
    assert_eq!(
        reopened
            .prepare_execution_fill_export()
            .await
            .unwrap()
            .unwrap(),
        first
    );
    let worker = worker(reopened);
    let mut exporter = ExecutionEvidenceExporter::new_fills();
    exporter.health.enabled = true;
    exporter
        .finish(
            &worker.sender,
            &publisher,
            &identity(),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    let journal = stop_worker(worker).await;
    assert!(
        journal
            .prepare_execution_fill_export()
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        journal
            .prepare_execution_evidence_export()
            .await
            .unwrap()
            .is_none()
    );
    assert!(journal.recovery_page(0, 100).await.unwrap().is_empty());
    let events = outbox
        .pending_batch(EventPriority::Audit, 100, 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(events.len(), 3);
    let mut count = 0;
    for row in events {
        assert_eq!(row.event.kind, EventKind::ExecutionFillPage);
        assert!(row.event.critical_sequence.is_none());
        let bytes = FlatbufferEventCodec::encode(&row.event).unwrap();
        let decoded = FlatbufferEventCodec::decode(&bytes).unwrap();
        assert_eq!(decoded.kind, EventKind::ExecutionFillPage);
        let transfer: ExecutionFillTransfer = serde_json::from_value(decoded.payload).unwrap();
        let page = transfer.page().unwrap();
        assert_eq!(page.after_sequence, count);
        count += page.records.len() as u64;
        assert!(page.recovery_complete.is_none());
        assert!(
            page.records
                .iter()
                .all(|record| record.observation.insight_id.is_none()
                    && record.observation.fill.cash.is_none())
        );
    }
    assert_eq!(count, 70);
    publisher.shutdown(Duration::from_secs(2)).await;
}

struct Fixture(std::path::PathBuf);
impl Fixture {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("aqe-evidence-handoff-{}", uuid::Uuid::new_v4())))
    }
    async fn journal(&self) -> BrokerCommandJournal {
        BrokerCommandJournal::open_with_state_history(
            self.0.join("commands.db"),
            "strategy",
            "account",
            true,
        )
        .await
        .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn identity() -> ExportIdentity {
    ExportIdentity {
        runtime_id: "runtime".into(),
        strategy_id: "strategy".into(),
        session_id: "session".into(),
        run_id: "run".into(),
    }
}
async fn save(journal: &mut BrokerCommandJournal, id: uuid::Uuid, revision: u64, price: f64) {
    let mut insight = Insight::default();
    insight.insight_id = id;
    insight.symbol = "CUSTOM-契約".repeat(9000);
    insight.limit_price = Some(price);
    journal
        .persist_execution_batch(
            &ExecutionCheckpointBatch::new(
                vec![InsightExecutionCheckpoint::capture(
                    &insight, revision, true, None,
                )],
                vec![],
            )
            .unwrap(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn frozen_evidence_reaches_durable_audit_outbox_and_retries_do_not_duplicate() {
    let fixture = Fixture::new();
    let mut journal = fixture.journal().await;
    let id = uuid::Uuid::new_v4();
    save(&mut journal, id, 1, 10.0).await;
    let page = journal
        .prepare_execution_evidence_export()
        .await
        .unwrap()
        .unwrap();
    let outbox = OutboxStore::open(fixture.0.join("outbox.db"))
        .await
        .unwrap();
    let publisher = LocalEventPublisher::spawn(
        // Keep audit rows pending until a durable consumer acknowledges them.
        // The default ephemeral bus immediately acknowledges and retires rows.
        Arc::new(InProcessEventBus::acknowledged()),
        Some(outbox.clone()),
        Default::default(),
    );
    let receipt = publish_page(&publisher, &identity(), &page).await.unwrap();
    let again = publish_page(&publisher, &identity(), &page).await.unwrap();
    assert_eq!(receipt, again);
    assert!(!publisher.admission_failed());
    let events = outbox
        .pending_batch(EventPriority::Audit, 100, 2 * 1024 * 1024)
        .await
        .unwrap();
    assert!(events.len() > 1);
    assert!(
        events
            .iter()
            .all(|row| row.event.kind == EventKind::ExecutionEvidenceChunk
                && row.event.critical_sequence.is_none())
    );
    let chunks = events
        .iter()
        .map(|row| {
            let bytes = FlatbufferEventCodec::encode(&row.event).unwrap();
            assert!(bytes.len() < crate::core::live_sync::LOCAL_EVENT_HARD_LIMIT_BYTES);
            let decoded = FlatbufferEventCodec::decode(&bytes).unwrap();
            assert_eq!(decoded.kind, EventKind::ExecutionEvidenceChunk);
            serde_json::from_value::<ExecutionEvidenceChunk>(decoded.payload).unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(assemble_execution_evidence(&chunks).unwrap(), page);
    // Source cursor has not moved just because publication succeeded.
    assert_eq!(
        journal
            .prepare_execution_evidence_export()
            .await
            .unwrap()
            .unwrap(),
        page
    );
    journal
        .acknowledge_execution_evidence_export(&receipt)
        .await
        .unwrap();
    assert!(
        journal
            .prepare_execution_evidence_export()
            .await
            .unwrap()
            .is_none()
    );
    save(&mut journal, id, 2, 11.0).await;
    let correction = journal
        .prepare_execution_evidence_export()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(correction.after_sequence, receipt.next_sequence);
    assert_eq!(
        correction.observations[0].record.state.limit_price,
        Some(11.0)
    );
    publisher.shutdown(Duration::from_secs(2)).await;
    drop(publisher);
    drop(outbox);
    drop(journal);
    let reopened = fixture.journal().await;
    assert_eq!(
        reopened
            .prepare_execution_evidence_export()
            .await
            .unwrap()
            .unwrap(),
        correction
    );
}

#[tokio::test]
async fn failed_or_wrong_scope_handoff_keeps_frozen_source_page() {
    let fixture = Fixture::new();
    let mut journal = fixture.journal().await;
    save(&mut journal, uuid::Uuid::new_v4(), 1, 10.0).await;
    let page = journal
        .prepare_execution_evidence_export()
        .await
        .unwrap()
        .unwrap();
    let publisher = LocalEventPublisher::spawn(
        Arc::new(InProcessEventBus::default()),
        None,
        Default::default(),
    );
    assert!(publish_page(&publisher, &identity(), &page).await.is_err());
    let mut wrong = identity();
    wrong.strategy_id = "another-strategy".into();
    assert!(
        publish_page(&publisher, &wrong, &page)
            .await
            .unwrap_err()
            .contains("scope")
    );
    assert_eq!(
        journal
            .prepare_execution_evidence_export()
            .await
            .unwrap()
            .unwrap(),
        page
    );
    assert!(
        !publisher.admission_failed(),
        "Analytics deferral must not stop trading"
    );
    publisher.shutdown(Duration::from_secs(2)).await;
}

fn enabled_exporter() -> ExecutionEvidenceExporter {
    let mut exporter = ExecutionEvidenceExporter::new();
    // Never mutate a process-wide environment variable in concurrent tests.
    exporter.health.enabled = true;
    exporter
}

fn worker(journal: BrokerCommandJournal) -> BrokerCommandWorker {
    BrokerCommandWorker::start(
        Arc::new(PaperBroker::new(AccountType::Paper, 100_000.0, 1)),
        journal,
        Default::default(),
    )
    .unwrap()
}

async fn stop_worker(mut worker: BrokerCommandWorker) -> BrokerCommandJournal {
    worker.events.close();
    worker.task.await.unwrap().unwrap()
}

#[tokio::test]
async fn final_drain_joins_active_export_and_delivers_every_remaining_page() {
    let fixture = Fixture::new();
    let mut journal = fixture.journal().await;
    let id = uuid::Uuid::new_v4();
    for revision in 1..=12 {
        save(&mut journal, id, revision, 10.0 + revision as f64).await;
    }
    let first = journal
        .prepare_execution_evidence_export()
        .await
        .unwrap()
        .unwrap();
    assert!(first.has_more, "The fixture spans multiple bounded pages");
    let publisher = LocalEventPublisher::spawn(
        Arc::new(InProcessEventBus::acknowledged()),
        Some(
            OutboxStore::open(fixture.0.join("outbox.db"))
                .await
                .unwrap(),
        ),
        crate::core::live_sync::LocalEventPublisherConfig {
            nats_unavailable_fallback_after: Duration::from_millis(1),
            critical_batch_delay: Duration::from_millis(1),
            ..Default::default()
        },
    );
    let worker = worker(journal);
    let mut exporter = enabled_exporter();
    exporter.poll(Some(worker.sender.clone()), &publisher, identity());
    assert!(exporter.active.is_some());
    let sequence = exporter
        .finish(
            &worker.sender,
            &publisher,
            &identity(),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    assert_eq!(sequence, Some(first.through_sequence));
    assert!(exporter.active.is_none());
    assert!(exporter.health.last_error.is_none());
    let journal = stop_worker(worker).await;
    assert!(
        journal
            .prepare_execution_evidence_export()
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(journal.scope(), ("strategy", "account"));
    assert!(
        !publisher.admission_failed(),
        "Read-only draining never fences trading"
    );
    publisher.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn disabled_or_unavailable_final_drain_retains_the_journal_page() {
    let fixture = Fixture::new();
    let mut journal = fixture.journal().await;
    save(&mut journal, uuid::Uuid::new_v4(), 1, 10.0).await;
    let page = journal
        .prepare_execution_evidence_export()
        .await
        .unwrap()
        .unwrap();
    let worker = worker(journal);
    let publisher = LocalEventPublisher::spawn(
        Arc::new(InProcessEventBus::default()),
        None,
        Default::default(),
    );
    let mut exporter = enabled_exporter();
    exporter.health.enabled = false;
    assert_eq!(
        exporter
            .finish(&worker.sender, &publisher, &identity(), Duration::ZERO)
            .await
            .unwrap(),
        None
    );
    assert_eq!(exporter.health.last_attempt_at_us, None);
    exporter.health.enabled = true;
    let error = exporter
        .finish(
            &worker.sender,
            &publisher,
            &identity(),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
    assert!(error.contains("durable local outbox"));
    assert_eq!(exporter.health.last_error.as_ref(), Some(&error));
    assert!(!publisher.admission_failed());
    let journal = stop_worker(worker).await;
    assert_eq!(
        journal.prepare_execution_evidence_export().await.unwrap(),
        Some(page)
    );
    publisher.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn final_drain_budget_cancels_and_joins_polling_task_without_losing_evidence() {
    let fixture = Fixture::new();
    let mut journal = fixture.journal().await;
    save(&mut journal, uuid::Uuid::new_v4(), 1, 10.0).await;
    let page = journal
        .prepare_execution_evidence_export()
        .await
        .unwrap()
        .unwrap();
    let worker = worker(journal);
    let publisher = LocalEventPublisher::spawn(
        Arc::new(InProcessEventBus::acknowledged()),
        Some(
            OutboxStore::open(fixture.0.join("outbox.db"))
                .await
                .unwrap(),
        ),
        Default::default(),
    );
    let mut exporter = enabled_exporter();
    let (reply, receiver) = oneshot::channel();
    let (started, start) = oneshot::channel();
    let task = tokio::spawn(async move {
        let _ = started.send(());
        std::future::pending::<()>().await;
        let _ = reply.send(Ok(None));
    });
    let aborted = task.abort_handle();
    exporter.active = Some((task, receiver));
    start.await.unwrap();
    let error = exporter
        .finish(
            &worker.sender,
            &publisher,
            &identity(),
            Duration::from_millis(1),
        )
        .await
        .unwrap_err();
    assert!(error.contains("budget expired"));
    assert!(
        aborted.is_finished(),
        "Timeout joins cancellation, never detaches a task"
    );
    assert!(exporter.active.is_none());
    assert_eq!(exporter.health.last_exported_source_sequence, None);
    let journal = stop_worker(worker).await;
    assert_eq!(
        journal.prepare_execution_evidence_export().await.unwrap(),
        Some(page)
    );
    publisher.shutdown(Duration::from_secs(2)).await;
}
