use super::*;
mod scheduling;

#[test]
fn marks_default_on_and_fill_delivery_remains_opt_in() {
    assert!(export_enabled(AnalyticsStream::Marks, None));
    assert!(!export_enabled(AnalyticsStream::Marks, Some("false")));
    assert!(!export_enabled(AnalyticsStream::Fills, None));
    assert!(export_enabled(AnalyticsStream::Fills, Some("true")));
}
use crate::core::broker::{
    command_worker::BrokerCommandWorker, paper_broker::PaperBroker, types::AccountType,
};
use crate::core::{
    broker::command_journal::{BrokerCommandJournal, ExecutionCheckpointBatch},
    insight::{Insight, execution_checkpoint::InsightExecutionCheckpoint},
    live_sync::{FlatbufferEventCodec, InProcessEventBus, OutboxStore},
};
use std::sync::Arc;

#[tokio::test]
async fn position_marks_handoff_retries_and_finish_preserve_separate_fill_cursor() {
    use crate::core::broker::{
        command_journal::{ExecutionFillBatch, marks::PositionMarkBatch},
        fill_history::ExecutionFillHistory,
    };
    use aqe_live_protocol::{POSITION_MARK_MINUTE_US, PositionMarkTransfer, PositionValuations};
    let fixture = Fixture::new();
    let mut journal = BrokerCommandJournal::open(
        fixture.0.join("commands.db"),
        "strategy",
        "account")
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
    let journal = BrokerCommandJournal::open(
        fixture.0.join("commands.db"),
        "strategy",
        "account")
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
    let mut exporter = ExecutionAnalyticsExporter::new_marks();
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
    let mut exporter = ExecutionAnalyticsExporter::new_fills();
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
        BrokerCommandJournal::open(
            self.0.join("commands.db"),
            "strategy",
            "account")
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
fn enabled_exporter() -> ExecutionAnalyticsExporter {
    let mut exporter = ExecutionAnalyticsExporter::new_fills();
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
