use super::*;
use crate::core::{
    broker::{
        command_journal::{BrokerCommandJournal, ExecutionFillBatch},
        commands::BrokerCommandError,
    },
    live_sync::{MIN_OPERATIONAL_HEADROOM_BYTES, PhysicalCaptureLimit},
};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("aqe-journal-gap-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        Self(root)
    }
    fn outbox(&self) -> PathBuf {
        self.0.join("publisher.db")
    }
    fn journal(&self) -> PathBuf {
        self.0.join("journal.db")
    }
    fn owner(&self, runtime: &str, run: &str) -> CaptureOwnerLease {
        CaptureOwnerLease::acquire(&self.outbox(), runtime, "strategy", "session", run).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn pause() -> AnalyticsCapturePause {
    AnalyticsCapturePause {
        version: 1,
        journal_source_id: "source".into(),
        strategy_id: "strategy".into(),
        account_id: "account".into(),
        generation: 1,
        execution_epoch: 1,
        started_at_us: 1_000_000,
        last_blocked_at_us: 2_000_000,
    }
}
fn fill(id: &str) -> aqe_live_protocol::ExecutionFillInput {
    serde_json::from_value(serde_json::json!({"insight_id":"insight","fill":{
        "version":1,"provider":"fixture","fill_id":id,"order_id":"order","position_id":"position","symbol":"ANY",
        "side":"buy","effect":"entry","quantity":1.0,"price":100.0,"occurred_at_us":100,"as_of_us":200,
        "cash":{"currency":"GBP","gross_pnl":0.0,"commission":1.0,"carry":0.0},"account_notional":null
    }})).unwrap()
}
fn event() -> CloudEvent {
    let mut e = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "insight",
        1,
        EventPriority::Critical,
        EventKind::InsightProjection,
        serde_json::json!({"data":"x".repeat(1024)}),
    );
    e.run_id = Some("run".into());
    e.critical_sequence = Some(1);
    e
}
async fn count(store: &OutboxStore, table: &str) -> i64 {
    store
        .connection
        .query(&format!("SELECT count(*) FROM {table}"), ())
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap()
        .get(0)
        .unwrap()
}

fn disk_snapshot(path: &std::path::Path) -> Vec<Vec<u8>> {
    [
        path.to_path_buf(),
        std::path::PathBuf::from(format!("{}-wal", path.display())),
    ]
    .into_iter()
    .map(|path| std::fs::read(path).unwrap_or_default())
    .collect()
}

#[tokio::test]
async fn real_source_pressure_handoff_survives_restart_without_resuming_or_acknowledging_data() {
    let fixture = Fixture::new();
    let mut journal = BrokerCommandJournal::open_with_state_history(
        fixture.journal(),
        "strategy",
        "account",
        false,
    )
    .await
    .unwrap();
    journal
        .record_execution_fills(&ExecutionFillBatch::new(vec![fill("accepted")]).unwrap())
        .await
        .unwrap();
    let original = journal.execution_fill_page(0, None, 128).await.unwrap();
    let budget = PhysicalCaptureLimit {
        physical_limit_bytes: MIN_OPERATIONAL_HEADROOM_BYTES + 1,
        operational_headroom_bytes: MIN_OPERATIONAL_HEADROOM_BYTES,
    };
    let mut journal = journal.with_analytics_capture_budget(budget).await.unwrap();
    assert!(matches!(
        journal
            .record_execution_fills(&ExecutionFillBatch::new(vec![fill("refused")]).unwrap())
            .await,
        Err(BrokerCommandError::CapturePaused)
    ));
    let source = journal.analytics_capture_pause().await.unwrap().unwrap();
    let store = OutboxStore::open(fixture.outbox()).await.unwrap();
    store.handoff_accepted_events(&[event()]).await.unwrap();
    let cursor = store
        .critical_sequence_cursor_for_runtime_scope("runtime", "strategy", "session")
        .await
        .unwrap();
    let owner = fixture.owner("runtime", "run");
    let receipt = store
        .handoff_analytics_capture_pause(&owner, &source, 100_000)
        .await
        .unwrap()
        .unwrap();
    assert!(receipt.covers(&source));
    let target = store.latest_capture_pause().await.unwrap().unwrap();
    assert_eq!(target.origin_runtime_id, None);
    assert_eq!(target.origin_run_id, None);
    assert_eq!(target.last_blocked_run_id, None);
    assert_eq!(target.owner_run_id.as_deref(), Some("run"));
    assert!(
        journal
            .acknowledge_analytics_capture_handoff(&receipt)
            .await
            .unwrap()
    );
    assert_eq!(
        journal.analytics_capture_pause().await.unwrap(),
        Some(source.clone())
    );
    assert_eq!(
        journal
            .execution_fill_page(0, None, 128)
            .await
            .unwrap()
            .records,
        original.records
    );
    assert_eq!(store.pending_count().await.unwrap(), 1);
    assert_eq!(
        store
            .critical_sequence_cursor_for_runtime_scope("runtime", "strategy", "session")
            .await
            .unwrap(),
        cursor
    );
    assert_eq!(count(&store, "capture_journal_gap_handoffs").await, 1);
    // Repeated handoff/receipt writes are exact idempotent diagnostic retries.
    let target_disk = disk_snapshot(&fixture.outbox());
    let source_disk = disk_snapshot(&fixture.journal());
    for _ in 0..3 {
        assert_eq!(
            store
                .handoff_analytics_capture_pause(&owner, &source, 100_000)
                .await
                .unwrap(),
            Some(receipt.clone())
        );
        assert!(
            journal
                .acknowledge_analytics_capture_handoff(&receipt)
                .await
                .unwrap()
        );
    }
    assert_eq!(
        disk_snapshot(&fixture.outbox()),
        target_disk,
        "same-owner exact retries append no WAL frames"
    );
    assert_eq!(
        disk_snapshot(&fixture.journal()),
        source_disk,
        "same-receipt retries append no WAL frames"
    );
    assert_eq!(store.latest_capture_pause().await.unwrap(), Some(target));
    drop(owner);
    drop(store);
    drop(journal);
    let mut journal = BrokerCommandJournal::open_with_state_history(
        fixture.journal(),
        "strategy",
        "account",
        false,
    )
    .await
    .unwrap()
    .with_analytics_capture_budget(budget)
    .await
    .unwrap();
    assert_eq!(
        journal.analytics_capture_handoff_receipt().await.unwrap(),
        Some(receipt.clone())
    );
    assert_eq!(
        journal.analytics_capture_pause().await.unwrap(),
        Some(source.clone())
    );
    let store = OutboxStore::open(fixture.outbox()).await.unwrap();
    let owner = fixture.owner("runtime", "run");
    assert_eq!(
        store
            .handoff_analytics_capture_pause(&owner, &source, 100_000)
            .await
            .unwrap(),
        Some(receipt)
    );
    journal.request_execution_stop().await.unwrap();
    assert!(journal.complete_execution_stop().await.unwrap());
    assert_eq!(store.pending_count().await.unwrap(), 1);
}

#[tokio::test]
async fn stale_or_reused_journal_generations_are_rejected_atomically() {
    let fixture = Fixture::new();
    let store = OutboxStore::open(fixture.outbox()).await.unwrap();
    let owner = fixture.owner("runtime", "run");
    let first = pause();
    let receipt = store
        .handoff_analytics_capture_pause(&owner, &first, 100_000)
        .await
        .unwrap()
        .unwrap();
    let old = store.latest_capture_pause().await.unwrap().unwrap();
    for changed in [
        AnalyticsCapturePause {
            started_at_us: first.started_at_us - 1,
            ..first.clone()
        },
        AnalyticsCapturePause {
            last_blocked_at_us: first.last_blocked_at_us + 1,
            ..first.clone()
        },
        AnalyticsCapturePause {
            execution_epoch: 2,
            ..first.clone()
        },
        AnalyticsCapturePause {
            account_id: "another".into(),
            ..first.clone()
        },
        AnalyticsCapturePause {
            strategy_id: "another".into(),
            ..first.clone()
        },
        AnalyticsCapturePause {
            version: 2,
            ..first.clone()
        },
        AnalyticsCapturePause {
            generation: 0,
            ..first.clone()
        },
    ] {
        assert!(
            store
                .handoff_analytics_capture_pause(&owner, &changed, 100_000)
                .await
                .is_err()
        );
        assert_eq!(
            store.latest_capture_pause().await.unwrap(),
            Some(old.clone())
        );
    }
    let next = AnalyticsCapturePause {
        generation: 2,
        execution_epoch: 2,
        started_at_us: 3_000_000,
        last_blocked_at_us: 4_000_000,
        ..first.clone()
    };
    let second = store
        .handoff_analytics_capture_pause(&owner, &next, 100_000)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(receipt.pause_id, second.pause_id);
    assert!(!receipt.covers(&next));
    assert_eq!(second.target_pause_generation, old.generation + 1);
    assert!(
        store
            .handoff_analytics_capture_pause(&owner, &first, 100_000)
            .await
            .is_err()
    );
    let backwards = AnalyticsCapturePause {
        generation: 3,
        execution_epoch: 1,
        ..next.clone()
    };
    assert!(
        store
            .handoff_analytics_capture_pause(&owner, &backwards, 100_000)
            .await
            .is_err()
    );
    assert_eq!(count(&store, "capture_journal_gap_handoffs").await, 1);
    assert_eq!(store.pending_count().await.unwrap(), 0);
}

#[tokio::test]
async fn wrong_file_import_or_strategy_leases_cannot_accept_a_journal_gap() {
    let fixture = Fixture::new();
    let store = OutboxStore::open(fixture.outbox()).await.unwrap();
    let wrong = CaptureOwnerLease::acquire(
        &fixture.0.join("other.db"),
        "runtime",
        "strategy",
        "session",
        "run",
    )
    .unwrap();
    assert!(
        store
            .handoff_analytics_capture_pause(&wrong, &pause(), 100_000)
            .await
            .is_err()
    );
    let import = CaptureOwnerLease::acquire_for_import(&fixture.outbox()).unwrap();
    assert!(
        store
            .handoff_analytics_capture_pause(&import, &pause(), 100_000)
            .await
            .is_err()
    );
    drop(import);
    let wrong = CaptureOwnerLease::acquire(&fixture.outbox(), "runtime", "other", "session", "run")
        .unwrap();
    assert!(
        store
            .handoff_analytics_capture_pause(&wrong, &pause(), 100_000)
            .await
            .is_err()
    );
    assert_eq!(store.latest_capture_pause().await.unwrap(), None);
}

#[tokio::test]
async fn handoff_map_is_bounded_and_exact_retries_fit_at_capacity() {
    let fixture = Fixture::new();
    let store = OutboxStore::open(fixture.outbox()).await.unwrap();
    let owner = fixture.owner("runtime", "run");
    let first = pause();
    let receipt = store
        .handoff_analytics_capture_pause(&owner, &first, 100_000)
        .await
        .unwrap()
        .unwrap();
    // Populate the fixed-size local map, not economic history or accepted work.
    let tx = turso::transaction::Transaction::new_unchecked(
        &store.connection,
        turso::transaction::TransactionBehavior::Immediate,
    )
    .await
    .unwrap();
    for i in 1..MAX_JOURNAL_GAP_SOURCES {
        tx.execute("INSERT INTO capture_journal_gap_handoffs VALUES(?1,1,1,'strategy','account','fixture','fixture')",params![format!("source-{i}")]).await.unwrap();
    }
    tx.commit().await.unwrap();
    let old = store.latest_capture_pause().await.unwrap();
    let another = AnalyticsCapturePause {
        journal_source_id: "new-source".into(),
        ..first.clone()
    };
    assert_eq!(
        store
            .handoff_analytics_capture_pause(&owner, &another, 100_000)
            .await
            .unwrap(),
        None
    );
    assert_eq!(store.latest_capture_pause().await.unwrap(), old);
    assert_eq!(
        store
            .handoff_analytics_capture_pause(&owner, &first, 100_000)
            .await
            .unwrap(),
        Some(receipt)
    );
    assert_eq!(
        count(&store, "capture_journal_gap_handoffs").await,
        MAX_JOURNAL_GAP_SOURCES
    );
}

#[tokio::test]
async fn unknown_journal_origin_stays_unknown_on_restart_adoption_and_new_refusals() {
    let fixture = Fixture::new();
    let store = OutboxStore::open(fixture.outbox()).await.unwrap();
    let owner = fixture.owner("runtime", "run");
    let source = pause();
    let receipt = store
        .handoff_analytics_capture_pause(&owner, &source, 100_000)
        .await
        .unwrap()
        .unwrap();
    drop(owner);
    drop(store);
    let store = OutboxStore::open(fixture.outbox()).await.unwrap();
    let owner = fixture.owner("next-runtime", "next-run");
    let adopted = store
        .adopt_latest_capture_pause(&owner)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(adopted.origin_runtime_id, None);
    assert_eq!(adopted.origin_run_id, None);
    assert_eq!(adopted.last_blocked_run_id, None);
    assert_eq!(adopted.owner_run_id.as_deref(), Some("next-run"));
    assert_eq!(adopted.generation, receipt.target_pause_generation + 1);
    // Receipt proves prior durable diagnostic handoff; it is not a new-run
    // recovery barrier and must not be rewritten to invent original provenance.
    assert_eq!(
        store
            .handoff_analytics_capture_pause(&owner, &source, 100_000)
            .await
            .unwrap(),
        Some(receipt)
    );
    let mut refused = event();
    refused.runtime_id = "next-runtime".into();
    refused.run_id = Some("next-run".into());
    assert!(matches!(
        store.capture_events_with_capacity(&[refused], 1).await,
        Err(OutboxStoreError::CapturePaused(_))
    ));
    let later = store.latest_capture_pause().await.unwrap().unwrap();
    assert_eq!(later.origin_runtime_id, None);
    assert_eq!(later.origin_run_id, None);
    assert_eq!(later.last_blocked_run_id.as_deref(), Some("next-run"));
    assert_eq!(later.started_at_us, source.started_at_us);
}

#[tokio::test]
async fn merging_source_facts_preserves_earliest_and_latest_known_provenance() {
    let fixture = Fixture::new();
    let store = OutboxStore::open(fixture.outbox()).await.unwrap();
    let owner = fixture.owner("runtime", "run");
    assert!(
        store
            .capture_events_with_capacity(&[event()], 1)
            .await
            .is_err()
    );
    let known = store.latest_capture_pause().await.unwrap().unwrap();
    let later = AnalyticsCapturePause {
        started_at_us: known.last_blocked_at_us + 100,
        last_blocked_at_us: known.last_blocked_at_us + 200,
        ..pause()
    };
    store
        .handoff_analytics_capture_pause(&owner, &later, 100_000)
        .await
        .unwrap()
        .unwrap();
    let mixed = store.latest_capture_pause().await.unwrap().unwrap();
    assert_eq!(mixed.origin_runtime_id.as_deref(), Some("runtime"));
    assert_eq!(mixed.origin_run_id.as_deref(), Some("run"));
    assert_eq!(mixed.last_blocked_run_id, None);
    let earlier = AnalyticsCapturePause {
        journal_source_id: "earlier-source".into(),
        ..pause()
    };
    store
        .handoff_analytics_capture_pause(&owner, &earlier, 100_000)
        .await
        .unwrap()
        .unwrap();
    let mixed = store.latest_capture_pause().await.unwrap().unwrap();
    assert_eq!(mixed.started_at_us, earlier.started_at_us);
    assert_eq!(mixed.origin_runtime_id, None);
    assert_eq!(mixed.origin_run_id, None);
    assert_eq!(mixed.last_blocked_at_us, later.last_blocked_at_us);
    assert_eq!(mixed.last_blocked_run_id, None);
}

#[tokio::test]
async fn target_generation_exhaustion_rolls_back_the_entire_handoff() {
    let fixture = Fixture::new();
    let store = OutboxStore::open(fixture.outbox()).await.unwrap();
    let owner = fixture.owner("runtime", "run");
    let source = pause();
    store
        .handoff_analytics_capture_pause(&owner, &source, 100_000)
        .await
        .unwrap()
        .unwrap();
    store
        .connection
        .execute(
            "UPDATE capture_pauses SET generation=9223372036854775807",
            (),
        )
        .await
        .unwrap();
    let old = store.latest_capture_pause().await.unwrap();
    let next = AnalyticsCapturePause {
        generation: 2,
        ..source.clone()
    };
    assert!(
        store
            .handoff_analytics_capture_pause(&owner, &next, 100_000)
            .await
            .is_err()
    );
    assert_eq!(store.latest_capture_pause().await.unwrap(), old);
    assert_eq!(count(&store, "capture_journal_gap_handoffs").await, 1);
    assert!(
        store
            .handoff_analytics_capture_pause(&owner, &source, 100_000)
            .await
            .unwrap()
            .is_some()
    );
}
