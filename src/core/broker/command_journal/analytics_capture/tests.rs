use super::*;
use crate::core::broker::command_journal::ExecutionFillBatch;
use crate::core::live_sync::MIN_OPERATIONAL_HEADROOM_BYTES;

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("aqe-source-capture-{}", uuid::Uuid::new_v4())))
    }
    async fn open(&self) -> BrokerCommandJournal {
        BrokerCommandJournal::open_with_state_history(
            self.0.join("journal.db"),
            "strategy",
            "account",
            false,
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
fn budget() -> PhysicalCaptureLimit {
    PhysicalCaptureLimit {
        physical_limit_bytes: 64 * 1024 * 1024,
        operational_headroom_bytes: MIN_OPERATIONAL_HEADROOM_BYTES,
    }
}
fn fill(id: &str) -> aqe_live_protocol::ExecutionFillInput {
    serde_json::from_value(serde_json::json!({"insight_id":"insight","fill":{
        "version":1,"provider":"fixture","fill_id":id,"order_id":"order","position_id":"position","symbol":"ANY",
        "side":"buy","effect":"entry","quantity":1.0,"price":100.0,"occurred_at_us":100,"as_of_us":200,
        "cash":{"currency":"GBP","gross_pnl":0.0,"commission":1.0,"carry":0.0},"account_notional":null
    }})).unwrap()
}
async fn count(journal: &BrokerCommandJournal, sql: &str) -> i64 {
    journal
        .connection
        .query(sql, ())
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap()
        .get(0)
        .unwrap()
}
async fn tighten(journal: &mut BrokerCommandJournal) {
    let mut rows = journal
        .connection
        .query("PRAGMA wal_checkpoint(TRUNCATE)", ())
        .await
        .unwrap();
    while rows.next().await.unwrap().is_some() {}
    drop(rows);
    let usage = journal.analytics_capture_usage().await.unwrap().unwrap();
    journal.analytics_capture_budget = Some(PhysicalCaptureLimit {
        physical_limit_bytes: MIN_OPERATIONAL_HEADROOM_BYTES + usage.projected_commit_bytes - 1,
        ..budget()
    });
}

fn handoff_receipt(pause: &AnalyticsCapturePause) -> AnalyticsCaptureHandoffReceipt {
    AnalyticsCaptureHandoffReceipt {
        version: 1,
        pause_id: pause.stable_id(),
        journal_source_id: pause.journal_source_id.clone(),
        source_generation: pause.generation,
        strategy_id: pause.strategy_id.clone(),
        account_id: pause.account_id.clone(),
        target_runtime_id: "runtime".into(),
        target_live_session_id: "session".into(),
        target_run_id: "run".into(),
        target_pause_generation: 1,
    }
}

#[tokio::test]
async fn managed_capture_completion_requires_the_exact_stored_diagnostic_receipt() {
    let fixture = Fixture::new();
    let mut journal = fixture
        .open()
        .await
        .with_analytics_capture_budget(budget())
        .await
        .unwrap();
    journal
        .record_execution_fills(
            &ExecutionFillBatch::new(vec![fill("accepted-before-recovery")]).unwrap(),
        )
        .await
        .unwrap();
    let accepted = journal.execution_fill_page(0, None, 128).await.unwrap();
    tighten(&mut journal).await;
    assert!(matches!(
        journal
            .record_execution_fills(&ExecutionFillBatch::new(vec![fill("refused")]).unwrap())
            .await,
        Err(BrokerCommandError::CapturePaused)
    ));
    let pause = journal.analytics_capture_pause().await.unwrap().unwrap();
    let receipt = handoff_receipt(&pause);
    let journal = journal
        .with_analytics_capture_budget(budget())
        .await
        .unwrap();
    assert!(
        !journal
            .complete_analytics_capture_with_handoff(&receipt)
            .await
            .unwrap(),
        "a constructed receipt is not durable source acknowledgement"
    );
    assert!(
        journal
            .acknowledge_analytics_capture_handoff(&receipt)
            .await
            .unwrap()
    );
    for changed in [
        AnalyticsCaptureHandoffReceipt {
            target_run_id: "wrong-target".into(),
            ..receipt.clone()
        },
        AnalyticsCaptureHandoffReceipt {
            journal_source_id: "foreign-source".into(),
            ..receipt.clone()
        },
        AnalyticsCaptureHandoffReceipt {
            account_id: "foreign-account".into(),
            ..receipt.clone()
        },
        AnalyticsCaptureHandoffReceipt {
            pause_id: uuid::Uuid::new_v4(),
            ..receipt.clone()
        },
        AnalyticsCaptureHandoffReceipt {
            target_pause_generation: receipt.target_pause_generation + 1,
            ..receipt.clone()
        },
    ] {
        assert!(
            !journal
                .complete_analytics_capture_with_handoff(&changed)
                .await
                .unwrap()
        );
    }
    assert_eq!(
        journal.analytics_capture_pause().await.unwrap(),
        Some(pause)
    );
    assert!(
        journal
            .complete_analytics_capture_with_handoff(&receipt)
            .await
            .unwrap()
    );
    assert!(
        !journal
            .complete_analytics_capture_with_handoff(&receipt)
            .await
            .unwrap()
    );
    assert!(journal.analytics_capture_pause().await.unwrap().is_none());
    assert_eq!(
        journal
            .execution_fill_page(0, None, 128)
            .await
            .unwrap()
            .records,
        accepted.records
    );
}

#[tokio::test]
async fn an_exact_receipt_cannot_override_current_physical_pressure() {
    let fixture = Fixture::new();
    let mut journal = fixture
        .open()
        .await
        .with_analytics_capture_budget(budget())
        .await
        .unwrap();
    tighten(&mut journal).await;
    assert!(matches!(
        journal
            .record_execution_fills(&ExecutionFillBatch::new(vec![fill("refused")]).unwrap())
            .await,
        Err(BrokerCommandError::CapturePaused)
    ));
    let pause = journal.analytics_capture_pause().await.unwrap().unwrap();
    let receipt = handoff_receipt(&pause);
    journal
        .acknowledge_analytics_capture_handoff(&receipt)
        .await
        .unwrap();
    assert!(
        !journal
            .complete_analytics_capture_with_handoff(&receipt)
            .await
            .unwrap()
    );
    assert_eq!(
        journal.analytics_capture_pause().await.unwrap(),
        Some(pause)
    );
}

#[tokio::test]
async fn stored_receipt_cannot_clear_changed_facts_under_the_same_generation() {
    let fixture = Fixture::new();
    let mut journal = fixture
        .open()
        .await
        .with_analytics_capture_budget(budget())
        .await
        .unwrap();
    tighten(&mut journal).await;
    assert!(matches!(
        journal
            .record_execution_fills(&ExecutionFillBatch::new(vec![fill("refused")]).unwrap())
            .await,
        Err(BrokerCommandError::CapturePaused)
    ));
    let pause = journal.analytics_capture_pause().await.unwrap().unwrap();
    let receipt = handoff_receipt(&pause);
    journal
        .acknowledge_analytics_capture_handoff(&receipt)
        .await
        .unwrap();
    let journal = journal
        .with_analytics_capture_budget(budget())
        .await
        .unwrap();
    // Simulate a broken older writer/corrupt local migration reusing a fence.
    // The stored receipt alone must not turn those different facts into proof.
    journal.connection.execute("UPDATE broker_analytics_capture SET last_blocked_at_us=last_blocked_at_us+1 WHERE singleton=1",()).await.unwrap();
    assert!(
        !journal
            .complete_analytics_capture_with_handoff(&receipt)
            .await
            .unwrap()
    );
    let current = journal.analytics_capture_pause().await.unwrap().unwrap();
    assert_eq!(current.generation, pause.generation);
    assert_eq!(current.last_blocked_at_us, pause.last_blocked_at_us + 1);
}

#[tokio::test]
async fn a_prior_generation_completion_never_clears_a_new_source_pause() {
    let fixture = Fixture::new();
    let mut journal = fixture
        .open()
        .await
        .with_analytics_capture_budget(budget())
        .await
        .unwrap();
    tighten(&mut journal).await;
    assert!(matches!(
        journal
            .record_execution_fills(&ExecutionFillBatch::new(vec![fill("first-refusal")]).unwrap())
            .await,
        Err(BrokerCommandError::CapturePaused)
    ));
    let receipt = handoff_receipt(&journal.analytics_capture_pause().await.unwrap().unwrap());
    journal
        .acknowledge_analytics_capture_handoff(&receipt)
        .await
        .unwrap();
    let mut journal = journal
        .with_analytics_capture_budget(budget())
        .await
        .unwrap();
    assert!(
        journal
            .complete_analytics_capture_with_handoff(&receipt)
            .await
            .unwrap()
    );
    tighten(&mut journal).await;
    assert!(matches!(
        journal
            .record_execution_fills(&ExecutionFillBatch::new(vec![fill("second-refusal")]).unwrap())
            .await,
        Err(BrokerCommandError::CapturePaused)
    ));
    let next = journal.analytics_capture_pause().await.unwrap().unwrap();
    assert_eq!(next.generation, receipt.source_generation + 1);
    let journal = journal
        .with_analytics_capture_budget(budget())
        .await
        .unwrap();
    assert!(
        !journal
            .complete_analytics_capture_with_handoff(&receipt)
            .await
            .unwrap()
    );
    assert_eq!(journal.analytics_capture_pause().await.unwrap(), Some(next));
}

#[tokio::test]
async fn qualified_status_is_constant_size_and_keeps_the_generation_after_resume() {
    let fixture = Fixture::new();
    let journal = fixture.open().await;
    assert!(journal.analytics_capture_status().await.unwrap().is_none());
    assert_eq!(count(&journal,"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='broker_analytics_capture'").await,0);
    let mut journal = journal
        .with_analytics_capture_budget(budget())
        .await
        .unwrap();
    let first = journal.analytics_capture_status().await.unwrap().unwrap();
    first.validate().unwrap();
    assert_eq!(first.generation, 0);
    assert!(first.pause.is_none());
    assert!(first.handoff_receipt.is_none());
    tighten(&mut journal).await;
    assert!(matches!(
        journal
            .record_execution_fills(&ExecutionFillBatch::new(vec![fill("status-refused")]).unwrap())
            .await,
        Err(BrokerCommandError::CapturePaused)
    ));
    let paused = journal.analytics_capture_status().await.unwrap().unwrap();
    assert_eq!(paused.generation, 1);
    assert_eq!(
        paused.pause,
        journal.analytics_capture_pause().await.unwrap()
    );
    assert!(paused.handoff_receipt.is_none());
    let receipt = handoff_receipt(paused.pause.as_ref().unwrap());
    assert!(
        journal
            .acknowledge_analytics_capture_handoff(&receipt)
            .await
            .unwrap()
    );
    let accepted = journal.analytics_capture_status().await.unwrap().unwrap();
    assert_eq!(accepted.handoff_receipt, Some(receipt));
    assert_eq!(accepted.journal_source_id, first.journal_source_id);
    let journal = journal
        .with_analytics_capture_budget(budget())
        .await
        .unwrap();
    assert!(
        journal
            .complete_analytics_capture_reconciliation(1)
            .await
            .unwrap()
    );
    let resumed = journal.analytics_capture_status().await.unwrap().unwrap();
    assert_eq!(resumed.generation, 1);
    assert!(resumed.pause.is_none());
    assert!(resumed.handoff_receipt.is_none());
    assert!(serde_json::to_vec(&resumed).unwrap().len() <= 4096);
    let mut rows=journal.connection.query("EXPLAIN QUERY PLAN SELECT generation,paused,handoff_receipt FROM broker_analytics_capture WHERE singleton=1",()).await.unwrap();
    let mut plan = String::new();
    while let Some(row) = rows.next().await.unwrap() {
        plan.push_str(&row.get::<String>(3).unwrap());
    }
    assert!(plan.contains("INTEGER PRIMARY KEY"));
    assert!(!plan.contains("SCAN"));
}

#[tokio::test]
async fn qualified_status_rejects_unknown_or_inconsistent_evidence() {
    let fixture = Fixture::new();
    let journal = fixture
        .open()
        .await
        .with_analytics_capture_budget(budget())
        .await
        .unwrap();
    let original = journal.analytics_capture_status().await.unwrap().unwrap();
    for changed in [
        AnalyticsCaptureStatus {
            version: 2,
            ..original.clone()
        },
        AnalyticsCaptureStatus {
            generation: u64::MAX,
            ..original.clone()
        },
        AnalyticsCaptureStatus {
            observed_at_us: 0,
            ..original.clone()
        },
        AnalyticsCaptureStatus {
            journal_source_id: "x".repeat(129),
            ..original.clone()
        },
        AnalyticsCaptureStatus {
            physical: PhysicalCaptureUsage {
                projected_commit_bytes: 0,
                ..original.physical
            },
            ..original.clone()
        },
    ] {
        assert!(changed.validate().is_err());
    }
    let orphan = AnalyticsCapturePause {
        version: 1,
        journal_source_id: original.journal_source_id.clone(),
        strategy_id: "strategy".into(),
        account_id: "account".into(),
        generation: 1,
        execution_epoch: 1,
        started_at_us: 100,
        last_blocked_at_us: 200,
    };
    assert!(
        AnalyticsCaptureStatus {
            handoff_receipt: Some(handoff_receipt(&orphan)),
            ..original.clone()
        }
        .validate()
        .is_err()
    );
    assert!(
        AnalyticsCaptureStatus {
            pause: Some(orphan),
            ..original.clone()
        }
        .validate()
        .is_err()
    );
    let mut wire = serde_json::to_value(original).unwrap();
    wire["guessedCurrentRunId"] = serde_json::json!("not-recorded");
    assert!(serde_json::from_value::<AnalyticsCaptureStatus>(wire).is_err());
}

#[tokio::test]
async fn exact_diagnostic_receipts_are_immutable_and_never_resume_capture() {
    let fixture = Fixture::new();
    let mut journal = fixture
        .open()
        .await
        .with_analytics_capture_budget(budget())
        .await
        .unwrap();
    tighten(&mut journal).await;
    assert!(matches!(
        journal
            .record_execution_fills(&ExecutionFillBatch::new(vec![fill("refused")]).unwrap())
            .await,
        Err(BrokerCommandError::CapturePaused)
    ));
    let pause = journal.analytics_capture_pause().await.unwrap().unwrap();
    let receipt = handoff_receipt(&pause);
    for changed in [
        AnalyticsCaptureHandoffReceipt {
            pause_id: uuid::Uuid::new_v4(),
            ..receipt.clone()
        },
        AnalyticsCaptureHandoffReceipt {
            account_id: "other".into(),
            ..receipt.clone()
        },
        AnalyticsCaptureHandoffReceipt {
            source_generation: 2,
            ..receipt.clone()
        },
    ] {
        assert!(
            !journal
                .acknowledge_analytics_capture_handoff(&changed)
                .await
                .unwrap()
        );
        assert_eq!(
            journal.analytics_capture_handoff_receipt().await.unwrap(),
            None
        );
    }
    let invalid = AnalyticsCaptureHandoffReceipt {
        version: 2,
        ..receipt.clone()
    };
    assert!(
        journal
            .acknowledge_analytics_capture_handoff(&invalid)
            .await
            .is_err()
    );
    assert!(
        journal
            .acknowledge_analytics_capture_handoff(&receipt)
            .await
            .unwrap()
    );
    assert!(
        journal
            .acknowledge_analytics_capture_handoff(&receipt)
            .await
            .unwrap()
    );
    let different = AnalyticsCaptureHandoffReceipt {
        target_run_id: "new-target".into(),
        ..receipt.clone()
    };
    assert!(
        !journal
            .acknowledge_analytics_capture_handoff(&different)
            .await
            .unwrap()
    );
    assert_eq!(
        journal.analytics_capture_handoff_receipt().await.unwrap(),
        Some(receipt.clone())
    );
    assert_eq!(
        journal.analytics_capture_pause().await.unwrap(),
        Some(pause.clone())
    );
    assert!(matches!(
        journal
            .record_execution_fills(&ExecutionFillBatch::new(vec![fill("still-paused")]).unwrap())
            .await,
        Err(BrokerCommandError::CapturePaused)
    ));
    // A new genuine pause has a new generation and clears the prior receipt.
    let mut journal = journal
        .with_analytics_capture_budget(budget())
        .await
        .unwrap();
    assert!(
        journal
            .complete_analytics_capture_reconciliation(pause.generation)
            .await
            .unwrap()
    );
    tighten(&mut journal).await;
    assert!(matches!(
        journal
            .record_execution_fills(&ExecutionFillBatch::new(vec![fill("refused-again")]).unwrap())
            .await,
        Err(BrokerCommandError::CapturePaused)
    ));
    let next = journal.analytics_capture_pause().await.unwrap().unwrap();
    assert_eq!(next.generation, pause.generation + 1);
    assert_eq!(
        journal.analytics_capture_handoff_receipt().await.unwrap(),
        None
    );
    assert!(
        !journal
            .acknowledge_analytics_capture_handoff(&receipt)
            .await
            .unwrap()
    );
    assert_eq!(journal.analytics_capture_pause().await.unwrap(), Some(next));
}

#[tokio::test]
async fn older_local_pause_schema_gains_the_receipt_column_without_rewriting_its_fact() {
    let fixture = Fixture::new();
    let journal = fixture.open().await;
    journal.connection.execute_batch(SCHEMA).await.unwrap();
    journal.connection.execute("UPDATE broker_analytics_capture SET generation=4,paused=1,started_at_us=100,last_blocked_at_us=200,execution_epoch=2",()).await.unwrap();
    let journal = journal
        .with_analytics_capture_budget(budget())
        .await
        .unwrap();
    let pause = journal.analytics_capture_pause().await.unwrap().unwrap();
    assert_eq!(
        (
            pause.generation,
            pause.execution_epoch,
            pause.started_at_us,
            pause.last_blocked_at_us
        ),
        (4, 2, 100, 200)
    );
    assert_eq!(
        journal.analytics_capture_handoff_receipt().await.unwrap(),
        None
    );
    assert!(
        journal
            .acknowledge_analytics_capture_handoff(&handoff_receipt(&pause))
            .await
            .unwrap()
    );
    assert_eq!(
        journal.analytics_capture_pause().await.unwrap(),
        Some(pause)
    );
}

#[test]
fn bounded_quality_fact_identity_covers_every_field_and_unknown_versions_are_rejected() {
    let pause = AnalyticsCapturePause {
        version: 1,
        journal_source_id: "source".into(),
        strategy_id: "strategy".into(),
        account_id: "account".into(),
        generation: 1,
        execution_epoch: 1,
        started_at_us: 100,
        last_blocked_at_us: 200,
    };
    assert!(pause.validate().is_ok());
    assert_eq!(pause.stable_id(), pause.clone().stable_id());
    for changed in [
        AnalyticsCapturePause {
            journal_source_id: "new".into(),
            ..pause.clone()
        },
        AnalyticsCapturePause {
            strategy_id: "new".into(),
            ..pause.clone()
        },
        AnalyticsCapturePause {
            account_id: "new".into(),
            ..pause.clone()
        },
        AnalyticsCapturePause {
            generation: 2,
            ..pause.clone()
        },
        AnalyticsCapturePause {
            execution_epoch: 2,
            ..pause.clone()
        },
        AnalyticsCapturePause {
            started_at_us: 101,
            ..pause.clone()
        },
        AnalyticsCapturePause {
            last_blocked_at_us: 201,
            ..pause.clone()
        },
    ] {
        assert_ne!(changed.stable_id(), pause.stable_id());
    }
    assert!(
        AnalyticsCapturePause {
            version: 2,
            ..pause.clone()
        }
        .validate()
        .is_err()
    );
    assert!(
        AnalyticsCapturePause {
            last_blocked_at_us: 99,
            ..pause.clone()
        }
        .validate()
        .is_err()
    );
    assert!(
        AnalyticsCapturePause {
            journal_source_id: "x".repeat(129),
            ..pause.clone()
        }
        .validate()
        .is_err()
    );
    let mut wire = serde_json::to_value(&pause).unwrap();
    wire["unqualifiedRunId"] = serde_json::json!("invented");
    assert!(serde_json::from_value::<AnalyticsCapturePause>(wire).is_err());
}

#[tokio::test]
async fn analytics_budget_is_opt_in_and_does_not_install_an_archive_or_a_pause() {
    let fixture = Fixture::new();
    let journal = fixture.open().await;
    assert_eq!(journal.analytics_capture_budget(), None);
    assert_eq!(journal.analytics_capture_usage().await.unwrap(), None);
    assert_eq!(journal.analytics_capture_pause().await.unwrap(), None);
    let journal = journal
        .with_analytics_capture_budget(budget())
        .await
        .unwrap();
    assert_eq!(journal.analytics_capture_budget(), Some(budget()));
    assert!(journal.analytics_capture_usage().await.unwrap().is_some());
    assert_eq!(journal.analytics_capture_pause().await.unwrap(), None);
    assert_eq!(
        count(&journal, "SELECT count(*) FROM broker_execution_evidence").await,
        0
    );
    assert_eq!(count(&journal, "PRAGMA cache_spill").await, 0);
}

#[tokio::test]
async fn source_pressure_rolls_back_fill_heads_and_keeps_stop_and_epoch_recovery_independent() {
    let fixture = Fixture::new();
    let mut journal = fixture
        .open()
        .await
        .with_analytics_capture_budget(budget())
        .await
        .unwrap();
    journal
        .record_execution_fills(&ExecutionFillBatch::new(vec![fill("accepted")]).unwrap())
        .await
        .unwrap();
    let original = journal.execution_fill_page(0, None, 128).await.unwrap();
    tighten(&mut journal).await;
    let mut corrected = fill("accepted");
    corrected.fill.price = 101.0;
    corrected.fill.as_of_us = 300;
    assert!(matches!(
        journal
            .record_execution_fills(&ExecutionFillBatch::new(vec![corrected]).unwrap())
            .await,
        Err(BrokerCommandError::CapturePaused)
    ));
    assert_eq!(
        journal
            .execution_fill_page(0, None, 128)
            .await
            .unwrap()
            .records,
        original.records
    );
    assert_eq!(
        count(&journal, "SELECT count(*) FROM broker_execution_fill_heads").await,
        1
    );
    let pause = journal.analytics_capture_pause().await.unwrap().unwrap();
    assert_eq!(pause.generation, 1);
    assert_eq!(pause.execution_epoch, 1);
    for index in 0..100 {
        assert!(matches!(
            journal
                .record_execution_fills(
                    &ExecutionFillBatch::new(vec![fill(&format!("retry-{index}"))]).unwrap()
                )
                .await,
            Err(BrokerCommandError::CapturePaused)
        ));
    }
    assert_eq!(
        journal.analytics_capture_pause().await.unwrap(),
        Some(pause.clone()),
        "paused retries do not append metadata or rewrite the original gap"
    );
    journal.request_execution_stop().await.unwrap();
    assert!(journal.complete_execution_stop().await.unwrap());
    assert_eq!(journal.begin_execution_epoch().await.unwrap().epoch, 2);
    assert_eq!(
        journal.analytics_capture_pause().await.unwrap(),
        Some(pause.clone())
    );
    // Already accepted bytes may still be frozen/exported while fresh capture is
    // paused. The receipt releases no broker checkpoint or command state.
    let transfer = journal
        .prepare_execution_fill_export()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(transfer.page().unwrap().records.len(), 1);
    journal
        .acknowledge_execution_fill_export(&transfer.receipt())
        .await
        .unwrap();
    assert_eq!(
        journal.analytics_capture_pause().await.unwrap(),
        Some(pause)
    );
    assert_eq!(
        count(&journal, "SELECT count(*) FROM broker_execution_fills").await,
        1
    );
}

#[tokio::test]
async fn a_local_gap_survives_restart_and_requires_exact_generation_and_capacity_to_resume() {
    let fixture = Fixture::new();
    let mut journal = fixture
        .open()
        .await
        .with_analytics_capture_budget(budget())
        .await
        .unwrap();
    tighten(&mut journal).await;
    let small = journal.analytics_capture_budget.unwrap();
    assert!(matches!(
        journal
            .record_execution_fills(&ExecutionFillBatch::new(vec![fill("omitted")]).unwrap())
            .await,
        Err(BrokerCommandError::CapturePaused)
    ));
    let pause = journal.analytics_capture_pause().await.unwrap().unwrap();
    assert!(
        !journal
            .complete_analytics_capture_reconciliation(pause.generation)
            .await
            .unwrap()
    );
    drop(journal);
    let mut journal = fixture
        .open()
        .await
        .with_analytics_capture_budget(small)
        .await
        .unwrap();
    assert_eq!(
        journal.analytics_capture_pause().await.unwrap(),
        Some(pause.clone())
    );
    journal = journal
        .with_analytics_capture_budget(budget())
        .await
        .unwrap();
    assert_eq!(
        journal.analytics_capture_pause().await.unwrap(),
        Some(pause.clone()),
        "raising a cap is not a reconciliation receipt"
    );
    assert!(
        !journal
            .complete_analytics_capture_reconciliation(pause.generation + 1)
            .await
            .unwrap()
    );
    assert!(
        journal
            .complete_analytics_capture_reconciliation(pause.generation)
            .await
            .unwrap()
    );
    assert!(journal.analytics_capture_pause().await.unwrap().is_none());
    journal
        .record_execution_fills(&ExecutionFillBatch::new(vec![fill("after-reconcile")]).unwrap())
        .await
        .unwrap();
    assert_eq!(
        count(&journal, "SELECT count(*) FROM broker_execution_fills").await,
        1
    );
}

#[tokio::test]
async fn invalid_budget_or_missing_footprint_never_falls_back_to_acceptance() {
    let fixture = Fixture::new();
    assert!(
        BrokerCommandJournal::open_with_state_history(
            fixture.0.join("journal.db"),
            "strategy",
            "account",
            true
        )
        .await
        .unwrap()
        .with_analytics_capture_budget(budget())
        .await
        .is_err(),
        "bounded analytics must not silently leave a second uncapped observation archive on"
    );
    assert!(
        fixture
            .open()
            .await
            .with_analytics_capture_budget(PhysicalCaptureLimit {
                physical_limit_bytes: MIN_OPERATIONAL_HEADROOM_BYTES,
                ..budget()
            })
            .await
            .is_err()
    );
    let mut journal = fixture
        .open()
        .await
        .with_analytics_capture_budget(budget())
        .await
        .unwrap();
    journal.path = fixture.0.join("missing.db");
    assert!(matches!(
        journal
            .record_execution_fills(
                &ExecutionFillBatch::new(vec![fill("not-acknowledged")]).unwrap()
            )
            .await,
        Err(BrokerCommandError::Journal(_))
    ));
    assert_eq!(
        count(&journal, "SELECT count(*) FROM broker_execution_fills").await,
        0
    );
    assert!(
        journal.analytics_capture_pause().await.unwrap().is_none(),
        "an unrecorded refusal is not claimed as a durable gap"
    );
    journal.request_execution_stop().await.unwrap();
    assert!(journal.complete_execution_stop().await.unwrap());
}

#[tokio::test]
async fn a_large_optional_batch_cannot_spill_before_its_post_image_is_refused() {
    let fixture = Fixture::new();
    let mut journal = fixture
        .open()
        .await
        .with_analytics_capture_budget(budget())
        .await
        .unwrap();
    let mut rows = journal
        .connection
        .query("PRAGMA wal_checkpoint(TRUNCATE)", ())
        .await
        .unwrap();
    while rows.next().await.unwrap().is_some() {}
    drop(rows);
    let before = journal.analytics_capture_usage().await.unwrap().unwrap();
    journal.analytics_capture_budget = Some(PhysicalCaptureLimit {
        physical_limit_bytes: MIN_OPERATIONAL_HEADROOM_BYTES
            + before.projected_commit_bytes
            + 16 * 1024,
        ..budget()
    });
    assert!(
        !journal
            .analytics_capture_usage()
            .await
            .unwrap()
            .unwrap()
            .exceeds_data_limit()
    );
    let batch = ExecutionFillBatch::new(
        (0..128)
            .map(|index| fill(&format!("batch-{index}")))
            .collect(),
    )
    .unwrap();
    assert!(matches!(
        journal.record_execution_fills(&batch).await,
        Err(BrokerCommandError::CapturePaused)
    ));
    assert_eq!(
        count(&journal, "SELECT count(*) FROM broker_execution_fills").await,
        0
    );
    assert_eq!(
        count(&journal, "SELECT count(*) FROM broker_execution_fill_heads").await,
        0
    );
    assert_eq!(
        journal
            .execution_fill_page(0, None, 128)
            .await
            .unwrap()
            .next_sequence,
        0
    );
    assert!(
        journal
            .analytics_capture_usage()
            .await
            .unwrap()
            .unwrap()
            .wal_bytes
            < 64 * 1024,
        "only the compact pause, not the rejected full batch, reached the WAL"
    );
}
