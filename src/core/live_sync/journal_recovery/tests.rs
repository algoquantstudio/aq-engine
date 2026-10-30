use super::*;
use crate::core::{
    broker::command_journal::{
        BrokerCommandJournal, ExitedAnalyticsCapture, ExitedAnalyticsCaptureError,
    },
    live_sync::{MIN_OPERATIONAL_HEADROOM_BYTES, OutboxStore, PhysicalCaptureLimit},
};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let root =
            std::env::temp_dir().join(format!("exited-journal-link-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        Self(root)
    }
    fn source(&self) -> PathBuf {
        self.0.join("publisher.db")
    }
    async fn journal(&self) -> BrokerCommandJournal {
        BrokerCommandJournal::open(
            self.0.join("commands.db"),
            "strategy",
            "account")
        .await
        .unwrap()
        .with_analytics_capture_budget(PhysicalCaptureLimit {
            physical_limit_bytes: 32 * 1024 * 1024,
            operational_headroom_bytes: MIN_OPERATIONAL_HEADROOM_BYTES,
        })
        .await
        .unwrap()
    }
    fn owner(&self, runtime: &str) -> CaptureOwnerLease {
        CaptureOwnerLease::acquire(&self.source(), runtime, "strategy", "session", "run").unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn fallback_links_are_bounded_idempotent_and_keep_original_delivery_identity() {
    let fixture = Fixture::new();
    let journal = fixture.journal().await;
    let owner = fixture.owner("original-runtime");
    let link = journal.recovery_link(&owner, 100_000).unwrap();
    owner.retain_journal_recovery(&link).await.unwrap();
    owner.retain_journal_recovery(&link).await.unwrap();
    for index in 1..MAX_LINKS {
        let mut next = link.clone();
        next.journal_source_id = format!("fixture-source-{index:04}");
        next.journal_path = PathBuf::from(format!("/{}", "\"".repeat(3500)));
        next.account_id = "a".repeat(128);
        assert!(serde_json::to_vec(&next).unwrap().len() > 7000);
        owner.retain_journal_recovery(&next).await.unwrap();
    }
    let mut overflow = link.clone();
    overflow.journal_source_id = "overflow".into();
    assert!(owner.retain_journal_recovery(&overflow).await.is_err());
    let size = std::fs::read_dir(&fixture.0)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|f| {
            f.file_name()
                .to_string_lossy()
                .starts_with("publisher.db.journals.db")
        })
        .map(|f| f.metadata().unwrap().len())
        .sum::<u64>();
    assert!(
        size < MIN_OPERATIONAL_HEADROOM_BYTES,
        "bounded metadata must fit its reserved headroom, got {size}"
    );
    drop(owner);
    let next = fixture.owner("new-runtime");
    let changed = journal.recovery_link(&next, 200_000).unwrap();
    next.retain_journal_recovery(&changed).await.unwrap();
    drop(next);
    let import = CaptureOwnerLease::acquire_for_import(&fixture.source()).unwrap();
    assert!(
        import
            .journal_recovery_links("foreign", "session")
            .await
            .is_err()
    );
    let mut original = None;
    let mut count = 0;
    loop {
        let links = import
            .journal_recovery_links("strategy", "session")
            .await
            .unwrap();
        if links.is_empty() {
            break;
        }
        assert!(links.len() <= 8);
        for saved in links {
            count += 1;
            if saved.journal_source_id == link.journal_source_id {
                original = Some(saved.clone());
            }
            import.forget_journal_recovery(&saved).await.unwrap();
        }
    }
    assert_eq!(count, MAX_LINKS);
    assert_eq!(original, Some(link));
    assert!(import.retire_empty_journal_recovery().await.unwrap());
    assert!(!CaptureOwnerLease::journal_recovery_path(&fixture.source()).exists());
    drop(journal);
}

#[tokio::test]
async fn exited_reader_rejects_retired_journal_formats_without_rewriting_them() {
    let fixture = Fixture::new();
    let journal = fixture.journal().await;
    let owner = fixture.owner("runtime");
    let link = journal.recovery_link(&owner, 100_000).unwrap();
    drop(journal);
    let database = turso::Builder::new_local(link.journal_path.to_str().unwrap())
        .build().await.unwrap();
    let connection = database.connect().unwrap();
    connection.execute_batch("PRAGMA user_version=1;").await.unwrap();
    assert!(matches!(
        ExitedAnalyticsCapture::open(&link).await,
        Err(ExitedAnalyticsCaptureError::Invalid)
    ));
    let mut rows = connection.query("PRAGMA user_version", ()).await.unwrap();
    assert_eq!(rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(), 1);
    drop(rows);
    let mut rows = connection.query(
        "SELECT source_id FROM broker_execution_evidence_source WHERE singleton=1", (),
    ).await.unwrap();
    assert_eq!(rows.next().await.unwrap().unwrap().get::<String>(0).unwrap(), link.journal_source_id);
}

#[tokio::test]
async fn exited_reader_never_takes_a_live_journal_and_fences_foreign_source_identity() {
    let fixture = Fixture::new();
    let journal = fixture.journal().await;
    let owner = fixture.owner("runtime");
    let link = journal.recovery_link(&owner, 100_000).unwrap();
    let pause = journal
        .suspend_analytics_for_source_initialization()
        .await
        .unwrap();
    assert!(matches!(
        ExitedAnalyticsCapture::open(&link).await,
        Err(ExitedAnalyticsCaptureError::Busy)
    ));
    drop(journal);
    let mut wrong = link.clone();
    wrong.journal_source_id = "foreign-source".into();
    assert!(matches!(
        ExitedAnalyticsCapture::open(&wrong).await,
        Err(ExitedAnalyticsCaptureError::Invalid)
    ));
    wrong = link.clone();
    wrong.account_id = "foreign-account".into();
    assert!(matches!(
        ExitedAnalyticsCapture::open(&wrong).await,
        Err(ExitedAnalyticsCaptureError::Invalid)
    ));
    wrong = link.clone();
    wrong.journal_path = fixture.0.join("missing.db");
    assert!(matches!(
        ExitedAnalyticsCapture::open(&wrong).await,
        Err(ExitedAnalyticsCaptureError::Unavailable)
    ));
    assert!(!wrong.journal_path.exists());
    let reader = ExitedAnalyticsCapture::open(&link).await.unwrap();
    assert_eq!(reader.pause().await.unwrap(), Some(pause.clone()));
    drop(owner);
    let import = CaptureOwnerLease::acquire_for_import(&fixture.source()).unwrap();
    let source = OutboxStore::open(fixture.source()).await.unwrap();
    let receipt = source
        .handoff_exited_journal_pause(&import, &link, &pause)
        .await
        .unwrap()
        .unwrap();
    let mut stale = receipt.clone();
    stale.source_generation += 1;
    assert!(!reader.acknowledge_handoff(&pause, &stale).await.unwrap());
    assert!(reader.acknowledge_handoff(&pause, &receipt).await.unwrap());
    assert!(reader.acknowledge_handoff(&pause, &receipt).await.unwrap());
    assert_eq!(reader.pause().await.unwrap(), Some(pause));
    drop(reader);
    let journal = fixture.journal().await;
    assert_eq!(
        journal.analytics_capture_handoff_receipt().await.unwrap(),
        Some(receipt)
    );
    assert!(
        journal
            .execution_fill_page(0, None, 128)
            .await
            .unwrap()
            .records
            .is_empty()
    );
    drop(journal);
    drop(source);
}

#[cfg(unix)]
#[tokio::test]
async fn source_recovery_links_and_journal_reads_reject_symlinks() {
    let fixture = Fixture::new();
    let journal = fixture.journal().await;
    let owner = fixture.owner("runtime");
    let link = journal.recovery_link(&owner, 100_000).unwrap();
    let registry = CaptureOwnerLease::journal_recovery_path(&fixture.source());
    std::os::unix::fs::symlink(fixture.0.join("unrelated.db"), &registry).unwrap();
    assert!(owner.retain_journal_recovery(&link).await.is_err());
    assert!(!fixture.0.join("unrelated.db").exists());
    drop(journal);
    let alias = fixture.0.join("alias.db");
    std::os::unix::fs::symlink(&link.journal_path, &alias).unwrap();
    let mut aliased = link;
    aliased.journal_path = alias;
    assert!(matches!(
        ExitedAnalyticsCapture::open(&aliased).await,
        Err(ExitedAnalyticsCaptureError::Invalid)
    ));
}
