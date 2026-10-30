use super::*;

fn event(revision: u64, state: &str) -> CloudEvent {
    let mut event = EventEnvelope::new(
        "runtime",
        "strategy",
        "session",
        "lifecycle",
        revision,
        EventPriority::Critical,
        EventKind::StrategyLifecycle,
        serde_json::json!({"state":state,"account":{"large":"x".repeat(2048)}}),
    );
    event.run_id = Some("run".into());
    event
}
fn fact(revision: u64, state: &str) -> SafetyObservation {
    SafetyObservation::from_event(&event(revision, state)).unwrap()
}
fn action(index: usize) -> SafetyObservation {
    let mut event = event(index as u64 + 1, "stopped");
    event.kind = EventKind::StrategyActionAcknowledgement;
    event.payload = serde_json::json!({"action":"stop","action_id":format!("strategy_actions:{index}"),"status":"acknowledged"});
    SafetyObservation::from_event(&event).unwrap()
}
async fn store() -> OutboxStore {
    OutboxStore::open(
        std::env::temp_dir().join(format!("safety-headroom-{}.db", uuid::Uuid::new_v4())),
    )
    .await
    .unwrap()
}
async fn rows(store: &OutboxStore) -> i64 {
    let _gate = store.operation_gate.lock().await;
    store
        .connection
        .query("SELECT count(*) FROM safety_observations", ())
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap()
        .get(0)
        .unwrap()
}

#[tokio::test]
async fn full_data_buffer_still_accepts_stop_facts_without_accepting_their_full_events() {
    let store = store().await;
    let mut retained = event(1, "running");
    retained.critical_sequence = Some(1);
    let bytes = FlatbufferEventCodec::encode_with_local_commit(&retained, now_epoch_micros())
        .unwrap()
        .len() as u64;
    store
        .capture_events_with_capacity(&[retained.clone()], bytes)
        .await
        .unwrap();
    let stopped = event(2, "stopped");
    assert!(matches!(
        store
            .capture_events_with_capacity(&[stopped.clone()], bytes)
            .await,
        Err(OutboxStoreError::CapturePaused(_))
    ));
    let facts = [SafetyObservation::from_event(&stopped).unwrap(), action(1)];
    assert_eq!(store.record_safety_only(&facts).await.unwrap(), facts);
    assert_eq!(store.capture_retained_bytes().await.unwrap(), bytes);
    assert_eq!(store.pending_count().await.unwrap(), 1);
    assert_eq!(
        store
            .critical_sequence_cursor_for_runtime_scope("runtime", "strategy", "session")
            .await
            .unwrap(),
        1
    );
    assert!(
        store
            .pending_events_for_entity(
                "strategy",
                "session",
                "lifecycle",
                EventKind::StrategyLifecycle,
                10
            )
            .await
            .unwrap()
            .iter()
            .all(|e| e.event_id != stopped.event_id)
    );
    assert_eq!(
        store
            .retirement_uncommitted_for_scope("strategy", "session")
            .await
            .unwrap(),
        3
    );
    let path = store.path().to_owned();
    drop(store);
    let store = OutboxStore::open(path).await.unwrap();
    assert_eq!(
        store
            .pending_safety_observations("runtime", "strategy", "session", "run")
            .await
            .unwrap()
            .len(),
        2
    );
    for fact in &facts {
        store.settle_safety_observation(fact, true).await.unwrap();
    }
    // A late failure cannot undo a successful independent receipt.
    store
        .settle_safety_observation(&facts[0], false)
        .await
        .unwrap();
    assert!(
        store
            .pending_safety_observations("runtime", "strategy", "session", "run")
            .await
            .unwrap()
            .is_empty()
    );
    store.prune_committed(0).await.unwrap();
    assert_eq!(rows(&store).await, 0);
    assert_eq!(
        store.pending_count().await.unwrap(),
        1,
        "trading evidence was not retired by a safety receipt"
    );
}

#[tokio::test]
async fn reserved_stop_slots_are_bounded_idempotent_and_terminal_stable() {
    let store = store().await;
    let mut facts = (0..256).map(action).collect::<Vec<_>>();
    facts.push(fact(1, "stopping"));
    store.record_safety_only(&facts).await.unwrap();
    store.record_safety_only(&facts).await.unwrap();
    assert_eq!(rows(&store).await, 257);
    assert!(store.record_safety_only(&[action(257)]).await.is_err());
    let terminal = fact(2, "stopped");
    store.record_safety_only(&[terminal.clone()]).await.unwrap();
    let retry = fact(3, "stopping");
    assert_eq!(
        store.record_safety_only(&[retry]).await.unwrap(),
        [terminal.clone()]
    );
    assert_eq!(rows(&store).await, 257);
    let mut other = terminal.clone();
    other.runtime_id = "other".into();
    assert!(
        store
            .record_safety_only(&[terminal.clone(), other])
            .await
            .is_err()
    );
    let mut changed = terminal.clone();
    changed.message = Some("different immutable fact".into());
    assert!(store.record_safety_only(&[changed]).await.is_err());
    assert!(
        store
            .record_safety_only(&vec![terminal; 258])
            .await
            .is_err()
    );
    assert_eq!(rows(&store).await, 257);
}

#[tokio::test]
async fn full_evidence_promotion_does_not_let_a_safety_receipt_delete_its_original() {
    let store = store().await;
    let mut stopped = event(1, "stopped");
    stopped.critical_sequence = Some(1);
    let fact = SafetyObservation::from_event(&stopped).unwrap();
    store.record_safety_only(&[fact.clone()]).await.unwrap();
    store.settle_safety_observation(&fact, true).await.unwrap();
    store.insert_events(&[stopped.clone()]).await.unwrap();
    store.prune_committed(0).await.unwrap();
    assert_eq!(rows(&store).await, 1);
    assert_eq!(store.pending_count().await.unwrap(), 1);
    store
        .mark_remote_committed(&[stopped.event_id], Some(1))
        .await
        .unwrap();
    store.prune_committed(0).await.unwrap();
    assert_eq!(rows(&store).await, 0);
}

#[tokio::test]
async fn cancelling_an_operational_transaction_rolls_back_before_the_next_writer() {
    let store = store().await;
    let writer = store.clone();
    let written = std::sync::Arc::new(tokio::sync::Notify::new());
    let signal = written.clone();
    let task = tokio::spawn(async move {
        let _gate = writer.operation_gate.lock().await;
        let _transaction = turso::transaction::Transaction::new_unchecked(
            &writer.connection,
            turso::transaction::TransactionBehavior::Immediate,
        )
        .await
        .unwrap();
        writer
            .record_safety_observation_locked(&fact(1, "stopping"), false)
            .await
            .unwrap();
        signal.notify_one();
        std::future::pending::<()>().await;
    });
    written.notified().await;
    task.abort();
    let _ = task.await;
    assert_eq!(rows(&store).await, 0);
    store
        .record_safety_only(&[fact(2, "stopped")])
        .await
        .unwrap();
    assert_eq!(rows(&store).await, 1);
}
