//! One bounded worker per store/scope, independent of normal data backpressure.
use super::*;

pub(crate) async fn flush(
    outbox: &OutboxStore,
    sink: &Arc<dyn RemoteBatchSink>,
    runtime: &str,
    strategy: &str,
    session: &str,
) -> Result<(), SyncAgentError> {
    let Some(run) = sink.safety_run_id() else {
        return Ok(());
    };
    for observation in outbox
        .pending_safety_observations(runtime, strategy, session, run)
        .await?
    {
        let delivered = matches!(sink.persist_safety(&observation).await, Ok(true));
        // Failure, lost acknowledgement and older servers retain compact work.
        // Exact-event settlement cannot acknowledge a concurrently newer state.
        outbox
            .settle_safety_observation(&observation, delivered)
            .await?;
    }
    Ok(())
}

pub(crate) async fn run(
    outbox: OutboxStore,
    sink: Arc<dyn RemoteBatchSink>,
    runtime: String,
    strategy: String,
    session: String,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), SyncAgentError> {
    // A slow/unavailable diagnostic endpoint must never delay stop delivery.
    // JoinSet's drop aborts the auxiliary task if this safety worker exits.
    let mut auxiliary = tokio::task::JoinSet::new();
    auxiliary.spawn(super::gaps::run(
        outbox.clone(),
        sink.clone(),
        strategy.clone(),
        session.clone(),
        shutdown.clone(),
    ));
    let notifier = outbox.safety_change_notifier();
    loop {
        if *shutdown.borrow() {
            break;
        }
        let notified = notifier.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        flush(&outbox, &sink, &runtime, &strategy, &session).await?;
        tokio::select! {biased;
            changed=shutdown.changed()=>{if changed.is_err()||*shutdown.borrow(){break;}},
            _=&mut notified=>{},
            _=tokio::time::sleep(Duration::from_secs(5))=>{},
        }
    }
    Ok(())
}
