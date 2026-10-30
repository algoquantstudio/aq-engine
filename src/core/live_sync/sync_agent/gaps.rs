//! Best-effort operational metadata, isolated from the mandatory stop worker.
use super::*;
#[cfg(test)]
mod tests;

pub(crate) async fn flush(
    outbox: &OutboxStore,
    sink: &Arc<dyn RemoteBatchSink>,
    strategy: &str,
    session: &str,
) -> Result<(), SyncAgentError> {
    if sink.safety_run_id().is_none() {
        return Ok(());
    }
    // A newer authenticated run may relay older sessions of the same strategy.
    // The service must revalidate the source session's owner before committing.
    for notice in outbox.pending_capture_gap_notices(strategy).await? {
        let receipt = sink
            .persist_capture_gap(&notice)
            .await
            .ok()
            .flatten()
            .filter(|receipt| receipt.covers(&notice));
        outbox
            .settle_capture_gap_notice(&notice, receipt.as_ref())
            .await?;
    }
    for notice in outbox.local_capture_gap_notices(strategy, session).await? {
        if let Ok(Some(receipt)) = sink.persist_capture_gap(&notice).await {
            outbox
                .settle_local_capture_gap_notice(&notice, &receipt)
                .await?;
        }
    }
    Ok(())
}

pub(crate) async fn run(
    outbox: OutboxStore,
    sink: Arc<dyn RemoteBatchSink>,
    strategy: String,
    session: String,
    mut shutdown: watch::Receiver<bool>,
) {
    let notifier = outbox.safety_change_notifier();
    loop {
        if *shutdown.borrow() {
            break;
        }
        let notified = notifier.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        tokio::select! { biased;
            _=shutdown.changed()=>break,
            result=flush(&outbox,&sink,&strategy,&session)=>{
                if let Err(error)=result {
                    log::debug!("Cloud-history diagnostic retained for retry: {error}");
                }
            },
        }
        tokio::select! { biased;
            _=shutdown.changed()=>break,
            _=&mut notified=>{},
            _=tokio::time::sleep(Duration::from_secs(5))=>{},
        }
    }
}
