//! Resumable maintenance of optional reconciliation envelopes. A durable new
//! job is the ownership witness; primary critical evidence is never selected.
use super::*;

fn db(error: impl std::fmt::Display) -> OutboxStoreError {
    OutboxStoreError::Database(error.to_string())
}

impl OutboxStore {
    /// Caller retains operation_gate across job creation and these pages.
    pub(super) async fn cleanup_reconciliation_pages(
        &self,
        start: &ReconciliationStart,
    ) -> Result<(), OutboxStoreError> {
        loop {
            let changed=self.execute_bounded_metadata(
                "UPDATE reconciliation_jobs SET status='superseded',updated_at_us=?3 WHERE generation IN (
                 SELECT generation FROM reconciliation_jobs WHERE strategy_id=?1 AND live_session_id=?2
                 AND generation!=?4 AND status NOT IN ('completed','superseded') LIMIT 64)",
                params![start.strategy_id.clone(),start.live_session_id.clone(),now_epoch_micros(),start.generation.clone()],
            ).await?;
            if changed == 0 {
                break;
            }
        }
        for dead_letters in [false, true] {
            let mut cursor = String::new();
            loop {
                let sql = if dead_letters {
                    "SELECT event_id,CASE WHEN length(wire_payload)<=?3 THEN wire_payload END,reason,length(wire_payload) FROM dead_letters
                     WHERE strategy_id=?1 AND event_id>?2 ORDER BY event_id LIMIT 64"
                } else {
                    "SELECT event_id,CASE WHEN length(wire_payload)<=?3 THEN wire_payload END,'',length(wire_payload) FROM outbox_events WHERE state='pending'
                     AND priority='reconciliation' AND strategy_id=?1 AND event_id>?2 ORDER BY event_id LIMIT 64"
                };
                let mut rows = self
                    .connection
                    .query(sql, params![start.strategy_id.clone(), cursor.clone(),LOCAL_EVENT_HARD_LIMIT_BYTES as i64])
                    .await
                    .map_err(db)?;
                let mut selected = Vec::new();
                let mut count = 0;
                while let Some(row) = rows.next().await.map_err(db)? {
                    count += 1;
                    cursor = row.get(0).map_err(db)?;
                    if row.get::<i64>(3).map_err(db)? > LOCAL_EVENT_HARD_LIMIT_BYTES as i64 {
                        return Err(OutboxStoreError::InvalidRecord(
                            "unsupported oversized reconciliation envelope; legacy upgrades are not supported"
                                .into(),
                        ));
                    }
                    let payload: Vec<u8> = row.get(1).map_err(db)?;
                    let Some(event) = FlatbufferEventCodec::decode(&payload).ok().filter(|event| {
                        event.live_session_id == start.live_session_id
                            && event.priority == EventPriority::Reconciliation
                    }) else {
                        continue;
                    };
                    let Some(generation) = event
                        .payload
                        .get("generation")
                        .and_then(serde_json::Value::as_str)
                    else {
                        continue;
                    };
                    let eligible = if dead_letters {
                        let reason: String = row.get(2).map_err(db)?;
                        let reason = reason.to_ascii_lowercase();
                        generation == start.generation
                            && [
                                "transaction conflict",
                                "resource busy",
                                "database is locked",
                                "this transaction can be retried",
                            ]
                            .iter()
                            .any(|part| reason.contains(part))
                    } else {
                        generation != start.generation
                    };
                    if eligible {
                        selected.push(cursor.clone());
                    }
                }
                drop(rows);
                if !selected.is_empty() {
                    let tx = turso::transaction::Transaction::new_unchecked(
                        &self.connection,
                        turso::transaction::TransactionBehavior::Immediate,
                    )
                    .await
                    .map_err(db)?;
                    for id in selected {
                        if dead_letters {
                            tx.execute("UPDATE outbox_events SET state='pending',attempt_count=0,next_attempt_at_us=?2,last_error=NULL WHERE event_id=?1 AND priority='reconciliation'",
                                params![id.clone(),now_epoch_micros()]).await.map_err(db)?;
                            // A dead-letter transport copy can be retired only
                            // when its original outbox row still owns the work.
                            tx.execute("DELETE FROM dead_letters WHERE event_id=?1 AND EXISTS(SELECT 1 FROM outbox_events WHERE event_id=?1 AND state='pending' AND priority='reconciliation')",
                                params![id]).await.map_err(db)?;
                        } else {
                            tx.execute("UPDATE outbox_events SET state='committed',remote_committed_at_us=?2,last_error='superseded by a newer reconciliation run' WHERE event_id=?1 AND state='pending' AND priority='reconciliation'",
                                params![id,now_epoch_micros()]).await.map_err(db)?;
                        }
                    }
                    self.commit_bounded_metadata(tx).await?;
                }
                if count < 64 {
                    break;
                }
            }
        }
        Ok(())
    }
}
