//! Bounded metadata-only recovery hints. Payloads, attempts and receipts never change.
use super::*;

const RETRY_HINTS_QUERY: &str = "SELECT event_id, next_attempt_at_us, last_error
    FROM outbox_events INDEXED BY idx_outbox_runtime_pending
    WHERE runtime_id=?1 AND strategy_id=?2 AND live_session_id=?3
      AND priority=?4 AND state='pending' AND next_attempt_at_us>?5
      AND last_error IS NOT NULL AND length(CAST(last_error AS BLOB))<=4096
    ORDER BY next_attempt_at_us LIMIT 256";

pub(crate) struct RetryHint {
    pub event_id: EventId,
    pub deadline_us: i64,
    pub error: String,
}

impl OutboxStore {
    /// Startup only: one indexed, bounded page per exact delivery lane. A new
    /// process must not forget every transient retry scheduled by its predecessor.
    pub(crate) async fn deferred_retry_hints(
        &self,
        priority: EventPriority,
        runtime: &str,
        strategy: &str,
        session: &str,
    ) -> Result<Vec<RetryHint>, OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let mut rows = self
            .connection
            .query(
                RETRY_HINTS_QUERY,
                params![
                    runtime.to_owned(),
                    strategy.to_owned(),
                    session.to_owned(),
                    priority_label(priority),
                    now_epoch_micros()
                ],
            )
            .await
            .map_err(|e| OutboxStoreError::Database(e.to_string()))?;
        let mut hints = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| OutboxStoreError::Database(e.to_string()))?
        {
            let id: String = row
                .get(0)
                .map_err(|e| OutboxStoreError::Database(e.to_string()))?;
            hints.push(RetryHint {
                event_id: id.parse().map_err(|_| {
                    OutboxStoreError::InvalidRecord("invalid retry event identity".into())
                })?,
                deadline_us: row
                    .get(1)
                    .map_err(|e| OutboxStoreError::Database(e.to_string()))?,
                error: row
                    .get(2)
                    .map_err(|e| OutboxStoreError::Database(e.to_string()))?,
            });
        }
        Ok(hints)
    }

    /// Wake only the exact remembered transient failure, not a later auth,
    /// sequencing or quota failure for the same event. One set-based UPDATE is
    /// atomic with the physical-budget check; all identities remain parameters.
    pub(crate) async fn wake_retry_events(
        &self,
        priority: EventPriority,
        runtime: &str,
        strategy: &str,
        session: &str,
        ids: &[EventId],
        error: &str,
    ) -> Result<u64, OutboxStoreError> {
        if ids.is_empty() {
            return Ok(0);
        }
        if ids.len() > 256 || error.len() > 4096 {
            return Err(OutboxStoreError::InvalidRecord(
                "retry recovery exceeds its bound".into(),
            ));
        }
        let _operation = self.operation_gate.lock().await;
        let tx = turso::transaction::Transaction::new_unchecked(
            &self.connection,
            turso::transaction::TransactionBehavior::Immediate,
        )
        .await
        .map_err(|e| OutboxStoreError::Database(e.to_string()))?;
        let placeholders = (7..7 + ids.len())
            .map(|n| format!("?{n}"))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "UPDATE outbox_events SET next_attempt_at_us=?1
             WHERE runtime_id=?2 AND strategy_id=?3 AND live_session_id=?4
               AND priority=?5 AND last_error=?6 AND state='pending'
               AND next_attempt_at_us>?1 AND event_id IN ({placeholders})"
        );
        let mut values = vec![
            turso::Value::Integer(now_epoch_micros()),
            turso::Value::Text(runtime.to_owned()),
            turso::Value::Text(strategy.to_owned()),
            turso::Value::Text(session.to_owned()),
            turso::Value::Text(priority_label(priority).into()),
            turso::Value::Text(error.to_owned()),
        ];
        values.extend(ids.iter().map(|id| turso::Value::Text(id.to_string())));
        let result = async {
            let changed = self
                .connection
                .execute(&sql, values)
                .await
                .map_err(|e| OutboxStoreError::Database(e.to_string()))?;
            self.check_operational_physical_in_transaction().await?;
            Ok(changed)
        }
        .await;
        match result {
            Ok(changed) => {
                tx.commit()
                    .await
                    .map_err(|e| OutboxStoreError::Database(e.to_string()))?;
                if changed > 0 {
                    self.priority_change_notifier(priority).notify_waiters();
                }
                Ok(changed)
            }
            Err(error) => {
                let _ = tx.rollback().await;
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn recovery_hint_lookup_uses_exact_scope_index_without_payload_sorting() {
        let path =
            std::env::temp_dir().join(format!("aqe-recovery-plan-{}.db", uuid::Uuid::new_v4()));
        let store = OutboxStore::open(&path).await.unwrap();
        let mut rows = store
            .connection
            .query(
                &format!("EXPLAIN QUERY PLAN {RETRY_HINTS_QUERY}"),
                params![
                    "runtime",
                    "strategy",
                    "session",
                    "critical",
                    now_epoch_micros()
                ],
            )
            .await
            .unwrap();
        let mut plan = String::new();
        while let Some(row) = rows.next().await.unwrap() {
            plan.push_str(&row.get::<String>(3).unwrap());
        }
        assert!(plan.contains("idx_outbox_runtime_pending"), "{plan}");
        assert!(!plan.contains("TEMP B-TREE"), "{plan}");
        assert!(!plan.contains("SCAN outbox_events"), "{plan}");
        drop(rows);
        drop(store);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }
}
