//! Compact stop facts have independent logical headroom, never a data-cap bypass.
use super::*;
use aqe_live_protocol::SafetyObservation;
use std::collections::HashSet;

pub const MAX_SAFETY_ENTITIES_PER_RUN: usize = 257; // one lifecycle + 256 stop actions

fn db(error: impl std::fmt::Display) -> OutboxStoreError {
    OutboxStoreError::Database(error.to_string())
}

impl OutboxStore {
    pub(super) async fn ensure_safety_headroom_columns(&self) -> Result<(), OutboxStoreError> {
        self.execute_bounded_metadata_batch(
            "CREATE INDEX IF NOT EXISTS idx_safety_scope ON safety_observations(strategy_id,live_session_id,delivered);
             CREATE INDEX IF NOT EXISTS idx_safety_transport ON safety_observations(delivered,next_attempt_at_us);
             CREATE INDEX IF NOT EXISTS idx_safety_transport_fresh ON safety_observations(delivered,next_attempt_at_us,updated_at_us DESC);
             CREATE INDEX IF NOT EXISTS idx_safety_cleanup ON safety_observations(full_evidence_required,delivered,updated_at_us);
             CREATE INDEX IF NOT EXISTS idx_safety_event ON safety_observations(event_id);"
        ).await?;
        Ok(())
    }

    pub(super) async fn record_safety_observation_locked(
        &self,
        observation: &SafetyObservation,
        full_evidence: bool,
    ) -> Result<SafetyObservation, OutboxStoreError> {
        observation
            .validate()
            .map_err(|e| OutboxStoreError::InvalidRecord(e.into()))?;
        let wire = serde_json::to_string(observation).map_err(db)?;
        let mut same_id=self.connection.query(
            "SELECT observation FROM safety_observations INDEXED BY idx_safety_event WHERE event_id=?1",
            params![observation.event_id.to_string()],
        ).await.map_err(db)?;
        while let Some(row) = same_id.next().await.map_err(db)? {
            let saved: SafetyObservation =
                serde_json::from_str(&row.get::<String>(0).map_err(db)?).map_err(db)?;
            if &saved != observation {
                return Err(OutboxStoreError::InvalidRecord(
                    "safety event identity was reused for a different fact".into(),
                ));
            }
        }
        drop(same_id);
        let rank = observation.state_rank();
        self.connection.execute(
            "INSERT INTO safety_observations(runtime_id,strategy_id,live_session_id,run_id,entity_key,
                event_id,revision,terminal,state_rank,observation,delivered,next_attempt_at_us,full_evidence_required,updated_at_us)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,0,0,?11,?12)
             ON CONFLICT(runtime_id,strategy_id,live_session_id,run_id,entity_key) DO UPDATE SET
                event_id=excluded.event_id,revision=excluded.revision,terminal=excluded.terminal,
                state_rank=excluded.state_rank,observation=excluded.observation,
                delivered=CASE WHEN safety_observations.event_id=excluded.event_id THEN safety_observations.delivered ELSE 0 END,
                next_attempt_at_us=CASE WHEN safety_observations.event_id=excluded.event_id THEN safety_observations.next_attempt_at_us ELSE 0 END,
                full_evidence_required=CASE WHEN safety_observations.event_id=excluded.event_id
                    THEN MAX(safety_observations.full_evidence_required,excluded.full_evidence_required) ELSE excluded.full_evidence_required END,
                updated_at_us=excluded.updated_at_us
             WHERE (safety_observations.event_id=excluded.event_id AND safety_observations.observation=excluded.observation)
                OR (safety_observations.terminal=0 AND excluded.revision>safety_observations.revision
                    AND excluded.state_rank>=safety_observations.state_rank)",
            params![observation.runtime_id.clone(),observation.strategy_id.clone(),observation.live_session_id.clone(),
                observation.run_id.clone(),observation.entity_key().to_owned(),observation.event_id.to_string(),
                observation.revision as i64,i64::from(observation.terminal()),rank,wire,i64::from(full_evidence),now_epoch_micros()],
        ).await.map_err(db)?;
        let mut rows=self.connection.query(
            "SELECT observation FROM safety_observations
             WHERE runtime_id=?1 AND strategy_id=?2 AND live_session_id=?3 AND run_id=?4 AND entity_key=?5",
            params![observation.runtime_id.clone(),observation.strategy_id.clone(),observation.live_session_id.clone(),
                observation.run_id.clone(),observation.entity_key().to_owned()],
        ).await.map_err(db)?;
        let wire: String = rows
            .next()
            .await
            .map_err(db)?
            .ok_or_else(|| db("safety write missing"))?
            .get(0)
            .map_err(db)?;
        serde_json::from_str(&wire).map_err(db)
    }

    /// Accept ONLY bounded stop lifecycle/action facts. The returned observations
    /// are operational receipts, NOT full-event durability or sequence receipts.
    /// All facts belong to one exact run; repeated updates compact by entity.
    pub async fn record_safety_only(
        &self,
        observations: &[SafetyObservation],
    ) -> Result<Vec<SafetyObservation>, OutboxStoreError> {
        let Some(first) = observations.first() else {
            return Ok(Vec::new());
        };
        if observations.len() > MAX_SAFETY_ENTITIES_PER_RUN {
            return Err(OutboxStoreError::InvalidRecord(
                "safety batch exceeds reserved headroom".into(),
            ));
        }
        for observation in observations {
            observation
                .validate()
                .map_err(|e| OutboxStoreError::InvalidRecord(e.into()))?;
            if !observation.same_run(first) {
                return Err(OutboxStoreError::InvalidRecord(
                    "safety batch crosses run ownership".into(),
                ));
            }
        }
        let _operation = self.operation_gate.lock().await;
        // A cancelled control/receiver must not strand a raw BEGIN. Turso's
        // guard rolls back on the next connection access after cancellation.
        let transaction = turso::transaction::Transaction::new_unchecked(
            &self.connection,
            turso::transaction::TransactionBehavior::Immediate,
        )
        .await
        .map_err(db)?;
        let result=async {
            let mut rows=self.connection.query(
                "SELECT entity_key FROM safety_observations
                 WHERE runtime_id=?1 AND strategy_id=?2 AND live_session_id=?3 AND run_id=?4 LIMIT 258",
                params![first.runtime_id.clone(),first.strategy_id.clone(),first.live_session_id.clone(),first.run_id.clone()],
            ).await.map_err(db)?;
            let mut entities=HashSet::new();
            while let Some(row)=rows.next().await.map_err(db)? {entities.insert(row.get::<String>(0).map_err(db)?);}
            drop(rows);
            entities.extend(observations.iter().map(|o|o.entity_key().to_owned()));
            if entities.len()>MAX_SAFETY_ENTITIES_PER_RUN || entities.iter().filter(|key|key.as_str()!="lifecycle").count()>256 {
                return Err(OutboxStoreError::InvalidRecord("stop-action headroom exhausted".into()));
            }
            let mut receipts=Vec::with_capacity(observations.len());
            for observation in observations {
                let stored=self.record_safety_observation_locked(observation,false).await?;
                if !stored.covers(observation) {
                    return Err(OutboxStoreError::InvalidRecord("safety fact conflicts with retained terminal state".into()));
                }
                receipts.push(stored);
            }
            Ok(receipts)
        }.await;
        match result {
            Ok(receipts) => {
                self.commit_bounded_metadata(transaction).await?;
                self.safety_notify.notify_waiters();
                Ok(receipts)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    /// Small global page for the publisher's one local safety-handoff worker.
    pub(crate) async fn pending_safety_handoffs(
        &self,
    ) -> Result<Vec<SafetyObservation>, OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        let mut rows = self
            .connection
            .query(
                "SELECT observation FROM safety_observations INDEXED BY idx_safety_transport_fresh
             WHERE delivered=0 AND next_attempt_at_us<=?1
             ORDER BY next_attempt_at_us,updated_at_us DESC LIMIT 8",
                params![now_epoch_micros()],
            )
            .await
            .map_err(db)?;
        let mut observations = Vec::new();
        while let Some(row) = rows.next().await.map_err(db)? {
            observations
                .push(serde_json::from_str(&row.get::<String>(0).map_err(db)?).map_err(db)?);
        }
        Ok(observations)
    }

    /// Process-exit recovery transfers compact facts before ordinary backlog.
    /// A crash after target commit but before source settlement repeats an
    /// idempotent handoff; it never releases the only durable owner early.
    pub async fn handoff_pending_safety_to(
        &self,
        target: &OutboxStore,
        runtime: &str,
        strategy: &str,
        session: &str,
        run: &str,
    ) -> Result<usize, OutboxStoreError> {
        let pending = self
            .pending_safety_observations(runtime, strategy, session, run)
            .await?;
        if pending.is_empty() {
            return Ok(0);
        }
        let receipts = target.record_safety_only(&pending).await?;
        if receipts.len() != pending.len()
            || receipts
                .iter()
                .zip(&pending)
                .any(|(saved, requested)| !saved.covers(requested))
        {
            return Err(OutboxStoreError::InvalidRecord(
                "incomplete operational handoff proof".into(),
            ));
        }
        for fact in &pending {
            self.settle_safety_observation(fact, true).await?;
        }
        Ok(pending.len())
    }
}

#[cfg(test)]
mod tests;
