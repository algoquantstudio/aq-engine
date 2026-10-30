//! Audited retries of an existing Cancel intent, never entry replay. The old
//! ambiguous completion stays durable throughout I/O and across process death.
use super::*;
use crate::core::broker::{
    commands::BrokerCommandAction,
    types::{OrderLifecycleSnapshot, OrderLifecycleStatus},
};

pub(super) const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS broker_cancel_recovery_attempts (
    command_id TEXT NOT NULL, attempt INTEGER NOT NULL CHECK(attempt > 0),
    attempted_at_us INTEGER NOT NULL, pending_evidence_json TEXT NOT NULL,
    previous_completion_json TEXT NOT NULL, result_json TEXT,
    PRIMARY KEY(command_id,attempt)
);";

impl BrokerCommandJournal {
    pub(crate) async fn begin_cancellation_recovery(
        &mut self,
        expected: &BrokerCommand,
        proof: &OrderLifecycleSnapshot,
        at: i64,
    ) -> Result<Option<i64>, BrokerCommandError> {
        let record = self
            .get(&expected.id)
            .await?
            .ok_or(BrokerCommandError::IdentityConflict)?;
        let BrokerCommandAction::Cancel { order_id } = &expected.action else {
            return Err(BrokerCommandError::IdentityConflict);
        };
        if record.command != *expected
            || expected.strategy_id != self.strategy_id
            || expected.account_id != self.account_id
            || order_id.is_empty()
            || proof.order_id != *order_id
            || proof.status != OrderLifecycleStatus::Pending
            || proof.position_id.is_some()
            || proof.position_qty.is_some()
            || proof
                .symbol
                .as_ref()
                .is_none_or(|symbol| symbol.trim().is_empty())
        {
            return Err(BrokerCommandError::IdentityConflict);
        }
        if record.state == JournalCommandState::Applied {
            return Ok(None);
        }
        let old = record
            .completion
            .ok_or(BrokerCommandError::IdentityConflict)?;
        if record.state != JournalCommandState::Completed
            || !matches!(old.outcome, BrokerCommandOutcome::Ambiguous { .. })
        {
            return Ok(None);
        }
        let old = serde_json::to_string(&old).map_err(journal_error)?;
        let proof = serde_json::to_string(proof).map_err(journal_error)?;
        // Single worker owns this connection. FULL-synchronous insertion must
        // succeed before any transport poll. Preserve original dispatch/receipt.
        self.connection.execute(
            "INSERT INTO broker_cancel_recovery_attempts(command_id,attempt,attempted_at_us,pending_evidence_json,previous_completion_json)
             SELECT ?1,COALESCE(MAX(attempt),0)+1,?2,?3,?4 FROM broker_cancel_recovery_attempts WHERE command_id=?1",
            params![expected.id.0.clone(),at,proof,old],
        ).await.map_err(journal_error)?;
        let mut rows = self
            .connection
            .query(
                "SELECT MAX(attempt) FROM broker_cancel_recovery_attempts WHERE command_id=?1",
                params![expected.id.0.clone()],
            )
            .await
            .map_err(journal_error)?;
        let row = rows
            .next()
            .await
            .map_err(journal_error)?
            .ok_or(BrokerCommandError::IdentityConflict)?;
        Ok(Some(row.get::<i64>(0).map_err(journal_error)?))
    }

    pub(crate) async fn finish_cancellation_recovery(
        &mut self,
        expected: &BrokerCommand,
        attempt: i64,
        result: &BrokerCommandCompletion,
    ) -> Result<Option<BrokerCommandCompletion>, BrokerCommandError> {
        let record = self
            .get(&expected.id)
            .await?
            .ok_or(BrokerCommandError::IdentityConflict)?;
        if record.command != *expected
            || !matches!(expected.action, BrokerCommandAction::Cancel { .. })
            || expected.account_id != self.account_id
            || expected.strategy_id != self.strategy_id
            || result.receipt != expected.receipt()
            || result.queued_at_us != record.queued_at_us
            || result.dispatched_at_us != record.dispatched_at_us
        {
            return Err(BrokerCommandError::IdentityConflict);
        }
        let result_json = serde_json::to_string(result).map_err(journal_error)?;
        let changed = self.connection.execute(
            "UPDATE broker_cancel_recovery_attempts SET result_json=?3 WHERE command_id=?1 AND attempt=?2 AND result_json IS NULL",
            params![expected.id.0.clone(),attempt,result_json],
        ).await.map_err(journal_error)?;
        if changed != 1 {
            return Err(BrokerCommandError::IdentityConflict);
        }
        // A native terminal event may have settled the original while the retry
        // was awaiting I/O. Never resurrect it or overwrite its reducer result.
        if record.state == JournalCommandState::Applied {
            return Ok(None);
        }
        let old = record
            .completion
            .ok_or(BrokerCommandError::IdentityConflict)?;
        if record.state != JournalCommandState::Completed
            || !matches!(old.outcome, BrokerCommandOutcome::Ambiguous { .. })
        {
            return Err(BrokerCommandError::IdentityConflict);
        }
        if !matches!(result.outcome, BrokerCommandOutcome::Acknowledged) {
            return Ok(Some(old));
        }
        let before = serde_json::to_string(&old).map_err(journal_error)?;
        let after = serde_json::to_string(result).map_err(journal_error)?;
        let changed = self.connection.execute(
            "UPDATE broker_commands SET completion_json=?2 WHERE command_id=?1 AND state='completed' AND completion_json=?3",
            params![expected.id.0.clone(),after,before],
        ).await.map_err(journal_error)?;
        if changed != 1 {
            return Err(BrokerCommandError::IdentityConflict);
        }
        Ok(Some(result.clone()))
    }
}

#[cfg(test)]
mod tests;
