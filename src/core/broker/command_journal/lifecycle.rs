//! Durable strategy/account admission epochs. A stopped epoch can only be
//! replaced after all its commands and canonical exposure have been settled.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExecutionPhase {
    Running,
    Stopping,
    Stopped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ExecutionLifecycle {
    pub epoch: u64,
    pub phase: ExecutionPhase,
    pub stop_requested_at_us: Option<i64>,
}

impl ExecutionLifecycle {
    pub(super) async fn install(connection: &Connection) -> Result<Self, BrokerCommandError> {
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS broker_execution_lifecycle (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                version INTEGER NOT NULL, epoch INTEGER NOT NULL CHECK(epoch>0),
                phase TEXT NOT NULL CHECK(phase IN ('running','stopping','stopped')),
                stop_requested_at_us INTEGER
             );
             INSERT INTO broker_execution_lifecycle(singleton,version,epoch,phase)
                VALUES(1,1,1,'running') ON CONFLICT(singleton) DO NOTHING;",
            )
            .await
            .map_err(journal_error)?;
        let mut rows = connection.query(
            "SELECT version,epoch,phase,stop_requested_at_us FROM broker_execution_lifecycle WHERE singleton=1", ()
        ).await.map_err(journal_error)?;
        let row = rows
            .next()
            .await
            .map_err(journal_error)?
            .ok_or_else(|| journal_error("missing execution lifecycle"))?;
        if row.get::<i64>(0).map_err(journal_error)? != 1 {
            return Err(journal_error("unsupported execution lifecycle version"));
        }
        let epoch =
            u64::try_from(row.get::<i64>(1).map_err(journal_error)?).map_err(journal_error)?;
        let phase = match row.get::<String>(2).map_err(journal_error)?.as_str() {
            "running" => ExecutionPhase::Running,
            "stopping" => ExecutionPhase::Stopping,
            "stopped" => ExecutionPhase::Stopped,
            _ => return Err(journal_error("unsupported execution lifecycle phase")),
        };
        let stop_requested_at_us: Option<i64> = row.get(3).map_err(journal_error)?;
        if epoch == 0 || (phase == ExecutionPhase::Running) != stop_requested_at_us.is_none() {
            return Err(journal_error("inconsistent execution lifecycle"));
        }
        Ok(Self {
            epoch,
            phase,
            stop_requested_at_us,
        })
    }
}

impl BrokerCommandJournal {
    pub(crate) fn execution_lifecycle(&self) -> ExecutionLifecycle {
        self.lifecycle
    }

    /// Idempotent, FULL-synchronous stop acknowledgement. Memory gating occurs
    /// before this I/O, but a caller must not claim durability before it returns.
    pub(crate) async fn request_execution_stop(&mut self) -> Result<(), BrokerCommandError> {
        if self.lifecycle.phase != ExecutionPhase::Running {
            return Ok(());
        }
        let time = chrono::Utc::now().timestamp_micros();
        let changed = self.connection.execute(
            "UPDATE broker_execution_lifecycle SET phase='stopping',stop_requested_at_us=?1 WHERE singleton=1 AND version=1 AND epoch=?2 AND phase='running'",
            params![time, self.lifecycle.epoch as i64],
        ).await.map_err(journal_error)?;
        if changed != 1 {
            return Err(journal_error("execution stop fence changed unexpectedly"));
        }
        self.lifecycle.phase = ExecutionPhase::Stopping;
        self.lifecycle.stop_requested_at_us = Some(time);
        Ok(())
    }

    /// Commit completion only after bounded indexed checks prove no unresolved
    /// command or materialized exposure/continuation remains. Missing broker
    /// events keep the scope Stopping; no terminal Insight is fabricated here.
    pub(crate) async fn complete_execution_stop(&mut self) -> Result<bool, BrokerCommandError> {
        if self.lifecycle.phase == ExecutionPhase::Running {
            return Err(journal_error("execution stop was not durably requested"));
        }
        self.ensure_execution_graph_index().await?;
        let tx = self
            .connection
            .transaction_with_behavior(turso::transaction::TransactionBehavior::Immediate)
            .await
            .map_err(journal_error)?;
        if !settled(&tx).await? {
            tx.rollback().await.map_err(journal_error)?;
            return Ok(false);
        }
        let changed = tx.execute(
            "UPDATE broker_execution_lifecycle SET phase='stopped' WHERE singleton=1 AND version=1 AND epoch=?1 AND phase IN ('stopping','stopped')",
            params![self.lifecycle.epoch as i64],
        ).await.map_err(journal_error)?;
        if changed != 1 {
            return Err(journal_error(
                "execution stop completion fence changed unexpectedly",
            ));
        }
        tx.commit().await.map_err(journal_error)?;
        self.lifecycle.phase = ExecutionPhase::Stopped;
        Ok(true)
    }

    /// Explicit startup may open a new epoch after confirmed completion. An
    /// unfinished stop is never cleared by restarting/reconnecting the process.
    pub(crate) async fn begin_execution_epoch(
        &mut self,
    ) -> Result<ExecutionLifecycle, BrokerCommandError> {
        if self.lifecycle.phase != ExecutionPhase::Stopped {
            return Ok(self.lifecycle);
        }
        self.ensure_execution_graph_index().await?;
        let next = self
            .lifecycle
            .epoch
            .checked_add(1)
            .filter(|epoch| *epoch <= i64::MAX as u64)
            .ok_or_else(|| journal_error("execution epoch exhausted"))?;
        let tx = self
            .connection
            .transaction_with_behavior(turso::transaction::TransactionBehavior::Immediate)
            .await
            .map_err(journal_error)?;
        if !settled(&tx).await? {
            tx.rollback().await.map_err(journal_error)?;
            return Err(journal_error(
                "stopped execution epoch still owns unresolved work",
            ));
        }
        let changed = tx.execute(
            "UPDATE broker_execution_lifecycle SET epoch=?1,phase='running',stop_requested_at_us=NULL WHERE singleton=1 AND version=1 AND epoch=?2 AND phase='stopped'",
            params![next as i64, self.lifecycle.epoch as i64],
        ).await.map_err(journal_error)?;
        if changed != 1 {
            return Err(journal_error("execution epoch fence changed unexpectedly"));
        }
        tx.commit().await.map_err(journal_error)?;
        self.lifecycle = ExecutionLifecycle {
            epoch: next,
            phase: ExecutionPhase::Running,
            stop_requested_at_us: None,
        };
        Ok(self.lifecycle)
    }
}

async fn settled(tx: &turso::transaction::Transaction<'_>) -> Result<bool, BrokerCommandError> {
    let mut commands = tx.query(
        "SELECT 1 FROM broker_commands WHERE state IN ('queued','dispatching','completed') LIMIT 1", (),
    ).await.map_err(journal_error)?;
    if commands.next().await.map_err(journal_error)?.is_some() {
        return Ok(false);
    }
    drop(commands);
    let mut rows = tx
        .query(
            "SELECT 1 FROM broker_execution_graph WHERE recovery_seed=1 LIMIT 1",
            (),
        )
        .await
        .map_err(journal_error)?;
    Ok(rows.next().await.map_err(journal_error)?.is_none())
}

#[cfg(test)]
mod tests;
