//! Durable ownership of broker mutations, separate from cloud-event delivery.
//! Only a successful FULL-synchronous journal transition permits broker dispatch.
use super::commands::{
    BrokerCommand, BrokerCommandCompletion, BrokerCommandError, BrokerCommandId,
    BrokerCommandOutcome,
};
use std::{fs::File, path::{Path,PathBuf}};
use turso::{Connection, params};

mod checkpoints;
mod source_identity;
mod fills;
pub(crate) mod marks;
pub use fills::ExecutionFillBatch;
pub(crate) mod lifecycle;
mod reconciliation;
mod cancellation_recovery;
mod analytics_capture;
mod cloud_history;
pub use analytics_capture::{AnalyticsCapturePause,AnalyticsCaptureHandoffReceipt,AnalyticsCaptureStatus};
mod exited_capture;
pub use exited_capture::{ExitedAnalyticsCapture,ExitedAnalyticsCaptureError};
pub(crate) mod recovery;
pub(crate) use checkpoints::{ExecutionCheckpointBatch, ExecutionCheckpointPage, ExecutionIntent};

const RECOVERY_PAGE_LIMIT: usize = 256;

fn journal_error(error: impl std::fmt::Display) -> BrokerCommandError {
    BrokerCommandError::Journal(error.to_string())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JournalCommandState {
    Queued,
    Dispatching,
    Completed,
    Applied,
}

impl JournalCommandState {
    fn parse(value: &str) -> Result<Self, BrokerCommandError> {
        match value {
            "queued" => Ok(Self::Queued),
            "dispatching" => Ok(Self::Dispatching),
            "completed" => Ok(Self::Completed),
            "applied" => Ok(Self::Applied),
            _ => Err(journal_error("unsupported command journal state")),
        }
    }
}

#[derive(Debug)]
pub struct JournalCommandRecord {
    pub sequence: i64,
    pub command: BrokerCommand,
    pub state: JournalCommandState,
    pub queued_at_us: i64,
    pub dispatched_at_us: Option<i64>,
    pub completion: Option<BrokerCommandCompletion>,
}

pub(crate) struct RecoveryCommand {
    pub admission_order: u64,
    pub record: JournalCommandRecord,
}

/// Intentionally not Clone. One worker owns this connection and its process lock.
/// Two live processes must not dispatch the same strategy/account journal.
pub struct BrokerCommandJournal {
    connection: Connection,
    path: PathBuf,
    analytics_capture_budget: Option<crate::core::live_sync::PhysicalCaptureLimit>,
    cloud_history: Option<cloud_history::CloudHistoryStore>,
    strategy_id: String,
    account_id: String,
    evidence_source_id: String,
    lifecycle: lifecycle::ExecutionLifecycle,
    _process_lock: File,
}

impl BrokerCommandJournal {
    pub async fn open(
        path: impl AsRef<Path>,
        strategy_id: &str,
        account_id: &str,
    ) -> Result<Self, BrokerCommandError> {
        Self::open_configured(path, strategy_id, account_id, None).await
    }

    /// Startup-only capacity selection, including resuming a previously
    /// refused optional-history copy. Never deletes accepted history to fit.
    /// The exclusive broker lock must be available; a running owner is refused.
    pub async fn open_with_cloud_history_capacity(
        path: impl AsRef<Path>,strategy_id:&str,account_id:&str,
        limit:crate::core::live_sync::PhysicalCaptureLimit,
    )->Result<Self,BrokerCommandError>{
        let limit=limit.validate().map_err(journal_error)?;
        let mut journal=Self::open_configured(path,strategy_id,account_id,Some(limit)).await?;
        Box::pin(cloud_history::migrate(&mut journal,limit)).await?;
        journal.with_analytics_capture_budget(limit).await
    }

    async fn open_configured(
        path: impl AsRef<Path>,strategy_id:&str,account_id:&str,
        cloud_capacity:Option<crate::core::live_sync::PhysicalCaptureLimit>,
    )->Result<Self,BrokerCommandError>{
        let strategy_id = strategy_id.trim().to_owned();
        let account_id = account_id.trim().to_owned();
        if strategy_id.is_empty() || account_id.is_empty() {
            return Err(journal_error("missing journal scope"));
        }
        let path = path.as_ref().to_path_buf();
        let lock_path = path.with_extension("commands.lock");
        let parent = path.parent().map(Path::to_path_buf);
        let process_lock =
            tokio::task::spawn_blocking(move || -> Result<File, BrokerCommandError> {
                if let Some(parent) = parent.filter(|p| !p.as_os_str().is_empty()) {
                    std::fs::create_dir_all(parent).map_err(journal_error)?;
                }
                let file = File::options()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(lock_path)
                    .map_err(journal_error)?;
                file.try_lock()
                    .map_err(|_| journal_error("command journal already has a live owner"))?;
                Ok(file)
            })
            .await
            .map_err(journal_error)??;
        let database = turso::Builder::new_local(path.to_string_lossy().as_ref())
            .build()
            .await
            .map_err(journal_error)?;
        let connection = database.connect().map_err(journal_error)?;
        crate::core::utils::storage_format::require_current(&connection, "broker journal", 1095846482, 2)
            .await.map_err(journal_error)?;
        let mut rows = connection
            .query("PRAGMA journal_mode = WAL", ())
            .await
            .map_err(journal_error)?;
        while rows.next().await.map_err(journal_error)?.is_some() {}
        drop(rows);
        connection.execute_batch(
            "PRAGMA application_id = 1095846482; PRAGMA user_version = 2;
             PRAGMA synchronous = FULL;
             PRAGMA busy_timeout = 5000;
             PRAGMA cache_size = -2048;
             CREATE TABLE IF NOT EXISTS broker_command_scope (
                 singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                 strategy_id TEXT NOT NULL, account_id TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS broker_commands (
                 sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                 command_id TEXT NOT NULL UNIQUE, command_json TEXT NOT NULL,
                 state TEXT NOT NULL CHECK(state IN ('queued','dispatching','completed','applied')),
                 queued_at_us INTEGER NOT NULL, dispatched_at_us INTEGER,
                 completion_json TEXT
             );
             CREATE INDEX IF NOT EXISTS idx_broker_commands_recovery ON broker_commands(state, sequence);
             CREATE TABLE IF NOT EXISTS broker_command_admission (
                 command_id TEXT PRIMARY KEY,
                 admission_order INTEGER NOT NULL UNIQUE CHECK(admission_order > 0)
             );
             CREATE TABLE IF NOT EXISTS broker_insight_checkpoints (
                 insight_id TEXT PRIMARY KEY, revision INTEGER NOT NULL CHECK(revision > 0),
                 checkpoint_json TEXT NOT NULL,
                 execution_epoch INTEGER NOT NULL DEFAULT 0,
                 order_id TEXT, symbol TEXT,
                 materialized INTEGER NOT NULL DEFAULT 0
             );
             CREATE TABLE IF NOT EXISTS broker_execution_graph (
                 insight_id TEXT PRIMARY KEY, revision INTEGER NOT NULL,
                 recovery_seed INTEGER NOT NULL, metadata_json TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_execution_graph_seed ON broker_execution_graph(recovery_seed, insight_id);
             CREATE TABLE IF NOT EXISTS broker_execution_recovery_meta (
                 singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                 graph_version INTEGER NOT NULL, graph_cursor TEXT NOT NULL,
                 graph_complete INTEGER NOT NULL CHECK(graph_complete IN (0,1))
             );
             INSERT INTO broker_execution_recovery_meta(singleton,graph_version,graph_cursor,graph_complete)
                 VALUES(1,1,'',0) ON CONFLICT(singleton) DO NOTHING;"
        ).await.map_err(journal_error)?;
        connection.execute_batch(cancellation_recovery::SCHEMA).await.map_err(journal_error)?;
        connection.execute(
            "INSERT INTO broker_command_scope(singleton,strategy_id,account_id) VALUES(1,?1,?2) ON CONFLICT(singleton) DO NOTHING",
            params![strategy_id.clone(), account_id.clone()],
        ).await.map_err(journal_error)?;
        let mut rows = connection
            .query(
                "SELECT strategy_id,account_id FROM broker_command_scope WHERE singleton=1",
                (),
            )
            .await
            .map_err(journal_error)?;
        let row = rows
            .next()
            .await
            .map_err(journal_error)?
            .ok_or_else(|| journal_error("missing journal scope"))?;
        if row.get::<String>(0).map_err(journal_error)? != strategy_id
            || row.get::<String>(1).map_err(journal_error)? != account_id
        {
            return Err(journal_error(
                "journal belongs to another strategy or broker account",
            ));
        }
        drop(rows);
        connection
            .execute_batch(source_identity::SCHEMA)
            .await
            .map_err(journal_error)?;
        let evidence_source_id = source_identity::install_source(&connection).await?;
        let lifecycle = lifecycle::ExecutionLifecycle::install(&connection).await?;
        checkpoints::install_owner_indexes(&connection).await?;
        if !cloud_history::has_location(&connection).await? {
            connection.execute_batch(fills::SCHEMA).await.map_err(journal_error)?;
            connection.execute_batch(marks::SCHEMA).await.map_err(journal_error)?;
        }
        let mut journal = Self {
            connection,
            path,
            analytics_capture_budget: None,
            cloud_history: None,
            strategy_id,
            account_id,
            evidence_source_id,
            lifecycle,
            _process_lock: process_lock,
        };
        Box::pin(cloud_history::reopen(&mut journal,cloud_capacity)).await?;
        Ok(journal)
    }

    pub fn scope(&self) -> (&str, &str) {
        (&self.strategy_id, &self.account_id)
    }

    pub(crate) async fn last_admission_order(&self) -> Result<u64, BrokerCommandError> {
        let mut rows = self.connection.query(
            "SELECT admission_order FROM broker_command_admission ORDER BY admission_order DESC LIMIT 1", (),
        ).await.map_err(journal_error)?;
        let Some(row) = rows.next().await.map_err(journal_error)? else {
            return Ok(0);
        };
        let value: i64 = row.get(0).map_err(journal_error)?;
        u64::try_from(value).map_err(journal_error)
    }

    /// Startup-only: low-level/legacy intents without owner ordering must not be
    /// silently omitted from recovery or guessed into a new dispatch sequence.
    pub(crate) async fn verify_recovery_ordering(&self) -> Result<(), BrokerCommandError> {
        let mut rows = self.connection.query(
            "SELECT c.command_id FROM broker_commands c WHERE c.state IN ('queued','dispatching','completed')
             AND NOT EXISTS (SELECT 1 FROM broker_command_admission a WHERE a.command_id=c.command_id) LIMIT 1", (),
        ).await.map_err(journal_error)?;
        if rows.next().await.map_err(journal_error)?.is_some() {
            return Err(journal_error(
                "legacy command is missing durable owner admission order; reconciliation is required",
            ));
        }
        Ok(())
    }

    pub(crate) async fn ordered_recovery_page(
        &self,
        after: u64,
        limit: usize,
    ) -> Result<Vec<RecoveryCommand>, BrokerCommandError> {
        let after = i64::try_from(after).map_err(journal_error)?;
        let mut rows = self.connection.query(
            "SELECT c.sequence,c.command_json,c.state,c.queued_at_us,c.dispatched_at_us,c.completion_json,a.admission_order
             FROM broker_command_admission a JOIN broker_commands c ON c.command_id=a.command_id
             WHERE a.admission_order>?1 AND c.state IN ('queued','dispatching','completed')
             ORDER BY a.admission_order LIMIT ?2", params![after, limit.clamp(1, RECOVERY_PAGE_LIMIT) as i64],
        ).await.map_err(journal_error)?;
        let mut page = Vec::new();
        while let Some(row) = rows.next().await.map_err(journal_error)? {
            let order: i64 = row.get(6).map_err(journal_error)?;
            page.push(RecoveryCommand {
                admission_order: u64::try_from(order).map_err(journal_error)?,
                record: Self::decode(row)?,
            });
        }
        Ok(page)
    }

    /// An identical retry returns the durable disposition. In particular, a
    /// Dispatching record must be reconciled; this API never resets it to Queued.
    pub async fn prepare(
        &mut self,
        command: &BrokerCommand,
        queued_at_us: i64,
    ) -> Result<JournalCommandRecord, BrokerCommandError> {
        if command.strategy_id != self.strategy_id || command.account_id != self.account_id {
            return Err(journal_error("command does not match journal scope"));
        }
        let encoded = command.encode()?;
        let inserted = self.connection.execute(
            "INSERT INTO broker_commands(command_id,command_json,state,queued_at_us) VALUES(?1,?2,'queued',?3) ON CONFLICT(command_id) DO NOTHING",
            params![command.id.0.clone(), encoded, queued_at_us],
        ).await.map_err(journal_error)?;
        if inserted == 1 {
            // The journal has one owner; no other statement can race this rowid.
            // Avoid rereading and decoding every newly accepted immutable request.
            return Ok(JournalCommandRecord {
                sequence: self.connection.last_insert_rowid(),
                command: command.clone(),
                state: JournalCommandState::Queued,
                queued_at_us,
                dispatched_at_us: None,
                completion: None,
            });
        }
        let record = self
            .get(&command.id)
            .await?
            .ok_or_else(|| journal_error("prepared intent not found"))?;
        if record.command != *command {
            return Err(BrokerCommandError::IdentityConflict);
        }
        Ok(record)
    }

    pub async fn get(
        &self,
        id: &BrokerCommandId,
    ) -> Result<Option<JournalCommandRecord>, BrokerCommandError> {
        let mut rows = self.connection.query(
            "SELECT sequence,command_json,state,queued_at_us,dispatched_at_us,completion_json FROM broker_commands WHERE command_id=?1",
            params![id.0.clone()],
        ).await.map_err(journal_error)?;
        rows.next()
            .await
            .map_err(journal_error)?
            .map(Self::decode)
            .transpose()
    }

    pub async fn mark_dispatched(
        &mut self,
        id: &BrokerCommandId,
        at_us: i64,
    ) -> Result<(), BrokerCommandError> {
        let changed = self.connection.execute(
            "UPDATE broker_commands SET state='dispatching',dispatched_at_us=?2 WHERE command_id=?1 AND state='queued'",
            params![id.0.clone(), at_us],
        ).await.map_err(journal_error)?;
        if changed != 1 {
            return Err(journal_error("dispatch requires a queued durable intent"));
        }
        Ok(())
    }

    /// Restore a definitely-unpolled dispatch to its durable queued state.
    ///
    /// This is only used by the live worker when it still owns an *unpolled*
    /// broker future. Recovered Dispatching records never call this method.
    pub(crate) async fn defer_unpolled_dispatch(
        &mut self,
        id: &BrokerCommandId,
        dispatched_at_us: i64,
    ) -> Result<(), BrokerCommandError> {
        let changed = self.connection.execute(
            "UPDATE broker_commands SET state='queued',dispatched_at_us=NULL WHERE command_id=?1 AND state='dispatching' AND dispatched_at_us=?2 AND completion_json IS NULL",
            params![id.0.clone(), dispatched_at_us],
        ).await.map_err(journal_error)?;
        if changed != 1 {
            return Err(journal_error("deferral lost unpolled dispatch ownership"));
        }
        Ok(())
    }

    /// A completion is durable before it is exposed to the strategy reducer.
    pub async fn complete(
        &mut self,
        completion: &BrokerCommandCompletion,
    ) -> Result<(), BrokerCommandError> {
        let id = &completion.receipt.id;
        let record = self
            .get(id)
            .await?
            .ok_or_else(|| journal_error("completion without intent"))?;
        let cancelled_without_dispatch = matches!(
            completion.outcome,
            BrokerCommandOutcome::CancelledBeforeDispatch
        ) && completion.dispatched_at_us.is_none();
        if record.command.receipt() != completion.receipt
            || record.queued_at_us != completion.queued_at_us
            || (record.dispatched_at_us != completion.dispatched_at_us
                && !cancelled_without_dispatch)
        {
            return Err(BrokerCommandError::IdentityConflict);
        }
        let json = serde_json::to_string(completion).map_err(journal_error)?;
        if let Some(existing) = record.completion {
            if serde_json::to_string(&existing).map_err(journal_error)? == json {
                return Ok(());
            }
            return Err(BrokerCommandError::IdentityConflict);
        }
        let changed = self.connection.execute(
            "UPDATE broker_commands SET state='completed',completion_json=?2,dispatched_at_us=?3 WHERE command_id=?1 AND state IN ('queued','dispatching')",
            params![id.0.clone(), json, completion.dispatched_at_us],
        ).await.map_err(journal_error)?;
        if changed != 1 {
            return Err(journal_error("completion lost journal ownership"));
        }
        Ok(())
    }

    /// Deterministic bounded recovery; the owner decides whether queued intents
    /// are still valid. Dispatching intents are never automatically replayed.
    pub async fn recovery_page(
        &self,
        after_sequence: i64,
        limit: usize,
    ) -> Result<Vec<JournalCommandRecord>, BrokerCommandError> {
        let limit = limit.clamp(1, RECOVERY_PAGE_LIMIT) as i64;
        let mut rows = self.connection.query(
            "SELECT sequence,command_json,state,queued_at_us,dispatched_at_us,completion_json
             FROM broker_commands WHERE state IN ('queued','dispatching','completed') AND sequence>?1
             ORDER BY sequence LIMIT ?2", params![after_sequence, limit],
        ).await.map_err(journal_error)?;
        let mut records = Vec::with_capacity(limit as usize);
        while let Some(row) = rows.next().await.map_err(journal_error)? {
            records.push(Self::decode(row)?);
        }
        Ok(records)
    }

    fn decode(row: turso::Row) -> Result<JournalCommandRecord, BrokerCommandError> {
        let command: BrokerCommand =
            serde_json::from_str(&row.get::<String>(1).map_err(journal_error)?)
                .map_err(journal_error)?;
        command.validate()?;
        let state = JournalCommandState::parse(&row.get::<String>(2).map_err(journal_error)?)?;
        let completion = row
            .get::<Option<String>>(5)
            .map_err(journal_error)?
            .map(|json| {
                serde_json::from_str::<BrokerCommandCompletion>(&json).map_err(journal_error)
            })
            .transpose()?;
        if matches!(
            state,
            JournalCommandState::Completed | JournalCommandState::Applied
        ) != completion.is_some()
        {
            return Err(journal_error("inconsistent journal completion"));
        }
        Ok(JournalCommandRecord {
            sequence: row.get(0).map_err(journal_error)?,
            command,
            state,
            queued_at_us: row.get(3).map_err(journal_error)?,
            dispatched_at_us: row.get(4).map_err(journal_error)?,
            completion,
        })
    }
}
