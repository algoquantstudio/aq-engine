//! Opt-in, offline migration of optional analytics into its own physical store.
//! The command-journal process lock covers both files. No broker command,
//! checkpoint, execution epoch or recovery graph is copied or quota-limited.
use super::*;
use crate::core::live_sync::PhysicalCaptureLimit;
use sha2::{Digest, Sha256};

const META: &str = "CREATE TABLE IF NOT EXISTS broker_cloud_history_location(
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL,
 state TEXT NOT NULL,physical_limit INTEGER NOT NULL,headroom INTEGER NOT NULL);";
const TABLES: &[&str] = &[
    "broker_execution_fill_history_clocks",
    "broker_execution_fill_export",
    "broker_execution_fills",
    "broker_execution_fill_heads",
    "broker_execution_fill_invalidated",
    "broker_execution_position_checks",
    "broker_position_marks",
    "broker_position_mark_heads",
    "broker_position_mark_export",
    "broker_analytics_capture",
];
const STORE_META: &str = "CREATE TABLE IF NOT EXISTS cloud_history_identity(
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL,
 strategy_id TEXT NOT NULL,account_id TEXT NOT NULL,source_id TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS cloud_history_copy_progress(
 table_name TEXT PRIMARY KEY,cursor INTEGER NOT NULL,complete INTEGER NOT NULL);
 CREATE TABLE IF NOT EXISTS cloud_history_retirement(
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),fill_cursor INTEGER NOT NULL);
 INSERT INTO cloud_history_retirement VALUES(1,0) ON CONFLICT(singleton) DO NOTHING;";

pub(super) struct CloudHistoryStore {
    pub connection: Connection,
    pub path: PathBuf,
}

pub(super) fn path_for(journal: &Path) -> PathBuf {
    let mut path = journal.as_os_str().to_os_string();
    path.push(".cloud-history.db");
    PathBuf::from(path)
}

pub(super) async fn connect(path: &Path) -> Result<Connection, BrokerCommandError> {
    let meta = std::fs::symlink_metadata(path).map_err(journal_error)?;
    if !meta.is_file() || meta.file_type().is_symlink() {
        return Err(journal_error(
            "cloud history must be a regular, non-symlink file",
        ));
    }
    let db = turso::Builder::new_local(path.to_string_lossy().as_ref())
        .build()
        .await
        .map_err(journal_error)?;
    let connection = db.connect().map_err(journal_error)?;
    let mut rows = connection
        .query("PRAGMA journal_mode=WAL", ())
        .await
        .map_err(journal_error)?;
    while rows.next().await.map_err(journal_error)?.is_some() {}
    drop(rows);
    connection
        .execute_batch(
            "PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000; PRAGMA cache_size=-2048;",
        )
        .await
        .map_err(journal_error)?;
    crate::core::live_sync::install_physical_capture_spill_control(&connection)
        .await
        .map_err(journal_error)?;
    Ok(connection)
}

pub(super) async fn identity(
    connection: &Connection,
    strategy: &str,
    account: &str,
    source: &str,
) -> Result<(), BrokerCommandError> {
    let mut rows = connection.query("SELECT version,strategy_id,account_id,source_id FROM cloud_history_identity WHERE singleton=1", ())
        .await.map_err(journal_error)?;
    let row = rows
        .next()
        .await
        .map_err(journal_error)?
        .ok_or_else(|| journal_error("missing cloud history identity"))?;
    if row.get::<i64>(0).map_err(journal_error)? != 1
        || row.get::<String>(1).map_err(journal_error)? != strategy
        || row.get::<String>(2).map_err(journal_error)? != account
        || row.get::<String>(3).map_err(journal_error)? != source
    {
        return Err(journal_error("cloud history identity mismatch"));
    }
    Ok(())
}

pub(super) async fn has_location(connection: &Connection) -> Result<bool, BrokerCommandError> {
    let mut rows = connection.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name='broker_cloud_history_location'", ())
        .await.map_err(journal_error)?;
    Ok(rows.next().await.map_err(journal_error)?.is_some())
}

/// Optional cursor/receipt/retention writes may use reserved operational
/// headroom, never exceed the complete partition. Refusal keeps accepted
/// evidence owned by its existing store and cannot affect broker recovery.
pub(super) async fn commit_operational(
    tx: turso::transaction::Transaction<'_>,
    path: &Path,
    limit: Option<PhysicalCaptureLimit>,
) -> Result<(), BrokerCommandError> {
    if let Some(limit) = limit {
        let usage = crate::core::live_sync::measure_physical_capture(&tx, path, limit)
            .await
            .map_err(journal_error)?;
        if usage.projected_commit_bytes > limit.physical_limit_bytes {
            tx.rollback().await.map_err(journal_error)?;
            return Err(BrokerCommandError::CapturePaused);
        }
    }
    tx.commit().await.map_err(journal_error)
}

/// Called under the exclusive broker lock, before any dispatch or worker.
pub(super) async fn reopen(journal: &mut BrokerCommandJournal,capacity:Option<PhysicalCaptureLimit>) -> Result<(), BrokerCommandError> {
    if !has_location(&journal.connection).await? {
        return Ok(());
    }
    let mut rows = journal.connection.query("SELECT version,state,physical_limit,headroom FROM broker_cloud_history_location WHERE singleton=1", ())
        .await.map_err(journal_error)?;
    let Some(row) = rows.next().await.map_err(journal_error)? else {
        return Ok(());
    };
    if row.get::<i64>(0).map_err(journal_error)? != 1 {
        return Err(journal_error("unsupported cloud history location version"));
    }
    let state: String = row.get(1).map_err(journal_error)?;
    let limit = capacity.unwrap_or(PhysicalCaptureLimit {
        physical_limit_bytes: u64::try_from(row.get::<i64>(2).map_err(journal_error)?)
            .map_err(journal_error)?,
        operational_headroom_bytes: u64::try_from(row.get::<i64>(3).map_err(journal_error)?)
            .map_err(journal_error)?,
    })
    .validate()
    .map_err(journal_error)?;
    drop(rows);
    if journal.state_history_enabled {
        return Err(journal_error(
            "split cloud history requires the duplicate state archive to remain disabled",
        ));
    }
    if state == "copying" {
        return Box::pin(migrate(journal, limit)).await;
    }
    if state != "ready" {
        return Err(journal_error("invalid cloud history location state"));
    }
    let path = path_for(&journal.path);
    let connection = connect(&path).await?;
    identity(
        &connection,
        &journal.strategy_id,
        &journal.account_id,
        &journal.evidence_source_id,
    )
    .await?;
    journal.cloud_history = Some(CloudHistoryStore { connection, path });
    journal.analytics_capture_budget = Some(limit);
    Ok(())
}

async fn columns(connection: &Connection, table: &str) -> Result<Vec<String>, BrokerCommandError> {
    let mut rows = connection
        .query(&format!("PRAGMA table_info({table})"), ())
        .await
        .map_err(journal_error)?;
    let mut names = vec![];
    while let Some(row) = rows.next().await.map_err(journal_error)? {
        names.push(row.get(1).map_err(journal_error)?);
    }
    if names.is_empty() {
        return Err(journal_error("missing legacy cloud history table"));
    }
    if names.iter().any(|name: &String| {
        name.is_empty() || !name.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
    }) {
        return Err(journal_error("unsupported cloud history column name"));
    }
    Ok(names)
}

// Primary-key/rowid pages bound memory; durable destination progress makes a
// killed migration resumable without loading its entire history in memory.
async fn copy_table(
    source: &Connection,
    target: &Connection,
    table: &str,
    target_path: &Path,
    limit: PhysicalCaptureLimit,
) -> Result<(), BrokerCommandError> {
    let names = columns(source, table).await?;
    if columns(target, table).await? != names {
        return Err(journal_error("unsupported legacy cloud history schema"));
    }
    let mut invalid = source
        .query(&format!("SELECT 1 FROM {table} WHERE rowid<=0 LIMIT 1"), ())
        .await
        .map_err(journal_error)?;
    if invalid.next().await.map_err(journal_error)?.is_some() {
        return Err(journal_error(
            "unsupported legacy cloud history row identity",
        ));
    }
    drop(invalid);
    let mut rows = target
        .query(
            "SELECT cursor,complete FROM cloud_history_copy_progress WHERE table_name=?1",
            params![table.to_owned()],
        )
        .await
        .map_err(journal_error)?;
    let progress = rows.next().await.map_err(journal_error)?;
    let mut cursor = progress
        .as_ref()
        .map(|r| r.get::<i64>(0))
        .transpose()
        .map_err(journal_error)?
        .unwrap_or(0);
    if progress
        .as_ref()
        .map(|r| r.get::<i64>(1))
        .transpose()
        .map_err(journal_error)?
        == Some(1)
    {
        return Ok(());
    }
    drop(rows);
    let insert = format!(
        "INSERT OR REPLACE INTO {table}(rowid,{}) VALUES({})",
        names.join(","),
        (1..=names.len() + 1)
            .map(|n| format!("?{n}"))
            .collect::<Vec<_>>()
            .join(",")
    );
    loop {
        let mut rows = source
            .query(
                &format!("SELECT rowid,* FROM {table} WHERE rowid>?1 ORDER BY rowid LIMIT 16"),
                params![cursor],
            )
            .await
            .map_err(journal_error)?;
        let mut page = vec![];
        while let Some(row) = rows.next().await.map_err(journal_error)? {
            let values = (0..=names.len())
                .map(|i| row.get_value(i))
                .collect::<Result<Vec<_>, _>>()
                .map_err(journal_error)?;
            // Existing technical payload limits are smaller. Fail closed on a
            // corrupt/unsupported legacy row, retaining its untouched source.
            if values
                .iter()
                .map(|v| match v {
                    turso::Value::Text(v) => v.len(),
                    turso::Value::Blob(v) => v.len(),
                    _ => 8,
                })
                .sum::<usize>()
                > 512 * 1024 + 8192
            {
                return Err(journal_error(
                    "legacy cloud history row exceeds migration bound",
                ));
            }
            cursor = row.get(0).map_err(journal_error)?;
            page.push(values);
        }
        drop(rows);
        let done = page.len() < 16;
        let tx = turso::transaction::Transaction::new_unchecked(
            target,
            turso::transaction::TransactionBehavior::Immediate,
        )
        .await
        .map_err(journal_error)?;
        for values in page {
            tx.execute(&insert, values).await.map_err(journal_error)?;
        }
        tx.execute("INSERT INTO cloud_history_copy_progress VALUES(?1,?2,?3) ON CONFLICT(table_name) DO UPDATE SET cursor=excluded.cursor,complete=excluded.complete",
            params![table.to_owned(),cursor,i64::from(done)]).await.map_err(journal_error)?;
        // This transfers EXISTING accepted evidence, not new capture. It may
        // use operational headroom but never exceed the complete partition.
        // Refusal also rolls back this page's durable copy checkpoint.
        commit_operational(tx,target_path,Some(limit)).await?;
        if done {
            break;
        }
    }
    Ok(())
}

async fn digest(connection: &Connection, table: &str) -> Result<Vec<u8>, BrokerCommandError> {
    let width = columns(connection, table).await?.len();
    let mut hash = Sha256::new();
    let mut cursor = 0i64;
    loop {
        let mut rows = connection
            .query(
                &format!("SELECT rowid,* FROM {table} WHERE rowid>?1 ORDER BY rowid LIMIT 16"),
                params![cursor],
            )
            .await
            .map_err(journal_error)?;
        let mut count = 0;
        while let Some(row) = rows.next().await.map_err(journal_error)? {
            count += 1;
            cursor = row.get(0).map_err(journal_error)?;
            for i in 0..=width {
                match row.get_value(i).map_err(journal_error)? {
                    turso::Value::Null => hash.update([0]),
                    turso::Value::Integer(v) => {
                        hash.update([1]);
                        hash.update(v.to_le_bytes());
                    }
                    turso::Value::Real(v) => {
                        hash.update([2]);
                        hash.update(v.to_bits().to_le_bytes());
                    }
                    turso::Value::Text(v) => {
                        hash.update([3]);
                        hash.update((v.len() as u64).to_le_bytes());
                        hash.update(v.as_bytes());
                    }
                    turso::Value::Blob(v) => {
                        hash.update([4]);
                        hash.update((v.len() as u64).to_le_bytes());
                        hash.update(&v);
                    }
                }
            }
        }
        if count < 16 {
            break;
        }
    }
    Ok(hash.finalize().to_vec())
}

pub(super) async fn migrate(
    journal: &mut BrokerCommandJournal,
    limit: PhysicalCaptureLimit,
) -> Result<(), BrokerCommandError> {
    let limit = limit.validate().map_err(journal_error)?;
    if journal.cloud_history.is_some() {
        journal.connection.execute("UPDATE broker_cloud_history_location SET physical_limit=?1,headroom=?2 WHERE singleton=1 AND state='ready'",
            params![limit.physical_limit_bytes as i64,limit.operational_headroom_bytes as i64]).await.map_err(journal_error)?;
        journal.analytics_capture_budget = Some(limit);
        return Ok(());
    }
    let path = path_for(&journal.path);
    let resuming = has_location(&journal.connection).await?;
    if !resuming && path.exists() {
        return Err(journal_error(
            "unowned cloud history destination already exists",
        ));
    }
    journal
        .connection
        .execute_batch(META)
        .await
        .map_err(journal_error)?;
    journal.connection.execute("INSERT INTO broker_cloud_history_location VALUES(1,1,'copying',?1,?2) ON CONFLICT(singleton) DO NOTHING",
        params![limit.physical_limit_bytes as i64,limit.operational_headroom_bytes as i64]).await.map_err(journal_error)?;
    if !path.exists() {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options
            .open(&path)
            .map_err(journal_error)?
            .sync_all()
            .map_err(journal_error)?;
    }
    let connection = connect(&path).await?;
    let metadata = turso::transaction::Transaction::new_unchecked(
        &connection, turso::transaction::TransactionBehavior::Immediate,
    ).await.map_err(journal_error)?;
    connection
        .execute_batch(STORE_META)
        .await
        .map_err(journal_error)?;
    connection.execute("INSERT INTO cloud_history_identity VALUES(1,1,?1,?2,?3) ON CONFLICT(singleton) DO NOTHING",
        params![journal.strategy_id.clone(),journal.account_id.clone(),journal.evidence_source_id.clone()]).await.map_err(journal_error)?;
    identity(
        &connection,
        &journal.strategy_id,
        &journal.account_id,
        &journal.evidence_source_id,
    )
    .await?;
    // Upgrade only optional metadata before copying; no broker state changes.
    journal
        .connection
        .execute_batch(super::analytics_capture::SCHEMA)
        .await
        .map_err(journal_error)?;
    super::analytics_capture::handoff::install(&journal.connection).await?;
    connection
        .execute_batch(fills::SCHEMA)
        .await
        .map_err(journal_error)?;
    connection
        .execute_batch(marks::SCHEMA)
        .await
        .map_err(journal_error)?;
    connection
        .execute_batch(super::analytics_capture::SCHEMA)
        .await
        .map_err(journal_error)?;
    super::analytics_capture::handoff::install(&connection).await?;
    commit_operational(metadata,&path,Some(limit)).await?;
    for table in TABLES {
        copy_table(&journal.connection, &connection, table, &path, limit).await?;
    }
    for table in TABLES {
        if digest(&journal.connection, table).await? != digest(&connection, table).await? {
            return Err(journal_error(
                "cloud history migration verification failed; legacy source retained",
            ));
        }
    }
    #[cfg(unix)]
    std::fs::File::open(
        path.parent()
            .ok_or_else(|| journal_error("cloud history directory is unavailable"))?,
    )
    .and_then(|directory| directory.sync_all())
    .map_err(journal_error)?;
    // A single FULL-synchronous source commit chooses the verified destination.
    // Tombstone views cause old binaries to refuse startup rather than silently
    // recreating a second writer/history after a downgrade.
    let tx = turso::transaction::Transaction::new_unchecked(
        &journal.connection,
        turso::transaction::TransactionBehavior::Immediate,
    )
    .await
    .map_err(journal_error)?;
    for table in TABLES {
        tx.execute_batch(&format!(
            "DROP TABLE {table}; CREATE VIEW {table} AS SELECT 1 AS cloud_history_moved WHERE 0;"
        ))
        .await
        .map_err(journal_error)?;
    }
    tx.execute("UPDATE broker_cloud_history_location SET state='ready' WHERE singleton=1 AND version=1 AND state='copying'", ()).await.map_err(journal_error)?;
    tx.commit().await.map_err(journal_error)?;
    journal.cloud_history = Some(CloudHistoryStore { connection, path });
    journal.analytics_capture_budget = Some(limit);
    Ok(())
}

impl BrokerCommandJournal {
    pub(super) async fn commit_analytics_maintenance(
        &self,
        tx: turso::transaction::Transaction<'_>,
    ) -> Result<(), BrokerCommandError> {
        commit_operational(
            tx,
            self.analytics_path(),
            self.cloud_history
                .as_ref()
                .and(self.analytics_capture_budget),
        )
        .await
    }
    pub(super) fn analytics_connection(&self) -> &Connection {
        self.cloud_history
            .as_ref()
            .map(|s| &s.connection)
            .unwrap_or(&self.connection)
    }
    pub(super) fn analytics_path(&self) -> &Path {
        self.cloud_history
            .as_ref()
            .map(|s| s.path.as_path())
            .unwrap_or(&self.path)
    }
    /// The exact export receipt transfers ownership to the durable publisher
    /// outbox, not directly to the cloud. Keep the latest fill/quantity facts
    /// needed for broker deduplication and every not-yet-handed-off row. Global
    /// anchors preserve AUTOINCREMENT/forecast clocks even after an empty drain.
    pub(super) async fn retire_handed_off_cloud_history(&self) -> Result<(), BrokerCommandError> {
        if self.cloud_history.is_none() {
            return Ok(());
        }
        let connection = self.analytics_connection();
        let tx = turso::transaction::Transaction::new_unchecked(
            connection,
            turso::transaction::TransactionBehavior::Immediate,
        )
        .await
        .map_err(journal_error)?;
        tx.execute_batch(
            "DELETE FROM broker_position_marks WHERE sequence IN (
            SELECT sequence FROM broker_position_marks
            WHERE sequence<=(SELECT cursor FROM broker_position_mark_export WHERE singleton=1)
              AND sequence<(SELECT MAX(sequence) FROM broker_position_marks)
            ORDER BY sequence LIMIT 128);",
        )
        .await
        .map_err(journal_error)?;
        // Page *before* testing retention predicates. LIMIT after NOT EXISTS
        // could otherwise scan millions of retained current heads per ACK.
        let mut rows = tx
            .query(
                "SELECT sequence FROM broker_execution_fills
            WHERE sequence>(SELECT fill_cursor FROM cloud_history_retirement WHERE singleton=1)
              AND sequence<=(SELECT cursor FROM broker_execution_fill_export WHERE singleton=1)
            ORDER BY sequence LIMIT 128",
                (),
            )
            .await
            .map_err(journal_error)?;
        let mut candidates = Vec::<i64>::new();
        while let Some(row) = rows.next().await.map_err(journal_error)? {
            candidates.push(row.get(0).map_err(journal_error)?);
        }
        drop(rows);
        let cursor = candidates.last().copied().unwrap_or(0);
        if !candidates.is_empty() {
            let sql=format!("DELETE FROM broker_execution_fills WHERE sequence IN ({})
                AND sequence<(SELECT MAX(sequence) FROM broker_execution_fills)
                AND NOT EXISTS(SELECT 1 FROM broker_execution_fill_heads h WHERE h.sequence=broker_execution_fills.sequence)
                AND NOT EXISTS(SELECT 1 FROM broker_execution_position_checks p WHERE p.ledger_sequence=broker_execution_fills.sequence)",
                (1..=candidates.len()).map(|i|format!("?{i}")).collect::<Vec<_>>().join(","));
            tx.execute(
                &sql,
                candidates
                    .into_iter()
                    .map(turso::Value::Integer)
                    .collect::<Vec<_>>(),
            )
            .await
            .map_err(journal_error)?;
        }
        tx.execute(
            "UPDATE cloud_history_retirement SET fill_cursor=?1 WHERE singleton=1",
            params![cursor],
        )
        .await
        .map_err(journal_error)?;
        self.commit_analytics_maintenance(tx).await?;
        self.try_reclaim_cloud_wal().await
    }

    pub(super) async fn try_reclaim_cloud_wal(&self) -> Result<(), BrokerCommandError> {
        if self.cloud_history.is_none() {
            return Ok(());
        }
        let connection = self.analytics_connection();
        // Do not let an external diagnostic reader hold the serialized worker
        // behind the normal five-second SQLite busy timeout.
        struct Restore<'a>(&'a Connection);
        impl Drop for Restore<'_> {
            fn drop(&mut self) {
                let _ = self.0.busy_timeout(std::time::Duration::from_secs(5));
            }
        }
        connection
            .busy_timeout(std::time::Duration::ZERO)
            .map_err(journal_error)?;
        let _restore = Restore(connection);
        match connection
            .query("PRAGMA wal_checkpoint(TRUNCATE)", ())
            .await
        {
            Ok(mut rows) => loop {
                match rows.next().await {
                    Ok(Some(_)) => {}
                    Ok(None) | Err(turso::Error::Busy(_) | turso::Error::BusySnapshot(_)) => break,
                    Err(error) => return Err(journal_error(error)),
                }
            },
            Err(turso::Error::Busy(_) | turso::Error::BusySnapshot(_)) => {}
            Err(error) => return Err(journal_error(error)),
        }
        Ok(())
    }
    /// Startup-only, under the existing exclusive execution lock. Standalone
    /// journals are unchanged unless explicitly opted into this migration.
    pub async fn with_separate_analytics_capture_budget(
        mut self,
        limit: PhysicalCaptureLimit,
    ) -> Result<Self, BrokerCommandError> {
        if self.state_history_enabled {
            return Err(journal_error(
                "split cloud history requires the duplicate state archive to be disabled",
            ));
        }
        Box::pin(migrate(&mut self, limit)).await?;
        self.with_analytics_capture_budget(limit).await
    }
}

#[cfg(test)]
mod tests;
