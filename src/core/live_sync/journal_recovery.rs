//! Bounded private discovery metadata, not another execution/evidence archive.
//! The source-file lease owns this small FULL-synchronous registry. A fallback
//! pause must be discoverable before the command worker can start.
use super::{CaptureOwnerError, CaptureOwnerLease};
use std::path::{Path, PathBuf};
use turso::params;

// v2 importers understand separately capped optional history. Existing v1
// links remain readable; old importers reject v2 rather than reading a moved
// primary-journal tombstone as an empty history.
pub const JOURNAL_RECOVERY_LINK_VERSION: u16 = 2;
const MAX_LINKS: i64 = 256;
const MAX_LINK_BYTES: usize = 8192;
#[cfg(test)]
mod tests;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JournalRecoveryLink {
    pub version: u16,
    pub journal_path: PathBuf,
    pub journal_source_id: String,
    pub journal_strategy_id: String,
    pub account_id: String,
    pub runtime_id: String,
    pub strategy_id: String,
    pub live_session_id: String,
    pub run_id: String,
    pub payload_limit_bytes: u64,
}
fn scope(value: &str) -> &str {
    value
        .trim()
        .strip_prefix("strategy:")
        .unwrap_or(value.trim())
}
impl JournalRecoveryLink {
    pub fn validate(&self) -> Result<(), CaptureOwnerError> {
        if ![1, JOURNAL_RECOVERY_LINK_VERSION].contains(&self.version)
            || !self.journal_path.is_absolute()
            || self
                .journal_path
                .to_str()
                .is_none_or(|p| p.len() > 4096 || p.contains('\0'))
            || [
                &self.journal_source_id,
                &self.journal_strategy_id,
                &self.account_id,
                &self.runtime_id,
                &self.strategy_id,
                &self.live_session_id,
                &self.run_id,
            ]
            .iter()
            .any(|v| v.trim().is_empty() || v.len() > 128)
            || scope(&self.journal_strategy_id) != scope(&self.strategy_id)
            || self.payload_limit_bytes == 0
            || self.payload_limit_bytes > i64::MAX as u64
            || serde_json::to_vec(self)
                .map_err(|_| CaptureOwnerError::Invalid)?
                .len()
                > MAX_LINK_BYTES
        {
            return Err(CaptureOwnerError::Invalid);
        }
        Ok(())
    }
    pub(crate) fn same_source(&self, other: &Self) -> bool {
        self.journal_path == other.journal_path
            && self.journal_source_id == other.journal_source_id
            && self.journal_strategy_id == other.journal_strategy_id
            && self.account_id == other.account_id
            && self.strategy_id == other.strategy_id
            && self.live_session_id == other.live_session_id
    }
}
fn regular(path: &Path) -> Result<bool, CaptureOwnerError> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.is_file() && !meta.file_type().is_symlink() => Ok(true),
        Ok(_) => Err(CaptureOwnerError::Invalid),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(CaptureOwnerError::Io),
    }
}
impl CaptureOwnerLease {
    pub fn journal_recovery_path(outbox: &Path) -> PathBuf {
        let mut path = outbox.as_os_str().to_os_string();
        path.push(".journals.db");
        PathBuf::from(path)
    }
    fn is_import(&self) -> bool {
        [&self.runtime, &self.strategy, &self.session, &self.run]
            .iter()
            .all(|v| v.is_empty())
    }
    async fn registry(
        &self,
        create: bool,
    ) -> Result<Option<(turso::Database, turso::Connection)>, CaptureOwnerError> {
        let path = Self::journal_recovery_path(&self.path);
        if !regular(&path)? {
            if !create {
                return Ok(None);
            }
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
            }
            options
                .open(&path)
                .and_then(|file| file.sync_all())
                .map_err(|_| CaptureOwnerError::Io)?;
        }
        for suffix in ["-wal", "-shm", "-journal"] {
            let mut family = path.as_os_str().to_os_string();
            family.push(suffix);
            regular(&PathBuf::from(family))?;
        }
        let database = turso::Builder::new_local(path.to_str().ok_or(CaptureOwnerError::Invalid)?)
            .build()
            .await
            .map_err(|_| CaptureOwnerError::Io)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                .map_err(|_| CaptureOwnerError::Io)?;
        }
        let connection = database.connect().map_err(|_| CaptureOwnerError::Io)?;
        connection
            .execute_batch(
                "PRAGMA synchronous=FULL; PRAGMA busy_timeout=1000; PRAGMA cache_size=-256;",
            )
            .await
            .map_err(|_| CaptureOwnerError::Io)?;
        if create {
            connection
                .execute_batch(
                    "CREATE TABLE IF NOT EXISTS journal_recovery_links(
                source_id TEXT PRIMARY KEY,version INTEGER NOT NULL CHECK(version=1),
                link TEXT NOT NULL CHECK(length(link)<=8192));",
                )
                .await
                .map_err(|_| CaptureOwnerError::Io)?;
        }
        // Pinned Turso does not implement wal_autocheckpoint/journal_size_limit.
        // This registry has one leased owner and no long-lived readers. Fold
        // the previous bounded transaction into the DB before another mutation
        // so repeated fallback launches cannot accumulate an unbounded WAL.
        let mut checkpoint = connection
            .query("PRAGMA wal_checkpoint(TRUNCATE)", ())
            .await
            .map_err(|_| CaptureOwnerError::Io)?;
        while let Some(row) = checkpoint.next().await.map_err(|_| CaptureOwnerError::Io)? {
            if row.get::<i64>(0).map_err(|_| CaptureOwnerError::Io)? != 0 {
                return Err(CaptureOwnerError::Io);
            }
        }
        drop(checkpoint);
        Ok(Some((database, connection)))
    }
    /// The first target owner remains immutable until transfer. A new run may
    /// rediscover this same source, but must not rewrite its old delivery scope.
    pub async fn retain_journal_recovery(
        &self,
        link: &JournalRecoveryLink,
    ) -> Result<(), CaptureOwnerError> {
        link.validate()?;
        if self.is_import()
            || link.runtime_id != self.runtime
            || link.strategy_id != self.strategy
            || link.live_session_id != self.session
            || link.run_id != self.run
        {
            return Err(CaptureOwnerError::Invalid);
        }
        let _guard = self.journal_recovery_gate.lock().await;
        let (_database, connection) = self.registry(true).await?.ok_or(CaptureOwnerError::Io)?;
        let tx = turso::transaction::Transaction::new_unchecked(
            &connection,
            turso::transaction::TransactionBehavior::Immediate,
        )
        .await
        .map_err(|_| CaptureOwnerError::Io)?;
        let mut rows=connection.query("SELECT CASE WHEN length(CAST(link AS BLOB))<=8192 THEN link ELSE NULL END FROM journal_recovery_links WHERE source_id=?1",params![link.journal_source_id.clone()])
            .await.map_err(|_|CaptureOwnerError::Io)?;
        if let Some(row) = rows.next().await.map_err(|_| CaptureOwnerError::Io)? {
            let wire: String = row.get(0).map_err(|_| CaptureOwnerError::Invalid)?;
            let saved: JournalRecoveryLink =
                serde_json::from_str(&wire).map_err(|_| CaptureOwnerError::Invalid)?;
            saved.validate()?;
            if !saved.same_source(link) {
                return Err(CaptureOwnerError::Invalid);
            }
            drop(rows);
            tx.commit().await.map_err(|_| CaptureOwnerError::Io)?;
            return Ok(());
        }
        drop(rows);
        let mut rows = connection
            .query("SELECT count(*) FROM journal_recovery_links", ())
            .await
            .map_err(|_| CaptureOwnerError::Io)?;
        let count: i64 = rows
            .next()
            .await
            .map_err(|_| CaptureOwnerError::Io)?
            .ok_or(CaptureOwnerError::Invalid)?
            .get(0)
            .map_err(|_| CaptureOwnerError::Invalid)?;
        drop(rows);
        if count >= MAX_LINKS {
            return Err(CaptureOwnerError::Invalid);
        }
        connection
            .execute(
                "INSERT INTO journal_recovery_links(source_id,version,link) VALUES(?1,1,?2)",
                params![
                    link.journal_source_id.clone(),
                    serde_json::to_string(link).map_err(|_| CaptureOwnerError::Invalid)?
                ],
            )
            .await
            .map_err(|_| CaptureOwnerError::Io)?;
        tx.commit().await.map_err(|_| CaptureOwnerError::Io)?;
        Ok(())
    }
    /// At most eight sources per import pass, fixed metadata only. Missing or
    /// malformed registries never authorize source or registration retirement.
    pub async fn journal_recovery_links(
        &self,
        strategy: &str,
        session: &str,
    ) -> Result<Vec<JournalRecoveryLink>, CaptureOwnerError> {
        if !self.is_import() {
            return Err(CaptureOwnerError::Invalid);
        }
        let _guard = self.journal_recovery_gate.lock().await;
        let Some((_database, connection)) = self.registry(false).await? else {
            return Ok(Vec::new());
        };
        let mut rows=connection.query("SELECT version,CASE WHEN length(CAST(link AS BLOB))<=8192 THEN link ELSE NULL END FROM journal_recovery_links ORDER BY source_id LIMIT 8",())
            .await.map_err(|_|CaptureOwnerError::Io)?;
        let mut links = Vec::new();
        while let Some(row) = rows.next().await.map_err(|_| CaptureOwnerError::Io)? {
            if row.get::<i64>(0).map_err(|_| CaptureOwnerError::Invalid)? != 1 {
                return Err(CaptureOwnerError::Invalid);
            }
            let wire: String = row.get(1).map_err(|_| CaptureOwnerError::Invalid)?;
            let link: JournalRecoveryLink =
                serde_json::from_str(&wire).map_err(|_| CaptureOwnerError::Invalid)?;
            link.validate()?;
            if link.strategy_id != strategy || link.live_session_id != session {
                return Err(CaptureOwnerError::Invalid);
            }
            links.push(link);
        }
        Ok(links)
    }
    /// Caller has committed the diagnostic into another durable owner (or has
    /// proven this exact journal has no pause). Never delete the journal itself.
    pub async fn forget_journal_recovery(
        &self,
        link: &JournalRecoveryLink,
    ) -> Result<(), CaptureOwnerError> {
        if !self.is_import() {
            return Err(CaptureOwnerError::Invalid);
        }
        link.validate()?;
        let _guard = self.journal_recovery_gate.lock().await;
        let Some((_database, connection)) = self.registry(false).await? else {
            return Ok(());
        };
        connection
            .execute(
                "DELETE FROM journal_recovery_links WHERE source_id=?1 AND link=?2",
                params![
                    link.journal_source_id.clone(),
                    serde_json::to_string(link).map_err(|_| CaptureOwnerError::Invalid)?
                ],
            )
            .await
            .map_err(|_| CaptureOwnerError::Io)?;
        Ok(())
    }
    pub async fn retire_empty_journal_recovery(&self) -> Result<bool, CaptureOwnerError> {
        if !self.is_import() {
            return Err(CaptureOwnerError::Invalid);
        }
        let _guard = self.journal_recovery_gate.lock().await;
        let Some((database, connection)) = self.registry(false).await? else {
            return Ok(true);
        };
        let mut rows = connection
            .query("SELECT source_id FROM journal_recovery_links LIMIT 1", ())
            .await
            .map_err(|_| CaptureOwnerError::Io)?;
        if rows
            .next()
            .await
            .map_err(|_| CaptureOwnerError::Io)?
            .is_some()
        {
            return Ok(false);
        }
        drop(rows);
        drop(connection);
        drop(database);
        let path = Self::journal_recovery_path(&self.path);
        for suffix in ["-wal", "-shm", "-journal", ""] {
            let mut file = path.as_os_str().to_os_string();
            file.push(suffix);
            match std::fs::remove_file(PathBuf::from(file)) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => return Err(CaptureOwnerError::Io),
            }
        }
        Ok(true)
    }
}
