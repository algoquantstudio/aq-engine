//! Metadata-only access under the existing exclusive command-journal lock.
//! Do not use normal journal open: it installs execution schemas/checkpoints.
use super::*;
use crate::core::live_sync::{
    CaptureOwnerLease, JOURNAL_RECOVERY_LINK_VERSION, JournalRecoveryLink,
};

#[derive(Debug)]
pub enum ExitedAnalyticsCaptureError {
    Busy,
    Unavailable,
    Invalid,
    Database,
}
impl std::fmt::Display for ExitedAnalyticsCaptureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Busy => "source execution journal still has an owner",
            Self::Unavailable => "source execution journal is unavailable; recovery link retained",
            Self::Invalid => "source execution journal recovery identity does not match",
            Self::Database => "source execution journal metadata could not be recovered",
        })
    }
}
impl std::error::Error for ExitedAnalyticsCaptureError {}
fn db(_: impl std::fmt::Display) -> ExitedAnalyticsCaptureError {
    ExitedAnalyticsCaptureError::Database
}

pub struct ExitedAnalyticsCapture {
    connection: Connection,
    path: PathBuf,
    limit: Option<crate::core::live_sync::PhysicalCaptureLimit>,
    link: JournalRecoveryLink,
    _lock: File,
}
impl BrokerCommandJournal {
    pub(crate) fn recovery_link(
        &self,
        owner: &CaptureOwnerLease,
        payload_limit_bytes: u64,
    ) -> Result<JournalRecoveryLink, BrokerCommandError> {
        let link = JournalRecoveryLink {
            version: JOURNAL_RECOVERY_LINK_VERSION,
            journal_path: self.path.canonicalize().map_err(journal_error)?,
            journal_source_id: self.evidence_source_id.clone(),
            journal_strategy_id: self.strategy_id.clone(),
            account_id: self.account_id.clone(),
            runtime_id: owner.runtime.clone(),
            strategy_id: owner.strategy.clone(),
            live_session_id: owner.session.clone(),
            run_id: owner.run.clone(),
            payload_limit_bytes,
        };
        link.validate().map_err(journal_error)?;
        Ok(link)
    }
}
impl ExitedAnalyticsCapture {
    pub async fn open(link: &JournalRecoveryLink) -> Result<Self, ExitedAnalyticsCaptureError> {
        use ExitedAnalyticsCaptureError::{Busy, Invalid, Unavailable};
        link.validate().map_err(|_| Invalid)?;
        let meta = std::fs::symlink_metadata(&link.journal_path).map_err(|_| Unavailable)?;
        if !meta.is_file()
            || meta.file_type().is_symlink()
            || link.journal_path.canonicalize().map_err(|_| Unavailable)? != link.journal_path
        {
            return Err(Invalid);
        }
        let lock_path = link.journal_path.with_extension("commands.lock");
        let meta = std::fs::symlink_metadata(&lock_path).map_err(|_| Unavailable)?;
        if !meta.is_file() || meta.file_type().is_symlink() {
            return Err(Invalid);
        }
        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        let lock = options.open(&lock_path).map_err(|_| Unavailable)?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => return Err(Busy),
            Err(_) => return Err(Unavailable),
        }
        let database = turso::Builder::new_local(link.journal_path.to_str().ok_or(Invalid)?)
            .build()
            .await
            .map_err(db)?;
        let connection = database.connect().map_err(db)?;
        crate::core::utils::storage_format::require_current(
            &connection, "broker journal", 1095846482, 2,
        )
        .await
        .map_err(|_| Invalid)?;
        connection
            .execute_batch(
                "PRAGMA synchronous=FULL; PRAGMA busy_timeout=1000; PRAGMA cache_size=-256;",
            )
            .await
            .map_err(db)?;
        let mut rows=connection.query("SELECT CASE WHEN length(CAST(strategy_id AS BLOB))<=128 THEN strategy_id ELSE NULL END,CASE WHEN length(CAST(account_id AS BLOB))<=128 THEN account_id ELSE NULL END FROM broker_command_scope WHERE singleton=1",()).await.map_err(db)?;
        let row = rows.next().await.map_err(db)?.ok_or(Invalid)?;
        if row.get::<String>(0).map_err(db)? != link.journal_strategy_id
            || row.get::<String>(1).map_err(db)? != link.account_id
        {
            return Err(Invalid);
        }
        drop(rows);
        let mut rows=connection.query("SELECT CASE WHEN length(CAST(source_id AS BLOB))<=128 THEN source_id ELSE NULL END FROM broker_execution_evidence_source WHERE singleton=1",()).await.map_err(db)?;
        if rows
            .next()
            .await
            .map_err(db)?
            .ok_or(Invalid)?
            .get::<String>(0)
            .map_err(db)?
            != link.journal_source_id
        {
            return Err(Invalid);
        }
        drop(rows);
        let (connection,path,limit) = if cloud_history::has_location(&connection).await.map_err(db)? {
            let mut rows = connection.query("SELECT version,state,physical_limit,headroom FROM broker_cloud_history_location WHERE singleton=1", ()).await.map_err(db)?;
            let row = rows.next().await.map_err(db)?.ok_or(Invalid)?;
            if row.get::<i64>(0).map_err(db)? != 1 || row.get::<String>(1).map_err(db)? != "ready" {
                // Never mutate/resume a copy from metadata-only host recovery.
                return Err(Unavailable);
            }
            let limit=crate::core::live_sync::PhysicalCaptureLimit {
                physical_limit_bytes:u64::try_from(row.get::<i64>(2).map_err(db)?).map_err(|_|Invalid)?,
                operational_headroom_bytes:u64::try_from(row.get::<i64>(3).map_err(db)?).map_err(|_|Invalid)?,
            }.validate().map_err(|_|Invalid)?;
            drop(rows);
            let path=cloud_history::path_for(&link.journal_path);
            let cloud = cloud_history::connect(&path).await.map_err(db)?;
            cloud_history::identity(&cloud, &link.journal_strategy_id, &link.account_id, &link.journal_source_id).await.map_err(db)?;
            (cloud,path,Some(limit))
        } else { (connection,link.journal_path.clone(),None) };
        Ok(Self {
            connection,
            path,
            limit,
            link: link.clone(),
            _lock: lock,
        })
    }
    pub async fn pause(
        &self,
    ) -> Result<Option<AnalyticsCapturePause>, ExitedAnalyticsCaptureError> {
        use ExitedAnalyticsCaptureError::Invalid;
        let mut rows=self.connection.query("SELECT version,generation,paused,execution_epoch,started_at_us,last_blocked_at_us FROM broker_analytics_capture WHERE singleton=1",()).await.map_err(db)?;
        let row = rows.next().await.map_err(db)?.ok_or(Invalid)?;
        if row.get::<i64>(0).map_err(db)? != 1 {
            return Err(Invalid);
        }
        match row.get::<i64>(2).map_err(db)? {
            0 => return Ok(None),
            1 => {}
            _ => return Err(Invalid),
        }
        let pause = AnalyticsCapturePause {
            version: 1,
            journal_source_id: self.link.journal_source_id.clone(),
            strategy_id: self.link.journal_strategy_id.clone(),
            account_id: self.link.account_id.clone(),
            generation: u64::try_from(row.get::<i64>(1).map_err(db)?).map_err(|_| Invalid)?,
            execution_epoch: u64::try_from(row.get::<i64>(3).map_err(db)?).map_err(|_| Invalid)?,
            started_at_us: row.get(4).map_err(db)?,
            last_blocked_at_us: row.get(5).map_err(db)?,
        };
        pause.validate().map_err(|_| Invalid)?;
        Ok(Some(pause))
    }
    /// An exact diagnostic receipt only. No command, checkpoint, execution epoch,
    /// fill/mark export cursor or pause/resume state can be changed here.
    pub async fn acknowledge_handoff(
        &self,
        pause: &AnalyticsCapturePause,
        receipt: &AnalyticsCaptureHandoffReceipt,
    ) -> Result<bool, ExitedAnalyticsCaptureError> {
        if pause.journal_source_id != self.link.journal_source_id
            || pause.strategy_id != self.link.journal_strategy_id
            || pause.account_id != self.link.account_id
            || !receipt.covers(pause)
        {
            return Ok(false);
        }
        let wire = serde_json::to_string(receipt).map_err(db)?;
        let mut rows=self.connection.query("SELECT handoff_receipt FROM broker_analytics_capture WHERE singleton=1 AND version=1 AND paused=1
            AND generation=?1 AND execution_epoch=?2 AND started_at_us=?3 AND last_blocked_at_us=?4",
            params![pause.generation as i64,pause.execution_epoch as i64,pause.started_at_us,pause.last_blocked_at_us]).await.map_err(db)?;
        let Some(row)=rows.next().await.map_err(db)? else {return Ok(false);};
        let saved:Option<String>=row.get(0).map_err(db)?;
        drop(rows);
        if let Some(saved)=saved {return Ok(saved==wire);}
        let tx=turso::transaction::Transaction::new_unchecked(&self.connection,turso::transaction::TransactionBehavior::Immediate).await.map_err(db)?;
        let changed=tx.execute("UPDATE broker_analytics_capture SET handoff_receipt=?1 WHERE singleton=1 AND version=1 AND paused=1
            AND generation=?2 AND execution_epoch=?3 AND started_at_us=?4 AND last_blocked_at_us=?5 AND (handoff_receipt IS NULL OR handoff_receipt=?1)",
            params![wire,pause.generation as i64,pause.execution_epoch as i64,pause.started_at_us,pause.last_blocked_at_us]).await.map_err(db)?;
        cloud_history::commit_operational(tx,&self.path,self.limit).await.map_err(db)?;
        Ok(changed == 1)
    }
}
