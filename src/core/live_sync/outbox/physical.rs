//! Opt-in physical admission accounting. This is not a filesystem quota: only
//! this store's new data capture is admitted here. Accepted evidence and the
//! separate broker execution journal must never be deleted to fit a limit.
use super::*;

pub const MIN_OPERATIONAL_HEADROOM_BYTES: u64 = crate::launch_config::MIN_CAPTURE_OPERATIONAL_HEADROOM_BYTES;

impl From<crate::launch_config::CapturePartitionConfig> for PhysicalCaptureLimit {
    fn from(value:crate::launch_config::CapturePartitionConfig)->Self{
        Self{physical_limit_bytes:value.physical_limit_bytes,operational_headroom_bytes:value.operational_headroom_bytes}
    }
}
impl From<crate::launch_config::CaptureOutboxConfig> for PhysicalCaptureBudget {
    fn from(value:crate::launch_config::CaptureOutboxConfig)->Self{
        Self{payload_limit_bytes:value.payload_limit_bytes,physical_limit_bytes:value.physical.physical_limit_bytes,operational_headroom_bytes:value.physical.operational_headroom_bytes}
    }
}

/// A physical admission partition, not billing usage or an OS disk quota.
/// Essential execution/safety writes must remain outside analytics admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PhysicalCaptureLimit {
    pub physical_limit_bytes: u64,
    pub operational_headroom_bytes: u64,
}
impl PhysicalCaptureLimit {
    pub fn validate(self) -> Result<Self, OutboxStoreError> {
        if self.physical_limit_bytes > i64::MAX as u64
            || self.operational_headroom_bytes < MIN_OPERATIONAL_HEADROOM_BYTES
            || self.physical_limit_bytes <= self.operational_headroom_bytes
        {
            return Err(OutboxStoreError::InvalidRecord(
                "invalid physical capture partition".into(),
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PhysicalCaptureBudget {
    pub payload_limit_bytes: u64,
    pub physical_limit_bytes: u64,
    pub operational_headroom_bytes: u64,
}
impl PhysicalCaptureBudget {
    pub fn validate(self) -> Result<Self, OutboxStoreError> {
        if self.payload_limit_bytes == 0 || self.payload_limit_bytes > i64::MAX as u64 {
            return Err(OutboxStoreError::InvalidRecord(
                "invalid physical capture budget".into(),
            ));
        }
        self.physical_limit().validate()?;
        Ok(self)
    }
    pub fn physical_limit(self) -> PhysicalCaptureLimit {
        PhysicalCaptureLimit {
            physical_limit_bytes: self.physical_limit_bytes,
            operational_headroom_bytes: self.operational_headroom_bytes,
        }
    }
    pub fn data_limit_bytes(self) -> u64 {
        self.physical_limit_bytes - self.operational_headroom_bytes
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PhysicalCaptureUsage {
    pub database_bytes: u64,
    pub wal_bytes: u64,
    pub shared_memory_bytes: u64,
    pub logical_database_bytes: u64,
    /// Conservative next-commit allowance, not actual bytes billed or written.
    /// With spilling disabled, one transaction can dirty at most all logical
    /// pages. Include an additional WAL frame per page plus its frame/header.
    pub projected_commit_bytes: u64,
    pub limit_bytes: u64,
    pub operational_headroom_bytes: u64,
}
impl PhysicalCaptureUsage {
    pub fn actual_bytes(&self) -> u64 {
        self.database_bytes
            .saturating_add(self.wal_bytes)
            .saturating_add(self.shared_memory_bytes)
    }
    pub fn exceeds_data_limit(&self) -> bool {
        self.projected_commit_bytes
            > self
                .limit_bytes
                .saturating_sub(self.operational_headroom_bytes)
    }

    /// Resource readiness only; callers must still reconcile the exact durable
    /// pause generation before allowing any new cloud capture.
    pub fn below_resume_threshold(&self) -> bool {
        self.projected_commit_bytes
            <= (self
                .limit_bytes
                .saturating_sub(self.operational_headroom_bytes)
                / 5)
                * 4
    }
}

fn db(e: impl std::fmt::Display) -> OutboxStoreError {
    OutboxStoreError::Database(e.to_string())
}
fn bytes(path: &Path, optional: bool) -> Result<u64, OutboxStoreError> {
    match std::fs::metadata(path) {
        Ok(meta) if meta.is_file() => Ok(meta.len()),
        Err(error) if optional && error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        _ => Err(OutboxStoreError::Database(
            "cloud buffer footprint unavailable".into(),
        )),
    }
}
fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut path = path.as_os_str().to_os_string();
    path.push(suffix);
    PathBuf::from(path)
}

pub(crate) async fn install_physical_capture_spill_control(
    connection: &Connection,
) -> Result<(), OutboxStoreError> {
    connection
        .execute_batch("PRAGMA cache_spill=0;")
        .await
        .map_err(db)?;
    let mut rows = connection
        .query("PRAGMA cache_spill", ())
        .await
        .map_err(db)?;
    let disabled = rows
        .next()
        .await
        .map_err(db)?
        .is_some_and(|row| row.get::<i64>(0).ok() == Some(0));
    if !disabled {
        return Err(OutboxStoreError::InvalidRecord(
            "physical capture requires verified spill control".into(),
        ));
    }
    Ok(())
}

async fn scalar_pragma(connection: &Connection, pragma: &str) -> Result<u64, OutboxStoreError> {
    let mut rows = connection.query(pragma, ()).await.map_err(db)?;
    let value = rows
        .next()
        .await
        .map_err(db)?
        .ok_or_else(|| db("physical capture geometry unavailable"))?
        .get::<i64>(0)
        .map_err(db)?;
    u64::try_from(value).map_err(db)
}

/// Caller holds its owning transaction/serialized connection. Shared with the
/// optional analytics journal so both boundaries use the same conservative
/// calculation and strict missing-file handling; no historical scan is needed.
pub(crate) async fn measure_physical_capture(
    connection: &Connection,
    path: &Path,
    limit: PhysicalCaptureLimit,
) -> Result<PhysicalCaptureUsage, OutboxStoreError> {
    limit.validate()?;
    let page_size = scalar_pragma(connection, "PRAGMA page_size").await?;
    let page_count = scalar_pragma(connection, "PRAGMA page_count").await?;
    if page_size < 512 || page_size > 65536 || !page_size.is_power_of_two() || page_count == 0 {
        return Err(OutboxStoreError::InvalidRecord(
            "invalid cloud buffer geometry".into(),
        ));
    }
    let database_bytes = bytes(path, false)?;
    let wal_bytes = bytes(&sibling(path, "-wal"), true)?;
    let shared_memory_bytes = bytes(&sibling(path, "-shm"), true)?;
    let logical_database_bytes = page_count
        .checked_mul(page_size)
        .ok_or_else(|| db("cloud buffer geometry overflow"))?;
    let commit_wal = page_count
        .checked_mul(page_size + 24)
        .and_then(|v| v.checked_add(32))
        .ok_or_else(|| db("cloud buffer geometry overflow"))?;
    let projected_commit_bytes = database_bytes
        .max(logical_database_bytes)
        .checked_add(wal_bytes)
        .and_then(|v| v.checked_add(shared_memory_bytes))
        .and_then(|v| v.checked_add(commit_wal))
        .ok_or_else(|| db("cloud buffer footprint overflow"))?;
    Ok(PhysicalCaptureUsage {
        database_bytes,
        wal_bytes,
        shared_memory_bytes,
        logical_database_bytes,
        projected_commit_bytes,
        limit_bytes: limit.physical_limit_bytes,
        operational_headroom_bytes: limit.operational_headroom_bytes,
    })
}

impl OutboxStore {
    /// Install before starting/cloning writers. A normal `open` is unchanged.
    /// Reopening does not silently inherit an old process's policy: launch
    /// configuration must explicitly supply the same budget on every launch.
    pub async fn open_with_physical_capture_budget(
        path: impl AsRef<Path>,
        budget: PhysicalCaptureBudget,
    ) -> Result<Self, OutboxStoreError> {
        let budget = budget.validate()?;
        let store = Self::open_configured(path, Some(budget)).await?;
        store.ensure_capture_capacity_schema().await?;
        Ok(store)
    }

    pub fn physical_capture_budget(&self) -> Option<PhysicalCaptureBudget> {
        self.physical_budget
    }

    pub(super) async fn physical_usage_in_transaction(
        &self,
    ) -> Result<Option<PhysicalCaptureUsage>, OutboxStoreError> {
        let Some(budget) = self.physical_budget else {
            return Ok(None);
        };
        measure_physical_capture(&self.connection, &self.path, budget.physical_limit())
            .await
            .map(Some)
    }

    pub async fn physical_capture_usage(
        &self,
    ) -> Result<Option<PhysicalCaptureUsage>, OutboxStoreError> {
        let _operation = self.operation_gate.lock().await;
        self.physical_usage_in_transaction().await
    }

    /// Reclaim checkpointable WAL without waiting for an external reader. This
    /// never deletes accepted rows or identities. A pinned reader leaves capture
    /// paused rather than occupying the safety worker's connection for seconds.
    pub async fn try_reclaim_physical_wal(&self) -> Result<bool, OutboxStoreError> {
        if self.physical_budget.is_none() {
            return Ok(false);
        }
        let _operation = self.operation_gate.lock().await;
        struct RestoreTimeout(Arc<Connection>);
        impl Drop for RestoreTimeout {
            fn drop(&mut self) {
                let _ = self.0.busy_timeout(Duration::from_secs(5));
            }
        }
        self.connection.busy_timeout(Duration::ZERO).map_err(db)?;
        let _restore = RestoreTimeout(self.connection.clone());
        let mut rows = match self
            .connection
            .query("PRAGMA wal_checkpoint(TRUNCATE)", ())
            .await
        {
            Ok(rows) => rows,
            Err(turso::Error::Busy(_) | turso::Error::BusySnapshot(_)) => return Ok(false),
            Err(error) => return Err(db(error)),
        };
        let mut complete = false;
        loop {
            match rows.next().await {
                Ok(Some(row)) => {
                    complete = row.get::<i64>(0).map_err(db)? == 0;
                }
                Ok(None) => break,
                Err(turso::Error::Busy(_) | turso::Error::BusySnapshot(_)) => return Ok(false),
                Err(error) => return Err(db(error)),
            }
        }
        Ok(complete)
    }

    /// Called before COMMIT, after payload/identity/cursor/index mutations. A
    /// refusal rolls back all of them. Operational facts use separate APIs and
    /// may use reserved headroom; they are never customer-data bypasses.
    pub(super) async fn check_physical_capture_in_transaction(
        &self,
    ) -> Result<(), OutboxStoreError> {
        #[cfg(test)]
        self.pause_mutation_for_test().await;
        let Some(physical) = self.physical_usage_in_transaction().await? else {
            return Ok(());
        };
        let budget = self
            .physical_budget
            .expect("physical snapshot has a policy");
        let retained = self.capture_retained_bytes_in_transaction().await?;
        if retained > budget.payload_limit_bytes || physical.exceeds_data_limit() {
            return Err(OutboxStoreError::CapturePaused(CaptureCapacity {
                retained_bytes: retained,
                requested_bytes: 0,
                limit_bytes: budget.payload_limit_bytes,
                physical: Some(physical),
            }));
        }
        Ok(())
    }

    /// Retry/acknowledgement metadata may use operational headroom, but cannot
    /// grow a pinned WAL indefinitely. Refusal rolls back the optional cloud
    /// operation; it never releases the only accepted copy or broker state.
    pub(super) async fn check_operational_physical_in_transaction(
        &self,
    ) -> Result<(), OutboxStoreError> {
        #[cfg(test)]
        self.pause_mutation_for_test().await;
        let Some(physical) = self.physical_usage_in_transaction().await? else {
            return Ok(());
        };
        if physical.projected_commit_bytes > physical.limit_bytes {
            return Err(OutboxStoreError::CapturePaused(CaptureCapacity {
                retained_bytes: self.retained_bytes_for_startup_refusal().await?,
                requested_bytes: 0,
                limit_bytes: self.physical_budget.unwrap().payload_limit_bytes,
                physical: Some(physical),
            }));
        }
        Ok(())
    }

    /// Caller holds operation_gate and the supplied transaction. Roll back the
    /// complete post-image if operational metadata cannot fit its partition.
    pub(super) async fn commit_bounded_metadata(
        &self,
        tx: turso::transaction::Transaction<'_>,
    ) -> Result<(), OutboxStoreError> {
        if let Err(error) = self.check_operational_physical_in_transaction().await {
            let _ = tx.rollback().await;
            return Err(error);
        }
        tx.commit().await.map_err(db)
    }

    #[cfg(test)]
    async fn pause_mutation_for_test(&self) {
        let pause = self.mutation_pause.lock().take();
        if let Some((entered, resume)) = pause {
            entered.notify_one();
            resume.notified().await;
        }
    }

    /// Caller holds operation_gate and has no open transaction. Recovery
    /// ownership changes commit only when their complete post-image fits.
    pub(super) async fn execute_bounded_metadata(
        &self,
        sql: impl AsRef<str>,
        params: impl turso::IntoParams,
    ) -> Result<u64, OutboxStoreError> {
        if self.physical_budget.is_none() {
            return self.connection.execute(sql, params).await.map_err(db);
        }
        let tx = turso::transaction::Transaction::new_unchecked(
            &self.connection, turso::transaction::TransactionBehavior::Immediate,
        ).await.map_err(db)?;
        let result = async {
            let changed = self.connection.execute(sql, params).await.map_err(db)?;
            self.check_operational_physical_in_transaction().await?;
            Ok(changed)
        }.await;
        match result {
            Ok(changed) => { tx.commit().await.map_err(db)?; Ok(changed) }
            Err(error) => { let _ = tx.rollback().await; Err(error) }
        }
    }

    /// Startup is single-owner and may precede the aggregate schema. Refusal
    /// diagnostics may inventory byte lengths once; ordinary admissions always
    /// use the incremental aggregate and never scan historical payloads.
    async fn retained_bytes_for_startup_refusal(&self) -> Result<u64, OutboxStoreError> {
        let mut schema = self.connection.query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='capture_capacity'", (),
        ).await.map_err(db)?;
        let installed = schema.next().await.map_err(db)?.is_some();
        drop(schema);
        if installed {
            return self.capture_retained_bytes_in_transaction().await;
        }
        let mut rows = self.connection.query(
            "SELECT COALESCE((SELECT SUM(length(wire_payload)) FROM outbox_events),0)
                  + COALESCE((SELECT SUM(length(wire_payload)) FROM dead_letters),0)", (),
        ).await.map_err(db)?;
        let bytes = rows.next().await.map_err(db)?.ok_or_else(||db("missing legacy byte inventory"))?
            .get::<i64>(0).map_err(db)?;
        u64::try_from(bytes).map_err(db)
    }

    /// Internal startup SQL only; no transaction-control statements or PRAGMAs.
    /// A refused post-image rolls back DDL, indexes and cleanup together.
    pub(super) async fn execute_bounded_metadata_batch(&self, sql: &str) -> Result<(), OutboxStoreError> {
        // Preserve atomic legacy cursor/table replacement even for an uncapped
        // standalone store. Only the admission check is optional.
        let tx = turso::transaction::Transaction::new_unchecked(
            &self.connection, turso::transaction::TransactionBehavior::Immediate,
        ).await.map_err(db)?;
        if let Err(error) = self.connection.execute_batch(sql).await.map_err(db) {
            let _ = tx.rollback().await;
            return Err(error);
        }
        self.commit_bounded_metadata(tx).await
    }

    /// Physical denial is detected against the attempted post-image. Restore
    /// truthful retained/requested payload figures after the transaction rolls
    /// back; the physical value remains explicitly a denied commit estimate.
    pub(super) async fn normalize_physical_refusal(
        &self,
        mut capacity: CaptureCapacity,
    ) -> Result<CaptureCapacity, OutboxStoreError> {
        if capacity.physical.is_some() {
            let retained = self.capture_retained_bytes_in_transaction().await?;
            capacity.requested_bytes = capacity.retained_bytes.saturating_sub(retained);
            capacity.retained_bytes = retained;
        }
        Ok(capacity)
    }
}

#[cfg(test)]
mod tests;
