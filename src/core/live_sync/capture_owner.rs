//! Exclusive local source ownership. This is not server authentication or
//! authority to rewrite accepted events; it fences local gap adoption/import.
use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
};

/// Local producer/importer compatibility gate, independent of the event wire
/// protocol and the gateway's server-side reconciliation contract version.
pub const CAPTURE_OWNER_LEASE_VERSION: u16 = 1;

#[derive(Debug)]
pub enum CaptureOwnerError {
    Busy,
    Invalid,
    Io,
}

impl std::fmt::Display for CaptureOwnerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Busy => "cloud source is still owned by another process",
            Self::Invalid => "cloud source ownership path or scope is invalid",
            Self::Io => "cloud source ownership file could not be opened or locked",
        })
    }
}
impl std::error::Error for CaptureOwnerError {}

pub struct CaptureOwnerLease {
    _lock: File,
    pub(super) path: PathBuf,
    pub(crate) runtime: String,
    pub(crate) strategy: String,
    pub(crate) session: String,
    pub(crate) run: String,
    observation_claim:std::sync::Mutex<Option<uuid::Uuid>>,
    pub(super) journal_recovery_gate:tokio::sync::Mutex<()>,
}

impl CaptureOwnerLease {
    /// Acquire before opening a source publisher or importing its exited work.
    /// Keep the lease alive until every producer/reader owned by that process
    /// has stopped. Historical event identities must never be rewritten.
    pub fn acquire(
        outbox: &Path,
        runtime: &str,
        strategy: &str,
        session: &str,
        run: &str,
    ) -> Result<Self, CaptureOwnerError> {
        if [runtime, strategy, session, run]
            .iter()
            .any(|s| s.is_empty() || s.len() > 512)
        {
            return Err(CaptureOwnerError::Invalid);
        }
        Self::acquire_inner(outbox, Some((runtime, strategy, session, run)))
    }

    /// Import owns the file but has no authority to adopt/resume its gap. This
    /// also supports older registrations whose run identity was not recorded.
    pub fn acquire_for_import(outbox: &Path) -> Result<Self, CaptureOwnerError> {
        Self::acquire_inner(outbox, None)
    }

    fn acquire_inner(
        outbox: &Path,
        scope: Option<(&str, &str, &str, &str)>,
    ) -> Result<Self, CaptureOwnerError> {
        let path = source_path(outbox)?;
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".capture-owner.lock");
        let lock_path = PathBuf::from(lock_path);
        reject_non_file(&lock_path)?;
        let mut options = OpenOptions::new();
        options.create(true).read(true).write(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        let lock = options
            .open(&lock_path)
            .map_err(|_| CaptureOwnerError::Io)?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => return Err(CaptureOwnerError::Busy),
            Err(_) => return Err(CaptureOwnerError::Io),
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            lock.set_permissions(std::fs::Permissions::from_mode(0o600))
                .map_err(|_| CaptureOwnerError::Io)?;
        }
        let (runtime, strategy, session, run) = scope.unwrap_or_default();
        Ok(Self {
            _lock: lock,
            path,
            runtime: runtime.into(),
            strategy: strategy.into(),
            session: session.into(),
            run: run.into(),
            observation_claim:std::sync::Mutex::new(None),
            journal_recovery_gate:tokio::sync::Mutex::new(()),
        })
    }

    pub(crate) fn matches_path(&self, path: &Path) -> bool {
        source_path(path).is_ok_and(|path| path == self.path)
    }
    /// An OS file lease is shared by publisher clones; it must not authorize
    /// two independently activated observers to overwrite each other's witness.
    pub(crate) fn claim_source_observation(&self,token:uuid::Uuid)->Result<(),CaptureOwnerError>{
        let mut claim=self.observation_claim.lock().map_err(|_|CaptureOwnerError::Invalid)?;
        if claim.is_some_and(|current|current!=token){return Err(CaptureOwnerError::Busy);}
        *claim=Some(token);Ok(())
    }
    pub(crate) fn release_source_observation(&self,token:uuid::Uuid){
        if let Ok(mut claim)=self.observation_claim.lock(){if *claim==Some(token){*claim=None;}}
    }
}

fn reject_non_file(path: &Path) -> Result<(), CaptureOwnerError> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.is_file() && !meta.file_type().is_symlink() => Ok(()),
        Ok(_) => Err(CaptureOwnerError::Invalid),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(CaptureOwnerError::Io),
    }
}

fn source_path(path: &Path) -> Result<PathBuf, CaptureOwnerError> {
    reject_non_file(path)?;
    let name = path.file_name().ok_or(CaptureOwnerError::Invalid)?;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let parent = parent.canonicalize().map_err(|_| CaptureOwnerError::Io)?;
    Ok(parent.join(name))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lease_fences_other_owners_and_is_bound_to_the_exact_source() {
        let dir = std::env::temp_dir().join(format!("capture-owner-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("publisher.db");
        let lease =
            CaptureOwnerLease::acquire(&path, "runtime", "strategy", "session", "run").unwrap();
        assert!(matches!(
            CaptureOwnerLease::acquire(&path, "next", "strategy", "session", "next-run"),
            Err(CaptureOwnerError::Busy)
        ));
        assert!(lease.matches_path(&path));
        assert!(!lease.matches_path(&dir.join("another.db")));
        drop(lease);
        let next =
            CaptureOwnerLease::acquire(&path, "next", "strategy", "session", "next-run").unwrap();
        drop(next);
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(
                dir.join("publisher.db.capture-owner.lock"),
                dir.join("link.db"),
            )
            .unwrap();
            assert!(matches!(
                CaptureOwnerLease::acquire(
                    &dir.join("link.db"),
                    "runtime",
                    "strategy",
                    "session",
                    "run"
                ),
                Err(CaptureOwnerError::Invalid)
            ));
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
}
