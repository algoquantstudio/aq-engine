//! Optional host-wide broker-account mutation fence.
//!
//! The process-local [`super::account_lane`] provides priority and capacity
//! reservations. This lock adds crash-safe exclusion between separate AQE
//! processes on the same host. It deliberately performs only one nonblocking
//! filesystem attempt at a time so a waiting entry never owns a Tokio worker or
//! prevents its process-local teardown queue from taking priority.

use super::BrokerCommandError;
use sha2::{Digest, Sha256};
use std::{fs::File, path::PathBuf, sync::Arc};

const COORDINATOR_DIRECTORY_ENV: &str = "AQE_ACCOUNT_COORDINATOR_DIR";
const MAX_COORDINATOR_PATH_BYTES: usize = 4096;
// Bound host filesystem state. A collision only serializes unrelated accounts;
// it cannot weaken exclusion for the same provider/account.
const LOCK_BUCKETS: u16 = 4096;

#[derive(Clone)]
pub(super) struct HostAccountCoordinator {
    lock_path: Arc<PathBuf>,
}

pub(super) struct HostAccountPermit {
    _lock: File,
}

impl HostAccountCoordinator {
    pub fn from_environment(
        provider: &str,
        account: &str,
    ) -> Result<Option<Self>, BrokerCommandError> {
        let Some(root) = crate::launch_config::setting(COORDINATOR_DIRECTORY_ENV)
            .map_err(BrokerCommandError::Invalid)? else {
            return Ok(None);
        };
        let root = PathBuf::from(root);
        if !root.is_absolute() || root.as_os_str().len() > MAX_COORDINATOR_PATH_BYTES {
            return Err(BrokerCommandError::Invalid(format!(
                "{COORDINATOR_DIRECTORY_ENV} must be a bounded absolute path"
            )));
        }
        Self::new(root, provider, account).map(Some)
    }

    fn new(root: PathBuf, provider: &str, account: &str) -> Result<Self, BrokerCommandError> {
        let provider = provider.trim();
        let account = account.trim();
        if provider.is_empty() || provider.len() > 512 || account.is_empty() || account.len() > 512
        {
            return Err(BrokerCommandError::Invalid(
                "invalid host mutation provider/account identity".into(),
            ));
        }
        let scope = serde_json::to_vec(&("aqe-host-account-v1", provider, account))
            .map_err(|error| BrokerCommandError::Invalid(error.to_string()))?;
        let digest = Sha256::digest(scope);
        let bucket = u16::from_be_bytes([digest[0], digest[1]]) % LOCK_BUCKETS;
        Ok(Self {
            lock_path: Arc::new(root.join(format!("account-{bucket:04x}.lock"))),
        })
    }

    pub async fn try_acquire(&self) -> Result<Option<HostAccountPermit>, BrokerCommandError> {
        let path = Arc::clone(&self.lock_path);
        tokio::task::spawn_blocking(move || {
            let parent = path.parent().ok_or_else(|| {
                BrokerCommandError::Journal("account coordinator has no directory".into())
            })?;
            std::fs::create_dir_all(parent).map_err(coordinator_error)?;
            let file = File::options()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(path.as_ref())
                .map_err(coordinator_error)?;
            match file.try_lock() {
                Ok(()) => Ok(Some(HostAccountPermit { _lock: file })),
                Err(std::fs::TryLockError::WouldBlock) => Ok(None),
                Err(std::fs::TryLockError::Error(error)) => Err(coordinator_error(error)),
            }
        })
        .await
        .map_err(coordinator_error)?
    }
}

fn coordinator_error(error: impl std::fmt::Display) -> BrokerCommandError {
    BrokerCommandError::Journal(format!("host account coordination failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> PathBuf {
        std::env::temp_dir().join(format!("aqe-account-lock-{}", uuid::Uuid::new_v4()))
    }

    #[tokio::test]
    async fn exact_account_scope_excludes_an_independent_coordinator() {
        let root = fixture();
        let first = HostAccountCoordinator::new(root.clone(), "broker", "account").unwrap();
        let second = HostAccountCoordinator::new(root.clone(), "broker", "account").unwrap();
        let held = first.try_acquire().await.unwrap().unwrap();
        assert!(second.try_acquire().await.unwrap().is_none());
        drop(held);
        assert!(second.try_acquire().await.unwrap().is_some());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn different_accounts_do_not_share_a_host_lock() {
        let root = fixture();
        let first = HostAccountCoordinator::new(root.clone(), "broker", "first").unwrap();
        let second = HostAccountCoordinator::new(root.clone(), "broker", "second").unwrap();
        let _first = first.try_acquire().await.unwrap().unwrap();
        let _second = second.try_acquire().await.unwrap().unwrap();
        drop((_first, _second));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn raw_broker_identity_is_not_exposed_in_the_lock_name() {
        let coordinator =
            HostAccountCoordinator::new(fixture(), "secret-provider", "secret-account").unwrap();
        let name = coordinator.lock_path.file_name().unwrap().to_string_lossy();
        assert!(name.starts_with("account-"));
        assert_eq!(name.len(), "account-0000.lock".len());
        assert!(!name.contains("secret"));
    }
}
