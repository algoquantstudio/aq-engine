//! Small admin-created recovery fences, separate from replaceable checkpoints.
//! Never delete or rewrite an execution journal to repair performance history.
use super::types::live_metrics::LiveMetricsCheckpoint;
use std::path::Path;

#[derive(serde::Deserialize)]
struct Fence {
    strategy_key: String,
    account_id: String,
    repair_id: String,
}

pub(super) fn accepts(
    path: &Path,
    strategy: &str,
    checkpoint: &LiveMetricsCheckpoint,
) -> Result<bool, String> {
    let path = path.with_extension("risk-reconciliation.json");
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(error) => {
            return Err(format!(
                "Cannot read metrics risk-reconciliation fence: {error}"
            ));
        }
    };
    if bytes.len() > 8192 {
        return Err("Oversized metrics risk-reconciliation fence".into());
    }
    let fence: Fence =
        serde_json::from_slice(&bytes).map_err(|_| "Invalid metrics risk-reconciliation fence")?;
    if fence.strategy_key != strategy || fence.repair_id.is_empty() || fence.account_id.is_empty() {
        return Err("Metrics risk-reconciliation fence scope is invalid".into());
    }
    Ok(fence.account_id != checkpoint.account_id
        || checkpoint
            .equity_risk_reconciliation
            .as_ref()
            .is_some_and(|proof| proof.repair_id == fence.repair_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::strategy::live_metrics::LivePerformanceTracker;
    #[test]
    fn old_writer_cannot_make_an_unreconciled_checkpoint_restorable() {
        struct Directory(std::path::PathBuf);
        impl Drop for Directory {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let directory = Directory(
            std::env::temp_dir().join(format!("aqe-risk-fence-{}", uuid::Uuid::new_v4())),
        );
        std::fs::create_dir(&directory.0).unwrap();
        let path = directory.0.join("strategy.json");
        let mut tracker = LivePerformanceTracker::default();
        tracker.initialize_for_account(
            Some("account".into()),
            100.0,
            100.0,
            chrono::Utc::now(),
            [],
        );
        let mut checkpoint = tracker.checkpoint().unwrap();
        assert!(accepts(&path, "strategy", &checkpoint).unwrap());
        let original = b"original recovery checkpoint";
        std::fs::write(&path, original).unwrap();
        std::fs::write(
            path.with_extension("risk-reconciliation.json"),
            r#"{"strategy_key":"strategy","account_id":"account","repair_id":"audited"}"#,
        )
        .unwrap();
        checkpoint.max_drawdown = 100.0;
        assert!(!accepts(&path, "strategy", &checkpoint).unwrap());
        super::super::metrics_checkpoint_writer::write_checkpoint(
            &path,
            &super::super::local_live_artifacts::LocalLiveMetricsCheckpoint {
                strategy_key: "strategy".into(),
                checkpoint: checkpoint.clone(),
            },
        )
        .unwrap();
        assert_eq!(
            std::fs::read(&path).unwrap(),
            original,
            "Unreconciled writes preserve the old recovery file"
        );
        checkpoint.equity_risk_reconciliation = Some(
            super::super::types::live_metrics::EquityRiskReconciliation {
                repair_id: "audited".into(),
                through: checkpoint.updated_at,
            },
        );
        assert!(accepts(&path, "strategy", &checkpoint).unwrap());
        checkpoint.max_drawdown = 4.25;
        super::super::metrics_checkpoint_writer::write_checkpoint(
            &path,
            &super::super::local_live_artifacts::LocalLiveMetricsCheckpoint {
                strategy_key: "strategy".into(),
                checkpoint: checkpoint.clone(),
            },
        )
        .unwrap();
        let written: super::super::local_live_artifacts::LocalLiveMetricsCheckpoint =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(written.checkpoint.max_drawdown, 4.25);
        checkpoint.account_id = "other-account".into();
        assert!(accepts(&path, "strategy", &checkpoint).unwrap());
        assert!(accepts(&path, "other-strategy", &checkpoint).is_err());
    }
}
