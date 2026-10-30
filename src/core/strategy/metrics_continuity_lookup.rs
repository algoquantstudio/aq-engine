//! Owned, single-flight Cloud continuity reads. No tracker mutation happens here.

use super::aqs_reader::MetricAccountOrigin;
use super::aqs_types::{AqsAuth, StrategyLiveMetricsRecord};
use std::future::Future;
use std::time::Duration;
use tokio::task::{JoinError, JoinSet};

pub(super) struct ContinuityLookupResult {
    pub strategy_id: String,
    pub account_id: String,
    pub metrics: Result<Option<StrategyLiveMetricsRecord>, String>,
    pub baseline: Option<MetricAccountOrigin>,
}

struct PreparedContinuity {
    metrics: Option<StrategyLiveMetricsRecord>,
    baseline: Option<MetricAccountOrigin>,
}

async fn read_prepared_continuity<C: surrealdb::Connection>(
    client: &surrealdb::Surreal<C>,
    auth: &AqsAuth,
) -> Result<PreparedContinuity, String> {
    let (metrics, baseline) = tokio::try_join!(
        read_with_deadline(super::aqs_reader::load_latest_live_metrics(client, auth)),
        read_with_deadline(super::aqs_reader::load_strategy_account_baseline(
            client, auth
        )),
    )?;
    if let Some(record) = metrics.as_ref() {
        require_current_checkpoint(record)?;
    }
    Ok(PreparedContinuity { metrics, baseline })
}

pub(super) fn require_current_checkpoint(record: &StrategyLiveMetricsRecord) -> Result<(), String> {
    let checkpoint = record
        .tracker_checkpoint
        .as_ref()
        .ok_or("unsupported legacy Cloud metrics: a full tracker checkpoint is required")?;
    if checkpoint.version != super::types::live_metrics::LIVE_METRICS_CHECKPOINT_VERSION {
        return Err("unsupported Cloud metrics checkpoint version".into());
    }
    Ok(())
}

async fn read_with_deadline<T>(
    read: impl Future<Output = Result<T, surrealdb::Error>>,
) -> Result<T, String> {
    tokio::time::timeout(Duration::from_secs(10), read)
        .await
        .map_err(|_| "Cloud continuity query timed out".to_string())?
        .map_err(|error| error.to_string())
}

#[derive(Default)]
pub(super) struct MetricsContinuityLookup {
    jobs: JoinSet<ContinuityLookupResult>,
}

impl MetricsContinuityLookup {
    pub fn is_running(&self) -> bool {
        !self.jobs.is_empty()
    }

    pub fn start<C: surrealdb::Connection>(
        &mut self,
        client: surrealdb::Surreal<C>,
        auth: AqsAuth,
        account_id: String,
    ) -> bool {
        let strategy_id = auth.strategy_id.clone();
        self.start_prepared_read(
            strategy_id,
            account_id,
            Duration::from_secs(30),
            async move { read_prepared_continuity(&client, &auth).await },
        )
    }

    #[cfg(test)]
    fn start_read(
        &mut self,
        strategy_id: String,
        account_id: String,
        timeout: Duration,
        read: impl Future<Output = Result<Option<StrategyLiveMetricsRecord>, String>> + Send + 'static,
    ) -> bool {
        self.start_prepared_read(strategy_id, account_id, timeout, async move {
            read.await.map(|metrics| PreparedContinuity {
                metrics,
                baseline: None,
            })
        })
    }

    fn start_prepared_read(
        &mut self,
        strategy_id: String,
        account_id: String,
        timeout: Duration,
        read: impl Future<Output = Result<PreparedContinuity, String>> + Send + 'static,
    ) -> bool {
        if self.is_running() {
            return false;
        }
        self.jobs.spawn(async move {
            let prepared = tokio::time::timeout(timeout, read)
                .await
                .unwrap_or_else(|_| Err("Cloud metrics continuity lookup timed out".into()));
            let (metrics, baseline) = match prepared {
                Ok(prepared) => (Ok(prepared.metrics), prepared.baseline),
                Err(error) => (Err(error), None),
            };
            ContinuityLookupResult {
                strategy_id,
                account_id,
                metrics,
                baseline,
            }
        });
        true
    }

    pub async fn next(&mut self) -> Option<Result<ContinuityLookupResult, JoinError>> {
        self.jobs.join_next().await
    }

    pub async fn shutdown(&mut self) {
        // JoinSet retains ownership if the caller cancels this drain; Drop also
        // aborts any remaining reads. This set never contains broker mutations.
        self.jobs.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn continuity_lookup_delayed_checkpoint_preserves_new_trades_and_exposure() {
        use crate::core::strategy::live_metrics::LivePerformanceTracker;
        use crate::core::strategy::types::live_metrics::HistoricalClosedInsightMetric;
        let now = chrono::Utc::now();
        let trade = |id: &str, pnl, at| HistoricalClosedInsightMetric {
            insight_id: id.into(),
            side: "Buy".into(),
            quantity: Some(1.0),
            limit_price: None,
            stop_price: None,
            filled_price: Some(100.0),
            close_price: Some(100.0 + pnl),
            broker_realized_pnl: Some(pnl),
            commission: Some(0.0),
            swap: Some(0.0),
            created_at: at,
            updated_at: at,
            filled_at: Some(at),
            closed_at: Some(at),
        };
        let mut historical = LivePerformanceTracker::default();
        historical.initialize_for_account(
            Some("account".into()),
            100.0,
            110.0,
            now - chrono::TimeDelta::hours(2),
            ["ETHUSD".into()],
        );
        historical.replace_trade_history([trade(
            "historical",
            10.0,
            now - chrono::TimeDelta::hours(1),
        )]);
        let record = StrategyLiveMetricsRecord::from_snapshot(
            historical.snapshot().unwrap(),
            historical.checkpoint(),
        );
        let mut current = LivePerformanceTracker::default();
        current.initialize_for_account(
            Some("account".into()),
            110.0,
            110.0,
            now,
            ["SOLUSD".into()],
        );
        current.mark_history_recovery_incomplete();
        let mut worker = MetricsContinuityLookup::default();
        let (release, wait) = tokio::sync::oneshot::channel();
        worker.start_read(
            "strategy".into(),
            "account".into(),
            Duration::from_secs(5),
            async move {
                wait.await.unwrap();
                Ok(Some(record))
            },
        );
        // Live state advances while the Cloud request is outstanding.
        current.replace_trade_history([trade(
            "during-lookup",
            20.0,
            now + chrono::TimeDelta::seconds(1),
        )]);
        current.update_equity(130.0, now + chrono::TimeDelta::seconds(2));
        current.update_open_position_metrics(
            3,
            4,
            7.0,
            2,
            1,
            vec!["SOLUSD".into()],
            now + chrono::TimeDelta::seconds(2),
        );
        release.send(()).unwrap();
        let checkpoint = worker
            .next()
            .await
            .unwrap()
            .unwrap()
            .metrics
            .unwrap()
            .unwrap()
            .tracker_checkpoint
            .unwrap();
        current
            .merge_historical_checkpoint(checkpoint.clone())
            .unwrap();
        current.mark_history_recovery_complete();
        let merged = current.checkpoint().unwrap();
        assert_eq!(merged.total_trades, 2);
        assert_eq!(merged.gross_profit, 30.0);
        assert_eq!(merged.starting_cash, 100.0);
        assert_eq!(merged.current_equity, 130.0);
        assert_eq!(merged.open_positions_count, 3);
        assert_eq!(merged.open_insights_count, 4);
        assert_eq!(merged.open_positions_unrealized_pnl, 7.0);
        current.merge_historical_checkpoint(checkpoint).unwrap();
        assert_eq!(current.checkpoint().unwrap(), merged);
    }

    #[tokio::test]
    async fn continuity_lookup_is_single_flight_and_does_not_block_owner() {
        let mut worker = MetricsContinuityLookup::default();
        let (release, wait) = tokio::sync::oneshot::channel();
        assert!(worker.start_read(
            "strategy".into(),
            "account".into(),
            Duration::from_secs(5),
            async move {
                wait.await.unwrap();
                Ok(None)
            }
        ));
        assert!(!worker.start_read(
            "strategy".into(),
            "account".into(),
            Duration::from_secs(5),
            async { panic!("duplicate must not start") }
        ));
        // Cancelling a selection of the completion future must not lose its job.
        assert!(
            tokio::time::timeout(Duration::from_millis(10), worker.next())
                .await
                .is_err()
        );
        assert!(worker.is_running());
        let (stop_tx, mut stop_rx) = tokio::sync::mpsc::channel(1);
        stop_tx.send(()).await.unwrap();
        tokio::select! {
            biased;
            Some(()) = stop_rx.recv() => {},
            _ = worker.next() => panic!("slow lookup blocked stop handling"),
        }
        release.send(()).unwrap();
        let result = worker.next().await.unwrap().unwrap();
        assert_eq!(result.strategy_id, "strategy");
        assert_eq!(result.account_id, "account");
        assert!(result.metrics.unwrap().is_none());
        assert!(!worker.is_running());
    }

    #[tokio::test]
    async fn continuity_lookup_failure_and_timeout_are_not_missing_history() {
        let mut worker = MetricsContinuityLookup::default();
        worker.start_read(
            "strategy".into(),
            "account".into(),
            Duration::from_secs(1),
            async { Err("auth unavailable".into()) },
        );
        assert!(worker.next().await.unwrap().unwrap().metrics.is_err());
        worker.start_read(
            "strategy".into(),
            "account".into(),
            Duration::from_millis(10),
            std::future::pending(),
        );
        assert!(worker.next().await.unwrap().unwrap().metrics.is_err());
        assert!(!worker.is_running());
    }

    #[tokio::test]
    async fn continuity_lookup_shutdown_and_drop_release_owned_reads() {
        let retained = Arc::new(());
        for explicit in [true, false] {
            let mut worker = MetricsContinuityLookup::default();
            let held = retained.clone();
            worker.start_read(
                "strategy".into(),
                "account".into(),
                Duration::from_secs(60),
                async move {
                    let _held = held;
                    std::future::pending().await
                },
            );
            if explicit {
                worker.shutdown().await;
                assert!(!worker.is_running());
            }
            drop(worker);
            tokio::task::yield_now().await;
            assert_eq!(Arc::strong_count(&retained), 1);
        }
    }

    #[tokio::test]
    async fn continuity_lookup_panic_is_reaped_and_later_attempt_can_start() {
        let mut worker = MetricsContinuityLookup::default();
        worker.start_read(
            "strategy".into(),
            "account".into(),
            Duration::from_secs(1),
            async {
                panic!("fake query panic");
            },
        );
        assert!(worker.next().await.unwrap().is_err());
        assert!(!worker.is_running());
        assert!(worker.start_read(
            "strategy".into(),
            "account".into(),
            Duration::from_secs(1),
            async { Ok(None) }
        ));
        assert!(
            worker
                .next()
                .await
                .unwrap()
                .unwrap()
                .metrics
                .unwrap()
                .is_none()
        );
    }
}

#[cfg(test)]
mod current_format_tests {
    use super::*;
    #[test]
    fn aggregate_only_cloud_metrics_are_not_reconstructed_or_promoted() {
        let mut tracker = super::super::live_metrics::LivePerformanceTracker::default();
        tracker.initialize_for_account(Some("account".into()), 100., 105., chrono::Utc::now(), []);
        let old = StrategyLiveMetricsRecord::from_snapshot(tracker.snapshot().unwrap(), None);
        assert!(
            require_current_checkpoint(&old)
                .unwrap_err()
                .contains("full tracker checkpoint")
        );
        let current = StrategyLiveMetricsRecord::from_snapshot(
            tracker.snapshot().unwrap(),
            tracker.checkpoint(),
        );
        require_current_checkpoint(&current).unwrap();
    }
}
