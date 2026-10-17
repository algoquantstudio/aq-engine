//! Durable normal-live ownership. Run identities are intentionally absent from
//! the journal path: restarting the same strategy/account must replay its work.
use super::*;
use crate::core::broker::commands::BrokerCommandError;

fn scope_strategy_id(value: &str) -> &str {
    let value = value.trim();
    value.strip_prefix("strategy:").unwrap_or(value)
}

impl<S: Strategy, E: Broker + OrderManagementProvider, D: DataFeed + DataProvider>
    StrategyState<S, E, D>
{
    /// Persist broker execution facts without changing orders, Insight state or
    /// recovery flags. Missing/failed delivery never claims complete analytics.
    pub(super) async fn persist_live_fill_evidence(
        &self,
        fills: Vec<aqe_live_protocol::ExecutionFillInput>,
    ) -> Result<(), BrokerCommandError> {
        if fills.is_empty() {
            return Ok(());
        }
        let batch = crate::core::broker::command_journal::ExecutionFillBatch::new(fills)?;
        let driver = self.command_driver.as_ref().ok_or_else(|| {
            BrokerCommandError::Journal("execution fill journal is unavailable".into())
        })?;
        driver.sender().record_execution_fills(batch).await
    }

    pub(super) fn live_execution_journal_path(&self, strategy: &str, account: &str) -> PathBuf {
        // Length-delimited encoding prevents ambiguous scope concatenations;
        // hashing also keeps untrusted account IDs out of filesystem paths.
        let scope = serde_json::to_vec(&(scope_strategy_id(strategy), account.trim()))
            .expect("string tuple serialization is infallible");
        let key = Uuid::new_v5(&Uuid::NAMESPACE_OID, &scope);
        self.artifact_root()
            .join(".aqs/execution/live")
            .join(key.to_string())
            .join("commands.db")
    }

    pub(super) fn live_execution_failure(&self) -> Option<BrokerCommandError> {
        self.execution_error.clone().or_else(|| {
            self.command_driver
                .as_ref()
                .and_then(|driver| driver.unresolved_execution_error())
        })
    }

    pub(super) fn live_execution_pending(&self) -> bool {
        self.has_pending_broker_events()
            || self
                .command_driver
                .as_ref()
                .is_some_and(|driver| driver.has_in_flight_work() || driver.pending_count() != 0)
    }

    /// Always await worker ownership, even if settlement proof failed. Closing
    /// its receiver preserves in-flight evidence in the journal, not in a
    /// detached task using an already-disconnected transport.
    pub(super) async fn stop_live_execution_worker(&mut self) -> Result<(), BrokerError> {
        match self.command_driver.take() {
            Some(driver) => driver.stop().await.map(drop).map_err(|error| {
                BrokerError::OrderError(format!("live execution worker shutdown failed: {error}"))
            }),
            None => Ok(()),
        }
    }

    pub(super) async fn finish_live_execution(
        &mut self,
        analytics: Option<&mut super::live_event_projection::LocalBatchSyncState>,
    ) -> Result<(), BrokerError> {
        let settled = match self.live_execution_failure() {
            Some(error) => Err(BrokerError::OrderError(error.to_string())),
            None => match &self.command_driver {
                Some(driver) => match driver.complete_execution_stop().await {
                    Ok(true) => Ok(()),
                    Ok(false) => Err(BrokerError::OrderError(
                        "Live teardown still has unresolved broker work or exposure; durable stop remains incomplete".into())),
                    Err(error) => Err(BrokerError::OrderError(error.to_string())),
                },
                None => Err(BrokerError::OrderError("Live execution driver is unavailable".into())),
            },
        };
        // Export the checkpoints durable so far after attempting settlement,
        // while the journal owner still services read/receipt requests.
        // Analytics deferral must never invalidate broker settlement or submit
        // any order; unfinished pages remain durable for a later launch.
        if let (Some(sync), Some(driver)) = (analytics, self.command_driver.as_ref()) {
            if let Err(error) = sync.finish_execution_evidence(&driver.sender()).await {
                log::warn!("Final execution analytics delivery deferred: {error}");
            }
        }
        let release = self.stop_live_execution_worker().await;
        settled.and(release)
    }
}

impl<
    S: Strategy,
    E: Broker + OrderManagementProvider + 'static,
    D: DataFeed + DataProvider + 'static,
> StrategyState<S, E, D>
{
    pub(super) async fn start_live_execution(
        &mut self,
        auth: Option<&AqsAuth>,
    ) -> Result<(), BrokerError> {
        let account = self.broker.account_snapshot()?;
        let cache = self.broker.live_snapshot_cache().ok_or_else(|| {
            BrokerError::OrderError(
                "Live execution requires a scoped account snapshot cache".into(),
            )
        })?;
        let strategy = self.live_metrics_strategy_key(auth);
        let strategy = scope_strategy_id(&strategy);
        if strategy.is_empty() {
            return Err(BrokerError::OrderError(
                "Live execution strategy identity is empty".into(),
            ));
        }
        let path = self.live_execution_journal_path(strategy, &account.account_id);
        self.start_command_driver_with_recovery_signal(
            path,
            strategy,
            account.account_id.trim(),
            Default::default(),
            false,
        )
        .await
        .map_err(|error| {
            BrokerError::OrderError(format!("Live execution recovery failed: {error}"))
        })?;
        self.command_driver
            .as_ref()
            .expect("driver installed before admission")
            .install_snapshot_guard(cache)
            .map_err(|error| {
                BrokerError::OrderError(format!("Live execution admission failed: {error}"))
            })?;
        self.broker.start_connection_worker(Default::default())
    }

    pub(super) async fn resume_after_recovered_stop(
        &mut self,
        auth: Option<&AqsAuth>,
    ) -> Result<(), BrokerError> {
        self.finish_live_execution(None).await?;
        self.prepare_recovered_live_restart()?;
        let account = self.broker.account_snapshot()?;
        let key = self.live_metrics_strategy_key(auth);
        let strategy = scope_strategy_id(&key);
        self.start_command_driver_with_recovery_signal(
            self.live_execution_journal_path(strategy, &account.account_id),
            strategy,
            account.account_id.trim(),
            Default::default(),
            false,
        )
        .await
        .map_err(|error| BrokerError::OrderError(error.to_string()))?;
        let cache = self
            .broker
            .live_snapshot_cache()
            .ok_or_else(|| BrokerError::AccountError("Missing recovery account snapshot".into()))?;
        self.command_driver
            .as_ref()
            .unwrap()
            .install_snapshot_guard(cache)
            .map_err(|error| BrokerError::OrderError(error.to_string()))?;
        if *self.shutdown_tx.borrow() {
            self.command_driver
                .as_ref()
                .unwrap()
                .request_execution_stop();
            return Err(BrokerError::OrderError(
                "Startup recovery interrupted while opening the next epoch".into(),
            ));
        }
        self.teardown_cleanup_requested = false;
        self.status = StrategyStatus::Running;
        Ok(())
    }

    /// Called only after the old epoch has durably settled and its worker exited.
    pub(super) fn prepare_recovered_live_restart(&mut self) -> Result<(), BrokerError> {
        // Never clear a stop received by this launch while the old epoch commits.
        if *self.shutdown_tx.borrow() {
            return Err(BrokerError::OrderError(
                "Startup recovery interrupted by a new stop request".into(),
            ));
        }
        if self.command_driver.is_some() {
            return Err(BrokerError::OrderError(
                "Previous execution worker still owns the recovered epoch".into(),
            ));
        }
        // Otherwise installation mistakes the recovered stop for a fresh stop
        // and irreversibly closes the new worker's entry gate.
        if self.insights.active_insight_count() != 0 {
            return Err(BrokerError::OrderError(
                "Recovered execution still has active insights".into(),
            ));
        }
        // Terminal rows belong to the settled epoch. Reusing them with the new
        // driver's revision cache would replay obsolete checkpoint revisions.
        self.insights = InsightCollection::new();
        self.insight_work = Default::default();
        self.insight_pipeline.deferred.clear();
        self.insight_pipeline.snapshot_dependencies = Default::default();
        let empty = Default::default();
        for pipes in self.insight_pipeline.pipeline.values_mut() {
            for pipe in pipes {
                pipe.retain_active_insights(&empty);
            }
        }
        self.status = StrategyStatus::Initialised;
        Ok(())
    }
}
