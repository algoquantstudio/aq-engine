use super::StrategyState;
use super::aqs_reader::{
    MetricAccountOrigin, load_closed_insight_metric_page, load_earliest_metric_account_origin,
    load_latest_live_metrics, load_latest_metric_account_origin, load_strategy_account_baseline,
};
use super::aqs_types::AqsAuth;
use super::live_metrics::LivePerformanceTracker;
use super::local_live_artifacts::{LocalLiveMetricsCheckpoint, LocalLiveRunArtifacts};
use super::traits::Strategy;
use super::types::live_metrics::LiveMetricsCheckpoint;
use crate::core::broker::traits::{Broker, DataFeed, DataProvider, OrderManagementProvider};
use crate::core::broker::types::Account;
use log::{info, warn};
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

const LIVE_SYNC_QUERY_TIMEOUT_SECS: u64 = 10;

impl<S, E, D> StrategyState<S, E, D>
where
    S: Strategy,
    E: Broker + OrderManagementProvider,
    D: DataFeed + DataProvider,
{
    pub(super) fn live_metrics_strategy_key(&self, auth: Option<&AqsAuth>) -> String {
        auth.map(|value| value.strategy_id.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| self.strategy_id.to_string())
    }

    pub(super) fn live_metrics_checkpoint_path(&self, strategy_key: &str) -> PathBuf {
        let safe_key: String = strategy_key
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                    character
                } else {
                    '_'
                }
            })
            .collect();
        self.artifact_root()
            .join(".aqs")
            .join("live-metrics")
            .join(format!("{safe_key}.json"))
    }

    pub(super) fn load_local_live_metrics_checkpoint(
        &self,
        strategy_key: &str,
    ) -> Option<LiveMetricsCheckpoint> {
        let path = self.live_metrics_checkpoint_path(strategy_key);
        let fallback_path = path.with_extension("previous");
        let (bytes, loaded_path) = match fs::read(&path) {
            Ok(bytes) => (bytes, path.clone()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match fs::read(&fallback_path) {
                    Ok(bytes) => (bytes, fallback_path),
                    Err(fallback_error)
                        if fallback_error.kind() == std::io::ErrorKind::NotFound =>
                    {
                        return None;
                    }
                    Err(fallback_error) => {
                        warn!(
                            "Failed to read fallback local live metrics checkpoint {}: {}",
                            fallback_path.display(),
                            fallback_error
                        );
                        return None;
                    }
                }
            }
            Err(error) => {
                warn!(
                    "Failed to read local live metrics checkpoint {}: {}",
                    path.display(),
                    error
                );
                return None;
            }
        };
        match serde_json::from_slice::<LocalLiveMetricsCheckpoint>(&bytes) {
            Ok(stored) if stored.strategy_key == strategy_key => {
                match super::metrics_risk_fence::accepts(&path, strategy_key, &stored.checkpoint) {
                    Ok(true) => Some(stored.checkpoint),
                    result => {
                        warn!(
                            "Local metrics risk history requires the audited Cloud checkpoint; original file retained: {:?}",
                            result.err()
                        );
                        None
                    }
                }
            }
            Ok(stored) => {
                warn!(
                    "Ignoring local live metrics checkpoint {} for unexpected strategy {}",
                    loaded_path.display(),
                    stored.strategy_key
                );
                None
            }
            Err(error) => {
                warn!(
                    "Ignoring corrupted local live metrics checkpoint {}: {}",
                    loaded_path.display(),
                    error
                );
                None
            }
        }
    }

    pub(super) fn persist_local_live_metrics_checkpoint(&mut self, strategy_key: &str) {
        let Some(checkpoint) = self.live_metrics.checkpoint() else {
            return;
        };
        if self.metrics_checkpoint_writer.is_none() {
            let path = self.live_metrics_checkpoint_path(strategy_key);
            match super::metrics_checkpoint_writer::MetricsCheckpointWriter::start(move |stored| {
                super::metrics_checkpoint_writer::write_checkpoint(&path, stored)
            }) {
                Ok(writer) => self.metrics_checkpoint_writer = Some(writer),
                Err(error) => {
                    warn!("Failed to start local metrics checkpoint writer: {error}");
                    return;
                }
            }
        }
        let stored = LocalLiveMetricsCheckpoint {
            strategy_key: strategy_key.to_string(),
            checkpoint,
        };
        if let Some(writer) = &self.metrics_checkpoint_writer {
            if let Err(error) = writer.submit(stored) {
                warn!("Failed to queue local metrics checkpoint: {error}");
            }
        }
    }

    pub(super) async fn stop_metrics_checkpoint_writer(&mut self) {
        if let Some(writer) = self.metrics_checkpoint_writer.as_mut() {
            if let Err(error) = writer.shutdown().await {
                warn!("Failed to drain local metrics checkpoint writer: {error}");
            }
        }
        self.metrics_checkpoint_writer = None;
    }

    pub(super) fn start_legacy_live_trade_metrics<C: surrealdb::Connection>(
        &mut self,
        client: &surrealdb::Surreal<C>,
        auth: &AqsAuth,
    ) {
        self.live_metrics_cloud_write_enabled = false;
        self.legacy_metrics_recovery_needed = true;
        if self.legacy_metrics_recovery.is_running() {
            return;
        }
        let token = match self.live_metrics.begin_trade_history_recovery() {
            Ok(token) => token,
            Err(error) => {
                warn!(
                    "Cannot start legacy metrics recovery for strategy {}: {error}",
                    auth.strategy_id
                );
                self.live_metrics_cloud_write_enabled = false;
                return;
            }
        };
        let client = client.clone();
        let auth = auth.clone();
        if let Err(error) = self
            .legacy_metrics_recovery
            .start(token.clone(), async move {
                super::legacy_metrics_recovery::load_trade_history_pages(
                    &auth.strategy_id,
                    |offset, limit| {
                        let client = &client;
                        let auth = &auth;
                        async move {
                            load_closed_insight_metric_page(client, auth, offset, limit)
                                .await
                                .map_err(|error| error.to_string())
                        }
                    },
                )
                .await
            })
        {
            self.live_metrics.cancel_trade_history_recovery(&token);
            warn!("Cannot queue legacy metrics recovery: {error}");
        }
    }

    pub(super) fn apply_legacy_live_trade_metrics(
        &mut self,
        completed: super::legacy_metrics_recovery::LegacyMetricsRecoveryResult,
        auth: Option<&AqsAuth>,
    ) {
        if !self
            .live_metrics
            .is_trade_history_recovery_current(&completed.token)
        {
            warn!("Ignoring obsolete legacy metrics recovery result");
            return;
        }
        let account_matches = self
            .broker
            .account_snapshot()
            .ok()
            .is_some_and(|account| completed.token.matches_account(&account.account_id));
        let result = if account_matches {
            completed.result.and_then(|rebuilt| {
                self.live_metrics
                    .try_apply_trade_history_recovery(&completed.token, rebuilt)
            })
        } else {
            Err("Broker account is unavailable or changed during legacy metrics recovery".into())
        };
        self.live_metrics
            .cancel_trade_history_recovery(&completed.token);
        match result {
            Ok(()) => {
                self.legacy_metrics_recovery_needed = false;
                self.live_metrics_cloud_write_enabled = true;
                let key = self.live_metrics_strategy_key(auth);
                self.persist_local_live_metrics_checkpoint(&key);
                info!("Legacy metrics recovery committed for strategy {key}");
            }
            Err(error) => {
                self.live_metrics.mark_history_recovery_incomplete();
                self.live_metrics_cloud_write_enabled = false;
                warn!(
                    "Legacy metrics recovery not committed; current metrics preserved and Cloud writes deferred: {error}"
                );
            }
        }
    }

    pub(super) async fn stop_legacy_metrics_recovery(&mut self) {
        if let Some(token) = self.legacy_metrics_recovery.shutdown().await {
            self.live_metrics.cancel_trade_history_recovery(&token);
        }
    }

    pub(super) async fn initialize_live_metrics_continuity<C: surrealdb::Connection>(
        &mut self,
        client: Option<&surrealdb::Surreal<C>>,
        auth: Option<&AqsAuth>,
        initial_account: Option<&Account>,
        executed_at: chrono::DateTime<chrono::Utc>,
        symbols: Vec<String>,
    ) -> Option<String> {
        let baseline_read = async {
            if let (Some(client), Some(auth)) = (client, auth) {
                match tokio::time::timeout(
                    Duration::from_secs(LIVE_SYNC_QUERY_TIMEOUT_SECS),
                    load_strategy_account_baseline(client, auth),
                )
                .await
                {
                    Ok(Ok(baseline)) => Ok(baseline),
                    Ok(Err(error)) => Err(error.to_string()),
                    Err(_) => Err("strategy account baseline lookup timed out".into()),
                }
            } else {
                Ok(None)
            }
        };
        let (baseline, warning) = tokio::join!(
            baseline_read,
            self.initialize_live_metrics_continuity_inner(
                client,
                auth,
                initial_account,
                executed_at,
                symbols,
            )
        );
        match baseline {
            Ok(Some(origin)) => {
                if initial_account
                    .is_some_and(|account| account.currency.trim() == origin.currency.trim())
                {
                    self.live_metrics.apply_account_equity_baseline(
                        &origin.account_id,
                        origin.equity,
                        origin.captured_at,
                    );
                }
            }
            Err(error) => {
                // An unavailable read is not proof of a brand new strategy.
                // Retry on reconnect rather than overwrite its saved origin.
                self.live_metrics_cloud_write_enabled = false;
                warn!("Live account baseline unavailable; Cloud metric writes deferred: {error}");
            }
            Ok(None) => {}
        }
        warning
    }

    async fn initialize_live_metrics_continuity_inner<C: surrealdb::Connection>(
        &mut self,
        client: Option<&surrealdb::Surreal<C>>,
        auth: Option<&AqsAuth>,
        initial_account: Option<&Account>,
        executed_at: chrono::DateTime<chrono::Utc>,
        symbols: Vec<String>,
    ) -> Option<String> {
        self.legacy_metrics_recovery_needed = false;
        self.live_metrics_cloud_write_enabled = true;
        let strategy_key = self.live_metrics_strategy_key(auth);
        let local_checkpoint = self.load_local_live_metrics_checkpoint(&strategy_key);
        let account_id = initial_account
            .map(|account| account.account_id.trim().to_string())
            .filter(|value| !value.is_empty());
        let starting_cash = initial_account
            // Compatibility field name: live return starts at observed equity,
            // not cash (which can exclude existing open-position P&L).
            .map(|account| account.equity)
            .unwrap_or_default();
        let current_equity = initial_account
            .map(|account| account.equity)
            .unwrap_or(starting_cash);

        if auth.is_some() && client.is_none() {
            self.live_metrics_cloud_write_enabled = false;
        }

        let Some(account_id) = account_id else {
            self.live_metrics.initialize_for_account(
                None,
                starting_cash,
                current_equity,
                executed_at,
                symbols,
            );
            return Some(
                "Broker account identity was unavailable; cumulative live metrics started fresh"
                    .to_string(),
            );
        };

        let mut cloud_metrics = None;
        if let (Some(client), Some(auth)) = (client, auth) {
            match tokio::time::timeout(
                Duration::from_secs(LIVE_SYNC_QUERY_TIMEOUT_SECS),
                load_latest_live_metrics(client, auth),
            )
            .await
            {
                Ok(Ok(metrics)) => cloud_metrics = metrics,
                Ok(Err(error)) => {
                    warn!(
                        "AQS live metrics continuity could not be loaded for strategy {}: {}; cloud metric writes are deferred for this run",
                        auth.strategy_id, error
                    );
                    self.live_metrics_cloud_write_enabled = false;
                }
                Err(_) => {
                    warn!(
                        "AQS live metrics continuity timed out for strategy {} after {}s; cloud metric writes are deferred for this run",
                        auth.strategy_id, LIVE_SYNC_QUERY_TIMEOUT_SECS
                    );
                    self.live_metrics_cloud_write_enabled = false;
                }
            }
        }

        if !self.live_metrics_cloud_write_enabled {
            if let Some(checkpoint) =
                local_checkpoint.filter(|checkpoint| checkpoint.account_id.trim() == account_id)
            {
                if self
                    .live_metrics
                    .restore(checkpoint, current_equity, executed_at, symbols.clone())
                    .is_ok()
                {
                    info!(
                        "Restored local live metrics checkpoint for strategy {} while AQS Cloud continuity was unavailable",
                        strategy_key
                    );
                    return None;
                }
            }
            self.live_metrics.initialize_for_account(
                Some(account_id),
                starting_cash,
                current_equity,
                executed_at,
                symbols,
            );
            self.live_metrics.mark_history_recovery_incomplete();
            return None;
        }

        let cloud_checkpoint = cloud_metrics
            .as_ref()
            .and_then(|metrics| metrics.tracker_checkpoint.clone());
        let candidates = [local_checkpoint.clone(), cloud_checkpoint.clone()]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let complete_candidates = candidates
            .iter()
            .filter(|checkpoint| checkpoint.history_recovery_complete)
            .cloned()
            .collect::<Vec<_>>();
        let checkpoint_candidates = if complete_candidates.is_empty() {
            // An incomplete local checkpoint created while Cloud continuity was
            // unavailable must not outrank a legacy Cloud metrics snapshot.
            if cloud_metrics
                .as_ref()
                .is_some_and(|metrics| metrics.tracker_checkpoint.is_none())
            {
                Vec::new()
            } else {
                candidates
            }
        } else {
            complete_candidates
        };
        let selected_checkpoint = LivePerformanceTracker::newest_compatible_checkpoint(
            &account_id,
            checkpoint_candidates,
        );

        if let Some(checkpoint) = selected_checkpoint {
            let checkpoint_time = checkpoint.updated_at;
            let recovery_complete = checkpoint.history_recovery_complete;
            match self.live_metrics.restore(
                checkpoint,
                current_equity,
                executed_at,
                symbols.clone(),
            ) {
                Ok(()) => {
                    info!(
                        "Restored live metrics checkpoint for strategy {} account {} updated at {}",
                        strategy_key, account_id, checkpoint_time
                    );
                    if !recovery_complete {
                        if let (Some(client), Some(auth)) = (client, auth) {
                            self.start_legacy_live_trade_metrics(client, auth);
                        }
                    }
                    return None;
                }
                Err(error) => warn!(
                    "Ignoring invalid live metrics checkpoint for strategy {}: {}",
                    strategy_key, error
                ),
            }
        }

        let mut legacy_account_id = None;
        if let (Some(client), Some(auth), Some(metrics)) = (client, auth, cloud_metrics.as_ref()) {
            let latest_origin = match tokio::time::timeout(
                Duration::from_secs(LIVE_SYNC_QUERY_TIMEOUT_SECS),
                load_latest_metric_account_origin(client, auth),
            )
            .await
            {
                Ok(Ok(origin)) => origin,
                Ok(Err(error)) => {
                    warn!(
                        "Could not load broker identity for legacy live metrics on strategy {}: {}",
                        auth.strategy_id, error
                    );
                    None
                }
                Err(_) => None,
            };
            legacy_account_id = latest_origin
                .as_ref()
                .map(|origin| origin.account_id.trim().to_string());
            if latest_origin
                .as_ref()
                .is_some_and(|origin| origin.account_id.trim() == account_id)
            {
                if self
                    .live_metrics
                    .restore_legacy_snapshot(
                        metrics.snapshot(),
                        account_id.clone(),
                        current_equity,
                        executed_at,
                        symbols.clone(),
                    )
                    .is_ok()
                {
                    if let Ok(Ok(Some(origin))) = tokio::time::timeout(
                        Duration::from_secs(LIVE_SYNC_QUERY_TIMEOUT_SECS),
                        load_earliest_metric_account_origin(client, auth, &account_id),
                    )
                    .await
                    {
                        self.live_metrics
                            .set_continuity_origin(origin.equity, origin.captured_at);
                    }
                    self.start_legacy_live_trade_metrics(client, auth);
                    info!(
                        "Recovered legacy live metrics continuity for strategy {} account {}",
                        auth.strategy_id, account_id
                    );
                    return None;
                }
            }
        }

        let previous_account = cloud_checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.account_id.trim().to_string())
            .or_else(|| {
                local_checkpoint
                    .as_ref()
                    .map(|checkpoint| checkpoint.account_id.trim().to_string())
            })
            .or(legacy_account_id)
            .or_else(|| cloud_metrics.as_ref().map(|_| "unavailable".to_string()));
        self.live_metrics.initialize_for_account(
            Some(account_id.clone()),
            starting_cash,
            current_equity,
            executed_at,
            symbols,
        );
        previous_account.map(|previous| {
            format!(
                "Broker account changed from '{}' to '{}'; cumulative live metrics started fresh",
                previous, account_id
            )
        })
    }

    pub(super) fn resolve_deferred_live_metrics_continuity<C: surrealdb::Connection>(
        &mut self,
        client: &surrealdb::Surreal<C>,
        auth: &AqsAuth,
        expected_account_id: &str,
        cloud_metrics: Option<super::aqs_types::StrategyLiveMetricsRecord>,
        legacy: Option<super::metrics_continuity_lookup::LegacyContinuity>,
        baseline: Option<MetricAccountOrigin>,
    ) -> bool {
        let resolved = self.resolve_deferred_live_metrics_continuity_inner(
            client,
            auth,
            expected_account_id,
            cloud_metrics,
            legacy,
        );
        if resolved {
            if let Some(origin) = baseline {
                if self
                    .broker
                    .account_snapshot()
                    .ok()
                    .is_some_and(|account| account.currency.trim() == origin.currency.trim())
                {
                    self.live_metrics.apply_account_equity_baseline(
                        &origin.account_id,
                        origin.equity,
                        origin.captured_at,
                    );
                }
                self.persist_local_live_metrics_checkpoint(
                    &self.live_metrics_strategy_key(Some(auth)),
                );
            }
        }
        resolved
    }

    fn resolve_deferred_live_metrics_continuity_inner<C: surrealdb::Connection>(
        &mut self,
        client: &surrealdb::Surreal<C>,
        auth: &AqsAuth,
        expected_account_id: &str,
        cloud_metrics: Option<super::aqs_types::StrategyLiveMetricsRecord>,
        legacy: Option<super::metrics_continuity_lookup::LegacyContinuity>,
    ) -> bool {
        if self.live_metrics_cloud_write_enabled {
            return true;
        }
        let Some(account) = self.broker.account_snapshot().ok() else {
            warn!(
                "AQS live metrics continuity retry could not read the current broker account for strategy {}",
                auth.strategy_id
            );
            return false;
        };
        let current_account_id = account.account_id.trim().to_string();
        if current_account_id.is_empty() || current_account_id != expected_account_id.trim() {
            warn!(
                "Ignoring stale Cloud continuity lookup for strategy {}; broker account identity changed while reading",
                auth.strategy_id
            );
            return false;
        }

        let has_cloud_metrics = cloud_metrics.is_some();
        if let Some(metrics) = cloud_metrics {
            if let Some(checkpoint) = metrics.tracker_checkpoint {
                if checkpoint.account_id.trim() != current_account_id {
                    self.live_metrics.mark_history_recovery_complete();
                    self.live_metrics_cloud_write_enabled = true;
                    warn!(
                        "Broker account changed from '{}' to '{}'; cumulative live metrics started fresh",
                        checkpoint.account_id.trim(),
                        current_account_id
                    );
                    return true;
                }
                let recovery_complete = checkpoint.history_recovery_complete;
                match self.live_metrics.merge_historical_checkpoint(checkpoint) {
                    Ok(()) => {
                        if recovery_complete {
                            self.live_metrics.mark_history_recovery_complete();
                        } else {
                            // A partial legacy rebuild is not continuity proof.
                            // Keep retries enabled and never overwrite Cloud
                            // history with incomplete local accumulators.
                            self.start_legacy_live_trade_metrics(client, auth);
                            return false;
                        }
                        self.live_metrics_cloud_write_enabled = true;
                        let strategy_key = self.live_metrics_strategy_key(Some(auth));
                        self.persist_local_live_metrics_checkpoint(&strategy_key);
                        info!(
                            "AQS live metrics continuity resolved after reconnect for strategy {}",
                            auth.strategy_id
                        );
                        return true;
                    }
                    Err(error) => {
                        warn!(
                            "AQS live metrics continuity retry refused an overlapping checkpoint for strategy {}: {}",
                            auth.strategy_id, error
                        );
                        return false;
                    }
                }
            }
        }

        if !has_cloud_metrics {
            self.live_metrics.mark_history_recovery_complete();
            self.live_metrics_cloud_write_enabled = true;
            return true;
        }

        // Lookup and risk-baseline preparation already finished on the owned
        // read task. Never rerun startup initialization here: it would overwrite
        // the trades/equity samples accepted while Cloud was unavailable.
        use super::live_metrics::LegacyRiskMerge;
        use super::metrics_continuity_lookup::LegacyContinuity;
        match legacy {
            Some(LegacyContinuity::AccountChanged(previous)) => {
                self.live_metrics.mark_history_recovery_complete();
                self.live_metrics_cloud_write_enabled = true;
                warn!(
                    "Legacy metrics belong to previous broker account '{previous}'; current account metrics remain separate"
                );
                true
            }
            Some(LegacyContinuity::Risk(baseline)) => {
                match self.live_metrics.merge_legacy_risk_baseline(baseline) {
                    Ok(LegacyRiskMerge::CompleteCheckpointPreferred) => {
                        self.live_metrics_cloud_write_enabled = true;
                        true
                    }
                    Ok(LegacyRiskMerge::NeedsTradeHistory) => {
                        self.start_legacy_live_trade_metrics(client, auth);
                        false
                    }
                    Err(error) => {
                        warn!(
                            "Legacy risk baseline could not be merged for strategy {}; preserving live metrics and deferring Cloud writes: {error}",
                            auth.strategy_id
                        );
                        false
                    }
                }
            }
            None => {
                warn!(
                    "Legacy metrics lookup did not provide account/baseline evidence for strategy {}; Cloud writes remain deferred",
                    auth.strategy_id
                );
                false
            }
        }
    }

    pub(super) fn persist_local_live_metrics_if_needed(
        &mut self,
        artifacts: &mut LocalLiveRunArtifacts,
        auth: Option<&AqsAuth>,
        force: bool,
    ) {
        if let Ok(account) = self.broker.account_snapshot() {
            self.live_metrics
                .update_equity(account.equity, chrono::Utc::now());
        }
        self.refresh_live_open_position_metrics();
        let strategy_key = self.live_metrics_strategy_key(auth);
        self.persist_local_live_metrics_checkpoint(&strategy_key);
        if let Err(error) = artifacts.queue_metrics_if_changed(self.live_metrics.snapshot(), force)
        {
            warn!(
                "Failed to persist local live metrics into {}: {}",
                artifacts.db_path.display(),
                error
            );
        }
    }
}
