use super::StrategyState;
use super::aqs_types::{self, AqsAuth};
use super::live_event_projection::{
    LocalBatchSyncState, chunk_reconciliation_records, classify_insight_history,
};
use super::traits::{Strategy, StrategyContext};
use crate::core::broker::traits::{Broker, DataFeed, DataProvider, OrderManagementProvider};
use crate::core::broker::types::{Account, BrokerError};
use crate::core::insight::InsightSnapshot;
use crate::core::insight::types::InsightState;
use crate::core::live_sync::{
    EventKind, EventPriority, InsightHistoryKind, ReconciliationChunk, ReconciliationComplete,
    ReconciliationRecord, ReconciliationStart, deterministic_event_id,
};
use log::info;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::time::{Duration, Instant};

// Leave lifecycle/control handling responsive during history bursts or slow
// outbox persistence. Unsent history stays owned by the insight and its cursor
// advances only for the prefix handed to the publisher.
const MAX_PROJECTION_EVENTS_PER_FLUSH: usize = 256;
const PROJECTION_QUEUE_HIGH_WATER: u64 = 4096;
const PROJECTION_QUEUE_BYTES_HIGH_WATER: u64 = 16 * 1024 * 1024;
const ACCOUNT_PROJECTION_HEARTBEAT: Duration = Duration::from_secs(1);
const METRICS_PROJECTION_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StartupCloudBoundary {
    LocallyAccepted,
    DeferredCaptureGap,
}

fn projection_backpressured(sync: &LocalBatchSyncState) -> bool {
    sync.publisher.pending_critical_count() >= PROJECTION_QUEUE_HIGH_WATER
        || sync.publisher.queued_critical_encoded_bytes() >= PROJECTION_QUEUE_BYTES_HIGH_WATER
}

struct InsightProjectionPlan {
    insight_id: uuid::Uuid,
    snapshot: InsightSnapshot,
    is_terminal: bool,
    history_offset: usize,
    history_len: usize,
    history_published: usize,
}

struct ProjectionRollback {
    insight_id: String,
    fingerprint: Option<[u8; 32]>,
    entity_revision: Option<u64>,
}

impl<S, E, D> StrategyState<S, E, D>
where
    S: Strategy,
    E: Broker + OrderManagementProvider,
    D: DataFeed + DataProvider,
{
    pub(super) fn latest_price_for_symbol(&self, symbol: &str) -> Option<f64> {
        self.latest_quote(symbol)
            .ok()
            .and_then(|quote| quote.last.or(Some((quote.bid + quote.ask) / 2.0)))
            .filter(|price| price.is_finite())
    }

    pub(super) fn insights_state_counts_json(&self) -> serde_json::Value {
        let mut counts = serde_json::Map::new();
        for (state, count) in self.insights.get_state_count() {
            counts.insert(
                format!("{:?}", state),
                serde_json::Value::from(count as u64),
            );
        }
        serde_json::Value::Object(counts)
    }

    pub(super) fn refresh_live_open_position_metrics(&mut self) {
        let mut open_positions_count = 0usize;
        let mut open_insights_count = 0usize;
        let mut open_positions_unrealized_pnl = 0.0f64;
        let mut open_positions_profitable_count = 0usize;
        let mut open_positions_losing_count = 0usize;
        let mut symbols = self
            .universe
            .values()
            .map(|asset| asset.symbol.clone())
            .collect::<Vec<_>>();
        let mut latest_prices = HashMap::<String, f64>::new();

        for insight_id in self.insights.active_insight_ids_unsorted() {
            let Some(insight) = self.insights.get(&insight_id) else {
                continue;
            };
            if insight.state.is_inactive() {
                continue;
            }
            open_insights_count += 1;

            if insight.state != InsightState::Filled {
                continue;
            }

            let remaining_qty = insight.remaining_quantity();
            if remaining_qty <= f64::EPSILON {
                continue;
            }

            let Some(entry_price) = insight
                .filled_price
                .or(insight.limit_price)
                .or(insight.stop_price)
            else {
                continue;
            };
            let current_price = if let Some(price) = latest_prices.get(&insight.symbol).copied() {
                price
            } else if let Some(price) = self.latest_price_for_symbol(&insight.symbol) {
                latest_prices.insert(insight.symbol.clone(), price);
                price
            } else {
                continue;
            };

            let gross_pnl = match insight.side {
                crate::core::broker::types::OrderSide::Buy => {
                    (current_price - entry_price) * remaining_qty
                }
                crate::core::broker::types::OrderSide::Sell => {
                    (entry_price - current_price) * remaining_qty
                }
            };
            let pnl = gross_pnl + insight.swap.unwrap_or(0.0) - insight.commission.unwrap_or(0.0);

            open_positions_count += 1;
            open_positions_unrealized_pnl += pnl;
            if pnl > 0.0 {
                open_positions_profitable_count += 1;
            } else if pnl < 0.0 {
                open_positions_losing_count += 1;
            }
            if !symbols.iter().any(|value| value == &insight.symbol) {
                symbols.push(insight.symbol.clone());
            }
        }

        symbols.sort();
        symbols.dedup();
        self.live_metrics.update_open_position_metrics(
            open_positions_count,
            open_insights_count,
            open_positions_unrealized_pnl,
            open_positions_profitable_count,
            open_positions_losing_count,
            symbols,
            chrono::Utc::now(),
        );
    }

    pub(super) async fn publish_dirty_insights_to_local_bus(
        &mut self,
        sync: &mut LocalBatchSyncState,
        auth: &AqsAuth,
        max_insights: Option<usize>,
        consume_dirty: bool,
    ) -> bool {
        if sync.publisher.cloud_capture_paused() { return false; }
        let insight_ids = self
            .insights
            .insight_ids_for_live_sync_limited(false, max_insights);
        let mut plans = Vec::new();
        let mut projection_events = Vec::new();
        let mut history_events = Vec::new();
        let mut rollbacks = Vec::new();
        // Shutdown uses repeated bounded turns too, never an unlimited enqueue.
        // Planning does not enqueue yet. Account for the whole planned batch,
        // not just the publisher's unchanged count inside the history loop.
        // Otherwise a partially drained queue at e.g. 4,090 could receive 256
        // more events and overshoot the projection high-water mark.
        let mut event_budget = MAX_PROJECTION_EVENTS_PER_FLUSH.min(
            PROJECTION_QUEUE_HIGH_WATER.saturating_sub(sync.publisher.pending_critical_count()) as usize);

        for insight_id in insight_ids {
            if event_budget == 0 || projection_backpressured(sync) {
                break;
            }
            let published_history = sync
                .history_offsets
                .get(&insight_id.to_string())
                .copied()
                .unwrap_or(0);
            let Some((mut snapshot, is_terminal, history_offset, history_len)) =
                self.insights.get(&insight_id).map(|insight| {
                    let history_len = insight.state_history.len();
                    let history_offset = if sync.split_insight_history {
                        published_history.min(history_len)
                    } else {
                        0
                    };
                    (
                        InsightSnapshot::from_insight_with_history_window(
                            insight,
                            history_offset,
                            if sync.split_insight_history {
                                event_budget
                            } else {
                                usize::MAX
                            },
                        ),
                        insight.state.is_inactive(),
                        history_offset,
                        history_len,
                    )
                })
            else {
                continue;
            };
            // History has its own append-only stream. Do not build a large JSON
            // tree for it just to remove that tree from every insight projection.
            let separate_history = if sync.split_insight_history {
                std::mem::take(&mut snapshot.state_history)
            } else {
                Vec::new()
            };
            let projection_revision = snapshot.updated_at.timestamp_micros().max(1) as u64;
            let mut projection = serde_json::to_value(&snapshot).unwrap_or_default();
            if let Some(object) = projection.as_object_mut() {
                // State history is append-only when split-history projection is enabled.
                if sync.split_insight_history {
                    object.remove("state_history");
                }
                object.insert(
                    "strategy_id".to_string(),
                    serde_json::Value::String(auth.strategy_id.clone()),
                );
                object.insert(
                    "live_session_id".to_string(),
                    serde_json::Value::String(sync.live_session_id.clone()),
                );
                object.insert(
                    "origin_run_id".to_string(),
                    serde_json::Value::String(sync.run_id.clone()),
                );
                // Critical sequences are local to a strategy process. Persist the
                // sequence's clock domain so startup reconciliation never compares
                // a previous run's sequence with the current run's cutoff.
                object.insert(
                    "last_event_run_id".to_string(),
                    serde_json::Value::String(sync.run_id.clone()),
                );
                object.insert(
                    "reconciliation_status".to_string(),
                    serde_json::Value::String("current".to_string()),
                );
            }
            let projection_entity = format!("insight:{}", snapshot.insight_id);
            let rollback = ProjectionRollback {
                insight_id: snapshot.insight_id.clone(),
                fingerprint: sync
                    .insight_projection_fingerprints
                    .get(&snapshot.insight_id)
                    .copied(),
                entity_revision: sync.entity_revisions.get(&projection_entity).copied(),
            };
            if let Some(projection_event) =
                sync.insight_projection_event(&snapshot.insight_id, projection_revision, projection)
            {
                projection_events.push(projection_event);
                rollbacks.push(rollback);
                event_budget -= 1;
            }

            let mut history_published = 0;
            if sync.split_insight_history {
                for (index, history) in separate_history.iter().enumerate() {
                    if event_budget == 0 || projection_backpressured(sync) {
                        break;
                    }
                    let revision = (history_offset + index) as u64 + 1;
                    let (fallback_kind, fallback_from, fallback_to) = classify_insight_history(
                        history.message.as_deref(),
                        history.state.as_str(),
                    );
                    let kind = history.kind.clone().unwrap_or_else(|| {
                        match fallback_kind {
                            InsightHistoryKind::StateTransition => "state_transition",
                            InsightHistoryKind::Configuration => "configuration",
                            InsightHistoryKind::Execution => "execution",
                            InsightHistoryKind::Diagnostic => "diagnostic",
                        }
                        .to_string()
                    });
                    let from_state = history.from_state.clone().or(fallback_from);
                    let to_state = history.to_state.clone().or(fallback_to);
                    let event = sync.event_at_revision(
                        format!("insight-history:{}", snapshot.insight_id),
                        revision,
                        EventPriority::Audit,
                        EventKind::InsightStateHistory,
                        serde_json::json!({
                            "insight_id": snapshot.insight_id,
                            "insight": format!("insights:{}", snapshot.insight_id),
                            "parent_id": snapshot.parent_id,
                            "is_split_insight": snapshot.is_split_insight,
                            "symbol": snapshot.symbol,
                            "strategy_type": snapshot.strategy_type,
                            "state": history.state,
                            "message": history.message,
                            "timestamp": history.at,
                            "revision": revision,
                            "kind": kind,
                            "from_state": from_state,
                            "to_state": to_state,
                            "origin_run_id": sync.run_id,
                        }),
                        None,
                    );
                    history_events.push(event);
                    event_budget -= 1;
                    history_published += 1;
                }
            }

            plans.push(InsightProjectionPlan {
                insight_id,
                snapshot,
                is_terminal,
                history_offset,
                history_len,
                history_published,
            });
        }

        if (!projection_events.is_empty() || !history_events.is_empty())
            && !sync
                .publish_projection_batch_durable(projection_events, history_events)
                .await
        {
            // A partial local commit is retried under the exact same deterministic IDs.
            // Restore source cursors/fingerprints so no newer mutation can be mistaken for
            // the failed boundary.
            for rollback in rollbacks {
                match rollback.fingerprint {
                    Some(value) => {
                        sync.insight_projection_fingerprints
                            .insert(rollback.insight_id.clone(), value);
                    }
                    None => {
                        sync.insight_projection_fingerprints
                            .remove(&rollback.insight_id);
                    }
                }
                let entity_id = format!("insight:{}", rollback.insight_id);
                match rollback.entity_revision {
                    Some(value) => {
                        sync.entity_revisions.insert(entity_id, value);
                    }
                    None => {
                        sync.entity_revisions.remove(&entity_id);
                    }
                }
            }
            return false;
        }

        let mut metrics_changed = false;
        let mut prune_after_publish = Vec::new();
        for plan in plans {
            if sync.split_insight_history {
                sync.history_offsets.insert(
                    plan.snapshot.insight_id.clone(),
                    plan.history_offset + plan.history_published,
                );
                if plan.history_offset + plan.history_published < plan.history_len {
                    // Do not clear dirty state or prune a partially exported row.
                    continue;
                }
            }

            let previous_state = sync.insight_states.insert(
                plan.snapshot.insight_id.clone(),
                plan.snapshot.state.clone(),
            );
            if previous_state.as_deref() != Some(plan.snapshot.state.as_str()) {
                metrics_changed = true;
                if plan.snapshot.state == "Closed" {
                    self.live_metrics.record_closed_insight(&plan.snapshot);
                }
            }
            if consume_dirty {
                self.insights.remove_dirty(&plan.insight_id);
            }
            if consume_dirty && plan.is_terminal {
                prune_after_publish.push(plan.insight_id);
            }
        }

        for insight_id in prune_after_publish {
            self.insights.prune_terminal_insight(&insight_id);
            // Execution persistence may still pin this terminal row. Its next
            // sync pass must retain the publication cursor/fingerprint; clearing
            // these while the row survives re-enqueues its entire immutable
            // history and creates another critical projection on every flush.
            if self.insights.get(&insight_id).is_some() {
                continue;
            }
            let insight_id = insight_id.to_string();
            sync.history_offsets.remove(&insight_id);
            sync.insight_states.remove(&insight_id);
            sync.insight_projection_fingerprints.remove(&insight_id);
            sync.entity_revisions
                .remove(&format!("insight:{insight_id}"));
            sync.entity_revisions
                .remove(&format!("insight-history:{insight_id}"));
        }
        metrics_changed
    }

    /// The broker is settled before this drain. Keep yielding to the durable
    /// publisher; completion requires both an exported history prefix and local
    /// acceptance/persistence of every queued event. A timeout is not success.
    pub(super) async fn drain_final_insights_to_local_bus(
        &mut self,
        sync: &mut LocalBatchSyncState,
        auth: &AqsAuth,
        timeout: Duration,
    ) -> Result<(), crate::core::broker::types::BrokerError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            self.publish_dirty_insights_to_local_bus(sync, auth, Some(128), true)
                .await;
            if !self.insights.has_dirty_insights() && sync.publisher.pending_critical_count() == 0 {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline || sync.publisher.health().shutting_down {
                return Err(crate::core::broker::types::BrokerError::ConnectionError(
                    "Final insight synchronization incomplete; retaining execution recovery data"
                        .into(),
                ));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Local execution must not roll back because a known, durably recorded
    /// cloud capture pause refused this optional synchronization boundary.
    /// Unknown scope, real admission failure and missing gap proof still fail.
    pub(super) async fn establish_startup_cloud_boundary(
        &self,
        sync: &mut LocalBatchSyncState,
        auth: &AqsAuth,
    ) -> Result<StartupCloudBoundary, BrokerError> {
        if !sync.publisher.admission_failed()
            && self
                .publish_startup_reconciliation_to_local_bus(sync, auth)
                .await
        {
            return Ok(StartupCloudBoundary::LocallyAccepted);
        }
        let recorded = sync
            .publisher
            .has_recorded_capture_gap(
                &sync.runtime_id,
                &sync.strategy_id,
                &sync.live_session_id,
                &sync.run_id,
            )
            .await
            .map_err(|error| BrokerError::ConnectionError(error.to_string()))?;
        if recorded {
            log::warn!("Cloud startup synchronization deferred by a durable capture gap; local broker execution continues and missing cloud history is not acknowledged");
            Ok(StartupCloudBoundary::DeferredCaptureGap)
        } else {
            Err(BrokerError::ConnectionError(
                "Startup reconciliation could not be committed to the local event journal".into(),
            ))
        }
    }

    /// Publishes the one broker-evidence reconciliation generation for this process run.
    ///
    /// Persisted insights are selected remotely by their run ownership, so copying AQE's local
    /// insight graph here only inflates the snapshot and can accidentally make stale rows look
    /// current. This path is intentionally cache-only and never performs broker or database I/O.
    pub(super) async fn publish_startup_reconciliation_to_local_bus(
        &self,
        sync: &mut LocalBatchSyncState,
        auth: &AqsAuth,
    ) -> bool {
        if sync.publisher.cloud_capture_paused() {
            // Do not collect/serialize a broker snapshot that cannot be admitted.
            return false;
        }
        let generation = deterministic_event_id(&format!(
            "aqs-reconciliation-v4:{}:{}:{}",
            auth.strategy_id, sync.live_session_id, sync.run_id
        ))
        .to_string();
        let broker_snapshot = self.broker.get_cached_lifecycle_snapshot();
        let snapshot_complete = broker_snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.snapshot_complete);
        let broker_features = Broker::get_features(&self.broker)
            .iter()
            .filter_map(|feature| {
                serde_json::to_value(feature)
                    .ok()
                    .and_then(|value| value.as_str().map(ToString::to_string))
            })
            .collect::<Vec<_>>();
        let mut records = Vec::<ReconciliationRecord>::new();
        if let Some(snapshot) = broker_snapshot {
            for order in snapshot.orders {
                records.push(ReconciliationRecord {
                    record_type: "broker_order".to_string(),
                    entity_id: order.order_id.clone(),
                    payload: serde_json::to_value(order).unwrap_or_default(),
                });
            }
            for (index, position) in snapshot.positions.into_iter().enumerate() {
                records.push(ReconciliationRecord {
                    record_type: "broker_position".to_string(),
                    entity_id: format!("{}:{index}", position.asset.symbol),
                    payload: serde_json::to_value(position).unwrap_or_default(),
                });
            }
            for terminal in snapshot.terminal_history {
                records.push(ReconciliationRecord {
                    record_type: "terminal_order".to_string(),
                    entity_id: terminal.order_id.clone(),
                    payload: serde_json::to_value(terminal).unwrap_or_default(),
                });
            }
        }

        let total_records = records.len() as u64;
        let chunks = chunk_reconciliation_records(records);
        let total_chunks = chunks.len() as u32;
        let start = ReconciliationStart {
            purpose: aqe_live_protocol::ReconciliationPurpose::Startup,
            critical_replay_through: None,
            generation: generation.clone(),
            runtime_id: sync.runtime_id.clone(),
            strategy_id: auth.strategy_id.clone(),
            live_session_id: sync.live_session_id.clone(),
            run_id: Some(sync.run_id.clone()),
            snapshot_complete,
            broker_features,
            critical_cutoff: Some(sync.publisher.critical_sequence_cursor()),
            total_chunks,
            total_records,
        };
        let mut events = Vec::with_capacity(chunks.len() + 2);
        let mut event = sync.event(
            format!("reconciliation:{generation}:start"),
            EventPriority::Reconciliation,
            EventKind::ReconciliationStart,
            serde_json::to_value(start).unwrap_or_default(),
            None,
        );
        event.event_id = deterministic_event_id(&format!("reconciliation:{generation}:start"));
        events.push(event);
        for (chunk_index, records) in chunks.into_iter().enumerate() {
            let chunk = ReconciliationChunk {
                generation: generation.clone(),
                chunk_index: chunk_index as u32,
                records,
            };
            let mut event = sync.event(
                format!("reconciliation:{generation}:chunk:{chunk_index}"),
                EventPriority::Reconciliation,
                EventKind::ReconciliationChunk,
                serde_json::to_value(chunk).unwrap_or_default(),
                None,
            );
            event.event_id =
                deterministic_event_id(&format!("reconciliation:{generation}:chunk:{chunk_index}"));
            events.push(event);
        }
        let complete = ReconciliationComplete {
            generation: generation.clone(),
            total_chunks,
            total_records,
            snapshot_complete,
        };
        let mut event = sync.event(
            format!("reconciliation:{generation}:complete"),
            EventPriority::Reconciliation,
            EventKind::ReconciliationComplete,
            serde_json::to_value(complete).unwrap_or_default(),
            None,
        );
        event.event_id = deterministic_event_id(&format!("reconciliation:{generation}:complete"));
        events.push(event);
        if !sync.publish_reconciliation_batch_durable(events).await {
            return false;
        }
        info!(
            "Durably admitted targeted startup reconciliation generation {} with {} broker evidence record(s)",
            generation, total_records
        );
        true
    }

    pub(super) fn publish_account_and_metrics_to_local_bus(
        &mut self,
        sync: &mut LocalBatchSyncState,
        account: Option<&Account>,
        force_account_heartbeat: bool,
    ) {
        let captured_at = chrono::Utc::now();
        if let Some(cache) = self.broker.live_snapshot_cache() {
            if let Ok(flows) = cache.account_cash_flows() {
                sync.publish_account_cash_flows(&flows);
            }
        }
        // A heartbeat is not evidence that an old cached broker value is fresh.
        // Both MT5 and paper use the broker-neutral snapshot readiness check.
        let fresh_account = self.broker.is_broker_connected().then(|| self.broker.account_snapshot().ok()).flatten();
        let account = account.and(fresh_account.as_deref());
        if let Some(account) = account {
            self.live_metrics
                .refresh_equity(account.equity, captured_at);
            let revision = captured_at.timestamp_micros().max(1) as u64;
            let fingerprint: [u8; 32] = Sha256::digest(
                serde_json::to_vec(account).expect("account projection is serializable"),
            )
            .into();
            let account_changed = sync.last_account_projection_fingerprint != Some(fingerprint);
            let heartbeat_due = force_account_heartbeat
                && sync
                    .last_account_projection_at
                    .is_none_or(|instant| instant.elapsed() >= ACCOUNT_PROJECTION_HEARTBEAT);
            if account_changed || heartbeat_due {
                let account_event = sync.event_at_revision(
                    format!("account:{}", account.account_id.trim()),
                    revision,
                    EventPriority::Telemetry,
                    EventKind::AccountProjection,
                    serde_json::json!({
                        "account": account,
                        "account_confirmed": true,
                        "captured_at": captured_at,
                        "complete": force_account_heartbeat,
                        "revision": revision,
                    }),
                    None,
                );
                sync.publish_telemetry(account_event);
                sync.last_account_projection_fingerprint = Some(fingerprint);
                sync.last_account_projection_at = Some(Instant::now());
            }

            let equity_due_by_time = sync
                .last_equity_history_at
                .is_none_or(|instant| instant.elapsed() >= Duration::from_secs(1));
            let equity_due_by_change = sync.last_equity_history_value.is_none_or(|previous| {
                let base = previous.abs().max(1.0);
                ((account.equity - previous).abs() / base) >= 0.0005
            });
            if equity_due_by_time || equity_due_by_change {
                let equity_event = sync.event_at_revision(
                    format!("equity:{}", account.account_id.trim()),
                    revision,
                    EventPriority::Telemetry,
                    EventKind::EquityProjection,
                    serde_json::json!({
                        "account_id": account.account_id.trim(),
                        "account_confirmed": true,
                        "currency": account.currency,
                        "equity": account.equity,
                        "cash": account.cash,
                        "buying_power": account.buying_power,
                        "captured_at": captured_at,
                        "revision": revision,
                    }),
                    None,
                );
                sync.publish_telemetry(equity_event);
                sync.last_equity_history_at = Some(Instant::now());
                sync.last_equity_history_value = Some(account.equity);
            }
        }

        self.refresh_live_open_position_metrics();
        let metrics_due = self.live_metrics.should_persist();
        if metrics_due {
            self.persist_local_live_metrics_checkpoint(&sync.strategy_id);
        }
        let metrics_projection_due = force_account_heartbeat
            || sync
                .last_metrics_projection_at
                .is_none_or(|instant| instant.elapsed() >= METRICS_PROJECTION_INTERVAL);
        if self.live_metrics_cloud_write_enabled && metrics_due && metrics_projection_due {
            if let Some(snapshot) = self.live_metrics.snapshot() {
                let revision = snapshot.updated_at.timestamp_micros().max(1) as u64;
                let record = aqs_types::StrategyLiveMetricsRecord::from_snapshot(
                    snapshot,
                    self.live_metrics.checkpoint(),
                );
                let event = sync.event_at_revision(
                    "live-metrics",
                    revision,
                    EventPriority::Telemetry,
                    EventKind::LiveMetricsProjection,
                    serde_json::to_value(record).unwrap_or_default(),
                    None,
                );
                sync.publish_telemetry(event);
                sync.last_metrics_projection_at = Some(Instant::now());
                self.live_metrics.mark_persisted();
            }
        }
    }
}
