use super::*;

#[cfg(test)]
mod tests;

impl LocalLiveRunArtifacts {
    /// Only not-yet-dispatched snapshots may be replaced. Once dispatched, a
    /// job keeps its key and immutable payload until commit is acknowledged.
    pub(in crate::core::strategy) fn queue_metrics_if_changed(
        &mut self,
        snapshot: Option<LiveMetricsSnapshot>,
        force: bool,
    ) -> Result<(), BrokerError> {
        let Some(snapshot) = snapshot else {
            return Ok(());
        };
        let latest = self
            .metrics_pending
            .as_ref()
            .or(self.metrics_inflight.as_ref())
            .map(|job| job.snapshot.as_ref())
            .or(self.last_metrics.as_deref());
        if force || latest != Some(&snapshot) {
            self.metrics_pending = Some(MetricsWrite {
                key: uuid::Uuid::new_v4().to_string(),
                snapshot: Arc::new(snapshot),
            });
        }
        self.poll_metrics_writes()
    }

    fn apply_metrics_commit(
        &mut self,
        result: Result<(Connection, Result<(), BrokerError>), tokio::task::JoinError>,
    ) -> Result<(), BrokerError> {
        self.metrics_retry_at = Instant::now() + Duration::from_secs(1);
        let (conn, result) = result.map_err(|error| {
            BrokerError::ConnectionError(format!("Local metrics writer failed: {error}"))
        })?;
        self.metrics_conn = Some(conn);
        result?;
        self.last_metrics = self.metrics_inflight.take().map(|job| job.snapshot);
        self.metrics_retry_at = Instant::now();
        Ok(())
    }

    pub(in crate::core::strategy) fn poll_metrics_writes(&mut self) -> Result<(), BrokerError> {
        if let Some(result) = self.metrics_writes.try_join_next() {
            self.apply_metrics_commit(result)?;
        }
        if !self.metrics_writes.is_empty() || Instant::now() < self.metrics_retry_at {
            return Ok(());
        }
        if self.metrics_inflight.is_none() {
            self.metrics_inflight = self.metrics_pending.take();
        }
        let Some(job) = self.metrics_inflight.clone() else {
            return Ok(());
        };
        // Move the connection, rather than cloning it: Turso attaches deferred
        // transaction rollback to this wrapper after cancellation or failure.
        let Some(mut conn) = self.metrics_conn.take() else {
            self.metrics_retry_at = Instant::now() + Duration::from_secs(1);
            return Err(BrokerError::ConnectionError(
                "Local metrics worker lost connection ownership; pending snapshot retained".into(),
            ));
        };
        let run_id = self.run_id.clone();
        let path = self.db_path.clone();
        let ready = self.event_ready.clone();
        self.metrics_writes.spawn(async move {
            let started = Instant::now();
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                Self::persist_metrics_job(&mut conn, &path, &run_id, &job),
            )
            .await
            .unwrap_or_else(|_| {
                Err(BrokerError::ConnectionError(
                    "Local metrics transaction timed out; retained for retry".into(),
                ))
            });
            log::debug!(
                "Local metrics transaction symbols={} elapsed_ms={} committed={}",
                job.snapshot.symbols.len(),
                started.elapsed().as_millis(),
                result.is_ok()
            );
            ready.notify_one();
            (conn, result)
        });
        Ok(())
    }

    pub(super) async fn drain_metrics_writes(&mut self) -> Result<(), BrokerError> {
        loop {
            self.poll_metrics_writes()?;
            if let Some(result) = self.metrics_writes.join_next().await {
                self.apply_metrics_commit(result)?;
            } else if self.metrics_pending.is_none() && self.metrics_inflight.is_none() {
                return Ok(());
            } else {
                tokio::time::sleep(
                    self.metrics_retry_at
                        .saturating_duration_since(Instant::now()),
                )
                .await;
            }
        }
    }

    async fn persist_metrics_job(
        conn: &mut Connection,
        db_path: &std::path::Path,
        run_id: &str,
        job: &MetricsWrite,
    ) -> Result<(), BrokerError> {
        let saved_snapshot = &job.snapshot;
        let tx = conn
            .transaction_with_behavior(turso::transaction::TransactionBehavior::Immediate)
            .await
            .map_err(|error| BrokerError::ConnectionError(error.to_string()))?;
        tx
            .execute(
                "INSERT INTO metrics_snapshots (
                    captured_at,
                    run_id,
                    starting_cash,
                    final_equity,
                    total_return,
                    total_return_pct,
                    total_trades,
                    winning_trades,
                    losing_trades,
                    win_rate,
                    max_drawdown,
                    cagr,
                    annualized_volatility,
                    sharpe_ratio,
                    sortino_ratio,
                    calmar_ratio,
                    max_drawdown_duration_days,
                    expectancy,
                    profit_factor,
                    payoff_ratio,
                    avg_winner,
                    avg_loser,
                    avg_winner_pct,
                    avg_loser_pct,
                    best_trade,
                    worst_trade,
                    consistency_score,
                    longest_winning_trade_held_secs,
                    longest_losing_trade_held_secs,
                    average_trade_held_secs,
                    open_positions_count,
                    open_insights_count,
                    open_positions_unrealized_pnl,
                    open_positions_profitable_count,
                    open_positions_losing_count,
                    executed_at,
                    finished_at,
                    updated_at,
                    snapshot_key
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33, ?34, ?35, ?36, ?37, ?38, ?39) ON CONFLICT(snapshot_key) DO NOTHING",
                params![
                    saved_snapshot.updated_at.to_rfc3339(),
                    run_id.to_string(),
                    saved_snapshot.starting_cash,
                    saved_snapshot.final_equity,
                    saved_snapshot.total_return,
                    saved_snapshot.total_return_pct,
                    saved_snapshot.total_trades as i64,
                    saved_snapshot.winning_trades as i64,
                    saved_snapshot.losing_trades as i64,
                    saved_snapshot.win_rate,
                    saved_snapshot.max_drawdown,
                    saved_snapshot.cagr,
                    saved_snapshot.annualized_volatility,
                    saved_snapshot.sharpe_ratio,
                    saved_snapshot.sortino_ratio,
                    saved_snapshot.calmar_ratio,
                    saved_snapshot.max_drawdown_duration_days,
                    saved_snapshot.expectancy,
                    saved_snapshot.profit_factor,
                    saved_snapshot.payoff_ratio,
                    saved_snapshot.avg_winner,
                    saved_snapshot.avg_loser,
                    saved_snapshot.avg_winner_pct,
                    saved_snapshot.avg_loser_pct,
                    saved_snapshot.best_trade,
                    saved_snapshot.worst_trade,
                    saved_snapshot.consistency_score,
                    saved_snapshot.longest_winning_trade_held_secs,
                    saved_snapshot.longest_losing_trade_held_secs,
                    saved_snapshot.average_trade_held_secs,
                    saved_snapshot.open_positions_count as i64,
                    saved_snapshot.open_insights_count as i64,
                    saved_snapshot.open_positions_unrealized_pnl,
                    saved_snapshot.open_positions_profitable_count as i64,
                    saved_snapshot.open_positions_losing_count as i64,
                    saved_snapshot.executed_at.to_rfc3339(),
                    saved_snapshot.finished_at.map(|value| value.to_rfc3339()),
                    saved_snapshot.updated_at.to_rfc3339(),
                    job.key.clone()
                ],
            )
            .await
            .map_err(|error| {
                BrokerError::ConnectionError(format!(
                    "Failed to write local live metrics into {}: {}",
                    db_path.display(),
                    error
                ))
            })?;
        let mut rows = tx
            .query(
                "SELECT id FROM metrics_snapshots WHERE snapshot_key=?1",
                params![job.key.clone()],
            )
            .await
            .map_err(|error| BrokerError::ConnectionError(error.to_string()))?;
        let row = rows
            .next()
            .await
            .map_err(|error| BrokerError::ConnectionError(error.to_string()))?
            .ok_or_else(|| {
                BrokerError::ConnectionError("Metrics snapshot insert returned no identity".into())
            })?;
        let snapshot_id = row
            .get::<i64>(0)
            .map_err(|error| BrokerError::ConnectionError(error.to_string()))?;
        drop(rows);
        for symbols in saved_snapshot.symbols.chunks(256) {
            let placeholders = std::iter::repeat_n("(?,?)", symbols.len())
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!(
                "INSERT OR IGNORE INTO metrics_snapshot_symbols (snapshot_id,symbol) VALUES {placeholders}"
            );
            let values = symbols
                .iter()
                .flat_map(|symbol| [Value::Integer(snapshot_id), Value::Text(symbol.clone())])
                .collect::<Vec<_>>();
            tx.execute(&sql, values)
                .await
                .map_err(|error| BrokerError::ConnectionError(error.to_string()))?;
        }
        tx.commit()
            .await
            .map_err(|error| BrokerError::ConnectionError(error.to_string()))?;
        Ok(())
    }
}
