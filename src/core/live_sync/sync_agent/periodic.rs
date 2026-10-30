//! One owned task per periodic lane, never one task per tick or event.
use super::*;

#[derive(Clone, Copy)]
pub(super) enum Job {
    Telemetry,
    Audit,
    Reconciliation,
    Health,
    Maintenance,
    CloudHeartbeat,
}

impl Job {
    pub const ALL: [Self; 6] = [
        Self::Telemetry,
        Self::Audit,
        Self::Reconciliation,
        Self::Health,
        Self::Maintenance,
        Self::CloudHeartbeat,
    ];

    fn interval(self, config: &SyncAgentConfig) -> Duration {
        let duration = match self {
            Self::Telemetry | Self::Audit => config.telemetry_flush_interval,
            Self::Reconciliation => config.reconciliation_flush_interval,
            Self::Health => config.health_interval,
            Self::Maintenance => Duration::from_secs(60),
            Self::CloudHeartbeat => Duration::from_secs(30),
        };
        // Public configuration must not panic an owned task with a zero interval.
        duration.max(Duration::from_millis(1))
    }

    fn priority(self) -> Option<EventPriority> {
        match self {
            Self::Telemetry => Some(EventPriority::Telemetry),
            Self::Audit => Some(EventPriority::Audit),
            Self::Reconciliation => Some(EventPriority::Reconciliation),
            _ => None,
        }
    }
}

impl SyncAgent {
    pub(super) async fn run_periodic(
        &self,
        job: Job,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), SyncAgentError> {
        if let Some(priority) = job.priority() {
            return self.run_delivery_lane(job, priority, shutdown).await;
        }
        let mut interval = tokio::time::interval(job.interval(&self.config));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { return Ok(()); }
                }
                _ = interval.tick() => self.periodic_step(job).await?,
            }
        }
    }

    async fn run_delivery_lane(
        &self,
        job: Job,
        priority: EventPriority,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), SyncAgentError> {
        let notifier = self.outbox.priority_change_notifier(priority);
        let interval = job.interval(&self.config);
        let mut next_flush = tokio::time::Instant::now();
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            if self.wait_for_cloud_resume(&mut shutdown).await {
                return Ok(());
            }
            // Keep the configured micro-batch cadence during bursts. A stream
            // of notifications must not turn into one database/HTTP call per event.
            if next_flush > tokio::time::Instant::now() {
                tokio::select! { biased;
                    changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { return Ok(()); } },
                    _ = tokio::time::sleep_until(next_flush) => {},
                }
                continue;
            }
            // Arm before reading durable state so an insertion cannot fall
            // between the empty query and the wait. Every scoped waiter wakes.
            let notified = notifier.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let deadline = self
                .outbox
                .next_pending_deadline_for_runtime_scope(
                    priority,
                    &self.config.runtime_id,
                    &self.config.strategy_id,
                    &self.config.live_session_id,
                )
                .await?;
            if deadline.is_some_and(|at| at <= now_epoch_micros()) {
                // Audit/reconciliation still perform the exact critical-backlog
                // check, but only when their own lane actually has due work.
                // Pace from dispatch, not completion: a slow remote request
                // must not add another flush interval before draining more work.
                next_flush = tokio::time::Instant::now() + interval;
                self.periodic_step(job).await?;
                continue;
            }
            // An infrequent safety check also covers separate store handles or
            // administrative recovery paths which did not emit a local hint.
            let wait = deadline
                .map(
                    |at| Duration::from_micros(at.saturating_sub(now_epoch_micros()).max(1) as u64),
                )
                .unwrap_or(Duration::from_secs(1))
                .min(Duration::from_secs(1));
            tokio::select! { biased;
                changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { return Ok(()); } },
                _ = &mut notified => {},
                _ = tokio::time::sleep(wait) => {},
            }
        }
    }

    pub(super) async fn periodic_step(&self, job: Job) -> Result<(), SyncAgentError> {
        match job {
            Job::Telemetry => {
                self.flush_priority(
                    EventPriority::Telemetry,
                    self.config.telemetry_batch_events,
                    self.config.telemetry_batch_bytes,
                )
                .await?
            }
            Job::Audit | Job::Reconciliation => {
                if !self
                    .critical_backlog_exceeds(self.config.reconciliation_pause_critical_backlog)
                    .await?
                {
                    let (priority, bytes) = match job {
                        Job::Audit => (EventPriority::Audit, 128 * 1024),
                        _ => (EventPriority::Reconciliation, 256 * 1024),
                    };
                    self.flush_priority(priority, 100, bytes).await?;
                }
            }
            Job::Health => self.publish_health().await?,
            Job::CloudHeartbeat => {
                if self.cloud_pause_remaining().is_none() {
                    match self.sink.heartbeat().await {
                        Err(error) if error.kind == RemoteSyncErrorKind::CloudPaused => {
                            self.pause_cloud_delivery(error.message)
                        }
                        Ok(()) => {
                            if self.health_state.lock().sync_state == "cloud_paused" {
                                self.set_health_state("recovering", None);
                            }
                        }
                        _ => {}
                    }
                }
            }
            Job::Maintenance => {
                if !self
                    .outbox
                    .claim_maintenance(std::time::Instant::now(), Duration::from_secs(60))
                {
                    return Ok(());
                }
                let _ = self.outbox.refresh_latency_rollups().await;
                let pruned = self
                    .outbox
                    .prune_committed(
                        self.config
                            .retention_grace
                            .as_micros()
                            .min(i64::MAX as u128) as i64,
                    )
                    .await
                    .unwrap_or(0);
                let _ = self
                    .outbox
                    .enforce_storage_policy(self.config.outbox_soft_limit_bytes)
                    .await;
                let _ = self.outbox.checkpoint_wal(pruned > 0).await;
            }
        }
        Ok(())
    }
}
