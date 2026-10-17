//! Page-bounded legacy trade reconstruction, independent of the live owner.

use super::live_metrics::{TradeHistoryRebuild, TradeRecoveryToken};
use super::types::live_metrics::HistoricalClosedInsightMetric;
use std::future::Future;
use std::time::Duration;
use tokio::time::Instant;

const PAGE_SIZE: usize = 250;
const PAGE_TIMEOUT: Duration = Duration::from_secs(8);
const TOTAL_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Default)]
pub(super) struct LegacyMetricsRecovery {
    jobs: tokio::task::JoinSet<Result<TradeHistoryRebuild, String>>,
    token: Option<TradeRecoveryToken>,
}

pub(super) struct LegacyMetricsRecoveryResult {
    pub token: TradeRecoveryToken,
    pub result: Result<TradeHistoryRebuild, String>,
}

impl LegacyMetricsRecovery {
    pub fn is_running(&self) -> bool {
        self.token.is_some()
    }

    pub fn start(
        &mut self,
        token: TradeRecoveryToken,
        work: impl Future<Output = Result<TradeHistoryRebuild, String>> + Send + 'static,
    ) -> Result<(), String> {
        if self.is_running() {
            return Err("Legacy metrics recovery is already running".into());
        }
        self.token = Some(token);
        self.jobs.spawn(work);
        Ok(())
    }

    pub async fn next(&mut self) -> Option<LegacyMetricsRecoveryResult> {
        let result = self.jobs.join_next().await?;
        let token = self
            .token
            .take()
            .expect("owned recovery task has an attempt token");
        Some(LegacyMetricsRecoveryResult {
            token,
            result: result.unwrap_or_else(|error| {
                Err(format!("Legacy metrics recovery task exited: {error}"))
            }),
        })
    }

    pub async fn shutdown(&mut self) -> Option<TradeRecoveryToken> {
        self.jobs.shutdown().await;
        self.token.take()
    }
}

pub(super) async fn load_trade_history_pages<F, Fut>(
    strategy_id: &str,
    fetch: F,
) -> Result<TradeHistoryRebuild, String>
where
    F: FnMut(usize, usize) -> Fut,
    Fut: Future<Output = Result<Vec<HistoricalClosedInsightMetric>, String>>,
{
    load_with_limits(strategy_id, PAGE_SIZE, PAGE_TIMEOUT, TOTAL_TIMEOUT, fetch).await
}

async fn load_with_limits<F, Fut>(
    strategy_id: &str,
    page_size: usize,
    page_timeout: Duration,
    total_timeout: Duration,
    mut fetch: F,
) -> Result<TradeHistoryRebuild, String>
where
    F: FnMut(usize, usize) -> Fut,
    Fut: Future<Output = Result<Vec<HistoricalClosedInsightMetric>, String>>,
{
    let started = Instant::now();
    let mut rebuilt = TradeHistoryRebuild::default();
    let mut offset = 0usize;
    loop {
        let remaining = total_timeout.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return Err(format!(
                "Legacy metrics recovery exceeded its total deadline at offset {offset}"
            ));
        }
        let page = tokio::time::timeout(page_timeout.min(remaining), fetch(offset, page_size))
            .await
            .map_err(|_| format!("Legacy metrics recovery timed out at offset {offset}"))?
            .map_err(|error| {
                format!("Legacy metrics recovery failed at offset {offset}: {error}")
            })?;
        let rows = page.len();
        if rows > page_size {
            return Err(format!(
                "Legacy metrics recovery exceeded its {page_size}-row page bound at offset {offset}"
            ));
        }
        rebuilt.push_page(page);
        log::info!(
            "Live metrics recovery strategy={} offset={} rows={} unique_trades={} elapsed_ms={}",
            strategy_id,
            offset,
            rows,
            rebuilt.len(),
            started.elapsed().as_millis()
        );
        if started.elapsed() >= total_timeout {
            return Err(format!(
                "Legacy metrics recovery exceeded its total deadline at offset {offset}"
            ));
        }
        if rows < page_size {
            return Ok(rebuilt);
        }
        offset = offset
            .checked_add(rows)
            .ok_or_else(|| "Legacy metrics recovery offset overflow".to_string())?;
        // A cached/ready database page must not let an arbitrarily long history
        // monopolize the executor. No full row survives beyond its page.
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
mod tests;
