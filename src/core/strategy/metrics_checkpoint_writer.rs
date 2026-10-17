//! Replaceable metrics checkpoints, not immutable events or order intents.
use super::local_live_artifacts::LocalLiveMetricsCheckpoint;
use parking_lot::{Condvar, Mutex};
use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

pub(super) type MetricsCheckpointWriter = CoalescedWriter<LocalLiveMetricsCheckpoint>;

struct State<T> {
    pending: Option<T>,
    closing: bool,
}

pub(super) struct CoalescedWriter<T> {
    shared: Arc<(Mutex<State<T>>, Condvar)>,
    thread: Option<JoinHandle<Result<(), String>>>,
}

impl<T: Send + 'static> CoalescedWriter<T> {
    pub(super) fn start(
        mut write: impl FnMut(&T) -> Result<(), String> + Send + 'static,
    ) -> Result<Self, String> {
        let shared = Arc::new((
            Mutex::new(State {
                pending: None,
                closing: false,
            }),
            Condvar::new(),
        ));
        let worker = shared.clone();
        let thread = std::thread::Builder::new()
            .name("aqe-metrics-checkpoint".into())
            .spawn(move || {
                let (state, wake) = &*worker;
                loop {
                    let value = {
                        let mut state = state.lock();
                        while state.pending.is_none() && !state.closing {
                            wake.wait(&mut state);
                        }
                        match state.pending.take() {
                            Some(value) => value,
                            None => return Ok(()),
                        }
                    };
                    if let Err(error) = write(&value) {
                        log::warn!("Local live metrics checkpoint write failed: {error}");
                        let mut state = state.lock();
                        // A newer cumulative snapshot supersedes the failed one.
                        let has_newer = state.pending.is_some();
                        if !has_newer {
                            state.pending = Some(value);
                        }
                        if state.closing {
                            if has_newer {
                                continue;
                            }
                            return Err(error);
                        }
                        let retry_at = std::time::Instant::now() + Duration::from_secs(1);
                        while !state.closing && std::time::Instant::now() < retry_at {
                            wake.wait_until(&mut state, retry_at);
                        }
                    }
                }
            })
            .map_err(|error| error.to_string())?;
        Ok(Self {
            shared,
            thread: Some(thread),
        })
    }

    pub(super) fn submit(&self, value: T) -> Result<(), String> {
        let (state, wake) = &*self.shared;
        let mut state = state.lock();
        if state.closing
            || self
                .thread
                .as_ref()
                .is_none_or(|thread| thread.is_finished())
        {
            return Err("metrics checkpoint writer is stopped".into());
        }
        // Only pending (not yet writing) replaceable snapshots are coalesced.
        let previous = state.pending.replace(value);
        wake.notify_one();
        drop(state);
        drop(previous);
        Ok(())
    }

    pub(super) async fn shutdown(&mut self) -> Result<(), String> {
        self.close();
        // Retain the join handle if this future is cancelled. Join only after
        // exit, never block the async executor on a filesystem operation.
        while self
            .thread
            .as_ref()
            .is_some_and(|thread| !thread.is_finished())
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        match self.thread.take() {
            Some(thread) => thread
                .join()
                .map_err(|_| "metrics checkpoint writer panicked".to_string())?,
            None => Ok(()),
        }
    }
}

impl<T> CoalescedWriter<T> {
    fn close(&self) {
        let (state, wake) = &*self.shared;
        state.lock().closing = true;
        wake.notify_one();
    }
}

impl<T> Drop for CoalescedWriter<T> {
    fn drop(&mut self) {
        // Emergency drop requests a drain; normal lifecycle explicitly joins.
        // The worker owns no StrategyState/adapter references.
        self.close();
    }
}

pub(super) fn write_checkpoint(
    path: &Path,
    stored: &LocalLiveMetricsCheckpoint,
) -> Result<(), String> {
    use std::fs::{self, OpenOptions};
    use std::io::{BufWriter, Write};
    let parent = path.parent().ok_or("checkpoint has no parent directory")?;
    fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    let temporary = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
    let result = (|| -> Result<(), String> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|error| error.to_string())?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, stored).map_err(|error| error.to_string())?;
        writer.flush().map_err(|error| error.to_string())?;
        writer
            .get_ref()
            .sync_all()
            .map_err(|error| error.to_string())?;
        drop(writer);
        // Same-directory replacement; never remove/move the old checkpoint
        // first. A failed replacement must leave that recovery copy intact.
        fs::rename(&temporary, path).map_err(|error| error.to_string())?;
        #[cfg(unix)]
        fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| error.to_string())?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

#[cfg(test)]
mod tests;
