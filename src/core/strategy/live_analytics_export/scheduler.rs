use super::*;

/// One export page in flight per strategy; rotate after every attempt, not
/// after draining a stream. A full fill backlog cannot monopolize mark delivery.
#[derive(Default)]
pub(crate) struct ExecutionExportScheduler {
    next_stream: usize,
}
impl ExecutionExportScheduler {
    pub(in crate::core::strategy) fn poll(
        &mut self,
        mut streams: [&mut ExecutionAnalyticsExporter; 2],
        sender: Option<BrokerCommandSender>,
        publisher: &LocalEventPublisher,
        identity: ExportIdentity,
    ) {
        for stream in &mut streams {
            stream.poll_completion();
            stream.refresh_health();
        }
        if streams.iter().any(|stream| stream.active.is_some()) {
            return;
        }
        for offset in 0..streams.len() {
            let index = (self.next_stream + offset) % streams.len();
            if streams[index].try_start(sender.clone(), publisher, &identity) {
                self.next_stream = (index + 1) % streams.len();
                break;
            }
        }
    }
}
