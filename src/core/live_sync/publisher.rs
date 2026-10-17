use super::{
    CloudEvent, EventBusError, EventDelivery, EventId, EventPriority, EventSubject,
    EventSubscription, FlatbufferEventCodec, InProcessEventBus, LocalEventBus, NatsEventBus,
    OutboxStore, SyncAcknowledgement,
};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, mpsc, watch};

#[derive(Clone, Debug)]
pub struct LocalEventPublisherConfig {
    /// Hard admission bounds, shared by all cloned producer handles.
    pub critical_queue_events: usize,
    pub critical_queue_bytes: usize,
    pub critical_batch_delay: Duration,
    pub critical_batch_events: usize,
    pub critical_batch_bytes: usize,
    pub telemetry_flush_delay: Duration,
    pub telemetry_entities: usize,
    pub telemetry_bytes: usize,
    pub volatile_identity_events: usize,
    pub nats_unavailable_fallback_after: Duration,
    pub graceful_shutdown_timeout: Duration,
}

impl Default for LocalEventPublisherConfig {
    fn default() -> Self {
        Self {
            critical_queue_events: 16_384,
            critical_queue_bytes: 64 * 1024 * 1024,
            critical_batch_delay: Duration::from_millis(10),
            critical_batch_events: 32,
            critical_batch_bytes: 64 * 1024,
            telemetry_flush_delay: Duration::from_millis(100),
            telemetry_entities: 4_096,
            telemetry_bytes: 16 * 1024 * 1024,
            volatile_identity_events: 65_536,
            nats_unavailable_fallback_after: Duration::from_millis(500),
            graceful_shutdown_timeout: Duration::from_secs(3),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PublisherHealth {
    #[serde(default)]
    pub admission_failed: bool,
    #[serde(default)]
    pub queued_encoded_bytes: u64,
    pub pending_critical: u64,
    pub pending_telemetry_entities: u64,
    #[serde(default)]
    pub queued_telemetry_encoded_bytes: u64,
    #[serde(default)]
    pub dropped_telemetry: u64,
    #[serde(default)]
    pub volatile_identity_evictions: u64,
    pub locally_accepted_critical: u64,
    pub deduplicated_critical: u64,
    pub emergency_outbox_writes: u64,
    pub publish_failures: u64,
    pub last_local_ack_latency_us: Option<u64>,
    pub shutting_down: bool,
}

#[derive(Default)]
struct PublisherCounters {
    admission_failed: AtomicBool,
    pending_critical: AtomicU64,
    queued_encoded_bytes: AtomicU64,
    accepted: AtomicU64,
    deduplicated: AtomicU64,
    emergency: AtomicU64,
    failures: AtomicU64,
    telemetry_dropped: AtomicU64,
    volatile_identity_evictions: AtomicU64,
    last_ack_us: AtomicU64,
    shutting_down: AtomicBool,
    finished: AtomicBool,
}

struct QueuedCritical {
    durable_receipt: Option<tokio::sync::oneshot::Sender<Result<EventId, EventBusError>>>,
    subject: EventSubject,
    event: CloudEvent,
    _memory: QueuedMemory,
}

struct QueuedTelemetry {
    subject: EventSubject,
    event: CloudEvent,
    bytes: usize,
}

#[derive(Default)]
struct TelemetryQueue {
    entries: HashMap<(String, String), QueuedTelemetry>,
    bytes: usize,
}

struct VolatileIdentityWindow {
    ids: HashSet<EventId>,
    order: VecDeque<EventId>,
    limit: usize,
}

impl VolatileIdentityWindow {
    fn new(limit: usize) -> Self {
        Self {
            ids: HashSet::with_capacity(limit.min(65_536)),
            order: VecDeque::with_capacity(limit.min(65_536)),
            limit,
        }
    }

    fn contains(&self, id: &EventId) -> bool {
        self.ids.contains(id)
    }

    fn insert(&mut self, id: EventId) -> bool {
        if !self.ids.insert(id) {
            return false;
        }
        self.order.push_back(id);
        if self.ids.len() <= self.limit {
            return false;
        }
        let Some(oldest) = self.order.pop_front() else {
            return false;
        };
        self.ids.remove(&oldest)
    }
}

// A queue reservation follows the envelope through persistence preparation and
// is released on every exit, including deduplication and a stopped receiver.
struct QueuedMemory {
    counters: Arc<PublisherCounters>,
    bytes: u64,
}
impl Drop for QueuedMemory {
    fn drop(&mut self) {
        self.counters
            .queued_encoded_bytes
            .fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

#[derive(Clone, Debug)]
struct EmergencyReplayScope {
    runtime_id: String,
    strategy_id: String,
    live_session_id: String,
}

#[derive(Clone)]
pub struct LocalEventPublisher {
    bus: Arc<dyn LocalEventBus>,
    critical_sender: mpsc::Sender<QueuedCritical>,
    critical_queue_bytes: u64,
    telemetry: Arc<Mutex<TelemetryQueue>>,
    telemetry_entity_limit: usize,
    telemetry_byte_limit: usize,
    telemetry_notify: Arc<Notify>,
    critical_sequence: Arc<AtomicU64>,
    // Volatile standalone buses have no durable cursor. Hosted/managed publishers
    // deduplicate in their SQLite worker instead of retaining every ID in RAM.
    critical_event_ids: Option<Arc<Mutex<VolatileIdentityWindow>>>,
    counters: Arc<PublisherCounters>,
    shutdown_sender: watch::Sender<bool>,
    dead_letter_outbox: Option<OutboxStore>,
}

impl LocalEventPublisher {
    pub fn supports_durable_admission(&self) -> bool {
        self.dead_letter_outbox.is_some()
    }

    /// Admission pressure for bulk projection producers. Unlike `health`, this
    /// hot-path read does not lock the telemetry map or construct a snapshot.
    pub fn pending_critical_count(&self) -> u64 {
        self.counters.pending_critical.load(Ordering::Relaxed)
    }

    pub fn queued_critical_encoded_bytes(&self) -> u64 {
        self.counters.queued_encoded_bytes.load(Ordering::Relaxed)
    }

    pub fn spawn(
        bus: Arc<dyn LocalEventBus>,
        emergency_outbox: Option<OutboxStore>,
        config: LocalEventPublisherConfig,
    ) -> Self {
        Self::spawn_with_initial_sequence(bus, emergency_outbox, config, 0)
    }

    pub fn spawn_with_initial_sequence(
        bus: Arc<dyn LocalEventBus>,
        emergency_outbox: Option<OutboxStore>,
        config: LocalEventPublisherConfig,
        initial_critical_sequence: u64,
    ) -> Self {
        Self::spawn_scoped(
            bus,
            emergency_outbox,
            config,
            initial_critical_sequence,
            None,
            None,
        )
    }

    /// Retains exclusive outbox ownership until the actual publisher task has
    /// finished draining, even when its outer strategy future is cancelled.
    pub(crate) fn spawn_owned_with_initial_sequence(
        bus: Arc<dyn LocalEventBus>,
        emergency_outbox: OutboxStore,
        config: LocalEventPublisherConfig,
        initial_sequence: u64,
        owner: Arc<dyn Send + Sync>,
    ) -> Self {
        Self::spawn_scoped(
            bus,
            Some(emergency_outbox),
            config,
            initial_sequence,
            None,
            Some(owner),
        )
    }

    fn spawn_scoped(
        bus: Arc<dyn LocalEventBus>,
        emergency_outbox: Option<OutboxStore>,
        config: LocalEventPublisherConfig,
        initial_critical_sequence: u64,
        emergency_replay_scope: Option<EmergencyReplayScope>,
        owner: Option<Arc<dyn Send + Sync>>,
    ) -> Self {
        let (critical_sender, critical_receiver) =
            mpsc::channel(config.critical_queue_events.max(1));
        let critical_queue_bytes = config.critical_queue_bytes.max(1) as u64;
        let telemetry_entity_limit = config.telemetry_entities.max(1);
        let telemetry_byte_limit = config.telemetry_bytes.max(1);
        let volatile_identity_limit = config
            .volatile_identity_events
            .max(config.critical_queue_events)
            .max(1);
        let (shutdown_sender, shutdown_receiver) = watch::channel(false);
        let telemetry = Arc::new(Mutex::new(TelemetryQueue::default()));
        let telemetry_notify = Arc::new(Notify::new());
        let counters = Arc::new(PublisherCounters::default());
        let critical_sequence = Arc::new(AtomicU64::new(initial_critical_sequence));
        let critical_event_ids = emergency_outbox.is_none().then(|| {
            Arc::new(Mutex::new(VolatileIdentityWindow::new(
                volatile_identity_limit,
            )))
        });

        let dead_letter_outbox = emergency_outbox.clone();
        tokio::spawn(run_publisher(
            Arc::clone(&bus),
            critical_receiver,
            Arc::clone(&telemetry),
            Arc::clone(&telemetry_notify),
            Arc::clone(&counters),
            shutdown_receiver,
            emergency_outbox,
            emergency_replay_scope,
            config,
            owner,
            Arc::clone(&critical_sequence),
        ));

        Self {
            bus,
            critical_sender,
            critical_queue_bytes,
            telemetry,
            telemetry_entity_limit,
            telemetry_byte_limit,
            telemetry_notify,
            critical_sequence,
            critical_event_ids,
            counters,
            shutdown_sender,
            dead_letter_outbox,
        }
    }

    pub async fn subscribe(
        &self,
        subject: EventSubject,
    ) -> Result<EventSubscription, EventBusError> {
        self.bus.subscribe(subject).await
    }

    pub async fn request_event(
        &self,
        subject: EventSubject,
        event: CloudEvent,
        timeout: Duration,
    ) -> Result<CloudEvent, EventBusError> {
        self.bus.request_event(subject, event, timeout).await
    }

    pub async fn respond_event(
        &self,
        delivery: &EventDelivery,
        event: CloudEvent,
    ) -> Result<(), EventBusError> {
        self.bus.respond_event(delivery, event).await
    }

    pub async fn acknowledge(
        &self,
        delivery: &EventDelivery,
        acknowledgement: SyncAcknowledgement,
    ) -> Result<(), EventBusError> {
        self.bus.acknowledge(delivery, acknowledgement).await
    }

    pub async fn from_environment(
        emergency_path: Option<PathBuf>,
        sequence_scope: Option<(&str, &str)>,
        config: LocalEventPublisherConfig,
    ) -> Result<Self, EventBusError> {
        let setting = |name| crate::launch_config::setting(name).map_err(EventBusError::Connect);
        let bus: Arc<dyn LocalEventBus> = match setting("AQE_EVENT_BUS")?
            .unwrap_or_else(|| "in_process".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            "nats" => {
                let url = setting("AQE_NATS_URL")?
                    .ok_or_else(|| EventBusError::Connect("AQE_NATS_URL is missing".to_string()))?;
                let token = setting("AQE_NATS_TOKEN")?
                    .ok_or_else(|| EventBusError::Connect("AQE_NATS_TOKEN is missing".to_string()))?;
                Arc::new(NatsEventBus::connect(&url, &token).await?)
            }
            "in_process" | "in-process" | "local" => Arc::new(InProcessEventBus::default()),
            other => {
                return Err(EventBusError::Connect(format!(
                    "unsupported AQE_EVENT_BUS value '{other}'"
                )));
            }
        };
        let emergency_outbox = match emergency_path {
            Some(path) => Some(
                OutboxStore::open(path)
                    .await
                    .map_err(|error| EventBusError::Connect(error.to_string()))?,
            ),
            None => None,
        };
        let emergency_replay_scope = match (setting("AQE_RUNTIME_ID")?, sequence_scope) {
            (Some(runtime_id), Some((strategy_id, live_session_id))) => {
                Some(EmergencyReplayScope {
                    runtime_id,
                    strategy_id: strategy_id.to_string(),
                    live_session_id: live_session_id.to_string(),
                })
            }
            _ => None,
        };
        let initial_sequence = match (emergency_outbox.as_ref(), emergency_replay_scope.as_ref()) {
            (Some(outbox), Some(scope)) => outbox
                .critical_sequence_cursor_for_runtime_scope(
                    &scope.runtime_id,
                    &scope.strategy_id,
                    &scope.live_session_id,
                )
                .await
                .map_err(|error| EventBusError::Connect(error.to_string()))?,
            (Some(outbox), None) => match sequence_scope {
                Some((strategy_id, live_session_id)) => outbox
                    .critical_sequence_cursor(strategy_id, live_session_id)
                    .await
                    .map_err(|error| EventBusError::Connect(error.to_string()))?,
                None => 0,
            },
            _ => 0,
        };
        Ok(Self::spawn_scoped(
            bus,
            emergency_outbox,
            config,
            initial_sequence,
            emergency_replay_scope,
            None,
        ))
    }

    pub fn publish_critical(
        &self,
        subject: EventSubject,
        event: CloudEvent,
    ) -> Result<EventId, EventBusError> {
        let result = self.try_publish_critical(subject, event);
        if matches!(result, Err(EventBusError::PublisherCapacity)) {
            self.mark_admission_failed();
        }
        result
    }

    pub fn mark_admission_failed(&self) {
        self.counters
            .admission_failed
            .store(true, Ordering::Release);
    }

    /// Retry-capable owners must retain the exact envelope on rejection.
    /// Capacity pressure alone does not poison their synchronization status.
    pub fn try_publish_critical(
        &self,
        subject: EventSubject,
        event: CloudEvent,
    ) -> Result<EventId, EventBusError> {
        self.enqueue_critical(subject, event, None)
    }

    /// Queue admission is not durability. This receipt resolves only after the
    /// publisher's SQLite transaction commits, independently of cloud delivery.
    pub fn queue_durable(
        &self,
        subject: EventSubject,
        event: CloudEvent,
    ) -> Result<tokio::sync::oneshot::Receiver<Result<EventId, EventBusError>>, EventBusError> {
        if self.dead_letter_outbox.is_none() {
            return Err(EventBusError::Publish(
                "durable admission requires a local outbox".into(),
            ));
        }
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.enqueue_critical(subject, event, Some(sender))?;
        Ok(receiver)
    }

    fn enqueue_critical(
        &self,
        subject: EventSubject,
        mut event: CloudEvent,
        durable_receipt: Option<tokio::sync::oneshot::Sender<Result<EventId, EventBusError>>>,
    ) -> Result<EventId, EventBusError> {
        if self.counters.shutting_down.load(Ordering::Acquire) {
            return Err(EventBusError::Publish(
                "local publisher is shutting down".into(),
            ));
        }
        if event.priority == EventPriority::Critical
            && self
                .critical_event_ids
                .as_ref()
                .is_some_and(|ids| ids.lock().contains(&event.event_id))
        {
            self.counters.deduplicated.fetch_add(1, Ordering::Relaxed);
            return Ok(event.event_id);
        }
        // Reserve before assigning identity/sequence or publishing a provisional
        // copy. Rejected work is not accepted and must remain with its producer.
        let permit = self
            .critical_sender
            .clone()
            .try_reserve_owned()
            .map_err(|error| {
                if matches!(error, mpsc::error::TrySendError::Full(_)) {
                    EventBusError::PublisherCapacity
                } else {
                    EventBusError::Publish("local publisher has stopped".into())
                }
            })?;
        let encoded_bytes = FlatbufferEventCodec::encode(&event)
            .map_err(|error| EventBusError::Codec(error.to_string()))?;
        let bytes = encoded_bytes.len() as u64;
        self.counters
            .queued_encoded_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|next| *next <= self.critical_queue_bytes)
            })
            .map_err(|_| EventBusError::PublisherCapacity)?;
        let memory = QueuedMemory {
            counters: self.counters.clone(),
            bytes,
        };
        if encoded_bytes.len() > super::LOCAL_EVENT_HARD_LIMIT_BYTES {
            if let Some(outbox) = self.dead_letter_outbox.clone() {
                let rejected = event.clone();
                let reason = format!(
                    "oversized local event: kind={} bytes={} max={}",
                    rejected.kind,
                    encoded_bytes.len(),
                    super::LOCAL_EVENT_HARD_LIMIT_BYTES
                );
                tokio::spawn(async move {
                    // Oversized diagnostics share admission limits too; a burst
                    // must not create an unbounded set of SQLite writer tasks.
                    let _permit = permit;
                    let _memory = memory;
                    if outbox
                        .insert_events(std::slice::from_ref(&rejected))
                        .await
                        .is_ok()
                    {
                        let _ = outbox.move_to_dead_letter(rejected.event_id, &reason).await;
                    }
                });
            }
            return Err(EventBusError::PayloadTooLarge {
                event_id: event.event_id,
                encoded_bytes: encoded_bytes.len(),
                max_bytes: super::LOCAL_EVENT_HARD_LIMIT_BYTES,
            });
        }
        // Reconciliation uses the same durable request/reply acknowledgement path as critical
        // lifecycle events but remains a separate outbox priority so it can be paused whenever
        // the critical backlog grows.
        if !matches!(
            event.priority,
            EventPriority::Audit | EventPriority::Reconciliation
        ) {
            event.priority = EventPriority::Critical;
        }
        if let Some(event_ids) = self
            .critical_event_ids
            .as_ref()
            .filter(|_| event.priority == EventPriority::Critical)
        {
            // Event identity is assigned before this boundary and is stable across retries.
            // Reserve a lifecycle sequence only for the first occurrence of that identity.
            // Allocating first and relying on SQLite's INSERT OR IGNORE creates an unrecoverable
            // sequence hole whenever a broker repeats the same lifecycle callback.
            let mut event_ids = event_ids.lock();
            if event_ids.contains(&event.event_id) {
                self.counters.deduplicated.fetch_add(1, Ordering::Relaxed);
                return Ok(event.event_id);
            }
            let sequence = match event.critical_sequence {
                Some(sequence) => {
                    self.critical_sequence
                        .fetch_max(sequence, Ordering::Relaxed);
                    sequence
                }
                None => self
                    .critical_sequence
                    .fetch_add(1, Ordering::Relaxed)
                    .saturating_add(1),
            };
            if event_ids.insert(event.event_id) {
                self.counters
                    .volatile_identity_evictions
                    .fetch_add(1, Ordering::Relaxed);
            }
            event.critical_sequence = Some(sequence);
        } else if event.priority != EventPriority::Critical {
            // Audit and reconciliation are durable but are not part of the ordered lifecycle
            // cursor, preventing background work from introducing gaps in the critical stream.
            event.critical_sequence = None;
        }
        let event_id = event.event_id;
        // The provisional stream is intentionally a one-shot copy made only by the live
        // publisher. Emergency/historical outbox replay bypasses this method and therefore can
        // never flood a local UI subscriber.
        if self.critical_event_ids.is_some() || event.priority != EventPriority::Critical {
            let _ = self.bus.publish_telemetry(
                EventSubject::provisional(&event.runtime_id, &event.strategy_id),
                event.clone(),
            );
        }
        self.counters
            .pending_critical
            .fetch_add(1, Ordering::Relaxed);
        permit.send(QueuedCritical {
            subject,
            event,
            _memory: memory,
            durable_receipt,
        });
        Ok(event_id)
    }

    pub fn publish_telemetry(
        &self,
        subject: EventSubject,
        mut event: CloudEvent,
    ) -> Result<(), EventBusError> {
        if self.counters.shutting_down.load(Ordering::Relaxed) {
            return Err(EventBusError::Publish(
                "local publisher is shutting down".to_string(),
            ));
        }
        event.priority = EventPriority::Telemetry;
        let encoded_bytes = FlatbufferEventCodec::encode(&event)
            .map_err(|error| EventBusError::Codec(error.to_string()))?
            .len();
        if encoded_bytes > super::LOCAL_EVENT_HARD_LIMIT_BYTES
            || encoded_bytes > self.telemetry_byte_limit
        {
            self.counters
                .telemetry_dropped
                .fetch_add(1, Ordering::Relaxed);
            return Err(EventBusError::PublisherCapacity);
        }
        let key = (subject.to_string(), event.entity_id.clone());
        let provisional_subject = EventSubject::provisional(&event.runtime_id, &event.strategy_id);
        let provisional_event = event.clone();
        let mut telemetry = self.telemetry.lock();
        let should_replace = telemetry
            .entries
            .get(&key)
            .is_none_or(|current| event.entity_revision >= current.event.entity_revision);
        if should_replace {
            let previous_bytes = telemetry.entries.get(&key).map_or(0, |entry| entry.bytes);
            let next_bytes = telemetry
                .bytes
                .saturating_sub(previous_bytes)
                .saturating_add(encoded_bytes);
            if (previous_bytes == 0 && telemetry.entries.len() >= self.telemetry_entity_limit)
                || next_bytes > self.telemetry_byte_limit
            {
                self.counters
                    .telemetry_dropped
                    .fetch_add(1, Ordering::Relaxed);
                return Err(EventBusError::PublisherCapacity);
            }
            telemetry.bytes = next_bytes;
            telemetry.entries.insert(
                key,
                QueuedTelemetry {
                    subject,
                    event,
                    bytes: encoded_bytes,
                },
            );
            drop(telemetry);
            let _ = self
                .bus
                .publish_telemetry(provisional_subject, provisional_event);
            self.telemetry_notify.notify_one();
        }
        Ok(())
    }

    pub fn health(&self) -> PublisherHealth {
        let ack = self.counters.last_ack_us.load(Ordering::Relaxed);
        let telemetry = self.telemetry.lock();
        let pending_telemetry_entities = telemetry.entries.len() as u64;
        let queued_telemetry_encoded_bytes = telemetry.bytes as u64;
        drop(telemetry);
        PublisherHealth {
            admission_failed: self.admission_failed(),
            queued_encoded_bytes: self.queued_critical_encoded_bytes(),
            pending_critical: self.counters.pending_critical.load(Ordering::Relaxed),
            pending_telemetry_entities,
            queued_telemetry_encoded_bytes,
            dropped_telemetry: self.counters.telemetry_dropped.load(Ordering::Relaxed),
            volatile_identity_evictions: self
                .counters
                .volatile_identity_evictions
                .load(Ordering::Relaxed),
            locally_accepted_critical: self.counters.accepted.load(Ordering::Relaxed),
            deduplicated_critical: self.counters.deduplicated.load(Ordering::Relaxed),
            emergency_outbox_writes: self.counters.emergency.load(Ordering::Relaxed),
            publish_failures: self.counters.failures.load(Ordering::Relaxed),
            last_local_ack_latency_us: (ack > 0).then_some(ack),
            shutting_down: self.counters.shutting_down.load(Ordering::Relaxed),
        }
    }

    /// Highest reserved critical sequence. Durable publishers advance it only after
    /// committing the reservation and payload. Events still queued at a snapshot
    /// cut-off conservatively count as newer and win over reconciliation results.
    pub fn critical_sequence_cursor(&self) -> u64 {
        self.critical_sequence.load(Ordering::Acquire)
    }

    pub async fn shutdown(&self, timeout: Duration) {
        self.counters.shutting_down.store(true, Ordering::Relaxed);
        let _ = self.shutdown_sender.send(true);
        let deadline = Instant::now() + timeout;
        while !self.counters.finished.load(Ordering::Acquire) && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    pub(crate) fn shutdown_complete(&self) -> bool {
        self.counters.finished.load(Ordering::Acquire) && !self.admission_failed()
    }

    /// Sticky: callers which ignored an enqueue failure must not report a
    /// successful synchronization merely because the accepted queue drained.
    pub fn admission_failed(&self) -> bool {
        self.counters.admission_failed.load(Ordering::Acquire)
    }
}

async fn run_publisher(
    bus: Arc<dyn LocalEventBus>,
    mut critical_receiver: mpsc::Receiver<QueuedCritical>,
    telemetry: Arc<Mutex<TelemetryQueue>>,
    telemetry_notify: Arc<Notify>,
    counters: Arc<PublisherCounters>,
    mut shutdown_receiver: watch::Receiver<bool>,
    emergency_outbox: Option<OutboxStore>,
    emergency_replay_scope: Option<EmergencyReplayScope>,
    config: LocalEventPublisherConfig,
    owner: Option<Arc<dyn Send + Sync>>,
    critical_sequence: Arc<AtomicU64>,
) {
    let _owner = owner;
    let mut telemetry_interval = tokio::time::interval_at(
        tokio::time::Instant::now() + config.telemetry_flush_delay,
        config.telemetry_flush_delay,
    );
    telemetry_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut emergency_replay_interval = tokio::time::interval(Duration::from_millis(100));
    emergency_replay_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut maintenance = tokio::time::interval(Duration::from_secs(30));
    maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut emergency_pending =
        emergency_outbox_has_pending(emergency_outbox.as_ref(), emergency_replay_scope.as_ref())
            .await;
    loop {
        tokio::select! {
            first = critical_receiver.recv() => {
                let Some(first) = first else { break };
                let batch = collect_critical_batch(first, &mut critical_receiver, &config).await;
                let batch = prepare_durable_batch(
                    bus.as_ref(), emergency_outbox.as_ref(), batch, &counters, &critical_sequence,
                ).await;
                if emergency_pending {
                    persist_queued_batch_to_emergency(
                        emergency_outbox.as_ref(),
                        batch,
                        &counters,
                    ).await;
                    emergency_pending = !replay_emergency_outbox(
                        bus.as_ref(), emergency_outbox.as_ref(), emergency_replay_scope.as_ref(),
                        &counters, &config,
                    ).await;
                } else {
                    emergency_pending = persist_or_publish_critical_batch(
                        bus.as_ref(),
                        emergency_outbox.as_ref(),
                        batch,
                        &counters,
                        &config,
                    )
                    .await;
                }
            }
            _ = telemetry_interval.tick() => {
                flush_telemetry(bus.as_ref(), &telemetry, &counters);
            }
            _ = maintenance.tick(), if emergency_outbox.is_some() => {
                if let Some(outbox) = &emergency_outbox {
                    let _ = outbox.prune_committed(0).await;
                    let _ = outbox.checkpoint_wal(false).await;
                }
            }
            _ = telemetry_notify.notified() => {}
            _ = emergency_replay_interval.tick(), if emergency_pending => {
                emergency_pending = !replay_emergency_outbox(
                    bus.as_ref(),
                    emergency_outbox.as_ref(),
                    emergency_replay_scope.as_ref(),
                    &counters,
                    &config,
                ).await;
            }
            changed = shutdown_receiver.changed() => {
                if changed.is_err() || *shutdown_receiver.borrow() {
                    // Refuse concurrent late sends, then drain already queued work
                    // with the same transactional micro-batches as normal operation.
                    critical_receiver.close();
                    if emergency_pending {
                        emergency_pending = !replay_emergency_outbox(
                            bus.as_ref(),
                            emergency_outbox.as_ref(),
                            emergency_replay_scope.as_ref(),
                            &counters,
                            &config,
                        ).await;
                    }
                    // close() rejects new reservations, but a producer may own
                    // a permit acquired before shutdown. recv() also waits for
                    // those permits; try_recv() could incorrectly finish early.
                    while let Some(first) = critical_receiver.recv().await {
                        let batch = collect_critical_batch(first, &mut critical_receiver, &config).await;
                        let batch = prepare_durable_batch(
                            bus.as_ref(), emergency_outbox.as_ref(), batch, &counters, &critical_sequence,
                        ).await;
                        if emergency_pending {
                            persist_queued_batch_to_emergency(
                                emergency_outbox.as_ref(),
                                batch,
                                &counters,
                            ).await;
                        } else {
                            emergency_pending = persist_or_publish_critical_batch(
                                bus.as_ref(),
                                emergency_outbox.as_ref(),
                                batch,
                                &counters,
                                &config,
                            ).await;
                        }
                    }
                    flush_telemetry(bus.as_ref(), &telemetry, &counters);
                    break;
                }
            }
        }
    }
    counters.finished.store(true, Ordering::Release);
}

async fn prepare_durable_batch(
    bus: &dyn LocalEventBus,
    outbox: Option<&OutboxStore>,
    batch: Vec<QueuedCritical>,
    counters: &PublisherCounters,
    sequence: &AtomicU64,
) -> Vec<QueuedCritical> {
    let Some(outbox) = outbox else { return batch };
    // Audit/history and reconciliation are just as durable as lifecycle work,
    // but do not consume lifecycle sequence numbers. Commit them before any
    // transport attempt: a failed emergency write must never discard them.
    let background = batch
        .iter()
        .filter(|queued| queued.event.priority != EventPriority::Critical)
        .map(|queued| queued.event.clone())
        .collect::<Vec<_>>();
    let mut rejected = HashSet::new();
    let mut groups = if background.is_empty() {
        Vec::new()
    } else {
        vec![background]
    };
    while let Some(mut group) = groups.pop() {
        loop {
            match outbox.insert_events(&group).await {
                Ok(_) => break,
                Err(super::OutboxStoreError::InvalidRecord(error)) => {
                    if group.len() > 1 {
                        let right = group.split_off(group.len() / 2);
                        groups.push(right);
                        groups.push(group);
                        break;
                    }
                    match outbox.reject_publisher_event(&group[0], &error).await {
                        Ok(()) => {
                            rejected.insert(group[0].event_id);
                            break;
                        }
                        Err(error) => log::warn!(
                            "Publisher background DLQ unavailable; retaining event: {error}"
                        ),
                    }
                }
                Err(error) => {
                    log::warn!(
                        "Publisher history/reconciliation persistence unavailable; retaining batch: {error}"
                    );
                }
            }
            counters.failures.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    let events = batch
        .iter()
        .filter(|q| q.event.priority == EventPriority::Critical)
        .map(|q| q.event.clone())
        .collect::<Vec<_>>();
    // A transient local storage failure retains this bounded batch in the worker;
    // it does not acknowledge, allocate a cursor separately, or discard the event.
    let mut groups = vec![events];
    let mut fresh = HashMap::new();
    while let Some(mut group) = groups.pop() {
        loop {
            match outbox
                .reserve_publisher_events(&group, sequence.load(Ordering::Acquire))
                .await
            {
                Ok(events) => {
                    for event in events {
                        sequence.fetch_max(event.critical_sequence.unwrap_or(0), Ordering::Release);
                        fresh.insert(event.event_id, event);
                    }
                    break;
                }
                Err(super::OutboxStoreError::InvalidRecord(error)) => {
                    if group.len() > 1 {
                        let right = group.split_off(group.len() / 2);
                        // Stack order preserves the original lifecycle order.
                        groups.push(right);
                        groups.push(group);
                        break;
                    }
                    match outbox.reject_publisher_event(&group[0], &error).await {
                        Ok(()) => {
                            log::warn!(
                                "Critical publisher rejected event {}: {error}",
                                group[0].event_id
                            );
                            rejected.insert(group[0].event_id);
                            break;
                        }
                        Err(error) => log::warn!(
                            "Critical publisher DLQ unavailable; retaining event: {error}"
                        ),
                    }
                }
                Err(error) => log::warn!(
                    "Critical publisher reservation unavailable; retaining batch: {error}"
                ),
            }
            counters.failures.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    let mut ready = Vec::with_capacity(batch.len());
    for mut queued in batch {
        if let Some(receipt) = queued.durable_receipt.take() {
            let result = if rejected.contains(&queued.event.event_id) {
                Err(EventBusError::Publish(
                    "local outbox rejected event identity".into(),
                ))
            } else {
                Ok(queued.event.event_id)
            };
            let _ = receipt.send(result);
        }
        if rejected.contains(&queued.event.event_id) {
            counters.failures.fetch_add(1, Ordering::Relaxed);
            counters.pending_critical.fetch_sub(1, Ordering::Relaxed);
        } else if queued.event.priority != EventPriority::Critical {
            ready.push(queued);
        } else if let Some(event) = fresh.remove(&queued.event.event_id) {
            let _ = bus.publish_telemetry(
                EventSubject::provisional(&event.runtime_id, &event.strategy_id),
                event.clone(),
            );
            queued.event = event;
            ready.push(queued);
        } else {
            counters.deduplicated.fetch_add(1, Ordering::Relaxed);
            counters.pending_critical.fetch_sub(1, Ordering::Relaxed);
        }
    }
    ready
}

async fn collect_critical_batch(
    first: QueuedCritical,
    receiver: &mut mpsc::Receiver<QueuedCritical>,
    config: &LocalEventPublisherConfig,
) -> Vec<QueuedCritical> {
    let started = Instant::now();
    let mut byte_count = first._memory.bytes as usize;
    let mut batch = vec![first];
    while batch.len() < config.critical_batch_events
        && byte_count < config.critical_batch_bytes
        && started.elapsed() < config.critical_batch_delay
    {
        match receiver.try_recv() {
            Ok(next) => {
                let size = next._memory.bytes as usize;
                if !batch.is_empty()
                    && byte_count.saturating_add(size) > config.critical_batch_bytes
                {
                    // The unbounded channel cannot push this value back. The configured 64 KiB
                    // threshold is a soft micro-batch target; retain correctness by including it.
                }
                byte_count = byte_count.saturating_add(size);
                batch.push(next);
            }
            Err(mpsc::error::TryRecvError::Empty) => {
                let remaining = config
                    .critical_batch_delay
                    .saturating_sub(started.elapsed());
                if remaining.is_zero() {
                    break;
                }
                tokio::time::sleep(remaining.min(Duration::from_millis(1))).await;
            }
            Err(mpsc::error::TryRecvError::Disconnected) => break,
        }
    }
    batch
}

/// Publishes a lifecycle micro-batch to NATS in sequence order, then waits for the independent
/// SQLite acknowledgements concurrently. Publishing in order preserves NATS subject ordering;
/// concurrent acknowledgement waits still let the sync agent commit the entire micro-batch in
/// one WAL transaction instead of forcing one transaction per event.
async fn persist_or_publish_critical_batch(
    bus: &dyn LocalEventBus,
    emergency_outbox: Option<&OutboxStore>,
    batch: Vec<QueuedCritical>,
    counters: &PublisherCounters,
    config: &LocalEventPublisherConfig,
) -> bool {
    let mut awaiting = Vec::with_capacity(batch.len());
    let mut used_emergency_outbox = false;
    for queued in batch {
        let started = Instant::now();
        let event = queued.event;
        match bus.publish_critical(queued.subject, event.clone()).await {
            Ok(handle) => awaiting.push((started, event, handle, queued._memory)),
            Err(_) => {
                counters.failures.fetch_add(1, Ordering::Relaxed);
                persist_emergency(emergency_outbox, &event, counters).await;
                used_emergency_outbox = true;
                counters.pending_critical.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    let results = futures::future::join_all(awaiting.into_iter().map(
        |(started, event, handle, memory)| async move {
            let accepted =
                tokio::time::timeout(config.nats_unavailable_fallback_after, handle.wait())
                    .await
                    .ok()
                    .and_then(Result::ok)
                    .is_some();
            (started, event, accepted, memory)
        },
    ))
    .await;
    let mut accepted_ids = Vec::with_capacity(results.len());
    for (started, event, accepted, _memory) in results {
        if accepted {
            accepted_ids.push(event.event_id);
            counters.accepted.fetch_add(1, Ordering::Relaxed);
            counters
                .last_ack_us
                .store(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        } else {
            counters.failures.fetch_add(1, Ordering::Relaxed);
            persist_emergency(emergency_outbox, &event, counters).await;
            used_emergency_outbox = true;
        }
        counters.pending_critical.fetch_sub(1, Ordering::Relaxed);
    }
    let retired = retire_publisher_payloads(emergency_outbox, &accepted_ids).await;
    used_emergency_outbox || !retired
}

async fn retire_publisher_payloads(outbox: Option<&OutboxStore>, ids: &[EventId]) -> bool {
    let Some(outbox) = outbox else { return true };
    if ids.is_empty() {
        return true;
    }
    // This is the separate publisher outbox. Its downstream durable owner has
    // acknowledged local acceptance, not necessarily a remote database commit.
    if outbox.mark_remote_committed(ids, None).await.is_err() {
        return false;
    }
    // The maintenance tick prunes full payloads, not identity reservations. Do
    // not run a table-wide delete and shrink_memory after every micro-batch.
    true
}

async fn persist_queued_batch_to_emergency(
    emergency_outbox: Option<&OutboxStore>,
    batch: Vec<QueuedCritical>,
    counters: &PublisherCounters,
) {
    let events = batch
        .iter()
        .map(|queued| queued.event.clone())
        .collect::<Vec<_>>();
    if let Some(outbox) = emergency_outbox {
        if let Ok(inserted) = outbox.insert_events(&events).await {
            counters
                .emergency
                .fetch_add(inserted as u64, Ordering::Relaxed);
        } else {
            counters
                .failures
                .fetch_add(events.len() as u64, Ordering::Relaxed);
        }
    } else {
        counters
            .failures
            .fetch_add(events.len() as u64, Ordering::Relaxed);
    }
    counters
        .pending_critical
        .fetch_sub(events.len() as u64, Ordering::Relaxed);
}

async fn emergency_outbox_has_pending(
    emergency_outbox: Option<&OutboxStore>,
    scope: Option<&EmergencyReplayScope>,
) -> bool {
    let Some(outbox) = emergency_outbox else {
        return false;
    };
    for priority in [
        EventPriority::Critical,
        EventPriority::Audit,
        EventPriority::Reconciliation,
    ] {
        let pending = match scope {
            Some(scope) => {
                outbox
                    .pending_batch_for_runtime_scope(
                        priority,
                        &scope.runtime_id,
                        &scope.strategy_id,
                        &scope.live_session_id,
                        1,
                        64 * 1024,
                    )
                    .await
            }
            None => outbox.pending_batch(priority, 1, 64 * 1024).await,
        };
        match pending {
            Ok(batch) if !batch.is_empty() => return true,
            Ok(_) => {}
            Err(_) => return true,
        }
    }
    false
}

/// Transfers one bounded emergency batch back to the host-owned outbox. The emergency store is
/// marked committed only after the sync agent replies `LocalAccepted`, so a strategy or host
/// restart cannot lose an event between the two durable stores.
async fn replay_emergency_outbox(
    bus: &dyn LocalEventBus,
    emergency_outbox: Option<&OutboxStore>,
    scope: Option<&EmergencyReplayScope>,
    counters: &PublisherCounters,
    config: &LocalEventPublisherConfig,
) -> bool {
    let Some(outbox) = emergency_outbox else {
        return true;
    };
    for priority in [
        EventPriority::Critical,
        EventPriority::Audit,
        EventPriority::Reconciliation,
    ] {
        let pending = match scope {
            Some(scope) => {
                outbox
                    .pending_batch_for_runtime_scope(
                        priority,
                        &scope.runtime_id,
                        &scope.strategy_id,
                        &scope.live_session_id,
                        config.critical_batch_events,
                        config.critical_batch_bytes,
                    )
                    .await
            }
            None => {
                outbox
                    .pending_batch(
                        priority,
                        config.critical_batch_events,
                        config.critical_batch_bytes,
                    )
                    .await
            }
        };
        let records = match pending {
            Ok(records) => records,
            Err(_) => return false,
        };
        if records.is_empty() {
            continue;
        }
        let record_count = records.len();
        let mut awaiting = Vec::with_capacity(record_count);
        for record in records {
            let event = record.event;
            let subject =
                EventSubject::for_priority(&event.runtime_id, &event.strategy_id, event.priority);
            let started = Instant::now();
            match bus.publish_critical(subject, event.clone()).await {
                Ok(handle) => awaiting.push((started, event, handle)),
                Err(_) => {
                    counters.failures.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            }
        }
        let results = futures::future::join_all(awaiting.into_iter().map(
            |(started, event, handle)| async move {
                let locally_accepted =
                    tokio::time::timeout(config.nats_unavailable_fallback_after, handle.wait())
                        .await
                        .ok()
                        .and_then(Result::ok)
                        .is_some();
                (started, event, locally_accepted)
            },
        ))
        .await;
        let mut accepted = Vec::with_capacity(record_count);
        let mut critical_through: Option<u64> = None;
        for (started, event, locally_accepted) in results {
            if !locally_accepted {
                counters.failures.fetch_add(1, Ordering::Relaxed);
                break;
            }
            counters.accepted.fetch_add(1, Ordering::Relaxed);
            counters
                .last_ack_us
                .store(started.elapsed().as_micros() as u64, Ordering::Relaxed);
            critical_through = match (critical_through, event.critical_sequence) {
                (Some(current), Some(sequence)) => Some(current.max(sequence)),
                (None, Some(sequence)) => Some(sequence),
                (current, None) => current,
            };
            accepted.push(event.event_id);
        }
        if !accepted.is_empty()
            && outbox
                .mark_remote_committed(&accepted, critical_through)
                .await
                .is_err()
        {
            return false;
        }
        if accepted.len() < record_count {
            return false;
        }
    }
    let _ = outbox.prune_committed(0).await;
    !emergency_outbox_has_pending(Some(outbox), scope).await
}

async fn persist_emergency(
    emergency_outbox: Option<&OutboxStore>,
    event: &CloudEvent,
    counters: &PublisherCounters,
) {
    if let Some(outbox) = emergency_outbox {
        if outbox
            .insert_events(std::slice::from_ref(event))
            .await
            .is_ok()
        {
            counters.emergency.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn flush_telemetry(
    bus: &dyn LocalEventBus,
    telemetry: &Mutex<TelemetryQueue>,
    counters: &PublisherCounters,
) {
    let pending = {
        let mut telemetry = telemetry.lock();
        telemetry.bytes = 0;
        std::mem::take(&mut telemetry.entries)
    };
    for (_, queued) in pending {
        if bus.publish_telemetry(queued.subject, queued.event).is_err() {
            counters.failures.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::live_sync::{EventEnvelope, EventKind, LocalDeliveryHandle};
    use async_trait::async_trait;
    use std::sync::atomic::AtomicBool;

    #[derive(Clone, Default)]
    struct RecoveringEventBus {
        inner: InProcessEventBus,
        available: Arc<AtomicBool>,
        pause: Arc<AtomicBool>,
        entered: Arc<Notify>,
        resume: Arc<Notify>,
    }

    #[async_trait]
    impl LocalEventBus for RecoveringEventBus {
        async fn publish_critical(
            &self,
            subject: EventSubject,
            event: CloudEvent,
        ) -> Result<LocalDeliveryHandle, EventBusError> {
            if self.pause.swap(false, Ordering::AcqRel) {
                self.entered.notify_one();
                self.resume.notified().await;
            }
            if !self.available.load(Ordering::Acquire) {
                return Err(EventBusError::Publish(
                    "temporarily unavailable".to_string(),
                ));
            }
            self.inner.publish_critical(subject, event).await
        }

        fn publish_telemetry(
            &self,
            subject: EventSubject,
            event: CloudEvent,
        ) -> Result<(), EventBusError> {
            self.inner.publish_telemetry(subject, event)
        }

        async fn subscribe(
            &self,
            subject: EventSubject,
        ) -> Result<EventSubscription, EventBusError> {
            self.inner.subscribe(subject).await
        }

        async fn acknowledge(
            &self,
            delivery: &EventDelivery,
            acknowledgement: SyncAcknowledgement,
        ) -> Result<(), EventBusError> {
            self.inner.acknowledge(delivery, acknowledgement).await
        }
    }

    fn event(priority: EventPriority, entity: &str, revision: u64) -> CloudEvent {
        event_for_runtime("runtime", priority, entity, revision)
    }

    fn event_for_runtime(
        runtime_id: &str,
        priority: EventPriority,
        entity: &str,
        revision: u64,
    ) -> CloudEvent {
        EventEnvelope::new(
            runtime_id,
            "strategy",
            "session",
            entity,
            revision,
            priority,
            EventKind::InsightProjection,
            serde_json::json!({"revision": revision}),
        )
    }

    #[tokio::test]
    async fn durable_receipt_requires_sqlite_commit_not_transport_delivery() {
        let path = std::env::temp_dir().join(format!("aqe-receipt-{}.db", uuid::Uuid::new_v4()));
        let outbox = OutboxStore::open(&path).await.unwrap();
        let bus = RecoveringEventBus::default();
        bus.pause.store(true, Ordering::Release);
        bus.available.store(true, Ordering::Release);
        let publisher = LocalEventPublisher::spawn(
            Arc::new(bus.clone()),
            Some(outbox.clone()),
            Default::default(),
        );
        let subject = EventSubject::for_priority("runtime", "strategy", EventPriority::Critical);
        let event = event(EventPriority::Critical, "durable-control", 1);
        let id = event.event_id;
        let receipt = publisher
            .queue_durable(subject.clone(), event.clone())
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), receipt)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            id
        );
        // Transport remains paused. A separately reopened DB must already see
        // the committed payload, proving the receipt is not just a RAM ACK.
        let reopened = OutboxStore::open(&path).await.unwrap();
        assert_eq!(reopened.health().await.unwrap().pending_critical, 1);
        assert_eq!(publisher.health().locally_accepted_critical, 0);
        bus.resume.notify_one();
        bus.pause.store(false, Ordering::Release);
        let duplicate = publisher.queue_durable(subject, event).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), duplicate)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            id
        );
        assert_eq!(publisher.critical_sequence_cursor(), 1);
        publisher.shutdown(Duration::from_secs(2)).await;
        assert!(publisher.shutdown_complete());
    }

    #[tokio::test]
    async fn volatile_publishers_cannot_issue_durable_receipts() {
        let publisher = LocalEventPublisher::spawn(
            Arc::new(InProcessEventBus::default()),
            None,
            Default::default(),
        );
        assert!(
            publisher
                .queue_durable(
                    EventSubject::for_priority("runtime", "strategy", EventPriority::Critical),
                    event(EventPriority::Critical, "control", 1),
                )
                .is_err()
        );
        assert_eq!(publisher.pending_critical_count(), 0);
        publisher.shutdown(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn shutdown_waits_for_admission_permits_already_owned_by_producers() {
        let publisher = LocalEventPublisher::spawn(
            Arc::new(InProcessEventBus::default()),
            None,
            Default::default(),
        );
        let permit = publisher
            .critical_sender
            .clone()
            .try_reserve_owned()
            .unwrap();
        publisher.shutdown(Duration::from_millis(30)).await;
        assert!(!publisher.shutdown_complete());
        drop(permit);
        publisher.shutdown(Duration::from_secs(2)).await;
        assert!(publisher.shutdown_complete());
    }

    #[tokio::test]
    async fn bounded_admission_rejects_before_sequence_and_provisional_publication() {
        let bus = Arc::new(InProcessEventBus::default());
        let mut provisional = bus
            .subscribe(EventSubject::provisional("runtime", "strategy"))
            .await
            .unwrap();
        let publisher = LocalEventPublisher::spawn(
            bus,
            None,
            LocalEventPublisherConfig {
                critical_queue_events: 1,
                ..Default::default()
            },
        );
        let subject = EventSubject::for_priority("runtime", "strategy", EventPriority::Critical);
        let first = event(EventPriority::Critical, "first", 1);
        publisher
            .publish_critical(subject.clone(), first.clone())
            .unwrap();
        publisher.publish_critical(subject.clone(), first).unwrap();
        let retry = event(EventPriority::Critical, "retry", 2);
        assert!(
            publisher
                .clone()
                .publish_critical(subject.clone(), retry.clone())
                .is_err()
        );
        assert_eq!(publisher.critical_sequence_cursor(), 1);
        assert_eq!(publisher.pending_critical_count(), 1);
        assert!(provisional.try_recv().is_ok());
        assert!(provisional.try_recv().is_err());
        tokio::time::timeout(Duration::from_secs(2), async {
            while publisher.pending_critical_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        publisher.publish_critical(subject, retry).unwrap();
        assert_eq!(publisher.critical_sequence_cursor(), 2);
        publisher.shutdown(Duration::from_secs(2)).await;
        assert_eq!(publisher.queued_critical_encoded_bytes(), 0);
        assert!(
            !publisher.shutdown_complete(),
            "ignored admission errors cannot become successful sync"
        );
    }

    #[tokio::test]
    async fn bounded_admission_bytes_include_inflight_transport() {
        let bus = RecoveringEventBus::default();
        bus.pause.store(true, Ordering::Release);
        bus.available.store(true, Ordering::Release);
        let first = event(EventPriority::Critical, "first", 1);
        let size = FlatbufferEventCodec::encode(&first).unwrap().len();
        let publisher = LocalEventPublisher::spawn(
            Arc::new(bus.clone()),
            None,
            LocalEventPublisherConfig {
                critical_queue_bytes: size,
                ..Default::default()
            },
        );
        let subject = EventSubject::for_priority("runtime", "strategy", EventPriority::Critical);
        publisher.publish_critical(subject.clone(), first).unwrap();
        tokio::time::timeout(Duration::from_secs(2), bus.entered.notified())
            .await
            .unwrap();
        assert_eq!(publisher.queued_critical_encoded_bytes(), size as u64);
        assert!(
            publisher
                .publish_critical(subject, event(EventPriority::Critical, "next", 2))
                .is_err()
        );
        assert_eq!(publisher.critical_sequence_cursor(), 1);
        bus.resume.notify_one();
        publisher.shutdown(Duration::from_secs(2)).await;
        assert_eq!(publisher.queued_critical_encoded_bytes(), 0);
    }

    #[tokio::test]
    async fn shutdown_deadline_does_not_claim_pending_transport_is_complete() {
        let bus = RecoveringEventBus::default();
        bus.pause.store(true, Ordering::Release);
        bus.available.store(true, Ordering::Release);
        let publisher = LocalEventPublisher::spawn(Arc::new(bus.clone()), None, Default::default());
        publisher
            .publish_critical(
                EventSubject::for_priority("runtime", "strategy", EventPriority::Critical),
                event(EventPriority::Critical, "strategy-lifecycle", 1),
            )
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), bus.entered.notified())
            .await
            .unwrap();
        publisher.shutdown(Duration::from_millis(10)).await;
        assert!(!publisher.shutdown_complete());
        assert_eq!(publisher.pending_critical_count(), 1);
        assert!(publisher.queued_critical_encoded_bytes() > 0);
        bus.resume.notify_one();
        publisher.shutdown(Duration::from_secs(1)).await;
        assert!(publisher.shutdown_complete());
        assert_eq!(publisher.pending_critical_count(), 0);
        assert_eq!(publisher.queued_critical_encoded_bytes(), 0);
    }

    #[tokio::test]
    async fn history_and_reconciliation_are_durable_before_transport_and_replay_after_restart() {
        let path =
            std::env::temp_dir().join(format!("publisher-background-{}.db", uuid::Uuid::new_v4()));
        let outbox = OutboxStore::open(&path).await.unwrap();
        let bus = RecoveringEventBus::default();
        bus.pause.store(true, Ordering::Release);
        let publisher = LocalEventPublisher::spawn(
            Arc::new(bus.clone()),
            Some(outbox.clone()),
            Default::default(),
        );
        for priority in [EventPriority::Audit, EventPriority::Reconciliation] {
            publisher
                .publish_critical(
                    EventSubject::for_priority("runtime", "strategy", priority),
                    event(priority, "background", 1),
                )
                .unwrap();
        }
        tokio::time::timeout(Duration::from_secs(2), bus.entered.notified())
            .await
            .unwrap();
        assert_eq!(publisher.pending_critical_count(), 2);
        assert!(publisher.queued_critical_encoded_bytes() > 0);
        assert_eq!(publisher.critical_sequence_cursor(), 0);
        for priority in [EventPriority::Audit, EventPriority::Reconciliation] {
            let rows = outbox.pending_batch(priority, 10, 65536).await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].event.critical_sequence, None);
        }
        bus.resume.notify_one();
        publisher.shutdown(Duration::from_secs(3)).await;
        assert!(publisher.shutdown_complete());
        assert_eq!(publisher.queued_critical_encoded_bytes(), 0);
        drop(publisher);
        drop(outbox);

        let outbox = OutboxStore::open(&path).await.unwrap();
        let bus = Arc::new(InProcessEventBus::default());
        let mut history = bus
            .subscribe(EventSubject::for_priority(
                "runtime",
                "strategy",
                EventPriority::Audit,
            ))
            .await
            .unwrap();
        let mut reconciliation = bus
            .subscribe(EventSubject::for_priority(
                "runtime",
                "strategy",
                EventPriority::Reconciliation,
            ))
            .await
            .unwrap();
        let publisher = LocalEventPublisher::spawn(bus, Some(outbox), Default::default());
        tokio::time::timeout(Duration::from_secs(3), async {
            assert_eq!(
                history.recv().await.unwrap().event.priority,
                EventPriority::Audit
            );
            assert_eq!(
                reconciliation.recv().await.unwrap().event.priority,
                EventPriority::Reconciliation
            );
        })
        .await
        .unwrap();
        publisher.shutdown(Duration::from_secs(3)).await;
        assert!(publisher.shutdown_complete());
        assert_eq!(publisher.critical_sequence_cursor(), 0);
        drop(publisher);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn background_identity_conflict_is_isolated_without_blocking_healthy_history() {
        let path = std::env::temp_dir().join(format!(
            "publisher-background-conflict-{}.db",
            uuid::Uuid::new_v4()
        ));
        let outbox = OutboxStore::open(&path).await.unwrap();
        let original = event(EventPriority::Audit, "original", 1);
        outbox
            .insert_events(std::slice::from_ref(&original))
            .await
            .unwrap();
        let mut conflict = original.clone();
        conflict.entity_id = "changed-identity".into();
        let bus = Arc::new(InProcessEventBus::default());
        let subject = EventSubject::for_priority("runtime", "strategy", EventPriority::Audit);
        let counters = Arc::new(PublisherCounters::default());
        let batch = [conflict, event(EventPriority::Audit, "healthy", 2)]
            .into_iter()
            .map(|event| {
                let bytes = FlatbufferEventCodec::encode(&event).unwrap().len() as u64;
                counters.pending_critical.fetch_add(1, Ordering::Relaxed);
                counters
                    .queued_encoded_bytes
                    .fetch_add(bytes, Ordering::Relaxed);
                QueuedCritical {
                    subject: subject.clone(),
                    event,
                    durable_receipt: None,
                    _memory: QueuedMemory {
                        counters: counters.clone(),
                        bytes,
                    },
                }
            })
            .collect();
        let batch = tokio::time::timeout(
            Duration::from_secs(3),
            prepare_durable_batch(
                bus.as_ref(),
                Some(&outbox),
                batch,
                &counters,
                &AtomicU64::new(0),
            ),
        )
        .await
        .unwrap();
        assert_eq!(outbox.health().await.unwrap().dead_letters, 1);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].event.entity_id, "healthy");
        assert_eq!(counters.pending_critical.load(Ordering::Relaxed), 1);
        drop(batch);
        assert_eq!(counters.queued_encoded_bytes.load(Ordering::Relaxed), 0);
        drop(outbox);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn emergency_replay_ignores_obsolete_runtime_generations() {
        let path = std::env::temp_dir().join(format!(
            "aqe-publisher-runtime-scoped-replay-{}.db",
            uuid::Uuid::new_v4()
        ));
        let outbox = OutboxStore::open(&path).await.unwrap();
        let mut obsolete =
            event_for_runtime("obsolete-runtime", EventPriority::Critical, "obsolete", 1);
        obsolete.critical_sequence = Some(1);
        let mut current =
            event_for_runtime("current-runtime", EventPriority::Critical, "current", 1);
        current.critical_sequence = Some(1);
        outbox.insert_events(&[obsolete, current]).await.unwrap();

        let bus = InProcessEventBus::default();
        let subject =
            EventSubject::for_priority("current-runtime", "strategy", EventPriority::Critical);
        let mut subscription = bus.subscribe(subject).await.unwrap();
        let scope = EmergencyReplayScope {
            runtime_id: "current-runtime".to_string(),
            strategy_id: "strategy".to_string(),
            live_session_id: "session".to_string(),
        };
        let replayed = replay_emergency_outbox(
            &bus,
            Some(&outbox),
            Some(&scope),
            &PublisherCounters::default(),
            &LocalEventPublisherConfig::default(),
        )
        .await;

        assert!(replayed);
        let delivery = subscription.recv().await.unwrap();
        assert_eq!(delivery.event.entity_id, "current");
        assert!(
            outbox
                .pending_batch_for_runtime_scope(
                    EventPriority::Critical,
                    "current-runtime",
                    "strategy",
                    "session",
                    1,
                    64 * 1024,
                )
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            outbox
                .pending_batch_for_runtime_scope(
                    EventPriority::Critical,
                    "obsolete-runtime",
                    "strategy",
                    "session",
                    1,
                    64 * 1024,
                )
                .await
                .unwrap()
                .len(),
            1
        );
        drop(outbox);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn critical_sequences_are_assigned_without_blocking_the_caller() {
        let bus = InProcessEventBus::default();
        let subject = EventSubject::for_priority("runtime", "strategy", EventPriority::Critical);
        let mut subscription = bus.subscribe(subject.clone()).await.unwrap();
        let publisher =
            LocalEventPublisher::spawn(Arc::new(bus), None, LocalEventPublisherConfig::default());
        publisher
            .publish_critical(subject.clone(), event(EventPriority::Critical, "one", 1))
            .unwrap();
        publisher
            .publish_critical(subject, event(EventPriority::Critical, "two", 1))
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while publisher.health().locally_accepted_critical < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(publisher.health().pending_critical, 0);
        let first = subscription.recv().await.unwrap();
        let second = subscription.recv().await.unwrap();
        assert_eq!(first.event.critical_sequence, Some(1));
        assert_eq!(second.event.critical_sequence, Some(2));
    }

    #[tokio::test]
    async fn duplicate_critical_identity_does_not_consume_another_sequence() {
        let bus = InProcessEventBus::default();
        let subject = EventSubject::for_priority("runtime", "strategy", EventPriority::Critical);
        let mut subscription = bus.subscribe(subject.clone()).await.unwrap();
        let publisher =
            LocalEventPublisher::spawn(Arc::new(bus), None, LocalEventPublisherConfig::default());
        let duplicate = event(EventPriority::Critical, "broker-order:123", 7);

        publisher
            .publish_critical(subject.clone(), duplicate.clone())
            .unwrap();
        publisher.publish_critical(subject, duplicate).unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            while publisher.health().locally_accepted_critical < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(publisher.critical_sequence_cursor(), 1);
        assert_eq!(publisher.health().deduplicated_critical, 1);
        assert_eq!(
            subscription.recv().await.unwrap().event.critical_sequence,
            Some(1)
        );
        assert!(subscription.try_recv().is_err());
    }

    #[tokio::test]
    async fn standalone_identity_deduplication_has_a_bounded_window() {
        let subject = EventSubject::for_priority("runtime", "strategy", EventPriority::Critical);
        let publisher = LocalEventPublisher::spawn(
            Arc::new(InProcessEventBus::default()),
            None,
            LocalEventPublisherConfig {
                critical_queue_events: 2,
                volatile_identity_events: 2,
                ..Default::default()
            },
        );
        let first = event(EventPriority::Critical, "first", 1);
        publisher
            .publish_critical(subject.clone(), first.clone())
            .unwrap();
        publisher
            .publish_critical(subject.clone(), event(EventPriority::Critical, "second", 1))
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while publisher.health().pending_critical != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        publisher
            .publish_critical(subject.clone(), event(EventPriority::Critical, "third", 1))
            .unwrap();
        publisher.publish_critical(subject, first).unwrap();

        assert_eq!(publisher.critical_sequence_cursor(), 4);
        assert_eq!(publisher.health().volatile_identity_evictions, 2);
        assert_eq!(
            publisher
                .critical_event_ids
                .as_ref()
                .unwrap()
                .lock()
                .ids
                .len(),
            2
        );
        publisher.shutdown(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn durable_publisher_restarts_without_an_in_memory_identity_set_or_sequence_gaps() {
        let path =
            std::env::temp_dir().join(format!("durable-publisher-{}.db", uuid::Uuid::new_v4()));
        let outbox = OutboxStore::open(&path).await.unwrap();
        let bus = Arc::new(InProcessEventBus::default());
        let subject = EventSubject::for_priority("runtime", "strategy", EventPriority::Critical);
        let provisional = EventSubject::provisional("runtime", "strategy");
        let mut deliveries = bus.subscribe(subject.clone()).await.unwrap();
        let mut local_ui = bus.subscribe(provisional).await.unwrap();
        let publisher =
            LocalEventPublisher::spawn(bus.clone(), Some(outbox.clone()), Default::default());
        assert!(publisher.critical_event_ids.is_none());
        let original = event(EventPriority::Critical, "first", 1);
        publisher
            .publish_critical(subject.clone(), original.clone())
            .unwrap();
        publisher
            .publish_critical(subject.clone(), original.clone())
            .unwrap();
        publisher.shutdown(Duration::from_secs(5)).await;
        assert!(publisher.counters.finished.load(Ordering::Acquire));
        assert_eq!(publisher.critical_sequence_cursor(), 1);
        assert_eq!(publisher.health().deduplicated_critical, 1);
        assert_eq!(
            deliveries.recv().await.unwrap().event.critical_sequence,
            Some(1)
        );
        assert!(deliveries.try_recv().is_err());
        assert_eq!(
            local_ui.recv().await.unwrap().event.critical_sequence,
            Some(1)
        );
        assert!(local_ui.try_recv().is_err());
        outbox.prune_committed(0).await.unwrap();
        drop(publisher);
        drop(outbox);

        let outbox = OutboxStore::open(&path).await.unwrap();
        let initial = outbox
            .critical_sequence_cursor_for_runtime_scope("runtime", "strategy", "session")
            .await
            .unwrap();
        let publisher = LocalEventPublisher::spawn_with_initial_sequence(
            bus,
            Some(outbox.clone()),
            Default::default(),
            initial,
        );
        let mut conflict = original.clone();
        conflict.payload = serde_json::json!({"invalid": "changed immutable body"});
        publisher
            .publish_critical(subject.clone(), original)
            .unwrap();
        publisher
            .publish_critical(subject.clone(), conflict)
            .unwrap();
        publisher
            .publish_critical(subject, event(EventPriority::Critical, "second", 2))
            .unwrap();
        publisher.shutdown(Duration::from_secs(5)).await;
        assert!(publisher.counters.finished.load(Ordering::Acquire));
        assert_eq!(publisher.critical_sequence_cursor(), 2);
        assert_eq!(publisher.health().pending_critical, 0);
        assert_eq!(outbox.health().await.unwrap().dead_letters, 1);
        assert_eq!(
            deliveries.recv().await.unwrap().event.critical_sequence,
            Some(2)
        );
        assert!(deliveries.try_recv().is_err());
        assert_eq!(
            local_ui.recv().await.unwrap().event.critical_sequence,
            Some(2)
        );
        assert!(local_ui.try_recv().is_err());
    }

    #[tokio::test]
    async fn durable_publisher_duplicate_burst_keeps_only_unique_sequences() {
        let path = std::env::temp_dir().join(format!("durable-burst-{}.db", uuid::Uuid::new_v4()));
        let outbox = OutboxStore::open(&path).await.unwrap();
        let publisher = LocalEventPublisher::spawn(
            Arc::new(InProcessEventBus::default()),
            Some(outbox.clone()),
            Default::default(),
        );
        let subject = EventSubject::for_priority("runtime", "strategy", EventPriority::Critical);
        for revision in 1..=1000 {
            let event = event(EventPriority::Critical, "burst", revision);
            publisher
                .publish_critical(subject.clone(), event.clone())
                .unwrap();
            publisher.publish_critical(subject.clone(), event).unwrap();
        }
        publisher.shutdown(Duration::from_secs(15)).await;
        assert!(publisher.counters.finished.load(Ordering::Acquire));
        assert!(publisher.critical_event_ids.is_none());
        assert_eq!(publisher.critical_sequence_cursor(), 1000);
        assert_eq!(publisher.health().deduplicated_critical, 1000);
        assert_eq!(publisher.health().locally_accepted_critical, 1000);
        assert_eq!(publisher.health().publish_failures, 0);
        assert_eq!(outbox.health().await.unwrap().pending_critical, 0);
    }

    #[tokio::test]
    async fn durable_reservation_before_publication_replays_after_restart() {
        let path =
            std::env::temp_dir().join(format!("publisher-before-send-{}.db", uuid::Uuid::new_v4()));
        let outbox = OutboxStore::open(&path).await.unwrap();
        let first = event(EventPriority::Critical, "first", 1);
        // Simulate process exit after the atomic SQLite commit but before bus publication.
        outbox
            .reserve_publisher_events(&[first.clone()], 0)
            .await
            .unwrap();
        drop(outbox);
        let outbox = OutboxStore::open(&path).await.unwrap();
        let bus = Arc::new(InProcessEventBus::default());
        let subject = EventSubject::for_priority("runtime", "strategy", EventPriority::Critical);
        let mut subscription = bus.subscribe(subject.clone()).await.unwrap();
        let mut provisional = bus
            .subscribe(EventSubject::provisional("runtime", "strategy"))
            .await
            .unwrap();
        let publisher = LocalEventPublisher::spawn_with_initial_sequence(
            bus,
            Some(outbox.clone()),
            Default::default(),
            1,
        );
        publisher.publish_critical(subject.clone(), first).unwrap();
        publisher
            .publish_critical(subject, event(EventPriority::Critical, "next", 2))
            .unwrap();
        publisher.shutdown(Duration::from_secs(5)).await;
        assert!(publisher.counters.finished.load(Ordering::Acquire));
        assert_eq!(
            subscription.recv().await.unwrap().event.critical_sequence,
            Some(1)
        );
        assert_eq!(
            subscription.recv().await.unwrap().event.critical_sequence,
            Some(2)
        );
        assert!(subscription.try_recv().is_err());
        assert_eq!(
            provisional.recv().await.unwrap().event.critical_sequence,
            Some(2)
        );
        assert!(provisional.try_recv().is_err());
        assert_eq!(publisher.health().deduplicated_critical, 1);
        assert_eq!(outbox.health().await.unwrap().pending_critical, 0);
    }

    #[tokio::test]
    async fn telemetry_is_latest_wins_per_entity() {
        let bus = InProcessEventBus::default();
        let subject = EventSubject::for_priority("runtime", "strategy", EventPriority::Telemetry);
        let mut subscription = bus.subscribe(subject.clone()).await.unwrap();
        let publisher = LocalEventPublisher::spawn(
            Arc::new(bus),
            None,
            LocalEventPublisherConfig {
                telemetry_flush_delay: Duration::from_millis(5),
                ..Default::default()
            },
        );
        publisher
            .publish_telemetry(
                subject.clone(),
                event(EventPriority::Telemetry, "account", 1),
            )
            .unwrap();
        publisher
            .publish_telemetry(subject, event(EventPriority::Telemetry, "account", 2))
            .unwrap();
        let delivery = tokio::time::timeout(Duration::from_secs(1), subscription.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delivery.event.entity_revision, 2);
    }

    #[tokio::test]
    async fn telemetry_cardinality_and_bytes_are_bounded_with_replacement_headroom() {
        let subject = EventSubject::for_priority("runtime", "strategy", EventPriority::Telemetry);
        let publisher = LocalEventPublisher::spawn(
            Arc::new(InProcessEventBus::default()),
            None,
            LocalEventPublisherConfig {
                telemetry_flush_delay: Duration::from_secs(3_600),
                telemetry_entities: 2,
                telemetry_bytes: 64 * 1024,
                ..Default::default()
            },
        );
        publisher
            .publish_telemetry(
                subject.clone(),
                event(EventPriority::Telemetry, "account", 1),
            )
            .unwrap();
        publisher
            .publish_telemetry(
                subject.clone(),
                event(EventPriority::Telemetry, "metrics", 1),
            )
            .unwrap();
        assert!(matches!(
            publisher.publish_telemetry(
                subject.clone(),
                event(EventPriority::Telemetry, "unbounded-cardinality", 1),
            ),
            Err(EventBusError::PublisherCapacity)
        ));
        publisher
            .publish_telemetry(subject, event(EventPriority::Telemetry, "account", 2))
            .unwrap();

        let health = publisher.health();
        assert_eq!(health.pending_telemetry_entities, 2);
        assert!(health.queued_telemetry_encoded_bytes > 0);
        assert_eq!(health.dropped_telemetry, 1);

        publisher.shutdown(Duration::from_secs(1)).await;
        let drained = publisher.health();
        assert_eq!(drained.pending_telemetry_entities, 0);
        assert_eq!(drained.queued_telemetry_encoded_bytes, 0);
    }

    #[tokio::test]
    async fn reconciliation_does_not_create_a_gap_in_the_lifecycle_cursor() {
        let publisher = LocalEventPublisher::spawn(
            Arc::new(InProcessEventBus::default()),
            None,
            LocalEventPublisherConfig::default(),
        );
        let reconciliation_subject =
            EventSubject::for_priority("runtime", "strategy", EventPriority::Reconciliation);
        let critical_subject =
            EventSubject::for_priority("runtime", "strategy", EventPriority::Critical);

        publisher
            .publish_critical(
                reconciliation_subject,
                event(EventPriority::Reconciliation, "snapshot", 1),
            )
            .unwrap();
        assert_eq!(publisher.critical_sequence_cursor(), 0);

        publisher
            .publish_critical(
                critical_subject,
                event(EventPriority::Critical, "insight", 1),
            )
            .unwrap();
        assert_eq!(publisher.critical_sequence_cursor(), 1);
    }

    #[tokio::test]
    async fn ten_thousand_critical_events_can_be_enqueued_without_a_bounded_channel_drop() {
        let publisher = LocalEventPublisher::spawn(
            Arc::new(InProcessEventBus::default()),
            None,
            LocalEventPublisherConfig::default(),
        );
        let subject = EventSubject::for_priority("runtime", "strategy", EventPriority::Critical);
        for revision in 1..=10_000 {
            publisher
                .publish_critical(
                    subject.clone(),
                    event(EventPriority::Critical, "burst", revision),
                )
                .unwrap();
        }
        assert_eq!(publisher.critical_sequence_cursor(), 10_000);
        tokio::time::timeout(Duration::from_secs(10), async {
            while publisher.health().pending_critical != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let health = publisher.health();
        assert_eq!(health.publish_failures, 0);
        assert_eq!(health.locally_accepted_critical, 10_000);
    }

    #[tokio::test]
    async fn oversized_singleton_is_dead_lettered_once_without_publication_retry() {
        let path = std::env::temp_dir().join(format!(
            "aqe-publisher-oversized-{}.db",
            uuid::Uuid::new_v4()
        ));
        let outbox = OutboxStore::open(&path).await.unwrap();
        let publisher = LocalEventPublisher::spawn(
            Arc::new(InProcessEventBus::default()),
            Some(outbox.clone()),
            LocalEventPublisherConfig::default(),
        );
        let subject =
            EventSubject::for_priority("runtime", "strategy", EventPriority::Reconciliation);
        let mut oversized = event(EventPriority::Reconciliation, "oversized", 1);
        oversized.payload = serde_json::json!({
            "blob": "x".repeat(crate::core::live_sync::LOCAL_EVENT_HARD_LIMIT_BYTES + 1024)
        });

        assert!(matches!(
            publisher.publish_critical(subject.clone(), oversized.clone()),
            Err(EventBusError::PayloadTooLarge { .. })
        ));
        assert!(matches!(
            publisher.publish_critical(subject, oversized),
            Err(EventBusError::PayloadTooLarge { .. })
        ));
        tokio::time::timeout(Duration::from_secs(2), async {
            while outbox.health().await.unwrap().dead_letters == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        assert_eq!(outbox.health().await.unwrap().dead_letters, 1);
        drop(publisher);
        drop(outbox);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn emergency_events_replay_after_the_local_bus_recovers() {
        let path = std::env::temp_dir().join(format!(
            "aqe-publisher-emergency-replay-{}.db",
            uuid::Uuid::new_v4()
        ));
        let outbox = OutboxStore::open(&path).await.unwrap();
        let bus = RecoveringEventBus::default();
        let subject = EventSubject::for_priority("runtime", "strategy", EventPriority::Critical);
        let mut subscription = bus.subscribe(subject.clone()).await.unwrap();
        let publisher = LocalEventPublisher::spawn(
            Arc::new(bus.clone()),
            Some(outbox.clone()),
            LocalEventPublisherConfig {
                nats_unavailable_fallback_after: Duration::from_millis(10),
                ..Default::default()
            },
        );
        publisher
            .publish_critical(subject, event(EventPriority::Critical, "recoverable", 1))
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while publisher.health().emergency_outbox_writes == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        bus.available.store(true, Ordering::Release);
        let delivery = tokio::time::timeout(Duration::from_secs(3), subscription.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delivery.event.entity_id, "recoverable");
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let pending = outbox
                    .pending_batch(EventPriority::Critical, 1, 64 * 1024)
                    .await
                    .unwrap();
                if pending.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        publisher.shutdown(Duration::from_secs(1)).await;
        drop(outbox);
        let _ = std::fs::remove_file(path);
    }
}
