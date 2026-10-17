use super::{
    CloudEvent, EventId, EventSubject, FlatbufferEventCodec, LOCAL_EVENT_HARD_LIMIT_BYTES,
    SyncAcknowledgement,
};
use async_trait::async_trait;
use futures::StreamExt;
use parking_lot::Mutex;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

const LOCAL_ACK_TIMEOUT: Duration = Duration::from_secs(2);
// async-nats defaults every subscription to 65,536 buffered messages. AQS subscriptions either
// drain directly into SQLite or carry latest-value telemetry, so a much smaller burst allowance
// is sufficient. Critical publishers fall back to the durable emergency outbox when an
// acknowledgement is missed; bounding this client-side queue therefore protects memory without
// weakening lifecycle durability.
pub const NATS_SUBSCRIPTION_CAPACITY: usize = 4_096;

#[derive(Debug, Clone)]
pub enum EventBusError {
    Connect(String),
    Publish(String),
    PublisherCapacity,
    Subscribe(String),
    Request(String),
    Codec(String),
    PayloadTooLarge {
        event_id: EventId,
        encoded_bytes: usize,
        max_bytes: usize,
    },
    AcknowledgementTimeout(EventId),
    AcknowledgementClosed(EventId),
}

impl fmt::Display for EventBusError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(message) => write!(formatter, "event bus connection failed: {message}"),
            Self::Publish(message) => write!(formatter, "event bus publish failed: {message}"),
            Self::PublisherCapacity => write!(
                formatter,
                "local publisher capacity unavailable; event was not accepted"
            ),
            Self::Subscribe(message) => write!(formatter, "event bus subscribe failed: {message}"),
            Self::Request(message) => write!(formatter, "event bus request failed: {message}"),
            Self::Codec(message) => write!(formatter, "event bus codec failed: {message}"),
            Self::PayloadTooLarge {
                event_id,
                encoded_bytes,
                max_bytes,
            } => write!(
                formatter,
                "event {event_id} is {encoded_bytes} bytes, exceeding the {max_bytes}-byte local application limit"
            ),
            Self::AcknowledgementTimeout(event_id) => {
                write!(formatter, "local acknowledgement timed out for {event_id}")
            }
            Self::AcknowledgementClosed(event_id) => {
                write!(formatter, "local acknowledgement closed for {event_id}")
            }
        }
    }
}

impl std::error::Error for EventBusError {}

#[derive(Clone, Debug)]
pub struct EventDelivery {
    pub subject: EventSubject,
    pub event: CloudEvent,
    reply_to: Option<String>,
}

pub struct EventSubscription {
    receiver: mpsc::Receiver<EventDelivery>,
}

impl EventSubscription {
    pub async fn recv(&mut self) -> Option<EventDelivery> {
        self.receiver.recv().await
    }

    pub fn try_recv(&mut self) -> Result<EventDelivery, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }
}

pub struct LocalDeliveryHandle {
    pub event_id: EventId,
    acknowledgement: oneshot::Receiver<Result<SyncAcknowledgement, EventBusError>>,
    cleanup: DeliveryCleanup,
}

/// Cancelling a publication must release its reply inbox, even if the remote
/// subscriber never answers. Otherwise each retry leaks a task/subscription.
#[derive(Default)]
struct DeliveryCleanup(Option<Box<dyn FnOnce() + Send>>);

impl Drop for DeliveryCleanup {
    fn drop(&mut self) {
        if let Some(cleanup) = self.0.take() {
            cleanup();
        }
    }
}

impl LocalDeliveryHandle {
    pub async fn wait(self) -> Result<SyncAcknowledgement, EventBusError> {
        let Self {
            event_id,
            acknowledgement,
            cleanup,
        } = self;
        let result = match tokio::time::timeout(LOCAL_ACK_TIMEOUT, acknowledgement).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(EventBusError::AcknowledgementClosed(event_id)),
            Err(_) => Err(EventBusError::AcknowledgementTimeout(event_id)),
        };
        drop(cleanup);
        result
    }
}

#[async_trait]
pub trait LocalEventBus: Send + Sync {
    async fn publish_critical(
        &self,
        subject: EventSubject,
        event: CloudEvent,
    ) -> Result<LocalDeliveryHandle, EventBusError>;

    fn publish_telemetry(
        &self,
        subject: EventSubject,
        event: CloudEvent,
    ) -> Result<(), EventBusError>;

    async fn subscribe(&self, subject: EventSubject) -> Result<EventSubscription, EventBusError>;

    async fn request_event(
        &self,
        _subject: EventSubject,
        _event: CloudEvent,
        _timeout: Duration,
    ) -> Result<CloudEvent, EventBusError> {
        Err(EventBusError::Request(
            "event request/reply is not supported by this local event bus".to_string(),
        ))
    }

    async fn respond_event(
        &self,
        _delivery: &EventDelivery,
        _event: CloudEvent,
    ) -> Result<(), EventBusError> {
        Err(EventBusError::Request(
            "event request/reply is not supported by this local event bus".to_string(),
        ))
    }

    async fn acknowledge(
        &self,
        delivery: &EventDelivery,
        acknowledgement: SyncAcknowledgement,
    ) -> Result<(), EventBusError>;
}

#[derive(Clone, Default)]
pub struct InProcessEventBus {
    subscribers: Arc<Mutex<HashMap<String, Vec<mpsc::Sender<EventDelivery>>>>>,
    request_replies: Arc<Mutex<HashMap<String, oneshot::Sender<CloudEvent>>>>,
    critical_replies:
        Arc<Mutex<HashMap<String, oneshot::Sender<Result<SyncAcknowledgement, EventBusError>>>>>,
    require_acknowledgement: bool,
}

impl InProcessEventBus {
    /// Use with a durable sync consumer. Default remains ephemeral for users who
    /// only need local notifications; this mode never acknowledges before the
    /// consumer explicitly confirms its SQLite commit.
    pub fn acknowledged() -> Self {
        Self {
            require_acknowledgement: true,
            ..Self::default()
        }
    }

    fn fan_out(&self, subject: EventSubject, event: CloudEvent) {
        let delivery = EventDelivery {
            subject: subject.clone(),
            event,
            reply_to: None,
        };
        self.deliver(delivery);
    }

    fn deliver(&self, delivery: EventDelivery) {
        let mut subscribers = self.subscribers.lock();
        let Some(targets) = subscribers.get_mut(delivery.subject.as_str()) else {
            return;
        };
        targets.retain(|target| match target.try_send(delivery.clone()) {
            Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => true,
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        });
    }
}

#[async_trait]
impl LocalEventBus for InProcessEventBus {
    async fn publish_critical(
        &self,
        subject: EventSubject,
        event: CloudEvent,
    ) -> Result<LocalDeliveryHandle, EventBusError> {
        validate_event_size(&event)?;
        let event_id = event.event_id;
        let (sender, receiver) = oneshot::channel();
        let cleanup = if self.require_acknowledgement {
            let inbox = format!("_ACK.{}", uuid::Uuid::new_v4());
            let replies = self.critical_replies.clone();
            {
                let mut pending = replies.lock();
                if pending.len() >= NATS_SUBSCRIPTION_CAPACITY {
                    return Err(EventBusError::Publish(
                        "in-process acknowledgement capacity reached".into(),
                    ));
                }
                pending.insert(inbox.clone(), sender);
            }
            self.deliver(EventDelivery {
                subject,
                event,
                reply_to: Some(inbox.clone()),
            });
            DeliveryCleanup(Some(Box::new(move || {
                replies.lock().remove(&inbox);
            })))
        } else {
            self.fan_out(subject, event);
            let _ = sender.send(Ok(SyncAcknowledgement::LocalAccepted {
                event_ids: vec![event_id],
            }));
            DeliveryCleanup::default()
        };
        Ok(LocalDeliveryHandle {
            event_id,
            acknowledgement: receiver,
            cleanup,
        })
    }

    fn publish_telemetry(
        &self,
        subject: EventSubject,
        event: CloudEvent,
    ) -> Result<(), EventBusError> {
        validate_event_size(&event)?;
        self.fan_out(subject, event);
        Ok(())
    }

    async fn subscribe(&self, subject: EventSubject) -> Result<EventSubscription, EventBusError> {
        let (sender, receiver) = mpsc::channel(1_024);
        self.subscribers
            .lock()
            .entry(subject.to_string())
            .or_default()
            .push(sender);
        Ok(EventSubscription { receiver })
    }

    async fn request_event(
        &self,
        subject: EventSubject,
        event: CloudEvent,
        timeout: Duration,
    ) -> Result<CloudEvent, EventBusError> {
        validate_event_size(&event)?;
        let inbox = format!("_INBOX.{}", uuid::Uuid::new_v4());
        let (sender, receiver) = oneshot::channel();
        self.request_replies.lock().insert(inbox.clone(), sender);
        let delivery = EventDelivery {
            subject: subject.clone(),
            event,
            reply_to: Some(inbox.clone()),
        };
        self.deliver(delivery);
        let replies = self.request_replies.clone();
        let cleanup_inbox = inbox.clone();
        let _cleanup = DeliveryCleanup(Some(Box::new(move || {
            replies.lock().remove(&cleanup_inbox);
        })));
        let result = tokio::time::timeout(timeout, receiver)
            .await
            .map_err(|_| EventBusError::Request("request timed out".to_string()))?
            .map_err(|_| EventBusError::Request("request response closed".to_string()));
        self.request_replies.lock().remove(&inbox);
        result
    }

    async fn respond_event(
        &self,
        delivery: &EventDelivery,
        event: CloudEvent,
    ) -> Result<(), EventBusError> {
        let Some(reply_to) = delivery.reply_to.as_ref() else {
            return Ok(());
        };
        if let Some(sender) = self.request_replies.lock().remove(reply_to) {
            sender
                .send(event)
                .map_err(|_| EventBusError::Request("request response closed".to_string()))?;
        }
        Ok(())
    }

    async fn acknowledge(
        &self,
        delivery: &EventDelivery,
        acknowledgement: SyncAcknowledgement,
    ) -> Result<(), EventBusError> {
        if let Some(inbox) = delivery.reply_to.as_ref() {
            if let Some(sender) = self.critical_replies.lock().remove(inbox) {
                let _ = sender.send(Ok(acknowledgement));
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct NatsEventBus {
    client: async_nats::Client,
}

impl NatsEventBus {
    pub async fn connect(url: &str, token: &str) -> Result<Self, EventBusError> {
        let client = async_nats::ConnectOptions::with_token(token.to_string())
            .subscription_capacity(NATS_SUBSCRIPTION_CAPACITY)
            .connect(url)
            .await
            .map_err(|error| EventBusError::Connect(error.to_string()))?;
        Ok(Self { client })
    }

    pub fn from_client(client: async_nats::Client) -> Self {
        Self { client }
    }

    /// Sends a small control-plane request over the private loopback NATS server. This is kept
    /// separate from lifecycle publication and is used for runtime-host-owned local queries.
    pub async fn request_json<Request, Response>(
        &self,
        subject: EventSubject,
        request: &Request,
        timeout: Duration,
    ) -> Result<Response, EventBusError>
    where
        Request: Serialize + ?Sized,
        Response: DeserializeOwned,
    {
        let payload =
            serde_json::to_vec(request).map_err(|error| EventBusError::Codec(error.to_string()))?;
        let response = tokio::time::timeout(
            timeout,
            self.client.request(subject.to_string(), payload.into()),
        )
        .await
        .map_err(|_| EventBusError::Request("request timed out".to_string()))?
        .map_err(|error| EventBusError::Request(error.to_string()))?;
        serde_json::from_slice(&response.payload)
            .map_err(|error| EventBusError::Codec(error.to_string()))
    }
}

#[async_trait]
impl LocalEventBus for NatsEventBus {
    async fn publish_critical(
        &self,
        subject: EventSubject,
        mut event: CloudEvent,
    ) -> Result<LocalDeliveryHandle, EventBusError> {
        event.latency.mark_local_bus_published();
        let event_id = event.event_id;
        let payload = FlatbufferEventCodec::encode(&event)
            .map_err(|error| EventBusError::Codec(error.to_string()))?;
        validate_encoded_size(event_id, payload.len())?;
        let inbox = self.client.new_inbox();
        let mut replies = self
            .client
            .subscribe(inbox.clone())
            .await
            .map_err(|error| EventBusError::Subscribe(error.to_string()))?;
        self.client
            .publish_with_reply(subject.to_string(), inbox, payload.into())
            .await
            .map_err(|error| EventBusError::Publish(error.to_string()))?;

        let (sender, receiver) = oneshot::channel();
        let reply_task = tokio::spawn(async move {
            let result = match replies.next().await {
                Some(message) => serde_json::from_slice::<SyncAcknowledgement>(&message.payload)
                    .map_err(|error| EventBusError::Codec(error.to_string())),
                None => Err(EventBusError::AcknowledgementClosed(event_id)),
            };
            let _ = sender.send(result);
        });

        Ok(LocalDeliveryHandle {
            event_id,
            acknowledgement: receiver,
            cleanup: DeliveryCleanup(Some(Box::new(move || reply_task.abort()))),
        })
    }

    fn publish_telemetry(
        &self,
        subject: EventSubject,
        mut event: CloudEvent,
    ) -> Result<(), EventBusError> {
        event.latency.mark_local_bus_published();
        let payload = FlatbufferEventCodec::encode(&event)
            .map_err(|error| EventBusError::Codec(error.to_string()))?;
        validate_encoded_size(event.event_id, payload.len())?;
        let client = self.client.clone();
        tokio::spawn(async move {
            let _ = client.publish(subject.to_string(), payload.into()).await;
        });
        Ok(())
    }

    async fn subscribe(&self, subject: EventSubject) -> Result<EventSubscription, EventBusError> {
        let mut subscription = self
            .client
            .subscribe(subject.to_string())
            .await
            .map_err(|error| EventBusError::Subscribe(error.to_string()))?;
        let (sender, receiver) = mpsc::channel(1_024);
        tokio::spawn(async move {
            while let Some(message) = subscription.next().await {
                let Ok(event) = FlatbufferEventCodec::decode(&message.payload) else {
                    continue;
                };
                let delivery = EventDelivery {
                    subject: subject.clone(),
                    event,
                    reply_to: message.reply.map(|reply| reply.to_string()),
                };
                if sender.send(delivery).await.is_err() {
                    break;
                }
            }
        });
        Ok(EventSubscription { receiver })
    }

    async fn request_event(
        &self,
        subject: EventSubject,
        mut event: CloudEvent,
        timeout: Duration,
    ) -> Result<CloudEvent, EventBusError> {
        event.latency.mark_local_bus_published();
        let payload = FlatbufferEventCodec::encode(&event)
            .map_err(|error| EventBusError::Codec(error.to_string()))?;
        validate_encoded_size(event.event_id, payload.len())?;
        let response = tokio::time::timeout(
            timeout,
            self.client.request(subject.to_string(), payload.into()),
        )
        .await
        .map_err(|_| EventBusError::Request("request timed out".to_string()))?
        .map_err(|error| EventBusError::Request(error.to_string()))?;
        FlatbufferEventCodec::decode(&response.payload)
            .map_err(|error| EventBusError::Codec(error.to_string()))
    }

    async fn respond_event(
        &self,
        delivery: &EventDelivery,
        event: CloudEvent,
    ) -> Result<(), EventBusError> {
        let Some(reply_to) = delivery.reply_to.as_ref() else {
            return Ok(());
        };
        let payload = FlatbufferEventCodec::encode(&event)
            .map_err(|error| EventBusError::Codec(error.to_string()))?;
        validate_encoded_size(event.event_id, payload.len())?;
        self.client
            .publish(reply_to.clone(), payload.into())
            .await
            .map_err(|error| EventBusError::Publish(error.to_string()))
    }

    async fn acknowledge(
        &self,
        delivery: &EventDelivery,
        acknowledgement: SyncAcknowledgement,
    ) -> Result<(), EventBusError> {
        let Some(reply_to) = delivery.reply_to.as_ref() else {
            return Ok(());
        };
        let payload = serde_json::to_vec(&acknowledgement)
            .map_err(|error| EventBusError::Codec(error.to_string()))?;
        self.client
            .publish(reply_to.clone(), payload.into())
            .await
            .map_err(|error| EventBusError::Publish(error.to_string()))
    }
}

fn validate_event_size(event: &CloudEvent) -> Result<(), EventBusError> {
    let bytes = FlatbufferEventCodec::encode(event)
        .map_err(|error| EventBusError::Codec(error.to_string()))?;
    validate_encoded_size(event.event_id, bytes.len())
}

fn validate_encoded_size(event_id: EventId, encoded_bytes: usize) -> Result<(), EventBusError> {
    if encoded_bytes > LOCAL_EVENT_HARD_LIMIT_BYTES {
        return Err(EventBusError::PayloadTooLarge {
            event_id,
            encoded_bytes,
            max_bytes: LOCAL_EVENT_HARD_LIMIT_BYTES,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::live_sync::{EventEnvelope, EventKind, EventPriority};

    fn critical_event() -> CloudEvent {
        EventEnvelope::new(
            "runtime",
            "strategy",
            "session",
            "one",
            1,
            EventPriority::Critical,
            EventKind::InsightProjection,
            serde_json::json!({"state":"New"}),
        )
    }

    #[tokio::test]
    async fn acknowledged_bus_waits_for_the_durable_consumer() {
        let bus = InProcessEventBus::acknowledged();
        let subject = EventSubject::for_priority("runtime", "strategy", EventPriority::Critical);
        let mut consumer = bus.subscribe(subject.clone()).await.unwrap();
        let mut handle = bus
            .publish_critical(subject, critical_event())
            .await
            .unwrap();
        let delivery = consumer.recv().await.unwrap();
        assert!(matches!(
            handle.acknowledgement.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        bus.acknowledge(
            &delivery,
            SyncAcknowledgement::LocalAccepted {
                event_ids: vec![delivery.event.event_id],
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            handle.wait().await.unwrap(),
            SyncAcknowledgement::LocalAccepted { .. }
        ));
        assert!(bus.critical_replies.lock().is_empty());
    }

    #[tokio::test]
    async fn cancelled_acknowledgements_release_bounded_reply_state() {
        let bus = InProcessEventBus::acknowledged();
        let subject = EventSubject::for_priority("runtime", "strategy", EventPriority::Critical);
        let handle = bus
            .publish_critical(subject.clone(), critical_event())
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), handle.wait())
                .await
                .is_err()
        );
        assert!(bus.critical_replies.lock().is_empty());
        let mut outstanding = Vec::new();
        for _ in 0..NATS_SUBSCRIPTION_CAPACITY {
            outstanding.push(
                bus.publish_critical(subject.clone(), critical_event())
                    .await
                    .unwrap(),
            );
        }
        assert!(
            bus.publish_critical(subject, critical_event())
                .await
                .is_err()
        );
        drop(outstanding);
        assert!(bus.critical_replies.lock().is_empty());
    }

    #[tokio::test]
    async fn a_full_consumer_queue_is_not_falsely_acknowledged() {
        let bus = InProcessEventBus::acknowledged();
        let subject = EventSubject::for_priority("runtime", "strategy", EventPriority::Critical);
        let _consumer = bus.subscribe(subject.clone()).await.unwrap();
        let mut outstanding = Vec::new();
        for _ in 0..1024 {
            outstanding.push(
                bus.publish_critical(subject.clone(), critical_event())
                    .await
                    .unwrap(),
            );
        }
        let handle = bus
            .publish_critical(subject, critical_event())
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), handle.wait())
                .await
                .is_err()
        );
        drop(outstanding);
        assert!(bus.critical_replies.lock().is_empty());
    }

    #[tokio::test]
    async fn cancelling_request_reply_does_not_retain_an_inbox() {
        let bus = InProcessEventBus::default();
        let subject = EventSubject::reconciliation_lifecycle_request("runtime", "strategy");
        let request = bus.request_event(subject, critical_event(), Duration::from_secs(10));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), request)
                .await
                .is_err()
        );
        assert!(bus.request_replies.lock().is_empty());
    }

    #[tokio::test]
    async fn cancelling_a_delivery_aborts_its_unanswered_reply_worker() {
        let retained = Arc::new(());
        let worker_retained = retained.clone();
        let reply_task = tokio::spawn(async move {
            let _retained = worker_retained;
            std::future::pending::<()>().await;
        });
        let (sender, receiver) = oneshot::channel();
        let handle = LocalDeliveryHandle {
            event_id: uuid::Uuid::new_v4(),
            acknowledgement: receiver,
            cleanup: DeliveryCleanup(Some(Box::new(move || reply_task.abort()))),
        };
        assert!(
            tokio::time::timeout(Duration::from_millis(10), handle.wait())
                .await
                .is_err()
        );
        drop(sender);
        tokio::time::timeout(Duration::from_secs(1), async {
            while Arc::strong_count(&retained) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reply worker was detached after publication cancellation");
    }

    #[tokio::test]
    async fn in_process_subscribers_receive_independent_event_copies() {
        let bus = InProcessEventBus::default();
        let subject = EventSubject::for_priority("runtime", "strategy", EventPriority::Critical);
        let mut first = bus.subscribe(subject.clone()).await.unwrap();
        let mut second = bus.subscribe(subject.clone()).await.unwrap();
        let event = EventEnvelope::new(
            "runtime",
            "strategy",
            "session",
            "entity",
            1,
            EventPriority::Critical,
            EventKind::StrategyLifecycle,
            serde_json::json!({"status": "running"}),
        );
        let event_id = event.event_id;
        let handle = bus.publish_critical(subject, event).await.unwrap();
        assert_eq!(first.recv().await.unwrap().event.event_id, event_id);
        assert_eq!(second.recv().await.unwrap().event.event_id, event_id);
        assert!(matches!(
            handle.wait().await.unwrap(),
            SyncAcknowledgement::LocalAccepted { .. }
        ));
    }

    #[tokio::test]
    async fn in_process_bus_supports_bounded_lifecycle_request_reply() {
        let bus = InProcessEventBus::default();
        let subject = EventSubject::reconciliation_lifecycle_request("runtime", "strategy");
        let mut subscription = bus.subscribe(subject.clone()).await.unwrap();
        let responder_bus = bus.clone();
        tokio::spawn(async move {
            let delivery = subscription.recv().await.unwrap();
            let response = EventEnvelope::new(
                "runtime",
                "strategy",
                "session",
                "lifecycle-result",
                2,
                EventPriority::Reconciliation,
                EventKind::ReconciliationLifecycleResult,
                serde_json::json!({"requestId": "request-1"}),
            );
            responder_bus
                .respond_event(&delivery, response)
                .await
                .unwrap();
        });
        let request = EventEnvelope::new(
            "runtime",
            "strategy",
            "session",
            "lifecycle-request",
            1,
            EventPriority::Reconciliation,
            EventKind::ReconciliationLifecycleRequest,
            serde_json::json!({"requestId": "request-1"}),
        );

        let response = bus
            .request_event(subject, request, Duration::from_secs(1))
            .await
            .unwrap();

        assert_eq!(response.kind, EventKind::ReconciliationLifecycleResult);
        assert_eq!(response.payload["requestId"], "request-1");
    }
}
