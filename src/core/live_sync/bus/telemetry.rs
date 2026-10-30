//! One owned best-effort lane per local NATS client, not a task per packet.
//! Critical request/reply publications never enter this lossy preview queue.
use super::*;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};

const QUEUE_EVENTS: usize = 128;
const QUEUE_BYTES: usize = 8 * 1024 * 1024;

struct Publication {
    subject: String,
    payload: Vec<u8>,
    _memory: OwnedSemaphorePermit,
}

struct Inner {
    sender: mpsc::Sender<Publication>,
    budget: Arc<Semaphore>,
    shutdown: watch::Sender<bool>,
}
impl Drop for Inner {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}

#[derive(Clone)]
pub(super) struct TelemetryDispatcher(Arc<Inner>);

impl TelemetryDispatcher {
    fn queue(
        events: usize,
        bytes: usize,
    ) -> (Self, mpsc::Receiver<Publication>, watch::Receiver<bool>) {
        let (sender, receiver) = mpsc::channel(events);
        let (shutdown, shutdown_rx) = watch::channel(false);
        (
            Self(Arc::new(Inner {
                sender,
                budget: Arc::new(Semaphore::new(bytes)),
                shutdown,
            })),
            receiver,
            shutdown_rx,
        )
    }

    pub fn spawn(client: async_nats::Client, runtime: &tokio::runtime::Handle) -> Self {
        let (dispatcher, receiver, shutdown) = Self::queue(QUEUE_EVENTS, QUEUE_BYTES);
        runtime.spawn(run(receiver, shutdown, move |subject, payload| {
            let client = client.clone();
            async move {
                let _ = client.publish(subject, payload.into()).await;
            }
        }));
        dispatcher
    }

    fn reserve(&self) -> Result<mpsc::OwnedPermit<Publication>, EventBusError> {
        self.0
            .sender
            .clone()
            .try_reserve_owned()
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => EventBusError::PublisherCapacity,
                mpsc::error::TrySendError::Closed(_) => {
                    EventBusError::Publish("local telemetry worker stopped".into())
                }
            })
    }

    fn finish(
        &self,
        permit: mpsc::OwnedPermit<Publication>,
        subject: String,
        payload: Vec<u8>,
    ) -> Result<(), EventBusError> {
        let bytes = u32::try_from(payload.len()).map_err(|_| EventBusError::PublisherCapacity)?;
        let memory = self
            .0
            .budget
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|_| EventBusError::PublisherCapacity)?;
        permit.send(Publication {
            subject,
            payload,
            _memory: memory,
        });
        Ok(())
    }

    pub fn publish(
        &self,
        subject: EventSubject,
        mut event: CloudEvent,
    ) -> Result<(), EventBusError> {
        // Reserve the count slot before encoding. Rejected previews do not
        // allocate a second large envelope or spawn a blocked sender task.
        let permit = self.reserve()?;
        event.latency.mark_local_bus_published();
        let payload = FlatbufferEventCodec::encode(&event)
            .map_err(|error| EventBusError::Codec(error.to_string()))?;
        validate_encoded_size(event.event_id, payload.len())?;
        self.finish(permit, subject.to_string(), payload)
    }
}

async fn run<F, Fut>(
    mut receiver: mpsc::Receiver<Publication>,
    mut shutdown: watch::Receiver<bool>,
    mut publish: F,
) where
    F: FnMut(String, Vec<u8>) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    loop {
        if *shutdown.borrow() {
            return;
        }
        let publication = tokio::select! {biased;
            changed=shutdown.changed()=>{if changed.is_err() || *shutdown.borrow(){return;}continue;},
            publication=receiver.recv()=>match publication {Some(publication)=>publication,None=>return},
        };
        let Publication {
            subject,
            payload,
            _memory,
        } = publication;
        tokio::select! {biased;
            changed=shutdown.changed()=>{if changed.is_err() || *shutdown.borrow(){return;}},
            _=publish(subject,payload)=>{},
        }
        drop(_memory);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enqueue(dispatcher: &TelemetryDispatcher, bytes: usize) -> Result<(), EventBusError> {
        dispatcher.finish(dispatcher.reserve()?, "fixture".into(), vec![0; bytes])
    }

    #[test]
    fn telemetry_queue_bounds_count_and_bytes_including_in_flight_ownership() {
        let (dispatcher, mut receiver, _) = TelemetryDispatcher::queue(2, 100);
        enqueue(&dispatcher, 40).unwrap();
        enqueue(&dispatcher, 40).unwrap();
        assert!(matches!(
            enqueue(&dispatcher, 1),
            Err(EventBusError::PublisherCapacity)
        ));
        let first = receiver.try_recv().unwrap();
        assert_eq!(dispatcher.0.budget.available_permits(), 20);
        assert!(matches!(
            enqueue(&dispatcher, 21),
            Err(EventBusError::PublisherCapacity)
        ));
        drop(first);
        enqueue(&dispatcher, 60).unwrap();
        assert_eq!(dispatcher.0.budget.available_permits(), 0);
        drop(receiver);
        assert_eq!(dispatcher.0.budget.available_permits(), 100);
    }

    #[test]
    fn denied_encoding_or_byte_reservation_releases_the_count_permit() {
        let (dispatcher, mut receiver, _) = TelemetryDispatcher::queue(1, 100);
        assert!(matches!(
            enqueue(&dispatcher, 101),
            Err(EventBusError::PublisherCapacity)
        ));
        assert_eq!(dispatcher.0.budget.available_permits(), 100);
        enqueue(&dispatcher, 100).unwrap();
        drop(receiver.try_recv().unwrap());
        assert_eq!(dispatcher.0.budget.available_permits(), 100);
    }

    #[tokio::test]
    async fn dropping_the_last_owner_cancels_a_stalled_writer_and_releases_queued_bytes() {
        let (dispatcher, receiver, shutdown) = TelemetryDispatcher::queue(4, 100);
        let budget = dispatcher.0.budget.clone();
        let started = Arc::new(tokio::sync::Notify::new());
        let active = started.clone();
        let task = tokio::spawn(run(receiver, shutdown, move |_, _| {
            let started = active.clone();
            async move {
                started.notify_one();
                std::future::pending::<()>().await;
            }
        }));
        enqueue(&dispatcher, 40).unwrap();
        started.notified().await;
        enqueue(&dispatcher, 40).unwrap();
        assert_eq!(budget.available_permits(), 20);
        drop(dispatcher);
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(budget.available_permits(), 100);
    }
}
