//! Abstracts the concept of a Publisher/Subscriber resource.
//!
//! This library enhances the testability of code that is part of a pub/sub architecture, and makes
//! calls to the pub/sub service easier to set up and manage.

use async_graphql::SimpleObject;
use std::{
    error::Error,
    fmt::{Debug, Display, Formatter, Result as FmtResult},
};
use tokio_stream::wrappers::BroadcastStream;
use tracing::error;
use uuid::Uuid;

pub mod kafka_impl;

/// A message from the pub-sub service.
/// Contains a key (optional) and a value.
///
/// Instances may be created with the [`new`](Message::new) method (specifying both key and value)
/// or the [`from_value`](Message::from_value) method (specifying only the value).
#[derive(Debug, Clone, PartialEq, SimpleObject)]
pub struct Message {
    pub key: Option<String>,
    pub value: String,
}
impl Message {
    /// Creates a new [`Message`] with the provided key and value.
    pub fn new(key: Option<String>, value: String) -> Self {
        Self { key, value }
    }
}

<<<<<<< HEAD
/// A trait for retrieving the instantaneous set of [`Message`]s on a topic.
#[tonic::async_trait]
pub trait Snapshot {
    /// Retrieves a snapshot of a message topic.
    /// This function connects to the message broker,
    /// loads all [`Message`]s currently on the specified topic, and returns them
    /// to the caller.
    async fn get(
        host: String, topic: String,
    ) -> Result<Vec<Message>, PubSubError>;
=======
struct MessageJob {
    consumer: Option<Consumer>,
    host: String,
    sender: Arc<Sender<Message>>,
    topic: String,
    uuid: Uuid,
}
impl MessageJob {
    fn check_connection(&mut self) {
        if self.consumer.is_none() {
            self.consumer = get_consumer(
                self.host.clone(),
                self.topic.clone(),
                Some(self.uuid.to_string()),
            )
            .ok();
        }
    }

    fn from(host: String, topic: String, sender: Arc<Sender<Message>>) -> Self {
        let uuid = Uuid::new_v4();
        Self {
            consumer: get_consumer(
                host.clone(),
                topic.clone(),
                Some(uuid.to_string()),
            )
            .ok(),
            host,
            sender,
            topic,
            uuid,
        }
    }

    fn run(&mut self) {
        loop {
            self.check_connection();
            let mut got_data = false;

            if let Some(cons) = &mut self.consumer {
                // sender.send() returns Result<usize, SendError<T>>, where the Ok path is the number of receivers
                // that got the message. We don't really care about that, but still want to capture any errors, so
                // we use .map(drop) to just drop the Ok path.
                let result = do_poll(cons, |msg| {
                    got_data = true;
                    self.sender.send(msg).map(drop)
                });
                if let Err(err) = result {
                    if err.downcast_ref::<SendError<Message>>().is_some() {
                        // The send stream is closed, so all receivers must have been dropped and there is no
                        // more need for this thread to run.
                        break;
                    } else {
                        // Something else went wrong with the consumer. Drop the current instance so we can
                        // attempt to reconnect on the next pass.
                        error!("{err}");
                        self.consumer = None;
                    }
                }
            }

            // Only sleep if no data was processed to avoid introducing latency during high-traffic periods.
            if !got_data {
                thread::sleep(Duration::from_millis(100));
            }
        }
    }
>>>>>>> 6f22692 (:zap: performance improvements)
}

/// A trait for subscribing to a message topic. Returns the values as a stream of [`Message`]s for clients to handle.
#[tonic::async_trait]
pub trait Subscriber: Debug {
    /// Streams [`Message`]s that appear on the subscribed topic. If an interruption occurs, the Subscriber will
    /// attempt to reconnect on its own.
    async fn subscribe(
        host: String, topic: String,
    ) -> Result<BroadcastStream<Message>, PubSubError>;
}

/// An implementation of [`Error`] to return when pub/sub operations do not succeed.
/// This will record the underlying error if provided.
/// Consumers of this library should use the [`Display`] trait when translating the error to users.
/// It returns a canned message that does not expose internal details, but the underlying error
/// can be accessed by searching the server logs for the corresponding ID.
pub struct PubSubError {
    id: Uuid,
}
impl PubSubError {
    /// Creates a new [`PubSubError`] with the provided cause.
    pub fn caused_by<E: Error + Send + Sync + 'static>(cause: E) -> Self {
        let id = Uuid::new_v4();
        error!("{id} PubSubError caused by: {cause:?}");
        Self { id }
    }
}
impl Debug for PubSubError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "Pub/Sub error. Check log entry with ID {}", self.id)
    }
}
impl Display for PubSubError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "An error occurred while performing a pub/sub operation (Error ID: {})",
            self.id
        )
    }
}
impl Error for PubSubError {}
