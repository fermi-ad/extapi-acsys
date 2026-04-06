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
