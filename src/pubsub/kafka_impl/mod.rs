//! Kafka implementation of the pub/sub traits.
//!
//! This module provides Kafka-backed implementations of [`Snapshot`](super::Snapshot) and
//! [`Subscriber`](super::Subscriber) using [`rdkafka`](https://crates.io/crates/rdkafka).
//!
//! ## High-level behavior
//!
//! - [`KafkaSubscriber`](KafkaSubscriber) is intended for *live* consumption. It shares a single
//!   underlying consumer per `(host, topic)` across all subscribers in this process via
//!   [`CONSUMER_MAP`](CONSUMER_MAP), and will attempt to reconnect on interruptions.
//! - [`KafkaSnapshot`](KafkaSnapshot) is intended for *historical* reads. It reads from the
//!   beginning of the topic and returns once it has observed the current high-watermark for each
//!   partition.
//!
//! ## Timeouts
//!
//! - The environment variable `KAFKA_CONNECTION_SECONDS` is used as a general Kafka operation
//!   timeout (metadata fetch, watermarks, etc.).
//! - Snapshot reads also apply a per-message timeout (currently 5 seconds) to avoid hanging
//!   indefinitely when no progress is being made.
//!
//! ## Resource management
//!
//! Live subscribers are cached per `(host, topic)` in a process-global map.
//!
//! - A background reaper runs every 10 seconds and evicts cached streams that have had **0 active
//!   receivers** for at least 60 seconds.
//! - Eviction drops the [`KafkaStream`](stream::KafkaStream) which cancels its background task.
//!
//! This prevents unbounded growth in long-running processes that may subscribe to many topics over
//! time.

use super::{Message, PubSubError, Snapshot, Subscriber};
use rdkafka::{
    ClientConfig, Message as RdMessage,
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
    types::RDKafkaErrorCode,
};
use rust_env_var_lib::env_var;
use std::{collections::HashMap, fmt::Debug, sync::LazyLock, time::Duration};
use stream::KafkaStream;
use tokio::{
    sync::RwLock,
    time::{Instant, sleep},
};
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use tracing::warn;
use uuid::Uuid;

mod stream;

#[cfg(test)]
mod testing_utils;
#[cfg(test)]
mod tests;

/// With [`Snapshot`] available for use, [`KafkaSubscriber`] can be a lightweight wrapper around a Kafka consumer. Each instance of [`KafkaSubscriber`]
/// will share a single underlying consumer for a given host and topic, conserving system resources. New connections will pick up from the end of the stream,
/// and any need to grab historical records can be handled by the [`KafkaSnapshot`] implementation of the [`Snapshot`] trait.
/// As such, we build a static map of the consumers to be shared by all instances of [`KafkaSubscriber`] that get requested for the same host and topic.
///
/// Entries are evicted by a background reaper once they have had 0 receivers for a grace period.
static CONSUMER_MAP: LazyLock<RwLock<HashMap<(String, String), StreamEntry>>> =
    LazyLock::new(RwLock::default);

static REAPER_STARTED: LazyLock<()> = LazyLock::new(|| {
    tokio::spawn(reap_unused_streams());
});

const REAPER_INTERVAL: Duration = Duration::from_secs(10);
const EVICT_AFTER_IDLE: Duration = Duration::from_secs(60);

#[derive(Debug)]
struct StreamEntry {
    stream: KafkaStream,
    last_used: Instant,
}

impl From<KafkaError> for PubSubError {
    fn from(value: KafkaError) -> Self {
        PubSubError::caused_by(value)
    }
}

/// Implementation of the [`Snapshot`] trait for Kafka connections.
#[derive(Debug)]
pub struct KafkaSnapshot;
impl KafkaSnapshot {
    fn configure_consumer(
        host: &str, topic: &str,
    ) -> Result<StreamConsumer, PubSubError> {
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", host)
            .set("group.id", Uuid::new_v4().as_hyphenated().to_string())
            .set("auto.offset.reset", "earliest")
            .create::<StreamConsumer>()?;
        consumer.subscribe(&[topic])?;
        Ok(consumer)
    }

    fn determine_max_offsets(
        consumer: &StreamConsumer, topic: &str,
    ) -> Result<HashMap<i32, i64>, KafkaError> {
        let timeout = get_kafka_timeout_val();
        let metadata = consumer.fetch_metadata(Some(topic), timeout)?;
        match metadata.topics().first() {
            Some(topic_metadata) => {
                let mut offsets = HashMap::new();
                for partition in topic_metadata.partitions() {
                    let (_, high) = consumer.fetch_watermarks(
                        topic,
                        partition.id(),
                        timeout,
                    )?;
                    if high > 0 {
                        // The "high watermark" is the next offset to be assigned. Subtracting 1 ensures we
                        // return the actual max offset for messages in the topic currently.
                        offsets.insert(partition.id(), high - 1);
                    }
                }
                Ok(offsets)
            }
            None => Err(KafkaError::MetadataFetch(
                RDKafkaErrorCode::InvalidPartitions,
            )),
        }
    }
}
#[tonic::async_trait]
impl Snapshot for KafkaSnapshot {
    async fn get(
        host: String, topic: String,
    ) -> Result<Vec<Message>, PubSubError> {
        let consumer = Self::configure_consumer(&host, &topic)?;
        let mut offsets = Self::determine_max_offsets(&consumer, &topic)?;

        let stream = consumer.stream().timeout(Duration::from_secs(5));
        tokio::pin!(stream);
        let mut data: Vec<Message> = Vec::new();
        while !offsets.is_empty() {
            let message_result = stream.next().await;
            match message_result {
                Some(Ok(msg_res)) => {
                    let msg = msg_res?;
                    let partition = msg.partition();
                    let offset = msg.offset();
                    offsets.retain(|k, v| *k != partition || *v > offset);

                    match convert_to_message(msg) {
                        Some(message) => data.push(message),
                        None => warn!(
                            "Received Kafka message with no payload on topic {}",
                            topic
                        ),
                    }
                }
                Some(Err(e)) => return Err(PubSubError::caused_by(e)),
                None => {
                    return Err(PubSubError::caused_by(
                        KafkaError::MessageConsumption(
                            RDKafkaErrorCode::PartitionEOF,
                        ),
                    ));
                }
            }
        }
        Ok(data)
    }
}

/// Implementation of the [`Subscriber`] trait for Kafka connections.
#[derive(Debug)]
pub struct KafkaSubscriber;
#[tonic::async_trait]
impl Subscriber for KafkaSubscriber {
    async fn subscribe(
        host: String, topic: String,
    ) -> Result<BroadcastStream<Message>, PubSubError> {
        // Ensure the reaper is started once we begin using the cache.
        *REAPER_STARTED;

        let key = (host.clone(), topic.clone());

        // Fast path: read lock.
        if let Some(entry) = CONSUMER_MAP.read().await.get(&key) {
            return Ok(entry.stream.get_stream());
        }

        // Slow path: write lock + double-check.
        let mut lock = CONSUMER_MAP.write().await;
        if let Some(entry) = lock.get_mut(&key) {
            entry.last_used = Instant::now();
            return Ok(entry.stream.get_stream());
        }

        let stream_provider = KafkaStream::new(host, topic).await;
        let stream = stream_provider.get_stream();
        lock.insert(
            key,
            StreamEntry {
                stream: stream_provider,
                last_used: Instant::now(),
            },
        );
        Ok(stream)
    }
}

fn convert_to_message(incoming: impl RdMessage) -> Option<Message> {
    match incoming.payload() {
        Some(value_bytes) => {
            let value = String::from_utf8_lossy(value_bytes).to_string();

            let key = incoming
                .key()
                .map(|bytes| String::from_utf8_lossy(bytes).to_string());
            Some(Message { key, value })
        }
        None => {
            warn!(
                "Received Kafka message with no payload on topic {}",
                incoming.topic()
            );
            None
        }
    }
}

fn get_kafka_timeout_val() -> Duration {
    let secs = env_var::get("KAFKA_CONNECTION_SECONDS").or(1);
    Duration::from_secs(secs)
}

async fn reap_unused_streams() {
    loop {
        sleep(REAPER_INTERVAL).await;

        let now = Instant::now();
        let mut lock = CONSUMER_MAP.write().await;

        // Evict entries that have had 0 receivers for the grace period.
        lock.retain(|_key, entry| {
            if entry.stream.receiver_count() > 0 {
                entry.last_used = now;
                true
            } else {
                now.duration_since(entry.last_used) < EVICT_AFTER_IDLE
            }
        });
    }
}
