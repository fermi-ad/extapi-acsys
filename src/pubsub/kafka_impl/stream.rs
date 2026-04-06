//! Kafka Streaming Module
//!
//! Describes the underlying [`KafkaStream`] struct used to implement the [`Subscriber`](super::Subscriber) trait for Kafka connections.
//! This struct is responsible for maintaining a connection to a Kafka topic and streaming messages from that topic to any number of subscribers.

use crate::pubsub::Message;
use rdkafka::{
    ClientConfig,
    consumer::{Consumer, ConsumerContext, MessageStream, StreamConsumer},
    error::KafkaError,
    message::{BorrowedMessage, Message as RdMessage},
};
use std::time::Duration;
use tokio::{
    select, spawn,
    sync::broadcast::{Sender, channel},
    time::sleep,
};
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};
use uuid::Uuid;

const MAX_WAIT_TIME: Duration = Duration::from_mins(5);

#[derive(Debug)]
pub struct KafkaStream {
    cancel_token: CancellationToken,
    sender: Sender<Message>,
}

impl KafkaStream {
    pub fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }
}
impl KafkaStream {
    pub async fn new(host: String, topic: String) -> Self {
        let (sender, _) = channel(100);
        let remote_sender = sender.clone();

        let cancel_token = CancellationToken::new();
        let remote_token = cancel_token.clone();

        spawn(start_stream(host, topic, remote_sender, remote_token));

        Self {
            sender,
            cancel_token,
        }
    }

    pub fn get_stream(&self) -> BroadcastStream<Message> {
        let receiver = self.sender.subscribe();
        BroadcastStream::new(receiver)
    }
}
impl Drop for KafkaStream {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}

async fn start_stream(
    host: String, topic: String, sender: Sender<Message>,
    token: CancellationToken,
) {
    let stream_id = Uuid::new_v4().as_hyphenated().to_string();
    let mut wait_time = Duration::from_secs(1);
    while !token.is_cancelled() {
        let mut builder = ClientConfig::new();
        builder.set("bootstrap.servers", &host);
        builder.set("group.id", &stream_id);

        // During testing, the low latency of the mock Kafka cluster means that messages are often produced on the broker before the
        // consumer is registered. Setting the auto offset reset to "earliest" ensures we see all messages during the test.
        #[cfg(test)]
        builder.set("auto.offset.reset", "earliest");

        match builder.create::<StreamConsumer>() {
            Ok(consumer) => {
                if let Err(e) = consumer.subscribe(&[&topic]) {
                    wait_time = handle_connection_err(e, wait_time).await;
                } else {
                    let message_stream = consumer.stream();
                    monitor_stream(
                        message_stream,
                        &sender,
                        &topic,
                        token.child_token(),
                    )
                    .await;
                    wait_time = Duration::from_secs(1);
                }
            }
            Err(e) => wait_time = handle_connection_err(e, wait_time).await,
        }
    }
}

async fn monitor_stream<C: ConsumerContext>(
    mut message_stream: MessageStream<'_, C>, sender: &Sender<Message>,
    topic: &str, token: CancellationToken,
) {
    select! {
        _ = token.cancelled() => {},
        _ = async {
            while let Some(message_result) = message_stream.next().await {
                match message_result {
                    Ok(msg) => {
                        if let Some(message) = convert_to_message(&msg) {
                            if let Err(e) = sender.send(message) {
                                error!(
                                    "Failed to send message from topic {topic} to channel: {e}"
                                );
                            }
                        } else {
                            warn!(
                                "Received a message on topic {topic} that could not be parsed as UTF-8.
                                Skipping.\n  Original message: {msg:?}"
                            );
                        }
                    }
                    Err(e) => {
                        error!(
                            "Error while reading from Kafka stream for topic {topic}: {e}"
                        );
                        break;
                    }
                }
            }
        } => {}
    }
}

fn convert_to_message(incoming: &BorrowedMessage) -> Option<Message> {
    incoming.payload().map(|value_bytes| {
        let value = String::from_utf8_lossy(value_bytes).to_string();

        let key = incoming
            .key()
            .map(|bytes| String::from_utf8_lossy(bytes).to_string());
        Message::new(key, value)
    })
}

async fn handle_connection_err(
    err: KafkaError, mut wait_time: Duration,
) -> Duration {
    error!("Kafka connection error: {err}");
    sleep(wait_time).await;
    wait_time *= 2;
    if wait_time > MAX_WAIT_TIME {
        MAX_WAIT_TIME
    } else {
        wait_time
    }
}
