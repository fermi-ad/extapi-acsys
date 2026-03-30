use async_graphql::SimpleObject;
use kafkang::{
    client::{FetchOffset, GroupOffsetStorage},
    consumer::Consumer,
};
use rust_env_var_lib::env_var;
use std::{
    error::Error,
    fmt::{self, Debug},
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};
use tokio::sync::broadcast::{self, Receiver, Sender, error::SendError};
use tokio_stream::wrappers::BroadcastStream;
use tracing::error;
use uuid::Uuid;

const CANNED_ERR_MESSAGE: &str = "An error occurred while attempting to connect to the message broker. See server logs for details.";
const KAFKA_CONN_SECS: &str = "KAFKA_CONNECTION_SECONDS";

/// A message from the pub-sub service.
/// Contains a key (optional) and a value.
///
/// Instances may be created with the [`new`](Message::new) method (specifying both key and value)
/// or the [`from_value`](Message::from_value) method (specifying only the value).
#[derive(Clone, Debug, PartialEq, SimpleObject)]
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
            if let Some(cons) = &mut self.consumer {
                // sender.send() returns Result<usize, SendError<T>>, where the Ok path is the number of receivers
                // that got the message. We don't really care about that, but still want to capture any errors, so
                // we use .map(drop) to just drop the Ok path.
                let result =
                    do_poll(cons, |msg| self.sender.send(msg).map(drop));
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
            thread::sleep(Duration::from_millis(100));
        }
    }
}

#[derive(Debug)]
pub struct PubSubError {
    message: &'static str,
}
impl Default for PubSubError {
    fn default() -> Self {
        Self {
            message: CANNED_ERR_MESSAGE,
        }
    }
}
impl fmt::Display for PubSubError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}
impl Error for PubSubError {}

pub struct Snapshot {
    pub data: Vec<Message>,
}
impl Snapshot {
    pub fn for_topic(host: String, topic: String) -> Result<Self, PubSubError> {
        let mut consumer = get_consumer(host, topic, None)?;
        let mut data: Vec<Message> = Vec::new();

        let mut cur_size: usize = 0;
        loop {
            match do_poll(&mut consumer, |msg| {
                data.push(msg);
                Result::<(), PubSubError>::Ok(())
            }) {
                Ok(_) => {
                    if cur_size < data.len() {
                        cur_size = data.len();
                    } else {
                        break;
                    }
                }
                Err(err) => {
                    error!("{err}");
                    return Err(PubSubError::default());
                }
            }
        }
        Ok(Self { data })
    }
}

/// A structure for subscribing to a message topic. Returns the values as a stream of messages for clients to handle.
#[derive(Debug)]
pub struct Subscriber {
    /// Keeps the channel open while the subscriber waits for clients to ask for a stream.
    _channel_lock: Receiver<Message>,
    sender: Arc<Sender<Message>>,
}
impl Subscriber {
    /// Generates a new subscriber for the provided topic.
    /// A new thread will be started and run in the background to poll for
    /// messages. The thread will terminate when this subscriber is dropped.
    pub fn for_topic(host: String, topic: String) -> Self {
        let (sender, _channel_lock) = broadcast::channel::<Message>(20);
        let thread_sender = Arc::new(sender);
        let instance_sender = Arc::clone(&thread_sender);
        let mut message_job = MessageJob::from(host, topic, thread_sender);
        let _task_handle = thread::spawn(move || {
            message_job.run();
        });

        Self {
            _channel_lock,
            sender: instance_sender,
        }
    }

    /// Streams messages that appear on the subscribed topic.
    pub fn get_stream(&self) -> BroadcastStream<Message> {
        BroadcastStream::new(self.sender.subscribe())
    }
}

fn do_poll<E: Error + 'static>(
    consumer: &mut Consumer,
    mut append_msg: impl FnMut(Message) -> Result<(), E>,
) -> Result<(), Box<dyn Error>> {
    let message_sets = consumer.poll()?;
    for set in message_sets.iter() {
        for msg in set.messages() {
            let key = str::from_utf8(msg.key).ok().map(String::from);
            let value = str::from_utf8(msg.value)?.to_string();
            append_msg(Message::new(key, value))?;
        }
        consumer.consume_messageset(&set)?;
    }
    if consumer.group().is_empty() {
        Ok(())
    } else {
        Ok(consumer.commit_consumed()?)
    }
}

fn get_consumer(
    host: String, topic: String, group: Option<String>,
) -> Result<Consumer, PubSubError> {
    let (sender, receiver) = mpsc::channel();
    let _ = thread::spawn(move || {
        let consumer = Consumer::from_hosts(vec![host])
            .with_topic(topic)
            .with_group(group.unwrap_or_default())
            .with_fallback_offset(FetchOffset::Earliest)
            .with_offset_storage(Some(GroupOffsetStorage::Kafka))
            .with_fetch_max_bytes_per_partition(1048576)
            .create()
            .map_err(|err| {
                error!("{}", err);
                PubSubError::default()
            });
        handle(sender.send(consumer));
    });
    let connection_seconds: u64 = env_var::expect(KAFKA_CONN_SECS);
    receiver
        .recv_timeout(Duration::from_secs(connection_seconds))
        .map_err(|err| {
            error!("{}", err);
            PubSubError::default()
        })?
}

fn handle<E: Error>(result: Result<(), E>) {
    match result {
        Ok(_) => (),
        Err(err) => error!("{}", err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pubsub_error_display() {
        let err = PubSubError::default();
        assert_eq!(CANNED_ERR_MESSAGE, format!("{}", err));
    }

    #[test]
    fn error_on_bad_subscriber_host() {
        let mut result = Subscriber::for_topic(
            String::from("my_host"),
            String::from("my_topic"),
        );
        let num_rec = result
            .sender
            .send(Message::new(None, "testing".to_string()))
            .unwrap();
        assert_eq!(num_rec, 1);
        assert_eq!(result._channel_lock.try_recv().unwrap().value, "testing");
    }

    #[test]
    fn handles_err() {
        assert_eq!(handle::<PubSubError>(Ok(())), ());
        assert_eq!(handle(Err(PubSubError::default())), ());
    }
}
