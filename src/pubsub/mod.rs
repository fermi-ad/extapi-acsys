use crate::env_var;
use kafka::{
    client::{FetchOffset, GroupOffsetStorage},
    consumer::Consumer,
};
use std::{
    error::Error,
    fmt::{self, Debug},
    sync::Arc,
    thread,
    time::Duration,
};
use tokio::sync::broadcast::{self, Receiver, Sender};
use tokio_stream::wrappers::BroadcastStream;
use tracing::error;

fn handle<E: Error>(result: Result<(), E>) {
    match result {
        Ok(_) => (),
        Err(err) => error!("{}", err),
    }
}
fn do_poll<R, E: Error>(
    consumer: &mut Consumer, mut append_msg: impl FnMut(String) -> Result<R, E>,
) -> Result<(), PubSubError> {
    match consumer.poll() {
        Ok(message_sets) => {
            for set in message_sets.iter() {
                for msg in set.messages() {
                    match str::from_utf8(msg.value) {
                        Ok(decoded) => match append_msg(decoded.to_owned()) {
                            Ok(_) => (),
                            Err(err) => {
                                handle(Err(err));
                                return Err(PubSubError::default());
                            }
                        },
                        Err(err) => error!("{}", err),
                    };
                }
                handle(consumer.consume_messageset(set));
            }
            handle(consumer.commit_consumed());
        }
        Err(err) => {
            error!("{}", err);
            let _ = append_msg(String::from("An error occurred while consuming messages. See server logs for details. Closing stream."));
            return Err(PubSubError::default());
        }
    };
    Ok(())
}

struct MessageJob {
    consumer: Consumer,
    sender: Arc<Sender<String>>,
}
impl MessageJob {
    pub fn run(&mut self) {
        while do_poll(&mut self.consumer, |msg: String| self.sender.send(msg))
            .is_ok()
        {
            thread::sleep(Duration::from_millis(100));
        }
    }
}

const KAFKA_HOST: &str = "KAFKA_HOST";
const DEFAULT_KAFKA_HOST: &str = "acsys-services.fnal.gov";

const KAFKA_PORT: &str = "KAFKA_PORT";
const DEFAULT_KAFKA_PORT: &str = "9092";
fn get_consumer(topic: String) -> Result<Consumer, PubSubError> {
    let host = env_var::get(KAFKA_HOST).as_str_or(DEFAULT_KAFKA_HOST);
    let port = env_var::get(KAFKA_PORT).as_str_or(DEFAULT_KAFKA_PORT);
    let addr = format!("{}:{}", host, port);
    Consumer::from_hosts(vec![addr])
        .with_topic(topic)
        .with_fallback_offset(FetchOffset::Earliest)
        .with_offset_storage(Some(GroupOffsetStorage::Kafka))
        .create()
        .map_err(|err| {
            error!("{}", err);
            PubSubError::default()
        })
}

pub struct Snapshot {
    pub data: Vec<String>,
}
impl Snapshot {
    pub fn for_topic(topic: String) -> Result<Self, PubSubError> {
        let mut consumer = get_consumer(topic)?;
        let mut data: Vec<String> = Vec::new();

        let mut cur_size: usize = 0;
        loop {
            match do_poll(&mut consumer, |msg: String| {
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
                Err(err) => return Err(err),
            }
        }
        Ok(Self { data })
    }
}

/// A structure for subscribing to a message topic. Returns the values as a stream of messages for clients to handle.
#[derive(Debug)]
pub struct Subscriber {
    /// Keeps the channel open while the subscriber waits for clients to ask for a stream.
    _channel_lock: Receiver<String>,
    sender: Arc<Sender<String>>,
}
impl Subscriber {
    fn from(consumer: Consumer) -> Self {
        let (sender, _channel_lock) = broadcast::channel::<String>(20);
        let thread_sender = Arc::new(sender);
        let instance_sender = Arc::clone(&thread_sender);
        let mut message_job = MessageJob {
            consumer,
            sender: thread_sender,
        };
        let _task_handle = thread::spawn(move || {
            message_job.run();
        });

        Self {
            _channel_lock,
            sender: instance_sender,
        }
    }

    /// Generates a new subscriber for the provided topic.
    /// A new thread will be started and run in the background to poll for
    /// messages. The thread will terminate when this subscriber is dropped.
    pub fn for_topic(topic: String) -> Result<Self, PubSubError> {
        let consumer = get_consumer(topic)?;
        Ok(Self::from(consumer))
    }

    /// Streams messages that appear on the subscribed topic.
    pub fn get_stream(&self) -> BroadcastStream<String> {
        BroadcastStream::new(self.sender.subscribe())
    }
}

const CANNED_ERR_MESSAGE: &str = "An error occurred while attempting to connect to the message broker. See server logs for details.";

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
impl std::error::Error for PubSubError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn pubsub_error_display() {
        let err = PubSubError::default();
        assert_eq!(CANNED_ERR_MESSAGE, format!("{}", err));
    }

    #[test]
    fn error_on_bad_subscriber_host() {
        unsafe {
            env::set_var(KAFKA_HOST, "bad_host");
        }
        let result = Subscriber::for_topic(String::from("my_topic"));
        let err = result
            .expect_err("Expected the connection to fail, but it succeeded");
        assert_eq!(CANNED_ERR_MESSAGE, format!("{}", err));
        unsafe {
            env::remove_var(KAFKA_HOST);
        }
    }

    #[test]
    fn handles_err() {
        assert_eq!(handle::<PubSubError>(Ok(())), ());
        assert_eq!(handle(Err(PubSubError::default())), ());
    }
}
