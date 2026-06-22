//! The tests for the Kafka Implementation Module

use super::{
    testing_utils::{Harness, KafkaPublisher},
    *,
};
use tokio_stream::StreamExt;

#[test]
fn from_kafka_error() {
    let result = PubSubError::from(KafkaError::Canceled);
    assert!(format!("{result}").starts_with(
        "An error occurred while performing a pub/sub operation (Error ID:"
    ));
}

#[tokio::test]
async fn kafka_consumer_and_producer() {
    let topic = String::from("test_topic");
    let test_harness = Harness::with_topics(vec![topic.clone()]).await;

    let mut stream =
        KafkaSubscriber::subscribe(test_harness.host(), topic.clone())
            .await
            .unwrap();

    let message = Message::new(None, "testing".to_string());
    let test_pub = KafkaPublisher::new(test_harness.host(), topic);
    test_pub.publish(message.clone()).await.unwrap();

    assert_eq!(message, stream.next().await.unwrap().unwrap());
}
