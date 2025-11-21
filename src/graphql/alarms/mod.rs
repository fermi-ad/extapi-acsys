use async_graphql::{Context, Error, Object, Subscription};
use tokio_stream::wrappers::BroadcastStream;

use crate::pubsub::Subscriber;

const ALARMS_KAFKA_TOPIC: &str = "ACsys";
pub fn get_alarms_subscriber() -> Option<Subscriber> {
    Subscriber::for_topic(String::from(ALARMS_KAFKA_TOPIC)).ok()
}

#[derive(Default)]
pub struct AlarmsQueries;
#[Object]
impl AlarmsQueries {
    async fn alarms_snapshot(
        &self, _ctxt: &Context<'_>,
    ) -> Result<Vec<String>, Error> {
        Ok(Vec::new())
    }
}

#[derive(Default)]
pub struct AlarmsSubscriptions;

#[Subscription]
impl<'ctx> AlarmsSubscriptions {
    async fn alarms(
        &self, ctxt: &Context<'ctx>,
    ) -> Result<BroadcastStream<String>, Error> {
        let subscriber = ctxt.data::<Option<Subscriber>>()?;
        match subscriber {
            Some(sub) => Ok(sub.get_stream()),
            None => Err(Error::new("No alarms Subscriber available")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_graphql::{EmptyMutation, Response, Schema};
    use futures::StreamExt;
    use std::env;

    #[test]
    fn get_alarms_subscriber_returns_none_when_bad_address() {
        unsafe {
            env::set_var("KAFKA_HOST_ADDR", "fake value");
        }
        assert!(get_alarms_subscriber().is_none());
        unsafe {
            env::remove_var("KAFKA_HOST_ADDR");
        }
    }

    #[tokio::test]
    async fn alarms_sub_returns_err_response_when_no_subscriber_provided() {
        let schema =
            Schema::build(AlarmsQueries, EmptyMutation, AlarmsSubscriptions)
                .finish();
        let result = schema.execute_stream(
            r#"
            subscription Alarms {
                alarms
            }
        "#,
        );
        let collection = result.collect::<Vec<Response>>().await;
        assert_eq!(collection.len(), 1);
        match collection.first() {
            Some(output) => {
                assert_eq!(output.errors.len(), 1);
                match output.errors.first() {
                    Some(err) => assert_eq!(err.message.as_str(), "Data `core::option::Option<extapi_dpm::pubsub::Subscriber>` does not exist."),
                    None => {
                        panic!("Err length was 1, but first() returned None")
                    }
                };
            }
            None => panic!("Results length was 1, but first() returned None"),
        };
    }

    #[tokio::test]
    async fn alarms_sub_returns_none_when_no_subscriber_provided() {
        let schema =
            Schema::build(AlarmsQueries, EmptyMutation, AlarmsSubscriptions)
                .data::<Option<Subscriber>>(None)
                .finish();
        let result = schema.execute_stream(
            r#"
            subscription Alarms {
                alarms
            }
        "#,
        );
        let collection = result.collect::<Vec<Response>>().await;
        assert_eq!(collection.len(), 1);
        match collection.first() {
            Some(output) => {
                assert_eq!(output.errors.len(), 1);
                match output.errors.first() {
                    Some(err) => assert_eq!(
                        err.message.as_str(),
                        "No alarms Subscriber available"
                    ),
                    None => {
                        panic!("Err length was 1, but first() returned None")
                    }
                };
            }
            None => panic!("Results length was 1, but first() returned None"),
        };
    }
}
