use async_graphql::{Context, Error, Object, Subscription};

use chrono::{DateTime, Utc};

use crate::env_var;

use crate::g_rpc::{alarms_db, proto::services::alarms};

use crate::pubsub::{Message, Snapshot, Subscriber};

use tokio_stream::wrappers::BroadcastStream;
use tonic::{Code, Request, Status};
use tracing::error;
mod types;
use types::{AlarmGroup, AlarmGroupMetadatum, AlarmTimer, UserLayout};

mod utils;

pub fn get_alarms_subscriber() -> Subscriber {
    Subscriber::for_topic(get_topic())
}

#[derive(Default)]
pub struct AlarmsMutations;
#[Object]
impl AlarmsMutations {
    async fn create_alarm_timer(
        &self, device: String, end_time: Option<DateTime<Utc>>,
        timer_type: String, updated_by: String,
    ) -> Result<AlarmTimer, Error> {
        let timer_type_enum = utils::string_to_timer_type(&timer_type);
        let end_time_ts = utils::datetime_to_timestamp(end_time);
        let alarm_timer = alarms::AlarmTimer {
            device,
            end_time: end_time_ts,
            timer_type: timer_type_enum as i32,
            updated_at: None,
            updated_by,
        };
        match alarms_db::timers::create(Request::new(alarm_timer.clone())).await
        {
            Ok(_) => Ok(AlarmTimer::from(alarm_timer)),
            Err(e) => handle_error(e, "creating alarm timer"),
        }
    }

    async fn delete_alarm_timer(
        &self, device: String, timer_type: String,
    ) -> Result<String, Error> {
        let timer_type_enum = utils::string_to_timer_type(&timer_type);
        let request = alarms::DeleteRequest {
            device: device.clone(),
            timer_type: timer_type_enum as i32,
        };
        match alarms_db::timers::delete(Request::new(request)).await {
            Ok(_) => Ok(device),
            Err(e) => handle_error(e, "deleting alarm timer"),
        }
    }

    async fn update_alarm_timer(
        &self, device: String, end_time: Option<DateTime<Utc>>,
        timer_type: String, updated_by: String,
    ) -> Result<AlarmTimer, Error> {
        let timer_type_enum = utils::string_to_timer_type(&timer_type);
        let end_time_ts = utils::datetime_to_timestamp(end_time);
        let alarm_timer_proto = alarms::AlarmTimer {
            device,
            end_time: end_time_ts,
            timer_type: timer_type_enum as i32,
            updated_at: None,
            updated_by,
        };
        match alarms_db::timers::update(Request::new(alarm_timer_proto.clone()))
            .await
        {
            Ok(_) => Ok(AlarmTimer::from(alarm_timer_proto)),
            Err(e) => handle_error(e, "updating alarm timer"),
        }
    }
}

#[derive(Default)]
pub struct AlarmsQueries;
#[Object]
impl AlarmsQueries {
    async fn alarms_group_metadata(
        &self,
    ) -> Result<Vec<AlarmGroupMetadatum>, Error> {
        match alarms_db::groups::read_metadata(Request::new(())).await {
            Ok(response) => {
                let mapped_response = response
                    .metadata
                    .into_iter()
                    .map(AlarmGroupMetadatum::from)
                    .collect();
                Ok(mapped_response)
            }
            Err(e) => handle_error(e, "reading alarm group metadata"),
        }
    }

    async fn alarms_groups(
        &self, groups: Vec<String>,
    ) -> Result<Vec<AlarmGroup>, Error> {
        match alarms_db::groups::read_groups(Request::new(
            alarms::GroupsRequest { groups },
        ))
        .await
        {
            Ok(response) => {
                let mapped_response = response
                    .alarm_groups
                    .into_iter()
                    .map(AlarmGroup::from)
                    .collect();
                Ok(mapped_response)
            }
            Err(e) => handle_error(e, "reading alarm groups"),
        }
    }

    async fn alarms_user_layouts(&self) -> Result<Vec<UserLayout>, Error> {
        match alarms_db::layouts::read_layouts(Request::new(())).await {
            Ok(response) => {
                let mapped_response = response
                    .layouts
                    .into_iter()
                    .map(UserLayout::from)
                    .collect();
                Ok(mapped_response)
            }
            Err(e) => handle_error(e, "reading user layouts"),
        }
    }

    async fn alarms_snapshot(&self) -> Result<Vec<Message>, Error> {
        match Snapshot::for_topic(get_topic()) {
            Ok(snapshot) => Ok(snapshot.data),
            Err(err) => Err(Error::new(format!("{err}"))),
        }
    }

    async fn alarms_timers(
        &self, timer_type: String, user: String,
    ) -> Result<Vec<AlarmTimer>, Error> {
        match alarms_db::timers::read(Request::new(alarms::ReadRequest {
            timer_type: utils::string_to_timer_type(&timer_type) as i32,
            user,
        }))
        .await
        {
            Ok(response) => {
                let timers = response
                    .alarm_timers
                    .into_iter()
                    .map(AlarmTimer::from)
                    .collect();
                Ok(timers)
            }
            Err(e) => handle_error(e, "reading alarm timer"),
        }
    }
}

#[derive(Default)]
pub struct AlarmsSubscriptions;

#[Subscription]
impl<'ctx> AlarmsSubscriptions {
    async fn alarms(
        &self, ctxt: &Context<'ctx>,
    ) -> Result<BroadcastStream<Message>, Error> {
        ctxt.data::<Subscriber>()
            .map(|subscriber| subscriber.get_stream())
    }
}

const ALARMS_KAFKA_TOPIC: &str = "ALARMS_KAFKA_TOPIC";
const DEFAULT_ALARMS_TOPIC: &str = "ACsys";
fn get_topic() -> String {
    env_var::get(ALARMS_KAFKA_TOPIC).or(DEFAULT_ALARMS_TOPIC.to_owned())
}

fn handle_error<T>(e: Status, gerund: &str) -> Result<T, Error> {
    error!("gRPC Error {gerund}: {e:?}");
    Err(match e.code() {
        Code::InvalidArgument | Code::Internal => Error::new(format!("{}", e)),
        _ => {
            Error::new(format!("Error {gerund}. See server logs for details."))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pubsub::PubSubError;
    use async_graphql::{Response, Schema};
    use futures::StreamExt;

    async fn test_query_returns_err(gql_query: &str, err_msg: &str) {
        let schema =
            Schema::build(AlarmsQueries, AlarmsMutations, AlarmsSubscriptions)
                .finish();
        let result = schema.execute(gql_query).await;
        let err = result.errors.first().unwrap();
        assert_eq!(err.message, err_msg);
    }

    #[tokio::test]
    async fn get_alarms_subscriber_returns_instance() {
        let instance = get_alarms_subscriber();
        let result = instance.get_stream().take(0).collect::<Vec<_>>().await;
        assert_eq!(0, result.len());
    }

    #[tokio::test]
    async fn alarms_sub_returns_err_response_when_no_subscriber_provided() {
        let schema =
            Schema::build(AlarmsQueries, AlarmsMutations, AlarmsSubscriptions)
                .finish();
        let result = schema.execute_stream(
            r#"
            subscription Alarms {
                alarms {
                  key,
                  value
                }
            }
        "#,
        );
        let collection = result.collect::<Vec<Response>>().await;
        assert_eq!(collection.len(), 1);
        let output = collection.first().unwrap();
        assert_eq!(output.errors.len(), 1);
        let err = output.errors.first().unwrap();
        assert_eq!(
            err.message.as_str(),
            "Data `extapi_dpm::pubsub::Subscriber` does not exist."
        );
    }

    #[tokio::test]
    async fn create_alarms_timer_returns_internal_err_on_bad_connection() {
        test_query_returns_err(
            r#"
            mutation Alarms {
                createAlarmTimer(device: "G:AMANDA", endTime: "2026-01-15T14:25:32.000Z", timerType: "test_type", updatedBy: "test_user") {
                    device
                    timerType
                    endTime
                    updatedBy
                    updatedAt
                }
            }
        "#,
            "code: 'Internal error', message: \"Could not connect to the database service. See server logs for details.\"",
        )
        .await;
    }

    #[tokio::test]
    async fn delete_alarms_timer_returns_internal_err_on_bad_connection() {
        test_query_returns_err(
            r#"
            mutation Alarms {
                deleteAlarmTimer(device: "G:AMANDA", timerType: "test_type")
            }
        "#,
            "code: 'Internal error', message: \"Could not connect to the database service. See server logs for details.\"",
        )
        .await;
    }

    #[tokio::test]
    async fn get_alarms_snapshot_returns_err_when_bad_address() {
        test_query_returns_err(
            r#"
            query Alarms {
                alarmsSnapshot {
                  key,
                  value
                }
            }
        "#,
            &format!("{}", PubSubError::default()),
        )
        .await;
    }

    #[test]
    fn handles_invalid_arg_err() {
        let result = handle_error::<()>(
            Status::invalid_argument("test invalid arg"),
            "testing alarm timer",
        );
        assert_eq!(result.unwrap_err().message, "code: 'Client specified an invalid argument', message: \"test invalid arg\"");

        let result = handle_error::<()>(
            Status::internal("test internal err"),
            "testing alarm timer",
        );
        assert_eq!(
            result.unwrap_err().message,
            "code: 'Internal error', message: \"test internal err\""
        );

        let result = handle_error::<()>(
            Status::not_found("test other err"),
            "testing alarm timer",
        );
        assert_eq!(
            result.unwrap_err().message,
            "Error testing alarm timer. See server logs for details."
        );
    }

    #[tokio::test]
    async fn read_alarms_timers_returns_internal_err_on_bad_connection() {
        test_query_returns_err(
            r#"
            query Alarms {
                alarmsTimers(timerType: "test_type", user: "test_user") {
                    device
                    timerType
                    endTime
                    updatedBy
                    updatedAt
                }
            }
        "#,
            "code: 'Internal error', message: \"Could not connect to the database service. See server logs for details.\"",
        )
        .await;
    }

    #[tokio::test]
    async fn read_group_metadata_returns_internal_err_on_bad_connection() {
        test_query_returns_err(
            r#"
            query Alarms {
                alarmsGroupMetadata {
                    name
                }
            }
        "#,
            "code: 'Internal error', message: \"Could not connect to the database service. See server logs for details.\"",
        )
        .await;
    }

    #[tokio::test]
    async fn read_groups_returns_internal_err_on_bad_connection() {
        test_query_returns_err(
            r#"
            query Alarms {
                alarmsGroups(groups: []) {
                    devices
                }
            }
        "#,
            "code: 'Internal error', message: \"Could not connect to the database service. See server logs for details.\"",
        )
        .await;
    }

    #[tokio::test]
    async fn read_user_layouts_returns_internal_err_on_bad_connection() {
        test_query_returns_err(
            r#"
            query Alarms {
                alarmsUserLayouts {
                    userName
                }
            }
        "#,
            "code: 'Internal error', message: \"Could not connect to the database service. See server logs for details.\"",
        )
        .await;
    }

    #[tokio::test]
    async fn update_alarms_timer_returns_internal_err_on_bad_connection() {
        test_query_returns_err(
            r#"
            mutation Alarms {
                updateAlarmTimer(device: "G:AMANDA", endTime: "2026-01-15T14:25:32.000Z", timerType: "test_type", updatedBy: "test_user") {
                    device
                    timerType
                    endTime
                    updatedBy
                    updatedAt
                }
            }
        "#,
            "code: 'Internal error', message: \"Could not connect to the database service. See server logs for details.\"",
        )
        .await;
    }
}
