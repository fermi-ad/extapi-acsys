//! Alarms GraphQL Module
//!
//! Provides the query implementations for the Alarms GraphQL interface.

use crate::{
    g_rpc::{
        alarms_db, alarms_svc,
        proto::{google::protobuf::Empty, services::alarms},
    },
    graphql::alarms::types::Alarm,
    pubsub::{Subscriber, kafka_impl::KafkaSubscriber},
};
use async_graphql::{Error, Object, Subscription};
use chrono::{DateTime, Utc};
use rust_env_var_lib::env_var;
use tokio_stream::{Stream, StreamExt};
use tonic::{Code, Request, Status};
use tracing::error;
use types::{AlarmGroup, AlarmGroupMetadatum, AlarmTimer, UserLayout};
use uuid::Uuid;

mod types;
mod utils;

/// Describes the mutations (data writes/updates) allowed by the GQL interface.
#[derive(Default)]
pub struct AlarmsMutations;
#[Object]
impl AlarmsMutations {
    /// A request to acknowledge the specified alarms.
    async fn acknowledge_alarms(
        &self, devices: Vec<String>, updated_by: String,
    ) -> Result<Vec<String>, Error> {
        match alarms_svc::acknowledge_alarms(devices.clone(), updated_by).await
        {
            Ok(_) => Ok(devices),
            Err(e) => handle_error(e, "acknowledging alarms"),
        }
    }

    /// A request to activate (unbypass) the specified alarms.
    async fn activate_alarms(
        &self, devices: Vec<String>, updated_by: String,
    ) -> Result<Vec<String>, Error> {
        match alarms_svc::activate_alarms(devices.clone(), updated_by).await {
            Ok(_) => Ok(devices),
            Err(e) => handle_error(e, "activating alarms"),
        }
    }

    /// A request to bypass the specified alarms.
    async fn bypass_alarms(
        &self, devices: Vec<String>, updated_by: String,
    ) -> Result<Vec<String>, Error> {
        match alarms_svc::bypass_alarms(devices.clone(), updated_by).await {
            Ok(_) => Ok(devices),
            Err(e) => handle_error(e, "bypassing alarms"),
        }
    }

    /// A request to create an alarms timer of the specified [`TimerType`](crate::g_rpc::proto::services::alarms::TimerType).
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

    /// A request to delete an alarms timer of the specified [`TimerType`](crate::g_rpc::proto::services::alarms::TimerType).
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

    /// A request to snooze the specified alarms.
    async fn snooze_alarms(
        &self, devices: Vec<String>, updated_by: String, wake: DateTime<Utc>,
    ) -> Result<Vec<String>, Error> {
        match alarms_svc::snooze_alarms(devices.clone(), updated_by, wake).await
        {
            Ok(_) => Ok(devices),
            Err(e) => handle_error(e, "snoozing alarms"),
        }
    }

    /// A request to update an alarms timer.
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

/// Describes the various queries (data reads) related to alarms.
#[derive(Default)]
pub struct AlarmsQueries;
#[Object]
impl AlarmsQueries {
    /// Reads all [`AlarmGroupMetadatum`] items in the database.
    async fn alarms_group_metadata(
        &self,
    ) -> Result<Vec<AlarmGroupMetadatum>, Error> {
        match alarms_db::groups::read_metadata(Request::new(Empty {})).await {
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

    /// Reads the [`AlarmGroup`] data for specified groups.
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

    /// Reads all [`UserLayout`]s in the database.
    async fn alarms_user_layouts(&self) -> Result<Vec<UserLayout>, Error> {
        match alarms_db::layouts::read_layouts(Request::new(Empty {})).await {
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

    /// Reads a snapshot of the alarms topic.
    async fn alarms_snapshot(&self) -> Result<Vec<Alarm>, Error> {
        alarms_svc::get_snapshot()
            .await
            .map(|statuses| statuses.into_iter().map(Alarm::from).collect())
            .or_else(|e| handle_error(e, "getting alarm snapshot"))
    }

    /// Reads all alarms timers of the specified [`TimerType`](crate::g_rpc::proto::services::alarms::TimerType) for the given user.
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

/// Describes long-lived data streams for alarms.
#[derive(Default)]
pub struct AlarmsSubscriptions;

#[Subscription]
impl AlarmsSubscriptions {
    /// Streams back all alarms from the alarms topic.
    async fn alarms(&self) -> Result<impl Stream<Item = Alarm>, Error> {
        KafkaSubscriber::subscribe(get_host(), get_topic())
            .await
            .map(|stream| {
                stream.filter_map(|stream_item| match stream_item {
                    Err(e) => {
                        error!("{e:?}");
                        None
                    }
                    Ok(message) => Alarm::try_from(message)
                        .inspect_err(|e| error!("{e:?}"))
                        .ok(),
                })
            })
            .map_err(|e| Error::new(format!("{e}")))
    }
}

const ALARMS_KAFKA_HOST: &str = "ALARMS_KAFKA_HOST";
fn get_host() -> String {
    env_var::expect(ALARMS_KAFKA_HOST)
}

const ALARMS_KAFKA_TOPIC: &str = "ALARMS_KAFKA_TOPIC";
fn get_topic() -> String {
    env_var::expect(ALARMS_KAFKA_TOPIC)
}

fn handle_error<T>(e: Status, gerund: &str) -> Result<T, Error> {
    let err_id = Uuid::new_v4();
    error!("{err_id} gRPC Error {gerund}: {e:?}");
    Err(match e.code() {
        Code::InvalidArgument => {
            Error::new(format!("{e} (Error ID: {err_id})"))
        }
        _ => Error::new(format!(
            "Error {gerund}. See server logs for details. (Error ID: {err_id})"
        )),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_graphql::Schema;

    async fn test_query_returns_err(gql_query: &str, err_msg: &str) {
        let schema =
            Schema::build(AlarmsQueries, AlarmsMutations, AlarmsSubscriptions)
                .finish();
        let result = schema.execute(gql_query).await;
        let err = result.errors.first().unwrap();
        println!("{err}");
        assert!(err.message.starts_with(err_msg));
    }

    #[tokio::test]
    async fn acknowledge_alarms_returns_internal_err_on_bad_connection() {
        test_query_returns_err(
            r#"
            mutation Alarms {
                acknowledgeAlarms(devices: ["G:AMANDA"], updatedBy: "test user")
            }
        "#,
            "Error acknowledging alarms. See server logs for details. (Error ID: ",
        )
        .await;
    }

    #[tokio::test]
    async fn activate_alarms_returns_internal_err_on_bad_connection() {
        test_query_returns_err(
            r#"
            mutation Alarms {
                activateAlarms(devices: ["G:AMANDA"], updatedBy: "test user")
            }
        "#,
            "Error activating alarms. See server logs for details. (Error ID: ",
        )
        .await;
    }

    #[tokio::test]
    async fn bypass_alarms_returns_internal_err_on_bad_connection() {
        test_query_returns_err(
            r#"
            mutation Alarms {
                bypassAlarms(devices: ["G:AMANDA"], updatedBy: "test user")
            }
        "#,
            "Error bypassing alarms. See server logs for details. (Error ID: ",
        )
        .await;
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
            "Error creating alarm timer. See server logs for details. (Error ID: ",
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
            "Error deleting alarm timer. See server logs for details. (Error ID: ",
        )
        .await;
    }

    #[tokio::test]
    async fn get_alarms_snapshot_returns_err_when_bad_address() {
        test_query_returns_err(
            r#"
            query Alarms {
                alarmsSnapshot {
                  acknowledgeable,
                  device,
                  epicsType,
                  severity,
                  source,
                  state,
                  time,
                  user,
                  wake,
                }
            }
        "#,
            "Error getting alarm snapshot. See server logs for details. (Error ID: ",
        )
        .await;
    }

    #[test]
    fn handles_invalid_arg_err() {
        let result = handle_error::<()>(
            Status::invalid_argument("test invalid arg"),
            "testing alarm timer",
        );
        assert!(
            result.unwrap_err().message.starts_with(
            "code: 'Client specified an invalid argument', message: \"test invalid arg\"")
        );

        let result = handle_error::<()>(
            Status::internal("test internal err"),
            "testing alarm timer",
        );
        assert!(result.unwrap_err().message.starts_with(
            "Error testing alarm timer. See server logs for details. (Error ID: "
        ));
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
            "Error reading alarm timer. See server logs for details. (Error ID: ",
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
            "Error reading alarm group metadata. See server logs for details. (Error ID: ",
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
            "Error reading alarm groups. See server logs for details. (Error ID: ",
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
            "Error reading user layouts. See server logs for details. (Error ID: ",
        )
        .await;
    }

    #[tokio::test]
    async fn snooze_alarms_returns_internal_err_on_bad_connection() {
        test_query_returns_err(
            r#"
            mutation Alarms {
                snoozeAlarms(devices: ["G:AMANDA"], updatedBy: "test user", wake: "2026-03-24T15:17:32.000Z")
            }
        "#,
            "Error snoozing alarms. See server logs for details. (Error ID: ",
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
            "Error updating alarm timer. See server logs for details. (Error ID: ",
        )
        .await;
    }
}
