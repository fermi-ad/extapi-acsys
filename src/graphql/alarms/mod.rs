//! Alarms GraphQL Module
//!
//! Provides the query implementations for the Alarms GraphQL interface.

use crate::{
    g_rpc::{alarms_db, alarms_svc},
    graphql::alarms::types::Alarm,
};
use async_graphql::{Error, Object, Subscription};
use chrono::{DateTime, Utc};
use rust_pubsub_lib::{KafkaSubscriber, StringMessage, Subscriber};
use tokio_stream::{Stream, StreamExt};
use tonic::{Code, Status};
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
        match alarms_db::timers::create(
            device, end_time, timer_type, updated_by,
        )
        .await
        {
            Ok(alarm_timer) => Ok(AlarmTimer::from(alarm_timer)),
            Err(e) => handle_error(e, "creating alarm timer"),
        }
    }

    /// A request to delete an alarms timer of the specified [`TimerType`](crate::g_rpc::proto::services::alarms::TimerType).
    async fn delete_alarm_timer(
        &self, device: String, timer_type: String,
    ) -> Result<String, Error> {
        match alarms_db::timers::delete(device.clone(), timer_type).await {
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

    /// A request to update an existing alarms timer of the specified
    /// [`TimerType`](crate::g_rpc::proto::services::alarms::TimerType).
    async fn update_alarm_timer(
        &self, device: String, end_time: Option<DateTime<Utc>>,
        timer_type: String, updated_by: String,
    ) -> Result<AlarmTimer, Error> {
        match alarms_db::timers::update(
            device, end_time, timer_type, updated_by,
        )
        .await
        {
            Ok(alarm_timer) => Ok(AlarmTimer::from(alarm_timer)),
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
        match alarms_db::groups::read_metadata().await {
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
        match alarms_db::groups::read_groups(groups).await {
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
        match alarms_db::layouts::read_layouts().await {
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
        match alarms_db::timers::read(timer_type, user).await {
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
pub struct AlarmsSubscriptions {
    host: String,
    topic: String,
}

impl AlarmsSubscriptions {
    pub fn new(host: String, topic: String) -> Self {
        Self { host, topic }
    }
}

#[Subscription]
impl AlarmsSubscriptions {
    /// Streams back all alarms from the alarms topic.
    async fn alarms(&self) -> impl Stream<Item = Alarm> {
        let stream =
            KafkaSubscriber::new(self.host.clone(), self.topic.clone())
                .get_stream::<StringMessage>()
                .await;

        stream.filter_map(|message| {
            Alarm::try_from(message)
                .inspect_err(|e| error!("{e:?}"))
                .ok()
        })
    }
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
    use std::time::Duration;

    use async_graphql::Schema;
    use rust_pubsub_lib::{
        KafkaPublisher, KafkaTestHarness, Message, Publisher,
    };
    use serde_json::json;
    use tokio::time::timeout;

    use crate::g_rpc::proto::common::alarm::status::{Severity, Source, State};

    use super::*;

    async fn test_query_returns_err(gql_query: &str, err_msg: &str) {
        let schema = Schema::build(
            AlarmsQueries,
            AlarmsMutations,
            AlarmsSubscriptions::default(),
        )
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

    #[tokio::test]
    async fn alarms_subscription_integration_test() {
        let (harness, topic) = KafkaTestHarness::with_new_topic("alarms").await;
        let host = harness.host().await;

        let schema = Schema::build(
            AlarmsQueries,
            AlarmsMutations,
            AlarmsSubscriptions::new(host.clone(), topic.clone()),
        )
        .finish();
        let mut stream = schema.execute_stream(
            r#"
            subscription {
                alarms {
                    device,
                    source,
                    state,
                }
            }
        "#,
        );

        let status = format!(
            r#"{{
            "device": "G:TEST",
            "source": {},
            "state": {},
            "severity": {},
            "user": "",
            "epics_type": "",
            "acknowledgeable": false
        }}"#,
            Source::Analog as i32,
            State::Ok as i32,
            Severity::Unknown as i32
        );

        let message =
            StringMessage::new(Some("G:TEST#Analog".to_string()), status);
        KafkaPublisher::new(host, topic)
            .publish(message)
            .await
            .expect("message should be sent successfully");

        let stream_content =
            timeout(Duration::from_millis(5000), stream.next())
                .await
                .expect("Message should arrive in reasonable time")
                .expect("The requested data should be provided");

        assert!(
            stream_content.is_ok(),
            "GraphQL errors: {:?}",
            stream_content.errors
        );

        let extracted_data = serde_json::to_value(&stream_content.data)
            .expect("GQL response data is deserializable");
        assert_eq!(
            extracted_data,
            json!({
                "alarms": {
                    "device": "G:TEST",
                    "source": "ANALOG",
                    "state": "OK"
                }
            })
        );
    }
}
