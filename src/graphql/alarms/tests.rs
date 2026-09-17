use async_graphql::Schema;
#[cfg(feature = "kafka")]
use rust_pubsub_lib::{KafkaPublisher, KafkaTestHarness, Message, Publisher};
#[cfg(feature = "kafka")]
use serde_json::json;
#[cfg(feature = "kafka")]
use std::time::Duration;
#[cfg(feature = "kafka")]
use tokio::time::timeout;

#[cfg(feature = "kafka")]
use crate::g_rpc::proto::common::alarm::status::{Severity, Source, State};

use super::*;

fn test_config() -> AlarmConfigWrapper {
    AlarmConfigWrapper {
        alarms_db: GrpcConfig {
            host_addr: "".into(),
        },
        alarms_kafka: KafkaConfig {
            host_addr: "".into(),
            topic: "".into(),
        },
        alarms_svc: GrpcConfig {
            host_addr: "".into(),
        },
    }
}

#[cfg(feature = "kafka")]
fn test_populated_config(
    kafka_host: String, kafka_topic: String,
) -> AlarmConfigWrapper {
    AlarmConfigWrapper {
        alarms_db: GrpcConfig {
            host_addr: "".into(),
        },
        alarms_kafka: KafkaConfig {
            host_addr: kafka_host,
            topic: kafka_topic,
        },
        alarms_svc: GrpcConfig {
            host_addr: "".into(),
        },
    }
}

async fn test_query_returns_err(gql_query: &str, err_msg: &str) {
    #[cfg(feature = "kafka")]
    let subscription = AlarmsSubscriptions;
    #[cfg(not(feature = "kafka"))]
    let subscription = async_graphql::EmptySubscription;

    let schema = Schema::build(AlarmsQueries, AlarmsMutations, subscription)
        .data(test_config())
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

#[cfg(feature = "kafka")]
#[tokio::test]
async fn alarms_subscription_integration_test() {
    let (harness, topic) = KafkaTestHarness::with_new_topic("alarms").await;
    let host = harness.host().await;

    let schema =
        Schema::build(AlarmsQueries, AlarmsMutations, AlarmsSubscriptions)
            .data(test_populated_config(host.clone(), topic.clone()))
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

    let message = StringMessage::new(Some("G:TEST#Analog".to_string()), status);
    KafkaPublisher::new(host, topic)
        .publish(message)
        .await
        .expect("message should be sent successfully");

    let stream_content = timeout(Duration::from_millis(5000), stream.next())
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
