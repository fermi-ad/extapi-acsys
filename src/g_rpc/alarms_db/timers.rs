//! Alarms DB Timers Module
//!
//! Provides functions for interacting with alarms timers.

use crate::{
    config::GrpcConfig,
    g_rpc::{
        proto::{
            google::protobuf::{Empty, Timestamp},
            services::alarm_timers::{
                AlarmTimer, AlarmTimers, DeleteRequest, ReadRequest, TimerType,
                alarm_timer_service_client::AlarmTimerServiceClient,
            },
        },
        utils::handle_rpc_error,
    },
};
use chrono::{DateTime, Timelike, Utc};
use rust_grpc_lib::auth::ForwardedToken;
use tonic::{Response, Status};

/// Creates a new [`AlarmTimer`] in the database.
///
/// `timer_type` must be a valid [`TimerType`] protobuf enum name
/// (e.g. `"TimerType_SNOOZE"`). Unrecognised values are silently
/// treated as [`TimerType::Unknown`].
///
/// `updated_at` is set to the current UTC time at the call site.
///
/// Returns the created [`AlarmTimer`] as submitted (the server
/// responds with `Empty`; no server-side fields are reflected back).
pub async fn create(
    alarms_db_config: &GrpcConfig, token: ForwardedToken, device: String,
    end_time: Option<DateTime<Utc>>, timer_type: String, updated_by: String,
) -> Result<AlarmTimer, Status> {
    let mut client = AlarmTimerServiceClient::from_endpoint_with_provider(
        &alarms_db_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "Alarms DB"))?;

    let timer = AlarmTimer {
        device,
        end_time: datetime_to_timestamp(end_time),
        timer_type: string_to_timer_type(&timer_type) as i32,
        updated_at: datetime_to_timestamp(Some(Utc::now())),
        updated_by,
    };
    let returned_copy = timer.clone();

    client.create(timer).await.map(|_| returned_copy)
}

/// Deletes the specified [`AlarmTimer`] from the database.
///
/// `timer_type` must be a valid [`TimerType`] protobuf enum name
/// (e.g. `"TimerType_SNOOZE"`). Unrecognised values are silently
/// treated as [`TimerType::Unknown`].
pub async fn delete(
    alarms_db_config: &GrpcConfig, token: ForwardedToken, device: String,
    timer_type: String,
) -> Result<Empty, Status> {
    let mut client = AlarmTimerServiceClient::from_endpoint_with_provider(
        &alarms_db_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "Alarms DB"))?;

    let request = DeleteRequest {
        device,
        timer_type: string_to_timer_type(&timer_type) as i32,
    };

    client.delete(request).await.map(Response::into_inner)
}

/// Reads all [`AlarmTimers`] of the specified [`TimerType`] for a given user.
pub async fn read(
    alarms_db_config: &GrpcConfig, token: ForwardedToken, timer_type: String,
    user: String,
) -> Result<AlarmTimers, Status> {
    let mut client = AlarmTimerServiceClient::from_endpoint_with_provider(
        &alarms_db_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "Alarms DB"))?;

    let request = ReadRequest {
        timer_type: string_to_timer_type(&timer_type) as i32,
        user,
    };

    client.read(request).await.map(Response::into_inner)
}

/// Updates an [`AlarmTimer`] in the database.
///
/// `timer_type` must be a valid [`TimerType`] protobuf enum name
/// (e.g. `"TimerType_SNOOZE"`). Unrecognised values are silently
/// treated as [`TimerType::Unknown`].
///
/// `updated_at` is set to the current UTC time at the call site.
///
/// Returns the updated [`AlarmTimer`] as submitted (the server
/// responds with `Empty`; no server-side fields are reflected back).
pub async fn update(
    alarms_db_config: &GrpcConfig, token: ForwardedToken, device: String,
    end_time: Option<DateTime<Utc>>, timer_type: String, updated_by: String,
) -> Result<AlarmTimer, Status> {
    let mut client = AlarmTimerServiceClient::from_endpoint_with_provider(
        &alarms_db_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "Alarms DB"))?;

    let timer = AlarmTimer {
        device,
        end_time: datetime_to_timestamp(end_time),
        timer_type: string_to_timer_type(&timer_type) as i32,
        updated_at: datetime_to_timestamp(Some(Utc::now())),
        updated_by,
    };
    let returned_copy = timer.clone();

    client.update(timer).await.map(|_| returned_copy)
}

fn datetime_to_timestamp(datetime: Option<DateTime<Utc>>) -> Option<Timestamp> {
    datetime.map(|dt| Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.nanosecond() as i32,
    })
}

fn string_to_timer_type(value: &str) -> TimerType {
    TimerType::from_str_name(value).unwrap_or(TimerType::Unknown)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datetime_to_timestamp_converts_properly() {
        let ts = Timestamp {
            seconds: 1_234_567_890,
            nanos: 0,
        };
        let dt = DateTime::parse_from_rfc3339("2009-02-13T23:31:30.000Z")
            .unwrap()
            .to_utc();
        let result = datetime_to_timestamp(Some(dt)).unwrap();
        assert_eq!(result, ts);

        assert!(datetime_to_timestamp(None).is_none());
    }

    #[test]
    fn string_to_timer_type_converts_properly() {
        assert_eq!(
            TimerType::BypassReminder,
            string_to_timer_type("TimerType_BYPASS_REMINDER")
        );
        assert_eq!(TimerType::Snooze, string_to_timer_type("TimerType_SNOOZE"));
        assert_eq!(
            TimerType::Unknown,
            string_to_timer_type("Not an instance of TimerType")
        );
    }
}
