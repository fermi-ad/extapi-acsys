//! Alarms DB Timers Module
//!
//! Provides functions for interacting with alarms timers.

use crate::g_rpc::{
    alarms_db::AlarmsDbConnectionAdapter,
    proto::{
        google::protobuf::{Empty, Timestamp},
        services::alarms::{
            AlarmTimer, AlarmTimers, DeleteRequest, ReadRequest, TimerType,
        },
    },
};
use chrono::{DateTime, Timelike, Utc};
use tonic::Status;

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
    device: String, end_time: Option<DateTime<Utc>>, timer_type: String,
    updated_by: String,
) -> Result<AlarmTimer, Status> {
    let timer = AlarmTimer {
        device,
        end_time: datetime_to_timestamp(end_time),
        timer_type: string_to_timer_type(&timer_type) as i32,
        updated_at: datetime_to_timestamp(Some(Utc::now())),
        updated_by,
    };
    let returned_copy = timer.clone();
    let do_create = |mut client: AlarmsDbConnectionAdapter| async move {
        client.timers_conn.create(timer).await
    };
    super::ALARMS_DB_CLIENT
        .run_with_client(do_create)
        .await
        .map(|_| returned_copy)
}

/// Deletes the specified [`AlarmTimer`] from the database.
///
/// `timer_type` must be a valid [`TimerType`] protobuf enum name
/// (e.g. `"TimerType_SNOOZE"`). Unrecognised values are silently
/// treated as [`TimerType::Unknown`].
pub async fn delete(
    device: String, timer_type: String,
) -> Result<Empty, Status> {
    let request = DeleteRequest {
        device,
        timer_type: string_to_timer_type(&timer_type) as i32,
    };
    let do_delete = |mut client: AlarmsDbConnectionAdapter| async move {
        client.timers_conn.delete(request).await
    };
    super::ALARMS_DB_CLIENT.run_with_client(do_delete).await
}

/// Reads all [`AlarmTimers`] of the specified [`TimerType`] for a given user.
pub async fn read(
    timer_type: String, user: String,
) -> Result<AlarmTimers, Status> {
    let request = ReadRequest {
        timer_type: string_to_timer_type(&timer_type) as i32,
        user,
    };
    let do_read = |mut client: AlarmsDbConnectionAdapter| async move {
        client.timers_conn.read(request).await
    };
    super::ALARMS_DB_CLIENT.run_with_client(do_read).await
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
    device: String, end_time: Option<DateTime<Utc>>, timer_type: String,
    updated_by: String,
) -> Result<AlarmTimer, Status> {
    let timer = AlarmTimer {
        device,
        end_time: datetime_to_timestamp(end_time),
        timer_type: string_to_timer_type(&timer_type) as i32,
        updated_at: datetime_to_timestamp(Some(Utc::now())),
        updated_by,
    };
    let returned_copy = timer.clone();
    let do_update = |mut client: AlarmsDbConnectionAdapter| async move {
        client.timers_conn.update(timer).await
    };
    super::ALARMS_DB_CLIENT
        .run_with_client(do_update)
        .await
        .map(|_| returned_copy)
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
