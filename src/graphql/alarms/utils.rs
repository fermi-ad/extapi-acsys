//! GraphQL Alarms Utilities
//!
//! Conversion helpers between protobuf wire types and GraphQL domain types:
//! - [`timer_type_to_string`] — converts a [`TimerType`] `i32` to its string name
//! - [`timestamp_to_datetime`] — converts a protobuf [`Timestamp`] to [`DateTime<Utc>`]

use crate::g_rpc::proto::google::protobuf::Timestamp;
use crate::g_rpc::proto::services::alarms::TimerType;
use chrono::{DateTime, Utc};

/// Converts a protobuf [`TimerType`] integer representation to its string name.
///
/// Unrecognised values are treated as [`TimerType::Unknown`].
pub fn timer_type_to_string(timer_type: i32) -> String {
    TimerType::try_from(timer_type)
        .unwrap_or(TimerType::Unknown)
        .as_str_name()
        .to_string()
}

/// Converts a protobuf [`Timestamp`] to a [`DateTime<Utc>`].
///
/// Returns `None` if `timestamp` is `None` or if the seconds/nanos
/// values cannot be represented as a valid [`DateTime`].
pub fn timestamp_to_datetime(
    timestamp: Option<Timestamp>,
) -> Option<DateTime<Utc>> {
    match timestamp {
        Some(ts) => DateTime::from_timestamp(ts.seconds, ts.nanos as u32),
        None => None,
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn timer_type_to_string_converts_properly() {
        assert_eq!(
            TimerType::Unknown.as_str_name(),
            timer_type_to_string(TimerType::Unknown as i32)
        );
        assert_eq!(
            TimerType::Snooze.as_str_name(),
            timer_type_to_string(TimerType::Snooze as i32)
        );
        assert_eq!(
            TimerType::BypassReminder.as_str_name(),
            timer_type_to_string(TimerType::BypassReminder as i32)
        );
        assert_eq!(
            TimerType::Unknown.as_str_name(),
            timer_type_to_string(9876)
        );
    }

    #[test]
    fn timestamp_to_datetime_converts_properly() {
        let ts = Timestamp {
            seconds: 1_234_567_890,
            nanos: 0,
        };
        let dt = timestamp_to_datetime(Some(ts)).unwrap();
        assert_eq!(
            dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            "2009-02-13T23:31:30.000Z"
        );

        assert!(timestamp_to_datetime(None).is_none());
    }
}
