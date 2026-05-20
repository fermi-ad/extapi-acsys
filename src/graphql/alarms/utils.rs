//! GraphQL Alarms Utilities
//!
//! Provides various utility functions that are useful in the context of alarms.

use crate::g_rpc::proto::google::protobuf::Timestamp;
use crate::g_rpc::proto::services::alarms::TimerType;
use chrono::{DateTime, Timelike, Utc};

pub fn datetime_to_timestamp(
    datetime: Option<DateTime<Utc>>,
) -> Option<Timestamp> {
    datetime.map(|dt| Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.nanosecond() as i32,
    })
}

pub fn string_to_timer_type(value: &str) -> TimerType {
    TimerType::from_str_name(value).unwrap_or(TimerType::Unknown)
}

pub fn timer_type_to_string(timer_type: i32) -> String {
    TimerType::try_from(timer_type)
        .unwrap_or(TimerType::Unknown)
        .as_str_name()
        .to_string()
}

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
