//! Alarms Types Module
//!
//! Describes GraphQL-friendly types to be used in the GraphQL Alarms Module.

use crate::{
    g_rpc::proto::{
        common::alarm::{
            Status,
            status::{Severity, Source, State},
        },
        services::alarms::{
            AlarmGroup as ProtoAlarmGroup,
            AlarmGroupMetadatum as ProtoGroupMetadatum,
            AlarmTimer as ProtoAlarmTimer, UserLayout as ProtoUserLayout,
        },
    },
    graphql::alarms::utils,
};
use async_graphql::SimpleObject;
use chrono::{DateTime, Utc};

#[derive(Clone, Debug, PartialEq, SimpleObject)]
pub struct Alarm {
    pub device: String,
    pub source: Source,
    pub state: State,
    pub severity: Severity,
    pub acknowledgeable: bool,
    pub time: Option<DateTime<Utc>>,
    pub epics_type: String,
    pub user: String,
    pub wake: Option<DateTime<Utc>>,
}

impl From<Status> for Alarm {
    fn from(value: Status) -> Self {
        let source = value.source();
        let state = value.state();
        let severity = value.severity();
        Self {
            device: value.device,
            source,
            state,
            severity,
            acknowledgeable: value.acknowledgeable,
            time: utils::timestamp_to_datetime(value.time),
            epics_type: value.epics_type,
            user: value.user,
            wake: utils::timestamp_to_datetime(value.wake),
        }
    }
}

#[derive(Clone, Debug, PartialEq, SimpleObject)]
pub struct AlarmGroup {
    pub metadata: Option<AlarmGroupMetadatum>,
    pub devices: Vec<String>,
    pub groups: Vec<String>,
}

impl From<ProtoAlarmGroup> for AlarmGroup {
    fn from(value: ProtoAlarmGroup) -> Self {
        AlarmGroup {
            metadata: value.metadata.map(AlarmGroupMetadatum::from),
            devices: value.devices,
            groups: value.groups,
        }
    }
}

#[derive(Clone, Debug, PartialEq, SimpleObject)]
pub struct AlarmGroupMetadatum {
    pub description: String,
    pub is_user_category: bool,
    pub name: String,
    pub updated_at: Option<DateTime<Utc>>,
    pub updated_by: String,
}

impl From<ProtoGroupMetadatum> for AlarmGroupMetadatum {
    fn from(value: ProtoGroupMetadatum) -> Self {
        AlarmGroupMetadatum {
            description: value.description,
            is_user_category: value.is_user_category,
            name: value.name,
            updated_at: utils::timestamp_to_datetime(value.updated_at),
            updated_by: value.updated_by,
        }
    }
}

#[derive(Clone, Debug, PartialEq, SimpleObject)]
pub struct AlarmTimer {
    pub device: String,
    pub timer_type: String,
    pub end_time: Option<DateTime<Utc>>,
    pub updated_by: String,
    pub updated_at: Option<DateTime<Utc>>,
}

impl From<ProtoAlarmTimer> for AlarmTimer {
    fn from(value: ProtoAlarmTimer) -> Self {
        AlarmTimer {
            device: value.device,
            timer_type: utils::timer_type_to_string(value.timer_type),
            end_time: utils::timestamp_to_datetime(value.end_time),
            updated_at: utils::timestamp_to_datetime(value.updated_at),
            updated_by: value.updated_by,
        }
    }
}

#[derive(Clone, Debug, PartialEq, SimpleObject)]
pub struct UserLayout {
    pub user_name: String,
    pub groups: Vec<String>,
}

impl From<ProtoUserLayout> for UserLayout {
    fn from(value: ProtoUserLayout) -> Self {
        UserLayout {
            user_name: value.user_name,
            groups: value.groups,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::g_rpc::proto::services::alarms::TimerType;
    use prost_types::Timestamp;

    #[test]
    fn alarm_from_proto_to_gql() {
        let status = Status {
            device: "M:BEAM".to_string(),
            source: Source::Analog as i32,
            state: State::Ok as i32,
            severity: Severity::Unknown as i32,
            acknowledgeable: false,
            time: utils::datetime_to_timestamp(Some(Utc::now())),
            epics_type: String::new(),
            user: "test user".to_string(),
            wake: None,
        };

        let output = Alarm::from(status.clone());

        assert_eq!(status.device, output.device);
        assert_eq!(status.source(), output.source);
        assert_eq!(status.state(), output.state);
        assert_eq!(status.severity(), output.severity);
        assert_eq!(status.acknowledgeable, output.acknowledgeable);
        assert_eq!(status.time, utils::datetime_to_timestamp(output.time));
        assert_eq!(status.epics_type, output.epics_type);
        assert_eq!(status.user, output.user);
        assert_eq!(status.wake, utils::datetime_to_timestamp(output.wake));
    }

    #[test]
    fn group_from_proto_to_gql() {
        let test_name = "device A";
        let test_desc = "desc";
        let test_seconds = 100;
        let test_nanos = 3242;
        let test_user = "user 1";
        let proto_meta = ProtoGroupMetadatum {
            name: test_name.to_string(),
            description: test_desc.to_string(),
            is_user_category: true,
            updated_at: Some(Timestamp {
                seconds: test_seconds,
                nanos: test_nanos,
            }),
            updated_by: test_user.to_string(),
        };
        let test_devices =
            vec![String::from("device 1"), String::from("device 2")];
        let test_groups = vec![String::from("group 1")];
        let proto = ProtoAlarmGroup {
            metadata: Some(proto_meta.clone()),
            devices: test_devices.clone(),
            groups: test_groups.clone(),
        };

        let result = AlarmGroup::from(proto);

        assert_eq!(result.devices, test_devices);
        assert_eq!(result.groups, test_groups);
        assert_eq!(
            result.metadata.unwrap(),
            AlarmGroupMetadatum::from(proto_meta)
        );
    }

    #[test]
    fn metadatum_from_proto_to_gql() {
        let test_name = "device A";
        let test_desc = "desc";
        let test_seconds = 100;
        let test_nanos = 3242;
        let test_user = "user 1";
        let proto = ProtoGroupMetadatum {
            name: test_name.to_string(),
            description: test_desc.to_string(),
            is_user_category: true,
            updated_at: Some(Timestamp {
                seconds: test_seconds,
                nanos: test_nanos,
            }),
            updated_by: test_user.to_string(),
        };

        let result = AlarmGroupMetadatum::from(proto);

        assert_eq!(result.name, test_name);
        assert_eq!(result.description, test_desc);
        assert!(result.is_user_category);
        assert_eq!(
            result.updated_at,
            DateTime::from_timestamp(test_seconds, test_nanos as u32)
        );
        assert_eq!(result.updated_by, test_user);
    }

    #[test]
    fn timer_from_proto_to_gql() {
        let test_device = "device A";
        let test_seconds = 100;
        let test_nanos = 3242;
        let test_user = "user 1";
        let proto = ProtoAlarmTimer {
            device: test_device.to_string(),
            end_time: Some(Timestamp {
                seconds: test_seconds,
                nanos: test_nanos,
            }),
            timer_type: TimerType::BypassReminder as i32,
            updated_at: None,
            updated_by: test_user.to_string(),
        };

        let result = AlarmTimer::from(proto);

        assert_eq!(result.device, test_device);
        assert_eq!(
            result.end_time,
            DateTime::from_timestamp(test_seconds, test_nanos as u32)
        );
        assert_eq!(result.timer_type, TimerType::BypassReminder.as_str_name());
        assert_eq!(result.updated_at, None);
        assert_eq!(result.updated_by, test_user);
    }

    #[test]
    fn user_layout_from_proto_to_gql() {
        let test_user = "user1";
        let grp1 = "group 1";
        let grp2 = "group 2";
        let test_groups = vec![grp1.to_string(), grp2.to_string()];
        let proto_layout = ProtoUserLayout {
            user_name: test_user.to_string(),
            groups: test_groups.clone(),
        };

        let result = UserLayout::from(proto_layout);

        assert_eq!(result.user_name, test_user);
        assert_eq!(result.groups, test_groups);
    }
}
