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
    pubsub::Message,
};
use async_graphql::SimpleObject;
use chrono::{DateTime, Utc};

#[cfg(test)]
mod tests;

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

impl TryFrom<Message> for Alarm {
    type Error = serde_json::Error;

    fn try_from(value: Message) -> Result<Self, Self::Error> {
        serde_json::from_str::<Status>(&value.value).map(Alarm::from)
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
