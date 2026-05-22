//! Alarms DB Alarm Groups Module
//!
//! Provides functions for interacting with alarms groups.

use crate::g_rpc::{
    alarms_db::AlarmsDbConnectionAdapter,
    proto::{
        google::protobuf::Empty,
        services::alarms::{AlarmGroupMetadata, AlarmGroups, GroupsRequest},
    },
};
use tonic::{Response, Status};

/// Requests all [`AlarmGroupMetadata`] from the database.
pub async fn read_metadata() -> Result<AlarmGroupMetadata, Status> {
    super::ALARMS_DB_CLIENT
        .run_with_client(get_group_metadata)
        .await
}

/// Requests the [`AlarmGroups`] data for the specified groups from the database.
pub async fn read_groups(groups: Vec<String>) -> Result<AlarmGroups, Status> {
    let do_read = |mut client: AlarmsDbConnectionAdapter| async move {
        client
            .groups_conn
            .get_groups(GroupsRequest { groups })
            .await
    };
    super::ALARMS_DB_CLIENT.run_with_client(do_read).await
}

async fn get_group_metadata(
    mut client: AlarmsDbConnectionAdapter,
) -> Result<Response<AlarmGroupMetadata>, Status> {
    client.groups_conn.get_group_metadata(Empty {}).await
}
