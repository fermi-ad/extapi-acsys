//! Alarms DB Alarm Groups Module
//!
//! Provides functions for interacting with alarms groups.

use crate::g_rpc::{
    alarms_db::AlarmsDbConnectionAdapter,
    proto::services::alarms::{AlarmGroupMetadata, AlarmGroups, GroupsRequest},
};
use tonic::{Request, Status};

/// Requests all [`AlarmGroupMetadata`] from the database.
pub async fn read_metadata(
    request: Request<()>,
) -> Result<AlarmGroupMetadata, Status> {
    let do_read = |mut client: AlarmsDbConnectionAdapter| async move {
        client.groups_conn.get_group_metadata(request).await
    };
    super::ALARMS_DB_CLIENT.run_with_client(do_read).await
}

/// Requests the [`AlarmGroups`] data for the specified groups from the database.
pub async fn read_groups(
    request: Request<GroupsRequest>,
) -> Result<AlarmGroups, Status> {
    let do_read = |mut client: AlarmsDbConnectionAdapter| async move {
        client.groups_conn.get_groups(request).await
    };
    super::ALARMS_DB_CLIENT.run_with_client(do_read).await
}
