use crate::g_rpc::proto::services::alarms::{
    alarm_group_service_client::AlarmGroupServiceClient, AlarmGroupMetadata,
    AlarmGroups, GroupsRequest,
};

use tonic::{Request, Status};

pub async fn read_metadata(
    request: Request<()>,
) -> Result<AlarmGroupMetadata, Status> {
    super::execute_with_client(
        AlarmGroupServiceClient::connect,
        |mut client| async move { client.get_group_metadata(request).await },
    )
    .await
}

pub async fn read_groups(
    request: Request<GroupsRequest>,
) -> Result<AlarmGroups, Status> {
    super::execute_with_client(
        AlarmGroupServiceClient::connect,
        |mut client| async move { client.get_groups(request).await },
    )
    .await
}
