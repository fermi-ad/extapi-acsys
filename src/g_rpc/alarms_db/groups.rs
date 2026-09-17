//! Alarms DB Alarm Groups Module
//!
//! Provides functions for interacting with alarms groups.

use crate::{
    config::GrpcConfig,
    g_rpc::{
        proto::{
            google::protobuf::Empty,
            services::alarm_groups::{
                AlarmGroupMetadata, AlarmGroups, GroupsRequest,
                alarm_group_service_client::AlarmGroupServiceClient,
            },
        },
        utils::handle_rpc_error,
    },
};
use rust_grpc_lib::auth::ForwardedToken;
use tonic::{Response, Status};

/// Requests all [`AlarmGroupMetadata`] from the database.
pub async fn read_metadata(
    alarms_db_config: &GrpcConfig, token: ForwardedToken,
) -> Result<AlarmGroupMetadata, Status> {
    let mut client = AlarmGroupServiceClient::from_endpoint_with_provider(
        &alarms_db_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "Alarms DB"))?;
    client
        .get_group_metadata(Empty {})
        .await
        .map(Response::into_inner)
}

/// Requests the [`AlarmGroups`] data for the specified groups from the database.
pub async fn read_groups(
    alarms_db_config: &GrpcConfig, token: ForwardedToken, groups: Vec<String>,
) -> Result<AlarmGroups, Status> {
    let mut client = AlarmGroupServiceClient::from_endpoint_with_provider(
        &alarms_db_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "Alarms DB"))?;
    client
        .get_groups(GroupsRequest { groups })
        .await
        .map(Response::into_inner)
}
