//! Alarms DB User Layouts Module
//!
//! Provides functions for interacting with alarms user layouts.

use crate::{
    config::GrpcConfig,
    g_rpc::{
        errors::handle_rpc_error,
        proto::{
            google::protobuf::Empty,
            services::alarm_user_layouts::{
                UserLayouts,
                user_layouts_service_client::UserLayoutsServiceClient,
            },
        },
    },
};
use rust_grpc_lib::auth::ForwardedToken;
use tonic::{Response, Status};

/// Requests all [`UserLayouts`] from the database.
pub async fn read_layouts(
    alarms_db_config: &GrpcConfig, token: ForwardedToken,
) -> Result<UserLayouts, Status> {
    let mut client = UserLayoutsServiceClient::from_endpoint_with_provider(
        &alarms_db_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "Alarms DB"))?;
    client
        .get_user_layouts(Empty {})
        .await
        .map(Response::into_inner)
}
