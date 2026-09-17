//! Timeline Generator gRPC Module

use crate::{
    config::GrpcConfig,
    g_rpc::{proto::google::protobuf::Empty, utils::handle_rpc_error},
};

use super::proto::services::tlg_placement::{
    TlgDevices, TlgPlacementResponse,
    tlg_placement_mutation_service_client::TlgPlacementMutationServiceClient,
    tlg_placement_service_client::TlgPlacementServiceClient,
};
use rust_grpc_lib::auth::ForwardedToken;
use tonic::{Response, Status};

pub async fn get_version(
    tlg_config: &GrpcConfig, token: ForwardedToken,
) -> Result<String, Status> {
    let mut client = TlgPlacementServiceClient::from_endpoint_with_provider(
        &tlg_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "TLG Service"))?;

    client
        .get_version(Empty {})
        .await
        .map(|v| v.into_inner().version)
}

pub async fn diagnostics(
    tlg_config: &GrpcConfig, token: ForwardedToken, devs: TlgDevices,
) -> Result<TlgPlacementResponse, Status> {
    let mut client =
        TlgPlacementMutationServiceClient::from_endpoint_with_provider(
            &tlg_config.host_addr,
            token,
        )
        .map_err(|err| handle_rpc_error(err, "TLG Mutation Service"))?;

    client
        .diagnostics_inline(devs)
        .await
        .map(Response::into_inner)
}

pub async fn placement(
    tlg_config: &GrpcConfig, token: ForwardedToken, devs: TlgDevices,
) -> Result<TlgPlacementResponse, Status> {
    let mut client =
        TlgPlacementMutationServiceClient::from_endpoint_with_provider(
            &tlg_config.host_addr,
            token,
        )
        .map_err(|err| handle_rpc_error(err, "TLG Mutation Service"))?;

    client
        .placement_inline(devs)
        .await
        .map(Response::into_inner)
}
