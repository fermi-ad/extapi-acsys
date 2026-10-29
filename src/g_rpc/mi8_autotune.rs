//! Authenticated client adapter for the MI8 Autotune gRPC service.

use rust_grpc_lib::auth::ForwardedToken;
use tonic::{Response, Status, Streaming};

use crate::{
    config::GrpcConfig,
    g_rpc::{
        errors::handle_rpc_error,
        proto::{
            google::protobuf::Empty,
            services::mi8_autotune::v1::{
                ListBpmsResponse, SubscribeArmedStateResponse,
                SubscribeAutotuneEventResponse, SubscribeOperatingModeResponse,
                autotune_service_client::AutotuneServiceClient,
            },
        },
    },
};

/// Returns the BPM names that the autotuner is configured to read.
pub async fn list_bpms(
    config: &GrpcConfig, token: ForwardedToken,
) -> Result<ListBpmsResponse, Status> {
    let mut client = AutotuneServiceClient::from_endpoint_with_provider(
        &config.host_addr,
        token,
    )
    .map_err(|error| handle_rpc_error(error, "MI8 Autotune Service"))?;

    client.list_bpms(Empty {}).await.map(Response::into_inner)
}

/// Streams the current armed state followed by upstream updates.
pub async fn subscribe_armed_state(
    config: &GrpcConfig, token: ForwardedToken,
) -> Result<Streaming<SubscribeArmedStateResponse>, Status> {
    let mut client = AutotuneServiceClient::from_endpoint_with_provider(
        &config.host_addr,
        token,
    )
    .map_err(|error| handle_rpc_error(error, "MI8 Autotune Service"))?;

    client
        .subscribe_armed_state(Empty {})
        .await
        .map(Response::into_inner)
}

/// Streams the current operating mode followed by upstream updates.
pub async fn subscribe_operating_mode(
    config: &GrpcConfig, token: ForwardedToken,
) -> Result<Streaming<SubscribeOperatingModeResponse>, Status> {
    let mut client = AutotuneServiceClient::from_endpoint_with_provider(
        &config.host_addr,
        token,
    )
    .map_err(|error| handle_rpc_error(error, "MI8 Autotune Service"))?;

    client
        .subscribe_operating_mode(Empty {})
        .await
        .map(Response::into_inner)
}

/// Streams the current autotune event and message followed by upstream updates.
pub async fn subscribe_events(
    config: &GrpcConfig, token: ForwardedToken,
) -> Result<Streaming<SubscribeAutotuneEventResponse>, Status> {
    let mut client = AutotuneServiceClient::from_endpoint_with_provider(
        &config.host_addr,
        token,
    )
    .map_err(|error| handle_rpc_error(error, "MI8 Autotune Service"))?;

    client
        .subscribe_events(Empty {})
        .await
        .map(Response::into_inner)
}
