//! Authenticated client adapter for the MI8 Autotune gRPC service.

use rust_grpc_lib::auth::{ClientJwtInterceptor, ForwardedToken};
use tonic::{
    Response, Status, Streaming, service::interceptor::InterceptedService,
    transport::Channel,
};

use crate::{
    config::GrpcConfig,
    g_rpc::{
        errors::handle_rpc_error,
        proto::{
            google::protobuf::Empty,
            services::mi8_autotune::v1::{
                ListBpmsResponse, SetArmedStateRequest,
                SetOperatingModeRequest, SubscribeArmedStateResponse,
                SubscribeAutotuneEventResponse, SubscribeOperatingModeResponse,
                autotune_service_client::AutotuneServiceClient,
            },
        },
    },
};

fn client(
    config: &GrpcConfig, token: ForwardedToken,
) -> Result<
    AutotuneServiceClient<
        InterceptedService<Channel, ClientJwtInterceptor<ForwardedToken>>,
    >,
    Status,
> {
    AutotuneServiceClient::from_endpoint_with_provider(&config.host_addr, token)
        .map_err(|error| handle_rpc_error(error, "MI8 Autotune Service"))
}

/// Returns the BPM names that the autotuner is configured to read.
pub async fn list_bpms(
    config: &GrpcConfig, token: ForwardedToken,
) -> Result<ListBpmsResponse, Status> {
    let mut client = client(config, token)?;

    client.list_bpms(Empty {}).await.map(Response::into_inner)
}

/// Requests an armed-state change using the caller's credentials.
pub async fn set_armed_state(
    config: &GrpcConfig, token: ForwardedToken, request: SetArmedStateRequest,
) -> Result<Empty, Status> {
    let mut client = client(config, token)?;

    client
        .set_armed_state(request)
        .await
        .map(Response::into_inner)
}

/// Requests an operating-mode change using the caller's credentials.
pub async fn set_operating_mode(
    config: &GrpcConfig, token: ForwardedToken,
    request: SetOperatingModeRequest,
) -> Result<Empty, Status> {
    let mut client = client(config, token)?;

    client
        .set_operating_mode(request)
        .await
        .map(Response::into_inner)
}

/// Streams the current armed state followed by upstream updates.
pub async fn subscribe_armed_state(
    config: &GrpcConfig, token: ForwardedToken,
) -> Result<Streaming<SubscribeArmedStateResponse>, Status> {
    let mut client = client(config, token)?;

    client
        .subscribe_armed_state(Empty {})
        .await
        .map(Response::into_inner)
}

/// Streams the current operating mode followed by upstream updates.
pub async fn subscribe_operating_mode(
    config: &GrpcConfig, token: ForwardedToken,
) -> Result<Streaming<SubscribeOperatingModeResponse>, Status> {
    let mut client = client(config, token)?;

    client
        .subscribe_operating_mode(Empty {})
        .await
        .map(Response::into_inner)
}

/// Streams the current autotune event and message followed by upstream updates.
pub async fn subscribe_events(
    config: &GrpcConfig, token: ForwardedToken,
) -> Result<Streaming<SubscribeAutotuneEventResponse>, Status> {
    let mut client = client(config, token)?;

    client
        .subscribe_events(Empty {})
        .await
        .map(Response::into_inner)
}
