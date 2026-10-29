//! Authenticated client adapter for the MI8 Autotune gRPC service.

use rust_grpc_lib::auth::ForwardedToken;
use tonic::{Response, Status};

use crate::{
    config::GrpcConfig,
    g_rpc::{
        errors::handle_rpc_error,
        proto::{
            google::protobuf::Empty,
            services::mi8_autotune::v1::{
                ListBpmsResponse,
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
