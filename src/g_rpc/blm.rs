//! Client adapter for the BLM gRPC service.

use rust_grpc_lib::auth::ForwardedToken;
use tonic::{Response, Status, Streaming};

use crate::{
    config::GrpcConfig,
    g_rpc::{
        errors::handle_rpc_error,
        proto::services::blm::v1::{
            ListIntegratedLossRequest, ListIntegratedLossResponse,
            SubscribeBeamThroughputRequest, SubscribeBeamThroughputResponse,
            SubscribeLossRatioRequest, SubscribeLossRatioResponse,
            blm_service_client::BlmServiceClient,
        },
    },
};

/// Returns the integrated-loss devices associated with a beamline and TCLK event.
pub async fn list_integrated_loss_devices(
    config: &GrpcConfig, token: ForwardedToken,
    request: ListIntegratedLossRequest,
) -> Result<ListIntegratedLossResponse, Status> {
    let mut client =
        BlmServiceClient::from_endpoint_with_provider(&config.host_addr, token)
            .map_err(|error| handle_rpc_error(error, "BLM Service"))?;

    client
        .list_integrated_loss_devices(request)
        .await
        .map(Response::into_inner)
}

/// Opens a stream of loss-ratio samples for a beamline and TCLK event.
pub async fn subscribe_loss_ratio(
    config: &GrpcConfig, token: ForwardedToken,
    request: SubscribeLossRatioRequest,
) -> Result<Streaming<SubscribeLossRatioResponse>, Status> {
    let mut client =
        BlmServiceClient::from_endpoint_with_provider(&config.host_addr, token)
            .map_err(|error| handle_rpc_error(error, "BLM Service"))?;

    client
        .subscribe_loss_ratio(request)
        .await
        .map(Response::into_inner)
}

/// Opens a stream of beam-throughput samples for a beamline.
pub async fn subscribe_beam_throughput(
    config: &GrpcConfig, token: ForwardedToken,
    request: SubscribeBeamThroughputRequest,
) -> Result<Streaming<SubscribeBeamThroughputResponse>, Status> {
    let mut client =
        BlmServiceClient::from_endpoint_with_provider(&config.host_addr, token)
            .map_err(|error| handle_rpc_error(error, "BLM Service"))?;

    client
        .subscribe_beam_throughput(request)
        .await
        .map(Response::into_inner)
}
