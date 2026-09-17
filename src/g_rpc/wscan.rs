//! This module implements the client side of the wire scan gRPC
//! protocol.

use crate::{
    config::GrpcConfig,
    g_rpc::{
        proto::scanner::{
            DetectorRequest, ScanProgress, ScanRequest, ScanResult,
            scanner_client::ScannerClient,
        },
        utils::handle_rpc_error,
    },
};
use rust_grpc_lib::auth::ForwardedToken;
use tonic::{Response, Status, Streaming};

pub async fn start_scan(
    scanner_config: &GrpcConfig, token: ForwardedToken, req: ScanRequest,
) -> Result<Response<Streaming<ScanResult>>, Status> {
    let mut client = ScannerClient::from_endpoint_with_provider(
        &scanner_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "Scanner"))?;

    client.start_scan(req).await
}

pub async fn get_progress(
    scanner_config: &GrpcConfig, token: ForwardedToken, id: String,
) -> Result<Response<ScanProgress>, Status> {
    let mut client = ScannerClient::from_endpoint_with_provider(
        &scanner_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "Scanner"))?;

    client
        .get_progress(DetectorRequest { detector_id: id })
        .await
}

pub async fn abort_scan(
    scanner_config: &GrpcConfig, token: ForwardedToken, id: String,
) -> Result<Response<ScanProgress>, Status> {
    let mut client = ScannerClient::from_endpoint_with_provider(
        &scanner_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "Scanner"))?;

    client.abort_scan(DetectorRequest { detector_id: id }).await
}
