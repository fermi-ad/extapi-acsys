//! Timeline Generator gRPC Module

use crate::g_rpc::proto::google::protobuf::Empty;

use super::proto::services::tlg_placement::{
    TlgDevices, TlgPlacementResponse,
    tlg_placement_mutation_service_client::TlgPlacementMutationServiceClient,
    tlg_placement_service_client::TlgPlacementServiceClient,
};
use rust_env_var_lib::env_var;
use tonic::{Status, transport};

const TLG_HOST: &str = "TLG_GRPC_HOST";

// Local helper function to get a connection to the gRPC service.

async fn get_service_client()
-> Result<TlgPlacementServiceClient<transport::Channel>, Status> {
    let host: String = env_var::expect(TLG_HOST);
    TlgPlacementServiceClient::connect(host)
        .await
        .map_err(|_| Status::unavailable("TLG service unavailable"))
}

async fn get_mutation_service_client()
-> Result<TlgPlacementMutationServiceClient<transport::Channel>, Status> {
    let host: String = env_var::expect(TLG_HOST);
    TlgPlacementMutationServiceClient::connect(host)
        .await
        .map_err(|_| Status::unavailable("TLG service unavailable"))
}

pub async fn get_version() -> Result<String, Status> {
    get_service_client()
        .await?
        .get_version(Empty {})
        .await
        .map(|v| v.into_inner().version)
}

pub async fn diagnostics(
    devs: TlgDevices,
) -> Result<TlgPlacementResponse, Status> {
    get_mutation_service_client()
        .await?
        .diagnostics_inline(devs)
        .await
        .map(|v| v.into_inner())
}

pub async fn placement(
    devs: TlgDevices,
) -> Result<TlgPlacementResponse, Status> {
    get_mutation_service_client()
        .await?
        .placement_inline(devs)
        .await
        .map(|v| v.into_inner())
}
