use proto::{
    tlg_placement_mutation_service_client::TlgPlacementMutationServiceClient,
    tlg_placement_service_client::TlgPlacementServiceClient, EmptyRequest,
    TlgDevices, TlgPlacementResponse,
};
use tonic::{transport, Status};

pub mod proto {
    tonic::include_proto!("_");
}

const URL: &str = "http://unknown.fnal.gov:50051/";

// Local helper function to get a connection to the gRPC service.

async fn get_service_client(
) -> Result<TlgPlacementServiceClient<transport::Channel>, Status> {
    TlgPlacementServiceClient::connect(URL)
        .await
        .map_err(|_| Status::unavailable("TLG service unavailable"))
}

async fn get_mutation_service_client(
) -> Result<TlgPlacementMutationServiceClient<transport::Channel>, Status> {
    TlgPlacementMutationServiceClient::connect(URL)
        .await
        .map_err(|_| Status::unavailable("TLG service unavailable"))
}

pub async fn get_version() -> Result<String, Status> {
    get_service_client()
        .await?
        .get_version(EmptyRequest {})
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
