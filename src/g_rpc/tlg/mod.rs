use crate::env_var;
use proto::services::tlg_placement::{
    tlg_placement_mutation_service_client::TlgPlacementMutationServiceClient,
    tlg_placement_service_client::TlgPlacementServiceClient, TlgDevices,
    TlgPlacementResponse,
};
use tonic::{transport, Status};

pub mod proto {
    pub mod services {
        pub mod tlg_placement {
            include!("../generated/services.tlg_placement.rs");
        }
    }
}

const TLG_HOST: &str = "TLG_GRPC_HOST";
const DEFAULT_TLG_HOST: &str = "10.200.24.116";

const TLG_PORT: &str = "TLG_GRPC_PORT";
const DEFAULT_TLG_PORT: &str = "9090";

fn build_address() -> String {
    let host = env_var::get(TLG_HOST).into_str_or(DEFAULT_TLG_HOST);
    let port = env_var::get(TLG_PORT).into_str_or(DEFAULT_TLG_PORT);
    format!("http://{}:{}", host, port)
}

// Local helper function to get a connection to the gRPC service.

async fn get_service_client(
) -> Result<TlgPlacementServiceClient<transport::Channel>, Status> {
    TlgPlacementServiceClient::connect(build_address())
        .await
        .map_err(|_| Status::unavailable("TLG service unavailable"))
}

async fn get_mutation_service_client(
) -> Result<TlgPlacementMutationServiceClient<transport::Channel>, Status> {
    TlgPlacementMutationServiceClient::connect(build_address())
        .await
        .map_err(|_| Status::unavailable("TLG service unavailable"))
}

pub async fn get_version() -> Result<String, Status> {
    get_service_client()
        .await?
        .get_version(())
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
