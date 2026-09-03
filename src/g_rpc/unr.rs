//! UNR gRPC Module
//!
//! Contains the logic for making calls to the UNR gRPC service, covering both
//! the BaseInfo and RelationshipInfo APIs. Both clients share the same
//! underlying [`tonic::transport::Channel`] via [`UnrConnectionAdapter`].

use crate::g_rpc::{
    connection_utils::{ConnectionAdapter, ConnectionPort},
    proto::{
        google::protobuf::Empty,
        services::unr::{
            BaseInfo, BaseRequest, BaseResponse, RelationshipInfo,
            RelationshipRequest, RelationshipResponse,
            base_info_service_client::BaseInfoServiceClient,
            relationship_info_service_client::RelationshipInfoServiceClient,
        },
    },
};
use std::sync::LazyLock;
use tokio::try_join;
use tonic::{
    Response, Status,
    transport::{Channel, Error},
};

/// The environment variable name to use when requesting the location of the UNR gRPC service.
const UNR_GRPC_HOST: &str = "UNR_GRPC_HOST";

/// A static instance of [`ConnectionPort`] wrapping [`UnrConnectionAdapter`].
/// Utilizes [`LazyLock`] to only instantiate upon the first reference to this field.
static UNR_CLIENT: LazyLock<ConnectionPort<UnrConnectionAdapter>> =
    LazyLock::new(|| ConnectionPort::new(UNR_GRPC_HOST));

/// Makes a request to the UNR gRPC service to create a new `BaseInfo` record.
pub async fn create_base_info(base_info: BaseInfo) -> Result<Empty, Status> {
    let do_create = |mut client: UnrConnectionAdapter| async move {
        client
            .base_info_conn
            .create(base_info)
            .await
            .map(Response::into_inner)
            .map(Into::into)
    };
    UNR_CLIENT.run_with_client(do_create).await
}

/// Makes a request to the UNR gRPC service to read `BaseInfo` records for the given device names.
/// If `device_names` is empty, the service returns all rows.
pub async fn read_base_info(
    device_names: Vec<String>,
) -> Result<BaseResponse, Status> {
    let do_read = |mut client: UnrConnectionAdapter| async move {
        client
            .base_info_conn
            .read(BaseRequest { device_names })
            .await
            .map(Response::into_inner)
            .map(Into::into)
    };
    UNR_CLIENT.run_with_client(do_read).await
}

/// Makes a request to the UNR gRPC service to update an existing `BaseInfo` record.
pub async fn update_base_info(base_info: BaseInfo) -> Result<Empty, Status> {
    let do_update = |mut client: UnrConnectionAdapter| async move {
        client
            .base_info_conn
            .update(base_info)
            .await
            .map(Response::into_inner)
            .map(Into::into)
    };
    UNR_CLIENT.run_with_client(do_update).await
}

/// Makes a request to the UNR gRPC service to delete `BaseInfo` records for the given device names.
pub async fn delete_base_info(
    device_names: Vec<String>,
) -> Result<Empty, Status> {
    let do_delete = |mut client: UnrConnectionAdapter| async move {
        client
            .base_info_conn
            .delete(BaseRequest { device_names })
            .await
            .map(Response::into_inner)
            .map(Into::into)
    };
    UNR_CLIENT.run_with_client(do_delete).await
}

/// Makes a request to the UNR gRPC service to get all children associated with a parent.
pub async fn read_relationships(
    parent_name: String,
) -> Result<RelationshipResponse, Status> {
    let do_read = |mut client: UnrConnectionAdapter| async move {
        client
            .relationship_info_conn
            .read(RelationshipRequest { parent_name })
            .await
            .map(Response::into_inner)
            .map(Into::into)
    };
    UNR_CLIENT.run_with_client(do_read).await
}

/// Makes a request to the UNR gRPC service to replace an existing parent's list of children (if any) with the provided list.
pub async fn update_relationships(
    relationship_info: RelationshipInfo,
) -> Result<Empty, Status> {
    let do_update = |mut client: UnrConnectionAdapter| async move {
        client
            .relationship_info_conn
            .update(relationship_info)
            .await
            .map(Response::into_inner)
            .map(Into::into)
    };
    UNR_CLIENT.run_with_client(do_update).await
}

/// Makes a request to the UNR gRPC service to remove all children from a parent's relationship list.
pub async fn delete_relationships(
    parent_name: String,
) -> Result<Empty, Status> {
    let do_delete = |mut client: UnrConnectionAdapter| async move {
        client
            .relationship_info_conn
            .delete(RelationshipRequest { parent_name })
            .await
            .map(Response::into_inner)
            .map(Into::into)
    };
    UNR_CLIENT.run_with_client(do_delete).await
}

/// Implementation of [`ConnectionAdapter`] to hold the clients that invoke the gRPC endpoints
/// supplied by the UNR service. Both clients share the same [`Channel`].
#[derive(Clone)]
struct UnrConnectionAdapter {
    pub base_info_conn: BaseInfoServiceClient<Channel>,
    pub relationship_info_conn: RelationshipInfoServiceClient<Channel>,
}

impl ConnectionAdapter for UnrConnectionAdapter {
    async fn new(host: String) -> Result<Self, Error> {
        let (base_info_conn, relationship_info_conn) = try_join!(
            BaseInfoServiceClient::connect(host.clone()),
            RelationshipInfoServiceClient::connect(host)
        )?;

        Ok(Self {
            base_info_conn,
            relationship_info_conn,
        })
    }
}
