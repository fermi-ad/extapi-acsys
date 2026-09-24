//! UNR gRPC Module
//!
//! Contains the logic for making calls to the UNR gRPC service, covering both
//! the Entity and Relationship APIs. Both clients share the same
//! underlying [`tonic::transport::Channel`] via [`UnrConnectionAdapter`].

use crate::g_rpc::{
    connection_utils::{ConnectionAdapter, ConnectionPort},
    proto::{
        google::protobuf::Empty,
        services::unr::{
            entity::{
                CreateEntityRequest, DeleteEntityRequest, Entity,
                ReadEntityRequest, ReadEntityResponse, UpdateEntityRequest,
                entity_service_client::EntityServiceClient,
            },
            relationship::{
                CreateRelationshipRequest, DeleteRelationshipRequest,
                ReadRelationshipRequest, ReadRelationshipResponse,
                Relationship,
                relationship_service_client::RelationshipServiceClient,
            },
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

/// Makes a request to the UNR gRPC service to create new [`Entity`] records.
pub async fn create_entities(entities: Vec<Entity>) -> Result<Empty, Status> {
    let do_create = |mut client: UnrConnectionAdapter| async move {
        client
            .entity_conn
            .create(CreateEntityRequest { entities })
            .await
            .map(Response::into_inner)
            .map(Into::into)
    };
    UNR_CLIENT.run_with_client(do_create).await
}

/// Makes a request to the UNR gRPC service to read [`Entity`] records for the given IDs.
/// If `ids` is empty, the service returns all rows.
pub async fn read_entities(
    ids: Vec<String>,
) -> Result<ReadEntityResponse, Status> {
    let do_read = |mut client: UnrConnectionAdapter| async move {
        client
            .entity_conn
            .read(ReadEntityRequest { ids })
            .await
            .map(Response::into_inner)
            .map(Into::into)
    };
    UNR_CLIENT.run_with_client(do_read).await
}

/// Makes a request to the UNR gRPC service to update existing [`Entity`] records.
pub async fn update_entities(entities: Vec<Entity>) -> Result<Empty, Status> {
    let do_update = |mut client: UnrConnectionAdapter| async move {
        client
            .entity_conn
            .update(UpdateEntityRequest { entities })
            .await
            .map(Response::into_inner)
            .map(Into::into)
    };
    UNR_CLIENT.run_with_client(do_update).await
}

/// Makes a request to the UNR gRPC service to delete [`Entity`] records for the given IDs.
pub async fn delete_entities(ids: Vec<String>) -> Result<Empty, Status> {
    let do_delete = |mut client: UnrConnectionAdapter| async move {
        client
            .entity_conn
            .delete(DeleteEntityRequest { ids })
            .await
            .map(Response::into_inner)
            .map(Into::into)
    };
    UNR_CLIENT.run_with_client(do_delete).await
}

/// Makes a request to the UNR gRPC service to create the given [`Relationship`] records.
pub async fn create_relationships(
    relationships: Vec<Relationship>,
) -> Result<Empty, Status> {
    let do_create = |mut client: UnrConnectionAdapter| async move {
        client
            .relationship_conn
            .create(CreateRelationshipRequest { relationships })
            .await
            .map(Response::into_inner)
            .map(Into::into)
    };
    UNR_CLIENT.run_with_client(do_create).await
}

/// Makes a request to the UNR gRPC service to read relationship metadata for the given entity IDs.
pub async fn read_relationships(
    ids: Vec<String>,
) -> Result<ReadRelationshipResponse, Status> {
    let do_read = |mut client: UnrConnectionAdapter| async move {
        client
            .relationship_conn
            .read(ReadRelationshipRequest { ids })
            .await
            .map(Response::into_inner)
            .map(Into::into)
    };
    UNR_CLIENT.run_with_client(do_read).await
}

/// Makes a request to the UNR gRPC service to delete the given [`Relationship`] records.
pub async fn delete_relationships(
    relationships: Vec<Relationship>,
) -> Result<Empty, Status> {
    let do_delete = |mut client: UnrConnectionAdapter| async move {
        client
            .relationship_conn
            .delete(DeleteRelationshipRequest { relationships })
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
    pub entity_conn: EntityServiceClient<Channel>,
    pub relationship_conn: RelationshipServiceClient<Channel>,
}

impl ConnectionAdapter for UnrConnectionAdapter {
    async fn new(host: String) -> Result<Self, Error> {
        let (entity_conn, relationship_conn) = try_join!(
            EntityServiceClient::connect(host.clone()),
            RelationshipServiceClient::connect(host)
        )?;

        Ok(Self {
            entity_conn,
            relationship_conn,
        })
    }
}
