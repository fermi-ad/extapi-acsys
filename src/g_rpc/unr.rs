//! UNR gRPC Module
//!
//! Contains the logic for making calls to the UNR gRPC service, covering both
//! the Entity and Relationship APIs. Both clients share the same
//! underlying [`tonic::transport::Channel`] via [`UnrConnectionAdapter`].

use rust_grpc_lib::auth::ForwardedToken;
use tonic::{Response, Status};

use crate::{
    config::GrpcConfig,
    g_rpc::{
        errors::handle_rpc_error,
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
    },
};

/// Makes a request to the UNR gRPC service to create new [`Entity`] records.
pub async fn create_entities(
    unr_config: &GrpcConfig, token: ForwardedToken, entities: Vec<Entity>,
) -> Result<Empty, Status> {
    let mut client = EntityServiceClient::from_endpoint_with_provider(
        &unr_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "UNR Service"))?;

    client
        .create(CreateEntityRequest { entities })
        .await
        .map(Response::into_inner)
}

/// Makes a request to the UNR gRPC service to read [`Entity`] records for the given IDs.
/// If `ids` is empty, the service returns all rows.
pub async fn read_entities(
    unr_config: &GrpcConfig, token: ForwardedToken, ids: Vec<String>,
) -> Result<ReadEntityResponse, Status> {
    let mut client = EntityServiceClient::from_endpoint_with_provider(
        &unr_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "UNR Service"))?;

    client
        .read(ReadEntityRequest { ids })
        .await
        .map(Response::into_inner)
}

/// Makes a request to the UNR gRPC service to update existing [`Entity`] records.
pub async fn update_entities(
    unr_config: &GrpcConfig, token: ForwardedToken, entities: Vec<Entity>,
) -> Result<Empty, Status> {
    let mut client = EntityServiceClient::from_endpoint_with_provider(
        &unr_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "UNR Service"))?;

    client
        .update(UpdateEntityRequest { entities })
        .await
        .map(Response::into_inner)
}

/// Makes a request to the UNR gRPC service to delete [`Entity`] records for the given IDs.
pub async fn delete_entities(
    unr_config: &GrpcConfig, token: ForwardedToken, ids: Vec<String>,
) -> Result<Empty, Status> {
    let mut client = EntityServiceClient::from_endpoint_with_provider(
        &unr_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "UNR Service"))?;

    client
        .delete(DeleteEntityRequest { ids })
        .await
        .map(Response::into_inner)
}

/// Makes a request to the UNR gRPC service to create the given [`Relationship`] records.
pub async fn create_relationships(
    unr_config: &GrpcConfig, token: ForwardedToken,
    relationships: Vec<Relationship>,
) -> Result<Empty, Status> {
    let mut client = RelationshipServiceClient::from_endpoint_with_provider(
        &unr_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "UNR Service"))?;

    client
        .create(CreateRelationshipRequest { relationships })
        .await
        .map(Response::into_inner)
}

/// Makes a request to the UNR gRPC service to read relationship metadata for the given entity IDs.
pub async fn read_relationships(
    unr_config: &GrpcConfig, token: ForwardedToken, ids: Vec<String>,
) -> Result<ReadRelationshipResponse, Status> {
    let mut client = RelationshipServiceClient::from_endpoint_with_provider(
        &unr_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "UNR Service"))?;

    client
        .read(ReadRelationshipRequest { ids })
        .await
        .map(Response::into_inner)
}

/// Makes a request to the UNR gRPC service to delete the given [`Relationship`] records.
pub async fn delete_relationships(
    unr_config: &GrpcConfig, token: ForwardedToken,
    relationships: Vec<Relationship>,
) -> Result<Empty, Status> {
    let mut client = RelationshipServiceClient::from_endpoint_with_provider(
        &unr_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "UNR Service"))?;

    client
        .delete(DeleteRelationshipRequest { relationships })
        .await
        .map(Response::into_inner)
}
