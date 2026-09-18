use crate::{
    config::GrpcConfig,
    g_rpc::proto::{
        google::protobuf::Empty,
        services::unr::{
            entity::{Entity, ReadEntityResponse},
            relationship::{ReadRelationshipResponse, Relationship},
        },
    },
};
use async_graphql::async_trait::async_trait;
use rust_grpc_lib::auth::ForwardedToken;
use tonic::Status;

/// Injectable UNR API surface used by the GraphQL layer.
///
/// This indirection enables unit tests to exercise resolver semantics without
/// standing up a gRPC server.
#[async_trait]
pub trait UnrApi: Send + Sync {
    async fn create_entities(
        &self, unr_config: &GrpcConfig, token: ForwardedToken,
        entities: Vec<Entity>,
    ) -> Result<Empty, Status>;

    async fn read_entities(
        &self, unr_config: &GrpcConfig, token: ForwardedToken, ids: Vec<String>,
    ) -> Result<ReadEntityResponse, Status>;

    async fn update_entities(
        &self, unr_config: &GrpcConfig, token: ForwardedToken,
        entities: Vec<Entity>,
    ) -> Result<Empty, Status>;

    async fn delete_entities(
        &self, unr_config: &GrpcConfig, token: ForwardedToken, ids: Vec<String>,
    ) -> Result<Empty, Status>;

    async fn create_relationships(
        &self, unr_config: &GrpcConfig, token: ForwardedToken,
        relationships: Vec<Relationship>,
    ) -> Result<Empty, Status>;

    async fn read_relationships(
        &self, unr_config: &GrpcConfig, token: ForwardedToken, ids: Vec<String>,
    ) -> Result<ReadRelationshipResponse, Status>;

    async fn delete_relationships(
        &self, unr_config: &GrpcConfig, token: ForwardedToken,
        relationships: Vec<Relationship>,
    ) -> Result<Empty, Status>;
}

/// Production implementation that delegates to the gRPC client module.
#[derive(Clone, Default)]
pub struct GrpcUnrApi;

#[async_trait]
impl UnrApi for GrpcUnrApi {
    async fn create_entities(
        &self, unr_config: &GrpcConfig, token: ForwardedToken,
        entities: Vec<Entity>,
    ) -> Result<Empty, Status> {
        crate::g_rpc::unr::create_entities(unr_config, token, entities).await
    }

    async fn read_entities(
        &self, unr_config: &GrpcConfig, token: ForwardedToken, ids: Vec<String>,
    ) -> Result<ReadEntityResponse, Status> {
        crate::g_rpc::unr::read_entities(unr_config, token, ids).await
    }

    async fn update_entities(
        &self, unr_config: &GrpcConfig, token: ForwardedToken,
        entities: Vec<Entity>,
    ) -> Result<Empty, Status> {
        crate::g_rpc::unr::update_entities(unr_config, token, entities).await
    }

    async fn delete_entities(
        &self, unr_config: &GrpcConfig, token: ForwardedToken, ids: Vec<String>,
    ) -> Result<Empty, Status> {
        crate::g_rpc::unr::delete_entities(unr_config, token, ids).await
    }

    async fn create_relationships(
        &self, unr_config: &GrpcConfig, token: ForwardedToken,
        relationships: Vec<Relationship>,
    ) -> Result<Empty, Status> {
        crate::g_rpc::unr::create_relationships(
            unr_config,
            token,
            relationships,
        )
        .await
    }

    async fn read_relationships(
        &self, unr_config: &GrpcConfig, token: ForwardedToken, ids: Vec<String>,
    ) -> Result<ReadRelationshipResponse, Status> {
        crate::g_rpc::unr::read_relationships(unr_config, token, ids).await
    }

    async fn delete_relationships(
        &self, unr_config: &GrpcConfig, token: ForwardedToken,
        relationships: Vec<Relationship>,
    ) -> Result<Empty, Status> {
        crate::g_rpc::unr::delete_relationships(
            unr_config,
            token,
            relationships,
        )
        .await
    }
}
