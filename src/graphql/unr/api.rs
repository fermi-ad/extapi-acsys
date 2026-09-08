use crate::g_rpc::proto::{
    google::protobuf::Empty,
    services::{
        base_info::{BaseInfo, BaseResponse},
        relationship_info::{RelationshipInfo, RelationshipResponse},
    },
};
use async_trait::async_trait;
use tonic::Status;

/// Injectable UNR API surface used by the GraphQL layer.
///
/// This indirection enables unit tests to exercise resolver semantics without
/// standing up a gRPC server.
#[async_trait]
pub trait UnrApi: Send + Sync {
    async fn create_base_info(
        &self, base_info: BaseInfo,
    ) -> Result<Empty, Status>;

    async fn read_base_info(
        &self, device_names: Vec<String>,
    ) -> Result<BaseResponse, Status>;

    async fn update_base_info(
        &self, base_info: BaseInfo,
    ) -> Result<Empty, Status>;

    async fn delete_base_info(
        &self, device_names: Vec<String>,
    ) -> Result<Empty, Status>;

    async fn read_relationships(
        &self, parent_name: String,
    ) -> Result<RelationshipResponse, Status>;

    async fn update_relationships(
        &self, relationship_info: RelationshipInfo,
    ) -> Result<Empty, Status>;

    async fn delete_relationships(
        &self, parent_name: String,
    ) -> Result<Empty, Status>;
}

/// Production implementation that delegates to the gRPC client module.
#[derive(Clone, Default)]
pub struct GrpcUnrApi;

#[async_trait]
impl UnrApi for GrpcUnrApi {
    async fn create_base_info(
        &self, base_info: BaseInfo,
    ) -> Result<Empty, Status> {
        crate::g_rpc::unr::create_base_info(base_info).await
    }

    async fn read_base_info(
        &self, device_names: Vec<String>,
    ) -> Result<BaseResponse, Status> {
        crate::g_rpc::unr::read_base_info(device_names).await
    }

    async fn update_base_info(
        &self, base_info: BaseInfo,
    ) -> Result<Empty, Status> {
        crate::g_rpc::unr::update_base_info(base_info).await
    }

    async fn delete_base_info(
        &self, device_names: Vec<String>,
    ) -> Result<Empty, Status> {
        crate::g_rpc::unr::delete_base_info(device_names).await
    }

    async fn read_relationships(
        &self, parent_name: String,
    ) -> Result<RelationshipResponse, Status> {
        crate::g_rpc::unr::read_relationships(parent_name).await
    }

    async fn update_relationships(
        &self, relationship_info: RelationshipInfo,
    ) -> Result<Empty, Status> {
        crate::g_rpc::unr::update_relationships(relationship_info).await
    }

    async fn delete_relationships(
        &self, parent_name: String,
    ) -> Result<Empty, Status> {
        crate::g_rpc::unr::delete_relationships(parent_name).await
    }
}
