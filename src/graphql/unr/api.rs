use crate::g_rpc::proto::{google::protobuf::Empty, services::unr};
use async_trait::async_trait;
use tonic::Status;

/// Injectable UNR API surface used by the GraphQL layer.
///
/// This indirection enables unit tests to exercise resolver semantics without
/// standing up a gRPC server.
#[async_trait]
pub trait UnrApi: Send + Sync {
    async fn create_base_info(
        &self, base_info: unr::BaseInfo,
    ) -> Result<Empty, Status>;

    async fn read_base_info(
        &self, device_names: Vec<String>,
    ) -> Result<unr::BaseResponse, Status>;

    async fn update_base_info(
        &self, base_info: unr::BaseInfo,
    ) -> Result<Empty, Status>;

    async fn delete_base_info(
        &self, device_names: Vec<String>,
    ) -> Result<Empty, Status>;

    async fn create_relationship(
        &self, relationship_info: unr::RelationshipInfo,
    ) -> Result<Empty, Status>;

    async fn read_relationship(
        &self, parent_name: String,
    ) -> Result<unr::RelationshipResponse, Status>;

    async fn update_relationship(
        &self, relationship_info: unr::RelationshipInfo,
    ) -> Result<Empty, Status>;

    async fn delete_relationship(
        &self, parent_name: String,
    ) -> Result<Empty, Status>;
}

/// Production implementation that delegates to the gRPC client module.
#[derive(Clone, Default)]
pub struct GrpcUnrApi;

#[async_trait]
impl UnrApi for GrpcUnrApi {
    async fn create_base_info(
        &self, base_info: unr::BaseInfo,
    ) -> Result<Empty, Status> {
        crate::g_rpc::unr::create_base_info(base_info).await
    }

    async fn read_base_info(
        &self, device_names: Vec<String>,
    ) -> Result<unr::BaseResponse, Status> {
        crate::g_rpc::unr::read_base_info(device_names).await
    }

    async fn update_base_info(
        &self, base_info: unr::BaseInfo,
    ) -> Result<Empty, Status> {
        crate::g_rpc::unr::update_base_info(base_info).await
    }

    async fn delete_base_info(
        &self, device_names: Vec<String>,
    ) -> Result<Empty, Status> {
        crate::g_rpc::unr::delete_base_info(device_names).await
    }

    async fn create_relationship(
        &self, relationship_info: unr::RelationshipInfo,
    ) -> Result<Empty, Status> {
        crate::g_rpc::unr::create_relationship(relationship_info).await
    }

    async fn read_relationship(
        &self, parent_name: String,
    ) -> Result<unr::RelationshipResponse, Status> {
        crate::g_rpc::unr::read_relationship(parent_name).await
    }

    async fn update_relationship(
        &self, relationship_info: unr::RelationshipInfo,
    ) -> Result<Empty, Status> {
        crate::g_rpc::unr::update_relationship(relationship_info).await
    }

    async fn delete_relationship(
        &self, parent_name: String,
    ) -> Result<Empty, Status> {
        crate::g_rpc::unr::delete_relationship(parent_name).await
    }
}
