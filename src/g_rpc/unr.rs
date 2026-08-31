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

#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
trait UnrBaseInfoClient: Send {
    async fn create(&mut self, base_info: BaseInfo) -> Result<Empty, Status>;
    async fn read(
        &mut self, request: BaseRequest,
    ) -> Result<BaseResponse, Status>;
    async fn update(&mut self, base_info: BaseInfo) -> Result<Empty, Status>;
    async fn delete(&mut self, request: BaseRequest) -> Result<Empty, Status>;
}

struct TonicUnrBaseInfoClient {
    inner: BaseInfoServiceClient<Channel>,
}

#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
trait UnrRelationshipClient: Send {
    async fn create(
        &mut self, relationship_info: RelationshipInfo,
    ) -> Result<Empty, Status>;
    async fn read(
        &mut self, request: RelationshipRequest,
    ) -> Result<RelationshipResponse, Status>;
    async fn update(
        &mut self, relationship_info: RelationshipInfo,
    ) -> Result<Empty, Status>;
    async fn delete(
        &mut self, request: RelationshipRequest,
    ) -> Result<Empty, Status>;
}

struct TonicUnrRelationshipClient {
    inner: RelationshipInfoServiceClient<Channel>,
}

#[async_trait::async_trait]
impl UnrRelationshipClient for TonicUnrRelationshipClient {
    async fn create(
        &mut self, relationship_info: RelationshipInfo,
    ) -> Result<Empty, Status> {
        self.inner
            .create(relationship_info)
            .await
            .map(Response::into_inner)
    }

    async fn read(
        &mut self, request: RelationshipRequest,
    ) -> Result<RelationshipResponse, Status> {
        self.inner.read(request).await.map(Response::into_inner)
    }

    async fn update(
        &mut self, relationship_info: RelationshipInfo,
    ) -> Result<Empty, Status> {
        self.inner
            .update(relationship_info)
            .await
            .map(Response::into_inner)
    }

    async fn delete(
        &mut self, request: RelationshipRequest,
    ) -> Result<Empty, Status> {
        self.inner.delete(request).await.map(Response::into_inner)
    }
}

#[async_trait::async_trait]
impl UnrBaseInfoClient for TonicUnrBaseInfoClient {
    async fn create(&mut self, base_info: BaseInfo) -> Result<Empty, Status> {
        self.inner.create(base_info).await.map(Response::into_inner)
    }

    async fn read(
        &mut self, request: BaseRequest,
    ) -> Result<BaseResponse, Status> {
        self.inner.read(request).await.map(Response::into_inner)
    }

    async fn update(&mut self, base_info: BaseInfo) -> Result<Empty, Status> {
        self.inner.update(base_info).await.map(Response::into_inner)
    }

    async fn delete(&mut self, request: BaseRequest) -> Result<Empty, Status> {
        self.inner.delete(request).await.map(Response::into_inner)
    }
}

/// The environment variable name to use when requesting the location of the UNR gRPC service.
const UNR_GRPC_HOST: &str = "UNR_GRPC_HOST";

/// A static instance of [`ConnectionPort`] wrapping [`UnrConnectionAdapter`].
/// Utilizes [`LazyLock`] to only instantiate upon the first reference to this field.
static UNR_CLIENT: LazyLock<ConnectionPort<UnrConnectionAdapter>> =
    LazyLock::new(|| ConnectionPort::new(UNR_GRPC_HOST));

async fn create_base_info_with(
    client: &mut dyn UnrBaseInfoClient, base_info: BaseInfo,
) -> Result<Empty, Status> {
    client.create(base_info).await
}

async fn read_base_info_with(
    client: &mut dyn UnrBaseInfoClient, device_names: Vec<String>,
) -> Result<BaseResponse, Status> {
    client.read(BaseRequest { device_names }).await
}

async fn update_base_info_with(
    client: &mut dyn UnrBaseInfoClient, base_info: BaseInfo,
) -> Result<Empty, Status> {
    client.update(base_info).await
}

async fn delete_base_info_with(
    client: &mut dyn UnrBaseInfoClient, device_names: Vec<String>,
) -> Result<Empty, Status> {
    client.delete(BaseRequest { device_names }).await
}

/// Makes a request to the UNR gRPC service to create a new `BaseInfo` record.
pub async fn create_base_info(base_info: BaseInfo) -> Result<Empty, Status> {
    let do_create = |client: UnrConnectionAdapter| async move {
        let mut base = TonicUnrBaseInfoClient {
            inner: client.base_info_conn,
        };
        create_base_info_with(&mut base, base_info)
            .await
            .map(Into::into)
    };
    UNR_CLIENT.run_with_client(do_create).await
}

/// Makes a request to the UNR gRPC service to read `BaseInfo` records for the given device names.
/// If `device_names` is empty, the service returns all rows.
pub async fn read_base_info(
    device_names: Vec<String>,
) -> Result<BaseResponse, Status> {
    let do_read = |client: UnrConnectionAdapter| async move {
        let mut base = TonicUnrBaseInfoClient {
            inner: client.base_info_conn,
        };
        read_base_info_with(&mut base, device_names)
            .await
            .map(Into::into)
    };
    UNR_CLIENT.run_with_client(do_read).await
}

/// Makes a request to the UNR gRPC service to update an existing `BaseInfo` record.
pub async fn update_base_info(base_info: BaseInfo) -> Result<Empty, Status> {
    let do_update = |client: UnrConnectionAdapter| async move {
        let mut base = TonicUnrBaseInfoClient {
            inner: client.base_info_conn,
        };
        update_base_info_with(&mut base, base_info)
            .await
            .map(Into::into)
    };
    UNR_CLIENT.run_with_client(do_update).await
}

/// Makes a request to the UNR gRPC service to delete `BaseInfo` records for the given device names.
pub async fn delete_base_info(
    device_names: Vec<String>,
) -> Result<Empty, Status> {
    let do_delete = |client: UnrConnectionAdapter| async move {
        let mut base = TonicUnrBaseInfoClient {
            inner: client.base_info_conn,
        };
        delete_base_info_with(&mut base, device_names)
            .await
            .map(Into::into)
    };
    UNR_CLIENT.run_with_client(do_delete).await
}

async fn create_relationships_with(
    client: &mut dyn UnrRelationshipClient, relationship_info: RelationshipInfo,
) -> Result<Empty, Status> {
    client.create(relationship_info).await
}

async fn read_relationships_with(
    client: &mut dyn UnrRelationshipClient, parent_name: String,
) -> Result<RelationshipResponse, Status> {
    client.read(RelationshipRequest { parent_name }).await
}

async fn update_relationships_with(
    client: &mut dyn UnrRelationshipClient, relationship_info: RelationshipInfo,
) -> Result<Empty, Status> {
    client.update(relationship_info).await
}

async fn delete_relationships_with(
    client: &mut dyn UnrRelationshipClient, parent_name: String,
) -> Result<Empty, Status> {
    client.delete(RelationshipRequest { parent_name }).await
}

/// Makes a request to the UNR gRPC service to add a set of children to a parent's relationship list.
pub async fn create_relationships(
    relationship_info: RelationshipInfo,
) -> Result<Empty, Status> {
    let do_create = |client: UnrConnectionAdapter| async move {
        let mut rel = TonicUnrRelationshipClient {
            inner: client.relationship_info_conn,
        };
        create_relationships_with(&mut rel, relationship_info)
            .await
            .map(Into::into)
    };
    UNR_CLIENT.run_with_client(do_create).await
}

/// Makes a request to the UNR gRPC service to get all children associated with a parent.
pub async fn read_relationships(
    parent_name: String,
) -> Result<RelationshipResponse, Status> {
    let do_read = |client: UnrConnectionAdapter| async move {
        let mut rel = TonicUnrRelationshipClient {
            inner: client.relationship_info_conn,
        };
        read_relationships_with(&mut rel, parent_name)
            .await
            .map(Into::into)
    };
    UNR_CLIENT.run_with_client(do_read).await
}

/// Makes a request to the UNR gRPC service to replace an existing parent's list of children with the provided list.
pub async fn update_relationships(
    relationship_info: RelationshipInfo,
) -> Result<Empty, Status> {
    let do_update = |client: UnrConnectionAdapter| async move {
        let mut rel = TonicUnrRelationshipClient {
            inner: client.relationship_info_conn,
        };
        update_relationships_with(&mut rel, relationship_info)
            .await
            .map(Into::into)
    };
    UNR_CLIENT.run_with_client(do_update).await
}

/// Makes a request to the UNR gRPC service to remove all children from a parent's relationship list.
pub async fn delete_relationships(
    parent_name: String,
) -> Result<Empty, Status> {
    let do_delete = |client: UnrConnectionAdapter| async move {
        let mut rel = TonicUnrRelationshipClient {
            inner: client.relationship_info_conn,
        };
        delete_relationships_with(&mut rel, parent_name)
            .await
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn create_base_info_with_calls_create_once_with_expected_payload() {
        let base_info = BaseInfo {
            device_name: "DEV".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        };

        let mut mock = MockUnrBaseInfoClient::new();
        mock.expect_create()
            .times(1)
            .withf({
                let base_info = base_info.clone();
                move |got| got == &base_info
            })
            .returning(|_| Ok(Empty {}));

        create_base_info_with(&mut mock, base_info).await.unwrap();
    }

    #[tokio::test]
    async fn read_base_info_with_calls_read_once_with_expected_request() {
        let device_names = vec!["A".to_string(), "B".to_string()];

        let mut mock = MockUnrBaseInfoClient::new();
        mock.expect_read()
            .times(1)
            .withf({
                let device_names = device_names.clone();
                move |req| req.device_names == device_names
            })
            .returning(|_| Ok(BaseResponse { base_info: vec![] }));

        read_base_info_with(&mut mock, device_names).await.unwrap();
    }

    #[tokio::test]
    async fn update_base_info_with_calls_update_once_with_expected_payload() {
        let base_info = BaseInfo {
            device_name: "DEV".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        };

        let mut mock = MockUnrBaseInfoClient::new();
        mock.expect_update()
            .times(1)
            .withf({
                let base_info = base_info.clone();
                move |got| got == &base_info
            })
            .returning(|_| Ok(Empty {}));

        update_base_info_with(&mut mock, base_info).await.unwrap();
    }

    #[tokio::test]
    async fn delete_base_info_with_calls_delete_once_with_expected_request() {
        let device_names = vec!["A".to_string(), "B".to_string()];

        let mut mock = MockUnrBaseInfoClient::new();
        mock.expect_delete()
            .times(1)
            .withf({
                let device_names = device_names.clone();
                move |req| req.device_names == device_names
            })
            .returning(|_| Ok(Empty {}));

        delete_base_info_with(&mut mock, device_names)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn create_relationships_with_calls_create_once_with_expected_payload()
    {
        let relationship_info = RelationshipInfo {
            parent_name: "P".to_string(),
            children_names: vec!["C1".to_string(), "C2".to_string()],
        };

        let mut mock = MockUnrRelationshipClient::new();
        mock.expect_create()
            .times(1)
            .withf({
                let relationship_info = relationship_info.clone();
                move |got| got == &relationship_info
            })
            .returning(|_| Ok(Empty {}));

        create_relationships_with(&mut mock, relationship_info)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn read_relationships_with_calls_read_once_with_expected_request() {
        let parent_name = "P".to_string();

        let mut mock = MockUnrRelationshipClient::new();
        mock.expect_read()
            .times(1)
            .withf({
                let parent_name = parent_name.clone();
                move |req| req.parent_name == parent_name
            })
            .returning(|_| {
                Ok(RelationshipResponse {
                    relationship_info: None,
                })
            });

        read_relationships_with(&mut mock, parent_name)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn update_relationships_with_calls_update_once_with_expected_payload()
    {
        let relationship_info = RelationshipInfo {
            parent_name: "P".to_string(),
            children_names: vec!["C1".to_string(), "C2".to_string()],
        };

        let mut mock = MockUnrRelationshipClient::new();
        mock.expect_update()
            .times(1)
            .withf({
                let relationship_info = relationship_info.clone();
                move |got| got == &relationship_info
            })
            .returning(|_| Ok(Empty {}));

        update_relationships_with(&mut mock, relationship_info)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn delete_relationships_with_calls_delete_once_with_expected_request()
    {
        let parent_name = "P".to_string();

        let mut mock = MockUnrRelationshipClient::new();
        mock.expect_delete()
            .times(1)
            .withf({
                let parent_name = parent_name.clone();
                move |req| req.parent_name == parent_name
            })
            .returning(|_| Ok(Empty {}));

        delete_relationships_with(&mut mock, parent_name)
            .await
            .unwrap();
    }
}
