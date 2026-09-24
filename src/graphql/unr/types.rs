use crate::g_rpc::proto::services::unr::{
    entity::Entity, relationship::RelationshipDetails,
};

use super::loader;
use async_graphql::{
    Context, Error, InputObject, Result, SimpleObject, Union,
    dataloader::{DataLoader, HashMapCache},
};
use uuid::Uuid;

/// Input for creating a device.
#[derive(Clone, Debug, InputObject)]
pub struct CreateDeviceInput {
    pub name: String,
    pub address: String,
    pub r#type: String,
    pub protocol: String,
    pub children: Option<Vec<String>>,
}

/// Input for updating a device.
///
/// `children` is intentionally omitted to avoid accidental relationship
/// clobbering during updates.
#[derive(Clone, Debug, InputObject)]
pub struct UpdateDeviceInput {
    pub name: String,
    pub address: String,
    pub r#type: String,
    pub protocol: String,
}

#[derive(Clone, Debug, SimpleObject)]
#[graphql(complex)]
pub struct Device {
    pub name: String,
}

impl Device {
    pub(super) fn new(name: String) -> Self {
        Self { name }
    }
}

#[async_graphql::ComplexObject]
impl Device {
    #[graphql(skip)]
    fn non_empty(s: String) -> Option<String> {
        (!s.is_empty()).then_some(s)
    }

    #[graphql(skip)]
    async fn load_entity(&self, ctx: &Context<'_>) -> Result<Option<Entity>> {
        let loader = ctx.data_unchecked::<DataLoader<loader::UnrEntityLoader, HashMapCache>>();
        loader
            .load_one(self.name.clone())
            .await
            .map_err(|e| Error::new(format!("Error reading entity: {e}")))
    }

    async fn address(&self, ctx: &Context<'_>) -> Result<Option<String>> {
        let entity = self.load_entity(ctx).await?;
        Ok(entity.and_then(|entity| Self::non_empty(entity.address)))
    }

    async fn r#type(&self, ctx: &Context<'_>) -> Result<Option<String>> {
        let entity = self.load_entity(ctx).await?;
        Ok(entity.and_then(|entity| Self::non_empty(entity.r#type)))
    }

    async fn protocol(&self, ctx: &Context<'_>) -> Result<Option<String>> {
        let entity = self.load_entity(ctx).await?;
        Ok(entity.and_then(|entity| Self::non_empty(entity.protocol)))
    }

    #[graphql(skip)]
    async fn load_relationships(
        &self, ctx: &Context<'_>,
    ) -> Result<Option<RelationshipDetails>> {
        let loader =
            ctx.data_unchecked::<DataLoader<loader::UnrRelationshipLoader, HashMapCache>>();
        loader.load_one(self.name.clone()).await.map_err(|e| {
            // Include an ID in the response so the error can be correlated
            // with server logs.
            let err_id = Uuid::new_v4();
            tracing::warn!("{err_id} relationship loader error: {e:?}");
            Error::new(format!(
                "Error reading relationship. See server logs for details. (Error ID: {err_id})"
            ))
        })
    }

    async fn children(&self, ctx: &Context<'_>) -> Result<Vec<Device>> {
        let details = self.load_relationships(ctx).await?;
        Ok(details
            .map(|d| d.children.into_iter().map(Device::new).collect())
            .unwrap_or_default())
    }

    async fn parent(&self, ctx: &Context<'_>) -> Result<Option<Device>> {
        let details = self.load_relationships(ctx).await?;
        Ok(details
            .and_then(|d| Self::non_empty(d.child_of))
            .map(Device::new))
    }
}

/// Indicates that a requested UNR device name was not found.
#[derive(Clone, Debug, SimpleObject, PartialEq, Eq)]
pub struct NotFound {
    pub name: String,
}

/// Result type for bulk UNR device queries.
#[derive(Union)]
pub enum DeviceQueryResult {
    Device(Device),
    NotFound(NotFound),
}
