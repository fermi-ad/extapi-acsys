use std::sync::Arc;

use async_graphql::{
    Context, Error, InputObject, Result, SimpleObject, Union,
    dataloader::{DataLoader, HashMapCache},
};
use rust_grpc_lib::auth::ForwardedToken;

use crate::{
    config::ExtapiGlobalConfig,
    g_rpc::proto::services::unr::entity::Entity,
    graphql::{
        auth_handlers::AuthInfo,
        unr::{api::UnrApi, handle_error, loader},
    },
};

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
        let loader =
            ctx.data::<DataLoader<loader::UnrEntityLoader, HashMapCache>>()?;
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

    async fn children(&self, ctx: &Context<'_>) -> Result<Vec<Device>> {
        let api = ctx.data::<Arc<dyn UnrApi>>()?;
        let global_config = ctx.data::<Arc<ExtapiGlobalConfig>>()?;
        let token = ctx
            .data_opt::<AuthInfo>()
            .and_then(|info| info.token())
            .unwrap_or_default();

        let resp = api
            .read_relationships(
                &global_config.unr,
                ForwardedToken::new(token),
                vec![self.name.clone()],
            )
            .await
            .map_err(|e| handle_error(e, "reading relationship"))?;

        Ok(resp
            .entries
            .into_iter()
            .find(|entry| entry.id == self.name)
            .map(|entry| entry.children.into_iter().map(Device::new).collect())
            .unwrap_or_default())
    }

    async fn parent(&self, ctx: &Context<'_>) -> Result<Option<Device>> {
        let api = ctx.data::<Arc<dyn UnrApi>>()?;
        let global_config = ctx.data::<Arc<ExtapiGlobalConfig>>()?;
        let token = ctx
            .data_opt::<AuthInfo>()
            .and_then(|info| info.token())
            .unwrap_or_default();

        let resp = api
            .read_relationships(
                &global_config.unr,
                ForwardedToken::new(token),
                vec![self.name.clone()],
            )
            .await
            .map_err(|e| handle_error(e, "reading relationship"))?;

        Ok(resp
            .entries
            .into_iter()
            .find(|entry| entry.id == self.name)
            .and_then(|entry| Self::non_empty(entry.child_of))
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
