use crate::g_rpc::proto::services::unr::BaseInfo;
use std::sync::Arc;

use super::{api::UnrApi, handle_error, loader};
use async_graphql::{
    Context, Error, InputObject, Result, SimpleObject, Union,
    dataloader::{DataLoader, HashMapCache},
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
    async fn load_base_info(
        &self, ctx: &Context<'_>,
    ) -> Result<Option<BaseInfo>> {
        let loader = ctx.data_unchecked::<DataLoader<loader::UnrBaseInfoLoader, HashMapCache>>();
        loader
            .load_one(self.name.clone())
            .await
            .map_err(|e| Error::new(format!("Error reading base info: {e}")))
    }

    async fn address(&self, ctx: &Context<'_>) -> Result<Option<String>> {
        let base_info = self.load_base_info(ctx).await?;
        Ok(base_info.and_then(|base_info| Self::non_empty(base_info.address)))
    }

    async fn r#type(&self, ctx: &Context<'_>) -> Result<Option<String>> {
        let base_info = self.load_base_info(ctx).await?;
        Ok(base_info.and_then(|base_info| Self::non_empty(base_info.r#type)))
    }

    async fn protocol(&self, ctx: &Context<'_>) -> Result<Option<String>> {
        let base_info = self.load_base_info(ctx).await?;
        Ok(base_info.and_then(|base_info| Self::non_empty(base_info.protocol)))
    }

    async fn children(&self, ctx: &Context<'_>) -> Result<Vec<Device>> {
        let api = ctx.data_unchecked::<Arc<dyn UnrApi>>();
        let resp = api
            .read_relationships(self.name.clone())
            .await
            .map_err(|e| handle_error(e, "reading relationship"))?;

        let children = resp
            .relationship_info
            .map(|ri| ri.children_names)
            .unwrap_or_default();

        Ok(children.into_iter().map(Device::new).collect())
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
