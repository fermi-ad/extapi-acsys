//! GraphQL Module for the Universal Name Registry service
//!
//! Provides a resource/graph-oriented GraphQL schema for UNR data.

use crate::g_rpc::proto::services::unr::BaseInfo;
use std::sync::Arc;

use self::api::UnrApi;
use async_graphql::{
    Context, Error, ErrorExtensions, Object, Result, SimpleObject,
    dataloader::{DataLoader, HashMapCache},
};
use tonic::{Code, Status};
use tracing::error;
use uuid::Uuid;

pub mod api;
pub mod loader;
pub mod types;

#[cfg(test)]
mod tests;

fn handle_error(e: Status, gerund: &str) -> Error {
    let err_id = Uuid::new_v4();
    error!("{err_id} gRPC Error {gerund}: {e:?}");
    match e.code() {
        Code::InvalidArgument => {
            Error::new(format!("{e} (Error ID: {err_id})"))
        }
        _ => Error::new(format!(
            "Error {gerund}. See server logs for details. (Error ID: {err_id})"
        )),
    }
}

async fn set_children_impl(
    api: &dyn UnrApi, parent: String, children: Vec<String>,
) -> Result<Device> {
    // Setting children to empty means "remove all relationships".
    if children.is_empty() {
        return api
            .delete_relationships(parent.clone())
            .await
            .map(|_| Device::new(parent))
            .map_err(|e| handle_error(e, "setting children"));
    }

    let relationship_info =
        crate::g_rpc::proto::services::unr::RelationshipInfo {
            parent_name: parent.clone(),
            children_names: children,
        };

    api.update_relationships(relationship_info)
        .await
        .map(|_| Device::new(parent))
        .map_err(|e| handle_error(e, "setting children"))
}

#[derive(Clone, Debug, SimpleObject)]
#[graphql(complex)]
pub struct Device {
    pub name: String,
}

impl Device {
    fn new(name: String) -> Self {
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
            .map_err(|_e| Error::new("Error reading base info."))
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

#[derive(Default)]
pub struct UnrQueries;

#[Object]
impl UnrQueries {
    async fn devices(
        &self, ctx: &Context<'_>, names: Option<Vec<String>>,
    ) -> Result<Vec<types::DeviceQueryResult>> {
        let names = names.unwrap_or_default();

        // Validate existence by checking BaseInfo in a single batched call.
        // Also prime the DataLoader cache for all returned devices.
        //
        // UNR semantics: empty `device_names` means "return all rows".
        let api = ctx.data_unchecked::<Arc<dyn UnrApi>>();
        let resp = api
            .read_base_info(names.clone())
            .await
            .map_err(|e| handle_error(e, "reading base info"))?;

        let loader = ctx.data_unchecked::<DataLoader<loader::UnrBaseInfoLoader, HashMapCache>>();

        // If the client requested specific names, we return a per-name union
        // (Device | NotFound). If they omitted `names`, we return all devices.
        if names.is_empty() {
            // Prime the DataLoader cache for all returned devices.
            for base_info in &resp.base_info {
                loader
                    .feed_one(base_info.device_name.clone(), base_info.clone())
                    .await;
            }

            // Return all devices (no NotFound entries).
            return Ok(resp
                .base_info
                .into_iter()
                .map(|bi| {
                    types::DeviceQueryResult::Device(Device::new(
                        bi.device_name,
                    ))
                })
                .collect());
        }

        let mut present: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for base_info in resp.base_info {
            present.insert(base_info.device_name.clone());
            loader
                .feed_one(base_info.device_name.clone(), base_info)
                .await;
        }

        Ok(names
            .into_iter()
            .map(|n| {
                if present.contains(&n) {
                    types::DeviceQueryResult::Device(Device::new(n))
                } else {
                    types::DeviceQueryResult::NotFound(types::NotFound {
                        name: n,
                    })
                }
            })
            .collect())
    }
}

#[derive(Default)]
pub struct UnrMutations;

#[Object]
impl UnrMutations {
    async fn create_device(
        &self, ctx: &Context<'_>, input: types::CreateDeviceInput,
    ) -> Result<Device> {
        let children = input.children.clone();
        let device_name = input.name.clone();

        let base_info = BaseInfo {
            device_name: device_name.clone(),
            address: input.address,
            r#type: input.r#type,
            protocol: input.protocol,
        };

        let api = ctx.data_unchecked::<Arc<dyn UnrApi>>();

        // Pre-validate children existence before creating anything.
        // This avoids partially-completed writes when a child doesn't exist.
        if let Some(children) = children.as_ref()
            && !children.is_empty()
        {
            let resp = api
                .read_base_info(children.clone())
                .await
                .map_err(|e| handle_error(e, "validating children"))?;

            let present: std::collections::HashSet<&str> = resp
                .base_info
                .iter()
                .map(|bi| bi.device_name.as_str())
                .collect();

            if let Some(missing) =
                children.iter().find(|c| !present.contains(c.as_str()))
            {
                return Err(Error::new(format!(
                    "Child device does not exist: {missing}"
                )));
            }
        }

        api.create_base_info(base_info.clone())
            .await
            .map_err(|e| handle_error(e, "creating device"))?;

        // Add relationships (if requested).
        // Note: this is not atomic with base_info creation; if this fails, the
        // device exists but has no/partial relationships (acceptable).
        if let Some(children) = children
            && let Err(e) =
                set_children_impl(api.as_ref(), device_name.clone(), children)
                    .await
        {
            return Err(e.extend_with(|_, ext| {
                ext.set("deviceCreated", true);
                ext.set("childrenAdded", false);
            }));
        }

        // Read-your-writes: prime/overwrite BaseInfo cache for this request.
        let loader = ctx.data_unchecked::<DataLoader<loader::UnrBaseInfoLoader, HashMapCache>>();
        loader.feed_one(device_name.clone(), base_info).await;

        Ok(Device::new(device_name))
    }

    async fn update_device(
        &self, ctx: &Context<'_>, input: types::UpdateDeviceInput,
    ) -> Result<Device> {
        let device_name = input.name.clone();

        let base_info = BaseInfo {
            device_name: device_name.clone(),
            address: input.address,
            r#type: input.r#type,
            protocol: input.protocol,
        };

        let api = ctx.data_unchecked::<Arc<dyn UnrApi>>();

        // grpc-unr-service update does not signal missing devices. Make
        // semantics explicit by pre-checking existence.
        let exists = api
            .read_base_info(vec![device_name.clone()])
            .await
            .map_err(|e| handle_error(e, "validating device exists"))?
            .base_info
            .iter()
            .any(|bi| bi.device_name == device_name);

        if !exists {
            return Err(Error::new(format!(
                "Device does not exist: {device_name}"
            )));
        }

        api.update_base_info(base_info.clone())
            .await
            .map_err(|e| handle_error(e, "updating device"))?;

        // Read-your-writes: prime/overwrite BaseInfo cache for this request.
        let loader = ctx.data_unchecked::<DataLoader<loader::UnrBaseInfoLoader, HashMapCache>>();
        loader.feed_one(device_name.clone(), base_info).await;

        Ok(Device::new(device_name))
    }

    async fn delete_devices(
        &self, ctx: &Context<'_>, names: Vec<String>,
    ) -> Result<Vec<String>> {
        if names.is_empty() {
            return Err(Error::new(
                "deleteDevices requires at least one device name",
            ));
        }

        let api = ctx.data_unchecked::<Arc<dyn UnrApi>>();
        api.delete_base_info(names.clone())
            .await
            .map_err(|e| handle_error(e, "deleting devices"))?;
        Ok(names)
    }

    async fn set_children(
        &self, ctx: &Context<'_>, parent: String, children: Vec<String>,
    ) -> Result<Device> {
        // Relationship mutation doesn't change BaseInfo, but we still want
        // read-your-writes for BaseInfo fields if the client requests them.
        // Ensure the loader has at least the parent cached if it exists.
        // (No-op if it doesn't.)
        let loader = ctx.data_unchecked::<DataLoader<loader::UnrBaseInfoLoader, HashMapCache>>();
        let _ = loader.load_one(parent.clone()).await;

        let api = ctx.data_unchecked::<Arc<dyn UnrApi>>();
        set_children_impl(api.as_ref(), parent, children).await
    }
}
