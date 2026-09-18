//! GraphQL Module for the Universal Name Registry service
//!
//! Provides a resource/graph-oriented GraphQL schema for UNR data.

use std::{collections::HashSet, sync::Arc};

use crate::{
    config::{ExtapiGlobalConfig, GrpcConfig},
    g_rpc::proto::services::unr::{entity::Entity, relationship::Relationship},
    graphql::auth_handlers::AuthInfo,
};

use self::api::UnrApi;
use async_graphql::{
    Context, Error, ErrorExtensions, Object, Result,
    dataloader::{DataLoader, HashMapCache},
};
use rust_grpc_lib::auth::ForwardedToken;
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
    api: &dyn UnrApi, unr_config: &GrpcConfig, token: ForwardedToken,
    parent: String, children: Vec<String>,
) -> Result<types::Device> {
    // Read the current children for this parent.
    let resp = api
        .read_relationships(unr_config, token.clone(), vec![parent.clone()])
        .await
        .map_err(|e| handle_error(e, "reading current relationships"))?;

    let current_children: HashSet<String> = resp
        .entries
        .into_iter()
        .find(|entry| entry.id == parent)
        .map(|entry| entry.children.into_iter().collect())
        .unwrap_or_default();

    let requested_children: HashSet<String> = children.into_iter().collect();

    // Delete relationships for children present in current but not requested.
    let to_delete: Vec<Relationship> = current_children
        .difference(&requested_children)
        .map(|child_id| Relationship {
            parent_id: parent.clone(),
            child_id: child_id.clone(),
        })
        .collect();

    if !to_delete.is_empty() {
        api.delete_relationships(unr_config, token.clone(), to_delete)
            .await
            .map_err(|e| handle_error(e, "deleting relationships"))?;
    }

    // Create relationships for children requested but not currently present.
    let to_create: Vec<Relationship> = requested_children
        .difference(&current_children)
        .map(|child_id| Relationship {
            parent_id: parent.clone(),
            child_id: child_id.clone(),
        })
        .collect();

    if !to_create.is_empty() {
        api.create_relationships(unr_config, token, to_create)
            .await
            .map_err(|e| handle_error(e, "creating relationships"))?;
    }

    Ok(types::Device::new(parent))
}

#[derive(Default)]
pub struct UnrQueries;

#[Object]
impl UnrQueries {
    async fn devices(
        &self, ctx: &Context<'_>, names: Option<Vec<String>>,
    ) -> Result<Vec<types::DeviceQueryResult>> {
        let names = names.unwrap_or_default();

        // Validate existence by checking entities in a single batched call.
        // Also prime the DataLoader cache for all returned devices.
        //
        // UNR semantics: empty `ids` means "return all rows".
        let api = ctx.data::<Arc<dyn UnrApi>>()?;
        let global_config = ctx.data::<Arc<ExtapiGlobalConfig>>()?;
        let token = ctx
            .data_opt::<AuthInfo>()
            .and_then(|info| info.token())
            .unwrap_or_default();
        let resp = api
            .read_entities(
                &global_config.unr,
                ForwardedToken::new(token),
                names.clone(),
            )
            .await
            .map_err(|e| handle_error(e, "reading entities"))?;

        let loader =
            ctx.data::<DataLoader<loader::UnrEntityLoader, HashMapCache>>()?;

        // If the client requested specific names, we return a per-name union
        // (Device | NotFound). If they omitted `names`, we return all devices.
        if names.is_empty() {
            // Prime the DataLoader cache for all returned entities.
            for entity in &resp.entities {
                loader.feed_one(entity.id.clone(), entity.clone()).await;
            }

            // Return all devices (no NotFound entries).
            return Ok(resp
                .entities
                .into_iter()
                .map(|e| {
                    types::DeviceQueryResult::Device(types::Device::new(e.id))
                })
                .collect());
        }

        let mut present = HashSet::new();
        for entity in resp.entities {
            present.insert(entity.id.clone());
            loader.feed_one(entity.id.clone(), entity).await;
        }

        Ok(names
            .into_iter()
            .map(|n| {
                if present.contains(&n) {
                    types::DeviceQueryResult::Device(types::Device::new(n))
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
    ) -> Result<types::Device> {
        let api = ctx.data::<Arc<dyn UnrApi>>()?;
        let global_config = ctx.data::<Arc<ExtapiGlobalConfig>>()?;
        let token = ctx
            .data_opt::<AuthInfo>()
            .and_then(|info| info.token())
            .unwrap_or_default();

        let children = input.children.clone();
        let device_name = input.name.clone();

        let entity = Entity {
            id: device_name.clone(),
            address: input.address,
            r#type: input.r#type,
            protocol: input.protocol,
        };

        // Pre-validate children existence before creating anything.
        // This avoids partially-completed writes when a child doesn't exist.
        if let Some(children) = children.as_ref()
            && !children.is_empty()
        {
            let resp = api
                .read_entities(
                    &global_config.unr,
                    ForwardedToken::new(token.clone()),
                    children.clone(),
                )
                .await
                .map_err(|e| handle_error(e, "validating children"))?;

            let present: HashSet<&str> = resp
                .entities
                .iter()
                .map(|entity| entity.id.as_str())
                .collect();

            if let Some(missing) =
                children.iter().find(|c| !present.contains(c.as_str()))
            {
                return Err(Error::new(format!(
                    "Child device does not exist: {missing}"
                )));
            }
        }

        api.create_entities(
            &global_config.unr,
            ForwardedToken::new(token.clone()),
            vec![entity],
        )
        .await
        .map_err(|e| handle_error(e, "creating device"))?;

        // Add relationships (if requested).
        // Note: this is not atomic with entity creation; if this fails, the
        // device exists but has no/partial relationships (acceptable).
        if let Some(children) = children
            && let Err(e) = set_children_impl(
                api.as_ref(),
                &global_config.unr,
                ForwardedToken::new(token),
                device_name.clone(),
                children,
            )
            .await
        {
            return Err(e.extend_with(|_, ext| {
                ext.set("deviceCreated", true);
                ext.set("childrenAdded", false);
            }));
        }

        Ok(types::Device::new(device_name))
    }

    async fn update_device(
        &self, ctx: &Context<'_>, input: types::UpdateDeviceInput,
    ) -> Result<types::Device> {
        let api = ctx.data::<Arc<dyn UnrApi>>()?;
        let global_config = ctx.data::<Arc<ExtapiGlobalConfig>>()?;
        let token = ctx
            .data_opt::<AuthInfo>()
            .and_then(|info| info.token())
            .unwrap_or_default();

        let device_name = input.name.clone();

        let entity = Entity {
            id: device_name.clone(),
            address: input.address,
            r#type: input.r#type,
            protocol: input.protocol,
        };

        // grpc-unr-service update does not signal missing devices. Make
        // semantics explicit by pre-checking existence.
        let exists = api
            .read_entities(
                &global_config.unr,
                ForwardedToken::new(token.clone()),
                vec![device_name.clone()],
            )
            .await
            .map_err(|e| handle_error(e, "validating device exists"))?
            .entities
            .iter()
            .any(|e| e.id == device_name);

        if !exists {
            return Err(Error::new(format!(
                "Device does not exist: {device_name}"
            )));
        }

        api.update_entities(
            &global_config.unr,
            ForwardedToken::new(token),
            vec![entity.clone()],
        )
        .await
        .map_err(|e| handle_error(e, "updating device"))?;

        // Read-your-writes: prime/overwrite BaseInfo cache for this request.
        let loader =
            ctx.data::<DataLoader<loader::UnrEntityLoader, HashMapCache>>()?;
        loader.feed_one(device_name.clone(), entity).await;

        Ok(types::Device::new(device_name))
    }

    async fn delete_devices(
        &self, ctx: &Context<'_>, names: Vec<String>,
    ) -> Result<Vec<String>> {
        let api = ctx.data::<Arc<dyn UnrApi>>()?;
        let global_config = ctx.data::<Arc<ExtapiGlobalConfig>>()?;
        let token = ctx
            .data_opt::<AuthInfo>()
            .and_then(|info| info.token())
            .unwrap_or_default();

        if names.is_empty() {
            return Err(Error::new(
                "deleteDevices requires at least one device name",
            ));
        }

        api.delete_entities(
            &global_config.unr,
            ForwardedToken::new(token),
            names.clone(),
        )
        .await
        .map_err(|e| handle_error(e, "deleting devices"))?;
        Ok(names)
    }

    async fn set_children(
        &self, ctx: &Context<'_>, parent: String, children: Vec<String>,
    ) -> Result<types::Device> {
        // Relationship mutation doesn't change entity data, but we still want
        // read-your-writes for entity fields if the client requests them.
        // Ensure the loader has at least the parent cached if it exists.
        // (No-op if it doesn't.)
        let loader =
            ctx.data::<DataLoader<loader::UnrEntityLoader, HashMapCache>>()?;
        let _ = loader.load_one(parent.clone()).await;

        let api = ctx.data::<Arc<dyn UnrApi>>()?;
        let global_config = ctx.data::<Arc<ExtapiGlobalConfig>>()?;
        let token = ctx
            .data_opt::<AuthInfo>()
            .and_then(|info| info.token())
            .unwrap_or_default();

        set_children_impl(
            api.as_ref(),
            &global_config.unr,
            ForwardedToken::new(token),
            parent,
            children,
        )
        .await
    }
}
