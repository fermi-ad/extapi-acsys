//! UNR GraphQL Module
//!
//! Provides a resource/graph-oriented GraphQL schema for UNR data.

use crate::g_rpc::{proto::services::unr::BaseInfo, unr};
use async_graphql::{
    Context, Error, Object, Result, SimpleObject,
    dataloader::{DataLoader, HashMapCache},
};
use tonic::{Code, Status};
use tracing::error;
use uuid::Uuid;

pub mod loader;
pub mod types;

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
    parent: String, children: Vec<String>,
) -> Result<Device> {
    let relationship_info =
        crate::g_rpc::proto::services::unr::RelationshipInfo {
            parent_name: parent.clone(),
            children_names: children,
        };

    unr::update_relationship(relationship_info)
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
    async fn address(&self, ctx: &Context<'_>) -> Result<Option<String>> {
        let loader = ctx.data_unchecked::<DataLoader<loader::UnrBaseInfoLoader, HashMapCache>>();
        let base_info = loader
            .load_one(self.name.clone())
            .await
            .map_err(|_e| Error::new("Error reading base info."))?;

        Ok(base_info.and_then(|base_info| {
            (!base_info.address.is_empty()).then(|| base_info.address)
        }))
    }

    async fn r#type(&self, ctx: &Context<'_>) -> Result<Option<String>> {
        let loader = ctx.data_unchecked::<DataLoader<loader::UnrBaseInfoLoader, HashMapCache>>();
        let base_info = loader
            .load_one(self.name.clone())
            .await
            .map_err(|_e| Error::new("Error reading base info."))?;

        Ok(base_info.and_then(|base_info| {
            (!base_info.r#type.is_empty()).then(|| base_info.r#type)
        }))
    }

    async fn protocol(&self, ctx: &Context<'_>) -> Result<Option<String>> {
        let loader = ctx.data_unchecked::<DataLoader<loader::UnrBaseInfoLoader, HashMapCache>>();
        let base_info = loader
            .load_one(self.name.clone())
            .await
            .map_err(|_e| Error::new("Error reading base info."))?;

        Ok(base_info.and_then(|base_info| {
            (!base_info.protocol.is_empty()).then(|| base_info.protocol)
        }))
    }

    async fn children(&self) -> Result<Vec<Device>> {
        let resp = unr::read_relationship(self.name.clone())
            .await
            .map_err(|e| handle_error(e, "reading relationship"))?;

        // Service returns repeated RelationshipInfo; we expect at most one for a parent.
        let children = resp
            .relationship_info
            .into_iter()
            .find(|ri| ri.parent_name == self.name)
            .map(|ri| ri.children_names)
            .unwrap_or_default();

        Ok(children.into_iter().map(Device::new).collect())
    }
}

#[derive(Default)]
pub struct UnrQueries;

#[Object]
impl UnrQueries {
    async fn device(
        &self, ctx: &Context<'_>, name: String,
    ) -> Result<Option<Device>> {
        // Validate existence by checking BaseInfo.
        // Also prime the DataLoader cache so subsequent field resolvers don't re-fetch.
        let resp = unr::read_base_info(vec![name.clone()])
            .await
            .map_err(|e| handle_error(e, "reading base info"))?;

        let mut it = resp.base_info.into_iter();
        let base_info = it.find(|base_info| base_info.device_name == name);

        if let Some(base_info) = base_info {
            let loader = ctx.data_unchecked::<DataLoader<loader::UnrBaseInfoLoader, HashMapCache>>();
            loader.feed_one(name.clone(), base_info).await;
            Ok(Some(Device::new(name)))
        } else {
            Ok(None)
        }
    }

    async fn devices(
        &self, ctx: &Context<'_>, names: Vec<String>,
    ) -> Result<Vec<Device>> {
        // Validate existence by checking BaseInfo in a single batched call.
        // Also prime the DataLoader cache for all returned devices.
        let resp = unr::read_base_info(names.clone())
            .await
            .map_err(|e| handle_error(e, "reading base info"))?;

        let loader = ctx.data_unchecked::<DataLoader<loader::UnrBaseInfoLoader, HashMapCache>>();

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
            .filter(|n| present.contains(n))
            .map(Device::new)
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

        unr::create_base_info(base_info.clone())
            .await
            .map_err(|e| handle_error(e, "creating device"))?;

        // Read-your-writes: prime/overwrite BaseInfo cache for this request.
        let loader = ctx.data_unchecked::<DataLoader<loader::UnrBaseInfoLoader, HashMapCache>>();
        loader.feed_one(device_name.clone(), base_info).await;

        if let Some(children) = children {
            // Replace adjacency list.
            set_children_impl(device_name.clone(), children).await?;
        }

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

        unr::update_base_info(base_info.clone())
            .await
            .map_err(|e| handle_error(e, "updating device"))?;

        // Read-your-writes: prime/overwrite BaseInfo cache for this request.
        let loader = ctx.data_unchecked::<DataLoader<loader::UnrBaseInfoLoader, HashMapCache>>();
        loader.feed_one(device_name.clone(), base_info).await;

        Ok(Device::new(device_name))
    }

    async fn delete_devices(&self, names: Vec<String>) -> Result<Vec<String>> {
        unr::delete_base_info(names.clone())
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

        set_children_impl(parent, children).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_graphql::{EmptySubscription, Schema};

    #[test]
    fn schema_builds() {
        let _schema =
            Schema::build(UnrQueries, UnrMutations, EmptySubscription)
                .data(DataLoader::with_cache(
                    loader::UnrBaseInfoLoader::default(),
                    tokio::spawn,
                    HashMapCache::default(),
                ))
                .finish();
    }

    #[tokio::test]
    async fn mutation_returns_err_on_bad_connection() {
        let schema = Schema::build(UnrQueries, UnrMutations, EmptySubscription)
            .data(DataLoader::with_cache(
                loader::UnrBaseInfoLoader::default(),
                tokio::spawn,
                HashMapCache::default(),
            ))
            .finish();

        let result = schema
            .execute(
                r#"
                mutation {
                  createDevice(input: { name: "X", address: "A", type: "T", protocol: "P" }) { name }
                }
                "#,
            )
            .await;

        assert!(!result.errors.is_empty());
        assert!(
            result.errors[0]
                .message
                .starts_with("Error creating device.")
        );
    }

    #[tokio::test]
    async fn read_your_writes_is_implemented_via_feed_one() {
        // This is a unit-level regression test that validates our chosen mechanism
        // for read-your-writes: mutations call `DataLoader::feed_one()`.
        //
        // We can't fully integration-test read-your-writes without a mock UNR gRPC
        // service, because the mutation must succeed to return BaseInfo-backed fields.
        //
        // So we assert the behavior at the DataLoader layer directly.
        let loader = DataLoader::with_cache(
            loader::UnrBaseInfoLoader::default(),
            tokio::spawn,
            HashMapCache::default(),
        );

        let base_info = BaseInfo {
            device_name: "D".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        };

        loader.feed_one("D".to_string(), base_info.clone()).await;

        let got = loader.load_one("D".to_string()).await.unwrap();
        assert_eq!(got, Some(base_info));
    }
}
