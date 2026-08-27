//! UNR GraphQL Module
//!
//! Provides a resource/graph-oriented GraphQL schema for UNR data.

use crate::g_rpc::{proto::services::unr::BaseInfo, unr};
use async_graphql::{Error, Object, Result, SimpleObject};
use tonic::{Code, Status};
use tracing::error;
use uuid::Uuid;

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

    // Prefer update; if NotFound, fall back to create.
    match unr::update_relationship(relationship_info.clone()).await {
        Ok(_) => Ok(Device::new(parent)),
        Err(e) if e.code() == Code::NotFound => {
            unr::create_relationship(relationship_info)
                .await
                .map_err(|e| handle_error(e, "setting children"))?;
            Ok(Device::new(parent))
        }
        Err(e) => Err(handle_error(e, "setting children")),
    }
}

#[derive(Clone, Debug, SimpleObject)]
#[graphql(complex)]
pub struct Device {
    pub name: String,
    pub address: Option<String>,
    pub r#type: Option<String>,
    pub protocol: Option<String>,
}

impl From<types::Device> for Device {
    fn from(v: types::Device) -> Self {
        Self {
            name: v.name,
            address: v.address,
            r#type: v.r#type,
            protocol: v.protocol,
        }
    }
}

impl Device {
    fn new(name: String) -> Self {
        Self {
            name,
            address: None,
            r#type: None,
            protocol: None,
        }
    }
}

#[async_graphql::ComplexObject]
impl Device {
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
    async fn device(&self, name: String) -> Result<Option<Device>> {
        let resp = unr::read_base_info(vec![name.clone()])
            .await
            .map_err(|e| handle_error(e, "reading base info"))?;

        let bi = resp.base_info.into_iter().find(|bi| bi.device_name == name);

        Ok(bi.map(|bi| Device {
            name: bi.device_name,
            address: (!bi.address.is_empty()).then(|| bi.address),
            r#type: (!bi.r#type.is_empty()).then(|| bi.r#type),
            protocol: (!bi.protocol.is_empty()).then(|| bi.protocol),
        }))
    }

    async fn devices(&self, names: Vec<String>) -> Result<Vec<Device>> {
        let resp = unr::read_base_info(names.clone())
            .await
            .map_err(|e| handle_error(e, "reading base info"))?;

        // Preserve request order; omit names that have no BaseInfo row.
        let mut by_name = std::collections::HashMap::new();
        for bi in resp.base_info {
            by_name.insert(
                bi.device_name.clone(),
                Device {
                    name: bi.device_name,
                    address: (!bi.address.is_empty()).then(|| bi.address),
                    r#type: (!bi.r#type.is_empty()).then(|| bi.r#type),
                    protocol: (!bi.protocol.is_empty()).then(|| bi.protocol),
                },
            );
        }

        Ok(names
            .into_iter()
            .filter_map(|n| by_name.remove(&n))
            .collect())
    }
}

#[derive(Default)]
pub struct UnrMutations;

#[Object]
impl UnrMutations {
    async fn create_device(
        &self, input: types::CreateDeviceInput,
    ) -> Result<Device> {
        let children = input.children.clone();
        let device_name = input.name.clone();

        let base_info = BaseInfo {
            device_name: device_name.clone(),
            address: input.address,
            r#type: input.r#type,
            protocol: input.protocol,
        };

        unr::create_base_info(base_info)
            .await
            .map_err(|e| handle_error(e, "creating device"))?;

        if let Some(children) = children {
            // Replace adjacency list.
            set_children_impl(device_name.clone(), children).await?;
        }

        Ok(Device::new(device_name))
    }

    async fn update_device(
        &self, input: types::UpdateDeviceInput,
    ) -> Result<Device> {
        let device_name = input.name.clone();

        let base_info = BaseInfo {
            device_name: device_name.clone(),
            address: input.address,
            r#type: input.r#type,
            protocol: input.protocol,
        };

        unr::update_base_info(base_info)
            .await
            .map_err(|e| handle_error(e, "updating device"))?;

        Ok(Device::new(device_name))
    }

    async fn delete_devices(&self, names: Vec<String>) -> Result<Vec<String>> {
        unr::delete_base_info(names.clone())
            .await
            .map_err(|e| handle_error(e, "deleting devices"))?;
        Ok(names)
    }

    async fn set_children(
        &self, parent: String, children: Vec<String>,
    ) -> Result<Device> {
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
            Schema::build(UnrQueries, UnrMutations, EmptySubscription).finish();
    }

    #[tokio::test]
    async fn mutation_returns_err_on_bad_connection() {
        let schema =
            Schema::build(UnrQueries, UnrMutations, EmptySubscription).finish();

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
}
