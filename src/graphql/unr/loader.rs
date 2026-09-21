use crate::g_rpc::proto::services::unr::{
    entity::Entity, relationship::RelationshipDetails,
};
use async_graphql::dataloader::Loader;
use std::{collections::HashMap, sync::Arc};

use crate::graphql::unr::api::UnrApi;

#[derive(Clone, Debug)]
pub struct LoaderError(pub String);

impl std::fmt::Display for LoaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for LoaderError {}
#[derive(Clone)]
pub struct UnrEntityLoader {
    pub api: Arc<dyn UnrApi>,
}

impl UnrEntityLoader {
    pub fn new(api: Arc<dyn UnrApi>) -> Self {
        Self { api }
    }
}

impl Loader<String> for UnrEntityLoader {
    type Value = Entity;
    type Error = LoaderError;

    async fn load(
        &self, keys: &[String],
    ) -> Result<HashMap<String, Self::Value>, Self::Error> {
        self.api
            .read_entities(keys.to_vec())
            .await
            .map_err(|e| {
                tracing::warn!("UnrEntityLoader: gRPC error: {e:?}");
                LoaderError(e.to_string())
            })
            .map(|resp| {
                resp.entities
                    .into_iter()
                    .map(|entity| (entity.id.clone(), entity))
                    .collect()
            })
    }
}

/// Batch loader for UNR Relationship records.
///
/// Keys are UNR entity IDs.
#[derive(Clone)]
pub struct UnrRelationshipLoader {
    pub api: Arc<dyn UnrApi>,
}

impl UnrRelationshipLoader {
    pub fn new(api: Arc<dyn UnrApi>) -> Self {
        Self { api }
    }
}

impl Loader<String> for UnrRelationshipLoader {
    type Value = RelationshipDetails;
    type Error = LoaderError;

    async fn load(
        &self, keys: &[String],
    ) -> Result<HashMap<String, Self::Value>, Self::Error> {
        self.api
            .read_relationships(keys.to_vec())
            .await
            .map_err(|e| {
                tracing::warn!("UnrRelationshipLoader: gRPC error: {e:?}");
                LoaderError(e.to_string())
            })
            .map(|resp| {
                resp.entries
                    .into_iter()
                    .map(|details| (details.id.clone(), details))
                    .collect()
            })
    }
}
