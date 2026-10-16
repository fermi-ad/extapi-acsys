use std::{collections::HashMap, sync::Arc};

use async_graphql::dataloader::Loader;
use rust_grpc_lib::auth::ForwardedToken;

use crate::config::ExtapiGlobalConfig;
use crate::g_rpc::proto::services::unr::{
    entity::Entity, relationship::RelationshipDetails,
};
use crate::graphql::unr::api::UnrApi;

#[derive(Clone, Debug)]
pub struct LoaderError(pub String);

impl std::fmt::Display for LoaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for LoaderError {}

/// Batch loader for UNR Entity records.
///
/// Keys are UNR entity IDs.
#[derive(Clone)]
pub struct UnrEntityLoader {
    api: Arc<dyn UnrApi>,
    global_config: Arc<ExtapiGlobalConfig>,
    token: ForwardedToken,
}

impl UnrEntityLoader {
    pub fn new(
        api: Arc<dyn UnrApi>, global_config: Arc<ExtapiGlobalConfig>,
        token: ForwardedToken,
    ) -> Self {
        Self {
            api,
            global_config,
            token,
        }
    }
}

impl Loader<String> for UnrEntityLoader {
    type Value = Entity;
    type Error = LoaderError;

    async fn load(
        &self, keys: &[String],
    ) -> Result<HashMap<String, Self::Value>, Self::Error> {
        // Unreachable in practice: `/unr` is not mounted when UNR is disabled.
        // Kept as a safety net; the loader's error type cannot carry the
        // FEATURE_DISABLED code.
        let unr_config = self
            .global_config
            .unr
            .require()
            .map_err(|e| LoaderError(e.message()))?;
        self.api
            .read_entities(unr_config, self.token.clone(), keys.to_vec())
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

#[derive(Clone)]
pub struct UnrRelationshipLoader {
    api: Arc<dyn UnrApi>,
    global_config: Arc<ExtapiGlobalConfig>,
    token: ForwardedToken,
}

impl UnrRelationshipLoader {
    pub fn new(
        api: Arc<dyn UnrApi>, global_config: Arc<ExtapiGlobalConfig>,
        token: ForwardedToken,
    ) -> Self {
        Self {
            api,
            global_config,
            token,
        }
    }
}

impl Loader<String> for UnrRelationshipLoader {
    type Value = RelationshipDetails;
    type Error = LoaderError;

    async fn load(
        &self, keys: &[String],
    ) -> Result<HashMap<String, Self::Value>, Self::Error> {
        // Unreachable in practice: `/unr` is not mounted when UNR is disabled.
        // Kept as a safety net; the loader's error type cannot carry the
        // FEATURE_DISABLED code.
        let unr_config = self
            .global_config
            .unr
            .require()
            .map_err(|e| LoaderError(e.message()))?;
        self.api
            .read_relationships(unr_config, self.token.clone(), keys.to_vec())
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
