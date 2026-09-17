use std::{collections::HashMap, sync::Arc};

use async_graphql::dataloader::Loader;
use rust_grpc_lib::auth::ForwardedToken;

use crate::config::GrpcConfig;
use crate::g_rpc::proto::services::unr::entity::Entity;
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
    token: ForwardedToken,
    unr_config: GrpcConfig,
}

impl UnrEntityLoader {
    pub fn new(
        api: Arc<dyn UnrApi>, unr_config: GrpcConfig, token: ForwardedToken,
    ) -> Self {
        Self {
            api,
            token,
            unr_config,
        }
    }
}

impl Loader<String> for UnrEntityLoader {
    type Value = Entity;
    type Error = LoaderError;

    async fn load(
        &self, keys: &[String],
    ) -> Result<HashMap<String, Self::Value>, Self::Error> {
        self.api
            .read_entities(&self.unr_config, self.token.clone(), keys.to_vec())
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
