use crate::g_rpc::proto::services::unr::BaseInfo;
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

/// Batch loader for UNR BaseInfo records.
///
/// Keys are UNR device names.
#[derive(Clone)]
pub struct UnrBaseInfoLoader {
    pub api: Arc<dyn UnrApi>,
}

impl UnrBaseInfoLoader {
    pub fn new(api: Arc<dyn UnrApi>) -> Self {
        Self { api }
    }
}

impl Loader<String> for UnrBaseInfoLoader {
    type Value = BaseInfo;
    type Error = LoaderError;

    async fn load(
        &self, keys: &[String],
    ) -> Result<HashMap<String, Self::Value>, Self::Error> {
        // De-dupe keys while preserving stable ordering.
        let mut uniq: Vec<String> = Vec::with_capacity(keys.len());
        let mut seen =
            std::collections::HashSet::<&str>::with_capacity(keys.len());
        for k in keys {
            if seen.insert(k.as_str()) {
                uniq.push(k.clone());
            }
        }

        self.api
            .read_base_info(uniq)
            .await
            .map_err(|e| {
                tracing::warn!("UnrBaseInfoLoader: gRPC error: {e:?}");
                LoaderError(e.to_string())
            })
            .map(|resp| {
                resp.base_info
                    .into_iter()
                    .map(|base_info| (base_info.device_name.clone(), base_info))
                    .collect()
            })
    }
}
