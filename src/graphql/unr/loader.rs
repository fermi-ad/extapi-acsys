use crate::g_rpc::proto::services::unr::BaseInfo;
use async_graphql::dataloader::Loader;
use std::{collections::HashMap, convert::Infallible, sync::Arc};

use crate::graphql::unr::api::UnrApi;

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
    type Error = Infallible;

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

        // UNR returns BaseInfo rows for the requested names.
        // Missing devices are simply absent from the response.
        let resp = self.api.read_base_info(uniq).await;

        // DataLoader's Loader::Error must be Clone; tonic::Status isn't.
        // We treat transport/service errors as "no values" and let callers
        // surface errors via their existing error-handling paths.
        let Ok(resp) = resp else {
            return Ok(HashMap::new());
        };

        Ok(resp
            .base_info
            .into_iter()
            .map(|base_info| (base_info.device_name.clone(), base_info))
            .collect())
    }
}
