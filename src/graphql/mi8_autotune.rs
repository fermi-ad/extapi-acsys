use std::sync::Arc;

use async_graphql::{Context, Object, Result, async_trait::async_trait};
use rust_grpc_lib::auth::ForwardedToken;
use tonic::Status;

use crate::{
    config::{ExtapiGlobalConfig, GrpcConfig},
    g_rpc::{
        mi8_autotune, proto::services::mi8_autotune::v1::ListBpmsResponse,
    },
    graphql::{auth_handlers::AuthInfo, errors::handle_graphql_error},
};

/// Small injectable API surface for testing resolver and authentication behavior.
#[async_trait]
pub trait Mi8AutotuneApi: Send + Sync {
    async fn list_bpms(
        &self, config: &GrpcConfig, token: ForwardedToken,
    ) -> std::result::Result<ListBpmsResponse, Status>;
}

pub struct GrpcMi8AutotuneApi;

#[async_trait]
impl Mi8AutotuneApi for GrpcMi8AutotuneApi {
    async fn list_bpms(
        &self, config: &GrpcConfig, token: ForwardedToken,
    ) -> std::result::Result<ListBpmsResponse, Status> {
        mi8_autotune::list_bpms(config, token).await
    }
}

#[derive(Default)]
pub struct Mi8AutotuneQueries;

#[Object]
impl Mi8AutotuneQueries {
    /// Returns the BPM names that the MI8 autotuner is configured to read.
    async fn bpms(&self, ctx: &Context<'_>) -> Result<Vec<String>> {
        let config = ctx.data::<Arc<ExtapiGlobalConfig>>()?;
        let upstream = config.mi8_autotune.require()?;
        let token = ctx
            .data_opt::<AuthInfo>()
            .and_then(AuthInfo::token)
            .unwrap_or_default();
        let response = ctx
            .data::<Arc<dyn Mi8AutotuneApi>>()?
            .list_bpms(upstream, ForwardedToken::new(token))
            .await
            .map_err(handle_graphql_error)?;

        Ok(response.bpms)
    }
}

#[cfg(test)]
mod tests;
