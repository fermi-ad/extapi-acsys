use std::sync::Arc;

use async_graphql::{
    Context, Object, Result, Subscription, async_trait::async_trait,
};
use futures_util::{Stream, StreamExt, stream::BoxStream};
use rust_grpc_lib::auth::ForwardedToken;
use tonic::Status;

use crate::{
    config::{ExtapiGlobalConfig, GrpcConfig},
    g_rpc::{
        mi8_autotune,
        proto::services::mi8_autotune::v1::{
            ListBpmsResponse, SubscribeArmedStateResponse,
            SubscribeAutotuneEventResponse, SubscribeOperatingModeResponse,
        },
    },
    graphql::{auth_handlers::AuthInfo, errors::handle_graphql_error},
};

mod types;
use types::{ArmedState, AutotuneEventUpdate, OperatingMode};

fn forwarded_token(ctx: &Context<'_>) -> ForwardedToken {
    ForwardedToken::new(
        ctx.data_opt::<AuthInfo>()
            .and_then(AuthInfo::token)
            .unwrap_or_default(),
    )
}

/// Small injectable API surface for testing resolver and authentication behavior.
#[async_trait]
pub trait Mi8AutotuneApi: Send + Sync {
    async fn list_bpms(
        &self, config: &GrpcConfig, token: ForwardedToken,
    ) -> std::result::Result<ListBpmsResponse, Status>;

    async fn subscribe_armed_state(
        &self, config: &GrpcConfig, token: ForwardedToken,
    ) -> std::result::Result<
        BoxStream<
            'static,
            std::result::Result<SubscribeArmedStateResponse, Status>,
        >,
        Status,
    >;

    async fn subscribe_operating_mode(
        &self, config: &GrpcConfig, token: ForwardedToken,
    ) -> std::result::Result<
        BoxStream<
            'static,
            std::result::Result<SubscribeOperatingModeResponse, Status>,
        >,
        Status,
    >;

    async fn subscribe_events(
        &self, config: &GrpcConfig, token: ForwardedToken,
    ) -> std::result::Result<
        BoxStream<
            'static,
            std::result::Result<SubscribeAutotuneEventResponse, Status>,
        >,
        Status,
    >;
}

pub struct GrpcMi8AutotuneApi;

#[async_trait]
impl Mi8AutotuneApi for GrpcMi8AutotuneApi {
    async fn list_bpms(
        &self, config: &GrpcConfig, token: ForwardedToken,
    ) -> std::result::Result<ListBpmsResponse, Status> {
        mi8_autotune::list_bpms(config, token).await
    }

    async fn subscribe_armed_state(
        &self, config: &GrpcConfig, token: ForwardedToken,
    ) -> std::result::Result<
        BoxStream<
            'static,
            std::result::Result<SubscribeArmedStateResponse, Status>,
        >,
        Status,
    > {
        Ok(mi8_autotune::subscribe_armed_state(config, token)
            .await?
            .boxed())
    }

    async fn subscribe_operating_mode(
        &self, config: &GrpcConfig, token: ForwardedToken,
    ) -> std::result::Result<
        BoxStream<
            'static,
            std::result::Result<SubscribeOperatingModeResponse, Status>,
        >,
        Status,
    > {
        Ok(mi8_autotune::subscribe_operating_mode(config, token)
            .await?
            .boxed())
    }

    async fn subscribe_events(
        &self, config: &GrpcConfig, token: ForwardedToken,
    ) -> std::result::Result<
        BoxStream<
            'static,
            std::result::Result<SubscribeAutotuneEventResponse, Status>,
        >,
        Status,
    > {
        Ok(mi8_autotune::subscribe_events(config, token).await?.boxed())
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
        let response = ctx
            .data::<Arc<dyn Mi8AutotuneApi>>()?
            .list_bpms(upstream, forwarded_token(ctx))
            .await
            .map_err(handle_graphql_error)?;

        Ok(response.bpms)
    }
}

#[derive(Default)]
pub struct Mi8AutotuneSubscriptions;

#[Subscription]
impl Mi8AutotuneSubscriptions {
    /// Streams the upstream's current armed state, then updates.
    async fn armed_state(
        &self, ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = Result<ArmedState>>> {
        let config = ctx.data::<Arc<ExtapiGlobalConfig>>()?;
        let upstream = config.mi8_autotune.require()?;
        let stream = ctx
            .data::<Arc<dyn Mi8AutotuneApi>>()?
            .subscribe_armed_state(upstream, forwarded_token(ctx))
            .await
            .map_err(handle_graphql_error)?;
        Ok(stream.map(|result| {
            result
                .map(|response| response.armed_state.into())
                .map_err(handle_graphql_error)
        }))
    }

    /// Streams the upstream's current operating mode, then updates.
    async fn operating_mode(
        &self, ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = Result<OperatingMode>>> {
        let config = ctx.data::<Arc<ExtapiGlobalConfig>>()?;
        let upstream = config.mi8_autotune.require()?;
        let stream = ctx
            .data::<Arc<dyn Mi8AutotuneApi>>()?
            .subscribe_operating_mode(upstream, forwarded_token(ctx))
            .await
            .map_err(handle_graphql_error)?;
        Ok(stream.map(|result| {
            result
                .map(|response| response.operating_mode.into())
                .map_err(handle_graphql_error)
        }))
    }

    /// Streams the upstream's current event and associated message, then updates.
    async fn events(
        &self, ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = Result<AutotuneEventUpdate>>> {
        let config = ctx.data::<Arc<ExtapiGlobalConfig>>()?;
        let upstream = config.mi8_autotune.require()?;
        let stream = ctx
            .data::<Arc<dyn Mi8AutotuneApi>>()?
            .subscribe_events(upstream, forwarded_token(ctx))
            .await
            .map_err(handle_graphql_error)?;
        Ok(stream.map(|result| {
            result
                .map(AutotuneEventUpdate::from)
                .map_err(handle_graphql_error)
        }))
    }
}

#[cfg(test)]
mod tests;
