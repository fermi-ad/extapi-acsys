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
        proto::{
            google::protobuf::Empty,
            services::mi8_autotune::v1::{
                self as proto, ListBpmsResponse, SetArmedStateRequest,
                SetOperatingModeRequest, SubscribeArmedStateResponse,
                SubscribeAutotuneEventResponse, SubscribeOperatingModeResponse,
            },
        },
    },
    graphql::{auth_handlers::forwarded_token, errors::handle_graphql_error},
};

mod types;
use types::{ArmedState, AutotuneEventUpdate, OperatingMode};

/// A boxed upstream stream whose items can fail with a gRPC status.
pub type StatusStream<T> = BoxStream<'static, std::result::Result<T, Status>>;

/// Small injectable API surface for testing resolver and authentication behavior.
#[async_trait]
pub trait Mi8AutotuneApi: Send + Sync {
    async fn list_bpms(
        &self, config: &GrpcConfig, token: ForwardedToken,
    ) -> std::result::Result<ListBpmsResponse, Status>;

    async fn set_armed_state(
        &self, config: &GrpcConfig, token: ForwardedToken,
        request: SetArmedStateRequest,
    ) -> std::result::Result<Empty, Status>;

    async fn set_operating_mode(
        &self, config: &GrpcConfig, token: ForwardedToken,
        request: SetOperatingModeRequest,
    ) -> std::result::Result<Empty, Status>;

    async fn subscribe_armed_state(
        &self, config: &GrpcConfig, token: ForwardedToken,
    ) -> std::result::Result<StatusStream<SubscribeArmedStateResponse>, Status>;

    async fn subscribe_operating_mode(
        &self, config: &GrpcConfig, token: ForwardedToken,
    ) -> std::result::Result<StatusStream<SubscribeOperatingModeResponse>, Status>;

    async fn subscribe_events(
        &self, config: &GrpcConfig, token: ForwardedToken,
    ) -> std::result::Result<StatusStream<SubscribeAutotuneEventResponse>, Status>;
}

pub struct GrpcMi8AutotuneApi;

#[async_trait]
impl Mi8AutotuneApi for GrpcMi8AutotuneApi {
    async fn list_bpms(
        &self, config: &GrpcConfig, token: ForwardedToken,
    ) -> std::result::Result<ListBpmsResponse, Status> {
        mi8_autotune::list_bpms(config, token).await
    }

    async fn set_armed_state(
        &self, config: &GrpcConfig, token: ForwardedToken,
        request: SetArmedStateRequest,
    ) -> std::result::Result<Empty, Status> {
        mi8_autotune::set_armed_state(config, token, request).await
    }

    async fn set_operating_mode(
        &self, config: &GrpcConfig, token: ForwardedToken,
        request: SetOperatingModeRequest,
    ) -> std::result::Result<Empty, Status> {
        mi8_autotune::set_operating_mode(config, token, request).await
    }

    async fn subscribe_armed_state(
        &self, config: &GrpcConfig, token: ForwardedToken,
    ) -> std::result::Result<StatusStream<SubscribeArmedStateResponse>, Status>
    {
        Ok(mi8_autotune::subscribe_armed_state(config, token)
            .await?
            .boxed())
    }

    async fn subscribe_operating_mode(
        &self, config: &GrpcConfig, token: ForwardedToken,
    ) -> std::result::Result<StatusStream<SubscribeOperatingModeResponse>, Status>
    {
        Ok(mi8_autotune::subscribe_operating_mode(config, token)
            .await?
            .boxed())
    }

    async fn subscribe_events(
        &self, config: &GrpcConfig, token: ForwardedToken,
    ) -> std::result::Result<StatusStream<SubscribeAutotuneEventResponse>, Status>
    {
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
pub struct Mi8AutotuneMutations;

#[Object]
impl Mi8AutotuneMutations {
    /// Requests an armed-state change. Success acknowledges the RPC, not observed state.
    async fn set_armed_state(
        &self, ctx: &Context<'_>, armed_state: ArmedState,
    ) -> Result<bool> {
        let config = ctx.data::<Arc<ExtapiGlobalConfig>>()?;
        let upstream = config.mi8_autotune.require()?;
        let armed_state = match armed_state {
            ArmedState::Armed => proto::ArmedState::Armed,
            ArmedState::Disarmed => proto::ArmedState::Disarmed,
            ArmedState::Unknown => {
                return Err(
                    "UNKNOWN is not a valid armed state to request".into()
                );
            }
        };
        ctx.data::<Arc<dyn Mi8AutotuneApi>>()?
            .set_armed_state(
                upstream,
                forwarded_token(ctx),
                SetArmedStateRequest {
                    armed_state: armed_state as i32,
                },
            )
            .await
            .map_err(handle_graphql_error)?;
        Ok(true)
    }

    /// Requests an operating-mode change. LIVE can request control-system settings.
    async fn set_operating_mode(
        &self, ctx: &Context<'_>, operating_mode: OperatingMode,
    ) -> Result<bool> {
        let config = ctx.data::<Arc<ExtapiGlobalConfig>>()?;
        let upstream = config.mi8_autotune.require()?;
        let operating_mode = match operating_mode {
            OperatingMode::Live => proto::OperatingMode::Live,
            OperatingMode::Shadow => proto::OperatingMode::Shadow,
            OperatingMode::Unknown => {
                return Err(
                    "UNKNOWN is not a valid operating mode to request".into()
                );
            }
        };
        ctx.data::<Arc<dyn Mi8AutotuneApi>>()?
            .set_operating_mode(
                upstream,
                forwarded_token(ctx),
                SetOperatingModeRequest {
                    operating_mode: operating_mode as i32,
                },
            )
            .await
            .map_err(handle_graphql_error)?;
        Ok(true)
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
