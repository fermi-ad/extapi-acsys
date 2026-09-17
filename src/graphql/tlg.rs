use crate::{config::GrpcConfig, g_rpc::tlg, graphql::auth_handlers::AuthInfo};
use async_graphql::{Context, Error, Object, Result};
use rust_grpc_lib::auth::ForwardedToken;
use tracing::error;

// Pull in our local types.

pub mod types;

#[derive(Default)]
pub struct TlgQueries;

#[Object]
impl TlgQueries {
    #[doc = "Returns the version of the TLG service"]
    async fn get_version(&self, ctx: &Context<'_>) -> Result<String> {
        let tlg_config = ctx.data::<GrpcConfig>()?;
        let token = ctx
            .data_opt::<AuthInfo>()
            .and_then(AuthInfo::token)
            .unwrap_or_default();

        tlg::get_version(tlg_config, ForwardedToken::new(token))
            .await
            .map_err(|e| Error::new(format!("{:?}", e)))
    }
}

#[derive(Default)]
pub struct TlgMutations;

#[Object]
impl TlgMutations {
    #[doc = "Returns the diagnostics of the requested devices"]
    async fn diagnostics_inline(
        &self, ctx: &Context<'_>, devices: types::TlgDevices,
    ) -> Result<types::TlgPlacementResponse> {
        let tlg_config = ctx.data::<GrpcConfig>()?;
        let token = ctx
            .data_opt::<AuthInfo>()
            .and_then(AuthInfo::token)
            .unwrap_or_default();

        match tlg::diagnostics(
            tlg_config,
            ForwardedToken::new(token),
            devices.into(),
        )
        .await
        {
            Ok(resp) => Ok(resp.into()),
            Err(e) => {
                let msg = format!("{:?}", e);

                error!("{}", &msg);
                Err(Error::new(msg))
            }
        }
    }

    #[doc = "Returns the placement of the requested devices"]
    async fn placement_inline(
        &self, ctx: &Context<'_>, devices: types::TlgDevices,
    ) -> Result<types::TlgPlacementResponse> {
        let tlg_config = ctx.data::<GrpcConfig>()?;
        let token = ctx
            .data_opt::<AuthInfo>()
            .and_then(AuthInfo::token)
            .unwrap_or_default();

        match tlg::placement(
            tlg_config,
            ForwardedToken::new(token),
            devices.into(),
        )
        .await
        {
            Ok(resp) => Ok(resp.into()),
            Err(e) => {
                let msg = format!("{:?}", e);

                error!("{}", &msg);
                Err(Error::new(msg))
            }
        }
    }
}
