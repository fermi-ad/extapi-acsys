use std::sync::Arc;

use crate::{
    config::ExtapiGlobalConfig,
    g_rpc::{
        proto::scanner::{ScanRequest, ScanResult},
        wscan,
    },
    graphql::auth_handlers::AuthInfo,
};

use async_graphql::{Context, Error, Object, Result, Subscription, types::ID};
use futures_util::{Stream, StreamExt};
use rust_grpc_lib::auth::ForwardedToken;
use tracing::{error, info};

// Pull in our local types.

pub mod types;

// Create a zero-sized struct to attach the GraphQL handlers.

#[derive(Default)]
pub struct ScannerQueries;

// Define the schema's query entry points. Any methods defined in this
// section will appear in the schema.

#[Object]
impl ScannerQueries {
    #[doc = "Queries the scanning service for scan configurations."]
    async fn retrieve_scans(
        &self,
        #[graphql(
            desc = "If `id` is `null`, all configurations are returned. If `id` is not `null`, the configuration associated with the ID will be returned. If no ID matches, an empty list is returned."
        )]
        _id: Option<ID>,
    ) -> Vec<types::ScanConfiguration> {
        vec![]
    }

    #[doc = "Requests the progress of the motion station associated with the `id`."]
    async fn get_progress(
        &self, ctx: &Context<'_>,
        #[graphql(desc = "Specifies which scanner station to query.")] id: ID,
    ) -> Result<types::ScanCurrentState> {
        let global_config = ctx.data::<Arc<ExtapiGlobalConfig>>()?;
        let token = ctx
            .data_opt::<AuthInfo>()
            .and_then(AuthInfo::token)
            .unwrap_or_default();

        wscan::get_progress(
            &global_config.wscan,
            ForwardedToken::new(token),
            id.0.clone(),
        )
        .await
        .map(|resp| types::ScanCurrentState::from(resp.into_inner()))
        .map_err(|e| {
            Error::new(format!("error scanning detector {}: {e}", id.0))
        })
    }
}

#[derive(Default)]
pub struct ScannerMutations;

#[Object]
impl ScannerMutations {
    #[doc = "Requests that a scan be started with the configuration specified \
	     by the `id` parameter. If a scan was successfully started, an ID \
	     will be returned. If it couldn't be started, `null` is returned."]
    async fn request_scan(&self, _id: ID) -> Option<ID> {
        None
    }

    #[doc = "Requests that a scan be stopped. The `id` parameter is the value \
	     obtained from a previous `request_scan` command or from a scan \
	     progress query."]
    async fn abort_scan(&self, ctx: &Context<'_>, id: ID) -> Result<bool> {
        let global_config = ctx.data::<Arc<ExtapiGlobalConfig>>()?;
        let token = ctx
            .data_opt::<AuthInfo>()
            .and_then(AuthInfo::token)
            .unwrap_or_default();

        Ok(wscan::abort_scan(
            &global_config.wscan,
            ForwardedToken::new(token),
            id.0,
        )
        .await
        .is_ok())
    }
}

#[derive(Default)]
pub struct ScannerSubscriptions;

#[Subscription]
impl ScannerSubscriptions {
    #[doc = "Starts a scan at the specified station."]
    async fn get_scanner_state(
        &self, ctx: &Context<'_>, id: ID,
    ) -> Result<impl Stream<Item = types::ScanResult>> {
        info!("requesting scan at station {}", &id.0);
        let global_config = ctx.data::<Arc<ExtapiGlobalConfig>>()?;
        let token = ctx
            .data_opt::<AuthInfo>()
            .and_then(AuthInfo::token)
            .unwrap_or_default();

        wscan::start_scan(
            &global_config.wscan,
            ForwardedToken::new(token),
            ScanRequest {
                detector_id: id.0,
                position_start: 0.0,
                position_end: 0.0,
                position_step: 0.0,
                sampling_duration: 0.0,
                pulses_per_sample: 0,
            },
        )
        .await
        .map_err(|e| {
            let message = format!("{e}");
            error!(message);
            Error::new(message)
        })
        .map(|s| {
            s.into_inner().map(Result::unwrap).map(
                |ScanResult { progress, voltage }| types::ScanResult {
                    progress: types::ScanCurrentState::from(progress.unwrap()),
                    voltage,
                },
            )
        })
    }
}
