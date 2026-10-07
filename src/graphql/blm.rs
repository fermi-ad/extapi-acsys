use std::sync::Arc;

use async_graphql::{Context, Error, Object, Result, Subscription};
use futures_util::{Stream, StreamExt};
use rust_grpc_lib::auth::ForwardedToken;

use crate::{
    config::ExtapiGlobalConfig,
    g_rpc::{
        blm,
        proto::services::blm::v1::{
            BeamLine, ListIntegratedLossRequest,
            SubscribeBeamThroughputRequest, SubscribeLossRatioRequest,
        },
    },
    graphql::auth_handlers::AuthInfo,
};

pub mod types;

use types::{BlmBeamThroughputSample, BlmDevice, BlmLossRatioSample};

fn forwarded_token(ctx: &Context<'_>) -> ForwardedToken {
    let token = ctx
        .data_opt::<AuthInfo>()
        .and_then(AuthInfo::token)
        .unwrap_or_default();
    ForwardedToken::new(token)
}

fn validate_tclk_event(value: i32) -> Result<u32> {
    u8::try_from(value)
        .map(u32::from)
        .map_err(|_| Error::new("tclkEvent must be between 0 and 255"))
}

fn sample_rate(value: i32) -> Result<u32> {
    if value > 0 {
        Ok(value.cast_unsigned())
    } else {
        Err(Error::new("sampleRateMs must be greater than zero"))
    }
}

#[derive(Default)]
pub struct BlmQueries;

#[Object]
impl BlmQueries {
    /// Returns integrated-loss devices for the selected beamline and TCLK event.
    async fn integrated_loss_devices(
        &self, ctx: &Context<'_>, beam_line: BeamLine, tclk_event: i32,
    ) -> Result<Vec<BlmDevice>> {
        let global_config = ctx.data::<Arc<ExtapiGlobalConfig>>()?;
        let response = blm::list_integrated_loss_devices(
            &global_config.blm,
            forwarded_token(ctx),
            ListIntegratedLossRequest {
                beam_line: beam_line as i32,
                tclk_event: validate_tclk_event(tclk_event)?,
            },
        )
        .await
        .map_err(|error| Error::new(format!("BLM service error: {error}")))?;

        Ok(response.data.into_iter().map(BlmDevice::from).collect())
    }
}

#[derive(Default)]
pub struct BlmSubscriptions;

#[Subscription]
impl BlmSubscriptions {
    /// Streams loss-ratio samples for the selected beamline and TCLK event.
    async fn loss_ratios(
        &self, ctx: &Context<'_>, beam_line: BeamLine, tclk_event: i32,
        sample_rate_ms: i32,
    ) -> Result<impl Stream<Item = Result<BlmLossRatioSample>>> {
        let global_config = ctx.data::<Arc<ExtapiGlobalConfig>>()?;
        let stream = blm::subscribe_loss_ratio(
            &global_config.blm,
            forwarded_token(ctx),
            SubscribeLossRatioRequest {
                beam_line: beam_line as i32,
                tclk_event: validate_tclk_event(tclk_event)?,
                sample_rate: sample_rate(sample_rate_ms)?,
            },
        )
        .await
        .map_err(|error| Error::new(format!("BLM service error: {error}")))?;

        Ok(stream.map(|result| {
            result.map(BlmLossRatioSample::from).map_err(|error| {
                Error::new(format!("BLM stream error: {error}"))
            })
        }))
    }

    /// Streams beam-throughput samples for the selected beamline.
    async fn beam_throughput(
        &self, ctx: &Context<'_>, beam_line: BeamLine, sample_rate_ms: i32,
    ) -> Result<impl Stream<Item = Result<BlmBeamThroughputSample>>> {
        let global_config = ctx.data::<Arc<ExtapiGlobalConfig>>()?;
        let stream = blm::subscribe_beam_throughput(
            &global_config.blm,
            forwarded_token(ctx),
            SubscribeBeamThroughputRequest {
                beam_line: beam_line as i32,
                sample_rate: sample_rate(sample_rate_ms)?,
            },
        )
        .await
        .map_err(|error| Error::new(format!("BLM service error: {error}")))?;

        Ok(stream.map(|result| {
            result.map(BlmBeamThroughputSample::from).map_err(|error| {
                Error::new(format!("BLM stream error: {error}"))
            })
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_rate_accepts_positive_values() {
        assert_eq!(sample_rate(1).unwrap(), 1);
    }

    #[test]
    fn sample_rate_rejects_zero_and_negative_values() {
        assert!(sample_rate(0).is_err());
        assert!(sample_rate(-1).is_err());
    }

    #[test]
    fn schema_preserves_protobuf_beam_line_names() {
        let schema = async_graphql::Schema::build(
            BlmQueries,
            async_graphql::EmptyMutation,
            BlmSubscriptions,
        )
        .finish();
        let sdl = schema.sdl();

        assert!(sdl.contains("BEAM_LINE_UNSPECIFIED"));
        assert!(sdl.contains("BEAM_LINE_400MEV"));
        assert!(sdl.contains("BEAM_LINE_BOOSTER"));
        assert!(sdl.contains("BEAM_LINE_DELIVERY_RING"));
    }
}
