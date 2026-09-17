use crate::{
    config::GrpcConfig,
    g_rpc::{
        proto::services::clock_event::{
            EventInfo, SubscribeReq, clock_event_client::ClockEventClient,
        },
        utils::handle_rpc_error,
    },
};
use rust_grpc_lib::auth::ForwardedToken;

pub async fn subscribe(
    clock_config: &GrpcConfig, token: ForwardedToken, events: &[i32],
) -> Result<tonic::Response<tonic::Streaming<EventInfo>>, tonic::Status> {
    let mut client = ClockEventClient::from_endpoint_with_provider(
        &clock_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "Clock Service"))?;

    let req = SubscribeReq {
        events: events.to_vec(),
    };

    client.subscribe(req).await
}
