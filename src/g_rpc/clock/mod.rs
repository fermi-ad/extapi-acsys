use crate::g_rpc::proto::services::aclk::{
    EventInfo, SubscribeReq, clock_event_client::ClockEventClient,
};
use rust_env_var_lib::env_var;
use tokio::time::Duration;
use tonic::transport::Endpoint;

const CLOCK_HOST: &str = "CLOCK_GRPC_HOST";

pub async fn subscribe(
    events: &[i32],
) -> Result<tonic::Response<tonic::Streaming<EventInfo>>, tonic::Status> {
    let host: String = env_var::expect(CLOCK_HOST);
    let endpoint = Endpoint::from_shared(host)
        .map_err(|e| {
            tonic::Status::invalid_argument(format!("Invalid host URI: {}", e))
        })?
        .connect_timeout(Duration::from_secs(2));
    let channel = endpoint
        .connect()
        .await
        .map_err(|_| tonic::Status::unavailable("clock service unavailable"))?;
    let mut client = ClockEventClient::new(channel);
    let req = SubscribeReq {
        events: events.to_vec(),
    };

    client.subscribe(req).await
}
