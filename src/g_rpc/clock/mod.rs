use tokio::time::Duration;
use tonic::transport::Endpoint;
use crate::g_rpc::proto::services::aclk::{
    clock_event_client::ClockEventClient, EventInfo, SubscribeReq,
};
use crate::env_var;

const CLOCK_HOST: &str = "CLOCK_GRPC_HOST";
const DEFAULT_CLOCK_HOST: &str = "http://clx76.fnal.gov:6803";

pub async fn subscribe(
    events: &[i32],
) -> Result<tonic::Response<tonic::Streaming<EventInfo>>, tonic::Status> {
    let host = env_var::get(CLOCK_HOST)
        .or_else(|| DEFAULT_CLOCK_HOST.to_owned());
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
