use crate::g_rpc::proto::services::aclk::{
    clock_event_client::ClockEventClient, EventInfo, SubscribeReq,
};

use crate::env_var;

const CLOCK_HOST: &str = "CLOCK_GRPC_HOST";
const DEFAULT_CLOCK_HOST: &str = "clx76.fnal.gov";

const CLOCK_PORT: &str = "CLOCK_GRPC_PORT";
const DEFAULT_CLOCK_PORT: &str = "6803";

pub async fn subscribe(
    events: &[i32],
) -> Result<tonic::Response<tonic::Streaming<EventInfo>>, tonic::Status> {
    let host = env_var::get(CLOCK_HOST).into_str_or(DEFAULT_CLOCK_HOST);
    let port = env_var::get(CLOCK_PORT).into_str_or(DEFAULT_CLOCK_PORT);
    let address = format!("http://{}:{}", host, port);
    match ClockEventClient::connect(address).await {
        Ok(mut client) => {
            let req = SubscribeReq {
                events: events.to_vec(),
            };

            client.subscribe(req).await
        }
        Err(_) => Err(tonic::Status::unavailable("clock service unavailable")),
    }
}
