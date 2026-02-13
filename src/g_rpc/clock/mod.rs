use crate::g_rpc::proto::services::aclk::{
    clock_event_client::ClockEventClient, EventInfo, SubscribeReq,
};

use crate::env_var;

const CLOCK_HOST: &str = "CLOCK_GRPC_HOST";

pub async fn subscribe(
    events: &[i32],
) -> Result<tonic::Response<tonic::Streaming<EventInfo>>, tonic::Status> {
    let host: String = env_var::expect(CLOCK_HOST);
    match ClockEventClient::connect(host).await {
        Ok(mut client) => {
            let req = SubscribeReq {
                events: events.to_vec(),
            };

            client.subscribe(req).await
        }
        Err(_) => Err(tonic::Status::unavailable("clock service unavailable")),
    }
}
