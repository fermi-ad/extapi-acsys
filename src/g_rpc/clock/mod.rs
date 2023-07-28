use proto::{clock_event_client::ClockEventClient, SubscribeReq};

pub mod proto {
    tonic::include_proto!("clock_event");
}

pub async fn subscribe(
    events: &[i32],
) -> Result<tonic::Response<tonic::Streaming<proto::EventInfo>>, tonic::Status>
{
    match ClockEventClient::connect("http://clx76.fnal.gov:6803/").await {
        Ok(mut client) => {
            let req = SubscribeReq {
                events: events.to_vec(),
            };

            client.subscribe(req).await
        }
        Err(_) => Err(tonic::Status::unavailable("clock service unavailable")),
    }
}
