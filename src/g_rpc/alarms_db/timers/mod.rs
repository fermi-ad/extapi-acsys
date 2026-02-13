use crate::g_rpc::proto::services::alarms::{
    AlarmTimer, AlarmTimers, DeleteRequest, ReadRequest,
    alarm_timer_service_client::AlarmTimerServiceClient,
};

use tonic::{Request, Status};

pub async fn create(request: Request<AlarmTimer>) -> Result<(), Status> {
    super::execute_with_client(
        AlarmTimerServiceClient::connect,
        |mut client| async move { client.create(request).await },
    )
    .await
}

pub async fn delete(request: Request<DeleteRequest>) -> Result<(), Status> {
    super::execute_with_client(
        AlarmTimerServiceClient::connect,
        |mut client| async move { client.delete(request).await },
    )
    .await
}

pub async fn read(
    request: Request<ReadRequest>,
) -> Result<AlarmTimers, Status> {
    super::execute_with_client(
        AlarmTimerServiceClient::connect,
        |mut client| async move { client.read(request).await },
    )
    .await
}

pub async fn update(request: Request<AlarmTimer>) -> Result<(), Status> {
    super::execute_with_client(
        AlarmTimerServiceClient::connect,
        |mut client| async move { client.update(request).await },
    )
    .await
}
