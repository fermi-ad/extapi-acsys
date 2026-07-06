//! Alarms Service gRPC Module
//!
//! Contains the logic for making calls to the grpc-alarms service

use crate::g_rpc::{
    connection_utils::{ConnectionAdapter, ConnectionPort},
    proto::{
        common::alarm,
        google::protobuf::{Empty, Timestamp},
        services::alarms::{
            AcknowledgeRequest, ActivateRequest, BypassRequest,
            SnapshotResponse, SnoozeRequest,
            alarm_commands_client::AlarmCommandsClient,
        },
    },
};
use chrono::{DateTime, Timelike, Utc};
use std::sync::LazyLock;
use tonic::{
    Response, Status,
    transport::{Channel, Error},
};

/// The environment variable name to use when requesting the location of the alarms gRPC service.
const GRPC_ALARMS_SERVICE_HOST: &str = "GRPC_ALARMS_SERVICE_HOST";

/// A static instance of [`ConnectionPort`] wrapping [`AlarmsServiceConnectionAdapter`].
/// Utilizes [`LazyLock`] to only instantiate upon the first reference to this field.
static ALARMS_SERVICE_CLIENT: LazyLock<
    ConnectionPort<AlarmsServiceConnectionAdapter>,
> = LazyLock::new(|| ConnectionPort::new(GRPC_ALARMS_SERVICE_HOST));

/// Makes a request to the alarms gRPC service to acknowledge the specified alarms.
pub async fn acknowledge_alarms(
    devices: Vec<String>, updated_by: String,
) -> Result<Empty, Status> {
    let request = AcknowledgeRequest {
        devices,
        user: updated_by,
    };
    let do_ack = |mut client: AlarmsServiceConnectionAdapter| async move {
        client.conn.acknowledge(request).await
    };
    ALARMS_SERVICE_CLIENT.run_with_client(do_ack).await
}

/// Makes a request to the alarms gRPC service to activate (unbypass) the specified alarms.
pub async fn activate_alarms(
    devices: Vec<String>, updated_by: String,
) -> Result<Empty, Status> {
    let request = ActivateRequest {
        devices,
        user: updated_by,
    };
    let do_activate = |mut client: AlarmsServiceConnectionAdapter| async move {
        client.conn.activate(request).await
    };
    ALARMS_SERVICE_CLIENT.run_with_client(do_activate).await
}

/// Makes a request to the alarms gRPC service to bypass the specified alarms.
pub async fn bypass_alarms(
    devices: Vec<String>, updated_by: String,
) -> Result<Empty, Status> {
    let request = BypassRequest {
        devices,
        user: updated_by,
    };
    let do_bypass = |mut client: AlarmsServiceConnectionAdapter| async move {
        client.conn.bypass(request).await
    };
    ALARMS_SERVICE_CLIENT.run_with_client(do_bypass).await
}

/// Makes a request to the alarms gRPC service to get a snapshot of the non-Ok alarms.
pub async fn get_snapshot() -> Result<Vec<alarm::Status>, Status> {
    let response = ALARMS_SERVICE_CLIENT
        .run_with_client(do_snapshot_request)
        .await?;
    Ok(response.snapshot)
}

/// Makes a request to the alarms gRPC service to snooze the specified alarms until the provided wake time.
pub async fn snooze_alarms(
    devices: Vec<String>, updated_by: String, wake: DateTime<Utc>,
) -> Result<Empty, Status> {
    let request = SnoozeRequest {
        devices,
        user: updated_by,
        wake: Some(Timestamp {
            seconds: wake.timestamp(),
            nanos: wake.nanosecond() as i32,
        }),
    };
    let do_snooze = |mut client: AlarmsServiceConnectionAdapter| async move {
        client.conn.snooze(request).await
    };
    ALARMS_SERVICE_CLIENT.run_with_client(do_snooze).await
}

async fn do_snapshot_request(
    mut client: AlarmsServiceConnectionAdapter,
) -> Result<Response<SnapshotResponse>, Status> {
    client.conn.get_snapshot(Empty {}).await
}

/// An implementation of [`ConnectionAdapter`] to hold the [`AlarmCommandsClient`] reference.
#[derive(Clone)]
struct AlarmsServiceConnectionAdapter {
    pub conn: AlarmCommandsClient<Channel>,
}
impl ConnectionAdapter for AlarmsServiceConnectionAdapter {
    async fn new(host: String) -> Result<Self, Error> {
        let conn = AlarmCommandsClient::connect(host).await?;
        Ok(Self { conn })
    }
}
