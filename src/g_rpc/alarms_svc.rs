//! Alarms Service gRPC Module
//!
//! Contains the logic for making calls to the grpc-alarms service

use chrono::{DateTime, Timelike, Utc};
use rust_grpc_lib::auth::ForwardedToken;
use tonic::{Response, Status};

use crate::{
    config::GrpcConfig,
    g_rpc::{
        proto::{
            common::alarm,
            google::protobuf::{Empty, Timestamp},
            services::alarm_commands::{
                AcknowledgeRequest, ActivateRequest, BypassRequest,
                SnoozeRequest, alarm_commands_client::AlarmCommandsClient,
            },
        },
        utils::handle_rpc_error,
    },
};

/// Makes a request to the alarms gRPC service to acknowledge the specified alarms.
pub async fn acknowledge_alarms(
    alarms_svc_config: &GrpcConfig, token: ForwardedToken,
    devices: Vec<String>, updated_by: String,
) -> Result<Empty, Status> {
    let mut client = AlarmCommandsClient::from_endpoint_with_provider(
        &alarms_svc_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "gRPC Alarms"))?;

    let request = AcknowledgeRequest {
        devices,
        user: updated_by,
    };
    client.acknowledge(request).await.map(Response::into_inner)
}

/// Makes a request to the alarms gRPC service to activate (unbypass) the specified alarms.
pub async fn activate_alarms(
    alarms_svc_config: &GrpcConfig, token: ForwardedToken,
    devices: Vec<String>, updated_by: String,
) -> Result<Empty, Status> {
    let mut client = AlarmCommandsClient::from_endpoint_with_provider(
        &alarms_svc_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "gRPC Alarms"))?;

    let request = ActivateRequest {
        devices,
        user: updated_by,
    };
    client.activate(request).await.map(Response::into_inner)
}

/// Makes a request to the alarms gRPC service to bypass the specified alarms.
pub async fn bypass_alarms(
    alarms_svc_config: &GrpcConfig, token: ForwardedToken,
    devices: Vec<String>, updated_by: String,
) -> Result<Empty, Status> {
    let mut client = AlarmCommandsClient::from_endpoint_with_provider(
        &alarms_svc_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "gRPC Alarms"))?;

    let request = BypassRequest {
        devices,
        user: updated_by,
    };
    client.bypass(request).await.map(Response::into_inner)
}

/// Makes a request to the alarms gRPC service to get a snapshot of the non-Ok alarms.
pub async fn get_snapshot(
    alarms_svc_config: &GrpcConfig, token: ForwardedToken,
) -> Result<Vec<alarm::Status>, Status> {
    let mut client = AlarmCommandsClient::from_endpoint_with_provider(
        &alarms_svc_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "gRPC Alarms"))?;

    client
        .get_snapshot(Empty {})
        .await
        .map(|resp| resp.into_inner().snapshot)
}

/// Makes a request to the alarms gRPC service to snooze the specified alarms until the provided wake time.
pub async fn snooze_alarms(
    alarms_svc_config: &GrpcConfig, token: ForwardedToken,
    devices: Vec<String>, updated_by: String, wake: DateTime<Utc>,
) -> Result<Empty, Status> {
    let mut client = AlarmCommandsClient::from_endpoint_with_provider(
        &alarms_svc_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "gRPC Alarms"))?;

    let request = SnoozeRequest {
        devices,
        user: updated_by,
        wake: Some(Timestamp {
            seconds: wake.timestamp(),
            nanos: wake.nanosecond() as i32,
        }),
    };
    client.snooze(request).await.map(Response::into_inner)
}
