//! Alarms DB Timers Module
//!
//! Provides functions for interacting with alarms timers.

use crate::g_rpc::{
    alarms_db::AlarmsDbConnectionAdapter,
    proto::services::alarms::{
        AlarmTimer, AlarmTimers, DeleteRequest, ReadRequest,
    },
};
use tonic::{Request, Status};

/// Creates a new [`AlarmTimer`] in the database.
pub async fn create(request: Request<AlarmTimer>) -> Result<(), Status> {
    let do_create = |mut client: AlarmsDbConnectionAdapter| async move {
        client.timers_conn.create(request).await
    };
    super::ALARMS_DB_CLIENT.run_with_client(do_create).await
}

/// Deletes the specified [`AlarmTimer`] from the database.
pub async fn delete(request: Request<DeleteRequest>) -> Result<(), Status> {
    let do_delete = |mut client: AlarmsDbConnectionAdapter| async move {
        client.timers_conn.delete(request).await
    };
    super::ALARMS_DB_CLIENT.run_with_client(do_delete).await
}

/// Reads all [`AlarmTimers`] of the specified [`TimerType`](crate::g_rpc::proto::services::alarms::TimerType) for a given user.
pub async fn read(
    request: Request<ReadRequest>,
) -> Result<AlarmTimers, Status> {
    let do_read = |mut client: AlarmsDbConnectionAdapter| async move {
        client.timers_conn.read(request).await
    };
    super::ALARMS_DB_CLIENT.run_with_client(do_read).await
}

/// Updates a specified [`AlarmTimer`] with new data.
pub async fn update(request: Request<AlarmTimer>) -> Result<(), Status> {
    let do_update = |mut client: AlarmsDbConnectionAdapter| async move {
        client.timers_conn.update(request).await
    };
    super::ALARMS_DB_CLIENT.run_with_client(do_update).await
}
