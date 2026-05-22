//! Alarms DB User Layouts Module
//!
//! Provides functions for interacting with alarms user layouts.

use crate::g_rpc::{
    alarms_db::AlarmsDbConnectionAdapter,
    proto::{google::protobuf::Empty, services::alarms::UserLayouts},
};
use tonic::{Response, Status};

/// Requests all [`UserLayouts`] from the database.
pub async fn read_layouts() -> Result<UserLayouts, Status> {
    super::ALARMS_DB_CLIENT
        .run_with_client(get_user_layouts)
        .await
}

// Named function (rather than a closure) so it can be passed directly
// to `run_with_client` without capturing.
async fn get_user_layouts(
    mut client: AlarmsDbConnectionAdapter,
) -> Result<Response<UserLayouts>, Status> {
    client.layouts_conn.get_user_layouts(Empty {}).await
}
