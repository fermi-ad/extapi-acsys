//! Alarms DB User Layouts Module
//!
//! Provides functions for interacting with alarms user layouts.

use crate::g_rpc::{
    alarms_db::AlarmsDbConnectionAdapter,
    proto::{google::protobuf::Empty, services::alarms::UserLayouts},
};
use tonic::{Request, Status};

/// Requests all [`UserLayouts`] from the database.
pub async fn read_layouts(
    request: Request<Empty>,
) -> Result<UserLayouts, Status> {
    let do_read = |mut client: AlarmsDbConnectionAdapter| async move {
        client.layouts_conn.get_user_layouts(request).await
    };
    super::ALARMS_DB_CLIENT.run_with_client(do_read).await
}
