//! Alarms DB Module
//!
//! Contains the logic for making calls to the Alarms Database gRPC Service.

pub mod groups;
pub mod layouts;
pub mod timers;

use crate::g_rpc::{
    connection_utils::{ConnectionAdapter, ConnectionPort},
    proto::services::alarms::{
        alarm_group_service_client::AlarmGroupServiceClient,
        alarm_timer_service_client::AlarmTimerServiceClient,
        user_layouts_service_client::UserLayoutsServiceClient,
    },
};
use std::sync::LazyLock;
use tokio::try_join;
use tonic::transport::{Channel, Error};

/// The environment variable name to use when requesting the location of the alarms DB service.
const GRPC_ALARMS_DB_HOST: &str = "GRPC_ALARMS_DB_HOST";

/// A static instance of [`ConnectionPort`] wrapping [`AlarmsDbConnectionAdapter`] to share among the submodules.
/// Utilizes [`LazyLock`] to only instantiate upon the first reference to this field.
static ALARMS_DB_CLIENT: LazyLock<ConnectionPort<AlarmsDbConnectionAdapter>> =
    LazyLock::new(|| ConnectionPort::new(GRPC_ALARMS_DB_HOST));

/// Implementation of [`ConnectionAdapter`] to hold the clients that invoke the gRPC endpoints supplied by the Alarms DB.
#[derive(Clone)]
struct AlarmsDbConnectionAdapter {
    pub groups_conn: AlarmGroupServiceClient<Channel>,
    pub layouts_conn: UserLayoutsServiceClient<Channel>,
    pub timers_conn: AlarmTimerServiceClient<Channel>,
}
impl ConnectionAdapter for AlarmsDbConnectionAdapter {
    async fn new(host: String) -> Result<Self, Error> {
        let (groups_conn, layouts_conn, timers_conn) = try_join!(
            AlarmGroupServiceClient::connect(host.clone()),
            UserLayoutsServiceClient::connect(host.clone()),
            AlarmTimerServiceClient::connect(host)
        )?;

        Ok(Self {
            groups_conn,
            layouts_conn,
            timers_conn,
        })
    }
}
