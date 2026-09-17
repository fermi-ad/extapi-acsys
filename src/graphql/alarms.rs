//! Alarms GraphQL Module
//!
//! Provides the query implementations for the Alarms GraphQL interface.

use crate::{
    config::{GrpcConfig, KafkaConfig},
    g_rpc::{alarms_db, alarms_svc},
    graphql::{alarms::types::Alarm, auth_handlers::AuthInfo},
};
#[cfg(feature = "kafka")]
use async_graphql::Subscription;
use async_graphql::{Context, Error, Object};
use chrono::{DateTime, Utc};
use rust_grpc_lib::auth::ForwardedToken;
#[cfg(feature = "kafka")]
use rust_pubsub_lib::{KafkaSubscriber, StringMessage, Subscriber};
#[cfg(feature = "kafka")]
use tokio_stream::{Stream, StreamExt};
use tonic::{Code, Status};
use tracing::error;
use types::{AlarmGroup, AlarmGroupMetadatum, AlarmTimer, UserLayout};
use uuid::Uuid;

#[cfg(test)]
mod tests;
mod types;
mod utils;

pub struct AlarmConfigWrapper {
    pub alarms_db: GrpcConfig,
    pub alarms_kafka: KafkaConfig,
    pub alarms_svc: GrpcConfig,
}

/// Describes the mutations (data writes/updates) allowed by the GQL interface.
#[derive(Default)]
pub struct AlarmsMutations;
#[Object]
impl AlarmsMutations {
    /// A request to acknowledge the specified alarms.
    async fn acknowledge_alarms(
        &self, ctx: &Context<'_>, devices: Vec<String>, updated_by: String,
    ) -> Result<Vec<String>, Error> {
        let alarms_svc_config = &ctx.data::<AlarmConfigWrapper>()?.alarms_svc;
        let token = ctx
            .data_opt::<AuthInfo>()
            .and_then(|info| info.token())
            .unwrap_or_default();

        match alarms_svc::acknowledge_alarms(
            alarms_svc_config,
            ForwardedToken::new(token),
            devices.clone(),
            updated_by,
        )
        .await
        {
            Ok(_) => Ok(devices),
            Err(e) => handle_error(e, "acknowledging alarms"),
        }
    }

    /// A request to activate (unbypass) the specified alarms.
    async fn activate_alarms(
        &self, ctx: &Context<'_>, devices: Vec<String>, updated_by: String,
    ) -> Result<Vec<String>, Error> {
        let alarms_svc_config = &ctx.data::<AlarmConfigWrapper>()?.alarms_svc;
        let token = ctx
            .data_opt::<AuthInfo>()
            .and_then(|info| info.token())
            .unwrap_or_default();

        match alarms_svc::activate_alarms(
            alarms_svc_config,
            ForwardedToken::new(token),
            devices.clone(),
            updated_by,
        )
        .await
        {
            Ok(_) => Ok(devices),
            Err(e) => handle_error(e, "activating alarms"),
        }
    }

    /// A request to bypass the specified alarms.
    async fn bypass_alarms(
        &self, ctx: &Context<'_>, devices: Vec<String>, updated_by: String,
    ) -> Result<Vec<String>, Error> {
        let alarms_svc_config = &ctx.data::<AlarmConfigWrapper>()?.alarms_svc;
        let token = ctx
            .data_opt::<AuthInfo>()
            .and_then(|info| info.token())
            .unwrap_or_default();

        match alarms_svc::bypass_alarms(
            alarms_svc_config,
            ForwardedToken::new(token),
            devices.clone(),
            updated_by,
        )
        .await
        {
            Ok(_) => Ok(devices),
            Err(e) => handle_error(e, "bypassing alarms"),
        }
    }

    /// A request to create an alarms timer of the specified [`TimerType`](crate::g_rpc::proto::services::alarms::TimerType).
    async fn create_alarm_timer(
        &self, ctx: &Context<'_>, device: String,
        end_time: Option<DateTime<Utc>>, timer_type: String,
        updated_by: String,
    ) -> Result<AlarmTimer, Error> {
        let alarms_db_config = &ctx.data::<AlarmConfigWrapper>()?.alarms_db;
        let token = ctx
            .data_opt::<AuthInfo>()
            .and_then(|info| info.token())
            .unwrap_or_default();

        match alarms_db::timers::create(
            alarms_db_config,
            ForwardedToken::new(token),
            device,
            end_time,
            timer_type,
            updated_by,
        )
        .await
        {
            Ok(alarm_timer) => Ok(AlarmTimer::from(alarm_timer)),
            Err(e) => handle_error(e, "creating alarm timer"),
        }
    }

    /// A request to delete an alarms timer of the specified [`TimerType`](crate::g_rpc::proto::services::alarms::TimerType).
    async fn delete_alarm_timer(
        &self, ctx: &Context<'_>, device: String, timer_type: String,
    ) -> Result<String, Error> {
        let alarms_db_config = &ctx.data::<AlarmConfigWrapper>()?.alarms_db;
        let token = ctx
            .data_opt::<AuthInfo>()
            .and_then(|info| info.token())
            .unwrap_or_default();

        match alarms_db::timers::delete(
            alarms_db_config,
            ForwardedToken::new(token),
            device.clone(),
            timer_type,
        )
        .await
        {
            Ok(_) => Ok(device),
            Err(e) => handle_error(e, "deleting alarm timer"),
        }
    }

    /// A request to snooze the specified alarms.
    async fn snooze_alarms(
        &self, ctx: &Context<'_>, devices: Vec<String>, updated_by: String,
        wake: DateTime<Utc>,
    ) -> Result<Vec<String>, Error> {
        let alarms_svc_config = &ctx.data::<AlarmConfigWrapper>()?.alarms_svc;
        let token = ctx
            .data_opt::<AuthInfo>()
            .and_then(|info| info.token())
            .unwrap_or_default();

        match alarms_svc::snooze_alarms(
            alarms_svc_config,
            ForwardedToken::new(token),
            devices.clone(),
            updated_by,
            wake,
        )
        .await
        {
            Ok(_) => Ok(devices),
            Err(e) => handle_error(e, "snoozing alarms"),
        }
    }

    /// A request to update an existing alarms timer of the specified
    /// [`TimerType`](crate::g_rpc::proto::services::alarms::TimerType).
    async fn update_alarm_timer(
        &self, ctx: &Context<'_>, device: String,
        end_time: Option<DateTime<Utc>>, timer_type: String,
        updated_by: String,
    ) -> Result<AlarmTimer, Error> {
        let alarms_db_config = &ctx.data::<AlarmConfigWrapper>()?.alarms_db;
        let token = ctx
            .data_opt::<AuthInfo>()
            .and_then(|info| info.token())
            .unwrap_or_default();

        match alarms_db::timers::update(
            alarms_db_config,
            ForwardedToken::new(token),
            device,
            end_time,
            timer_type,
            updated_by,
        )
        .await
        {
            Ok(alarm_timer) => Ok(AlarmTimer::from(alarm_timer)),
            Err(e) => handle_error(e, "updating alarm timer"),
        }
    }
}

/// Describes the various queries (data reads) related to alarms.
#[derive(Default)]
pub struct AlarmsQueries;
#[Object]
impl AlarmsQueries {
    /// Reads all [`AlarmGroupMetadatum`] items in the database.
    async fn alarms_group_metadata(
        &self, ctx: &Context<'_>,
    ) -> Result<Vec<AlarmGroupMetadatum>, Error> {
        let alarms_db_config = &ctx.data::<AlarmConfigWrapper>()?.alarms_db;
        let token = ctx
            .data_opt::<AuthInfo>()
            .and_then(|info| info.token())
            .unwrap_or_default();

        match alarms_db::groups::read_metadata(
            alarms_db_config,
            ForwardedToken::new(token),
        )
        .await
        {
            Ok(response) => {
                let mapped_response = response
                    .metadata
                    .into_iter()
                    .map(AlarmGroupMetadatum::from)
                    .collect();
                Ok(mapped_response)
            }
            Err(e) => handle_error(e, "reading alarm group metadata"),
        }
    }

    /// Reads the [`AlarmGroup`] data for specified groups.
    async fn alarms_groups(
        &self, ctx: &Context<'_>, groups: Vec<String>,
    ) -> Result<Vec<AlarmGroup>, Error> {
        let alarms_db_config = &ctx.data::<AlarmConfigWrapper>()?.alarms_db;
        let token = ctx
            .data_opt::<AuthInfo>()
            .and_then(|info| info.token())
            .unwrap_or_default();

        match alarms_db::groups::read_groups(
            alarms_db_config,
            ForwardedToken::new(token),
            groups,
        )
        .await
        {
            Ok(response) => {
                let mapped_response = response
                    .alarm_groups
                    .into_iter()
                    .map(AlarmGroup::from)
                    .collect();
                Ok(mapped_response)
            }
            Err(e) => handle_error(e, "reading alarm groups"),
        }
    }

    /// Reads all [`UserLayout`]s in the database.
    async fn alarms_user_layouts(
        &self, ctx: &Context<'_>,
    ) -> Result<Vec<UserLayout>, Error> {
        let alarms_db_config = &ctx.data::<AlarmConfigWrapper>()?.alarms_db;
        let token = ctx
            .data_opt::<AuthInfo>()
            .and_then(|info| info.token())
            .unwrap_or_default();

        match alarms_db::layouts::read_layouts(
            alarms_db_config,
            ForwardedToken::new(token),
        )
        .await
        {
            Ok(response) => {
                let mapped_response = response
                    .layouts
                    .into_iter()
                    .map(UserLayout::from)
                    .collect();
                Ok(mapped_response)
            }
            Err(e) => handle_error(e, "reading user layouts"),
        }
    }

    /// Reads a snapshot of the alarms topic.
    async fn alarms_snapshot(
        &self, ctx: &Context<'_>,
    ) -> Result<Vec<Alarm>, Error> {
        let alarms_svc_config = &ctx.data::<AlarmConfigWrapper>()?.alarms_svc;
        let token = ctx
            .data_opt::<AuthInfo>()
            .and_then(|info| info.token())
            .unwrap_or_default();

        alarms_svc::get_snapshot(alarms_svc_config, ForwardedToken::new(token))
            .await
            .map(|statuses| statuses.into_iter().map(Alarm::from).collect())
            .or_else(|e| handle_error(e, "getting alarm snapshot"))
    }

    /// Reads all alarms timers of the specified [`TimerType`](crate::g_rpc::proto::services::alarms::TimerType) for the given user.
    async fn alarms_timers(
        &self, ctx: &Context<'_>, timer_type: String, user: String,
    ) -> Result<Vec<AlarmTimer>, Error> {
        let alarms_db_config = &ctx.data::<AlarmConfigWrapper>()?.alarms_db;
        let token = ctx
            .data_opt::<AuthInfo>()
            .and_then(|info| info.token())
            .unwrap_or_default();

        match alarms_db::timers::read(
            alarms_db_config,
            ForwardedToken::new(token),
            timer_type,
            user,
        )
        .await
        {
            Ok(response) => {
                let timers = response
                    .alarm_timers
                    .into_iter()
                    .map(AlarmTimer::from)
                    .collect();
                Ok(timers)
            }
            Err(e) => handle_error(e, "reading alarm timer"),
        }
    }
}

/// Describes long-lived data streams for alarms.
#[cfg(feature = "kafka")]
#[derive(Default)]
pub struct AlarmsSubscriptions;

#[cfg(feature = "kafka")]
#[Subscription]
impl AlarmsSubscriptions {
    /// Streams back all alarms from the alarms topic.
    async fn alarms(
        &self, ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = Alarm>, Error> {
        let kafka_config = &ctx.data::<AlarmConfigWrapper>()?.alarms_kafka;
        let stream = KafkaSubscriber::new(
            kafka_config.host_addr.clone(),
            kafka_config.topic.clone(),
        )
        .get_stream::<StringMessage>()
        .await;

        Ok(stream.filter_map(|message| {
            Alarm::try_from(message)
                .inspect_err(|e| error!("{e:?}"))
                .ok()
        }))
    }
}

fn handle_error<T>(e: Status, gerund: &str) -> Result<T, Error> {
    let err_id = Uuid::new_v4();
    error!("{err_id} gRPC Error {gerund}: {e:?}");
    Err(match e.code() {
        Code::InvalidArgument => {
            Error::new(format!("{e} (Error ID: {err_id})"))
        }
        _ => Error::new(format!(
            "Error {gerund}. See server logs for details. (Error ID: {err_id})"
        )),
    })
}
