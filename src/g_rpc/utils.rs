//! Utility Module

use std::fmt::Display;

use tonic::Status;
use tracing::error;
use uuid::Uuid;

pub fn handle_rpc_error<Err: Display>(err: Err, service: &str) -> Status {
    let err_token = Uuid::new_v4();
    error!("{err_token} Error returned from call to {service}: {err}");
    Status::internal(format!(
        "See server logs for details; reference token {err_token}"
    ))
}
