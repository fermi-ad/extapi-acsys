use async_graphql::{InputObject, SimpleObject, Union};

use crate::graphql::types as global;

/// Input for creating a device.
#[derive(Clone, Debug, InputObject)]
pub struct CreateDeviceInput {
    pub name: String,
    pub address: String,
    pub r#type: String,
    pub protocol: String,
    pub children: Option<Vec<String>>,
}

/// Input for updating a device.
///
/// `children` is intentionally omitted to avoid accidental relationship
/// clobbering during updates.
#[derive(Clone, Debug, InputObject)]
pub struct UpdateDeviceInput {
    pub name: String,
    pub address: String,
    pub r#type: String,
    pub protocol: String,
}

/// Indicates that a requested UNR device name was not found.
#[derive(Clone, Debug, SimpleObject, PartialEq, Eq)]
pub struct NotFound {
    pub name: String,
}

/// Result type for bulk UNR device queries.
#[derive(Union)]
pub enum DeviceQueryResult {
    Device(super::Device),
    NotFound(NotFound),
}
