use async_graphql::InputObject;

/// GraphQL-visible representation of a UNR device node.
///
/// Note: `children` is resolved in [`crate::graphql::unr`](crate::graphql::unr).
#[derive(Clone, Debug)]
pub struct Device {
    pub name: String,
    pub address: Option<String>,
    pub r#type: Option<String>,
    pub protocol: Option<String>,
}

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
