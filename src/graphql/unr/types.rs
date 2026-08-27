use async_graphql::InputObject;

/// GraphQL-visible representation of a UNR device node.
///
/// Note: `children` is resolved in [`crate::graphql::unr`](crate::graphql::unr).
#[derive(Clone, Debug)]
pub struct Device {
    pub name: String,
    /// Nullable because UNR may not have base info for a device.
    pub address: Option<String>,
    /// Nullable because UNR may not have base info for a device.
    pub r#type: Option<String>,
    /// Nullable because UNR may not have base info for a device.
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
/// `children` is intentionally ommitted to avoid accidental relationship
/// clobbering during updates.
#[derive(Clone, Debug, InputObject)]
pub struct UpdateDeviceInput {
    pub name: String,
    pub address: String,
    pub r#type: String,
    pub protocol: String,
}
