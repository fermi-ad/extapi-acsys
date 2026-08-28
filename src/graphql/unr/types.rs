use async_graphql::InputObject;

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
