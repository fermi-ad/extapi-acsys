use async_graphql::*;
use chrono::*;

/// Contains information about a clock event that occurred.
#[derive(SimpleObject)]
pub struct EventInfo {
    pub timestamp: DateTime<Utc>,
    pub event: u16,
}
