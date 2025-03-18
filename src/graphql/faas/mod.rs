use async_graphql::*;

const CLINK_OFFSET: u64 = (24 * 365 * 2 + 6) * 60 * 60;

#[derive(Default)]
pub struct FaasQueries;

// Define the schema's query entry points. Any methods defined in this
// section will appear in the schema.

#[doc = "These queries are used to access our \"Functions as a Service\" \
	 services."]
#[Object]
impl FaasQueries {
    async fn clinks_to_unix(&self, clinks: u64) -> u64 {
        clinks + CLINK_OFFSET
    }

    async fn unix_to_clinks(&self, time: u64) -> Option<u64> {
        Some(time)
            .filter(|v| *v >= CLINK_OFFSET)
            .map(|v| v - CLINK_OFFSET)
    }
}
