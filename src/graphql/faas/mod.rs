use async_graphql::*;
use tracing::{info, instrument};

const CLINK_OFFSET: u64 = (24 * 365 * 2 + 6) * 60 * 60;

#[derive(Default)]
pub struct FaasQueries;

// Define the schema's query entry points. Any methods defined in this
// section will appear in the schema.

#[doc = "These queries are used to access our \"Functions as a Service\" \
	 services."]
#[Object]
impl FaasQueries {
    #[doc = "Converts \"clinks\" to a Unix timestamp (seconds since Jan 1, \
	    1970 UTC.)"]
    #[graphql(deprecation = "This is a test API and will be removed.")]
    #[instrument(skip(self))]
    async fn clinks_to_unix(&self, clinks: u64) -> u64 {
        clinks + CLINK_OFFSET
    }

    #[doc = "Converts a Unix timestamp (seconds since Jan 1, 1970 UTC) into \
	     \"clinks\". Since there is a range of Unix time that can't be \
	     represented in \"clinks\", `null` will be returned when the \
	     conversion fails."]
    #[graphql(deprecation = "This is a test API and will be removed.")]
    #[instrument(skip(self))]
    async fn unix_to_clinks(&self, time: u64) -> Option<u64> {
        Some(time)
            .filter(|v| *v >= CLINK_OFFSET)
            .map(|v| v - CLINK_OFFSET)
    }
}
