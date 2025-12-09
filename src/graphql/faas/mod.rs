use crate::info;
use async_graphql::*;
use reqwest;
use std::collections::HashMap;
use tracing::instrument;

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
    async fn clinks_to_unix(&self, clinks: u64) -> Option<String> {
        info!("[ClinkToUnix] Processing Clinks: {clinks}");
        let result: String = reqwest::get(format!(
            "https://ad-services.fnal.gov/faas/clinks/{}",
            clinks
        ))
        .await
        .ok()?
        .text()
        .await
        .unwrap();

        Some(result)

        // .await
        // .ok()?
        // .json::<HashMap<String, String>>()
        // .await
        // .unwrap();

        //result["unix"].parse().unwrap()
    }

    #[doc = "Converts a Unix timestamp (seconds since Jan 1, 1970 UTC) into \
	     \"clinks\". Since there is a range of Unix time that can't be \
	     represented in \"clinks\", `null` will be returned when the \
	     conversion fails."]
    #[graphql(deprecation = "This is a test API and will be removed.")]
    #[instrument(skip(self))]
    async fn unix_to_clinks(&self, time: u64) -> Option<String> {
        let result =
            reqwest::get("https://ad-services.fnal.gov/faas/fun-hello-py")
                .await
                .ok()?
                .text()
                .await
                .unwrap();
        Some(result)
    }
}
