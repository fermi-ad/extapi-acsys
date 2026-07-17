use crate::info;
use async_graphql::*;
use serde::{Deserialize, Serialize};
use tracing::instrument;

#[derive(Default)]
pub struct FaasQueries;

#[derive(Serialize, Deserialize, Debug)]
struct ClinksUnix {
    clinks: u64,
    unix: u64,
}

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
        info!("Processing Clinks: {clinks}");

        let res: Option<reqwest::Response> = reqwest::get(format!(
            "https://ad-services.fnal.gov/faas/clinks/{}",
            clinks
        ))
        .await
        .ok();

        if let Some(resp) = res {
            match resp.json::<ClinksUnix>().await {
                Ok(clunx) => clunx.unix,
                Err(er) => {
                    info!("Error: {er}");
                    0
                }
            }
        } else {
            info!("Response was not received");
            0
        }
    }

    #[doc = "Converts a Unix timestamp (seconds since Jan 1, 1970 UTC) into \
	     \"clinks\". Since there is a range of Unix time that can't be \
	     represented in \"clinks\", `null` will be returned when the \
	     conversion fails."]
    #[graphql(deprecation = "This is a test API and will be removed.")]
    #[instrument(skip(self))]
    async fn unix_to_clinks(&self, time: u64) -> Option<u64> {
        info!("Processing Unix: {time}");

        let res: Option<reqwest::Response> = reqwest::get(format!(
            "https://ad-services.fnal.gov/faas/unix/{}",
            time
        ))
        .await
        .ok();

        if let Some(resp) = res {
            match resp.json::<ClinksUnix>().await {
                Ok(clunx) => Some(clunx.clinks),
                Err(er) => {
                    info!("Error: {er}");
                    Some(0)
                }
            }
        } else {
            info!("Response was not received");
            Some(0)
        }
    }
}
