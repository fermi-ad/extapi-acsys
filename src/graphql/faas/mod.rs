use crate::info;
use async_graphql::*;
use reqwest;
use serde::{Deserialize, Serialize};
use tracing::instrument;

#[derive(Default)]
pub struct FaasQueries;

#[derive(Serialize, Deserialize, Debug)]
struct ClinksUnix {
    clinks: String,
    unix: String,
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
    async fn clinks_to_unix(&self, clinks: u64) -> Option<String> {
        info!("[ClinkToUnix] Processing Clinks: {clinks}");
        // let result: String = reqwest::get(format!(
        //     "https://ad-services.fnal.gov/faas/clinks/{}",
        //     clinks
        // ))
        // .await
        // .ok()?
        // .text()
        // .await
        // .unwrap();

        // Some(result)

        let res: std::result::Result<ClinksUnix, reqwest::Error> =
            reqwest::get(format!(
                "https://ad-services.fnal.gov/faas/clinks/{}",
                clinks
            ))
            .await
            .ok()?
            .json::<ClinksUnix>()
            .await;

        let unwrapped = match res {
            Ok(gh) => gh.unix,
            Err(er) => String::from("error found"),
        };

        info!("[ClinksToUnix] Processing ClinksUnix object unix {unwrapped}");

        Some(unwrapped)
    }

    #[doc = "Converts a Unix timestamp (seconds since Jan 1, 1970 UTC) into \
	     \"clinks\". Since there is a range of Unix time that can't be \
	     represented in \"clinks\", `null` will be returned when the \
	     conversion fails."]
    #[graphql(deprecation = "This is a test API and will be removed.")]
    #[instrument(skip(self))]
    async fn unix_to_clinks(&self, unix: u64) -> Option<String> {
        info!("[UnixToClinks] Processing Unix: {unix}");
	let result =
            reqwest::get(format!("https://ad-services.fnal.gov/faas/unix/{}", unix))
                .await
                .ok()?
                .text()
                .await
                .unwrap();
        Some(result)
    }
}
