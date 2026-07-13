use crate::info;
use std::collections::HashMap;
use async_graphql::*;
use reqwest::RequestBuilder;
use serde::{Deserialize, Serialize};
use tracing::instrument;

// Pull in global types.

use super::types as global;


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
                Ok(clunx) => {info!("[ClinksToUnix] show unix value {0}", clunx.unix) ; clunx.unix},

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
    async fn unix_to_clinks(&self, unix: u64) -> Option<u64> {
        info!("[UnixToClinks] Processing Unix: {unix}");

        let res: Option<reqwest::Response> = reqwest::get(format!(
            "https://ad-services.fnal.gov/faas/unix/{}",
            unix
        ))
        .await
        .ok();

        if let Some(resp) = res {
            match resp.json::<ClinksUnix>().await {
                Ok(clunx) => {info!("[UnixToClinks] show clinks value {0}", clunx.clinks); Some(clunx.clinks)},
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

    #[doc = "Retrieve all systems"]
    #[instrument(skip(self))]
    async fn retrieve_systems(&self) -> HashMap<String, String> {
        use futures::TryFutureExt;
        info!("Made it to Retrieve systems");
        
        let m = reqwest::get("https://ad-services.fnal.gov/faas/systems")
            .and_then(|r| r.json::<HashMap<String, String>>())
            .await
            .unwrap_or_default();
        info!("[AcsysProxy] Retrieve all systems - Lets see {:?}", m);
        m
    }

    #[doc = "Retrieve all scans"]
    #[instrument(skip(self))]
    async fn retrieve_scans(&self) -> HashMap<String, String> {
        use futures::TryFutureExt;
        info!("Made it to retrieve all scans");
        let m = reqwest::get("https://ad-services.fnal.gov/faas/scans")
            .and_then(|r| r.json::<HashMap<String, String>>())
            .await
            .unwrap_or_default();
        info!("[AcsysProxy] Retrieve all scans - Lets see {:?}", m);
        m
    }

    #[doc = "Retrieve all scans by System Id"]
    #[instrument(skip(self))]
    async fn retrieve_scans_by_system_id(
        &self, system_id: String,
    ) -> HashMap<String, String> {
        use futures::TryFutureExt;
        info!("Retrieve all scans by system id");
        let m = reqwest::get(format!(
            "https://ad-services.fnal.gov/faas/scans/{}",
            system_id
        ))
        .and_then(|r| r.json::<HashMap<String, String>>())
        .await
        .unwrap_or_default();
        info!("[AcsysProxy] Retrieve all scans - Lets see {:?}", m);
        m
    }

    #[doc = "Retrieve scans by Scan Id"]
    #[instrument(skip(self))]
    async fn retrieve_scan_by_id(&self, scan_id: String) -> HashMap<String, String> {
        use futures::TryFutureExt;
        info!("[AcsysProxy] Retrieve scans by scan id with scan id {}", scan_id);
        let m = reqwest::get(format!(
            "https://ad-services.fnal.gov/faas/scan/{}",
            scan_id
        ))
        .and_then(|r| r.json::<HashMap<String, String>>())
        .await
        .unwrap_or_default();
        info!("[AcsysProxy] Retrieve scan - Lets see {:?}", m);
        m
    }

    #[doc = "Retrieve system by system id"]
    #[instrument(skip(self))]
    async fn retrieve_system(&self, system_id: String) -> HashMap<String, String> {
        use futures::TryFutureExt;

        let m = reqwest::get(format!(
            "https://ad-services.fnal.gov/faas/system/{}",
            system_id
        ))
        .and_then(|r| r.json::<HashMap<String, String>>())
        .await
        .unwrap_or_default();

        m
    }

    async fn retrieve_scanner_config_by_system_and_scanner_type(&self, system_id: String, scanner_type: String) {

    }

    async fn start_scan(&self, _ctxt: &Context<'_>, scan_id: String) -> Option<String> {
        info!("[AcsysProxy] starting scan");
        //let client = reqwest::Client::new();
        //let request_builder: RequestBuilder = 
           // client
           // .get("https://ad-services.fnal.gov/faas/dpm-grpc-test-rw-url-trigger"
           // ).send();
            //.bearer_auth(
            //   _ctxt.data::<global::AuthInfo>().unwrap().token().unwrap(),
            //);
            //
        let res = reqwest::get("https://ad-services.fnal.gov/faas/zreadset").await.ok();
        //info!("[AcsysProxy] show res {}", res);
        Some(String::from("Scan Started -- adj"))
    }
}
