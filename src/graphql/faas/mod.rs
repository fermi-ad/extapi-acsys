use std::collections::HashMap;

use crate::info;
use async_graphql::*;
use chrono::format;
use reqwest;
use serde::{Deserialize, Serialize};
use serde_json;
use tracing::instrument;

const CLINK_OFFSET: u64 = (24 * 365 * 2 + 6) * 60 * 60;

#[derive(Default)]

pub struct FaasQueries;

#[derive(Serialize, Deserialize)]
struct SystemConfiguration {
    id: String,
    name: String,
}

#[derive(Serialize, Deserialize)]
struct ScanConfiguration {
    id: String,
    name: String,
    system_id: String,
    system_name: String,
    acquisition_parameters: Vec<String>,
    scan_parameters: Vec<String>,
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
        info!("[ClinkToUnix] Processing Clinks: {clinks}");
        let result = reqwest::get(format!(
            "https://ad-services.fnal.gov/faas/clinks/{}",
            clinks
        ))
        .await
        .unwrap()
        .json::<HashMap<String, String>>()
        .await
        .unwrap();

        //let result: Value = serde_json::from_str(rest_call).unwrap();
        result["unix"].parse().unwrap()
    }

    #[doc = "Converts a Unix timestamp (seconds since Jan 1, 1970 UTC) into \
	     \"clinks\". Since there is a range of Unix time that can't be \
	     represented in \"clinks\", `null` will be returned when the \
	     conversion fails."]
    #[graphql(deprecation = "This is a test API and will be removed.")]
    #[instrument(skip(self))]
    async fn unix_to_clinks(&self, unix: u64) -> Option<String> {
        let result =
            reqwest::get("https://ad-services.fnal.gov/faas/fun-hello-py")
                .await
                .ok()?
                .text()
                .await
                .unwrap();
        Some(result)
        // info!("[UnixToClinks] Processing Unix: {unix}");

        // let result = reqwest::get(format!(
        //     "https://ad-services.fnal.gov:6802/faas/unix/{}",
        //     unix
        // ))
        // .await
        // .ok()?
        // .json::<HashMap<String, String>>()
        // .await
        // .unwrap();

        // //let result: Value = serde_json::from_str(rest_call).unwrap();
        // Some(result["clinks"].parse().unwrap())
    }

    // #[doc = "Retrieve all scans"]
    // #[graphql(deprecation = "This is a test API and will be removed.")]
    // #[instrument(skip(self))]
    // async fn retrieve_scans(&self) -> HashMap<String, String> {
    //     let rest_call =
    //         reqwest::get("http://adkube03.fnal.gov:31314/scans/").await?;
    //     let body = res.text().await?;
    //     let scans_list: Vec<ScanConfiguration> = serde_json::from_str(body);
    //     let mut scans: HashMap<String, String> = HashMap::new();
    //     for scan in scans_list.iter() {
    //         scans.insert(scan.id, scan.name);
    //     }
    //     scans
    // }

    // #[doc = "Retrieve a scan based on the input id"]
    // #[graphql(deprecation = "This is a test API and will be removed.")]
    // #[instrument(skip(self))]
    // async fn retrieve_scan(&self, id: String) -> ScanConfiguration {
    //     let res =
    //         reqwest::get("http://adkube03.fnal.gov:31314/scan/" + id).await?;
    //     let body = res.text().await?;
    //     let s: ScanConfiguration = serde_json::from_str(body);
    //     s
    // }

    // #[doc = "Retrieve all systems"]
    // #[graphql(deprecation = "This is a test API and will be removed.")]
    // #[instrument(skip(self))]
    // async fn retrieve_systems(&self) -> HashMap<String, String> {
    //     let res =
    //         reqwest::get("http://adkube03.fnal.gov:31314/systems/").await?;
    //     let body = res.text().await?;
    //     let systems_list: Vec<SystemConfiguration> =
    //         serde_json::from_str(&body);
    //     let mut systems: HashMap<String, String> = HashMap::new();
    //     for syst in systems_list.iter() {
    //         systems.insert(syst.id, syst.name);
    //     }
    //     systems
    // }

    // #[doc = "Retrieve a system based on the system id"]
    // #[graphql(deprecation = "This is a test API and will be removed.")]
    // #[instrument(skip(self))]
    // async fn retrieve_system(&self, system_id: String) -> SystemConfiguration {
    //     let res =
    //         reqwest::get("http://adkube03.fnal.gov:31314/system/" + system_id)
    //             .await?;
    //     let body = res.text().await?;
    //     let s: SystemConfiguration = serde_json::from_str(body)?;
    //     s
    // }
}
