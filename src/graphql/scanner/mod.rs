use crate::g_rpc::wscan;

use async_graphql::{types::ID, Object, Subscription};
use futures_util::{stream, Stream, StreamExt};
use std::pin::Pin;
use tracing::{error, info};

// Pull in our local types.

pub mod types;

// Create a zero-sized struct to attach the GraphQL handlers.

#[derive(Default)]
pub struct ScannerQueries;

// Define the schema's query entry points. Any methods defined in this
// section will appear in the schema.

#[Object]
impl ScannerQueries {
    /// Queries the scanning service for scan configurations. If `id` is `null`, all configurations are returned. If `id` is not `null`, the configuration associated with the ID will be returned. If no ID matches, an empty list is returned.
    async fn retrieve_scans(
        &self, _id: Option<ID>,
    ) -> Vec<types::ScanConfiguration> {
        vec![]
    }

    /// Requests the progress of the motion station associated with the `id`.
    async fn get_progress(&self, id: ID) -> types::ScanCurrentState {
        match wscan::get_progress(id.0.clone()).await {
            Ok(resp) => types::ScanCurrentState::from(resp.into_inner()),
            Err(e) => types::ScanCurrentState {
                detector_id: id,
                state: types::ScanState::Error(types::ScanStateError {
                    err_message: format!("error: {}", e),
                    position: None,
                }),
            },
        }
    }
}

#[derive(Default)]
pub struct ScannerMutations;

#[Object]
impl ScannerMutations {
    /// Requests that a scan be started with the configuration specified by the `id` parameter. If a scan was successfully started, an ID will be returned. If it couldn't be started, `null` is returned.
    async fn request_scan(&self, _id: ID) -> Option<ID> {
        None
    }

    /// Requests that a scan be stopped. The `id` parameter is the value obtained from a previous `request_scan` command or from a scan progress query.
    async fn abort_scan(&self, id: ID) -> bool {
        wscan::abort_scan(id.0.clone()).await.is_ok()
    }
}

type ScanStream = Pin<Box<dyn Stream<Item = types::ScanResult> + Send>>;

#[derive(Default)]
pub struct ScannerSubscriptions;

#[Subscription]
impl ScannerSubscriptions {
    /// Starts a scan at the specified station.
    async fn get_scanner_state(&self, id: ID) -> ScanStream {
        info!("requesting scan at station {}", &id.0);
        match wscan::start_scan(id.0, 0.0, 0.0, 0.0, 0.0, 0).await {
            Ok(s) => Box::pin(s.into_inner().map(Result::unwrap).map(
                |wscan::proto::ScanResult { progress, voltage }| {
                    types::ScanResult {
                        progress: types::ScanCurrentState::from(progress.unwrap()),
                        voltage,
                    }
                },
            )) as ScanStream,
            Err(e) => {
                error!("{}", &e);
                Box::pin(stream::empty()) as ScanStream
            }
        }
    }
}
