use crate::g_rpc::wscan;

use async_graphql::*;
use futures_util::{stream, Stream, StreamExt};
use std::{collections::HashMap, pin::Pin};
use tracing::{error, info};

// Pull in our local types.

pub mod types;

// Create a zero-sized struct to attach the GraphQL handlers.

#[derive(Default)]
pub struct Queries;

// Define the schema's query entry points. Any methods defined in this
// section will appear in the schema.

#[Object]
impl Queries {
    async fn retrieve_scans(&self) -> types::KnownStations {
        match wscan::retrieve_scans().await {
            Ok(map) => types::KnownStations { map },
            Err(e) => {
		error!("error retrieving stations: {}", e);
		types::KnownStations {
                    map: HashMap::new(),
		}
	    },
        }
    }

    /// Requests the progress of the motion station associated with the `id`.
    async fn get_progress(&self, id: String) -> types::ScanProgress {
        match wscan::get_progress(id.clone()).await {
            Ok(resp) => {
                let wscan::proto::ScanProgress {
                    message,
                    detector_id,
                    start_time,
                    current_position,
                    progress_percentage,
                } = resp.into_inner();

                types::ScanProgress {
                    message,
                    detector_id,
                    start_time: Some(start_time),
                    current_position: Some(current_position),
                    progress_percentage: Some(progress_percentage),
                }
            }
            Err(e) => types::ScanProgress {
                message: format!("error: {}", e),
                detector_id: id,
                start_time: None,
                current_position: None,
                progress_percentage: None,
            },
        }
    }

    /// Requests that any motion in the specified station be stopped.
    async fn abort_scan(&self, id: String) -> types::ScanProgress {
        match wscan::abort_scan(id.clone()).await {
            Ok(resp) => {
                let wscan::proto::ScanProgress {
                    message,
                    detector_id,
                    start_time,
                    current_position,
                    progress_percentage,
                } = resp.into_inner();

                types::ScanProgress {
                    message,
                    detector_id,
                    start_time: Some(start_time),
                    current_position: Some(current_position),
                    progress_percentage: Some(progress_percentage),
                }
            }
            Err(e) => types::ScanProgress {
                message: format!("error: {}", e),
                detector_id: id,
                start_time: None,
                current_position: None,
                progress_percentage: None,
            },
        }
    }
}

type ScanStream = Pin<Box<dyn Stream<Item = types::ScanResult> + Send>>;

#[derive(Default)]
pub struct Subscriptions;

#[Subscription]
impl Subscriptions {
    /// Starts a scan at the specified station.
    async fn start_scan(&self, id: String) -> ScanStream {
        info!("requesting scan at station {}", &id);
        match wscan::start_scan(id, 0.0, 0.0, 0.0, 0.0, 0).await {
            Ok(s) => Box::pin(s.into_inner().map(Result::unwrap).map(
                |wscan::proto::ScanResult { progress, voltage }| {
                    let wscan::proto::ScanProgress {
                        message,
                        detector_id,
                        start_time,
                        current_position,
                        progress_percentage,
                    } = progress.unwrap();

                    types::ScanResult {
                        progress: types::ScanProgress {
                            message,
                            detector_id,
                            start_time: Some(start_time),
                            current_position: Some(current_position),
                            progress_percentage: Some(progress_percentage),
                        },
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
