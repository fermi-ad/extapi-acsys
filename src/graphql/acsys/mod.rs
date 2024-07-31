use crate::g_rpc::dpm;

use async_graphql::*;
use futures_util::{stream, Stream, StreamExt};
use std::pin::Pin;
use tokio::time::Instant;
use tonic::Status;
use tracing::{error, info, warn};

// Pull in global types.

use super::types as global;

// Pull in our local types.

pub mod types;

// Create a zero-sized struct to attach the GraphQL handlers.

#[derive(Default)]
pub struct ACSysQueries;

// Define the schema's query entry points. Any methods defined in this
// section will appear in the schema.

#[Object]
impl ACSysQueries {
    /// Retrieve the next data point for the specified devices. Depending upon the event in the DRF string, the data may come back immediately or after a delay.
    async fn accelerator_data(
        &self, _drfs: Vec<String>,
    ) -> Vec<global::DataReply> {
        vec![]
    }
}

#[derive(Default)]
pub struct ACSysMutations;

#[Object]
impl ACSysMutations {
    /// Sends a setting to a device.
    ///
    /// Not all devices can be set -- most are read-only. For ACNET devices, the `device` string should use DRF notation to specify one of the two settable properties: `.SETTING` and `.CONTROL`.
    async fn set_device(
        &self, device: String, value: global::DevValue,
    ) -> global::StatusReply {
        let now = Instant::now();
        let result =
            dpm::set_device("DEADBEEF", device.clone(), value.into()).await;

        info!(
            "setDevice({}) => rpc: {} μs",
            &device,
            now.elapsed().as_micros()
        );

        global::StatusReply {
            status: match result {
                Ok(status) => status as i16,

                Err(e) => {
                    error!("set_device: {}", &e);

                    -1
                }
            },
        }
    }
}

fn mk_xlater(
    names: Vec<String>,
) -> Box<
    dyn (FnMut(Result<dpm::proto::Reading, Status>) -> global::DataReply)
        + Send
        + Sync,
> {
    Box::new(move |e: Result<dpm::proto::Reading, Status>| {
        let e = e.unwrap();

        if let Some(data) = e.data {
            global::DataReply {
                ref_id: e.index as i32,
                cycle: 1,
                data: global::DataInfo {
                    timestamp: std::time::SystemTime::now().into(),
                    result: data.into(),
                    di: 0,
                    name: names[e.index as usize].clone(),
                },
            }
        } else {
            warn!("returned data: {:?}", &e.data);
            unreachable!()
        }
    })
}

type DataStream = Pin<Box<dyn Stream<Item = global::DataReply> + Send>>;

#[derive(Default)]
pub struct ACSysSubscriptions;

#[Subscription]
impl ACSysSubscriptions {
    async fn accelerator_data(&self, drfs: Vec<String>) -> DataStream {
        let hdr = format!("monitoring({:?})", &drfs);
        let now = Instant::now();
        let stream = match dpm::acquire_devices("", drfs.clone()).await {
            Ok(s) => {
                Box::pin(s.into_inner().map(mk_xlater(drfs))) as DataStream
            }
            Err(e) => {
                error!("{}", &e);
                Box::pin(stream::empty()) as DataStream
            }
        };

        info!("{} => rpc: {} μs", hdr, now.elapsed().as_micros());
        stream
    }
}
