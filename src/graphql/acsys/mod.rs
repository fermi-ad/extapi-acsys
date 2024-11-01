use crate::g_rpc::dpm;

use async_graphql::*;
use chrono::{DateTime, Utc};
use futures_util::{stream, Stream, StreamExt};
use std::pin::Pin;
use tokio::time::Instant;
use tonic::Status;
use tracing::{error, info, warn};

const N: usize = 500;

// Pull in global types.

use super::types as global;

// Pull in our local types.

pub mod types;

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

// Create a zero-sized struct to attach the GraphQL handlers.

#[derive(Default)]
pub struct ACSysQueries;

// Define the schema's query entry points. Any methods defined in this
// section will appear in the schema.

#[doc = "These queries are used to access accelerator data."]
#[Object]
impl ACSysQueries {
    #[doc = "Retrieve the next data point for the specified devices. Depending upon the event in the DRF string, the data may come back immediately or after a delay.

*NOTE: This query hasn't been implemented yet.*"]
    async fn accelerator_data(
        &self,
        #[graphql(
            desc = "An array of device names. The returned values will be in the same order as specified in this array."
        )]
        _device_list: Vec<String>,
        #[graphql(
            desc = "Returns device values that are equal to or greater than this timestamp. If this parameter is `null`, then the current, live value is returned. NOTE: THIS FEATURE HAS NOT BEEN ADDED YET."
        )]
        _when: Option<DateTime<Utc>>,
    ) -> Vec<global::DataReply> {
        vec![]
    }
}

#[derive(Default)]
pub struct ACSysMutations;

#[Object]
impl ACSysMutations {
    #[doc = "Sends a setting to a device.

Not all devices can be set -- most are read-only. To be able to set a device, your SSO account must be associated with every device you may want to set."]
    async fn set_device(
        &self,
        #[graphql(
            desc = "The device to be set. This parameter should be expressed as a DRF entity. For instance, for ACNET devices, the device name should be appended with `.SETTING` or `.CONTROL`."
        )]
        device: String,
        #[graphql(desc = "The value of the setting.")] value: global::DevValue,
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

type DataStream = Pin<Box<dyn Stream<Item = global::DataReply> + Send>>;
type PlotStream = Pin<Box<dyn Stream<Item = types::PlotReplyData> + Send>>;

#[derive(Default)]
pub struct ACSysSubscriptions;

#[Subscription]
impl ACSysSubscriptions {
    #[doc = ""]
    async fn accelerator_data(
        &self,
        #[graphql(
            desc = "A array of DRF strings. Each entry of the returned stream will have a index to associate the reading with the DRF that started it."
        )]
        drfs: Vec<String>,
        #[graphql(
            desc = "The stream will return device data starting at this timestamp. If the control system cannot find data at the actual timestamp, it will return the oldest data it has that's greater then the timestamp. If this parameter is `null`, it will simply return live data. NOTE: THIS FEATURE HAS NOT BEEN ADDED YET."
        )]
        _start_time: Option<DateTime<Utc>>,
        #[graphql(
            desc = "The stream will close once the device data's timestamp reaches this value. This parameter must be greater than the `startTime` parameter. If this parameter is `null`, the stream will return live data until the client closes it. NOTE: THIS FEATURE HAS NOT BEEN ADDED YET."
        )]
        _end_time: Option<DateTime<Utc>>,
    ) -> DataStream {
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

    #[doc = ""]
    async fn start_plot(
        &self,
        #[graphql(
            desc = "List of DRF strings that indicate the devices and return rates in which the client is interested."
        )]
        drf_list: Vec<String>,
        #[graphql(
            desc = "Indicates how much data the client is able to display. If the plot generates more points than this window, the service will decimate the data set to fit. The data is first filtered by the `xMin` and `xMax` parameters before being decimated. If this parameter is `null`, all data will be returned."
        )]
        window_size: Option<usize>,
        #[graphql(desc = "The delay between points in a waveform.")]
        _update_delay: Option<usize>,
        #[graphql(
            desc = "Minimum timestamp. All data before this timestamp will be filtered from the result set."
        )]
        x_min: Option<usize>,
        #[graphql(
            desc = "Maximum timestamp. All data after this timestamp will be filtered from the result set."
        )]
        x_max: Option<usize>,
    ) -> PlotStream {
        let r = x_min.unwrap_or(0)..x_max.unwrap_or(N);
        let step = window_size
            .filter(|v| *v > 0)
            .map(|v| r.len() / v)
            .unwrap_or(0);
        let r = r.step_by(step);

        let reply = drf_list.iter().fold(
            types::PlotReplyData {
                plot_id: "demo".into(),
                data: vec![],
            },
            |mut acc, device| {
                acc.data.push(match device.as_str() {
                    "const" => types::PlotChannelData {
                        channel_units: "A".into(),
                        channel_status: 0,
                        channel_data: const_data(&mut r.clone(), 5.0),
                    },
                    "sine" => types::PlotChannelData {
                        channel_units: "V".into(),
                        channel_status: 0,
                        channel_data: sine_data(&mut r.clone()),
                    },
                    _ => types::PlotChannelData {
                        channel_units: "".into(),
                        channel_status: -1,
                        channel_data: vec![],
                    },
                });
                acc
            },
        );

        Box::pin(stream::once(async { reply })) as PlotStream
    }
}

fn const_data(r: &mut dyn Iterator<Item = usize>, y: f64) -> Vec<types::PlotDataPoint> {
    r.map(|idx| types::PlotDataPoint { x: idx as f64, y })
        .collect()
}

fn sine_data(r: &mut dyn Iterator<Item = usize>) -> Vec<types::PlotDataPoint> {
    let k = (std::f64::consts::PI * 2.0) / (N as f64);

    r.map(|idx| types::PlotDataPoint {
        x: idx as f64,
        y: f64::sin(k * (idx as f64)),
    })
    .collect()
}
