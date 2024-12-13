use crate::g_rpc::dpm;

use async_graphql::*;
use chrono::{DateTime, Utc};
use futures::future;
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

use crate::g_rpc::dpm::Connection;

fn mk_xlater(
    names: Vec<String>,
) -> Box<
    dyn (FnMut(Result<dpm::proto::Reading, Status>) -> global::DataReply)
        + Send
        + Sync,
> {
    Box::new(move |e: Result<dpm::proto::Reading, Status>| match e {
        Ok(e) => {
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
        }
        Err(e) => {
            warn!("channel error: {}", &e);
            global::DataReply {
                ref_id: 0,
                cycle: 1,
                data: global::DataInfo {
                    timestamp: std::time::SystemTime::now().into(),
                    result: global::DataType::StatusReply(
                        global::StatusReply { status: -1 },
                    ),
                    di: 0,
                    name: "".into(),
                },
            }
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
    #[doc = "Retrieve the next data point for the specified devices. \
	     Depending upon the event in the DRF string, the data may \
	     come back immediately or after a delay.

*NOTE: This query hasn't been implemented yet.*"]
    async fn accelerator_data(
        &self,
        #[graphql(
            desc = "An array of device names. The returned values will be \
		    in the same order as specified in this array."
        )]
        _device_list: Vec<String>,
        #[graphql(
            desc = "Returns device values that are equal to or greater than \
		    this timestamp. If this parameter is `null`, then the \
		    current, live value is returned. NOTE: THIS FEATURE HAS \
		    NOT BEEN ADDED YET."
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

Not all devices can be set -- most are read-only. To be able to set a \
device, your SSO account must be associated with every device you may \
want to set."]
    async fn set_device(
        &self, ctxt: &Context<'_>,
        #[graphql(
            desc = "The device to be set. This parameter should be expressed \
		    as a DRF entity. For instance, for ACNET devices, the \
		    device name should be appended with `.SETTING` or \
		    `.CONTROL`."
        )]
        device: String,
        #[graphql(desc = "The value of the setting.")] value: global::DevValue,
    ) -> global::StatusReply {
        let now = Instant::now();
        let result = dpm::set_device(
            ctxt.data::<Connection>().unwrap(),
            "DEADBEEF",
            device.clone(),
            value.into(),
        )
        .await;

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

// Returns the portion of the DRF string that precedes any event
// specification.

fn strip_event(drf: &str) -> &str {
    &drf[0..drf.find('@').unwrap_or_else(|| drf.len())]
}

const NULL_WAVEFORM: &str = "Z:CACHE@N";
const CONST_WAVEFORM: &str = "API TEST CONST";
const RAMP_WAVEFORM: &str = "API TEST RAMP";
const PARABOLA_WAVEFORM: &str = "API TEST PARABOLA";
const SINE_WAVEFORM: &str = "API TEST SINE";

// Adds a periodic event to a device name to create a DRF specification. The
// delay indicates the number of milliseconds. If the delay is None, then
// the delay is 1 second.

fn add_periodic(delay: Option<usize>) -> impl Fn(&str) -> String {
    move |device| match device {
        CONST_WAVEFORM | RAMP_WAVEFORM | PARABOLA_WAVEFORM | SINE_WAVEFORM => {
            NULL_WAVEFORM.into()
        }
        _ => format!("{device}@p,{}", delay.unwrap_or(1000)),
    }
}

fn stuff_fake_data(
    r: &mut dyn Iterator<Item = usize>, drfs: &[String],
    chans: &mut [types::PlotChannelData],
) {
    for (idx, chan) in chans.iter_mut().enumerate() {
        match drfs[idx].as_str() {
            CONST_WAVEFORM => chan.channel_data = const_data(r, 5.0),
            RAMP_WAVEFORM => chan.channel_data = ramp_data(r),
            PARABOLA_WAVEFORM => chan.channel_data = parabola_data(r),
            SINE_WAVEFORM => chan.channel_data = sine_data(r),
            _ => (),
        }
    }
}

fn to_plot_data(
    len: usize, window_size: &Option<usize>, data: &global::DataInfo,
) -> (i16, Vec<types::PlotDataPoint>) {
    let step = window_size
        .filter(|v| *v > 0 && *v <= len)
        .map(|v| (len + v - 1) / v)
        .unwrap_or(1);

    match &data.result {
        global::DataType::Scalar(y) => (
            0,
            vec![types::PlotDataPoint {
                x: 0.0,
                y: y.scalar_value,
            }],
        ),
        global::DataType::ScalarArray(a) => (
            0,
            a.scalar_array_value
                .iter()
                .enumerate()
                .step_by(step)
                .map(|(idx, y)| types::PlotDataPoint {
                    x: idx as f64,
                    y: *y,
                })
                .collect(),
        ),
        global::DataType::StatusReply(v) => (v.status, vec![]),
        _ => (-1, vec![]),
    }
}

type DataStream = Pin<Box<dyn Stream<Item = global::DataReply> + Send>>;
type PlotStream = Pin<Box<dyn Stream<Item = types::PlotReplyData> + Send>>;

#[derive(Default)]
pub struct ACSysSubscriptions;

#[Subscription]
impl<'ctx> ACSysSubscriptions {
    #[doc = ""]
    async fn accelerator_data(
        &self, ctxt: &Context<'ctx>,
        #[graphql(
            desc = "A array of DRF strings. Each entry of the returned stream \
		    will have a index to associate the reading with the DRF \
		    that started it."
        )]
        drfs: Vec<String>,
        #[graphql(
            desc = "The stream will return device data starting at this \
		    timestamp. If the control system cannot find data at \
		    the actual timestamp, it will return the oldest data \
		    it has that's greater then the timestamp. If this \
		    parameter is `null`, it will simply return live data. \
		    NOTE: THIS FEATURE HAS NOT BEEN ADDED YET."
        )]
        _start_time: Option<DateTime<Utc>>,
        #[graphql(
            desc = "The stream will close once the device data's timestamp \
		    reaches this value. This parameter must be greater than \
		    the `startTime` parameter. If this parameter is `null`, \
		    the stream will return live data until the client closes \
		    it. NOTE: THIS FEATURE HAS NOT BEEN ADDED YET."
        )]
        _end_time: Option<DateTime<Utc>>,
    ) -> DataStream {
        let now = Instant::now();

        info!("monitoring {:?}", &drfs);

        match dpm::acquire_devices(
            ctxt.data::<Connection>().unwrap(),
            "",
            drfs.clone(),
        )
        .await
        {
            Ok(s) => {
                info!("rpc: {} μs", now.elapsed().as_micros());
                Box::pin(s.into_inner().map(mk_xlater(drfs))) as DataStream
            }
            Err(e) => {
                error!("{}", &e);
                Box::pin(stream::empty()) as DataStream
            }
        }
    }

    #[doc = ""]
    async fn start_plot(
        &self, ctxt: &Context<'ctx>,
        #[graphql(
            desc = "List of DRF strings that indicate the devices and return \
		    rates in which the client is interested."
        )]
        drf_list: Vec<String>,
        #[graphql(
            desc = "Indicates how much data the client is able to display. \
		    If the plot generates more points than this window, the \
		    service will decimate the data set to fit. The data is \
		    first filtered by the `xMin` and `xMax` parameters before \
		    being decimated. If this parameter is `null`, all data \
		    will be returned."
        )]
        window_size: Option<usize>,
        #[graphql(
            desc = "The delay, in milliseconds, between points in a waveform."
        )]
        update_delay: Option<usize>,
        #[graphql(
            desc = "Minimum timestamp. All data before this timestamp will be \
		    filtered from the result set."
        )]
        x_min: Option<usize>,
        #[graphql(
            desc = "Maximum timestamp. All data after this timestamp will be \
		    filtered from the result set."
        )]
        x_max: Option<usize>,
    ) -> PlotStream {
        info!("incoming plot with delay {:?}", &update_delay);

        // Add the periodic rate to each of the device names after stripping
        // any event specifier.

        let drfs: Vec<_> = drf_list
            .iter()
            .map(|v| strip_event(&v))
            .map(add_periodic(update_delay))
            .collect();

        let r = x_min.unwrap_or(0)..(x_max.map(|v| v + 1).unwrap_or(N));
        let mut reply = types::PlotReplyData {
            plot_id: "demo".into(),
            data: drfs
                .iter()
                .map(|_| types::PlotChannelData {
                    channel_units: "V".into(),
                    channel_status: 0,
                    channel_data: vec![],
                })
                .collect(),
        };

        stuff_fake_data(&mut r.clone(), &drf_list, &mut reply.data);

        match self.accelerator_data(ctxt, drfs, None, None).await {
            Ok(strm) => {
                let s = strm.filter_map(move |e: global::DataReply| {
                    reply.data[e.ref_id as usize].channel_data =
                        to_plot_data(r.len(), &window_size, &e.data).1;
                    reply.data[e.ref_id as usize].channel_status = 0;

                    if reply.data.iter().all(|e| {
                        e.channel_status != 0 || !e.channel_data.is_empty()
                    }) {
                        let mut temp = types::PlotReplyData {
                            plot_id: "demo".into(),
                            data: reply
                                .data
                                .iter()
                                .map(|e| types::PlotChannelData {
                                    channel_units: e.channel_units.clone(),
                                    channel_status: e.channel_status,
                                    channel_data: vec![],
                                })
                                .collect(),
                        };

                        std::mem::swap(&mut temp, &mut reply);
                        stuff_fake_data(
                            &mut r.clone(),
                            &drf_list,
                            &mut reply.data,
                        );
                        future::ready(Some(temp))
                    } else {
                        future::ready(None)
                    }
                });

                Box::pin(s) as PlotStream
            }
            Err(e) => {
                error!("{:?}", &e);
                Box::pin(stream::empty()) as PlotStream
            }
        }
    }
}

fn const_data(
    r: &mut dyn Iterator<Item = usize>, y: f64,
) -> Vec<types::PlotDataPoint> {
    r.map(|idx| types::PlotDataPoint { x: idx as f64, y })
        .collect()
}

fn ramp_data(r: &mut dyn Iterator<Item = usize>) -> Vec<types::PlotDataPoint> {
    r.map(|idx| types::PlotDataPoint {
        x: idx as f64,
        y: idx as f64,
    })
    .collect()
}

fn parabola_data(
    r: &mut dyn Iterator<Item = usize>,
) -> Vec<types::PlotDataPoint> {
    r.map(|idx| {
        let x = idx as f64;

        types::PlotDataPoint {
            x,
            y: (x * x) / 125.0 - 4.0 * x + 500.0,
        }
    })
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

#[cfg(test)]
mod test {
    #[test]
    fn test_removing_event() {
        use super::strip_event;

        assert_eq!(strip_event("abc"), "abc");
        assert_eq!(strip_event("abc@e,23"), "abc");

        assert_eq!(strip_event(""), "");
        assert_eq!(strip_event("@"), "");
    }

    #[test]
    fn test_add_periodic() {
        use super::add_periodic;
        use super::{
            CONST_WAVEFORM, NULL_WAVEFORM, PARABOLA_WAVEFORM, RAMP_WAVEFORM,
            SINE_WAVEFORM,
        };

        assert_eq!(add_periodic(None)(CONST_WAVEFORM), NULL_WAVEFORM);
        assert_eq!(add_periodic(None)(RAMP_WAVEFORM), NULL_WAVEFORM);
        assert_eq!(add_periodic(None)(PARABOLA_WAVEFORM), NULL_WAVEFORM);
        assert_eq!(add_periodic(None)(SINE_WAVEFORM), NULL_WAVEFORM);

        assert_eq!(add_periodic(Some(1234))(CONST_WAVEFORM), NULL_WAVEFORM);
        assert_eq!(add_periodic(Some(1234))(RAMP_WAVEFORM), NULL_WAVEFORM);
        assert_eq!(add_periodic(Some(1234))(PARABOLA_WAVEFORM), NULL_WAVEFORM);
        assert_eq!(add_periodic(Some(1234))(SINE_WAVEFORM), NULL_WAVEFORM);

        assert_eq!(add_periodic(None)("M:OUTTMP"), "M:OUTTMP@p,1000");
        assert_eq!(add_periodic(Some(1234))("M:OUTTMP"), "M:OUTTMP@p,1234");
    }
}
