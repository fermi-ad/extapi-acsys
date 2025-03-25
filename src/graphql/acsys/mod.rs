use crate::g_rpc::dpm;

use async_graphql::*;
use chrono::{DateTime, Utc};
use futures::future;
use futures_util::{stream, Stream, StreamExt};
use std::{collections::HashSet, pin::Pin};
use tokio::time::Instant;
use tonic::Status;
use tracing::{debug, error, info, instrument, warn};

const N: usize = 500;

// Pull in global types.

use super::types as global;

// Pull in our local types.

mod plotconfigdb;
pub mod types;

pub fn new_context() -> plotconfigdb::T {
    plotconfigdb::T::new()
}

use crate::g_rpc::dpm::Connection;

// Converts a gRPC proto::Reading structure into a GraphQL
// global::DataReply object.

fn reading_to_reply(
    names: &[String], rdg: &dpm::proto::Reading,
) -> global::DataReply {
    if let Some(ref data) = rdg.data {
        global::DataReply {
            ref_id: rdg.index as i32,
            cycle: 1,
            data: global::DataInfo {
                timestamp: std::time::SystemTime::now().into(),
                result: data.into(),
                di: 0,
                name: names[rdg.index as usize].clone(),
            },
        }
    } else {
        warn!("returned data: {:?}", &rdg.data);
        unreachable!()
    }
}

// Returns a function that translates a gRPC proto::Reading structures
// into a GraphQL DataReply object. This is used with the
// Stream::map() method to translate a stream of gRPC types to GraphQL
// types.

fn mk_xlater(
    names: Vec<String>,
) -> Box<
    dyn (FnMut(Result<dpm::proto::Reading, Status>) -> global::DataReply)
        + Send
        + Sync,
> {
    Box::new(move |e: Result<dpm::proto::Reading, Status>| match e {
        Ok(e) => reading_to_reply(&names, &e),
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
    #[doc = "Retrieve the next data point for the specified devices.

      Depending upon the event in the DRF string, the data may come back \
      immediately or after a delay."]
    #[instrument(skip(self, ctxt))]
    async fn accelerator_data(
        &self, ctxt: &Context<'_>,
        #[graphql(
            desc = "An array of device names. The returned values will be \
		    in the same order as specified in this array."
        )]
        device_list: Vec<String>,
        #[graphql(
            desc = "Returns device values that are equal to or greater than \
		    this timestamp. If this parameter is `null`, then the \
		    current, live value is returned. NOTE: THIS FEATURE HAS \
		    NOT BEEN ADDED YET."
        )]
        _when: Option<DateTime<Utc>>,
    ) -> Vec<global::DataReply> {
        // Strip any event designation and append the once-immediate.

        let drfs: Vec<_> = device_list
            .iter()
            .map(|v| format!("{}@i", strip_event(v)))
            .collect();

        // Build a set of integers representing the indices of the request.
        // As replies arrive, the corresponding index will be removed from
        // the set. When the set is empty, the stream will close.

        let mut remaining: HashSet<usize> = (0..drfs.len()).collect();

        // Allocate storage for the reply.

        let mut results: Vec<Option<global::DataReply>> =
            vec![None; drfs.len()];

        let mut s = dpm::acquire_devices(
            ctxt.data::<Connection>().unwrap(),
            ctxt.data::<global::AuthInfo>()
                .ok()
                .and_then(|auth| {
                    if let Some(account) = auth.unsafe_account() {
                        info!("account: {:?}", &account)
                    } else {
                        warn!("couldn't get account info")
                    }

                    global::AuthInfo::token(auth)
                })
                .as_ref(),
            drfs.clone(),
        )
        .await
        .unwrap()
        .into_inner();

        while let Some(reply) = s.next().await {
            match reply {
                Ok(reply) => {
                    let index = reply.index as usize;

                    results[index] = Some(reading_to_reply(&drfs, &reply));

                    remaining.remove(&index);
                    if remaining.is_empty() {
                        return results.drain(..).map(|v| v.unwrap()).collect();
                    }
                }
                Err(e) => {
                    warn!("one-shot failed : {}", e);
                    break;
                }
            }
        }
        vec![]
    }

    #[doc = "Retrieve plot configuration(s).

      Returns a plot configuration associated with the specified ID. If the \
      ID is `null`, all configurations are returned. Both style of requests \
      return an array result -- it's just that specifying an ID will return \
      an array with 0 or 1 element."]
    #[instrument(skip(self, ctxt))]
    async fn plot_configuration(
        &self, ctxt: &Context<'_>, configuration_id: Option<usize>,
    ) -> Vec<types::PlotConfigurationSnapshot> {
        info!("returning plot configuration(s)");

        ctxt.data_unchecked::<plotconfigdb::T>()
            .find(configuration_id)
            .await
    }

    #[doc = "Obtain the user's last configuration.

      If the application saved the user's last plot configuration, this query \
      will return it. If there is no configuration for the user, `null` is \
      returned. The user's account is retrieved from the authentication token \
      that is included in the request."]
    #[instrument(skip(self, ctxt))]
    async fn users_last_configuration(
        &self, ctxt: &Context<'_>,
    ) -> Option<types::PlotConfigurationSnapshot> {
        if let Ok(auth) = ctxt.data::<global::AuthInfo>() {
            if let Some(account) = auth.unsafe_account() {
                info!("account: {:?}", &account);

                return ctxt
                    .data_unchecked::<plotconfigdb::T>()
                    .find_user(&account)
                    .await;
            }
        }
        None
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
    #[instrument(skip(self, ctxt, value))]
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
            ctxt.data::<global::AuthInfo>().unwrap().token(),
            device.clone(),
            value.into(),
        )
        .await;

        info!("done in {} μs", now.elapsed().as_micros());

        global::StatusReply {
            status: match result {
                Ok(status) => status as i16,

                Err(e) => {
                    error!("{}", &e);

                    -1
                }
            },
        }
    }

    #[instrument(skip(self, ctxt))]
    async fn update_plot_configuration(
        &self, ctxt: &Context<'_>, config: types::PlotConfigurationSnapshot,
    ) -> Option<usize> {
        info!("updating config");
        ctxt.data_unchecked::<plotconfigdb::T>()
            .update(config)
            .await
    }

    #[instrument(skip(self, ctxt))]
    async fn delete_plot_configuration(
        &self, ctxt: &Context<'_>, configuration_id: usize,
    ) -> global::StatusReply {
        info!("deleting config");
        ctxt.data_unchecked::<plotconfigdb::T>()
            .remove(&configuration_id)
            .await;
        global::StatusReply { status: 0 }
    }

    #[doc = "Sets the user's default configuration.

      The content of the configuration are used to set the default \
      configuration for the user. All fields, except the ID and name \
      fields, are used (the latter two will be set to internal values \
      so it can be retrieved with the `usersLastConfiguration` query.) \
      The user's account name is obtained from the authentication token \
      that accompanies the request."]
    #[instrument(skip(self, ctxt))]
    async fn users_configuration(
        &self, ctxt: &Context<'_>, config: types::PlotConfigurationSnapshot,
    ) -> global::StatusReply {
        if let Ok(auth) = ctxt.data::<global::AuthInfo>() {
            if let Some(account) = auth.unsafe_account() {
                info!("account: {:?}", &account);

                ctxt.data_unchecked::<plotconfigdb::T>()
                    .update_user(&account, config)
                    .await;
                return global::StatusReply { status: 0 };
            } else {
                warn!("AuthInfo doesn't have account information");
            }
        } else {
            error!("no AuthInfo state found");
        }
        global::StatusReply { status: -1 }
    }
}

// Returns the portion of the DRF string that precedes any event
// specification.

fn strip_event(drf: &str) -> &str {
    &drf[0..drf.find('@').unwrap_or(drf.len())]
}

const NULL_WAVEFORM: &str = "Z:CACHE@N";
const CONST_WAVEFORM: &str = "API TEST CONST";
const RAMP_WAVEFORM: &str = "API TEST RAMP";
const PARABOLA_WAVEFORM: &str = "API TEST PARABOLA";
const SINE_WAVEFORM: &str = "API TEST SINE";

// Adds a periodic event to a device name to create a DRF specification. The
// delay indicates the number of milliseconds. If the delay is None, then
// the delay is 1 second.

fn add_event(
    delay: Option<usize>, event: Option<u8>,
) -> impl Fn(&str) -> String {
    let event = match (delay, event) {
        (_, None) => {
            format!("p,{}u", delay.filter(|v| *v > 0).unwrap_or(1_000_000))
        }
        (None, Some(e)) => format!("e,{:X},e", e),
        (Some(d), Some(e)) => format!("e,{:X},e,{}", e, (d + 500) / 1_000),
    };

    // If we're using the faked sources, we still need to reserve the slot
    // in the array of devices. So we insert a DRF string that uses the
    // "never" event.

    move |device| match device {
        CONST_WAVEFORM | RAMP_WAVEFORM | PARABOLA_WAVEFORM | SINE_WAVEFORM => {
            NULL_WAVEFORM.into()
        }
        _ => format!("{device}@{}", event),
    }
}

fn stuff_fake_data(
    r: &mut dyn Iterator<Item = usize>, drfs: &[String], ts: f64,
    chans: &mut [types::PlotChannelData],
) {
    for (idx, chan) in chans.iter_mut().enumerate() {
        match drfs[idx].as_str() {
            CONST_WAVEFORM => chan.channel_data = const_data(r, ts, 5.0),
            RAMP_WAVEFORM => chan.channel_data = ramp_data(r, ts),
            PARABOLA_WAVEFORM => chan.channel_data = parabola_data(r, ts),
            SINE_WAVEFORM => chan.channel_data = sine_data(r, ts),
            _ => (),
        }
    }
}

fn to_plot_data(
    len: usize, window_size: &Option<usize>, data: &global::DataInfo,
) -> (i16, Vec<types::PlotDataPoint>) {
    match &data.result {
        global::DataType::Scalar(y) => (
            0,
            vec![types::PlotDataPoint {
                t: None,
                x: data.timestamp.timestamp_micros() as f64 / 1_000_000.0,
                y: y.scalar_value,
            }],
        ),
        global::DataType::ScalarArray(a) => {
            let ts = data.timestamp.timestamp_micros() as f64 / 1_000_000.0;
            let step = window_size
                .filter(|v| *v > 0 && *v <= len)
                .map(|v| len.div_ceil(v))
                .unwrap_or(1);

            (
                0,
                a.scalar_array_value
                    .iter()
                    .enumerate()
                    .step_by(step)
                    .map(|(idx, y)| types::PlotDataPoint {
                        t: Some(ts),
                        x: idx as f64,
                        y: *y,
                    })
                    .collect(),
            )
        }
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
    #[doc = "Retrieve data from accelerator devices.

      Accepts a list of DRF strings and streams the resulting data as it gets \
      generated."]

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
            ctxt.data::<global::AuthInfo>()
                .ok()
                .and_then(global::AuthInfo::token)
                .as_ref(),
            drfs.clone(),
        )
        .await
        {
            Ok(s) => {
                debug!("rpc: {} μs", now.elapsed().as_micros());
                Box::pin(s.into_inner().map(mk_xlater(drfs))) as DataStream
            }
            Err(e) => {
                error!("{}", &e);
                Box::pin(stream::empty()) as DataStream
            }
        }
    }

    #[doc = "Retrieve correlated plot data.

      This query sets up a request which returns a stream of data, presumably \
      used for plotting. Unlike the `acceleratorData` query, this stream \
      returns data for all the devices in one reply. Since the data is \
      correlated, all the devices are collected on the same event."]
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
            desc = "The number of waveforms to return. If omitted, the \
		    service will return waveforms until the client cancels \
		    the subscription."
        )]
        n_acquisitions: Option<usize>,
        #[graphql(
            desc = "If `triggerEvent` is null, this parameter specifies the \
		    delay, in microseconds, between points in a waveform. If \
		    a trigger event is specified, then this specifies the \
		    delay after the event when the signal should be sampled. \
		    If this parameter is null, then there will be no delay \
		    after a trigger event or a 1 Hz sample rate will be used."
        )]
        update_delay: Option<usize>,
        #[graphql(
            desc = "The number of waveforms to return. If omitted, the \
		    service will return waveforms until the client cancels \
		    the subscription."
        )]
        trigger_event: Option<u8>,
        #[graphql(
            desc = "Minimum timestamp. All data before this timestamp will be \
		    filtered from the result set."
        )]
        x_min: Option<f64>,
        #[graphql(
            desc = "Maximum timestamp. All data after this timestamp will be \
		    filtered from the result set."
        )]
        x_max: Option<f64>,
    ) -> PlotStream {
        info!("incoming plot with delay {:?}", update_delay);

        // Add the periodic rate to each of the device names after stripping
        // any event specifier.

        let drfs: Vec<_> = drf_list
            .iter()
            .map(|v| strip_event(v))
            .map(add_event(update_delay, trigger_event))
            .collect();

        let r = x_min.map(|v| v as usize).unwrap_or(0)
            ..(x_max.map(|v| (v as usize) + 1).unwrap_or(N));
        let mut reply = types::PlotReplyData {
            plot_id: "demo".into(),
            tstamp: 0.0,
            data: drfs
                .iter()
                .map(|_| types::PlotChannelData {
                    channel_units: "V".into(),
                    channel_status: 0,
                    channel_data: vec![],
                })
                .collect(),
        };

        stuff_fake_data(&mut r.clone(), &drf_list, 0.0, &mut reply.data);

        match self.accelerator_data(ctxt, drfs, None, None).await {
            Ok(strm) => {
                let s = strm.filter_map(move |e: global::DataReply| {
                    reply.data[e.ref_id as usize].channel_data =
                        to_plot_data(r.len(), &window_size, &e.data).1;
                    reply.data[e.ref_id as usize].channel_status = 0;

                    if reply.data.iter().all(|e| {
                        e.channel_status != 0 || !e.channel_data.is_empty()
                    }) {
                        // XXX: All timestamps should be doubles instead of
                        // converting to and from ASCII ISO values.

                        let ts = e.data.timestamp.timestamp_micros() as f64
                            / 1_000_000.0;
                        let mut temp = types::PlotReplyData {
                            plot_id: "demo".into(),
                            tstamp: ts,
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

                        reply.tstamp = ts;
                        std::mem::swap(&mut temp, &mut reply);
                        stuff_fake_data(
                            &mut r.clone(),
                            &drf_list,
                            ts,
                            &mut reply.data,
                        );
                        future::ready(Some(temp))
                    } else {
                        future::ready(None)
                    }
                });

                if let Some(n) = n_acquisitions.map(|v| v.max(1)) {
                    Box::pin(s.take(n)) as PlotStream
                } else {
                    Box::pin(s) as PlotStream
                }
            }
            Err(e) => {
                error!("{:?}", &e);
                Box::pin(stream::empty()) as PlotStream
            }
        }
    }
}

fn const_data(
    r: &mut dyn Iterator<Item = usize>, ts: f64, y: f64,
) -> Vec<types::PlotDataPoint> {
    r.map(|idx| types::PlotDataPoint {
        t: Some(ts),
        x: idx as f64,
        y,
    })
    .collect()
}

fn ramp_data(
    r: &mut dyn Iterator<Item = usize>, ts: f64,
) -> Vec<types::PlotDataPoint> {
    r.map(|idx| types::PlotDataPoint {
        t: Some(ts),
        x: idx as f64,
        y: idx as f64,
    })
    .collect()
}

fn parabola_data(
    r: &mut dyn Iterator<Item = usize>, ts: f64,
) -> Vec<types::PlotDataPoint> {
    r.map(|idx| {
        let x = idx as f64;

        types::PlotDataPoint {
            t: Some(ts),
            x,
            y: (x * x) / 125.0 - 4.0 * x + 500.0,
        }
    })
    .collect()
}

fn sine_data(
    r: &mut dyn Iterator<Item = usize>, ts: f64,
) -> Vec<types::PlotDataPoint> {
    let k = (std::f64::consts::PI * 2.0) / (N as f64);

    r.map(|idx| types::PlotDataPoint {
        t: Some(ts),
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
        use super::add_event;
        use super::{
            CONST_WAVEFORM, NULL_WAVEFORM, PARABOLA_WAVEFORM, RAMP_WAVEFORM,
            SINE_WAVEFORM,
        };

        assert_eq!(add_event(None, None)(CONST_WAVEFORM), NULL_WAVEFORM);
        assert_eq!(add_event(None, None)(RAMP_WAVEFORM), NULL_WAVEFORM);
        assert_eq!(add_event(None, None)(PARABOLA_WAVEFORM), NULL_WAVEFORM);
        assert_eq!(add_event(None, None)(SINE_WAVEFORM), NULL_WAVEFORM);

        assert_eq!(add_event(Some(1234), None)(CONST_WAVEFORM), NULL_WAVEFORM);
        assert_eq!(add_event(Some(1234), None)(RAMP_WAVEFORM), NULL_WAVEFORM);
        assert_eq!(
            add_event(Some(1234), None)(PARABOLA_WAVEFORM),
            NULL_WAVEFORM
        );
        assert_eq!(add_event(Some(1234), None)(SINE_WAVEFORM), NULL_WAVEFORM);

        assert_eq!(add_event(None, None)("M:OUTTMP"), "M:OUTTMP@p,1000000u");
        assert_eq!(add_event(Some(1234), None)("M:OUTTMP"), "M:OUTTMP@p,1234u");

        assert_eq!(add_event(None, Some(0x2))(CONST_WAVEFORM), NULL_WAVEFORM);
        assert_eq!(add_event(None, Some(0xff))(RAMP_WAVEFORM), NULL_WAVEFORM);
        assert_eq!(
            add_event(None, Some(0x0))(PARABOLA_WAVEFORM),
            NULL_WAVEFORM
        );
        assert_eq!(add_event(None, Some(0x10))(SINE_WAVEFORM), NULL_WAVEFORM);

        assert_eq!(
            add_event(Some(1234), Some(0x02))(CONST_WAVEFORM),
            NULL_WAVEFORM
        );
        assert_eq!(
            add_event(Some(1234), Some(0x8f))(RAMP_WAVEFORM),
            NULL_WAVEFORM
        );
        assert_eq!(
            add_event(Some(1234), Some(0x29))(PARABOLA_WAVEFORM),
            NULL_WAVEFORM
        );
        assert_eq!(
            add_event(Some(1234), Some(0x30))(SINE_WAVEFORM),
            NULL_WAVEFORM
        );

        assert_eq!(add_event(None, Some(0x02))("M:OUTTMP"), "M:OUTTMP@e,2,e");
        assert_eq!(
            add_event(Some(12345), Some(0x8f))("M:OUTTMP"),
            "M:OUTTMP@e,8F,e,12"
        );
        assert_eq!(
            add_event(Some(12499), Some(0x8f))("M:OUTTMP"),
            "M:OUTTMP@e,8F,e,12"
        );
        assert_eq!(
            add_event(Some(12500), Some(0x8f))("M:OUTTMP"),
            "M:OUTTMP@e,8F,e,13"
        );
    }
}
