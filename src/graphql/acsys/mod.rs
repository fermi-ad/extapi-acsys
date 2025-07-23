use crate::g_rpc::{
    dpm,
    proto::services::daq::{self, reading_reply},
};

use async_graphql::*;
use chrono::{DateTime, Utc};
use futures::future;
use futures_util::{Stream, StreamExt};
use std::{collections::HashSet, pin::Pin, sync::Arc};
use tokio::time::Instant;
use tonic::Status;
use tracing::{error, info, instrument, warn};

// When a waveform / array device is read, clients may only want a
// subset of the data. Plotting apps, for instance, only have so many
// horizontal pixels. If the client doesn't specify a max number of
// point to return, then this value is used. The data will be
// decimated so that, at most, this many points are returned.

const DEF_MAX_WAVEFORM: usize = 500;

// Pull in global types.

use super::types as global;

// Pull in our local types.

mod datastream;
mod plotconfigdb;
pub mod types;

pub fn new_context() -> plotconfigdb::T {
    plotconfigdb::T::new()
}

use crate::g_rpc::dpm::Connection;

// Useful function to return the current time as a floating point
// number.

fn now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as f64
        / 1_000_000.0
}

// Converts a gRPC proto::ReadingReply structure into a GraphQL
// global::DataReply object.

fn reading_to_reply(rdg: &daq::ReadingReply) -> global::DataReply {
    match &rdg.value {
        Some(reading_reply::Value::Readings(rdgs)) => global::DataReply {
            ref_id: rdg.index as i32,
            data: rdgs
                .reading
                .iter()
                .map(|v| global::DataInfo {
                    timestamp: v
                        .timestamp
                        .map(|v| {
                            v.seconds as f64 + v.nanos as f64 / 1_000_000_000.0
                        })
                        .unwrap(),
                    result: v
                        .data
                        .as_ref()
                        .map(|v| v.try_into())
                        .unwrap()
                        .unwrap(),
                })
                .collect(),
        },
        Some(reading_reply::Value::Status(status)) => global::DataReply {
            ref_id: rdg.index as i32,
            data: vec![global::DataInfo {
                timestamp: now(),
                result: global::DataType::StatusReply(global::StatusReply {
                    status: (status.facility_code + status.status_code * 256)
                        as i16,
                }),
            }],
        },
        None => unreachable!(),
    }
}

fn xlat_reply(e: Result<daq::ReadingReply, Status>) -> global::DataReply {
    match e {
        Ok(e) => reading_to_reply(&e),
        Err(e) => {
            warn!("channel error: {}", &e);
            global::DataReply {
                ref_id: -1,
                data: vec![global::DataInfo {
                    timestamp: now(),
                    result: global::DataType::StatusReply(
                        global::StatusReply { status: -1 },
                    ),
                }],
            }
        }
    }
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
            desc = "Returns device values at or before this timestamp. If \
		    this parameter is `null`, then the current, live value \
		    is returned. NOTE: THIS FEATURE HAS NOT BEEN ADDED YET."
        )]
        _when: Option<DateTime<Utc>>,
    ) -> Result<Vec<global::DataReply>> {
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

        let mut results: Vec<global::DataReply> =
            vec![global::DataReply::default(); drfs.len()];

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

                    results[index] = reading_to_reply(&reply);

                    remaining.remove(&index);
                    if remaining.is_empty() {
                        return Ok(results);
                    }
                }
                Err(e) => return Err(Error::new(format!("{}", e).as_str())),
            }
        }
        Err(Error::new("DPM didn't return all data"))
    }

    #[doc = "Retrieve plot configuration(s).

Returns a plot configuration associated with the specified ID. If the \
ID is `null`, all configurations are returned. Both style of requests \
return an array result -- it's just that specifying an ID will return \
an array with 0 or 1 element."]
    #[instrument(skip(self, ctxt))]
    async fn plot_configuration(
        &self, ctxt: &Context<'_>, configuration_id: Option<usize>,
    ) -> Vec<Arc<types::PlotConfigurationSnapshot>> {
        info!("returning plot configuration(s)");

        ctxt.data_unchecked::<plotconfigdb::T>()
            .find(configuration_id)
            .await
    }

    #[doc = "Obtain the user's last configuration.

If the application saved the user's last plot configuration, this query \
will return it. If there is no configuration for the user, `null` is \
returned. The user's account is retrieved from the authentication token \
that is included in the request.

TEMPORARY: The `user` parameter can be used to retrieve a user's last \
configuration. The convention is to prepend an underscore to the account \
name. Once we use the new authentication method, we'll be able to look-up \
the username and this parameter will be removed."]
    #[instrument(skip(self, ctxt))]
    async fn users_last_configuration(
        &self, ctxt: &Context<'_>, user: Option<String>,
    ) -> Option<Arc<types::PlotConfigurationSnapshot>> {
        info!("new request");
        if let Ok(auth) = ctxt.data::<global::AuthInfo>() {
            // TEMPORARY: If a user account is specified, use it.

            if let Some(account) = user.or_else(|| auth.unsafe_account()) {
                info!("using account: {:?}", &account);

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
    ) -> Result<global::StatusReply> {
        let now = Instant::now();
        let result = dpm::set_device(
            ctxt.data::<Connection>().unwrap(),
            ctxt.data::<global::AuthInfo>().unwrap().token(),
            device.clone(),
            value.into(),
        )
        .await;

        info!("done in {} μs", now.elapsed().as_micros());

        match result {
            Ok(status) => Ok(global::StatusReply {
                status: status[0] as i16,
            }),
            Err(e) => Err(Error::new(format!("{}", e).as_str())),
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
fields, are used. The user's account name is obtained from the \
authentication token that accompanies the request.

TEMPORARY: The `user` parameter can be used to specify the user \
account with which to associate the configuration. The convention \
is to prepend an underscore to the account name. Once we use the \
new authentication method, we'll be able to look-up the username \
and this parameter will be removed."]
    #[instrument(skip(self, ctxt, config))]
    async fn users_configuration(
        &self, ctxt: &Context<'_>, user: Option<String>,
        config: types::PlotConfigurationSnapshot,
    ) -> Result<global::StatusReply> {
        info!("new request");
        if let Ok(auth) = ctxt.data::<global::AuthInfo>() {
            // TEMPORARY: If a user account is specified, use it.

            if let Some(account) = user.or_else(|| auth.unsafe_account()) {
                info!("using account: {:?}", &account);

                ctxt.data_unchecked::<plotconfigdb::T>()
                    .update_user(&account, config)
                    .await;
                Ok(global::StatusReply { status: 0 })
            } else {
                Err(Error::new("unable to verify user credentials"))
            }
        } else {
            Err(Error::new("no user credentials provided"))
        }
    }
}

// Returns the portion of the DRF string that precedes any event
// specification.

fn strip_event(drf: &str) -> &str {
    &drf[0..drf.find('@').unwrap_or(drf.len())]
}

// Returns the portion of the DRF string that precedes any source
// specification.

fn strip_source(drf: &str) -> &str {
    &drf[0..drf.find('<').unwrap_or(drf.len())]
}

const NULL_WAVEFORM: &str = "Z:CACHE@N";
const CONST_WAVEFORM: &str = "API TEST CONST";
const RAMP_WAVEFORM: &str = "API TEST RAMP";
const PARABOLA_WAVEFORM: &str = "API TEST PARABOLA";
const SINE_WAVEFORM: &str = "API TEST SINE";

// Adds an event specification to a device name to create a DRF specification.
// If the `event` parameter is `None`, the `delay` parameter represents the
// periodic sample time, in microseconds. If an event is specified, the delay
// represents the millisecond delay after the event to do the sample.

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

// This function is a (long-term) temporary artifact. It generates one of
// several interesting waveforms that the plot app can use until we get
// the device API finalized.

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
    len: usize, window_size: &Option<usize>, data: &[global::DataInfo],
) -> (i16, Vec<types::PlotDataPoint>) {
    let mut result = vec![];

    for entry in data {
        let ts = entry.timestamp;

        match &entry.result {
            global::DataType::Scalar(y) => result.push(types::PlotDataPoint {
                t: ts,
                x: ts,
                y: y.scalar_value,
            }),
            global::DataType::ScalarArray(a) => {
                let step = window_size
                    .filter(|v| *v > 0 && *v <= len)
                    .map(|v| len.div_ceil(v))
                    .unwrap_or(1);

                result.extend(
                    a.scalar_array_value.iter().enumerate().step_by(step).map(
                        |(idx, y)| types::PlotDataPoint {
                            t: ts,
                            x: idx as f64,
                            y: *y,
                        },
                    ),
                )
            }
            global::DataType::StatusReply(v) => return (v.status, vec![]),
            _ => return (-1, vec![]),
        }
    }
    (0, result)
}

type DataStream = Pin<Box<dyn Stream<Item = global::DataReply> + Send>>;
type PlotStream = Pin<Box<dyn Stream<Item = types::PlotReplyData> + Send>>;

#[derive(Default)]
pub struct ACSysSubscriptions;

// Private methods used by subscriptions.

impl<'ctx> ACSysSubscriptions {
    // Returns a stream of live data for a list of devices. If an end-time
    // is specified, the stream will end once it is reached.

    async fn live_data(
        ctxt: &Context<'ctx>, drfs: &[String], end_time: Option<f64>,
    ) -> Result<DataStream> {
        // Strip any source designation and append the once-immediate.

        let processed_drfs: Vec<_> =
            drfs.iter().map(|v| strip_source(v).into()).collect();

        // Make the gRPC data request to DPM.

        match dpm::acquire_devices(
            ctxt.data::<Connection>().unwrap(),
            ctxt.data::<global::AuthInfo>()
                .ok()
                .and_then(global::AuthInfo::token)
                .as_ref(),
            processed_drfs,
        )
        .await
        {
            Ok(s) => {
                use tokio_stream::StreamExt;

                Ok(
                    // If there's an end time, we need to limit the stream.
                    if let Some(end_time) = end_time {
                        // Build a set of integers representing the indices
                        // of the request. As replies arrive, the corresponding
                        // index will be removed from the set when the timestamp
                        // exceeds the end time. When the set is empty, the
                        // stream will close.

                        let mut remaining: HashSet<usize> =
                            (0..drfs.len()).collect();

                        // Return the stream of results. The stream from DPM
                        // is first wrapped with a TakeWhile stream which will
                        // return elements until the array of data is empty.
                        // That stream is wrapped with a Map stream which
                        // converts each element to a `global::DataReply` type.

                        Box::pin(StreamExt::map_while(
                            s.into_inner(),
                            move |v| {
                                // Use pattern matching to dig deep into the
                                // returned reply. Only an empty array will
                                // end the stream.

                                if let Ok(daq::ReadingReply {
                                    index: idx,
                                    value:
                                        Some(reading_reply::Value::Readings(
                                            daq::Readings {
                                                reading: ref rdg,
                                                ..
                                            },
                                        )),
                                    ..
                                }) = v
                                {
                                    if let Some(ts) = rdg[0].timestamp {
                                        let ts = (ts.seconds as f64)
                                            + (ts.nanos as f64
                                                / 1_000_000_000.0);

                                        if ts <= end_time {
                                            remaining.remove(&(idx as usize));
                                        }
                                    }
                                }

                                if remaining.is_empty() {
                                    None
                                } else {
                                    Some(xlat_reply(v))
                                }
                            },
                        )) as DataStream
                    } else {
                        Box::pin(StreamExt::map(s.into_inner(), xlat_reply))
                            as DataStream
                    },
                )
            }
            Err(e) => Err(Error::new(format!("{}", e).as_str())),
        }
    }

    // When we get archived data from DPM, we get a stream of
    // `ReadingReply`s. When all the data is sent, we get an empty
    // array. This function maps each element from a `ReplyReply`
    // into a `global::DataReply`. Also, when an empty array is
    // found, the stream is closed.

    fn process_archive_stream(
        s: impl Stream<Item = Result<daq::ReadingReply, tonic::Status>>
            + Send
            + 'static,
    ) -> DataStream {
        use tokio_stream::StreamExt;

        Box::pin(StreamExt::map(s, xlat_reply)) as DataStream
    }

    // Returns a stream containing archived data for a device.

    async fn archived_data(
        ctxt: &Context<'ctx>, device: &str, start_time: f64, end_time: f64,
    ) -> Result<DataStream> {
        let drf = format!(
            "{}<-LOGGER:{}:{}",
            strip_source(device),
            (start_time * 1_000.0) as u128,
            (end_time * 1_000.0) as u128
        );

        // Make the gRPC data request to DPM.

        match dpm::acquire_devices(
            ctxt.data::<Connection>().unwrap(),
            ctxt.data::<global::AuthInfo>()
                .ok()
                .and_then(global::AuthInfo::token)
                .as_ref(),
            vec![drf],
        )
        .await
        {
            Ok(s) => Ok(Self::process_archive_stream(s.into_inner())),
            Err(e) => Err(Error::new(format!("{}", e).as_str())),
        }
    }

    // A helper method to handle plots that request continuous data.

    async fn handle_continuous(
        &self, ctxt: &Context<'ctx>, drfs: Vec<String>,
        window_size: Option<usize>, n_acquisitions: Option<usize>,
        x_min: Option<f64>, x_max: Option<f64>, start_time: Option<f64>,
        end_time: Option<f64>,
    ) -> Result<PlotStream> {
        let r = x_min.map(|v| v as usize).unwrap_or(0)
            ..(x_max.map(|v| (v as usize) + 1).unwrap_or(DEF_MAX_WAVEFORM));
        let now = now();
        let mut reply = types::PlotReplyData {
            plot_id: "demo".into(),
            timestamp: now,
            data: drfs
                .iter()
                .map(|_| types::PlotChannelData {
                    channel_rate: "Unknown".into(),
                    channel_units: "V".into(),
                    channel_status: 0,
                    channel_data: vec![],
                })
                .collect(),
        };

        stuff_fake_data(&mut r.clone(), &drfs, 0.0, &mut reply.data);

        let strm = self
            .accelerator_data(ctxt, drfs.clone(), start_time, end_time)
            .await?;
        let s = strm.filter_map(move |e: global::DataReply| {
            let (sts, mut data) = to_plot_data(r.len(), &window_size, &e.data);

            // Take all the points from the current reply and extend
            // the outgoing data.

            reply.data[e.ref_id as usize].channel_data.append(&mut data);

            // Set-up a nested scope range so we can modify the currently
            // tracked status.

            {
                let curr: &mut i16 =
                    &mut reply.data[e.ref_id as usize].channel_status;

                *curr = std::cmp::min(*curr, sts);
            }

            // If we have data (or status) for every channel, we can
            // determine what needs to be sent to the client.

            if reply
                .data
                .iter()
                .all(|e| e.channel_status != 0 || !e.channel_data.is_empty())
            {
                let ts = e.data[0].timestamp;
                let mut temp = types::PlotReplyData {
                    plot_id: "demo".into(),
                    timestamp: now,
                    data: reply
                        .data
                        .iter()
                        .map(|e| types::PlotChannelData {
                            channel_rate: "Unknown".into(),
                            channel_units: e.channel_units.clone(),
                            channel_status: e.channel_status,
                            channel_data: vec![],
                        })
                        .collect(),
                };

                std::mem::swap(&mut temp, &mut reply);
                stuff_fake_data(&mut r.clone(), &drfs, ts, &mut reply.data);
                future::ready(Some(temp))
            } else {
                future::ready(None)
            }
        });

        if let Some(n) = n_acquisitions.map(|v| v.max(1)) {
            Ok(Box::pin(s.take(n)) as PlotStream)
        } else {
            Ok(Box::pin(s) as PlotStream)
        }
    }

    // This method is used to drop all points before a given timestamp.
    // This is used when we get a known timestamp, but haven't seen the
    // event of interest yet.

    fn flush(buf: &mut types::PlotReplyData, ts: f64) {
        for chan in buf.data.iter_mut() {
            let idx = chan.channel_data.partition_point(|v| v.x < ts);

            chan.channel_data.drain(0..idx);
        }
    }

    fn prep_outgoing(
        remaining: &mut types::PlotReplyData, out: &mut types::PlotReplyData,
        ev_ts: f64, ts: f64,
    ) {
        // "zip" together the vectors containing the devices' data. We want
        // to handle the two buffers together and this guarantees we're
        // handling the proper pairs.

        for (out_chan, rem_chan) in
            out.data.iter_mut().zip(remaining.data.iter_mut())
        {
            let idx = out_chan.channel_data.partition_point(|v| v.x < ts);

            rem_chan.channel_data.clear();
            rem_chan
                .channel_data
                .extend(out_chan.channel_data.drain(idx..));

            for out_data in out_chan.channel_data.iter_mut() {
                out_data.t = out_data.x;
                out_data.x -= ev_ts;
            }
        }
    }

    // A helper method to handle plots that want to sync their data to
    // a clock event.

    async fn handle_triggered(
        &self, ctxt: &Context<'ctx>, drfs: Vec<String>, trigger_event: u8,
        start_time: Option<f64>, end_time: Option<f64>,
    ) -> Result<PlotStream> {
        use crate::g_rpc::clock;
        use async_stream::stream;

        // This is an empty reply. It is the starting point that is used
        // to accumulate when the event fires.

        let template = types::PlotReplyData {
            plot_id: "demo".into(),
            timestamp: now(),
            data: drfs
                .iter()
                .map(|_| types::PlotChannelData {
                    channel_rate: "Unknown".into(),
                    channel_units: "V".into(),
                    channel_status: 0,
                    channel_data: vec![],
                })
                .collect(),
        };

        // Subscribe for clock events. Along with the trigger event, we
        // also subscribe to the $0F event. We do this because we don't
        // know when the next trigger event is going to occur. However,
        // the $0F happens at 15Hz and we can use its timestamp to know
        // whether we can forward the accumulated data (since we know
        // the next trigger event will have a higher timestamp than the
        // currently received $0F.)

        let clock_list: &[i32] = if trigger_event != 0x0f {
            &[0x0f, trigger_event as i32]
        } else {
            &[0x0f]
        };
        let mut tclk = clock::subscribe(clock_list).await?.into_inner();
        let mut dev_data = self
            .accelerator_data(ctxt, drfs.clone(), start_time, end_time)
            .await?;

        #[rustfmt::skip]
        let strm = stream! {
	    let mut event_time: Option<f64> = None;
	    let mut outgoing = template.clone();
	    let mut divisor = 0;

	    // Infinitely loop until one of the streams has an error or
	    // the client cancels the subscription.

	    loop {
		tokio::select! {
		    opt_rdg = dev_data.next() => {
			if let Some(rdg) = opt_rdg {
			    for d in rdg.data.iter() {
				// We only handle scalar data for triggered
				// plots.

				if let global::DataType::Scalar(y) = &d.result {
				    outgoing.data[rdg.ref_id as usize]
					.channel_data
					.push(types::PlotDataPoint {
					    t: d.timestamp,
					    x: d.timestamp,
					    y: y.scalar_value,
					})
				} else {
				    error!(
					"nonscalar data in triggered plot for device {}",
					&drfs[rdg.ref_id as usize]
				    );
				    break
				}
			    }
			} else {
			    error!("data stream closed");
			    break
			}
		    }

		    // If we receive a tclk event, we need to process our
		    // accumulated data.

		    opt_ev = tclk.next() => {
			if let Some(Ok(ei)) = opt_ev {
			    let triggered = ei.event == (trigger_event as i32);
			    let ts = ei.stamp.unwrap();
			    let ts = ts.seconds as f64 + ts.nanos as f64
				/ 1_000_000_000.0;

			    // If the event time is `None`, we haven't seen
			    // a trigger yet. In this case, we throw away
			    // all the data with a timestamp less than this
			    // clock's.

			    if let Some(ev_ts) = event_time {
				if triggered || divisor == 0 {
				    // Process the outgoing reply. Any data
				    // with a timestamp later than `ts` is
				    // saved in `remaining`.

				    let mut remaining = template.clone();

				    Self::prep_outgoing(
					&mut remaining,
					&mut outgoing,
					ev_ts,
					ts
				    );

				    // If there's any data ready to go out,
				    // send it.

				    if outgoing
					.data
					.iter()
					.any(|v| !v.channel_data.is_empty()) {
					yield outgoing;
				    }

				    // The remaining data becomes the new,
				    // outgoing reply.

				    outgoing = remaining;
				}
			    } else {
				Self::flush(&mut outgoing, ts)
			    }

			    // If it's our trigger event, update the time.

			    if triggered {
				event_time = Some(ts);
			    }

			    // If it's the 15 Hz event, update the divisor.

			    if ei.event == 0x0f {
				divisor = (divisor + 1) % 5;
			    }
			} else {
			    error!("clock stream failed : {:?}", opt_ev);
			    break
			}
		    }
		}
	    }
	};

        Ok(Box::pin(strm) as PlotStream)
    }
}

#[Subscription]
impl<'ctx> ACSysSubscriptions {
    #[doc = "Retrieve data from accelerator devices.

Accepts a list of DRF strings and streams the resulting data. The \
`start_time` and `end_time` parameters determine the range in which \
data should be returned for the device(s). Dates in the past will \
retrieve data from archivers and dates in the future will return \
live data."]
    #[instrument(skip(self, ctxt))]
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
		    timestamp -- represented as seconds since Jan 1st, \
		    1970 UTC. If the control system cannot find data at \
		    the actual timestamp, it will return the oldest data \
		    it has that's greater then the timestamp. If this \
		    parameter is `null`, it will simply return live data."
        )]
        start_time: Option<f64>,
        #[graphql(
            desc = "The stream will close once the device data's timestamp \
		    reaches this value -- represented as seconds since Jan \
		    1st, 1970 UTC. This parameter must be greater than the \
		    `startTime` parameter. If this parameter is `null`, the \
		    stream will return live data until the client closes it."
        )]
        end_time: Option<f64>,
    ) -> Result<DataStream> {
        let now = now();
        let need_live = end_time.map(|v| v >= now).unwrap_or(true);
        let need_archived = start_time.map(|v| v <= now).unwrap_or(false);

        info!("new request");

        // If we need live data, start the collection now. This gives some
        // time for the data to also be saved in a data logger.

        let s_live = if need_live {
            ACSysSubscriptions::live_data(ctxt, &drfs, end_time).await?
        } else {
            Box::pin(tokio_stream::empty()) as DataStream
        };

        // Build up the set of streams that will return archived data.

        let s_archived = if need_archived {
            let mut streams = tokio_stream::StreamMap::new();

            for drf in drfs {
                let stream = ACSysSubscriptions::archived_data(
                    ctxt,
                    &drf,
                    start_time.unwrap(),
                    end_time.unwrap_or(now),
                )
                .await?;

                streams.insert(drf, Box::pin(stream) as DataStream);
            }
            Box::pin(tokio_stream::StreamExt::map(streams, |v| v.1))
                as DataStream
        } else {
            Box::pin(tokio_stream::empty()) as DataStream
        };

        Ok(datastream::merge(s_archived, s_live))
    }

    #[doc = "Retrieve correlated plot data.

This query sets up a request which returns a stream of data, presumably \
used for plotting. Unlike the `acceleratorData` query, this stream \
returns data for all the devices in one reply. Since the data is \
correlated, all the devices are collected on the same event."]
    #[instrument(skip(self, ctxt))]
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
        start_time: Option<f64>, end_time: Option<f64>,
    ) -> Result<PlotStream> {
        info!("new request");

        // Add the periodic rate to each of the device names after stripping
        // any event specifier.

        let drfs: Vec<_> = drf_list
            .iter()
            .map(|v| strip_event(v))
            .map(add_event(update_delay, None))
            .collect();

        if let Some(event) = trigger_event {
            self.handle_triggered(ctxt, drfs, event, start_time, end_time)
                .await
        } else {
            self.handle_continuous(
                ctxt,
                drfs,
                window_size,
                n_acquisitions,
                x_min,
                x_max,
                start_time,
                end_time,
            )
            .await
        }
    }
}

fn const_data(
    r: &mut dyn Iterator<Item = usize>, ts: f64, y: f64,
) -> Vec<types::PlotDataPoint> {
    r.map(|idx| types::PlotDataPoint {
        t: ts,
        x: idx as f64,
        y,
    })
    .collect()
}

fn ramp_data(
    r: &mut dyn Iterator<Item = usize>, ts: f64,
) -> Vec<types::PlotDataPoint> {
    r.map(|idx| types::PlotDataPoint {
        t: ts,
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
            t: ts,
            x,
            y: (x * x) / 125.0 - 4.0 * x + 500.0,
        }
    })
    .collect()
}

fn sine_data(
    r: &mut dyn Iterator<Item = usize>, ts: f64,
) -> Vec<types::PlotDataPoint> {
    let k = (std::f64::consts::PI * 2.0) / (DEF_MAX_WAVEFORM as f64);

    r.map(|idx| types::PlotDataPoint {
        t: ts,
        x: idx as f64,
        y: f64::sin(k * (idx as f64)),
    })
    .collect()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_removing_event() {
        use super::strip_event;

        assert_eq!(strip_event("abc"), "abc");
        assert_eq!(strip_event("abc@e,23"), "abc");

        assert_eq!(strip_event(""), "");
        assert_eq!(strip_event("@"), "");
    }

    #[test]
    fn test_removing_source() {
        use super::strip_source;

        assert_eq!(strip_source("abc"), "abc");
        assert_eq!(strip_source("abc@e,23"), "abc@e,23");
        assert_eq!(strip_source("abc<-JUNK"), "abc");
        assert_eq!(strip_source("abc@e,23<-JUNK"), "abc@e,23");

        assert_eq!(strip_source(""), "");
        assert_eq!(strip_source("<"), "");
        assert_eq!(strip_source("abc@e,23<-JUNK<-MOREJUNK"), "abc@e,23");
    }

    #[test]
    fn test_add_event_specification() {
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

    #[test]
    fn test_flush() {
        const POINT_DATA: &[types::PlotDataPoint] = &[
            types::PlotDataPoint {
                t: 1.0,
                x: 1.0,
                y: 10.0,
            },
            types::PlotDataPoint {
                t: 2.0,
                x: 2.0,
                y: 11.0,
            },
            types::PlotDataPoint {
                t: 3.0,
                x: 3.0,
                y: 12.0,
            },
            types::PlotDataPoint {
                t: 4.0,
                x: 4.0,
                y: 13.0,
            },
            types::PlotDataPoint {
                t: 5.0,
                x: 5.0,
                y: 14.0,
            },
        ];

        let mut buf = types::PlotReplyData {
            plot_id: "test".to_owned(),
            timestamp: 0.0,
            data: vec![types::PlotChannelData {
                channel_rate: "Unknown".into(),
                channel_units: "V".to_owned(),
                channel_status: 0,
                channel_data: POINT_DATA.to_owned(),
            }],
        };

        ACSysSubscriptions::flush(&mut buf, 0.0);

        assert_eq!(buf.data[0].channel_data, POINT_DATA);

        ACSysSubscriptions::flush(&mut buf, 3.5);

        assert_eq!(buf.data[0].channel_data, &POINT_DATA[3..]);

        ACSysSubscriptions::flush(&mut buf, 10.0);

        assert!(buf.data[0].channel_data.is_empty());
    }

    #[test]
    fn test_partitioning() {
        const POINT_DATA: &[types::PlotDataPoint] = &[
            types::PlotDataPoint {
                t: 1.0,
                x: 1.0,
                y: 10.0,
            },
            types::PlotDataPoint {
                t: 2.0,
                x: 2.0,
                y: 11.0,
            },
            types::PlotDataPoint {
                t: 3.0,
                x: 3.0,
                y: 12.0,
            },
            types::PlotDataPoint {
                t: 4.0,
                x: 4.0,
                y: 13.0,
            },
            types::PlotDataPoint {
                t: 5.0,
                x: 5.0,
                y: 14.0,
            },
        ];

        let mut buf = types::PlotReplyData {
            plot_id: "test".to_owned(),
            timestamp: 0.0,
            data: vec![types::PlotChannelData {
                channel_rate: "Unknown".into(),
                channel_units: "V".to_owned(),
                channel_status: 0,
                channel_data: POINT_DATA.to_owned(),
            }],
        };

        let mut rem = buf.clone();

        ACSysSubscriptions::prep_outgoing(&mut rem, &mut buf, 0.5, 0.0);

        assert!(buf.data[0].channel_data.is_empty());
        assert_eq!(rem.data[0].channel_data, POINT_DATA);

        buf = rem.clone();

        ACSysSubscriptions::prep_outgoing(&mut rem, &mut buf, 0.5, 3.5);

        assert_eq!(
            buf.data[0].channel_data,
            &[
                types::PlotDataPoint {
                    t: 1.0,
                    x: 0.5,
                    y: 10.0,
                },
                types::PlotDataPoint {
                    t: 2.0,
                    x: 1.5,
                    y: 11.0,
                },
                types::PlotDataPoint {
                    t: 3.0,
                    x: 2.5,
                    y: 12.0,
                }
            ],
        );
        assert_eq!(rem.data[0].channel_data, &POINT_DATA[3..]);

        buf = rem.clone();

        ACSysSubscriptions::prep_outgoing(&mut rem, &mut buf, 0.5, 10.0);

        assert_eq!(
            buf.data[0].channel_data,
            &[
                types::PlotDataPoint {
                    t: 4.0,
                    x: 3.5,
                    y: 13.0,
                },
                types::PlotDataPoint {
                    t: 5.0,
                    x: 4.5,
                    y: 14.0,
                },
            ]
        );
        assert!(rem.data[0].channel_data.is_empty());
    }
}
