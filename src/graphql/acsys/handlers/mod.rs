use crate::g_rpc::clock;
use crate::g_rpc::devdb;
use crate::g_rpc::dpm;
use async_graphql::*;
use futures_util::{stream, Stream, StreamExt};
use std::pin::Pin;
use tokio::time::Instant;
use tonic::Status;
use tracing::{error, info, warn};

// This module contains the GraphQL types that we'll use for the API.

pub mod types;

// Converts a `DigitalControlItem`, from the gRPC API, into a
// `DigControlEntry` struct, used in the GraphQL API.

fn to_dig_ctrl(
    item: &devdb::proto::DigitalControlItem,
) -> types::DigControlEntry {
    types::DigControlEntry {
        value: item.value as i32,
        short_name: item.short_name.clone(),
        long_name: item.long_name.clone(),
    }
}

// Converts a `DigitalStatusItem`, from the gRPC API, into a
// `DigStatusEntry` struct used by the GraphQL API.

fn to_dig_status(
    item: &devdb::proto::DigitalStatusItem,
) -> types::DigStatusEntry {
    types::DigStatusEntry {
        mask_val: item.mask_val,
        match_val: item.match_val,
        invert: item.invert,
        short_name: item.short_name.to_owned(),
        long_name: item.long_name.to_owned(),
        true_str: item.true_str.to_owned(),
        true_color: item.true_color,
        true_char: item.true_char.to_owned(),
        false_str: item.false_str.to_owned(),
        false_color: item.false_color,
        false_char: item.false_char.to_owned(),
    }
}

fn to_ext_dig_status(
    item: &devdb::proto::DigitalExtStatusItem,
) -> types::DigExtStatusEntry {
    types::DigExtStatusEntry {
        bit_no: item.bit_no,
        color0: item.color0,
        name0: item.name0.clone(),
        color1: item.color1,
        name1: item.name1.clone(),
        description: item.description.clone(),
    }
}

// Converts an `InfoEntry` structure, from the gRPC API, into a
// `DeviceInfoResult` struct, used in the GraphQL API. This function
// is intended to be used by an iterator's `.map()` method.

fn to_info_result(item: &devdb::proto::InfoEntry) -> types::DeviceInfoResult {
    match &item.result {
        // If the `InfoEntry` contains device information, transfer
        // the information.
        Some(devdb::proto::info_entry::Result::Device(di)) => {
            types::DeviceInfoResult::DeviceInfo(types::DeviceInfo {
                description: di.description.clone(),
                reading: di.reading.as_ref().map(|p| types::ReadingProp {
                    primary_units: p.primary_units.clone(),
                    common_units: p.common_units.clone(),
                    min_val: p.min_val,
                    max_val: p.max_val,
                    primary_index: p.p_index,
                    common_index: p.c_index,
                    coeff: p.coeff.clone(),
                }),
                setting: di.setting.as_ref().map(|p| types::SettingProp {
                    primary_units: p.primary_units.clone(),
                    common_units: p.common_units.clone(),
                    min_val: p.min_val,
                    max_val: p.max_val,
                    primary_index: p.p_index,
                    common_index: p.c_index,
                    coeff: p.coeff.clone(),
                }),
                dig_control: di.dig_control.as_ref().map(|p| {
                    types::DigControl {
                        entries: p.cmds.iter().map(to_dig_ctrl).collect(),
                    }
                }),
                dig_status: di.dig_status.as_ref().map(|p| types::DigStatus {
                    entries: p.bits.iter().map(to_dig_status).collect(),
                    ext_entries: p
                        .ext_bits
                        .iter()
                        .map(to_ext_dig_status)
                        .collect(),
                }),
            })
        }

        // If the `InfoEntry` contains an error status, translate it
        // into the GraphQL error status.
        Some(devdb::proto::info_entry::Result::ErrMsg(msg)) => {
            types::DeviceInfoResult::ErrorReply(types::ErrorReply {
                message: msg.clone(),
            })
        }

        // This response should never happen. For some reason, the
        // Rust library implements gRPC unions as an enumeration
        // wrapped in an Option. Maybe `None` represents a default
        // value? Whatever the reason, the DevDB gRPC service always
        // returns a value for this field so we should never see it as
        // `None`.
        None => types::DeviceInfoResult::ErrorReply(types::ErrorReply {
            message: "empty response".into(),
        }),
    }
}

// Create a zero-sized struct to attach the GraphQL handlers.

pub struct Queries;

// Define the schema's query entry points. Any methods defined in this
// section will appear in the schema.

#[Object]
impl Queries {
    /// Retrieve the next data point for the specified devices. Depending upon the event in the DRF string, the data may come back immediately or after a delay.
    async fn accelerator_data(
        &self, _drfs: Vec<String>,
    ) -> Vec<types::DataReply> {
        vec![]
    }

    /// Retrieves device information. The parameter specifies the device. The reply will contain the device's information or an error status indicating why the query failed.
    async fn device_info(
        &self, devices: Vec<String>,
    ) -> types::DeviceInfoReply {
        let now = Instant::now();
        let result = devdb::get_device_info(&devices).await;
        let rpc_time = now.elapsed().as_micros();

        let reply = match result {
            Ok(s) => s.into_inner().set.iter().map(to_info_result).collect(),
            Err(e) => {
                let err_msg = format!("{}", &e);

                devices
                    .iter()
                    .map(|_| {
                        types::DeviceInfoResult::ErrorReply(types::ErrorReply {
                            message: err_msg.clone(),
                        })
                    })
                    .collect()
            }
        };

        let total_time = now.elapsed().as_micros();

        info!(
            "deviceInfo({:?}) => total: {} μs, rpc: {} μs, local: {} μs",
            &devices[..],
            total_time,
            rpc_time,
            total_time - rpc_time
        );
        types::DeviceInfoReply { result: reply }
    }
}

pub struct Mutations;

#[Object]
impl Mutations {
    /// Sends a setting to a device.
    ///
    /// Not all devices can be set -- most are read-only. For ACNET devices, the `device` string should use DRF notation to specify one of the two settable properties: `.SETTING` and `.CONTROL`.
    async fn set_device(
        &self, device: String, value: types::DevValue,
    ) -> types::StatusReply {
        let now = Instant::now();
        let result =
            dpm::set_device("DEADBEEF", device.clone(), value.into()).await;

        info!(
            "setDevice({}) => rpc: {} μs",
            &device,
            now.elapsed().as_micros()
        );

        types::StatusReply {
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
    dyn (FnMut(Result<dpm::proto::Reading, Status>) -> types::DataReply)
        + Send
        + Sync,
> {
    Box::new(move |e: Result<dpm::proto::Reading, Status>| {
        let e = e.unwrap();

        if let Some(data) = e.data {
            types::DataReply {
                ref_id: e.index as i32,
                cycle: 1,
                data: types::DataInfo {
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

type DataStream = Pin<Box<dyn Stream<Item = types::DataReply> + Send>>;
type EventStream = Pin<Box<dyn Stream<Item = types::EventInfo> + Send>>;

pub struct Subscriptions;

#[Subscription]
impl Subscriptions {
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

    async fn report_events(&self, events: Vec<i32>) -> EventStream {
        info!("subscribing to clock events: {:?}", &events);
        match clock::subscribe(&events).await {
            Ok(s) => Box::pin(s.into_inner().map(Result::unwrap).map(
                |clock::proto::EventInfo { stamp, event, .. }| {
                    let stamp = stamp.unwrap();

                    types::EventInfo {
                        timestamp: (std::time::UNIX_EPOCH
                            + std::time::Duration::from_millis(
                                (stamp.seconds * 1_000) as u64
                                    + (stamp.nanos / 1_000_000) as u64,
                            ))
                        .into(),
                        event: event as u16,
                    }
                },
            )) as EventStream,
            Err(e) => {
                error!("{}", &e);
                Box::pin(stream::empty()) as EventStream
            }
        }
    }
}
