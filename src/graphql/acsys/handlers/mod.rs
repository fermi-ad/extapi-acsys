use crate::g_rpc::clock;
use crate::g_rpc::devdb;
use crate::g_rpc::dpm;
use async_graphql::*;
use futures_util::{stream, Stream, StreamExt};
use std::pin::Pin;
use tonic::Status;
use tracing::{error, info, warn};

// This module contains the GraphQL types that we'll use for the API.

mod types;

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
                reading: di.reading.as_ref().map(|p| types::DeviceProperty {
                    primary_units: p.primary_units.clone(),
                    common_units: p.common_units.clone(),
                }),
                setting: di.setting.as_ref().map(|p| types::DeviceProperty {
                    primary_units: p.primary_units.clone(),
                    common_units: p.common_units.clone(),
                }),
                dig_control: di.dig_control.as_ref().map(|p| {
                    types::DigControl {
                        entries: p.cmds.iter().map(to_dig_ctrl).collect(),
                    }
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

pub struct QueryRoot;

// Define the schema's query entry points. Any methods defined in this
// section will appear in the schema.

#[Object]
impl QueryRoot {
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
        let result = match devdb::get_device_info(&devices).await {
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

        types::DeviceInfoReply { result }
    }
}

pub struct MutationRoot;

#[Object]
impl MutationRoot {
    /// Sends a setting to a device.
    async fn set_device(
        &self, device: String, value: types::DevValue,
    ) -> types::StatusReply {
        types::StatusReply { status: 0 }
    }
}

fn xlat_type(t: &dpm::proto::Data) -> types::DataType {
    match t.value.as_ref() {
        Some(dpm::proto::data::Value::Scalar(v)) => {
            types::DataType::Scalar(types::Scalar { scalar_value: *v })
        }
        Some(dpm::proto::data::Value::ScalarArr(v)) => {
            types::DataType::ScalarArray(types::ScalarArray {
                scalar_array_value: v.value.clone(),
            })
        }
        Some(dpm::proto::data::Value::Status(v)) => {
            types::DataType::StatusReply(types::StatusReply {
                status: *v as i16,
            })
        }
        Some(v) => {
            info!("can't translate {:?}", v);
            todo!()
        }
        _ => todo!(),
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

        if let Some(ref data) = e.data {
            types::DataReply {
                ref_id: e.index as i32,
                cycle: 1,
                data: types::DataInfo {
                    timestamp: std::time::SystemTime::now().into(),
                    result: xlat_type(data),
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

pub struct SubscriptionRoot;

#[Subscription]
impl SubscriptionRoot {
    async fn accelerator_data(&self, drfs: Vec<String>) -> DataStream {
        match dpm::acquire_devices("", drfs.clone()).await {
            Ok(s) => {
                Box::pin(s.into_inner().map(mk_xlater(drfs))) as DataStream
            }
            Err(e) => {
                error!("{}", &e);
                Box::pin(stream::empty()) as DataStream
            }
        }
    }

    async fn report_events(&self, events: Vec<i32>) -> EventStream {
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
