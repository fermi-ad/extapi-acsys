use crate::g_rpc::devdb;

use async_graphql::*;
use tokio::time::Instant;
use tracing::info;

// Pull in global types.

use super::types as global;

// Pull in our local types.

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
                    is_contr_setting: p.is_contr_setting,
                    is_destructive_read: p.is_destructive_read,
                    is_fe_scaling: p.is_fe_scaling,
                    is_step_motor: p.is_step_motor,
                }),
                setting: di.setting.as_ref().map(|p| types::SettingProp {
                    primary_units: p.primary_units.clone(),
                    common_units: p.common_units.clone(),
                    min_val: p.min_val,
                    max_val: p.max_val,
                    primary_index: p.p_index,
                    common_index: p.c_index,
                    coeff: p.coeff.clone(),
                    is_contr_setting: p.is_contr_setting,
                    is_destructive_read: p.is_destructive_read,
                    is_fe_scaling: p.is_fe_scaling,
                    is_knobbable: p.is_knobbable,
                    is_step_motor: p.is_step_motor,
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
            types::DeviceInfoResult::ErrorReply(global::ErrorReply {
                message: msg.clone(),
            })
        }

        // This response should never happen. For some reason, the
        // Rust library implements gRPC unions as an enumeration
        // wrapped in an Option. Maybe `None` represents a default
        // value? Whatever the reason, the DevDB gRPC service always
        // returns a value for this field so we should never see it as
        // `None`.
        None => types::DeviceInfoResult::ErrorReply(global::ErrorReply {
            message: "empty response".into(),
        }),
    }
}

// Create a zero-sized struct to attach the GraphQL handlers.

#[derive(Default)]
pub struct DevDBQueries;

// Define the schema's query entry points. Any methods defined in this
// section will appear in the schema.

#[Object]
impl DevDBQueries {
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
                        types::DeviceInfoResult::ErrorReply(
                            global::ErrorReply {
                                message: err_msg.clone(),
                            },
                        )
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
