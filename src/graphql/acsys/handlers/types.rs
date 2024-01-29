use async_graphql::*;
use chrono::*;

/// Contains an informaive message describing why a request resulted in an error.
#[derive(SimpleObject)]
pub struct ErrorReply {
    pub message: String,
}

/// Contains an ACNET status code. The Data Pool Manager currently returns these status codes, but they may go away in the future since EPICS has its own set of error codes.
#[derive(SimpleObject)]
pub struct StatusReply {
    pub status: i16,
}

/// Represents a simple, floating point value.
#[derive(SimpleObject)]
pub struct Scalar {
    pub scalar_value: f64,
}

/// Represents an array of floating point values.
#[derive(SimpleObject)]
pub struct ScalarArray {
    pub scalar_array_value: Vec<f64>,
}

/// Contains the raw, unscaled data returned by a device.
#[derive(SimpleObject)]
pub struct Raw {
    pub raw_value: Vec<u8>,
}

/// Contains a textual value returned by a device.
#[derive(SimpleObject)]
pub struct Text {
    pub text_value: String,
}

/// Represents an array of textual values.
#[derive(SimpleObject)]
pub struct TextArray {
    pub text_array_value: Vec<String>,
}

/// Represents a generic return type. EPICS devices have a hierarchy and this return type can model those values. Note that the value associated with the key can be another `StructData`, so arbitrarily deep trees can be created.
#[derive(SimpleObject)]
pub struct StructData {
    pub key: String,
    pub struct_value: Box<DataType>,
}

/// The control system supports several types and this entity can repesent any of them.
#[derive(Union)]
pub enum DataType {
    /// This represents an ACNET status reply. If a device request results in an error from the front-end, the data pool mananger will forward the status.
    StatusReply(StatusReply),

    /// Represents a simple, scalar value. This is a scaled, floating point value.
    Scalar(Scalar),

    /// Represents an array of scalar values. In EPICS, this would correspond to a "waveform" device.
    ScalarArray(ScalarArray),

    /// This value is used to return the raw, binary data from the device reading.
    Raw(Raw),

    /// Used for devices that return strings.
    Text(Text),

    /// Used for devices that return arrays of strings.
    TextArray(TextArray),

    /// Represents structured data. The value is a map type where the key is a string that represents a field name and the value is one of the values of this enumeration. This means you can nest `StructData` types to make arbitrarily complex types.
    StructData(StructData),
}

/// This structure holds information associated with a device's reading, A "reading" is the latest value of any of a device's properties.
#[derive(SimpleObject)]
pub struct DataInfo {
    /// Timestamp representing when the data was sampled. This value is provided as milliseconds since 1970, UTC.
    pub timestamp: DateTime<Utc>,

    /// The value of the device when sampled.
    pub result: DataType,

    /// The device's index (in the device database.)
    pub di: i32,

    /// The name of the device.
    pub name: String,
}

/// This structure wraps a device reading with some routing information: a `refId` to correlate which device, in the array of devices passed, this reply is for. It also has a `cycle` field so that reading from different devices can correlate which cycle they correspond.
#[derive(SimpleObject)]
pub struct DataReply {
    /// This is an index to indicate which entry, in the passed array of DRF strings, this reply corresponds.
    pub ref_id: i32,

    /// The cycle number in which the device was sampled. This can be used to correlate readings from several devices.
    pub cycle: u64,

    /// The returned data.
    pub data: DataInfo,
}

/// Holds data associated with a property of a device.
#[derive(SimpleObject)]
pub struct DeviceProperty {
    /// Specifies the engineering units for the primary transform of the device. This field might be `null`, if there aren't units for this transform.
    pub primary_units: Option<String>,

    /// Specifies the engineering units for the common transform of the device. This field might be `null`, if there aren't units for this transform.
    pub common_units: Option<String>,

    /// The maximum value this device will read and allow to be set. This field is a recommendation for applications to follow. The actual hardware driver will enforce the limits.
    pub min_val: f64,

    /// The minimum value this device will read and allow to be set. This field is a recommendation for applications to follow. The actual hardware driver will enforce the limits.
    pub max_val: f64,

    /// The index of the primary scaling transform.
    pub primary_index: u32,

    /// The index of the common scaling transform.
    pub common_index: u32,

    /// The coefficients to be used with the common scaling transform. There will be 0 - 10 coefficients, depending on the transform. The transform documentation refers to the constants as "c1" through "c10". These correspond to the indices 0 through 9, respectively.
    pub coeff: Vec<f64>,
}

/// Represents a legacy form to describe a basic status bit.
///
/// The BASIC STATUS property of a device traditionally modeled a power supply's set of status bits (on/off, ready/tripped, etc.) This structure models the data associated with each of these statuses and allows them to be renamed.
#[derive(SimpleObject)]
pub struct DigStatusEntry {
    /// This value is logically ANDed with the active, raw status to filter the bit that aren't related to the current status.
    pub mask_val: u32,

    /// This is the value that the masked status needs to be in order to consider it in a good state.
    pub match_val: u32,

    /// If this field is true, then the raw status is complemented before masking.
    pub invert: bool,

    /// A short name for this status.
    pub short_name: String,

    /// A longer version of the name of this status.
    pub long_name: String,

    /// A string representing the value when it's in a good state.
    pub true_str: String,

    /// The color to use when the status is in a good state.
    pub true_color: u32,

    /// A character to display that represents a good state.
    pub true_char: String,

    /// A string representing the value when it's in a bad state.
    pub false_str: String,

    /// The color to use when the status is in a bad state.
    pub false_color: u32,

    /// A character to display that represents a bad state.
    pub false_char: String,
}

/// Represents a more modern way to define the bits in the basic status.
#[derive(SimpleObject)]
pub struct DigExtStatusEntry {
    /// Indicates with which bit in the status this entry corresponds. The LSB is 0.
    pub bit_no: u32,

    /// The color to use when this bit is `false`.
    pub color0: u32,

    /// The descriptive name when this bit is `false`.
    pub name0: String,

    /// The color to use when this bit is `true`.
    pub color1: u32,

    /// The descriptive name when this bit is `false`.
    pub name1: String,

    /// The description of this bit's purpose.
    pub description: String,
}

/// The configuration of the device's basic status property.
///
/// This structure contains both the legacy and modern forms of configurations used to describe a device's basic status property.
#[derive(SimpleObject)]
pub struct DigStatus {
    /// Holds the legacy, "power supply" configuration.
    pub entries: Vec<DigStatusEntry>,

    /// Hold the modern, bit definitions.
    pub ext_entries: Vec<DigExtStatusEntry>,
}

/// Describes one digital control command used by a device.
#[derive(SimpleObject)]
pub struct DigControlEntry {
    /// The actual integer value to send to the device in order to perform the command.
    pub value: i32,

    /// The name of the command and can be used by applications to create a descriptive menu.
    pub short_name: String,

    /// A more descriptive name of the command.
    pub long_name: String,
}

/// Describes the digital control commands for a device.
#[derive(SimpleObject)]
pub struct DigControl {
    pub entries: Vec<DigControlEntry>,
}

/// A structure containing device information.
#[derive(SimpleObject)]
pub struct DeviceInfo {
    /// A text field that summarizes the device's purpose.
    pub description: String,

    /// Holds informations related to the reading property. If the device doesn't have a reading property, this field will be `null`.
    pub reading: Option<DeviceProperty>,

    /// Holds informations related to the setting property. If the device doesn't have a setting property, this field will be `null`.
    pub setting: Option<DeviceProperty>,

    pub dig_control: Option<DigControl>,
    pub dig_status: Option<DigStatus>,
}

/// The result of the device info query. It can return device information or an error message describing why information wasn't returned.
#[derive(Union)]
pub enum DeviceInfoResult {
    DeviceInfo(DeviceInfo),
    ErrorReply(ErrorReply),
}

/// The reply to the deviceInfo query.
#[derive(SimpleObject)]
pub struct DeviceInfoReply {
    pub result: Vec<DeviceInfoResult>,
}

/// Contains information about a clock event that occurred.
#[derive(SimpleObject)]
pub struct EventInfo {
    pub timestamp: DateTime<Utc>,
    pub event: u16,
}

#[derive(InputObject)]
pub struct DevValue {
    pub int_val: Option<i32>,
    pub scalar_val: Option<f64>,
    pub scalar_array_val: Option<Vec<f64>>,
    pub raw_val: Option<Vec<u8>>,
    pub text_val: Option<String>,
    pub text_array_val: Option<Vec<String>>,
}

// --------------------------------------------------------------------------
// This section defines some useful traits for types in this module.

use crate::g_rpc::dpm::proto;
use tracing::warn;

// Defining this trait allows us to convert a `DevValue` into a
// `proto::Data` type by using the `.into()` method.

impl Into<proto::Data> for DevValue {
    fn into(self) -> proto::Data {
        match self {
            DevValue {
                int_val: Some(v),
                scalar_val: _,
                scalar_array_val: _,
                raw_val: _,
                text_val: _,
                text_array_val: _,
            } => proto::Data {
                value: Some(proto::data::Value::Status(v)),
            },
            DevValue {
                int_val: None,
                scalar_val: Some(v),
                scalar_array_val: _,
                raw_val: _,
                text_val: _,
                text_array_val: _,
            } => proto::Data {
                value: Some(proto::data::Value::Scalar(v)),
            },
            DevValue {
                int_val: None,
                scalar_val: None,
                scalar_array_val: Some(v),
                raw_val: _,
                text_val: _,
                text_array_val: _,
            } => proto::Data {
                value: Some(proto::data::Value::ScalarArr(
                    proto::data::ScalarArray { value: v },
                )),
            },
            DevValue {
                int_val: None,
                scalar_val: None,
                scalar_array_val: None,
                raw_val: Some(v),
                text_val: _,
                text_array_val: _,
            } => proto::Data {
                value: Some(proto::data::Value::Raw(v)),
            },
            DevValue {
                int_val: None,
                scalar_val: None,
                scalar_array_val: None,
                raw_val: None,
                text_val: Some(v),
                text_array_val: _,
            } => proto::Data {
                value: Some(proto::data::Value::Text(v)),
            },
            DevValue {
                int_val: None,
                scalar_val: None,
                scalar_array_val: None,
                raw_val: None,
                text_val: None,
                text_array_val: Some(v),
            } => proto::Data {
                value: Some(proto::data::Value::TextArr(
                    proto::data::TextArray { value: v },
                )),
            },
            DevValue {
                int_val: None,
                scalar_val: None,
                scalar_array_val: None,
                raw_val: None,
                text_val: None,
                text_array_val: None,
            } => proto::Data {
                value: Some(proto::data::Value::Raw(vec![])),
            },
        }
    }
}

// Defining this trait allows us to convert a `proto::Data` type into a
// `DataType` by using the `.into()` method.

impl Into<DataType> for proto::Data {
    fn into(self) -> DataType {
        match self.value {
            Some(proto::data::Value::Scalar(v)) => {
                DataType::Scalar(Scalar { scalar_value: v })
            }
            Some(proto::data::Value::ScalarArr(v)) => {
                DataType::ScalarArray(ScalarArray {
                    scalar_array_value: v.value,
                })
            }
            Some(proto::data::Value::Status(v)) => {
                DataType::StatusReply(StatusReply { status: v as i16 })
            }
            Some(v) => {
                warn!("can't translate {:?}", &v);
                todo!()
            }
            _ => todo!(),
        }
    }
}
