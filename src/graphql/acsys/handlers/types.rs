use async_graphql::*;
use chrono::*;

#[derive(SimpleObject)]
pub struct ErrorReply {
    pub message: String,
}

#[derive(SimpleObject)]
pub struct StatusReply {
    pub status: i16,
}

#[derive(SimpleObject)]
pub struct Scalar {
    pub scalar_value: f64,
}

#[derive(SimpleObject)]
pub struct ScalarArray {
    pub scalar_array_value: Vec<f64>,
}

#[derive(SimpleObject)]
pub struct Raw {
    pub raw_value: Vec<u8>,
}

#[derive(SimpleObject)]
pub struct Text {
    pub text_value: String,
}

#[derive(SimpleObject)]
pub struct TextArray {
    pub text_array_value: Vec<String>,
}

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
}

#[derive(SimpleObject)]
pub struct DigStatusEntry {
    pub mask_val: u32,
    pub match_val: u32,
    pub invert: bool,
    pub short_name: String,
    pub long_name: String,
    pub true_str: String,
    pub true_color: u32,
    pub true_char: String,
    pub false_str: String,
    pub false_color: u32,
    pub false_char: String,
}

#[derive(SimpleObject)]
pub struct DigExtStatusEntry {
    pub bit_no: u32,
    pub color0: u32,
    pub name0: String,
    pub color1: u32,
    pub name1: String,
    pub description: String,
}

#[derive(SimpleObject)]
pub struct DigStatus {
    pub entries: Vec<DigStatusEntry>,
    pub ext_entries: Vec<DigExtStatusEntry>,
}

/// Describes one digital control command used by a device. `name` is the name of the command and can be used by applications to create a descriptive menu. `value` is the actual integer value to send to the device in order to perform the command.
#[derive(SimpleObject)]
pub struct DigControlEntry {
    pub value: i32,
    pub short_name: String,
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
