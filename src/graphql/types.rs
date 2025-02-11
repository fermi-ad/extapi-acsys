use async_graphql::*;
use chrono::*;
use serde::Deserialize;

pub struct AuthInfo(Option<String>);

#[derive(Deserialize)]
struct Claims {
    sub: String,
}

impl AuthInfo {
    pub fn new(info: &Option<String>) -> Self {
        AuthInfo(info.as_ref().and_then(|v| {
            if let ["Bearer", token] = v.split(' ').collect::<Vec<&str>>()[..] {
                Some(token.to_string())
            } else {
                None
            }
        }))
    }

    #[cfg(test)]
    pub fn has_token(&self) -> bool {
        self.0.is_some()
    }

    pub fn token(&self) -> Option<String> {
        self.0.clone()
    }

    pub fn unsafe_account(&self) -> Option<String> {
        use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};

        self.0.as_ref().and_then(|token| {
            let mut validation = Validation::new(Algorithm::HS256);

            validation.insecure_disable_signature_validation();

            if let Ok(decoded) = decode::<Claims>(
                token,
                &DecodingKey::from_secret("".as_ref()), // No secret required
                &validation,
            ) {
                Some(decoded.claims.sub)
            } else {
                None
            }
        })
    }
}

/// Contains an informative message describing why a request resulted in an error.
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
// `proto::Data` type.

impl From<DevValue> for proto::Data {
    fn from(val: DevValue) -> Self {
        match val {
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
// `DataType`.

impl From<proto::Data> for DataType {
    fn from(val: proto::Data) -> Self {
        match val.value {
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
            Some(proto::data::Value::Text(v)) => {
                DataType::Text(Text { text_value: v })
            }
            Some(proto::data::Value::TextArr(proto::data::TextArray {
                value: v,
            })) => DataType::TextArray(TextArray {
                text_array_value: v,
            }),
            Some(v) => {
                warn!("can't translate {:?}", &v);
                todo!()
            }
            _ => todo!(),
        }
    }
}
