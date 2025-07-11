use async_graphql::*;
use chrono::*;
use serde::Deserialize;

#[derive(Debug)]
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

#[doc = "Contains an informative message describing why a request resulted \
	 in an error."]
#[derive(SimpleObject)]
pub struct ErrorReply {
    pub message: String,
}

#[doc = "Contains an ACNET status code. The Data Pool Manager currently \
	 returns these status codes, but they may go away in the future \
	 since EPICS has its own set of error codes."]
#[derive(SimpleObject, Clone)]
pub struct StatusReply {
    pub status: i16,
}

#[doc = "Represents a simple, floating point value."]
#[derive(SimpleObject, Clone)]
pub struct Scalar {
    pub scalar_value: f64,
}

#[doc = "Represents an array of floating point values."]
#[derive(SimpleObject, Clone)]
pub struct ScalarArray {
    pub scalar_array_value: Vec<f64>,
}

#[doc = "Contains the raw, unscaled data returned by a device."]
#[derive(SimpleObject, Clone)]
pub struct Raw {
    pub raw_value: Vec<u8>,
}

#[doc = "Contains a textual value returned by a device."]
#[derive(SimpleObject, Clone)]
pub struct Text {
    pub text_value: String,
}

#[doc = "Represents an array of textual values."]
#[derive(SimpleObject, Clone)]
pub struct TextArray {
    pub text_array_value: Vec<String>,
}

#[doc = "Represents a generic return type. EPICS devices have a hierarchy \
	 and this return type can model those values. Note that the value \
	 associated with the key can be another `StructData`, so arbitrarily \
	 deep trees can be created."]
#[derive(SimpleObject, Clone)]
pub struct StructData {
    pub key: String,
    pub struct_value: Box<DataType>,
}

#[doc = "The control system supports several types and this entity can \
	 repesent any of them."]
#[derive(Union, Clone)]
pub enum DataType {
    #[doc = "This represents an ACNET status reply. If a device request \
	     results in an error from the front-end, the data pool mananger \
	     will forward the status."]
    StatusReply(StatusReply),

    #[doc = "Represents a simple, scalar value. This is a scaled, floating \
	     point value."]
    Scalar(Scalar),

    #[doc = "Represents an array of scalar values. In EPICS, this would \
	     correspond to a \"waveform\" device."]
    ScalarArray(ScalarArray),

    #[doc = "This value is used to return the raw, binary data from the \
	     device reading."]
    Raw(Raw),

    #[doc = "Used for devices that return strings."]
    Text(Text),

    #[doc = "Used for devices that return arrays of strings."]
    TextArray(TextArray),

    #[doc = "Represents structured data. The value is a map type where the \
	     key is a string that represents a field name and the value is \
	     one of the values of this enumeration. This means you can nest \
	     `StructData` types to make arbitrarily complex types."]
    StructData(StructData),
}

#[doc = "This structure holds information associated with a device's reading, \
	 A \"reading\" is the latest value of any of a device's properties."]
#[derive(SimpleObject, Clone)]
#[graphql(complex)]
pub struct DataInfo {
    #[doc = "Timestamp representing when the data was sampled. This value is \
	     provided as seconds since 1970, UTC. The fractional portion of \
	     the value can represent nanoseconds, but we have few -- if any -- \
	     systems that provide that resolution."]
    pub timestamp: f64,

    #[doc = "The value of the device when sampled."]
    pub result: DataType,
}

#[ComplexObject]
impl DataInfo {
    #[doc = "The timestamp as an ISO formatted string. This value is fairly \
	     expensive to generate, so the `timestamp` field should be \
	     preferred to this one. This field is mainly used for debugging \
	     or when using a tool that returns human-readable results."]
    pub async fn iso_timestamp(&self) -> DateTime<Utc> {
        DateTime::<Utc>::UNIX_EPOCH
            + Duration::microseconds((self.timestamp * 1_000_000.0) as i64)
    }
}

#[doc = "This structure wraps a device's reading(s) with some routing \
	 information: a `refId` to correlate which device, in the array \
	 of devices passed, this reply is for. It also has a `cycle` \
	 field so that reading from different devices can correlate which \
	 cycle they correspond."]
#[derive(SimpleObject, Clone, Default)]
pub struct DataReply {
    #[doc = "This is an index to indicate which entry, in the passed array of \
	     DRF strings, this reply corresponds."]
    pub ref_id: i32,

    #[doc = "The returned data."]
    pub data: Vec<DataInfo>,
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

use crate::g_rpc::proto::common::device;

// Defining this trait allows us to convert a `DevValue` into a
// `proto::Data` type.

impl From<DevValue> for device::Value {
    fn from(val: DevValue) -> Self {
        match val {
            // TODO: Need to make an integer a valid device type.
            DevValue {
                int_val: Some(v),
                scalar_val: _,
                scalar_array_val: _,
                raw_val: _,
                text_val: _,
                text_array_val: _,
            } => device::Value {
                value: Some(device::value::Value::Scalar(v as f64)),
            },
            DevValue {
                int_val: None,
                scalar_val: Some(v),
                scalar_array_val: _,
                raw_val: _,
                text_val: _,
                text_array_val: _,
            } => device::Value {
                value: Some(device::value::Value::Scalar(v)),
            },
            DevValue {
                int_val: None,
                scalar_val: None,
                scalar_array_val: Some(v),
                raw_val: _,
                text_val: _,
                text_array_val: _,
            } => device::Value {
                value: Some(device::value::Value::ScalarArr(
                    device::value::ScalarArray { value: v },
                )),
            },
            DevValue {
                int_val: None,
                scalar_val: None,
                scalar_array_val: None,
                raw_val: Some(v),
                text_val: _,
                text_array_val: _,
            } => device::Value {
                value: Some(device::value::Value::Raw(v)),
            },
            DevValue {
                int_val: None,
                scalar_val: None,
                scalar_array_val: None,
                raw_val: None,
                text_val: Some(v),
                text_array_val: _,
            } => device::Value {
                value: Some(device::value::Value::Text(v)),
            },
            DevValue {
                int_val: None,
                scalar_val: None,
                scalar_array_val: None,
                raw_val: None,
                text_val: None,
                text_array_val: Some(v),
            } => device::Value {
                value: Some(device::value::Value::TextArr(
                    device::value::TextArray { value: v },
                )),
            },
            DevValue {
                int_val: None,
                scalar_val: None,
                scalar_array_val: None,
                raw_val: None,
                text_val: None,
                text_array_val: None,
            } => device::Value {
                value: Some(device::value::Value::Raw(vec![])),
            },
        }
    }
}

// Defining this trait allows us to convert a `device::Value` type into a
// `DataType`.

impl TryFrom<device::Value> for DataType {
    type Error = std::io::Error;

    fn try_from(val: device::Value) -> Result<Self, Self::Error> {
        match val.value {
            Some(device::value::Value::Scalar(v)) => {
                Ok(DataType::Scalar(Scalar { scalar_value: v }))
            }
            Some(device::value::Value::ScalarArr(v)) => {
                Ok(DataType::ScalarArray(ScalarArray {
                    scalar_array_value: v.value,
                }))
            }
            Some(device::value::Value::Text(v)) => {
                Ok(DataType::Text(Text { text_value: v }))
            }
            Some(device::value::Value::TextArr(v)) => {
                Ok(DataType::TextArray(TextArray {
                    text_array_value: v.value.clone(),
                }))
            }
            Some(_) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "received a device type we don't yet translate",
            )),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "received a device type that is not recognized",
            )),
        }
    }
}

impl TryFrom<&device::Value> for DataType {
    type Error = std::io::Error;

    fn try_from(val: &device::Value) -> Result<Self, Self::Error> {
        match &val.value {
            Some(device::value::Value::Scalar(v)) => {
                Ok(DataType::Scalar(Scalar { scalar_value: *v }))
            }
            Some(device::value::Value::ScalarArr(v)) => {
                Ok(DataType::ScalarArray(ScalarArray {
                    scalar_array_value: v.value.clone(),
                }))
            }
            Some(device::value::Value::Text(v)) => Ok(DataType::Text(Text {
                text_value: v.clone(),
            })),
            Some(device::value::Value::TextArr(v)) => {
                Ok(DataType::TextArray(TextArray {
                    text_array_value: v.value.clone(),
                }))
            }
            Some(_) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "received a device type we don't yet translate",
            )),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "received a device type that is not recognized",
            )),
        }
    }
}
