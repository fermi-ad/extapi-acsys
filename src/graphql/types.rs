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
pub struct DataInfo {
    #[doc = "Timestamp representing when the data was sampled. This value is \
	     provided as milliseconds since 1970, UTC."]
    pub timestamp: DateTime<Utc>,

    #[doc = "The value of the device when sampled."]
    pub result: DataType,

    #[doc = "The device's index (in the device database.)"]
    pub di: i32,

    #[doc = "The name of the device."]
    pub name: String,
}

#[doc = "This structure wraps a device reading with some routing information: \
	 a `refId` to correlate which device, in the array of devices passed, \
	 this reply is for. It also has a `cycle` field so that reading from \
	 different devices can correlate which cycle they correspond."]
#[derive(SimpleObject, Clone)]
pub struct DataReply {
    #[doc = "This is an index to indicate which entry, in the passed array of \
	     DRF strings, this reply corresponds."]
    pub ref_id: i32,

    #[doc = "The cycle number in which the device was sampled. This can be \
	     used to correlate readings from several devices."]
    pub cycle: u64,

    #[doc = "The returned data."]
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

impl From<&proto::Data> for DataType {
    fn from(val: &proto::Data) -> Self {
        match &val.value {
            Some(proto::data::Value::Scalar(v)) => {
                DataType::Scalar(Scalar { scalar_value: *v })
            }
            Some(proto::data::Value::ScalarArr(v)) => {
                DataType::ScalarArray(ScalarArray {
                    scalar_array_value: v.value.clone(),
                })
            }
            Some(proto::data::Value::Status(v)) => {
                DataType::StatusReply(StatusReply { status: *v as i16 })
            }
            Some(proto::data::Value::Text(v)) => DataType::Text(Text {
                text_value: v.clone(),
            }),
            Some(proto::data::Value::TextArr(proto::data::TextArray {
                value: v,
            })) => DataType::TextArray(TextArray {
                text_array_value: v.clone(),
            }),
            Some(v) => {
                warn!("can't translate {:?}", &v);
                todo!()
            }
            _ => todo!(),
        }
    }
}
