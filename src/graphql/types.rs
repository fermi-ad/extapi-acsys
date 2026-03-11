use crate::g_rpc::proto::common::device;
use async_graphql::{ComplexObject, InputObject, SimpleObject, Union};
use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Deserializer};
use serde_json::{self, Value};
use std::collections::HashMap;

#[derive(Debug)]
pub struct AuthInfo {
    bearer_token: Option<String>,
}
impl AuthInfo {
    pub fn new(info: Option<String>) -> Self {
        AuthInfo {
            bearer_token: info.and_then(|v| {
                if let ["Bearer", token] =
                    v.split(' ').collect::<Vec<&str>>()[..]
                {
                    Some(token.to_string())
                } else {
                    None
                }
            }),
        }
    }

    #[cfg(test)]
    pub fn has_token(&self) -> bool {
        self.bearer_token.is_some()
    }

    pub fn token(&self) -> Option<String> {
        self.bearer_token.clone()
    }

    pub fn token_ref(&self) -> Option<&String> {
        self.bearer_token.as_ref()
    }

    pub fn unsafe_account(&self) -> Option<String> {
        self.bearer_token.as_ref().and_then(|token| {
            if let [_, body, _] = token.split('.').collect::<Vec<&str>>()[..] {
                if let Ok(json) = STANDARD_NO_PAD.decode(body) {
                    let result: Result<
                        HashMap<String, Value>,
                        serde_json::Error,
                    > = serde_json::from_slice(&json);

                    if let Ok(result) = result {
                        if let Some(Value::String(user)) =
                            result.get("preferred_username")
                        {
                            return Some(user.clone());
                        }
                    }
                }
            }
            None
        })
    }
}

#[doc = "Contains an informative message describing why a request resulted \
	 in an error."]
#[derive(SimpleObject, Debug, PartialEq)]
pub struct ErrorReply {
    pub message: String,
}

#[doc = "Contains an ACNET status code. The Data Pool Manager currently \
	 returns these status codes, but they may go away in the future \
	 since EPICS has its own set of error codes."]
#[derive(SimpleObject, Clone, Debug, PartialEq)]
pub struct StatusReply {
    pub status: i16,
}

#[doc = "Represents a simple, floating point value."]
#[derive(SimpleObject, Clone, Debug, PartialEq)]
pub struct Scalar {
    pub scalar_value: f32,
}

#[doc = "Represents an array of floating point values."]
#[derive(SimpleObject, Clone, Debug, PartialEq)]
pub struct ScalarArray {
    pub scalar_array_value: Vec<f32>,
}

#[doc = "Contains the raw, unscaled data returned by a device."]
#[derive(SimpleObject, Clone, Debug, PartialEq)]
pub struct Raw {
    pub raw_value: String,
}

#[doc = "Contains a textual value returned by a device."]
#[derive(SimpleObject, Clone, Debug, PartialEq)]
pub struct Text {
    pub text_value: String,
}

#[doc = "Represents an array of textual values."]
#[derive(SimpleObject, Clone, Debug, PartialEq)]
pub struct TextArray {
    pub text_array_value: Vec<String>,
}

#[doc = "Represents a generic return type. EPICS devices have a hierarchy \
	 and this return type can model those values. Note that the value \
	 associated with the key can be another `StructData`, so arbitrarily \
	 deep trees can be created."]
#[derive(SimpleObject, Clone, Debug, PartialEq)]
pub struct StructData {
    pub key: String,
    pub struct_value: Box<DataType>,
}

#[doc = "The control system supports several types and this entity can \
	 repesent any of them."]
#[derive(Union, Clone, Debug, PartialEq)]
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
#[derive(SimpleObject, Clone, Debug, PartialEq)]
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
#[derive(SimpleObject, Clone, Default, Debug, PartialEq)]
pub struct DataReply {
    #[doc = "This is an index to indicate which entry, in the passed array of \
	     DRF strings, this reply corresponds."]
    pub ref_id: i32,

    #[doc = "The returned data."]
    pub data: Vec<DataInfo>,
}

fn deserialize_base64<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    STANDARD_NO_PAD.decode(s).map_err(serde::de::Error::custom)
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub enum DevValue {
    Integer(i32),
    Scalar(f64),
    ScalarArray(Vec<f64>),
    #[serde(deserialize_with = "deserialize_base64")]
    Raw(Vec<u8>),
    Text(String),
    TextArray(Vec<String>),
}

// --------------------------------------------------------------------------
// This section defines some useful traits for types in this module.

// Defining this trait allows us to convert a `DevValue` into a
// `proto::Data` type.

impl From<DevValue> for device::Value {
    fn from(val: DevValue) -> Self {
        match val {
            DevValue::Integer(v) => device::Value {
                value: Some(device::value::Value::Scalar(v as f64)),
            },
            DevValue::Scalar(v) => device::Value {
                value: Some(device::value::Value::Scalar(v)),
            },
            DevValue::ScalarArray(v) => device::Value {
                value: Some(device::value::Value::ScalarArr(
                    device::value::ScalarArray { value: v },
                )),
            },
            DevValue::Raw(v) => device::Value {
                value: Some(device::value::Value::Raw(v)),
            },
            DevValue::Text(v) => device::Value {
                value: Some(device::value::Value::Text(v)),
            },
            DevValue::TextArray(v) => device::Value {
                value: Some(device::value::Value::TextArr(
                    device::value::TextArray { value: v },
                )),
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
                Ok(DataType::Scalar(Scalar {
                    scalar_value: v as f32,
                }))
            }
            Some(device::value::Value::ScalarArr(v)) => {
                Ok(DataType::ScalarArray(ScalarArray {
                    scalar_array_value: v
                        .value
                        .into_iter()
                        .map(|v| v as f32)
                        .collect(),
                }))
            }
            Some(device::value::Value::Raw(v)) => Ok(DataType::Raw(Raw {
                raw_value: STANDARD_NO_PAD.encode(v),
            })),
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
                Ok(DataType::Scalar(Scalar {
                    scalar_value: *v as f32,
                }))
            }
            Some(device::value::Value::ScalarArr(v)) => {
                Ok(DataType::ScalarArray(ScalarArray {
                    scalar_array_value: v
                        .value
                        .iter()
                        .map(|v| *v as f32)
                        .collect(),
                }))
            }
            Some(device::value::Value::Raw(v)) => Ok(DataType::Raw(Raw {
                raw_value: STANDARD_NO_PAD.encode(v),
            })),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::g_rpc::proto::common::device;

    #[test]
    fn test_dev_value_deserialization() {
        // Test Integer
        let json_int = "123";
        let expected_int = DevValue::Integer(123);
        let deserialized_int: DevValue =
            serde_json::from_str(json_int).unwrap();
        assert_eq!(deserialized_int, expected_int);

        // Test Scalar
        let json_scalar = "123.45";
        let expected_scalar = DevValue::Scalar(123.45);
        let deserialized_scalar: DevValue =
            serde_json::from_str(json_scalar).unwrap();
        assert_eq!(deserialized_scalar, expected_scalar);

        // Test ScalarArray
        let json_scalar_array = "[1.0, 2.5, -3.0]";
        let expected_scalar_array = DevValue::ScalarArray(vec![1.0, 2.5, -3.0]);
        let deserialized_scalar_array: DevValue =
            serde_json::from_str(json_scalar_array).unwrap();
        assert_eq!(deserialized_scalar_array, expected_scalar_array);

        // Test Raw (Valid Base64)
        let json_raw = "\"AQID\"";
        let expected_raw = DevValue::Raw(vec![1, 2, 3]);
        let deserialized_raw: DevValue =
            serde_json::from_str(json_raw).unwrap();
        assert_eq!(deserialized_raw, expected_raw);

        // Test Text
        let json_text = "\"hello world\"";
        let expected_text = DevValue::Text("hello world".to_string());
        let deserialized_text: DevValue =
            serde_json::from_str(json_text).unwrap();
        assert_eq!(deserialized_text, expected_text);

        // Test TextArray
        let json_text_array = "[\"hello\", \"world\"]";
        let expected_text_array =
            DevValue::TextArray(vec!["hello".to_string(), "world".to_string()]);
        let deserialized_text_array: DevValue =
            serde_json::from_str(json_text_array).unwrap();
        assert_eq!(deserialized_text_array, expected_text_array);

        // Test that a JSON array of integers deserializes to ScalarArray.
        let json_int_array = "[1, 2, 3]";
        let expected_as_scalar_array =
            DevValue::ScalarArray(vec![1.0, 2.0, 3.0]);
        let deserialized_int_array: DevValue =
            serde_json::from_str(json_int_array).unwrap();
        assert_eq!(deserialized_int_array, expected_as_scalar_array);
    }

    #[test]
    fn test_auth_info() {
        // Test no token
        let auth_none = AuthInfo::new(None);
        assert!(!auth_none.has_token());
        assert_eq!(auth_none.unsafe_account(), None);

        // Test non-bearer token
        let auth_basic = AuthInfo::new(Some("Basic some_token".to_string()));
        assert!(!auth_basic.has_token());
        assert_eq!(auth_basic.unsafe_account(), None);

        // Test valid bearer token but malformed JWT (not 3 parts)
        let auth_jwt_malformed =
            AuthInfo::new(Some("Bearer malformed.jwt".to_string()));
        assert!(auth_jwt_malformed.has_token());
        assert_eq!(auth_jwt_malformed.unsafe_account(), None);

        // Test valid bearer token, valid JWT structure, but bad base64
        let auth_bad_b64 =
            AuthInfo::new(Some("Bearer header.bad-base64.sig".to_string()));
        assert!(auth_bad_b64.has_token());
        assert_eq!(auth_bad_b64.unsafe_account(), None);

        // Test happy path
        // `{"preferred_username": "testuser"}` -> b64 is `eyJwcmVmZXJyZWRfdXNlcm5hbWUiOiJ0ZXN0dXNlciJ9`
        let jwt_body = "eyJwcmVmZXJyZWRfdXNlcm5hbWUiOiJ0ZXN0dXNlciJ9";
        let token = format!("Bearer header.{}.sig", jwt_body);
        let auth_ok = AuthInfo::new(Some(token));
        assert!(auth_ok.has_token());
        assert_eq!(auth_ok.unsafe_account(), Some("testuser".to_string()));
    }

    #[test]
    fn test_datatype_conversion() {
        // Test Scalar to f32
        let dev_val_scalar = device::Value {
            value: Some(device::value::Value::Scalar(123.456)),
        };
        let data_type: DataType = dev_val_scalar.try_into().unwrap();
        assert_eq!(
            data_type,
            DataType::Scalar(Scalar {
                scalar_value: 123.456_f32
            })
        );

        // Test ScalarArray to Vec<f32>
        let dev_val_scalar_arr = device::Value {
            value: Some(device::value::Value::ScalarArr(
                device::value::ScalarArray {
                    value: vec![1.0, 2.0, 3.0],
                },
            )),
        };
        let data_type: DataType = dev_val_scalar_arr.try_into().unwrap();
        assert_eq!(
            data_type,
            DataType::ScalarArray(ScalarArray {
                scalar_array_value: vec![1.0_f32, 2.0_f32, 3.0_f32]
            })
        );

        // Test Raw to base64 String
        let dev_val_raw = device::Value {
            value: Some(device::value::Value::Raw(vec![1, 2, 3, 255])),
        };
        let data_type: DataType = dev_val_raw.try_into().unwrap();
        assert_eq!(
            data_type,
            DataType::Raw(Raw {
                raw_value: "AQID/w".to_string()
            })
        );
    }
}
