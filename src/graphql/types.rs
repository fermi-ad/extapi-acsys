use crate::g_rpc::proto::common::device;
use async_graphql::{
    ComplexObject, InputObject, Scalar, ScalarType, SimpleObject, Union,
};
use base64::{Engine, engine::general_purpose::STANDARD_NO_PAD};
use chrono::{DateTime, Duration, Utc};
use serde_json::{self, Value};

#[derive(Debug)]
pub struct AuthInfo {
    bearer_token: Option<String>,
}

#[doc = "A signed or unsigned 64-bit integer. Values are serialized as strings \
         so JavaScript clients do not lose precision."]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BigInt {
    Int(i64),
    Uint(u64),
}

impl std::fmt::Display for BigInt {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Int(value) => value.fmt(formatter),
            Self::Uint(value) => value.fmt(formatter),
        }
    }
}

#[Scalar]
impl ScalarType for BigInt {
    fn parse(
        value: async_graphql::Value,
    ) -> async_graphql::InputValueResult<Self> {
        match value {
            async_graphql::Value::String(value) => value
                .parse::<i64>()
                .map(Self::Int)
                .or_else(|_| value.parse::<u64>().map(Self::Uint))
                .map_err(|_| {
                    async_graphql::InputValueError::custom(
                        "Expected a signed or unsigned 64-bit integer",
                    )
                }),
            async_graphql::Value::Number(value) => {
                value.as_i64().map(Self::Int).ok_or_else(|| {
                    async_graphql::InputValueError::custom(
                        "Expected an integer within the signed 64-bit range; \
                         use a string for larger unsigned values",
                    )
                })
            }
            value => Err(async_graphql::InputValueError::expected_type(value)),
        }
    }

    fn to_value(&self) -> async_graphql::Value {
        async_graphql::Value::String(self.to_string())
    }
}

#[doc = "Represents a signed or unsigned 64-bit integer value."]
#[derive(SimpleObject, Clone, Debug, PartialEq)]
pub struct BigIntScalar {
    pub int_value: BigInt,
}

#[doc = "Represents an array of signed or unsigned 64-bit integer values."]
#[derive(SimpleObject, Clone, Debug, PartialEq)]
pub struct BigIntArray {
    pub big_int_array_value: Vec<BigInt>,
}

impl AuthInfo {
    pub fn new(info: Option<String>) -> Self {
        AuthInfo {
            bearer_token: info
                .and_then(|v| v.strip_prefix("Bearer ").map(String::from)),
        }
    }

    #[cfg(test)]
    pub fn has_token(&self) -> bool {
        self.bearer_token.is_some()
    }

    pub fn token(&self) -> Option<String> {
        self.bearer_token.clone()
    }

    pub fn unsafe_account(&self) -> Option<String> {
        self.bearer_token.as_deref().and_then(|token| {
            let body = token.split('.').nth(1)?;
            let json = STANDARD_NO_PAD.decode(body).ok()?;
            let result: Value = serde_json::from_slice(&json).ok()?;

            result
                .get("preferred_username")
                .and_then(Value::as_str)
                .map(String::from)
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
    pub scalar_value: f64,
}

#[doc = "Represents an array of floating point values."]
#[derive(SimpleObject, Clone, Debug, PartialEq)]
pub struct ScalarArray {
    pub scalar_array_value: Vec<f64>,
}

#[doc = "Contains the raw, unscaled data returned by a device."]
#[derive(SimpleObject, Clone, Debug, PartialEq)]
pub struct Raw {
    pub raw_value: Vec<u8>,
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

    #[doc = "Represents a signed or unsigned 64-bit integer value."]
    BigInt(BigIntScalar),

    #[doc = "Represents an array of signed or unsigned 64-bit integer values."]
    BigIntArray(BigIntArray),
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

#[derive(InputObject)]
pub struct DevValue {
    pub int_val: Option<i32>,
    pub scalar_val: Option<f64>,
    pub scalar_array_val: Option<Vec<f64>>,
    pub raw_val: Option<Vec<u8>>,
    pub text_val: Option<String>,
    pub text_array_val: Option<Vec<String>>,
    pub big_int_val: Option<BigInt>,
}

// --------------------------------------------------------------------------
// This section defines some useful traits for types in this module.

// Defining this trait allows us to convert a `DevValue` into a
// `proto::Data` type.

impl From<DevValue> for device::Value {
    #[inline(never)]
    fn from(val: DevValue) -> Self {
        match val {
            // Keep the legacy `intVal` behavior for existing clients.
            DevValue {
                int_val: Some(v),
                scalar_val: _,
                scalar_array_val: _,
                raw_val: _,
                text_val: _,
                text_array_val: _,
                big_int_val: _,
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
                big_int_val: _,
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
                big_int_val: _,
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
                big_int_val: _,
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
                big_int_val: _,
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
                big_int_val: _,
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
                big_int_val: Some(v),
            } => device::Value {
                value: Some(match v {
                    BigInt::Int(v) => device::value::Value::Int(v),
                    BigInt::Uint(v) => device::value::Value::Uint(v),
                }),
            },
            DevValue {
                int_val: None,
                scalar_val: None,
                scalar_array_val: None,
                raw_val: None,
                text_val: None,
                text_array_val: None,
                big_int_val: None,
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

    #[inline(never)]
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
            Some(device::value::Value::Raw(v)) => {
                Ok(DataType::Raw(Raw { raw_value: v }))
            }
            Some(device::value::Value::Text(v)) => {
                Ok(DataType::Text(Text { text_value: v }))
            }
            Some(device::value::Value::TextArr(v)) => {
                Ok(DataType::TextArray(TextArray {
                    text_array_value: v.value,
                }))
            }
            Some(device::value::Value::Int(v)) => {
                Ok(DataType::BigInt(BigIntScalar {
                    int_value: BigInt::Int(v),
                }))
            }
            Some(device::value::Value::Uint(v)) => {
                Ok(DataType::BigInt(BigIntScalar {
                    int_value: BigInt::Uint(v),
                }))
            }
            Some(device::value::Value::IntArr(v)) => {
                Ok(DataType::BigIntArray(BigIntArray {
                    big_int_array_value: v
                        .value
                        .into_iter()
                        .map(BigInt::Int)
                        .collect(),
                }))
            }
            Some(device::value::Value::UintArr(v)) => {
                Ok(DataType::BigIntArray(BigIntArray {
                    big_int_array_value: v
                        .value
                        .into_iter()
                        .map(BigInt::Uint)
                        .collect(),
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

    #[test]
    fn big_int_serializes_signed_and_unsigned_values_as_strings() {
        assert_eq!(
            BigInt::Int(i64::MIN).to_value(),
            async_graphql::Value::String(i64::MIN.to_string()),
        );
        assert_eq!(
            BigInt::Uint(u64::MAX).to_value(),
            async_graphql::Value::String(u64::MAX.to_string()),
        );
    }

    #[test]
    fn big_int_parses_string_and_numeric_integers() {
        assert_eq!(
            BigInt::parse(async_graphql::Value::String(i64::MIN.to_string()))
                .unwrap(),
            BigInt::Int(i64::MIN),
        );
        assert_eq!(
            BigInt::parse(async_graphql::Value::String(u64::MAX.to_string()))
                .unwrap(),
            BigInt::Uint(u64::MAX),
        );
        assert_eq!(
            BigInt::parse(async_graphql::Value::Number(42.into())).unwrap(),
            BigInt::Int(42),
        );
    }

    #[test]
    fn big_int_rejects_invalid_or_out_of_range_values() {
        assert!(
            BigInt::parse(async_graphql::Value::String(
                "18446744073709551616".to_string(),
            ))
            .is_err()
        );
        assert!(
            BigInt::parse(async_graphql::Value::String("1.5".to_string()))
                .is_err()
        );
        assert!(
            BigInt::parse(async_graphql::Value::Number(u64::MAX.into()))
                .is_err()
        );
    }

    #[test]
    fn device_signed_integer_converts_to_big_int() {
        let value = device::Value {
            value: Some(device::value::Value::Int(i64::MIN)),
        };

        assert_eq!(
            DataType::try_from(value).unwrap(),
            DataType::BigInt(BigIntScalar {
                int_value: BigInt::Int(i64::MIN),
            }),
        );
    }

    #[test]
    fn device_unsigned_integer_converts_to_big_int() {
        let value = device::Value {
            value: Some(device::value::Value::Uint(u64::MAX)),
        };

        assert_eq!(
            DataType::try_from(value).unwrap(),
            DataType::BigInt(BigIntScalar {
                int_value: BigInt::Uint(u64::MAX),
            }),
        );
    }

    #[test]
    fn device_integer_arrays_convert_to_big_int_array() {
        let signed = device::Value {
            value: Some(device::value::Value::IntArr(
                device::value::Int64Array {
                    value: vec![i64::MIN, i64::MAX],
                },
            )),
        };
        let unsigned = device::Value {
            value: Some(device::value::Value::UintArr(
                device::value::Uint64Array {
                    value: vec![0, u64::MAX],
                },
            )),
        };

        assert_eq!(
            DataType::try_from(signed).unwrap(),
            DataType::BigIntArray(BigIntArray {
                big_int_array_value: vec![
                    BigInt::Int(i64::MIN),
                    BigInt::Int(i64::MAX),
                ],
            }),
        );
        assert_eq!(
            DataType::try_from(unsigned).unwrap(),
            DataType::BigIntArray(BigIntArray {
                big_int_array_value: vec![
                    BigInt::Uint(0),
                    BigInt::Uint(u64::MAX),
                ],
            }),
        );
    }

    #[test]
    fn big_int_input_converts_without_floating_point_precision_loss() {
        let signed = DevValue {
            int_val: None,
            scalar_val: None,
            scalar_array_val: None,
            raw_val: None,
            text_val: None,
            text_array_val: None,
            big_int_val: Some(BigInt::Int(i64::MIN)),
        };
        let unsigned = DevValue {
            int_val: None,
            scalar_val: None,
            scalar_array_val: None,
            raw_val: None,
            text_val: None,
            text_array_val: None,
            big_int_val: Some(BigInt::Uint(9_007_199_254_740_993)),
        };

        assert_eq!(
            device::Value::from(signed).value,
            Some(device::value::Value::Int(i64::MIN)),
        );
        assert_eq!(
            device::Value::from(unsigned).value,
            Some(device::value::Value::Uint(9_007_199_254_740_993)),
        );
    }

    struct CompatibilityQuery;

    #[async_graphql::Object]
    impl CompatibilityQuery {
        async fn data(&self) -> DataInfo {
            DataInfo {
                timestamp: 1.5,
                result: DataType::Scalar(Scalar { scalar_value: 42.5 }),
            }
        }

        async fn big_int_data(&self) -> DataInfo {
            DataInfo {
                timestamp: 1.5,
                result: DataType::BigInt(BigIntScalar {
                    int_value: BigInt::Uint(u64::MAX),
                }),
            }
        }

        async fn big_int_array_data(&self) -> DataInfo {
            DataInfo {
                timestamp: 1.5,
                result: DataType::BigIntArray(BigIntArray {
                    big_int_array_value: vec![
                        BigInt::Int(i64::MIN),
                        BigInt::Uint(u64::MAX),
                    ],
                }),
            }
        }

        async fn echo_big_int(&self, value: BigInt) -> BigInt {
            value
        }
    }

    #[tokio::test]
    async fn existing_scalar_and_timestamp_behavior_is_unchanged() {
        let schema = async_graphql::Schema::new(
            CompatibilityQuery,
            async_graphql::EmptyMutation,
            async_graphql::EmptySubscription,
        );
        let response = schema
            .execute(
                "{ data { timestamp isoTimestamp result { \
                 ... on Scalar { scalarValue } } } }",
            )
            .await;

        assert!(response.errors.is_empty(), "{:?}", response.errors);
        assert_eq!(
            response.data,
            async_graphql::value!({
                "data": {
                    "timestamp": 1.5,
                    "isoTimestamp": "1970-01-01T00:00:01.500+00:00",
                    "result": { "scalarValue": 42.5 },
                }
            }),
        );
    }

    #[tokio::test]
    async fn graphql_big_int_input_accepts_string_and_numeric_integers() {
        let schema = async_graphql::Schema::new(
            CompatibilityQuery,
            async_graphql::EmptyMutation,
            async_graphql::EmptySubscription,
        );
        let response = schema
            .execute("{ string: echoBigInt(value: \"18446744073709551615\") number: echoBigInt(value: 42) }")
            .await;

        assert!(response.errors.is_empty(), "{:?}", response.errors);
        assert_eq!(
            response.data,
            async_graphql::value!({
                "string": u64::MAX.to_string(),
                "number": "42",
            }),
        );
    }

    #[tokio::test]
    async fn graphql_big_int_values_are_returned_as_strings() {
        let schema = async_graphql::Schema::new(
            CompatibilityQuery,
            async_graphql::EmptyMutation,
            async_graphql::EmptySubscription,
        );
        let response = schema
            .execute(
                "{ bigIntData { result { \
                 ... on BigIntScalar { intValue } } } \
                 bigIntArrayData { result { \
                 ... on BigIntArray { bigIntArrayValue } } } }",
            )
            .await;

        assert!(response.errors.is_empty(), "{:?}", response.errors);
        assert_eq!(
            response.data,
            async_graphql::value!({
                "bigIntData": {
                    "result": { "intValue": u64::MAX.to_string() },
                },
                "bigIntArrayData": {
                    "result": {
                        "bigIntArrayValue": [
                            i64::MIN.to_string(),
                            u64::MAX.to_string(),
                        ],
                    },
                },
            }),
        );
    }
}
