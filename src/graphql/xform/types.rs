use crate::graphql::types::Scalar;
use async_graphql::*;
use chrono::*;

#[derive(SimpleObject)]
pub struct XFormResult {
    #[doc = "Timestamp representing when the data was sampled. This value is \
	     provided as milliseconds since 1970, UTC."]
    pub timestamp: DateTime<Utc>,

    #[doc = "The value of the device when sampled."]
    pub result: Scalar,
}

#[derive(InputObject, Debug)]
pub struct XFormDeviceExpr {
    pub device: String,
}

#[derive(InputObject, Debug)]
pub struct XFormAvgExpr {
    pub expr: Box<XFormExpr>,
    pub n: u32,
}

#[derive(InputObject, Debug)]
pub struct XFormExpr {
    pub dev_ex: Option<XFormDeviceExpr>,
    pub avg_ex: Option<XFormAvgExpr>,
}

impl std::fmt::Display for XFormExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            XFormExpr {
                dev_ex: Some(XFormDeviceExpr { device }),
                avg_ex: None,
            } => write!(f, "{}", device),
            XFormExpr {
                dev_ex: None,
                avg_ex: Some(XFormAvgExpr { expr, n }),
            } => write!(f, "AVG({}, {})", &expr, &n),
            _ => write!(f, "** BAD COMPONENT: '{:?}' **", self),
        }
    }
}

#[derive(InputObject)]
pub struct XFormRequest {
    pub event: String,
    pub expr: XFormExpr,
}
