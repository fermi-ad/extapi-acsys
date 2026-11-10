use super::global;
use crate::g_rpc::proto::services::clock_event::EventInfo;

use async_graphql::*;
use chrono::{DateTime, Duration, Utc};
use std::sync::Arc;

#[derive(SimpleObject, Clone)]
pub struct PlotConfig {
    pub config_id: usize,
    pub config_name: Arc<str>,
    pub config: Arc<str>,
}

#[derive(SimpleObject, Clone)]
pub struct PlotChannelData {
    #[doc = "The engineering units of the device."]
    pub channel_units: String,
    #[doc = "The negotiated return rate for the data. If a device's readings \
	     are requested at a higher rate than the device can support, the \
	     front-end will negotiate down to an acheivable rate. This field \
	     represents the actual sample rate of the data."]
    pub channel_rate: String,
    pub status_string: Option<String>,
    #[doc = "The global status of the reading. This field will either be `0` \
	     (successful reads) or a negative status, indicating a fatal error \
	     occurred trying to get the device's data."]
    pub channel_status: i16,
    #[doc = "A set of data points. If the return rate is slow (<= 1Hz), this \
	     list will only have one element."]
    pub channel_data: Vec<global::DataInfo>,
}

#[doc = "Contains plot data for a given plot request."]
#[derive(SimpleObject, Clone)]
#[graphql(complex)]
pub struct PlotReplyData {
    #[doc = "A unique identifier for the plot request. This identifier will \
	     be cached for a limited time. Other clients can specify it to \
	     re-use the configuration."]
    pub plot_id: String,
    #[doc = "The time of the original request."]
    pub timestamp: f64,
    #[doc = "If requesting a triggered plot, this will be the timestamp of \
	     the last clock event (i.e. the \"trigger\"). All the timestamps \
	     in the data will be relative to this timestamp."]
    pub trigger_timestamp: Option<f64>,
    #[doc = "The latest set of data points for the plot. Depending on the \
	     sample rate or how much history is requested, this array will \
	     contain a chunk of data."]
    pub data: Vec<PlotChannelData>,
}

#[ComplexObject]
impl PlotReplyData {
    #[doc = "The time of the original request in ISO format."]
    pub async fn iso_timestamp(&self) -> DateTime<Utc> {
        DateTime::<Utc>::UNIX_EPOCH
            + Duration::microseconds((self.timestamp * 1_000_000.0) as i64)
    }
}

#[doc = "Information about a clock event."]
#[derive(SimpleObject, Clone)]
#[graphql(complex)]
pub struct AClkEvent {
    #[doc = "The absolute timestamp (UTC) of the event."]
    pub timestamp: f64,
    #[doc = "The event code."]
    pub event: u16,
    #[doc = "Extra event-specific data"]
    pub data: u64,
    #[doc = "Description of the event."]
    pub description: Arc<str>,
}

#[ComplexObject]
impl AClkEvent {
    #[doc = "Timestamp of the event in ISO format."]
    pub async fn iso_timestamp(&self) -> DateTime<Utc> {
        DateTime::<Utc>::UNIX_EPOCH
            + Duration::microseconds((self.timestamp * 1_000_000.0) as i64)
    }
}

// Convert from EventInfo to AClkEvent
//
// XXX: This trait should go away and a custom conversion method used instead.
// The description is going to be obtained through a different gRPC call so it
// should be used when making these conversions.

impl From<EventInfo> for AClkEvent {
    fn from(e: EventInfo) -> Self {
        Self {
            timestamp: e
                .stamp
                .map_or(0.0, |s| s.seconds as f64 + s.nanos as f64 * 1e-9),
            event: e.event as u16,
            data: 0,
            description: "".into(),
        }
    }
}
