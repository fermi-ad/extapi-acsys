use super::global;
use async_graphql::*;
use chrono::{DateTime, Duration, Utc};

#[derive(SimpleObject, Clone)]
pub struct PlotChannelData {
    #[doc = "The engineering units of the device."]
    pub channel_units: String,
    #[doc = "The negotiated return rate for the data. If a device's readings \
	     are requested at a higher rate than the device can support, the \
	     front-end will negotiate down to an acheivable rate. This field \
	     represents the actual sample rate of the data."]
    pub channel_rate: String,
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
    pub async fn iso_timestamp(&self) -> DateTime<Utc> {
        DateTime::<Utc>::UNIX_EPOCH
            + Duration::microseconds((self.timestamp * 1_000_000.0) as i64)
    }
}

#[doc = "Holds the configuration for a plot channel."]
#[derive(InputObject, SimpleObject, Debug, Clone)]
#[graphql(input_name = "ChannelSettingSnapshotIn")]
pub struct ChannelSettingSnapshot {
    pub device: String,
    pub y_min: Option<f64>,
    pub y_max: Option<f64>,
    pub line_color: Option<u32>,
    pub marker_index: Option<u32>,
}

#[derive(InputObject, SimpleObject, Debug, Clone, Default)]
#[graphql(input_name = "PlotConfigurationSnapshotIn")]
pub struct PlotConfigurationSnapshot {
    #[doc = "Unique identifier for the plot configuration"]
    pub configuration_id: Option<usize>,
    pub configuration_name: String,
    pub channels: Vec<ChannelSettingSnapshot>,
    pub x_min: Option<f64>,
    pub x_max: Option<f64>,
    pub start_time: Option<f64>,
    pub end_time: Option<f64>,
    pub time_delta: Option<f64>,
    pub is_scalar: bool,
    pub is_one_shot: bool,
    pub is_show_labels: bool,
    pub is_persistent: bool,
    pub is_blink: bool,
    pub data_limit: usize,
    #[doc = "If `triggerEvent` is null, this parameter specifies the \
	     delay, in milliseconds, between points in a waveform. If a \
	     trigger event is specified, then this specifies the delay \
	     after the event when the signal should be sampled. If this \
	     parameter is null, then there will be no delay after a trigger \
	     event or a 1 Hz sample rate will be used."]
    pub update_delay: Option<usize>,
    #[doc = "The number of waveforms to return. If omitted, the service \
	     will return waveforms until the client cancels the subscription."]
    pub n_acquisitions: Option<usize>,
    pub tclk_event: Option<u8>,
    pub sample_on_event: Option<String>,
    pub x_axis: Option<String>,
}
