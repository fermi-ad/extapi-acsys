use async_graphql::*;

#[derive(SimpleObject)]
pub struct PlotDataPoint {
    pub x: f64,
    pub y: f64,
}

#[derive(SimpleObject)]
pub struct PlotChannelData {
    pub channel_units: String,

    pub channel_status: i16,

    pub channel_data: Vec<PlotDataPoint>,
}

/// Contains plot data for a given plot request.
#[derive(SimpleObject)]
pub struct PlotReplyData {
    #[doc = "A unique identifier for the plot request. This identifier will \
	     be cached for a limited time. Other clients can specify it to \
	     re-use the configuration."]
    pub plot_id: String,

    pub data: Vec<PlotChannelData>,
}

/// Holds the configuration for a plot channel.
#[derive(InputObject, SimpleObject, Debug, Clone)]
#[graphql(input_name = "ChannelSettingSnapshotIn")]
pub struct ChannelSettingSnapshot {
    pub device: String,
    pub line_color: Option<u32>,
    pub marker_index: Option<u32>,
}

#[derive(InputObject, SimpleObject, Debug, Clone)]
#[graphql(input_name = "PlotConfigurationSnapshotIn")]
pub struct PlotConfigurationSnapshot {
    #[doc = "Unique identifier for the plot configuration"]
    pub configuration_id: Option<usize>,
    pub configuration_name: String,
    pub channels: Vec<ChannelSettingSnapshot>,
    pub x_min: Option<f64>,
    pub x_max: Option<f64>,
    pub y_min: Option<f64>,
    pub y_max: Option<f64>,
    pub is_show_labels: bool,
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
}
