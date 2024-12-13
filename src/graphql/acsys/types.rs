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
