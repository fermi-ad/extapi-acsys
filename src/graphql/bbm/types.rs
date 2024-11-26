use async_graphql::{Enum, Object, SimpleObject};

#[doc = "Specifies a beamline."]
#[derive(Enum, Clone, Copy, PartialEq, Eq)]
pub enum Beamline {
    Booster,
    MainInjector,
    MiniBoone,
    Muon,
    Numi,
    Source,
    Switchyard,
}

pub struct BbmInfo;

#[doc = "Holds a set of beam budget data. Based on the query, this will \
	 hold one or more sets of data."]
#[Object]
impl BbmInfo {
    #[doc = "Contains all the information requested in the query."]
    pub async fn data(
        &self,
        #[graphql(
            desc = "If `null`, beam budget information for all beamlines \
		    will be returned. If not `null`, it must be a list of \
		    machines. The contents will contain information only \
		    for the specified machines."
        )]
        _which: Option<Vec<Beamline>>,
    ) -> Vec<BbmData> {
        vec![]
    }
}

pub struct BbmData {
    beamline: Beamline,
}

#[doc = "Holds budget information for a beamline. The query needs to specify \
	 the starting time and the number of 5-minute integrations to include \
	 in the result."]
#[Object]
impl BbmData {
    #[doc = "Indicates which beamline the data is associated."]
    async fn beamline(&self) -> Beamline {
        self.beamline
    }

    #[doc = "Contains the history of the associated machine. The array \
	     returned by this query will always have `nBins` entries. If \
	     there isn't data in a 5-minute window, that entry will be \
	     `null`. This helps the application differentiate between a \
	     zero reading and a lack of data (in case it wants to display \
	     the error.)"]
    async fn history(
        &self,
        #[graphql(
            desc = "Specifies the start time of the data. The time is given \
		    in seconds since the Unix epoch (UTC)."
        )]
        _start_time: i32,
        #[graphql(
            desc = "Indicates how many 5-minute integration samples to include."
        )]
        _n_bins: i32,
    ) -> Vec<Option<f64>> {
        vec![]
    }
}

#[doc = "Describes the configuration for a device that returns integrated \
	 beam infomation."]
#[derive(SimpleObject)]
pub struct BbmDeviceCfg {
    #[doc = "The name of the device."]
    pub name: String,
    #[doc = "The clock event on which the device should be read."]
    pub event: u8,
    #[doc = "The delay from the clock event (in case there's no clock event \
	     on the exact moment. Usually this parameter is 0."]
    #[graphql(default = 0)]
    pub delay: usize,
}
