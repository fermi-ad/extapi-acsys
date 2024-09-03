use async_graphql::*;

// Pull in our local types.

pub mod types;

#[derive(Default)]
pub struct BbmQueries;

// Define the schema's query entry points. Any methods defined in this
// section will appear in the schema.

#[doc = "Fermilab tracks the amount of beam transferred through various beamlines. There is a limit that can be transmitted in order to control the amount of radiation that is generated. These queries return information related to the Beam Budget monitoring systems."]
#[Object]
impl BbmQueries {
    #[doc = "Retrieves beam budget information."]
    async fn bbm_budget_info(&self) -> types::BbmInfo {
        types::BbmInfo {}
    }

    #[doc = "Returns device configuration for a specified beamline."]
    async fn bbm_beamline_config(
        &self, _beamline: types::Beamline,
    ) -> Vec<types::BbmDeviceCfg> {
        vec![]
    }
}
