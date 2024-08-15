use async_graphql::{Enum, Object, SimpleObject};
use chrono::{DateTime, Utc};

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

#[doc = "Holds a set of beam budget data. Based on the query, this will hold one or more sets of data."]
pub struct BeamBudgetData;

#[Object]
impl BeamBudgetData {
    #[doc = "Contains all the information requested in the query."]
    pub async fn info(
        &self,
        #[graphql(
            desc = "If `null`, beam budget information for all beamlines will be returned. If not `null`, it must be a list of machines. The contents will contain information only for the specified machines."
        )]
        _which: Option<Vec<Beamline>>,
    ) -> Vec<BudgetInfo> {
        vec![]
    }
}

#[doc = "Holds budget information for a machine."]
pub struct BudgetInfo {
    beamline: Beamline,
}

#[Object]
impl BudgetInfo {
    #[doc = "Indicates which machine the data is associated."]
    async fn beamline(&self) -> Beamline {
        self.beamline
    }

    #[doc = "Contains the history of the  associated machine."]
    async fn history(
        &self,
        #[graphql(
            desc = "Specifies which run's budget to return. If `null`, the latest run's totals are returned."
        )]
        _run: Option<i32>,
    ) -> Vec<BeamHistory> {
        vec![]
    }
}

#[derive(SimpleObject)]
pub struct BeamHistory {
    #[doc = "The sample time."]
    pub timestamp: DateTime<Utc>,
    #[doc = "The total measured beam up to the sample time."]
    pub total: f64,
    #[doc = "The budgeted beam up to the sample time."]
    pub budget: f64,
}
