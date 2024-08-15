use async_graphql::*;

// Pull in our local types.

pub mod types;

#[derive(Default)]
pub struct BBMQueries;

// Define the schema's query entry points. Any methods defined in this
// section will appear in the schema.

#[Object]
impl BBMQueries {
    #[doc = "Retrieves beam budget information."]
    async fn get_beam_budget_info(&self) -> types::BeamBudgetData {
        types::BeamBudgetData {}
    }
}
