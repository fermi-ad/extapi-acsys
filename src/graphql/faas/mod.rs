use async_graphql::*;

#[derive(Default)]
pub struct FaasQueries;

// Define the schema's query entry points. Any methods defined in this
// section will appear in the schema.

#[doc = "These queries are used to access our \"Functions as a Service\" \
	 services."]
#[Object]
impl FaasQueries {
    async fn clinks_to_unix(&self, clinks: u64) -> u64 {
        todo!()
    }

    async fn unix_to_clinks(&self, time: u64) -> u64 {
        todo!()
    }
}
