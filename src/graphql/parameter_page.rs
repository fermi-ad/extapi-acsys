use std::sync::Arc;

use async_graphql::{Context, Error, ErrorExtensions, ID, Object, Result};

mod store;
mod types;

#[cfg(test)]
mod tests;

pub(super) use store::{InMemoryParameterPageStore, ParameterPageStore};
use types::{ParameterPage, ParameterPageInput, ParameterPageSummary};

pub(super) struct ParameterPageQueries;

#[Object]
impl ParameterPageQueries {
    /// Lists page IDs and titles in title order.
    async fn parameter_pages(
        &self, ctx: &Context<'_>,
    ) -> Result<Vec<ParameterPageSummary>> {
        ctx.data::<Arc<dyn ParameterPageStore>>()?.list().await
    }

    /// Retrieves a parameter page by ID.
    async fn parameter_page(
        &self, ctx: &Context<'_>, id: ID,
    ) -> Result<Option<ParameterPage>> {
        ctx.data::<Arc<dyn ParameterPageStore>>()?.fetch(id).await
    }
}

pub(super) struct ParameterPageMutations;

#[Object]
impl ParameterPageMutations {
    /// Saves a new page with a server-assigned ID and revision.
    async fn create_parameter_page(
        &self, ctx: &Context<'_>, input: ParameterPageInput,
    ) -> Result<ParameterPage> {
        input.validate()?;
        ctx.data::<Arc<dyn ParameterPageStore>>()?
            .create(input)
            .await
    }

    /// Replaces the page title and contents if the revision still matches.
    async fn update_parameter_page(
        &self, ctx: &Context<'_>, id: ID, expected_revision: String,
        input: ParameterPageInput,
    ) -> Result<ParameterPage> {
        input.validate()?;
        ctx.data::<Arc<dyn ParameterPageStore>>()?
            .update(id, &expected_revision, input)
            .await
    }

    /// Deletes the page if the revision still matches and returns its ID.
    async fn delete_parameter_page(
        &self, ctx: &Context<'_>, id: ID, expected_revision: String,
    ) -> Result<ID> {
        ctx.data::<Arc<dyn ParameterPageStore>>()?
            .delete(id, &expected_revision)
            .await
    }
}

fn page_error(code: &str, message: &str) -> Error {
    Error::new(message)
        .extend_with(|_, extensions| extensions.set("code", code))
}
