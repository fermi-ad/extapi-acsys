use std::sync::Arc;

use async_graphql::{Error, ErrorExtensions, ID, Object, Result};

mod store;
mod types;

#[cfg(test)]
mod tests;

pub(super) use store::{InMemoryParameterPageStore, ParameterPageStore};
use types::{ParameterPage, ParameterPageInput, ParameterPageSummary};

pub(super) struct ParameterPageQueries<T> {
    pub(super) store: Arc<T>,
}

#[Object]
impl<T: ParameterPageStore + 'static> ParameterPageQueries<T> {
    /// Lists page IDs and titles in title order.
    async fn parameter_pages(&self) -> Result<Vec<ParameterPageSummary>> {
        self.store.list().await
    }

    /// Retrieves a parameter page by ID.
    async fn parameter_page(&self, id: ID) -> Result<Option<ParameterPage>> {
        self.store.fetch(id).await
    }
}

pub(super) struct ParameterPageMutations<T> {
    pub(super) store: Arc<T>,
}

#[Object]
impl<T: ParameterPageStore + 'static> ParameterPageMutations<T> {
    /// Saves a new page with a server-assigned ID and revision.
    async fn create_parameter_page(
        &self, input: ParameterPageInput,
    ) -> Result<ParameterPage> {
        input.validate()?;
        self.store.create(input).await
    }

    /// Replaces the page title and contents if the revision still matches.
    async fn update_parameter_page(
        &self, id: ID, expected_revision: String, input: ParameterPageInput,
    ) -> Result<ParameterPage> {
        input.validate()?;
        self.store.update(id, &expected_revision, input).await
    }

    /// Deletes the page if the revision still matches and returns its ID.
    async fn delete_parameter_page(
        &self, id: ID, expected_revision: String,
    ) -> Result<ID> {
        self.store.delete(id, &expected_revision).await
    }
}

fn page_error(code: &str, message: &str) -> Error {
    Error::new(message)
        .extend_with(|_, extensions| extensions.set("code", code))
}
