use std::{collections::HashMap, future::Future};

use async_graphql::ID;
use rust_grpc_lib::auth::ForwardedToken;
use tokio::sync::RwLock;
use uuid::Uuid;

use super::{ParameterPage, ParameterPageInput, ParameterPageSummary};

#[derive(Debug, PartialEq, Eq)]
pub(in crate::graphql) enum StoreError {
    NotFound,
    Conflict,
}

pub(in crate::graphql) type StoreResult<T> = Result<T, StoreError>;

pub(in crate::graphql) trait ParameterPageStore:
    Send + Sync
{
    /// Returns page summaries in title order, using ID to break ties.
    fn list(
        &self, token: ForwardedToken,
    ) -> impl Future<Output = StoreResult<Vec<ParameterPageSummary>>> + Send;
    fn fetch(
        &self, id: ID, token: ForwardedToken,
    ) -> impl Future<Output = StoreResult<Option<ParameterPage>>> + Send;
    /// Assigns the new page's ID and revision.
    fn create(
        &self, input: ParameterPageInput, token: ForwardedToken,
    ) -> impl Future<Output = StoreResult<ParameterPage>> + Send;
    /// Atomically replaces a matching revision and assigns a new revision.
    fn update(
        &self, id: ID, expected_revision: &str, input: ParameterPageInput,
        token: ForwardedToken,
    ) -> impl Future<Output = StoreResult<ParameterPage>> + Send;
    /// Atomically removes the page if the revision matches.
    fn delete(
        &self, id: ID, expected_revision: &str, token: ForwardedToken,
    ) -> impl Future<Output = StoreResult<ID>> + Send;
}

#[derive(Default)]
pub(in crate::graphql) struct InMemoryParameterPageStore {
    pages: RwLock<HashMap<ID, ParameterPage>>,
}

impl ParameterPageStore for InMemoryParameterPageStore {
    async fn list(
        &self, _token: ForwardedToken,
    ) -> StoreResult<Vec<ParameterPageSummary>> {
        let mut pages: Vec<_> = self
            .pages
            .read()
            .await
            .values()
            .map(ParameterPageSummary::from)
            .collect();
        pages.sort_unstable_by(|a, b| {
            a.title
                .cmp(&b.title)
                .then_with(|| a.id.as_str().cmp(b.id.as_str()))
        });
        Ok(pages)
    }

    async fn fetch(
        &self, id: ID, _token: ForwardedToken,
    ) -> StoreResult<Option<ParameterPage>> {
        Ok(self.pages.read().await.get(&id).cloned())
    }

    async fn create(
        &self, input: ParameterPageInput, _token: ForwardedToken,
    ) -> StoreResult<ParameterPage> {
        let id = ID::from(Uuid::new_v4().to_string());
        let page = ParameterPage::new(id, Uuid::new_v4().to_string(), input);
        self.pages
            .write()
            .await
            .insert(page.id.clone(), page.clone());
        Ok(page)
    }

    async fn update(
        &self, id: ID, expected_revision: &str, input: ParameterPageInput,
        _token: ForwardedToken,
    ) -> StoreResult<ParameterPage> {
        let mut pages = self.pages.write().await;
        let page = matching_page(&mut pages, &id, expected_revision)?;
        *page = ParameterPage::new(id, Uuid::new_v4().to_string(), input);
        Ok(page.clone())
    }

    async fn delete(
        &self, id: ID, expected_revision: &str, _token: ForwardedToken,
    ) -> StoreResult<ID> {
        let mut pages = self.pages.write().await;
        matching_page(&mut pages, &id, expected_revision)?;
        pages.remove(&id);
        Ok(id)
    }
}

fn matching_page<'a>(
    pages: &'a mut HashMap<ID, ParameterPage>, id: &ID, expected_revision: &str,
) -> StoreResult<&'a mut ParameterPage> {
    let page = pages.get_mut(id).ok_or(StoreError::NotFound)?;
    if page.revision != expected_revision {
        return Err(StoreError::Conflict);
    }
    Ok(page)
}
