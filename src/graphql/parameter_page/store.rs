use std::{collections::HashMap, future::Future};

use async_graphql::{ID, Result};
use rust_grpc_lib::auth::ForwardedToken;
use tokio::sync::RwLock;
use uuid::Uuid;

use super::{
    ParameterPage, ParameterPageInput, ParameterPageSummary, page_error,
};

pub(in crate::graphql) trait ParameterPageStore:
    Send + Sync
{
    fn list(
        &self, token: ForwardedToken,
    ) -> impl Future<Output = Result<Vec<ParameterPageSummary>>> + Send;
    fn fetch(
        &self, id: ID, token: ForwardedToken,
    ) -> impl Future<Output = Result<Option<ParameterPage>>> + Send;
    fn create(
        &self, input: ParameterPageInput, token: ForwardedToken,
    ) -> impl Future<Output = Result<ParameterPage>> + Send;
    fn update(
        &self, id: ID, expected_revision: &str, input: ParameterPageInput,
        token: ForwardedToken,
    ) -> impl Future<Output = Result<ParameterPage>> + Send;
    fn delete(
        &self, id: ID, expected_revision: &str, token: ForwardedToken,
    ) -> impl Future<Output = Result<ID>> + Send;
}

#[derive(Default)]
pub(in crate::graphql) struct InMemoryParameterPageStore {
    pages: RwLock<HashMap<ID, ParameterPage>>,
}

impl ParameterPageStore for InMemoryParameterPageStore {
    async fn list(
        &self, _token: ForwardedToken,
    ) -> Result<Vec<ParameterPageSummary>> {
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
    ) -> Result<Option<ParameterPage>> {
        Ok(self.pages.read().await.get(&id).cloned())
    }

    async fn create(
        &self, input: ParameterPageInput, _token: ForwardedToken,
    ) -> Result<ParameterPage> {
        let id = ID::from(Uuid::new_v4().to_string());
        let page = ParameterPage::new(id, input);
        self.pages
            .write()
            .await
            .insert(page.id.clone(), page.clone());
        Ok(page)
    }

    async fn update(
        &self, id: ID, expected_revision: &str, input: ParameterPageInput,
        _token: ForwardedToken,
    ) -> Result<ParameterPage> {
        let mut pages = self.pages.write().await;
        let page = matching_page(&mut pages, &id, expected_revision)?;
        *page = ParameterPage::new(id, input);
        Ok(page.clone())
    }

    async fn delete(
        &self, id: ID, expected_revision: &str, _token: ForwardedToken,
    ) -> Result<ID> {
        let mut pages = self.pages.write().await;
        matching_page(&mut pages, &id, expected_revision)?;
        pages.remove(&id);
        Ok(id)
    }
}

fn matching_page<'a>(
    pages: &'a mut HashMap<ID, ParameterPage>, id: &ID, expected_revision: &str,
) -> Result<&'a mut ParameterPage> {
    let page = pages.get_mut(id).ok_or_else(|| {
        page_error("NOT_FOUND", "The parameter page was not found.")
    })?;
    if page.revision != expected_revision {
        return Err(page_error(
            "CONFLICT",
            "The parameter page changed. Reload it before trying again.",
        ));
    }
    Ok(page)
}
