use std::collections::{
    HashMap,
    hash_map::{Entry, OccupiedEntry},
};

use async_graphql::{ID, Result, async_trait::async_trait};
use tokio::sync::RwLock;
use uuid::Uuid;

use super::{
    ParameterPage, ParameterPageInput, ParameterPageSummary, page_error,
};

#[async_trait]
pub(in crate::graphql) trait ParameterPageStore:
    Send + Sync
{
    async fn list(&self) -> Result<Vec<ParameterPageSummary>>;
    async fn fetch(&self, id: ID) -> Result<Option<ParameterPage>>;
    async fn create(&self, input: ParameterPageInput) -> Result<ParameterPage>;
    async fn update(
        &self, id: ID, expected_revision: &str, input: ParameterPageInput,
    ) -> Result<ParameterPage>;
    async fn delete(&self, id: ID, expected_revision: &str) -> Result<ID>;
}

#[derive(Default)]
pub(in crate::graphql) struct InMemoryParameterPageStore {
    pages: RwLock<HashMap<ID, ParameterPage>>,
}

#[async_trait]
impl ParameterPageStore for InMemoryParameterPageStore {
    async fn list(&self) -> Result<Vec<ParameterPageSummary>> {
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

    async fn fetch(&self, id: ID) -> Result<Option<ParameterPage>> {
        Ok(self.pages.read().await.get(&id).cloned())
    }

    async fn create(&self, input: ParameterPageInput) -> Result<ParameterPage> {
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
    ) -> Result<ParameterPage> {
        let mut pages = self.pages.write().await;
        let mut entry = matching_page(&mut pages, id, expected_revision)?;
        let page = ParameterPage::new(entry.key().clone(), input);
        entry.insert(page.clone());
        Ok(page)
    }

    async fn delete(&self, id: ID, expected_revision: &str) -> Result<ID> {
        let mut pages = self.pages.write().await;
        let entry = matching_page(&mut pages, id, expected_revision)?;
        Ok(entry.remove_entry().0)
    }
}

fn matching_page<'a>(
    pages: &'a mut HashMap<ID, ParameterPage>, id: ID, expected_revision: &str,
) -> Result<OccupiedEntry<'a, ID, ParameterPage>> {
    let Entry::Occupied(entry) = pages.entry(id) else {
        return Err(page_error(
            "NOT_FOUND",
            "The parameter page was not found.",
        ));
    };
    if entry.get().revision != expected_revision {
        return Err(page_error(
            "CONFLICT",
            "The parameter page changed. Reload it before trying again.",
        ));
    }
    Ok(entry)
}
