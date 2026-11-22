use std::collections::HashMap;

use async_graphql::{Context, ID, Object, Result, SimpleObject};

#[derive(Clone, SimpleObject)]
pub(super) struct ParameterPage {
    pub(super) id: ID,
    pub(super) title: String,
}

pub(super) type ParameterPageStore = HashMap<ID, ParameterPage>;

pub(super) struct ParameterPageQueries;

#[Object]
impl ParameterPageQueries {
    /// Retrieves a parameter page by ID.
    async fn parameter_page(
        &self, ctx: &Context<'_>, id: ID,
    ) -> Result<Option<ParameterPage>> {
        let pages = ctx.data::<ParameterPageStore>()?;
        Ok(pages.get(&id).cloned())
    }
}

#[cfg(test)]
mod tests {
    use async_graphql::{EmptyMutation, EmptySubscription, Schema};
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode, header},
    };
    use serde_json::{Value, json};
    use tower::Service;

    use crate::graphql::{PARAMETER_PAGE_ENDPOINT, create_parameter_page_router};

    use super::*;

    #[tokio::test]
    async fn resolver_matches_the_full_case_sensitive_id() {
        let id = ID::from("page-123");
        let pages = ParameterPageStore::from([(
            id.clone(),
            ParameterPage {
                id,
                title: "Example page".into(),
            },
        )]);
        let schema =
            Schema::build(ParameterPageQueries, EmptyMutation, EmptySubscription)
                .data(pages)
                .finish();

        let result = schema
            .execute(
                r#"{
                    exact: parameterPage(id: "page-123") { id title }
                    differentCase: parameterPage(id: "PAGE-123") { id }
                    prefix: parameterPage(id: "page-12") { id }
                }"#,
            )
            .await;

        assert!(result.errors.is_empty(), "{:?}", result.errors);
        assert_eq!(
            result.data.into_json().unwrap(),
            json!({
                "exact": { "id": "page-123", "title": "Example page" },
                "differentCase": null,
                "prefix": null
            }),
        );
    }

    #[tokio::test]
    async fn resolver_reports_an_unconfigured_store_as_an_error() {
        let schema =
            Schema::build(ParameterPageQueries, EmptyMutation, EmptySubscription)
                .finish();
        let result = schema
            .execute(r#"{ parameterPage(id: "page-123") { id } }"#)
            .await;

        assert_eq!(result.errors.len(), 1);
        assert_eq!(result.data, async_graphql::Value::Null);
    }

    #[tokio::test]
    async fn parameter_page_query_returns_a_page_or_null() {
        let id = ID::from("page-123");
        let pages = [(
            id.clone(),
            ParameterPage {
                id,
                title: "Example page".into(),
            },
        )]
        .into();
        let mut router = create_parameter_page_router(pages);
        let request = Request::post(PARAMETER_PAGE_ENDPOINT)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                json!({
                    "query": r#"query($id: ID!) {
                        parameterPage(id: $id) { id title }
                        missing: parameterPage(id: "missing") { id title }
                    }"#,
                    "variables": { "id": "page-123" }
                })
                .to_string(),
            ))
            .unwrap();

        let response = router.call(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1 << 20).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({
                "data": {
                    "parameterPage": { "id": "page-123", "title": "Example page" },
                    "missing": null
                }
            }),
        );
    }
}
