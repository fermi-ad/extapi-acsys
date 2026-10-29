use std::sync::Mutex;

use async_graphql::{EmptyMutation, EmptySubscription, Schema, value};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use rust_grpc_lib::auth::TokenProvider;
use serde_json::json;
use tower::Service;

use super::*;
use crate::config::{Upstream, get_test_config};

struct FakeApi {
    response: std::result::Result<ListBpmsResponse, Status>,
    calls: Mutex<Vec<(String, String)>>,
}

impl FakeApi {
    fn new(response: std::result::Result<ListBpmsResponse, Status>) -> Self {
        Self {
            response,
            calls: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl Mi8AutotuneApi for FakeApi {
    async fn list_bpms(
        &self, config: &GrpcConfig, token: ForwardedToken,
    ) -> std::result::Result<ListBpmsResponse, Status> {
        self.calls
            .lock()
            .unwrap()
            .push((config.host_addr.clone(), token.get_token().unwrap()));
        self.response.clone()
    }
}

fn config() -> ExtapiGlobalConfig {
    let mut config = get_test_config();
    config.mi8_autotune = Upstream::Enabled(GrpcConfig {
        host_addr: "http://mi8-autotune.test:50051".to_owned(),
    });
    config
}

fn schema(
    config: ExtapiGlobalConfig, api: Arc<FakeApi>,
) -> Schema<Mi8AutotuneQueries, EmptyMutation, EmptySubscription> {
    Schema::build(Mi8AutotuneQueries, EmptyMutation, EmptySubscription)
        .data(Arc::new(config))
        .data(api as Arc<dyn Mi8AutotuneApi>)
        .finish()
}

#[tokio::test]
async fn bpms_preserves_upstream_names_and_order() {
    let api = Arc::new(FakeApi::new(Ok(ListBpmsResponse {
        bpms: vec!["I:BP02".to_owned(), "I:BP01".to_owned()],
    })));
    let response = schema(config(), api.clone()).execute("{ bpms }").await;

    assert!(response.errors.is_empty(), "{:?}", response.errors);
    assert_eq!(response.data, value!({"bpms": ["I:BP02", "I:BP01"]}));
    assert_eq!(api.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn bpms_accepts_an_empty_list() {
    let api = Arc::new(FakeApi::new(Ok(ListBpmsResponse { bpms: vec![] })));
    let response = schema(config(), api).execute("{ bpms }").await;

    assert!(response.errors.is_empty());
    assert_eq!(response.data, value!({"bpms": []}));
}

#[tokio::test]
async fn upstream_errors_are_safe_and_include_an_error_id() {
    for status in [
        Status::internal("private upstream details and token=secret"),
        Status::unauthenticated("private authentication details"),
        Status::unavailable("private upstream hostname"),
    ] {
        let api = Arc::new(FakeApi::new(Err(status)));
        let response = schema(config(), api).execute("{ bpms }").await;

        assert_eq!(response.errors.len(), 1);
        let message = &response.errors[0].message;
        let id = message
            .strip_prefix(
                "Error encountered. See server logs for details. (Error ID: ",
            )
            .and_then(|message| message.strip_suffix(')'))
            .expect("safe error message with an error ID");
        assert!(uuid::Uuid::parse_str(id).is_ok());
        assert!(!message.contains("private"));
        assert!(!message.contains("secret"));
    }
}

#[tokio::test]
async fn disabled_config_returns_feature_disabled_without_calling_upstream() {
    let api = Arc::new(FakeApi::new(Ok(ListBpmsResponse { bpms: vec![] })));
    let mut config = config();
    config.mi8_autotune =
        Upstream::disabled("MI8 Autotune", ["MI8_AUTOTUNE_GRPC_HOST"]);
    let response = schema(config, api.clone()).execute("{ bpms }").await;

    assert_eq!(response.errors.len(), 1);
    assert_eq!(
        response.errors[0].extensions.as_ref().unwrap().get("code"),
        Some(&value!("FEATURE_DISABLED")),
    );
    assert!(api.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn http_handler_forwards_each_callers_token_and_config() {
    let api = Arc::new(FakeApi::new(Ok(ListBpmsResponse {
        bpms: vec!["I:BP01".to_owned()],
    })));
    let site = crate::graphql::create_mi8_autotune_router_with_api(
        Arc::new(config()),
        api.clone(),
    );

    for token in [
        Some("first-caller-token"),
        Some("second-caller-token"),
        None,
    ] {
        let mut request = Request::builder()
            .method("POST")
            .uri("/mi8-autotune")
            .header("content-type", "application/json");
        if let Some(token) = token {
            request =
                request.header("authorization", format!("Bearer {token}"));
        }
        let response = site
            .clone()
            .call(request.body(Body::from(r#"{"query":"{ bpms }"}"#)).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            json!({"data": {"bpms": ["I:BP01"]}}),
        );
    }

    let calls = api.calls.lock().unwrap();
    assert_eq!(calls.len(), 3);
    for ((host, actual_token), expected_token) in
        calls
            .iter()
            .zip(["first-caller-token", "second-caller-token", ""])
    {
        assert_eq!(host, "http://mi8-autotune.test:50051");
        assert_eq!(actual_token, expected_token);
    }
}

#[test]
fn schema_is_query_only_with_non_null_bpm_names() {
    let api = Arc::new(FakeApi::new(Ok(ListBpmsResponse { bpms: vec![] })));
    let sdl = schema(config(), api).sdl();

    assert!(sdl.contains("bpms: [String!]!"), "{sdl}");
    assert!(!sdl.contains("mutation:"), "{sdl}");
    assert!(!sdl.contains("subscription:"), "{sdl}");
}
