use super::*;
use crate::{
    config::GrpcConfig,
    g_rpc::proto::{
        google::protobuf::Empty,
        services::unr::{
            entity::{Entity, ReadEntityResponse},
            relationship::{
                ReadRelationshipResponse, Relationship, RelationshipDetails,
            },
        },
    },
};
use async_graphql::async_trait::async_trait;
use async_graphql::dataloader::{DataLoader, HashMapCache};
use async_graphql::{EmptySubscription, Schema, dataloader::Loader};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::Value;
use std::sync::Arc;
use std::{collections::HashMap, sync::Mutex};
use tonic::{Code, Status};
use tower::Service;

#[derive(Default)]
struct FakeUnrApi {
    /// Entity store keyed by entity ID.
    entities: Mutex<HashMap<String, Entity>>,
    /// Relationship edge list: each entry is (parent_id, child_id).
    edges: Mutex<Vec<Relationship>>,

    /// Failure injection for tests.
    ///
    /// Semantics:
    /// - `None` => don't fail
    /// - `Some((0, code))` => fail the next UNR call with `code`
    /// - `Some((n, code))` where `n > 0` => succeed next `n` calls, then fail with `code`
    fail_after: Mutex<Option<(usize, Code)>>,

    /// Count how many times `read_entities` was called.
    read_entities_calls: Mutex<usize>,
}

impl FakeUnrApi {
    fn check_fail(&self) -> Result<(), Status> {
        let mut guard = self.fail_after.lock().unwrap();
        match *guard {
            None => Ok(()),
            Some((0, code)) => {
                *guard = None;
                Err(Status::new(code, "forced failure"))
            }
            Some((n, code)) => {
                *guard = Some((n - 1, code));
                Ok(())
            }
        }
    }
}

#[async_trait]
impl UnrApi for FakeUnrApi {
    async fn create_entities(
        &self, _conf: &GrpcConfig, _token: ForwardedToken,
        new_entities: Vec<Entity>,
    ) -> Result<Empty, Status> {
        self.check_fail()?;

        let mut store = self.entities.lock().unwrap();
        for entity in new_entities {
            if store.contains_key(&entity.id) {
                return Err(Status::new(Code::AlreadyExists, "exists"));
            }
            store.insert(entity.id.clone(), entity);
        }
        Ok(Empty {})
    }

    async fn read_entities(
        &self, _conf: &GrpcConfig, _token: ForwardedToken, ids: Vec<String>,
    ) -> Result<ReadEntityResponse, Status> {
        self.check_fail()?;

        *self.read_entities_calls.lock().unwrap() += 1;

        let store = self.entities.lock().unwrap();
        let mut out: Vec<Entity> = Vec::new();

        if ids.is_empty() {
            out.extend(store.values().cloned());
        } else {
            for id in ids {
                if let Some(e) = store.get(&id) {
                    out.push(e.clone());
                }
            }
        }

        Ok(ReadEntityResponse { entities: out })
    }

    async fn update_entities(
        &self, _conf: &GrpcConfig, _token: ForwardedToken, updated: Vec<Entity>,
    ) -> Result<Empty, Status> {
        self.check_fail()?;

        let mut store = self.entities.lock().unwrap();
        for entity in updated {
            if store.contains_key(&entity.id) {
                store.insert(entity.id.clone(), entity);
            }
        }
        Ok(Empty {})
    }

    async fn delete_entities(
        &self, _conf: &GrpcConfig, _token: ForwardedToken, ids: Vec<String>,
    ) -> Result<Empty, Status> {
        self.check_fail()?;

        let mut store = self.entities.lock().unwrap();
        for id in ids {
            store.remove(&id);
        }
        Ok(Empty {})
    }

    async fn create_relationships(
        &self, _conf: &GrpcConfig, _token: ForwardedToken,
        relationships: Vec<Relationship>,
    ) -> Result<Empty, Status> {
        self.check_fail()?;

        let mut edges = self.edges.lock().unwrap();
        for rel in relationships {
            // Avoid duplicates.
            if !edges.iter().any(|e| {
                e.parent_id == rel.parent_id && e.child_id == rel.child_id
            }) {
                edges.push(rel);
            }
        }
        Ok(Empty {})
    }

    async fn read_relationships(
        &self, _conf: &GrpcConfig, _token: ForwardedToken, ids: Vec<String>,
    ) -> Result<ReadRelationshipResponse, Status> {
        self.check_fail()?;

        let edges = self.edges.lock().unwrap();
        let mut entries: Vec<RelationshipDetails> = Vec::new();

        for id in &ids {
            // Children: edges where this ID is the parent.
            let children: Vec<String> = edges
                .iter()
                .filter(|e| &e.parent_id == id)
                .map(|e| e.child_id.clone())
                .collect();

            // Parent: edge where this ID is the child (at most one).
            let child_of = edges
                .iter()
                .find(|e| &e.child_id == id)
                .map(|e| e.parent_id.clone())
                .unwrap_or_default();

            entries.push(RelationshipDetails {
                id: id.clone(),
                children,
                child_of,
            });
        }

        Ok(ReadRelationshipResponse { entries })
    }

    async fn delete_relationships(
        &self, _conf: &GrpcConfig, _token: ForwardedToken,
        relationships: Vec<Relationship>,
    ) -> Result<Empty, Status> {
        self.check_fail()?;

        let mut edges = self.edges.lock().unwrap();
        for rel in &relationships {
            edges.retain(|e| {
                !(e.parent_id == rel.parent_id && e.child_id == rel.child_id)
            });
        }
        Ok(Empty {})
    }
}

fn testing_config() -> GrpcConfig {
    GrpcConfig {
        host_addr: String::new(),
    }
}

fn testing_token() -> ForwardedToken {
    ForwardedToken::new("")
}

fn schema_with_api(
    api: Arc<dyn UnrApi>,
) -> Schema<UnrQueries, UnrMutations, EmptySubscription> {
    Schema::build(UnrQueries, UnrMutations, EmptySubscription)
            .data(api.clone())
            // Tests execute the schema directly (bypassing the HTTP handler), so
            // attach a loader here to emulate request-scoped injection.
            .data(DataLoader::with_cache(
                loader::UnrEntityLoader::new(api, testing_config(), testing_token()),
                tokio::spawn,
                HashMapCache::default(),
            ))
            .data(testing_config())
            .finish()
}

fn json_data(result: async_graphql::Response) -> Value {
    serde_json::to_value(result.data).expect("response data is JSON")
}

fn mk_request_scoped_loader(
    api: Arc<dyn UnrApi>,
) -> DataLoader<loader::UnrEntityLoader, HashMapCache> {
    DataLoader::with_cache(
        loader::UnrEntityLoader::new(api, testing_config(), testing_token()),
        tokio::spawn,
        HashMapCache::default(),
    )
}

async fn assert_err_starts_with(
    schema: &Schema<UnrQueries, UnrMutations, EmptySubscription>, gql: &str,
    prefix: &str,
) {
    let result = schema.execute(gql).await;
    let err = result.errors.first().expect("expected error");
    assert!(err.message.starts_with(prefix), "got error: {err}");
}

#[test]
fn schema_builds() {
    let api: Arc<dyn UnrApi> = Arc::new(api::GrpcUnrApi);
    let _schema = schema_with_api(api);
}

#[test]
fn handle_error_invalid_argument_exposes_message() {
    let e = Status::new(Code::InvalidArgument, "bad input");
    let err = handle_error(e, "creating device");
    assert!(err.message.contains("bad input"));
    assert!(err.message.contains("Error ID:"));
}

#[test]
fn handle_error_non_invalid_argument_is_generic() {
    let e = Status::new(Code::Unavailable, "transport details");
    let err = handle_error(e, "creating device");
    assert!(err.message.starts_with("Error creating device."));
    assert!(err.message.contains("Error ID:"));
    assert!(!err.message.contains("transport details"));
}

#[tokio::test]
async fn set_children_empty_is_idempotent() {
    let api = Arc::new(FakeUnrApi::default());
    let config = testing_config();
    // no relationships exist
    let got = set_children_impl(
        api.as_ref(),
        &config,
        testing_token(),
        "P".to_string(),
        vec![],
    )
    .await;
    assert!(got.is_ok());
    assert_eq!(got.unwrap().name, "P");
}

#[tokio::test]
async fn set_children_non_empty_creates_relationships() {
    let api = Arc::new(FakeUnrApi::default());
    let config = testing_config();
    let got = set_children_impl(
        api.as_ref(),
        &config,
        testing_token(),
        "P".to_string(),
        vec!["C1".to_string(), "C2".to_string()],
    )
    .await
    .unwrap();
    assert_eq!(got.name, "P");

    let resp = api
        .read_relationships(&config, testing_token(), vec!["P".to_string()])
        .await
        .unwrap();
    let entry = resp.entries.iter().find(|e| e.id == "P").unwrap();
    let mut children = entry.children.clone();
    children.sort();
    assert_eq!(children, vec!["C1".to_string(), "C2".to_string()]);
}

#[tokio::test]
async fn set_children_existing_relationship_is_replaced() {
    let api = Arc::new(FakeUnrApi::default());
    let config = testing_config();

    api.create_relationships(
        &config,
        testing_token(),
        vec![Relationship {
            parent_id: "P".to_string(),
            child_id: "OLD".to_string(),
        }],
    )
    .await
    .unwrap();

    set_children_impl(
        api.as_ref(),
        &config,
        testing_token(),
        "P".to_string(),
        vec!["NEW".to_string()],
    )
    .await
    .unwrap();

    let resp = api
        .read_relationships(&config, testing_token(), vec!["P".to_string()])
        .await
        .unwrap();
    let entry = resp.entries.iter().find(|e| e.id == "P").unwrap();
    assert_eq!(entry.children, vec!["NEW".to_string()]);
}

#[tokio::test]
async fn loader_dedupes_keys_and_maps_by_entity_id() {
    let api = Arc::new(FakeUnrApi::default());
    let config = testing_config();
    api.create_entities(
        &config,
        testing_token(),
        vec![Entity {
            id: "A".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        }],
    )
    .await
    .unwrap();

    let loader = loader::UnrEntityLoader::new(api, config, testing_token());
    let out = loader
        .load(&["A".to_string(), "A".to_string(), "B".to_string()])
        .await
        .unwrap();
    assert!(out.contains_key("A"));
    assert!(!out.contains_key("B"));
}

#[tokio::test]
async fn loader_transport_errors_bubble_up() {
    let api = Arc::new(FakeUnrApi::default());

    // Fail the next UNR call (read_entities).
    *api.fail_after.lock().unwrap() = Some((0, Code::Unavailable));

    let loader =
        loader::UnrEntityLoader::new(api, testing_config(), testing_token());
    let err = loader
        .load(&["A".to_string()])
        .await
        .expect_err("expected loader error");

    assert!(
        err.to_string().contains("forced failure"),
        "expected underlying tonic status message to be preserved, got: {:?}",
        err
    );
}

#[tokio::test]
async fn dataloader_cache_is_used_within_single_request() {
    let api = Arc::new(FakeUnrApi::default());
    let config = testing_config();
    api.create_entities(
        &config,
        testing_token(),
        vec![Entity {
            id: "D".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        }],
    )
    .await
    .unwrap();

    let schema = schema_with_api(api.clone());

    // Attach a request-scoped loader and query multiple entity-backed fields.
    // `devices()` primes the loader; subsequent field resolvers should hit cache.
    let req = async_graphql::Request::new(
        r#"
            query {
              devices(names:["D"]) {
                __typename
                ... on Device { name address type protocol }
              }
            }
            "#,
    )
    .data(mk_request_scoped_loader(api.clone()));

    let result = schema.execute(req).await;
    assert!(result.errors.is_empty(), "errors: {:?}", result.errors);

    // Expect exactly one UNR entity fetch for the whole request.
    assert_eq!(*api.read_entities_calls.lock().unwrap(), 1);
}

#[tokio::test]
async fn dataloader_cache_does_not_carry_over_across_requests() {
    let api = Arc::new(FakeUnrApi::default());
    let config = testing_config();
    api.create_entities(
        &config,
        testing_token(),
        vec![Entity {
            id: "D".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        }],
    )
    .await
    .unwrap();

    let schema = schema_with_api(api.clone());

    let gql = r#"
            query {
              devices(names:["D"]) {
                __typename
                ... on Device { name address type protocol }
              }
            }
        "#;

    // First request with its own loader.
    let r1 = schema
        .execute(
            async_graphql::Request::new(gql)
                .data(mk_request_scoped_loader(api.clone())),
        )
        .await;
    assert!(r1.errors.is_empty(), "errors: {:?}", r1.errors);

    // Second request with a fresh loader should trigger another UNR fetch.
    let r2 = schema
        .execute(
            async_graphql::Request::new(gql)
                .data(mk_request_scoped_loader(api.clone())),
        )
        .await;
    assert!(r2.errors.is_empty(), "errors: {:?}", r2.errors);

    assert_eq!(*api.read_entities_calls.lock().unwrap(), 2);
}

#[tokio::test]
async fn http_handler_injects_request_scoped_loader_no_cache_bleed() {
    let api = Arc::new(FakeUnrApi::default());
    let config = testing_config();
    api.create_entities(
        &config,
        testing_token(),
        vec![Entity {
            id: "D".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        }],
    )
    .await
    .unwrap();

    let mut app = crate::graphql::create_unr_router_with_api(
        api.clone(),
        GrpcConfig {
            host_addr: String::new(),
        },
    );

    let gql = r#"{ "query": "query { devices(names:[\"D\"]) { __typename ... on Device { name address type protocol } } }" }"#;

    // Note: this endpoint requires an Authorization header (any value) to
    // satisfy `AuthInfo` parsing in the handler.

    // First HTTP request.
    let resp1 = app
        .call(
            Request::builder()
                .method("POST")
                .uri("/unr")
                .header("content-type", "application/json")
                .header("authorization", "Bearer TEST")
                .body(Body::from(gql))
                .unwrap(),
        )
        .await
        .unwrap();
    if resp1.status() != StatusCode::OK {
        let body = axum::body::to_bytes(resp1.into_body(), usize::MAX)
            .await
            .unwrap();
        panic!(
            "unexpected status {} body: {}",
            StatusCode::BAD_REQUEST,
            String::from_utf8_lossy(&body)
        );
    }

    // Second HTTP request.
    let resp2 = app
        .call(
            Request::builder()
                .method("POST")
                .uri("/unr")
                .header("content-type", "application/json")
                .header("authorization", "Bearer TEST")
                .body(Body::from(gql))
                .unwrap(),
        )
        .await
        .unwrap();
    if resp2.status() != StatusCode::OK {
        let body = axum::body::to_bytes(resp2.into_body(), usize::MAX)
            .await
            .unwrap();
        panic!(
            "unexpected status {} body: {}",
            StatusCode::BAD_REQUEST,
            String::from_utf8_lossy(&body)
        );
    }

    // If the loader were schema-scoped, the second request could reuse the cache
    // and this would remain 1. With request-scoped injection, it must be 2.
    assert_eq!(*api.read_entities_calls.lock().unwrap(), 2);
}

#[tokio::test]
async fn device_fields_resolve_from_loader_and_strip_empty() {
    let api = Arc::new(FakeUnrApi::default());
    let config = testing_config();
    api.create_entities(
        &config,
        testing_token(),
        vec![Entity {
            id: "D".to_string(),
            address: "".to_string(),
            r#type: "T".to_string(),
            protocol: "".to_string(),
        }],
    )
    .await
    .unwrap();

    let schema = schema_with_api(api);
    let result = schema
        .execute(
            r#"
                query {
                  devices(names:["D"]) {
                    __typename
                    ... on Device { name address type protocol }
                    ... on NotFound { name }
                  }
                }
                "#,
        )
        .await;

    assert!(result.errors.is_empty(), "errors: {:?}", result.errors);
    let v = json_data(result);
    assert_eq!(v["devices"][0]["name"], "D");
    assert!(v["devices"][0]["address"].is_null());
    assert_eq!(v["devices"][0]["type"], "T");
    assert!(v["devices"][0]["protocol"].is_null());
}

#[tokio::test]
async fn device_children_resolves_relationships() {
    let api = Arc::new(FakeUnrApi::default());
    let config = testing_config();

    // devices() validates existence via entities; ensure parent exists.
    api.create_entities(
        &config,
        testing_token(),
        vec![Entity {
            id: "P".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        }],
    )
    .await
    .unwrap();

    api.create_relationships(
        &config,
        testing_token(),
        vec![Relationship {
            parent_id: "P".to_string(),
            child_id: "C".to_string(),
        }],
    )
    .await
    .unwrap();

    let schema = schema_with_api(api);
    let result = schema
        .execute(
            r#"
                query {
                  devices(names:["P"]) {
                    __typename
                    ... on Device { name children { name } }
                    ... on NotFound { name }
                  }
                }
                "#,
        )
        .await;

    assert!(result.errors.is_empty(), "errors: {:?}", result.errors);
    let v = json_data(result);
    assert_eq!(v["devices"][0]["children"][0]["name"], "C");
}

#[tokio::test]
async fn device_parent_resolves_relationship() {
    let api = Arc::new(FakeUnrApi::default());

    let config = testing_config();
    // Seed parent and child entities.
    api.create_entities(
        &config,
        testing_token(),
        vec![
            Entity {
                id: "PARENT".to_string(),
                address: "ADDR".to_string(),
                r#type: "TYPE".to_string(),
                protocol: "PROTO".to_string(),
            },
            Entity {
                id: "CHILD".to_string(),
                address: "ADDR".to_string(),
                r#type: "TYPE".to_string(),
                protocol: "PROTO".to_string(),
            },
        ],
    )
    .await
    .unwrap();

    api.create_relationships(
        &config,
        testing_token(),
        vec![Relationship {
            parent_id: "PARENT".to_string(),
            child_id: "CHILD".to_string(),
        }],
    )
    .await
    .unwrap();

    let schema = schema_with_api(api);
    let result = schema
        .execute(
            r#"
                query {
                  devices(names:["CHILD"]) {
                    __typename
                    ... on Device { name parent { name } }
                    ... on NotFound { name }
                  }
                }
                "#,
        )
        .await;

    assert!(result.errors.is_empty(), "errors: {:?}", result.errors);
    let v = json_data(result);
    assert_eq!(v["devices"][0]["parent"]["name"], "PARENT");
}

#[tokio::test]
async fn device_parent_is_null_when_no_parent() {
    let api = Arc::new(FakeUnrApi::default());
    let config = testing_config();

    api.create_entities(
        &config,
        testing_token(),
        vec![Entity {
            id: "ORPHAN".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        }],
    )
    .await
    .unwrap();

    let schema = schema_with_api(api);
    let result = schema
        .execute(
            r#"
                query {
                  devices(names:["ORPHAN"]) {
                    __typename
                    ... on Device { name parent { name } }
                    ... on NotFound { name }
                  }
                }
                "#,
        )
        .await;

    assert!(result.errors.is_empty(), "errors: {:?}", result.errors);
    let v = json_data(result);
    assert!(v["devices"][0]["parent"].is_null());
}

#[tokio::test]
async fn devices_query_returns_not_found_for_requested_name() {
    let api = Arc::new(FakeUnrApi::default());
    let config = testing_config();

    api.create_entities(
        &config,
        testing_token(),
        vec![Entity {
            id: "A".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        }],
    )
    .await
    .unwrap();

    let schema = schema_with_api(api);
    let result = schema
        .execute(
            r#"
                query {
                  devices(names:["A","B"]) {
                    __typename
                    ... on Device { name }
                    ... on NotFound { name }
                  }
                }
                "#,
        )
        .await;

    assert!(result.errors.is_empty(), "errors: {:?}", result.errors);
    let v = json_data(result);

    assert_eq!(v["devices"][0]["__typename"], "Device");
    assert_eq!(v["devices"][0]["name"], "A");

    assert_eq!(v["devices"][1]["__typename"], "NotFound");
    assert_eq!(v["devices"][1]["name"], "B");
}

#[tokio::test]
async fn devices_query_without_names_returns_all_devices() {
    let api = Arc::new(FakeUnrApi::default());
    let config = testing_config();
    api.create_entities(
        &config,
        testing_token(),
        vec![
            Entity {
                id: "A".to_string(),
                address: "ADDR".to_string(),
                r#type: "TYPE".to_string(),
                protocol: "PROTO".to_string(),
            },
            Entity {
                id: "B".to_string(),
                address: "ADDR".to_string(),
                r#type: "TYPE".to_string(),
                protocol: "PROTO".to_string(),
            },
        ],
    )
    .await
    .unwrap();

    let schema = schema_with_api(api);
    let result = schema
        .execute(
            r#"
                query {
                  devices {
                    __typename
                    ... on Device { name }
                    ... on NotFound { name }
                  }
                }
                "#,
        )
        .await;

    assert!(result.errors.is_empty(), "errors: {:?}", result.errors);
    let v = json_data(result);
    let names = v["devices"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["name"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert!(names.contains(&"A".to_string()));
    assert!(names.contains(&"B".to_string()));
}

#[tokio::test]
async fn mutation_create_device_works() {
    let api: Arc<dyn UnrApi> = Arc::new(FakeUnrApi::default());
    let schema = schema_with_api(api.clone());
    let config = testing_config();

    // Seed child so createDevice's child pre-validation passes.
    api.create_entities(
        &config,
        testing_token(),
        vec![Entity {
            id: "C".to_string(),
            address: "".to_string(),
            r#type: "".to_string(),
            protocol: "".to_string(),
        }],
    )
    .await
    .unwrap();

    let r = schema
            .execute(
                r#"
                mutation {
                  createDevice(input:{name:"A", address:"ADDR", type:"TYPE", protocol:"PROTO", children:["C"]}) {
                    name
                    children { name }
                  }
                }
                "#,
            )
            .await;
    assert!(r.errors.is_empty(), "errors: {:?}", r.errors);

    let v = json_data(r);
    assert_eq!(v["createDevice"]["name"], "A");
    assert_eq!(v["createDevice"]["children"][0]["name"], "C");
}

#[tokio::test]
async fn mutation_create_device_missing_child_fails_without_creating_parent() {
    let api: Arc<dyn UnrApi> = Arc::new(FakeUnrApi::default());
    let schema = schema_with_api(api.clone());

    let r = schema
            .execute(
                r#"
                mutation {
                  createDevice(input:{name:"A", address:"ADDR", type:"TYPE", protocol:"PROTO", children:["MISSING"]}) {
                    name
                  }
                }
                "#,
            )
            .await;

    assert!(!r.errors.is_empty(), "expected error");
    let config = testing_config();

    // Ensure parent was not created.
    let resp = api
        .read_entities(&config, testing_token(), vec!["A".to_string()])
        .await
        .unwrap();
    assert!(resp.entities.is_empty());
}

#[tokio::test]
async fn mutation_create_device_relationship_failure_includes_partial_success_extensions()
 {
    let api = Arc::new(FakeUnrApi::default());
    let schema = schema_with_api(api.clone());
    let config = testing_config();

    // Seed child so pre-validation passes.
    api.create_entities(
        &config,
        testing_token(),
        vec![Entity {
            id: "C".to_string(),
            address: "".to_string(),
            r#type: "".to_string(),
            protocol: "".to_string(),
        }],
    )
    .await
    .unwrap();

    // Force the relationship step to fail.
    // Call order inside createDevice (when children are provided):
    // 1) read_entities (child pre-validation)
    // 2) create_entities (parent)
    // 3) read_relationships (set_children reads current state)
    // 4) create_relationships (set_children adds new edges)
    *api.fail_after.lock().unwrap() = Some((3, Code::InvalidArgument));

    let r = schema
            .execute(
                r#"
                mutation {
                  createDevice(input:{name:"A", address:"ADDR", type:"TYPE", protocol:"PROTO", children:["C"]}) {
                    name
                  }
                }
                "#,
            )
            .await;

    assert!(!r.errors.is_empty(), "expected error: {r:#?}");

    let err = r.errors.first().unwrap();
    eprintln!("GraphQL error: {err:#?}");
    let ext = err.extensions.as_ref().expect("expected extensions");
    assert_eq!(
        ext.get("deviceCreated").unwrap(),
        &async_graphql::Value::from(true)
    );
    assert_eq!(
        ext.get("childrenAdded").unwrap(),
        &async_graphql::Value::from(false)
    );

    // Entity was created successfully.
    let resp = api
        .read_entities(&config, testing_token(), vec!["A".to_string()])
        .await
        .unwrap();
    assert_eq!(resp.entities.len(), 1);
}

#[tokio::test]
async fn mutation_create_device_returns_written_fields() {
    let api: Arc<dyn UnrApi> = Arc::new(FakeUnrApi::default());
    let schema = schema_with_api(api);

    let r = schema
            .execute(
                r#"
                mutation {
                  createDevice(input:{name:"A", address:"ADDR", type:"TYPE", protocol:"PROTO"}) {
                    name
                    address
                    type
                    protocol
                  }
                }
                "#,
            )
            .await;
    assert!(r.errors.is_empty(), "errors: {:?}", r.errors);

    let v = json_data(r);
    assert_eq!(v["createDevice"]["name"], "A");
    assert_eq!(v["createDevice"]["address"], "ADDR");
    assert_eq!(v["createDevice"]["type"], "TYPE");
    assert_eq!(v["createDevice"]["protocol"], "PROTO");
}

#[tokio::test]
async fn mutation_update_device_works() {
    let api: Arc<dyn UnrApi> = Arc::new(FakeUnrApi::default());
    let schema = schema_with_api(api.clone());
    let config = testing_config();

    // seed
    api.create_entities(
        &config,
        testing_token(),
        vec![Entity {
            id: "A".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        }],
    )
    .await
    .unwrap();

    let r = schema
            .execute(
                r#"
                mutation {
                  updateDevice(input:{name:"A", address:"ADDR2", type:"TYPE2", protocol:"PROTO2"}) { name }
                }
                "#,
            )
            .await;
    assert!(r.errors.is_empty(), "errors: {:?}", r.errors);

    let v = json_data(r);
    assert_eq!(v["updateDevice"]["name"], "A");
}

#[tokio::test]
async fn mutation_update_device_missing_device_fails() {
    let api: Arc<dyn UnrApi> = Arc::new(FakeUnrApi::default());
    let schema = schema_with_api(api);

    assert_err_starts_with(
            &schema,
            r#"
            mutation {
              updateDevice(input:{name:"MISSING", address:"ADDR2", type:"TYPE2", protocol:"PROTO2"}) { name }
            }
            "#,
            "Device does not exist:",
        )
        .await;
}

#[tokio::test]
async fn mutation_update_device_returns_updated_fields() {
    let api: Arc<dyn UnrApi> = Arc::new(FakeUnrApi::default());
    let schema = schema_with_api(api.clone());
    let config = testing_config();

    // seed
    api.create_entities(
        &config,
        testing_token(),
        vec![Entity {
            id: "A".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        }],
    )
    .await
    .unwrap();

    let r = schema
            .execute(
                r#"
                mutation {
                  updateDevice(input:{name:"A", address:"ADDR2", type:"TYPE2", protocol:"PROTO2"}) {
                    name
                    address
                    type
                    protocol
                  }
                }
                "#,
            )
            .await;
    assert!(r.errors.is_empty(), "errors: {:?}", r.errors);

    let v = json_data(r);
    assert_eq!(v["updateDevice"]["name"], "A");
    assert_eq!(v["updateDevice"]["address"], "ADDR2");
    assert_eq!(v["updateDevice"]["type"], "TYPE2");
    assert_eq!(v["updateDevice"]["protocol"], "PROTO2");
}

#[tokio::test]
async fn mutation_set_children_creates_relationship() {
    let api: Arc<dyn UnrApi> = Arc::new(FakeUnrApi::default());
    let schema = schema_with_api(api.clone());
    let config = testing_config();

    // seed entity so setChildren can prime loader
    api.create_entities(
        &config,
        testing_token(),
        vec![Entity {
            id: "A".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        }],
    )
    .await
    .unwrap();

    let r = schema
        .execute(
            r#"
                mutation {
                  setChildren(parent:"A", children:["C2"]) { name }
                }
                "#,
        )
        .await;
    assert!(r.errors.is_empty(), "errors: {:?}", r.errors);

    let v = json_data(r);
    assert_eq!(v["setChildren"]["name"], "A");

    // verify relationship stored
    let resp = api
        .read_relationships(&config, testing_token(), vec!["A".to_string()])
        .await
        .unwrap();
    let entry = resp.entries.iter().find(|e| e.id == "A").unwrap();
    assert_eq!(entry.children, vec!["C2".to_string()]);
}

#[tokio::test]
async fn mutation_set_children_empty_not_found_is_ok() {
    let api: Arc<dyn UnrApi> = Arc::new(FakeUnrApi::default());
    let schema = schema_with_api(api.clone());
    let config = testing_config();

    // seed entity so setChildren can prime loader
    api.create_entities(
        &config,
        testing_token(),
        vec![Entity {
            id: "A".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        }],
    )
    .await
    .unwrap();

    // deleting when no relationships exist should still succeed
    let r = schema
        .execute(
            r#"
                mutation {
                  setChildren(parent:"A", children:[]) { name }
                }
                "#,
        )
        .await;
    assert!(r.errors.is_empty(), "errors: {:?}", r.errors);
    let v = json_data(r);
    assert_eq!(v["setChildren"]["name"], "A");

    let resp = api
        .read_relationships(&config, testing_token(), vec!["A".to_string()])
        .await
        .unwrap();
    let entry = resp.entries.iter().find(|e| e.id == "A").unwrap();
    assert!(entry.children.is_empty());
}

#[tokio::test]
async fn mutation_set_children_empty_deletes_existing_relationship() {
    let api: Arc<dyn UnrApi> = Arc::new(FakeUnrApi::default());
    let schema = schema_with_api(api.clone());
    let config = testing_config();

    // seed entity so setChildren can prime loader
    api.create_entities(
        &config,
        testing_token(),
        vec![Entity {
            id: "A".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        }],
    )
    .await
    .unwrap();

    // create then delete should remove relationship
    api.create_relationships(
        &config,
        testing_token(),
        vec![Relationship {
            parent_id: "A".to_string(),
            child_id: "C".to_string(),
        }],
    )
    .await
    .unwrap();

    let r = schema
        .execute(
            r#"
                mutation {
                  setChildren(parent:"A", children:[]) { name }
                }
                "#,
        )
        .await;
    assert!(r.errors.is_empty(), "errors: {:?}", r.errors);

    let resp = api
        .read_relationships(&config, testing_token(), vec!["A".to_string()])
        .await
        .unwrap();
    let entry = resp.entries.iter().find(|e| e.id == "A").unwrap();
    assert!(entry.children.is_empty());
}

#[tokio::test]
async fn mutation_set_children_replaces_existing_with_diff() {
    let api = Arc::new(FakeUnrApi::default());
    let schema = schema_with_api(api.clone());
    let config = testing_config();

    // seed entity so setChildren can prime loader
    api.create_entities(
        &config,
        testing_token(),
        vec![Entity {
            id: "A".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        }],
    )
    .await
    .unwrap();

    api.create_relationships(
        &config,
        testing_token(),
        vec![Relationship {
            parent_id: "A".to_string(),
            child_id: "OLD".to_string(),
        }],
    )
    .await
    .unwrap();

    let r = schema
        .execute(
            r#"
                mutation {
                  setChildren(parent:"A", children:["NEW"]) { name }
                }
                "#,
        )
        .await;
    assert!(r.errors.is_empty(), "errors: {:?}", r.errors);

    let resp = api
        .read_relationships(&config, testing_token(), vec!["A".to_string()])
        .await
        .unwrap();
    let entry = resp.entries.iter().find(|e| e.id == "A").unwrap();
    assert_eq!(entry.children, vec!["NEW".to_string()]);
}

#[tokio::test]
async fn mutation_delete_devices_works() {
    let api: Arc<dyn UnrApi> = Arc::new(FakeUnrApi::default());
    let schema = schema_with_api(api.clone());
    let config = testing_config();

    // seed
    api.create_entities(
        &config,
        testing_token(),
        vec![Entity {
            id: "A".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        }],
    )
    .await
    .unwrap();

    let r = schema
        .execute(
            r#"
                mutation {
                  deleteDevices(names:["A"])
                }
                "#,
        )
        .await;
    assert!(r.errors.is_empty(), "errors: {:?}", r.errors);

    let v = json_data(r);
    assert_eq!(v["deleteDevices"][0], "A");

    // verify deleted
    let resp = api
        .read_entities(&config, testing_token(), vec!["A".to_string()])
        .await
        .unwrap();
    assert!(resp.entities.is_empty());
}

#[tokio::test]
async fn mutation_delete_devices_empty_list_fails() {
    let api: Arc<dyn UnrApi> = Arc::new(FakeUnrApi::default());
    let schema = schema_with_api(api);

    assert_err_starts_with(
        &schema,
        r#"
            mutation {
              deleteDevices(names:[])
            }
            "#,
        "deleteDevices requires at least one device name",
    )
    .await;
}

#[tokio::test]
async fn mutation_returns_err_on_bad_connection() {
    let api: Arc<dyn UnrApi> = Arc::new(api::GrpcUnrApi);
    let schema = schema_with_api(api);
    assert_err_starts_with(
            &schema,
            r#"
            mutation {
              createDevice(input: { name: "X", address: "A", type: "T", protocol: "P" }) { name }
            }
            "#,
            "Error creating device.",
        )
        .await;
}
