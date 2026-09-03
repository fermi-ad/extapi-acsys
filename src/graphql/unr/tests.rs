use super::*;

#[cfg(test)]
mod unr_tests {
    use super::*;
    use crate::g_rpc::proto::{
        google::protobuf::Empty,
        services::unr::{BaseResponse, RelationshipResponse},
    };
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
        base: Mutex<HashMap<String, BaseInfo>>,
        rel: Mutex<HashMap<String, Vec<String>>>,

        /// Failure injection for tests.
        ///
        /// Semantics:
        /// - `None` => don't fail
        /// - `Some((0, code))` => fail the next UNR call with `code`
        /// - `Some((n, code))` where `n > 0` => succeed next `n` calls, then fail with `code`
        fail_after: Mutex<Option<(usize, Code)>>,

        /// Count how many times `read_base_info` was called.
        read_base_info_calls: Mutex<usize>,
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

    #[async_trait::async_trait]
    impl UnrApi for FakeUnrApi {
        async fn create_base_info(
            &self, base_info: BaseInfo,
        ) -> Result<Empty, Status> {
            self.check_fail()?;

            let mut base = self.base.lock().unwrap();
            if base.contains_key(&base_info.device_name) {
                return Err(Status::new(Code::AlreadyExists, "exists"));
            }
            base.insert(base_info.device_name.clone(), base_info);
            Ok(Empty {})
        }

        async fn read_base_info(
            &self, device_names: Vec<String>,
        ) -> Result<BaseResponse, Status> {
            self.check_fail()?;

            *self.read_base_info_calls.lock().unwrap() += 1;

            let base = self.base.lock().unwrap();
            let mut out: Vec<BaseInfo> = Vec::new();

            if device_names.is_empty() {
                out.extend(base.values().cloned());
            } else {
                for n in device_names {
                    if let Some(bi) = base.get(&n) {
                        out.push(bi.clone());
                    }
                }
            }

            Ok(BaseResponse { base_info: out })
        }

        async fn update_base_info(
            &self, base_info: BaseInfo,
        ) -> Result<Empty, Status> {
            self.check_fail()?;

            let mut base = self.base.lock().unwrap();
            if base.contains_key(&base_info.device_name) {
                base.insert(base_info.device_name.clone(), base_info);
            }
            Ok(Empty {})
        }

        async fn delete_base_info(
            &self, device_names: Vec<String>,
        ) -> Result<Empty, Status> {
            self.check_fail()?;

            let mut base = self.base.lock().unwrap();
            for n in device_names {
                base.remove(&n);
            }
            Ok(Empty {})
        }

        async fn read_relationships(
            &self, parent_name: String,
        ) -> Result<RelationshipResponse, Status> {
            self.check_fail()?;

            let rel = self.rel.lock().unwrap();
            let children = rel.get(&parent_name).cloned().unwrap_or_default();

            Ok(RelationshipResponse {
                relationship_info: Some(
                    crate::g_rpc::proto::services::unr::RelationshipInfo {
                        parent_name,
                        children_names: children,
                    },
                ),
            })
        }

        async fn update_relationships(
            &self,
            relationship_info: crate::g_rpc::proto::services::unr::RelationshipInfo,
        ) -> Result<Empty, Status> {
            self.check_fail()?;

            let mut rel = self.rel.lock().unwrap();
            rel.insert(
                relationship_info.parent_name,
                relationship_info.children_names,
            );
            Ok(Empty {})
        }

        async fn delete_relationships(
            &self, parent_name: String,
        ) -> Result<Empty, Status> {
            self.check_fail()?;

            let mut rel = self.rel.lock().unwrap();
            let _ = rel.remove(&parent_name);
            Ok(Empty {})
        }
    }

    fn schema_with_api(
        api: Arc<dyn UnrApi>,
    ) -> Schema<UnrQueries, UnrMutations, EmptySubscription> {
        Schema::build(UnrQueries, UnrMutations, EmptySubscription)
            .data(api.clone())
            // Tests execute the schema directly (bypassing the HTTP handler), so
            // attach a loader here to emulate request-scoped injection.
            .data(DataLoader::with_cache(
                loader::UnrBaseInfoLoader::new(api),
                tokio::spawn,
                HashMapCache::default(),
            ))
            .finish()
    }

    fn json_data(result: async_graphql::Response) -> Value {
        serde_json::to_value(result.data).expect("response data is JSON")
    }

    fn mk_request_scoped_loader(
        api: Arc<dyn UnrApi>,
    ) -> DataLoader<loader::UnrBaseInfoLoader, HashMapCache> {
        DataLoader::with_cache(
            loader::UnrBaseInfoLoader::new(api),
            tokio::spawn,
            HashMapCache::default(),
        )
    }

    fn mk_unr_router_with_api(api: Arc<dyn UnrApi>) -> axum::Router {
        // Use the same UNR router wiring as production.
        crate::graphql::create_unr_router_with_api(api)
    }

    async fn assert_err_starts_with(
        schema: &Schema<UnrQueries, UnrMutations, EmptySubscription>,
        gql: &str, prefix: &str,
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
        let err = super::handle_error(e, "creating device");
        assert!(err.message.contains("bad input"));
        assert!(err.message.contains("Error ID:"));
    }

    #[test]
    fn handle_error_non_invalid_argument_is_generic() {
        let e = Status::new(Code::Unavailable, "transport details");
        let err = super::handle_error(e, "creating device");
        assert!(err.message.starts_with("Error creating device."));
        assert!(err.message.contains("Error ID:"));
        assert!(!err.message.contains("transport details"));
    }

    #[tokio::test]
    async fn set_children_empty_is_idempotent() {
        let api = Arc::new(FakeUnrApi::default());
        // no relationships exist
        let got =
            super::set_children_impl(api.as_ref(), "P".to_string(), vec![])
                .await;
        assert!(got.is_ok());
        assert_eq!(got.unwrap().name, "P");
    }

    #[tokio::test]
    async fn set_children_non_empty_creates_relationships() {
        let api = Arc::new(FakeUnrApi::default());
        let got = super::set_children_impl(
            api.as_ref(),
            "P".to_string(),
            vec!["C1".to_string(), "C2".to_string()],
        )
        .await
        .unwrap();
        assert_eq!(got.name, "P");

        let rel = api.read_relationships("P".to_string()).await.unwrap();
        assert_eq!(
            rel.relationship_info.unwrap().children_names,
            vec!["C1".to_string(), "C2".to_string()]
        );
    }

    #[tokio::test]
    async fn set_children_existing_relationship_is_replaced() {
        let api = Arc::new(FakeUnrApi::default());

        // pre-create relationship
        api.update_relationships(
            crate::g_rpc::proto::services::unr::RelationshipInfo {
                parent_name: "P".to_string(),
                children_names: vec!["OLD".to_string()],
            },
        )
        .await
        .unwrap();

        super::set_children_impl(
            api.as_ref(),
            "P".to_string(),
            vec!["NEW".to_string()],
        )
        .await
        .unwrap();

        let rel = api.read_relationships("P".to_string()).await.unwrap();
        assert_eq!(
            rel.relationship_info.unwrap().children_names,
            vec!["NEW".to_string()]
        );
    }

    #[tokio::test]
    async fn loader_dedupes_keys_and_maps_by_device_name() {
        let api = Arc::new(FakeUnrApi::default());
        api.create_base_info(BaseInfo {
            device_name: "A".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        })
        .await
        .unwrap();

        let loader = loader::UnrBaseInfoLoader::new(api);
        let out = loader
            .load(&["A".to_string(), "A".to_string(), "B".to_string()])
            .await
            .unwrap();
        assert!(out.contains_key("A"));
        assert!(!out.contains_key("B"));
    }

    #[tokio::test]
    async fn dataloader_cache_is_used_within_single_request() {
        let api = Arc::new(FakeUnrApi::default());
        api.create_base_info(BaseInfo {
            device_name: "D".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        })
        .await
        .unwrap();

        let schema = schema_with_api(api.clone());

        // Attach a request-scoped loader and query multiple BaseInfo-backed fields.
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

        // Expect exactly one UNR base-info fetch for the whole request.
        assert_eq!(*api.read_base_info_calls.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn dataloader_cache_does_not_carry_over_across_requests() {
        let api = Arc::new(FakeUnrApi::default());
        api.create_base_info(BaseInfo {
            device_name: "D".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        })
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

        assert_eq!(*api.read_base_info_calls.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn http_handler_injects_request_scoped_loader_no_cache_bleed() {
        let api = Arc::new(FakeUnrApi::default());
        api.create_base_info(BaseInfo {
            device_name: "D".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        })
        .await
        .unwrap();

        let mut app = mk_unr_router_with_api(api.clone());

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
        assert_eq!(*api.read_base_info_calls.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn device_fields_resolve_from_loader_and_strip_empty() {
        let api = Arc::new(FakeUnrApi::default());
        api.create_base_info(BaseInfo {
            device_name: "D".to_string(),
            address: "".to_string(),
            r#type: "T".to_string(),
            protocol: "".to_string(),
        })
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

        // devices() validates existence via BaseInfo; ensure parent exists.
        api.create_base_info(BaseInfo {
            device_name: "P".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        })
        .await
        .unwrap();

        api.update_relationships(
            crate::g_rpc::proto::services::unr::RelationshipInfo {
                parent_name: "P".to_string(),
                children_names: vec!["C".to_string()],
            },
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
    async fn devices_query_returns_not_found_for_requested_name() {
        let api = Arc::new(FakeUnrApi::default());
        api.create_base_info(BaseInfo {
            device_name: "A".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        })
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
        for n in ["A", "B"] {
            api.create_base_info(BaseInfo {
                device_name: n.to_string(),
                address: "ADDR".to_string(),
                r#type: "TYPE".to_string(),
                protocol: "PROTO".to_string(),
            })
            .await
            .unwrap();
        }

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

        // Seed child so createDevice's child pre-validation passes.
        api.create_base_info(BaseInfo {
            device_name: "C".to_string(),
            address: "".to_string(),
            r#type: "".to_string(),
            protocol: "".to_string(),
        })
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
    async fn mutation_create_device_missing_child_fails_without_creating_parent()
     {
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

        // Ensure parent was not created.
        let resp = api.read_base_info(vec!["A".to_string()]).await.unwrap();
        assert!(resp.base_info.is_empty());
    }

    #[tokio::test]
    async fn mutation_create_device_relationship_failure_includes_partial_success_extensions()
     {
        let api = Arc::new(FakeUnrApi::default());
        let schema = schema_with_api(api.clone());

        // Seed child so pre-validation passes.
        api.create_base_info(BaseInfo {
            device_name: "C".to_string(),
            address: "".to_string(),
            r#type: "".to_string(),
            protocol: "".to_string(),
        })
        .await
        .unwrap();

        // Force the relationship update to fail.
        // Call order inside createDevice (when children are provided):
        // 1) read_base_info (child pre-validation)
        // 2) create_base_info (parent)
        // 3) update_relationships (set children)
        *api.fail_after.lock().unwrap() = Some((2, Code::InvalidArgument));

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

        // BaseInfo was created successfully.
        let resp = api.read_base_info(vec!["A".to_string()]).await.unwrap();
        assert_eq!(resp.base_info.len(), 1);
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

        // seed
        api.create_base_info(BaseInfo {
            device_name: "A".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        })
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

        // seed
        api.create_base_info(BaseInfo {
            device_name: "A".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        })
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

        // seed base info so setChildren can prime loader
        api.create_base_info(BaseInfo {
            device_name: "A".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        })
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
        let rel = api.read_relationships("A".to_string()).await.unwrap();
        assert_eq!(
            rel.relationship_info.unwrap().children_names,
            vec!["C2".to_string()]
        );
    }

    #[tokio::test]
    async fn mutation_set_children_empty_not_found_is_ok() {
        let api: Arc<dyn UnrApi> = Arc::new(FakeUnrApi::default());
        let schema = schema_with_api(api.clone());

        // seed base info so setChildren can prime loader
        api.create_base_info(BaseInfo {
            device_name: "A".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        })
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

        let rel = api.read_relationships("A".to_string()).await.unwrap();
        assert_eq!(
            rel.relationship_info.unwrap().children_names,
            Vec::<String>::new()
        );
    }

    #[tokio::test]
    async fn mutation_set_children_empty_deletes_existing_relationship() {
        let api: Arc<dyn UnrApi> = Arc::new(FakeUnrApi::default());
        let schema = schema_with_api(api.clone());

        // seed base info so setChildren can prime loader
        api.create_base_info(BaseInfo {
            device_name: "A".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        })
        .await
        .unwrap();

        // create then delete should remove relationship
        api.update_relationships(
            crate::g_rpc::proto::services::unr::RelationshipInfo {
                parent_name: "A".to_string(),
                children_names: vec!["C".to_string()],
            },
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

        let rel = api.read_relationships("A".to_string()).await.unwrap();
        assert_eq!(
            rel.relationship_info.unwrap().children_names,
            Vec::<String>::new()
        );
    }

    #[tokio::test]
    async fn mutation_set_children_already_exists_falls_back_to_update() {
        let api = Arc::new(FakeUnrApi::default());
        let schema = schema_with_api(api.clone());

        // seed base info so setChildren can prime loader
        api.create_base_info(BaseInfo {
            device_name: "A".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        })
        .await
        .unwrap();

        // pre-create relationship so create_relationships returns AlreadyExists
        api.update_relationships(
            crate::g_rpc::proto::services::unr::RelationshipInfo {
                parent_name: "A".to_string(),
                children_names: vec!["OLD".to_string()],
            },
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

        let rel = api.read_relationships("A".to_string()).await.unwrap();
        assert_eq!(
            rel.relationship_info.unwrap().children_names,
            vec!["NEW".to_string()]
        );
    }

    #[tokio::test]
    async fn mutation_delete_devices_works() {
        let api: Arc<dyn UnrApi> = Arc::new(FakeUnrApi::default());
        let schema = schema_with_api(api.clone());

        // seed
        api.create_base_info(BaseInfo {
            device_name: "A".to_string(),
            address: "ADDR".to_string(),
            r#type: "TYPE".to_string(),
            protocol: "PROTO".to_string(),
        })
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
        let resp = api.read_base_info(vec!["A".to_string()]).await.unwrap();
        assert!(resp.base_info.is_empty());
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
}
