use std::sync::Mutex;

use async_graphql::InputType;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use rust_grpc_lib::auth::{ForwardedToken, TokenProvider};
use serde_json::{Value, json};
use tokio::sync::Barrier;
use tower::Service;

use crate::graphql::{PARAMETER_PAGE_ENDPOINT, create_parameter_page_router};

use super::{store::StoreResult, *};

const PAGE_FIELDS: &str = r#"
    fragment SavedPage on ParameterPage {
        id title revision
        subSystems {
            title
            tabs {
                title
                subPages {
                    title
                    entries {
                        __typename
                        ... on ParameterPageCommentEntry { text }
                        ... on ParameterPageParameterEntry {
                            drf label proportion isAcnet
                        }
                        ... on ParameterPageMultEntry { numberOfEntries description }
                    }
                }
            }
        }
    }
"#;
const CREATE: &str = r#"
    mutation Create($input: ParameterPageInput!) {
        createParameterPage(input: $input) { ...SavedPage }
    }
"#;
const FETCH: &str = r#"
    query Fetch($id: ID!) { parameterPage(id: $id) { ...SavedPage } }
"#;
const UPDATE: &str = r#"
    mutation Update($id: ID!, $revision: String!, $input: ParameterPageInput!) {
        updateParameterPage(id: $id, expectedRevision: $revision, input: $input) {
            ...SavedPage
        }
    }
"#;

const DELETE: &str = r#"
    mutation Delete($id: ID!, $revision: String!) {
        deleteParameterPage(id: $id, expectedRevision: $revision)
    }
"#;
const TEST_TOKEN: &str = "test-parameter-page-token";

#[derive(Default)]
struct RecordingStore {
    store: InMemoryParameterPageStore,
    calls: Mutex<Vec<(&'static str, String)>>,
}

impl RecordingStore {
    fn record(&self, operation: &'static str, token: &ForwardedToken) {
        self.calls
            .lock()
            .unwrap()
            .push((operation, token.get_token().unwrap()));
    }
}

impl ParameterPageStore for RecordingStore {
    async fn list(
        &self, token: ForwardedToken,
    ) -> StoreResult<Vec<ParameterPageSummary>> {
        self.record("list", &token);
        self.store.list(token).await
    }

    async fn fetch(
        &self, id: ID, token: ForwardedToken,
    ) -> StoreResult<Option<ParameterPage>> {
        self.record("fetch", &token);
        self.store.fetch(id, token).await
    }

    async fn create(
        &self, input: ParameterPageInput, token: ForwardedToken,
    ) -> StoreResult<ParameterPage> {
        self.record("create", &token);
        self.store.create(input, token).await
    }

    async fn update(
        &self, id: ID, expected_revision: &str, input: ParameterPageInput,
        token: ForwardedToken,
    ) -> StoreResult<ParameterPage> {
        self.record("update", &token);
        self.store.update(id, expected_revision, input, token).await
    }

    async fn delete(
        &self, id: ID, expected_revision: &str, token: ForwardedToken,
    ) -> StoreResult<ID> {
        self.record("delete", &token);
        self.store.delete(id, expected_revision, token).await
    }
}

fn fixture(title: &str) -> Value {
    json!({
        "title": title,
        "subSystems": [
            { "title": "Diagnostics", "tabs": [
                { "title": "Readings", "subPages": [
                    { "title": "Active readings", "entries": [
                        { "comment": { "text": "Before the group" } },
                        { "mult": {
                            "numberOfEntries": 1, "description": "Adjustments"
                        } },
                        { "parameter": {
                            "drf": "acsys:example", "label": "Readback",
                            "proportion": -0.25, "isAcnet": false
                        } },
                        { "parameter": { "drf": "S:EXAMPLE" } }
                    ] },
                    { "title": "Titled empty subpage", "entries": [] }
                ] }
            ] },
            { "title": "Reference", "tabs": [
                { "title": "Notes", "subPages": [{ "title": "", "entries": [] }] }
            ] }
        ]
    })
}

fn store_input(title: &str) -> ParameterPageInput {
    ParameterPageInput::parse(Some(
        async_graphql::Value::from_json(fixture(title)).unwrap(),
    ))
    .unwrap()
}

async fn post(router: &mut Router, query: &str, variables: Value) -> Value {
    let request = Request::post(PARAMETER_PAGE_ENDPOINT)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .body(Body::from(
            json!({ "query": query, "variables": variables }).to_string(),
        ))
        .unwrap();
    let response = router.call(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn response_data(response: &Value) -> &Value {
    assert!(response.get("errors").is_none(), "{response}");
    &response["data"]
}

fn testing_token() -> ForwardedToken {
    ForwardedToken::new("")
}

#[tokio::test]
async fn store_lists_summaries_in_title_order_and_fetches_exact_ids() {
    let store = InMemoryParameterPageStore::default();
    assert!(store.list(testing_token()).await.unwrap().is_empty());

    let last = store
        .create(store_input("Zeta"), testing_token())
        .await
        .unwrap();
    let first = store
        .create(store_input("Alpha"), testing_token())
        .await
        .unwrap();
    let ids: Vec<_> = store
        .list(testing_token())
        .await
        .unwrap()
        .into_iter()
        .map(|page| page.id)
        .collect();
    assert_eq!(ids, [first.id.clone(), last.id]);
    assert_eq!(
        store
            .fetch(first.id.clone(), testing_token())
            .await
            .unwrap(),
        Some(first.clone())
    );
    let prefix = ID::from(first.id.as_str()[..8].to_owned());
    assert_eq!(store.fetch(prefix, testing_token()).await.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_updates_accept_only_one_write_at_the_same_revision() {
    let store = Arc::new(InMemoryParameterPageStore::default());
    let original = store
        .create(store_input("Original"), testing_token())
        .await
        .unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let spawn_update = |title| {
        let store = Arc::clone(&store);
        let id = original.id.clone();
        let revision = original.revision.clone();
        let input = store_input(title);
        let barrier = Arc::clone(&barrier);
        tokio::spawn(async move {
            barrier.wait().await;
            store.update(id, &revision, input, testing_token()).await
        })
    };

    let (left, right) =
        tokio::join!(spawn_update("Left"), spawn_update("Right"));
    let (saved, rejected) = match (left.unwrap(), right.unwrap()) {
        (Ok(saved), Err(rejected)) | (Err(rejected), Ok(saved)) => {
            (saved, rejected)
        }
        outcomes => panic!(
            "Expected one successful write and one conflict: {outcomes:?}"
        ),
    };
    assert_eq!(rejected, StoreError::Conflict);
    assert_ne!(saved.revision, original.revision);
    assert_eq!(
        store.fetch(original.id, testing_token()).await.unwrap(),
        Some(saved),
    );
}

#[tokio::test]
async fn parameter_page_round_trips_and_can_be_updated_and_deleted_over_http() {
    let store = Arc::new(RecordingStore::default());
    let mut router = create_parameter_page_router(store.clone());
    let list = "{ parameterPages { id title } }";
    let response = post(&mut router, list, json!({})).await;
    assert_eq!(response_data(&response)["parameterPages"], json!([]));

    let input = fixture("Example page");
    let response = post(
        &mut router,
        &[CREATE, PAGE_FIELDS].concat(),
        json!({ "input": input }),
    )
    .await;
    let created = &response_data(&response)["createParameterPage"];
    let id = created["id"].as_str().unwrap();
    let revision = created["revision"].as_str().unwrap();
    assert!(!id.is_empty() && !revision.is_empty());
    assert_eq!(created["title"], "Example page");
    let subpages = &created["subSystems"][0]["tabs"][0]["subPages"];
    assert_eq!(
        subpages[1],
        json!({ "title": "Titled empty subpage", "entries": [] })
    );
    assert_eq!(
        subpages[0]["entries"],
        json!([
            { "__typename": "ParameterPageCommentEntry", "text": "Before the group" },
            {
                "__typename": "ParameterPageMultEntry",
                "numberOfEntries": 1, "description": "Adjustments"
            },
            {
                "__typename": "ParameterPageParameterEntry", "drf": "acsys:example",
                "label": "Readback", "proportion": -0.25, "isAcnet": false
            },
            {
                "__typename": "ParameterPageParameterEntry", "drf": "S:EXAMPLE",
                "label": null, "proportion": 1.0, "isAcnet": true
            }
        ])
    );

    let response = post(&mut router, list, json!({})).await;
    assert_eq!(
        response_data(&response)["parameterPages"],
        json!([{ "id": id, "title": "Example page" }]),
    );
    let response = post(
        &mut router,
        &[FETCH, PAGE_FIELDS].concat(),
        json!({ "id": id }),
    )
    .await;
    assert_eq!(&response_data(&response)["parameterPage"], created);

    let mut edited = fixture("Renamed");
    edited["subSystems"].as_array_mut().unwrap().reverse();
    let response = post(
        &mut router,
        &[UPDATE, PAGE_FIELDS].concat(),
        json!({ "id": id, "revision": revision, "input": edited }),
    )
    .await;
    let updated = &response_data(&response)["updateParameterPage"];
    assert_eq!(updated["id"], id);
    assert_eq!(updated["title"], "Renamed");
    assert_ne!(updated["revision"], revision);
    assert_eq!(updated["subSystems"][0]["title"], "Reference");
    assert_eq!(updated["subSystems"][1], created["subSystems"][0]);

    let stale_delete = post(
        &mut router,
        DELETE,
        json!({ "id": id, "revision": revision }),
    )
    .await;
    assert_eq!(stale_delete["errors"][0]["extensions"]["code"], "CONFLICT");

    let response = post(
        &mut router,
        &[FETCH, PAGE_FIELDS].concat(),
        json!({ "id": id }),
    )
    .await;
    assert_eq!(&response_data(&response)["parameterPage"], updated);

    let response = post(
        &mut router,
        DELETE,
        json!({ "id": id, "revision": updated["revision"] }),
    )
    .await;
    assert_eq!(response_data(&response)["deleteParameterPage"], id);
    let response = post(
        &mut router,
        "query($id: ID!) {
            parameterPage(id: $id) { id }
            parameterPages { id }
        }",
        json!({ "id": id }),
    )
    .await;
    assert_eq!(
        response_data(&response),
        &json!({ "parameterPage": null, "parameterPages": [] })
    );

    let missing_delete = post(
        &mut router,
        DELETE,
        json!({ "id": id, "revision": updated["revision"] }),
    )
    .await;
    assert_eq!(
        missing_delete["errors"][0]["extensions"]["code"],
        "NOT_FOUND"
    );

    let calls = store.calls.lock().unwrap();
    for operation in ["list", "fetch", "create", "update", "delete"] {
        assert!(calls.iter().any(|(called, _)| *called == operation));
    }
    assert!(calls.iter().all(|(_, token)| token == TEST_TOKEN));
}

#[tokio::test]
async fn parameter_page_rejects_invalid_hierarchies_and_stale_or_missing_writes()
 {
    let mut router = create_parameter_page_router(Arc::new(
        InMemoryParameterPageStore::default(),
    ));
    let create = [CREATE, PAGE_FIELDS].concat();
    let mut no_tabs = fixture("No tabs");
    no_tabs["subSystems"][0]["tabs"] = json!([]);
    let mut no_subpages = fixture("No subpages");
    no_subpages["subSystems"][0]["tabs"][0]["subPages"] = json!([]);
    for invalid in [
        json!({ "title": "No subsystems", "subSystems": [] }),
        no_tabs,
        no_subpages,
    ] {
        let response =
            post(&mut router, &create, json!({ "input": invalid })).await;
        assert_eq!(
            response["errors"][0]["extensions"]["code"],
            "BAD_USER_INPUT"
        );
    }
    let response =
        post(&mut router, "{ parameterPages { id } }", json!({})).await;
    assert_eq!(response_data(&response)["parameterPages"], json!([]));

    let response = post(
        &mut router,
        &create,
        json!({ "input": fixture("Original") }),
    )
    .await;
    let created = &response_data(&response)["createParameterPage"];
    let update = [UPDATE, PAGE_FIELDS].concat();
    for (id, revision, error_code) in [
        (&created["id"], json!("stale"), "CONFLICT"),
        (&json!("missing"), created["revision"].clone(), "NOT_FOUND"),
    ] {
        let response = post(
            &mut router, &update,
            json!({ "id": id, "revision": revision, "input": fixture("Rejected") }),
        ).await;
        assert_eq!(response["errors"][0]["extensions"]["code"], error_code);
    }
    let response = post(
        &mut router,
        &[FETCH, PAGE_FIELDS].concat(),
        json!({ "id": created["id"] }),
    )
    .await;
    assert_eq!(&response_data(&response)["parameterPage"], created);
}

fn with_entries(entries: Value) -> Value {
    let mut input = fixture("Entry validation");
    input["subSystems"][0]["tabs"][0]["subPages"][0]["entries"] = entries;
    input
}

#[tokio::test]
async fn parameter_page_validates_mult_group_boundaries_and_drfs() {
    let mut router = create_parameter_page_router(Arc::new(
        InMemoryParameterPageStore::default(),
    ));
    let create = [CREATE, PAGE_FIELDS].concat();
    for (entries, message) in [
        (
            json!([{ "mult": { "numberOfEntries": -1 } }]),
            "A Mult entry's numberOfEntries must be nonnegative.",
        ),
        (
            json!([{ "mult": { "numberOfEntries": 1 } }]),
            "A Mult group must fit within its subpage.",
        ),
        (
            json!([
                { "mult": { "numberOfEntries": 1 } },
                { "mult": { "numberOfEntries": 0 } },
            ]),
            "Mult groups cannot overlap.",
        ),
        (
            json!([{ "parameter": { "drf": "  \t" } }]),
            "A parameter entry's drf must be nonblank.",
        ),
    ] {
        let response = post(
            &mut router,
            &create,
            json!({ "input": with_entries(entries) }),
        )
        .await;
        let error = &response["errors"][0];
        assert_eq!(error["extensions"]["code"], "BAD_USER_INPUT");
        assert_eq!(error["message"], message);
    }

    let valid = with_entries(json!([
        { "mult": { "numberOfEntries": 0 } },
        { "mult": { "numberOfEntries": 1 } },
        { "parameter": { "drf": "S:EXAMPLE" } },
    ]));
    let response = post(&mut router, &create, json!({ "input": valid })).await;
    let created = &response_data(&response)["createParameterPage"];
    let response = post(
        &mut router,
        &[UPDATE, PAGE_FIELDS].concat(),
        json!({
            "id": created["id"],
            "revision": created["revision"],
            "input": with_entries(json!([{ "parameter": { "drf": "" } }])),
        }),
    )
    .await;
    assert_eq!(
        response["errors"][0]["extensions"]["code"],
        "BAD_USER_INPUT"
    );

    let response = post(
        &mut router,
        &[FETCH, PAGE_FIELDS].concat(),
        json!({ "id": created["id"] }),
    )
    .await;
    assert_eq!(&response_data(&response)["parameterPage"], created);
}

#[tokio::test]
async fn parameter_page_entry_one_of_is_enforced_by_graphql() {
    let mut router = create_parameter_page_router(Arc::new(
        InMemoryParameterPageStore::default(),
    ));
    let input = with_entries(json!([{
        "comment": { "text": "Ambiguous entry" },
        "parameter": { "drf": "S:EXAMPLE" },
    }]));
    let response = post(
        &mut router,
        &[CREATE, PAGE_FIELDS].concat(),
        json!({ "input": input }),
    )
    .await;
    let error = &response["errors"][0];
    assert!(error.is_object(), "{response}");
    assert!(error["extensions"]["code"].is_null(), "{error}");
}
