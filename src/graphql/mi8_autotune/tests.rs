use std::sync::Mutex;

use async_graphql::{Schema, value};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use futures_util::stream;
use rust_grpc_lib::auth::TokenProvider;
use serde_json::json;
use tower::Service;

use super::*;
use crate::{
    config::{Upstream, get_test_config},
    graphql::auth_handlers::AuthInfo,
};

struct FakeApi {
    response: std::result::Result<ListBpmsResponse, Status>,
    calls: Mutex<Vec<(String, String)>>,
    mutation_response: Mutex<std::result::Result<Empty, Status>>,
    mutations: Mutex<Vec<(&'static str, i32)>>,
    armed: Mutex<
        Option<
            std::result::Result<
                StatusStream<SubscribeArmedStateResponse>,
                Status,
            >,
        >,
    >,
    mode: Mutex<
        Option<
            std::result::Result<
                StatusStream<SubscribeOperatingModeResponse>,
                Status,
            >,
        >,
    >,
    events: Mutex<
        Option<
            std::result::Result<
                StatusStream<SubscribeAutotuneEventResponse>,
                Status,
            >,
        >,
    >,
}

impl FakeApi {
    fn new(response: std::result::Result<ListBpmsResponse, Status>) -> Self {
        Self {
            response,
            calls: Mutex::new(Vec::new()),
            mutation_response: Mutex::new(Ok(Empty {})),
            mutations: Mutex::new(Vec::new()),
            armed: Mutex::new(Some(Ok(stream::empty().boxed()))),
            mode: Mutex::new(Some(Ok(stream::empty().boxed()))),
            events: Mutex::new(Some(Ok(stream::empty().boxed()))),
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

    async fn set_armed_state(
        &self, config: &GrpcConfig, token: ForwardedToken,
        request: SetArmedStateRequest,
    ) -> std::result::Result<Empty, Status> {
        self.calls
            .lock()
            .unwrap()
            .push((config.host_addr.clone(), token.get_token().unwrap()));
        self.mutations
            .lock()
            .unwrap()
            .push(("armed", request.armed_state));
        self.mutation_response.lock().unwrap().clone()
    }

    async fn set_operating_mode(
        &self, config: &GrpcConfig, token: ForwardedToken,
        request: SetOperatingModeRequest,
    ) -> std::result::Result<Empty, Status> {
        self.calls
            .lock()
            .unwrap()
            .push((config.host_addr.clone(), token.get_token().unwrap()));
        self.mutations
            .lock()
            .unwrap()
            .push(("mode", request.operating_mode));
        self.mutation_response.lock().unwrap().clone()
    }

    async fn subscribe_armed_state(
        &self, config: &GrpcConfig, token: ForwardedToken,
    ) -> std::result::Result<StatusStream<SubscribeArmedStateResponse>, Status>
    {
        self.calls
            .lock()
            .unwrap()
            .push((config.host_addr.clone(), token.get_token().unwrap()));
        self.armed.lock().unwrap().take().unwrap()
    }

    async fn subscribe_operating_mode(
        &self, config: &GrpcConfig, token: ForwardedToken,
    ) -> std::result::Result<StatusStream<SubscribeOperatingModeResponse>, Status>
    {
        self.calls
            .lock()
            .unwrap()
            .push((config.host_addr.clone(), token.get_token().unwrap()));
        self.mode.lock().unwrap().take().unwrap()
    }

    async fn subscribe_events(
        &self, config: &GrpcConfig, token: ForwardedToken,
    ) -> std::result::Result<StatusStream<SubscribeAutotuneEventResponse>, Status>
    {
        self.calls
            .lock()
            .unwrap()
            .push((config.host_addr.clone(), token.get_token().unwrap()));
        self.events.lock().unwrap().take().unwrap()
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
) -> Schema<Mi8AutotuneQueries, Mi8AutotuneMutations, Mi8AutotuneSubscriptions>
{
    Schema::build(
        Mi8AutotuneQueries,
        Mi8AutotuneMutations,
        Mi8AutotuneSubscriptions,
    )
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
fn schema_exposes_mutations_with_non_null_bpms_and_subscriptions() {
    let api = Arc::new(FakeApi::new(Ok(ListBpmsResponse { bpms: vec![] })));
    let sdl = schema(config(), api).sdl();

    assert!(sdl.contains("bpms: [String!]!"), "{sdl}");
    assert!(sdl.contains("mutation: Mi8AutotuneMutations"), "{sdl}");
    assert!(
        sdl.contains("setArmedState(armedState: ArmedState!): Boolean!"),
        "{sdl}"
    );
    assert!(
        sdl.contains(
            "setOperatingMode(operatingMode: OperatingMode!): Boolean!"
        ),
        "{sdl}"
    );
    assert!(
        sdl.contains("subscription: Mi8AutotuneSubscriptions"),
        "{sdl}"
    );
    for field in [
        "armedState: ArmedState!",
        "operatingMode: OperatingMode!",
        "events: AutotuneEventUpdate!",
        "autotuneEvent: AutotuneEvent!",
        "errorMessage: String!",
    ] {
        assert!(sdl.contains(field), "{sdl}");
    }
    assert!(
        !sdl.lines().any(|line| {
            let field = line.trim_start();
            field.starts_with("settings:") || field.starts_with("settings(")
        }),
        "{sdl}"
    );
}

#[test]
fn enum_mappings_cover_every_known_value_and_unknown_numbers() {
    for (number, expected) in
        [ArmedState::Unknown, ArmedState::Armed, ArmedState::Disarmed]
            .into_iter()
            .enumerate()
    {
        assert_eq!(ArmedState::from(number as i32), expected);
    }
    for (number, expected) in [
        OperatingMode::Unknown,
        OperatingMode::Live,
        OperatingMode::Shadow,
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(OperatingMode::from(number as i32), expected);
    }
    for (number, expected) in [
        types::AutotuneEvent::Unknown,
        types::AutotuneEvent::NoAuto,
        types::AutotuneEvent::FirstPass,
        types::AutotuneEvent::Move,
        types::AutotuneEvent::MovePending,
        types::AutotuneEvent::DaqError,
        types::AutotuneEvent::BigMove,
        types::AutotuneEvent::VernierMove,
        types::AutotuneEvent::BeamOk,
        types::AutotuneEvent::BigSetting,
        types::AutotuneEvent::IntensityLow,
        types::AutotuneEvent::AuxiliaryBad,
        types::AutotuneEvent::SettingError,
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(types::AutotuneEvent::from(number as i32), expected);
    }
    for number in [-1, 99, i32::MIN, i32::MAX] {
        assert_eq!(ArmedState::from(number), ArmedState::Unknown);
        assert_eq!(OperatingMode::from(number), OperatingMode::Unknown);
        assert_eq!(
            types::AutotuneEvent::from(number),
            types::AutotuneEvent::Unknown
        );
    }
}

fn empty_api() -> Arc<FakeApi> {
    Arc::new(FakeApi::new(Ok(ListBpmsResponse { bpms: vec![] })))
}

const MUTATIONS: [&str; 2] = [
    "mutation { setArmedState(armedState: ARMED) }",
    "mutation { setOperatingMode(operatingMode: LIVE) }",
];

#[tokio::test]
async fn mutations_map_valid_inputs_and_acknowledge_success() {
    let api = empty_api();
    let schema = schema(config(), api.clone());
    for (operation, expected) in [
        (
            "mutation { setArmedState(armedState: ARMED) }",
            value!({"setArmedState": true}),
        ),
        (
            "mutation { setArmedState(armedState: DISARMED) }",
            value!({"setArmedState": true}),
        ),
        (
            "mutation { setOperatingMode(operatingMode: LIVE) }",
            value!({"setOperatingMode": true}),
        ),
        (
            "mutation { setOperatingMode(operatingMode: SHADOW) }",
            value!({"setOperatingMode": true}),
        ),
    ] {
        let response = schema.execute(operation).await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        assert_eq!(response.data, expected);
    }
    assert_eq!(
        *api.mutations.lock().unwrap(),
        [("armed", 1), ("armed", 2), ("mode", 1), ("mode", 2)]
    );
    assert_eq!(api.calls.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn mutations_reject_unknown_without_calling_upstream() {
    let api = empty_api();
    let schema = schema(config(), api.clone());
    for operation in [
        "mutation { setArmedState(armedState: UNKNOWN) }",
        "mutation { setOperatingMode(operatingMode: UNKNOWN) }",
    ] {
        let response = schema.execute(operation).await;
        assert_eq!(response.errors.len(), 1);
        assert!(
            response.errors[0]
                .message
                .starts_with("UNKNOWN is not a valid")
        );
        assert_eq!(response.data, value!(null));
    }
    assert!(api.calls.lock().unwrap().is_empty());
    assert!(api.mutations.lock().unwrap().is_empty());
}

#[tokio::test]
async fn disabled_mutations_do_not_call_upstream() {
    let api = empty_api();
    let mut config = config();
    config.mi8_autotune =
        Upstream::disabled("MI8 Autotune", ["MI8_AUTOTUNE_GRPC_HOST"]);
    let schema = schema(config, api.clone());
    for operation in MUTATIONS {
        let response = schema.execute(operation).await;
        assert_eq!(response.errors.len(), 1);
        assert_eq!(
            response.errors[0].extensions.as_ref().unwrap().get("code"),
            Some(&value!("FEATURE_DISABLED"))
        );
        assert_eq!(response.data, value!(null));
    }
    assert!(api.calls.lock().unwrap().is_empty());
    assert!(api.mutations.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mutation_upstream_errors_are_safe_and_do_not_report_success() {
    for status in [
        Status::internal("private token=secret"),
        Status::unauthenticated("private auth"),
        Status::permission_denied("private authorization"),
        Status::unavailable("private host"),
    ] {
        let api = empty_api();
        *api.mutation_response.lock().unwrap() = Err(status);
        let schema = schema(config(), api.clone());
        for operation in MUTATIONS {
            let response = schema.execute(operation).await;
            assert_safe_error(&response);
            assert_eq!(response.data, value!(null));
        }
        assert_eq!(api.calls.lock().unwrap().len(), 2);
        assert_eq!(*api.mutations.lock().unwrap(), [("armed", 1), ("mode", 1)]);
    }
}

#[tokio::test]
async fn http_mutations_forward_each_callers_token_and_config() {
    let api = empty_api();
    let site = crate::graphql::create_mi8_autotune_router_with_api(
        Arc::new(config()),
        api.clone(),
    );
    for token in [Some("first-caller"), Some("second-caller"), None] {
        for (operation, field) in MUTATIONS
            .into_iter()
            .zip(["setArmedState", "setOperatingMode"])
        {
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
                .call(
                    request
                        .body(Body::from(
                            json!({"query": operation}).to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = to_bytes(response.into_body(), 4096).await.unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
                json!({"data": {field: true}})
            );
        }
    }
    let calls = api.calls.lock().unwrap();
    assert_eq!(calls.len(), 6);
    for ((host, actual_token), expected_token) in calls.iter().zip([
        "first-caller",
        "first-caller",
        "second-caller",
        "second-caller",
        "",
        "",
    ]) {
        assert_eq!(host, "http://mi8-autotune.test:50051");
        assert_eq!(actual_token, expected_token);
    }
    assert_eq!(
        *api.mutations.lock().unwrap(),
        [("armed", 1), ("mode", 1)].repeat(3)
    );
}

const OPERATIONS: [&str; 3] = [
    "subscription { armedState }",
    "subscription { operatingMode }",
    "subscription { events { autotuneEvent errorMessage } }",
];

#[tokio::test]
async fn subscriptions_forward_initial_and_subsequent_messages_then_complete() {
    let api = empty_api();
    *api.armed.lock().unwrap() =
        Some(Ok(stream::iter([2, 1, 99].map(|armed_state| {
            Ok(SubscribeArmedStateResponse { armed_state })
        }))
        .boxed()));
    *api.mode.lock().unwrap() =
        Some(Ok(stream::iter([2, 1, -1].map(|operating_mode| {
            Ok(SubscribeOperatingModeResponse { operating_mode })
        }))
        .boxed()));
    *api.events.lock().unwrap() = Some(Ok(stream::iter([
        Ok(SubscribeAutotuneEventResponse {
            autotune_event: 8,
            error_message: String::new(),
        }),
        Ok(SubscribeAutotuneEventResponse {
            autotune_event: 12,
            error_message: "trim failed".to_owned(),
        }),
        Ok(SubscribeAutotuneEventResponse {
            autotune_event: 99,
            error_message: "future event".to_owned(),
        }),
    ])
    .boxed()));
    let schema = schema(config(), api.clone());
    let expected = [
        vec![
            value!({"armedState": "DISARMED"}),
            value!({"armedState": "ARMED"}),
            value!({"armedState": "UNKNOWN"}),
        ],
        vec![
            value!({"operatingMode": "SHADOW"}),
            value!({"operatingMode": "LIVE"}),
            value!({"operatingMode": "UNKNOWN"}),
        ],
        vec![
            value!({"events": {"autotuneEvent": "BEAM_OK", "errorMessage": ""}}),
            value!({"events": {"autotuneEvent": "SETTING_ERROR", "errorMessage": "trim failed"}}),
            value!({"events": {"autotuneEvent": "UNKNOWN", "errorMessage": "future event"}}),
        ],
    ];
    for (operation, expected) in OPERATIONS.into_iter().zip(expected) {
        let responses =
            schema.execute_stream(operation).collect::<Vec<_>>().await;
        assert!(
            responses.iter().all(|response| response.errors.is_empty()),
            "{responses:?}"
        );
        assert_eq!(
            responses
                .into_iter()
                .map(|response| response.data)
                .collect::<Vec<_>>(),
            expected
        );
    }
    assert_eq!(api.calls.lock().unwrap().len(), 3);
}

fn assert_safe_error(response: &async_graphql::Response) {
    assert_eq!(response.errors.len(), 1);
    let message = &response.errors[0].message;
    let id = message
        .strip_prefix(
            "Error encountered. See server logs for details. (Error ID: ",
        )
        .and_then(|message| message.strip_suffix(')'))
        .expect("safe error ID");
    assert!(uuid::Uuid::parse_str(id).is_ok());
    assert!(!message.contains("private"));
    assert!(!message.contains("secret"));
}

#[tokio::test]
async fn subscription_initiation_and_stream_errors_are_safe() {
    for status in [
        Status::internal("private token=secret"),
        Status::unauthenticated("private auth"),
        Status::unavailable("private host"),
    ] {
        for initiation in [true, false] {
            let api = empty_api();
            if initiation {
                *api.armed.lock().unwrap() = Some(Err(status.clone()));
                *api.mode.lock().unwrap() = Some(Err(status.clone()));
                *api.events.lock().unwrap() = Some(Err(status.clone()));
            } else {
                *api.armed.lock().unwrap() = Some(Ok(stream::iter([
                    Ok(SubscribeArmedStateResponse { armed_state: 1 }),
                    Err(status.clone()),
                ])
                .boxed()));
                *api.mode.lock().unwrap() = Some(Ok(stream::iter([
                    Ok(SubscribeOperatingModeResponse { operating_mode: 1 }),
                    Err(status.clone()),
                ])
                .boxed()));
                *api.events.lock().unwrap() = Some(Ok(stream::iter([
                    Ok(SubscribeAutotuneEventResponse {
                        autotune_event: 8,
                        error_message: String::new(),
                    }),
                    Err(status.clone()),
                ])
                .boxed()));
            }
            let schema = schema(config(), api);
            for operation in OPERATIONS {
                let responses =
                    schema.execute_stream(operation).collect::<Vec<_>>().await;
                assert_eq!(responses.len(), if initiation { 1 } else { 2 });
                if !initiation {
                    assert!(responses[0].errors.is_empty());
                }
                assert_safe_error(responses.last().unwrap());
            }
        }
    }
}

#[tokio::test]
async fn disabled_subscriptions_do_not_call_upstream() {
    let api = empty_api();
    let mut config = config();
    config.mi8_autotune =
        Upstream::disabled("MI8 Autotune", ["MI8_AUTOTUNE_GRPC_HOST"]);
    let schema = schema(config, api.clone());
    for operation in OPERATIONS {
        let responses =
            schema.execute_stream(operation).collect::<Vec<_>>().await;
        assert_eq!(responses.len(), 1);
        assert_eq!(
            responses[0].errors[0]
                .extensions
                .as_ref()
                .unwrap()
                .get("code"),
            Some(&value!("FEATURE_DISABLED"))
        );
    }
    assert!(api.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn subscriptions_forward_each_callers_token_and_config() {
    use axum::extract::FromRequestParts;
    for token in [Some("first-caller"), Some("second-caller"), None] {
        let api = empty_api();
        let schema = schema(config(), api.clone());
        for operation in OPERATIONS {
            let mut builder = Request::builder();
            if let Some(token) = token {
                builder =
                    builder.header("authorization", format!("Bearer {token}"));
            }
            let (mut parts, _) = builder.body(()).unwrap().into_parts();
            let auth = AuthInfo::from_request_parts(&mut parts, &())
                .await
                .ok()
                .unwrap();
            let responses = schema
                .execute_stream(
                    async_graphql::Request::new(operation).data(auth),
                )
                .collect::<Vec<_>>()
                .await;
            assert!(responses.is_empty());
        }
        for (host, actual_token) in api.calls.lock().unwrap().iter() {
            assert_eq!(host, "http://mi8-autotune.test:50051");
            assert_eq!(actual_token, token.unwrap_or_default());
        }
        assert_eq!(api.calls.lock().unwrap().len(), 3);
    }
}

#[tokio::test]
async fn pending_streams_emit_nothing_synthetic_and_are_dropped_on_cancellation()
 {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct DropSignal(Arc<AtomicBool>);
    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    for operation in OPERATIONS {
        let api = empty_api();
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = DropSignal(dropped.clone());
        // A captured guard makes ownership/drop observable without a background task.
        match operation {
            "subscription { armedState }" => {
                *api.armed.lock().unwrap() = Some(Ok(stream::pending()
                    .map(move |item| {
                        let _ = &guard;
                        item
                    })
                    .boxed()))
            }
            "subscription { operatingMode }" => {
                *api.mode.lock().unwrap() = Some(Ok(stream::pending()
                    .map(move |item| {
                        let _ = &guard;
                        item
                    })
                    .boxed()))
            }
            _ => {
                *api.events.lock().unwrap() = Some(Ok(stream::pending()
                    .map(move |item| {
                        let _ = &guard;
                        item
                    })
                    .boxed()))
            }
        }
        let schema = schema(config(), api.clone());
        let mut responses = schema.execute_stream(operation);
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(20),
                responses.next()
            )
            .await
            .is_err()
        );
        assert_eq!(api.calls.lock().unwrap().len(), 1);
        assert!(!dropped.load(Ordering::SeqCst));
        drop(responses);
        assert!(dropped.load(Ordering::SeqCst));
    }
}

#[tokio::test]
async fn graphiql_configures_subscription_endpoint() {
    let router = crate::graphql::create_mi8_autotune_router_with_api(
        Arc::new(config()),
        empty_api(),
    );
    let response = router
        .clone()
        .call(
            Request::builder()
                .uri("/mi8-autotune")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    assert!(
        std::str::from_utf8(&body)
            .unwrap()
            .contains("/mi8-autotune/s")
    );
}
