//! GraphQL Service Module
//!
//! This module contains the code for the GraphQL service. It defines the various GraphQL
//! schemas and resolvers, and starts the web server that receives GraphQL queries.

use crate::{
    config::ExtapiGlobalConfig,
    g_rpc::dpm::build_connection,
    graphql::{
        auth_handlers::{
            graphql_handler, graphql_ws_handler, unr_graphql_handler,
        },
        unr::{
            UnrMutations, UnrQueries,
            api::{GrpcUnrApi, UnrApi},
        },
    },
};
use async_graphql::{EmptyMutation, EmptySubscription, Schema};
use axum::{Router, body::Bytes, response::Html, routing::get};
use http::{Method, header};
use std::{
    fmt::Write as _,
    net::{IpAddr, Ipv6Addr, SocketAddr},
    sync::Arc,
};
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

mod acsys;
mod alarms;
mod auth_handlers;
mod bbm;
mod blm;
mod devdb;
mod errors;
mod faas;
mod scanner;
mod tlg;
mod types;
mod unr;

/// A GraphQL API that is mounted on this deployment.
struct Link {
    path: &'static str,
    label: &'static str,
}

// Returns an HTML document that has links to the GraphQL APIs that are
// mounted on this deployment.

fn base_page(links: &[Link]) -> String {
    let items = links.iter().fold(String::new(), |mut out, link| {
        let Link { path, label } = link;
        let _ = writeln!(out, "      <li><a href=\"{path}\">{label}</a></li>");
        out
    });

    format!(
        r"
<html>
  <body>
    <p>Some quick links:</p>

    <ul>
{items}    </ul>
  </body>
</html>
"
    )
}

// Creates the portion of the site map that handles the ACSys GraphQL API.

async fn create_acsys_router(global_config: Arc<ExtapiGlobalConfig>) -> Router {
    const Q_ENDPOINT: &str = "/acsys";
    const S_ENDPOINT: &str = "/acsys/s";

    let schema = Schema::build(
        acsys::ACSysQueries,
        acsys::ACSysMutations,
        acsys::ACSysSubscriptions,
    )
    .data(
        build_connection()
            .await
            .expect("couldn't make connection to DPM"),
    )
    .data(global_config)
    .finish();

    let graphiql = axum::response::Html(
        async_graphql::http::GraphiQLSource::build()
            .endpoint(Q_ENDPOINT)
            .subscription_endpoint(S_ENDPOINT)
            .finish(),
    );

    Router::new()
        .route(
            Q_ENDPOINT,
            get(graphiql)
                .post(graphql_handler)
                .with_state(schema.clone()),
        )
        .route(S_ENDPOINT, get(graphql_ws_handler).with_state(schema))
}

fn create_alarms_router(global_config: Arc<ExtapiGlobalConfig>) -> Router {
    const Q_ENDPOINT: &str = "/alarms";

    #[cfg(feature = "kafka")]
    {
        const S_ENDPOINT: &str = "/alarms/s";

        let schema = Schema::build(
            alarms::AlarmsQueries,
            alarms::AlarmsMutations,
            alarms::AlarmsSubscriptions,
        )
        .data(global_config)
        .finish();
        let graphiql = axum::response::Html(
            async_graphql::http::GraphiQLSource::build()
                .endpoint(Q_ENDPOINT)
                .subscription_endpoint(S_ENDPOINT)
                .finish(),
        );

        Router::new()
            .route(
                Q_ENDPOINT,
                get(graphiql)
                    .post(graphql_handler)
                    .with_state(schema.clone()),
            )
            .route(S_ENDPOINT, get(graphql_ws_handler).with_state(schema))
    }

    #[cfg(not(feature = "kafka"))]
    {
        let schema = Schema::build(
            alarms::AlarmsQueries,
            alarms::AlarmsMutations,
            EmptySubscription,
        )
        .data(global_config)
        .finish();
        let graphiql = axum::response::Html(
            async_graphql::http::GraphiQLSource::build()
                .endpoint(Q_ENDPOINT)
                .finish(),
        );
        Router::new().route(
            Q_ENDPOINT,
            get(graphiql).post(graphql_handler).with_state(schema),
        )
    }
}

// Creates the portion of the site map that handles the Beam Budget
// Monitoring GraphQL API.

fn create_bbm_router() -> Router {
    const Q_ENDPOINT: &str = "/bbm";

    let schema =
        Schema::build(bbm::BbmQueries, EmptyMutation, EmptySubscription)
            .finish();

    let graphiql = axum::response::Html(
        async_graphql::http::GraphiQLSource::build()
            .endpoint(Q_ENDPOINT)
            .finish(),
    );

    Router::new().route(
        Q_ENDPOINT,
        get(graphiql)
            .post(graphql_handler)
            .with_state(schema.clone()),
    )
}

// Creates the portion of the site map that handles the BLM GraphQL API.

fn create_blm_router(global_config: Arc<ExtapiGlobalConfig>) -> Router {
    const Q_ENDPOINT: &str = "/blm";
    const S_ENDPOINT: &str = "/blm/s";

    let schema =
        Schema::build(blm::BlmQueries, EmptyMutation, blm::BlmSubscriptions)
            .data(global_config)
            .finish();

    let graphiql = axum::response::Html(
        async_graphql::http::GraphiQLSource::build()
            .endpoint(Q_ENDPOINT)
            .subscription_endpoint(S_ENDPOINT)
            .finish(),
    );

    Router::new()
        .route(
            Q_ENDPOINT,
            get(graphiql)
                .post(graphql_handler)
                .with_state(schema.clone()),
        )
        .route(S_ENDPOINT, get(graphql_ws_handler).with_state(schema))
}

// Creates the portion of the site map that handles the Device Database
// GraphQL API.

fn create_devdb_router(global_config: Arc<ExtapiGlobalConfig>) -> Router {
    const Q_ENDPOINT: &str = "/devdb";

    let schema =
        Schema::build(devdb::DevDBQueries, EmptyMutation, EmptySubscription)
            .register_output_type::<devdb::types::DeviceProperty>()
            .data(global_config)
            .finish();

    let graphiql = axum::response::Html(
        async_graphql::http::GraphiQLSource::build()
            .endpoint(Q_ENDPOINT)
            .finish(),
    );

    Router::new().route(
        Q_ENDPOINT,
        get(graphiql).post(graphql_handler).with_state(schema),
    )
}

fn create_unr_router_with_api(
    api: Arc<dyn UnrApi>, global_config: Arc<ExtapiGlobalConfig>,
) -> Router {
    const Q_ENDPOINT: &str = "/unr";

    let schema = Schema::build(UnrQueries, UnrMutations, EmptySubscription)
        // UNR can be queried recursively via `Device.children`; cap depth/complexity
        // to prevent expensive full-graph traversals.
        .limit_depth(4)
        .limit_complexity(200)
        .data(api.clone())
        .data(global_config.clone())
        .finish();

    let graphiql = axum::response::Html(
        async_graphql::http::GraphiQLSource::build()
            .endpoint(Q_ENDPOINT)
            .finish(),
    );

    Router::new().route(
        Q_ENDPOINT,
        get(graphiql).post(unr_graphql_handler).with_state((
            schema,
            api,
            global_config,
        )),
    )
}

fn create_unr_router(global_config: Arc<ExtapiGlobalConfig>) -> Router {
    let api = Arc::new(GrpcUnrApi);
    create_unr_router_with_api(api, global_config)
}

fn create_faas_router() -> Router {
    const Q_ENDPOINT: &str = "/faas";

    let schema =
        Schema::build(faas::FaasQueries, EmptyMutation, EmptySubscription)
            .finish();

    let graphiql = axum::response::Html(
        async_graphql::http::GraphiQLSource::build()
            .endpoint(Q_ENDPOINT)
            .finish(),
    );

    Router::new().route(
        Q_ENDPOINT,
        get(graphiql).post(graphql_handler).with_state(schema),
    )
}

fn create_tlg_router(global_config: Arc<ExtapiGlobalConfig>) -> Router {
    const Q_ENDPOINT: &str = "/tlg";

    let schema =
        Schema::build(tlg::TlgQueries, tlg::TlgMutations, EmptySubscription)
            .data(global_config)
            .finish();

    let graphiql = axum::response::Html(
        async_graphql::http::GraphiQLSource::build()
            .endpoint(Q_ENDPOINT)
            .finish(),
    );

    Router::new().route(
        Q_ENDPOINT,
        get(graphiql).post(graphql_handler).with_state(schema),
    )
}

// Creates the portion of the site map that handles the Wire Scanner GraphQL
// API.

fn create_wscan_router(global_config: Arc<ExtapiGlobalConfig>) -> Router {
    const Q_ENDPOINT: &str = "/wscan";
    const S_ENDPOINT: &str = "/wscan/s";

    let schema = Schema::build(
        scanner::ScannerQueries,
        scanner::ScannerMutations,
        scanner::ScannerSubscriptions,
    )
    .data(global_config)
    .finish();

    let graphiql = axum::response::Html(
        async_graphql::http::GraphiQLSource::build()
            .endpoint(Q_ENDPOINT)
            .subscription_endpoint(S_ENDPOINT)
            .finish(),
    );

    Router::new()
        .route(
            Q_ENDPOINT,
            get(graphiql)
                .post(graphql_handler)
                .with_state(schema.clone()),
        )
        .route(S_ENDPOINT, get(graphql_ws_handler).with_state(schema))
}

/// Accumulates the routes for the site and the landing-page links for the
/// routes that were actually mounted.
struct SiteBuilder {
    router: Router,
    links: Vec<Link>,
}

impl SiteBuilder {
    fn new() -> Self {
        Self {
            router: Router::new(),
            links: Vec::new(),
        }
    }

    // Mounts `build()` at `link.path` when `enabled`; otherwise the route
    // does not exist and requests to it get a 404. The skip is logged once
    // at startup.

    fn mount(
        mut self, link: Link, enabled: bool, build: impl FnOnce() -> Router,
    ) -> Self {
        if enabled {
            self.router = self.router.merge(build());
            self.links.push(link);
        } else {
            info!(
                route = link.path,
                "route not mounted because its upstream(s) are disabled"
            );
        }
        self
    }

    // Adds the landing page, which links to every mounted route, and the
    // CORS layer.

    fn finish(mut self) -> Router {
        self.links.sort_by_key(|link| link.path);
        // `Bytes` is reference-counted, so the per-request handler clone
        // does not copy the page.
        let page = Bytes::from(base_page(&self.links));

        self.router
            .route("/", get(move || async move { Html(page) }))
            .layer(
                CorsLayer::new()
                    .allow_methods([Method::OPTIONS, Method::GET, Method::POST])
                    .allow_headers([
                        header::AUTHORIZATION,
                        header::CONTENT_TYPE,
                        header::SEC_WEBSOCKET_PROTOCOL,
                        header::ACCESS_CONTROL_ALLOW_ORIGIN,
                    ])
                    .allow_origin(Any),
            )
    }
}

// Creates the web site for the various GraphQL APIs.
//
// A route is mounted only when the upstream(s) it needs are configured. The
// ACSys, BBM and FaaS routes depend on no environment variables and are
// always mounted (ACSys's DevDB- and clock-backed fields still return a
// FEATURE_DISABLED error when those upstreams are missing). The alarms route
// is mounted when either of its gRPC upstreams is available; its other
// resolvers report FEATURE_DISABLED individually.
//
// Resolvers in routes gated on a single upstream (BLM, DevDB, TLG, UNR,
// wire scanner) still call `require()`. That check cannot fail while the
// route is gated, but keeps them safe if the mounting rules change.
async fn create_site(cfg: Arc<ExtapiGlobalConfig>) -> Router {
    // Built up front because `mount` takes a synchronous builder.
    let acsys = create_acsys_router(Arc::clone(&cfg)).await;

    SiteBuilder::new()
        .mount(
            Link {
                path: "/acsys",
                label: "ACSys (data acquisition)",
            },
            true,
            || acsys,
        )
        .mount(
            Link {
                path: "/alarms",
                label: "Alarms",
            },
            cfg.alarms_db.is_enabled() || cfg.alarms_svc.is_enabled(),
            || create_alarms_router(Arc::clone(&cfg)),
        )
        .mount(
            Link {
                path: "/bbm",
                label: "Beam Budget monitoring (WIP)",
            },
            true,
            create_bbm_router,
        )
        .mount(
            Link {
                path: "/blm",
                label: "Beam Loss Monitoring",
            },
            cfg.blm.is_enabled(),
            || create_blm_router(Arc::clone(&cfg)),
        )
        .mount(
            Link {
                path: "/devdb",
                label: "Device Database",
            },
            cfg.devdb.is_enabled(),
            || create_devdb_router(Arc::clone(&cfg)),
        )
        .mount(
            Link {
                path: "/faas",
                label: "Functions as a Service",
            },
            true,
            create_faas_router,
        )
        .mount(
            Link {
                path: "/tlg",
                label: "Timeline Generator placement",
            },
            cfg.tlg.is_enabled(),
            || create_tlg_router(Arc::clone(&cfg)),
        )
        .mount(
            Link {
                path: "/unr",
                label: "UNR (device graph)",
            },
            cfg.unr.is_enabled(),
            || create_unr_router(Arc::clone(&cfg)),
        )
        .mount(
            Link {
                path: "/wscan",
                label: "Wire Scanner (WIP)",
            },
            cfg.wscan.is_enabled(),
            || create_wscan_router(Arc::clone(&cfg)),
        )
        .finish()
}

// Starts the web server that receives GraphQL queries. The
// configuration of the server is pulled together by obtaining
// configuration information from the submodules. All accesses are
// wrapped with CORS support from the `warp` crate.

pub async fn start_service(port: u16, global_config: Arc<ExtapiGlobalConfig>) {
    let bind_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port);

    // Load TLS certificate information. If there's an error, we panic.

    let rustls_config = axum_server::tls_rustls::RustlsConfig::from_pem_file(
        "/etc/ssl/private/acsys-proxy.fnal.gov/cert.pem",
        "/etc/ssl/private/acsys-proxy.fnal.gov/key.pem",
    )
    .await
    .expect("couldn't load certificate info from PEM file(s)");

    info!("site certificate successfully read");

    // Build up the routes for the site.

    let app = create_site(global_config).await;

    info!("web site handlers built successfully");

    // Start the server.

    axum_server::tls_rustls::bind_rustls(bind_addr, rustls_config)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

#[cfg(test)]
mod tests {
    use crate::{
        config::{Upstream, get_test_config},
        graphql::auth_handlers::AuthInfo,
    };

    use super::*;
    use async_graphql::{Context, Object};
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
        routing::post,
    };
    use http::header;
    use tower::Service;

    // Create a simple GraphQL site. This site is more for testing the
    // meta information than testing GraphQL (we assume the authors of
    // the async-graphql crate are testing their product.)

    #[derive(Default)]
    pub struct TestQuery;

    #[Object]
    impl TestQuery {
        async fn authenticated(&self, ctxt: &Context<'_>) -> bool {
            ctxt.data_unchecked::<AuthInfo>().token().is_some()
        }
    }

    // Build a simple, crappy GraphQL endpoint.

    fn mk_test_site() -> Router {
        const Q_ENDPOINT: &str = "/test";

        let schema =
            Schema::build(TestQuery, EmptyMutation, EmptySubscription).finish();

        Router::new()
            .route(Q_ENDPOINT, post(graphql_handler).with_state(schema))
    }

    // This test checks to see whether a GraphQL resolver will be able
    // to see the authorization information passed in via the
    // AUTHORIZATION header. This test doesn't make any requests to
    // KeyCloak -- it's just making sure it can pull the authorization
    // info from the http headers.

    #[tokio::test]
    async fn test_authentication() {
        let mut site = mk_test_site();
        let query = r#"{ "query" : "{ authenticated }" }"#;

        {
            let response = site
                .as_service()
                .call(
                    Request::builder()
                        .method("POST")
                        .uri("/test")
                        .header("content-type", "application/json")
                        .body(Body::from(query))
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::OK);

            let body = response.into_body();

            assert_eq!(
                to_bytes(body, 1024).await.unwrap(),
                b"{\"data\":{\"authenticated\":false}}"[..]
            );
        }

        {
            let response = site
                .as_service()
                .call(
                    Request::builder()
                        .method("POST")
                        .uri("/test")
                        .header("content-type", "application/json")
                        .header(header::AUTHORIZATION, "Basic MYJWTTOKEN")
                        .body(Body::from(query))
                        .unwrap(),
                )
                .await
                .unwrap();

            // The TypedHeader rejects the request before it hits the GraphQL resolver (good!)
            // Only Bearer/OAuth 2.0 is supported.
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }

        {
            let response = site
                .as_service()
                .call(
                    Request::builder()
                        .method("POST")
                        .uri("/test")
                        .header("content-type", "application/json")
                        .header(header::AUTHORIZATION, "Bearer MYJWTTOKEN")
                        .body(Body::from(query))
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::OK);

            let body = response.into_body();

            assert_eq!(
                to_bytes(body, 1024).await.unwrap(),
                b"{\"data\":{\"authenticated\":true}}"[..]
            );
        }
    }

    // Route gating. These build the real site from a config and check
    // which routes exist.

    fn disabled<T>() -> Upstream<T> {
        Upstream::disabled("test", ["TEST_VAR"])
    }

    async fn status_of(site: &Router, method: &str, uri: &str) -> StatusCode {
        site.clone()
            .call(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{ "query": "{ __typename }" }"#))
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    async fn landing_page(site: &Router) -> String {
        let response = site
            .clone()
            .call(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/html; charset=utf-8"
        );
        String::from_utf8(
            to_bytes(response.into_body(), 1 << 20)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn all_routes_mounted_when_all_upstreams_enabled() {
        let site = create_site(Arc::new(get_test_config())).await;

        for path in [
            "/acsys", "/alarms", "/bbm", "/blm", "/devdb", "/faas", "/tlg",
            "/unr", "/wscan",
        ] {
            assert_eq!(
                status_of(&site, "POST", path).await,
                StatusCode::OK,
                "{path} should be mounted"
            );
        }
    }

    #[tokio::test]
    async fn routes_are_unmounted_when_their_upstream_is_disabled() {
        let mut config = get_test_config();
        config.blm = disabled();
        config.devdb = disabled();
        config.tlg = disabled();
        config.unr = disabled();
        config.wscan = disabled();
        let site = create_site(Arc::new(config)).await;

        for path in ["/blm", "/devdb", "/tlg", "/unr", "/wscan"] {
            assert_eq!(
                status_of(&site, "POST", path).await,
                StatusCode::NOT_FOUND,
                "{path} should not be mounted"
            );
        }
        // Routes whose upstreams are still configured, or that need none,
        // are unaffected.
        for path in ["/acsys", "/alarms", "/bbm", "/faas"] {
            assert_eq!(status_of(&site, "POST", path).await, StatusCode::OK);
        }

        let page = landing_page(&site).await;
        assert!(page.contains("href=\"/acsys\""));
        assert!(!page.contains("href=\"/blm\""));
        assert!(!page.contains("href=\"/unr\""));
    }

    #[tokio::test]
    async fn alarms_route_needs_at_least_one_alarms_upstream() {
        let mut config = get_test_config();
        config.alarms_db = disabled();
        let site = create_site(Arc::new(config)).await;
        assert_eq!(status_of(&site, "POST", "/alarms").await, StatusCode::OK);

        let mut config = get_test_config();
        config.alarms_db = disabled();
        config.alarms_svc = disabled();
        let site = create_site(Arc::new(config)).await;
        assert_eq!(
            status_of(&site, "POST", "/alarms").await,
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn site_boots_with_every_upstream_disabled() {
        let mut config = get_test_config();
        config.alarms_db = disabled();
        #[cfg(feature = "kafka")]
        {
            config.alarms_kafka = disabled();
        }
        config.alarms_svc = disabled();
        config.blm = disabled();
        config.clock = disabled();
        config.devdb = disabled();
        config.mi8_autotune = disabled();
        config.tlg = disabled();
        config.unr = disabled();
        config.wscan = disabled();
        let site = create_site(Arc::new(config)).await;

        assert_eq!(status_of(&site, "GET", "/").await, StatusCode::OK);
        assert_eq!(status_of(&site, "POST", "/faas").await, StatusCode::OK);
        assert_eq!(status_of(&site, "POST", "/bbm").await, StatusCode::OK);
        assert_eq!(
            status_of(&site, "POST", "/unr").await,
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn disabled_upstream_inside_a_mounted_route_returns_graphql_error() {
        let mut config = get_test_config();
        config.devdb = disabled();
        config.clock = disabled();
        let site = create_site(Arc::new(config)).await;

        // /acsys stays up; the DevDB-backed query reports FEATURE_DISABLED.
        let response = site
            .clone()
            .call(
                Request::builder()
                    .method("POST")
                    .uri("/acsys")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{ "query": "{ plotConfiguration(id: 1) { configId } }" }"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = String::from_utf8(
            to_bytes(response.into_body(), 1 << 20)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        assert!(body.contains("FEATURE_DISABLED"), "{body}");
    }
}
