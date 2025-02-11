use async_graphql::*;
use async_graphql_axum::{
    GraphQLRequest, GraphQLResponse, GraphQLSubscription,
};
use axum::{
    extract::State,
    http::header::{HeaderMap, AUTHORIZATION},
    response::Html,
    routing::get,
    Router,
};

use crate::g_rpc::dpm::build_connection;

mod acsys;
mod bbm;
mod clock;
mod devdb;
mod scanner;
mod types;
mod xform;

#[doc = "Fields in this section return data and won't cause side-effects in \
the control system. Some queries may require privileges, but none will \
affect the accelerator."]
#[derive(MergedObject, Default)]
struct Query(acsys::ACSysQueries);

#[doc = "Queries in this section will affect the control system; updating \
database tables and/or controlling accelerator hardware are possible. These \
requests will always need to be accompanied by an authentication token and \
will, most-likely, be tracked and audited."]
#[derive(MergedObject, Default)]
struct Mutation(acsys::ACSysMutations);

#[doc = "This section contains requests that return a stream of results. \
These requests are similar to Queries in that they don't affect the state \
of the accelerator or any other state of the control system."]
#[derive(MergedSubscription, Default)]
struct Subscription(
    acsys::ACSysSubscriptions,
    clock::ClockSubscriptions,
    xform::XFormSubscriptions,
);

struct AuthInfo(Option<String>);

// Generic function which adds `AuthInfo` to the context. This
// function can be used for all the GraphQL schemas.

async fn graphql_handler<Q, M, S>(
    State(schema): State<Schema<Q, M, S>>, headers: HeaderMap,
    req: GraphQLRequest,
) -> GraphQLResponse
where
    Q: ObjectType + Send + Sync + 'static,
    M: ObjectType + Send + Sync + 'static,
    S: SubscriptionType + Send + Sync + 'static,
{
    let mut req = req.into_inner();

    req = req.data(AuthInfo(
        headers
            .get(AUTHORIZATION)
            .map(|v| v.to_str().unwrap().to_string()),
    ));

    schema.execute(req).await.into()
}

// Returns an HTML document that has links to the various GraphQL APIs.

async fn base_page() -> Html<&'static str> {
    Html(
        r#"
<html>
  <body>
    <p>Some quick links:</p>

    <ul>
      <li><a href="/acsys">ACSys</a> (data acquisition)</li>
      <li><a href="/bbm">Beam Budget monitoring</a> (WIP)</li>
      <li><a href="/devdb">Device Database</a></li>
      <li><a href="/wscan">Wire Scanner</a> (WIP)</li>
    </ul>
  </body>
</html>
"#,
    )
}

// Creates the portion of the site map that handles the ACSys GraphQL API.

async fn create_acsys_router() -> Router {
    const Q_ENDPOINT: &str = "/acsys";
    const S_ENDPOINT: &str = "/acsys/s";

    let acsys_schema = Schema::build(
        Query::default(),
        Mutation::default(),
        Subscription::default(),
    )
    .data(build_connection().await.unwrap())
    .data(acsys::new_context())
    .finish();

    let acsys_graphiql = axum::response::Html(
        async_graphql::http::GraphiQLSource::build()
            .endpoint(Q_ENDPOINT)
            .subscription_endpoint(S_ENDPOINT)
            .finish(),
    );

    Router::new()
        .route(
            Q_ENDPOINT,
            get(acsys_graphiql)
                .post(graphql_handler)
                .with_state(acsys_schema.clone()),
        )
        .route_service(S_ENDPOINT, GraphQLSubscription::new(acsys_schema))
}

// Creates the portion of the site map that handles the Beam Budget
// Monitoring GraphQL API.

fn create_bbm_router() -> Router {
    const Q_ENDPOINT: &str = "/bbm";
    const S_ENDPOINT: &str = "/bbm/s";

    let bbm_schema =
        Schema::build(bbm::BbmQueries, EmptyMutation, EmptySubscription)
            .finish();

    let bbm_graphiql = axum::response::Html(
        async_graphql::http::GraphiQLSource::build()
            .endpoint(Q_ENDPOINT)
            .subscription_endpoint(S_ENDPOINT)
            .finish(),
    );

    Router::new()
        .route(
            Q_ENDPOINT,
            get(bbm_graphiql)
                .post(graphql_handler)
                .with_state(bbm_schema.clone()),
        )
        .route_service(S_ENDPOINT, GraphQLSubscription::new(bbm_schema))
}

// Creates the portion of the site map that handles the Device Database
// GraphQL API.

fn create_devdb_router() -> Router {
    const Q_ENDPOINT: &str = "/devdb";
    const S_ENDPOINT: &str = "/devdb/s";

    let devdb_schema =
        Schema::build(devdb::DevDBQueries, EmptyMutation, EmptySubscription)
            .register_output_type::<devdb::types::DeviceProperty>()
            .finish();

    let devdb_graphiql = axum::response::Html(
        async_graphql::http::GraphiQLSource::build()
            .endpoint(Q_ENDPOINT)
            .subscription_endpoint(S_ENDPOINT)
            .finish(),
    );

    Router::new()
        .route(
            Q_ENDPOINT,
            get(devdb_graphiql)
                .post(graphql_handler)
                .with_state(devdb_schema.clone()),
        )
        .route_service(S_ENDPOINT, GraphQLSubscription::new(devdb_schema))
}

// Creates the portion of the site map that handles the Wire Scanner GraphQL
// API.

fn create_wscan_router() -> Router {
    const Q_ENDPOINT: &str = "/wscan";
    const S_ENDPOINT: &str = "/wscan/s";

    let wscan_schema = Schema::build(
        scanner::ScannerQueries,
        scanner::ScannerMutations,
        scanner::ScannerSubscriptions,
    )
    .finish();

    let wscan_graphiql = axum::response::Html(
        async_graphql::http::GraphiQLSource::build()
            .endpoint(Q_ENDPOINT)
            .subscription_endpoint(S_ENDPOINT)
            .finish(),
    );

    Router::new()
        .route(
            Q_ENDPOINT,
            get(wscan_graphiql)
                .post(graphql_handler)
                .with_state(wscan_schema.clone()),
        )
        .route_service(S_ENDPOINT, GraphQLSubscription::new(wscan_schema))
}

// Creates the web site for the various GraphQL APIs.

async fn create_site() -> Router {
    use ::http::{header, Method};
    use tower_http::cors::{Any, CorsLayer};

    Router::new()
        .route("/", get(base_page))
        .merge(create_acsys_router().await)
        .merge(create_bbm_router())
        .merge(create_devdb_router())
        .merge(create_wscan_router())
        .layer(
            CorsLayer::new()
                .allow_methods([Method::OPTIONS, Method::GET, Method::POST])
                .allow_headers([
                    header::CONTENT_TYPE,
                    header::SEC_WEBSOCKET_PROTOCOL,
                    header::ACCESS_CONTROL_ALLOW_ORIGIN,
                ])
                .allow_origin(Any),
        )
}

// Starts the web server that receives GraphQL queries. The
// configuration of the server is pulled together by obtaining
// configuration information from the submodules. All accesses are
// wrapped with CORS support from the `warp` crate.

pub async fn start_service() {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    // Define the binding address for the web service. The address is
    // different between the operational and development versions.

    #[cfg(not(debug_assertions))]
    const BIND_ADDR: SocketAddr =
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 8000));
    #[cfg(debug_assertions)]
    const BIND_ADDR: SocketAddr =
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 8001));

    // Load TLS certificate information. If there's an error, we panic.

    let config = axum_server::tls_rustls::RustlsConfig::from_pem_file(
        "/etc/ssl/private/acsys-proxy.fnal.gov/cert.pem",
        "/etc/ssl/private/acsys-proxy.fnal.gov/key.pem",
    )
    .await
    .expect("couldn't load certificate info from PEM file(s)");

    // Build up the routes for the site.

    let app = create_site().await;

    // Start the server.

    axum_server::tls_rustls::bind_rustls(BIND_ADDR, config)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
