use async_graphql::*;
use async_graphql_axum::{GraphQL, GraphQLSubscription};
use axum::{routing::get, Router};

mod acsys;
mod bbm;
mod clock;
mod devdb;
mod scanner;
mod types;
mod xform;

#[doc = "Fields in this section return data and won't cause side-effects in the control system. Some queries may require privileges, but none will affect the accelerator."]
#[derive(MergedObject, Default)]
struct Query(
    acsys::ACSysQueries,
    bbm::BBMQueries,
    devdb::DevDBQueries,
    scanner::ScannerQueries,
);

#[doc = "Queries in this section will affect the control system; updating database tables and/or controlling accelerator hardware are possible. These requests will always need to be accompanied by an authentication token and will, most-likely, be tracked and audited."]
#[derive(MergedObject, Default)]
struct Mutation(acsys::ACSysMutations, scanner::ScannerMutations);

#[doc = "This section contains requests that return a stream of results. These requests are similar to Queries in that they don't affect the state of the accelerator or any other state of the control system."]
#[derive(MergedSubscription, Default)]
struct Subscription(
    acsys::ACSysSubscriptions,
    clock::ClockSubscriptions,
    scanner::ScannerSubscriptions,
    xform::XFormSubscriptions,
);

//const AUTH_HEADER: &str = "acsys-auth-jwt";

// Starts the web server that receives GraphQL queries. The
// configuration of the server is pulled together by obtaining
// configuration information from the submodules. All accesses are
// wrapped with CORS support from the `warp` crate.

pub async fn start_service() {
    use ::http::{header, Method};
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use tower_http::cors::{Any, CorsLayer};

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

    const Q_ENDPOINT: &str = "/acsys";
    const S_ENDPOINT: &str = "/acsys/s";

    // Build the GraphQL schema. Also, define the GraphQL interface
    // (DeviceProperty) that we use in the schema.

    let schema = Schema::build(
        Query::default(),
        Mutation::default(),
        Subscription::default(),
    )
    .register_output_type::<devdb::types::DeviceProperty>()
    .finish();

    // Create a handler that provides a GraphQL editor so people don't
    // have to install their own.

    let graphiql = axum::response::Html(
        async_graphql::http::GraphiQLSource::build()
            .endpoint(Q_ENDPOINT)
            .subscription_endpoint(S_ENDPOINT)
            .finish(),
    );

    // Build up the routes for the site.

    let app = Router::new()
        .route(
            Q_ENDPOINT,
            get(graphiql).post_service(GraphQL::new(schema.clone())),
        )
        .route_service(S_ENDPOINT, GraphQLSubscription::new(schema))
        .layer(
            CorsLayer::new()
                .allow_methods([Method::OPTIONS, Method::GET, Method::POST])
                .allow_headers([
                    header::CONTENT_TYPE,
                    header::SEC_WEBSOCKET_PROTOCOL,
                    header::ACCESS_CONTROL_ALLOW_ORIGIN,
                ])
                .allow_origin(Any),
        );

    // Start the server on port 8000!

    axum_server::tls_rustls::bind_rustls(BIND_ADDR, config)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
