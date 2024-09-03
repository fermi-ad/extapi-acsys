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
struct Query(acsys::ACSysQueries, scanner::ScannerQueries);

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

// Starts the web server that receives GraphQL queries. The
// configuration of the server is pulled together by obtaining
// configuration information from the submodules. All accesses are
// wrapped with CORS support from the `warp` crate.

pub async fn start_service() {
    use ::http::{header, Method};
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use tower_http::cors::{Any, CorsLayer};

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

    // Define the URL paths for each of the API services.

    const Q_ACSYS_ENDPOINT: &str = "/acsys";
    const S_ACSYS_ENDPOINT: &str = "/acsys/s";
    const Q_BBM_ENDPOINT: &str = "/bbm";
    const S_BBM_ENDPOINT: &str = "/bbm/s";
    const Q_DEVDB_ENDPOINT: &str = "/devdb";
    const S_DEVDB_ENDPOINT: &str = "/devdb/s";

    // Build GraphQL schemas for each of the APIs.

    let acsys_schema = Schema::build(
        Query::default(),
        Mutation::default(),
        Subscription::default(),
    )
    .finish();

    let bbm_schema = Schema::build(
        bbm::BbmQueries,
        EmptyMutation,
        EmptySubscription,
    )
    .finish();

    let devdb_schema = Schema::build(
        devdb::DevDBQueries,
        EmptyMutation,
        EmptySubscription,
    )
    .register_output_type::<devdb::types::DeviceProperty>()
    .finish();

    // Create a handlers that provides GraphQL editors for each, major
    // API section so people don't have to install their own editors.

    let acsys_graphiql = axum::response::Html(
        async_graphql::http::GraphiQLSource::build()
            .endpoint(Q_ACSYS_ENDPOINT)
            .subscription_endpoint(S_ACSYS_ENDPOINT)
            .finish(),
    );

    let bbm_graphiql = axum::response::Html(
        async_graphql::http::GraphiQLSource::build()
            .endpoint(Q_BBM_ENDPOINT)
            .subscription_endpoint(S_BBM_ENDPOINT)
            .finish(),
    );

    let devdb_graphiql = axum::response::Html(
        async_graphql::http::GraphiQLSource::build()
            .endpoint(Q_DEVDB_ENDPOINT)
            .subscription_endpoint(S_DEVDB_ENDPOINT)
            .finish(),
    );

    // Build up the routes for the site.

    let app = Router::new()
        .route(
            Q_ACSYS_ENDPOINT,
            get(acsys_graphiql)
                .post_service(GraphQL::new(acsys_schema.clone())),
        )
        .route_service(S_ACSYS_ENDPOINT, GraphQLSubscription::new(acsys_schema))
        .route(
            Q_BBM_ENDPOINT,
            get(bbm_graphiql).post_service(GraphQL::new(bbm_schema.clone())),
        )
        .route_service(S_BBM_ENDPOINT, GraphQLSubscription::new(bbm_schema))
        .route(
            Q_DEVDB_ENDPOINT,
            get(devdb_graphiql)
                .post_service(GraphQL::new(devdb_schema.clone())),
        )
        .route_service(S_DEVDB_ENDPOINT, GraphQLSubscription::new(devdb_schema))
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

    // Start the server.

    axum_server::tls_rustls::bind_rustls(BIND_ADDR, config)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
