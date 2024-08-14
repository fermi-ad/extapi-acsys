use async_graphql::*;
use async_graphql_axum::{GraphQL, GraphQLSubscription};
use axum::{routing::get, Router};
use tokio::net::TcpListener;

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
    //let filter = filter("acsys").with(
    //    warp::cors()
    //        .allow_any_origin()
    //        .allow_headers(vec![
    //            AUTH_HEADER,
    //            "content-type",
    //            "Access-Control-Allow-Origin",
    //            "Sec-WebSocket-Protocol",
    //        ])
    //        .allow_methods(vec!["OPTIONS", "GET", "POST"]),
    //);

    //warp::serve(filter)
    //    .tls()
    //    .cert_path(Path::new("/etc/ssl/private/acsys-proxy.fnal.gov/cert.pem"))
    //    .key_path(Path::new("/etc/ssl/private/acsys-proxy.fnal.gov/key.pem"))
    //    .run(([0, 0, 0, 0], 8000))
    //    .await;

    const ENDPOINT: &str = "/acsys";

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
            .endpoint(ENDPOINT)
            .subscription_endpoint(ENDPOINT)
            .finish(),
    );

    // Build up the routes for the site. We're using the same path
    // because each of the three services uses a different method
    // (GET, POST, and WS.)

    let app = Router::new()
        .route(ENDPOINT, get(graphiql))
        .route_service(ENDPOINT, GraphQL::new(schema.clone()))
        .route_service(ENDPOINT, GraphQLSubscription::new(schema));

    // Start the server on port 8000!

    axum::serve(TcpListener::bind("0.0.0.0:8000").await.unwrap(), app)
        .await
        .unwrap();
}
