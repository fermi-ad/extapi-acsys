use crate::g_rpc::dpm::build_connection;
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
use std::net::IpAddr;
use tracing::{info, instrument};

mod acsys;
mod alarms;
mod bbm;
mod devdb;
mod faas;
mod scanner;
mod tlg;
mod types;

// Generic function which adds `AuthInfo` to the context. This
// function can be used for all the GraphQL schemas.

#[instrument(name = "GRAPHQL", skip(schema, req, headers),
	     fields(who = tracing::field::Empty))]
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

    req = req.data(types::AuthInfo::new(
        &headers
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
      <li><a href="/alarms">Alarms</a></li>
      <li><a href="/bbm">Beam Budget monitoring</a> (WIP)</li>
      <li><a href="/devdb">Device Database</a></li>
      <li><a href="/faas">Functions as a Service</a></li>
      <li><a href="/tlg">Timeline Generator placement</a></li>
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
    .data(acsys::new_context())
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
        .route_service(S_ENDPOINT, GraphQLSubscription::new(schema))
}

fn create_alarms_router() -> Router {
    const Q_ENDPOINT: &str = "/alarms";
    const S_ENDPOINT: &str = "/alarms/s";

    let schema = Schema::build(
        alarms::AlarmsQueries,
        EmptyMutation,
        alarms::AlarmsSubscriptions,
    )
    .data(alarms::get_alarms_subscriber())
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
        .route_service(S_ENDPOINT, GraphQLSubscription::new(schema))
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

// Creates the portion of the site map that handles the Device Database
// GraphQL API.

fn create_devdb_router() -> Router {
    const Q_ENDPOINT: &str = "/devdb";

    let schema =
        Schema::build(devdb::DevDBQueries, EmptyMutation, EmptySubscription)
            .register_output_type::<devdb::types::DeviceProperty>()
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

fn create_tlg_router() -> Router {
    const Q_ENDPOINT: &str = "/tlg";

    let schema =
        Schema::build(tlg::TlgQueries, tlg::TlgMutations, EmptySubscription)
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

fn create_wscan_router() -> Router {
    const Q_ENDPOINT: &str = "/wscan";
    const S_ENDPOINT: &str = "/wscan/s";

    let schema = Schema::build(
        scanner::ScannerQueries,
        scanner::ScannerMutations,
        scanner::ScannerSubscriptions,
    )
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
        .route_service(S_ENDPOINT, GraphQLSubscription::new(schema))
}

// Creates the web site for the various GraphQL APIs.

async fn create_site() -> Router {
    use ::http::{header, Method};
    use tower_http::cors::{Any, CorsLayer};

    Router::new()
        .route("/", get(base_page))
 //       .merge(create_acsys_router().await)
//        .merge(create_bbm_router())
//        .merge(create_devdb_router())
        .merge(create_faas_router())
//        .merge(create_tlg_router())
//        .merge(create_wscan_router())
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

// Starts the web server that receives GraphQL queries. The
// configuration of the server is pulled together by obtaining
// configuration information from the submodules. All accesses are
// wrapped with CORS support from the `warp` crate.

pub async fn start_service(address: IpAddr, port: u16) {
    use std::net::SocketAddr;

    let bind_addr: SocketAddr = SocketAddr::new(address, port);

    // Load TLS certificate information. If there's an error, we panic.

    // let config = axum_server::tls_rustls::RustlsConfig::from_pem_file(
    //     "/etc/ssl/private/acsys-proxy.fnal.gov/cert.pem",
    //     "/etc/ssl/private/acsys-proxy.fnal.gov/key.pem",
    // )
    // .await
    // .expect("couldn't load certificate info from PEM file(s)");

    info!("site certificate successfully read");

    // Build up the routes for the site.

    let app = create_site().await;

    info!("web site handlers built successfully");

    // Start the server.

    axum_server::tls_rustls::bind_rustls(bind_addr, config)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

#[cfg(test)]
mod tests {
    use super::{graphql_handler, types::AuthInfo};
    use async_graphql::{
        Context, EmptyMutation, EmptySubscription, Object, Schema,
    };
    use axum::{routing::post, Router};

    // Create a simple GraphQL site. This site is more for testing the
    // meta information than testing GraphQL (we assume the authors of
    // the async-graphql crate are testing their product.)

    #[derive(Default)]
    pub struct TestQuery;

    #[Object]
    impl TestQuery {
        async fn authenticated(&self, ctxt: &Context<'_>) -> bool {
            ctxt.data_unchecked::<AuthInfo>().has_token()
        }
    }

    // Build a simple, crappy GraphQL endpoint.

    fn mk_test_site() -> Router {
        const Q_ENDPOINT: &str = "/test";

        let schema = Schema::build(
            TestQuery::default(),
            EmptyMutation,
            EmptySubscription,
        )
        .finish();

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
        use axum::{
            body::{to_bytes, Body},
            http::{Request, StatusCode},
        };
        use http::header::AUTHORIZATION;
        use tower::Service;

        let mut site = Router::new().merge(mk_test_site());
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
                        .header(AUTHORIZATION, "Basic MYJWTTOKEN")
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
                        .header(AUTHORIZATION, "Bearer MYJWTTOKEN")
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
}
