use std::{path::Path, convert::Infallible};
use async_graphql::*;
use warp::{Filter, Rejection};

mod acsys;
mod scanner;
mod types;

type MySchema =
    Schema<acsys::Queries, acsys::Mutations, acsys::Subscriptions>;

const AUTH_HEADER: &str = "acsys-auth-jwt";

fn auth_filter(
) -> impl Filter<Extract = (Option<String>,), Error = Rejection> + Copy {
    warp::header::optional::<String>(AUTH_HEADER)
}

// Returns a Warp Filter that organizes the DPM portion of the web
// site. The base path is passed in and this function adds filters to
// recognize and provide GraphQL request support.

fn filter(
    path: &str,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone + '_
{
    // Create the schema object which is used to reply to GraphQL
    // queries and subscriptions.

    let schema = Schema::build(
        acsys::Queries,
        acsys::Mutations,
        acsys::Subscriptions,
    )
    .register_output_type::<types::DeviceProperty>()
    .finish();

    // Build the query portion. This Warp Filter recognizes GraphQL
    // query and mutation requests.

    let graphql_query = async_graphql_warp::graphql(schema.clone())
        .and(auth_filter())
        .and_then(
            |(schema, request): (MySchema, async_graphql::Request),
             _hdr: Option<String>| async move {
                let resp = schema.execute(request).await;

                Ok::<_, Infallible>(async_graphql_warp::GraphQLResponse::from(
                    resp,
                ))
            },
        )
        .with(warp::log("query"));

    // Build the subscription portion. This Warp Filter recognizes
    // GraphQL subscription requests, which require upgrading the
    // connection to a WebSocket. This is handled by the library.

    let graphql_sub = async_graphql_warp::graphql_subscription(schema)
        .with(warp::log("subs"));

    // Build the sub-site. Look, first, for the leading path and then
    // look for any of the above services.

    warp::path(path).and(graphql_query.or(graphql_sub))
}

// Starts the web server that receives GraphQL queries. The
// configuration of the server is pulled together by obtaining
// configuration information from the submodules. All accesses are
// wrapped with CORS support from the `warp` crate.

pub async fn start_service() {
    let filter = filter("acsys").with(
        warp::cors()
            .allow_any_origin()
            .allow_headers(vec![
                AUTH_HEADER,
                "content-type",
                "Access-Control-Allow-Origin",
                "Sec-WebSocket-Protocol",
            ])
            .allow_methods(vec!["OPTIONS", "GET", "POST"]),
    );

    warp::serve(filter)
        .tls()
        .cert_path(Path::new("/etc/ssl/private/acsys-proxy.fnal.gov/cert.pem"))
        .key_path(Path::new("/etc/ssl/private/acsys-proxy.fnal.gov/key.pem"))
        .run(([0, 0, 0, 0], 8000))
        .await;
}
