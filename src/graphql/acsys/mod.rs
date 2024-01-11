use async_graphql::*;
use std::convert::Infallible;
use warp::{Filter, Rejection};

mod handlers;

type MySchema =
    Schema<handlers::Queries, handlers::Mutations, handlers::Subscriptions>;

// Returns a Warp Filter that organizes the DPM protion of the web
// site.

pub fn filter(
    path: &str,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone + '_
{
    // Create the schema object which is used to reply to GraphQL
    // queries and subscriptions.

    let schema = Schema::build(
        handlers::Queries,
        handlers::Mutations,
        handlers::Subscriptions,
    )
    .finish();

    // Build the query portion.

    let graphql_query = async_graphql_warp::graphql(schema.clone())
        .and_then(
            |(schema, request): (MySchema, async_graphql::Request)| async move {
                let resp = schema.execute(request).await;

                Ok::<_, Infallible>(async_graphql_warp::GraphQLResponse::from(
                    resp,
                ))
            },
        )
        .with(warp::log("query"));

    // Build the subscription portion.

    let graphql_sub = async_graphql_warp::graphql_subscription(schema)
        .with(warp::log("subs"));

    // Build the sub-site. Look, first, for the leading path and then
    // look for any of the above services.

    warp::path(path).and(graphql_query.or(graphql_sub))
}
