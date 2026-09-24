use std::sync::Arc;

use async_graphql::{
    Data, EmptySubscription, ObjectType, Request, Schema, SubscriptionType,
    dataloader::{DataLoader, HashMapCache},
    http::{WebSocket, WebSocketProtocols, WsMessage},
};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::{
    Json,
    extract::{FromRequestParts, State, WebSocketUpgrade, ws::Message},
    response::{IntoResponse, Response},
};
use axum_extra::TypedHeader;
use base64::{Engine, engine::general_purpose::STANDARD_NO_PAD};
use futures::{
    SinkExt,
    future::{Ready, ready},
};
use futures_util::StreamExt;
use headers::{Authorization, authorization::Bearer};
use http::{StatusCode, header, request::Parts};
use rust_grpc_lib::auth::ForwardedToken;
use serde_json::{Value, json};
use tracing::{field::Empty, instrument};

use crate::{
    config::ExtapiGlobalConfig,
    graphql::unr::{
        UnrMutations, UnrQueries,
        api::UnrApi,
        loader::{UnrEntityLoader, UnrRelationshipLoader},
    },
};

#[instrument(name = "GRAPHQL", skip(schema, req, auth),
	     fields(who = Empty))]
pub async fn graphql_handler<Q, M, S>(
    State(schema): State<Schema<Q, M, S>>, auth: AuthInfo, req: GraphQLRequest,
) -> GraphQLResponse
where
    Q: ObjectType + Send + Sync + 'static,
    M: ObjectType + Send + Sync + 'static,
    S: SubscriptionType + Send + Sync + 'static,
{
    schema.execute(with_auth(req, auth)).await.into()
}

type UnrSchema = Schema<UnrQueries, UnrMutations, EmptySubscription>;

#[instrument(name = "GRAPHQL", skip(schema, api, global_config, req, auth),
	     fields(who = Empty))]
pub async fn unr_graphql_handler(
    State((schema, api, global_config)): State<(
        UnrSchema,
        Arc<dyn UnrApi>,
        Arc<ExtapiGlobalConfig>,
    )>,
    auth: AuthInfo, req: GraphQLRequest,
) -> GraphQLResponse {
    let token = ForwardedToken::new(auth.token().unwrap_or_default());
    let decorated_request = with_auth(req, auth)
        .data(DataLoader::with_cache(
            UnrEntityLoader::new(
                api.clone(),
                global_config.clone(),
                token.clone(),
            ),
            tokio::spawn,
            HashMapCache::default(),
        ))
        .data(DataLoader::with_cache(
            UnrRelationshipLoader::new(api, global_config, token),
            tokio::spawn,
            HashMapCache::default(),
        ));

    schema.execute(decorated_request).await.into()
}

pub async fn graphql_ws_handler<Q, M, S>(
    State(schema): State<Schema<Q, M, S>>, ws: WebSocketUpgrade,
) -> impl IntoResponse
where
    Q: ObjectType + Send + Sync + 'static,
    M: ObjectType + Send + Sync + 'static,
    S: SubscriptionType + Send + Sync + 'static,
{
    ws.on_upgrade(move |socket| async move {
        let (mut axum_sender, axum_receiver) = socket.split();

        let mut ws_engine = WebSocket::new(
            schema,
            axum_receiver.filter_map(async |msg| match msg {
                Ok(Message::Text(text)) => Some(text.to_string()),
                _ => None,
            }),
            WebSocketProtocols::GraphQLWS,
        )
        .on_connection_init(websocket_init_handler)
        .boxed();

        while let Some(gql_msg) = ws_engine.next().await {
            match gql_msg {
                WsMessage::Close(_, _) => {
                    let _ = axum_sender.send(Message::Close(None)).await;
                    break;
                }
                WsMessage::Text(text) => {
                    if axum_sender
                        .send(Message::Text(text.into()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
    })
}

#[derive(Debug)]
pub struct AuthInfo {
    bearer_token: Option<String>,
}

impl AuthInfo {
    pub fn token(&self) -> Option<String> {
        self.bearer_token.clone()
    }

    pub fn unsafe_account(&self) -> Option<String> {
        self.bearer_token.as_deref().and_then(|token| {
            let body = token.split('.').nth(1)?;
            let json = STANDARD_NO_PAD.decode(body).ok()?;
            let result: Value = serde_json::from_slice(&json).ok()?;

            result
                .get("preferred_username")
                .and_then(Value::as_str)
                .map(String::from)
        })
    }
}

impl<S> FromRequestParts<S> for AuthInfo
where
    S: Send + Sync,
{
    type Rejection = Unauthorized;

    async fn from_request_parts(
        parts: &mut Parts, state: &S,
    ) -> Result<Self, Self::Rejection> {
        match TypedHeader::<Authorization<Bearer>>::from_request_parts(
            parts, state,
        )
        .await
        {
            Ok(TypedHeader(auth)) => Ok(AuthInfo {
                bearer_token: Some(auth.token().to_owned()),
            }),
            Err(e) if e.reason().is_missing() => {
                Ok(AuthInfo { bearer_token: None })
            }
            Err(_) => Err(Unauthorized),
        }
    }
}

pub struct Unauthorized;
impl IntoResponse for Unauthorized {
    fn into_response(self) -> Response {
        (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer realm=\"\"")],
            Json(json!({
                "error": "invalid_scheme",
                "message": "The provided authentication scheme is invalid. Please use 'Bearer' or omit the header."
            }))
        ).into_response()
    }
}

fn websocket_init_handler(value: Value) -> Ready<async_graphql::Result<Data>> {
    let extracted_token = value
        .as_object()
        .and_then(|payload| payload.get("Authorization"))
        .and_then(Value::as_str);

    if let Some(token_val) = extracted_token
        && !token_val.starts_with("Bearer ")
    {
        return ready(Err(async_graphql::Error::new(
            "Unauthorized: must use Bearer token scheme",
        )));
    }

    let token = extracted_token
        .and_then(|token_val| token_val.strip_prefix("Bearer "))
        .map(str::to_string);

    let mut data = Data::default();
    data.insert(AuthInfo {
        bearer_token: token,
    });
    ready(Ok(data))
}

/// Injects per-request auth data from the Authorization header.
/// Must be called by every handler — this service is zero-trust.
fn with_auth(req: GraphQLRequest, auth: AuthInfo) -> Request {
    req.into_inner().data(auth)
}
