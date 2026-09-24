use std::fmt::Debug;

use async_graphql::Error;
use tracing::error;
use uuid::Uuid;

pub fn handle_graphql_error<Err: Debug>(err: Err) -> Error {
    let err_id = Uuid::new_v4();
    error!("{err_id}: {err:?}");
    Error::new(format!(
        "Error encountered. See server logs for details. (Error ID: {err_id})"
    ))
}
