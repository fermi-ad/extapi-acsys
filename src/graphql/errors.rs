use std::fmt::Debug;

use async_graphql::{Error, ErrorExtensions};
use tracing::error;
use uuid::Uuid;

use crate::config::FeatureDisabled;

/// Lets resolvers write `config.x.require()?` and get a GraphQL error with
/// `extensions.code = "FEATURE_DISABLED"`.
impl From<FeatureDisabled> for Error {
    fn from(err: FeatureDisabled) -> Self {
        Error::new(err.message())
            .extend_with(|_, e| e.set("code", "FEATURE_DISABLED"))
    }
}

pub fn handle_graphql_error<Err: Debug>(err: Err) -> Error {
    let err_id = Uuid::new_v4();
    error!("{err_id}: {err:?}");
    Error::new(format!(
        "Error encountered. See server logs for details. (Error ID: {err_id})"
    ))
}

#[cfg(test)]
mod tests {
    use crate::config::{GrpcConfig, Upstream};

    fn code_of(err: &async_graphql::Error) -> Option<String> {
        let ext = err.extensions.as_ref()?;
        match ext.get("code")? {
            async_graphql::Value::String(code) => Some(code.clone()),
            _ => None,
        }
    }

    // A resolver that propagates `require()` with `?` must keep the
    // FEATURE_DISABLED code, not silently fall back to a plain error.
    #[test]
    fn question_mark_on_require_keeps_feature_disabled_code() {
        let up: Upstream<GrpcConfig> =
            Upstream::disabled("BLM", ["BLM_GRPC_HOST"]);
        let resolver = || -> async_graphql::Result<()> {
            up.require()?;
            Ok(())
        };

        let err = resolver().unwrap_err();
        assert_eq!(
            err.message,
            "The BLM feature is not available in this deployment"
        );
        assert_eq!(code_of(&err).as_deref(), Some("FEATURE_DISABLED"));
    }
}
