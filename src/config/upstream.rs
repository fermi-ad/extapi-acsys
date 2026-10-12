//! Optional upstream dependencies.
//!
//! An [`Upstream<T>`] is either `Enabled` with its configuration, or `Disabled`
//! because its environment variable(s) were missing/empty. Disabled upstreams
//! never fail startup; they fail *per request* with a [`FeatureDisabled`] error
//! that converts cleanly into a GraphQL error or a gRPC [`tonic::Status`].

use std::{error::Error, fmt};

use tracing::warn;

/// Why and which feature is disabled. Kept for startup logging only; the
/// env var names are never sent to API clients.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Disabled {
    pub feature: &'static str,
    pub missing_vars: Vec<&'static str>,
}

#[derive(Clone, Debug)]
pub enum Upstream<T> {
    Enabled(T),
    Disabled(Disabled),
}

impl<T> Upstream<T> {
    pub fn disabled(feature: &'static str, missing: &[&'static str]) -> Self {
        Self::Disabled(Disabled {
            feature,
            missing_vars: missing.to_vec(),
        })
    }

    pub fn is_enabled(&self) -> bool {
        matches!(self, Self::Enabled(_))
    }

    /// The single choke point for business logic: get the config or a typed error.
    pub fn require(&self) -> Result<&T, FeatureDisabled> {
        match self {
            Self::Enabled(cfg) => Ok(cfg),
            Self::Disabled(d) => Err(FeatureDisabled(d.feature)),
        }
    }

    /// Same as [`require`](Self::require), but yields an `async_graphql::Error`
    /// so resolvers can simply write `config.blm.require_gql()?`.
    pub fn require_gql(&self) -> async_graphql::Result<&T> {
        self.require().map_err(FeatureDisabled::into_graphql)
    }

    pub fn disabled_info(&self) -> Option<&Disabled> {
        match self {
            Self::Enabled(_) => None,
            Self::Disabled(d) => Some(d),
        }
    }
}

/// Emits one structured warning for a disabled upstream (no-op if enabled).
pub fn warn_if_disabled<T>(upstream: &Upstream<T>) {
    if let Some(Disabled {
        feature,
        missing_vars,
    }) = upstream.disabled_info()
    {
        warn!(
            feature,
            missing_env = %missing_vars.join(","),
            "environment variable not set; feature disabled"
        );
    }
}

/// Returned when a request needs an upstream that this deployment lacks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FeatureDisabled(pub &'static str);

impl fmt::Display for FeatureDisabled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "The {} feature is not available in this deployment",
            self.0
        )
    }
}
impl Error for FeatureDisabled {}

impl FeatureDisabled {
    pub fn into_graphql(self) -> async_graphql::Error {
        use async_graphql::ErrorExtensions;
        async_graphql::Error::new(self.to_string())
            .extend_with(|_, e| e.set("code", "FEATURE_DISABLED"))
    }
}

impl From<FeatureDisabled> for tonic::Status {
    fn from(err: FeatureDisabled) -> Self {
        tonic::Status::unimplemented(err.to_string())
    }
}
