//! Optional upstream dependencies.
//!
//! An [`Upstream<T>`] is either `Enabled` with its configuration, or `Disabled`
//! because its environment variable(s) were missing/blank. Disabled upstreams
//! never fail startup (unless listed in `REQUIRED_UPSTREAMS`); they fail *per
//! request* with a [`FeatureDisabled`] error.

use tracing::warn;

/// Why and which feature is disabled. Kept for startup logging only; the
/// env var names are never sent to API clients.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Disabled {
    pub feature: &'static str,
    pub missing_vars: Vec<&'static str>,
}

impl Disabled {
    /// Emits one structured warning naming the feature and its missing vars.
    pub fn warn(&self) {
        warn!(
            feature = self.feature,
            missing_env = %self.missing_vars.join(","),
            "environment variable not set or blank; feature disabled"
        );
    }
}

#[derive(Clone, Debug)]
pub enum Upstream<T> {
    Enabled(T),
    Disabled(Disabled),
}

impl<T> Upstream<T> {
    pub fn disabled(
        feature: &'static str,
        missing_vars: impl IntoIterator<Item = &'static str>,
    ) -> Self {
        Self::Disabled(Disabled {
            feature,
            missing_vars: missing_vars.into_iter().collect(),
        })
    }

    #[must_use]
    pub fn is_enabled(&self) -> bool {
        matches!(self, Self::Enabled(_))
    }

    /// The single choke point for business logic: get the config or a typed
    /// error. In a GraphQL resolver, `config.x.require()?` yields an error
    /// with `extensions.code = "FEATURE_DISABLED"` (see `graphql::errors`).
    pub fn require(&self) -> Result<&T, FeatureDisabled> {
        match self {
            Self::Enabled(cfg) => Ok(cfg),
            Self::Disabled(d) => Err(FeatureDisabled(d.feature)),
        }
    }

    #[must_use]
    pub fn disabled_info(&self) -> Option<&Disabled> {
        match self {
            Self::Enabled(_) => None,
            Self::Disabled(d) => Some(d),
        }
    }
}

/// Returned when a request needs an upstream that this deployment lacks.
///
/// This deliberately does **not** implement `Display` or `Error`.
/// `async_graphql::Error` has a blanket `From<T: Display>` that would silently
/// drop the `FEATURE_DISABLED` code; without `Display`, `?` in a resolver must
/// use the coded conversion in `graphql::errors`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FeatureDisabled(pub &'static str);

impl FeatureDisabled {
    /// Client-facing message. Names the feature, never the env var.
    #[must_use]
    pub fn message(self) -> String {
        format!("The {} feature is not available in this deployment", self.0)
    }
}
