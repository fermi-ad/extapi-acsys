use std::{error::Error, fmt, sync::Arc};

use rust_env_var_lib::env_var;

mod upstream;
pub use upstream::{Disabled, FeatureDisabled, Upstream};

#[cfg(test)]
mod tests;

/// Comma-separated list of upstream keys (the [`ExtapiGlobalConfig`] field
/// names, e.g. `devdb,unr`) that must be configured for startup to succeed.
const REQUIRED_UPSTREAMS_VAR: &str = "REQUIRED_UPSTREAMS";

/// Reads an env var, treating unset and blank values identically.
fn read_var(name: &str) -> Option<String> {
    env_var::get(name)
        .to_option::<String>()
        .filter(|v| !v.trim().is_empty())
}

/// Builds the global config from the environment.
///
/// An upstream whose environment variable(s) are missing or blank is recorded
/// as [`Upstream::Disabled`] and a warning is logged. Startup fails only for
/// upstreams listed in `REQUIRED_UPSTREAMS`.
pub fn get_global_config() -> Result<Arc<ExtapiGlobalConfig>, ConfigError> {
    let config = ExtapiGlobalConfig::from_env();
    config.log_disabled_features();
    config.check_required(read_var(REQUIRED_UPSTREAMS_VAR).as_deref())?;
    Ok(Arc::new(config))
}

/// Startup failure caused by `REQUIRED_UPSTREAMS`.
#[derive(Debug, PartialEq, Eq)]
pub enum ConfigError {
    /// Keys in `REQUIRED_UPSTREAMS` that name no known upstream. Rejected so
    /// a typo cannot silently turn a requirement off.
    UnknownRequiredUpstreams {
        unknown: Vec<String>,
        known: Vec<&'static str>,
    },
    /// Required upstreams whose environment variables are missing or blank.
    MissingRequiredUpstreams(Vec<Disabled>),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownRequiredUpstreams { unknown, known } => write!(
                f,
                "{REQUIRED_UPSTREAMS_VAR} names unknown upstream(s): {}; \
                 known upstreams: {}",
                unknown.join(", "),
                known.join(", ")
            ),
            Self::MissingRequiredUpstreams(missing) => {
                let details: Vec<_> = missing
                    .iter()
                    .map(|d| {
                        format!(
                            "{} (set {})",
                            d.feature,
                            d.missing_vars.join(", ")
                        )
                    })
                    .collect();
                write!(
                    f,
                    "required upstream(s) not configured: {}",
                    details.join("; ")
                )
            }
        }
    }
}

impl Error for ConfigError {}

/// Convenience method for tests that require a global config object.
/// Returns a distinct instance so parallel tests do not interfere with each other.
#[cfg(test)]
pub fn get_test_config() -> ExtapiGlobalConfig {
    let grpc = || {
        Upstream::Enabled(GrpcConfig {
            host_addr: String::new(),
        })
    };
    ExtapiGlobalConfig {
        alarms_db: grpc(),
        #[cfg(feature = "kafka")]
        alarms_kafka: Upstream::Enabled(KafkaConfig {
            host_addr: String::new(),
            topic: String::new(),
        }),
        alarms_svc: grpc(),
        blm: grpc(),
        clock: grpc(),
        devdb: grpc(),
        mi8_autotune: grpc(),
        tlg: grpc(),
        unr: grpc(),
        wscan: grpc(),
    }
}

#[derive(Debug)]
pub struct ExtapiGlobalConfig {
    pub alarms_db: Upstream<GrpcConfig>,
    #[cfg(feature = "kafka")]
    pub alarms_kafka: Upstream<KafkaConfig>,
    pub alarms_svc: Upstream<GrpcConfig>,
    pub blm: Upstream<GrpcConfig>,
    pub clock: Upstream<GrpcConfig>,
    pub devdb: Upstream<GrpcConfig>,
    pub mi8_autotune: Upstream<GrpcConfig>,
    pub tlg: Upstream<GrpcConfig>,
    pub unr: Upstream<GrpcConfig>,
    pub wscan: Upstream<GrpcConfig>,
}

impl ExtapiGlobalConfig {
    fn from_env() -> Self {
        Self {
            alarms_db: GrpcConfig::optional("Alarms DB", "GRPC_ALARMS_DB_HOST"),
            #[cfg(feature = "kafka")]
            alarms_kafka: KafkaConfig::optional(
                "Alarms Kafka",
                "ALARMS_KAFKA_HOST",
                "ALARMS_KAFKA_TOPIC",
            ),
            alarms_svc: GrpcConfig::optional(
                "Alarms service",
                "GRPC_ALARMS_SERVICE_HOST",
            ),
            blm: GrpcConfig::optional("BLM", "BLM_GRPC_HOST"),
            clock: GrpcConfig::optional("Clock", "CLOCK_GRPC_HOST"),
            devdb: GrpcConfig::optional("DevDB", "DEVDB_GRPC_HOST"),
            mi8_autotune: GrpcConfig::optional(
                "MI8 Autotune",
                "MI8_AUTOTUNE_GRPC_HOST",
            ),
            tlg: GrpcConfig::optional("TLG", "TLG_GRPC_HOST"),
            unr: GrpcConfig::optional("UNR", "UNR_GRPC_HOST"),
            wscan: GrpcConfig::optional("Wire Scanner", "SCANNER_GRPC_HOST"),
        }
    }

    /// Every upstream as `(REQUIRED_UPSTREAMS key, disabled reason)`. This is
    /// the one list that startup logging and `REQUIRED_UPSTREAMS` share; add
    /// new upstreams here.
    fn upstreams(&self) -> Vec<(&'static str, Option<&Disabled>)> {
        vec![
            ("alarms_db", self.alarms_db.disabled_info()),
            #[cfg(feature = "kafka")]
            ("alarms_kafka", self.alarms_kafka.disabled_info()),
            ("alarms_svc", self.alarms_svc.disabled_info()),
            ("blm", self.blm.disabled_info()),
            ("clock", self.clock.disabled_info()),
            ("devdb", self.devdb.disabled_info()),
            ("mi8_autotune", self.mi8_autotune.disabled_info()),
            ("tlg", self.tlg.disabled_info()),
            ("unr", self.unr.disabled_info()),
            ("wscan", self.wscan.disabled_info()),
        ]
    }

    /// Warns once per disabled upstream.
    fn log_disabled_features(&self) {
        self.upstreams()
            .into_iter()
            .filter_map(|(_, disabled)| disabled)
            .for_each(Disabled::warn);
    }

    /// Fails if `required` (a `REQUIRED_UPSTREAMS` value) names an unknown
    /// key, or an upstream that is disabled. Reports every problem at once.
    fn check_required(
        &self, required: Option<&str>,
    ) -> Result<(), ConfigError> {
        let Some(required) = required else {
            return Ok(());
        };
        let upstreams = self.upstreams();
        let keys: Vec<&str> = required
            .split(',')
            .map(str::trim)
            .filter(|key| !key.is_empty())
            .collect();

        let unknown: Vec<String> = keys
            .iter()
            .filter(|key| upstreams.iter().all(|(name, _)| name != *key))
            .map(ToString::to_string)
            .collect();
        if !unknown.is_empty() {
            return Err(ConfigError::UnknownRequiredUpstreams {
                unknown,
                known: upstreams.iter().map(|(name, _)| *name).collect(),
            });
        }

        let missing: Vec<Disabled> = upstreams
            .into_iter()
            .filter(|(name, _)| keys.contains(name))
            .filter_map(|(_, disabled)| disabled.cloned())
            .collect();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(ConfigError::MissingRequiredUpstreams(missing))
        }
    }
}

#[derive(Clone, Debug)]
pub struct GrpcConfig {
    pub host_addr: String,
}
impl GrpcConfig {
    /// Reads the host from `host_addr_var`. A missing or blank var disables the
    /// feature instead of failing startup.
    fn optional(
        feature: &'static str, host_addr_var: &'static str,
    ) -> Upstream<Self> {
        match read_var(host_addr_var) {
            Some(host_addr) => Upstream::Enabled(GrpcConfig { host_addr }),
            None => Upstream::disabled(feature, [host_addr_var]),
        }
    }
}

#[cfg(feature = "kafka")]
#[derive(Clone, Debug)]
pub struct KafkaConfig {
    pub host_addr: String,
    pub topic: String,
}

#[cfg(feature = "kafka")]
impl KafkaConfig {
    /// Enabled only when both variables are set; otherwise reports every
    /// missing variable.
    fn optional(
        feature: &'static str, host_addr_var: &'static str,
        topic_var: &'static str,
    ) -> Upstream<Self> {
        match (read_var(host_addr_var), read_var(topic_var)) {
            (Some(host_addr), Some(topic)) => {
                Upstream::Enabled(KafkaConfig { host_addr, topic })
            }
            (host, topic) => Upstream::disabled(
                feature,
                [
                    (host.is_none(), host_addr_var),
                    (topic.is_none(), topic_var),
                ]
                .into_iter()
                .filter_map(|(absent, var)| absent.then_some(var)),
            ),
        }
    }
}
