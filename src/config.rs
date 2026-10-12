use std::sync::Arc;

use rust_env_var_lib::env_var;

mod upstream;
pub use upstream::{Upstream, warn_if_disabled};

#[cfg(test)]
mod tests;

/// Reads an env var, treating unset and blank values identically.
fn read_var(name: &str) -> Option<String> {
    env_var::get(name)
        .to_option::<String>()
        .filter(|v| !v.trim().is_empty())
}

/// Builds the global config from the environment.
///
/// This never fails: an upstream whose environment variable(s) are missing or
/// blank is recorded as [`Upstream::Disabled`] and a warning is logged.
pub fn get_global_config() -> Arc<ExtapiGlobalConfig> {
    let config = ExtapiGlobalConfig {
        alarms_db: GrpcConfig::optional("Alarms DB", "GRPC_ALARMS_DB_HOST"),
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
        tlg: GrpcConfig::optional("TLG", "TLG_GRPC_HOST"),
        unr: GrpcConfig::optional("UNR", "UNR_GRPC_HOST"),
        wscan: GrpcConfig::optional("Wire Scanner", "SCANNER_GRPC_HOST"),
    };

    config.log_disabled_features();

    Arc::new(config)
}

/// Convenience method for tests that require a global config object.
/// Returns a distinct instance so parallel tests do not interfere with each other.
#[cfg(test)]
pub fn get_test_config() -> ExtapiGlobalConfig {
    ExtapiGlobalConfig {
        alarms_db: Upstream::Enabled(GrpcConfig {
            host_addr: String::new(),
        }),
        alarms_kafka: Upstream::Enabled(KafkaConfig {
            host_addr: String::new(),
            topic: String::new(),
        }),
        alarms_svc: Upstream::Enabled(GrpcConfig {
            host_addr: String::new(),
        }),
        blm: Upstream::Enabled(GrpcConfig {
            host_addr: String::new(),
        }),
        clock: Upstream::Enabled(GrpcConfig {
            host_addr: String::new(),
        }),
        devdb: Upstream::Enabled(GrpcConfig {
            host_addr: String::new(),
        }),
        tlg: Upstream::Enabled(GrpcConfig {
            host_addr: String::new(),
        }),
        unr: Upstream::Enabled(GrpcConfig {
            host_addr: String::new(),
        }),
        wscan: Upstream::Enabled(GrpcConfig {
            host_addr: String::new(),
        }),
    }
}

#[derive(Debug)]
pub struct ExtapiGlobalConfig {
    pub alarms_db: Upstream<GrpcConfig>,
    pub alarms_kafka: Upstream<KafkaConfig>,
    pub alarms_svc: Upstream<GrpcConfig>,
    pub blm: Upstream<GrpcConfig>,
    pub clock: Upstream<GrpcConfig>,
    pub devdb: Upstream<GrpcConfig>,
    pub tlg: Upstream<GrpcConfig>,
    pub unr: Upstream<GrpcConfig>,
    pub wscan: Upstream<GrpcConfig>,
}

impl ExtapiGlobalConfig {
    /// Warns once per disabled optional upstream. Add new optional upstreams here.
    fn log_disabled_features(&self) {
        warn_if_disabled(&self.alarms_db);
        warn_if_disabled(&self.alarms_kafka);
        warn_if_disabled(&self.alarms_svc);
        warn_if_disabled(&self.blm);
        warn_if_disabled(&self.clock);
        warn_if_disabled(&self.devdb);
        warn_if_disabled(&self.tlg);
        warn_if_disabled(&self.unr);
        warn_if_disabled(&self.wscan);
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
            None => Upstream::disabled(feature, &[host_addr_var]),
        }
    }
}

#[derive(Clone, Debug)]
pub struct KafkaConfig {
    pub host_addr: String,
    pub topic: String,
}
impl KafkaConfig {
    /// Enabled only when both variables are set; otherwise reports every
    /// missing variable.
    #[cfg(feature = "kafka")]
    fn optional(
        feature: &'static str, host_addr_var: &'static str,
        topic_var: &'static str,
    ) -> Upstream<Self> {
        match (read_var(host_addr_var), read_var(topic_var)) {
            (Some(host_addr), Some(topic)) => {
                Upstream::Enabled(KafkaConfig { host_addr, topic })
            }
            (host, topic) => {
                let missing: Vec<_> = [
                    (host.is_none(), host_addr_var),
                    (topic.is_none(), topic_var),
                ]
                .into_iter()
                .filter_map(|(absent, var)| absent.then_some(var))
                .collect();
                Upstream::disabled(feature, &missing)
            }
        }
    }

    /// Without the `kafka` feature nothing consumes this config, so it is
    /// never reported as disabled.
    #[cfg(not(feature = "kafka"))]
    fn optional(
        _feature: &'static str, host_addr_var: &'static str,
        topic_var: &'static str,
    ) -> Upstream<Self> {
        Upstream::Enabled(KafkaConfig {
            host_addr: read_var(host_addr_var).unwrap_or_default(),
            topic: read_var(topic_var).unwrap_or_default(),
        })
    }
}
