use std::{error::Error, fmt, sync::Arc};

use rust_env_var_lib::env_var;

#[cfg(test)]
mod tests;

pub fn get_global_config() -> Result<Arc<ExtapiGlobalConfig>, ConfigError> {
    let config = ExtapiGlobalConfig {
        alarms_db: GrpcConfig::new("GRPC_ALARMS_DB_HOST")?,
        alarms_kafka: KafkaConfig::new(
            "ALARMS_KAFKA_HOST",
            "ALARMS_KAFKA_TOPIC",
        )?,
        alarms_svc: GrpcConfig::new("GRPC_ALARMS_SERVICE_HOST")?,
        clock: GrpcConfig::new("CLOCK_GRPC_HOST")?,
        devdb: GrpcConfig::new("DEVDB_GRPC_HOST")?,
        tlg: GrpcConfig::new("TLG_GRPC_HOST")?,
        unr: GrpcConfig::new("UNR_GRPC_HOST")?,
        wscan: GrpcConfig::new("SCANNER_GRPC_HOST")?,
    };

    Ok(Arc::new(config))
}

/// Convenience method for tests that require a global config object.
/// Returns a distinct instance so parallel tests do not interfere with each other.
#[cfg(test)]
pub fn get_test_config() -> ExtapiGlobalConfig {
    ExtapiGlobalConfig {
        alarms_db: GrpcConfig {
            host_addr: String::new(),
        },
        alarms_kafka: KafkaConfig {
            host_addr: String::new(),
            topic: String::new(),
        },
        alarms_svc: GrpcConfig {
            host_addr: String::new(),
        },
        clock: GrpcConfig {
            host_addr: String::new(),
        },
        devdb: GrpcConfig {
            host_addr: String::new(),
        },
        tlg: GrpcConfig {
            host_addr: String::new(),
        },
        unr: GrpcConfig {
            host_addr: String::new(),
        },
        wscan: GrpcConfig {
            host_addr: String::new(),
        },
    }
}

#[derive(Debug)]
pub struct ExtapiGlobalConfig {
    pub alarms_db: GrpcConfig,
    pub alarms_kafka: KafkaConfig,
    pub alarms_svc: GrpcConfig,
    pub clock: GrpcConfig,
    pub devdb: GrpcConfig,
    pub tlg: GrpcConfig,
    pub unr: GrpcConfig,
    pub wscan: GrpcConfig,
}

#[derive(Clone, Debug)]
pub struct GrpcConfig {
    pub host_addr: String,
}
impl GrpcConfig {
    fn new(host_addr_var: &'static str) -> Result<Self, ConfigError> {
        Ok(GrpcConfig {
            host_addr: env_var::get(host_addr_var)
                .to_option()
                .ok_or(ConfigError(host_addr_var))?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct KafkaConfig {
    pub host_addr: String,
    pub topic: String,
}
impl KafkaConfig {
    fn new(
        host_addr_var: &'static str, topic_var: &'static str,
    ) -> Result<Self, ConfigError> {
        #[cfg(feature = "kafka")]
        {
            Ok(KafkaConfig {
                host_addr: env_var::get(host_addr_var)
                    .to_option()
                    .ok_or(ConfigError(host_addr_var))?,
                topic: env_var::get(topic_var)
                    .to_option()
                    .ok_or(ConfigError(topic_var))?,
            })
        }
        #[cfg(not(feature = "kafka"))]
        {
            Ok(KafkaConfig {
                host_addr: env_var::get(host_addr_var)
                    .to_option()
                    .unwrap_or_default(),
                topic: env_var::get(topic_var).to_option().unwrap_or_default(),
            })
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ConfigError(&'static str);
impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Environment variable '{}' is not set", self.0)
    }
}
impl Error for ConfigError {}
