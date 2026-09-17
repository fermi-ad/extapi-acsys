use std::{error::Error, fmt};

use rust_env_var_lib::env_var;

#[cfg(test)]
mod tests;

pub fn get_global_config() -> Result<ExtapiGlobalConfig, ConfigError> {
    Ok(ExtapiGlobalConfig {
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
    })
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
#[cfg(feature = "kafka")]
impl KafkaConfig {
    fn new(
        host_addr_var: &'static str, topic_var: &'static str,
    ) -> Result<Self, ConfigError> {
        Ok(KafkaConfig {
            host_addr: env_var::get(host_addr_var)
                .to_option()
                .ok_or(ConfigError(host_addr_var))?,
            topic: env_var::get(topic_var)
                .to_option()
                .ok_or(ConfigError(topic_var))?,
        })
    }
}
#[cfg(not(feature = "kafka"))]
impl KafkaConfig {
    fn new(
        host_addr_var: &'static str, topic_var: &'static str,
    ) -> Result<Self, ConfigError> {
        Ok(KafkaConfig {
            host_addr: env_var::get(host_addr_var)
                .to_option()
                .unwrap_or_default(),
            topic: env_var::get(topic_var).to_option().unwrap_or_default(),
        })
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
