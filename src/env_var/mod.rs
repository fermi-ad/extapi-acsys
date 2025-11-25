use std::env::{self, VarError};
use tracing::{error, warn};

pub struct EnvVal {
    var_name: String,
    result: Result<String, VarError>,
}
impl EnvVal {
    pub fn as_str_or(self, default: &str) -> String {
        self.result.unwrap_or_else(|err| {
            warn!("{}. Using default: {}", err, default);
            default.to_string()
        })
    }

    pub fn as_u16_or(self, default: u16) -> u16 {
        match self.result {
            Ok(val) => match u16::from_str_radix(&val, 10) {
                Ok(parsed) => parsed,
                Err(err) => {
                    error!("Could not read the value for {}. {}. Using default: {}", self.var_name, err, default);
                    default
                }
            },
            Err(err) => {
                warn!("{}. Using default: {}", err, default);
                default
            }
        }
    }
}

pub fn get(var: &str) -> EnvVal {
    EnvVal {
        var_name: var.to_owned(),
        result: env::var(var),
    }
}
