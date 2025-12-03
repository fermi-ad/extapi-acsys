use std::{
    env::{self, VarError},
    fmt::Display,
    str::FromStr,
};
use tracing::{error, warn};

pub struct EnvVal {
    var_name: String,
    result: Result<String, VarError>,
}

impl EnvVal {
    pub fn or<T: FromStr + Display>(self, default: T) -> T {
        match self.result {
            Ok(val) => {
                match val.parse::<T>() {
                    Ok(parsed) => parsed,
                    Err(_) => {
                        error!("Could not read the value for {}. Using default: {}", self.var_name, default);
                        default
                    }
                }
            }
            Err(err) => {
                warn!("{}: {}. Using default: {}", err, self.var_name, default);
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
