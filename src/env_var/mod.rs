use std::{
    env::{self, VarError},
    fmt::Display,
    str::FromStr,
};
use tracing::{error, warn};

/// An intermediary object to handle converting the [`Result`] from [`std::env::var`] into a concrete type.    
pub struct EnvVal {
    var_name: String,
    result: Result<String, VarError>,
}

impl EnvVal {
    /// Unwraps the result of reading the specified environment variable. In the event there was an error,
    /// logs the error and returns the provided default value.
    pub fn or<T: FromStr + Display>(self, default: T) -> T {
        match self.result {
            Ok(val) => match val.parse::<T>() {
                Ok(parsed) => parsed,
                Err(_) => {
                    error!(
                        "Could not read the value for {}. Using default: {}",
                        self.var_name, default
                    );
                    default
                }
            },
            Err(err) => {
                warn!("{}: {}. Using default: {}", err, self.var_name, default);
                default
            }
        }
    }

    /// Unwraps the result of reading the specified environment variable. In the event there was an error,
    /// logs the error and generates the default value using the provided function.
    pub fn or_else<T: FromStr + Display>(
        self, default_fn: impl Fn() -> T,
    ) -> T {
        match self.result {
            Ok(val) => match val.parse::<T>() {
                Ok(parsed) => parsed,
                Err(_) => {
                    let generated_default = default_fn();
                    error!(
                        "Could not read the value for {}. Using default: {}",
                        self.var_name, generated_default
                    );
                    generated_default
                }
            },
            Err(err) => {
                let generated_default = default_fn();
                warn!(
                    "{}: {}. Using default: {}",
                    err, self.var_name, generated_default
                );
                generated_default
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
