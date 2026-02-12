use std::{
    env,
    fmt::{Debug, Display},
    str::FromStr,
};

pub fn expect<T: FromStr + Display>(var: &str) -> T
where
    <T as FromStr>::Err: Debug,
{
    env::var(var)
    .unwrap_or_else(|_| panic!("Value for {var} was not set"))
    .parse::<T>()
    .unwrap_or_else(|_| panic!("The value set for {var} could not be converted to the desired type"))
}
