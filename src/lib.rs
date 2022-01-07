use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod ctl;
mod distributor;
pub mod monitor;
pub mod rpc;
pub mod service;

#[cfg(test)]
pub mod test;

mod constant {
    use std::time::Duration;

    pub const REPLICA_SIZE: usize = 3;
    pub const MONITOR_ADDR: &str = "10.0.0.1:8000";
    pub const PG_NUM: usize = 256;

    pub const FORWARD_TIMEOUT: Duration = Duration::from_millis(2000);
    pub const FORWARD_RETRY: u32 = 3;

    pub const MONITOR_CHECK_PERIOD: Duration = Duration::from_millis(2000);
    pub const DOWN_TIMEOUT: Duration = Duration::from_millis(10_000);
    pub const OUT_TIMEOUT: Duration = Duration::from_millis(20_000);

    pub const HEARTBEAT_PERIOD: Duration = Duration::from_millis(3000);

    pub const RECOVER_TIMEOUT: Duration = Duration::from_millis(5000);
    pub const RECOVER_RETRY: u32 = 3;
}

pub type PgId = usize;

#[derive(Error, Debug, Serialize, Deserialize)]
pub enum Error {
    #[error("The requested server is not primary")]
    NotPrimary,
    #[error("The target is not responsible for the request's key")]
    WrongTarget,
    #[error("Network error: {0}")]
    NetworkError(String),
    #[error("Version {0} does not exist in monitor")]
    VersionDoesNotExist(u64),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::NetworkError(err.to_string())
    }
}

type Result<T> = std::result::Result<T, Error>;
