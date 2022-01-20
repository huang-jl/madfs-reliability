use ctl::PgState;
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
    pub const FILE_PATH: &str = "service.blob";

    pub const FORWARD_TIMEOUT: Duration = Duration::from_millis(1000);
    pub const FORWARD_RETRY: u32 = 3;

    pub const MONITOR_CHECK_PERIOD: Duration = Duration::from_millis(2000);
    pub const DOWN_TIMEOUT: Duration = Duration::from_millis(10_000);
    pub const OUT_TIMEOUT: Duration = Duration::from_millis(20_000);

    pub const HEARTBEAT_PERIOD: Duration = Duration::from_millis(3000);

    pub const RECOVER_TIMEOUT: Duration = Duration::from_millis(3000);
    pub const RECOVER_RETRY: u32 = 3;

    /// Timeout of peering request
    pub const PEER_TIMEOUT: Duration = Duration::from_millis(500);

    pub const PG_HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(500);

    pub const HEAL_REQ_TIMEOUT: Duration = Duration::from_millis(3000);
}

pub type PgId = usize;
pub type TargetMapVersion = u64;
pub type PgMapVersion = u64;
pub type PgVersion = u64;

#[derive(Error, Debug, Serialize, Deserialize)]
pub enum Error {
    #[error("The requested server is not primary")]
    NotPrimary,
    #[error("The target is not responsible for the request's key or pgid")]
    WrongTarget,
    #[error("Network error: {0}")]
    NetworkError(String),
    #[error("Version {0} does not exist in monitor")]
    VersionDoesNotExist(u64),
    #[error("The pg on corressponding server is unavailable: {0:?}")]
    PgUnavailable(PgState),
    #[error("The pg is not more up-to-date")]
    PgNotNewer, // Used when find peers send some pg which is not more up-to-date during healing
    #[error("The epoch is too old, try to update TargetMap from Monitor")]
    StaleEpoch,
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::NetworkError(err.to_string())
    }
}

type Result<T> = std::result::Result<T, Error>;
