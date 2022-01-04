use std::ops::Deref;

// use madsim::net::rpc::{Request, hash_str};
use madsim::Request;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;

pub mod ctl;
pub mod monitor;
pub mod service;
mod distributor;

#[cfg(test)]
pub mod test;

mod constant {
    use std::time::Duration;

    pub const REPLICA_SIZE: usize = 3;

    pub const FORWARD_TIMEOUT: Duration = Duration::from_millis(2000);
    pub const FORWARD_RETRY: u32 = 3;

    pub const MONITOR_CHECK_PERIOD: Duration = Duration::from_millis(2000);
    pub const DOWN_TIMEOUT: Duration = Duration::from_millis(10_000);
    pub const OUT_TIMEOUT: Duration = Duration::from_millis(20_000);

    pub const HEARTBEAT_PERIOD: Duration = Duration::from_millis(3000);
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Error from service: {0}")]
    ServerError(#[from] ctl::ServerError),
    #[error("Io relevant errors occur: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Error from monitor: {0}")]
    MonitorError(#[from] monitor::MonitorError),
}

#[derive(Clone, Debug, Serialize, Deserialize, Request)]
#[rtype("()")]
/// Request forwarded to Secondary by Primary
struct ForwardReq<T>
where
    T: Send + Sync + Serialize + DeserializeOwned + 'static,
{
    #[serde(bound = "")]
    op: T,
}

impl<T> Deref for ForwardReq<T>
where
    T: Send + Sync + Serialize + DeserializeOwned + 'static,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.op
    }
}

impl<T> ForwardReq<T>
where
    T: Send + Sync + Serialize + DeserializeOwned + 'static,
{
    pub fn new(op: T) -> Self {
        Self { op }
    }
}
