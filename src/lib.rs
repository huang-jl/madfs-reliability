use std::ops::Deref;

// use madsim::net::rpc::{Request, hash_str};
use madsim::Request;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;

pub mod ctl;
pub mod service;

#[cfg(test)]
mod test;

mod constant {
    use std::time::Duration;

    pub const FORWARD_TIMEOUT: Duration = Duration::from_millis(2000);
    pub const FORWARD_RETRY: u32 = 3;
}

#[derive(Error, Debug, Serialize, Deserialize)]
pub enum Error {
    #[error("Request {request_type} has been timeout and retry {retry_times} times still failed")]
    RetryNotSuccess{
        request_type: String,
        retry_times: u32,
    },
    #[error("Io relevant errors occur: {0}")]
    IoError(String),
    #[error("The requested server is not primary")]
    NotPrimary,
}

#[derive(Clone, Debug, Serialize, Deserialize, Request)]
#[rtype("Result<(), Error>")]
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
