use crate::ctl::ServerError;
use madsim::net::rpc::{Request as Req, Serialize};
use madsim::Request;
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::Debug;
use log::info;

pub trait ServiceInput {
    fn check_modify_operation(&self) -> bool;
}

pub trait Service {
    type Input: ServiceInput + Req<Response = Result<Self::Output, ServerError>> + Clone + Debug;
    type Output;
    /// Dispatch args to the inner method. We only care about *modify* operations here.
    fn dispatch_write(&mut self, args: Self::Input) -> Self::Output;
    fn dispatch_read(&self, args: Self::Input) -> Self::Output;
}

pub struct KvService {
    kv: HashMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Request)]
#[rtype("Result<KvRes, ServerError>")]
pub enum KvArgs {
    Put { key: String, value: String },
    Get(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum KvRes {
    Put,
    Get(Option<String>),
}

impl ServiceInput for KvArgs {
    fn check_modify_operation(&self) -> bool {
        match self {
            KvArgs::Put { .. } => true,
            _ => false,
        }
    }
}

impl KvArgs {
    pub fn get_key(&self) -> String {
        match self {
            KvArgs::Put { key, .. } => key.clone(),
            KvArgs::Get(key) => key.clone(),
        }
    }

    pub fn get_value(&self) -> Option<String> {
        match self {
            KvArgs::Get(..) => None,
            KvArgs::Put { value, .. } => Some(value.clone()),
        }
    }
}

impl KvRes {
    pub fn get_value(&self) -> Option<String> {
        match self {
            KvRes::Put => None,
            KvRes::Get(res) => res.clone(),
        }
    }
}

impl Service for KvService {
    type Input = KvArgs;
    type Output = KvRes;

    fn dispatch_write(&mut self, args: Self::Input) -> KvRes {
        assert!(args.check_modify_operation());
        match args {
            KvArgs::Put { key, value } => {
                self.put(key, value);
                KvRes::Put
            }
            _ => unreachable!(),
        }
    }

    fn dispatch_read(&self, args: Self::Input) -> KvRes {
        assert!(!args.check_modify_operation());
        match args {
            KvArgs::Get(key) => {
                let value = self.get(key);
                KvRes::Get(value)
            }
            _ => unreachable!(),
        }
    }
}

impl KvService {
    pub fn new() -> Self {
        KvService { kv: HashMap::new() }
    }
    fn put(&mut self, key: String, value: String) {
        info!("Put key: {}, value: {}", key, value);
        self.kv.insert(key, value);
    }

    fn get(&self, key: String) -> Option<String> {
        let res = self.kv.get(&key).cloned();
        info!("Get key: {}, return: {:?}", key, res);
        res
    }
}
