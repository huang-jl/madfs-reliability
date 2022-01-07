use crate::{Result, monitor::*};
use madsim::{net::rpc::Request, Request};
use serde::{Deserialize, Serialize};

pub trait KvRequest: Request {
    fn key(&self) -> &str;
    fn value(&self) -> Option<&str>;
}

#[derive(Debug, Clone, Serialize, Deserialize, Request)]
#[rtype("Result<Option<String>>")]
pub struct Get(pub String);

#[derive(Debug, Clone, Serialize, Deserialize, Request)]
#[rtype("Result<()>")]
pub struct Put {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Request)]
#[rtype("<T as Request>::Response")]
pub struct ForwardReq<T>(#[serde(bound(deserialize = ""))]pub T)
where
    T: Request;

impl KvRequest for Get {
    fn key(&self) -> &str {
        &self.0
    }

    fn value(&self) -> Option<&str> {
        None
    }
}

impl KvRequest for Put {
    fn key(&self) -> &str {
        &self.key
    }

    fn value(&self) -> Option<&str> {
        Some(&self.value)
    }
}


// Monitor related

#[derive(Debug, Serialize, Deserialize, Request)]
#[rtype("Result<TargetMap>")]
pub struct FetchTargetMapReq(pub Option<TargetMapVersion>);

#[derive(Debug, Serialize, Deserialize, Request)]
#[rtype("Result<PgMap>")]
pub struct FetchPgMapReq(pub Option<PgMapVersion>);

#[derive(Debug, Serialize, Deserialize, Request)]
#[rtype("HeartBeatRes")]
pub struct HeartBeat {
    pub target_map_version: TargetMapVersion,
    pub pg_map_version: PgMapVersion,
    pub target_info: TargetInfo,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HeartBeatRes {
    pub target_map: Option<TargetMap>,
    pub pg_map: Option<PgMap>,
}