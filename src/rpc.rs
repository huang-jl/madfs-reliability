use crate::{
    ctl::heal::HealJob, monitor::*, service::Value, PgId, PgVersion, Result, TargetMapVersion,
};
use madsim::{net::rpc::Request, Request};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub trait KvRequest: Request {
    fn key(&self) -> &str;
    fn value(&self) -> Option<&[u8]>;
    fn take(self) -> (Option<String>, Option<Vec<u8>>);
}

#[derive(Debug, Clone, Serialize, Deserialize, Request)]
#[rtype("Result<Option<Vec<u8>>>")]
pub struct Get(pub String);

#[derive(Debug, Clone, Serialize, Deserialize, Request)]
#[rtype("Result<()>")]
pub struct Put {
    pub key: String,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Request)]
#[rtype("<T as Request>::Response")]
pub struct ForwardReq<T>(#[serde(bound(deserialize = ""))] pub T)
where
    T: Request;

#[derive(Debug, Clone, Serialize, Deserialize, Request)]
#[rtype("HashMap<PgId, PgVersion>")]
pub struct ConsultPgVersion(pub Vec<PgId>);

#[derive(Debug, Clone, Serialize, Deserialize, Request)]
#[rtype("Result<()>")]
/// When some server find its local pg is stale, it will send this kind of request to the up-to-date server.
/// The up-to-date server will add `HealJob` to its healing procedure queue.
pub struct HealJobReq(pub HealJob);

#[derive(Debug, Clone, Serialize, Deserialize, Request)]
#[rtype("Result<()>")]
pub struct HealReq {
    pub pgid: PgId,
    pub pg_ver: PgVersion,
    pub data: Vec<(String, Value)>,
}

impl KvRequest for Get {
    fn key(&self) -> &str {
        &self.0
    }

    fn value(&self) -> Option<&[u8]> {
        None
    }

    fn take(self) -> (Option<String>, Option<Vec<u8>>) {
        (Some(self.0), None)
    }
}

impl KvRequest for Put {
    fn key(&self) -> &str {
        &self.key
    }

    fn value(&self) -> Option<&[u8]> {
        Some(&self.value)
    }

    fn take(self) -> (Option<String>, Option<Vec<u8>>) {
        (Some(self.key), Some(self.value))
    }
}

// Monitor related

#[derive(Debug, Serialize, Deserialize, Request)]
#[rtype("Result<TargetMap>")]
pub struct FetchTargetMapReq(pub Option<TargetMapVersion>);

#[derive(Debug, Serialize, Deserialize, Request)]
#[rtype("HeartBeatRes")]
pub struct HeartBeat {
    pub target_map_version: TargetMapVersion,
    pub target_info: TargetInfo,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HeartBeatRes {
    pub target_map: Option<TargetMap>,
}

#[derive(Debug, Serialize, Deserialize, Request)]
#[rtype("Result<RecoverRes>")]
pub struct Recover(pub PgId);

#[derive(Debug, Serialize, Deserialize)]
pub struct RecoverRes {
    pub data: Vec<u8>,
    pub version: PgVersion,
}
