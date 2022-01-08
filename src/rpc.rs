use crate::{PgId, PgVersion, Result, TargetMapVersion, ctl::PgInfo, monitor::*};
use madsim::{net::rpc::Request, Request};
use serde::{Deserialize, Serialize};

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
#[rtype("PgInfo")]
pub struct ConsultPgInfo(pub PgId);

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
