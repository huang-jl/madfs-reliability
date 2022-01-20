use std::fmt::Display;

use crate::{monitor::*, PgId, PgVersion, Result, TargetMapVersion};
use madsim::{net::rpc::Request, Request};
use serde::{Deserialize, Serialize};

pub trait KvRequest: Request {
    fn key(&self) -> &str;
    fn value(&self) -> Option<&[u8]>;
    fn take(self) -> (Option<String>, Option<Vec<u8>>);
}

pub trait EpochRequest {
    fn epoch(&self) -> TargetMapVersion;
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
#[rtype("Result<()>")]
pub struct ForwardReq {
    pub id: u64,
    pub op: Put,
}

#[derive(Debug, Clone, Serialize, Deserialize, Request)]
#[rtype("Result<PgVersion>")]
pub struct PeerConsult {
    pub epoch: TargetMapVersion,
    pub pgid: PgId,
}

#[derive(Clone, Serialize, Deserialize, Request)]
#[rtype("Result<()>")]
pub struct PeerFinish {
    pub epoch: TargetMapVersion,
    pub pgid: PgId,
    pub logs: Vec<(u64, Put)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Request)]
#[rtype("Result<Vec<(u64, Put)>>")]
pub struct HealReq {
    pub pgid: PgId,
    pub pg_ver: PgVersion,
}

#[derive(Debug, Clone, Serialize, Deserialize, Request)]
#[rtype("Result<()>")]
pub struct PgHeartbeat {
    pub pgid: PgId,
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

impl EpochRequest for PeerConsult {
    fn epoch(&self) -> TargetMapVersion {
        self.epoch
    }
}

impl EpochRequest for PeerFinish {
    fn epoch(&self) -> TargetMapVersion {
        self.epoch
    }
}

impl Display for PeerFinish {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PeerFinish {{\npgid: {}, epoch: {}, log length: {}}}",
            self.pgid,
            self.epoch,
            self.logs.len()
        )
    }
}

// Monitor related

#[derive(Debug, Serialize, Deserialize, Request)]
#[rtype("Result<TargetMap>")]
pub struct FetchTargetMapReq(pub Option<TargetMapVersion>);

#[derive(Debug, Serialize, Deserialize, Request)]
#[rtype("HeartBeatRes")]
/// Heartbeat used for monitor
pub struct HeartBeat {
    pub target_map_version: TargetMapVersion,
    pub target_info: TargetInfo,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HeartBeatRes {
    pub target_map: Option<TargetMap>,
}
