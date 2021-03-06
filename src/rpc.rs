use std::fmt::Display;

use crate::{monitor::*, PgId, PgVersion, Result, TargetMapVersion};
use madsim::{net::rpc::Request, Request};
use serde::{Deserialize, Serialize};
use madsim_reliability_macros::EpochRequest;

pub trait KvRequest: Request {
    fn key(&self) -> &str;
    fn value(&self) -> Option<&[u8]>;
    fn take(self) -> (Option<String>, Option<Vec<u8>>);
}

pub trait EpochRequest {
    fn epoch(&self) -> TargetMapVersion;
}

#[derive(Debug, Clone, Serialize, Deserialize, Request, EpochRequest)]
#[rtype("Result<Option<Vec<u8>>>")]
pub struct Get {
    pub epoch: TargetMapVersion,
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Request, EpochRequest)]
#[rtype("Result<()>")]
pub struct Put {
    pub key: String,
    pub value: Vec<u8>,
    pub epoch: TargetMapVersion,
}

#[derive(Debug, Clone, Serialize, Deserialize, Request, EpochRequest)]
#[rtype("Result<()>")]
pub struct ForwardReq {
    pub id: u64,
    pub op: Put,
    pub epoch: TargetMapVersion,
}

#[derive(Clone, Serialize, Deserialize, Request, EpochRequest)]
#[rtype("Result<()>")]
/// Primary checks pgs periodically.
/// When it finds pg is in `Inconsistent`,
/// it will send the `HealReq` to secondary, aiming to heal this pg.
pub struct HealReq {
    pub pgid: PgId,
    pub epoch: TargetMapVersion,
    pub heal_data: HealData,
}

#[derive(Debug, Clone, Serialize, Deserialize, Request, EpochRequest)]
#[rtype("Result<PgVersion>")]
pub struct PgConsult {
    pub epoch: TargetMapVersion,
    pub pgid: PgId,
}

#[derive(Clone, Serialize, Deserialize, Request, EpochRequest)]
#[rtype("Result<()>")]
pub struct PeerFinish {
    pub epoch: TargetMapVersion,
    pub pgid: PgId,
    pub heal_data: HealData,
}

#[derive(Debug, Clone, Serialize, Deserialize, Request, EpochRequest)]
#[rtype("Result<HealData>")]
pub struct FetchHealData {
    pub pgid: PgId,
    pub pg_ver: PgVersion,
    pub epoch: TargetMapVersion,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealData {
    pub logs: Vec<(PgVersion, Put)>,
    pub snapshot: Option<(PgVersion, Vec<u8>)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Request, EpochRequest)]
#[rtype("Result<()>")]
pub struct PgHeartbeat {
    pub pgid: PgId,
    pub epoch: TargetMapVersion,
}

impl KvRequest for Get {
    fn key(&self) -> &str {
        &self.key
    }

    fn value(&self) -> Option<&[u8]> {
        None
    }

    fn take(self) -> (Option<String>, Option<Vec<u8>>) {
        (Some(self.key), None)
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


impl Display for PeerFinish {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PeerFinish {{ pgid: {}, epoch: {}, snapshot_id :{:?}, log length: {} }}",
            self.pgid,
            self.epoch,
            self.heal_data.snapshot.as_ref().map(|x| x.0),
            self.heal_data.logs.len()
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
