use crate::constant::*;
use log::{info, warn};
use madsim::{
    task,
    time::{sleep, Instant},
    Request,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

pub mod client;

#[cfg(test)]
mod test;

#[derive(Debug, Clone)]
/// Simple Monitor: Assume it will nerver crash or make mistakes.
/// In production Monitor service should be maintained by clusters running Raft.
///
/// For now also assume that we cannot add server after init the cluster.
pub struct Monitor {
    inner: Arc<Mutex<Inner>>,
}

pub type TargetMapVersion = u64;
pub type PgMapVersion = u64;
pub type PgVersion = u64;

#[derive(Debug)]
struct Inner {
    target_map: BTreeMap<TargetMapVersion, TargetMap>,
    pg_map: BTreeMap<PgMapVersion, PgMap>,
    heartbeat: Vec<Instant>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetMap {
    version: TargetMapVersion,
    pub map: Vec<TargetInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgMap {
    version: PgMapVersion,
    pub map: Vec<PgInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetInfo {
    id: u64,
    state: TargetState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
/// Target state
/// - UP/DOWN: target is healthy(has a network url) or dead.
/// - IN/OUT: we only assign data to in targets.
pub enum TargetState {
    /// Initial state
    Init(SocketAddr),
    /// Target is UP and IN
    UpIn(SocketAddr),
    /// Target is DOWN but IN
    DownIn,
    /// Target is UP but OUT
    UpOut(SocketAddr),
    /// Target is DOWN but OUT
    DownOut,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PgInfo {
    version: PgVersion,
    state: PgState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PgState {
    Active,   // Everything is alright
    Inactive, // The cluster jusr start
    Unclean,  // Recovery is happening
    Stale,    // All nodes is down
}

#[derive(Debug, thiserror::Error, Serialize, Deserialize)]
pub enum MonitorError {
    #[error("Version {0} does not exist in monitor")]
    DoesNotExist(u64),
}

type Result<T> = std::result::Result<T, MonitorError>;

#[derive(Debug, Serialize, Deserialize, Request)]
#[rtype("Result<TargetMap>")]
struct FetchTargetMapReq(Option<TargetMapVersion>);

#[derive(Debug, Serialize, Deserialize, Request)]
#[rtype("Result<PgMap>")]
struct FetchPgMapReq(Option<PgMapVersion>);

#[derive(Debug, Serialize, Deserialize, Request)]
#[rtype("HeartBeatRes")]
struct HeartBeat {
    target_map_version: TargetMapVersion,
    pg_map_version: PgMapVersion,
    target_info: TargetInfo,
}

#[derive(Debug, Serialize, Deserialize)]
struct HeartBeatRes {
    target_map: Option<TargetMap>,
    pg_map: Option<PgMap>,
}

#[madsim::service]
impl Monitor {
    pub fn new(pg_num: usize, server_addrs: Vec<SocketAddr>) -> Self {
        let monitor = Monitor {
            inner: Arc::new(Mutex::new(Inner::new(pg_num, server_addrs))),
        };
        monitor.add_rpc_handler();
        monitor.background_check();
        monitor
    }

    #[rpc]
    async fn get_target_map(&self, request: FetchTargetMapReq) -> Result<TargetMap> {
        let inner = self.inner.lock().unwrap();
        inner.get_target_map(request.0)
    }

    #[rpc]
    async fn get_pg_map(&self, request: FetchPgMapReq) -> Result<PgMap> {
        let inner = self.inner.lock().unwrap();
        inner.get_pg_map(request.0)
    }

    #[rpc]
    async fn heartbeat(&self, request: HeartBeat) -> HeartBeatRes {
        info!(
            "Receive heartbeat from {} ({:?})",
            request.target_info.id,
            request.target_info.get_addr()
        );
        let mut inner = self.inner.lock().unwrap();
        inner.heartbeat(request)
    }

    fn background_check(&self) {
        let this = self.clone();
        task::spawn(async move {
            loop {
                sleep(MONITOR_CHECK_PERIOD).await;
                let mut inner = this.inner.lock().unwrap();
                inner.check_heartbeat();
            }
        })
        .detach();
    }
}

impl Inner {
    fn new(pg_num: usize, server_addrs: Vec<SocketAddr>) -> Self {
        Inner {
            heartbeat: (0..server_addrs.len()).map(|_| Instant::now()).collect(),
            target_map: [(0, TargetMap::empty()), (1, TargetMap::init(server_addrs))]
                .into_iter()
                .collect(),
            pg_map: [(0, PgMap::empty()), (1, PgMap::init(pg_num))]
                .into_iter()
                .collect(),
        }
    }

    fn get_target_map(&self, version: Option<TargetMapVersion>) -> Result<TargetMap> {
        match version {
            Some(version) => self
                .target_map
                .get(&version)
                .map_or(Err(MonitorError::DoesNotExist(version)), |map| {
                    Ok(map.clone())
                }),
            None => Ok(self.target_map.values().next_back().unwrap().clone()),
        }
    }

    fn get_pg_map(&self, version: Option<PgMapVersion>) -> Result<PgMap> {
        match version {
            Some(version) => self
                .pg_map
                .get(&version)
                .map_or(Err(MonitorError::DoesNotExist(version)), |map| {
                    Ok(map.clone())
                }),
            None => Ok(self.pg_map.values().next_back().unwrap().clone()),
        }
    }

    fn heartbeat(&mut self, request: HeartBeat) -> HeartBeatRes {
        let target_id = request.target_info.id as usize;
        assert!(target_id < self.heartbeat.len());
        self.heartbeat[target_id] = Instant::now();

        // Piggy back updated map
        let mut res = HeartBeatRes {
            pg_map: None,
            target_map: None,
        };
        if request.target_map_version < self.get_lastest_target_map_version() {
            res.target_map = Some(self.get_target_map(None).unwrap());
        }
        if request.pg_map_version < self.get_lastest_pg_map_version() {
            res.pg_map = Some(self.get_pg_map(None).unwrap());
        }
        res
    }

    fn get_lastest_target_map_version(&self) -> TargetMapVersion {
        *self.target_map.keys().next_back().unwrap()
    }

    fn get_lastest_pg_map_version(&self) -> PgMapVersion {
        *self.pg_map.keys().next_back().unwrap()
    }

    fn check_heartbeat(&mut self) {
        let mut update = false;
        let mut target_map = self.get_target_map(None).unwrap();
        target_map
            .map
            .iter_mut()
            .enumerate()
            .filter(|(_, info)| info.is_active())
            .for_each(|(id, target_info)| {
                if self.heartbeat[id].elapsed() > DOWN_TIMEOUT {
                    warn!("Target {} be marked Down & In", id);
                    target_info.state = TargetState::DownIn;
                    update = true;
                }
            });
        target_map
            .map
            .iter_mut()
            .enumerate()
            .filter(|(_, info)| info.is_down_in())
            .for_each(|(id, target_info)| {
                if self.heartbeat[id].elapsed() > OUT_TIMEOUT {
                    warn!("Target {} be marked Down & Out", id);
                    target_info.state = TargetState::DownOut;
                    update = true;
                }
            });
        if update {
            target_map.version += 1;
            self.target_map.insert(target_map.version, target_map);
        }
    }
}

impl TargetMap {
    /// Used for placeholder
    pub fn empty() -> Self {
        TargetMap {
            version: 0,
            map: Vec::new(),
        }
    }
    /// The first valid TargetMap
    pub fn init(server_addrs: Vec<SocketAddr>) -> Self {
        let map = server_addrs
            .into_iter()
            .enumerate()
            .map(|(id, addr)| TargetInfo {
                id: id as _,
                state: TargetState::UpIn(addr),
            })
            .collect();
        TargetMap { version: 0, map }
    }

    pub fn is_active(&self, target_id: usize) -> bool {
        self.map[target_id].is_active()
    }

    pub fn get_version(&self) -> TargetMapVersion {
        self.version
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }
}

impl PgMap {
    /// Used for placeholder
    pub fn empty() -> Self {
        PgMap {
            version: 0,
            map: Vec::new(),
        }
    }

    /// The first valid PgMap
    pub fn init(pg_num: usize) -> Self {
        let map = (0..pg_num)
            .map(|_| PgInfo {
                state: PgState::Inactive,
                version: 0,
            })
            .collect();
        PgMap { version: 1, map }
    }

    pub fn pg_num(&self) -> usize {
        self.map.len()
    }

    pub fn get_version(&self) -> PgMapVersion {
        self.version
    }
}

impl TargetInfo {
    pub fn get_addr(&self) -> Option<SocketAddr> {
        match self.state {
            TargetState::UpIn(addr) => Some(addr),
            TargetState::UpOut(addr) => Some(addr),
            TargetState::Init(addr) => Some(addr),
            _ => None,
        }
    }
    /// Active means Up and In
    pub fn is_active(&self) -> bool {
        matches!(self.state, TargetState::UpIn(..))
    }

    pub fn is_down_in(&self) -> bool {
        matches!(self.state, TargetState::DownIn)
    }
}