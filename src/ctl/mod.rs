use self::logging::LogManager;
use crate::{
    constant::*,
    distributor::{Distributor, SimpleHashDistributor},
    monitor::{client::ServerClient, TargetMap},
    rpc::{
        EpochRequest, ForwardReq, Get, HealReq, KvRequest, PeerConsult, PeerFinish, PgHeartbeat,
        Put,
    },
    service::Store,
    Error, PgId, PgVersion, Result, TargetMapVersion,
};
use futures::{lock::Mutex, stream::FuturesUnordered, StreamExt};
use log::{debug, error, info, warn};
use madsim::{net::NetLocalHandle, task, time::Instant};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::Debug,
    io,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

pub mod heal;
pub mod heartbeat;
mod logging;
pub mod peer;
// pub mod recover;

/// Reliable wrapper of service
/// PG information is stored in `inner.service` K/V
pub struct ReliableCtl<T> {
    inner: Arc<Inner<T>>,
    monitor_client: Arc<ServerClient>,
    distributor: Arc<dyn Distributor<REPLICA_SIZE>>,
    /// (Constant) The total number of Placement Group.
    pg_num: usize,
}

// The #[derive(Clone)] bindly bind T: Clone, but we do not need it.
// We only need Arc<Mutex<T>>: Clone
impl<T> Clone for ReliableCtl<T>
where
    T: Store,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            monitor_client: self.monitor_client.clone(),
            distributor: self.distributor.clone(),
            pg_num: self.pg_num,
        }
    }
}

struct Inner<T> {
    service: Mutex<T>,
    logger: LogManager,
    distributor: Box<dyn Distributor<REPLICA_SIZE>>,
    pgs: Mutex<HashMap<PgId, PgInfo>>,
    /// Recording the next index (e.g. 5 means the next operation will be assigned index 5)
    sequencer: Vec<AtomicU64>,
}

#[derive(Debug, Clone)]
struct PgInfo {
    state: PgState,
    /// The next log id that supposed to apply
    applied_ptr: u64,
    /// Timestamp of heartbeat
    heartbeat_ts: Option<Instant>,
    /// Whether is primary
    primary: bool,
    /// Version number of target map
    epoch: TargetMapVersion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PgState {
    /// Everything is alright
    Active,
    /// The pg is unavailable (during peering).
    Inactive,
    /// Error occuring when forwarding or heartbeat
    Inconsistent,
    /// The pg is not of responsibility
    Irresponsible,
}

#[madsim::service]
impl<T> ReliableCtl<T>
where
    T: Store + Sync + Send + 'static,
{
    pub async fn new(pg_num: usize, service: T, monitor_client: Arc<ServerClient>) -> Self {
        let ctl = ReliableCtl {
            inner: Arc::new(Inner::new(pg_num, service).await),
            monitor_client,
            distributor: Arc::new(SimpleHashDistributor::new(pg_num)),
            pg_num,
        };
        ctl.init().await;
        ctl
    }

    /// 1. initialize pg state: All pgs of responsibility are marked Unprepared and wait for peering procedure
    /// 2. add rpc handler
    /// 3. start background procedure
    async fn init(&self) {
        // reply logs
        for pgid in 0..self.pg_num {
            for log_id in 0..self.inner.logger.upper(pgid) {
                self.inner.apply_log(pgid, log_id).await;
            }
        }
        self.add_rpc_handler();
        self.start_background_task();
    }

    fn start_background_task(&self) {
        // Peer tasks
        let this = self.clone();
        task::spawn(async move {
            this.background_peer().await;
        })
        .detach();
        let this = self.clone();
        task::spawn(async move {
            this.background_heal().await;
        })
        .detach();
    }

    /*
     * RPC Handler
     */

    #[rpc]
    /// Handler for request directly from client.
    async fn put(&self, request: Put) -> Result<()> {
        let pgid = self.distributor.assign_pgid(request.key().as_bytes());
        debug!(
            "Receive Put from client, key = {}, pgid = {}",
            request.key(),
            pgid
        );
        self.inner.check_pg_state(pgid).await?;

        if self.inner.is_primary(pgid).await {
            let op_id = self.inner.assign_log_id(pgid);
            let forward_res = self.forward_put_by_primary(&request, op_id).await;

            self.inner.add_log(op_id, &request).await;
            self.inner.apply_log(pgid, op_id).await;

            if let Err(err) = forward_res {
                // Error occur when forwarding, conservatively mark it Inconsistent.
                error!(
                    "Error occur when forwarding, pg {} been marked Inconsistent: {}",
                    pgid, err
                );
                self.inner.set_pg_state(pgid, PgState::Inconsistent).await;
                Err(err)
            } else {
                Ok(())
            }
        } else {
            Err(Error::NotPrimary)
        }
    }

    #[rpc]
    /// Handler for get request from client.
    async fn get(&self, request: Get) -> Result<Option<Vec<u8>>> {
        debug!("Receive Get from client, key = {}", request.key());
        self.check_epoch(&request)?;
        let pgid = self.distributor.assign_pgid(request.key().as_bytes());
        self.inner.check_pg_state(pgid).await?;

        if self.inner.is_responsible(pgid).await {
            Ok(self.inner.get(request.key()).await)
        } else {
            Err(Error::WrongTarget)
        }
    }

    #[rpc]
    /// Handler for forward request from primary.
    async fn forward(&self, request: ForwardReq) -> Result<()> {
        self.check_epoch(&request)?;
        let ForwardReq { id, op, .. } = request;
        let pgid = self.distributor.assign_pgid(op.key().as_bytes());
        debug!(
            "Get forward put request, key = {}, pgid = {}",
            op.key(),
            pgid
        );
        self.inner.check_pg_state(pgid).await?;

        if !self.inner.is_secondary(pgid).await {
            return Err(Error::WrongTarget);
        }
        self.inner.add_log(id, &op).await;
        self.inner.apply_log(pgid, id).await;
        Ok(())
    }

    #[rpc]
    async fn peer_consult(&self, request: PeerConsult) -> Result<PgVersion> {
        debug!("Get peer consult request: {:?}", request);
        let PeerConsult { pgid, epoch } = request;
        let local_epoch = self.get_epoch();
        if epoch < local_epoch {
            return Err(Error::EpochNotMatch(local_epoch));
        } else if epoch > local_epoch {
            debug!(
                "Get Peer consult Request (epoch = {}) which epoch is higher than local ({})",
                epoch, local_epoch
            );
            self.monitor_client.update_target_map().await;
        }
        self.inner
            .pgs
            .lock()
            .await
            .entry(pgid)
            .and_modify(|info| {
                info.state = PgState::Inactive;
                info.primary = false;
                info.epoch = epoch;
                info.heartbeat_ts = None;
            })
            .or_insert(PgInfo {
                state: PgState::Inactive,
                primary: false,
                applied_ptr: 0,
                heartbeat_ts: None,
                epoch,
            });
        Ok(self.inner.get_pg_version(pgid))
    }

    #[rpc]
    async fn peer_finish(&self, request: PeerFinish) -> Result<()> {
        debug!("Get peer finish request: {}", request);
        self.check_epoch(&request)?;
        let PeerFinish { pgid, mut logs, .. } = request;
        let local_pg_ver = self.inner.get_pg_version(pgid);
        logs.sort_by(|a, b| a.0.cmp(&b.0));
        for (log_id, op) in logs
            .into_iter()
            .filter(|(log_id, _)| *log_id >= local_pg_ver)
        {
            self.inner.add_log(log_id, &op).await;
            self.inner.apply_log(pgid, log_id).await;
        }
        self.inner.set_pg_state(pgid, PgState::Active).await;
        self.inner.update_pg_heartbeat_ts(pgid).await;
        Ok(())
    }

    #[rpc]
    async fn heal(&self, request: HealReq) -> Result<Vec<(u64, Put)>> {
        debug!("Get heal request {:?}", request);
        self.check_epoch(&request)?;
        let HealReq { pgid, pg_ver, .. } = request;
        let local_pg_ver = self.inner.get_pg_version(pgid);
        if local_pg_ver <= pg_ver {
            return Err(Error::PgNotNewer);
        }
        let mut res = Vec::new();
        for log_id in pg_ver..local_pg_ver {
            res.push((log_id, self.inner.logger.get(pgid, log_id).await.unwrap()));
        }
        Ok(res)
    }

    #[rpc]
    async fn pg_heartbeat(&self, request: PgHeartbeat) -> Result<()> {
        self.check_epoch(&request)?;
        let PgHeartbeat { pgid, .. } = request;
        self.inner.check_pg_state(pgid).await?;
        self.inner.update_pg_heartbeat_ts(pgid).await;
        Ok(())
    }

    async fn forward_put_by_primary(&self, request: &Put, op_id: u64) -> Result<()> {
        // locate the peer servers (do not forget to exclude self)
        let peers = self.get_target_addrs(request.key());
        debug!("Peers = {:?}", peers);
        // prepare request to peers
        let mut tasks = peers
            .into_iter()
            .skip(1)
            .map(|peer| {
                let request = request.clone();
                async move {
                    let net = NetLocalHandle::current();
                    for _ in 0..FORWARD_RETRY {
                        let request = ForwardReq {
                            id: op_id,
                            op: request.clone(),
                            epoch: self.get_epoch(),
                        };
                        match net
                            .call_timeout(peer.to_owned(), request, FORWARD_TIMEOUT)
                            .await
                        {
                            Ok(x) => return x,
                            Err(err) if err.kind() == io::ErrorKind::TimedOut => {}
                            Err(err) => return Err(Error::NetworkError(err.to_string())),
                        }
                    }
                    Err(Error::NetworkError(format!(
                        "Forward Request cannot get response from {}, retry {} times",
                        peer, FORWARD_RETRY
                    )))
                }
            })
            .collect::<FuturesUnordered<_>>();

        while let Some(res) = tasks.next().await {
            res?
        }
        Ok(())
    }

    /*
     * Helper methods
     */

    fn local_addr(&self) -> SocketAddr {
        NetLocalHandle::current().local_addr()
    }

    fn get_target_map(&self) -> TargetMap {
        self.monitor_client.get_local_target_map()
    }

    fn get_target_addrs(&self, key: &str) -> [SocketAddr; REPLICA_SIZE] {
        let pgid = self.distributor.assign_pgid(key.as_bytes());
        self.distributor.locate(pgid, &self.get_target_map())
    }

    fn get_epoch(&self) -> TargetMapVersion {
        self.monitor_client.get_local_target_map().get_version()
    }

    fn check_epoch<R>(&self, request: &R) -> Result<()>
    where
        R: EpochRequest,
    {
        let epoch = request.epoch();
        let local_epoch = self.get_epoch();
        if local_epoch > epoch {
            debug!(
                "Get Peer consult Request (epoch = {}) with lower epoch than local (epoch = {})",
                epoch, local_epoch
            );
            Err(Error::EpochNotMatch(local_epoch))
        } else if local_epoch < epoch {
            debug!(
                "Get Peer consult Request (epoch = {}) with higher epoch than local (epoch = {})",
                epoch, local_epoch
            );
            let this = self.clone();
            task::spawn(async move {
                this.monitor_client.update_target_map().await;
            })
            .detach();
            Err(Error::EpochNotMatch(local_epoch))
        } else {
            Ok(())
        }
    }
}

impl<T> Inner<T>
where
    T: Store,
{
    async fn new(pg_num: usize, service: T) -> Self {
        let logger = LogManager::new(pg_num).await;
        // The sequence number is the same as highest logger index at beginning
        let sequencer = (0..pg_num)
            .map(|pgid| AtomicU64::new(logger.upper(pgid)))
            .collect();
        Self {
            service: Mutex::new(service),
            logger,
            distributor: Box::new(SimpleHashDistributor::new(pg_num)),
            pgs: Mutex::new(HashMap::new()),
            sequencer,
        }
    }
    /*
     * Helper methods
     */

    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        let pgid = self.distributor.assign_pgid(key.as_bytes());
        self.service.lock().await.get(&format!("{}.{}", pgid, key))
    }

    fn get_pg_version(&self, pgid: PgId) -> PgVersion {
        self.logger.upper(pgid)
    }

    async fn get_pg_state(&self, pgid: PgId) -> Option<PgState> {
        self.pgs.lock().await.get(&pgid).map(|info| info.state)
    }

    async fn update_pg_heartbeat_ts(&self, pgid: PgId) {
        if let Some(info) = self.pgs.lock().await.get_mut(&pgid) {
            info.heartbeat_ts = Some(Instant::now());
        }
    }

    async fn set_pg_state(&self, pgid: PgId, state: PgState) {
        self.pgs
            .lock()
            .await
            .entry(pgid)
            .and_modify(|info| info.state = state);
    }

    async fn add_log(&self, log_id: u64, op: &Put) {
        self.logger.log(op, log_id).await;
    }

    async fn apply_log(&self, pgid: PgId, log_id: u64) {
        let op = self.logger.get(pgid, log_id).await.unwrap();
        let (k, v) = op.take();
        self.service
            .lock()
            .await
            .put(format!("{}.{}", pgid, k.unwrap()), v.unwrap());
        self.pgs
            .lock()
            .await
            .entry(pgid)
            .and_modify(|info| info.applied_ptr += 1)
            .or_insert(PgInfo {
                state: PgState::Inactive,
                applied_ptr: 1,
                heartbeat_ts: None,
                primary: false,
                epoch: 0,
            });
    }

    /// Check whether this server can serve `pgid`.
    ///
    /// Only Active can return Ok
    async fn check_pg_state(&self, pgid: PgId) -> Result<()> {
        match self.get_pg_state(pgid).await {
            Some(PgState::Active) => Ok(()),
            Some(PgState::Irresponsible) | None => Err(Error::WrongTarget),
            Some(x) => Err(Error::PgUnavailable(x)),
        }
    }

    async fn is_primary(&self, pgid: PgId) -> bool {
        let pgs = self.pgs.lock().await;
        pgs.get(&pgid)
            .map_or(false, |info| info.state == PgState::Active && info.primary)
    }

    async fn is_secondary(&self, pgid: PgId) -> bool {
        let pgs = self.pgs.lock().await;
        pgs.get(&pgid)
            .map_or(false, |info| info.state == PgState::Active && !info.primary)
    }

    async fn is_responsible(&self, pgid: PgId) -> bool {
        let pgs = self.pgs.lock().await;
        pgs.get(&pgid)
            .map_or(false, |info| info.state == PgState::Active)
    }

    fn assign_log_id(&self, pgid: PgId) -> u64 {
        self.sequencer[pgid].fetch_add(1, Ordering::SeqCst)
    }
}
