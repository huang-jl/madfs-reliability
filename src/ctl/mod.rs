use self::logging::LogManager;
use crate::{
    call_timeout_retry,
    constant::*,
    distributor::{Distributor, SimpleHashDistributor},
    monitor::{client::ServerClient, TargetMap},
    rpc::{
        EpochRequest, FetchHealData, ForwardReq, Get, HealData, HealReq, KvRequest, PeerFinish,
        PgConsult, PgHeartbeat, Put,
    },
    service::Store,
    Error, PgId, PgVersion, Result, TargetMapVersion,
};
use futures::{lock::Mutex, stream::FuturesUnordered, Future, StreamExt};
use log::{debug, error};
use madsim::{fs, net::NetLocalHandle, task, time::Instant};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt::Debug,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex as StdMutex,
    },
    task::Waker,
};

mod cleaner;
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
    pgs: StdMutex<BTreeMap<PgId, PgInfo>>,
    /// Recording the next index (e.g. 5 means the next operation will be assigned index 5)
    sequencer: Vec<AtomicU64>,
    wakers: StdMutex<BTreeMap<PgId, BTreeMap<u64, Waker>>>,
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
    /// The pg is unavailable (need peering).
    Inactive,
    /// Error occur during peering, need to repeering
    Damaged,
    /// Error occuring when forwarding or heartbeat
    Inconsistent,
    /// The pg is not of responsibility
    Irresponsible,
}

struct ApplyTask<'a, T> {
    inner: &'a Inner<T>,
    log_id: u64,
    pgid: PgId,
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
            let lower = self.inner.logger.lower(pgid);
            let upper = self.inner.logger.upper(pgid);
            if lower > 0 {
                self.inner
                    .install(
                        pgid,
                        lower,
                        fs::read(format!("snapshot.{}", pgid))
                            .await
                            .expect("Cannot get snapshot"),
                    )
                    .await;
            }
            for log_id in lower..upper {
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
        self.inner.check_pg_state(pgid)?;
        self.check_epoch(&request)?;

        if self.inner.is_primary(pgid) {
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
                self.inner.set_pg_state(pgid, PgState::Inconsistent);
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
        self.inner.check_pg_state(pgid)?;

        if self.inner.is_responsible(pgid) {
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
            "Get forward put request, key = {}, pgid = {}, id = {}",
            op.key(),
            pgid,
            id
        );
        self.inner.check_pg_state(pgid)?;

        if !self.inner.is_secondary(pgid) {
            return Err(Error::WrongTarget);
        }
        self.inner.add_log(id, &op).await;
        self.inner.apply_log(pgid, id).await;
        Ok(())
    }

    /// PgConsult allow two epoch not match because it may happen
    /// during peering, and peers do not know cluster map update
    #[rpc]
    async fn pg_consult(&self, request: PgConsult) -> Result<PgVersion> {
        debug!("Get pg consult request: {:?}", request);
        let PgConsult { pgid, epoch } = request;
        let local_epoch = self.get_epoch();
        if epoch < local_epoch {
            return Err(Error::EpochNotMatch(local_epoch));
        } else if epoch > local_epoch {
            self.monitor_client.update_target_map().await;
            if epoch != self.get_epoch() {
                return Err(Error::EpochNotMatch(local_epoch));
            }
        }
        Ok(self.inner.get_pg_version(pgid))
    }

    #[rpc]
    async fn peer_finish(&self, request: PeerFinish) -> Result<()> {
        debug!("Get peer finish request: {}", request);
        self.check_epoch(&request)?;
        let PeerFinish {
            pgid, heal_data, ..
        } = request;
        self.handle_heal_data(pgid, heal_data).await;
        Ok(())
    }

    #[rpc]
    async fn fetch_heal_res(&self, request: FetchHealData) -> Result<HealData> {
        debug!("Get heal request {:?}", request);
        self.check_epoch(&request)?;
        let FetchHealData { pgid, pg_ver, .. } = request;
        let local_pg_ver = self.inner.get_pg_version(pgid);
        if local_pg_ver < pg_ver {
            return Err(Error::PgStale);
        }
        Ok(self.gen_heal_res(pgid, pg_ver).await)
    }

    #[rpc]
    async fn pg_heartbeat(&self, request: PgHeartbeat) -> Result<()> {
        self.check_epoch(&request)?;
        let PgHeartbeat { pgid, .. } = request;
        self.inner.check_pg_state(pgid)?;
        self.inner.update_pg_heartbeat_ts(pgid);
        Ok(())
    }

    #[rpc]
    async fn heal_req(&self, request: HealReq) -> Result<()> {
        self.check_epoch(&request)?;
        let HealReq {
            pgid, heal_data, ..
        } = request;
        self.handle_heal_data(pgid, heal_data).await;
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
                    let request = ForwardReq {
                        id: op_id,
                        op: request.clone(),
                        epoch: self.get_epoch(),
                    };
                    call_timeout_retry(peer.to_owned(), request, FORWARD_TIMEOUT, RETRY_TIMES).await
                }
            })
            .collect::<FuturesUnordered<_>>();

        while let Some(res) = tasks.next().await {
            res??;
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

    /// 1. local epoch is greater: refusing the request.
    /// 2. local epoch is less: refusing the request and try to update target map.
    /// 3. equal: passing the check.
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
            pgs: StdMutex::new(BTreeMap::new()),
            sequencer,
            wakers: StdMutex::new(BTreeMap::new()),
        }
    }
    /*
     * Helper methods
     */

    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        let pgid = self.distributor.assign_pgid(key.as_bytes());
        self.service.lock().await.get(&format!("{}.{}", pgid, key))
    }

    /// snapshot PG `pgid` until `applied_ptr`
    async fn snapshot(&self, pgid: PgId) {
        let service = self.service.lock().await; // lock the service
        let info = self.pgs.lock().unwrap().get(&pgid).cloned();
        if let Some(info) = info {
            let data = service.get_pg_data(pgid);
            let file = fs::File::create(format!("snapshot.{}", pgid))
                .await
                .unwrap();
            file.write_all_at(&data, 0).await.unwrap();
            self.logger.snapshot(pgid, info.applied_ptr).await;
            debug!("Snapshot pg {} until {}", pgid, info.applied_ptr);
        }
    }

    async fn install(&self, pgid: PgId, id: PgVersion, data: Vec<u8>) {
        self.service.lock().await.push_pg_data(pgid, data);
        self.pgs
            .lock()
            .unwrap()
            .entry(pgid)
            .or_insert(PgInfo::default())
            .applied_ptr = id;
    }

    fn get_pg_version(&self, pgid: PgId) -> PgVersion {
        self.logger.upper(pgid)
    }

    fn get_pg_state(&self, pgid: PgId) -> Option<PgState> {
        self.pgs.lock().unwrap().get(&pgid).map(|info| info.state)
    }

    fn update_pg_heartbeat_ts(&self, pgid: PgId) {
        if let Some(info) = self.pgs.lock().unwrap().get_mut(&pgid) {
            info.heartbeat_ts = Some(Instant::now());
        }
    }

    fn set_pg_state(&self, pgid: PgId, state: PgState) {
        self.pgs
            .lock()
            .unwrap()
            .entry(pgid)
            .and_modify(|info| info.state = state);
    }

    async fn add_log(&self, log_id: u64, op: &Put) {
        self.logger.log(op, log_id).await;
    }

    async fn apply_log(&self, pgid: PgId, log_id: u64) {
        self.wait_for_apply(pgid, log_id).await;
        let op = self.logger.get(pgid, log_id).await.unwrap();
        let (k, v) = op.take();
        self.service
            .lock()
            .await
            .put(format!("{}.{}", pgid, k.unwrap()), v.unwrap());
        let mut pgs = self.pgs.lock().unwrap();
        let info = pgs
            .get_mut(&pgid)
            .expect(&format!("Cannot get Pg {}'s info", pgid));
        // let info = pgs.entry(pgid).or_insert(PgInfo {
        //     state: PgState::Inactive,
        //     applied_ptr: 0,
        //     heartbeat_ts: None,
        //     primary: false,
        //     epoch: 0,
        // });
        assert_eq!(
            info.applied_ptr, log_id,
            "applying log {} for pg {} does not obey order",
            log_id, pgid
        );
        info.applied_ptr += 1;
        // wake the waiting task
        if let Some(wakers) = self.wakers.lock().unwrap().get_mut(&pgid) {
            if let Some(waker) = wakers.remove(&info.applied_ptr) {
                waker.wake();
            }
        }
    }

    /// Check whether this server can serve `pgid`.
    ///
    /// Only Active can return Ok
    fn check_pg_state(&self, pgid: PgId) -> Result<()> {
        match self.get_pg_state(pgid) {
            Some(PgState::Active) => Ok(()),
            Some(PgState::Irresponsible) | None => Err(Error::WrongTarget),
            Some(x) => Err(Error::PgUnavailable(x)),
        }
    }

    fn is_primary(&self, pgid: PgId) -> bool {
        let pgs = self.pgs.lock().unwrap();
        pgs.get(&pgid)
            .map_or(false, |info| info.state == PgState::Active && info.primary)
    }

    fn is_secondary(&self, pgid: PgId) -> bool {
        let pgs = self.pgs.lock().unwrap();
        pgs.get(&pgid)
            .map_or(false, |info| info.state == PgState::Active && !info.primary)
    }

    fn is_responsible(&self, pgid: PgId) -> bool {
        let pgs = self.pgs.lock().unwrap();
        pgs.get(&pgid)
            .map_or(false, |info| info.state == PgState::Active)
    }

    fn assign_log_id(&self, pgid: PgId) -> u64 {
        self.sequencer[pgid].fetch_add(1, Ordering::SeqCst)
    }

    /// `ApplyTask` is a future which will wait until `applied_ptr == log_id`
    fn wait_for_apply(&self, pgid: PgId, log_id: u64) -> ApplyTask<T> {
        ApplyTask {
            inner: self,
            log_id,
            pgid,
        }
    }
}

impl Default for PgInfo {
    fn default() -> Self {
        Self {
            state: PgState::Inconsistent,
            applied_ptr: 0,
            heartbeat_ts: None,
            primary: false,
            epoch: 0,
        }
    }
}

impl<'a, T> Future for ApplyTask<'a, T> {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut pgs = self.inner.pgs.lock().unwrap();
        let mut global_wakers = self.inner.wakers.lock().unwrap();
        let info = pgs.entry(self.pgid).or_insert(PgInfo::default());
        let wakers = global_wakers.entry(self.pgid).or_insert(BTreeMap::new());
        if info.applied_ptr == self.log_id {
            return std::task::Poll::Ready(());
        }

        wakers.insert(self.log_id, cx.waker().clone());

        std::task::Poll::Pending
    }
}
