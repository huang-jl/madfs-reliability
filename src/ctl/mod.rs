use self::heal::HealJob;
use crate::{
    constant::*,
    distributor::{Distributor, SimpleHashDistributor},
    monitor::{client::ServerClient, TargetMap},
    rpc::{
        ConsultPgVersion, ForwardReq, Get, HealJobReq, HealReq, KvRequest, Put, Recover, RecoverRes,
    },
    service::Store,
    Error, PgId, PgVersion, Result,
};
use futures::{channel::mpsc, lock::Mutex, stream::FuturesUnordered, StreamExt};
use log::{debug, info, warn};
use madsim::{net::NetLocalHandle, task, time::sleep};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug, io, net::SocketAddr, sync::Arc, time::Duration};

pub mod heal;
pub mod peer;
pub mod recover;

/// Reliable wrapper of service
/// PG information is stored in `inner.service` K/V
pub struct ReliableCtl<T> {
    inner: Arc<Mutex<Inner<T>>>,
    monitor_client: Arc<ServerClient>,
    distributor: Arc<dyn Distributor<REPLICA_SIZE>>,
    heal_tx: mpsc::UnboundedSender<HealJob>,
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
            heal_tx: self.heal_tx.clone(),
        }
    }
}

struct Inner<T> {
    service: T,
    distributor: Box<dyn Distributor<REPLICA_SIZE>>,
    pg_states: Vec<PgState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PgState {
    /// Everything is alright
    Active,
    /// This server do not responsible for this PG
    Inactive,
    /// Cannot peering with other servers which are responsible for this pg
    Unprepared,
    /// Pg is absent (maybe recovering is in progress), needs to be recovered
    Absent,
    /// This pg on this server is out of date, some pg on other peers has higher version
    Stale,
    /// Pg will enter this state when there is a Put request,
    ///  just forwarding to peers and waiting for their response.
    Degrade,
}

#[madsim::service]
impl<T> ReliableCtl<T>
where
    T: Store + Sync + Send + 'static,
{
    pub async fn new(service: T, monitor_client: Arc<ServerClient>) -> Self {
        let (heal_tx, heal_rx) = mpsc::unbounded();
        let ctl = ReliableCtl {
            inner: Arc::new(Mutex::new(Inner::new(service))),
            monitor_client,
            distributor: Arc::new(SimpleHashDistributor),
            heal_tx,
        };
        ctl.init(heal_rx).await;
        ctl
    }

    /// 1. initialize pg state: All pgs of responsibility are marked Unprepared and wait for peering procedure
    /// 2. add rpc handler
    /// 3. start background procedure
    async fn init(&self, heal_rx: mpsc::UnboundedReceiver<HealJob>) {
        let mut inner = self.inner.lock().await;
        (0..PG_NUM)
            .filter(|&pgid| self.is_responsible_pg(pgid))
            .for_each(|pgid| inner.set_pg_state(pgid, PgState::Unprepared));
        self.add_rpc_handler();
        self.start_background_task(heal_rx);
    }

    fn start_background_task(&self, mut heal_rx: mpsc::UnboundedReceiver<HealJob>) {
        // Recovery task
        let this = self.clone();
        task::spawn(async move {
            this.background_recovery().await;
        })
        .detach();
        // Peering task
        let this = self.clone();
        task::spawn(async move {
            this.background_peer().await;
        })
        .detach();
        // Healing task
        let this = self.clone();
        task::spawn(async move {
            while let Some(job) = heal_rx.next().await {
                this.heal_for(job).await;
            }
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
        self.inner.lock().await.check_pg_state(pgid)?;

        if self.is_primary(request.key()) {
            self.inner.lock().await.set_pg_state(pgid, PgState::Degrade);
            let forward_res = self.forward_put_by_primary(request.clone()).await;

            let mut inner = self.inner.lock().await;
            if let Err(err) = forward_res {
                // Error occur when forwarding, conservatively mark it Unprepared.
                inner.set_pg_state(pgid, PgState::Unprepared);
                return Err(err);
            }
            inner.put(
                request.key().to_owned(),
                request.value().unwrap().to_owned(),
            );
            inner.set_pg_state(pgid, PgState::Active);
            Ok(())
        } else {
            Err(Error::NotPrimary)
        }
    }

    #[rpc]
    /// Handler for get request from client.
    async fn get(&self, request: Get) -> Result<Option<Vec<u8>>> {
        debug!("Receive Get from client, key = {}", request.key());
        let pgid = self.distributor.assign_pgid(request.key().as_bytes());
        self.inner.lock().await.check_pg_state(pgid)?;

        if self.is_responsible_pg(pgid) {
            Ok(self.inner.lock().await.get(request.key()))
        } else {
            Err(Error::WrongTarget)
        }
    }

    #[rpc]
    /// Handler for forward request from primary.
    async fn forward(&self, request: ForwardReq<Put>) -> Result<()> {
        let ForwardReq(request) = request;
        let pgid = self.distributor.assign_pgid(request.key().as_bytes());
        debug!(
            "Get forward put request, key = {}, pgid = {}",
            request.key(),
            pgid
        );
        self.inner.lock().await.check_pg_state(pgid)?;

        if !self.is_secondary(request.key()) {
            return Err(Error::WrongTarget);
        }
        let (key, value) = request.take();
        self.inner.lock().await.put(key.unwrap(), value.unwrap());
        Ok(())
    }

    #[rpc]
    /// Handler for recover request from peer server.
    async fn recover(&self, request: Recover) -> Result<RecoverRes> {
        let Recover(pgid) = request;
        self.inner.lock().await.check_pg_state(pgid)?;

        let inner = self.inner.lock().await;
        let pg_ver = inner.get_pg_version(pgid);
        Ok(RecoverRes {
            data: inner.service.get_pg_data(pgid),
            version: pg_ver,
        })
    }

    #[rpc]
    async fn consult_pg_info(&self, request: ConsultPgVersion) -> HashMap<PgId, PgVersion> {
        let ConsultPgVersion(pgs) = request;
        let inner = self.inner.lock().await;
        pgs.into_iter()
            .map(|pgid| (pgid, inner.get_pg_version(pgid)))
            .collect()
    }

    #[rpc]
    async fn heal_job(&self, request: HealJobReq) -> Result<()> {
        let HealJobReq(job) = request;
        let pgid = job.pgid;
        self.inner.lock().await.check_pg_state(pgid)?;
        self.heal_tx
            .unbounded_send(job)
            .expect("Send Heal jobs on futures::channel failed");
        info!("Pg {}'s heal job has been push to the queue", pgid);
        Ok(())
    }

    #[rpc]
    async fn heal(&self, request: HealReq) -> Result<()> {
        let HealReq { pgid, pg_ver, data } = request;
        let mut inner = self.inner.lock().await;
        let local_pg_ver = inner.get_pg_version(pgid);
        if local_pg_ver > pg_ver {
            return Err(Error::PgNotNewer);
        } else if local_pg_ver == pg_ver {
            return Ok(());
        }
        inner.service.push_heal_data(pgid, data);
        inner.set_pg_state(pgid, PgState::Active);
        inner.set_pg_version(pgid, pg_ver);
        warn!("Pg {} has been healed", pgid);
        Ok(())
    }

    async fn forward_put_by_primary(&self, request: Put) -> Result<()> {
        // locate the peer servers (do not forget to exclude self)
        let peers = self.get_target_addrs(request.key());
        info!("Peers = {:?}", peers);
        // prepare request to peers
        let mut tasks = peers
            .into_iter()
            .skip(1)
            .map(|peer| {
                let request = request.clone();
                async move {
                    let net = NetLocalHandle::current();
                    for _ in 0..FORWARD_RETRY {
                        let request = ForwardReq(request.clone());
                        match net
                            .call_timeout(peer.to_owned(), request, FORWARD_TIMEOUT)
                            .await
                        {
                            Ok(_) => return Ok(()),
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

    fn is_primary(&self, key: &str) -> bool {
        let target_addrs = self.get_target_addrs(key);
        self.local_addr() == target_addrs[0]
    }

    fn is_secondary(&self, key: &str) -> bool {
        let target_addrs = self.get_target_addrs(key);
        target_addrs
            .iter()
            .skip(1)
            .any(|addr| *addr == self.local_addr())
    }

    fn is_responsible_pg(&self, pgid: PgId) -> bool {
        let target_map = self.get_target_map();
        let target_addrs = self.distributor.locate(pgid, &target_map);
        target_addrs.iter().any(|addr| *addr == self.local_addr())
    }

    fn get_target_map(&self) -> TargetMap {
        self.monitor_client.get_local_target_map()
    }

    fn get_target_addrs(&self, key: &str) -> [SocketAddr; REPLICA_SIZE] {
        let pgid = self.distributor.assign_pgid(key.as_bytes());
        self.distributor.locate(pgid, &self.get_target_map())
    }
}

impl<T> Inner<T>
where
    T: Store,
{
    fn new(service: T) -> Self {
        let this = Self {
            distributor: Box::new(SimpleHashDistributor::<REPLICA_SIZE>),
            service,
            pg_states: vec![PgState::Inactive; PG_NUM],
        };
        this
    }
    /*
     * Helper methods
     */

    fn put(&mut self, key: String, value: Vec<u8>) {
        let pgid = self.distributor.assign_pgid(key.as_bytes());
        self.service.put(format!("{}.{}", pgid, key), value);
        // Increment version
        self.incr_pg_version(pgid);
        info!("Put {}, version = {}", key, self.get_pg_version(pgid));
    }

    fn get(&mut self, key: &str) -> Option<Vec<u8>> {
        let pgid = self.distributor.assign_pgid(key.as_bytes());
        self.service.get(&format!("{}.{}", pgid, key))
    }

    fn get_pg_version(&self, pgid: PgId) -> PgVersion {
        assert!(pgid < PG_NUM);
        let key = format!("pgver.{}", pgid);
        self.service
            .get(&key)
            .map(|data| PgVersion::from_be_bytes(data.try_into().unwrap()))
            .unwrap_or(0)
    }

    fn set_pg_version(&mut self, pgid: PgId, version: PgVersion) {
        let key = format!("pgver.{}", pgid);
        self.service.put(key, version.to_be_bytes().into());
    }

    fn incr_pg_version(&mut self, pgid: PgId) {
        let key = format!("pgver.{}", pgid);
        let ver = self.get_pg_version(pgid) + 1;
        self.service.put(key, ver.to_be_bytes().into());
    }

    fn get_pg_state(&self, pgid: PgId) -> PgState {
        self.pg_states[pgid]
    }

    fn set_pg_state(&mut self, pgid: PgId, state: PgState) {
        self.pg_states[pgid] = state;
    }

    /// Check whether this server can serve `pgid`.
    ///
    /// Only Active can return Ok
    fn check_pg_state(&self, pgid: PgId) -> Result<()> {
        match self.get_pg_state(pgid) {
            PgState::Active => Ok(()),
            PgState::Inactive => Err(Error::WrongTarget),
            // Unprepared, Absent, Stale
            x => Err(Error::PgUnavailable(x)),
        }
    }
}
