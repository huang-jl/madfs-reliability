use crate::{
    constant::*,
    distributor::{Distributor, SimpleHashDistributor},
    monitor::{client::ServerClient, TargetMap},
    rpc::{ConsultPgInfo, ForwardReq, Get, KvRequest, Put, Recover, RecoverRes},
    service::Store,
    Error, PgId, PgVersion, Result,
};
use futures::{
    lock::Mutex,
    stream::{FuturesOrdered, FuturesUnordered},
    StreamExt,
};
use log::{error, info, warn};
use madsim::{net::NetLocalHandle, task, time::sleep};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, io, net::SocketAddr, sync::Arc, time::Duration};

/// Reliable wrapper of service
/// PG information is stored in `inner.service` K/V
pub struct ReliableCtl<T> {
    inner: Arc<Mutex<Inner<T>>>,
    monitor_client: Arc<ServerClient>,
    distributor: Arc<dyn Distributor<REPLICA_SIZE>>,
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
        }
    }
}

struct Inner<T> {
    service: T,
    distributor: Box<dyn Distributor<REPLICA_SIZE>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgInfo {
    version: PgVersion,
    state: PgState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PgState {
    Active,   // Everything is alright
    Inactive, // This server do not responsible for this PG
    Unclean,  // Recovery is happening
    Stale,    // This pg on this server is out of date
}

impl PgInfo {
    /// Default state is PgState::Inactive
    pub fn new() -> Self {
        PgInfo {
            version: 0,
            state: PgState::Inactive,
        }
    }
}

#[madsim::service]
impl<T> ReliableCtl<T>
where
    T: Store + Sync + Send + 'static,
{
    pub fn new(service: T, monitor_client: Arc<ServerClient>) -> Self {
        let ctl = ReliableCtl {
            inner: Arc::new(Mutex::new(Inner::new(service))),
            monitor_client,
            distributor: Arc::new(SimpleHashDistributor),
        };
        ctl.add_rpc_handler();
        ctl.start_background_task();
        ctl
    }

    /// Checking for all PGs, aims to:
    /// 1. Find the pgs of responsibility
    /// 2. Check whether local pg is the most up-to-date
    /// 3. Update PgInfo
    async fn peering(&self) {
        let local_addr = self.local_addr();
        for pgid in 0..PG_NUM {
            let target_map = self.get_target_map();
            let target_addrs = self.distributor.locate(pgid, &target_map);
            if !target_addrs.iter().any(|addr| *addr == local_addr) {
                continue;
            }
            let tasks = target_addrs
                .into_iter()
                .filter(|addr| *addr != local_addr)
                .map(|addr| {
                    let request = ConsultPgInfo(pgid);
                    async move {
                        let net = NetLocalHandle::current();
                        net.call_timeout(addr, request, CONSULT_TIMEOUT).await
                    }
                })
                .collect::<FuturesOrdered<_>>();
            let peer_res: Vec<io::Result<PgInfo>> = tasks.collect().await;
            let mut inner = self.inner.lock().await;
            let mut local_pg_info = inner.get_pg_info(pgid);
            // Scan the results:
            // 1. log the errors
            // 2. check if there is a more up-to-date pg on peers
            if peer_res
                .iter()
                .filter(|res| match res {
                    Err(err) => {
                        warn!("Error occur when peering: {}", err);
                        false
                    }
                    _ => true,
                })
                .map(|res| res.as_ref().unwrap())
                .any(|res| res.version > local_pg_info.version)
            {
                warn!(
                    "Detect pg {} is stale, local version = {}",
                    pgid, local_pg_info.version
                );
                local_pg_info.state = PgState::Stale;
                inner.set_pg_info(pgid, local_pg_info);
            } else if peer_res.iter().all(|res| !matches!(res, Err(_))) {
                info!(
                    "Pg {} peer succeed, local version = {}",
                    pgid, local_pg_info.version
                );
                // The pg on this server is the newest and all peers response the Consult Request
                local_pg_info.state = PgState::Active;
                inner.set_pg_info(pgid, local_pg_info);
            } else {
                // Network error during peering.
                // Conservatively mark this pg as `Unclean` to prevent it from serving request
                local_pg_info.state = PgState::Unclean;
                inner.set_pg_info(pgid, local_pg_info);
            }
        }
    }

    fn start_background_task(&self) {
        let this = self.clone();
        task::spawn(async move {
            this.background_recovery().await;
        })
        .detach();
        let this = self.clone();
        task::spawn(async move {
            loop {
                this.peering().await;
                sleep(Duration::from_millis(5000)).await;
            }
        })
        .detach();
    }

    /*
     * Background task
     */
    async fn background_recovery(&self) {
        let mut watch_version = self.get_target_map().get_version();
        loop {
            self.monitor_client
                .watch_for_target_map(Some(watch_version))
                .await;
            let target_map = self.get_target_map();
            for pgid in 0..PG_NUM {
                let target_addrs = self.distributor.locate(pgid, &target_map);
                if target_addrs
                    .iter()
                    .find(|addr| **addr == self.local_addr())
                    .is_some()
                {
                    let local_info = self.inner.lock().await.get_pg_info(pgid);
                    match local_info.state {
                        PgState::Active => {
                            todo!("If the current pg version is lagging, need to recovery [maybe another healing background task]")
                        }
                        PgState::Inactive => {
                            warn!("Start recovering pg {}", pgid);
                            if let Err(err) = self.recover_for(pgid, target_addrs[0].clone()).await
                            {
                                error!(
                                    "Error occur when recovering pg {} from primary {}: {}",
                                    pgid, target_addrs[0], err
                                );
                                continue;
                            }
                        }
                        PgState::Unclean => {
                            // Indicates last time the recovery did not succedd
                            warn!("Start recovering pg {} again: probably last time recovered incorrectly", pgid);
                            if let Err(err) = self.recover_for(pgid, target_addrs[0].clone()).await
                            {
                                error!(
                                    "Error occur when recovering pg {} from primary {}: {}",
                                    pgid, target_addrs[0], err
                                );
                                continue;
                            }
                        }
                        PgState::Stale => {
                            todo!("For now we did not cares about stale pgstate here")
                        }
                    }
                }
            }
            watch_version = target_map.get_version();
        }
    }

    /// For now: asking recovery pg data from primary
    async fn recover_for(&self, pgid: PgId, primary: SocketAddr) -> Result<()> {
        // 1. change the state of local pg
        {
            let mut inner = self.inner.lock().await;
            inner.set_pg_state(pgid, PgState::Unclean);
        }
        // 2. get whole pg data from primary
        let net = NetLocalHandle::current();
        let mut response: Option<RecoverRes> = None;
        for _ in 0..RECOVER_RETRY {
            let request = Recover(pgid);
            match net.call_timeout(primary, request, RECOVER_TIMEOUT).await {
                Ok(res) => response = Some(res?),
                Err(err) => {
                    warn!("Send Recover request get error: {}", err);
                    continue;
                }
            }
        }
        // 3. push pg data into local and update pg_info
        match response {
            Some(RecoverRes { data, version }) => {
                let mut inner = self.inner.lock().await;
                inner.service.push_pg_data(pgid, data);
                inner.set_pg_info(
                    pgid,
                    PgInfo {
                        state: PgState::Active,
                        version,
                    },
                );
                Ok(())
            }
            None => Err(Error::NetworkError(format!(
                "Recover still error after retry for {} times",
                RECOVER_RETRY
            ))),
        }
    }

    /*
     * RPC Handler
     */

    #[rpc]
    /// Handler for request directly from client.
    async fn put(&self, request: Put) -> Result<()> {
        info!("Receive Put from client, key = {}", request.key());
        let pgid = self.distributor.assign_pgid(request.key().as_bytes());
        self.check_pg_state(pgid).await?;

        if self.is_primary(request.key()) {
            self.forward_put_by_primary(request.clone()).await?;
            Ok(self.inner.lock().await.put(
                request.key().to_owned(),
                request.value().unwrap().to_owned(),
            ))
        } else {
            Err(Error::NotPrimary)
        }
    }

    #[rpc]
    /// Handler for get request from client.
    async fn get(&self, request: Get) -> Result<Option<Vec<u8>>> {
        info!("Receive Get from client, key = {}", request.key());
        let pgid = self.distributor.assign_pgid(request.key().as_bytes());
        self.check_pg_state(pgid).await?;

        if self.is_primary(request.key()) || self.is_secondary(request.key()) {
            Ok(self.inner.lock().await.get(request.key()))
        } else {
            Err(Error::WrongTarget)
        }
    }

    #[rpc]
    /// Handler for forward request from primary.
    async fn forward(&self, request: ForwardReq<Put>) -> Result<()> {
        let ForwardReq(request) = request;
        info!("Get forward put request, key = {}", request.key());
        let pgid = self.distributor.assign_pgid(request.key().as_bytes());
        self.check_pg_state(pgid).await?;

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
        self.check_pg_state(pgid).await?;

        let inner = self.inner.lock().await;
        let pg_info = inner.get_pg_info(pgid);
        Ok(RecoverRes {
            data: inner.service.get_pg_data(pgid),
            version: pg_info.version,
        })
    }

    #[rpc]
    async fn consult_pg_info(&self, request: ConsultPgInfo) -> PgInfo {
        let ConsultPgInfo(pgid) = request;
        self.inner.lock().await.get_pg_info(pgid)
    }

    async fn forward_put_by_primary(&self, request: Put) -> Result<()> {
        let mut tasks = FuturesUnordered::new();
        // 1. Locate the peer servers
        let peers = self.get_target_addrs(request.key());
        // 2. Send request to peers
        for peer in peers.into_iter().skip(1) {
            let request = request.clone();
            tasks.push(async move {
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
            });
        }
        for res in tasks.next().await {
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
            .find(|addr| **addr == self.local_addr())
            .is_some()
    }

    fn get_target_map(&self) -> TargetMap {
        self.monitor_client.get_local_target_map()
    }

    fn get_target_addrs(&self, key: &str) -> [SocketAddr; REPLICA_SIZE] {
        let pgid = self.distributor.assign_pgid(key.as_bytes());
        self.distributor.locate(pgid, &self.get_target_map())
    }

    async fn check_pg_state(&self, pgid: PgId) -> Result<()> {
        let inner = self.inner.lock().await;
        match inner.get_pg_info(pgid).state {
            PgState::Active => Ok(()),
            PgState::Inactive => Err(Error::WrongTarget),
            PgState::Unclean => Err(Error::PgIsRecovering),
            PgState::Stale => todo!(),
        }
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
        };
        this
    }
    /*
     * Helper methods
     */

    fn put(&mut self, key: String, value: Vec<u8>) {
        let pgid = self.distributor.assign_pgid(key.as_bytes());
        self.service.put(format!("{}.{}", pgid, key), value);
        self.increase_pg_version(pgid);
    }

    fn get(&mut self, key: &str) -> Option<Vec<u8>> {
        let pgid = self.distributor.assign_pgid(key.as_bytes());
        self.service.get(&format!("{}.{}", pgid, key))
    }

    fn get_pg_info(&self, pgid: PgId) -> PgInfo {
        assert!(pgid < PG_NUM);
        let key = format!("pginfo.{}", pgid);
        self.service
            .get(&key)
            .map(|data| bincode::deserialize(&data).unwrap())
            .unwrap_or(PgInfo::new())
    }

    fn set_pg_info(&mut self, pgid: PgId, pg_info: PgInfo) {
        let key = format!("pginfo.{}", pgid);
        self.service.put(key, bincode::serialize(&pg_info).unwrap());
    }

    fn set_pg_state(&mut self, pgid: PgId, state: PgState) {
        let mut pg_info = self.get_pg_info(pgid);
        pg_info.state = state;
        self.set_pg_info(pgid, pg_info);
    }

    fn increase_pg_version(&mut self, pgid: PgId) {
        let mut pg_info = self.get_pg_info(pgid);
        pg_info.version += 1;
        self.set_pg_info(pgid, pg_info);
    }
}
