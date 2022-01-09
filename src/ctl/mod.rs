use self::heal::HealJob;
use crate::{
    constant::*,
    distributor::{Distributor, SimpleHashDistributor},
    monitor::{client::ServerClient, TargetMap},
    rpc::{ConsultPgInfo, ForwardReq, Get, KvRequest, Put, Recover, RecoverRes},
    service::Store,
    Error, PgId, PgVersion, Result,
};
use futures::{
    channel::mpsc,
    lock::Mutex,
    stream::{FuturesOrdered, FuturesUnordered},
    StreamExt,
};
use log::{error, info, warn};
use madsim::{net::NetLocalHandle, task, time::sleep};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, io, net::SocketAddr, sync::Arc, time::Duration};

mod heal;
mod peer;
mod recover;

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgInfo {
    /// version = 0 means this pg is not served before,
    /// since the first `put` will increment version to 1.
    version: PgVersion,
    state: PgState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
        let (heal_tx, heal_rx) = mpsc::unbounded();
        let ctl = ReliableCtl {
            inner: Arc::new(Mutex::new(Inner::new(service))),
            monitor_client,
            distributor: Arc::new(SimpleHashDistributor),
            heal_tx,
        };
        ctl.add_rpc_handler();
        ctl.start_background_task(heal_rx);
        ctl
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
            loop {
                this.peering().await;
                sleep(Duration::from_millis(5000)).await;
            }
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
                if target_addrs.iter().any(|addr| *addr == self.local_addr()) {
                    let local_info = self.inner.lock().await.get_pg_info(pgid);
                    match local_info.state {
                        PgState::Active => {}
                        PgState::Absent | PgState::Inactive => {
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
                        PgState::Stale | PgState::Unprepared => {
                            // For now we did not cares about stale or unprepraed state in recovery
                        }
                    }
                }
            }
            watch_version = target_map.get_version();
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
            self.inner.lock().await.put(
                request.key().to_owned(),
                request.value().unwrap().to_owned(),
            );
            Ok(())
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
        info!("Peers = {:?}", peers);
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

    /// Check whether this server can serve `pgid`.
    async fn check_pg_state(&self, pgid: PgId) -> Result<()> {
        match self.inner.lock().await.get_pg_info(pgid).state {
            // TODO: Can stale pg pass checking ?
            PgState::Active | PgState::Stale => Ok(()),
            PgState::Inactive => Err(Error::WrongTarget),
            // Unprepared, Absent
            x => Err(Error::PgUnavailable(x)),
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
        info!("Put {}", key);
        let pgid = self.distributor.assign_pgid(key.as_bytes());
        self.service.put(format!("{}.{}", pgid, key), value);
        // Increment version
        self.update_pg_info(pgid, |mut pg_info| {
            pg_info.version += 1;
            pg_info
        });
    }

    fn get(&mut self, key: &str) -> Option<Vec<u8>> {
        info!("Get {}", key);
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

    fn update_pg_info<F>(&mut self, pgid: PgId, f: F)
    where
        F: FnOnce(PgInfo) -> PgInfo,
    {
        let updated_pg_info = f(self.get_pg_info(pgid));
        let key = format!("pginfo.{}", pgid);
        self.service
            .put(key, bincode::serialize(&updated_pg_info).unwrap());
    }
}
