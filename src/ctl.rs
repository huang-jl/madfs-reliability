use crate::{
    constant::*,
    distributor::{Distributor, SimpleHashDistributor},
    monitor::{client::ServerClient, TargetMap},
    rpc::{ForwardReq, Get, KvRequest, Put, Recover},
    service::Store,
    Error, PgId, Result,
};
use futures::{lock::Mutex, stream::FuturesUnordered, StreamExt};
use log::{error, info, warn};
use madsim::{net::NetLocalHandle, task};
use std::{fmt::Debug, io, net::SocketAddr, sync::Arc};

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
    pg_infos: Vec<PgInfo>,
    distributor: Box<dyn Distributor<REPLICA_SIZE>>,
}

#[derive(Debug, Clone)]
pub struct PgInfo {
    version: u64,
    state: PgState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
            inner: Arc::new(Mutex::new(Inner {
                service,
                pg_infos: vec![PgInfo::new(); PG_NUM],
                distributor: Box::new(SimpleHashDistributor),
            })),
            monitor_client,
            distributor: Arc::new(SimpleHashDistributor),
        };
        ctl.add_rpc_handler();
        ctl.start_background_task();
        ctl
    }

    #[rpc]
    async fn get(&self, request: Get) -> Result<Option<String>> {
        info!("Receive Get from client: {:?}", request);
        if self.is_primary(request.key()) || self.is_secondary(request.key()) {
            Ok(self.inner.lock().await.get(request.key()))
        } else {
            Err(Error::WrongTarget)
        }
    }

    fn start_background_task(&self) {
        let this = self.clone();
        task::spawn(async move {
            this.background_recovery().await;
        })
        .detach();
    }

    /*
     * Background task
     */
    async fn background_recovery(&self) {
        loop {
            let _ = self.monitor_client.watch_for_target_map().await;
            let target_map = self.get_target_map();
            for pgid in 0..PG_NUM {
                let target_addrs = self.distributor.locate(pgid, &target_map);
                if target_addrs
                    .iter()
                    .find(|addr| **addr == self.local_addr())
                    .is_some()
                {
                    let local_info = self.inner.lock().await.pg_infos[pgid].clone();
                    match local_info.state {
                        PgState::Active => {
                            todo!("If the current pg version is lagging, need to recovery [maybe another healing background task]")
                        }
                        PgState::Inactive => {
                            warn!("Start recovering pg {}", pgid);
                            if let Err(err) = self.recovery(pgid, target_addrs[0].clone()).await {
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
                            if let Err(err) = self.recovery(pgid, target_addrs[0].clone()).await {
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
        }
    }

    async fn recovery(&self, pgid: PgId, primary: SocketAddr) -> Result<()> {
        // 1. change the state of local pg
        {
            let mut inner = self.inner.lock().await;
            inner.pg_infos[pgid].state = PgState::Unclean;
        }
        // 2. get whole pg data from primary
        let net = NetLocalHandle::current();
        let mut data: Option<Vec<u8>> = None;
        for _ in 0..RECOVER_RETRY {
            let request = Recover(pgid);
            match net.call_timeout(primary, request, RECOVER_TIMEOUT).await {
                Ok(res) => data = Some(res?),
                Err(_) => continue,
            }
        }
        // 3. push pg data into local
        match data {
            Some(data) => {
                let mut inner = self.inner.lock().await;
                inner.service.push_pg_data(pgid, data);
                inner.pg_infos[pgid].state = PgState::Active;
                Ok(())
            }
            None => Err(Error::NetworkError(format!(
                "Recovery still error after retry for {} times",
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
        info!("Receive Put from client: {:?}", request);
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
    /// Handle forward request from primary.
    async fn forward(&self, request: ForwardReq<Put>) -> Result<()> {
        info!("Get forward put request: {:?}", request);
        let ForwardReq(request) = request;
        if !self.is_secondary(request.key()) {
            return Err(Error::WrongTarget);
        }
        self.inner.lock().await.put(
            request.key().to_owned(),
            request.value().unwrap().to_owned(),
        );
        Ok(())
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
}

impl<T> Inner<T>
where
    T: Store,
{
    /*
     * Helper methods
     */

    fn put(&mut self, key: String, value: String) {
        let pgid = self.distributor.assign_pgid(key.as_bytes());
        self.service.put(format!("{}.{}", pgid, key), value);
    }

    fn get(&mut self, key: &str) -> Option<String> {
        let pgid = self.distributor.assign_pgid(key.as_bytes());
        self.service.get(&format!("{}.{}", pgid, key))
    }
}
