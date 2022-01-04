use crate::{
    constant::REPLICA_SIZE,
    ctl::ReliableCtl,
    distributor::{Distributor, SimpleHashDistributor},
    monitor::{
        client::{Client as MonitorClient, ServerClient},
        Monitor,
    },
    service::{KvArgs, KvRes, KvService, ServiceInput},
    Error,
};
use log::*;
use madsim::{net::NetLocalHandle, Handle, LocalHandle};
use rand::{thread_rng, Rng};
use std::{
    io::{self, ErrorKind},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

pub const MONITOR_ADDR: &str = "10.0.0.1:8000";

#[derive(thiserror::Error, Debug)]
enum ClientError {
    #[error("")]
    IoError(#[from] std::io::Error),
}

pub struct KvServerCluster {
    servers: Vec<ReliableCtl<KvService>>,
    addrs: Vec<SocketAddr>,
    handles: Vec<LocalHandle>,
}

pub struct ClientId(u64);

pub struct Client {
    id: ClientId,
    handle: LocalHandle,
    monitor_client: Arc<MonitorClient>,
    distributor: Box<dyn Distributor<REPLICA_SIZE>>,
}

pub fn gen_random_put(key_len: usize, val_len: usize) -> KvArgs {
    let mut chartset = Vec::new();
    for i in 'A'..'Z' {
        chartset.push(i);
    }
    for i in 'a'..'z' {
        chartset.push(i);
    }

    let mut rng = thread_rng();
    let mut gen_random = |length: usize| -> String {
        (0..length)
            .map(|_| {
                let idx = rng.gen_range(0..chartset.len());
                chartset[idx]
            })
            .collect()
    };
    KvArgs::Put {
        key: gen_random(key_len),
        value: gen_random(val_len),
    }
}

/// - `pg_num` - total number of placement group
/// - `addr` - str representation of `SocketAddr` (e.g. `"127.0.0.1:5432"`)
/// - `server_addrs` - list of serivce servers
///
/// For now we assumes that there will be no new server added to cluster
pub async fn create_monitor(pg_num: usize, addr: &str, server_addrs: Vec<SocketAddr>) -> Monitor {
    let addr = addr.parse::<SocketAddr>().unwrap();
    let handle = madsim::Handle::current();
    let host = handle.create_host(addr).unwrap();
    handle.net.connect(host.local_addr());
    host.spawn(async move { Monitor::new(pg_num, server_addrs) })
        .await
}

pub fn gen_server_addr(id: usize) -> SocketAddr {
    format!("0.0.1.{}:0", id).parse().unwrap()
}

pub fn str_to_addr(addr: &str) -> SocketAddr {
    addr.parse().unwrap()
}

impl ClientId {
    pub fn to_addr(&self) -> SocketAddr {
        ([0, 1, 0, self.0 as _], 0).into()
    }
}

impl Client {
    pub fn new(id: u64, monitor_client: Arc<MonitorClient>) -> Self {
        let handle = Handle::current();
        let id = ClientId(id);
        let client_addr = id.to_addr();
        let client = Client {
            id,
            handle: handle.create_host(client_addr).unwrap(),
            monitor_client,
            distributor: Box::new(SimpleHashDistributor),
        };
        handle.net.connect(client_addr);
        client
    }

    pub async fn send(&self, request: KvArgs, retry: Option<u32>) -> Result<KvRes, Error> {
        let targets = self.get_target_addrs(&request).await;
        self.send_to(request, targets[0], retry).await
    }

    pub async fn get_target_addrs(&self, request: &KvArgs) -> [SocketAddr; REPLICA_SIZE] {
        let pg_map = self.monitor_client.get_local_pg_map().await;
        let target_map = self.monitor_client.get_local_target_map().await;
        let pgid = self.distributor.assign_pgid(request.key_bytes(), &pg_map);
        self.distributor.locate(pgid, &target_map)
    }

    pub async fn send_to(
        &self,
        request: KvArgs,
        target: SocketAddr,
        retry: Option<u32>,
    ) -> Result<KvRes, Error> {
        const TIMEOUT: Duration = Duration::from_millis(2000);
        self.handle
            .spawn(async move {
                let net = NetLocalHandle::current();
                match retry {
                    Some(retry) => {
                        for _ in 0..retry {
                            match net.call_timeout(target, request.clone(), TIMEOUT).await {
                                Ok(res) => return res.map_err(|err| err.into()),
                                Err(err) if err.kind() == io::ErrorKind::TimedOut => {}
                                Err(err) => return Err(Error::IoError(err)),
                            }
                        }
                        Err(Error::IoError(std::io::Error::from(ErrorKind::TimedOut)))
                    }
                    None => match net.call_timeout(target, request, TIMEOUT).await {
                        Ok(res) => return res.map_err(|err| err.into()),
                        Err(err) => return Err(Error::IoError(err)),
                    },
                }
            })
            .await
    }
}

impl KvServerCluster {
    pub async fn new(server_num: usize, monitor_addr: &str) -> Self {
        let monitor_addr = monitor_addr.parse().unwrap();
        let handle = Handle::current();
        let addrs = (0..server_num)
            .map(|id| SocketAddr::from(([0, 0, 1, id as u8], 0)))
            .collect::<Vec<_>>();
        let handles: Vec<_> = addrs
            .iter()
            .map(|addr| {
                let local_handle = handle.create_host(addr).unwrap();
                handle.net.connect(addr.clone());
                local_handle
            })
            .collect();
        let mut servers = Vec::new();
        for id in 0..server_num {
            servers.push(
                handles[id]
                    .spawn(async move {
                        ReliableCtl::new(
                            KvService::new(),
                            ServerClient::new(id as _, monitor_addr).await.unwrap(),
                        )
                    })
                    .await,
            );
        }
        KvServerCluster {
            servers,
            addrs,
            handles,
        }
    }

    pub fn get_addrs(&self) -> Vec<SocketAddr> {
        self.addrs.clone()
    }
}
