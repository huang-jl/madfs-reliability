use crate::{
    constant::REPLICA_SIZE,
    ctl::ReliableCtl,
    distributor::{Distributor, SimpleHashDistributor},
    monitor::{
        client::{Client as MonitorClient, ServerClient},
        Monitor,
    },
    rpc::KvRequest,
    service::KvService,
    Error, Result,
};
use lazy_static::lazy_static;
use log::*;
use madsim::{
    net::{rpc::Request, NetLocalHandle},
    Handle, LocalHandle,
};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::{
    io::{self, ErrorKind},
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

lazy_static! {
    static ref RNG: Mutex<StdRng> = Mutex::new(StdRng::seed_from_u64(
        std::env::var("SEED")
            .unwrap_or("2017010191".to_owned())
            .parse()
            .unwrap(),
    ));
}

#[derive(thiserror::Error, Debug)]
enum ClientError {
    #[error("")]
    IoError(#[from] std::io::Error),
}

pub struct KvServerCluster {
    servers: Vec<ReliableCtl<KvService>>,
    addrs: Vec<SocketAddr>,
    handle: Handle,
}

#[derive(Debug, Clone, Copy)]
pub struct ClientId(u64);

pub struct Client {
    id: ClientId,
    handle: LocalHandle,
    pub monitor_client: Arc<MonitorClient>,
    distributor: Box<dyn Distributor<REPLICA_SIZE>>,
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            handle: self.handle.clone(),
            monitor_client: self.monitor_client.clone(),
            distributor: Box::new(SimpleHashDistributor::<REPLICA_SIZE>),
        }
    }
}

pub fn gen_random_put(key_len: usize, val_len: usize) -> (String, Vec<u8>) {
    let mut chartset = Vec::new();
    for i in 'A'..'Z' {
        chartset.push(i);
    }
    for i in 'a'..'z' {
        chartset.push(i);
    }
    let gen_random = |length: usize| -> String {
        (0..length)
            .map(|_| {
                let idx = RNG.lock().unwrap().gen_range(0..chartset.len());
                chartset[idx]
            })
            .collect()
    };
    (gen_random(key_len), gen_random(val_len).into_bytes())
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

    pub async fn send<T>(&self, request: T, retry: Option<u32>) -> Result<<T as Request>::Response>
    where
        T: KvRequest + Clone,
    {
        let targets = self.get_target_addrs(&request.key().as_bytes()).await;
        self.send_to(request, targets[0], retry).await
    }

    pub async fn get_target_addrs(&self, key: &[u8]) -> [SocketAddr; REPLICA_SIZE] {
        let pgid = self.distributor.assign_pgid(key);
        let target_map = self.monitor_client.get_local_target_map().await;
        self.distributor.locate(pgid, &target_map)
    }

    pub async fn send_to<T>(
        &self,
        request: T,
        target: SocketAddr,
        retry: Option<u32>,
    ) -> Result<<T as Request>::Response>
    where
        T: KvRequest + Clone,
    {
        const TIMEOUT: Duration = Duration::from_millis(10_000);
        // Add pgid prefix to the requesting key
        self.handle
            .spawn(async move {
                let net = NetLocalHandle::current();
                match retry {
                    Some(retry) => {
                        for _ in 0..retry {
                            match net.call_timeout(target, request.clone(), TIMEOUT).await {
                                Ok(res) => return Ok(res),
                                Err(err) if err.kind() == io::ErrorKind::TimedOut => {}
                                Err(err) => return Err(Error::NetworkError(err.to_string())),
                            }
                        }
                        Err(Error::NetworkError(
                            std::io::Error::from(ErrorKind::TimedOut).to_string(),
                        ))
                    }
                    None => match net.call_timeout(target, request, TIMEOUT).await {
                        Ok(res) => return Ok(res),
                        Err(err) => return Err(Error::NetworkError(err.to_string())),
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
        addrs.iter().for_each(|addr| {
            handle.create_host(addr).unwrap();
            handle.net.connect(addr.clone());
        });
        let mut servers = Vec::new();
        for id in 0..server_num {
            let local_handle = handle.get_host(addrs[id]).unwrap();
            servers.push(
                local_handle
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
            handle,
        }
    }

    pub fn get_addrs(&self) -> Vec<SocketAddr> {
        self.addrs.clone()
    }

    pub fn crash(&self, idx: usize) {
        self.handle.kill(self.addrs[idx]);
    }

    pub fn disconnect(&self, idx: usize) {
        self.handle.net.disconnect(self.addrs[idx]);
    }

    pub fn connect(&self, idx: usize) {
        self.handle.net.connect(self.addrs[idx]);
    }
}
