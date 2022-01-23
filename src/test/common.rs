use crate::{Error, PgId, Result, TargetMapVersion, constant::*, ctl::ReliableCtl, distributor::{Distributor, SimpleHashDistributor}, monitor::{Monitor, TargetMap, client::{Client as MonitorClient, ServerClient}}, rpc::KvRequest, service::KvService};
use lazy_static::lazy_static;
use log::*;
use madsim::{
    net::{rpc::Request, NetLocalHandle},
    time::sleep,
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
    handle: Handle,
    pg_num: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct ClientId(u64);

pub struct Client {
    id: ClientId,
    handle: LocalHandle,
    pub monitor_client: Arc<MonitorClient>,
    distributor: Arc<dyn Distributor<REPLICA_SIZE>>,
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            handle: self.handle.clone(),
            monitor_client: self.monitor_client.clone(),
            distributor: self.distributor.clone(),
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
pub async fn create_monitor(pg_num: usize, server_addrs: Vec<SocketAddr>) -> Monitor {
    let handle = madsim::Handle::current();
    let host = handle
        .create_host(MONITOR_ADDR.parse::<SocketAddr>().unwrap())
        .unwrap();
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
    pub fn new(pg_num: usize, id: u64, monitor_client: Arc<MonitorClient>) -> Self {
        let handle = Handle::current();
        let id = ClientId(id);
        let client_addr = id.to_addr();
        let client = Client {
            id,
            handle: handle.create_host(client_addr).unwrap(),
            monitor_client,
            distributor: Arc::new(SimpleHashDistributor::new(pg_num)),
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

    pub fn get_pgid(&self, key: &[u8]) -> PgId {
        self.distributor.assign_pgid(key)
    }

    pub async fn get_target_addrs(&self, key: &[u8]) -> [SocketAddr; REPLICA_SIZE] {
        let pgid = self.distributor.assign_pgid(key);
        let target_map = self.monitor_client.get_local_target_map();
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
        const TIMEOUT: Duration = Duration::from_secs(5);
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
                            sleep(Duration::from_millis(2000)).await;
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

    pub async fn update_target_map(&self) {
        let this = self.clone();
        self.handle
            .spawn(async move {
                this.monitor_client.update_target_map().await;
            })
            .await;
    }

    // We check that the data is consistent/same among all replicas and
    // check the final state is some value we put before instead of some damaged or weird data.
    pub async fn check_consistency(&self, key: &str, potential_vals: &[Vec<u8>]) {
        let targets = self.get_target_addrs(key.as_bytes()).await;
        let mut target_vals = Vec::new();
        for target in targets {
            let request = crate::rpc::Get {
                epoch: self.monitor_client.get_local_target_map().get_version(),
                key: key.to_owned(),
            };
            let res = self
                .send_to(request, target, None)
                .await;
            assert!(
                matches!(res, Ok(Ok(Some(_)))),
                "Send get (key = {}) to {} get : {:?}",
                key,
                target,
                res
            );
            let res = res.unwrap().unwrap().unwrap();
            target_vals.push(res);
        }
        assert!(target_vals
            .iter()
            .zip(target_vals.iter().skip(1))
            .all(|(a, b)| a == b));
        assert!(target_vals
            .iter()
            .all(|val| potential_vals.iter().any(|v| v == val)));
    }

    pub fn get_epoch(&self) -> TargetMapVersion {
        self.monitor_client.get_local_target_map().get_version()
    }
}

impl KvServerCluster {
    pub async fn new(pg_num: usize, server_num: usize) -> Self {
        let monitor_addr = MONITOR_ADDR.parse().unwrap();
        let handle = Handle::current();
        let mut servers = Vec::new();
        for id in 0..server_num {
            handle.create_host(gen_server_addr(id)).unwrap();
        }
        for id in 0..server_num {
            let local_handle = handle.get_host(gen_server_addr(id)).unwrap();
            servers.push(
                local_handle
                    .spawn(async move {
                        ReliableCtl::new(
                            pg_num,
                            KvService::new(),
                            ServerClient::new(id as _, monitor_addr).await.unwrap(),
                        )
                        .await
                    })
                    .await,
            );
        }
        KvServerCluster {
            servers,
            handle,
            pg_num,
        }
    }

    pub fn crash(&self, idx: usize) {
        self.handle.kill(gen_server_addr(idx));
    }

    pub async fn restart(&self, idx: usize) {
        let local_handle = self.handle.create_host(gen_server_addr(idx)).unwrap();
        let pg_num = self.pg_num;
        local_handle
            .spawn(async move {
                ReliableCtl::new(
                    pg_num,
                    KvService::new(),
                    ServerClient::new(idx as _, MONITOR_ADDR.parse().unwrap())
                        .await
                        .unwrap(),
                )
                .await
            })
            .await;
    }

    pub fn disconnect(&self, idx: usize) {
        self.handle.net.disconnect(gen_server_addr(idx));
    }

    pub fn connect(&self, idx: usize) {
        self.handle.net.connect(gen_server_addr(idx));
    }
}