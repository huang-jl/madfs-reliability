use crate::{
    ctl::ReliableCtl,
    service::{KvArgs, KvRes, KvService},
    Error,
};
use log::*;
use madsim::{net::NetLocalHandle, Handle, LocalHandle};
use rand::{thread_rng, Rng};
use std::{io, net::SocketAddr, time::Duration};

struct KvServerCluster {
    servers: Vec<ReliableCtl<KvService>>,
    addrs: Vec<SocketAddr>,
    handles: Vec<LocalHandle>,
    primary: usize,
}

impl KvServerCluster {
    async fn new(server_num: usize, primary_id: usize) -> Self {
        assert!(primary_id < server_num);

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
            let mut peers = addrs.clone();
            peers.remove(id);
            servers.push(
                handles[id]
                    .spawn(async move {
                        ReliableCtl::new(
                            KvService::new(),
                            peers,
                            if id == primary_id { true } else { false },
                        )
                    })
                    .await,
            );
        }
        KvServerCluster {
            servers,
            addrs,
            handles,
            primary: primary_id,
        }
    }

    fn get_primary_addr(&self) -> SocketAddr {
        self.addrs[self.primary].clone()
    }

    fn get_addrs(&self) -> Vec<SocketAddr> {
        self.addrs.clone()
    }
}

struct ClientId(u64);

struct Client {
    id: ClientId,
    handle: LocalHandle,
    servers: Vec<SocketAddr>,
}

impl Client {
    fn new(id: u64, addrs: Vec<SocketAddr>) -> Self {
        let handle = Handle::current();
        let id = ClientId(id);
        let client_addr = id.to_addr();
        let client = Client {
            id,
            handle: handle.create_host(client_addr).unwrap(),
            servers: addrs,
        };
        handle.net.connect(client_addr);
        client
    }

    async fn send_with_index(
        &self,
        request: &KvArgs,
        idx: usize,
        retry: Option<u32>,
    ) -> Result<KvRes, Error> {
        let target = self.servers[idx];
        self.send_to(request.clone(), target.clone(), retry).await
    }

    async fn send_to(
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
                                Ok(res) => return res,
                                Err(err) if err.kind() == io::ErrorKind::TimedOut => {}
                                Err(err) => return Err(Error::IoError(err.to_string())),
                            }
                        }
                        Err(Error::RetryNotSuccess {
                            request_type: "KvArgs".to_owned(),
                            retry_times: retry,
                        })
                    }
                    None => match net.call_timeout(target, request, TIMEOUT).await {
                        Ok(res) => return res,
                        Err(err) => return Err(Error::IoError(err.to_string())),
                    },
                }
            })
            .await
    }
}

fn gen_random_put(key_len: usize, val_len: usize) -> KvArgs {
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

impl ClientId {
    fn to_addr(&self) -> SocketAddr {
        ([0, 1, 0, self.0 as _], 0).into()
    }
}

#[madsim::test]
async fn simple_test() {
    let cluster = KvServerCluster::new(3, 0).await;
    let client = Client::new(0, cluster.get_addrs());

    let request = gen_random_put(10, 20);
    let res = client.send_with_index(&request, 0, None).await;
    assert!(matches!(res, Ok(KvRes::Put)));

    for i in 1..=2 {
        let key = request.get_key();
        let res = client.send_with_index(&KvArgs::Get(key), i, None).await;
        assert_eq!(
            res.unwrap().get_value().unwrap(),
            request.get_value().unwrap()
        );
    }
}
