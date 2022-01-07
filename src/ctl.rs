use crate::{
    constant::*,
    distributor::{Distributor, SimpleHashDistributor},
    monitor::{client::ServerClient, PgMap, TargetMap},
    rpc::{ForwardReq, Get, KvRequest, Put},
    service::Store,
    Error, PgId, Result,
};
use futures::{lock::Mutex, stream::FuturesUnordered, Future, StreamExt};
use log::info;
use madsim::net::NetLocalHandle;
use std::{io, net::SocketAddr, sync::Arc};

#[derive(Debug)]
pub struct ReliableCtl<T> {
    inner: Arc<Mutex<Inner<T>>>,
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
        }
    }
}

struct Inner<T> {
    service: T,
    monitor_client: Arc<ServerClient>,
    distributor: Box<dyn Distributor<REPLICA_SIZE>>,
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
                monitor_client,
                distributor: Box::new(SimpleHashDistributor),
            })),
        };
        ctl.add_rpc_handler();
        ctl
    }

    #[rpc]
    async fn get(&self, request: Get) -> Result<Option<String>> {
        info!("Receive Get from client: {:?}", request);
        if self.is_primary(&request).await || self.is_secondary(&request).await {
            Ok(self.inner.lock().await.get(request.key()))
        } else {
            Err(Error::WrongTarget)
        }
    }

    #[rpc]
    /// Handler for request directly from client.
    async fn put(&self, request: Put) -> Result<()> {
        info!("Receive Put from client: {:?}", request);
        if self.is_primary(&request).await {
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
        if !self.is_secondary(&request).await {
            return Err(Error::WrongTarget);
        }
        self.inner.lock().await.put(
            request.key().to_owned(),
            request.value().unwrap().to_owned(),
        );
        Ok(())
    }

    async fn forward_put_by_primary(&self, request: Put) -> Result<()> {
        let mut tasks = {
            let inner = self.inner.lock().await;
            inner.gen_forward_task(&request).await
        };
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

    async fn is_primary<R>(&self, input: &R) -> bool
    where
        R: KvRequest,
    {
        let target_addrs = self.inner.lock().await.get_target_addrs(input).await;
        self.local_addr() == target_addrs[0]
    }

    async fn is_secondary<R>(&self, input: &R) -> bool
    where
        R: KvRequest,
    {
        let target_addrs = self.inner.lock().await.get_target_addrs(input).await;
        target_addrs
            .iter()
            .skip(1)
            .find(|addr| **addr == self.local_addr())
            .is_some()
    }
}

impl<T> Inner<T>
where
    T: Store,
{
    /*
     * The following methods are used by primary
     */
    async fn gen_forward_task<R>(
        &self,
        request: &R,
    ) -> FuturesUnordered<impl Future<Output = Result<()>>>
    where
        R: KvRequest + Clone,
    {
        // 1. Locate the peer servers
        let peers = self.get_target_addrs(request).await;
        // 2. Send request to peers
        let tasks = FuturesUnordered::new();
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
        tasks
    }

    /*
     * Helper methods
     */
    async fn get_target_map(&self) -> TargetMap {
        self.monitor_client.get_local_target_map().await
    }

    async fn get_pg_map(&self) -> PgMap {
        self.monitor_client.get_local_pg_map().await
    }

    async fn assign_pgid<R>(&self, args: &R) -> PgId
    where
        R: KvRequest,
    {
        self.distributor.assign_pgid(args.key().as_bytes())
    }

    async fn get_target_addrs<R>(&self, args: &R) -> [SocketAddr; REPLICA_SIZE]
    where
        R: KvRequest,
    {
        let pgid = self.assign_pgid(args).await;
        self.distributor.locate(pgid, &self.get_target_map().await)
    }

    fn put(&mut self, key: String, value: String) {
        let pgid = self.distributor.assign_pgid(key.as_bytes());
        self.service.put(format!("{}.{}", pgid, key), value);
    }

    fn get(&mut self, key: &str) -> Option<String> {
        let pgid = self.distributor.assign_pgid(key.as_bytes());
        self.service.get(&format!("{}.{}", pgid, key))
    }
}
