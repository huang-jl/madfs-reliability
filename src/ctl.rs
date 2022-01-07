use crate::{
    constant::*,
    distributor::{Distributor, SimpleHashDistributor},
    monitor::{client::ServerClient, PgMap, TargetMap},
    service::{Service, ServiceInput},
    ForwardReq, PgId,
};
use futures::{lock::Mutex, stream::FuturesUnordered, Future, StreamExt};
use log::info;
use madsim::net::NetLocalHandle;
use serde::{Deserialize, Serialize};
use std::{io, net::SocketAddr, sync::Arc};

#[derive(thiserror::Error, Debug, Serialize, Deserialize)]
pub enum ServerError {
    #[error("The requested server is not primary")]
    NotPrimary,
    #[error("The target is not responsible for the request's key")]
    WrongTarget,
    #[error("Network error: {0}")]
    NetworkError(String),
}

#[derive(Debug)]
pub struct ReliableCtl<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

// The #[derive(Clone)] bindly bind T: Clone, but we do not need it.
// We only need Arc<Mutex<T>>: Clone
impl<T> Clone for ReliableCtl<T>
where
    T: Service,
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
    T: Service + Sync + Send + 'static,
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
    /// Handler for request directly from client.
    async fn handle_request(&self, request: T::Input) -> Result<T::Output, ServerError> {
        info!("Receive request from client: {:?}", request);
        if request.check_modify_operation() {
            if self.is_primary(&request).await {
                self.modification_by_primary(request).await
            } else {
                Err(ServerError::NotPrimary)
            }
        } else {
            if self.is_primary(&request).await || self.is_secondary(&request).await {
                Ok(self.inner.lock().await.service.dispatch_read(request))
            } else {
                Err(ServerError::WrongTarget)
            }
        }
    }

    #[rpc]
    /// Handle forward request from primary.
    async fn handle_forward(&self, request: ForwardReq<T::Input>) -> Result<(), ServerError> {
        info!("Get forward request: {:?}", request);
        let ForwardReq { op: args } = request;
        if !self.is_secondary(&args).await {
            return Err(ServerError::WrongTarget);
        }
        self.inner.lock().await.apply_modification_to_service(args);
        Ok(())
    }

    async fn modification_by_primary(&self, request: T::Input) -> Result<T::Output, ServerError> {
        let mut tasks = {
            let inner = self.inner.lock().await;
            inner.gen_forward_task(&request).await
        };
        for res in tasks.next().await {
            res?
        }
        Ok(self
            .inner
            .lock()
            .await
            .apply_modification_to_service(request))
    }

    /*
     * Helper methods
     */

    fn local_addr(&self) -> SocketAddr {
        NetLocalHandle::current().local_addr()
    }

    async fn is_primary(&self, input: &T::Input) -> bool {
        let target_addrs = self.inner.lock().await.get_target_addrs(&input).await;
        self.local_addr() == target_addrs[0]
    }

    async fn is_secondary(&self, input: &T::Input) -> bool {
        let target_addrs = self.inner.lock().await.get_target_addrs(&input).await;
        target_addrs
            .iter()
            .skip(1)
            .find(|addr| **addr == self.local_addr())
            .is_some()
    }
}

impl<T> Inner<T>
where
    T: Service,
{
    /*
     * The following methods are used by primary
     */
    async fn gen_forward_task(
        &self,
        args: &T::Input,
    ) -> FuturesUnordered<impl Future<Output = Result<(), ServerError>>> {
        assert!(args.check_modify_operation());
        // 1. Locate the peer servers
        let peers = self.get_target_addrs(args).await;
        // 2. Send request to peers
        let tasks = FuturesUnordered::new();
        for peer in peers.into_iter().skip(1) {
            let args = args.clone();
            tasks.push(async move {
                let net = NetLocalHandle::current();
                for _ in 0..FORWARD_RETRY {
                    let request = ForwardReq::new(args.clone());
                    match net
                        .call_timeout(peer.to_owned(), request, FORWARD_TIMEOUT)
                        .await
                    {
                        Ok(_) => return Ok(()),
                        Err(err) if err.kind() == io::ErrorKind::TimedOut => {}
                        Err(err) => return Err(ServerError::NetworkError(err.to_string())),
                    }
                }
                Err(ServerError::NetworkError(format!(
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

    fn apply_modification_to_service(&mut self, op: T::Input) -> T::Output {
        assert!(op.check_modify_operation());
        self.service.dispatch_write(op)
    }

    async fn get_target_map(&self) -> TargetMap {
        self.monitor_client.get_local_target_map().await
    }

    async fn get_pg_map(&self) -> PgMap {
        self.monitor_client.get_local_pg_map().await
    }

    async fn assign_pgid(&self, args: &T::Input) -> PgId {
        self.distributor.assign_pgid(args.key_bytes())
    }

    async fn get_target_addrs(&self, args: &T::Input) -> [SocketAddr; REPLICA_SIZE] {
        let pgid = self.assign_pgid(args).await;
        self.distributor.locate(pgid, &self.get_target_map().await)
    }
}
