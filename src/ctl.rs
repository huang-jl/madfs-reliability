use std::{
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use crate::{
    constant::*,
    service::{Service, ServiceInput},
    ForwardReq,
};
use futures::{stream::FuturesUnordered, Future, StreamExt};
use log::info;
use madsim::net::{rpc::Request, NetLocalHandle};
use serde::{Deserialize, Serialize};

#[derive(thiserror::Error, Debug, Serialize, Deserialize)]
pub enum ServerError {
    #[error("The requested server is not primary")]
    NotPrimary,
    #[error("Network error: {0}")]
    NetworkError(String),
}

pub struct ReliableCtl<T>
where
    T: Service,
{
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

struct Inner<T>
where
    T: Service,
{
    service: T,
    peers: Vec<SocketAddr>,
    primary: bool,
}

#[madsim::service]
impl<T> ReliableCtl<T>
where
    T: Service + Send + 'static,
    <T as Service>::Input: Request,
{
    pub fn new(service: T, peers: Vec<SocketAddr>, primary: bool) -> Self {
        let ctl = ReliableCtl {
            inner: Arc::new(Mutex::new(Inner {
                service,
                peers,
                primary,
            })),
        };
        ctl.add_rpc_handler();
        ctl
    }

    #[rpc]
    /// Handler for request directly from client
    async fn handle_request(&self, request: T::Input) -> Result<T::Output, ServerError> {
        info!("Receive request from client: {:?}", request);
        if request.check_modify_operation() {
            if self.inner.lock().unwrap().primary {
                self.modification_by_primary(request).await
            } else {
                Err(ServerError::NotPrimary)
            }
        } else {
            Ok(self.inner.lock().unwrap().service.dispatch_read(request))
        }
    }

    #[rpc]
    /// Handler for the first phase for 2pc, which comes from primary server
    async fn handle_forward(&self, request: ForwardReq<T::Input>) {
        info!("Get forward request: {:?}", request);
        let ForwardReq { op } = request;
        self.inner.lock().unwrap().apply_modification_to_service(op);
    }

    async fn modification_by_primary(&self, request: T::Input) -> Result<T::Output, ServerError> {
        let mut tasks = {
            let inner = self.inner.lock().unwrap();
            inner.gen_forward_task(&request)
        };
        for res in tasks.next().await {
            res?
        }
        Ok(self
            .inner
            .lock()
            .unwrap()
            .apply_modification_to_service(request))
    }
}

impl<T> Inner<T>
where
    T: Service,
{
    /*
     * The following methods are used by primary
     */
    fn gen_forward_task(
        &self,
        args: &T::Input,
    ) -> FuturesUnordered<impl Future<Output = Result<(), ServerError>>> {
        assert!(args.check_modify_operation());

        let tasks = FuturesUnordered::new();
        for peer in self.peers.clone() {
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
     * The following methods are used by secondary
     */

    fn apply_modification_to_service(&mut self, op: T::Input) -> T::Output {
        assert!(op.check_modify_operation());
        self.service.dispatch_write(op)
    }
}
