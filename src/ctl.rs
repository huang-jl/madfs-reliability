use std::{
    collections::HashMap,
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use crate::{
    constant::*,
    service::{Service, ServiceInput},
    Error, ForwardReq,
};
use futures::{stream::FuturesUnordered, Future, StreamExt};
use log::{error, info};
use madsim::net::{rpc::Request, NetLocalHandle};

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

#[derive(Copy, Clone, Debug)]
enum Decision {
    Commit,
    Abort,
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
    async fn handle_request(&self, request: T::Input) -> Result<T::Output, Error> {
        info!("Receive request from client: {:?}", request);
        if request.check_modify_operation() {
            if self.inner.lock().unwrap().primary {
                self.modification_by_primary(request).await
            } else {
                Err(Error::NotPrimary)
            }
        } else {
            Ok(self.inner.lock().unwrap().service.dispatch_read(request))
        }
    }

    #[rpc]
    /// Handler for the first phase for 2pc, which comes from primary server
    async fn handle_forward(&self, request: ForwardReq<T::Input>) -> Result<(), Error> {
        info!("Get forward request: {:?}", request);
        let ForwardReq { op } = request;
        self.inner.lock().unwrap().apply_modification_to_service(op);
        Ok(())
    }

    async fn modification_by_primary(&self, request: T::Input) -> Result<T::Output, Error> {
        let mut tasks = {
            let inner = self.inner.lock().unwrap();
            inner.gen_forward_task(&request)
        };
        for res in tasks.next().await {
            match res {
                Ok(_) => {}
                Err(err) => return Err(err),
            }
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
    ) -> FuturesUnordered<impl Future<Output = Result<(), Error>>> {
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
                        Ok(res) => return res,
                        Err(err) if err.kind() == io::ErrorKind::TimedOut => {}
                        Err(err) => return Err(Error::IoError(err.to_string())),
                    }
                }
                Err(Error::RetryNotSuccess {
                    request_type: "ForwardReq".to_owned(),
                    retry_times: FORWARD_RETRY,
                })
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
