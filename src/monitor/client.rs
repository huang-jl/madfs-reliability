use super::{TargetInfo, TargetMap, TargetMapVersion, TargetState};
use crate::{constant::*, rpc::*, Error, Result};
use futures::Future;
use madsim::{net::NetLocalHandle, task, time::sleep};
use std::{
    net::SocketAddr,
    sync::Arc,
    task::{Poll, Waker},
};

#[derive(Debug)]
/// Monitor client used for server
pub struct ServerClient {
    id: u64,
    /// Address of monitor
    addr: SocketAddr,
    // Use async locks here in case of dead lock
    target_map: std::sync::Mutex<TargetMap>,
    watch: std::sync::Mutex<Option<Waker>>,
}

/// Monitor client used for client
pub struct Client {
    addr: SocketAddr,
    // Use async locks here in case of dead lock
    target_map: std::sync::Mutex<TargetMap>,
}

pub struct WatchForTargetMap {
    client: Arc<ServerClient>,
    prev_version: u64,
}

impl ServerClient {
    ///* `id` - Id of current Server
    ///* `addr` - Address of monitor
    pub async fn new(id: u64, addr: SocketAddr) -> Result<Arc<ServerClient>> {
        let net = NetLocalHandle::current();
        let heartbeat = HeartBeat {
            target_map_version: 0,
            target_info: TargetInfo {
                id,
                state: TargetState::UpIn(net.local_addr()),
            },
        };
        let response = net.call(addr, heartbeat).await?;

        let client = Arc::new(ServerClient {
            id,
            addr,
            target_map: std::sync::Mutex::new(response.target_map.unwrap()),
            watch: std::sync::Mutex::new(None),
        });
        // background task: send Heartbeat to monitor
        task::spawn(client.clone().heartbeat()).detach();
        Ok(client)
    }

    /// Send heartbeat to monitor periodically
    async fn heartbeat(self: Arc<ServerClient>) {
        let net = NetLocalHandle::current();
        loop {
            sleep(HEARTBEAT_PERIOD).await;
            let request = HeartBeat {
                target_map_version: self.target_map.lock().unwrap().get_version(),
                target_info: self.local_target_info(),
            };
            match net.call(self.addr, request).await {
                Ok(HeartBeatRes {
                    target_map: Some(updated_map),
                }) => {
                    // check if the piggy-backed map is more up-to-date
                    let mut local_map = self.target_map.lock().unwrap();
                    if local_map.get_version() < updated_map.get_version() {
                        *local_map = updated_map;
                        // call waker to wake up the task
                        if let Some(waker) = self.watch.lock().unwrap().take() {
                            waker.wake();
                        }
                    }
                }
                Ok(_) => {}
                Err(err) => panic!("Error occur at server heartbeat with monitor: {:?}", err),
            }
        }
    }

    fn local_target_info(&self) -> TargetInfo {
        TargetInfo {
            id: self.id,
            state: TargetState::UpIn(NetLocalHandle::current().local_addr()),
        }
    }

    pub fn get_local_target_map(&self) -> TargetMap {
        self.target_map.lock().unwrap().clone()
    }

    pub async fn fetch_target_map(&self, version: Option<TargetMapVersion>) -> Option<TargetMap> {
        let net = NetLocalHandle::current();
        match net.call(self.addr, FetchTargetMapReq(version)).await {
            Ok(Ok(map)) => Some(map),
            Ok(Err(err)) => {
                assert!(matches!(err, Error::VersionDoesNotExist(..)));
                None
            }
            Err(err) => {
                panic!("Error occur at server fetch from monitor: {:?}", err);
            }
        }
    }

    pub async fn update_target_map(&self) {
        let updated_map = self.fetch_target_map(None).await.unwrap();
        let mut local_map = self.target_map.lock().unwrap();
        if local_map.get_version() < updated_map.get_version() {
            *local_map = updated_map;
            // call waker to wake up the task
            if let Some(waker) = self.watch.lock().unwrap().take() {
                waker.wake();
            }
        }
    }

    /// This functions can only be called once until the returned future is consumed.
    ///
    /// Multiple [WatchForTargetMap](self::WatchForTargetMap) futures will be forgeted to wakeup (except for the last one).
    pub fn watch_for_target_map(
        self: &Arc<Self>,
        version: Option<TargetMapVersion>,
    ) -> WatchForTargetMap {
        WatchForTargetMap {
            client: self.clone(),
            prev_version: version.unwrap_or(self.get_local_target_map().get_version()),
        }
    }
}

impl Client {
    ///* `id` - Id of current Server
    ///* `addr` - Address of monitor
    pub async fn new(addr: SocketAddr) -> Result<Arc<Client>> {
        let net = NetLocalHandle::current();
        let request = FetchTargetMapReq(None);
        let target_map = net.call(addr, request).await?.unwrap();

        let client = Arc::new(Client {
            addr,
            target_map: std::sync::Mutex::new(target_map),
        });
        Ok(client)
    }

    pub async fn get_local_target_map(&self) -> TargetMap {
        self.target_map.lock().unwrap().clone()
    }

    pub async fn fetch_target_map(&self, version: Option<TargetMapVersion>) -> Option<TargetMap> {
        let net = NetLocalHandle::current();
        match net.call(self.addr, FetchTargetMapReq(version)).await {
            Ok(Ok(map)) => Some(map),
            Ok(Err(err)) => {
                assert!(matches!(err, Error::VersionDoesNotExist(..)));
                None
            }
            Err(err) => {
                panic!("Error occur at server fetch from monitor: {:?}", err);
            }
        }
    }

    pub async fn update_target_map(&self) {
        let lastest_map = self.fetch_target_map(None).await.unwrap();
        *self.target_map.lock().unwrap() = lastest_map;
    }
}

impl Future for WatchForTargetMap {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if self.prev_version < self.client.get_local_target_map().get_version() {
            Poll::Ready(())
        } else {
            *self.client.watch.lock().unwrap() = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}
