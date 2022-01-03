use super::{
    FetchPgMapReq, FetchTargetMapReq, HeartBeat, PgMap, TargetInfo, TargetMap, TargetMapVersion,
    TargetState,
};
use crate::{constant::*, monitor::MonitorError, Error};
use futures::lock::Mutex;
use madsim::{net::NetLocalHandle, task, time::sleep};
use std::{net::SocketAddr, sync::Arc};

/// Monitor client used for server
pub struct ServerClient {
    id: u64,
    /// Address of monitor
    addr: SocketAddr,
    // Use async locks here in case of dead lock
    target_map: Mutex<TargetMap>,
    pg_map: Mutex<PgMap>,
}

/// Monitor client used for client
pub struct Client {
    addr: SocketAddr,
    // Use async locks here in case of dead lock
    target_map: Mutex<TargetMap>,
    pg_map: Mutex<PgMap>,
}


impl ServerClient {
    ///* `id` - Id of current Server
    ///* `addr` - Address of monitor
    pub async fn new(id: u64, addr: SocketAddr) -> Result<Arc<ServerClient>, Error> {
        let net = NetLocalHandle::current();
        let heartbeat = HeartBeat {
            target_map_version: 0,
            pg_map_version: 0,
            target_info: TargetInfo {
                id,
                state: TargetState::UpIn(net.local_addr()),
            },
        };
        let response = net.call(addr, heartbeat).await?;

        let client = Arc::new(ServerClient {
            id,
            addr,
            target_map: Mutex::new(response.target_map.unwrap()),
            pg_map: Mutex::new(response.pg_map.unwrap()),
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
                pg_map_version: self.pg_map.lock().await.get_version(),
                target_map_version: self.target_map.lock().await.get_version(),
                target_info: self.local_target_info(),
            };
            if let Err(err) = net.call(self.addr, request).await {
                panic!("Error occur at server heartbeat with monitor: {:?}", err);
            }
        }
    }

    fn local_target_info(&self) -> TargetInfo {
        TargetInfo {
            id: self.id,
            state: TargetState::UpIn(NetLocalHandle::current().local_addr()),
        }
    }

    pub async fn get_local_target_map(&self) -> TargetMap {
        self.target_map.lock().await.clone()
    }

    pub async fn get_local_pg_map(&self) -> PgMap {
        self.pg_map.lock().await.clone()
    }

    pub async fn fetch_target_map(
        &self,
        version: Option<TargetMapVersion>,
    ) -> Option<TargetMap> {
        let net = NetLocalHandle::current();
        match net.call(self.addr, FetchTargetMapReq(version)).await {
            Ok(Ok(map)) => Some(map),
            Ok(Err(err)) => {
                assert!(matches!(err, MonitorError::DoesNotExist(..)));
                None
            }
            Err(err) => {
                panic!("Error occur at server fetch from monitor: {:?}", err);
            }
        }
    }

    pub async fn fetch_pg_map(&self, version: Option<TargetMapVersion>) -> Option<PgMap> {
        let net = NetLocalHandle::current();
        match net.call(self.addr, FetchPgMapReq(version)).await {
            Ok(Ok(map)) => Some(map),
            Ok(Err(err)) => {
                assert!(matches!(err, MonitorError::DoesNotExist(..)));
                None
            }
            Err(err) => {
                panic!("Error occur at server fetch from monitor: {:?}", err);
            }
        }
    }

    pub async fn update_target_map(&self) {
        let mut map = self.target_map.lock().await;
        *map = self.fetch_target_map(None).await.unwrap();
    }

    pub async fn update_pg_map(&self) {
        let mut map = self.pg_map.lock().await;
        *map = self.fetch_pg_map(None).await.unwrap();
    }
}

impl Client {
    ///* `id` - Id of current Server
    ///* `addr` - Address of monitor
    pub async fn new(addr: SocketAddr) -> Result<Arc<Client>, Error> {
        let net = NetLocalHandle::current();
        let request = FetchTargetMapReq(None);
        let target_map = net.call(addr, request).await?.unwrap();
        let request = FetchPgMapReq(None);
        let pg_map = net.call(addr, request).await?.unwrap();

        let client = Arc::new(Client {
            addr,
            target_map: Mutex::new(target_map),
            pg_map: Mutex::new(pg_map),
        });
        Ok(client)
    }

    pub async fn get_local_target_map(&self) -> TargetMap {
        self.target_map.lock().await.clone()
    }

    pub async fn get_local_pg_map(&self) -> PgMap {
        self.pg_map.lock().await.clone()
    }

    pub async fn fetch_target_map(
        &self,
        version: Option<TargetMapVersion>,
    ) -> Option<TargetMap> {
        let net = NetLocalHandle::current();
        match net.call(self.addr, FetchTargetMapReq(version)).await {
            Ok(Ok(map)) => Some(map),
            Ok(Err(err)) => {
                assert!(matches!(err, MonitorError::DoesNotExist(..)));
                None
            }
            Err(err) => {
                panic!("Error occur at server fetch from monitor: {:?}", err);
            }
        }
    }

    pub async fn fetch_pg_map(&self, version: Option<TargetMapVersion>) -> Option<PgMap> {
        let net = NetLocalHandle::current();
        match net.call(self.addr, FetchPgMapReq(version)).await {
            Ok(Ok(map)) => Some(map),
            Ok(Err(err)) => {
                assert!(matches!(err, MonitorError::DoesNotExist(..)));
                None
            }
            Err(err) => {
                panic!("Error occur at server fetch from monitor: {:?}", err);
            }
        }
    }

    pub async fn update_target_map(&self) {
        let mut map = self.target_map.lock().await;
        *map = self.fetch_target_map(None).await.unwrap();
    }

    pub async fn update_pg_map(&self) {
        let mut map = self.pg_map.lock().await;
        *map = self.fetch_pg_map(None).await.unwrap();
    }
}
