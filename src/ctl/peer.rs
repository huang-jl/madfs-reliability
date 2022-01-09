use super::{PgInfo, PgState, ReliableCtl};
use crate::{constant::*, rpc::ConsultPgInfo, service::Store, Error, PgId, Result};
use futures::stream::{FuturesOrdered, StreamExt};
use log::{info, warn};
use madsim::net::NetLocalHandle;

impl<T> ReliableCtl<T>
where
    T: Store + Sync + Send + 'static,
{
    /// Checking for all PGs, aims to:
    /// 1. Find the pgs of responsibility
    /// 2. Check whether local pg is the most up-to-date
    /// 3. Update PgInfo
    pub(super) async fn peering(&self) {
        let local_addr = self.local_addr();
        for pgid in 0..PG_NUM {
            let target_map = self.get_target_map();
            let target_addrs = self.distributor.locate(pgid, &target_map);
            if !target_addrs.iter().any(|addr| *addr == local_addr) {
                continue;
            }
            let tasks = target_addrs
                .into_iter()
                .filter(|addr| *addr != local_addr)
                .map(|addr| {
                    let request = ConsultPgInfo(pgid);
                    async move {
                        let net = NetLocalHandle::current();
                        net.call_timeout(addr, request, CONSULT_TIMEOUT).await
                    }
                })
                .collect::<FuturesOrdered<_>>();
            let peer_res: Vec<std::io::Result<PgInfo>> = tasks.collect().await;

            let mut inner = self.inner.lock().await;
            let local_pg_version = inner.get_pg_info(pgid).version;
            // Scan the results:
            // 1. log the errors
            // 2. check if there is a more up-to-date pg on peers
            if peer_res
                .iter()
                .filter(|res| match res {
                    Err(err) => {
                        warn!("Error occur when peering: {}", err);
                        false
                    }
                    _ => true,
                })
                .map(|res| res.as_ref().unwrap())
                .any(|res| res.version > local_pg_version)
            {
                warn!(
                    "Detect pg {} is stale, local version = {}",
                    pgid, local_pg_version
                );
                inner.update_pg_info(pgid, |mut pg_info| {
                    pg_info.state = PgState::Stale;
                    pg_info
                });
            } else if peer_res.iter().all(|res| !matches!(res, Err(_))) {
                info!(
                    "Pg {} peer succeed, local version = {}",
                    pgid, local_pg_version
                );
                // The pg on this server is the newest and all peers response the Consult Request
                inner.update_pg_info(pgid, |mut pg_info| {
                    pg_info.state = PgState::Active;
                    pg_info
                });
            } else {
                // Network error occur during peering,
                // although local pg version is the highest among those servers which can response.
                // Conservatively mark this pg as `Unprepared` to prevent it from serving request.
                inner.update_pg_info(pgid, |mut pg_info| {
                    pg_info.state = PgState::Unprepared;
                    pg_info
                });
            }
        }
    }
}
