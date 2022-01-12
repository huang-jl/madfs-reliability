use std::net::SocketAddr;

use super::{PgInfo, PgState, ReliableCtl};
use crate::{
    constant::*,
    ctl::heal::HealJob,
    rpc::{ConsultPgInfo, HealJobReq},
    service::Store,
    PgId,
};
use futures::stream::{FuturesOrdered, StreamExt};
use log::{info, warn};
use madsim::{net::NetLocalHandle, task};
use std::collections::HashMap;

impl<T> ReliableCtl<T>
where
    T: Store + Sync + Send + 'static,
{
    /// Checking for all PGs *concurrently*,
    /// aim to maintain the local pg state by peering with other servers.
    ///
    /// Steps:
    /// 1. scan all pgids to find those of responsibility, and calculate their target addresses
    /// 2. group pgids by target address and send the grouped pgids to each target in one packet
    /// 3. receive pg info from targets and group by pgid for analyzing
    /// 4. check peer's pg info for each pg and update the local pg info
    pub(super) async fn peering(&self) {
        for (pgid, peer_res) in self.collect_pg_info().await {
            self.peer_for(pgid, peer_res).await;
        }
    }

    async fn peer_for(&self, pgid: PgId, peer_res: Vec<(SocketAddr, PgInfo)>) {
        // Scan the results:
        // 1. log the errors
        // 2. check if there is a more up-to-date pg on peers
        if peer_res.iter().count() < REPLICA_SIZE - 1 {
            // Network error occur during peering,
            // although local pg version is the highest among those servers which can response.
            // Conservatively mark this pg as `Unprepared` to prevent it from serving request.
            warn!("Do not collect enough pg info from peers of pg {}, may be network error during peering for pg", pgid);
            self.inner.lock().await.update_pg_info(pgid, |mut pg_info| {
                pg_info.state = PgState::Unprepared;
                pg_info
            });
        } else if self.check_absent(pgid, &peer_res).await {
            self.inner.lock().await.update_pg_info(pgid, |mut pg_info| {
                pg_info.state = PgState::Absent;
                pg_info
            });
        } else if self.check_stale(pgid, &peer_res).await {
            warn!("Detect pg {} is stale", pgid);
            // mark this pg as stale
            self.inner.lock().await.update_pg_info(pgid, |mut pg_info| {
                pg_info.state = PgState::Stale;
                pg_info
            });
        } else {
            info!("Pg {} peer succeed", pgid);
            // The pg on this server is the newest and all peers response the Consult Request
            self.inner.lock().await.update_pg_info(pgid, |mut pg_info| {
                pg_info.state = PgState::Active;
                pg_info
            });
        }
    }

    /// Consult peers to collect each peer's pg info of every pg of responsibility *concurrently*.
    /// Aim to maintain the local pg state (e.g. whether the pg is stale or unprepared).
    ///
    /// Return the results of consult group by pgid.
    async fn collect_pg_info(&self) -> HashMap<PgId, Vec<(SocketAddr, PgInfo)>> {
        let mut requests: HashMap<SocketAddr, Vec<PgId>> = HashMap::new();
        let target_map = self.get_target_map();
        // Only need to collect pgs which its state not in Absent.
        // The absent pg will be solved by recovery procedure.
        let pgids = {
            let inner = self.inner.lock().await;
            (0..PG_NUM)
                .filter(|&pgid| inner.get_pg_info(pgid).state != PgState::Absent)
                .collect::<Vec<_>>()
        };
        for pgid in pgids {
            // 1. skip those out of responsibility
            let peers = self.distributor.locate(pgid, &target_map);
            if !peers.iter().any(|addr| *addr == self.local_addr()) {
                continue;
            }
            for peer in peers.into_iter().filter(|addr| *addr != self.local_addr()) {
                requests
                    .entry(peer)
                    .and_modify(|pgs| pgs.push(pgid))
                    .or_insert(vec![pgid]);
            }
        }
        let results: HashMap<SocketAddr, std::io::Result<HashMap<PgId, PgInfo>>> =
            {
                requests.into_iter().map(|(addr, pgs)| {
                let request = ConsultPgInfo(pgs);
                let net = NetLocalHandle::current();
                async move {
                    (addr, net.call_timeout(addr, request, CONSULT_TIMEOUT).await)
                }
            }).collect::<FuturesOrdered<_>>().collect().await
            };
        let mut res: HashMap<PgId, Vec<(SocketAddr, PgInfo)>> = HashMap::new();
        for (addr, result) in results.into_iter().filter(|(addr, res)| match res {
            Err(err) => {
                warn!(
                    "Networking error happened during peering with {}: {}",
                    addr, err
                );
                false
            }
            _ => true,
        }) {
            for (pgid, pg_info) in result.unwrap().into_iter() {
                match res.get_mut(&pgid) {
                    Some(val) => val.push((addr, pg_info)),
                    None => {
                        res.insert(pgid, vec![(addr, pg_info)]);
                    }
                }
            }
        }
        res
    }

    /// Check whether `pgid` is stale, according to collected pg info (`peer_res`) from `peers`.
    /// Make sure there is no `Err(..)` in `peer_res` before calling this method.
    ///
    /// If `pgid` is stale, this method will send heal request to the up-to-date server.
    async fn check_stale(&self, pgid: PgId, peer_res: &Vec<(SocketAddr, PgInfo)>) -> bool {
        let inner = self.inner.lock().await;
        let local_pg_version = inner.get_pg_info(pgid).version;

        // Find the largest version number among peers
        if let Some((addr, pg_info)) = peer_res.iter().max_by(|a, b| a.1.version.cmp(&b.1.version))
        {
            // If local_pg_version == 0, then it cannot be stale.
            // It can only be absent or the cluster just make a fresh start.
            if pg_info.version > local_pg_version && local_pg_version > 0 {
                // send heal request
                let request = HealJobReq(HealJob {
                    addr: self.local_addr(),
                    pgid,
                    keys: inner.service.get_heal_data(pgid),
                });
                let addr = addr.clone();
                task::spawn(async move {
                    let net = NetLocalHandle::current();
                    if let Err(err) = net.call_timeout(addr, request, HEAL_REQ_TIMEOUT).await {
                        warn!("Send Heal request to {} receive error: {}", addr, err);
                    }
                })
                .detach();
                return true;
            }
        }
        false
    }

    async fn check_absent(&self, pgid: PgId, peer_res: &Vec<(SocketAddr, PgInfo)>) -> bool {
        let local_pg_version = self.inner.lock().await.get_pg_info(pgid).version;
        local_pg_version == 0 && peer_res.iter().any(|(_, pg_info)| pg_info.version > 0)
    }
}
