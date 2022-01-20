//! Peering Details:
//! Only occurs after cluster map changes:
//! 1. Primary first mark this pg inactive.
//! 2. Primary ask all the peers of the specific pg for their pg versions while also mark them unavailable.
//! 3. Find the highest pg version among all peers and let the primary up-to-date (e.g. collect the missing logs).
//! 4. Primary brings all secondary up-to-date and mark this pg active.

use std::net::SocketAddr;

use super::{PgState, ReliableCtl};
use crate::{
    constant::*,
    ctl::PgInfo,
    monitor::TargetMap,
    rpc::{PeerConsult, PeerFinish},
    service::Store,
    PgId, PgVersion, Result,
};
use futures::{
    select,
    stream::{FuturesOrdered, FuturesUnordered, StreamExt},
    FutureExt,
};
use log::{debug, error, warn};
use madsim::{net::NetLocalHandle, time::sleep};

impl<T> ReliableCtl<T>
where
    T: Store + Sync + Send + 'static,
{
    /// When target map changes, the peering procedure will start.
    pub(super) async fn background_peer(&self) {
        loop {
            let target_map = self.get_target_map();
            let mut peer_tasks = FuturesUnordered::new();
            let mut pgs = self.inner.pgs.lock().await; //*
            for pgid in 0..self.pg_num {
                let target_addr = self.distributor.locate(pgid, &target_map);
                if !target_addr.iter().any(|addr| *addr == self.local_addr()) {
                    pgs.entry(pgid)
                        .and_modify(|info| info.state = PgState::Irresponsible);
                } else {
                    let epoch = target_map.get_version();
                    let pg_info = pgs
                        .entry(pgid)
                        .and_modify(|info| {
                            if info.epoch < epoch {
                                info.primary = false;
                                info.heartbeat_ts = None;
                                info.epoch = epoch;
                                info.state = PgState::Inactive;
                            }
                        })
                        .or_insert(PgInfo {
                            applied_ptr: 0,
                            heartbeat_ts: None,
                            primary: false,
                            state: PgState::Inactive,
                            epoch,
                        });
                    // Start peering if primary
                    if target_addr[0] == self.local_addr() {
                        pg_info.primary = true;
                        let this = self.clone();
                        let target_map = target_map.clone();
                        peer_tasks
                            .push(async move { (pgid, this.peer_for(pgid, &target_map).await) });
                    }
                }
            }
            drop(pgs); //*
            while let Some((pgid, res)) = peer_tasks.next().await {
                if let Err(err) = res {
                    error!("Error occur when peering pg {}: {}", pgid, err);
                }
            }
            select! {
                () = self.monitor_client
                .watch_for_target_map(Some(target_map.get_version()))
                .fuse() => {},
                () = self.heartbeat().fuse() => {}
            };
        }
    }

    /// First collect all pg versions, then select the one with highest version and let
    /// primary up-to-date. Finally the primary trys to send logs to make each replica up-to-date.
    ///    
    /// 1. Primary should call this when cluster map changes before pg can start serving.
    /// 2. Primary can call this to bring the damaged pg recovered.
    pub(super) async fn peer_for(&self, pgid: PgId, target_map: &TargetMap) -> Result<()> {
        assert!(self.is_responsible_pg(pgid));
        // collect pg version
        let (peers, peer_res) = self.collect_pg_ver(pgid, target_map).await?;
        // if peer has higher version then heal it (makes it up-to-date)
        let local_pg_ver = self.inner.get_pg_version(pgid);
        if let Some(addr) = peer_res
            .iter()
            .zip(peers.iter())
            .filter(|(ver, _)| **ver > local_pg_ver)
            .max_by(|a, b| a.0.cmp(b.0))
            .map(|x| x.1)
        {
            self.heal_for(pgid, *addr).await?;
        }
        // let every replica consistent.
        let local_pg_ver = self.inner.get_pg_version(pgid);
        let mut requests = FuturesUnordered::new();
        for (addr, ver) in peers.into_iter().zip(peer_res.into_iter()) {
            let mut logs = Vec::new();
            for log_id in ver..local_pg_ver {
                logs.push((log_id, self.inner.logger.get(pgid, log_id).await.unwrap()));
            }
            let net = NetLocalHandle::current();
            requests.push(async move {
                net.call_timeout(
                    addr,
                    PeerFinish {
                        pgid,
                        logs,
                        epoch: target_map.get_version(),
                    },
                    PEER_TIMEOUT,
                )
                .await
            });
        }
        while let Some(res) = requests.next().await {
            res??
        }
        self.inner.set_pg_state(pgid, PgState::Active).await;
        self.inner.update_pg_heartbeat_ts(pgid).await;
        debug!("Pg {} on primary {} peer finish", pgid, self.local_addr());
        Ok(())
    }

    /// Return peer address and their pg versions
    async fn collect_pg_ver(
        &self,
        pgid: PgId,
        target_map: &TargetMap,
    ) -> Result<(Vec<SocketAddr>, Vec<PgVersion>)> {
        let peers = self
            .distributor
            .locate(pgid, target_map)
            .into_iter()
            .filter(|addr| *addr != self.local_addr())
            .collect::<Vec<_>>();
        // collect the pg version of each peers
        let mut requests = peers
            .iter()
            .map(|addr| {
                let net = NetLocalHandle::current();
                let addr = *addr;
                async move {
                    net.call_timeout(
                        addr,
                        PeerConsult {
                            pgid,
                            epoch: target_map.get_version(),
                        },
                        PEER_TIMEOUT,
                    )
                    .await
                }
            })
            .collect::<FuturesOrdered<_>>();
        let mut peer_res: Vec<PgVersion> = Vec::new();
        while let Some(res) = requests.next().await {
            peer_res.push(res??);
        }
        Ok((peers, peer_res))
    }
}
