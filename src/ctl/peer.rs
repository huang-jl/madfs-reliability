//! Peering Details:
//! Only occurs after cluster map changes:
//! 1. Primary first mark this pg inactive.
//! 2. Primary ask all the peers of the specific pg for their pg versions while also mark them unavailable.
//! 3. Find the highest pg version among all peers and let the primary up-to-date (e.g. collect the missing logs).
//! 4. Primary brings all secondary up-to-date and mark this pg active.

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
    join, select,
    stream::{FuturesOrdered, FuturesUnordered, StreamExt},
    FutureExt,
};
use log::{debug, error};
use madsim::{net::NetLocalHandle, time::sleep};
use std::{net::SocketAddr, time::Duration};

impl<T> ReliableCtl<T>
where
    T: Store + Sync + Send + 'static,
{
    /// When target map changes, the peering procedure will start.
    ///
    /// *Important*: Once finish peering will the `background_heal`
    ///  and `background_snapshot` begin.
    pub(super) async fn background_peer(&self) {
        loop {
            let target_map = self.get_target_map();
            let mut peer_tasks = FuturesUnordered::new();
            {
                let mut pgs = self.inner.pgs.lock().unwrap();
                for pgid in 0..self.pg_num {
                    let target_addr = self.distributor.locate(pgid, &target_map);
                    // case 1. find the irresponsible pgid and mark it (later will clean it)
                    // case 2. mark the responsible pg inactive first (after peering will mark it active)
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
                        // Start peering task if local is primary
                        if target_addr[0] == self.local_addr() {
                            pg_info.primary = true;
                            let this = self.clone();
                            let target_map = target_map.clone();
                            peer_tasks.push(async move {
                                (pgid, this.peer_for(pgid, &target_map).await)
                            });
                        }
                    }
                }
            }
            while let Some((pgid, res)) = peer_tasks.next().await {
                if let Err(err) = res {
                    error!("Error occur when peering pg {}: {}", pgid, err);
                    self.inner.set_pg_state(pgid, PgState::Damaged);
                } else {
                    debug!(
                        "Finish peering pg {} in epoch {}",
                        pgid,
                        target_map.get_version()
                    );
                }
            }
            select! {
                () = self.monitor_client
                .watch_for_target_map(Some(target_map.get_version()))
                .fuse() => {},
                _ = async {
                    join!(self.background_heal(), self.background_snapshot(),
                     self.backgounrd_repeer(), self.background_heartbeat())
                }.fuse() => {}
            };
        }
    }

    pub(super) async fn backgounrd_repeer(&self) {
        loop {
            sleep(Duration::from_secs(2)).await;
            // Scan the local pgs and find those state is in `Damaged`.
            // Then try to repeer for this kind of pg.
            let mut repeer_tasks = self
                .inner
                .pgs
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, info)| info.state == PgState::Damaged && info.primary)
                .map(|x| {
                    let pgid = *x.0;
                    let target_map = self.get_target_map();
                    let this = self.clone();
                    async move { (pgid, this.peer_for(pgid, &target_map).await) }
                })
                .collect::<FuturesUnordered<_>>();
            while let Some((pgid, res)) = repeer_tasks.next().await {
                match res {
                    Ok(()) => {}
                    Err(err) => error!("Error occuring when repeering pg {}: {}", pgid, err),
                }
            }
        }
    }

    /// First collect all pg versions, then select the one with highest version and let
    /// primary up-to-date. Finally the primary trys to send logs to make each replica up-to-date.
    ///    
    /// 1. Primary should call this when cluster map changes before pg can start serving.
    /// 2. Primary can call this to bring the damaged pg recovered.
    pub(super) async fn peer_for(&self, pgid: PgId, target_map: &TargetMap) -> Result<()> {
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
        let mut requests = FuturesUnordered::new();
        for (addr, ver) in peers.into_iter().zip(peer_res.into_iter()) {
            let net = NetLocalHandle::current();
            requests.push(async move {
                net.call_timeout(
                    addr,
                    PeerFinish {
                        epoch: target_map.get_version(),
                        pgid,
                        heal: self.gen_heal_res(pgid, ver).await,
                    },
                    PEER_TIMEOUT,
                )
                .await
            });
        }
        while let Some(res) = requests.next().await {
            res??
        }
        self.inner.set_pg_state(pgid, PgState::Active);
        self.inner.update_pg_heartbeat_ts(pgid);
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
