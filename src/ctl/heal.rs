//! When peering procedure detect there is lag between local pg version and peers' pg version,
//! server will send Heal request to the most up-to-date server. The most up-to-date server will
//! response with the missing logs.

use super::{Error, PgState, ReliableCtl, Result};
use crate::constant::HEAL_REQ_TIMEOUT;
use crate::rpc::{HealReq, HealRes};
use crate::PgVersion;
use crate::{service::Store, PgId};
use futures::stream::{FuturesUnordered, StreamExt};
use log::{debug, error};
use madsim::{fs, net::NetLocalHandle, time::sleep};
use std::{net::SocketAddr, time::Duration};

impl<T> ReliableCtl<T>
where
    T: Store + Sync + Send + 'static,
{
    pub(super) async fn background_heal(&self) {
        loop {
            sleep(Duration::from_secs(2)).await;
            // Scan the local pgs and find those state is in `Inconsitent`.
            // Then send heal request to primary (we guarantee that primary is up-to-date in its epoch).
            let mut heal_tasks = self
                .inner
                .pgs
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, info)| info.state == PgState::Inconsistent && !info.primary)
                .map(|x| {
                    let this = self.clone();
                    let target_map = self.get_target_map();
                    let pgid = *x.0;
                    let primary_addr = self.distributor.locate(pgid, &target_map)[0];
                    async move { (pgid, this.heal_for(pgid, primary_addr).await) }
                })
                .collect::<FuturesUnordered<_>>();
            while let Some((pgid, res)) = heal_tasks.next().await {
                match res {
                    Ok(()) => self.inner.set_pg_state(pgid, PgState::Active),
                    Err(err) => error!("Error occuring when healing pg {}: {}", pgid, err),
                }
            }
        }
    }

    pub(super) async fn heal_for(&self, pgid: PgId, target_addr: SocketAddr) -> Result<()> {
        // 1. send local pg version to `target_addr`
        let request = HealReq {
            pgid,
            pg_ver: self.inner.get_pg_version(pgid),
            epoch: self.get_epoch(),
        };
        let net = NetLocalHandle::current();
        // 2. peer will response with a HealRes according to sending pg version
        match net
            .call_timeout(target_addr, request.clone(), HEAL_REQ_TIMEOUT)
            .await
        {
            Ok(Ok(res)) => {
                self.handle_heal_res(pgid, res).await;
                debug!(
                    "Heal for pg {}, current version = {}",
                    pgid,
                    self.inner.get_pg_version(pgid)
                );
                Ok(())
            }
            Ok(Err(err)) => Err(err),
            Err(err) => Err(Error::from(err)),
        }
    }

    /// Generate the heal response for pg `pgid`
    ///
    /// `ver` - the pg version of the remote peer
    pub(super) async fn gen_heal_res(&self, pgid: PgId, ver: PgVersion) -> HealRes {
        let local_pg_ver = self.inner.get_pg_version(pgid);
        let local_lower = self.inner.logger.lower(pgid);
        let mut logs = Vec::new();
        for log_id in ver.max(local_lower)..local_pg_ver {
            logs.push((log_id, self.inner.logger.get(pgid, log_id).await.unwrap()));
        }
        let snapshot = if ver < local_lower {
            Some(
                fs::read(format!("snapshot.{}", pgid))
                    .await
                    .expect("Cannot get snapshot"),
            )
        } else {
            None
        };
        HealRes {
            logs,
            snapshot: snapshot.map(|x| (local_lower, x)),
        }
    }

    /// When receive a `HealRes` from peers, call this function to heal pg `pgid`.
    pub(super) async fn handle_heal_res(&self, pgid: PgId, heal_res: HealRes) {
        let HealRes { mut logs, snapshot } = heal_res;
        let local_pg_ver = self.inner.get_pg_version(pgid);
        if let Some((id, data)) = snapshot {
            if id > local_pg_ver {
                self.inner.install(pgid, id, data).await;
            }
        }
        logs.sort_by(|a, b| a.0.cmp(&b.0));
        for (log_id, op) in logs
            .into_iter()
            .filter(|(log_id, _)| *log_id >= local_pg_ver)
        {
            self.inner.add_log(log_id, &op).await;
            self.inner.apply_log(pgid, log_id).await;
        }
        self.inner.set_pg_state(pgid, PgState::Active);
        self.inner.update_pg_heartbeat_ts(pgid);
    }
}
