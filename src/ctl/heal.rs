//! When peering procedure detect there is lag between local pg version and peers' pg version,
//! server will send Heal request to the most up-to-date server. The most up-to-date server will
//! response with the missing logs.

use super::{call_timeout_retry, Error, PgState, ReliableCtl, Result};
use crate::constant::{HEAL_REQ_TIMEOUT, RETRY_TIMES};
use crate::rpc::{HealData, HealReq};
use crate::PgVersion;
use crate::{service::Store, PgId};
use futures::stream::{FuturesUnordered, StreamExt};
use log::error;
use madsim::{fs, time::sleep};
use std::time::Duration;

impl<T> ReliableCtl<T>
where
    T: Store + Sync + Send + 'static,
{
    pub(super) async fn background_heal(&self) {
        loop {
            sleep(Duration::from_secs(3)).await;
            // Scan the local pgs and find those state is in `Inconsitent`.
            // Then send heal request to primary (we guarantee that primary is up-to-date in its epoch).
            let mut heal_tasks = self
                .inner
                .pgs
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, info)| info.state == PgState::Inconsistent && info.primary)
                .map(|x| {
                    let this = self.clone();
                    let pgid = *x.0;
                    async move { (pgid, this.heal_for(pgid).await) }
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

    /// Generate the heal response of pg `pgid` for the peer which pg version is `ver`.
    ///
    /// `ver` - the pg version of the remote peer
    pub(super) async fn gen_heal_res(&self, pgid: PgId, ver: PgVersion) -> HealData {
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
        HealData {
            logs,
            snapshot: snapshot.map(|x| (local_lower, x)),
        }
    }

    /// When receive a `HealRes` from peers, call this function to heal pg `pgid`.
    pub(super) async fn handle_heal_data(&self, pgid: PgId, heal_data: HealData) {
        let HealData { mut logs, snapshot } = heal_data;
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

    /// Healing is guided by primary.
    /// 1. Collect all peers' pg versions.
    /// 2. Send the `HealRes` to lagging peers.
    pub async fn heal_for(&self, pgid: PgId) -> Result<()> {
        let target_map = self.get_target_map();
        let (peers, peer_vers) = self.collect_pg_ver(pgid, &target_map).await?;
        let local_pg_ver = self.inner.get_pg_version(pgid);
        if let Some((addr, ver)) = peers
            .iter()
            .zip(peer_vers.iter())
            .find(|(_, ver)| **ver > local_pg_ver)
        {
            error!(
                "Secondary {} has higher pg version = {} than primary = {}, for pg {}",
                addr, ver, local_pg_ver, pgid
            );
            return Err(Error::PgStale);
        }
        let mut requests = peers
            .into_iter()
            .zip(peer_vers.into_iter())
            .map(|(addr, ver)| async move {
                let request = HealReq {
                    epoch: self.get_epoch(),
                    pgid,
                    heal_data: self.gen_heal_res(pgid, ver).await,
                };
                call_timeout_retry(addr, request, HEAL_REQ_TIMEOUT, RETRY_TIMES).await
            })
            .collect::<FuturesUnordered<_>>();
        while let Some(res) = requests.next().await {
            res??;
        }
        Ok(())
    }
}
