//! When peering procedure detect there is lag between local pg version and peers' pg version,
//! server will send Heal request to the most up-to-date server. The most up-to-date server will
//! response with the missing logs.

use super::{Error, PgState, ReliableCtl, Result};
use crate::constant::HEAL_REQ_TIMEOUT;
use crate::rpc::{HealReq, KvRequest};
use crate::{service::Store, PgId};
use futures::stream::{FuturesUnordered, StreamExt};
use log::{debug, error};
use madsim::net::NetLocalHandle;
use madsim::time::sleep;
use std::net::SocketAddr;
use std::time::Duration;

impl<T> ReliableCtl<T>
where
    T: Store + Sync + Send + 'static,
{
    pub(super) async fn background_heal(&self) {
        loop {
            let mut heal_tasks = self
                .inner
                .pgs
                .lock()
                .await
                .iter()
                .filter(|(_, info)| info.state == PgState::Inconsistent)
                .map(|x| {
                    let this = self.clone();
                    let target_map = self.get_target_map();
                    let pgid = *x.0;
                    async move { (pgid, this.peer_for(pgid, &target_map).await) }
                })
                .collect::<FuturesUnordered<_>>();
            while let Some((pgid, res)) = heal_tasks.next().await {
                if let Err(err) = res {
                    error!("Error occuring when healing pg {}: {}", pgid, err);
                }
            }
            sleep(Duration::from_secs(2)).await;
        }
    }

    // send heal request to `target_addr` and get the latest log
    pub(super) async fn heal_for(&self, pgid: PgId, target_addr: SocketAddr) -> Result<()> {
        let request = HealReq {
            pgid,
            pg_ver: self.inner.get_pg_version(pgid),
        };
        let net = NetLocalHandle::current();
        match net
            .call_timeout(target_addr, request.clone(), HEAL_REQ_TIMEOUT)
            .await
        {
            Ok(Ok(res)) => {
                let curr_ver = self.inner.get_pg_version(pgid);
                let mut res = res
                    .into_iter()
                    .filter(|(log_id, _)| *log_id >= curr_ver)
                    .collect::<Vec<_>>();
                res.sort_by(|a, b| a.0.cmp(&b.0));
                for (id, op) in res {
                    self.inner.add_log(id, &op).await;
                    self.inner.apply_log(pgid, id).await;
                }
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
}
