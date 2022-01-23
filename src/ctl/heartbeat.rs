use super::{Error, PgState, ReliableCtl, Result};
use crate::constant::PG_HEARTBEAT_TIMEOUT;
use crate::rpc::PgHeartbeat;
use crate::{service::Store, PgId};
use futures::stream::{FuturesOrdered, FuturesUnordered, StreamExt};
use log::{debug, error, warn};
use madsim::net::NetLocalHandle;
use madsim::time::{sleep, Instant};
use std::time::Duration;

impl<T> ReliableCtl<T>
where
    T: Store + Sync + Send + 'static,
{
    /// Primary will periodically send heartbeat to secondary.
    /// If secondary do not get heartbeat or primary do not get secondary's response,
    /// then mark this pg Inactive.
    pub(super) async fn heartbeat(&self) {
        loop {
            sleep(Duration::from_secs(1)).await;

            let mut pgs = self.inner.pgs.lock().await;
            pgs.iter_mut()
                .filter(|(_, info)| {
                    info.state == PgState::Active
                        && info
                            .heartbeat_ts
                            .map_or(true, |t| t.elapsed() > Duration::from_secs(3))
                })
                .for_each(|(pgid, info)| {
                    warn!(
                        "Pg {} do not get heartbeat for long time, being marked Inconsistent",
                        pgid
                    );
                    info.state = PgState::Inconsistent;
                });
            let mut requests = {
                pgs.iter()
                    .filter(|(_, info)| {
                        // Only send heartbeat if self is primary.
                        info.primary && info.state == PgState::Active
                    })
                    .map(|(pgid, _)| {
                        let this = self.clone();
                        let pgid = *pgid;
                        async move { (pgid, this.heartbeat_for(pgid).await) }
                    })
                    .collect::<FuturesUnordered<_>>()
            };
            drop(pgs);
            while let Some((pgid, res)) = requests.next().await {
                if let Err(err) = res {
                    error!("Error occuring when heartbeat pg {}: {}", pgid, err);
                }
            }
        }
    }

    async fn heartbeat_for(&self, pgid: PgId) -> Result<()> {
        let target_map = self.get_target_map();
        let target_addrs = self.distributor.locate(pgid, &target_map);
        let mut requests = target_addrs
            .into_iter()
            .filter(|addr| *addr != self.local_addr())
            .map(|addr| {
                let net = NetLocalHandle::current();
                async move {
                    (
                        addr,
                        net.call_timeout(
                            addr,
                            PgHeartbeat {
                                pgid,
                                epoch: self.get_epoch(),
                            },
                            PG_HEARTBEAT_TIMEOUT,
                        )
                        .await,
                    )
                }
            })
            .collect::<FuturesOrdered<_>>();
        while let Some((addr, res)) = requests.next().await {
            if matches!(res, Err(_) | Ok(Err(_))) {
                warn!("Heartbeat pg {} with {} get: {:?}", pgid, addr, res);
            }
            res??;
        }
        self.inner.update_pg_heartbeat_ts(pgid).await;
        Ok(())
    }
}
