use super::{PgState, ReliableCtl, Result};
use crate::call_timeout_retry;
use crate::constant::{PG_HEARTBEAT_TIMEOUT, RETRY_TIMES};
use crate::rpc::PgHeartbeat;
use crate::{service::Store, PgId};
use futures::stream::{FuturesOrdered, FuturesUnordered, StreamExt};
use log::{debug, error, warn};
use madsim::time::sleep;
use std::time::Duration;

impl<T> ReliableCtl<T>
where
    T: Store + Sync + Send + 'static,
{
    /// Primary will periodically send heartbeat to secondary.
    /// If secondary do not get heartbeat or primary do not get secondary's response,
    /// then mark this pg Inactive.
    pub(super) async fn background_heartbeat(&self) {
        loop {
            sleep(Duration::from_secs(1)).await;

            let mut requests = {
                let mut pgs = self.inner.pgs.lock().unwrap();
                // 1. find those `Active` pgs do not get heartbeart more than 3 seconds
                // 2. mark them Inconsistent
                pgs.iter_mut()
                    .filter(|(_, info)| {
                        info.state == PgState::Active
                            && info
                                .heartbeat_ts
                                .map_or(true, |t| t.elapsed() > Duration::from_secs(3))
                    })
                    .for_each(|(pgid, info)| {
                        warn!("Pg {} heartbeat failed, being marked Inconsistent", pgid);
                        info.state = PgState::Inconsistent;
                    });
                // Send heartbeat requests to peers (guided by primary)
                // *Important* : only `Active` pg need heartbeat.
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
        let mut requests =
            target_addrs
                .into_iter()
                .filter(|addr| *addr != self.local_addr())
                .map(|addr| {
                    let request = PgHeartbeat {
                        pgid,
                        epoch: self.get_epoch(),
                    };
                    async move {
                        call_timeout_retry(addr, request, PG_HEARTBEAT_TIMEOUT, RETRY_TIMES).await
                    }
                })
                .collect::<FuturesOrdered<_>>();
        while let Some(res) = requests.next().await {
            res??;
        }
        self.inner.update_pg_heartbeat_ts(pgid);
        Ok(())
    }
}
