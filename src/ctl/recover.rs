use super::{PgInfo, PgState, ReliableCtl};
use crate::{
    constant::*,
    rpc::{Recover, RecoverRes},
    service::Store,
    Error, PgId, Result,
};
use log::{error, warn};
use madsim::net::NetLocalHandle;

impl<T> ReliableCtl<T>
where
    T: Store + Sync + Send + 'static,
{
    /// For now: check and recover pgid one by one
    pub(super) async fn background_recovery(&self) {
        let mut watch_version = self.get_target_map().get_version();
        loop {
            // wait until target map is updated
            self.monitor_client
                .watch_for_target_map(Some(watch_version))
                .await;
            // scan each pgid to check whether lacking anyone
            for pgid in (0..PG_NUM)
                .into_iter()
                .filter(|&pgid| self.is_responsible_pg(pgid))
            {
                let local_info = self.inner.lock().await.get_pg_info(pgid);
                match local_info.state {
                    PgState::Active => {}
                    PgState::Absent | PgState::Inactive => {
                        warn!("Start recovering pg {}", pgid);
                        if let Err(err) = self.recover_for(pgid).await {
                            error!("Error occur when recovering pg {}: {}", pgid, err);
                            continue;
                        }
                    }
                    PgState::Stale | PgState::Unprepared => {
                        // For now we did not cares about stale or unprepraed state in recovery
                    }
                }
            }
            watch_version = self.get_target_map().get_version();
        }
    }

    /// For now: asking recovery pg data from primary
    async fn recover_for(&self, pgid: PgId) -> Result<()> {
        // 1. change the state of local pg
        {
            let mut inner = self.inner.lock().await;
            inner.update_pg_info(pgid, |mut pg_info| {
                pg_info.state = PgState::Absent;
                pg_info
            });
        }
        // 2. get target address and check responsibility
        let target_addrs = self.distributor.locate(pgid, &self.get_target_map());
        if !target_addrs.iter().any(|addr| *addr == self.local_addr()) {
            return Err(Error::WrongTarget);
        }
        // 3. get whole pg data from primary
        let net = NetLocalHandle::current();
        let mut response: Option<RecoverRes> = None;
        for _ in 0..RECOVER_RETRY {
            let request = Recover(pgid);
            match net
                .call_timeout(target_addrs[0], request, RECOVER_TIMEOUT)
                .await
            {
                Ok(res) => response = Some(res?),
                Err(err) => {
                    warn!("Send Recover request get error: {}", err);
                    continue;
                }
            }
        }
        // 3. push pg data into local and update pg_info
        match response {
            Some(RecoverRes { data, version }) => {
                let mut inner = self.inner.lock().await;
                inner.service.push_pg_data(pgid, data);
                inner.update_pg_info(pgid, |_| PgInfo {
                    state: PgState::Active,
                    version,
                });
                Ok(())
            }
            None => Err(Error::NetworkError(format!(
                "Recover pg {} still error after retry for {} times",
                pgid, RECOVER_RETRY
            ))),
        }
    }
}
