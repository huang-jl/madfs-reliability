use std::time::Duration;

use super::{PgState, ReliableCtl};
use crate::{
    constant::*,
    rpc::{Recover, RecoverRes},
    service::Store,
    Error, PgId, Result,
};
use log::{error, warn};
use madsim::{net::NetLocalHandle, time::sleep};

impl<T> ReliableCtl<T>
where
    T: Store + Sync + Send + 'static,
{
    /// For now: check and recover pgid one by one
    pub(super) async fn background_recovery(&self) {
        loop {
            for pgid in (0..PG_NUM)
                .into_iter()
                .filter(|&pgid| self.is_responsible_pg(pgid))
            {
                let pg_state = self.inner.lock().await.get_pg_state(pgid);
                if pg_state == PgState::Absent {
                    warn!("Start recovering pg {}", pgid);
                    if let Err(err) = self.recover_for(pgid).await {
                        error!("Error occur when recovering pg {}: {}", pgid, err);
                        continue;
                    }
                }
            }
            sleep(Duration::from_millis(1500)).await;
        }
    }

    /// For now: asking recovery pg data from the first server
    async fn recover_for(&self, pgid: PgId) -> Result<()> {
        // 1. get target address and check responsibility
        let target_addrs = self.distributor.locate(pgid, &self.get_target_map());
        if !target_addrs.iter().any(|addr| *addr == self.local_addr()) {
            return Err(Error::WrongTarget);
        }
        let target_addr = target_addrs
            .into_iter()
            .filter(|addr| *addr != self.local_addr())
            .next()
            .unwrap();
        // 2. get whole pg data from the first server
        let net = NetLocalHandle::current();
        let mut response: Option<RecoverRes> = None;
        for _ in 0..RECOVER_RETRY {
            let request = Recover(pgid);
            match net
                .call_timeout(target_addr, request, RECOVER_TIMEOUT)
                .await
            {
                Ok(res) => response = Some(res?),
                Err(err) => {
                    warn!("Send Recover request get error: {}", err);
                    continue;
                }
            }
        }
        // 3. push pg data into local and update local pg state and version
        match response {
            Some(RecoverRes { data, version }) => {
                let mut inner = self.inner.lock().await;
                inner.service.push_pg_data(pgid, data);
                inner.set_pg_state(pgid, PgState::Active);
                inner.set_pg_version(pgid, version);
                inner.persist().await;
                Ok(())
            }
            None => Err(Error::NetworkError(format!(
                "Recover pg {} still error after retry for {} times",
                pgid, RECOVER_RETRY
            ))),
        }
    }
}
