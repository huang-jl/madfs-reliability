use std::net::SocketAddr;
use super::{PgInfo, PgState, ReliableCtl};
use crate::{
    constant::*,
    rpc::{Recover, RecoverRes},
    service::Store,
    Error, PgId, Result,
};
use log::warn;
use madsim::net::NetLocalHandle;

impl<T> ReliableCtl<T>
where
    T: Store + Sync + Send + 'static,
{
    /// For now: asking recovery pg data from primary
    pub(super) async fn recover_for(&self, pgid: PgId, primary: SocketAddr) -> Result<()> {
        // 1. change the state of local pg
        {
            let mut inner = self.inner.lock().await;
            inner.update_pg_info(pgid, |mut pg_info| {
                pg_info.state = PgState::Absent;
                pg_info
            });
        }
        // 2. get whole pg data from primary
        let net = NetLocalHandle::current();
        let mut response: Option<RecoverRes> = None;
        for _ in 0..RECOVER_RETRY {
            let request = Recover(pgid);
            match net.call_timeout(primary, request, RECOVER_TIMEOUT).await {
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
