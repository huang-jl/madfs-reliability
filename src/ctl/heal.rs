//! Healing procedure is guided by the most up-to-date server instead of the laggy server.
//! Because the laggy server may lose some [Put](crate::rpc::Put) request,
//!  and there is no way for it to find those keys.
//!
//! The solution is when laggy server detect stale pgs by peering procedure, then
//! it will send heal jobs to the most up-to-date server.
//! Then this up-to-date server start handling the healing job one by one.
//!
//! We use a mpsc channel to accommodate the HealJob s for each server's healing background procedure.

use super::{PgState, ReliableCtl};
use crate::constant::HEAL_REQ_TIMEOUT;
use crate::rpc::HealReq;
use crate::service::Value;
use crate::{service::Store, PgId};
use log::warn;
use madsim::net::NetLocalHandle;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealJob {
    pub pgid: PgId,
    pub addr: SocketAddr,
    pub keys: Vec<(String, u64)>,
}

impl<T> ReliableCtl<T>
where
    T: Store + Sync + Send + 'static,
{
    // Healing the stale pg by scanning all keys and get the latest version from peers
    pub(super) async fn heal_for(&self, job: HealJob) {
        let HealJob { pgid, addr, keys } = job;
        let keys = keys.into_iter().collect::<HashMap<_, _>>();
        let request = {
            let inner = self.inner.lock().await;
            let local_keys = inner.service.get_heal_data(pgid);
            // Only keep those in `local_keys`:
            // 1. do not exist in `keys`
            // 2. version is greater than version in `keys`
            let res = local_keys
                .into_iter()
                .filter(|(key, local_ver)| keys.get(key).map_or(true, |ver| local_ver > ver))
                .map(|(key, ver)| {
                    let value = Value {
                        data: inner.service.get(&key).unwrap(),
                        version: ver,
                    };
                    (key, value)
                })
                .collect::<Vec<_>>();
            HealReq {
                pgid,
                pg_ver: inner.get_pg_version(pgid),
                data: res,
            }
        };
        let net = NetLocalHandle::current();
        match net.call_timeout(addr, request, HEAL_REQ_TIMEOUT).await {
            Err(err) => warn!("Send Heal Request to {} get error: {}", addr, err),
            Ok(Err(err)) => warn!("Send Heal Request to {} get error: {}", addr, err),
            _ => {}
        }
    }
}
