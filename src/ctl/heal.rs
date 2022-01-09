//! Healing procedure is guided by the most up-to-date server instead of the laggy server.
//! Because the laggy server may lose some [Put](crate::rpc::Put) request,
//!  and there is no way for it to find those keys.
//! 
//! The solution is when laggy server detect stale pgs by peering procedure, then
//! it will send heal jobs to the most up-to-date server.
//! Then this up-to-date server start handling the healing job one by one.
//! 
//! We use a mpsc channel to accommodate the HealJob s for each server's healing background procedure.

use super::{PgInfo, PgState, ReliableCtl};
use crate::{service::Store, PgId};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealJob {
    pgid: PgId,
    addr: SocketAddr,
}

impl<T> ReliableCtl<T>
where
    T: Store + Sync + Send + 'static,
{
    // Healing the stale pg by scanning all keys and get the latest version from peers
    pub(super) async fn heal_for(&self, job: HealJob) {
        todo!()
    }
}
