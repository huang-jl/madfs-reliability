use crate::monitor::{PgMap, TargetMap};
use ahash::AHasher;
use std::{
    hash::{Hash, Hasher},
    net::SocketAddr,
    str::FromStr,
};

pub type PgId = usize;

pub trait Distributor<const REPLICA_SIZE: usize>: Send + Sync {
    fn assign_pgid(&self, key: &[u8], pg_map: &PgMap) -> PgId;
    fn locate(&self, pgid: PgId, target_map: &TargetMap) -> [SocketAddr; REPLICA_SIZE];
}

struct SimpleHashDistributor<const N: usize>;

impl<const N: usize> Distributor<N> for SimpleHashDistributor<N> {
    fn locate(&self, pgid: PgId, target_map: &TargetMap) -> [SocketAddr; N] {
        let mut hasher = AHasher::default();
        let mut ans = ["0.0.0.0:1".parse().unwrap(); N];
        let mut start = 0;
        let mut count = 0;
        while count < N {
            pgid.hash(&mut hasher);
            start.hash(&mut hasher);
            let host_id = hasher.finish() as usize;
            match target_map.map[host_id].get_addr() {
                Some(addr) => {
                    ans[count] = addr;
                    count += 1;
                }
                None => {}
            }
            start += 1;
        }
        ans
    }

    fn assign_pgid(&self, key: &[u8], pg_map: &PgMap) -> PgId {
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        let pg_num = pg_map.pg_num();
        assert_eq!(pg_num & (pg_num - 1), 0);
        hasher.finish() as usize & pg_num
    }
}
