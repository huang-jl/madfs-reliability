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

pub struct SimpleHashDistributor<const N: usize>;

impl<const N: usize> Distributor<N> for SimpleHashDistributor<N> {
    fn locate(&self, pgid: PgId, target_map: &TargetMap) -> [SocketAddr; N] {
        let mut ans = [None; N];
        let mut start = 0;
        let mut count = 0;
        while count < N {
            let mut hasher = AHasher::default();
            pgid.hash(&mut hasher);
            start.hash(&mut hasher);
            let host_id = hasher.finish() as usize % target_map.len();
            // Only consider active server
            // TODO: consider DownIn server
            // 1. Check if target is active
            // 2. Check if host is duplicated
            if target_map.map[host_id].is_active()
                && ans
                    .iter()
                    .filter(|item| item.is_some())
                    .find(|item| **item == target_map.map[host_id].get_addr())
                    .is_none()
            {
                ans[count] = target_map.map[host_id].get_addr();
                count += 1;
            }
            start += 1;
        }
        ans.iter()
            .map(|item| item.unwrap())
            .collect::<Vec<_>>()
            .try_into()
            .unwrap()
    }

    fn assign_pgid(&self, key: &[u8], pg_map: &PgMap) -> PgId {
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        let pg_num = pg_map.pg_num();
        assert_eq!(pg_num & (pg_num - 1), 0);
        hasher.finish() as usize & pg_num
    }
}

#[cfg(test)]
mod test {
    use crate::constant::REPLICA_SIZE;
    use crate::distributor::{Distributor, SimpleHashDistributor};
    use crate::monitor::{TargetInfo, TargetMap, TargetState};
    use crate::test::common::*;
    use rand::{seq::SliceRandom, thread_rng};

    fn init(server_num: usize, shuffle: bool) -> (TargetMap, SimpleHashDistributor<REPLICA_SIZE>) {
        let mut target_map = TargetMap::empty();
        target_map.map = (0..server_num)
            .map(|id| match id % 4 {
                0 => TargetInfo::new(id as _, TargetState::UpIn(gen_server_addr(id))),
                1 => TargetInfo::new(id as _, TargetState::UpOut(gen_server_addr(id))),
                2 => TargetInfo::new(id as _, TargetState::DownIn),
                3 => TargetInfo::new(id as _, TargetState::DownOut),
                _ => unreachable!(),
            })
            .collect();

        if shuffle {
            target_map.map.shuffle(&mut thread_rng());
        }

        (target_map, SimpleHashDistributor::<REPLICA_SIZE>)
    }

    #[test]
    fn correct_location() {
        const PG_NUM: usize = 256;
        let (target_map, distributor) = init(100, true);

        for pgid in 0..PG_NUM {
            let mut target_addrs = distributor.locate(pgid, &target_map);
            target_addrs.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
            println!("Target addrs = {:?}", target_addrs);
            for (ele1, ele2) in target_addrs.iter().zip(target_addrs.iter().skip(1)) {
                assert_ne!(ele1, ele2);
            }
            for target_addr in target_addrs.iter() {
                let target_info = target_map
                    .map
                    .iter()
                    .find(|info| match info.get_addr() {
                        Some(addr) => addr == *target_addr,
                        None => false,
                    })
                    .unwrap();
                assert!(target_info.is_active());
            }
        }
    }

    #[test]
    fn deterministic() {
        const TIMES: usize = 256;
        for _ in 0..TIMES {
            let pgid = rand::random();
            let (target_map, distributor) = init(100, true);
            let target_addrs = distributor.locate(pgid, &target_map);
            for _ in 0..10 {
                let target_addrs2 = distributor.locate(pgid, &target_map);
                assert_eq!(target_addrs, target_addrs2);
            }
        }
    }
}
